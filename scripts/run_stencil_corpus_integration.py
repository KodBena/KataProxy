#!/usr/bin/env python3
"""Run the stencil corpus integration suite against a disposable Redis server."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis


ROOT = Path(__file__).resolve().parents[1]


def _free_loopback_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _stop_process(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=5.0)
    except (OSError, subprocess.TimeoutExpired):
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except OSError:
            pass
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            pass


def _run_logged(
    command: list[str],
    *,
    log_path: Path,
    environment: dict[str, str],
    timeout_seconds: float,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as output:
        output.write("$ " + " ".join(command) + "\n\n")
        output.flush()
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=(os.name == "posix"),
        )
        try:
            return int(process.wait(timeout=timeout_seconds))
        except subprocess.TimeoutExpired:
            _stop_process(process)
            output.write(
                f"\nCommand timed out after {timeout_seconds:.1f} seconds.\n"
            )
            output.flush()
            return 124


def _junit_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        return {"parse_error": str(exc)}
    suites = (
        [root] if root.tag == "testsuite"
        else list(root.iter("testsuite"))
    )
    return {
        "tests": len(list(root.iter("testcase"))),
        "failures": sum(int(suite.attrib.get("failures", "0")) for suite in suites),
        "errors": sum(int(suite.attrib.get("errors", "0")) for suite in suites),
        "skipped": sum(int(suite.attrib.get("skipped", "0")) for suite in suites),
        "time_seconds": sum(float(suite.attrib.get("time", "0")) for suite in suites),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default=".artifacts/stencil-corpus",
        help="directory for JSON reports, JUnit XML, and process logs",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=int(os.environ.get("STENCIL_BENCHMARK_ITERATIONS", "1000")),
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=int(os.environ.get("STENCIL_BENCHMARK_WARMUP", "100")),
    )
    args = parser.parse_args(argv)
    if args.iterations <= 0 or args.warmup < 0:
        parser.error("iterations must be positive and warmup non-negative")

    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    junit_path = output_dir / "integration-junit.xml"
    pytest_log = output_dir / "integration.log"
    benchmark_log = output_dir / "benchmark.log"
    benchmark_path = output_dir / "benchmark-report.json"
    redis_log = output_dir / "redis.log"
    report_path = output_dir / "integration-report.json"

    report: dict[str, Any] = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "suite": "stencil-corpus-integration",
        "redis": {
            "isolation": "dedicated redis-server process",
            "database": 0,
            "persistence": "disabled; temporary server directory",
        },
        "benchmark": {
            "iterations": args.iterations,
            "warmup": args.warmup,
            "threshold_status": "pending_engineering_decision",
        },
        "artifacts": {
            "integration_junit": str(junit_path.relative_to(ROOT)),
            "integration_log": str(pytest_log.relative_to(ROOT)),
            "benchmark_json": str(benchmark_path.relative_to(ROOT)),
            "benchmark_log": str(benchmark_log.relative_to(ROOT)),
            "redis_log": str(redis_log.relative_to(ROOT)),
        },
    }

    process: subprocess.Popen[str] | None = None
    redis_client: redis.Redis | None = None
    exit_code = 1
    try:
        redis_server = shutil.which("redis-server")
        if redis_server is None:
            raise RuntimeError(
                "redis-server is required; install Redis 6.2 or newer "
                "(the integration suite requires XAUTOCLAIM)"
            )

        with tempfile.TemporaryDirectory(
            prefix="kataproxy-stencil-corpus-redis-",
        ) as temporary:
            temporary_root = Path(temporary)
            port = _free_loopback_port()
            process = subprocess.Popen(
                [
                    redis_server,
                    "--bind", "127.0.0.1",
                    "--protected-mode", "yes",
                    "--port", str(port),
                    "--save", "",
                    "--appendonly", "no",
                    "--dir", str(temporary_root),
                    "--daemonize", "no",
                    "--loglevel", "notice",
                ],
                cwd=ROOT,
                stdout=redis_log.open("w", encoding="utf-8"),
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=(os.name == "posix"),
            )
            redis_client = redis.Redis(
                host="127.0.0.1",
                port=port,
                db=0,
                socket_connect_timeout=0.2,
                socket_timeout=0.5,
                decode_responses=False,
            )
            readiness_deadline = time.monotonic() + 10.0
            while True:
                if process.poll() is not None:
                    raise RuntimeError(
                        f"isolated redis-server exited with code {process.returncode}"
                    )
                try:
                    redis_client.ping()
                    break
                except redis.RedisError:
                    if time.monotonic() >= readiness_deadline:
                        raise RuntimeError(
                            "isolated redis-server did not become ready within 10 seconds"
                        )
                    time.sleep(0.025)

            info = redis_client.info(section="server")
            redis_url = f"redis://127.0.0.1:{port}/0"
            test_environment = os.environ.copy()
            # These variables are read while production modules import. They
            # must be set before pytest imports the proxy/stencil modules.
            test_environment.update({
                "KATAPROXY_TEST_REDIS_URL": redis_url,
                "KATAPROXY_REDIS_HOST": "127.0.0.1",
                "KATAPROXY_REDIS_PORT": str(port),
                "KATAPROXY_REDIS_DB": "0",
                "KATAPROXY_STENCIL_CORPUS_DB": str(
                    temporary_root / "stencil-corpus.sqlite3"
                ),
            })
            test_environment.pop("KATAPROXY_STENCIL_CORPUS_ENABLED", None)
            report["redis"].update({
                "version": info.get("redis_version"),
                "host": "127.0.0.1",
                "port": port,
                "url": redis_url,
                "developer_or_production_redis_touched": False,
            })

            pytest_command = [
                sys.executable,
                "-m", "pytest",
                "-q",
                "tests/test_stencil_corpus_integration.py",
                f"--junitxml={junit_path}",
            ]
            pytest_exit = _run_logged(
                pytest_command,
                log_path=pytest_log,
                environment=test_environment,
                timeout_seconds=300.0,
            )
            report["integration"] = {
                "exit_code": pytest_exit,
                "junit": _junit_summary(junit_path),
                "log": str(pytest_log.relative_to(ROOT)),
            }

            benchmark_command = [
                sys.executable,
                "benchmark/benchmark_stencil_corpus_capture.py",
                "--redis-url", redis_url,
                "--iterations", str(args.iterations),
                "--warmup", str(args.warmup),
                "--output-json", str(benchmark_path),
            ]
            benchmark_exit = _run_logged(
                benchmark_command,
                log_path=benchmark_log,
                environment=test_environment,
                timeout_seconds=180.0,
            )
            benchmark_data: dict[str, Any] | None = None
            if benchmark_path.exists():
                try:
                    parsed = json.loads(benchmark_path.read_text(encoding="utf-8"))
                    if isinstance(parsed, dict):
                        benchmark_data = parsed
                except (OSError, json.JSONDecodeError):
                    benchmark_data = None
            report["benchmark"] = {
                "exit_code": benchmark_exit,
                "result_json": (
                    str(benchmark_path.relative_to(ROOT))
                    if benchmark_path.exists() else None
                ),
                "comparison": (
                    benchmark_data.get("comparison")
                    if benchmark_data is not None else None
                ),
                "environment": (
                    benchmark_data.get("environment")
                    if benchmark_data is not None else None
                ),
                "acceptance": (
                    benchmark_data.get("acceptance")
                    if benchmark_data is not None else {
                        "threshold_status": "pending_engineering_decision"
                    }
                ),
            }
            exit_code = 0 if pytest_exit == 0 and benchmark_exit == 0 else 1

    except KeyboardInterrupt:
        report["error"] = "interrupted"
        exit_code = 130
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        exit_code = 2
    finally:
        if redis_client is not None:
            try:
                redis_client.close()
            except Exception:
                pass
        _stop_process(process)
        report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["exit_code"] = exit_code
        report["status"] = "passed" if exit_code == 0 else "failed"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print(
        "Stencil corpus integration: "
        + ("PASS" if exit_code == 0 else "FAIL")
    )
    integration = report.get("integration")
    if isinstance(integration, dict):
        summary = integration.get("junit")
        if isinstance(summary, dict):
            print(
                "  Integration tests: "
                f"{summary.get('tests', 0)} total, "
                f"{summary.get('failures', 0)} failures, "
                f"{summary.get('errors', 0)} errors"
            )
        else:
            print(f"  Integration pytest exit code: {integration.get('exit_code')}")
    benchmark = report.get("benchmark")
    if isinstance(benchmark, dict):
        comparison = benchmark.get("comparison")
        if isinstance(comparison, dict):
            print(
                "  Benchmark median (disabled/enabled): "
                f"{comparison.get('capture_disabled_median_ms', float('nan')):.4f} / "
                f"{comparison.get('capture_enabled_median_ms', float('nan')):.4f} ms"
            )
            print(
                "  Benchmark p95 (disabled/enabled): "
                f"{comparison.get('capture_disabled_p95_ms', float('nan')):.4f} / "
                f"{comparison.get('capture_enabled_p95_ms', float('nan')):.4f} ms"
            )
            print(
                "  Overhead median/p95: "
                f"{comparison.get('median_overhead_ms', float('nan')):.4f} / "
                f"{comparison.get('p95_overhead_ms', float('nan')):.4f} ms"
            )
        else:
            print(f"  Benchmark exit code: {benchmark.get('exit_code')}")
    if "error" in report:
        print(f"  Error: {report['error']}")
    print(f"  Report: {report_path.relative_to(ROOT)}")
    print(f"  Logs: {pytest_log.relative_to(ROOT)}, {benchmark_log.relative_to(ROOT)}, {redis_log.relative_to(ROOT)}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
