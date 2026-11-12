"""Supervise the production stencil-corpus materializer child process.

The worker remains the existing materializer CLI. Keeping it in a child process
isolates blocking Redis/SQLite work from the proxy event loop and leaves the
CLI independently usable for recovery and tests.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import sys
from pathlib import Path

from transformers.stencil_corpus_publisher import default_corpus_database_path

logger = logging.getLogger("kataproxy.stencil_corpus_supervisor")

_READY_MARKER = b"KATAPROXY_STENCIL_CORPUS_READY\n"
_PROGRESS_PREFIX = "KATAPROXY_STENCIL_CORPUS_PROGRESS "


class StencilCorpusMaterializerSupervisor:
    """Own one materializer process, restart failures, and stop within a bound."""

    def __init__(
        self,
        database_path: str | Path | None = None,
        *,
        poll_interval_seconds: float = 1.0,
        block_ms: int = 100,
        startup_timeout_seconds: float = 10.0,
        shutdown_timeout_seconds: float = 5.0,
        restart_initial_delay_seconds: float = 1.0,
        restart_max_delay_seconds: float = 30.0,
    ) -> None:
        for name, value in (
            ("poll_interval_seconds", poll_interval_seconds),
            ("startup_timeout_seconds", startup_timeout_seconds),
            ("shutdown_timeout_seconds", shutdown_timeout_seconds),
            ("restart_initial_delay_seconds", restart_initial_delay_seconds),
            ("restart_max_delay_seconds", restart_max_delay_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if restart_max_delay_seconds < restart_initial_delay_seconds:
            raise ValueError(
                "restart_max_delay_seconds must be >= restart_initial_delay_seconds"
            )
        if isinstance(block_ms, bool) or not isinstance(block_ms, int) or block_ms < 0:
            raise ValueError("block_ms must be a non-negative integer")

        self.database_path = (
            default_corpus_database_path()
            if database_path is None else Path(database_path).expanduser()
        )
        self.poll_interval_seconds = poll_interval_seconds
        self.block_ms = block_ms
        self.startup_timeout_seconds = startup_timeout_seconds
        self.shutdown_timeout_seconds = shutdown_timeout_seconds
        self.restart_initial_delay_seconds = restart_initial_delay_seconds
        self.restart_max_delay_seconds = restart_max_delay_seconds

        self._task: asyncio.Task[None] | None = None
        self._process: asyncio.subprocess.Process | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._started_event: asyncio.Event | None = None
        self._restart_event: asyncio.Event | None = None
        self._materialization_event: asyncio.Event | None = None
        self._startup_error: Exception | None = None
        self._restart_count = 0
        self._last_exit_code: int | None = None
        self._last_counts: dict[str, int] = {}

    @property
    def is_running(self) -> bool:
        return (
            self._task is not None
            and not self._task.done()
            and self._process is not None
            and self._process.returncode is None
        )

    @property
    def worker_pid(self) -> int | None:
        if self._process is None or self._process.returncode is not None:
            return None
        return self._process.pid

    @property
    def restart_count(self) -> int:
        return self._restart_count

    @property
    def last_exit_code(self) -> int | None:
        return self._last_exit_code

    @property
    def last_counts(self) -> dict[str, int]:
        return dict(self._last_counts)

    async def start(self) -> None:
        """Start supervision and wait for the worker's SQLite-ready handshake."""
        if self._task is not None and not self._task.done():
            raise RuntimeError("stencil corpus materializer supervisor is already running")

        self._stop_event = asyncio.Event()
        self._started_event = asyncio.Event()
        self._restart_event = asyncio.Event()
        self._materialization_event = asyncio.Event()
        self._startup_error = None
        self._process = None
        self._stdout_task = None
        self._last_counts = {}
        self._task = asyncio.create_task(
            self._run(), name="stencil-corpus-materializer-supervisor",
        )
        try:
            await asyncio.wait_for(
                self._started_event.wait(),
                timeout=self.startup_timeout_seconds + 1.0,
            )
        except asyncio.TimeoutError as exc:
            await self.stop()
            raise RuntimeError(
                "stencil corpus materializer did not report startup readiness"
            ) from exc

        if self._startup_error is not None:
            error = self._startup_error
            await self.stop()
            raise RuntimeError(
                "stencil corpus materializer failed before startup readiness"
            ) from error
        if self._process is None:
            await self.stop()
            raise RuntimeError(
                "stencil corpus materializer exited before startup readiness"
            )
        logger.info(
            "Stencil corpus materializer supervised (pid=%s, database=%s)",
            self.worker_pid,
            self.database_path,
        )

    async def _spawn_worker(self) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "transformers.stencil_corpus_materializer",
            "--database",
            str(self.database_path),
            "--poll-interval",
            str(self.poll_interval_seconds),
            "--block-ms",
            str(self.block_ms),
            "--supervised-ready",
            stdout=asyncio.subprocess.PIPE,
            stderr=None,
        )
        try:
            if process.stdout is None:
                raise RuntimeError("materializer stdout readiness channel is unavailable")
            ready_line = await asyncio.wait_for(
                process.stdout.readline(),
                timeout=self.startup_timeout_seconds,
            )
            if ready_line != _READY_MARKER:
                try:
                    exit_code = await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    exit_code = None
                raise RuntimeError(
                    "materializer exited before SQLite readiness "
                    f"(exit_code={exit_code}, output={ready_line!r})"
                )
        except BaseException:
            if process.returncode is None:
                process.kill()
                try:
                    await asyncio.wait_for(process.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    logger.critical(
                        "Could not reap failed stencil corpus materializer startup process"
                    )
            raise
        return process

    async def _consume_stdout(
        self, process: asyncio.subprocess.Process,
    ) -> None:
        if process.stdout is None:
            return
        while True:
            line = await process.stdout.readline()
            if not line:
                return
            decoded = line.decode("utf-8", errors="replace").strip()
            if not decoded.startswith(_PROGRESS_PREFIX):
                continue
            try:
                payload = json.loads(decoded[len(_PROGRESS_PREFIX):])
            except json.JSONDecodeError:
                logger.warning("Ignoring malformed materializer progress message: %s", decoded)
                continue
            if not isinstance(payload, dict):
                continue
            self._last_counts = {
                str(key): value
                for key, value in payload.items()
                if isinstance(value, int) and not isinstance(value, bool)
            }
            if self._materialization_event is not None:
                self._materialization_event.set()

    async def _wait_before_restart(self, delay_seconds: float) -> bool:
        stop_event = self._stop_event
        if stop_event is None:
            return True
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
            return True
        except asyncio.TimeoutError:
            return stop_event.is_set()

    async def _run(self) -> None:
        started_event = self._started_event
        stop_event = self._stop_event
        if started_event is None or stop_event is None:
            return

        first_start = True
        delay_seconds = self.restart_initial_delay_seconds
        while not stop_event.is_set():
            try:
                process = await self._spawn_worker()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if first_start:
                    self._startup_error = exc
                    started_event.set()
                    logger.exception(
                        "Stencil corpus materializer failed during startup"
                    )
                    return
                logger.exception(
                    "Stencil corpus materializer restart attempt failed; "
                    "retrying in %.1f seconds",
                    delay_seconds,
                )
                if await self._wait_before_restart(delay_seconds):
                    return
                delay_seconds = min(
                    delay_seconds * 2.0, self.restart_max_delay_seconds,
                )
                continue

            self._process = process
            self._stdout_task = asyncio.create_task(
                self._consume_stdout(process),
                name="stencil-corpus-materializer-output",
            )
            if first_start:
                first_start = False
                started_event.set()
            else:
                self._restart_count += 1
                if self._restart_event is not None:
                    self._restart_event.set()
                logger.warning(
                    "Stencil corpus materializer restarted (restart=%d, pid=%s)",
                    self._restart_count,
                    process.pid,
                )
                delay_seconds = self.restart_initial_delay_seconds

            exit_code = await process.wait()
            if self._stdout_task is not None:
                await self._stdout_task
                self._stdout_task = None
            self._last_exit_code = exit_code
            if self._process is process:
                self._process = None
            if stop_event.is_set():
                return

            logger.error(
                "Stencil corpus materializer exited unexpectedly (exit_code=%s); "
                "restarting in %.1f seconds",
                exit_code,
                delay_seconds,
            )
            if await self._wait_before_restart(delay_seconds):
                return
            delay_seconds = min(
                delay_seconds * 2.0, self.restart_max_delay_seconds,
            )

    async def wait_for_restart(self, timeout_seconds: float = 10.0) -> bool:
        """Wait for an unexpected worker exit to be followed by a new ready worker."""
        event = self._restart_event
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return False
        return True

    def clear_restart_event(self) -> None:
        if self._restart_event is not None:
            self._restart_event.clear()

    async def wait_for_materialization(self, timeout_seconds: float = 10.0) -> bool:
        """Wait for a worker pass that ingested or replayed corpus events."""
        event = self._materialization_event
        if event is None:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return False
        return True

    def clear_materialization_event(self) -> None:
        if self._materialization_event is not None:
            self._materialization_event.clear()

    async def stop(self) -> None:
        """Request graceful worker shutdown; force-stop only after a bounded wait."""
        stop_event = self._stop_event
        task = self._task
        if task is None:
            return
        if stop_event is not None:
            stop_event.set()

        process = self._process
        if process is not None and process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass

        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=self.shutdown_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Stencil corpus materializer did not stop within %.1f seconds; "
                "sending a forced termination",
                self.shutdown_timeout_seconds,
            )
            process = self._process
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
            except asyncio.TimeoutError:
                logger.critical(
                    "Stencil corpus materializer supervisor did not finish after "
                    "forced worker termination"
                )
                task.cancel()
                await asyncio.wait({task}, timeout=1.0)
        except Exception:
            logger.exception("Error while stopping stencil corpus materializer supervisor")
        finally:
            if task.done():
                self._task = None
                self._process = None
                self._stdout_task = None

    async def restart(self) -> None:
        """Stop this worker and start a fresh one without changing its database."""
        await self.stop()
        await self.start()
