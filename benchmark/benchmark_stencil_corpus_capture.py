#!/usr/bin/env python3
"""Measure synchronous stencil-corpus event-hook overhead.

Run from the repository root against a disposable Redis database:

    python benchmark/benchmark_stencil_corpus_capture.py \
        --redis-url redis://127.0.0.1:6379/15 --iterations 1000 --warmup 100

For a Go-derived workload, pass --corpus-db pointing at a populated
normalized corpus database. Without it, a deterministic connected-component
fixture is used. The benchmark writes only unique temporary stream keys and
deletes them on exit; it never writes the production corpus stream.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import sqlite3
import statistics
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from transformers import stencil_cover_enricher as stencil
from transformers.stencil_corpus_events import (
    make_instance_seen_event,
    serialize_event,
)
from transformers.stencil_corpus_publisher import (
    CORPUS_STREAM_KEY,
    CorpusEventPublisher,
    SQLiteEventSpool,
    publish_instance_seen,
)
from transformers.stencil_solver_api import STENCIL_CANONICALIZATION_VERSION
from transformers.stencil_solver_runtime import canonical_problem_from_key


def _fallback_keys() -> tuple[bytes, ...]:
    components = (
        ((0, 0),),
        ((0, 0), (0, 1)),
        ((0, 0), (1, 0), (1, 1)),
        ((0, 0), (0, 1), (0, 2), (1, 1)),
        ((0, 0), (0, 1), (1, 0), (1, 1)),
        ((0, 0), (0, 1), (0, 2), (1, 0), (1, 1)),
    )
    result: list[bytes] = []
    for cells in components:
        width = max(x for _y, x in cells) + 1
        flat = tuple(y * width + x for y, x in cells)
        key, _frame = stencil._component_shape_key(
            flat, width, stencil.STENCIL_FAMILY,
        )
        result.append(key)
    return tuple(result)


def _decode_key_cells(key: bytes) -> tuple[tuple[int, ...], int]:
    if len(key) < 3:
        raise ValueError("canonical component key is truncated")
    height, width = key[0], key[1]
    expected = (width * height + 7) // 8
    bitmap = key[2:]
    if width <= 0 or height <= 0 or len(bitmap) != expected:
        raise ValueError("canonical component key has invalid dimensions/bitmap")
    cells = tuple(
        index for index in range(width * height)
        if bitmap[index >> 3] & (1 << (index & 7))
    )
    if not cells:
        raise ValueError("canonical component key has no occupied cells")
    return cells, width


def _load_workload(database: str | None) -> tuple[tuple[bytes, ...], str]:
    if database is None:
        return _fallback_keys(), "built-in deterministic connected component fixture"

    family_fingerprint = canonical_problem_from_key(
        _fallback_keys()[0], stencil.STENCIL_FAMILY,
    ).family_fingerprint
    path = Path(database).expanduser()
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """
            SELECT canonical_key
            FROM instances
            WHERE family_fingerprint = ?
              AND canonicalization_version = ?
            ORDER BY sighting_count DESC, last_seen_at_utc DESC
            LIMIT 10000
            """,
            (family_fingerprint, STENCIL_CANONICALIZATION_VERSION),
        ).fetchall()
    keys = tuple(bytes(row[0]) for row in rows)
    if not keys:
        raise ValueError(
            f"no instances for the active stencil family in corpus database {path}"
        )
    return keys, f"SQLite corpus {path} ({len(keys)} available instances)"


class _RedirectedRedis:
    """Route the publisher's fixed stream name to a unique benchmark key."""

    def __init__(self, client: Any, stream_key: str) -> None:
        self.client = client
        self.stream_key = stream_key

    def xadd(self, name: str, fields: dict[bytes, bytes]) -> Any:
        if name != CORPUS_STREAM_KEY:
            raise ValueError(f"unexpected stream name: {name}")
        return self.client.xadd(self.stream_key, fields)


class _FailingRedis:
    def xadd(self, _name: str, _fields: dict[bytes, bytes]) -> Any:
        raise ConnectionError("intentional benchmark fallback")


def _measure(
    action: Callable[[], Any],
    *,
    warmup: int,
    iterations: int,
) -> list[int]:
    for _ in range(warmup):
        action()
    durations: list[int] = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        action()
        durations.append(time.perf_counter_ns() - started)
    return durations


def _summary(durations_ns: Sequence[int]) -> dict[str, float | int]:
    ordered = sorted(durations_ns)
    p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return {
        "observations": len(ordered),
        "median_ms": statistics.median(ordered) / 1_000_000,
        "p95_ms": ordered[p95_index] / 1_000_000,
    }


def _new_event(key: bytes) -> dict[str, Any]:
    problem = canonical_problem_from_key(key, stencil.STENCIL_FAMILY)
    return make_instance_seen_event(problem, key, producer_path="foreground")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--corpus-db", default=None)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    args = parser.parse_args(argv)
    if args.iterations <= 0 or args.warmup < 0:
        parser.error("--iterations must be positive and --warmup non-negative")

    keys, workload_description = _load_workload(args.corpus_db)
    canonical_inputs = tuple(_decode_key_cells(key) for key in keys)
    canonical_cycle = itertools.cycle(canonical_inputs)

    def canonicalization() -> Any:
        cells, width = next(canonical_cycle)
        return stencil._component_shape_key(cells, width, stencil.STENCIL_FAMILY)

    event_cycle = itertools.cycle(keys)
    event_construction = lambda: _new_event(next(event_cycle))
    total = args.iterations + args.warmup
    events = [_new_event(keys[index % len(keys)]) for index in range(total)]
    payloads = [serialize_event(event) for event in events]
    serialization_cycle = itertools.cycle(events)
    serialization = lambda: serialize_event(next(serialization_cycle))
    redis_payload_cycle = itertools.cycle(payloads)
    spool_payload_cycle = itertools.cycle(payloads)

    os.environ.pop("KATAPROXY_STENCIL_CORPUS_ENABLED", None)
    disabled_cycle = itertools.cycle(keys)
    capture_disabled = lambda: publish_instance_seen(
        next(disabled_cycle), stencil.STENCIL_FAMILY, "foreground",
    )

    redis_module = __import__("redis")
    redis_client = redis_module.Redis.from_url(
        args.redis_url, decode_responses=True,
    )
    stream_prefix = f"kataproxy:stencil-cover:benchmark:{uuid.uuid4().hex}"
    direct_stream = stream_prefix + ":direct"
    publisher_stream = stream_prefix + ":publisher"
    redis_client.ping()
    server_info = redis_client.info(section="server")
    persistence: dict[str, Any] = {}
    for setting in ("appendonly", "appendfsync"):
        try:
            persistence.update(redis_client.config_get(setting))
        except Exception as exc:
            persistence[setting] = f"unavailable ({type(exc).__name__})"

    with tempfile.TemporaryDirectory(prefix="kataproxy-stencil-benchmark-") as directory:
        root = Path(directory)
        direct_spool = SQLiteEventSpool(root / "direct-spool.sqlite3")
        fallback_publisher = CorpusEventPublisher(
            redis_client_factory=_FailingRedis,
            sqlite_path=root / "publisher-fallback.sqlite3",
        )
        redirected_publisher = CorpusEventPublisher(
            redis_client_factory=lambda: _RedirectedRedis(redis_client, publisher_stream),
            sqlite_path=root / "unused-spool.sqlite3",
        )
        os.environ["KATAPROXY_STENCIL_CORPUS_ENABLED"] = "1"

        try:
            direct_redis = lambda: redis_client.xadd(
                direct_stream, {b"event": next(redis_payload_cycle)},
            )
            direct_sqlite = lambda: direct_spool.append(next(spool_payload_cycle))
            redis_cycle = itertools.cycle(keys)

            def capture_redis() -> None:
                result = publish_instance_seen(
                    next(redis_cycle), stencil.STENCIL_FAMILY, "foreground",
                    publisher=redirected_publisher,
                )
                if result != "redis_stream":
                    raise RuntimeError(
                        f"expected Redis publication, observed {result!r}"
                    )

            fallback_cycle = itertools.cycle(keys)

            def capture_fallback() -> None:
                result = publish_instance_seen(
                    next(fallback_cycle), stencil.STENCIL_FAMILY, "foreground",
                    publisher=fallback_publisher,
                )
                if result != "sqlite_spool":
                    raise RuntimeError(
                        f"expected SQLite fallback, observed {result!r}"
                    )

            measurements = {
                "canonicalization_only": _summary(_measure(
                    canonicalization, warmup=args.warmup, iterations=args.iterations,
                )),
                "event_construction_only": _summary(_measure(
                    event_construction, warmup=args.warmup, iterations=args.iterations,
                )),
                "serialization_only": _summary(_measure(
                    serialization, warmup=args.warmup, iterations=args.iterations,
                )),
                "redis_xadd_only": _summary(_measure(
                    direct_redis, warmup=args.warmup, iterations=args.iterations,
                )),
                "sqlite_spool_append_only": _summary(_measure(
                    direct_sqlite, warmup=args.warmup, iterations=args.iterations,
                )),
                "capture_disabled_hook": _summary(_measure(
                    capture_disabled, warmup=args.warmup, iterations=args.iterations,
                )),
                "capture_enabled_redis_hook": _summary(_measure(
                    capture_redis, warmup=args.warmup, iterations=args.iterations,
                )),
                "capture_enabled_sqlite_fallback_hook": _summary(_measure(
                    capture_fallback, warmup=args.warmup, iterations=args.iterations,
                )),
            }
            disabled = measurements["capture_disabled_hook"]
            for name in (
                "capture_enabled_redis_hook",
                "capture_enabled_sqlite_fallback_hook",
            ):
                enabled = measurements[name]
                # Difference of aggregate quantiles, not paired per-request deltas.
                enabled["median_over_disabled_ms"] = enabled["median_ms"] - disabled["median_ms"]
                enabled["p95_over_disabled_ms"] = enabled["p95_ms"] - disabled["p95_ms"]
        finally:
            redis_client.delete(direct_stream, publisher_stream)
            redis_client.close()

    print(json.dumps({
        "environment": {
            "python": sys.version.split()[0],
            "redis_version": server_info.get("redis_version"),
            "redis_persistence_config": persistence,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "workload": workload_description,
            "component_keys": len(keys),
            "active_family_fingerprint": canonical_problem_from_key(
                keys[0], stencil.STENCIL_FAMILY,
            ).family_fingerprint,
        },
        "measurements": measurements,
        "interpretation": (
            "These are synchronous corpus-hook measurements, not total KataGo "
            "request latency. Canonicalization is measured separately because "
            "the foreground emits instance_seen after canonicalization."
        ),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
