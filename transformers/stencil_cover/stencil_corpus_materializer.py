"""Materialize Redis Stream and local-spool events into the SQLite corpus.

The order is deliberate: drain this host's SQLite fallback spool first, then
claim stale Redis pending entries, then read new stream entries. SQLite commit
always precedes XACK, so a crash causes a safe receipt-checked replay.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import threading
import uuid
from collections.abc import Mapping
from typing import Any, Callable, Sequence

from .stencil_cache_storage import create_redis_client
from .stencil_corpus_publisher import (
    CORPUS_CONSUMER_GROUP,
    CORPUS_STREAM_KEY,
    default_corpus_database_path,
)
from .stencil_corpus_store import (
    IngestResult,
    MissingInstanceSighting,
    SQLiteCorpusStore,
)
from .stencil_redis_stream_port import RedisStreamPort, SynchronousRedisStreamAdapter

logger = logging.getLogger("kataproxy.stencil_corpus_materializer")


def _normalise_stream_id(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, str):
        return value
    raise TypeError("Redis stream message ID must be text or bytes")


def _event_payload(fields: object) -> bytes | str:
    if not isinstance(fields, Mapping):
        raise TypeError("Redis stream fields must be a mapping")
    payload = fields.get(b"event", fields.get("event"))
    if isinstance(payload, (bytes, str)):
        return payload
    raise ValueError("Redis stream entry does not contain a valid event payload")


def _parse_stream_messages(value: object) -> tuple[tuple[str, object], ...]:
    if not isinstance(value, (list, tuple)):
        raise TypeError("Redis stream messages must be a sequence")
    messages: list[tuple[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise TypeError(f"Redis stream message {index} must be an id/fields pair")
        message_id, fields = item
        messages.append((_normalise_stream_id(message_id), fields))
    return tuple(messages)


class StencilCorpusMaterializer:
    """Sequential, replay-safe Redis Stream + SQLite-spool consumer."""

    def __init__(
        self,
        store: SQLiteCorpusStore,
        redis_client: RedisStreamPort | None = None,
        *,
        consumer_name: str | None = None,
        batch_size: int = 100,
        claim_idle_ms: int = 60_000,
        after_sqlite_commit: Callable[[str], None] | None = None,
    ) -> None:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if isinstance(claim_idle_ms, bool) or not isinstance(claim_idle_ms, int) or claim_idle_ms <= 0:
            raise ValueError("claim_idle_ms must be a positive integer")
        self.store = store
        self.redis = redis_client
        self.consumer_name = consumer_name or (
            f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:12]}"
        )
        self.batch_size = batch_size
        self.claim_idle_ms = claim_idle_ms
        # Deterministic interruption seam: invoked after SQLite commit and
        # before XACK. Normally unset; integration tests use it to terminate
        # a real consumer process at the crash-recovery boundary.
        self._after_sqlite_commit = after_sqlite_commit
        self._group_ready = False
        self._claim_cursor = "0-0"

    def ensure_consumer_group(self) -> None:
        if self.redis is None or self._group_ready:
            return
        try:
            self.redis.xgroup_create(
                CORPUS_STREAM_KEY,
                CORPUS_CONSUMER_GROUP,
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    def _claim_stale_pending(self) -> tuple[tuple[str, object], ...]:
        if self.redis is None:
            return ()
        response = self.redis.xautoclaim(
            CORPUS_STREAM_KEY,
            CORPUS_CONSUMER_GROUP,
            self.consumer_name,
            self.claim_idle_ms,
            start_id=self._claim_cursor,
            count=self.batch_size,
        )
        if not isinstance(response, (list, tuple)) or len(response) not in (2, 3):
            raise TypeError("Redis XAUTOCLAIM returned an unexpected response shape")
        if len(response) == 3 and not isinstance(response[2], (list, tuple)):
            raise TypeError("Redis XAUTOCLAIM deleted-entry list must be a sequence")
        self._claim_cursor = _normalise_stream_id(response[0])
        return _parse_stream_messages(response[1])

    def _read_new(self, block_ms: int | None) -> tuple[tuple[str, object], ...]:
        if self.redis is None:
            return ()
        kwargs: dict[str, Any] = {"count": self.batch_size}
        if block_ms is not None and block_ms > 0:
            kwargs["block"] = block_ms
        response = self.redis.xreadgroup(
            CORPUS_CONSUMER_GROUP,
            self.consumer_name,
            {CORPUS_STREAM_KEY: ">"},
            **kwargs,
        )
        if response is None:
            return ()
        if not isinstance(response, (list, tuple)):
            raise TypeError("Redis XREADGROUP response must be a sequence or None")
        collected: list[tuple[str, object]] = []
        for index, stream_entry in enumerate(response):
            if not isinstance(stream_entry, (list, tuple)) or len(stream_entry) != 2:
                raise TypeError(
                    f"Redis XREADGROUP stream entry {index} must be a name/rows pair"
                )
            stream_name, rows = stream_entry
            if _normalise_stream_id(stream_name) != CORPUS_STREAM_KEY:
                raise ValueError("Redis XREADGROUP returned an unrequested stream")
            collected.extend(_parse_stream_messages(rows))
        return tuple(collected)

    def _ingest_spool(self) -> dict[str, int]:
        counts = {"spool_ingested": 0, "duplicates": 0, "deferred": 0, "errors": 0}
        for item in self.store.pending_spool_events(self.batch_size):
            try:
                result = self.store.ingest_spool_event(item)
            except MissingInstanceSighting:
                counts["deferred"] += 1
                logger.warning(
                    "SQLite spool attempt %s is waiting for its instance_seen event",
                    item.event_id,
                )
                continue
            except Exception:
                counts["errors"] += 1
                logger.exception(
                    "Could not materialize SQLite-spooled corpus event %s; "
                    "the raw spool row remains available for retry",
                    item.event_id,
                )
                continue
            if result.inserted:
                counts["spool_ingested"] += 1
            else:
                counts["duplicates"] += 1
        return counts

    def _ingest_stream_messages(
        self, messages: tuple[tuple[str, object], ...],
    ) -> dict[str, int]:
        counts = {"stream_ingested": 0, "acked": 0, "duplicates": 0, "deferred": 0, "errors": 0}
        if self.redis is None:
            return counts
        for message_id, fields in messages:
            try:
                payload = _event_payload(fields)
                result: IngestResult = self.store.ingest(payload)
            except MissingInstanceSighting:
                counts["deferred"] += 1
                logger.warning(
                    "Redis corpus event %s is waiting for its instance_seen event; "
                    "leaving it pending",
                    message_id,
                )
                continue
            except Exception:
                counts["errors"] += 1
                logger.exception(
                    "Could not materialize Redis corpus event %s; leaving it pending",
                    message_id,
                )
                continue

            # store.ingest returns only after its SQLite transaction commits.
            # This optional boundary is deliberately before XACK so a real
            # process interruption can be tested without timing races.
            if self._after_sqlite_commit is not None:
                self._after_sqlite_commit(message_id)

            # XACK is intentionally after store.ingest's transaction commit.
            try:
                acknowledged = self.redis.xack(
                    CORPUS_STREAM_KEY,
                    CORPUS_CONSUMER_GROUP,
                    message_id,
                )
            except Exception:
                counts["errors"] += 1
                logger.exception(
                    "SQLite committed corpus event %s but Redis XACK failed; "
                    "the event will be replayed idempotently",
                    message_id,
                )
                continue
            if acknowledged:
                counts["acked"] += 1
            if result.inserted:
                counts["stream_ingested"] += 1
            else:
                counts["duplicates"] += 1
        return counts

    def run_once(self, *, block_ms: int | None = None) -> dict[str, int]:
        """Drain one local spool batch and one Redis stream batch."""
        self.store.initialize()
        counts = self._ingest_spool()
        if self.redis is None:
            return counts

        self.ensure_consumer_group()
        pending = self._claim_stale_pending()
        new_messages = self._read_new(block_ms)
        stream_counts = self._ingest_stream_messages(pending + new_messages)
        for key, value in stream_counts.items():
            counts[key] = counts.get(key, 0) + value
        return counts


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="SQLite corpus database path (default: KATAPROXY_STENCIL_CORPUS_DB/XDG state path)",
    )
    parser.add_argument(
        "--spool-only",
        action="store_true",
        help="ingest only the local SQLite raw-event spool; do not connect to Redis",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="process one spool batch and one Redis batch, then exit",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="seconds to sleep when no events were materialized",
    )
    parser.add_argument(
        "--block-ms",
        type=int,
        default=1_000,
        help="maximum Redis blocking read interval (0 disables blocking)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="maximum events per source per pass",
    )
    parser.add_argument(
        "--claim-idle-ms",
        type=int,
        default=60_000,
        help="minimum pending age before XAUTOCLAIM can recover an abandoned event",
    )
    parser.add_argument(
        "--supervised-ready",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    stop_requested = threading.Event()

    def request_shutdown(_signum: int, _frame: Any) -> None:
        stop_requested.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    if args.block_ms < 0:
        parser.error("--block-ms must be non-negative")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.claim_idle_ms <= 0:
        parser.error("--claim-idle-ms must be positive")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    database = default_corpus_database_path() if args.database is None else args.database
    try:
        store = SQLiteCorpusStore(database)
        store.initialize()
    except Exception:
        logger.exception("Could not initialize the SQLite stencil corpus database")
        return 1

    # SQLite initialization is the startup-critical boundary. The parent
    # supervisor uses this marker as a readiness handshake; Redis outages
    # after this point are reported by the CLI and restarted by the parent.
    if args.supervised_ready:
        print("KATAPROXY_STENCIL_CORPUS_READY", flush=True)

    redis_client: RedisStreamPort | None = None
    if not args.spool_only:
        try:
            redis_client = SynchronousRedisStreamAdapter(create_redis_client())
        except Exception:
            logger.exception(
                "Could not connect to Redis DB 7; use --spool-only to drain "
                "the local fallback without Redis"
            )
            return 1

    try:
        materializer = StencilCorpusMaterializer(
            store,
            redis_client,
            batch_size=args.batch_size,
            claim_idle_ms=args.claim_idle_ms,
        )
        while not stop_requested.is_set():
            counts = materializer.run_once(
                block_ms=None if args.once or args.block_ms == 0 else args.block_ms,
            )
            if any(counts.values()):
                logger.info("Stencil corpus materializer pass: %s", counts)
            if args.supervised_ready and (
                counts.get("stream_ingested", 0)
                + counts.get("spool_ingested", 0)
                + counts.get("duplicates", 0)
                > 0
            ):
                print(
                    "KATAPROXY_STENCIL_CORPUS_PROGRESS "
                    + json.dumps(counts, sort_keys=True),
                    flush=True,
                )
            if args.once:
                return 0
            if (
                not any(counts.values())
                or counts.get("errors", 0) > 0
                or counts.get("deferred", 0) > 0
            ):
                stop_requested.wait(args.poll_interval)
        logger.info("Stencil corpus materializer stopped")
        return 0
    except KeyboardInterrupt:
        logger.info("Stencil corpus materializer stopped")
        return 0
    except Exception:
        logger.exception("Stencil corpus materializer failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
