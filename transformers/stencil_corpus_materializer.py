"""Materialize Redis Stream and local-spool events into the SQLite corpus.

The order is deliberate: drain this host's SQLite fallback spool first, then
claim stale Redis pending entries, then read new stream entries. SQLite commit
always precedes XACK, so a crash causes a safe receipt-checked replay.
"""
from __future__ import annotations

import argparse
import logging
import os
import socket
import time
import uuid
from typing import Any, Protocol, Sequence

from transformers.stencil_cache_storage import create_redis_client
from transformers.stencil_corpus_publisher import (
    CORPUS_CONSUMER_GROUP,
    CORPUS_STREAM_KEY,
    default_corpus_database_path,
)
from transformers.stencil_corpus_store import (
    IngestResult,
    MissingInstanceSighting,
    SQLiteCorpusStore,
)

logger = logging.getLogger("kataproxy.stencil_corpus_materializer")


class RedisStreamPort(Protocol):
    """Small structural protocol for the Redis Streams operations used here."""

    def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False,
    ) -> Any: ...

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> Any: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    def xack(self, name: str, groupname: str, *ids: str) -> int: ...


def _normalise_stream_id(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    if isinstance(value, str):
        return value
    raise TypeError("Redis stream message ID must be text or bytes")


def _event_payload(fields: Any) -> bytes | str:
    if not isinstance(fields, dict):
        raise TypeError("Redis stream fields must be a mapping")
    payload = fields.get(b"event", fields.get("event"))
    if isinstance(payload, (bytes, str)):
        return payload
    raise ValueError("Redis stream entry does not contain an event payload")


def _parse_stream_messages(value: Any) -> tuple[tuple[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    messages: list[tuple[str, Any]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
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

    def _claim_stale_pending(self) -> tuple[tuple[str, Any], ...]:
        if self.redis is None:
            return ()
        autoclaim = getattr(self.redis, "xautoclaim", None)
        if not callable(autoclaim):
            raise RuntimeError(
                "Redis client/server must support XAUTOCLAIM for pending-event recovery"
            )
        response = autoclaim(
            CORPUS_STREAM_KEY,
            CORPUS_CONSUMER_GROUP,
            self.consumer_name,
            self.claim_idle_ms,
            start_id=self._claim_cursor,
            count=self.batch_size,
        )
        if not isinstance(response, (list, tuple)) or len(response) < 2:
            raise RuntimeError("Redis XAUTOCLAIM returned an unexpected response")
        self._claim_cursor = _normalise_stream_id(response[0])
        return _parse_stream_messages(response[1])

    def _read_new(self, block_ms: int | None) -> tuple[tuple[str, Any], ...]:
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
        if not isinstance(response, (list, tuple)):
            return ()
        collected: list[tuple[str, Any]] = []
        for stream_entry in response:
            if not isinstance(stream_entry, (list, tuple)) or len(stream_entry) != 2:
                continue
            _stream_name, rows = stream_entry
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
        self, messages: tuple[tuple[str, Any], ...],
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
    args = parser.parse_args(argv)
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

    redis_client: RedisStreamPort | None = None
    if not args.spool_only:
        try:
            redis_client = create_redis_client()
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
        while True:
            counts = materializer.run_once(
                block_ms=None if args.once or args.block_ms == 0 else args.block_ms,
            )
            if any(counts.values()):
                logger.info("Stencil corpus materializer pass: %s", counts)
            if args.once:
                return 0
            if (
                not any(counts.values())
                or counts.get("errors", 0) > 0
                or counts.get("deferred", 0) > 0
            ):
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        logger.info("Stencil corpus materializer stopped")
        return 0
    except Exception:
        logger.exception("Stencil corpus materializer failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
