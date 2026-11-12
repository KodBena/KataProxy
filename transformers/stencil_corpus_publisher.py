"""Best-effort corpus event transport: Redis Streams with a durable SQLite spool.

The spool stores the exact v1 event bytes. It is not a second event format;
a later materializer can ingest those bytes idempotently after Redis recovers.
"""
from __future__ import annotations

import hashlib
import logging
import math
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Protocol

from transformers.stencil_cache_storage import create_redis_client
from transformers.stencil_corpus_events import (
    ProducerPath,
    make_instance_seen_event,
    serialize_event,
)

logger = logging.getLogger("kataproxy.stencil_corpus_publisher")

CORPUS_STREAM_KEY = "kataproxy:stencil-cover:corpus:events:v1"
CORPUS_CONSUMER_GROUP = "stencil-corpus-materializer-v1"
_SPOOL_USER_VERSION = 1
_ENABLE_ENV = "KATAPROXY_STENCIL_CORPUS_ENABLED"
_DATABASE_ENV = "KATAPROXY_STENCIL_CORPUS_DB"


class CorpusEventConflict(RuntimeError):
    """A duplicate event UUID was presented with different immutable bytes."""


class CorpusEventSink(Protocol):
    """Small substitutable port implemented by the publisher."""

    def publish(self, event: dict[str, Any]) -> str: ...


def corpus_events_enabled() -> bool:
    """Corpus capture is opt-in so existing deployments gain no I/O by default."""
    return os.environ.get(_ENABLE_ENV, "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def default_corpus_database_path() -> Path:
    configured = os.environ.get(_DATABASE_ENV)
    if configured:
        return Path(configured).expanduser()
    state_home = os.environ.get("XDG_STATE_HOME")
    base = Path(state_home).expanduser() if state_home else Path.home() / ".local" / "state"
    return base / "kataproxy" / "stencil-cover-corpus.sqlite3"


class SQLiteEventSpool:
    """Append-only, idempotent local fallback for exact serialized event bytes."""

    def __init__(self, path: str | Path, timeout_seconds: float = 2.0) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("SQLite spool timeout must be positive and finite")
        self.path = Path(path).expanduser()
        self.timeout_seconds = timeout_seconds
        self._init_lock = threading.Lock()
        self._initialised = False

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            str(self.path), timeout=self.timeout_seconds,
        )
        connection.execute(
            f"PRAGMA busy_timeout = {int(self.timeout_seconds * 1000)}"
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialise(self) -> None:
        with self._init_lock:
            if self._initialised:
                return
            existed = self.path.exists() and self.path.stat().st_size > 0
            connection = self._connect()
            try:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                if version_row is None:
                    raise RuntimeError("could not read SQLite corpus user_version")
                version = int(version_row[0])
                if version > _SPOOL_USER_VERSION:
                    raise RuntimeError(
                        "SQLite corpus database is newer than this event spool "
                        f"(user_version={version}, supported={_SPOOL_USER_VERSION})"
                    )
                connection.execute("PRAGMA journal_mode = WAL")
                if existed and version == 0:
                    stamp = (
                        time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                        + f"-{time.time_ns()}"
                    )
                    backup_path = self.path.with_name(
                        self.path.name + f".pre-v1-{stamp}.bak"
                    )
                    backup = sqlite3.connect(str(backup_path))
                    try:
                        connection.backup(backup)
                    finally:
                        backup.close()
                table_row = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='stencil_corpus_spool_events'"
                ).fetchone()
                if version == 1 and table_row is None:
                    raise RuntimeError(
                        "SQLite corpus spool declares user_version=1 but its event table is missing"
                    )
                if version == 0:
                    connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS stencil_corpus_spool_events (
                            event_id TEXT PRIMARY KEY,
                            event_json BLOB NOT NULL,
                            payload_sha256 TEXT NOT NULL
                                CHECK(
                                    length(payload_sha256) = 64
                                    AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
                                ),
                            queued_at_utc TEXT NOT NULL
                        )
                        """
                    )
                    columns = tuple(
                        row[1] for row in connection.execute(
                            "PRAGMA table_info(stencil_corpus_spool_events)"
                        ).fetchall()
                    )
                    if columns != (
                        "event_id", "event_json", "payload_sha256", "queued_at_utc",
                    ):
                        raise RuntimeError(
                            "SQLite corpus spool event table has an unsupported schema"
                        )
                    connection.execute("PRAGMA user_version = 1")
                else:
                    columns = tuple(
                        row[1] for row in connection.execute(
                            "PRAGMA table_info(stencil_corpus_spool_events)"
                        ).fetchall()
                    )
                    if columns != (
                        "event_id", "event_json", "payload_sha256", "queued_at_utc",
                    ):
                        raise RuntimeError(
                            "SQLite corpus spool event table has an unsupported schema"
                        )
                connection.commit()
                self._initialised = True
            finally:
                connection.close()

    def append(self, payload: bytes) -> bool:
        """Persist one canonical event; return False for an identical replay."""
        from transformers.stencil_corpus_events import parse_event

        event = parse_event(payload)
        if serialize_event(event) != payload:
            raise ValueError("SQLite spool accepts only canonical serialized event bytes")
        event_id = event["event_id"]
        digest = hashlib.sha256(payload).hexdigest()
        self._initialise()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT payload_sha256, event_json FROM stencil_corpus_spool_events "
                "WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if existing is not None:
                if existing[0] != digest or bytes(existing[1]) != payload:
                    raise CorpusEventConflict(
                        f"event_id {event_id} already exists with different bytes"
                    )
                connection.commit()
                return False
            from transformers.stencil_corpus_events import utc_timestamp

            connection.execute(
                "INSERT INTO stencil_corpus_spool_events "
                "(event_id, event_json, payload_sha256, queued_at_utc) "
                "VALUES (?, ?, ?, ?)",
                (event_id, sqlite3.Binary(payload), digest, utc_timestamp()),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


class CorpusEventPublisher:
    """Publish immutable envelopes to Redis; spool exact bytes if Redis fails."""

    def __init__(
        self,
        *,
        redis_client_factory: Callable[[], Any] = create_redis_client,
        sqlite_path: str | Path | None = None,
        redis_client: Any | None = None,
    ) -> None:
        self._redis_client_factory = redis_client_factory
        self._redis_client = redis_client
        self._redis_lock = threading.Lock()
        self._warned_redis_failure = False
        self._spool = SQLiteEventSpool(
            default_corpus_database_path() if sqlite_path is None else sqlite_path,
        )

    def _get_redis_client(self) -> Any:
        with self._redis_lock:
            if self._redis_client is None:
                self._redis_client = self._redis_client_factory()
            return self._redis_client

    def publish(self, event: dict[str, Any]) -> str:
        """Return 'redis_stream', 'sqlite_spool', or 'failed'; never raise I/O errors."""
        payload = serialize_event(event)
        event_id = event["event_id"]
        try:
            client = self._get_redis_client()
            # No MAXLEN/trimming argument: an unmaterialized event must not be
            # evicted by the producer.
            client.xadd(CORPUS_STREAM_KEY, {b"event": payload})
            with self._redis_lock:
                self._warned_redis_failure = False
            return "redis_stream"
        except Exception as redis_error:
            with self._redis_lock:
                should_warn = not self._warned_redis_failure
                self._warned_redis_failure = True
            if should_warn:
                logger.warning(
                    "Stencil corpus Redis Stream unavailable; spooling events "
                    "to SQLite (%s)",
                    redis_error,
                )
            try:
                self._spool.append(payload)
                return "sqlite_spool"
            except Exception as spool_error:
                logger.critical(
                    "Could not persist stencil corpus event %s; Redis error: %s; "
                    "SQLite spool error: %s",
                    event_id,
                    redis_error,
                    spool_error,
                    exc_info=True,
                )
                return "failed"


_DEFAULT_PUBLISHER: CorpusEventPublisher | None = None
_DEFAULT_PUBLISHER_LOCK = threading.Lock()


def get_default_event_publisher() -> CorpusEventPublisher | None:
    """Lazily create the opt-in publisher used by foreground and bulk paths."""
    global _DEFAULT_PUBLISHER
    if not corpus_events_enabled():
        return None
    with _DEFAULT_PUBLISHER_LOCK:
        if _DEFAULT_PUBLISHER is None:
            _DEFAULT_PUBLISHER = CorpusEventPublisher()
        return _DEFAULT_PUBLISHER


def publish_instance_seen(
    shape_key: bytes,
    family: Any,
    producer_path: ProducerPath = "foreground",
    publisher: CorpusEventSink | None = None,
) -> str | None:
    """Emit a component sighting before local-LRU or Redis-result cache lookup."""
    target = get_default_event_publisher() if publisher is None else publisher
    if target is None:
        return None
    try:
        from transformers.stencil_solver_runtime import canonical_problem_from_key

        problem = canonical_problem_from_key(shape_key, family)
        event = make_instance_seen_event(problem, shape_key, producer_path)
        return target.publish(event)
    except Exception:
        # Corpus capture is deliberately best-effort and cannot suppress the
        # original KataGo analysis response.
        logger.exception(
            "Could not construct or publish stencil instance_seen event for key %s",
            shape_key.hex(),
        )
        return "failed"
