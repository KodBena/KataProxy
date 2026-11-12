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
from typing import Any, Callable, Protocol, Sequence

from transformers.stencil_cache_storage import create_redis_client
from transformers.stencil_corpus_events import (
    ProducerPath,
    make_instance_seen_event,
    parse_event,
    serialize_event,
    utc_timestamp,
)
from transformers.stencil_solver_problem import canonical_problem_from_key

logger = logging.getLogger("kataproxy.stencil_corpus_publisher")

CORPUS_STREAM_KEY = "kataproxy:stencil-cover:corpus:events:v1"
CORPUS_CONSUMER_GROUP = "stencil-corpus-materializer-v1"
_SPOOL_USER_VERSION = 2
_ENABLE_ENV = "KATAPROXY_STENCIL_CORPUS_ENABLED"
_DATABASE_ENV = "KATAPROXY_STENCIL_CORPUS_DB"


class CorpusEventConflict(RuntimeError):
    """A duplicate event UUID was presented with different immutable bytes."""


class CorpusEventSink(Protocol):
    """Small substitutable port implemented by the publisher."""

    def publish(self, event: dict[str, Any]) -> str:
        """Publish one event; see publish_many for transport/fallback semantics."""
        return self.publish_many((event,))

    def publish_many(self, events: Sequence[dict[str, Any]]) -> str:
        """Publish a request-local batch in one Redis transaction or spool it."""
        prepared = tuple(
            (event["event_id"], serialize_event(event)) for event in events
        )
        if not prepared:
            return "redis_stream"

        try:
            client = self._get_redis_client()
            # One transactional pipeline makes all XADD commands one network
            # exchange. Do not trim: every unmaterialized event must be retained.
            with client.pipeline(transaction=True) as pipeline:
                for _event_id, payload in prepared:
                    pipeline.xadd(CORPUS_STREAM_KEY, {b"event": payload})
                pipeline.execute()
            with self._redis_lock:
                self._warned_redis_failure = False
            return "redis_stream"
        except Exception as redis_error:
            with self._redis_lock:
                should_warn = not self._warned_redis_failure
                self._warned_redis_failure = True
            if should_warn:
                logger.warning(
                    "Stencil corpus Redis Stream unavailable; spooling event "
                    "batch to SQLite (%s)",
                    redis_error,
                )
            try:
                self._spool.append_many(tuple(
                    payload for _event_id, payload in prepared
                ))
                return "sqlite_spool"
            except Exception as spool_error:
                event_ids = ", ".join(
                    event_id for event_id, _payload in prepared
                )
                logger.critical(
                    "Could not persist stencil corpus event batch [%s]; "
                    "Redis error: %s; SQLite spool error: %s",
                    event_ids,
                    redis_error,
                    spool_error,
                    exc_info=True,
                )
                return "failed"



def publish_instance_seen_many(
    shape_keys: Sequence[bytes],
    family: Any,
    producer_path: ProducerPath = "foreground",
    publisher: CorpusEventSink | None = None,
) -> str | None:
    """Capture several component sightings with one publisher batch operation."""
    target = get_default_event_publisher() if publisher is None else publisher
    if target is None or not shape_keys:
        return None

    events: list[dict[str, Any]] = []
    construction_failed = False
    for shape_key in shape_keys:
        try:
            problem = canonical_problem_from_key(shape_key, family)
            events.append(make_instance_seen_event(
                problem, shape_key, producer_path,
            ))
        except Exception:
            construction_failed = True
            logger.exception(
                "Could not construct stencil instance_seen event for key %s",
                shape_key.hex(),
            )
    if not events:
        return "failed" if construction_failed else None

    try:
        batch_publish = getattr(target, "publish_many", None)
        if callable(batch_publish):
            result = batch_publish(events)
        else:
            # Preserve compatibility with injected sinks that only implement
            # the original one-event protocol. Production uses publish_many.
            results: list[str] = []
            for event in events:
                try:
                    results.append(target.publish(event))
                except Exception:
                    logger.exception(
                        "Injected stencil corpus sink failed to publish event %s",
                        event.get("event_id", "<unknown>"),
                    )
                    results.append("failed")
            result = (
                "failed" if "failed" in results
                else "sqlite_spool" if "sqlite_spool" in results
                else "redis_stream"
            )
    except Exception:
        logger.exception("Could not publish stencil instance_seen event batch")
        return "failed"
    return "failed" if construction_failed else result


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
