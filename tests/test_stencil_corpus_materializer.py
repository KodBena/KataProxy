# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from transformers import stencil_cover_enricher as stencil
from transformers.stencil_corpus_events import (
    make_instance_seen_event,
    make_solver_attempt_event,
    serialize_event,
)
from transformers.stencil_corpus_materializer import StencilCorpusMaterializer
from transformers.stencil_corpus_publisher import (
    CORPUS_CONSUMER_GROUP,
    CORPUS_STREAM_KEY,
    CorpusEventPublisher,
    SQLiteEventSpool,
)
from transformers.stencil_corpus_store import (
    SQLiteCorpusStore,
)
from transformers.stencil_solver_api import (
    BackendStatusReported,
    Incumbent,
    NativeEffortUnavailable,
    NoNativeEffortLimit,
    NoNativeEffortReason,
    NoSolverFailure,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverBudget,
    SolverIdentity,
    SolverOutcome,
)
from transformers.stencil_solver_problem import canonical_problem_from_key
from transformers.stencil_solver_runtime import solve_component


def _family() -> tuple[tuple[str, tuple[tuple[int, int], ...]], ...]:
    return (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )


def _key() -> bytes:
    value, _frame = stencil._component_shape_key((0, 1), 2, _family())
    return value


def _seen_event(
    event_id: str = "12345678-1234-5678-1234-567812345678",
) -> dict[str, Any]:
    problem = canonical_problem_from_key(_key(), _family())
    return make_instance_seen_event(
        problem,
        _key(),
        event_id=event_id,
    )


class _DiscardPublisher:
    def publish(self, _event: dict[str, Any]) -> str:
        return "redis_stream"


class _Backend:
    @property
    def identity(self) -> SolverIdentity:
        config = '{"mode":"materializer-test"}'
        return SolverIdentity(
            backend_name="materializer-test",
            backend_version="1",
            adapter_version="test-v1",
            configuration_json=config,
            configuration_fingerprint=hashlib.sha256(config.encode("utf-8")).hexdigest(),
        )

    def solve(self, problem: Any, budget: Any) -> SolverOutcome:
        return SolverOutcome(
            status=OutcomeStatus.OPTIMAL,
            incumbent=Incumbent(
                1, PlacementWitness((("1x2", ((0, 0), (0, 1))),)),
            ),
            lower_bound=ProvenLowerBound(1),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
            ),
            backend_status=BackendStatusReported("OPTIMAL"),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def _attempt_event(
    event_id: str = "22345678-1234-5678-1234-567812345678",
) -> dict[str, Any]:
    budget = SolverBudget(
        wall_time_limit_ms=250,
        native_effort_limit=NoNativeEffortLimit(),
        policy_id="foreground-250ms",
    )
    execution = solve_component(
        _Backend(), _key(), _family(), budget,
        event_publisher=_DiscardPublisher(),
    )
    return make_solver_attempt_event(
        execution,
        _key(),
        budget,
        "foreground",
        event_id=event_id,
    )


class _FakeRedis:
    def __init__(self, store: SQLiteCorpusStore) -> None:
        self.store = store
        self.groups: list[tuple[Any, ...]] = []
        self.reads: list[tuple[Any, ...]] = []
        self.claims: list[tuple[Any, ...]] = []
        self.acks: list[str] = []
        self.new_messages: list[tuple[bytes, dict[bytes, bytes]]] = []
        self.pending_messages: list[tuple[bytes, dict[bytes, bytes]]] = []
        self.raise_on_ack = False
        self._lock = threading.Lock()
        self._next_stream_id = 0
        self._event_ids: dict[str, str] = {}

    def xadd(self, name: str, fields: dict[bytes, bytes]) -> str:
        assert name == CORPUS_STREAM_KEY
        with self._lock:
            self._next_stream_id += 1
            message_id = f"{self._next_stream_id}-0"
            self.new_messages.append(_message(message_id, fields[b"event"]))
            return message_id

    def has_backlog(self) -> bool:
        with self._lock:
            return bool(self.new_messages or self.pending_messages)

    def xgroup_create(self, *args: Any, **kwargs: Any) -> str:
        with self._lock:
            self.groups.append((args, kwargs))
        return "OK"

    def xautoclaim(self, *args: Any, **kwargs: Any) -> tuple[str, list[Any], list[Any]]:
        with self._lock:
            self.claims.append((args, kwargs))
            messages = self.pending_messages
            self.pending_messages = []
            for message_id, fields in messages:
                stream_id = message_id.decode("ascii")
                event = json.loads(fields[b"event"].decode("utf-8"))
                event_id = event.get("event_id")
                if isinstance(event_id, str) and event_id:
                    self._event_ids[stream_id] = event_id
        return "0-0", messages, []

    def xreadgroup(self, *args: Any, **kwargs: Any) -> list[Any]:
        with self._lock:
            self.reads.append((args, kwargs))
            messages = self.new_messages
            self.new_messages = []
            self.pending_messages.extend(messages)
            for message_id, fields in messages:
                stream_id = message_id.decode("ascii")
                event = json.loads(fields[b"event"].decode("utf-8"))
                event_id = event.get("event_id")
                if isinstance(event_id, str) and event_id:
                    self._event_ids[stream_id] = event_id
        return [(CORPUS_STREAM_KEY.encode("utf-8"), messages)] if messages else []

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == CORPUS_STREAM_KEY
        assert groupname == CORPUS_CONSUMER_GROUP
        if self.raise_on_ack:
            raise ConnectionError("simulated process failure immediately before XACK")
        for stream_id in ids:
            event_id = self._event_ids[stream_id]
            with sqlite3.connect(self.store.path) as connection:
                found = connection.execute(
                    "SELECT 1 FROM corpus_event_receipts WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
            if found is None:
                raise AssertionError("XACK happened before the SQLite receipt committed")
            with self._lock:
                self.acks.append(stream_id)
                self.pending_messages = [
                    message for message in self.pending_messages
                    if message[0].decode("ascii") != stream_id
                ]
        return len(ids)


def _message(
    message_id: str,
    payload: bytes,
) -> tuple[bytes, dict[bytes, bytes]]:
    return message_id.encode("ascii"), {b"event": payload}


def test_stream_materializer_creates_group_and_commits_before_ack(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    seen = serialize_event(_seen_event())
    attempt = serialize_event(_attempt_event())
    redis.new_messages = [
        _message("1-0", seen),
        _message("2-0", attempt),
    ]
    materializer = StencilCorpusMaterializer(
        store, redis, consumer_name="test-consumer",
    )

    counts = materializer.run_once()

    assert redis.groups[0][0] == (
        CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP,
    )
    assert redis.groups[0][1] == {"id": "0-0", "mkstream": True}
    assert len(redis.claims) == 1
    assert len(redis.reads) == 1
    assert redis.acks == ["1-0", "2-0"]
    assert counts["stream_ingested"] == 2
    assert counts["acked"] == 2
    assert store.counts()["instances"] == 1
    assert store.counts()["attempts"] == 1


def test_pending_events_are_claimed_before_new_entries(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    redis.pending_messages = [_message("8-0", serialize_event(_seen_event()))]
    materializer = StencilCorpusMaterializer(store, redis, consumer_name="claim-test")

    counts = materializer.run_once()

    assert counts["stream_ingested"] == 1
    assert redis.claims
    assert redis.reads
    assert redis.acks == ["8-0"]


def test_stream_attempt_without_sighting_is_left_pending(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    redis.new_messages = [_message("2-0", serialize_event(_attempt_event()))]
    materializer = StencilCorpusMaterializer(store, redis, consumer_name="defer-test")

    counts = materializer.run_once()

    assert counts["deferred"] == 1
    assert counts["acked"] == 0
    assert redis.acks == []
    assert store.counts()["receipts"] == 0


def test_crash_after_sqlite_commit_before_xack_replays_idempotently(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    payload = serialize_event(_seen_event())
    redis.new_messages = [_message("1-0", payload)]
    redis.raise_on_ack = True
    materializer = StencilCorpusMaterializer(store, redis, consumer_name="lost-ack")

    first = materializer.run_once()
    assert first["stream_ingested"] == 0
    assert first["errors"] == 1
    assert redis.acks == []
    assert redis.has_backlog()
    assert store.counts()["instances"] == 1
    assert store.counts()["receipts"] == 1

    redis.raise_on_ack = False
    second = materializer.run_once()
    assert second["duplicates"] == 1
    assert second["acked"] == 1
    assert store.counts()["instances"] == 1
    assert store.counts()["receipts"] == 1


def test_spool_only_materializer_drains_local_fallback_rows(tmp_path: Path) -> None:
    path = tmp_path / "corpus.sqlite3"
    spool = SQLiteEventSpool(path)
    store = SQLiteCorpusStore(path)
    payload = serialize_event(_seen_event())
    spool.append(payload)

    materializer = StencilCorpusMaterializer(store, redis_client=None)
    counts = materializer.run_once()

    assert counts["spool_ingested"] == 1
    assert store.counts()["spool_pending"] == 0
    assert store.counts()["instances"] == 1


def test_materializer_leaves_invalid_spool_rows_for_operator_repair(
    tmp_path: Path,
) -> None:
    path = tmp_path / "corpus.sqlite3"
    spool = SQLiteEventSpool(path)
    valid_payload = serialize_event(_seen_event())
    spool.append(valid_payload)
    event_id = _seen_event()["event_id"]
    # The raw spool validates at append time, so inject a corrupt row directly
    # to exercise the materializer's poison-event retention path.
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM stencil_corpus_spool_events")
        connection.execute(
            "INSERT INTO stencil_corpus_spool_events "
            "(event_id, event_json, payload_sha256, queued_at_utc) "
            "VALUES (?, ?, ?, ?)",
            (
                event_id,
                b'{"invalid":true}',
                hashlib.sha256(b'{"invalid":true}').hexdigest(),
                "2026-11-12T10:15:30Z",
            ),
        )
    store = SQLiteCorpusStore(path)
    materializer = StencilCorpusMaterializer(store, redis_client=None)

    counts = materializer.run_once()

    assert counts["errors"] == 1
    assert store.counts()["spool_pending"] == 1


def test_materializer_rejects_invalid_batch_configuration(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    with pytest.raises(ValueError, match="batch_size"):
        StencilCorpusMaterializer(store, batch_size=0)
    with pytest.raises(ValueError, match="claim_idle_ms"):
        StencilCorpusMaterializer(store, claim_idle_ms=0)


def test_stream_backlog_accumulates_without_materializer_then_drains(
    tmp_path: Path,
) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    publisher = CorpusEventPublisher(redis_client=redis, sqlite_path=tmp_path / "spool.sqlite3")

    # There is deliberately no materializer running between publication and
    # the assertions below: the stream retains both immutable events.
    assert publisher.publish(_seen_event()) == "redis_stream"
    assert publisher.publish(_attempt_event()) == "redis_stream"
    assert store.counts()["receipts"] == 0
    assert len(redis.new_messages) == 2

    materializer = StencilCorpusMaterializer(store, redis, consumer_name="backlog-drain")
    counts = materializer.run_once(block_ms=0)

    assert counts["stream_ingested"] == 2
    assert counts["acked"] == 2
    assert redis.acks == ["1-0", "2-0"]
    assert store.counts() == {
        "instances": 1,
        "attempts": 1,
        "receipts": 2,
        "spool_pending": 0,
    }


def test_concurrent_event_producers_and_materializer_do_not_duplicate_attempts(
    tmp_path: Path,
) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    publisher = CorpusEventPublisher(redis_client=redis, sqlite_path=tmp_path / "spool.sqlite3")

    # Publish the sighting before concurrent attempts so the test exercises
    # producer/consumer races rather than the intentional FK deferral path.
    assert publisher.publish(_seen_event()) == "redis_stream"
    materializer = StencilCorpusMaterializer(
        store, redis, consumer_name="concurrent-materializer", batch_size=64,
    )
    producers_done = threading.Event()
    consumer_errors: list[BaseException] = []

    def consume_until_drained() -> None:
        try:
            while not producers_done.is_set() or redis.has_backlog():
                counts = materializer.run_once(block_ms=0)
                if not any(counts.values()):
                    time.sleep(0.001)
        except BaseException as exc:
            consumer_errors.append(exc)
            producers_done.set()

    def produce_attempts(producer_index: int) -> None:
        for _ in range(10):
            event_id = str(uuid.uuid4())
            assert publisher.publish(_attempt_event(event_id)) == "redis_stream"

    with ThreadPoolExecutor(max_workers=5) as executor:
        consumer = executor.submit(consume_until_drained)
        producers = [
            executor.submit(produce_attempts, index)
            for index in range(4)
        ]
        for future in producers:
            future.result(timeout=10)
        producers_done.set()
        consumer.result(timeout=10)

    assert consumer_errors == []
    assert not redis.has_backlog()
    assert store.counts() == {
        "instances": 1,
        "attempts": 40,
        "receipts": 41,
        "spool_pending": 0,
    }
    with sqlite3.connect(store.path) as connection:
        attempt_ids = connection.execute(
            "SELECT attempt_id FROM solver_attempts",
        ).fetchall()
        assert len(attempt_ids) == 40
        assert len({row[0] for row in attempt_ids}) == 40


def test_malformed_stream_event_is_not_acknowledged(tmp_path: Path) -> None:
    store = SQLiteCorpusStore(tmp_path / "corpus.sqlite3")
    redis = _FakeRedis(store)
    redis.new_messages = [_message("9-0", b'{"not":"a corpus event"}')]
    materializer = StencilCorpusMaterializer(
        store, redis, consumer_name="malformed-stream",
    )

    counts = materializer.run_once(block_ms=0)

    assert counts["errors"] == 1
    assert counts["acked"] == 0
    assert redis.acks == []
    assert redis.has_backlog()
    assert store.counts()["receipts"] == 0
