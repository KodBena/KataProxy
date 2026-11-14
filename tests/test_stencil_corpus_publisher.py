# SPDX-License-Identifier: MIT
from __future__ import annotations

import copy
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import transformers.stencil_cover.stencil_region_components as region_components
import transformers.stencil_cover.stencil_region_solver as region_solver
import transformers.stencil_cover.stencil_solver_geometry as geometry
from transformers.stencil_cover.stencil_cache_storage import SolverCacheRequest
from transformers.stencil_cover.stencil_corpus_events import (
    make_instance_seen_event,
    serialize_event,
)
from transformers.stencil_cover.stencil_corpus_store import SQLiteCorpusStore
from transformers.stencil_cover.stencil_corpus_publisher import (
    CORPUS_STREAM_KEY,
    CorpusEventConflict,
    CorpusEventPublisher,
    SQLiteEventSpool,
    corpus_events_enabled,
)
from transformers.stencil_cover.stencil_solver_api import (
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
from transformers.stencil_cover.stencil_solver_problem import canonical_problem_from_key
from transformers.stencil_cover.stencil_solver_runtime import solve_component


def _family() -> tuple[tuple[str, tuple[tuple[int, int], ...]], ...]:
    return (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )


def _event(event_id: str = "12345678-1234-5678-1234-567812345678") -> dict[str, Any]:
    family = _family()
    key, _frame = region_components._component_shape_key((0, 1), 2, family)
    return make_instance_seen_event(
        canonical_problem_from_key(key, family),
        key,
        event_id=event_id,
        emitted_at_utc=datetime(2026, 11, 12, 10, 15, 30, tzinfo=timezone.utc),
    )


class _FakePipeline:
    def __init__(self, client: "_FakeRedis") -> None:
        self.client = client
        self.commands: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_FakePipeline":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def xadd(self, *args: Any) -> "_FakePipeline":
        self.commands.append(args)
        return self

    def execute(self) -> list[str]:
        return [self.client.xadd(*args) for args in self.commands]


class _FakeRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.pipeline_calls = 0

    def pipeline(self, *, transaction: bool = True) -> _FakePipeline:
        assert transaction is True
        self.pipeline_calls += 1
        return _FakePipeline(self)

    def xadd(self, *args: Any) -> str:
        self.calls.append(args)
        return "1-0"


def test_publisher_writes_untrimmed_event_to_redis_first(tmp_path: Path) -> None:
    client = _FakeRedis()
    publisher = CorpusEventPublisher(
        redis_client_factory=lambda: client,
        sqlite_path=tmp_path / "spool.sqlite3",
    )
    event = _event()

    assert publisher.publish(event) == "redis_stream"
    assert len(client.calls) == 1
    assert client.calls[0][0] == CORPUS_STREAM_KEY
    assert client.calls[0][1] == {b"event": serialize_event(event)}
    assert len(client.calls[0]) == 2
    assert not (tmp_path / "spool.sqlite3").exists()


def test_publisher_sends_a_batch_through_one_transactional_pipeline(
    tmp_path: Path,
) -> None:
    client = _FakeRedis()
    publisher = CorpusEventPublisher(
        redis_client_factory=lambda: client,
        sqlite_path=tmp_path / "spool.sqlite3",
    )
    events = [
        _event(f"12345678-1234-5678-1234-{index:012d}")
        for index in range(1, 4)
    ]

    assert publisher.publish_many(events) == "redis_stream"
    assert client.pipeline_calls == 1
    assert len(client.calls) == len(events)
    assert all(call[0] == CORPUS_STREAM_KEY for call in client.calls)
    assert [call[1] for call in client.calls] == [
        {b"event": serialize_event(event)} for event in events
    ]
    assert not (tmp_path / "spool.sqlite3").exists()


def test_publisher_spools_a_batch_when_redis_is_unavailable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fallback-batch.sqlite3"

    def fail_to_connect() -> Any:
        raise ConnectionError("Redis is offline")

    publisher = CorpusEventPublisher(
        redis_client_factory=fail_to_connect,
        sqlite_path=path,
    )
    events = [
        _event(f"12345678-1234-5678-1234-{index:012d}")
        for index in range(4, 7)
    ]

    assert publisher.publish_many(events) == "sqlite_spool"
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT event_id, event_json FROM stencil_corpus_spool_events "
            "ORDER BY event_id"
        ).fetchall()
    assert [row[0] for row in rows] == sorted(
        event["event_id"] for event in events
    )
    assert {bytes(row[1]) for row in rows} == {
        serialize_event(event) for event in events
    }


def test_sqlite_spool_is_idempotent_and_preserves_exact_event_bytes(tmp_path: Path) -> None:
    path = tmp_path / "corpus.sqlite3"
    spool = SQLiteEventSpool(path)
    payload = serialize_event(_event())

    assert spool.append(payload) is True
    assert spool.append(payload) is False

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT event_id, event_json, payload_sha256 FROM stencil_corpus_spool_events"
        ).fetchone()
        assert row is not None
        assert row[0] == "12345678-1234-5678-1234-567812345678"
        assert bytes(row[1]) == payload
        assert row[2] == hashlib.sha256(payload).hexdigest()
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2

    store = SQLiteCorpusStore(path)
    pending = store.pending_spool_events()
    assert len(pending) == 1
    assert pending[0].event_id == "12345678-1234-5678-1234-567812345678"
    assert pending[0].payload == payload
    assert pending[0].payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert store.ingest_spool_event(pending[0]).inserted is True
    assert store.pending_spool_events() == ()
    assert store.counts()["instances"] == 1
    assert store.counts()["receipts"] == 1


def test_sqlite_spool_rejects_same_event_id_with_different_immutable_bytes(
    tmp_path: Path,
) -> None:
    spool = SQLiteEventSpool(tmp_path / "corpus.sqlite3")
    first = _event()
    second = copy.deepcopy(first)
    second["emitted_at_utc"] = "2026-11-12T10:15:31.000000Z"

    spool.append(serialize_event(first))
    with pytest.raises(CorpusEventConflict, match="different bytes"):
        spool.append(serialize_event(second))


def test_publisher_falls_back_to_sqlite_when_redis_is_unavailable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "fallback.sqlite3"

    def fail_to_connect() -> Any:
        raise ConnectionError("Redis is offline")

    publisher = CorpusEventPublisher(
        redis_client_factory=fail_to_connect,
        sqlite_path=path,
    )
    event = _event()
    payload = serialize_event(event)

    assert publisher.publish(event) == "sqlite_spool"
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT event_json FROM stencil_corpus_spool_events WHERE event_id = ?",
            (event["event_id"],),
        ).fetchone()
    assert row is not None
    assert bytes(row[0]) == payload


def test_publisher_logs_critical_but_does_not_raise_when_both_paths_fail(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    # A regular file cannot be used as the parent directory of the spool path.
    obstruction = tmp_path / "not-a-directory"
    obstruction.write_text("occupied", encoding="utf-8")

    def fail_to_connect() -> Any:
        raise ConnectionError("Redis unavailable")

    publisher = CorpusEventPublisher(
        redis_client_factory=fail_to_connect,
        sqlite_path=obstruction / "corpus.sqlite3",
    )
    event = _event()

    assert publisher.publish(event) == "failed"
    assert "Could not persist stencil corpus event" in caplog.text
    assert "Redis unavailable" in caplog.text
    assert "SQLite spool error" in caplog.text
    assert event["event_id"] in caplog.text
    assert any(record.levelno == logging.CRITICAL for record in caplog.records)


def test_publisher_requires_valid_canonical_event_before_transport(tmp_path: Path) -> None:
    client = _FakeRedis()
    publisher = CorpusEventPublisher(
        redis_client_factory=lambda: client,
        sqlite_path=tmp_path / "spool.sqlite3",
    )
    malformed = _event()
    malformed["instance"]["cell_count"] = 1

    with pytest.raises(ValueError, match="canonical key"):
        publisher.publish(malformed)
    assert client.calls == []


def test_spool_backups_existing_unversioned_database_before_initializing(
    tmp_path: Path,
) -> None:
    path = tmp_path / "existing.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE legacy_data (value TEXT)")
        connection.execute("INSERT INTO legacy_data VALUES ('keep')")

    SQLiteEventSpool(path).append(serialize_event(_event()))

    backups = list(tmp_path.glob("existing.sqlite3.pre-v1-*.bak"))
    assert len(backups) == 1
    with sqlite3.connect(backups[0]) as backup:
        assert backup.execute("SELECT value FROM legacy_data").fetchone() == ("keep",)


def test_corpus_capture_is_enabled_by_default_with_explicit_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KATAPROXY_STENCIL_CORPUS_ENABLED", raising=False)
    assert corpus_events_enabled() is True
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "false")
    assert corpus_events_enabled() is False
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "0")
    assert corpus_events_enabled() is False
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "true")
    assert corpus_events_enabled() is True


class _CapturingPublisher:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def publish(self, event: dict[str, Any]) -> str:
        self.events.append(event)
        return "redis_stream"


class _FakeBackend:
    @property
    def identity(self) -> SolverIdentity:
        config = '{"mode":"event-test"}'
        return SolverIdentity(
            backend_name="event-test",
            backend_version="1",
            adapter_version="event-test-v1",
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


def test_shared_runtime_publishes_real_attempt_with_the_selected_path() -> None:
    family = _family()
    key, _frame = region_components._component_shape_key((0, 1), 2, family)
    publisher = _CapturingPublisher()
    execution = solve_component(
        _FakeBackend(),
        key,
        family,
        SolverBudget(
            wall_time_limit_ms=10_000,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="bulk-10000ms",
        ),
        producer_path="bulk",
        event_publisher=publisher,
    )

    assert execution.started_at_utc.endswith("Z")
    assert execution.reported_outcome.status is OutcomeStatus.OPTIMAL
    assert execution.placements_disposition == "validated"
    assert len(publisher.events) == 1
    assert publisher.events[0]["event_type"] == "solver_attempt"
    assert publisher.events[0]["producer_path"] == "bulk"
    assert publisher.events[0]["elapsed_wall_ns"] == execution.elapsed_wall_ns



def test_foreground_sighting_is_emitted_before_the_result_cache_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = _family()
    encoded = geometry.encode_cached_placements([("1x2", (0, 1))], family)
    events: list[str] = []

    def record_sighting(
        shape_keys: tuple[bytes, ...], seen_family: Any, producer_path: str,
    ) -> None:
        assert shape_keys
        assert tuple(seen_family) == family
        assert producer_path == "foreground"
        events.append("sighting")

    def cache_hit(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        assert request.budget.wall_time_limit_ms == 250
        assert tuple(request.family) == family
        events.append("cache")
        return True, 0, encoded

    monkeypatch.setattr(region_solver, "publish_instance_seen_many", record_sighting)
    monkeypatch.setattr(region_solver, "_solve_component_cached", cache_hit)
    result = region_solver._solve_region([0, 1], 2, 1, family)

    assert result["optimal"] is True
    assert events == ["sighting", "cache"]


def test_disconnected_components_are_published_in_one_batch_before_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = (("1x1", ((0, 0),)),)
    events: list[str] = []
    captured_keys: list[bytes] = []

    def record_batch(
        shape_keys: tuple[bytes, ...], seen_family: Any, producer_path: str,
    ) -> None:
        assert tuple(seen_family) == family
        assert producer_path == "foreground"
        captured_keys.extend(shape_keys)
        events.append("batch")

    def cache_hit(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        assert events == ["batch"] or events == ["batch", "cache"]
        events.append("cache")
        _width, _height, cells = geometry.decode_component_shape_key(
            request.shape_key,
        )
        encoded = geometry.encode_cached_placements(
            [("1x1", (point,)) for point in cells], family,
        )
        return True, 0, encoded

    monkeypatch.setattr(region_solver, "publish_instance_seen_many", record_batch)
    monkeypatch.setattr(region_solver, "_solve_component_cached", cache_hit)
    result = region_solver._solve_region([0, 1, 8], 3, 3, family)

    assert len(captured_keys) == 2
    assert events == ["batch", "cache", "cache"]
    assert result["tileCount"] == 3



def test_solver_result_is_not_lost_when_corpus_sink_raises() -> None:
    class _BrokenPublisher:
        def publish(self, _event: dict[str, Any]) -> str:
            raise OSError("spool offline")

    family = _family()
    key, _frame = region_components._component_shape_key((0, 1), 2, family)
    execution = solve_component(
        _FakeBackend(),
        key,
        family,
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
        event_publisher=_BrokenPublisher(),
    )

    assert execution.outcome.status is OutcomeStatus.OPTIMAL
    assert execution.cache_result[0:2] == (True, 0)
