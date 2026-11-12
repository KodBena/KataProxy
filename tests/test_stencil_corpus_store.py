# SPDX-License-Identifier: MIT
from __future__ import annotations

import copy
import hashlib
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from transformers import stencil_cover_enricher as stencil
from transformers.stencil_corpus_events import (
    make_instance_seen_event,
    make_solver_attempt_event,
    serialize_event,
)
from transformers.stencil_corpus_publisher import SQLiteEventSpool
from transformers.stencil_corpus_store import (
    CORPUS_DATABASE_VERSION,
    CorpusStoreConflict,
    MissingInstanceSighting,
    SQLiteCorpusStore,
)
from transformers.stencil_solver_api import (
    BackendStatusReported,
    Incumbent,
    NativeEffortReported,
    NoNativeEffortLimit,
    NoSolverFailure,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverBudget,
    SolverIdentity,
    SolverOutcome,
)
from transformers.stencil_solver_runtime import canonical_problem_from_key, solve_component


def _family() -> tuple[tuple[str, tuple[tuple[int, int], ...]], ...]:
    return (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )


def _key() -> bytes:
    value, _frame = stencil._component_shape_key((0, 1), 2, _family())
    return value


def _instance_event(
    event_id: str = "12345678-1234-5678-1234-567812345678",
    emitted_at: str = "2026-11-12T10:15:30.000000Z",
) -> dict[str, Any]:
    problem = canonical_problem_from_key(_key(), _family())
    return make_instance_seen_event(
        problem,
        _key(),
        event_id=event_id,
        emitted_at_utc=datetime.fromisoformat(emitted_at.replace("Z", "+00:00")),
    )


class _Backend:
    @property
    def identity(self) -> SolverIdentity:
        config = '{"mode":"sqlite-store-test"}'
        return SolverIdentity(
            backend_name="sqlite-store-test",
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
            native_effort=NativeEffortReported(3, "mip_nodes"),
            backend_status=BackendStatusReported("OPTIMAL"),
            backend_metrics_json='{"nodes":3}',
            error=NoSolverFailure(),
        )


class _DiscardPublisher:
    def publish(self, _event: dict[str, Any]) -> str:
        return "redis_stream"


def _attempt_event(
    event_id: str = "22345678-1234-5678-1234-567812345678",
) -> dict[str, Any]:
    execution = solve_component(
        _Backend(),
        _key(),
        _family(),
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
        event_publisher=_DiscardPublisher(),
    )
    return make_solver_attempt_event(
        execution,
        _key(),
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
        "foreground",
        event_id=event_id,
    )


def test_store_migrates_raw_spool_v1_to_normalized_v2_with_backup(
    tmp_path: Path,
) -> None:
    path = tmp_path / "corpus.sqlite3"
    spool = SQLiteEventSpool(path)
    first_payload = serialize_event(_instance_event())
    assert spool.append(first_payload) is True

    store = SQLiteCorpusStore(path)
    assert store.initialize() == CORPUS_DATABASE_VERSION
    assert store.counts() == {
        "instances": 0,
        "attempts": 0,
        "receipts": 0,
        "spool_pending": 1,
    }

    backups = list(tmp_path.glob("corpus.sqlite3.pre-v2-*.bak"))
    assert len(backups) == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert connection.execute(
            "SELECT event_json FROM stencil_corpus_spool_events"
        ).fetchone()[0] == first_payload


def test_instance_sighting_and_solver_attempt_materialize_with_identity() -> None:

    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteCorpusStore(Path(directory) / "corpus.sqlite3")
        seen = _instance_event()
        attempt = _attempt_event()

        assert store.ingest(serialize_event(seen)).inserted is True
        assert store.ingest(serialize_event(attempt)).inserted is True
        assert store.counts() == {
            "instances": 1,
            "attempts": 1,
            "receipts": 2,
            "spool_pending": 0,
        }

        with sqlite3.connect(store.path) as connection:
            instance = connection.execute(
                "SELECT family_fingerprint, canonical_width, canonical_height, "
                "cell_count, sighting_count FROM instances"
            ).fetchone()
            assert instance is not None
            assert instance[1:] == (2, 1, 2, 1)

            identity = connection.execute(
                "SELECT backend_name, configuration_json "
                "FROM solver_identities"
            ).fetchone()
            assert identity == ("sqlite-store-test", '{"mode":"sqlite-store-test"}')

            row = connection.execute(
                "SELECT source_path, budget_json, outcome_status, optimality_proven, "
                "incumbent_tile_count, lower_bound, native_effort_value, "
                "placements_disposition, fallback_disposition "
                "FROM solver_attempts"
            ).fetchone()
            assert row is not None
            assert row[0] == "foreground"
            assert row[2:] == ("optimal", 1, 1, 1, 3.0, "validated", "not_needed")


def test_duplicate_receipt_is_idempotent_but_conflicting_payload_is_rejected() -> None:

    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteCorpusStore(Path(directory) / "corpus.sqlite3")
        event = _instance_event()
        payload = serialize_event(event)
        assert store.ingest(payload).inserted is True
        assert store.ingest(payload).inserted is False

        conflicting = copy.deepcopy(event)
        conflicting["emitted_at_utc"] = "2026-11-12T10:15:31.000000Z"
        with pytest.raises(CorpusStoreConflict, match="different immutable bytes"):
            store.ingest(serialize_event(conflicting))

        with sqlite3.connect(store.path) as connection:
            assert connection.execute(
                "SELECT sighting_count FROM instances"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT COUNT(*) FROM corpus_event_receipts"
            ).fetchone() == (1,)


def test_attempt_before_instance_sighting_is_deferred_without_a_receipt() -> None:

    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteCorpusStore(Path(directory) / "corpus.sqlite3")
        attempt = _attempt_event()
        with pytest.raises(MissingInstanceSighting):
            store.ingest(serialize_event(attempt))
        assert store.counts()["receipts"] == 0

        store.ingest(serialize_event(_instance_event()))
        assert store.ingest(serialize_event(attempt)).inserted is True


def test_spool_ingestion_removes_raw_row_in_the_same_transaction() -> None:

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "corpus.sqlite3"
        spool = SQLiteEventSpool(path)
        store = SQLiteCorpusStore(path)
        event = _instance_event()
        payload = serialize_event(event)
        assert spool.append(payload) is True

        items = store.pending_spool_events()
        assert len(items) == 1
        result = store.ingest_spool_event(items[0])

        assert result.inserted is True
        assert store.counts() == {
            "instances": 1,
            "attempts": 0,
            "receipts": 1,
            "spool_pending": 0,
        }


def test_replaying_spooled_event_after_stream_ingest_only_deletes_spool_row() -> None:

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "corpus.sqlite3"
        spool = SQLiteEventSpool(path)
        store = SQLiteCorpusStore(path)
        payload = serialize_event(_instance_event())
        store.ingest(payload)
        spool.append(payload)

        item = store.pending_spool_events()[0]
        assert store.ingest_spool_event(item).inserted is False
        assert store.counts()["receipts"] == 1
        assert store.counts()["instances"] == 1
        assert store.counts()["spool_pending"] == 0


def test_store_rejects_a_database_from_a_newer_schema(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(RuntimeError, match="newer than this writer"):
        SQLiteCorpusStore(path).initialize()


def test_each_instance_sighting_updates_global_and_path_aggregates() -> None:

    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteCorpusStore(Path(directory) / "corpus.sqlite3")
        first = _instance_event(
            event_id="12345678-1234-5678-1234-567812345678",
            emitted_at="2026-11-12T10:15:30.000000Z",
        )
        second = _instance_event(
            event_id="32345678-1234-5678-1234-567812345678",
            emitted_at="2026-11-12T10:15:31.000000Z",
        )
        second["producer_path"] = "bulk"

        store.ingest(serialize_event(first))
        store.ingest(serialize_event(second))

        with sqlite3.connect(store.path) as connection:
            assert connection.execute(
                "SELECT sighting_count, first_seen_at_utc, last_seen_at_utc FROM instances"
            ).fetchone() == (
                2, "2026-11-12T10:15:30.000000Z", "2026-11-12T10:15:31.000000Z",
            )
            paths = connection.execute(
                "SELECT producer_path, sighting_count FROM instance_sighting_paths "
                "ORDER BY producer_path"
            ).fetchall()
            assert paths == [("bulk", 1), ("foreground", 1)]



def test_spool_row_changed_after_read_is_not_deleted_or_materialized() -> None:

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "corpus.sqlite3"
        spool = SQLiteEventSpool(path)
        store = SQLiteCorpusStore(path)
        first = _instance_event()
        payload = serialize_event(first)
        assert spool.append(payload) is True
        item = store.pending_spool_events()[0]

        conflicting = copy.deepcopy(first)
        conflicting["emitted_at_utc"] = "2026-11-12T10:15:32.000000Z"
        conflicting_payload = serialize_event(conflicting)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE stencil_corpus_spool_events "
                "SET event_json = ?, payload_sha256 = ? WHERE event_id = ?",
                (
                    conflicting_payload,
                    hashlib.sha256(conflicting_payload).hexdigest(),
                    item.event_id,
                ),
            )

        with pytest.raises(CorpusStoreConflict, match="changed after it was read"):
            store.ingest_spool_event(item)
        assert store.counts()["spool_pending"] == 1
        assert store.counts()["receipts"] == 0
