# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import struct
import uuid
from pathlib import Path
from typing import Any

import pytest

from transformers.stencil_cover.stencil_ownership import (
    OwnershipRegions,
    make_ownership_evidence,
)
from transformers.stencil_cover.stencil_ownership_coherent import CoherentOwnershipPolicy
from transformers.stencil_cover.stencil_ownership_threshold import ThresholdOwnershipPolicy
from transformers.stencil_cover import stencil_region_solver as region_solver
import sproxy_config as cfg
from transformers.stencil_cover.stencil_ownership_solver_api import (
    OwnershipLabeling,
    OwnershipOptimizationRecord,
    OwnershipSolverStatus,
)
from transformers.stencil_cover.stencil_ownership_store import (
    OWNERSHIP_DATABASE_VERSION,
    OWNERSHIP_FLOAT_CODEC,
    OWNERSHIP_LABEL_CODEC,
    OwnershipStoreSchemaError,
    SQLiteOwnershipStore,
)
from transformers.stencil_cover.stencil_solver_api import (
    NoNativeEffortLimit,
    SolverBudget,
    SolverIdentity,
)


def _evidence(
    means: tuple[float, ...] = (0.8, -0.7, 0.2, -0.1),
    stdev: tuple[float, ...] | None = (0.05, 0.1, 0.2, 0.3),
) -> Any:
    opaque: dict[str, Any] = {"ownership": list(means)}
    if stdev is not None:
        opaque["ownershipStdev"] = list(stdev)
    return make_ownership_evidence(
        opaque,
        [None] * len(means),
        2,
        2,
        {},
        requires_ownership_stdev=stdev is not None,
    )


def _regions() -> OwnershipRegions:
    return OwnershipRegions(
        positive=(0,),
        negative=(1,),
        policy_id="coherent",
        diagnostics={"objective": 1.25, "optimal": False, "solverStatus": "seed_only"},
    )


def _connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("ATTACH DATABASE ? AS ownership", (str(path),))
    return connection


def _optimization_record() -> OwnershipOptimizationRecord:
    configuration = '{"formulation":"unit-test"}'
    identity = SolverIdentity(
        backend_name="test-highs",
        backend_version="1.0",
        adapter_version="ownership-test-v1",
        configuration_json=configuration,
        configuration_fingerprint=hashlib.sha256(
            configuration.encode("utf-8")
        ).hexdigest(),
    )
    initial = OwnershipLabeling(((0, 1), (1, 0), (2, -1), (3, 0)))
    optimizer = OwnershipLabeling(((0, 1), (1, -1), (2, 0), (3, 0)))
    return OwnershipOptimizationRecord(
        attempt_id=str(uuid.uuid4()),
        started_at_utc="2026-11-14T05:00:00.000000Z",
        elapsed_wall_ns=1234567,
        budget=SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="ownership-test-250ms",
        ),
        solver_identity=identity,
        status=OwnershipSolverStatus.OPTIMAL,
        initial_heuristic=initial,
        initial_objective=4.5,
        optimizer_incumbent=optimizer,
        optimizer_objective=3.25,
        selected_labeling=optimizer,
        selected_objective=3.25,
        lower_bound=3.25,
        absolute_gap=0.0,
        backend_reported_gap=0.0,
        backend_status="optimal",
        error=None,
    )


def _record(
    store: SQLiteOwnershipStore,
    evidence: Any,
    policy: CoherentOwnershipPolicy | None = None,
) -> Any:
    selected_policy = (
        CoherentOwnershipPolicy(time_limit_ms=250) if policy is None else policy
    )
    return store.record(
        evidence,
        _regions(),
        selected_policy,
        turn_number=7,
        is_during_search=True,
        response_context={
            "rootInfo": {"scoreLead": 1.75, "visits": 128},
            "winner": "W",
        },
    )


def test_store_attaches_a_separate_schema_and_initializes_its_own_version(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)

    assert store.initialize() == OWNERSHIP_DATABASE_VERSION
    receipt = _record(store, _evidence())

    connection = _connection(path)
    try:
        databases = {
            str(row[1]): str(row[2])
            for row in connection.execute("PRAGMA database_list").fetchall()
        }
        assert databases["ownership"] == str(path)
        assert connection.execute("PRAGMA ownership.user_version").fetchone() == (
            OWNERSHIP_DATABASE_VERSION,
        )
        assert connection.execute(
            "SELECT name FROM ownership.sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall() == [
            ("attempts",),
            ("evidence",),
            ("models",),
            ("observations",),
            ("schema_migrations",),
            ("solver_identities",),
        ]
        assert receipt.attempt_id is None
    finally:
        connection.close()


def test_raw_maps_are_stored_as_lossless_binary64_and_board_is_preserved(
    tmp_path: Path,
) -> None:
    means = (0.125, -0.375, math.nextafter(0.5, 1.0), -0.0)
    deviations = (0.01, 0.02, 0.03, 0.04)
    evidence = _evidence(means, deviations)
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    receipt = _record(store, evidence)

    connection = _connection(path)
    try:
        row = connection.execute(
            """
            SELECT ownership_codec, ownership_blob, stdev_blob,
                   board_codec, board_blob
            FROM ownership.evidence WHERE evidence_id = ?
            """,
            (receipt.evidence_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == OWNERSHIP_FLOAT_CODEC
        assert bytes(row[1]) == struct.pack("<4d", *means)
        assert bytes(row[2]) == struct.pack("<4d", *deviations)
        assert row[3] == "cell-state-u8-empty0-black1-white2-v1"
        assert bytes(row[4]) == bytes([0, 0, 0, 0])
    finally:
        connection.close()


def test_identical_evidence_is_deduplicated_but_each_response_is_an_observation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    evidence = _evidence()

    first = _record(
        store, evidence,
        CoherentOwnershipPolicy(kappa=0.8, beta=0.3, time_limit_ms=250),
    )
    second = _record(
        store, evidence,
        CoherentOwnershipPolicy(kappa=0.8, beta=0.3, time_limit_ms=2_000),
    )

    assert first.evidence_id == second.evidence_id
    assert first.model_id == second.model_id
    assert first.observation_id != second.observation_id
    assert store.counts() == {
        "evidence": 1,
        "models": 1,
        "attempts": 0,
        "observations": 2,
    }


def test_model_identity_changes_with_objective_parameters_not_time_budget(
    tmp_path: Path,
) -> None:
    store = SQLiteOwnershipStore(tmp_path / "ownership.sqlite3")
    evidence = _evidence()
    first = _record(store, evidence, CoherentOwnershipPolicy(kappa=0.8, beta=0.3))
    same_model_new_budget = _record(
        store, evidence,
        CoherentOwnershipPolicy(kappa=0.8, beta=0.3, time_limit_ms=4_000),
    )
    different_model = _record(
        store, evidence,
        CoherentOwnershipPolicy(kappa=0.9, beta=0.3, time_limit_ms=250),
    )

    assert first.model_id == same_model_new_budget.model_id
    assert first.model_id != different_model.model_id
    assert store.counts()["evidence"] == 1
    assert store.counts()["models"] == 2
    assert store.counts()["observations"] == 3


def test_distinct_binary64_inputs_do_not_merge_after_rounding(tmp_path: Path) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    first_value = 0.25
    second_value = math.nextafter(first_value, 1.0)
    first = _record(store, _evidence((first_value, -0.7, 0.2, -0.1)))
    second = _record(store, _evidence((second_value, -0.7, 0.2, -0.1)))

    assert first.evidence_id != second.evidence_id
    assert store.counts()["evidence"] == 2


def test_attempt_keeps_seed_optimizer_witness_and_selected_output_separate(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    evidence = _evidence()
    base = _regions()
    record = _optimization_record()
    assert record.initial_heuristic is not None
    assert record.optimizer_incumbent is not None
    selected = OwnershipRegions(
        positive=(0,),
        negative=(1,),
        policy_id="coherent",
        diagnostics=base.diagnostics,
        solver_evidence=record,
    )
    receipt = store.record(
        evidence,
        selected,
        CoherentOwnershipPolicy(),
        turn_number=3,
        is_during_search=False,
        response_context={"rootInfo": {"scoreLead": 2.0}},
    )

    assert receipt.attempt_id == record.attempt_id
    connection = _connection(path)
    try:
        attempt = connection.execute(
            """
            SELECT status, elapsed_wall_ns, initial_labeling_blob,
                   initial_objective, optimizer_labeling_blob,
                   optimizer_objective, lower_bound_disposition,
                   lower_bound, absolute_gap, budget_json
            FROM ownership.attempts WHERE attempt_id = ?
            """,
            (record.attempt_id,),
        ).fetchone()
        observation = connection.execute(
            """
            SELECT attempt_id, selected_labeling_codec, selected_labeling_blob,
                   diagnostics_json, context_json
            FROM ownership.observations WHERE observation_id = ?
            """,
            (receipt.observation_id,),
        ).fetchone()
        assert attempt is not None
        assert attempt[0] == "optimal"
        assert attempt[1] == 1234567
        assert bytes(attempt[2]) == struct.pack(
            "<I", 4,
        ) + b"".join(
            struct.pack("<Ib", point, label)
            for point, label in record.initial_heuristic.point_labels
        )
        assert attempt[3] == 4.5
        assert bytes(attempt[4]) == struct.pack(
            "<I", 4,
        ) + b"".join(
            struct.pack("<Ib", point, label)
            for point, label in record.optimizer_incumbent.point_labels
        )
        assert attempt[5:9] == (3.25, "reported", 3.25, 0.0)
        assert json.loads(str(attempt[9]))["wall_time_limit_ms"] == 250
        assert observation is not None
        assert observation[0] == record.attempt_id
        assert observation[1] == OWNERSHIP_LABEL_CODEC
        assert bytes(observation[2]) == bytes(attempt[4])
        assert json.loads(str(observation[3]))["objective"] == 1.25
        assert json.loads(str(observation[4]))["rootInfo"]["scoreLead"] == 2.0
    finally:
        connection.close()


def test_store_rejects_a_database_with_a_newer_schema(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(OwnershipStoreSchemaError, match="newer than this writer"):
        SQLiteOwnershipStore(path).initialize()

def test_region_builder_persists_each_observation_directly_to_the_ownership_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_CORPUS_ENABLED", True)
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_CORPUS_DB", path)
    def no_prepared_components(
        _points: Any,
        _width: Any,
        _height: Any,
        _family: Any,
    ) -> list[Any]:
        return []

    def no_sighting_publish(
        _keys: Any,
        _family: Any,
        _producer: Any,
    ) -> None:
        return None

    def empty_region_result(
        _prepared: Any,
        _width: Any,
        _family: Any,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        return {
            "components": [],
            "placements": [],
            "tileCount": 0,
            "optimal": True,
            "status": "empty",
        }

    monkeypatch.setattr(
        region_solver, "_prepare_region_components", no_prepared_components,
    )
    monkeypatch.setattr(
        region_solver, "publish_instance_seen_many", no_sighting_publish,
    )
    monkeypatch.setattr(
        region_solver, "_solve_prepared_region", empty_region_result,
    )

    result = region_solver._build_stencil_cover(
        {
            "ownership": [0.0, 0.0, 0.0, 0.0],
            "ownershipStdev": [0.1, 0.2, 0.3, 0.4],
        },
        [None] * 4,
        2,
        2,
        {"truncation_threshold": 0.0},
        ThresholdOwnershipPolicy(),
        turn_number=9,
        is_during_search=True,
        response_context={"rootInfo": {"scoreLead": 2.25}},
    )

    assert result["ownershipPolicy"] == "threshold"
    assert SQLiteOwnershipStore(path).counts() == {
        "evidence": 1,
        "models": 1,
        "attempts": 0,
        "observations": 1,
    }
    connection = _connection(path)
    try:
        observation = connection.execute(
            "SELECT turn_number, is_during_search, context_json "
            "FROM ownership.observations"
        ).fetchone()
        assert observation is not None
        assert observation[0:2] == (9, 1)
        assert json.loads(str(observation[2]))["rootInfo"]["scoreLead"] == 2.25
        evidence_row = connection.execute(
            "SELECT stdev_disposition, stdev_blob FROM ownership.evidence"
        ).fetchone()
        assert evidence_row is not None
        assert evidence_row[0] == "present"
        assert bytes(evidence_row[1]) == struct.pack("<4d", 0.1, 0.2, 0.3, 0.4)
    finally:
        connection.close()


def test_attempt_retains_a_certified_lower_bound_without_an_incumbent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    evidence = _evidence()
    base = _optimization_record()
    assert base.initial_heuristic is not None
    assert base.initial_objective is not None
    seed = base.initial_heuristic
    seed_objective = base.initial_objective
    no_incumbent = OwnershipOptimizationRecord(
        attempt_id=str(uuid.uuid4()),
        started_at_utc="2026-11-14T05:01:00.000000Z",
        elapsed_wall_ns=7654321,
        budget=base.budget,
        solver_identity=base.solver_identity,
        status=OwnershipSolverStatus.SEED_ONLY,
        initial_heuristic=seed,
        initial_objective=seed_objective,
        optimizer_incumbent=None,
        optimizer_objective=None,
        selected_labeling=seed,
        selected_objective=seed_objective,
        lower_bound=1.5,
        absolute_gap=None,
        backend_reported_gap=None,
        backend_status="time_limit_no_incumbent",
        error=None,
    )
    regions = OwnershipRegions(
        positive=(0,),
        negative=(2,),
        policy_id="coherent",
        diagnostics={"solverStatus": "seed_only", "objective": seed_objective},
        solver_evidence=no_incumbent,
    )
    receipt = store.record(
        evidence,
        regions,
        CoherentOwnershipPolicy(),
        turn_number=4,
        is_during_search=False,
    )
    connection = _connection(path)
    try:
        attempt = connection.execute(
            "SELECT optimizer_disposition, lower_bound_disposition, "
            "lower_bound, absolute_gap FROM ownership.attempts WHERE attempt_id = ?",
            (receipt.attempt_id,),
        ).fetchone()
        assert attempt == ("not_returned", "reported", 1.5, None)
    finally:
        connection.close()


