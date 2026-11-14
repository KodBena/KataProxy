from __future__ import annotations

import base64
import hashlib
import json
import math
import uuid
import sqlite3
import struct
from pathlib import Path
from typing import Any

import pytest

from scripts.analyze_ownership_corpus import (
    build_sensitivity,
    build_summary,
    export_record,
    load_archive,
)
from transformers.stencil_cover.stencil_ownership import (
    OwnershipRegions,
    make_ownership_evidence,
)
from transformers.stencil_cover.stencil_ownership_coherent import CoherentOwnershipPolicy
from transformers.stencil_cover.stencil_ownership_cut import CoherentCutOwnershipPolicy
from transformers.stencil_cover.stencil_ownership_solver_api import (
    OwnershipLabeling,
    OwnershipOptimizationRecord,
    OwnershipSolverStatus,
)
from transformers.stencil_cover.stencil_ownership_store import SQLiteOwnershipStore
from transformers.stencil_cover.stencil_solver_api import (
    NoNativeEffortLimit,
    SolverBudget,
    SolverIdentity,
)


def _evidence(
    means: tuple[float, ...],
    stdev: tuple[float, ...] = (0.05, 0.05, 0.05, 0.05),
) -> Any:
    return make_ownership_evidence(
        {"ownership": list(means), "ownershipStdev": list(stdev)},
        [None, None, None, None],
        2,
        2,
        {},
        requires_ownership_stdev=True,
    )


def _record(
    store: SQLiteOwnershipStore,
    policy: CoherentOwnershipPolicy | CoherentCutOwnershipPolicy,
    evidence: Any,
    positive: tuple[int, ...],
    negative: tuple[int, ...],
) -> Any:
    regions = OwnershipRegions(
        positive=positive,
        negative=negative,
        policy_id=policy.policy_id,
        diagnostics={"solverStatus": "unit_test"},
    )
    return store.record(
        evidence,
        regions,
        policy,
        turn_number=11,
        is_during_search=True,
        response_context={"rootInfo": {"visits": 64}},
    )


def test_coherent_policy_uses_a_better_feasible_warm_start() -> None:
    evidence = _evidence((0.8, -0.7, 0.2, -0.3))
    warm_start = OwnershipLabeling(((0, 0), (1, 0), (2, 0), (3, 0)))
    policy = CoherentOwnershipPolicy(beta=10.0, time_limit_ms=250)

    result = policy.classify(evidence, initial_labeling=warm_start)

    assert result.solver_evidence is not None
    assert result.solver_evidence.initial_heuristic == warm_start
    assert result.diagnostics["warmStartUsed"] is True
    assert result.diagnostics["warmStartObjective"] == 0.0


def test_coherent_policy_rejects_an_infeasible_warm_start() -> None:
    evidence = _evidence((0.8, -0.7, 0.2, -0.3))
    infeasible = OwnershipLabeling(((0, 1), (1, 1), (2, 1), (3, 1)))

    with pytest.raises(ValueError, match="aggregate interval"):
        CoherentOwnershipPolicy(time_limit_ms=50).classify(
            evidence,
            initial_labeling=infeasible,
        )


def test_summary_uses_shared_energy_and_compares_matching_coherent_models(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    evidence = _evidence((0.8, -0.7, 0.2, -0.3))
    mip = CoherentOwnershipPolicy()
    cut = CoherentCutOwnershipPolicy()

    _record(store, mip, evidence, (0,), (1,))
    _record(store, cut, evidence, (2,), (1,))

    rows = load_archive(path)
    summary = build_summary(rows)

    assert summary["observation_count"] == 2
    assert summary["distinct_evidence_count"] == 1
    assert summary["model_count"] == 2
    assert len(summary["models"]) == 2
    models = {model["policy_id"]: model for model in summary["models"]}
    assert models["coherent"]["coherent_aggregate_feasibility_rate"] == 1.0
    assert models["coherent-cut"]["coherent_aggregate_feasibility_rate"] == 1.0
    assert models["coherent"]["reference_comparison_count"] == 1
    assert models["coherent-cut"]["reference_comparison_count"] == 1
    assert models["coherent"]["coherent_energy_gap"]["p50"] == 0.0
    assert models["coherent-cut"]["coherent_energy_gap"]["p50"] > 0.0
    assert models["coherent-cut"]["coherent_energy_ratio"]["p50"] > 1.0
    tally = summary["unique_problem_tally"]
    assert tally["unique_problem_count"] == 1
    assert tally["fully_solved_count"] == 0
    assert tally["approximate_feasible_count"] == 1
    assert tally["no_feasible_witness_count"] == 0
    assert tally["best_reference_source_counts"] == {"selected_output": 1}


def test_sensitivity_pairs_only_identical_replayed_boards(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    policy = CoherentOwnershipPolicy()

    _record(
        store,
        policy,
        _evidence((0.8, -0.7, 0.2, -0.3)),
        (0,),
        (1,),
    )
    _record(
        store,
        policy,
        _evidence((0.79, -0.69, 0.19, -0.29)),
        (2,),
        (1,),
    )

    result = build_sensitivity(load_archive(path))
    assert result["potential_pair_count"] == 1
    assert result["evaluated_pair_count"] == 1
    assert result["sampled_group_count"] == 0
    assert result["sampled_snapshot_count"] == 0
    assert result["returned_pair_count"] == 1
    assert "no game/session identifier" in result["pair_scope"]
    pair = result["pairs"][0]
    assert pair["same_evidence"] is False
    assert pair["continuous_input_drift"]["ownership_l2"] == pytest.approx(math.sqrt(0.0004))
    assert pair["output_sensitivity"]["empty_hamming_count"] == 2
    assert pair["output_sensitivity"]["empty_hamming_fraction"] == 0.5
    assert pair["output_sensitivity"]["net_sum_jump"] == 0
    assert pair["energy_continuity"]["absolute_energy_delta_per_empty_mean_l2"] is not None
    assert pair["single_snapshot_diagnostics"]["first_aggregate_feasible"] is True
    assert pair["single_snapshot_diagnostics"]["second_aggregate_feasible"] is True


def test_jsonl_export_preserves_the_exact_binary_evidence_blobs(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    means = (0.125, -0.375, math.nextafter(0.5, 1.0), -0.0)
    deviations = (0.01, 0.02, 0.03, 0.04)
    _record(
        store,
        CoherentOwnershipPolicy(),
        _evidence(means, deviations),
        (0,),
        (1,),
    )

    row = load_archive(path)[0]
    exported = export_record(row)
    evidence = exported["evidence"]
    blobs = evidence["raw_blobs_base64"]
    assert base64.b64decode(blobs["ownership"]) == struct.pack("<4d", *means)
    assert base64.b64decode(blobs["ownership_stdev"]) == struct.pack("<4d", *deviations)
    assert base64.b64decode(blobs["board"]) == bytes([0, 0, 0, 0])
    assert exported["model"]["model_json_raw"] == json.dumps(
        row.model_document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    assert evidence["codecs"]["ownership"] == "ieee754-binary64-little-endian-v1"


def test_analysis_rejects_future_schema_without_migrating_it(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(ValueError, match="unsupported ownership corpus schema version 99"):
        load_archive(path)

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)


def test_export_decodes_attempt_witnesses_and_solver_identity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(path)
    policy = CoherentOwnershipPolicy()
    evidence = _evidence((0.8, -0.7, 0.2, -0.3))
    configuration = '{"threads":1}'
    identity = SolverIdentity(
        backend_name="test-highs",
        backend_version="1.0",
        adapter_version="test-adapter-v1",
        configuration_json=configuration,
        configuration_fingerprint=hashlib.sha256(
            configuration.encode("utf-8")
        ).hexdigest(),
    )
    initial = OwnershipLabeling(((0, 1), (1, -1), (2, 0), (3, 0)))
    incumbent = OwnershipLabeling(((0, 1), (1, 0), (2, -1), (3, 0)))
    attempt = OwnershipOptimizationRecord(
        attempt_id=str(uuid.uuid4()),
        started_at_utc="2026-11-14T05:00:00.000000Z",
        elapsed_wall_ns=1_234_567,
        budget=SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="ownership-test-250ms",
        ),
        solver_identity=identity,
        status=OwnershipSolverStatus.FEASIBLE,
        initial_heuristic=initial,
        initial_objective=3.0,
        optimizer_incumbent=incumbent,
        optimizer_objective=2.5,
        selected_labeling=incumbent,
        selected_objective=2.5,
        lower_bound=2.0,
        absolute_gap=0.5,
        backend_reported_gap=0.2,
        backend_status="time_limit",
        error=None,
    )
    regions = OwnershipRegions(
        positive=(0,),
        negative=(2,),
        policy_id=policy.policy_id,
        solver_evidence=attempt,
    )
    store.record(
        evidence,
        regions,
        policy,
        turn_number=12,
        is_during_search=False,
    )

    row = load_archive(path)[0]
    assert row.attempt is not None
    assert row.attempt["status"] == "feasible"
    assert row.attempt["elapsed_wall_ns"] == 1_234_567
    assert row.attempt["initial_labels"] == {0: 1, 1: -1, 2: 0, 3: 0}
    assert row.attempt["optimizer_labels"] == {0: 1, 1: 0, 2: -1, 3: 0}
    assert row.solver_identity is not None
    assert row.solver_identity["backend_name"] == "test-highs"
    exported = export_record(row)
    assert base64.b64decode(
        exported["solver_attempt"]["raw_labeling_blobs_base64"]["initial"]
    ) == struct.pack("<I", 4) + b"".join(
        struct.pack("<Ib", point, label)
        for point, label in initial.point_labels
    )
    assert exported["solver_identity"]["configuration_json_raw"] == configuration
