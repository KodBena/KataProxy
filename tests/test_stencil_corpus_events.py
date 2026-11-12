# SPDX-License-Identifier: MIT
from __future__ import annotations

import base64
import copy
import hashlib
from types import SimpleNamespace
from typing import Any

import pytest

from transformers import stencil_cover_enricher as stencil
from transformers.stencil_corpus_events import (
    CORPUS_EVENT_SCHEMA,
    PLACEMENT_PAYLOAD_CODEC,
    CorpusEventError,
    instance_id_for,
    make_instance_seen_event,
    make_solver_attempt_event,
    parse_event,
    serialize_event,
    validate_event,
)
from transformers.stencil_solver_api import (
    BackendStatusReported,
    Incumbent,
    LowerBoundUnavailable,
    NativeEffortReported,
    NativeEffortUnavailable,
    NoIncumbent,
    NoIncumbentReason,
    NoNativeEffortLimit,
    NoNativeEffortReason,
    NoSolverFailure,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverBudget,
    SolverFailure,
    SolverIdentity,
    SolverOutcome,
)
from transformers.stencil_solver_runtime import canonical_problem_from_key


def _family() -> tuple[tuple[str, tuple[tuple[int, int], ...]], ...]:
    return (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )


def _key_and_problem() -> tuple[bytes, Any]:
    key, _frame = stencil._component_shape_key((0, 1), 2, _family())
    return key, canonical_problem_from_key(key, _family())


def _identity() -> SolverIdentity:
    config = '{"presolve":true}'
    return SolverIdentity(
        backend_name="test-highs-substitute",
        backend_version="1.0",
        adapter_version="fake-adapter-v1",
        configuration_json=config,
        configuration_fingerprint=hashlib.sha256(config.encode("utf-8")).hexdigest(),
    )


def _budget() -> SolverBudget:
    return SolverBudget(
        wall_time_limit_ms=250,
        native_effort_limit=NoNativeEffortLimit(),
        policy_id="foreground-250ms",
    )


def _execution(
    outcome: SolverOutcome,
    *,
    normalized: SolverOutcome | None = None,
    placements_disposition: str = "validated",
    fallback_used: bool = False,
) -> Any:
    _key, problem = _key_and_problem()
    return SimpleNamespace(
        problem=problem,
        identity=_identity(),
        reported_outcome=outcome,
        outcome=outcome if normalized is None else normalized,
        started_at_utc="2026-11-12T10:15:30.000000Z",
        elapsed_wall_ns=3_400_000,
        placements_disposition=placements_disposition,
        fallback_used=fallback_used,
    )


def _optimal_outcome() -> SolverOutcome:
    return SolverOutcome(
        status=OutcomeStatus.OPTIMAL,
        incumbent=Incumbent(
            1, PlacementWitness((("1x2", ((0, 0), (0, 1))),)),
        ),
        lower_bound=ProvenLowerBound(1),
        native_effort=NativeEffortReported(42, "mip_nodes"),
        backend_status=BackendStatusReported("OPTIMAL"),
        backend_metrics_json='{"mip_gap":0.0,"nodes":42}',
        error=NoSolverFailure(),
    )


def test_instance_seen_event_has_frozen_identity_and_round_trips_canonically() -> None:
    key, problem = _key_and_problem()
    event = make_instance_seen_event(
        problem,
        key,
        event_id="12345678-1234-5678-1234-567812345678",
    )

    assert event["schema"] == CORPUS_EVENT_SCHEMA
    assert event["event_type"] == "instance_seen"
    assert event["producer_path"] == "foreground"
    instance = event["instance"]
    assert instance["family_fingerprint"] == problem.family_fingerprint
    assert instance["canonicalization_version"] == problem.canonicalization_version
    assert instance["cell_count"] == 2
    assert instance_id_for(
        problem.family_fingerprint, problem.canonicalization_version, key,
    ) == instance_id_for(
        instance["family_fingerprint"],
        instance["canonicalization_version"],
        base64.b64decode(instance["canonical_key_b64"]),
    )

    encoded = serialize_event(event)
    assert parse_event(encoded) == event
    assert serialize_event(parse_event(encoded)) == encoded


def test_solver_attempt_event_carries_identity_budget_metrics_and_validated_witness() -> None:
    key, _problem = _key_and_problem()
    event = make_solver_attempt_event(
        _execution(_optimal_outcome()),
        key,
        _budget(),
        "foreground",
        event_id="12345678-1234-5678-1234-567812345678",
    )

    assert event["event_type"] == "solver_attempt"
    assert event["attempt_id"] == event["event_id"]
    assert event["solver_identity"]["fingerprint"] == _identity().fingerprint
    assert event["budget"]["wall_time_limit_ms"] == 250
    assert event["budget"]["native_effort_limit"] == {
        "disposition": "not_requested",
        "reason": "not_requested",
    }
    assert event["outcome"]["status"] == "optimal"
    assert event["outcome"]["optimality_proven"] is True
    assert event["outcome"]["incumbent"] == {
        "disposition": "present", "tile_count": 1,
    }
    assert event["outcome"]["lower_bound"] == {
        "disposition": "reported", "value": 1,
    }
    assert event["outcome"]["native_effort"] == {
        "disposition": "reported", "value": 42, "unit": "mip_nodes",
    }
    placements = event["outcome"]["placements"]
    assert placements["disposition"] == "validated"
    assert placements["codec_version"] == PLACEMENT_PAYLOAD_CODEC
    assert event["fallback_disposition"] == "not_needed"
    validate_event(event)


def test_no_incumbent_attempt_is_recorded_separately_from_singleton_fallback() -> None:
    key, _problem = _key_and_problem()
    outcome = SolverOutcome(
        status=OutcomeStatus.NO_INCUMBENT,
        incumbent=NoIncumbent(NoIncumbentReason.BUDGET_EXHAUSTED),
        lower_bound=LowerBoundUnavailable(),
        native_effort=NativeEffortUnavailable(
            NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT,
        ),
        backend_status=BackendStatusReported("TIME_LIMIT"),
        backend_metrics_json="{}",
        error=NoSolverFailure(),
    )
    event = make_solver_attempt_event(
        _execution(
            outcome,
            placements_disposition="not_returned",
            fallback_used=True,
        ),
        key,
        _budget(),
        "bulk",
    )
    assert event["producer_path"] == "bulk"
    assert event["outcome"]["status"] == "no_incumbent"
    assert event["outcome"]["error"] == {"disposition": "none"}
    assert event["outcome"]["placements"] == {"disposition": "not_returned"}
    assert event["fallback_disposition"] == "singleton_fallback_used"
    validate_event(event)


def test_invalid_witness_records_rejection_and_preserves_raw_incumbent_objective() -> None:
    key, _problem = _key_and_problem()
    raw = _optimal_outcome()
    normalized = SolverOutcome(
        status=OutcomeStatus.ERROR,
        incumbent=NoIncumbent(NoIncumbentReason.INVOCATION_FAILED),
        lower_bound=raw.lower_bound,
        native_effort=raw.native_effort,
        backend_status=raw.backend_status,
        backend_metrics_json=raw.backend_metrics_json,
        error=SolverFailure(
            "InvalidPlacementWitness",
            "overlapping placement",
            '{"validation_failure":true}',
        ),
    )
    event = make_solver_attempt_event(
        _execution(
            raw,
            normalized=normalized,
            placements_disposition="rejected_invalid",
            fallback_used=True,
        ),
        key,
        _budget(),
        "foreground",
    )
    assert event["outcome"]["status"] == "error"
    assert event["outcome"]["incumbent"] == {
        "disposition": "present", "tile_count": 1,
    }
    assert event["outcome"]["placements"] == {"disposition": "rejected_invalid"}
    assert event["outcome"]["error"]["disposition"] == "present"
    validate_event(event)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda event: event.update(event_id="NOT-A-UUID"), "canonical lowercase UUID"),
        (lambda event: event["instance"].update(cell_count=1), "disagree with canonical key"),
        (lambda event: event.update(attempt_id="different"), "attempt_id must equal event_id"),
        (lambda event: event["outcome"].update(optimality_proven=False), "disagrees with status"),
        (lambda event: event["outcome"]["lower_bound"].update(value=2),
         "matching proven bound"),
        (lambda event: event["outcome"]["native_effort"].update(value=float("nan")),
         "finite"),
    ],
)
def test_validator_rejects_inconsistent_event_envelopes(mutate: Any, message: str) -> None:
    key, _problem = _key_and_problem()
    event = make_solver_attempt_event(
        _execution(_optimal_outcome()),
        key,
        _budget(),
        "foreground",
        event_id="12345678-1234-5678-1234-567812345678",
    )
    invalid = copy.deepcopy(event)
    mutate(invalid)
    with pytest.raises(CorpusEventError, match=message):
        validate_event(invalid)


def test_event_parser_rejects_non_json_and_noncanonical_instance_keys() -> None:
    with pytest.raises(CorpusEventError, match="valid UTF-8 JSON"):
        parse_event(b"not json")

    key, problem = _key_and_problem()
    event = make_instance_seen_event(problem, key)
    event["instance"]["canonical_key_b64"] = base64.b64encode(b"bad").decode("ascii")
    with pytest.raises(CorpusEventError, match="valid component key"):
        serialize_event(event)


def test_serializer_rejects_nan_in_backend_metrics() -> None:
    key, _problem = _key_and_problem()
    outcome = SolverOutcome(
        status=OutcomeStatus.OPTIMAL,
        incumbent=Incumbent(1, PlacementWitness((("1x2", ((0, 0), (0, 1))),))),
        lower_bound=ProvenLowerBound(1),
        native_effort=NativeEffortUnavailable(
            NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
        ),
        backend_status=BackendStatusReported("OPTIMAL"),
        backend_metrics_json='{"unsafe":NaN}',
        error=NoSolverFailure(),
    )
    execution = _execution(outcome)
    with pytest.raises(ValueError):
        make_solver_attempt_event(execution, key, _budget(), "foreground")
