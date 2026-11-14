"""Shared canonical-problem construction and backend invocation boundary."""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from .stencil_corpus_events import ProducerPath, make_solver_attempt_event
from .stencil_corpus_publisher import (
    CorpusEventSink, get_default_event_publisher,
)
from .stencil_solver_api import (
    BackendStatusUnavailable, CanonicalProblem, Incumbent, LowerBoundUnavailable,
    NativeEffortUnavailable, NoIncumbent, NoIncumbentReason, NoNativeEffortReason,
    NoSolverFailure, OutcomeStatus, PlacementWitness, SolverBackend, SolverBudget,
    SolverFailure, SolverIdentity, SolverOutcome,
)
from .stencil_solver_problem import canonical_problem_from_key
from .stencil_solver_geometry import (
    Stencil, encode_cached_placements, verify_canonical_placements,
)

logger = logging.getLogger("kataproxy.stencil_solver_runtime")


@dataclass(frozen=True)
class SolverExecution:
    canonical_key: bytes
    problem: CanonicalProblem
    identity: SolverIdentity
    attempt_id: str
    outcome: SolverOutcome
    reported_outcome: SolverOutcome
    started_at_utc: str
    elapsed_wall_ns: int
    placements_disposition: str
    cache_result: tuple[bool, int, bytes]
    fallback_used: bool
    result_provenance: dict[str, Any]


def _error_outcome(exc: Exception) -> SolverOutcome:
    return SolverOutcome(
        status=OutcomeStatus.ERROR,
        incumbent=NoIncumbent(NoIncumbentReason.INVOCATION_FAILED),
        lower_bound=LowerBoundUnavailable(),
        native_effort=NativeEffortUnavailable(
            NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT
        ),
        backend_status=BackendStatusUnavailable(),
        backend_metrics_json="{}",
        error=SolverFailure(
            error_type=type(exc).__name__,
            message=str(exc) or type(exc).__name__,
            details_json=json.dumps(
                {"adapter_exception": True}, sort_keys=True, separators=(",", ":")
            ),
        ),
    )


def _cache_result(
    problem: CanonicalProblem, outcome: SolverOutcome,
) -> tuple[SolverOutcome, tuple[bool, int, bytes], bool, str]:
    placements_disposition = "not_returned"
    if (
        isinstance(outcome.incumbent, Incumbent)
        and isinstance(outcome.incumbent.placement_witness, PlacementWitness)
        and outcome.status in (OutcomeStatus.OPTIMAL, OutcomeStatus.FEASIBLE)
    ):
        placements = outcome.incumbent.placement_witness.placements
        try:
            verify_canonical_placements(
                problem.occupied_cells, placements, problem.stencil_family
            )
            flat_placements = [
                (name, tuple(y * problem.width + x for y, x in cells))
                for name, cells in placements
            ]
            encoded = encode_cached_placements(flat_placements, problem.stencil_family)
            optimal = outcome.status is OutcomeStatus.OPTIMAL
            return outcome, (optimal, 0 if optimal else 1, encoded), False, "validated"
        except Exception as exc:
            placements_disposition = "rejected_invalid"
            logger.error("Rejected invalid solver cover: %s", exc)
            # A proof paired with a malformed cover is not a validated result.
            # Preserve backend diagnostics while marking the invocation ERROR.
            outcome = SolverOutcome(
                status=OutcomeStatus.ERROR,
                incumbent=NoIncumbent(NoIncumbentReason.INVOCATION_FAILED),
                lower_bound=outcome.lower_bound,
                native_effort=outcome.native_effort,
                backend_status=outcome.backend_status,
                backend_metrics_json=outcome.backend_metrics_json,
                error=SolverFailure(
                    error_type="InvalidPlacementWitness",
                    message=str(exc) or "solver returned an invalid placement witness",
                    details_json=json.dumps(
                        {"validation_failure": True},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )

    # Preserve the legacy fallback choice: prefer an existing one-cell family
    # member in family order, otherwise use the synthetic "1x1" name (index 255).
    singleton_name = next(
        (name for name, cells in problem.stencil_family if len(cells) == 1),
        "1x1",
    )
    singleton_cover = [
        (singleton_name, (y * problem.width + x,))
        for y, x in problem.occupied_cells
    ]
    return (
        outcome,
        (False, 2, encode_cached_placements(singleton_cover, problem.stencil_family)),
        True,
        placements_disposition,
    )


def solve_component(
    backend: SolverBackend,
    shape_key: bytes,
    family: Sequence[Stencil],
    budget: SolverBudget,
    *,
    producer_path: ProducerPath = "foreground",
    event_publisher: CorpusEventSink | None = None,
) -> SolverExecution:
    """Shared foreground/background path; no cache, Redis, SQLite, or request state."""
    problem = canonical_problem_from_key(shape_key, family)
    identity = backend.identity
    attempt_id = str(uuid.uuid4())
    started_at_utc = datetime.now(timezone.utc).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")
    started_ns = time.perf_counter_ns()
    try:
        outcome = backend.solve(problem, budget)
        if not isinstance(outcome, SolverOutcome):
            raise TypeError("solver backend returned a value that is not SolverOutcome")
    except Exception as exc:
        outcome = _error_outcome(exc)
    elapsed_wall_ns = time.perf_counter_ns() - started_ns
    reported_outcome = outcome
    (
        normalized_outcome, cache_result, fallback_used, placements_disposition,
    ) = _cache_result(problem, outcome)
    execution = SolverExecution(
        canonical_key=shape_key,
        problem=problem,
        identity=identity,
        attempt_id=attempt_id,
        outcome=normalized_outcome,
        reported_outcome=reported_outcome,
        started_at_utc=started_at_utc,
        elapsed_wall_ns=elapsed_wall_ns,
        placements_disposition=placements_disposition,
        cache_result=cache_result,
        fallback_used=fallback_used,
        result_provenance={"disposition": "legacy_unknown"},
    )
    selected_publisher = (
        get_default_event_publisher() if event_publisher is None else event_publisher
    )
    try:
        event = make_solver_attempt_event(
            execution, shape_key, budget, producer_path, event_id=attempt_id,
        )
        outcome_document = event["outcome"]
        provenance = {
            "disposition": "present",
            "attempt_id": attempt_id,
            "solver_identity": event["solver_identity"],
            "producer_path": event["producer_path"],
            "budget": event["budget"],
            "started_at_utc": event["started_at_utc"],
            "elapsed_wall_ns": event["elapsed_wall_ns"],
            "outcome": {
                "status": outcome_document["status"],
                "optimality_proven": outcome_document["optimality_proven"],
                "incumbent": outcome_document["incumbent"],
                "lower_bound": outcome_document["lower_bound"],
                "native_effort": outcome_document["native_effort"],
                "backend_status": outcome_document["backend_status"],
                "error": outcome_document["error"],
                "fallback_disposition": event["fallback_disposition"],
            },
        }
        execution = SolverExecution(
            canonical_key=execution.canonical_key,
            problem=execution.problem,
            identity=execution.identity,
            attempt_id=execution.attempt_id,
            outcome=execution.outcome,
            reported_outcome=execution.reported_outcome,
            started_at_utc=execution.started_at_utc,
            elapsed_wall_ns=execution.elapsed_wall_ns,
            placements_disposition=execution.placements_disposition,
            cache_result=execution.cache_result,
            fallback_used=execution.fallback_used,
            result_provenance=provenance,
        )
        if selected_publisher is not None:
            selected_publisher.publish(event)
    except Exception:
        # Provenance/event capture must never suppress or alter the solver result.
        logger.exception(
            "Could not construct or publish stencil solver_attempt event for key %s",
            shape_key.hex(),
        )
    return execution
