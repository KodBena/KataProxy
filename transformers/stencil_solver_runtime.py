"""Shared canonical-problem construction and backend invocation boundary."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from transformers.stencil_cache_storage import _stable_digest
from transformers.stencil_solver_api import (
    BackendStatusUnavailable, CanonicalProblem, Incumbent, LowerBoundUnavailable,
    NativeEffortUnavailable, NoIncumbent, NoIncumbentReason, NoNativeEffortReason,
    NoSolverFailure, OutcomeStatus, PlacementWitness, SolverBackend, SolverBudget,
    SolverFailure, SolverIdentity, SolverOutcome, STENCIL_CANONICALIZATION_VERSION,
)
from transformers.stencil_solver_geometry import (
    Stencil, decode_component_shape_key, encode_cached_placements,
    verify_canonical_placements,
)

logger = logging.getLogger("kataproxy.stencil_solver_runtime")


@dataclass(frozen=True)
class SolverExecution:
    canonical_key: bytes
    problem: CanonicalProblem
    identity: SolverIdentity
    outcome: SolverOutcome
    reported_outcome: SolverOutcome
    started_at_utc: str
    elapsed_wall_ns: int
    placements_disposition: str
    cache_result: tuple[bool, int, bytes]
    fallback_used: bool


def canonical_problem_from_key(
    shape_key: bytes, family: Sequence[Stencil],
) -> CanonicalProblem:
    width, height, flat_cells = decode_component_shape_key(shape_key)
    occupied_cells = tuple((point // width, point % width) for point in flat_cells)
    family_value = tuple(
        (name, tuple((int(y), int(x)) for y, x in cells))
        for name, cells in family
    )
    return CanonicalProblem(
        width=width,
        height=height,
        occupied_cells=occupied_cells,
        stencil_family=family_value,
        family_fingerprint=_stable_digest(family_value),
        canonicalization_version=STENCIL_CANONICALIZATION_VERSION,
    )


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
) -> SolverExecution:
    """Shared foreground/background path; no cache, Redis, SQLite, or request state."""
    problem = canonical_problem_from_key(shape_key, family)
    identity = backend.identity
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
    return SolverExecution(
        canonical_key=shape_key,
        problem=problem,
        identity=identity,
        outcome=normalized_outcome,
        reported_outcome=reported_outcome,
        started_at_utc=started_at_utc,
        elapsed_wall_ns=elapsed_wall_ns,
        placements_disposition=placements_disposition,
        cache_result=cache_result,
        fallback_used=fallback_used,
    )
