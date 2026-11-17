# SPDX-License-Identifier: MIT

from __future__ import annotations

from itertools import product

import pytest

from transformers.stencil_cover.stencil_ownership import OwnershipEvidence
from transformers.stencil_cover.stencil_ownership_forest import (
    ReducedOwnershipProblem,
    build_reduced_problem,
    full_energy_from_reduction,
)
from transformers.stencil_cover.stencil_ownership_continuous_tv import (
    continuous_tv_relaxation,
)


def _evidence(
    means: tuple[float, ...],
    *,
    width: int,
    height: int,
    board: tuple[str | None, ...] | None = None,
) -> OwnershipEvidence:
    board_cells = tuple(None for _ in means) if board is None else board
    return OwnershipEvidence(
        ownership=means,
        ownership_stdev=tuple(0.05 for _ in means),
        board=board_cells,
        width=width,
        height=height,
        empty_points=tuple(
            point for point, colour in enumerate(board_cells) if colour is None
        ),
        dead_black=(),
        dead_white=(),
        policy_metadata={},
    )


def _exact_energy(problem: ReducedOwnershipProblem) -> float:
    candidates: list[float] = []
    for values in product((-1, 0, 1), repeat=len(problem.points)):
        total = sum(values)
        if not problem.aggregate_lower <= total <= problem.aggregate_upper:
            continue
        candidates.append(full_energy_from_reduction(
            problem, dict(zip(problem.points, values)),
        ))
    assert candidates
    return min(candidates)


def test_continuous_tv_dual_bound_is_below_exhaustive_integer_optimum() -> None:
    evidence = _evidence(
        (0.72, -0.61, 0.28, -0.19),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    result = continuous_tv_relaxation(
        problem, evidence, maximum_iterations=1500, tolerance=1e-5,
    )

    assert result.lower_bound <= _exact_energy(problem) + 1e-5
    for labeling in (result.coarea_labeling, result.argmax_labeling):
        labels = dict(labeling)
        assert tuple(sorted(labels)) == problem.points
        assert problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper
        assert full_energy_from_reduction(problem, labels) >= result.lower_bound - 1e-5
    assert result.iterations > 0
    assert result.coarea_candidate_count >= 1
    assert result.solver_message


def test_continuous_tv_roundings_are_deterministic() -> None:
    evidence = _evidence(
        (0.65, 0.65, 0.65, 0.65, 0.65, 0.65),
        width=3,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    first = continuous_tv_relaxation(problem, evidence, maximum_iterations=500)
    second = continuous_tv_relaxation(problem, evidence, maximum_iterations=500)

    assert first == second


def test_continuous_tv_handles_zero_dimensional_problem() -> None:
    evidence = _evidence((0.0,), width=1, height=1, board=("B",))
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    result = continuous_tv_relaxation(problem, evidence)

    assert result.coarea_labeling == ()
    assert result.argmax_labeling == ()
    assert result.lower_bound == pytest.approx(problem.fixed_edge_offset)
    assert result.primal_dual_gap == 0.0
    assert result.iterations == 0


def test_continuous_tv_rejects_invalid_iteration_budget() -> None:
    evidence = _evidence((0.2,), width=1, height=1)
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    with pytest.raises(ValueError, match="maximum_iterations"):
        continuous_tv_relaxation(problem, evidence, maximum_iterations=0)


def test_continuous_tv_rejects_boolean_tolerance() -> None:
    evidence = _evidence((0.2,), width=1, height=1)
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    with pytest.raises(ValueError, match="tolerance"):
        continuous_tv_relaxation(problem, evidence, tolerance=True)
