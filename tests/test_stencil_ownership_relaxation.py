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
from transformers.stencil_cover.stencil_ownership_relaxation import (
    ordered_label_lp_relaxation,
)


def _evidence(
    means: tuple[float, ...],
    *,
    width: int,
    height: int,
    deviations: tuple[float, ...] | None = None,
) -> OwnershipEvidence:
    return OwnershipEvidence(
        ownership=means,
        ownership_stdev=(
            tuple(0.05 for _ in means) if deviations is None else deviations
        ),
        board=tuple(None for _ in means),
        width=width,
        height=height,
        empty_points=tuple(range(len(means))),
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
        labels = dict(zip(problem.points, values))
        candidates.append(full_energy_from_reduction(problem, labels))
    assert candidates
    return min(candidates)


def test_ordered_label_lp_bound_is_below_exhaustive_integer_optimum() -> None:
    evidence = _evidence(
        (0.72, -0.61, 0.28, -0.19),
        width=2,
        height=2,
        deviations=(0.03, 0.07, 0.04, 0.06),
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    result = ordered_label_lp_relaxation(problem)

    assert result.lower_bound <= _exact_energy(problem) + 1e-7
    labels = dict(result.labeling)
    assert problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper
    assert full_energy_from_reduction(problem, labels) >= result.lower_bound - 1e-7
    assert result.threshold_count >= 1
    assert result.solver_message


def test_ordered_label_lp_rounding_is_deterministic_and_full_energy_consistent() -> None:
    evidence = _evidence(
        (0.65, 0.65, 0.65, 0.65, 0.65, 0.65),
        width=3,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    first = ordered_label_lp_relaxation(problem)
    second = ordered_label_lp_relaxation(problem)

    assert first == second
    assert dict(first.labeling).keys() == set(problem.points)
    assert problem.aggregate_lower <= sum(
        label for _point, label in first.labeling
    ) <= problem.aggregate_upper
    assert first.lower_bound <= full_energy_from_reduction(
        problem, dict(first.labeling),
    ) + 1e-7


def test_ordered_label_lp_handles_empty_problem() -> None:
    evidence = _evidence((), width=1, height=1)
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    result = ordered_label_lp_relaxation(problem)

    assert result.labeling == ()
    assert result.lower_bound == pytest.approx(problem.fixed_edge_offset)
    assert result.threshold_count == 0
