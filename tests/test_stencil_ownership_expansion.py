# SPDX-License-Identifier: MIT

from __future__ import annotations

from itertools import product

import pytest

from transformers.stencil_cover.stencil_ownership import OwnershipEvidence
from transformers.stencil_cover.stencil_ownership_expansion import (
    _binary_expansion_move,
    ordered_label_expansion,
)
from transformers.stencil_cover.stencil_ownership_forest import (
    ReducedOwnershipProblem,
    build_reduced_problem,
    budget_exact_marginal_labeling,
    full_energy_from_reduction,
)
from transformers.stencil_cover.stencil_ownership_repair import ownership_energy
from transformers.stencil_cover.stencil_ownership_graphcut import ownership_graph


def _evidence(
    means: tuple[float, ...],
    *,
    deviations: tuple[float, ...] | None = None,
    width: int | None = None,
    height: int | None = None,
    board: tuple[str | None, ...] | None = None,
    dead_black: tuple[int, ...] = (),
    dead_white: tuple[int, ...] = (),
) -> OwnershipEvidence:
    resolved_width = len(means) if width is None else width
    resolved_height = 1 if height is None else height
    resolved_board = tuple(None for _ in means) if board is None else board
    return OwnershipEvidence(
        ownership=means,
        ownership_stdev=(
            tuple(0.05 for _ in means) if deviations is None else deviations
        ),
        board=resolved_board,
        width=resolved_width,
        height=resolved_height,
        empty_points=tuple(
            point for point, colour in enumerate(resolved_board) if colour is None
        ),
        dead_black=dead_black,
        dead_white=dead_white,
        policy_metadata={},
    )


def _move_energy(
    problem: ReducedOwnershipProblem,
    labels: dict[int, int],
) -> float:
    index = {point: position for position, point in enumerate(problem.points)}
    unary = sum(
        problem.unary_absolute[index[point]] * abs(labels[point])
        + problem.unary_linear[index[point]] * labels[point]
        for point in problem.points
    )
    spatial = problem.beta * sum(
        abs(labels[first] - labels[second])
        for first, second in problem.edges
    )
    return float(unary + spatial)


def test_binary_expansion_cut_matches_exhaustive_move_minimum() -> None:
    evidence = _evidence(
        (0.72, -0.61, 0.24, -0.18),
        deviations=(0.03, 0.08, 0.04, 0.07),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    current = budget_exact_marginal_labeling(problem)

    for alpha in (-1, 0, 1):
        actual = _binary_expansion_move(problem, current, alpha, 1_000_000_000)
        assert all(value in (current[point], alpha) for point, value in actual.items())
        actual_energy = _move_energy(problem, actual)
        enumerated = min(
            _move_energy(
                problem,
                {
                    point: alpha if choose_alpha else current[point]
                    for point, choose_alpha in zip(problem.points, choices)
                },
            )
            for choices in product((False, True), repeat=len(problem.points))
        )
        assert actual_energy == pytest.approx(enumerated, abs=2e-7)


def test_ordered_expansion_is_feasible_monotone_and_energy_cross_checked() -> None:
    evidence = _evidence(
        (0.8, -0.75, 0.65, -0.7, 0.15, -0.1),
        deviations=(0.03, 0.02, 0.04, 0.05, 0.03, 0.02),
        width=3,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    result = ordered_label_expansion(
        problem, evidence, kappa=1.0, maximum_passes=6,
    )

    assert result.checkpoints[0].energy == pytest.approx(result.initial_energy)
    assert result.checkpoints[-1].energy == pytest.approx(result.final_energy)
    previous = result.initial_energy
    for checkpoint in result.checkpoints:
        labels = dict(checkpoint.labeling)
        assert problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper
        assert checkpoint.energy == pytest.approx(
            full_energy_from_reduction(problem, labels),
        )
        assert checkpoint.energy <= previous + 1e-9
        previous = checkpoint.energy
    assert result.final_energy <= result.initial_energy + 1e-9
    assert result.cut_calls == 3 * result.passes_completed
    assert result.accepted_moves >= 0


def test_ordered_expansion_uses_aggregate_repair_and_keeps_interval() -> None:
    evidence = _evidence(
        (0.65, 0.65, 0.65, 0.65, 0.65, 0.65),
        deviations=(0.02, 0.02, 0.02, 0.02, 0.02, 0.02),
        width=3,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    assert problem.aggregate_lower == 4
    assert problem.aggregate_upper == 4

    result = ordered_label_expansion(problem, evidence, kappa=1.0)
    assert result.repair_attempts > 0
    labels = dict(result.labeling)
    assert sum(labels.values()) == 4
    assert problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper
    assert result.final_energy <= result.initial_energy + 1e-9


def test_ordered_expansion_rejects_invalid_controls() -> None:
    evidence = _evidence((0.3, -0.2, 0.1))
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    with pytest.raises(ValueError, match="maximum_passes"):
        ordered_label_expansion(problem, evidence, kappa=1.0, maximum_passes=-1)
    with pytest.raises(ValueError, match="capacity_scale"):
        ordered_label_expansion(problem, evidence, kappa=1.0, capacity_scale=0)


def test_ordered_expansion_full_energy_includes_fixed_dead_stone_interactions() -> None:
    evidence = _evidence(
        (0.35, 0.0, -0.3, 0.1, 0.0, -0.1),
        deviations=(0.02, 0.04, 0.03, 0.05, 0.03, 0.02),
        width=3,
        height=2,
        board=(None, "B", None, None, "W", None),
        dead_black=(1,),
        dead_white=(4,),
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    result = ordered_label_expansion(problem, evidence, kappa=1.0)
    full_labels = {
        **{point: 1 for point in evidence.dead_black},
        **{point: -1 for point in evidence.dead_white},
        **dict(result.labeling),
    }
    _neighbours, edges = ownership_graph(
        tuple(sorted(full_labels)), evidence.width, evidence.height,
    )
    full_energy = ownership_energy(
        full_labels, evidence, evidence.empty_points, edges, 1.0, 0.25,
    )
    assert full_energy == pytest.approx(result.final_energy)
