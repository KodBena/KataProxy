# SPDX-License-Identifier: MIT

from __future__ import annotations

from itertools import product

import pytest

from transformers.stencil_cover.stencil_ownership import OwnershipEvidence
from transformers.stencil_cover.stencil_ownership_forest import (
    ReducedOwnershipProblem,
    aggregate_interval,
    budget_exact_marginal_labeling,
    build_reduced_problem,
    forest_majorize_minimize,
    full_energy_from_reduction,
    majorizer_energy,
    mm_unary_adjustments,
    reduced_energy,
    solve_forest_dp,
    spanning_forest_edges,
)
from transformers.stencil_cover.stencil_ownership_graphcut import ownership_graph
from transformers.stencil_cover.stencil_ownership_repair import ownership_energy
from scripts.run_ownership_forest_mm import _method_metrics


def _evidence(
    means: tuple[float, ...],
    deviations: tuple[float, ...] | None = None,
    width: int | None = None,
    height: int | None = None,
    board: tuple[str | None, ...] | None = None,
    dead_black: tuple[int, ...] = (),
    dead_white: tuple[int, ...] = (),
) -> OwnershipEvidence:
    resolved_width = width if width is not None else len(means)
    resolved_height = height if height is not None else 1
    resolved_board = board if board is not None else tuple(None for _ in means)
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


def _unary_energy(
    problem: ReducedOwnershipProblem,
    labels: dict[int, int],
) -> float:
    return sum(
        problem.unary_absolute[index] * abs(labels[point])
        + problem.unary_linear[index] * labels[point]
        for index, point in enumerate(problem.points)
    )


def _enumerated_minimum(
    problem: ReducedOwnershipProblem,
    retained_edges: tuple[tuple[int, int], ...],
) -> float:
    points = problem.points
    candidates: list[float] = []
    for values in product((-1, 0, 1), repeat=len(points)):
        labels = dict(zip(points, values))
        total = sum(values)
        if not problem.aggregate_lower <= total <= problem.aggregate_upper:
            continue
        unary = sum(
            problem.unary_absolute[index] * abs(labels[point])
            + problem.unary_linear[index] * labels[point]
            for index, point in enumerate(points)
        )
        spatial = problem.beta * sum(
            abs(labels[first] - labels[second])
            for first, second in retained_edges
        )
        candidates.append(unary + spatial)
    assert candidates
    return min(candidates)


def test_dead_stone_interactions_fold_into_unaries_without_changing_full_energy() -> None:
    evidence = _evidence(
        means=(0.2, 0.0, -0.2, 0.1, 0.0, -0.1),
        deviations=(0.02, 0.0, 0.03, 0.04, 0.0, 0.05),
        width=3,
        height=2,
        board=(None, "B", None, None, "W", None),
        dead_black=(1,),
        dead_white=(4,),
    )
    kappa = 1.0
    beta = 0.25
    problem = build_reduced_problem(evidence, kappa, beta, 0.5)
    empty_labels = {0: 1, 2: -1, 3: 1, 5: -1}
    full_labels = {**empty_labels, 1: 1, 4: -1}
    _neighbours, edges = ownership_graph(
        tuple(sorted(full_labels)), evidence.width, evidence.height,
    )

    expected = ownership_energy(
        full_labels,
        evidence,
        evidence.empty_points,
        edges,
        kappa,
        beta,
    )
    assert problem.edges == ((0, 3), (2, 5))
    assert problem.fixed_edge_offset == pytest.approx(6.0 * beta)
    assert full_energy_from_reduction(problem, empty_labels) == pytest.approx(expected)


def test_marginal_sort_is_exact_for_the_separable_global_budget_problem() -> None:
    evidence = _evidence(
        means=(0.8, -0.6, 0.2, -0.1),
        deviations=(0.1, 0.03, 0.2, 0.01),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    labels = budget_exact_marginal_labeling(problem)

    assert problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper
    assert _unary_energy(problem, labels) == pytest.approx(
        _enumerated_minimum(problem, ()),
    )


def test_forest_dp_matches_enumeration_and_preserves_global_exchange() -> None:
    evidence = _evidence(
        means=(0.6, -0.4, -0.4, 0.6),
        deviations=(0.05, 0.07, 0.04, 0.09),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    retained = ((0, 1), (2, 3))
    result = solve_forest_dp(problem, retained)

    assert result.component_count == 2
    assert sum(label for _point, label in result.labeling) == 0
    assert result.surrogate_energy == pytest.approx(
        _enumerated_minimum(problem, retained),
    )


def test_empty_retained_forest_equals_budget_exact_marginal_sort() -> None:
    evidence = _evidence(
        means=(0.3, -0.7, 0.8, -0.2, 0.1),
        deviations=(0.02, 0.08, 0.04, 0.03, 0.09),
        width=5,
        height=1,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    marginal = budget_exact_marginal_labeling(problem)
    forest = solve_forest_dp(problem, ())

    separable_optimum = _enumerated_minimum(problem, ())
    assert forest.surrogate_energy == pytest.approx(separable_optimum)
    assert _unary_energy(problem, marginal) == pytest.approx(separable_optimum)
    assert _unary_energy(problem, dict(forest.labeling)) == pytest.approx(
        separable_optimum,
    )


def test_forest_dp_rejects_a_cycle() -> None:
    evidence = _evidence(
        means=(0.1, -0.1, 0.2, -0.2),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)

    with pytest.raises(ValueError, match="acyclic"):
        solve_forest_dp(problem, ((0, 1), (0, 2), (1, 3), (2, 3)))


def test_majorizer_touches_and_upper_bounds_original_energy() -> None:
    evidence = _evidence(
        means=(0.75, -0.65, -0.55, 0.7),
        deviations=(0.03, 0.04, 0.06, 0.02),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    retained = spanning_forest_edges(problem)
    reference = budget_exact_marginal_labeling(problem)
    adjustments = mm_unary_adjustments(problem, retained, reference)
    reference_energy = reduced_energy(problem, reference)

    assert majorizer_energy(problem, retained, reference, reference) == pytest.approx(
        reference_energy,
    )
    for candidate_values in product((-1, 0, 1), repeat=len(problem.points)):
        candidate = dict(zip(problem.points, candidate_values))
        total = sum(candidate_values)
        if not problem.aggregate_lower <= total <= problem.aggregate_upper:
            continue
        assert majorizer_energy(problem, retained, reference, candidate) + 1e-9 >= (
            reduced_energy(problem, candidate)
        )
    assert set(adjustments) == set(problem.points)


def test_forest_mm_is_feasible_and_non_increasing() -> None:
    evidence = _evidence(
        means=(0.8, -0.75, 0.65, -0.7, 0.15, -0.1),
        deviations=(0.03, 0.02, 0.04, 0.05, 0.03, 0.02),
        width=3,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    forest = spanning_forest_edges(problem)
    result = forest_majorize_minimize(problem, forest, 6)

    assert problem.aggregate_lower <= sum(label for _point, label in result.labeling)
    assert sum(label for _point, label in result.labeling) <= problem.aggregate_upper
    assert result.energy <= result.initial_energy + 1e-9
    previous = result.initial_energy
    for iteration in result.iterations:
        assert iteration.original_energy_after <= previous + 1e-9
        assert iteration.surrogate_energy <= iteration.surrogate_at_reference + 1e-9
        previous = iteration.original_energy_after
    assert result.energy == pytest.approx(full_energy_from_reduction(
        problem, dict(result.labeling),
    ))


def test_aggregate_interval_rejects_unattainable_integer_sum() -> None:
    with pytest.raises(ValueError, match="no attainable integer"):
        aggregate_interval((0.1, 0.1), 0.0)



def test_runner_metrics_report_aggregate_and_full_graph_tv_explicitly() -> None:
    candidate = {0: 1, 1: 1, 2: -1, 3: -1}
    reference = {0: 1, 1: 0, 2: 0, 3: -1}
    edges = ((0, 1), (0, 2), (1, 3), (2, 3))

    metrics = _method_metrics(
        candidate,
        candidate_energy=3.0,
        reference=reference,
        reference_energy=2.5,
        reference_is_optimal=True,
        reference_lower_bound=2.0,
        empty_points=(0, 1, 2, 3),
        all_edges=edges,
        expected_empty_sum=0.0,
        aggregate_lower=0,
        aggregate_upper=0,
        beta=0.25,
    )

    assert metrics["aggregate_empty_sum"] == 0
    assert metrics["aggregate_expected_empty_sum"] == 0.0
    assert metrics["aggregate_residual"] == 0.0
    assert metrics["aggregate_feasible"] is True
    assert metrics["reference_aggregate_feasible"] is True
    assert metrics["total_variation_energy"] == pytest.approx(1.0)
    assert metrics["reference_total_variation_energy"] == pytest.approx(1.0)
    assert metrics["boundary_edge_count"] == 2
    assert metrics["reference_boundary_edge_count"] == 4
    assert metrics["boundary_symmetric_difference"] == 2
    assert metrics["unary_energy"] == pytest.approx(2.0)
    assert metrics["unary_energy_difference_vs_reference"] == pytest.approx(0.5)

def test_forest_dp_progress_reports_structural_work_monotonically() -> None:
    evidence = _evidence(
        means=(0.6, -0.4, -0.4, 0.6),
        deviations=(0.05, 0.07, 0.04, 0.09),
        width=2,
        height=2,
    )
    problem = build_reduced_problem(evidence, 1.0, 0.25, 0.5)
    events: list[tuple[int, int, int]] = []

    result = solve_forest_dp(
        problem,
        spanning_forest_edges(problem),
        progress=lambda completed, total, states: events.append(
            (completed, total, states)
        ),
    )

    assert events
    assert events[-1][0] == events[-1][1]
    assert all(
        0 < completed <= total
        for completed, total, _states in events
    )
    assert all(
        previous[0] <= current[0]
        and previous[1] == current[1]
        and previous[2] <= current[2]
        for previous, current in zip(events, events[1:])
    )
    assert events[-1][2] == result.dp_state_count

