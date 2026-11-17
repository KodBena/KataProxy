# SPDX-License-Identifier: MIT
"""Continuous convex ordered-label TV relaxation for offline research.

The primal variables are two ordered thresholds (a, b), with
0 <= b <= a <= 1. The objective is affine unary cost plus graph total
variation on each threshold. The aggregate interval remains a hard convex
constraint. A deterministic Chambolle--Pock primal-dual method supplies a
dual lower bound and a fractional labeling; co-area and argmax roundings are
evaluated independently against the original integer objective.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

import numpy as np
from numpy.typing import NDArray

from .stencil_ownership import OwnershipEvidence
from .stencil_ownership_forest import (
    ReducedOwnershipProblem,
    budget_exact_marginal_labeling,
    full_energy_from_reduction,
    reduced_energy,
)
from .stencil_ownership_graphcut import ownership_graph
from .stencil_ownership_repair import component_ids


@dataclass(frozen=True)
class ContinuousTVResult:
    """Primal-dual relaxation and both deterministic integral roundings."""

    coarea_labeling: tuple[tuple[int, int], ...]
    argmax_labeling: tuple[tuple[int, int], ...]
    lower_bound: float
    feasible_primal_energy: float
    primal_dual_gap: float
    iterations: int
    converged: bool
    coarea_candidate_count: int
    coarea_repaired_count: int
    argmax_was_repaired: bool
    aggregate_primal_residual: float
    solver_message: str


def _project_ordered_triangle(
    first: NDArray[np.float64],
    second: NDArray[np.float64],
) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Euclidean projection onto {(a,b): 0 <= b <= a <= 1}."""
    inside = (second >= 0.0) & (second <= first) & (first <= 1.0)
    bottom_a = np.clip(first, 0.0, 1.0)
    bottom_b = np.zeros_like(second)
    right_a = np.ones_like(first)
    right_b = np.clip(second, 0.0, 1.0)
    diagonal = np.clip((first + second) * 0.5, 0.0, 1.0)
    bottom_distance = (first - bottom_a) ** 2 + second**2
    right_distance = (first - right_a) ** 2 + (second - right_b) ** 2
    diagonal_distance = (first - diagonal) ** 2 + (second - diagonal) ** 2
    use_bottom = (bottom_distance <= right_distance) & (bottom_distance <= diagonal_distance)
    use_right = (~use_bottom) & (right_distance <= diagonal_distance)
    projected_a = np.where(use_bottom, bottom_a, np.where(use_right, right_a, diagonal))
    projected_b = np.where(use_bottom, bottom_b, np.where(use_right, right_b, diagonal))
    return np.where(inside, first, projected_a), np.where(inside, second, projected_b)


def _coarea_candidates(
    problem: ReducedOwnershipProblem,
    first: NDArray[np.float64],
    second: NDArray[np.float64],
) -> tuple[dict[int, int], ...]:
    """Enumerate distinct common-threshold labelings at breakpoints and midpoints."""
    breakpoints = sorted({
        0.0, 1.0, *(float(value) for value in first),
        *(float(value) for value in second),
    })
    thresholds = sorted({
        *breakpoints,
        *((left + right) * 0.5 for left, right in zip(breakpoints, breakpoints[1:])),
    })
    candidates: dict[tuple[int, ...], dict[int, int]] = {}
    for threshold in thresholds:
        labels = {
            point: int(threshold <= first[index])
            + int(threshold <= second[index]) - 1
            for index, point in enumerate(problem.points)
        }
        candidates[tuple(labels[point] for point in problem.points)] = labels
    return tuple(candidates[key] for key in sorted(candidates))


def _argmax_rounding(
    problem: ReducedOwnershipProblem,
    first: NDArray[np.float64],
    second: NDArray[np.float64],
) -> dict[int, int]:
    """Argmax over simplex masses (1-a, a-b, b), ties ordered -1, 0, +1."""
    labels: dict[int, int] = {}
    for index, point in enumerate(problem.points):
        masses = (
            1.0 - float(first[index]),
            float(first[index] - second[index]),
            float(second[index]),
        )
        labels[point] = max(range(3), key=lambda item: (masses[item], -item)) - 1
    return labels


def _repair_to_interval(
    labels: Mapping[int, int],
    problem: ReducedOwnershipProblem,
    evidence: OwnershipEvidence,
) -> tuple[dict[int, int], bool, int]:
    """Deterministic cheapest one-cell repair using exact reduced unary costs."""
    repaired = dict(labels)
    current = sum(repaired.values())
    if problem.aggregate_lower <= current <= problem.aggregate_upper:
        return repaired, False, 0
    target = min(max(current, problem.aggregate_lower), problem.aggregate_upper)
    direction = 1 if target > current else -1
    eligible = tuple(sorted(
        set(evidence.empty_points) | set(evidence.dead_black) | set(evidence.dead_white)
    ))
    neighbours, _edges = ownership_graph(eligible, evidence.width, evidence.height)
    components = component_ids(eligible, neighbours)
    fixed_labels = {
        **{point: 1 for point in evidence.dead_black},
        **{point: -1 for point in evidence.dead_white},
    }
    point_index = {point: index for index, point in enumerate(problem.points)}
    seeds = {
        components[point] for point in eligible
        if repaired.get(point, fixed_labels.get(point)) == direction
    }
    move_count = 0
    while current != target:
        choices: list[tuple[float, int]] = []
        for point in problem.points:
            old = repaired[point]
            new = old + direction
            if new < -1 or new > 1:
                continue
            component = components[point]
            if (
                old == 0 and new == direction and component in seeds
                and not any(
                    repaired.get(adjacent, fixed_labels.get(adjacent)) == direction
                    for adjacent in neighbours[point]
                )
            ):
                continue
            position = point_index[point]
            unary_delta = (
                problem.unary_absolute[position] * (abs(new) - abs(old))
                + problem.unary_linear[position] * (new - old)
            )
            spatial_delta = problem.beta * sum(
                abs(new - repaired[adjacent]) - abs(old - repaired[adjacent])
                for adjacent in neighbours[point] if adjacent in repaired
            )
            choices.append((float(unary_delta + spatial_delta), point))
        if not choices:
            raise RuntimeError("no ownership label is eligible for continuous-TV aggregate repair")
        _delta, point = min(choices, key=lambda item: (item[0], item[1]))
        repaired[point] += direction
        current += direction
        move_count += 1
        if repaired[point] == direction:
            seeds.add(components[point])
    if not problem.aggregate_lower <= current <= problem.aggregate_upper:
        raise ArithmeticError("continuous-TV aggregate repair failed its postcondition")
    return repaired, True, move_count


def _best_coarea(
    candidates: tuple[dict[int, int], ...],
    problem: ReducedOwnershipProblem,
    evidence: OwnershipEvidence,
) -> tuple[dict[int, int], int]:
    scored: list[tuple[float, tuple[int, ...], dict[int, int]]] = []
    repaired_count = 0
    for candidate in candidates:
        labels, repaired, _moves = _repair_to_interval(candidate, problem, evidence)
        repaired_count += int(repaired)
        key = tuple(labels[point] for point in problem.points)
        scored.append((reduced_energy(problem, labels), key, labels))
    if not scored:
        fallback = budget_exact_marginal_labeling(problem)
        return fallback, 0
    return min(scored, key=lambda item: (item[0], item[1]))[2], repaired_count


def _operator_forward(
    first: NDArray[np.float64],
    second: NDArray[np.float64],
    edge_first: NDArray[np.int64],
    edge_second: NDArray[np.int64],
) -> tuple[NDArray[np.float64], NDArray[np.float64], float]:
    return (
        first[edge_first] - first[edge_second],
        second[edge_first] - second[edge_second],
        float(np.sum(first + second)),
    )


def _operator_adjoint(
    dual_first: NDArray[np.float64],
    dual_second: NDArray[np.float64],
    dual_sum: float,
    count: int,
    edge_first: NDArray[np.int64],
    edge_second: NDArray[np.int64],
) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    first = np.full(count, dual_sum, dtype=np.float64)
    second = np.full(count, dual_sum, dtype=np.float64)
    np.add.at(first, edge_first, dual_first)
    np.add.at(first, edge_second, -dual_first)
    np.add.at(second, edge_first, dual_second)
    np.add.at(second, edge_second, -dual_second)
    return first, second


def _dual_lower_bound(
    dual_first: NDArray[np.float64],
    dual_second: NDArray[np.float64],
    dual_sum: float,
    coefficient_first: NDArray[np.float64],
    coefficient_second: NDArray[np.float64],
    aggregate_lower_shifted: int,
    aggregate_upper_shifted: int,
    edge_first: NDArray[np.int64],
    edge_second: NDArray[np.int64],
) -> float:
    """Evaluate the Fenchel dual; TV dual feasibility is maintained by clipping."""
    adjoint_first, adjoint_second = _operator_adjoint(
        dual_first, dual_second, dual_sum, len(coefficient_first),
        edge_first, edge_second,
    )
    first = -adjoint_first - coefficient_first
    second = -adjoint_second - coefficient_second
    support = np.maximum(0.0, np.maximum(first, first + second))
    interval_support = (
        dual_sum * aggregate_upper_shifted
        if dual_sum >= 0.0 else dual_sum * aggregate_lower_shifted
    )
    return float(-np.sum(support) - interval_support)


def continuous_tv_relaxation(
    problem: ReducedOwnershipProblem,
    evidence: OwnershipEvidence,
    *,
    maximum_iterations: int = 2000,
    tolerance: float = 1e-5,
) -> ContinuousTVResult:
    """Solve the convex ordered-label TV relaxation with Chambolle--Pock.

    The hard aggregate interval is represented by one dual scalar. The returned
    dual objective is a lower bound at every iteration; a feasible continuous
    primal point is obtained by interpolating toward the budget-exact seed.
    """
    if isinstance(maximum_iterations, bool) or not isinstance(maximum_iterations, int) or maximum_iterations <= 0:
        raise ValueError("maximum_iterations must be a positive integer")
    if isinstance(tolerance, bool) or not isinstance(tolerance, (int, float)) or not math.isfinite(float(tolerance)) or tolerance < 0:
        raise ValueError("tolerance must be finite and non-negative")
    points = problem.points
    count = len(points)
    if count == 0:
        energy = float(problem.fixed_edge_offset)
        return ContinuousTVResult(
            (), (), energy, energy, 0.0, 0, True, 1, 0, False, 0.0,
            "empty problem; exact zero-dimensional relaxation",
        )

    point_index = {point: index for index, point in enumerate(points)}
    edge_first = np.asarray([point_index[a] for a, _ in problem.edges], dtype=np.int64)
    edge_second = np.asarray([point_index[b] for _, b in problem.edges], dtype=np.int64)
    absolute = np.asarray(problem.unary_absolute, dtype=np.float64)
    linear = np.asarray(problem.unary_linear, dtype=np.float64)
    coefficient_first = -absolute + linear
    coefficient_second = absolute + linear
    constant = float(np.sum(absolute - linear))

    # For a four-neighbour graph, ||D||^2 <= 8. The aggregate row has
    # squared norm 2n; the sum is a conservative bound for ||K||^2.
    operator_bound_squared = 8.0 + 2.0 * count
    step = 0.99 / math.sqrt(operator_bound_squared)
    tau = step
    sigma = step
    first = np.full(count, 0.5, dtype=np.float64)
    second = np.full(count, 0.25, dtype=np.float64)
    first_bar = first.copy()
    second_bar = second.copy()
    dual_first = np.zeros(len(problem.edges), dtype=np.float64)
    dual_second = np.zeros(len(problem.edges), dtype=np.float64)
    dual_sum = 0.0
    shifted_lower = problem.aggregate_lower + count
    shifted_upper = problem.aggregate_upper + count
    converged = False
    iterations = 0
    previous_feasible_primal = math.inf
    feasible_seed = budget_exact_marginal_labeling(problem)
    seed_first = np.asarray([1.0 if feasible_seed[p] >= 0 else 0.0 for p in points])
    seed_second = np.asarray([1.0 if feasible_seed[p] == 1 else 0.0 for p in points])

    for iteration in range(1, maximum_iterations + 1):
        edge_a, edge_b, sum_value = _operator_forward(
            first_bar, second_bar, edge_first, edge_second,
        )
        dual_first = np.clip(dual_first + sigma * edge_a, -problem.beta, problem.beta)
        dual_second = np.clip(dual_second + sigma * edge_b, -problem.beta, problem.beta)
        aggregate_trial = dual_sum + sigma * sum_value
        dual_sum = aggregate_trial - sigma * min(
            max(aggregate_trial / sigma, float(shifted_lower)),
            float(shifted_upper),
        )

        adjoint_first, adjoint_second = _operator_adjoint(
            dual_first, dual_second, dual_sum, count, edge_first, edge_second,
        )
        next_first, next_second = _project_ordered_triangle(
            first - tau * (adjoint_first + coefficient_first),
            second - tau * (adjoint_second + coefficient_second),
        )
        first_bar = 2.0 * next_first - first
        second_bar = 2.0 * next_second - second
        first, second = next_first, next_second
        iterations = iteration

        if iteration % 25 == 0 or iteration == maximum_iterations:
            aggregate = float(np.sum(first + second) - count)
            if aggregate < problem.aggregate_lower:
                fraction = (
                    (problem.aggregate_lower - aggregate)
                    / (float(np.sum(seed_first + seed_second) - count) - aggregate)
                )
            elif aggregate > problem.aggregate_upper:
                fraction = (
                    (problem.aggregate_upper - aggregate)
                    / (float(np.sum(seed_first + seed_second) - count) - aggregate)
                )
            else:
                fraction = 0.0
            fraction = min(1.0, max(0.0, fraction))
            feasible_first = (1.0 - fraction) * first + fraction * seed_first
            feasible_second = (1.0 - fraction) * second + fraction * seed_second
            feasible_primal = (
                float(np.dot(coefficient_first, feasible_first))
                + float(np.dot(coefficient_second, feasible_second))
                + problem.beta * float(np.sum(np.abs(feasible_first[edge_first] - feasible_first[edge_second])))
                + problem.beta * float(np.sum(np.abs(feasible_second[edge_first] - feasible_second[edge_second])))
                + constant + problem.fixed_edge_offset
            )
            dual = (
                _dual_lower_bound(
                    dual_first, dual_second, dual_sum,
                    coefficient_first, coefficient_second,
                    shifted_lower, shifted_upper, edge_first, edge_second,
                ) + constant + problem.fixed_edge_offset
            )
            if feasible_primal < dual - 1e-6:
                raise ArithmeticError("continuous-TV primal objective fell below its dual lower bound")
            gap = max(0.0, feasible_primal - dual)
            if (
                gap <= tolerance
                and abs(previous_feasible_primal - feasible_primal) <= tolerance
            ):
                converged = True
                break
            previous_feasible_primal = feasible_primal

    aggregate = float(np.sum(first + second) - count)
    if aggregate < problem.aggregate_lower:
        denominator = float(np.sum(seed_first + seed_second) - count) - aggregate
        fraction = (problem.aggregate_lower - aggregate) / denominator
    elif aggregate > problem.aggregate_upper:
        denominator = float(np.sum(seed_first + seed_second) - count) - aggregate
        fraction = (problem.aggregate_upper - aggregate) / denominator
    else:
        fraction = 0.0
    fraction = min(1.0, max(0.0, fraction))
    feasible_first = (1.0 - fraction) * first + fraction * seed_first
    feasible_second = (1.0 - fraction) * second + fraction * seed_second
    feasible_primal = (
        float(np.dot(coefficient_first, feasible_first))
        + float(np.dot(coefficient_second, feasible_second))
        + problem.beta * float(np.sum(np.abs(feasible_first[edge_first] - feasible_first[edge_second])))
        + problem.beta * float(np.sum(np.abs(feasible_second[edge_first] - feasible_second[edge_second])))
        + constant + problem.fixed_edge_offset
    )
    lower_bound = (
        _dual_lower_bound(
            dual_first, dual_second, dual_sum,
            coefficient_first, coefficient_second,
            shifted_lower, shifted_upper, edge_first, edge_second,
        ) + constant + problem.fixed_edge_offset
    )
    if not math.isfinite(lower_bound) or lower_bound > feasible_primal + 1e-5:
        raise ArithmeticError("continuous-TV dual lower bound is inconsistent with feasible primal point")

    coarea_candidates = _coarea_candidates(problem, first, second)
    coarea, repaired_count = _best_coarea(coarea_candidates, problem, evidence)
    argmax = _argmax_rounding(problem, first, second)
    argmax, argmax_repaired, _argmax_moves = _repair_to_interval(argmax, problem, evidence)
    for labels in (coarea, argmax):
        if not problem.aggregate_lower <= sum(labels.values()) <= problem.aggregate_upper:
            raise ArithmeticError("continuous-TV rounding violated aggregate feasibility")
    aggregate_residual = max(
        0.0,
        float(problem.aggregate_lower) - aggregate,
        aggregate - float(problem.aggregate_upper),
    )
    return ContinuousTVResult(
        coarea_labeling=tuple(sorted(coarea.items())),
        argmax_labeling=tuple(sorted(argmax.items())),
        lower_bound=float(lower_bound),
        feasible_primal_energy=float(feasible_primal),
        primal_dual_gap=float(max(0.0, feasible_primal - lower_bound)),
        iterations=iterations,
        converged=converged,
        coarea_candidate_count=len(coarea_candidates),
        coarea_repaired_count=repaired_count,
        argmax_was_repaired=argmax_repaired,
        aggregate_primal_residual=float(aggregate_residual),
        solver_message=(
            "Chambolle-Pock stopped on feasible primal/dual gap and primal stability"
            if converged else "Chambolle-Pock reached iteration budget"
        ),
    )
