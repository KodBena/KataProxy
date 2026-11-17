# SPDX-License-Identifier: MIT
"""Ordered-label LP relaxation for offline ownership-quantization research.

The relaxation is a lower-bounding model, not a production policy. Its
threshold rounding is deterministic and aggregate-feasible by construction of
the fallback path.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping

from scipy.optimize import linprog
from scipy.sparse import coo_matrix

from .stencil_ownership_forest import (
    ReducedOwnershipProblem,
    budget_exact_marginal_labeling,
    full_energy_from_reduction,
    reduced_energy,
)


@dataclass(frozen=True)
class OrderedLabelLPResult:
    """Numerically solved LP bound and one feasible integral rounding."""

    labeling: tuple[tuple[int, int], ...]
    lower_bound: float
    lp_objective: float
    threshold_count: int
    feasible_rounding_count: int
    used_marginal_fallback: bool
    fractional_thresholds: tuple[tuple[int, float, float], ...]
    solver_message: str


def _lp_roundings(
    problem: ReducedOwnershipProblem,
    first_thresholds: tuple[float, ...],
    second_thresholds: tuple[float, ...],
) -> tuple[dict[int, int], ...]:
    """Enumerate every distinct common-threshold rounding deterministically."""
    breakpoints = sorted({0.0, 1.0, *first_thresholds, *second_thresholds})
    thresholds = set(breakpoints)
    thresholds.update(
        (left + right) / 2.0
        for left, right in zip(breakpoints, breakpoints[1:])
    )
    roundings: dict[tuple[int, ...], dict[int, int]] = {}
    for threshold in sorted(thresholds):
        labels = {
            point: int(threshold <= first_thresholds[index])
            + int(threshold <= second_thresholds[index])
            - 1
            for index, point in enumerate(problem.points)
        }
        key = tuple(labels[point] for point in problem.points)
        roundings[key] = labels
    return tuple(roundings[key] for key in sorted(roundings))


def ordered_label_lp_relaxation(
    problem: ReducedOwnershipProblem,
) -> OrderedLabelLPResult:
    """Solve the ordered-threshold TV LP and return its best feasible rounding.

    For q in {-1, 0, +1}, define y1=1[q>=0] and y2=1[q>=+1].
    The order constraint y2<=y1 makes q=y1+y2-1. The ternary TV metric
    decomposes exactly as |y1_i-y1_j|+|y2_i-y2_j|. Relaxing y1/y2 to [0,1]
    and linearizing each absolute difference yields a valid LP lower bound
    for the original integer problem, including its hard aggregate interval.
    """
    points = problem.points
    count = len(points)
    if count == 0:
        return OrderedLabelLPResult(
            labeling=(),
            lower_bound=float(problem.fixed_edge_offset),
            lp_objective=float(problem.fixed_edge_offset),
            threshold_count=0,
            feasible_rounding_count=1,
            used_marginal_fallback=False,
            fractional_thresholds=(),
            solver_message="empty problem; exact zero-dimensional relaxation",
        )

    edge_count = len(problem.edges)
    first_start = 0
    second_start = count
    first_edge_start = 2 * count
    second_edge_start = first_edge_start + edge_count
    variable_count = 2 * count + 2 * edge_count

    constant = sum(
        absolute - linear
        for absolute, linear in zip(
            problem.unary_absolute, problem.unary_linear,
        )
    )
    objective = [0.0] * variable_count
    for index, (absolute, linear) in enumerate(zip(
        problem.unary_absolute, problem.unary_linear,
    )):
        objective[first_start + index] = -absolute + linear
        objective[second_start + index] = absolute + linear
    for index in range(2 * edge_count):
        objective[first_edge_start + index] = problem.beta

    row_indices: list[int] = []
    column_indices: list[int] = []
    coefficients: list[float] = []
    right_hand_sides: list[float] = []
    row = 0

    def add_constraint(terms: tuple[tuple[int, float], ...], rhs: float) -> None:
        nonlocal row
        for column, coefficient in terms:
            row_indices.append(row)
            column_indices.append(column)
            coefficients.append(coefficient)
        right_hand_sides.append(rhs)
        row += 1

    # Ordered-label constraints: y2_i - y1_i <= 0.
    for index in range(count):
        add_constraint((
            (second_start + index, 1.0),
            (first_start + index, -1.0),
        ), 0.0)

    # z >= |y_i-y_j| for each threshold and each graph edge.
    point_index = {point: index for index, point in enumerate(points)}
    for edge_index, (first_point, second_point) in enumerate(problem.edges):
        first = point_index[first_point]
        second = point_index[second_point]
        for threshold_start, edge_start in (
            (first_start, first_edge_start),
            (second_start, second_edge_start),
        ):
            auxiliary = edge_start + edge_index
            add_constraint((
                (threshold_start + first, 1.0),
                (threshold_start + second, -1.0),
                (auxiliary, -1.0),
            ), 0.0)
            add_constraint((
                (threshold_start + second, 1.0),
                (threshold_start + first, -1.0),
                (auxiliary, -1.0),
            ), 0.0)

    # L <= sum(q) <= U, with q_i = y1_i + y2_i - 1.
    upper_terms = tuple(
        (index, 1.0) for index in range(2 * count)
    )
    lower_terms = tuple(
        (index, -1.0) for index in range(2 * count)
    )
    add_constraint(upper_terms, float(problem.aggregate_upper + count))
    add_constraint(lower_terms, float(-problem.aggregate_lower - count))

    matrix = coo_matrix(
        (coefficients, (row_indices, column_indices)),
        shape=(row, variable_count),
        dtype=float,
    ).tocsr()
    solved = linprog(
        objective,
        A_ub=matrix,
        b_ub=right_hand_sides,
        bounds=[(0.0, 1.0)] * variable_count,
        method="highs",
    )
    if not solved.success or solved.x is None or solved.fun is None:
        raise RuntimeError(
            "ordered-label LP relaxation did not solve to optimality: "
            f"status={solved.status}; message={solved.message}"
        )
    if len(solved.x) != variable_count or not math.isfinite(float(solved.fun)):
        raise ArithmeticError("ordered-label LP returned malformed primal values")

    first_values = tuple(float(solved.x[first_start + index]) for index in range(count))
    second_values = tuple(float(solved.x[second_start + index]) for index in range(count))
    for first_threshold, second_threshold in zip(first_values, second_values):
        if (
            not math.isfinite(first_threshold)
            or not math.isfinite(second_threshold)
            or first_threshold < -1e-7
            or first_threshold > 1.0 + 1e-7
            or second_threshold < -1e-7
            or second_threshold > first_threshold + 1e-7
        ):
            raise ArithmeticError("ordered-label LP violated threshold variable bounds")

    lower_bound = float(constant + float(solved.fun) + problem.fixed_edge_offset)
    if not math.isfinite(lower_bound):
        raise ArithmeticError("ordered-label LP lower bound is not finite")

    roundings = _lp_roundings(problem, first_values, second_values)
    feasible: list[tuple[float, tuple[int, ...], dict[int, int]]] = []
    for labels in roundings:
        total = sum(labels.values())
        if not problem.aggregate_lower <= total <= problem.aggregate_upper:
            continue
        energy = reduced_energy(problem, labels)
        key = tuple(labels[point] for point in points)
        feasible.append((energy, key, labels))

    used_fallback = not feasible
    if feasible:
        selected = min(feasible, key=lambda item: (item[0], item[1]))[2]
    else:
        selected = budget_exact_marginal_labeling(problem)

    selected_energy = full_energy_from_reduction(problem, selected)
    if lower_bound > selected_energy + 1e-6:
        raise ArithmeticError(
            "LP lower bound exceeds a feasible integral labeling; "
            "the relaxation or objective translation is inconsistent"
        )

    return OrderedLabelLPResult(
        labeling=tuple(sorted(selected.items())),
        lower_bound=lower_bound,
        lp_objective=float(constant + float(solved.fun)),
        threshold_count=len(roundings),
        feasible_rounding_count=len(feasible),
        used_marginal_fallback=used_fallback,
        fractional_thresholds=tuple(
            (point, first_values[index], second_values[index])
            for index, point in enumerate(points)
        ),
        solver_message=str(solved.message),
    )
