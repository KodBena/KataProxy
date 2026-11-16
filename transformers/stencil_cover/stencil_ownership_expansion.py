# SPDX-License-Identifier: MIT
"""Ordered-label alpha-expansion with deterministic aggregate repair.

This is an offline research solver. It preserves the coherent ownership
objective and aggregate interval, but does not implement OwnershipRegionPolicy.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
import time
from typing import Mapping

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import maximum_flow

from .stencil_ownership import OwnershipEvidence
from .stencil_ownership_forest import (
    ReducedOwnershipProblem,
    budget_exact_marginal_labeling,
    full_energy_from_reduction,
)
from .stencil_ownership_graphcut import ownership_graph
from .stencil_ownership_repair import (
    component_ids,
    greedy_sum_repair,
    ownership_energy,
)


_LABELS = (-1, 0, 1)
_CAPACITY_SCALE = 1_000_000_000
_MAX_TOTAL_INTEGER_CAPACITY = 1_000_000_000
_ENERGY_TOLERANCE = 1e-9


@dataclass(frozen=True)
class ExpansionCheckpoint:
    """Best feasible labeling and cumulative latency after one sweep."""

    pass_number: int
    elapsed_wall_ns: int
    energy: float
    labeling: tuple[tuple[int, int], ...]
    cut_calls: int
    accepted_moves: int
    repair_attempts: int
    repair_moves: int
    repair_delta_sum_over_attempts: float


@dataclass(frozen=True)
class OrderedExpansionResult:
    """Final expansion result plus the latency/energy trajectory."""

    initial_energy: float
    final_energy: float
    labeling: tuple[tuple[int, int], ...]
    checkpoints: tuple[ExpansionCheckpoint, ...]
    passes_completed: int
    cut_calls: int
    accepted_moves: int
    repair_attempts: int
    repair_moves: int
    repair_delta_sum_over_attempts: float
    requested_capacity_scale: int


def _binary_expansion_move(
    problem: ReducedOwnershipProblem,
    current: Mapping[int, int],
    alpha: int,
    capacity_scale: int,
) -> dict[int, int]:
    """Exactly minimize one alpha-expansion move up to capacity quantization.

    A binary variable x_i selects alpha (x_i=1) or keeps current[i] (x_i=0).
    For each metric pairwise term, the resulting binary pair potential is
    submodular and is represented by a non-negative undirected cut capacity.
    Unary shifts are accumulated into terminal arcs. The requested scale is
    downscaled per move as needed to keep the sum of integer arc capacities at
    or below 1e9, avoiding overflow in SciPy's max-flow implementation.
    """
    if alpha not in _LABELS:
        raise ValueError("alpha must be one of -1, 0, or +1")
    if tuple(sorted(current)) != problem.points:
        raise ValueError("current labeling must cover exactly the problem points")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value not in _LABELS
        for value in current.values()
    ):
        raise ValueError("current ownership labels must be integer -1, 0, or +1")

    count = len(problem.points)
    if count == 0:
        return {}
    index = {point: position for position, point in enumerate(problem.points)}
    unary_delta = [
        (
            problem.unary_absolute[position] * abs(alpha)
            + problem.unary_linear[position] * alpha
        ) - (
            problem.unary_absolute[position] * abs(current[point])
            + problem.unary_linear[position] * current[point]
        )
        for position, point in enumerate(problem.points)
    ]
    pair_capacities: list[tuple[int, int, float]] = []

    for first_point, second_point in problem.edges:
        first = index[first_point]
        second = index[second_point]
        first_label = current[first_point]
        second_label = current[second_point]
        e00 = problem.beta * abs(first_label - second_label)
        e10 = problem.beta * abs(alpha - second_label)
        e01 = problem.beta * abs(first_label - alpha)
        e11 = 0.0
        if e00 + e11 > e01 + e10 + 1e-9:
            raise ArithmeticError("alpha-expansion pair potential is not submodular")
        cut_capacity = 0.5 * (e10 + e01 - e00 - e11)
        if cut_capacity < -1e-9:
            raise ArithmeticError("alpha-expansion produced a negative cut capacity")
        cut_capacity = max(0.0, cut_capacity)
        unary_delta[first] += e10 - e00 - cut_capacity
        unary_delta[second] += e01 - e00 - cut_capacity
        if cut_capacity > 0.0:
            pair_capacities.append((first, second, cut_capacity))

    source = count
    sink = count + 1
    rows: list[int] = []
    columns: list[int] = []
    floating_capacities: list[float] = []

    def add_arc(start: int, end: int, capacity: float) -> None:
        if capacity < 0.0 or not math.isfinite(capacity):
            raise ArithmeticError("minimum-cut capacity must be finite and non-negative")
        if capacity > 0.0:
            rows.append(start)
            columns.append(end)
            floating_capacities.append(float(capacity))

    for node, coefficient in enumerate(unary_delta):
        if coefficient > 0.0:
            add_arc(node, sink, coefficient)
        elif coefficient < 0.0:
            add_arc(source, node, -coefficient)

    for first, second, capacity in pair_capacities:
        add_arc(first, second, capacity)
        add_arc(second, first, capacity)

    total_capacity = sum(floating_capacities)
    effective_capacity_scale = (
        float(capacity_scale)
        if total_capacity == 0.0
        else min(
            float(capacity_scale),
            _MAX_TOTAL_INTEGER_CAPACITY / total_capacity,
        )
    )
    capacities = [
        int(round(capacity * effective_capacity_scale))
        for capacity in floating_capacities
    ]
    capacity_matrix = coo_matrix(
        (
            np.asarray(capacities, dtype=np.int64),
            (
                np.asarray(rows, dtype=np.int32),
                np.asarray(columns, dtype=np.int32),
            ),
        ),
        shape=(count + 2, count + 2),
        dtype=np.int64,
    ).tocsr()
    flow = maximum_flow(capacity_matrix, source, sink, method="dinic")
    residual = (capacity_matrix - flow.flow).tocsr()

    reached = np.zeros(count + 2, dtype=np.bool_)
    reached[source] = True
    pending: deque[int] = deque((source,))
    while pending:
        node = pending.popleft()
        for edge_index in range(residual.indptr[node], residual.indptr[node + 1]):
            adjacent = int(residual.indices[edge_index])
            if residual.data[edge_index] > 0 and not reached[adjacent]:
                reached[adjacent] = True
                pending.append(adjacent)

    return {
        point: alpha if bool(reached[index[point]]) else current[point]
        for point in problem.points
    }


def ordered_label_expansion(
    problem: ReducedOwnershipProblem,
    evidence: OwnershipEvidence,
    *,
    kappa: float,
    maximum_passes: int = 8,
    initial_labeling: Mapping[int, int] | None = None,
    capacity_scale: int = _CAPACITY_SCALE,
) -> OrderedExpansionResult:
    """Run deterministic ternary alpha-expansion with interval repair.

    Each pass considers alpha in (-1, 0, +1) order. An infeasible expansion
    proposal is projected to the nearest aggregate-interval endpoint using the
    existing deterministic connected-growth repair. A proposal is accepted
    only when it strictly lowers the original full unary-plus-TV energy.
    """
    if (
        isinstance(maximum_passes, bool)
        or not isinstance(maximum_passes, int)
        or maximum_passes < 0
    ):
        raise ValueError("maximum_passes must be a non-negative integer")
    if (
        isinstance(capacity_scale, bool)
        or not isinstance(capacity_scale, int)
        or capacity_scale <= 0
    ):
        raise ValueError("capacity_scale must be a positive integer")
    if (
        isinstance(kappa, bool)
        or not isinstance(kappa, (int, float))
        or not math.isfinite(float(kappa))
        or kappa < 0
    ):
        raise ValueError("kappa must be finite and non-negative")
    if evidence.ownership_stdev is None:
        raise ValueError("ordered expansion requires ownershipStdev")
    if tuple(sorted(evidence.empty_points)) != problem.points:
        raise ValueError("reduced problem points do not match evidence empty points")

    started_ns = time.perf_counter_ns()
    initial = (
        budget_exact_marginal_labeling(problem)
        if initial_labeling is None
        else dict(initial_labeling)
    )
    if tuple(sorted(initial)) != problem.points:
        raise ValueError("initial labeling must cover exactly the problem points")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value not in _LABELS
        for value in initial.values()
    ):
        raise ValueError("initial ownership labels must be integer -1, 0, or +1")
    initial_sum = sum(initial.values())
    if not problem.aggregate_lower <= initial_sum <= problem.aggregate_upper:
        raise ValueError("initial labeling violates the aggregate interval")

    empty_points = tuple(evidence.empty_points)
    fixed_positive = set(evidence.dead_black)
    fixed_negative = set(evidence.dead_white)
    if fixed_positive & fixed_negative:
        raise ValueError("fixed dead-Black and dead-White points overlap")
    fixed_labels = {
        **{point: 1 for point in fixed_positive},
        **{point: -1 for point in fixed_negative},
    }
    eligible = tuple(sorted(set(empty_points) | set(fixed_labels)))
    neighbours, all_edges = ownership_graph(
        eligible, evidence.width, evidence.height,
    )
    component_for = component_ids(eligible, neighbours)

    current = dict(initial)
    current_full = {**current, **fixed_labels}
    current_energy = full_energy_from_reduction(problem, current)
    cross_checked_energy = ownership_energy(
        current_full,
        evidence,
        empty_points,
        all_edges,
        float(kappa),
        problem.beta,
    )
    if abs(current_energy - cross_checked_energy) > 1e-6:
        raise ArithmeticError("reduced and full ownership energies disagree")

    cut_calls = 0
    accepted_moves = 0
    repair_attempts = 0
    repair_moves_total = 0
    repair_delta_sum_over_attempts_total = 0.0

    def checkpoint(pass_number: int) -> ExpansionCheckpoint:
        return ExpansionCheckpoint(
            pass_number=pass_number,
            elapsed_wall_ns=time.perf_counter_ns() - started_ns,
            energy=float(current_energy),
            labeling=tuple(sorted(current.items())),
            cut_calls=cut_calls,
            accepted_moves=accepted_moves,
            repair_attempts=repair_attempts,
            repair_moves=repair_moves_total,
            repair_delta_sum_over_attempts=float(repair_delta_sum_over_attempts_total),
        )

    checkpoints = [checkpoint(0)]
    passes_completed = 0
    for pass_number in range(1, maximum_passes + 1):
        accepted_this_pass = 0
        for alpha in _LABELS:
            proposal = _binary_expansion_move(
                problem, current, alpha, capacity_scale,
            )
            cut_calls += 1
            proposed_sum = sum(proposal.values())
            proposed_full = {**proposal, **fixed_labels}

            if not problem.aggregate_lower <= proposed_sum <= problem.aggregate_upper:
                target_sum = (
                    problem.aggregate_lower
                    if proposed_sum < problem.aggregate_lower
                    else problem.aggregate_upper
                )
                proposed_full, repair_delta, repair_moves = greedy_sum_repair(
                    proposed_full,
                    evidence,
                    empty_points,
                    eligible,
                    neighbours,
                    component_for,
                    target_sum,
                    float(kappa),
                    problem.beta,
                )
                repair_attempts += 1
                repair_moves_total += repair_moves
                repair_delta_sum_over_attempts_total += repair_delta
                proposal = {
                    point: proposed_full[point] for point in empty_points
                }

            candidate_sum = sum(proposal.values())
            if not problem.aggregate_lower <= candidate_sum <= problem.aggregate_upper:
                raise RuntimeError("cardinality repair failed to restore feasibility")
            candidate_energy = ownership_energy(
                proposed_full,
                evidence,
                empty_points,
                all_edges,
                float(kappa),
                problem.beta,
            )
            reduced_candidate_energy = full_energy_from_reduction(problem, proposal)
            if abs(candidate_energy - reduced_candidate_energy) > 1e-6:
                raise ArithmeticError("repaired candidate failed full-energy cross-check")

            if candidate_energy < current_energy - _ENERGY_TOLERANCE:
                current = proposal
                current_energy = float(candidate_energy)
                accepted_this_pass += 1
                accepted_moves += 1

        passes_completed = pass_number
        checkpoints.append(checkpoint(pass_number))
        if accepted_this_pass == 0:
            break

    return OrderedExpansionResult(
        initial_energy=float(checkpoints[0].energy),
        final_energy=float(current_energy),
        labeling=tuple(sorted(current.items())),
        checkpoints=tuple(checkpoints),
        passes_completed=passes_completed,
        cut_calls=cut_calls,
        accepted_moves=accepted_moves,
        repair_attempts=repair_attempts,
        repair_moves=repair_moves_total,
        repair_delta_sum_over_attempts=float(repair_delta_sum_over_attempts_total),
        requested_capacity_scale=capacity_scale,
    )
