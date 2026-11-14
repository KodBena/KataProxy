# SPDX-License-Identifier: MIT
"""Cardinality-aware forest optimization and budget-exact MM for ownership maps.

The module is an offline research core. It does not implement an
OwnershipRegionPolicy and is not imported by the production policy factory.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
from typing import Mapping, Sequence

from .stencil_ownership import OwnershipEvidence
from .stencil_ownership_graphcut import ownership_graph


_LABELS = (-1, 0, 1)
_NUMERIC_TOLERANCE = 1e-9
Edge = tuple[int, int]
Labeling = dict[int, int]
UnaryAdjustments = Mapping[int, tuple[float, float, float]]


@dataclass(frozen=True)
class ReducedOwnershipProblem:
    """The exact empty-point problem after fixed dead-stone edges are folded in."""

    points: tuple[int, ...]
    unary_absolute: tuple[float, ...]
    unary_linear: tuple[float, ...]
    edges: tuple[Edge, ...]
    aggregate_lower: int
    aggregate_upper: int
    expected_sum: float
    fixed_edge_offset: float
    beta: float
    width: int
    height: int

    def __post_init__(self) -> None:
        if tuple(sorted(set(self.points))) != self.points:
            raise ValueError("problem points must be sorted and unique")
        if len(self.unary_absolute) != len(self.points):
            raise ValueError("absolute unary coefficients must match problem points")
        if len(self.unary_linear) != len(self.points):
            raise ValueError("linear unary coefficients must match problem points")
        if self.aggregate_lower > self.aggregate_upper:
            raise ValueError("aggregate interval is empty")
        if self.aggregate_lower < -len(self.points) or self.aggregate_upper > len(self.points):
            raise ValueError("aggregate interval exceeds attainable ternary sums")
        if not math.isfinite(self.expected_sum):
            raise ValueError("expected sum must be finite")
        if not math.isfinite(self.fixed_edge_offset):
            raise ValueError("fixed-edge offset must be finite")
        if not math.isfinite(self.beta) or self.beta < 0:
            raise ValueError("beta must be finite and non-negative")
        if self.width <= 0 or self.height <= 0:
            raise ValueError("board dimensions must be positive")
        if any(not math.isfinite(value) or value < 0 for value in self.unary_absolute):
            raise ValueError("absolute unary coefficients must be finite and non-negative")
        if any(not math.isfinite(value) for value in self.unary_linear):
            raise ValueError("linear unary coefficients must be finite")
        point_set = set(self.points)
        if tuple(sorted(set(self.edges))) != self.edges:
            raise ValueError("empty-point edges must be sorted and unique")
        if any(
            first >= second or first not in point_set or second not in point_set
            for first, second in self.edges
        ):
            raise ValueError("every edge must join two distinct empty problem points")


@dataclass(frozen=True)
class ForestDPResult:
    """Exact optimum of one retained-forest surrogate."""

    labeling: tuple[tuple[int, int], ...]
    surrogate_energy: float
    selected_sum: int
    component_count: int
    dp_state_count: int
    retained_edge_count: int


@dataclass(frozen=True)
class MMIteration:
    """One exactly minimized majorizer and its original-energy disposition."""

    pass_number: int
    surrogate_energy: float
    surrogate_at_reference: float
    original_energy_before: float
    original_energy_after: float
    dp_state_count: int


@dataclass(frozen=True)
class ForestMMResult:
    """Best feasible labeling found by a bounded number of forest-MM passes."""

    initial_labeling: tuple[tuple[int, int], ...]
    initial_energy: float
    labeling: tuple[tuple[int, int], ...]
    energy: float
    iterations: tuple[MMIteration, ...]
    retained_edges: tuple[Edge, ...]
    omitted_edge_tv_energy: float


def aggregate_interval(
    ownership_means: Sequence[float],
    score_tolerance: float,
) -> tuple[float, int, int]:
    """Return expected sum and the original objective's attainable integer interval."""
    if (
        isinstance(score_tolerance, bool)
        or not isinstance(score_tolerance, (int, float))
        or not math.isfinite(float(score_tolerance))
        or score_tolerance < 0
    ):
        raise ValueError("score_tolerance must be finite and non-negative")
    expected = sum(float(value) for value in ownership_means)
    if not math.isfinite(expected):
        raise ValueError("ownership means must have a finite sum")
    count = len(ownership_means)
    lower = max(-count, math.ceil(expected - score_tolerance - 1e-9))
    upper = min(count, math.floor(expected + score_tolerance + 1e-9))
    if lower > upper:
        raise ValueError("ownership score tolerance contains no attainable integer net sum")
    return float(expected), int(lower), int(upper)


def build_reduced_problem(
    evidence: OwnershipEvidence,
    kappa: float,
    beta: float,
    score_tolerance: float,
) -> ReducedOwnershipProblem:
    """Fold empty/dead and dead/dead interactions into exact unary/constant terms.

    The aggregate interval is deliberately derived from the unadjusted ownership
    means. Living stones are absent from the eligible graph, so they break paths.
    """
    for name, value in (("kappa", kappa), ("beta", beta)):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        ):
            raise ValueError(f"{name} must be finite and non-negative")
    if evidence.ownership_stdev is None:
        raise ValueError("forest ownership approximation requires ownershipStdev")

    area = evidence.width * evidence.height
    if evidence.width <= 0 or evidence.height <= 0:
        raise ValueError("board dimensions must be positive")
    if (
        len(evidence.ownership) != area
        or len(evidence.ownership_stdev) != area
        or len(evidence.board) != area
    ):
        raise ValueError("ownership, stdev, and board arrays must match board dimensions")
    if any(
        not math.isfinite(value) or value < -1.00001 or value > 1.00001
        for value in evidence.ownership
    ):
        raise ValueError("ownership means must be finite and lie in [-1, 1]")
    if any(
        not math.isfinite(value) or value < -1e-5 or value > 1.00001
        for value in evidence.ownership_stdev
    ):
        raise ValueError("ownership stdev values must be finite and lie in [0, 1]")
    if any(colour not in (None, "B", "W") for colour in evidence.board):
        raise ValueError("board contains an unsupported cell disposition")
    points = tuple(sorted(evidence.empty_points))
    actual_empty = tuple(
        point for point, colour in enumerate(evidence.board) if colour is None
    )
    if points != actual_empty:
        raise ValueError("empty-point index does not match the replayed board")
    fixed_positive = set(evidence.dead_black)
    fixed_negative = set(evidence.dead_white)
    if fixed_positive & fixed_negative:
        raise ValueError("dead-Black and dead-White points overlap")
    if any(
        point < 0 or point >= area or evidence.board[point] != "B"
        for point in fixed_positive
    ):
        raise ValueError("dead-Black points must identify Black board cells")
    if any(
        point < 0 or point >= area or evidence.board[point] != "W"
        for point in fixed_negative
    ):
        raise ValueError("dead-White points must identify White board cells")
    if (fixed_positive | fixed_negative) & set(points):
        raise ValueError("empty points overlap fixed dead-stone points")
    fixed_labels = {
        **{point: 1 for point in fixed_positive},
        **{point: -1 for point in fixed_negative},
    }
    eligible = tuple(sorted(set(points) | set(fixed_labels)))
    _neighbours, eligible_edges = ownership_graph(
        eligible, evidence.width, evidence.height,
    )
    empty_set = set(points)
    positive_dead_neighbours = {point: 0 for point in points}
    negative_dead_neighbours = {point: 0 for point in points}
    empty_edges: list[Edge] = []
    fixed_offset = 0.0

    for first, second in eligible_edges:
        first_empty = first in empty_set
        second_empty = second in empty_set
        if first_empty and second_empty:
            empty_edges.append((first, second))
        elif first_empty != second_empty:
            empty_point = first if first_empty else second
            dead_point = second if first_empty else first
            if fixed_labels[dead_point] == 1:
                positive_dead_neighbours[empty_point] += 1
            else:
                negative_dead_neighbours[empty_point] += 1
        else:
            fixed_offset += beta * abs(
                fixed_labels[first] - fixed_labels[second]
            )

    absolute: list[float] = []
    linear: list[float] = []
    fixed_offset += beta * sum(
        positive_dead_neighbours[point] + negative_dead_neighbours[point]
        for point in points
    )
    for point in points:
        mean = evidence.ownership[point]
        deviation = evidence.ownership_stdev[point]
        absolute.append(1.0 + 2.0 * kappa * deviation)
        linear.append(
            -2.0 * mean
            + beta * (
                negative_dead_neighbours[point]
                - positive_dead_neighbours[point]
            )
        )

    expected, lower, upper = aggregate_interval(
        tuple(evidence.ownership[point] for point in points),
        score_tolerance,
    )
    return ReducedOwnershipProblem(
        points=points,
        unary_absolute=tuple(absolute),
        unary_linear=tuple(linear),
        edges=tuple(sorted(empty_edges)),
        aggregate_lower=lower,
        aggregate_upper=upper,
        expected_sum=expected,
        fixed_edge_offset=float(fixed_offset),
        beta=float(beta),
        width=evidence.width,
        height=evidence.height,
    )


def reduced_energy(
    problem: ReducedOwnershipProblem,
    labels: Mapping[int, int],
) -> float:
    """Evaluate the reduced objective, excluding its label-independent offset."""
    _validate_labeling(problem, labels)
    index = {point: position for position, point in enumerate(problem.points)}
    unary = sum(
        problem.unary_absolute[position] * abs(labels[point])
        + problem.unary_linear[position] * labels[point]
        for position, point in enumerate(problem.points)
    )
    spatial = problem.beta * sum(
        abs(labels[first] - labels[second])
        for first, second in problem.edges
    )
    return float(unary + spatial)


def full_energy_from_reduction(
    problem: ReducedOwnershipProblem,
    labels: Mapping[int, int],
) -> float:
    """Restore the exact fixed-edge constant to the reduced objective."""
    return reduced_energy(problem, labels) + problem.fixed_edge_offset


def _validate_labeling(
    problem: ReducedOwnershipProblem,
    labels: Mapping[int, int],
) -> None:
    if tuple(sorted(labels)) != problem.points:
        raise ValueError("labeling must cover exactly the empty problem points")
    for label in labels.values():
        if isinstance(label, bool) or not isinstance(label, int) or label not in _LABELS:
            raise ValueError("ownership labels must be integer -1, 0, or +1")
    total = sum(labels.values())
    if not problem.aggregate_lower <= total <= problem.aggregate_upper:
        raise ValueError("labeling violates the original aggregate interval")


def budget_exact_marginal_labeling(
    problem: ReducedOwnershipProblem,
) -> Labeling:
    """Solve the separable reduced objective exactly under the global sum interval.

    Each point contributes the ordered increments -1→0 and 0→+1. Their
    nondecreasing costs follow from a_i >= 0; sorting all increments and
    choosing the best admissible prefix is therefore exact.
    """
    count = len(problem.points)
    if count == 0:
        return {}

    base_energy = sum(
        absolute - linear
        for absolute, linear in zip(problem.unary_absolute, problem.unary_linear)
    )
    increments: list[tuple[float, int, int]] = []
    for point, absolute, linear in zip(
        problem.points, problem.unary_absolute, problem.unary_linear,
    ):
        increments.append((-absolute + linear, point, 0))
        increments.append((absolute + linear, point, 1))
    increments.sort()

    minimum_prefix = count + problem.aggregate_lower
    maximum_prefix = count + problem.aggregate_upper
    cumulative = 0.0
    best_prefix = minimum_prefix
    best_key: tuple[float, float, int] | None = None
    for prefix in range(0, maximum_prefix + 1):
        if prefix >= minimum_prefix:
            net_sum = -count + prefix
            candidate_key = (
                base_energy + cumulative,
                abs(net_sum - problem.expected_sum),
                net_sum,
            )
            if best_key is None or candidate_key < best_key:
                best_key = candidate_key
                best_prefix = prefix
        if prefix < maximum_prefix:
            cumulative += increments[prefix][0]

    labels = {point: -1 for point in problem.points}
    for _cost, point, _stage in increments[:best_prefix]:
        labels[point] += 1
    _validate_labeling(problem, labels)
    return labels


def spanning_forest_edges(
    problem: ReducedOwnershipProblem,
) -> tuple[Edge, ...]:
    """Build a deterministic breadth-first spanning forest of the empty graph."""
    adjacency = {point: [] for point in problem.points}
    for first, second in problem.edges:
        adjacency[first].append(second)
        adjacency[second].append(first)
    for adjacent in adjacency.values():
        adjacent.sort()

    visited: set[int] = set()
    forest: list[Edge] = []
    for root in problem.points:
        if root in visited:
            continue
        visited.add(root)
        pending = deque((root,))
        while pending:
            point = pending.popleft()
            for adjacent in adjacency[point]:
                if adjacent in visited:
                    continue
                visited.add(adjacent)
                pending.append(adjacent)
                forest.append((min(point, adjacent), max(point, adjacent)))
    return tuple(sorted(forest))


def _validated_forest(
    problem: ReducedOwnershipProblem,
    retained_edges: Sequence[Edge],
) -> tuple[Edge, ...]:
    parent = {point: point for point in problem.points}

    def find(point: int) -> int:
        while parent[point] != point:
            parent[point] = parent[parent[point]]
            point = parent[point]
        return point

    normalized: list[Edge] = []
    for raw_first, raw_second in retained_edges:
        first, second = sorted((raw_first, raw_second))
        edge = (first, second)
        if first == second or edge not in problem.edges:
            raise ValueError("retained edges must be existing empty-empty graph edges")
        normalized.append(edge)
    if len(set(normalized)) != len(normalized):
        raise ValueError("retained forest contains duplicate edges")
    for first, second in normalized:
        first_root = find(first)
        second_root = find(second)
        if first_root == second_root:
            raise ValueError("retained edges must be acyclic")
        parent[first_root] = second_root
    return tuple(sorted(normalized))


def solve_forest_dp(
    problem: ReducedOwnershipProblem,
    retained_edges: Sequence[Edge],
    unary_adjustments: UnaryAdjustments | None = None,
) -> ForestDPResult:
    """Exactly minimize a forest surrogate while preserving the global integer sum.

    The tree tables retain root labels and subtree sums. Disconnected component
    profiles are combined with a single global min-plus convolution, so
    zero-sum exchanges between components remain available.
    """
    forest = _validated_forest(problem, retained_edges)
    adjustments = unary_adjustments or {}
    point_set = set(problem.points)
    if not set(adjustments).issubset(point_set):
        raise ValueError("unary adjustments reference points outside the problem")
    for values in adjustments.values():
        if len(values) != 3 or any(not math.isfinite(value) for value in values):
            raise ValueError("each unary adjustment must contain three finite costs")

    unary: dict[int, dict[int, float]] = {}
    for index, point in enumerate(problem.points):
        extra = adjustments.get(point, (0.0, 0.0, 0.0))
        unary[point] = {
            label: (
                problem.unary_absolute[index] * abs(label)
                + problem.unary_linear[index] * label
                + extra[position]
            )
            for position, label in enumerate(_LABELS)
        }

    adjacency = {point: [] for point in problem.points}
    for first, second in forest:
        adjacency[first].append(second)
        adjacency[second].append(first)
    for adjacent in adjacency.values():
        adjacent.sort()

    components: list[tuple[int, ...]] = []
    unseen = set(problem.points)
    while unseen:
        root = min(unseen)
        component_parent = {root: -1}
        order = [root]
        unseen.remove(root)
        for point in order:
            for adjacent in adjacency[point]:
                if adjacent == component_parent[point] or adjacent not in unseen:
                    continue
                unseen.remove(adjacent)
                component_parent[adjacent] = point
                order.append(adjacent)
        components.append(tuple(order))

    node_tables: dict[int, dict[tuple[int, int], float]] = {}
    node_children: dict[int, tuple[int, ...]] = {}
    node_back: dict[int, list[dict[tuple[int, int], tuple[int, int, int]]]] = {}
    component_profiles: list[dict[int, float]] = []
    component_root_choices: list[dict[int, int]] = []
    dp_state_count = 0

    for component in components:
        root = component[0]
        parent: dict[int, int] = {root: -1}
        order = [root]
        for point in order:
            for adjacent in adjacency[point]:
                if adjacent == parent[point] or adjacent in parent:
                    continue
                parent[adjacent] = point
                order.append(adjacent)

        for point in reversed(order):
            children = tuple(
                adjacent for adjacent in adjacency[point]
                if parent.get(adjacent) == point
            )
            node_children[point] = children
            current = {
                (label, label): unary[point][label]
                for label in _LABELS
            }
            stage_back: list[dict[tuple[int, int], tuple[int, int, int]]] = []
            for child in children:
                child_table = node_tables[child]
                merged: dict[tuple[int, int], float] = {}
                back: dict[tuple[int, int], tuple[int, int, int]] = {}
                for (label, subtotal), subtotal_cost in sorted(current.items()):
                    for (child_label, child_sum), child_cost in sorted(child_table.items()):
                        total = subtotal + child_sum
                        key = (label, total)
                        candidate = (
                            subtotal_cost + child_cost
                            + problem.beta * abs(label - child_label)
                        )
                        if key not in merged or candidate < merged[key]:
                            merged[key] = candidate
                            back[key] = (subtotal, child_label, child_sum)
                current = merged
                stage_back.append(back)
                dp_state_count += len(current)
            node_tables[point] = current
            node_back[point] = stage_back
            dp_state_count += len(current)

        profile: dict[int, float] = {}
        root_choices: dict[int, int] = {}
        for (label, total), cost in sorted(node_tables[root].items()):
            if total not in profile or cost < profile[total]:
                profile[total] = cost
                root_choices[total] = label
        component_profiles.append(profile)
        component_root_choices.append(root_choices)

    global_profile: dict[int, float] = {0: 0.0}
    global_back: list[dict[int, tuple[int, int]]] = []
    for profile in component_profiles:
        merged_global: dict[int, float] = {}
        back_global: dict[int, tuple[int, int]] = {}
        for previous_sum, previous_cost in sorted(global_profile.items()):
            for component_sum, component_cost in sorted(profile.items()):
                total = previous_sum + component_sum
                candidate = previous_cost + component_cost
                if total not in merged_global or candidate < merged_global[total]:
                    merged_global[total] = candidate
                    back_global[total] = (previous_sum, component_sum)
        global_profile = merged_global
        global_back.append(back_global)
        dp_state_count += len(global_profile)

    feasible_sums = [
        total for total in global_profile
        if problem.aggregate_lower <= total <= problem.aggregate_upper
    ]
    if not feasible_sums:
        raise RuntimeError("forest DP found no labeling in the aggregate interval")
    selected_sum = min(
        feasible_sums,
        key=lambda total: (
            global_profile[total],
            abs(total - problem.expected_sum),
            total,
        ),
    )
    surrogate_energy = global_profile[selected_sum]

    component_sums = [0 for _component in components]
    remaining_sum = selected_sum
    for component_index in range(len(components) - 1, -1, -1):
        previous_sum, component_sum = global_back[component_index][remaining_sum]
        component_sums[component_index] = component_sum
        remaining_sum = previous_sum

    labels: Labeling = {}

    def recover(point: int, label: int, subtree_sum: int) -> None:
        children = node_children[point]
        for child_index in range(len(children) - 1, -1, -1):
            child = children[child_index]
            previous_sum, child_label, child_sum = node_back[point][child_index][
                (label, subtree_sum)
            ]
            recover(child, child_label, child_sum)
            subtree_sum = previous_sum
        if subtree_sum != label:
            raise RuntimeError("forest DP backpointer did not recover the node label")
        labels[point] = label

    for root, component_sum, root_choices in zip(
        (component[0] for component in components),
        component_sums,
        component_root_choices,
    ):
        recover(root, root_choices[component_sum], component_sum)

    _validate_labeling(problem, labels)
    return ForestDPResult(
        labeling=tuple(sorted(labels.items())),
        surrogate_energy=float(surrogate_energy),
        selected_sum=selected_sum,
        component_count=len(components),
        dp_state_count=dp_state_count,
        retained_edge_count=len(forest),
    )


def mm_unary_adjustments(
    problem: ReducedOwnershipProblem,
    retained_edges: Sequence[Edge],
    reference_labels: Mapping[int, int],
) -> dict[int, tuple[float, float, float]]:
    """Build the budget-exact edge majorizer's additional unary costs."""
    _validate_labeling(problem, reference_labels)
    forest = _validated_forest(problem, retained_edges)
    retained = set(forest)
    adjustments = {
        point: [0.0, 0.0, 0.0]
        for point in problem.points
    }
    for first, second in problem.edges:
        if (first, second) in retained:
            continue
        midpoint = (reference_labels[first] + reference_labels[second]) / 2.0
        for position, label in enumerate(_LABELS):
            adjustments[first][position] += problem.beta * abs(label - midpoint)
            adjustments[second][position] += problem.beta * abs(label - midpoint)
    return {
        point: (costs[0], costs[1], costs[2])
        for point, costs in adjustments.items()
    }


def majorizer_energy(
    problem: ReducedOwnershipProblem,
    retained_edges: Sequence[Edge],
    reference_labels: Mapping[int, int],
    candidate_labels: Mapping[int, int],
) -> float:
    """Evaluate Q_T(candidate | reference) in reduced-objective coordinates."""
    _validate_labeling(problem, reference_labels)
    _validate_labeling(problem, candidate_labels)
    forest = _validated_forest(problem, retained_edges)
    retained = set(forest)
    unary = 0.0
    for index, point in enumerate(problem.points):
        label = candidate_labels[point]
        unary += (
            problem.unary_absolute[index] * abs(label)
            + problem.unary_linear[index] * label
        )
    exact_spatial = problem.beta * sum(
        abs(candidate_labels[first] - candidate_labels[second])
        for first, second in forest
    )
    majorized_spatial = 0.0
    for first, second in problem.edges:
        if (first, second) in retained:
            continue
        midpoint = (reference_labels[first] + reference_labels[second]) / 2.0
        majorized_spatial += problem.beta * (
            abs(candidate_labels[first] - midpoint)
            + abs(candidate_labels[second] - midpoint)
        )
    return float(unary + exact_spatial + majorized_spatial)


def forest_majorize_minimize(
    problem: ReducedOwnershipProblem,
    retained_edges: Sequence[Edge],
    maximum_passes: int,
    initial_labeling: Mapping[int, int] | None = None,
) -> ForestMMResult:
    """Run a bounded, monotone sequence of exactly solved forest majorizers."""
    if (
        isinstance(maximum_passes, bool)
        or not isinstance(maximum_passes, int)
        or maximum_passes < 0
    ):
        raise ValueError("maximum_passes must be a non-negative integer")
    forest = _validated_forest(problem, retained_edges)
    initial = (
        budget_exact_marginal_labeling(problem)
        if initial_labeling is None else dict(initial_labeling)
    )
    _validate_labeling(problem, initial)
    current = dict(initial)
    initial_energy = full_energy_from_reduction(problem, current)
    current_energy = initial_energy
    iterations: list[MMIteration] = []

    for pass_number in range(1, maximum_passes + 1):
        adjustments = mm_unary_adjustments(problem, forest, current)
        surrogate_at_reference = majorizer_energy(
            problem, forest, current, current,
        )
        if abs(surrogate_at_reference - reduced_energy(problem, current)) > 1e-7:
            raise ArithmeticError("MM surrogate does not touch the original energy")
        optimized = solve_forest_dp(problem, forest, adjustments)
        candidate = dict(optimized.labeling)
        _validate_labeling(problem, candidate)
        surrogate_value = majorizer_energy(
            problem, forest, current, candidate,
        )
        original_after = full_energy_from_reduction(problem, candidate)
        original_before = current_energy
        if surrogate_value > surrogate_at_reference + 1e-7:
            raise ArithmeticError("exact forest DP increased its minimized majorizer")
        if original_after > original_before + 1e-7:
            raise ArithmeticError("MM pass increased the original ownership energy")
        if original_after >= original_before - 1e-9:
            break
        iterations.append(MMIteration(
            pass_number=pass_number,
            surrogate_energy=float(surrogate_value),
            surrogate_at_reference=float(surrogate_at_reference),
            original_energy_before=float(original_before),
            original_energy_after=float(original_after),
            dp_state_count=optimized.dp_state_count,
        ))
        current = candidate
        current_energy = original_after

    retained_set = set(forest)
    omitted_edges = tuple(edge for edge in problem.edges if edge not in retained_set)
    omitted_edge_tv = problem.beta * sum(
        abs(current[first] - current[second])
        for first, second in omitted_edges
    )
    return ForestMMResult(
        initial_labeling=tuple(sorted(initial.items())),
        initial_energy=float(initial_energy),
        labeling=tuple(sorted(current.items())),
        energy=float(current_energy),
        iterations=tuple(iterations),
        retained_edges=forest,
        omitted_edge_tv_energy=float(omitted_edge_tv),
    )
