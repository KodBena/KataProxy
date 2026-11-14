# SPDX-License-Identifier: MIT
"""Deterministic spatial repair for ownership sums skipped by the cut path."""

from __future__ import annotations

import heapq
from typing import Dict, List, Mapping, Sequence, Tuple

from .stencil_ownership import OwnershipEvidence


def component_ids(
    points: Sequence[int],
    neighbours: Mapping[int, Sequence[int]],
) -> Dict[int, int]:
    """Identify eligible subgraphs separated by living stones."""
    component_for: Dict[int, int] = {}
    next_component = 0
    for start in points:
        if start in component_for:
            continue
        pending = [start]
        component_for[start] = next_component
        while pending:
            point = pending.pop()
            for adjacent in neighbours[point]:
                if adjacent not in component_for:
                    component_for[adjacent] = next_component
                    pending.append(adjacent)
        next_component += 1
    return component_for


def ownership_energy(
    labels: Mapping[int, int],
    evidence: OwnershipEvidence,
    empty_points: Sequence[int],
    edges: Sequence[Tuple[int, int]],
    kappa: float,
    beta: float,
) -> float:
    """Evaluate the declared unary-plus-TV objective for one labeling."""
    stdev = evidence.ownership_stdev
    if stdev is None:
        raise ValueError("coherent ownership policy requires ownershipStdev")
    unary = 0.0
    for point in empty_points:
        label = labels[point]
        if label != 0:
            unary += (
                1.0
                - 2.0 * label * evidence.ownership[point]
                + 2.0 * kappa * stdev[point]
            )
    spatial = beta * sum(
        abs(labels[first] - labels[second])
        for first, second in edges
    )
    return float(unary + spatial)


def greedy_sum_repair(
    labels: Mapping[int, int],
    evidence: OwnershipEvidence,
    empty_points: Sequence[int],
    eligible: Sequence[int],
    neighbours: Mapping[int, Sequence[int]],
    component_for: Mapping[int, int],
    target_net: int,
    kappa: float,
    beta: float,
) -> tuple[Dict[int, int], float, int]:
    """Meet the integer sum by cheapest one-cell moves with connected growth.

    A newly-created +1/-1 cell grows a same-sign component when one exists in
    its eligible connected component. Disconnected eligible areas (separated
    by living stones) can each establish their own seed. The repair is a
    feasible deterministic projection, not a claim of global optimality.
    """
    stdev = evidence.ownership_stdev
    if stdev is None:
        raise ValueError("coherent ownership policy requires ownershipStdev")

    repaired = dict(labels)
    current_net = sum(repaired[point] for point in empty_points)
    if current_net == target_net:
        return repaired, 0.0, 0

    direction = 1 if target_net > current_net else -1
    generations = {point: 0 for point in empty_points}
    component_seeds = {
        component_for[point]
        for point in eligible
        if repaired[point] == direction
    }
    move_heap: List[Tuple[float, int, int]] = []

    def unary_cost(point: int, label: int) -> float:
        if label == 0:
            return 0.0
        return (
            1.0
            - 2.0 * label * evidence.ownership[point]
            + 2.0 * kappa * stdev[point]
        )

    def move_delta(point: int) -> float | None:
        old_label = repaired[point]
        new_label = old_label + direction
        if new_label < -1 or new_label > 1:
            return None
        component = component_for[point]
        if (
            old_label == 0
            and new_label == direction
            and component in component_seeds
            and not any(
                repaired[adjacent] == direction
                for adjacent in neighbours[point]
            )
        ):
            return None

        delta = unary_cost(point, new_label) - unary_cost(point, old_label)
        delta += beta * sum(
            abs(new_label - repaired[adjacent])
            - abs(old_label - repaired[adjacent])
            for adjacent in neighbours[point]
        )
        return float(delta)

    def push_move(point: int) -> None:
        delta = move_delta(point)
        if delta is not None:
            heapq.heappush(
                move_heap,
                (delta, point, generations[point]),
            )

    for point in empty_points:
        push_move(point)

    total_delta = 0.0
    move_count = 0
    while current_net != target_net:
        while move_heap:
            delta, point, generation = heapq.heappop(move_heap)
            current_delta = move_delta(point)
            if (
                generation == generations[point]
                and current_delta is not None
                and abs(current_delta - delta) < 1e-9
            ):
                break
        else:
            raise RuntimeError("no ownership label is eligible for aggregate repair")

        old_label = repaired[point]
        repaired[point] = old_label + direction
        current_net += direction
        total_delta += delta
        move_count += 1
        if repaired[point] == direction:
            component_seeds.add(component_for[point])

        affected = {point}
        affected.update(
            adjacent
            for adjacent in neighbours[point]
            if adjacent in generations
        )
        for changed in affected:
            generations[changed] += 1
            push_move(changed)

    if current_net != target_net:
        raise RuntimeError("ownership aggregate repair ended at the wrong sum")
    return repaired, float(total_delta), move_count
