# SPDX-License-Identifier: MIT
"""Exact fixed-multiplier graph-cut solver for ternary ownership regions."""

from __future__ import annotations

from collections import deque
import math
from typing import Dict, List, Sequence, Tuple

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import maximum_flow

from .stencil_ownership import OwnershipEvidence


_CAPACITY_SCALE = 1_000_000_000


def ownership_graph(
    points: Sequence[int],
    width: int,
    height: int,
) -> tuple[Dict[int, List[int]], List[Tuple[int, int]]]:
    """Build 4-neighbour adjacency once for all eligible cells."""
    allowed = set(points)
    neighbours: Dict[int, List[int]] = {point: [] for point in points}
    edges: List[Tuple[int, int]] = []
    for point in points:
        x = point % width
        right = point + 1
        below = point + width
        if x + 1 < width and right in allowed:
            neighbours[point].append(right)
            neighbours[right].append(point)
            edges.append((point, right))
        if below < width * height and below in allowed:
            neighbours[point].append(below)
            neighbours[below].append(point)
            edges.append((point, below))
    return neighbours, edges


def minimum_cut_labels(
    evidence: OwnershipEvidence,
    eligible: Sequence[int],
    empty_points: Sequence[int],
    edges: Sequence[Tuple[int, int]],
    fixed_positive: set[int],
    fixed_negative: set[int],
    kappa: float,
    beta: float,
    lagrange: float,
) -> Dict[int, int]:
    """Minimize E(q)+lambda*sum(q) by a binary submodular minimum cut.

    Each ternary label is encoded by two ordered bits:
      -1 -> (0, 0), 0 -> (1, 0), +1 -> (1, 1).

    The ordering implication is a hard directed arc. Unary costs and the
    four-neighbour total-variation term are represented by terminal and
    symmetric pairwise arcs. Integer capacities at 1e-9 resolution make the
    graph-cut arithmetic deterministic for the ownership values used here.
    """
    node_index = {point: index for index, point in enumerate(eligible)}
    node_count = len(eligible)
    source = 2 * node_count
    sink = source + 1
    variable_count = sink + 1

    empty_set = set(empty_points)
    stdev = evidence.ownership_stdev
    if stdev is None:
        raise ValueError("coherent ownership policy requires ownershipStdev")

    unary_coefficients: Dict[int, Tuple[float, float]] = {}
    finite_cut_bound = 1.0
    for point in empty_points:
        mean = evidence.ownership[point]
        uncertainty = stdev[point]
        cost_negative = 1.0 + 2.0 * mean + 2.0 * kappa * uncertainty
        cost_positive = 1.0 - 2.0 * mean + 2.0 * kappa * uncertainty
        # q=a+b-1; adding lambda*q adds lambda to both bit coefficients.
        coefficient_a = -cost_negative + lagrange
        coefficient_b = cost_positive + lagrange
        unary_coefficients[point] = (coefficient_a, coefficient_b)
        finite_cut_bound += abs(coefficient_a) + abs(coefficient_b)

    # A cut crosses at most one directed arc per differing bit; each board
    # edge has two bits, so 2*beta is a bound on its pairwise contribution.
    finite_cut_bound += 2.0 * beta * len(edges)
    infinite_capacity = int(math.ceil(finite_cut_bound * _CAPACITY_SCALE)) + 1

    rows: List[int] = []
    columns: List[int] = []
    capacities: List[int] = []

    def add_arc(start: int, end: int, capacity: int) -> None:
        if capacity > 0:
            rows.append(start)
            columns.append(end)
            capacities.append(capacity)

    def add_finite_arc(start: int, end: int, capacity: float) -> None:
        scaled = int(round(max(0.0, capacity) * _CAPACITY_SCALE))
        add_arc(start, end, scaled)

    # The ordered-label implication b<=a.
    for index in range(node_count):
        add_arc(node_count + index, index, infinite_capacity)

    for point, index in node_index.items():
        a_node = index
        b_node = node_count + index
        if point in fixed_positive:
            add_arc(source, a_node, infinite_capacity)
            add_arc(source, b_node, infinite_capacity)
        elif point in fixed_negative:
            add_arc(a_node, sink, infinite_capacity)
            add_arc(b_node, sink, infinite_capacity)
        elif point in empty_set:
            coefficient_a, coefficient_b = unary_coefficients[point]
            if coefficient_a < 0:
                add_finite_arc(source, a_node, -coefficient_a)
            elif coefficient_a > 0:
                add_finite_arc(a_node, sink, coefficient_a)
            if coefficient_b < 0:
                add_finite_arc(source, b_node, -coefficient_b)
            elif coefficient_b > 0:
                add_finite_arc(b_node, sink, coefficient_b)
        else:
            raise RuntimeError("eligible point has no ownership disposition")

    # For ordered ternary labels:
    # |q_i-q_j| = |a_i-a_j| + |b_i-b_j|.
    for first_point, second_point in edges:
        first = node_index[first_point]
        second = node_index[second_point]
        for offset in (0, node_count):
            first_bit = offset + first
            second_bit = offset + second
            add_finite_arc(first_bit, second_bit, beta)
            add_finite_arc(second_bit, first_bit, beta)

    capacity_matrix = coo_matrix(
        (
            np.asarray(capacities, dtype=np.int64),
            (
                np.asarray(rows, dtype=np.int32),
                np.asarray(columns, dtype=np.int32),
            ),
        ),
        shape=(variable_count, variable_count),
        dtype=np.int64,
    ).tocsr()

    result = maximum_flow(
        capacity_matrix, source, sink, method="dinic",
    )
    residual = (capacity_matrix - result.flow).tocsr()

    reached = np.zeros(variable_count, dtype=np.bool_)
    reached[source] = True
    pending = deque([source])
    while pending:
        start = pending.popleft()
        first_edge = residual.indptr[start]
        last_edge = residual.indptr[start + 1]
        for edge_index in range(first_edge, last_edge):
            adjacent = int(residual.indices[edge_index])
            if residual.data[edge_index] > 0 and not reached[adjacent]:
                reached[adjacent] = True
                pending.append(adjacent)

    a_bits = reached[:node_count]
    b_bits = reached[node_count:2 * node_count]
    if np.any(b_bits & ~a_bits):
        raise RuntimeError("minimum cut violated the ordered ternary-label constraint")

    labels = {
        point: int(a_bits[index]) + int(b_bits[index]) - 1
        for point, index in node_index.items()
    }
    for point in fixed_positive:
        if labels[point] != 1:
            raise RuntimeError("minimum cut changed a fixed White-owned dead chain")
    for point in fixed_negative:
        if labels[point] != -1:
            raise RuntimeError("minimum cut changed a fixed Black-owned dead chain")
    return labels
