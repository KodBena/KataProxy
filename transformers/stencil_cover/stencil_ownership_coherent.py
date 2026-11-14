# SPDX-License-Identifier: MIT
"""Spatially coherent ternary ownership quantization using a bounded HiGHS MIP."""

from __future__ import annotations

import heapq
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Tuple

import highspy
import numpy as np

from .stencil_ownership import OwnershipEvidence, OwnershipRegions


@dataclass(frozen=True)
class CoherentOwnershipPolicy:
    """Quantize ownership while preserving its aggregate and discouraging speckle.

    The model uses two ordered binary indicators per point for labels -1, 0, +1.
    Total variation is represented exactly by absolute differences between
    neighbouring indicators. A feasible spatially-aware labeling is supplied
    to HiGHS before branch-and-bound, so a solver timeout still has a valid
    answer to return.
    """

    kappa: float = 1.0
    beta: float = 0.25
    score_tolerance: float = 0.5
    time_limit_ms: int = 250

    policy_id = "coherent"
    requires_ownership_stdev = True

    def __post_init__(self) -> None:
        for name, value in (
            ("kappa", self.kappa),
            ("beta", self.beta),
            ("score_tolerance", self.score_tolerance),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"{name} must be a finite non-negative number")
        if (
            isinstance(self.time_limit_ms, bool)
            or not isinstance(self.time_limit_ms, int)
            or self.time_limit_ms <= 0
        ):
            raise ValueError("time_limit_ms must be a positive integer")

    def validate_request_metadata(self, metadata: Mapping[str, Any]) -> None:
        # The coherent policy is configured at process startup; it has no
        # per-query tuning metadata.
        return None

    def classify(self, evidence: OwnershipEvidence) -> OwnershipRegions:
        started_at = time.perf_counter()
        deadline = started_at + self.time_limit_ms / 1000.0
        stdev = evidence.ownership_stdev
        if stdev is None:
            raise ValueError(
                "coherent ownership policy requires the ownershipStdev map"
            )

        empty_points = evidence.empty_points
        fixed_positive = set(evidence.dead_black)
        fixed_negative = set(evidence.dead_white)
        eligible = tuple(sorted(set(empty_points) | fixed_positive | fixed_negative))
        if not empty_points:
            actual_fixed_net = len(fixed_positive) - len(fixed_negative)
            return OwnershipRegions(
                positive=tuple(sorted(fixed_positive)),
                negative=tuple(sorted(fixed_negative)),
                policy_id=self.policy_id,
                diagnostics={
                    "targetNetOwnership": float(actual_fixed_net),
                    "actualNetOwnership": float(actual_fixed_net),
                    "absoluteResidual": 0.0,
                    "emptyPointCount": 0,
                    "spatialEdgeCount": 0,
                    "solverStatus": "not_required",
                    "optimal": True,
                },
            )

        expected_empty_net = sum(
            evidence.ownership[point] for point in empty_points
        )
        minimum_net = max(
            -len(empty_points),
            math.ceil(expected_empty_net - self.score_tolerance - 1e-9),
        )
        maximum_net = min(
            len(empty_points),
            math.floor(expected_empty_net + self.score_tolerance + 1e-9),
        )
        if minimum_net > maximum_net:
            raise ValueError(
                "ownership score tolerance contains no attainable integer net sum"
            )
        target_empty_net = min(
            maximum_net,
            max(minimum_net, math.floor(expected_empty_net + 0.5)),
        )

        node_index = {point: index for index, point in enumerate(eligible)}
        eligible_set = set(eligible)
        neighbours: Dict[int, List[int]] = {point: [] for point in eligible}
        edge_pairs: List[Tuple[int, int]] = []
        for point in eligible:
            x = point % evidence.width
            if x + 1 < evidence.width and point + 1 in eligible_set:
                first = node_index[point]
                second = node_index[point + 1]
                edge_pairs.append((first, second))
                neighbours[point].append(point + 1)
                neighbours[point + 1].append(point)
            below = point + evidence.width
            if (
                below < evidence.width * evidence.height
                and below in eligible_set
            ):
                first = node_index[point]
                second = node_index[below]
                edge_pairs.append((first, second))
                neighbours[point].append(below)
                neighbours[below].append(point)

        empty_set = set(empty_points)

        def unary_cost(point: int, label: int) -> float:
            mean = evidence.ownership[point]
            uncertainty = stdev[point]
            if label == 0:
                return 0.0
            if label == 1:
                return 1.0 - 2.0 * mean + 2.0 * self.kappa * uncertainty
            return 1.0 + 2.0 * mean + 2.0 * self.kappa * uncertainty

        # Start from the independent unary minima, then meet the aggregate
        # exactly using cheapest one-cell moves. Marginals are kept in a heap
        # and only the moved point and its neighbours need to be refreshed.
        labels: Dict[int, int] = {}
        for point in eligible:
            if point in fixed_positive:
                labels[point] = 1
            elif point in fixed_negative:
                labels[point] = -1
            else:
                labels[point] = min(
                    (0, 1, -1),
                    key=lambda label: (
                        unary_cost(point, label),
                        abs(label),
                        -label,
                    ),
                )

        current_empty_net = sum(labels[point] for point in empty_points)
        if current_empty_net != target_empty_net:
            direction = 1 if current_empty_net < target_empty_net else -1
            generations = {point: 0 for point in empty_points}
            move_heap: List[Tuple[float, int, int]] = []

            def move_delta(point: int) -> float:
                old_label = labels[point]
                new_label = old_label + direction
                delta = unary_cost(point, new_label) - unary_cost(point, old_label)
                delta += self.beta * sum(
                    abs(new_label - labels[adjacent])
                    - abs(old_label - labels[adjacent])
                    for adjacent in neighbours[point]
                )
                return float(delta)

            def push_move(point: int) -> None:
                if -1 <= labels[point] + direction <= 1:
                    heapq.heappush(
                        move_heap,
                        (move_delta(point), point, generations[point]),
                    )

            for point in empty_points:
                push_move(point)

            while current_empty_net != target_empty_net:
                while move_heap:
                    _delta, point, generation = heapq.heappop(move_heap)
                    if generation != generations[point]:
                        continue
                    break
                else:
                    raise RuntimeError(
                        "could not construct a ternary labeling for the target net"
                    )

                labels[point] += direction
                current_empty_net += direction
                affected = {point}
                affected.update(
                    adjacent
                    for adjacent in neighbours[point]
                    if adjacent in empty_set
                )
                for changed in affected:
                    generations[changed] += 1
                    push_move(changed)

        node_count = len(eligible)
        edge_count = len(edge_pairs)
        variable_count = 2 * node_count + 2 * edge_count
        objective = np.zeros(variable_count, dtype=np.float64)
        lower_bounds = np.zeros(variable_count, dtype=np.float64)
        upper_bounds = np.ones(variable_count, dtype=np.float64)
        integer_columns = np.arange(2 * node_count, dtype=np.int32)

        for point, index in node_index.items():
            a_index = 2 * index
            b_index = a_index + 1
            if point in fixed_positive:
                lower_bounds[a_index] = upper_bounds[a_index] = 1.0
                lower_bounds[b_index] = upper_bounds[b_index] = 1.0
            elif point in fixed_negative:
                lower_bounds[a_index] = upper_bounds[a_index] = 0.0
                lower_bounds[b_index] = upper_bounds[b_index] = 0.0
            elif point in empty_set:
                mean = evidence.ownership[point]
                uncertainty = stdev[point]
                cost_negative = (
                    1.0 + 2.0 * mean + 2.0 * self.kappa * uncertainty
                )
                cost_positive = (
                    1.0 - 2.0 * mean + 2.0 * self.kappa * uncertainty
                )
                # q=a+b-1 encodes -1=(0,0), 0=(1,0), +1=(1,1).
                # Up to the label-independent cost(-1), these are the exact
                # unary coefficients for the three requested labels.
                objective[a_index] = -cost_negative
                objective[b_index] = cost_positive
            else:
                raise RuntimeError(
                    "eligible ownership point has no board disposition"
                )

        for edge_index in range(edge_count):
            first_aux = 2 * node_count + 2 * edge_index
            objective[first_aux] = self.beta
            objective[first_aux + 1] = self.beta

        initial_solution = np.zeros(variable_count, dtype=np.float64)
        for point, index in node_index.items():
            label = labels[point]
            initial_solution[2 * index] = 1.0 if label >= 0 else 0.0
            initial_solution[2 * index + 1] = 1.0 if label == 1 else 0.0
        for edge_index, (first, second) in enumerate(edge_pairs):
            for bit in (0, 1):
                auxiliary = 2 * node_count + 2 * edge_index + bit
                first_bit = 2 * first + bit
                second_bit = 2 * second + bit
                initial_solution[auxiliary] = abs(
                    initial_solution[first_bit] - initial_solution[second_bit]
                )

        row_indices: List[int] = []
        column_indices: List[int] = []
        coefficients: List[float] = []
        row_lower: List[float] = []
        row_upper: List[float] = []
        row = 0

        def add_row(
            entries: Tuple[Tuple[int, float], ...],
            minimum: float,
            maximum: float,
        ) -> None:
            nonlocal row
            for column, value in entries:
                row_indices.append(row)
                column_indices.append(column)
                coefficients.append(value)
            row_lower.append(minimum)
            row_upper.append(maximum)
            row += 1

        # Ordered label indicators: b <= a.
        for index in range(node_count):
            add_row(((2 * index + 1, 1.0), (2 * index, -1.0)), -np.inf, 0.0)

        # |q_i-q_j| = |a_i-a_j| + |b_i-b_j| for ordered ternary labels.
        for edge_index, (first, second) in enumerate(edge_pairs):
            for bit in (0, 1):
                auxiliary = 2 * node_count + 2 * edge_index + bit
                first_bit = 2 * first + bit
                second_bit = 2 * second + bit
                add_row(
                    ((auxiliary, 1.0), (first_bit, -1.0), (second_bit, 1.0)),
                    0.0,
                    np.inf,
                )
                add_row(
                    ((auxiliary, 1.0), (first_bit, 1.0), (second_bit, -1.0)),
                    0.0,
                    np.inf,
                )

        sum_entries = tuple(
            (2 * node_index[point] + bit, 1.0)
            for point in empty_points
            for bit in (0, 1)
        )
        add_row(
            sum_entries,
            float(len(empty_points) + minimum_net),
            float(len(empty_points) + maximum_net),
        )

        # Convert the row-oriented accumulator directly into the compact CSR
        # representation expected by Highs.addRows; no second sparse-matrix
        # conversion or per-row Python/C++ call is required.
        row_counts = np.bincount(
            np.asarray(row_indices, dtype=np.int32),
            minlength=row,
        )
        row_starts = np.empty(row + 1, dtype=np.int32)
        row_starts[0] = 0
        np.cumsum(row_counts, out=row_starts[1:])
        column_array = np.asarray(column_indices, dtype=np.int32)
        coefficient_array = np.asarray(coefficients, dtype=np.float64)
        row_lower_array = np.asarray(row_lower, dtype=np.float64)
        row_upper_array = np.asarray(row_upper, dtype=np.float64)

        solution = initial_solution
        solver_status = "feasible_time_limit"
        optimal = False
        remaining_seconds = deadline - time.perf_counter()
        if remaining_seconds > 0.005:
            highs = highspy.Highs()
            for option_name, option_value in (
                ("output_flag", False),
                ("threads", 1),
            ):
                option_status = highs.setOptionValue(option_name, option_value)
                if option_status == highspy.HighsStatus.kError:
                    raise RuntimeError(
                        f"HiGHS rejected its {option_name} solver option"
                    )

            highs.addVars(variable_count, lower_bounds, upper_bounds)
            all_columns = np.arange(variable_count, dtype=np.int32)
            highs.changeColsCost(variable_count, all_columns, objective)
            integrality = np.full(
                2 * node_count,
                int(highspy.HighsVarType.kInteger),
                dtype=np.uint8,
            )
            highs.changeColsIntegrality(
                len(integer_columns),
                integer_columns,
                integrality,
            )
            highs.addRows(
                row,
                row_lower_array,
                row_upper_array,
                len(coefficients),
                row_starts,
                column_array,
                coefficient_array,
            )
            highs.setSolution(
                variable_count,
                all_columns,
                initial_solution,
            )

            # Model construction and the feasible start count against the same
            # end-to-end budget. Leave a small reserve for returning the result.
            remaining_seconds = deadline - time.perf_counter()
            solver_seconds = remaining_seconds * 0.8
            if solver_seconds > 0.001:
                highs.setOptionValue("time_limit", solver_seconds)
                highs.run()
                model_status = highs.getModelStatus()
                candidate = highs.getSolution()
                candidate_values = getattr(candidate, "col_value", None)
                if (
                    bool(getattr(candidate, "value_valid", False))
                    and candidate_values is not None
                    and len(candidate_values) == variable_count
                ):
                    solution = np.asarray(candidate_values, dtype=np.float64)
                    optimal = model_status == highspy.HighsModelStatus.kOptimal
                    solver_status = (
                        "optimal" if optimal else "feasible_time_limit"
                    )

        if solution.shape != (variable_count,) or not np.isfinite(solution).all():
            raise RuntimeError(
                "coherent ownership optimizer returned a malformed incumbent"
            )

        labels = {}
        for point, index in node_index.items():
            a_value = float(solution[2 * index])
            b_value = float(solution[2 * index + 1])
            a_label = int(round(a_value))
            b_label = int(round(b_value))
            if (
                abs(a_value - a_label) > 1e-5
                or abs(b_value - b_label) > 1e-5
                or not 0 <= b_label <= a_label <= 1
            ):
                raise RuntimeError(
                    "coherent ownership incumbent violates ternary-label encoding"
                )
            labels[point] = a_label + b_label - 1

        actual_empty_net = sum(labels[point] for point in empty_points)
        if not minimum_net <= actual_empty_net <= maximum_net:
            raise RuntimeError(
                "coherent ownership incumbent violates the aggregate constraint"
            )

        positive = fixed_positive | {
            point for point in empty_points if labels[point] == 1
        }
        negative = fixed_negative | {
            point for point in empty_points if labels[point] == -1
        }
        dead_net = len(fixed_positive) - len(fixed_negative)
        residual = abs(actual_empty_net - expected_empty_net)
        objective_value = float(np.dot(objective, solution))
        return OwnershipRegions(
            positive=tuple(sorted(positive)),
            negative=tuple(sorted(negative)),
            policy_id=self.policy_id,
            diagnostics={
                "targetNetOwnership": float(expected_empty_net + dead_net),
                "actualNetOwnership": float(actual_empty_net + dead_net),
                "emptyExpectedNet": float(expected_empty_net),
                "emptyQuantizedNet": float(actual_empty_net),
                "absoluteResidual": float(residual),
                "emptyPointCount": len(empty_points),
                "spatialEdgeCount": edge_count,
                "solverStatus": solver_status,
                "optimal": optimal,
                "objective": objective_value,
            },
        )
