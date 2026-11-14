# SPDX-License-Identifier: MIT
"""Spatially coherent ternary ownership quantization using a global MIP."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Mapping, Any, Tuple

import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix

from .stencil_ownership import OwnershipEvidence, OwnershipRegions


@dataclass(frozen=True)
class CoherentOwnershipPolicy:
    """Quantize ownership while preserving its aggregate and discouraging speckle.

    The model uses two ordered binary indicators per point for labels -1, 0, +1.
    Total variation is represented exactly by absolute differences between
    neighbouring indicators, and the aggregate constraint is enforced in the
    same mixed-integer optimization problem.
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
        stdev = evidence.ownership_stdev
        if stdev is None:
            raise ValueError(
                "coherent ownership policy requires the ownershipStdev map"
            )

        empty_points = evidence.empty_points
        fixed_positive = set(evidence.dead_black)
        fixed_negative = set(evidence.dead_white)
        eligible = tuple(sorted(
            set(empty_points) | fixed_positive | fixed_negative
        ))
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

        node_index = {point: index for index, point in enumerate(eligible)}
        edge_pairs: List[Tuple[int, int]] = []
        eligible_set = set(eligible)
        for point in eligible:
            x = point % evidence.width
            if x + 1 < evidence.width and point + 1 in eligible_set:
                edge_pairs.append((node_index[point], node_index[point + 1]))
            below = point + evidence.width
            if below < evidence.width * evidence.height and below in eligible_set:
                edge_pairs.append((node_index[point], node_index[below]))

        node_count = len(eligible)
        edge_count = len(edge_pairs)
        variable_count = 2 * node_count + 2 * edge_count
        objective = np.zeros(variable_count, dtype=np.float64)
        lower_bounds = np.zeros(variable_count, dtype=np.float64)
        upper_bounds = np.ones(variable_count, dtype=np.float64)
        integrality = np.zeros(variable_count, dtype=np.int32)
        integrality[:2 * node_count] = 1

        empty_set = set(empty_points)
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
                cost_negative = 1.0 + 2.0 * mean + 2.0 * self.kappa * uncertainty
                cost_positive = 1.0 - 2.0 * mean + 2.0 * self.kappa * uncertainty
                # cost(-1)=cost_negative, cost(0)=0, cost(+1)=cost_positive
                # with q=a+b-1 and b<=a, up to a label-independent constant.
                objective[a_index] = -cost_negative
                objective[b_index] = cost_positive
            else:
                raise RuntimeError("eligible ownership point has no board disposition")

        for edge_index in range(edge_count):
            first_aux = 2 * node_count + 2 * edge_index
            objective[first_aux] = self.beta
            objective[first_aux + 1] = self.beta

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
                    0.0, np.inf,
                )
                add_row(
                    ((auxiliary, 1.0), (first_bit, 1.0), (second_bit, -1.0)),
                    0.0, np.inf,
                )

        # Keep the integer sum of empty-point labels within the configured
        # tolerance of their aggregate ownership expectation.
        expected_empty_net = sum(evidence.ownership[point] for point in empty_points)
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

        matrix = coo_matrix(
            (coefficients, (row_indices, column_indices)),
            shape=(row, variable_count),
            dtype=np.float64,
        ).tocsc()
        result = milp(
            c=objective,
            integrality=integrality,
            bounds=Bounds(lower_bounds, upper_bounds),
            constraints=LinearConstraint(
                matrix,
                np.asarray(row_lower, dtype=np.float64),
                np.asarray(row_upper, dtype=np.float64),
            ),
            options={
                "time_limit": self.time_limit_ms / 1000.0,
                "disp": False,
            },
        )

        if result.x is None or result.status not in (0, 1):
            raise RuntimeError(
                "coherent ownership optimization produced no usable incumbent "
                f"(status={result.status}: {result.message})"
            )
        solution = np.asarray(result.x, dtype=np.float64)
        if solution.shape != (variable_count,) or not np.isfinite(solution).all():
            raise RuntimeError("coherent ownership optimizer returned a malformed incumbent")

        labels: Dict[int, int] = {}
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
                raise RuntimeError("coherent ownership incumbent violates ternary-label encoding")
            labels[point] = a_label + b_label - 1

        actual_empty_net = sum(labels[point] for point in empty_points)
        if not minimum_net <= actual_empty_net <= maximum_net:
            raise RuntimeError("coherent ownership incumbent violates the aggregate constraint")

        positive = fixed_positive | {
            point for point in empty_points if labels[point] == 1
        }
        negative = fixed_negative | {
            point for point in empty_points if labels[point] == -1
        }
        dead_net = len(fixed_positive) - len(fixed_negative)
        residual = abs(actual_empty_net - expected_empty_net)
        solver_status = "optimal" if result.status == 0 else "feasible_time_limit"
        if result.fun is None or not math.isfinite(float(result.fun)):
            raise RuntimeError("coherent ownership optimizer returned no finite objective")
        objective_value = float(result.fun)
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
                "optimal": result.status == 0,
                "objective": objective_value,
            },
        )
