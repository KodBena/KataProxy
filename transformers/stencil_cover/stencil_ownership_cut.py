# SPDX-License-Identifier: MIT
"""Spatially coherent ternary ownership quantization using graph cuts."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Mapping

from .stencil_ownership import OwnershipEvidence, OwnershipRegions
from .stencil_ownership_graphcut import minimum_cut_labels, ownership_graph
from .stencil_ownership_repair import (
    component_ids,
    greedy_sum_repair,
    ownership_energy,
)


_MAX_LAGRANGE_CUTS = 24
_REPAIR_RESERVE_FRACTION = 0.18
_REPAIR_RESERVE_FLOOR_MS = 8.0


@dataclass(frozen=True)
class CoherentCutOwnershipPolicy:
    """Fast spatial quantizer with explicit optimality/repair diagnostics.

    Each fixed-multiplier subproblem is solved by an exact s-t minimum cut.
    Since the discrete Lagrange path can skip an aggregate sum, a deterministic
    adjacent-cell repair is used only when no cut on the path satisfies the
    configured integer interval. Repaired outputs are marked feasible, never
    described as globally optimal.
    """

    kappa: float = 1.0
    beta: float = 0.25
    score_tolerance: float = 0.5
    time_limit_ms: int = 250

    policy_id = "coherent-cut"
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
        # Objective weights and uncertainty treatment are process-level
        # configuration, not per-request tuning data.
        return None

    def classify(self, evidence: OwnershipEvidence) -> OwnershipRegions:
        started_at = time.perf_counter()
        stdev = evidence.ownership_stdev
        if stdev is None:
            raise ValueError("coherent ownership policy requires ownershipStdev")

        empty_points = tuple(evidence.empty_points)
        fixed_positive = set(evidence.dead_black)
        fixed_negative = set(evidence.dead_white)
        eligible = tuple(sorted(set(empty_points) | fixed_positive | fixed_negative))
        dead_net = len(fixed_positive) - len(fixed_negative)

        if not empty_points:
            return OwnershipRegions(
                positive=tuple(sorted(fixed_positive)),
                negative=tuple(sorted(fixed_negative)),
                policy_id=self.policy_id,
                diagnostics={
                    "targetNetOwnership": float(dead_net),
                    "actualNetOwnership": float(dead_net),
                    "emptyExpectedNet": 0.0,
                    "emptyQuantizedNet": 0.0,
                    "absoluteResidual": 0.0,
                    "emptyPointCount": 0,
                    "spatialEdgeCount": 0,
                    "solverStatus": "not_required",
                    "optimal": True,
                    "wallTimeMs": (time.perf_counter() - started_at) * 1000.0,
                },
            )

        expected_empty_net = float(sum(
            evidence.ownership[point] for point in empty_points
        ))
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

        target_net = min(
            maximum_net,
            max(minimum_net, math.floor(expected_empty_net + 0.5)),
        )
        neighbours, edges = ownership_graph(
            eligible, evidence.width, evidence.height,
        )
        component_for = component_ids(eligible, neighbours)

        # Bounds include the maximum local TV influence, so the two endpoints
        # of the monotone path favour all-empty +1 and all-empty -1 labelings.
        c_negative = [
            1.0 + 2.0 * evidence.ownership[point] + 2.0 * self.kappa * stdev[point]
            for point in empty_points
        ]
        c_positive = [
            1.0 - 2.0 * evidence.ownership[point] + 2.0 * self.kappa * stdev[point]
            for point in empty_points
        ]
        lambda_bound = (
            max(
                max(abs(value) for value in c_negative),
                max(abs(value) for value in c_positive),
            )
            + 8.0 * self.beta
            + 1.0
        )
        low_lambda = -lambda_bound
        high_lambda = lambda_bound

        best_path_labels: dict[int, int] | None = None
        best_path_lambda = 0.0
        best_path_net = 0
        best_path_residual = math.inf
        best_path_energy = math.inf
        best_feasible_labels: dict[int, int] | None = None
        best_feasible_lambda = 0.0
        best_feasible_net = 0
        best_feasible_energy = math.inf
        unconstrained_feasible = False
        cut_calls = 0

        def consider_cut(lambda_value: float) -> int:
            nonlocal best_path_labels, best_path_lambda, best_path_net
            nonlocal best_path_residual, best_path_energy
            nonlocal best_feasible_labels, best_feasible_lambda
            nonlocal best_feasible_net, best_feasible_energy
            nonlocal unconstrained_feasible, cut_calls

            candidate = minimum_cut_labels(
                evidence,
                eligible,
                empty_points,
                edges,
                fixed_positive,
                fixed_negative,
                self.kappa,
                self.beta,
                lambda_value,
            )
            cut_calls += 1
            candidate_net = sum(candidate[point] for point in empty_points)
            candidate_residual = abs(candidate_net - expected_empty_net)
            candidate_energy = ownership_energy(
                candidate,
                evidence,
                empty_points,
                edges,
                self.kappa,
                self.beta,
            )
            if (
                candidate_residual < best_path_residual - 1e-9
                or (
                    abs(candidate_residual - best_path_residual) <= 1e-9
                    and candidate_energy < best_path_energy
                )
            ):
                best_path_labels = candidate
                best_path_lambda = lambda_value
                best_path_net = candidate_net
                best_path_residual = candidate_residual
                best_path_energy = candidate_energy

            if minimum_net <= candidate_net <= maximum_net:
                if candidate_energy < best_feasible_energy:
                    best_feasible_labels = candidate
                    best_feasible_lambda = lambda_value
                    best_feasible_net = candidate_net
                    best_feasible_energy = candidate_energy
                if lambda_value == 0.0:
                    unconstrained_feasible = True
            return candidate_net

        # Lambda=0 is the unconstrained energy minimum. If it already lies in
        # the aggregate interval, it is also the global constrained optimum.
        first_net = consider_cut(0.0)
        optimal = minimum_net <= first_net <= maximum_net

        if not optimal:
            if first_net > maximum_net:
                low_lambda = 0.0
            else:
                high_lambda = 0.0

            deadline = started_at + self.time_limit_ms / 1000.0
            reserve_seconds = max(
                _REPAIR_RESERVE_FLOOR_MS / 1000.0,
                self.time_limit_ms / 1000.0 * _REPAIR_RESERVE_FRACTION,
            )
            for _iteration in range(1, _MAX_LAGRANGE_CUTS):
                if time.perf_counter() >= deadline - reserve_seconds:
                    break
                lambda_value = (low_lambda + high_lambda) / 2.0
                candidate_net = consider_cut(lambda_value)
                if candidate_net > maximum_net:
                    low_lambda = lambda_value
                elif candidate_net < minimum_net:
                    high_lambda = lambda_value
                elif candidate_net > target_net:
                    low_lambda = lambda_value
                elif candidate_net < target_net:
                    high_lambda = lambda_value
                else:
                    break

        repair_moves = 0
        repair_delta = 0.0
        if optimal:
            if best_path_labels is None:
                raise RuntimeError("ownership graph cut produced no labeling")
            labels = best_path_labels
            selected_lambda = best_path_lambda
            selected_path_net = best_path_net
            solver_status = "optimal"
        elif best_feasible_labels is not None:
            labels = best_feasible_labels
            selected_lambda = best_feasible_lambda
            selected_path_net = best_feasible_net
            optimal = minimum_net == maximum_net or unconstrained_feasible
            solver_status = "optimal" if optimal else "feasible_supported"
        else:
            if best_path_labels is None:
                raise RuntimeError("ownership graph cut produced no labeling")
            labels, repair_delta, repair_moves = greedy_sum_repair(
                best_path_labels,
                evidence,
                empty_points,
                eligible,
                neighbours,
                component_for,
                target_net,
                self.kappa,
                self.beta,
            )
            selected_lambda = best_path_lambda
            selected_path_net = best_path_net
            solver_status = "feasible_repaired"
            optimal = False

        actual_empty_net = sum(labels[point] for point in empty_points)
        if not minimum_net <= actual_empty_net <= maximum_net:
            raise RuntimeError("coherent ownership labeling violates the aggregate constraint")

        positive = fixed_positive | {
            point for point in empty_points if labels[point] == 1
        }
        negative = fixed_negative | {
            point for point in empty_points if labels[point] == -1
        }
        objective_value = ownership_energy(
            labels,
            evidence,
            empty_points,
            edges,
            self.kappa,
            self.beta,
        )
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        residual = abs(actual_empty_net - expected_empty_net)
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
                "spatialEdgeCount": len(edges),
                "solverStatus": solver_status,
                "optimal": bool(optimal),
                "objective": float(objective_value),
                "lagrangePathNet": int(selected_path_net),
                "lagrangePathResidual": float(
                    abs(selected_path_net - expected_empty_net)
                ),
                "lagrangeLambda": float(selected_lambda),
                "cutCalls": int(cut_calls),
                "repairMoves": int(repair_moves),
                "repairObjectiveDelta": float(repair_delta),
                "timeLimitMs": int(self.time_limit_ms),
                "wallTimeMs": float(elapsed_ms),
                "timeBudgetExceeded": bool(elapsed_ms > self.time_limit_ms),
                "capacityScale": 1_000_000_000,
            },
        )
