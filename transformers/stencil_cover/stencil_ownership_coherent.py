# SPDX-License-Identifier: MIT
"""Spatially coherent ternary ownership quantization using a bounded HiGHS MIP."""

from __future__ import annotations

import heapq
import hashlib
import json
import math
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Dict, List, Mapping, Tuple, cast

import highspy
import numpy as np

from .stencil_ownership import OwnershipEvidence, OwnershipRegions
from .stencil_ownership_repair import ownership_energy
from .stencil_ownership_solver_api import (
    OwnershipLabeling,
    OwnershipOptimizationRecord,
    OwnershipSolverStatus,
)
from .stencil_solver_api import NoNativeEffortLimit, SolverBudget, SolverIdentity



_OWNERSHIP_HIGHS_ADAPTER_VERSION = "kataproxy-ownership-highs-adapter-v1"
_OWNERSHIP_MIP_FORMULATION_VERSION = "ordered-ternary-tv-mip-v1"

try:
    _HIGHS_PACKAGE_VERSION = version("highspy")
except PackageNotFoundError:
    _HIGHS_PACKAGE_VERSION = str(getattr(highspy, "__version__", "unknown"))


def _ownership_solver_identity() -> SolverIdentity:
    """Identify the MIP adapter/configuration independently of its time budget."""
    configuration_json = json.dumps(
        {
            "formulation": _OWNERSHIP_MIP_FORMULATION_VERSION,
            "threads": 1,
            "objective": "unary-plus-total-variation-full-energy",
            "initial_solution": "spatial-greedy-feasible-v1",
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    configuration_fingerprint = hashlib.sha256(
        configuration_json.encode("utf-8")
    ).hexdigest()
    return SolverIdentity(
        backend_name="highs-mip",
        backend_version=_HIGHS_PACKAGE_VERSION,
        adapter_version=_OWNERSHIP_HIGHS_ADAPTER_VERSION,
        configuration_json=configuration_json,
        configuration_fingerprint=configuration_fingerprint,
    )


def _ownership_labeling(labels: Dict[int, int]) -> OwnershipLabeling:
    return OwnershipLabeling(tuple(sorted(
        (int(point), int(label)) for point, label in labels.items()
    )))


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
    model_version = "coherent-ownership-mip-v2"
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
        started_at_utc = datetime.now(timezone.utc).isoformat(
            timespec="microseconds",
        ).replace("+00:00", "Z")
        attempt_started_ns = time.perf_counter_ns()
        attempt_id = str(uuid.uuid4())
        budget = SolverBudget(
            wall_time_limit_ms=self.time_limit_ms,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id=f"ownership-foreground-{self.time_limit_ms}ms",
        )
        solver_identity = _ownership_solver_identity()
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

        # Preserve the feasible heuristic as its own evidence item. A HiGHS
        # incumbent is a second item, never a replacement for the seed record.
        initial_heuristic = dict(labels)
        energy_edges = tuple(
            (eligible[first], eligible[second]) for first, second in edge_pairs
        )
        initial_objective = ownership_energy(
            initial_heuristic, evidence, empty_points, energy_edges,
            self.kappa, self.beta,
        )

        optimizer_labels: Dict[int, int] | None = None
        optimizer_objective: float | None = None
        lower_bound: float | None = None
        backend_reported_gap: float | None = None
        backend_status: str | None = None
        optimizer_error: str | None = None
        solver_status = OwnershipSolverStatus.SEED_ONLY
        remaining_seconds = deadline - time.perf_counter()
        if remaining_seconds > 0.005:
            try:
                highs_constructor = cast(Any, highspy.Highs)
                highs = highs_constructor()
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

                # Model construction and the feasible start count against the
                # same deadline. Keep a reserve for validation and evidence capture.
                remaining_seconds = deadline - time.perf_counter()
                solver_seconds = remaining_seconds * 0.8
                if solver_seconds > 0.001:
                    option_status = highs.setOptionValue("time_limit", solver_seconds)
                    if option_status == highspy.HighsStatus.kError:
                        raise RuntimeError("HiGHS rejected its time_limit option")
                    highs.run()
                    model_status = highs.getModelStatus()
                    backend_status = str(model_status)
                    candidate = highs.getSolution()
                    candidate_values = getattr(candidate, "col_value", None)
                    candidate_valid = (
                        bool(getattr(candidate, "value_valid", False))
                        and candidate_values is not None
                        and len(candidate_values) == variable_count
                    )
                    if candidate_valid:
                        candidate_solution = np.asarray(
                            candidate_values, dtype=np.float64,
                        )
                        if (
                            candidate_solution.shape != (variable_count,)
                            or not np.isfinite(candidate_solution).all()
                        ):
                            raise RuntimeError(
                                "HiGHS returned a malformed ownership incumbent"
                            )
                        decoded: Dict[int, int] = {}
                        for point, index in node_index.items():
                            a_value = float(candidate_solution[2 * index])
                            b_value = float(candidate_solution[2 * index + 1])
                            a_label = int(round(a_value))
                            b_label = int(round(b_value))
                            if (
                                abs(a_value - a_label) > 1e-5
                                or abs(b_value - b_label) > 1e-5
                                or not 0 <= b_label <= a_label <= 1
                            ):
                                raise RuntimeError(
                                    "HiGHS incumbent violates ternary-label encoding"
                                )
                            decoded[point] = a_label + b_label - 1
                        candidate_net = sum(decoded[point] for point in empty_points)
                        if not minimum_net <= candidate_net <= maximum_net:
                            raise RuntimeError(
                                "HiGHS incumbent violates the aggregate constraint"
                            )
                        optimizer_labels = decoded
                        optimizer_objective = ownership_energy(
                            decoded, evidence, empty_points, energy_edges,
                            self.kappa, self.beta,
                        )
                        solver_status = (
                            OwnershipSolverStatus.OPTIMAL
                            if model_status == highspy.HighsModelStatus.kOptimal
                            else OwnershipSolverStatus.FEASIBLE
                        )
                    elif model_status == highspy.HighsModelStatus.kOptimal:
                        optimizer_error = (
                            "HiGHS reported optimality without a valid labeling witness"
                        )
                        solver_status = OwnershipSolverStatus.ERROR

                    # HiGHS' MIP objective omits the label-independent
                    # sum(C_i(-1)) constant. Restore it before recording the
                    # dual bound so it shares the full-energy scale above.
                    info = highs.getInfo()
                    raw_bound = getattr(info, "mip_dual_bound", None)
                    try:
                        dual_bound = (
                            float(raw_bound) if raw_bound is not None else float("nan")
                        )
                    except (TypeError, ValueError, OverflowError):
                        dual_bound = float("nan")
                    highs_infinity = float(getattr(highspy, "kHighsInf", 1e30))
                    if math.isfinite(dual_bound) and abs(dual_bound) < highs_infinity:
                        objective_offset = sum(
                            1.0 + 2.0 * evidence.ownership[point]
                            + 2.0 * self.kappa * stdev[point]
                            for point in empty_points
                        )
                        adjusted_bound = dual_bound + objective_offset
                        if (
                            optimizer_objective is None
                            or adjusted_bound <= optimizer_objective + 1e-7
                        ):
                            lower_bound = float(adjusted_bound)
                    raw_gap = getattr(info, "mip_gap", None)
                    try:
                        reported_gap = (
                            float(raw_gap) if raw_gap is not None else float("nan")
                        )
                    except (TypeError, ValueError, OverflowError):
                        reported_gap = float("nan")
                    if math.isfinite(reported_gap) and reported_gap >= 0:
                        backend_reported_gap = reported_gap
                    if solver_status is OwnershipSolverStatus.OPTIMAL:
                        # The optimal model status is the proof; use the fully
                        # normalized primal objective as its matching lower bound.
                        lower_bound = optimizer_objective
            except Exception as exc:
                optimizer_error = str(exc) or type(exc).__name__
                solver_status = OwnershipSolverStatus.ERROR

        # The seed remains the answer when the bounded optimizer did not
        # supply a validated incumbent. It is feasible, not an optimum claim.
        labels = (
            dict(initial_heuristic)
            if optimizer_labels is None
            else dict(optimizer_labels)
        )
        objective_value = ownership_energy(
            labels, evidence, empty_points, energy_edges, self.kappa, self.beta,
        )
        absolute_gap = (
            max(0.0, optimizer_objective - lower_bound)
            if optimizer_objective is not None and lower_bound is not None
            else None
        )
        elapsed_wall_ns = time.perf_counter_ns() - attempt_started_ns
        optimization_record = OwnershipOptimizationRecord(
            attempt_id=attempt_id,
            started_at_utc=started_at_utc,
            elapsed_wall_ns=elapsed_wall_ns,
            budget=budget,
            solver_identity=solver_identity,
            status=solver_status,
            initial_heuristic=_ownership_labeling(initial_heuristic),
            initial_objective=initial_objective,
            optimizer_incumbent=(
                None if optimizer_labels is None else _ownership_labeling(optimizer_labels)
            ),
            optimizer_objective=optimizer_objective,
            selected_labeling=_ownership_labeling(labels),
            selected_objective=objective_value,
            lower_bound=lower_bound,
            absolute_gap=absolute_gap,
            backend_reported_gap=backend_reported_gap,
            backend_status=backend_status,
            error=optimizer_error,
        )

        actual_empty_net = sum(labels[point] for point in empty_points)
        if not minimum_net <= actual_empty_net <= maximum_net:
            raise RuntimeError(
                "selected ownership labeling violates the aggregate constraint"
            )

        positive = fixed_positive | {
            point for point in empty_points if labels[point] == 1
        }
        negative = fixed_negative | {
            point for point in empty_points if labels[point] == -1
        }
        dead_net = len(fixed_positive) - len(fixed_negative)
        residual = abs(actual_empty_net - expected_empty_net)
        diagnostics: Dict[str, int | float | str | bool] = {
            "targetNetOwnership": float(expected_empty_net + dead_net),
            "actualNetOwnership": float(actual_empty_net + dead_net),
            "emptyExpectedNet": float(expected_empty_net),
            "emptyQuantizedNet": float(actual_empty_net),
            "absoluteResidual": float(residual),
            "emptyPointCount": len(empty_points),
            "spatialEdgeCount": edge_count,
            "solverStatus": solver_status.value,
            "optimal": solver_status is OwnershipSolverStatus.OPTIMAL,
            "objective": float(objective_value),
            "initialHeuristicObjective": float(initial_objective),
            "elapsedWallNs": elapsed_wall_ns,
            "attemptId": attempt_id,
            "solverIdentityFingerprint": solver_identity.fingerprint,
            "optimizerIncumbentAvailable": optimizer_labels is not None,
        }
        if optimizer_objective is not None:
            diagnostics["optimizerObjective"] = float(optimizer_objective)
        if lower_bound is not None:
            diagnostics["lowerBound"] = float(lower_bound)
        if absolute_gap is not None:
            diagnostics["absoluteOptimalityGap"] = float(absolute_gap)
        if backend_reported_gap is not None:
            diagnostics["backendReportedMipGap"] = float(backend_reported_gap)
        if backend_status is not None:
            diagnostics["backendStatus"] = backend_status

        return OwnershipRegions(
            positive=tuple(sorted(positive)),
            negative=tuple(sorted(negative)),
            policy_id=self.policy_id,
            diagnostics=diagnostics,
            solver_evidence=optimization_record,
        )
