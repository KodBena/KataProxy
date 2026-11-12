"""Default HiGHS adapter for the solver-neutral stencil-cover port.

Import this module only at the solver composition boundary. Optional solver
packages belong in their own adapter modules and are not imported here.
"""
from __future__ import annotations

import hashlib
import json
import math
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Mapping

import highspy
import numpy as np

from transformers.stencil_solver_api import (
    BackendStatusReported, CanonicalProblem, Incumbent, LowerBoundUnavailable,
    NativeEffortReported, NativeEffortUnavailable, NoIncumbent, NoIncumbentReason,
    NoLowerBoundReason, NoNativeEffortReason, NoNativeEffortLimit, NoSolverFailure,
    OutcomeStatus, PlacementWitness, ProvenLowerBound, SolverBackend, SolverBudget,
    SolverIdentity, SolverOutcome,
)

ADAPTER_VERSION = "kataproxy-highs-adapter-v1"

try:
    _HIGHS_PACKAGE_VERSION = version("highspy")
except PackageNotFoundError:
    _HIGHS_PACKAGE_VERSION = str(getattr(highspy, "__version__", "unknown"))


def _candidate_placements(
    problem: CanonicalProblem,
) -> list[tuple[str, tuple[tuple[int, int], ...]]]:
    """Enumerate family placements plus the guaranteed singleton columns."""
    allowed = set(problem.occupied_cells)
    found: dict[tuple[tuple[int, int], ...], str] = {}
    for name, cells in problem.stencil_family:
        stencil_height = max(y for y, _x in cells) + 1
        stencil_width = max(x for _y, x in cells) + 1
        if stencil_width > problem.width or stencil_height > problem.height:
            continue
        for y0 in range(problem.height - stencil_height + 1):
            for x0 in range(problem.width - stencil_width + 1):
                placement = tuple(sorted((y0 + dy, x0 + dx) for dy, dx in cells))
                if all(cell in allowed for cell in placement):
                    found.setdefault(placement, name)
    for cell in problem.occupied_cells:
        found.setdefault((cell,), "1x1")
    return [(name, cells) for cells, name in found.items()]


class HiGHSBackend(SolverBackend):
    """HiGHS set-partitioning MIP backend; honors the requested wall-time limit."""

    def __init__(self, configuration: Mapping[str, Any] | None = None) -> None:
        config = dict(configuration or {})
        unknown = set(config) - {"log_to_console"}
        if unknown:
            raise ValueError(f"unsupported HiGHS configuration keys: {sorted(unknown)}")
        log_to_console = config.get("log_to_console", False)
        if not isinstance(log_to_console, bool):
            raise TypeError("HiGHS log_to_console must be bool")
        self._log_to_console = log_to_console
        canonical_config = json.dumps(
            {"log_to_console": log_to_console, "model": "set_partitioning_integer_v1"},
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
        )
        self._identity = SolverIdentity(
            backend_name="highs",
            backend_version=_HIGHS_PACKAGE_VERSION,
            adapter_version=ADAPTER_VERSION,
            configuration_json=canonical_config,
            configuration_fingerprint=hashlib.sha256(canonical_config.encode("utf-8")).hexdigest(),
        )

    @property
    def identity(self) -> SolverIdentity:
        return self._identity

    def solve(self, problem: CanonicalProblem, budget: SolverBudget) -> SolverOutcome:
        if not isinstance(budget.native_effort_limit, NoNativeEffortLimit):
            raise ValueError("HiGHS adapter does not accept a native effort limit")
        candidates = _candidate_placements(problem)
        point_to_columns: dict[tuple[int, int], list[int]] = {
            cell: [] for cell in problem.occupied_cells
        }
        for column, (_name, cells) in enumerate(candidates):
            for cell in cells:
                point_to_columns[cell].append(column)
        if any(not columns for columns in point_to_columns.values()):
            raise RuntimeError("stencil family failed to provide singleton feasibility")

        highs = highspy.Highs()
        highs.setOptionValue("log_to_console", self._log_to_console)
        highs.setOptionValue("time_limit", budget.wall_time_limit_ms / 1000.0)
        variable_count = len(candidates)
        highs.addVars(
            variable_count, np.zeros(variable_count, dtype=np.float64),
            np.ones(variable_count, dtype=np.float64),
        )
        indices = np.arange(variable_count, dtype=np.int32)
        highs.changeColsCost(variable_count, indices, np.ones(variable_count, dtype=np.float64))
        integrality = np.full(variable_count, int(highspy.HighsVarType.kInteger), dtype=np.uint8)
        highs.changeColsIntegrality(variable_count, indices, integrality)
        for cell in problem.occupied_cells:
            columns = np.asarray(point_to_columns[cell], dtype=np.int32)
            highs.addRow(
                1.0, 1.0, int(len(columns)), columns,
                np.ones(len(columns), dtype=np.float64),
            )
        highs.run()

        model_status = highs.getModelStatus()
        backend_status = BackendStatusReported(str(model_status))
        info = highs.getInfo()
        raw_nodes = getattr(info, "mip_node_count", None)
        try:
            node_count = float(raw_nodes) if raw_nodes is not None else float("nan")
        except (TypeError, ValueError, OverflowError):
            node_count = float("nan")
        if math.isfinite(node_count) and node_count >= 0:
            native_value: int | float = (
                int(node_count) if node_count.is_integer() else node_count
            )
            native_effort = NativeEffortReported(native_value, "mip_nodes")
        else:
            native_effort = NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT
            )

        metrics: dict[str, Any] = {
            "candidate_placement_count": variable_count,
            "constraint_count": len(problem.occupied_cells),
        }
        raw_bound = getattr(info, "mip_dual_bound", None)
        lower_bound = LowerBoundUnavailable(NoLowerBoundReason.BACKEND_DID_NOT_REPORT_BOUND)
        try:
            dual_bound = float(raw_bound) if raw_bound is not None else float("nan")
        except (TypeError, ValueError, OverflowError):
            dual_bound = float("nan")
        if math.isfinite(dual_bound):
            metrics["mip_dual_bound"] = dual_bound
            # The integer objective makes floor(bound) a conservative integer LB.
            conservative_integer_bound = math.floor(dual_bound)
            if conservative_integer_bound >= 0:
                lower_bound = ProvenLowerBound(conservative_integer_bound)

        solution = highs.getSolution()
        values = getattr(solution, "col_value", None)
        chosen = (
            [i for i, value in enumerate(values) if float(value) > 0.5]
            if values is not None and len(values) == variable_count else []
        )
        coverage = {cell: 0 for cell in problem.occupied_cells}
        for column in chosen:
            for cell in candidates[column][1]:
                coverage[cell] += 1
        if not chosen or not all(count == 1 for count in coverage.values()):
            return SolverOutcome(
                status=OutcomeStatus.NO_INCUMBENT,
                incumbent=NoIncumbent(NoIncumbentReason.BACKEND_RETURNED_NO_SOLUTION),
                lower_bound=lower_bound,
                native_effort=native_effort,
                backend_status=backend_status,
                backend_metrics_json=json.dumps(metrics, sort_keys=True, separators=(",", ":")),
                error=NoSolverFailure(),
            )

        selected = tuple((candidates[i][0], candidates[i][1]) for i in chosen)
        tile_count = len(selected)
        optimal = model_status == highspy.HighsModelStatus.kOptimal
        if optimal:
            lower_bound = ProvenLowerBound(tile_count)
        elif isinstance(lower_bound, ProvenLowerBound) and lower_bound.value > tile_count:
            lower_bound = LowerBoundUnavailable(NoLowerBoundReason.BACKEND_DID_NOT_REPORT_BOUND)
        return SolverOutcome(
            status=OutcomeStatus.OPTIMAL if optimal else OutcomeStatus.FEASIBLE,
            incumbent=Incumbent(tile_count, PlacementWitness(selected)),
            lower_bound=lower_bound,
            native_effort=native_effort,
            backend_status=backend_status,
            backend_metrics_json=json.dumps(metrics, sort_keys=True, separators=(",", ":")),
            error=NoSolverFailure(),
        )
