# SPDX-License-Identifier: MIT
"""Ownership-specific optimization evidence contracts.

These values describe ternary board labelings and their optimization evidence.
They deliberately do not inherit the stencil-cover incumbent or witness types.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TypeAlias

from .stencil_solver_api import SolverBudget, SolverIdentity


PointLabel: TypeAlias = tuple[int, int]


class OwnershipSolverStatus(str, Enum):
    """What the bounded optimizer established about the returned labeling."""

    OPTIMAL = "optimal"
    FEASIBLE = "feasible"
    FEASIBLE_SUPPORTED = "feasible_supported"
    FEASIBLE_REPAIRED = "feasible_repaired"
    SEED_ONLY = "seed_only"
    ERROR = "error"
    NOT_REQUIRED = "not_required"


@dataclass(frozen=True)
class OwnershipLabeling:
    """A labeling of eligible flat board points; living stones are absent."""

    point_labels: tuple[PointLabel, ...]

    def __post_init__(self) -> None:
        points: list[int] = []
        for item in self.point_labels:
            if not isinstance(item, tuple) or len(item) != 2:
                raise TypeError("ownership point labels must be (point, label) pairs")
            point, label = item
            if isinstance(point, bool) or not isinstance(point, int) or point < 0:
                raise ValueError("ownership labeling points must be non-negative integers")
            if isinstance(label, bool) or not isinstance(label, int) or label not in (-1, 0, 1):
                raise ValueError("ownership labels must be -1, 0, or +1")
            points.append(point)
        if tuple(sorted(set(points))) != tuple(points):
            raise ValueError("ownership labeling points must be unique and sorted")


@dataclass(frozen=True)
class OwnershipOptimizationRecord:
    """One ownership-policy optimizer invocation and its selected witness."""

    attempt_id: str
    started_at_utc: str
    elapsed_wall_ns: int
    budget: SolverBudget
    solver_identity: SolverIdentity
    status: OwnershipSolverStatus
    initial_heuristic: OwnershipLabeling | None
    initial_objective: float | None
    optimizer_incumbent: OwnershipLabeling | None
    optimizer_objective: float | None
    selected_labeling: OwnershipLabeling
    selected_objective: float
    lower_bound: float | None
    absolute_gap: float | None
    backend_reported_gap: float | None
    backend_status: str | None
    error: str | None

    def __post_init__(self) -> None:
        try:
            parsed_id = str(uuid.UUID(self.attempt_id))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("ownership attempt_id must be a canonical UUID") from exc
        if parsed_id != self.attempt_id:
            raise ValueError("ownership attempt_id must be lowercase canonical UUID")
        if not isinstance(self.started_at_utc, str) or not self.started_at_utc.endswith("Z"):
            raise ValueError("ownership started_at_utc must be an RFC 3339 UTC timestamp")
        try:
            timestamp = datetime.fromisoformat(self.started_at_utc[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError("ownership started_at_utc is malformed") from exc
        if timestamp.utcoffset() is None:
            raise ValueError("ownership started_at_utc must be timezone-aware")
        if (
            isinstance(self.elapsed_wall_ns, bool)
            or not isinstance(self.elapsed_wall_ns, int)
            or self.elapsed_wall_ns < 0
        ):
            raise ValueError("ownership elapsed_wall_ns must be a non-negative integer")
        if not isinstance(self.budget, SolverBudget):
            raise TypeError("ownership budget must be SolverBudget")
        if not isinstance(self.solver_identity, SolverIdentity):
            raise TypeError("ownership solver_identity must be SolverIdentity")
        if not isinstance(self.status, OwnershipSolverStatus):
            raise TypeError("ownership status must be OwnershipSolverStatus")
        if not isinstance(self.selected_labeling, OwnershipLabeling):
            raise TypeError("selected_labeling must be OwnershipLabeling")
        if not math.isfinite(float(self.selected_objective)):
            raise ValueError("selected objective must be finite")

        selected_points = tuple(point for point, _label in self.selected_labeling.point_labels)
        for name, labeling in (
            ("initial_heuristic", self.initial_heuristic),
            ("optimizer_incumbent", self.optimizer_incumbent),
        ):
            if labeling is not None:
                if not isinstance(labeling, OwnershipLabeling):
                    raise TypeError(f"{name} must be OwnershipLabeling or None")
                points = tuple(point for point, _label in labeling.point_labels)
                if points != selected_points:
                    raise ValueError(
                        f"{name} and selected labeling must cover the same eligible points"
                    )
        if (self.initial_heuristic is None) != (self.initial_objective is None):
            raise ValueError("initial heuristic and objective must be present together")
        if self.initial_objective is not None and not math.isfinite(float(self.initial_objective)):
            raise ValueError("initial heuristic objective must be finite")
        if (self.optimizer_incumbent is None) != (self.optimizer_objective is None):
            raise ValueError("optimizer incumbent and objective must be present together")
        if self.optimizer_objective is not None and not math.isfinite(float(self.optimizer_objective)):
            raise ValueError("optimizer objective must be finite")
        if self.lower_bound is not None and not math.isfinite(float(self.lower_bound)):
            raise ValueError("ownership lower bound must be finite")
        if self.absolute_gap is not None and (
            not math.isfinite(float(self.absolute_gap)) or self.absolute_gap < 0
        ):
            raise ValueError("ownership absolute gap must be finite and non-negative")
        if self.backend_reported_gap is not None and (
            not math.isfinite(float(self.backend_reported_gap))
            or self.backend_reported_gap < 0
        ):
            raise ValueError("backend-reported gap must be finite and non-negative")
        if self.optimizer_objective is not None and self.lower_bound is not None:
            if self.lower_bound > self.optimizer_objective + 1e-7:
                raise ValueError("ownership lower bound exceeds optimizer objective")
            if self.absolute_gap is None:
                raise ValueError("a reported lower bound requires its absolute gap")
            expected_gap = max(0.0, self.optimizer_objective - self.lower_bound)
            if abs(self.absolute_gap - expected_gap) > 1e-6:
                raise ValueError("ownership absolute gap disagrees with objective and bound")
        elif self.absolute_gap is not None:
            raise ValueError("ownership absolute gap requires incumbent and lower bound")

        optimizer_statuses = (
            OwnershipSolverStatus.OPTIMAL,
            OwnershipSolverStatus.FEASIBLE,
            OwnershipSolverStatus.FEASIBLE_SUPPORTED,
        )
        if self.status in optimizer_statuses:
            if self.optimizer_incumbent is None:
                raise ValueError("optimizer status requires a validated optimizer incumbent")
            if self.error is not None:
                raise ValueError("successful optimizer status cannot carry an optimizer error")
        elif self.status is OwnershipSolverStatus.SEED_ONLY:
            if self.initial_heuristic is None or self.optimizer_incumbent is not None:
                raise ValueError("seed_only requires a seed and no optimizer incumbent")
        elif self.status is OwnershipSolverStatus.FEASIBLE_REPAIRED:
            if self.optimizer_incumbent is not None:
                raise ValueError("feasible_repaired cannot claim a solver incumbent")
        elif self.status is OwnershipSolverStatus.NOT_REQUIRED:
            if self.optimizer_incumbent is not None:
                raise ValueError("not_required cannot claim a solver incumbent")
        elif self.status is OwnershipSolverStatus.ERROR:
            if not isinstance(self.error, str) or not self.error:
                raise ValueError("error status requires a diagnostic error message")
        if self.backend_status is not None and not self.backend_status:
            raise ValueError("backend_status must be non-empty when reported")
