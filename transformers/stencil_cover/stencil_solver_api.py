"""Solver-neutral contracts for stencil-cover backends.

This module defines data shapes only. It deliberately contains no solver imports,
registry, factory, cache, transport, or persistence implementation.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, TypeAlias


# This identifies the current key bytes; it is carried beside the bytes, not
# prepended to them, so freezing the contract does not change existing keys.
STENCIL_CANONICALIZATION_VERSION = "kata-proxy-stencil-d4-lsb-v1"

Cell: TypeAlias = tuple[int, int]
Placement: TypeAlias = tuple[str, tuple[Cell, ...]]

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class CanonicalProblem:
    """A component in its canonical, solver-facing coordinate frame."""

    width: int
    height: int
    occupied_cells: tuple[Cell, ...]
    stencil_family: tuple[tuple[str, tuple[Cell, ...]], ...]
    family_fingerprint: str
    canonicalization_version: str

    def __post_init__(self) -> None:
        for name, value in (("width", self.width), ("height", self.height)):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 255:
                raise ValueError(f"{name} must be an integer in [1, 255]")
        if not self.occupied_cells:
            raise ValueError("canonical problem must contain at least one cell")
        if tuple(sorted(set(self.occupied_cells))) != self.occupied_cells:
            raise ValueError("occupied_cells must be unique and sorted by (y, x)")
        for y, x in self.occupied_cells:
            if (
                isinstance(y, bool) or not isinstance(y, int)
                or isinstance(x, bool) or not isinstance(x, int)
                or not 0 <= y < self.height or not 0 <= x < self.width
            ):
                raise ValueError("occupied cell is outside the canonical dimensions")
        if not self.stencil_family:
            raise ValueError("stencil_family must not be empty")
        names = tuple(name for name, _cells in self.stencil_family)
        if (
            any(not isinstance(name, str) or not name for name in names)
            or len(set(names)) != len(names)
        ):
            raise ValueError("stencil names must be non-empty and unique")
        for _name, cells in self.stencil_family:
            if not cells:
                raise ValueError("stencil geometry must not be empty")
            for y, x in cells:
                if (
                    isinstance(y, bool) or not isinstance(y, int) or y < 0
                    or isinstance(x, bool) or not isinstance(x, int) or x < 0
                ):
                    raise ValueError("stencil cells must be non-negative integer coordinates")
        if not isinstance(self.family_fingerprint, str) or not _SHA256_RE.fullmatch(
            self.family_fingerprint
        ):
            raise ValueError("family_fingerprint must be a lowercase SHA-256 hex digest")
        if not self.canonicalization_version:
            raise ValueError("canonicalization_version must not be empty")

    @property
    def cell_count(self) -> int:
        return len(self.occupied_cells)


class NoNativeEffortLimitReason(str, Enum):
    NOT_REQUESTED = "not_requested"


@dataclass(frozen=True)
class NativeEffortLimit:
    value: int | float
    unit: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.value, bool)
            or not isinstance(self.value, (int, float))
            or not math.isfinite(float(self.value))
            or self.value <= 0
        ):
            raise ValueError("native effort limit must be a positive finite number")
        if not self.unit:
            raise ValueError("native effort unit must not be empty")


@dataclass(frozen=True)
class NoNativeEffortLimit:
    reason: NoNativeEffortLimitReason = NoNativeEffortLimitReason.NOT_REQUESTED


NativeEffortBudget: TypeAlias = NativeEffortLimit | NoNativeEffortLimit


@dataclass(frozen=True)
class SolverBudget:
    """Requested wall-clock bound plus a separately-unitized native limit."""

    wall_time_limit_ms: int
    native_effort_limit: NativeEffortBudget
    policy_id: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.wall_time_limit_ms, bool)
            or not isinstance(self.wall_time_limit_ms, int)
            or self.wall_time_limit_ms <= 0
        ):
            raise ValueError("wall_time_limit_ms must be a positive integer")
        if not isinstance(self.native_effort_limit, (NativeEffortLimit, NoNativeEffortLimit)):
            raise TypeError("native_effort_limit must carry a value or a typed not-requested reason")
        if not self.policy_id:
            raise ValueError("policy_id must not be empty")


@dataclass(frozen=True)
class SolverIdentity:
    """Backend implementation/configuration identity; excludes per-call budget."""

    backend_name: str
    backend_version: str
    adapter_version: str
    configuration_json: str
    configuration_fingerprint: str

    def __post_init__(self) -> None:
        for name, value in (
            ("backend_name", self.backend_name),
            ("backend_version", self.backend_version),
            ("adapter_version", self.adapter_version),
        ):
            if not value:
                raise ValueError(f"{name} must not be empty")
        try:
            config = json.loads(self.configuration_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("configuration_json must contain valid JSON") from exc
        if not isinstance(config, dict):
            raise ValueError("solver configuration must be a JSON object")
        canonical = json.dumps(
            config, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        )
        if canonical != self.configuration_json:
            raise ValueError("configuration_json must use canonical compact JSON")
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if self.configuration_fingerprint != expected:
            raise ValueError("configuration_fingerprint does not match configuration_json")

    @property
    def fingerprint(self) -> str:
        identity = {
            "adapter_version": self.adapter_version,
            "backend_name": self.backend_name,
            "backend_version": self.backend_version,
            "configuration_fingerprint": self.configuration_fingerprint,
        }
        encoded = json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class OutcomeStatus(str, Enum):
    OPTIMAL = "optimal"
    FEASIBLE = "feasible"
    NO_INCUMBENT = "no_incumbent"
    ERROR = "error"


class NoIncumbentReason(str, Enum):
    BUDGET_EXHAUSTED = "budget_exhausted"
    INFEASIBLE_PROVEN = "infeasible_proven"
    BACKEND_RETURNED_NO_SOLUTION = "backend_returned_no_solution"
    INVOCATION_FAILED = "invocation_failed"


class NoPlacementWitnessReason(str, Enum):
    BACKEND_DID_NOT_RETURN_PLACEMENTS = "backend_did_not_return_placements"


@dataclass(frozen=True)
class PlacementWitness:
    placements: tuple[Placement, ...]

    def __post_init__(self) -> None:
        if not self.placements:
            raise ValueError("a placement witness must contain at least one placement")
        for name, cells in self.placements:
            if not name or not cells:
                raise ValueError("each placement requires a stencil name and occupied cells")


@dataclass(frozen=True)
class PlacementWitnessUnavailable:
    reason: NoPlacementWitnessReason = (
        NoPlacementWitnessReason.BACKEND_DID_NOT_RETURN_PLACEMENTS
    )


@dataclass(frozen=True)
class Incumbent:
    tile_count: int
    placement_witness: PlacementWitness | PlacementWitnessUnavailable

    def __post_init__(self) -> None:
        if (
            isinstance(self.tile_count, bool)
            or not isinstance(self.tile_count, int)
            or self.tile_count <= 0
        ):
            raise ValueError("incumbent tile_count must be a positive integer")
        if isinstance(self.placement_witness, PlacementWitness):
            if len(self.placement_witness.placements) != self.tile_count:
                raise ValueError("placement count must equal incumbent tile_count")


@dataclass(frozen=True)
class NoIncumbent:
    reason: NoIncumbentReason


@dataclass(frozen=True)
class ProvenLowerBound:
    value: int

    def __post_init__(self) -> None:
        if isinstance(self.value, bool) or not isinstance(self.value, int) or self.value < 0:
            raise ValueError("lower bound must be a non-negative integer")


class NoLowerBoundReason(str, Enum):
    BACKEND_DID_NOT_REPORT_BOUND = "backend_did_not_report_bound"


@dataclass(frozen=True)
class LowerBoundUnavailable:
    reason: NoLowerBoundReason = NoLowerBoundReason.BACKEND_DID_NOT_REPORT_BOUND


LowerBound: TypeAlias = ProvenLowerBound | LowerBoundUnavailable


@dataclass(frozen=True)
class NativeEffortReported:
    value: int | float
    unit: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.value, bool)
            or not isinstance(self.value, (int, float))
            or not math.isfinite(float(self.value))
            or self.value < 0
        ):
            raise ValueError("native effort must be a non-negative finite number")
        if not self.unit:
            raise ValueError("native effort unit must not be empty")


class NoNativeEffortReason(str, Enum):
    BACKEND_DOES_NOT_EXPOSE_EFFORT = "backend_does_not_expose_effort"
    BACKEND_DID_NOT_REPORT_EFFORT = "backend_did_not_report_effort"


@dataclass(frozen=True)
class NativeEffortUnavailable:
    reason: NoNativeEffortReason


NativeEffort: TypeAlias = NativeEffortReported | NativeEffortUnavailable


@dataclass(frozen=True)
class BackendStatusReported:
    value: str

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("reported backend status must not be empty")


class NoBackendStatusReason(str, Enum):
    BACKEND_DID_NOT_REPORT_STATUS = "backend_did_not_report_status"


@dataclass(frozen=True)
class BackendStatusUnavailable:
    reason: NoBackendStatusReason = NoBackendStatusReason.BACKEND_DID_NOT_REPORT_STATUS


BackendStatus: TypeAlias = BackendStatusReported | BackendStatusUnavailable


@dataclass(frozen=True)
class SolverFailure:
    error_type: str
    message: str
    details_json: str = "{}"

    def __post_init__(self) -> None:
        if not self.error_type or not self.message:
            raise ValueError("solver failure requires error_type and message")
        try:
            details = json.loads(self.details_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("failure details must be valid JSON") from exc
        if not isinstance(details, dict):
            raise ValueError("failure details must be a JSON object")


@dataclass(frozen=True)
class NoSolverFailure:
    pass


SolverError: TypeAlias = SolverFailure | NoSolverFailure


@dataclass(frozen=True)
class SolverOutcome:
    """Normalized outcome. Only OPTIMAL carries a proof of optimality."""

    status: OutcomeStatus
    incumbent: Incumbent | NoIncumbent
    lower_bound: LowerBound
    native_effort: NativeEffort
    backend_status: BackendStatus
    backend_metrics_json: str
    error: SolverError

    def __post_init__(self) -> None:
        if not isinstance(self.status, OutcomeStatus):
            raise TypeError("status must be an OutcomeStatus")
        if not isinstance(self.incumbent, (Incumbent, NoIncumbent)):
            raise TypeError("incumbent must be explicitly present or absent")
        if not isinstance(self.lower_bound, (ProvenLowerBound, LowerBoundUnavailable)):
            raise TypeError("lower_bound must be explicitly reported or unavailable")
        if not isinstance(self.native_effort, (NativeEffortReported, NativeEffortUnavailable)):
            raise TypeError("native_effort must be explicitly reported or unavailable")
        if not isinstance(self.backend_status, (BackendStatusReported, BackendStatusUnavailable)):
            raise TypeError("backend_status must be explicitly reported or unavailable")
        if not isinstance(self.error, (SolverFailure, NoSolverFailure)):
            raise TypeError("error must be a failure or an explicit no-failure value")
        try:
            metrics = json.loads(self.backend_metrics_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("backend_metrics_json must be valid JSON") from exc
        if not isinstance(metrics, dict):
            raise ValueError("backend metrics must be a JSON object")

        if self.status is OutcomeStatus.OPTIMAL:
            if not isinstance(self.incumbent, Incumbent) or isinstance(self.error, SolverFailure):
                raise ValueError("optimal outcome requires an incumbent and no error")
            if not isinstance(self.lower_bound, ProvenLowerBound):
                raise ValueError("optimal outcome requires a proven lower bound")
            if self.lower_bound.value != self.incumbent.tile_count:
                raise ValueError("optimal outcome requires lower_bound == incumbent objective")
        elif self.status is OutcomeStatus.FEASIBLE:
            if not isinstance(self.incumbent, Incumbent) or isinstance(self.error, SolverFailure):
                raise ValueError("feasible outcome requires an incumbent and no error")
            if (
                isinstance(self.lower_bound, ProvenLowerBound)
                and self.lower_bound.value > self.incumbent.tile_count
            ):
                raise ValueError("feasible outcome lower bound exceeds incumbent objective")
        elif self.status is OutcomeStatus.NO_INCUMBENT:
            if not isinstance(self.incumbent, NoIncumbent) or isinstance(self.error, SolverFailure):
                raise ValueError("no_incumbent outcome requires no incumbent and no error")
        elif self.status is OutcomeStatus.ERROR:
            if not isinstance(self.incumbent, NoIncumbent) or not isinstance(self.error, SolverFailure):
                raise ValueError("error outcome requires a failure and no incumbent")

    @property
    def optimality_proven(self) -> bool:
        return self.status is OutcomeStatus.OPTIMAL


class SolverBackend(Protocol):
    """Port implemented by a selected backend; no transformer knowledge."""

    @property
    def identity(self) -> SolverIdentity: ...

    def solve(
        self, problem: CanonicalProblem, budget: SolverBudget,
    ) -> SolverOutcome: ...
