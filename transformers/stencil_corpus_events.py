"""Versioned, validated event contract for the stencil-cover corpus.

This module is deliberately transport- and storage-neutral. Producers create
immutable JSON-compatible envelopes here; Redis Streams and SQLite consumers
can then share exactly the same bytes and validation rules.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from transformers.stencil_solver_api import (
    BackendStatusReported,
    BackendStatusUnavailable,
    Incumbent,
    LowerBoundUnavailable,
    NativeEffortLimit,
    NativeEffortReported,
    NativeEffortUnavailable,
    NoBackendStatusReason,
    NoIncumbent,
    NoLowerBoundReason,
    NoNativeEffortLimit,
    NoNativeEffortReason,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverBudget,
    SolverFailure,
    SolverIdentity,
)
from transformers.stencil_solver_geometry import decode_component_shape_key

if TYPE_CHECKING:
    from transformers.stencil_solver_runtime import SolverExecution

CORPUS_EVENT_SCHEMA = "kataproxy.stencil-cover.corpus-event.v1"
PLACEMENT_PAYLOAD_CODEC = "kataproxy.stencil-cover.placements-json.v1"
EVENT_TYPES = frozenset(("instance_seen", "solver_attempt"))
PRODUCER_PATHS = frozenset(("foreground", "bulk"))
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_JSON = dict[str, Any]
ProducerPath = Literal["foreground", "bulk"]


class CorpusEventError(ValueError):
    """An event violates the frozen corpus wire contract."""


def utc_timestamp(value: datetime | None = None) -> str:
    """Return an RFC 3339 UTC timestamp with a terminal Z."""
    stamp = datetime.now(timezone.utc) if value is None else value
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("corpus timestamps must be timezone-aware")
    return stamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z",
    )


def _parse_utc_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise CorpusEventError(f"{field} must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise CorpusEventError(f"{field} is not a valid RFC 3339 timestamp") from exc
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise CorpusEventError(f"{field} must use UTC")
    return parsed


def instance_id_for(
    family_fingerprint: str,
    canonicalization_version: str,
    canonical_key: bytes,
) -> str:
    """Hash the durable identity tuple without including solver or budget data."""
    canonical = {
        "canonical_key_b64": base64.b64encode(canonical_key).decode("ascii"),
        "canonicalization_version": canonicalization_version,
        "family_fingerprint": family_fingerprint,
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_problem_key(problem: Any, canonical_key: bytes) -> None:
    if not isinstance(canonical_key, bytes):
        raise TypeError("canonical_key must be bytes")
    width, height, flat_cells = decode_component_shape_key(canonical_key)
    occupied = tuple((index // width, index % width) for index in flat_cells)
    if (width, height, occupied) != (
        problem.width, problem.height, problem.occupied_cells,
    ):
        raise ValueError("canonical_key does not identify the supplied canonical problem")


def _instance_document(problem: Any, canonical_key: bytes) -> _JSON:
    _validate_problem_key(problem, canonical_key)
    return {
        "family_fingerprint": problem.family_fingerprint,
        "canonicalization_version": problem.canonicalization_version,
        "canonical_key_b64": base64.b64encode(canonical_key).decode("ascii"),
        "width": problem.width,
        "height": problem.height,
        "cell_count": problem.cell_count,
    }


def make_instance_seen_event(
    problem: Any,
    canonical_key: bytes,
    producer_path: ProducerPath = "foreground",
    *,
    event_id: str | None = None,
    emitted_at_utc: datetime | None = None,
) -> _JSON:
    """Create one sighting event; callers invoke this before cache lookup."""
    if producer_path not in PRODUCER_PATHS:
        raise ValueError("producer_path must be foreground or bulk")
    identifier = str(uuid.uuid4()) if event_id is None else event_id
    event = {
        "schema": CORPUS_EVENT_SCHEMA,
        "event_id": identifier,
        "event_type": "instance_seen",
        "emitted_at_utc": utc_timestamp(emitted_at_utc),
        "producer_path": producer_path,
        "instance": _instance_document(problem, canonical_key),
    }
    validate_event(event)
    return event


def _native_effort_budget_document(budget: SolverBudget) -> _JSON:
    native = budget.native_effort_limit
    if isinstance(native, NativeEffortLimit):
        return {
            "disposition": "limited",
            "value": native.value,
            "unit": native.unit,
        }
    if isinstance(native, NoNativeEffortLimit):
        return {
            "disposition": "not_requested",
            "reason": native.reason.value,
        }
    raise TypeError("unknown native effort budget type")


def _native_effort_document(value: Any) -> _JSON:
    if isinstance(value, NativeEffortReported):
        return {"disposition": "reported", "value": value.value, "unit": value.unit}
    if isinstance(value, NativeEffortUnavailable):
        if value.reason is NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT:
            return {"disposition": "not_supported", "reason": value.reason.value}
        return {"disposition": "not_reported", "reason": value.reason.value}
    raise TypeError("unknown native effort outcome type")


def _placement_document(execution: SolverExecution) -> _JSON:
    disposition = execution.placements_disposition
    if disposition == "validated":
        raw = execution.reported_outcome
        if not isinstance(raw.incumbent, Incumbent) or not isinstance(
            raw.incumbent.placement_witness, PlacementWitness,
        ):
            raise ValueError("validated placements require a backend placement witness")
        payload = {
            "placements": [
                {
                    "stencil": name,
                    "cells": [[y, x] for y, x in cells],
                }
                for name, cells in raw.incumbent.placement_witness.placements
            ],
            "version": 1,
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        return {
            "disposition": "validated",
            "payload_b64": base64.b64encode(encoded).decode("ascii"),
            "codec_version": PLACEMENT_PAYLOAD_CODEC,
        }
    if disposition in ("not_returned", "rejected_invalid"):
        return {"disposition": disposition}
    raise ValueError("unknown placements disposition")


def make_solver_attempt_event(
    execution: SolverExecution,
    canonical_key: bytes,
    budget: SolverBudget,
    producer_path: ProducerPath,
    *,
    event_id: str | None = None,
    emitted_at_utc: datetime | None = None,
) -> _JSON:
    """Build an attempt event from one actual shared-runtime invocation."""
    if producer_path not in PRODUCER_PATHS:
        raise ValueError("producer_path must be foreground or bulk")
    identifier = str(uuid.uuid4()) if event_id is None else event_id
    identity: SolverIdentity = execution.identity
    raw_outcome = execution.reported_outcome
    outcome = execution.outcome

    if isinstance(raw_outcome.incumbent, Incumbent):
        incumbent: _JSON = {
            "disposition": "present",
            "tile_count": raw_outcome.incumbent.tile_count,
        }
    elif isinstance(raw_outcome.incumbent, NoIncumbent):
        incumbent = {"disposition": "not_returned"}
    else:
        raise TypeError("backend outcome has an unsupported incumbent type")

    if isinstance(raw_outcome.lower_bound, ProvenLowerBound):
        lower_bound: _JSON = {
            "disposition": "reported",
            "value": raw_outcome.lower_bound.value,
        }
    elif isinstance(raw_outcome.lower_bound, LowerBoundUnavailable):
        lower_bound = {
            "disposition": "not_reported",
            "reason": raw_outcome.lower_bound.reason.value,
        }
    else:
        raise TypeError("backend outcome has an unsupported lower-bound type")

    if isinstance(raw_outcome.backend_status, BackendStatusReported):
        backend_status: _JSON = {
            "disposition": "reported",
            "value": raw_outcome.backend_status.value,
        }
    elif isinstance(raw_outcome.backend_status, BackendStatusUnavailable):
        backend_status = {
            "disposition": "not_reported",
            "reason": raw_outcome.backend_status.reason.value,
        }
    else:
        raise TypeError("backend outcome has an unsupported backend-status type")

    metrics = json.loads(raw_outcome.backend_metrics_json)
    if not isinstance(metrics, dict):
        raise ValueError("backend metrics must be a JSON object")

    if isinstance(outcome.error, SolverFailure):
        error: _JSON = {
            "disposition": "present",
            "error_type": outcome.error.error_type,
            "message": outcome.error.message,
            "details": json.loads(outcome.error.details_json),
        }
    else:
        error = {"disposition": "none"}

    event = {
        "schema": CORPUS_EVENT_SCHEMA,
        "event_id": identifier,
        "event_type": "solver_attempt",
        "emitted_at_utc": utc_timestamp(emitted_at_utc),
        "producer_path": producer_path,
        "instance": _instance_document(execution.problem, canonical_key),
        "attempt_id": identifier,
        "solver_identity": {
            "fingerprint": identity.fingerprint,
            "backend_name": identity.backend_name,
            "backend_version": identity.backend_version,
            "adapter_version": identity.adapter_version,
            "configuration_json": identity.configuration_json,
            "configuration_fingerprint": identity.configuration_fingerprint,
        },
        "budget": {
            "wall_time_limit_ms": budget.wall_time_limit_ms,
            "policy_id": budget.policy_id,
            "native_effort_limit": _native_effort_budget_document(budget),
        },
        "started_at_utc": execution.started_at_utc,
        "elapsed_wall_ns": execution.elapsed_wall_ns,
        "outcome": {
            "status": outcome.status.value,
            "optimality_proven": outcome.optimality_proven,
            "incumbent": incumbent,
            "lower_bound": lower_bound,
            "native_effort": _native_effort_document(raw_outcome.native_effort),
            "backend_status": backend_status,
            "backend_metrics": metrics,
            "placements": _placement_document(execution),
            "error": error,
        },
        "fallback_disposition": (
            "singleton_fallback_used" if execution.fallback_used else "not_needed"
        ),
    }
    validate_event(event)
    return event


def _expect_object(value: Any, field: str) -> _JSON:
    if not isinstance(value, dict):
        raise CorpusEventError(f"{field} must be an object")
    return value


def _expect_exact_keys(
    value: _JSON, required: set[str], optional: set[str], field: str,
) -> None:
    missing = required - set(value)
    extra = set(value) - required - optional
    if missing:
        raise CorpusEventError(f"{field} is missing required fields: {sorted(missing)}")
    if extra:
        raise CorpusEventError(f"{field} contains unsupported fields: {sorted(extra)}")


def _expect_int(value: Any, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise CorpusEventError(f"{field} must be an integer >= {minimum}")
    return value


def _expect_finite_number(value: Any, field: str, *, positive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CorpusEventError(f"{field} must be numeric")
    if not math.isfinite(float(value)) or (positive and value <= 0) or (
        not positive and value < 0
    ):
        operator = "positive" if positive else "non-negative"
        raise CorpusEventError(f"{field} must be finite and {operator}")
    return float(value)


def _validate_instance(value: Any) -> tuple[int, int, tuple[tuple[int, int], ...]]:
    instance = _expect_object(value, "instance")
    _expect_exact_keys(
        instance,
        {
            "family_fingerprint", "canonicalization_version",
            "canonical_key_b64", "width", "height", "cell_count",
        },
        set(),
        "instance",
    )
    family_fingerprint = instance["family_fingerprint"]
    if not isinstance(family_fingerprint, str) or not _HEX_SHA256.fullmatch(
        family_fingerprint,
    ):
        raise CorpusEventError("instance.family_fingerprint must be lowercase SHA-256 hex")
    version = instance["canonicalization_version"]
    if not isinstance(version, str) or not version:
        raise CorpusEventError("instance.canonicalization_version must be non-empty")
    encoded = instance["canonical_key_b64"]
    if not isinstance(encoded, str):
        raise CorpusEventError("instance.canonical_key_b64 must be a string")
    try:
        key = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise CorpusEventError("instance canonical key is not valid Base64") from exc
    if base64.b64encode(key).decode("ascii") != encoded:
        raise CorpusEventError("instance canonical key Base64 must use padded canonical encoding")
    try:
        width, height, cells = decode_component_shape_key(key)
    except Exception as exc:
        raise CorpusEventError("instance canonical key is not a valid component key") from exc
    declared_width = _expect_int(instance["width"], "instance.width", 1)
    declared_height = _expect_int(instance["height"], "instance.height", 1)
    declared_count = _expect_int(instance["cell_count"], "instance.cell_count", 1)
    if (width, height, len(cells)) != (
        declared_width, declared_height, declared_count,
    ):
        raise CorpusEventError("instance dimensions or cell_count disagree with canonical key")
    occupied = tuple((index // width, index % width) for index in cells)
    return width, height, occupied


def _validate_budget(value: Any) -> None:
    budget = _expect_object(value, "budget")
    _expect_exact_keys(
        budget, {"wall_time_limit_ms", "policy_id", "native_effort_limit"},
        set(), "budget",
    )
    _expect_int(budget["wall_time_limit_ms"], "budget.wall_time_limit_ms", 1)
    if not isinstance(budget["policy_id"], str) or not budget["policy_id"]:
        raise CorpusEventError("budget.policy_id must be non-empty")
    native = _expect_object(budget["native_effort_limit"], "budget.native_effort_limit")
    disposition = native.get("disposition")
    if disposition == "limited":
        _expect_exact_keys(native, {"disposition", "value", "unit"}, set(), "native effort limit")
        _expect_finite_number(native["value"], "native effort limit value", positive=True)
        if not isinstance(native["unit"], str) or not native["unit"]:
            raise CorpusEventError("native effort limit unit must be non-empty")
    elif disposition == "not_requested":
        _expect_exact_keys(native, {"disposition", "reason"}, set(), "native effort limit")
        if native["reason"] != "not_requested":
            raise CorpusEventError("unsupported native effort limit reason")
    else:
        raise CorpusEventError("unknown native effort limit disposition")


def _validate_identity(value: Any) -> None:
    identity = _expect_object(value, "solver_identity")
    _expect_exact_keys(
        identity,
        {
            "fingerprint", "backend_name", "backend_version", "adapter_version",
            "configuration_json", "configuration_fingerprint",
        },
        set(),
        "solver_identity",
    )
    try:
        canonical = SolverIdentity(
            backend_name=identity["backend_name"],
            backend_version=identity["backend_version"],
            adapter_version=identity["adapter_version"],
            configuration_json=identity["configuration_json"],
            configuration_fingerprint=identity["configuration_fingerprint"],
        )
    except (TypeError, ValueError) as exc:
        raise CorpusEventError("solver_identity is malformed") from exc
    if canonical.fingerprint != identity["fingerprint"]:
        raise CorpusEventError("solver_identity fingerprint does not match its fields")


def _validate_outcome(
    value: Any,
    expected_width: int,
    expected_height: int,
    expected_cells: tuple[tuple[int, int], ...],
) -> None:
    outcome = _expect_object(value, "outcome")
    _expect_exact_keys(
        outcome,
        {
            "status", "optimality_proven", "incumbent", "lower_bound",
            "native_effort", "backend_status", "backend_metrics", "placements", "error",
        },
        set(),
        "outcome",
    )
    status = outcome["status"]
    if not isinstance(status, str) or status not in {
        member.value for member in OutcomeStatus
    }:
        raise CorpusEventError("outcome.status is unsupported")
    optimality_proven = outcome["optimality_proven"]
    if not isinstance(optimality_proven, bool) or optimality_proven != (
        status == OutcomeStatus.OPTIMAL.value
    ):
        raise CorpusEventError("outcome.optimality_proven disagrees with status")

    incumbent = _expect_object(outcome["incumbent"], "outcome.incumbent")
    if incumbent.get("disposition") == "present":
        _expect_exact_keys(
            incumbent, {"disposition", "tile_count"}, set(), "outcome.incumbent",
        )
        tile_count = _expect_int(incumbent["tile_count"], "incumbent.tile_count", 1)
    elif incumbent.get("disposition") == "not_returned":
        _expect_exact_keys(incumbent, {"disposition"}, set(), "outcome.incumbent")
        tile_count = None
    else:
        raise CorpusEventError("unknown incumbent disposition")

    lower = _expect_object(outcome["lower_bound"], "outcome.lower_bound")
    if lower.get("disposition") == "reported":
        _expect_exact_keys(lower, {"disposition", "value"}, set(), "outcome.lower_bound")
        lower_value = _expect_int(lower["value"], "lower_bound.value")
    elif lower.get("disposition") == "not_reported":
        _expect_exact_keys(
            lower, {"disposition", "reason"}, set(), "outcome.lower_bound",
        )
        if lower["reason"] != NoLowerBoundReason.BACKEND_DID_NOT_REPORT_BOUND.value:
            raise CorpusEventError("unsupported lower-bound unavailability reason")
        lower_value = None
    else:
        raise CorpusEventError("unknown lower-bound disposition")

    if status == OutcomeStatus.OPTIMAL.value:
        if tile_count is None or lower_value != tile_count:
            raise CorpusEventError("optimal outcome requires incumbent and matching proven bound")
    if status == OutcomeStatus.FEASIBLE.value:
        if tile_count is None:
            raise CorpusEventError("feasible outcome requires an incumbent")
        if lower_value is not None and lower_value > tile_count:
            raise CorpusEventError("feasible lower bound exceeds incumbent objective")
    if status == OutcomeStatus.NO_INCUMBENT.value and tile_count is not None:
        raise CorpusEventError("no_incumbent outcome cannot contain an incumbent")

    effort = _expect_object(outcome["native_effort"], "outcome.native_effort")
    if effort.get("disposition") == "reported":
        _expect_exact_keys(effort, {"disposition", "value", "unit"}, set(), "native effort")
        _expect_finite_number(effort["value"], "native effort value", positive=False)
        if not isinstance(effort["unit"], str) or not effort["unit"]:
            raise CorpusEventError("native effort unit must be non-empty")
    elif effort.get("disposition") in ("not_supported", "not_reported"):
        _expect_exact_keys(effort, {"disposition", "reason"}, set(), "native effort")
        allowed = {
            NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT.value,
            NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT.value,
        }
        if effort["reason"] not in allowed:
            raise CorpusEventError("unsupported native effort unavailability reason")
        expected = (
            "not_supported"
            if effort["reason"] == NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT.value
            else "not_reported"
        )
        if effort["disposition"] != expected:
            raise CorpusEventError("native effort disposition disagrees with reason")
    else:
        raise CorpusEventError("unknown native effort disposition")

    backend = _expect_object(outcome["backend_status"], "outcome.backend_status")
    if backend.get("disposition") == "reported":
        _expect_exact_keys(
            backend, {"disposition", "value"}, set(), "outcome.backend_status",
        )
        if not isinstance(backend["value"], str) or not backend["value"]:
            raise CorpusEventError("reported backend status must be non-empty")
    elif backend.get("disposition") == "not_reported":
        _expect_exact_keys(
            backend, {"disposition", "reason"}, set(), "outcome.backend_status",
        )
        if backend["reason"] != NoBackendStatusReason.BACKEND_DID_NOT_REPORT_STATUS.value:
            raise CorpusEventError("unsupported backend-status unavailability reason")
    else:
        raise CorpusEventError("unknown backend-status disposition")

    metrics = _expect_object(outcome["backend_metrics"], "outcome.backend_metrics")
    try:
        json.dumps(metrics, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise CorpusEventError("backend metrics must be finite JSON data") from exc

    placements = _expect_object(outcome["placements"], "outcome.placements")
    disposition = placements.get("disposition")
    if disposition == "validated":
        if status not in (OutcomeStatus.OPTIMAL.value, OutcomeStatus.FEASIBLE.value):
            raise CorpusEventError("validated placements require an incumbent outcome")
        _expect_exact_keys(
            placements, {"disposition", "payload_b64", "codec_version"},
            set(), "outcome.placements",
        )
        if not isinstance(placements["codec_version"], str) or (
            placements["codec_version"] != PLACEMENT_PAYLOAD_CODEC
        ):
            raise CorpusEventError("unsupported placement payload codec")
        encoded = placements["payload_b64"]
        if not isinstance(encoded, str):
            raise CorpusEventError("placement payload must be Base64 text")
        try:
            raw = base64.b64decode(encoded, validate=True)
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorpusEventError("placement payload is not valid Base64 JSON") from exc
        if base64.b64encode(raw).decode("ascii") != encoded:
            raise CorpusEventError("placement payload Base64 must be canonical")
        if (
            not isinstance(payload, dict)
            or isinstance(payload.get("version"), bool)
            or payload.get("version") != 1
            or not isinstance(payload.get("placements"), list)
        ):
            raise CorpusEventError("placement payload has an unsupported structure")
        if len(payload["placements"]) == 0:
            raise CorpusEventError("validated placement payload must not be empty")
        if tile_count is None or len(payload["placements"]) != tile_count:
            raise CorpusEventError("placement count disagrees with incumbent tile_count")
        covered: set[tuple[int, int]] = set()
        for placement in payload["placements"]:
            item = _expect_object(placement, "placement")
            _expect_exact_keys(item, {"stencil", "cells"}, set(), "placement")
            if not isinstance(item["stencil"], str) or not item["stencil"]:
                raise CorpusEventError("placement stencil name must be non-empty")
            if not isinstance(item["cells"], list) or not item["cells"]:
                raise CorpusEventError("placement cells must be a non-empty list")
            placement_cells: set[tuple[int, int]] = set()
            for cell in item["cells"]:
                if (
                    not isinstance(cell, list) or len(cell) != 2
                    or any(isinstance(coord, bool) or not isinstance(coord, int) or coord < 0
                           for coord in cell)
                ):
                    raise CorpusEventError("placement cells must be non-negative [y, x] pairs")
                y, x = cell
                if y >= expected_height or x >= expected_width:
                    raise CorpusEventError("placement cell lies outside the canonical component")
                coordinate = (y, x)
                if coordinate in placement_cells or coordinate in covered:
                    raise CorpusEventError("validated placements must be pairwise disjoint")
                placement_cells.add(coordinate)
            covered.update(placement_cells)
        if covered != set(expected_cells):
            raise CorpusEventError("validated placements do not cover the canonical component")
        if status not in (OutcomeStatus.OPTIMAL.value, OutcomeStatus.FEASIBLE.value):
            raise CorpusEventError("validated placements require an incumbent outcome")
    elif disposition in ("not_returned", "rejected_invalid"):
        _expect_exact_keys(placements, {"disposition"}, set(), "outcome.placements")
        if disposition == "rejected_invalid" and status != OutcomeStatus.ERROR.value:
            raise CorpusEventError("rejected placements require an error outcome")
        if (
            status == OutcomeStatus.ERROR.value
            and tile_count is not None
            and disposition != "rejected_invalid"
        ):
            raise CorpusEventError("an invalid incumbent witness must be marked rejected_invalid")
        if disposition == "rejected_invalid" and tile_count is None:
            raise CorpusEventError("rejected placements require a reported incumbent")
    else:
        raise CorpusEventError("unknown placements disposition")

    error = _expect_object(outcome["error"], "outcome.error")
    if error.get("disposition") == "present":
        _expect_exact_keys(
            error, {"disposition", "error_type", "message", "details"},
            set(), "outcome.error",
        )
        if not isinstance(error["error_type"], str) or not error["error_type"]:
            raise CorpusEventError("error_type must be non-empty")
        if not isinstance(error["message"], str) or not error["message"]:
            raise CorpusEventError("error message must be non-empty")
        _expect_object(error["details"], "outcome.error.details")
        if status != OutcomeStatus.ERROR.value:
            raise CorpusEventError("error payload requires outcome status error")
    elif error.get("disposition") == "none":
        _expect_exact_keys(error, {"disposition"}, set(), "outcome.error")
        if status == OutcomeStatus.ERROR.value:
            raise CorpusEventError("error outcome requires an error payload")
    else:
        raise CorpusEventError("unknown error disposition")


def validate_event(value: Any) -> None:
    """Validate all fields and status/disposition companion invariants."""
    event = _expect_object(value, "event")
    event_type = event.get("event_type")
    if not isinstance(event_type, str):
        raise CorpusEventError("event_type must be a string")
    common = {
        "schema", "event_id", "event_type", "emitted_at_utc",
        "producer_path", "instance",
    }
    attempt = {
        "attempt_id", "solver_identity", "budget", "started_at_utc",
        "elapsed_wall_ns", "outcome", "fallback_disposition",
    }
    _expect_exact_keys(event, common if event_type == "instance_seen" else common | attempt, set(),
                       "event")
    if event.get("schema") != CORPUS_EVENT_SCHEMA:
        raise CorpusEventError("unsupported corpus event schema")
    if event_type not in EVENT_TYPES:
        raise CorpusEventError("unsupported corpus event type")
    identifier = event.get("event_id")
    try:
        canonical_uuid = str(uuid.UUID(identifier))
    except (ValueError, TypeError, AttributeError) as exc:
        raise CorpusEventError("event_id must be a canonical lowercase UUID") from exc
    if canonical_uuid != identifier:
        raise CorpusEventError("event_id must be a canonical lowercase UUID")
    _parse_utc_timestamp(event.get("emitted_at_utc"), "emitted_at_utc")
    producer_path = event.get("producer_path")
    if not isinstance(producer_path, str) or producer_path not in PRODUCER_PATHS:
        raise CorpusEventError("producer_path must be foreground or bulk")
    width, height, occupied = _validate_instance(event.get("instance"))

    if event_type == "instance_seen":
        return

    if event.get("attempt_id") != identifier:
        raise CorpusEventError("attempt_id must equal event_id")
    _validate_identity(event.get("solver_identity"))
    _validate_budget(event.get("budget"))
    _parse_utc_timestamp(event.get("started_at_utc"), "started_at_utc")
    _expect_int(event.get("elapsed_wall_ns"), "elapsed_wall_ns")
    _validate_outcome(event.get("outcome"), width, height, occupied)
    fallback = event.get("fallback_disposition")
    if not isinstance(fallback, str) or fallback not in (
        "not_needed", "singleton_fallback_used",
    ):
        raise CorpusEventError("unsupported fallback disposition")
    placements = event["outcome"]["placements"]["disposition"]
    if fallback == "not_needed" and placements != "validated":
        raise CorpusEventError("a non-fallback attempt must contain validated placements")
    if fallback == "singleton_fallback_used" and placements == "validated":
        raise CorpusEventError("a singleton fallback cannot claim solver placements")


def serialize_event(event: _JSON) -> bytes:
    """Validate and return canonical UTF-8 JSON bytes."""
    validate_event(event)
    try:
        return json.dumps(
            event, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CorpusEventError("event is not canonical JSON data") from exc


def parse_event(raw: bytes | str) -> _JSON:
    """Decode and validate event bytes received from a stream or spool."""
    try:
        if isinstance(raw, bytes):
            decoded = raw.decode("utf-8")
        elif isinstance(raw, str):
            decoded = raw
        else:
            raise TypeError("event payload must be bytes or text")
        event = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise CorpusEventError("event payload is not valid UTF-8 JSON") from exc
    validate_event(event)
    if serialize_event(event) != decoded.encode("utf-8"):
        raise CorpusEventError("event payload must use canonical compact JSON")
    return event
