# SPDX-License-Identifier: MIT
"""Direct, append-only SQLite archive for ownership quantization evidence.

This store is independent of the stencil-placement cache/corpus. Each
connection attaches its own database file as the SQLite schema named
ownership; there is no Redis transport, spool, materializer, or solver worker.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import struct
import uuid
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, cast

from .stencil_ownership import OwnershipEvidence, OwnershipRegionPolicy, OwnershipRegions
from .stencil_ownership_solver_api import OwnershipLabeling, OwnershipOptimizationRecord
from .stencil_solver_api import NativeEffortLimit

OWNERSHIP_DATABASE_VERSION = 1
OWNERSHIP_EVIDENCE_SCHEMA = "kataproxy.ownership-evidence.v1"
OWNERSHIP_MODEL_SCHEMA = "kataproxy.ownership-model.v1"
OWNERSHIP_COORDINATE_CONVENTION = "row-major-y-down-v1"
OWNERSHIP_FLOAT_CODEC = "ieee754-binary64-little-endian-v1"
OWNERSHIP_BOARD_CODEC = "cell-state-u8-empty0-black1-white2-v1"
OWNERSHIP_LABEL_CODEC = "point-u32le-label-i8-v1"
OWNERSHIP_INDEX_CODEC = "point-index-u32le-v1"
OWNERSHIP_OBJECTIVE_VERSION = "ternary-unary-tv-aggregate-v1"
OWNERSHIP_DEAD_CHAIN_VERSION = "katago-anticipated-status-ownership-v1"


class OwnershipStoreError(RuntimeError):
    """Base exception for the independent ownership evidence database."""


class OwnershipStoreConflict(OwnershipStoreError):
    """An immutable identity was reused with conflicting stored content."""


class OwnershipStoreSchemaError(OwnershipStoreError):
    """The database schema is unknown, corrupt, or newer than this writer."""


class OwnershipStoreValidationError(OwnershipStoreError):
    """Evidence, model, or witness data violates the ownership archive contract."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _json_value(value: Any) -> Any:
    """Convert only explicit JSON-shaped values; never stringify unknown objects."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise OwnershipStoreValidationError("JSON evidence contains a non-finite float")
        return value
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, Mapping):
        converted: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise OwnershipStoreValidationError("JSON evidence keys must be strings")
            converted[key] = _json_value(item)
        return converted
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise OwnershipStoreValidationError(
        f"unsupported value in ownership JSON evidence: {type(value).__qualname__}"
    )


def _pack_float64(values: tuple[float, ...], field_name: str) -> bytes:
    if any(not math.isfinite(float(value)) for value in values):
        raise OwnershipStoreValidationError(f"{field_name} contains a non-finite value")
    if not values:
        raise OwnershipStoreValidationError(f"{field_name} must not be empty")
    return struct.pack(f"<{len(values)}d", *(float(value) for value in values))


def _pack_indices(values: tuple[int, ...], field_name: str) -> bytes:
    if tuple(sorted(set(values))) != values:
        raise OwnershipStoreValidationError(f"{field_name} must be sorted and unique")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFFFFFF
        for value in values
    ):
        raise OwnershipStoreValidationError(f"{field_name} must contain uint32 point indexes")
    return struct.pack(f"<{len(values)}I", *values) if values else b""


def _pack_labeling(labeling: OwnershipLabeling | None) -> bytes | None:
    if labeling is None:
        return None
    entries = labeling.point_labels
    payload = bytearray(struct.pack("<I", len(entries)))
    for point, label in entries:
        payload.extend(struct.pack("<Ib", point, label))
    return bytes(payload)


def _pack_output_labeling(
    evidence: OwnershipEvidence,
    regions: OwnershipRegions,
) -> bytes:
    eligible = tuple(sorted(
        set(evidence.empty_points)
        | set(evidence.dead_black)
        | set(evidence.dead_white)
    ))
    positive = set(regions.positive)
    negative = set(regions.negative)
    if positive & negative:
        raise OwnershipStoreValidationError("positive and negative ownership regions overlap")
    if not positive.issubset(eligible) or not negative.issubset(eligible):
        raise OwnershipStoreValidationError("ownership output labels an ineligible board point")
    if not set(evidence.dead_black).issubset(positive):
        raise OwnershipStoreValidationError("dead Black stones must remain White-owned")
    if not set(evidence.dead_white).issubset(negative):
        raise OwnershipStoreValidationError("dead White stones must remain Black-owned")

    labels = tuple(
        (point, 1 if point in positive else -1 if point in negative else 0)
        for point in eligible
    )
    return _pack_labeling(OwnershipLabeling(labels)) or b""


def _evidence_payload(
    evidence: OwnershipEvidence,
) -> tuple[str, bytes, bytes | None, bytes, bytes, bytes, bytes]:
    if (
        isinstance(evidence.width, bool) or not isinstance(evidence.width, int)
        or isinstance(evidence.height, bool) or not isinstance(evidence.height, int)
        or evidence.width <= 0 or evidence.height <= 0
    ):
        raise OwnershipStoreValidationError("ownership evidence dimensions must be positive")
    area = evidence.width * evidence.height
    if len(evidence.ownership) != area or len(evidence.board) != area:
        raise OwnershipStoreValidationError("ownership map and board do not match dimensions")
    if evidence.ownership_stdev is not None and len(evidence.ownership_stdev) != area:
        raise OwnershipStoreValidationError("ownershipStdev map does not match dimensions")

    mean_blob = _pack_float64(evidence.ownership, "ownership")
    stdev_blob = (
        None if evidence.ownership_stdev is None
        else _pack_float64(evidence.ownership_stdev, "ownershipStdev")
    )
    board_codes = {"": 0, "B": 1, "W": 2}
    try:
        board_blob = bytes(board_codes["" if cell is None else cell] for cell in evidence.board)
    except KeyError as exc:
        raise OwnershipStoreValidationError("board contains an unknown stone colour") from exc

    empty_blob = _pack_indices(evidence.empty_points, "empty_points")
    dead_black_blob = _pack_indices(evidence.dead_black, "dead_black")
    dead_white_blob = _pack_indices(evidence.dead_white, "dead_white")
    if set(evidence.empty_points) & (set(evidence.dead_black) | set(evidence.dead_white)):
        raise OwnershipStoreValidationError("empty points overlap fixed dead-chain points")
    if set(evidence.dead_black) & set(evidence.dead_white):
        raise OwnershipStoreValidationError("dead Black and dead White sets overlap")

    header = _canonical_json({
        "schema": OWNERSHIP_EVIDENCE_SCHEMA,
        "width": evidence.width,
        "height": evidence.height,
        "coordinate_convention": OWNERSHIP_COORDINATE_CONVENTION,
        "float_codec": OWNERSHIP_FLOAT_CODEC,
        "stdev_disposition": "present" if stdev_blob is not None else "not_available",
        "board_codec": OWNERSHIP_BOARD_CODEC,
        "index_codec": OWNERSHIP_INDEX_CODEC,
        "dead_chain_version": OWNERSHIP_DEAD_CHAIN_VERSION,
    }).encode("utf-8")
    digest = hashlib.sha256()
    digest.update(header)
    for payload in (
        mean_blob,
        stdev_blob or b"",
        board_blob,
        empty_blob,
        dead_black_blob,
        dead_white_blob,
    ):
        digest.update(struct.pack("<Q", len(payload)))
        digest.update(payload)
    return (
        digest.hexdigest(),
        mean_blob,
        stdev_blob,
        board_blob,
        empty_blob,
        dead_black_blob,
        dead_white_blob,
    )


def _model_document(
    policy: OwnershipRegionPolicy,
    evidence: OwnershipEvidence,
) -> tuple[str, str, str]:
    policy_config: dict[str, Any] = {}
    if is_dataclass(policy):
        policy_config = {
            field.name: _json_value(getattr(policy, field.name))
            for field in fields(cast(Any, policy))
            if field.name not in {
                "time_limit_ms",
                "policy_id",
                "requires_ownership_stdev",
            }
        }
    else:
        try:
            attributes = vars(policy)
        except TypeError:
            attributes = {}
        policy_config = {
            key: _json_value(value)
            for key, value in attributes.items()
            if not key.startswith("_") and key not in {"time_limit_ms"}
            and not callable(value)
        }

    model_version = policy.model_version
    if not isinstance(model_version, str) or not model_version.strip():
        raise OwnershipStoreValidationError(
            "ownership policy must declare a non-empty model_version"
        )
    document = {
        "schema": OWNERSHIP_MODEL_SCHEMA,
        "objective_version": OWNERSHIP_OBJECTIVE_VERSION,
        "policy_model_version": model_version,
        "dead_chain_version": OWNERSHIP_DEAD_CHAIN_VERSION,
        "coordinate_convention": OWNERSHIP_COORDINATE_CONVENTION,
        "policy_id": policy.policy_id,
        "policy_class": f"{type(policy).__module__}.{type(policy).__qualname__}",
        "policy_config": policy_config,
        "request_metadata": _json_value(evidence.policy_metadata),
    }
    encoded = _canonical_json(document)
    return (
        hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        encoded,
        str(policy.policy_id),
    )


def _budget_document(record: OwnershipOptimizationRecord) -> dict[str, Any]:
    native = record.budget.native_effort_limit
    if isinstance(native, NativeEffortLimit):
        native_document: dict[str, Any] = {
            "disposition": "requested",
            "value": native.value,
            "unit": native.unit,
        }
    else:
        native_document = {
            "disposition": "not_requested",
            "reason": native.reason.value,
        }
    return {
        "wall_time_limit_ms": record.budget.wall_time_limit_ms,
        "policy_id": record.budget.policy_id,
        "native_effort": native_document,
    }


@dataclass(frozen=True)
class OwnershipObservationReceipt:
    evidence_id: str
    model_id: str
    observation_id: str
    attempt_id: str | None


class SQLiteOwnershipStore:
    """Append-only ownership archive, attached as the SQLite ownership schema."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: float = 2.0,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("ownership SQLite timeout must be positive and finite")
        self.path = Path(path).expanduser()
        self.timeout_seconds = float(timeout_seconds)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(":memory:", timeout=self.timeout_seconds)
        try:
            connection.execute(
                "ATTACH DATABASE ? AS ownership",
                (str(self.path),),
            )
            connection.execute(
                f"PRAGMA busy_timeout = {int(self.timeout_seconds * 1000)}"
            )
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA ownership.journal_mode = WAL")
            connection.execute("PRAGMA ownership.synchronous = FULL")
            return connection
        except Exception:
            connection.close()
            raise

    def _backup_before_migration(
        self,
        connection: sqlite3.Connection,
        from_version: int,
    ) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = self.path.with_name(
            self.path.name + f".pre-v{from_version + 1}-{stamp}.bak"
        )
        backup = sqlite3.connect(str(backup_path))
        try:
            connection.backup(backup, name="ownership")
        finally:
            backup.close()

    @staticmethod
    def _create_v1_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE ownership.schema_migrations (
                version INTEGER PRIMARY KEY CHECK(version > 0),
                applied_at_utc TEXT NOT NULL,
                note TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ownership.evidence (
                evidence_id TEXT PRIMARY KEY CHECK(
                    length(evidence_id) = 64
                    AND evidence_id NOT GLOB '*[^0-9a-f]*'
                ),
                schema_version TEXT NOT NULL,
                width INTEGER NOT NULL CHECK(width > 0),
                height INTEGER NOT NULL CHECK(height > 0),
                coordinate_convention TEXT NOT NULL,
                ownership_codec TEXT NOT NULL,
                ownership_blob BLOB NOT NULL,
                stdev_disposition TEXT NOT NULL CHECK(
                    stdev_disposition IN ('present', 'not_available')
                ),
                stdev_blob BLOB,
                board_codec TEXT NOT NULL,
                board_blob BLOB NOT NULL,
                index_codec TEXT NOT NULL,
                empty_points_blob BLOB NOT NULL,
                dead_black_blob BLOB NOT NULL,
                dead_white_blob BLOB NOT NULL,
                dead_chain_version TEXT NOT NULL,
                CHECK(
                    (stdev_disposition = 'present' AND stdev_blob IS NOT NULL)
                    OR
                    (stdev_disposition = 'not_available' AND stdev_blob IS NULL)
                )
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ownership.models (
                model_id TEXT PRIMARY KEY CHECK(
                    length(model_id) = 64
                    AND model_id NOT GLOB '*[^0-9a-f]*'
                ),
                schema_version TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                model_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ownership.solver_identities (
                solver_identity_id TEXT PRIMARY KEY CHECK(
                    length(solver_identity_id) = 64
                    AND solver_identity_id NOT GLOB '*[^0-9a-f]*'
                ),
                backend_name TEXT NOT NULL,
                backend_version TEXT NOT NULL,
                adapter_version TEXT NOT NULL,
                configuration_json TEXT NOT NULL,
                configuration_fingerprint TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ownership.attempts (
                attempt_id TEXT PRIMARY KEY,
                evidence_id TEXT NOT NULL REFERENCES evidence(evidence_id),
                model_id TEXT NOT NULL REFERENCES models(model_id),
                solver_identity_id TEXT NOT NULL
                    REFERENCES solver_identities(solver_identity_id),
                status TEXT NOT NULL CHECK(status IN (
                    'optimal', 'feasible', 'feasible_supported',
                    'feasible_repaired', 'seed_only', 'error', 'not_required'
                )),
                started_at_utc TEXT NOT NULL,
                elapsed_wall_ns INTEGER NOT NULL CHECK(elapsed_wall_ns >= 0),
                budget_json TEXT NOT NULL,
                initial_disposition TEXT NOT NULL CHECK(
                    initial_disposition IN ('present', 'not_present')
                ),
                initial_labeling_blob BLOB,
                initial_objective REAL,
                optimizer_disposition TEXT NOT NULL CHECK(
                    optimizer_disposition IN ('present', 'not_returned')
                ),
                optimizer_labeling_blob BLOB,
                optimizer_objective REAL,
                lower_bound_disposition TEXT NOT NULL CHECK(
                    lower_bound_disposition IN ('reported', 'not_reported')
                ),
                lower_bound REAL,
                absolute_gap REAL,
                backend_reported_gap REAL,
                backend_status TEXT,
                error TEXT,
                CHECK(
                    (initial_disposition = 'present'
                        AND initial_labeling_blob IS NOT NULL
                        AND initial_objective IS NOT NULL)
                    OR
                    (initial_disposition = 'not_present'
                        AND initial_labeling_blob IS NULL
                        AND initial_objective IS NULL)
                ),
                CHECK(
                    (optimizer_disposition = 'present'
                        AND optimizer_labeling_blob IS NOT NULL
                        AND optimizer_objective IS NOT NULL)
                    OR
                    (optimizer_disposition = 'not_returned'
                        AND optimizer_labeling_blob IS NULL
                        AND optimizer_objective IS NULL)
                ),
                CHECK(
                    (lower_bound_disposition = 'reported' AND lower_bound IS NOT NULL)
                    OR
                    (lower_bound_disposition = 'not_reported' AND lower_bound IS NULL)
                ),
                CHECK(absolute_gap IS NULL OR (
                    optimizer_objective IS NOT NULL
                    AND lower_bound_disposition = 'reported'
                    AND lower_bound IS NOT NULL
                )),
                CHECK(
                    (status = 'error' AND error IS NOT NULL AND length(error) > 0)
                    OR (status <> 'error' AND error IS NULL)
                ),
                CHECK(status <> 'optimal' OR (
                    optimizer_disposition = 'present'
                    AND lower_bound IS NOT NULL
                    AND absolute_gap IS NOT NULL
                    AND absolute_gap <= 0.000001
                ))
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE ownership.observations (
                observation_id TEXT PRIMARY KEY,
                evidence_id TEXT NOT NULL REFERENCES evidence(evidence_id),
                model_id TEXT NOT NULL REFERENCES models(model_id),
                attempt_id TEXT UNIQUE REFERENCES attempts(attempt_id),
                observed_at_utc TEXT NOT NULL,
                turn_number INTEGER NOT NULL CHECK(turn_number >= 0),
                is_during_search INTEGER NOT NULL CHECK(is_during_search IN (0, 1)),
                selected_labeling_codec TEXT NOT NULL,
                selected_labeling_blob BLOB NOT NULL,
                diagnostics_json TEXT NOT NULL,
                context_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX ownership.idx_observations_evidence_time
                ON observations(evidence_id, observed_at_utc)
            """
        )
        connection.execute(
            """
            CREATE INDEX ownership.idx_observations_model_time
                ON observations(model_id, observed_at_utc)
            """
        )
        connection.execute(
            """
            CREATE INDEX ownership.idx_attempts_evidence_started
                ON attempts(evidence_id, started_at_utc)
            """
        )
        connection.execute(
            """
            CREATE INDEX ownership.idx_attempts_model_started
                ON attempts(model_id, started_at_utc)
            """
        )
        connection.execute(
            """
            CREATE INDEX ownership.idx_attempts_status_started
                ON attempts(status, started_at_utc)
            """
        )

    def initialize(self) -> int:
        """Create v1 or reject unknown/future schema state; no hidden auto-reset."""
        existed = self.path.exists() and self.path.stat().st_size > 0
        connection = self._connect()
        try:
            # The common path is read-only. Serialize writers only when a
            # migration is actually required, then re-check under the lock.
            row = connection.execute("PRAGMA ownership.user_version").fetchone()
            if row is None:
                raise OwnershipStoreSchemaError("could not read ownership.user_version")
            current = int(row[0])
            if current > OWNERSHIP_DATABASE_VERSION:
                raise OwnershipStoreSchemaError(
                    "ownership database is newer than this writer "
                    f"(user_version={current}, supported={OWNERSHIP_DATABASE_VERSION})"
                )
            if current == OWNERSHIP_DATABASE_VERSION:
                return current

            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("PRAGMA ownership.user_version").fetchone()
            if row is None:
                raise OwnershipStoreSchemaError("could not read ownership.user_version")
            current = int(row[0])
            if current > OWNERSHIP_DATABASE_VERSION:
                raise OwnershipStoreSchemaError(
                    "ownership database is newer than this writer "
                    f"(user_version={current}, supported={OWNERSHIP_DATABASE_VERSION})"
                )
            if current == OWNERSHIP_DATABASE_VERSION:
                connection.commit()
                return current
            existing_objects = connection.execute(
                "SELECT type, name FROM ownership.sqlite_master "
                "WHERE type IN ('table', 'view', 'trigger', 'index') "
                "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall()
            if existing_objects:
                connection.commit()
                if existed:
                    self._backup_before_migration(connection, current)
                names = ", ".join(
                    f"{item[0]}:{item[1]}" for item in existing_objects
                )
                raise OwnershipStoreSchemaError(
                    "unversioned ownership database contains schema objects; "
                    f"preserved a backup and refused to reinterpret them: {names}"
                )

            self._create_v1_schema(connection)
            connection.execute(
                """
                INSERT INTO ownership.schema_migrations(version, applied_at_utc, note)
                VALUES (?, ?, ?)
                """,
                (
                    OWNERSHIP_DATABASE_VERSION,
                    _utc_now(),
                    "initial lossless ownership evidence and witness archive",
                ),
            )
            connection.execute(
                f"PRAGMA ownership.user_version = {OWNERSHIP_DATABASE_VERSION}"
            )
            connection.commit()
            return OWNERSHIP_DATABASE_VERSION
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _insert_evidence(
        connection: sqlite3.Connection,
        evidence: OwnershipEvidence,
        payload: tuple[str, bytes, bytes | None, bytes, bytes, bytes, bytes],
    ) -> str:
        (
            evidence_id,
            mean_blob,
            stdev_blob,
            board_blob,
            empty_blob,
            dead_black_blob,
            dead_white_blob,
        ) = payload
        stdev_disposition = "present" if stdev_blob is not None else "not_available"
        existing = connection.execute(
            """
            SELECT width, height, ownership_blob, stdev_blob, board_blob,
                   empty_points_blob, dead_black_blob, dead_white_blob,
                   stdev_disposition
            FROM ownership.evidence WHERE evidence_id = ?
            """,
            (evidence_id,),
        ).fetchone()
        expected = (
            evidence.width,
            evidence.height,
            mean_blob,
            stdev_blob,
            board_blob,
            empty_blob,
            dead_black_blob,
            dead_white_blob,
            stdev_disposition,
        )
        if existing is not None:
            actual = tuple(
                bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else value
                for value in existing
            )
            normalized_expected = tuple(
                bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else value
                for value in expected
            )
            if actual != normalized_expected:
                raise OwnershipStoreConflict("ownership evidence digest collision")
            return evidence_id

        connection.execute(
            """
            INSERT INTO ownership.evidence (
                evidence_id, schema_version, width, height, coordinate_convention,
                ownership_codec, ownership_blob, stdev_disposition, stdev_blob,
                board_codec, board_blob, index_codec, empty_points_blob,
                dead_black_blob, dead_white_blob, dead_chain_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                OWNERSHIP_EVIDENCE_SCHEMA,
                evidence.width,
                evidence.height,
                OWNERSHIP_COORDINATE_CONVENTION,
                OWNERSHIP_FLOAT_CODEC,
                sqlite3.Binary(mean_blob),
                stdev_disposition,
                sqlite3.Binary(stdev_blob) if stdev_blob is not None else None,
                OWNERSHIP_BOARD_CODEC,
                sqlite3.Binary(board_blob),
                OWNERSHIP_INDEX_CODEC,
                sqlite3.Binary(empty_blob),
                sqlite3.Binary(dead_black_blob),
                sqlite3.Binary(dead_white_blob),
                OWNERSHIP_DEAD_CHAIN_VERSION,
            ),
        )
        return evidence_id

    @staticmethod
    def _insert_model(
        connection: sqlite3.Connection,
        model_id: str,
        model_json: str,
        policy_id: str,
    ) -> None:
        existing = connection.execute(
            "SELECT model_json, policy_id FROM ownership.models WHERE model_id = ?",
            (model_id,),
        ).fetchone()
        if existing is not None:
            if tuple(existing) != (model_json, policy_id):
                raise OwnershipStoreConflict("ownership model digest collision")
            return
        connection.execute(
            """
            INSERT INTO ownership.models(
                model_id, schema_version, policy_id, model_json, created_at_utc
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (model_id, OWNERSHIP_MODEL_SCHEMA, policy_id, model_json, _utc_now()),
        )

    @staticmethod
    def _insert_solver_identity(
        connection: sqlite3.Connection,
        record: OwnershipOptimizationRecord,
    ) -> str:
        identity = record.solver_identity
        identifier = identity.fingerprint
        expected = (
            identity.backend_name,
            identity.backend_version,
            identity.adapter_version,
            identity.configuration_json,
            identity.configuration_fingerprint,
        )
        existing = connection.execute(
            """
            SELECT backend_name, backend_version, adapter_version,
                   configuration_json, configuration_fingerprint
            FROM ownership.solver_identities WHERE solver_identity_id = ?
            """,
            (identifier,),
        ).fetchone()
        if existing is not None:
            if tuple(existing) != expected:
                raise OwnershipStoreConflict("ownership solver identity fingerprint collision")
            return identifier
        connection.execute(
            """
            INSERT INTO ownership.solver_identities(
                solver_identity_id, backend_name, backend_version, adapter_version,
                configuration_json, configuration_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (identifier, *expected),
        )
        return identifier

    @classmethod
    def _insert_attempt(
        cls,
        connection: sqlite3.Connection,
        record: OwnershipOptimizationRecord,
        evidence_id: str,
        model_id: str,
    ) -> str:
        identity_id = cls._insert_solver_identity(connection, record)
        initial_blob = _pack_labeling(record.initial_heuristic)
        optimizer_blob = _pack_labeling(record.optimizer_incumbent)
        connection.execute(
            """
            INSERT INTO ownership.attempts (
                attempt_id, evidence_id, model_id, solver_identity_id, status,
                started_at_utc, elapsed_wall_ns, budget_json,
                initial_disposition, initial_labeling_blob, initial_objective,
                optimizer_disposition, optimizer_labeling_blob, optimizer_objective,
                lower_bound_disposition, lower_bound, absolute_gap,
                backend_reported_gap, backend_status, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.attempt_id,
                evidence_id,
                model_id,
                identity_id,
                record.status.value,
                record.started_at_utc,
                record.elapsed_wall_ns,
                _canonical_json(_budget_document(record)),
                "present" if initial_blob is not None else "not_present",
                sqlite3.Binary(initial_blob) if initial_blob is not None else None,
                record.initial_objective,
                "present" if optimizer_blob is not None else "not_returned",
                sqlite3.Binary(optimizer_blob) if optimizer_blob is not None else None,
                record.optimizer_objective,
                "reported" if record.lower_bound is not None else "not_reported",
                record.lower_bound,
                record.absolute_gap,
                record.backend_reported_gap,
                record.backend_status,
                record.error,
            ),
        )
        return record.attempt_id

    def record(
        self,
        evidence: OwnershipEvidence,
        regions: OwnershipRegions,
        policy: OwnershipRegionPolicy,
        *,
        turn_number: int,
        is_during_search: bool,
        response_context: Mapping[str, Any] | None = None,
    ) -> OwnershipObservationReceipt:
        """Commit immutable evidence, model, optional attempt, and output witness."""
        if isinstance(turn_number, bool) or not isinstance(turn_number, int) or turn_number < 0:
            raise OwnershipStoreValidationError("turn_number must be a non-negative integer")
        if not isinstance(is_during_search, bool):
            raise OwnershipStoreValidationError("is_during_search must be bool")
        if regions.policy_id != policy.policy_id:
            raise OwnershipStoreValidationError("region result and policy IDs disagree")

        evidence_payload = _evidence_payload(evidence)
        evidence_id = evidence_payload[0]
        model_id, model_json, policy_id = _model_document(policy, evidence)
        selected_blob = _pack_output_labeling(evidence, regions)
        diagnostics_json = _canonical_json(_json_value(regions.diagnostics))
        context_json = _canonical_json(_json_value(response_context or {}))
        observation_id = str(uuid.uuid4())
        observed_at = _utc_now()
        record = regions.solver_evidence

        self.initialize()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            evidence_id = self._insert_evidence(connection, evidence, evidence_payload)
            self._insert_model(connection, model_id, model_json, policy_id)
            attempt_id: str | None = None
            if record is not None:
                attempt_id = self._insert_attempt(
                    connection, record, evidence_id, model_id,
                )
            connection.execute(
                """
                INSERT INTO ownership.observations (
                    observation_id, evidence_id, model_id, attempt_id,
                    observed_at_utc, turn_number, is_during_search,
                    selected_labeling_codec, selected_labeling_blob,
                    diagnostics_json, context_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_id,
                    evidence_id,
                    model_id,
                    attempt_id,
                    observed_at,
                    turn_number,
                    1 if is_during_search else 0,
                    OWNERSHIP_LABEL_CODEC,
                    sqlite3.Binary(selected_blob),
                    diagnostics_json,
                    context_json,
                ),
            )
            connection.commit()
            return OwnershipObservationReceipt(
                evidence_id=evidence_id,
                model_id=model_id,
                observation_id=observation_id,
                attempt_id=attempt_id,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def counts(self) -> dict[str, int]:
        """Return row counts without exposing ownership maps or witnesses."""
        self.initialize()
        connection = self._connect()
        try:
            result: dict[str, int] = {}
            for name, table in (
                ("evidence", "evidence"),
                ("models", "models"),
                ("attempts", "attempts"),
                ("observations", "observations"),
            ):
                row = connection.execute(
                    f"SELECT COUNT(*) FROM ownership.{table}"
                ).fetchone()
                result[name] = int(row[0]) if row is not None else 0
            return result
        finally:
            connection.close()
