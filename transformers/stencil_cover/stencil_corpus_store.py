"""Normalized SQLite corpus store for immutable stencil-cover events.

The raw-event spool and normalized corpus tables intentionally share one
SQLite file. Event ingestion, receipt insertion, aggregate updates and spool
removal are transactional; Redis acknowledgement is the caller's next step.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .stencil_corpus_events import (
    CORPUS_EVENT_SCHEMA,
    instance_id_for,
    parse_event,
)

logger = logging.getLogger("kataproxy.stencil_corpus_store")

CORPUS_DATABASE_VERSION = 2
_SPOOL_TABLE = "stencil_corpus_spool_events"


class CorpusStoreError(RuntimeError):
    """Base exception for normalized corpus persistence failures."""


class CorpusStoreConflict(CorpusStoreError):
    """A durable event/identity ID was reused with conflicting immutable data."""


class MissingInstanceSighting(CorpusStoreError):
    """An attempt arrived before its instance_seen event was materialized."""


@dataclass(frozen=True)
class SpoolEvent:
    event_id: str
    payload: bytes
    payload_sha256: str


@dataclass(frozen=True)
class IngestResult:
    inserted: bool
    event_id: str
    event_type: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )


def _decode_instance(event: dict[str, Any]) -> tuple[str, bytes, int, int, int]:
    instance = event["instance"]
    key = base64.b64decode(instance["canonical_key_b64"], validate=True)
    identifier = instance_id_for(
        instance["family_fingerprint"],
        instance["canonicalization_version"],
        key,
    )
    return (
        identifier,
        key,
        instance["width"],
        instance["height"],
        instance["cell_count"],
    )


class SQLiteCorpusStore:
    """Own schema migrations and atomic materialization of corpus events."""

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_seconds: float = 5.0,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("SQLite corpus timeout must be positive")
        self.path = Path(path).expanduser()
        self.timeout_seconds = timeout_seconds

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            str(self.path),
            timeout=self.timeout_seconds,
        )
        connection.execute(
            f"PRAGMA busy_timeout = {int(self.timeout_seconds * 1000)}"
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _backup_before_migration(
        self, connection: sqlite3.Connection, from_version: int,
    ) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = self.path.with_name(
            self.path.name + f".pre-v{from_version + 1}-{stamp}.bak"
        )
        backup = sqlite3.connect(str(backup_path))
        try:
            connection.backup(backup)
        finally:
            backup.close()

    @staticmethod
    def _create_spool_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS stencil_corpus_spool_events (
                event_id TEXT PRIMARY KEY,
                event_json BLOB NOT NULL,
                payload_sha256 TEXT NOT NULL CHECK(
                    length(payload_sha256) = 64
                    AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                queued_at_utc TEXT NOT NULL
            )
            """
        )

    @staticmethod
    def _create_normalized_schema(connection: sqlite3.Connection) -> None:
        script = """
            CREATE TABLE IF NOT EXISTS corpus_schema_migrations (
                version INTEGER PRIMARY KEY CHECK(version > 0),
                applied_at_utc TEXT NOT NULL,
                note TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS corpus_event_receipts (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL CHECK(
                    event_type IN ('instance_seen', 'solver_attempt')
                ),
                schema_version TEXT NOT NULL,
                emitted_at_utc TEXT NOT NULL,
                ingested_at_utc TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256) = 64)
            );

            CREATE TABLE IF NOT EXISTS instances (
                instance_id TEXT PRIMARY KEY,
                family_fingerprint TEXT NOT NULL,
                canonicalization_version TEXT NOT NULL,
                canonical_key BLOB NOT NULL,
                canonical_width INTEGER NOT NULL CHECK(canonical_width > 0),
                canonical_height INTEGER NOT NULL CHECK(canonical_height > 0),
                cell_count INTEGER NOT NULL CHECK(cell_count > 0),
                first_seen_at_utc TEXT NOT NULL,
                last_seen_at_utc TEXT NOT NULL,
                sighting_count INTEGER NOT NULL CHECK(sighting_count > 0),
                UNIQUE(
                    family_fingerprint,
                    canonicalization_version,
                    canonical_key
                )
            );

            CREATE TABLE IF NOT EXISTS instance_sighting_paths (
                instance_id TEXT NOT NULL REFERENCES instances(instance_id),
                producer_path TEXT NOT NULL CHECK(
                    producer_path IN ('foreground', 'bulk')
                ),
                first_seen_at_utc TEXT NOT NULL,
                last_seen_at_utc TEXT NOT NULL,
                sighting_count INTEGER NOT NULL CHECK(sighting_count > 0),
                PRIMARY KEY(instance_id, producer_path)
            );

            CREATE TABLE IF NOT EXISTS solver_identities (
                solver_identity_id TEXT PRIMARY KEY,
                backend_name TEXT NOT NULL,
                backend_version TEXT NOT NULL,
                adapter_version TEXT NOT NULL,
                configuration_json TEXT NOT NULL,
                configuration_fingerprint TEXT NOT NULL,
                UNIQUE(
                    backend_name,
                    backend_version,
                    adapter_version,
                    configuration_fingerprint
                )
            );

            CREATE TABLE IF NOT EXISTS solver_attempts (
                attempt_id TEXT PRIMARY KEY REFERENCES corpus_event_receipts(event_id),
                instance_id TEXT NOT NULL REFERENCES instances(instance_id),
                solver_identity_id TEXT NOT NULL
                    REFERENCES solver_identities(solver_identity_id),
                source_path TEXT NOT NULL CHECK(source_path IN ('foreground', 'bulk')),
                budget_json TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                elapsed_wall_ns INTEGER NOT NULL CHECK(elapsed_wall_ns >= 0),
                timing_breakdown_json TEXT NOT NULL,
                outcome_status TEXT NOT NULL CHECK(
                    outcome_status IN ('optimal', 'feasible', 'no_incumbent', 'error')
                ),
                optimality_proven INTEGER NOT NULL CHECK(optimality_proven IN (0, 1)),
                incumbent_disposition TEXT NOT NULL CHECK(
                    incumbent_disposition IN ('present', 'not_returned')
                ),
                incumbent_tile_count INTEGER,
                lower_bound_disposition TEXT NOT NULL CHECK(
                    lower_bound_disposition IN ('reported', 'not_reported')
                ),
                lower_bound INTEGER,
                native_effort_disposition TEXT NOT NULL CHECK(
                    native_effort_disposition IN ('reported', 'not_supported', 'not_reported')
                ),
                native_effort_value REAL,
                native_effort_unit TEXT,
                backend_status_disposition TEXT NOT NULL CHECK(
                    backend_status_disposition IN ('reported', 'not_reported')
                ),
                backend_status TEXT,
                backend_metrics_json TEXT NOT NULL,
                placements_disposition TEXT NOT NULL CHECK(
                    placements_disposition IN ('validated', 'not_returned', 'rejected_invalid')
                ),
                placements_payload BLOB,
                placements_codec_version TEXT,
                fallback_disposition TEXT NOT NULL CHECK(
                    fallback_disposition IN ('not_needed', 'singleton_fallback_used')
                ),
                error_disposition TEXT NOT NULL CHECK(error_disposition IN ('present', 'none')),
                error_json TEXT,
                CHECK(
                    (incumbent_disposition = 'present' AND incumbent_tile_count IS NOT NULL
                        AND incumbent_tile_count > 0)
                    OR
                    (incumbent_disposition = 'not_returned' AND incumbent_tile_count IS NULL)
                ),
                CHECK(
                    (lower_bound_disposition = 'reported' AND lower_bound IS NOT NULL
                        AND lower_bound >= 0)
                    OR
                    (lower_bound_disposition = 'not_reported' AND lower_bound IS NULL)
                ),
                CHECK(
                    (native_effort_disposition = 'reported'
                        AND native_effort_value IS NOT NULL AND native_effort_value >= 0
                        AND native_effort_unit IS NOT NULL AND length(native_effort_unit) > 0)
                    OR
                    (native_effort_disposition <> 'reported'
                        AND native_effort_value IS NULL AND native_effort_unit IS NULL)
                ),
                CHECK(
                    (backend_status_disposition = 'reported'
                        AND backend_status IS NOT NULL AND length(backend_status) > 0)
                    OR
                    (backend_status_disposition = 'not_reported' AND backend_status IS NULL)
                ),
                CHECK(
                    (placements_disposition = 'validated'
                        AND placements_payload IS NOT NULL
                        AND placements_codec_version IS NOT NULL)
                    OR
                    (placements_disposition <> 'validated'
                        AND placements_payload IS NULL AND placements_codec_version IS NULL)
                ),
                CHECK(
                    (error_disposition = 'present'
                        AND outcome_status = 'error' AND error_json IS NOT NULL)
                    OR
                    (error_disposition = 'none'
                        AND outcome_status <> 'error' AND error_json IS NULL)
                ),
                CHECK(optimality_proven = CASE WHEN outcome_status = 'optimal' THEN 1 ELSE 0 END),
                CHECK(
                    outcome_status <> 'optimal'
                    OR (
                        incumbent_disposition = 'present'
                        AND lower_bound_disposition = 'reported'
                        AND lower_bound = incumbent_tile_count
                    )
                ),
                CHECK(
                    lower_bound IS NULL OR incumbent_tile_count IS NULL
                    OR lower_bound <= incumbent_tile_count
                ),
                CHECK(
                    (fallback_disposition = 'not_needed'
                        AND placements_disposition = 'validated')
                    OR
                    (fallback_disposition = 'singleton_fallback_used'
                        AND placements_disposition <> 'validated')
                ),
                CHECK(
                    placements_disposition <> 'rejected_invalid'
                    OR outcome_status = 'error'
                )
            );

            CREATE INDEX IF NOT EXISTS idx_attempt_instance_started
                ON solver_attempts(instance_id, started_at_utc);
            CREATE INDEX IF NOT EXISTS idx_attempt_solver_started
                ON solver_attempts(solver_identity_id, started_at_utc);
            CREATE INDEX IF NOT EXISTS idx_attempt_status_started
                ON solver_attempts(outcome_status, started_at_utc);
            CREATE INDEX IF NOT EXISTS idx_attempt_source_started
                ON solver_attempts(source_path, started_at_utc);
            """
        statement = ""
        for line in script.splitlines():
            statement += line + "\n"
            if sqlite3.complete_statement(statement):
                if statement.strip():
                    connection.execute(statement)
                statement = ""
        if statement.strip():
            raise CorpusStoreError("normalized corpus migration has an incomplete SQL statement")

    def initialize(self) -> int:
        """Apply append-only migrations one version at a time."""
        existed = self.path.exists() and self.path.stat().st_size > 0
        connection = self._connect()
        try:
            version_row = connection.execute("PRAGMA user_version").fetchone()
            if version_row is None:
                raise CorpusStoreError("could not read SQLite corpus user_version")
            version = int(version_row[0])
            if version > CORPUS_DATABASE_VERSION:
                raise CorpusStoreError(
                    "SQLite corpus database is newer than this writer "
                    f"(user_version={version}, supported={CORPUS_DATABASE_VERSION})"
                )
            connection.execute("PRAGMA journal_mode = WAL")
            while version < CORPUS_DATABASE_VERSION:
                if existed:
                    self._backup_before_migration(connection, version)
                if version == 0:
                    connection.execute("BEGIN IMMEDIATE")
                    self._create_spool_schema(connection)
                    connection.execute("PRAGMA user_version = 1")
                    connection.commit()
                    version = 1
                    continue
                if version == 1:
                    connection.execute("BEGIN IMMEDIATE")
                    self._create_spool_schema(connection)
                    self._create_normalized_schema(connection)
                    now = _utc_now()
                    connection.execute(
                        "INSERT OR IGNORE INTO corpus_schema_migrations "
                        "(version, applied_at_utc, note) VALUES (?, ?, ?)",
                        (1, now, "Create raw immutable event spool"),
                    )
                    connection.execute(
                        "INSERT INTO corpus_schema_migrations "
                        "(version, applied_at_utc, note) VALUES (?, ?, ?)",
                        (2, now, "Materialize v1 events into normalized corpus tables"),
                    )
                    connection.execute("PRAGMA user_version = 2")
                    connection.commit()
                    version = 2
                    continue
                raise CorpusStoreError(f"no migration path from SQLite version {version}")
            return version
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _ingest_on_connection(
        self,
        connection: sqlite3.Connection,
        payload: bytes,
        *,
        spool_event_id: str | None = None,
        spool_sha256: str | None = None,
    ) -> IngestResult:
        event = parse_event(payload)
        if not isinstance(payload, bytes):
            raise TypeError("event ingestion requires canonical UTF-8 bytes")
        event_id = event["event_id"]
        digest = hashlib.sha256(payload).hexdigest()
        spool_row: tuple[Any, Any] | None = None
        if spool_event_id is not None:
            if spool_event_id != event_id:
                raise CorpusStoreConflict("spool row event_id disagrees with its event payload")
            if spool_sha256 != digest:
                raise CorpusStoreConflict("spool row checksum disagrees with event payload")
            raw_spool_row = connection.execute(
                "SELECT event_json, payload_sha256 FROM stencil_corpus_spool_events "
                "WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if raw_spool_row is not None:
                spool_row = (bytes(raw_spool_row[0]), str(raw_spool_row[1]))
                if spool_row != (payload, digest):
                    raise CorpusStoreConflict(
                        f"spool row {event_id} changed after it was read"
                    )

        existing = connection.execute(
            "SELECT payload_sha256, event_type FROM corpus_event_receipts "
            "WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            if existing[0] != digest:
                raise CorpusStoreConflict(
                    f"event_id {event_id} was replayed with different immutable bytes"
                )
            if spool_event_id is not None and spool_row is not None:
                deleted = connection.execute(
                    f"DELETE FROM {_SPOOL_TABLE} WHERE event_id = ? AND payload_sha256 = ?",
                    (event_id, digest),
                )
                if deleted.rowcount != 1:
                    raise CorpusStoreConflict(
                        f"spool row {event_id} changed during duplicate-event cleanup"
                    )
            return IngestResult(False, event_id, str(existing[1]))

        if spool_event_id is not None and spool_row is None:
            raise CorpusStoreConflict(
                f"spool row {event_id} disappeared before its event was materialized"
            )

        instance = event["instance"]
        instance_id, canonical_key, width, height, cell_count = _decode_instance(event)
        if event["event_type"] == "solver_attempt":
            found = connection.execute(
                "SELECT 1 FROM instances WHERE instance_id = ?",
                (instance_id,),
            ).fetchone()
            if found is None:
                raise MissingInstanceSighting(
                    f"attempt {event_id} is waiting for instance_seen {instance_id}"
                )

        connection.execute(
            """
            INSERT INTO corpus_event_receipts
                (event_id, event_type, schema_version, emitted_at_utc,
                 ingested_at_utc, payload_sha256)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                event["event_type"],
                CORPUS_EVENT_SCHEMA,
                event["emitted_at_utc"],
                _utc_now(),
                digest,
            ),
        )

        if event["event_type"] == "instance_seen":
            self._ingest_instance_seen(
                connection,
                event,
                instance_id,
                canonical_key,
                width,
                height,
                cell_count,
            )
        else:
            self._ingest_solver_attempt(connection, event, instance_id)

        if spool_event_id is not None:
            deleted = connection.execute(
                f"DELETE FROM {_SPOOL_TABLE} WHERE event_id = ? AND payload_sha256 = ?",
                (event_id, digest),
            )
            if deleted.rowcount != 1:
                raise CorpusStoreConflict(
                    f"spool row {event_id} changed before transactional removal"
                )
        return IngestResult(True, event_id, event["event_type"])

    @staticmethod
    def _ingest_instance_seen(
        connection: sqlite3.Connection,
        event: dict[str, Any],
        instance_id: str,
        canonical_key: bytes,
        width: int,
        height: int,
        cell_count: int,
    ) -> None:
        instance = event["instance"]
        emitted_at = event["emitted_at_utc"]
        connection.execute(
            """
            INSERT INTO instances (
                instance_id, family_fingerprint, canonicalization_version,
                canonical_key, canonical_width, canonical_height, cell_count,
                first_seen_at_utc, last_seen_at_utc, sighting_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(instance_id) DO UPDATE SET
                first_seen_at_utc = MIN(instances.first_seen_at_utc, excluded.first_seen_at_utc),
                last_seen_at_utc = MAX(instances.last_seen_at_utc, excluded.last_seen_at_utc),
                sighting_count = instances.sighting_count + 1
            """,
            (
                instance_id,
                instance["family_fingerprint"],
                instance["canonicalization_version"],
                sqlite3.Binary(canonical_key),
                width,
                height,
                cell_count,
                emitted_at,
                emitted_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO instance_sighting_paths (
                instance_id, producer_path, first_seen_at_utc,
                last_seen_at_utc, sighting_count
            ) VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(instance_id, producer_path) DO UPDATE SET
                first_seen_at_utc = MIN(
                    instance_sighting_paths.first_seen_at_utc,
                    excluded.first_seen_at_utc
                ),
                last_seen_at_utc = MAX(
                    instance_sighting_paths.last_seen_at_utc,
                    excluded.last_seen_at_utc
                ),
                sighting_count = instance_sighting_paths.sighting_count + 1
            """,
            (
                instance_id,
                event["producer_path"],
                emitted_at,
                emitted_at,
            ),
        )

    @staticmethod
    def _ingest_solver_attempt(
        connection: sqlite3.Connection,
        event: dict[str, Any],
        instance_id: str,
    ) -> None:
        identity = event["solver_identity"]
        solver_identity_id = identity["fingerprint"]
        connection.execute(
            """
            INSERT OR IGNORE INTO solver_identities (
                solver_identity_id, backend_name, backend_version,
                adapter_version, configuration_json, configuration_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                solver_identity_id,
                identity["backend_name"],
                identity["backend_version"],
                identity["adapter_version"],
                identity["configuration_json"],
                identity["configuration_fingerprint"],
            ),
        )
        stored_identity = connection.execute(
            """
            SELECT backend_name, backend_version, adapter_version,
                   configuration_json, configuration_fingerprint
            FROM solver_identities WHERE solver_identity_id = ?
            """,
            (solver_identity_id,),
        ).fetchone()
        expected_identity = (
            identity["backend_name"],
            identity["backend_version"],
            identity["adapter_version"],
            identity["configuration_json"],
            identity["configuration_fingerprint"],
        )
        if stored_identity != expected_identity:
            raise CorpusStoreConflict("solver identity fingerprint collision")

        budget = event["budget"]
        outcome = event["outcome"]
        incumbent = outcome["incumbent"]
        lower_bound = outcome["lower_bound"]
        effort = outcome["native_effort"]
        backend_status = outcome["backend_status"]
        placements = outcome["placements"]
        error = outcome["error"]

        incumbent_count = (
            incumbent["tile_count"] if incumbent["disposition"] == "present" else None
        )
        lower_value = (
            lower_bound["value"] if lower_bound["disposition"] == "reported" else None
        )
        effort_value = effort["value"] if effort["disposition"] == "reported" else None
        effort_unit = effort["unit"] if effort["disposition"] == "reported" else None
        backend_status_value = (
            backend_status["value"]
            if backend_status["disposition"] == "reported"
            else None
        )
        placement_payload = (
            base64.b64decode(placements["payload_b64"], validate=True)
            if placements["disposition"] == "validated"
            else None
        )
        placement_codec = (
            placements["codec_version"]
            if placements["disposition"] == "validated"
            else None
        )
        error_json = _canonical_json(error) if error["disposition"] == "present" else None

        connection.execute(
            """
            INSERT INTO solver_attempts (
                attempt_id, instance_id, solver_identity_id, source_path,
                budget_json, started_at_utc, elapsed_wall_ns, timing_breakdown_json,
                outcome_status, optimality_proven,
                incumbent_disposition, incumbent_tile_count,
                lower_bound_disposition, lower_bound,
                native_effort_disposition, native_effort_value, native_effort_unit,
                backend_status_disposition, backend_status, backend_metrics_json,
                placements_disposition, placements_payload, placements_codec_version,
                fallback_disposition, error_disposition, error_json
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, '{}',
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                event["attempt_id"],
                instance_id,
                solver_identity_id,
                event["producer_path"],
                _canonical_json(budget),
                event["started_at_utc"],
                event["elapsed_wall_ns"],
                outcome["status"],
                1 if outcome["optimality_proven"] else 0,
                incumbent["disposition"],
                incumbent_count,
                lower_bound["disposition"],
                lower_value,
                effort["disposition"],
                effort_value,
                effort_unit,
                backend_status["disposition"],
                backend_status_value,
                _canonical_json(outcome["backend_metrics"]),
                placements["disposition"],
                sqlite3.Binary(placement_payload) if placement_payload is not None else None,
                placement_codec,
                event["fallback_disposition"],
                error["disposition"],
                error_json,
            ),
        )

    def ingest(self, payload: bytes | str) -> IngestResult:
        """Materialize one canonical event and its receipt in one transaction."""
        if isinstance(payload, str):
            payload_bytes = payload.encode("utf-8")
        elif isinstance(payload, bytes):
            payload_bytes = payload
        else:
            raise TypeError("event payload must be bytes or text")
        self.initialize()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = self._ingest_on_connection(connection, payload_bytes)
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def pending_spool_events(self, limit: int = 100) -> tuple[SpoolEvent, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("spool batch limit must be a positive integer")
        self.initialize()
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT event_id, event_json, payload_sha256
                FROM stencil_corpus_spool_events
                ORDER BY rowid
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return tuple(
                SpoolEvent(
                    event_id=str(row[0]),
                    payload=bytes(row[1]),
                    payload_sha256=str(row[2]),
                )
                for row in rows
            )
        finally:
            connection.close()

    def ingest_spool_event(self, item: SpoolEvent) -> IngestResult:
        """Ingest and remove a local spool row atomically."""
        self.initialize()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = self._ingest_on_connection(
                connection,
                item.payload,
                spool_event_id=item.event_id,
                spool_sha256=item.payload_sha256,
            )
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def counts(self) -> dict[str, int]:
        """Return small operator-facing materialization counts."""
        self.initialize()
        connection = self._connect()
        try:
            result: dict[str, int] = {}
            for key, table in (
                ("instances", "instances"),
                ("attempts", "solver_attempts"),
                ("receipts", "corpus_event_receipts"),
                ("spool_pending", _SPOOL_TABLE),
            ):
                row = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                result[key] = int(row[0]) if row is not None else 0
            return result
        finally:
            connection.close()
