#!/usr/bin/env python3
"""Read-only analysis and lossless export for the ownership evidence archive."""

from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import sqlite3
import struct
import sys
from typing import Any, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from transformers.stencil_cover.stencil_ownership import OwnershipEvidence
from transformers.stencil_cover.stencil_ownership_graphcut import ownership_graph
from transformers.stencil_cover.stencil_ownership_repair import ownership_energy


DATABASE_VERSION = 1
FLOAT_CODEC = "ieee754-binary64-little-endian-v1"
BOARD_CODEC = "cell-state-u8-empty0-black1-white2-v1"
INDEX_CODEC = "point-index-u32le-v1"
LABEL_CODEC = "point-u32le-label-i8-v1"
OBJECTIVE_VERSION = "ternary-unary-tv-aggregate-v1"
EVIDENCE_SCHEMA = "kataproxy.ownership-evidence.v1"
MODEL_SCHEMA = "kataproxy.ownership-model.v1"
COORDINATE_CONVENTION = "row-major-y-down-v1"
COHERENT_POLICIES = frozenset({"coherent", "coherent-cut"})


@dataclass(frozen=True)
class ArchiveRow:
    observation_id: str
    evidence_id: str
    model_id: str
    attempt_id: str | None
    observed_at_utc: str
    turn_number: int
    is_during_search: bool
    width: int
    height: int
    coordinate_convention: str
    ownership: tuple[float, ...]
    ownership_stdev: tuple[float, ...] | None
    board: tuple[str | None, ...]
    board_blob: bytes
    empty_points: tuple[int, ...]
    dead_black: tuple[int, ...]
    dead_white: tuple[int, ...]
    dead_chain_version: str
    evidence_schema: str
    model_schema: str
    model_created_at_utc: str
    policy_id: str
    model_document: dict[str, Any]
    model_json_raw: str
    diagnostics_json_raw: str
    context_json_raw: str
    selected_labels: dict[int, int]
    diagnostics: dict[str, Any]
    context: dict[str, Any]
    attempt: dict[str, Any] | None
    budget_json_raw: str | None
    solver_identity: dict[str, Any] | None
    solver_configuration_json_raw: str | None
    raw_blobs: dict[str, bytes | None]


def _object_json(value: str, field_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must contain a JSON object")
    return parsed


def _unpack_float64(blob: bytes, count: int, field_name: str) -> tuple[float, ...]:
    if len(blob) != count * 8:
        raise ValueError(
            f"{field_name} blob has {len(blob)} bytes; expected {count * 8}"
        )
    values = struct.unpack(f"<{count}d", blob)
    if any(not math.isfinite(value) for value in values):
        raise ValueError(f"{field_name} blob contains a non-finite value")
    return tuple(float(value) for value in values)


def _unpack_indices(blob: bytes, area: int, field_name: str) -> tuple[int, ...]:
    if len(blob) % 4:
        raise ValueError(f"{field_name} blob length is not divisible by four")
    count = len(blob) // 4
    values = struct.unpack(f"<{count}I", blob) if count else ()
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"{field_name} values are not sorted and unique")
    if any(value >= area for value in values):
        raise ValueError(f"{field_name} contains a point outside the board")
    return tuple(int(value) for value in values)


def _unpack_labels(
    blob: bytes | None,
    eligible: tuple[int, ...],
    field_name: str,
) -> dict[int, int] | None:
    if blob is None:
        return None
    if len(blob) < 4:
        raise ValueError(f"{field_name} blob is shorter than its count prefix")
    count = struct.unpack_from("<I", blob, 0)[0]
    expected_length = 4 + count * 5
    if len(blob) != expected_length:
        raise ValueError(
            f"{field_name} blob has {len(blob)} bytes; expected {expected_length}"
        )
    result: dict[int, int] = {}
    previous = -1
    for offset in range(4, len(blob), 5):
        point, label = struct.unpack_from("<Ib", blob, offset)
        if point <= previous:
            raise ValueError(f"{field_name} points are not strictly increasing")
        if label not in (-1, 0, 1):
            raise ValueError(f"{field_name} contains invalid label {label}")
        result[int(point)] = int(label)
        previous = int(point)
    if tuple(result) != eligible:
        raise ValueError(f"{field_name} points do not match the eligible-point set")
    return result


def _default_database_path() -> Path:
    configured = os.environ.get("KATAPROXY_STENCIL_OWNERSHIP_CORPUS_DB")
    if configured:
        return Path(configured).expanduser()
    state_home = os.environ.get("XDG_STATE_HOME")
    base = Path(state_home).expanduser() if state_home else Path.home() / ".local" / "state"
    return base / "kataproxy" / "ownership-quantization.sqlite3"


def _read_query() -> str:
    return """
        SELECT
            o.observation_id AS observation_id,
            o.evidence_id AS evidence_id,
            o.model_id AS model_id,
            o.attempt_id AS observation_attempt_id,
            o.observed_at_utc AS observed_at_utc,
            o.turn_number AS turn_number,
            o.is_during_search AS is_during_search,
            o.selected_labeling_codec AS selected_labeling_codec,
            o.selected_labeling_blob AS selected_labeling_blob,
            o.diagnostics_json AS diagnostics_json,
            o.context_json AS context_json,
            e.schema_version AS evidence_schema,
            e.width AS width,
            e.height AS height,
            e.coordinate_convention AS coordinate_convention,
            e.ownership_codec AS ownership_codec,
            e.ownership_blob AS ownership_blob,
            e.stdev_disposition AS stdev_disposition,
            e.stdev_blob AS stdev_blob,
            e.board_codec AS board_codec,
            e.board_blob AS board_blob,
            e.index_codec AS index_codec,
            e.empty_points_blob AS empty_points_blob,
            e.dead_black_blob AS dead_black_blob,
            e.dead_white_blob AS dead_white_blob,
            e.dead_chain_version AS dead_chain_version,
            m.schema_version AS model_schema,
            m.policy_id AS policy_id,
            m.model_json AS model_json,
            m.created_at_utc AS model_created_at_utc,
            a.attempt_id AS attempt_id,
            a.status AS attempt_status,
            a.started_at_utc AS started_at_utc,
            a.elapsed_wall_ns AS elapsed_wall_ns,
            a.budget_json AS budget_json,
            a.initial_disposition AS initial_disposition,
            a.initial_labeling_blob AS initial_labeling_blob,
            a.initial_objective AS initial_objective,
            a.optimizer_disposition AS optimizer_disposition,
            a.optimizer_labeling_blob AS optimizer_labeling_blob,
            a.optimizer_objective AS optimizer_objective,
            a.lower_bound_disposition AS lower_bound_disposition,
            a.lower_bound AS lower_bound,
            a.absolute_gap AS absolute_gap,
            a.backend_reported_gap AS backend_reported_gap,
            a.backend_status AS backend_status,
            a.error AS attempt_error,
            si.solver_identity_id AS solver_identity_id,
            si.backend_name AS backend_name,
            si.backend_version AS backend_version,
            si.adapter_version AS adapter_version,
            si.configuration_json AS solver_configuration_json,
            si.configuration_fingerprint AS solver_configuration_fingerprint
        FROM observations AS o
        JOIN evidence AS e USING (evidence_id)
        JOIN models AS m USING (model_id)
        LEFT JOIN attempts AS a ON a.attempt_id = o.attempt_id
        LEFT JOIN solver_identities AS si
            ON si.solver_identity_id = a.solver_identity_id
        ORDER BY o.observed_at_utc, o.observation_id
    """


def load_archive(database: str | Path) -> list[ArchiveRow]:
    """Read and validate schema v1 without creating, migrating, or modifying it."""
    path = Path(database).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"ownership corpus does not exist: {path}")
    uri = path.as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only = ON")
        version_row = connection.execute("PRAGMA user_version").fetchone()
        if version_row is None or int(version_row[0]) != DATABASE_VERSION:
            actual = None if version_row is None else int(version_row[0])
            raise ValueError(
                f"unsupported ownership corpus schema version {actual}; "
                f"this tool supports {DATABASE_VERSION}"
            )
        table_names = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {"evidence", "models", "attempts", "observations", "solver_identities"}
        missing = sorted(required - table_names)
        if missing:
            raise ValueError(f"ownership corpus is missing tables: {', '.join(missing)}")

        result: list[ArchiveRow] = []
        for row in connection.execute(_read_query()):
            width = int(row["width"])
            height = int(row["height"])
            area = width * height
            if width <= 0 or height <= 0:
                raise ValueError("ownership corpus contains invalid board dimensions")

            if row["evidence_schema"] != EVIDENCE_SCHEMA:
                raise ValueError(f"unsupported evidence schema: {row['evidence_schema']}")
            if row["model_schema"] != MODEL_SCHEMA:
                raise ValueError(f"unsupported model schema: {row['model_schema']}")
            if row["ownership_codec"] != FLOAT_CODEC:
                raise ValueError(f"unsupported ownership codec: {row['ownership_codec']}")
            if row["board_codec"] != BOARD_CODEC:
                raise ValueError(f"unsupported board codec: {row['board_codec']}")
            if row["index_codec"] != INDEX_CODEC:
                raise ValueError(f"unsupported index codec: {row['index_codec']}")
            if row["selected_labeling_codec"] != LABEL_CODEC:
                raise ValueError(
                    f"unsupported selected-labeling codec: {row['selected_labeling_codec']}"
                )

            ownership_blob = bytes(row["ownership_blob"])
            ownership = _unpack_float64(ownership_blob, area, "ownership")
            stdev_blob = (
                None if row["stdev_blob"] is None else bytes(row["stdev_blob"])
            )
            if row["stdev_disposition"] == "present":
                if stdev_blob is None:
                    raise ValueError("stdev is marked present but its blob is NULL")
                stdev = _unpack_float64(stdev_blob, area, "ownershipStdev")
            elif row["stdev_disposition"] == "not_available" and stdev_blob is None:
                stdev = None
            else:
                raise ValueError("invalid ownershipStdev disposition/blob combination")

            board_blob = bytes(row["board_blob"])
            if len(board_blob) != area or any(value not in (0, 1, 2) for value in board_blob):
                raise ValueError("board blob has an invalid size or cell code")
            board = tuple(
                None if value == 0 else "B" if value == 1 else "W"
                for value in board_blob
            )
            empty_points = _unpack_indices(
                bytes(row["empty_points_blob"]), area, "empty_points"
            )
            dead_black = _unpack_indices(
                bytes(row["dead_black_blob"]), area, "dead_black"
            )
            dead_white = _unpack_indices(
                bytes(row["dead_white_blob"]), area, "dead_white"
            )
            actual_empty = tuple(
                point for point, colour in enumerate(board) if colour is None
            )
            if actual_empty != empty_points:
                raise ValueError("empty-point index does not match the replayed board")
            if any(board[point] != "B" for point in dead_black):
                raise ValueError("dead-Black index does not identify a Black stone")
            if any(board[point] != "W" for point in dead_white):
                raise ValueError("dead-White index does not identify a White stone")
            if set(dead_black) & set(dead_white):
                raise ValueError("dead-Black and dead-White sets overlap")
            eligible = tuple(sorted(set(empty_points) | set(dead_black) | set(dead_white)))
            if set(empty_points) & (set(dead_black) | set(dead_white)):
                raise ValueError("empty points overlap dead-chain points")

            selected_blob = bytes(row["selected_labeling_blob"])
            selected_labels = _unpack_labels(
                selected_blob, eligible, "selected labeling"
            )
            if selected_labels is None:
                raise ValueError("selected labeling is unexpectedly absent")

            model_document = _object_json(str(row["model_json"]), "model_json")
            diagnostics = _object_json(str(row["diagnostics_json"]), "diagnostics_json")
            context = _object_json(str(row["context_json"]), "context_json")

            attempt: dict[str, Any] | None = None
            budget_json_raw: str | None = None
            solver_identity: dict[str, Any] | None = None
            if row["attempt_id"] is not None:
                budget_json_raw = str(row["budget_json"])
                budget = _object_json(budget_json_raw, "budget_json")
                initial_blob = (
                    None if row["initial_labeling_blob"] is None
                    else bytes(row["initial_labeling_blob"])
                )
                optimizer_blob = (
                    None if row["optimizer_labeling_blob"] is None
                    else bytes(row["optimizer_labeling_blob"])
                )
                initial_labels = _unpack_labels(
                    initial_blob, eligible, "initial heuristic labeling"
                )
                optimizer_labels = _unpack_labels(
                    optimizer_blob, eligible, "optimizer labeling"
                )
                attempt = {
                    "attempt_id": str(row["attempt_id"]),
                    "status": str(row["attempt_status"]),
                    "started_at_utc": str(row["started_at_utc"]),
                    "elapsed_wall_ns": int(row["elapsed_wall_ns"]),
                    "budget": budget,
                    "initial_disposition": str(row["initial_disposition"]),
                    "initial_labels": initial_labels,
                    "initial_objective": row["initial_objective"],
                    "optimizer_disposition": str(row["optimizer_disposition"]),
                    "optimizer_labels": optimizer_labels,
                    "optimizer_objective": row["optimizer_objective"],
                    "lower_bound_disposition": str(row["lower_bound_disposition"]),
                    "lower_bound": row["lower_bound"],
                    "absolute_gap": row["absolute_gap"],
                    "backend_reported_gap": row["backend_reported_gap"],
                    "backend_status": row["backend_status"],
                    "error": row["attempt_error"],
                    "raw_initial_labeling_blob": initial_blob,
                    "raw_optimizer_labeling_blob": optimizer_blob,
                }
                if row["solver_identity_id"] is not None:
                    solver_identity = {
                        "solver_identity_id": str(row["solver_identity_id"]),
                        "backend_name": str(row["backend_name"]),
                        "backend_version": str(row["backend_version"]),
                        "adapter_version": str(row["adapter_version"]),
                        "configuration": _object_json(
                            str(row["solver_configuration_json"]),
                            "solver_configuration_json",
                        ),
                        "configuration_fingerprint": str(
                            row["solver_configuration_fingerprint"]
                        ),
                    }

            result.append(ArchiveRow(
                observation_id=str(row["observation_id"]),
                evidence_id=str(row["evidence_id"]),
                model_id=str(row["model_id"]),
                attempt_id=(
                    None if row["observation_attempt_id"] is None
                    else str(row["observation_attempt_id"])
                ),
                observed_at_utc=str(row["observed_at_utc"]),
                turn_number=int(row["turn_number"]),
                is_during_search=bool(row["is_during_search"]),
                width=width,
                height=height,
                coordinate_convention=str(row["coordinate_convention"]),
                ownership=ownership,
                ownership_stdev=stdev,
                board=board,
                board_blob=board_blob,
                empty_points=empty_points,
                dead_black=dead_black,
                dead_white=dead_white,
                dead_chain_version=str(row["dead_chain_version"]),
                evidence_schema=str(row["evidence_schema"]),
                model_schema=str(row["model_schema"]),
                model_created_at_utc=str(row["model_created_at_utc"]),
                policy_id=str(row["policy_id"]),
                model_document=model_document,
                model_json_raw=str(row["model_json"]),
                diagnostics_json_raw=str(row["diagnostics_json"]),
                context_json_raw=str(row["context_json"]),
                selected_labels=selected_labels,
                diagnostics=diagnostics,
                context=context,
                attempt=attempt,
                budget_json_raw=budget_json_raw,
                solver_identity=solver_identity,
                solver_configuration_json_raw=(
                    None if row["solver_configuration_json"] is None
                    else str(row["solver_configuration_json"])
                ),
                raw_blobs={
                    "ownership": ownership_blob,
                    "ownership_stdev": stdev_blob,
                    "board": board_blob,
                    "empty_points": bytes(row["empty_points_blob"]),
                    "dead_black": bytes(row["dead_black_blob"]),
                    "dead_white": bytes(row["dead_white_blob"]),
                    "selected_labeling": selected_blob,
                    "initial_labeling": (
                        None if attempt is None
                        else attempt["raw_initial_labeling_blob"]
                    ),
                    "optimizer_labeling": (
                        None if attempt is None
                        else attempt["raw_optimizer_labeling_blob"]
                    ),
                },
            ))
        return result
    finally:
        connection.close()


def _coherent_config(row: ArchiveRow) -> dict[str, float] | None:
    if (
        row.model_document.get("objective_version") != OBJECTIVE_VERSION
        or row.coordinate_convention != COORDINATE_CONVENTION
    ):
        return None
    raw = row.model_document.get("policy_config")
    if not isinstance(raw, dict):
        return None
    values: dict[str, float] = {}
    for key in ("kappa", "beta", "score_tolerance"):
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        if not math.isfinite(float(value)):
            return None
        values[key] = float(value)
    return values


def _objective_key(row: ArchiveRow, config: dict[str, float]) -> str:
    document = {
        "objective_version": row.model_document["objective_version"],
        "coordinate_convention": row.coordinate_convention,
        "dead_chain_version": row.dead_chain_version,
        "policy_parameters": config,
        "request_metadata": row.model_document.get("request_metadata", {}),
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _evidence(row: ArchiveRow) -> OwnershipEvidence:
    return OwnershipEvidence(
        ownership=row.ownership,
        ownership_stdev=row.ownership_stdev,
        board=row.board,
        width=row.width,
        height=row.height,
        empty_points=row.empty_points,
        dead_black=row.dead_black,
        dead_white=row.dead_white,
        policy_metadata=row.model_document.get("request_metadata", {}),
    )


def _coherent_metrics(row: ArchiveRow) -> dict[str, Any] | None:
    config = _coherent_config(row)
    if config is None or row.ownership_stdev is None:
        return None
    eligible = tuple(sorted(
        set(row.empty_points) | set(row.dead_black) | set(row.dead_white)
    ))
    _neighbours, edges = ownership_graph(eligible, row.width, row.height)
    evidence = _evidence(row)
    energy = ownership_energy(
        row.selected_labels,
        evidence,
        row.empty_points,
        edges,
        config["kappa"],
        config["beta"],
    )
    empty_net = sum(row.selected_labels[point] for point in row.empty_points)
    expected_net = sum(row.ownership[point] for point in row.empty_points)
    count = len(row.empty_points)
    minimum_net = max(-count, math.ceil(expected_net - config["score_tolerance"] - 1e-9))
    maximum_net = min(count, math.floor(expected_net + config["score_tolerance"] + 1e-9))
    margins: list[float] = []
    for point in row.empty_points:
        mean = row.ownership[point]
        stdev = row.ownership_stdev[point]
        costs = sorted((
            0.0,
            1.0 - 2.0 * mean + 2.0 * config["kappa"] * stdev,
            1.0 + 2.0 * mean + 2.0 * config["kappa"] * stdev,
        ))
        margins.append(float(costs[1] - costs[0]))
    return {
        "objective_key": _objective_key(row, config),
        "energy": float(energy),
        "empty_net": int(empty_net),
        "expected_empty_net": float(expected_net),
        "minimum_empty_net": int(minimum_net),
        "maximum_empty_net": int(maximum_net),
        "aggregate_feasible": bool(minimum_net <= empty_net <= maximum_net),
        "unary_margin_fraction": (
            None if not margins
            else sum(margin <= 0.05 for margin in margins) / len(margins)
        ),
        "eligible_count": len(eligible),
        "edge_count": len(edges),
        "parameters": config,
    }


def _reference_candidates(row: ArchiveRow) -> Iterable[tuple[dict[int, int], bool, str]]:
    if row.attempt is not None:
        status = row.attempt["status"]
        optimizer = row.attempt["optimizer_labels"]
        if optimizer is not None:
            yield optimizer, status == "optimal", "optimizer_incumbent"
        initial = row.attempt["initial_labels"]
        if initial is not None:
            yield initial, False, "initial_heuristic"
    yield row.selected_labels, False, "selected_output"


def _reference_index(
    rows: Sequence[ArchiveRow],
) -> dict[tuple[str, str], dict[str, Any]]:
    references: dict[tuple[str, str], dict[str, Any]] = {}
    candidates_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        config = _coherent_config(row)
        if (
            row.policy_id not in COHERENT_POLICIES
            or config is None
            or row.ownership_stdev is None
        ):
            continue
        objective_key = _objective_key(row, config)
        metrics = _coherent_metrics(row)
        if metrics is None:
            continue
        eligible = tuple(sorted(
            set(row.empty_points) | set(row.dead_black) | set(row.dead_white)
        ))
        _neighbours, edges = ownership_graph(eligible, row.width, row.height)
        evidence = _evidence(row)
        for labels, proven_optimal, source in _reference_candidates(row):
            empty_net = sum(labels[point] for point in row.empty_points)
            expected_net = sum(row.ownership[point] for point in row.empty_points)
            n = len(row.empty_points)
            low = max(-n, math.ceil(expected_net - config["score_tolerance"] - 1e-9))
            high = min(n, math.floor(expected_net + config["score_tolerance"] + 1e-9))
            if not low <= empty_net <= high:
                continue
            candidate_energy = ownership_energy(
                labels, evidence, row.empty_points, edges,
                config["kappa"], config["beta"],
            )
            key = (row.evidence_id, objective_key)
            candidate = {
                "energy": float(candidate_energy),
                "optimal_proven": bool(proven_optimal),
                "source": source,
                "observation_id": row.observation_id,
            }
            candidates_by_key.setdefault(key, []).append(candidate)

    for key, candidates in candidates_by_key.items():
        proven = [
            candidate for candidate in candidates
            if candidate["optimal_proven"]
        ]
        comparison_pool = proven if proven else candidates
        best = min(comparison_pool, key=lambda item: float(item["energy"]))
        references[key] = {
            **best,
            "optimal_proven": bool(proven),
        }
    return references


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    return {
        "count": len(finite),
        "p50": _quantile(finite, 0.50),
        "p95": _quantile(finite, 0.95),
        "p99": _quantile(finite, 0.99),
        "mean": None if not finite else sum(finite) / len(finite),
    }


_ATTEMPT_STATUS_RANK = {
    "optimal": 7,
    "feasible": 6,
    "feasible_supported": 6,
    "feasible_repaired": 5,
    "seed_only": 4,
    "not_required": 7,
    "error": 1,
    "no_attempt": 0,
}


def _unique_problem_tally(
    rows: Sequence[ArchiveRow],
    references: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    """Count distinct evidence/objective problems, not repeated observations."""
    grouped: dict[tuple[str, str], list[ArchiveRow]] = defaultdict(list)
    for row in rows:
        config = _coherent_config(row)
        if (
            row.policy_id not in COHERENT_POLICIES
            or config is None
            or row.ownership_stdev is None
        ):
            continue
        key = (row.evidence_id, _objective_key(row, config))
        grouped[key].append(row)

    solve_states: Counter[str] = Counter()
    best_attempt_statuses: Counter[str] = Counter()
    reference_sources: Counter[str] = Counter()
    attempt_count = 0
    observation_count = 0
    for key, problem_rows in grouped.items():
        observation_count += len(problem_rows)
        statuses: list[str] = []
        for row in problem_rows:
            if row.attempt is not None:
                attempt_count += 1
                statuses.append(str(row.attempt.get("status", "error")))
        best_attempt = max(
            statuses or ["no_attempt"],
            key=lambda status: _ATTEMPT_STATUS_RANK.get(status, 0),
        )
        best_attempt_statuses[best_attempt] += 1

        reference = references.get(key)
        if reference is None:
            solve_states["no_feasible_witness"] += 1
            reference_sources["none"] += 1
        elif bool(reference["optimal_proven"]):
            solve_states["optimal_proven"] += 1
            reference_sources[str(reference["source"])] += 1
        else:
            solve_states["approximate_feasible"] += 1
            reference_sources[str(reference["source"])] += 1

    return {
        "identity": "distinct evidence_id plus coherent objective identity across coherent policies",
        "unique_problem_count": len(grouped),
        "observation_count": observation_count,
        "attempt_count": attempt_count,
        "solve_state_counts": dict(sorted(solve_states.items())),
        "best_attempt_status_counts": dict(sorted(best_attempt_statuses.items())),
        "best_reference_source_counts": dict(sorted(reference_sources.items())),
        "fully_solved_count": solve_states["optimal_proven"],
        "approximate_feasible_count": solve_states["approximate_feasible"],
        "no_feasible_witness_count": solve_states["no_feasible_witness"],
    }


def build_summary(rows: Sequence[ArchiveRow]) -> dict[str, Any]:
    """Summarize observed models, solver attempts, feasibility, and energy."""
    references = _reference_index(rows)
    grouped: dict[str, list[ArchiveRow]] = defaultdict(list)
    for row in rows:
        grouped[row.model_id].append(row)

    models: list[dict[str, Any]] = []
    for model_id, model_rows in sorted(grouped.items()):
        first = model_rows[0]
        solver_statuses: Counter[str] = Counter()
        elapsed_ms: list[float] = []
        feasible: list[float] = []
        energy_gaps: list[float] = []
        energy_ratios: list[float] = []
        margin_fractions: list[float] = []
        reference_count = 0
        for row in model_rows:
            status = (
                str(row.attempt["status"])
                if row.attempt is not None
                else str(row.diagnostics.get("solverStatus", "no_attempt"))
            )
            solver_statuses[status] += 1
            if row.attempt is not None:
                elapsed_ms.append(float(row.attempt["elapsed_wall_ns"]) / 1_000_000.0)
            metrics = _coherent_metrics(row)
            if metrics is None:
                continue
            feasible.append(1.0 if metrics["aggregate_feasible"] else 0.0)
            if metrics["unary_margin_fraction"] is not None:
                margin_fractions.append(float(metrics["unary_margin_fraction"]))
            key = (row.evidence_id, str(metrics["objective_key"]))
            reference = references.get(key)
            if reference is None:
                continue
            reference_count += 1
            gap = float(metrics["energy"]) - float(reference["energy"])
            energy_gaps.append(gap)
            if float(reference["energy"]) > 1e-12:
                energy_ratios.append(float(metrics["energy"]) / float(reference["energy"]))
        models.append({
            "model_id": model_id,
            "policy_id": first.policy_id,
            "model_document": first.model_document,
            "observation_count": len(model_rows),
            "distinct_evidence_count": len({row.evidence_id for row in model_rows}),
            "attempt_count": sum(row.attempt is not None for row in model_rows),
            "solver_status_counts": dict(sorted(solver_statuses.items())),
            "attempt_latency_ms": _distribution(elapsed_ms),
            "coherent_aggregate_feasibility_rate": (
                None if not feasible else sum(feasible) / len(feasible)
            ),
            "coherent_energy_gap": _distribution(energy_gaps),
            "coherent_energy_ratio": _distribution(energy_ratios),
            "reference_comparison_count": reference_count,
            "unary_margin_fraction": _distribution(margin_fractions),
        })
    return {
        "format": "kataproxy.ownership-corpus-summary.v1",
        "database_schema_version": DATABASE_VERSION,
        "observation_count": len(rows),
        "distinct_evidence_count": len({row.evidence_id for row in rows}),
        "model_count": len(grouped),
        "attempt_count": sum(row.attempt is not None for row in rows),
        "unique_problem_tally": _unique_problem_tally(rows, references),
        "reference_scope": (
            "best feasible witness recorded for the same evidence and matching "
            "coherent objective parameters; optimal_proven is not inferred from "
            "a feasible or repaired result"
        ),
        "models": models,
    }


def export_record(row: ArchiveRow) -> dict[str, Any]:
    """Return a lossless JSON-compatible representation of one observation."""
    attempt: dict[str, Any] | None = None
    if row.attempt is not None:
        attempt = {
            key: value
            for key, value in row.attempt.items()
            if not key.startswith("raw_")
        }
        attempt["raw_labeling_blobs_base64"] = {
            "initial": (
                None if row.raw_blobs["initial_labeling"] is None
                else base64.b64encode(row.raw_blobs["initial_labeling"] or b"").decode("ascii")
            ),
            "optimizer": (
                None if row.raw_blobs["optimizer_labeling"] is None
                else base64.b64encode(row.raw_blobs["optimizer_labeling"] or b"").decode("ascii")
            ),
        }
    return {
        "format": "kataproxy.ownership-observation.v1",
        "observation": {
            "observation_id": row.observation_id,
            "evidence_id": row.evidence_id,
            "model_id": row.model_id,
            "attempt_id": row.attempt_id,
            "observed_at_utc": row.observed_at_utc,
            "turn_number": row.turn_number,
            "is_during_search": row.is_during_search,
            "diagnostics": row.diagnostics,
            "diagnostics_json_raw": row.diagnostics_json_raw,
            "context": row.context,
            "context_json_raw": row.context_json_raw,
        },
        "evidence": {
            "schema": row.evidence_schema,
            "width": row.width,
            "height": row.height,
            "coordinate_convention": row.coordinate_convention,
            "codecs": {
                "ownership": FLOAT_CODEC,
                "ownership_stdev": FLOAT_CODEC,
                "board": BOARD_CODEC,
                "point_indexes": INDEX_CODEC,
                "labels": LABEL_CODEC,
            },
            "ownership": list(row.ownership),
            "ownership_stdev": (
                None if row.ownership_stdev is None
                else list(row.ownership_stdev)
            ),
            "board": list(row.board),
            "empty_points": list(row.empty_points),
            "dead_black": list(row.dead_black),
            "dead_white": list(row.dead_white),
            "dead_chain_version": row.dead_chain_version,
            "raw_blobs_base64": {
                key: (
                    None if value is None
                    else base64.b64encode(value).decode("ascii")
                )
                for key, value in row.raw_blobs.items()
                if key not in {"initial_labeling", "optimizer_labeling"}
            },
        },
        "model": {
            "schema": row.model_schema,
            "policy_id": row.policy_id,
            "created_at_utc": row.model_created_at_utc,
            "document": row.model_document,
            "model_json_raw": row.model_json_raw,
        },
        "selected_labeling": [[point, label] for point, label in row.selected_labels.items()],
        "solver_attempt": attempt,
        "budget_json_raw": row.budget_json_raw,
        "solver_identity": (
            None if row.solver_identity is None
            else {
                **row.solver_identity,
                "configuration_json_raw": row.solver_configuration_json_raw,
            }
        ),
    }


def _boundary_edges(
    labels: dict[int, int],
    edges: Sequence[tuple[int, int]],
) -> set[tuple[int, int]]:
    return {
        (first, second)
        for first, second in edges
        if labels[first] != labels[second]
    }


def _changed_components(
    changed: set[int],
    width: int,
    height: int,
) -> list[int]:
    remaining = set(changed)
    sizes: list[int] = []
    while remaining:
        start = remaining.pop()
        pending = [start]
        size = 0
        while pending:
            point = pending.pop()
            size += 1
            x = point % width
            neighbours = []
            if x > 0:
                neighbours.append(point - 1)
            if x + 1 < width:
                neighbours.append(point + 1)
            if point >= width:
                neighbours.append(point - width)
            if point + width < width * height:
                neighbours.append(point + width)
            for adjacent in neighbours:
                if adjacent in remaining:
                    remaining.remove(adjacent)
                    pending.append(adjacent)
        sizes.append(size)
    return sorted(sizes, reverse=True)


def _margin_fraction(row: ArchiveRow, config: dict[str, float], threshold: float) -> float | None:
    stdev = row.ownership_stdev
    if stdev is None or not row.empty_points:
        return None
    fragile = 0
    for point in row.empty_points:
        mean = row.ownership[point]
        uncertainty = stdev[point]
        costs = sorted((
            0.0,
            1.0 - 2.0 * mean + 2.0 * config["kappa"] * uncertainty,
            1.0 + 2.0 * mean + 2.0 * config["kappa"] * uncertainty,
        ))
        fragile += costs[1] - costs[0] <= threshold
    return fragile / len(row.empty_points)


def _sensitivity_pair(
    first: ArchiveRow,
    second: ArchiveRow,
    margin_threshold: float,
) -> dict[str, Any]:
    config = _coherent_config(first)
    first_stdev = first.ownership_stdev
    second_stdev = second.ownership_stdev
    if config is None or first_stdev is None or second_stdev is None:
        raise ValueError("sensitivity pair must have coherent objective maps with stdev")
    if (
        first.model_id != second.model_id
        or first.width != second.width
        or first.height != second.height
        or first.board_blob != second.board_blob
    ):
        raise ValueError("sensitivity pair does not share model and replayed board")

    mean_deltas = [
        after - before for before, after in zip(first.ownership, second.ownership)
    ]
    empty_mean_deltas = [
        second.ownership[point] - first.ownership[point]
        for point in first.empty_points
    ]
    ownership_l2 = math.sqrt(sum(value * value for value in mean_deltas))
    empty_mean_l2 = math.sqrt(
        sum(value * value for value in empty_mean_deltas)
    )
    stdev_deltas = [
        after - before
        for before, after in zip(first_stdev, second_stdev)
    ]
    common_eligible = tuple(sorted(
        set(first.selected_labels) & set(second.selected_labels)
    ))
    _neighbours, edges = ownership_graph(
        common_eligible, first.width, first.height
    )
    first_boundaries = _boundary_edges(first.selected_labels, edges)
    second_boundaries = _boundary_edges(second.selected_labels, edges)
    changed = {
        point for point in first.empty_points
        if first.selected_labels[point] != second.selected_labels[point]
    }
    components = _changed_components(changed, first.width, first.height)
    boundary_union = first_boundaries | second_boundaries
    touches_boundary = {
        point
        for edge in boundary_union
        for point in edge
        if point in changed
    }
    first_evidence = _evidence(first)
    second_evidence = _evidence(second)
    first_eligible = tuple(sorted(
        set(first.empty_points) | set(first.dead_black) | set(first.dead_white)
    ))
    second_eligible = tuple(sorted(
        set(second.empty_points) | set(second.dead_black) | set(second.dead_white)
    ))
    _first_neighbours, first_edges = ownership_graph(
        first_eligible, first.width, first.height
    )
    _second_neighbours, second_edges = ownership_graph(
        second_eligible, second.width, second.height
    )
    first_energy = ownership_energy(
        first.selected_labels, first_evidence, first.empty_points, first_edges,
        config["kappa"], config["beta"],
    )
    second_energy = ownership_energy(
        second.selected_labels, second_evidence, second.empty_points, second_edges,
        config["kappa"], config["beta"],
    )
    first_net = sum(first.selected_labels[point] for point in first.empty_points)
    second_net = sum(second.selected_labels[point] for point in second.empty_points)
    first_metrics = _coherent_metrics(first)
    second_metrics = _coherent_metrics(second)
    return {
        "model_id": first.model_id,
        "policy_id": first.policy_id,
        "first_observation_id": first.observation_id,
        "second_observation_id": second.observation_id,
        "first_evidence_id": first.evidence_id,
        "second_evidence_id": second.evidence_id,
        "same_evidence": first.evidence_id == second.evidence_id,
        "observed_at_utc": [first.observed_at_utc, second.observed_at_utc],
        "continuous_input_drift": {
            "ownership_mean_absolute": (
                None if not mean_deltas
                else sum(abs(value) for value in mean_deltas) / len(mean_deltas)
            ),
            "ownership_l2": ownership_l2,
            "ownership_linf": max((abs(value) for value in mean_deltas), default=0.0),
            "empty_points_mean_l2": empty_mean_l2,
            "stdev_l2": math.sqrt(sum(value * value for value in stdev_deltas)),
        },
        "output_sensitivity": {
            "empty_hamming_count": len(changed),
            "empty_hamming_fraction": (
                None if not first.empty_points
                else len(changed) / len(first.empty_points)
            ),
            "net_sum_first": first_net,
            "net_sum_second": second_net,
            "net_sum_jump": second_net - first_net,
            "boundary_edge_count": len(edges),
            "boundary_symmetric_difference_count": len(
                first_boundaries ^ second_boundaries
            ),
            "boundary_symmetric_difference_fraction": (
                None if not edges
                else len(first_boundaries ^ second_boundaries) / len(edges)
            ),
            "total_variation_first": sum(
                abs(first.selected_labels[a] - first.selected_labels[b])
                for a, b in edges
            ),
            "total_variation_second": sum(
                abs(second.selected_labels[a] - second.selected_labels[b])
                for a, b in edges
            ),
            "total_variation_absolute_delta": abs(
                sum(abs(second.selected_labels[a] - second.selected_labels[b]) for a, b in edges)
                - sum(abs(first.selected_labels[a] - first.selected_labels[b]) for a, b in edges)
            ),
            "changed_component_sizes": components,
            "changed_component_count": len(components),
            "largest_changed_component_fraction": (
                None if not changed or not first.empty_points
                else components[0] / len(first.empty_points)
            ),
            "changed_points_touching_boundary_fraction": (
                None if not changed
                else len(touches_boundary) / len(changed)
            ),
            "dead_black_disposition_changes": len(
                set(first.dead_black) ^ set(second.dead_black)
            ),
            "dead_white_disposition_changes": len(
                set(first.dead_white) ^ set(second.dead_white)
            ),
        },
        "energy_continuity": {
            "energy_first": first_energy,
            "energy_second": second_energy,
            "energy_delta": second_energy - first_energy,
            "absolute_energy_delta_per_empty_mean_l2": (
                None if empty_mean_l2 == 0.0
                else abs(second_energy - first_energy) / empty_mean_l2
            ),
        },
        "single_snapshot_diagnostics": {
            "unary_margin_threshold": margin_threshold,
            "first_unary_margin_fraction": _margin_fraction(
                first, config, margin_threshold
            ),
            "second_unary_margin_fraction": _margin_fraction(
                second, config, margin_threshold
            ),
            "first_aggregate_feasible": (
                None if first_metrics is None
                else first_metrics["aggregate_feasible"]
            ),
            "second_aggregate_feasible": (
                None if second_metrics is None
                else second_metrics["aggregate_feasible"]
            ),
        },
    }


def build_sensitivity(
    rows: Sequence[ArchiveRow],
    *,
    max_pairs: int = 1000,
    margin_threshold: float = 0.05,
    max_snapshots_per_group: int = 200,
) -> dict[str, Any]:
    """Rank nearest same-board coherent snapshots by continuous-map drift."""
    if max_pairs < 1:
        raise ValueError("max_pairs must be positive")
    if max_snapshots_per_group < 2:
        raise ValueError("max_snapshots_per_group must be at least two")
    if not math.isfinite(margin_threshold) or margin_threshold < 0:
        raise ValueError("margin_threshold must be finite and non-negative")

    groups: dict[tuple[str, int, int, bytes], list[ArchiveRow]] = defaultdict(list)
    for row in rows:
        if _coherent_config(row) is None or row.ownership_stdev is None:
            continue
        groups[(row.model_id, row.width, row.height, row.board_blob)].append(row)

    nearest: list[tuple[float, int, ArchiveRow, ArchiveRow]] = []
    sequence = 0
    candidate_count = 0
    sampled_group_count = 0
    sampled_snapshot_count = 0
    total_snapshot_count = sum(len(group_rows) for group_rows in groups.values())
    potential_pair_count = sum(
        len(group_rows) * (len(group_rows) - 1) // 2
        for group_rows in groups.values()
    )
    for group_rows in groups.values():
        ordered = sorted(
            group_rows,
            key=lambda row: (row.observed_at_utc, row.observation_id),
        )
        if len(ordered) > max_snapshots_per_group:
            last_index = len(ordered) - 1
            indexes = [
                round(index * last_index / (max_snapshots_per_group - 1))
                for index in range(max_snapshots_per_group)
            ]
            ordered = [ordered[index] for index in indexes]
            sampled_group_count += 1
            sampled_snapshot_count += len(group_rows) - len(ordered)
        for first_index in range(len(ordered)):
            for second_index in range(first_index + 1, len(ordered)):
                first = ordered[first_index]
                second = ordered[second_index]
                if first.observation_id == second.observation_id:
                    continue
                drift = math.sqrt(sum(
                    (after - before) * (after - before)
                    for before, after in zip(first.ownership, second.ownership)
                ))
                candidate_count += 1
                sequence += 1
                item = (-drift, sequence, first, second)
                if len(nearest) < max_pairs:
                    heapq.heappush(nearest, item)
                elif item[0] > nearest[0][0]:
                    heapq.heapreplace(nearest, item)

    selected = [
        _sensitivity_pair(item[2], item[3], margin_threshold)
        for item in nearest
    ]
    selected.sort(
        key=lambda pair: (
            float(pair["continuous_input_drift"]["ownership_l2"]),
            str(pair["first_observation_id"]),
            str(pair["second_observation_id"]),
        )
    )
    return {
        "format": "kataproxy.ownership-sensitivity.v1",
        "pair_scope": (
            "same exact replayed board, dimensions, and model identity only; "
            "the archive contains no game/session identifier, so pairs are not "
            "asserted to be same-game temporal sequences"
        ),
        "total_snapshot_count": total_snapshot_count,
        "sampled_snapshot_count": sampled_snapshot_count,
        "sampled_group_count": sampled_group_count,
        "max_snapshots_per_group": max_snapshots_per_group,
        "potential_pair_count": potential_pair_count,
        "snapshot_pair_count": candidate_count,
        "evaluated_pair_count": candidate_count,
        "returned_pair_count": len(selected),
        "pair_results_truncated": candidate_count > len(selected),
        "truncated": (
            candidate_count > len(selected) or sampled_snapshot_count > 0
        ),
        "margin_threshold": margin_threshold,
        "group_count": len(groups),
        "pairs": selected,
    }


def _write_json(path: str, value: Any) -> None:
    encoded = json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path == "-":
        sys.stdout.write(encoded)
    else:
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(encoded, encoding="utf-8")


def _write_jsonl(path: str, rows: Sequence[ArchiveRow]) -> None:
    if path == "-":
        output = sys.stdout
        for row in rows:
            output.write(
                json.dumps(export_record(row), sort_keys=True, separators=(",", ":"), allow_nan=False)
                + "\n"
            )
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(
                json.dumps(export_record(row), sort_keys=True, separators=(",", ":"), allow_nan=False)
                + "\n"
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        default=str(_default_database_path()),
        help="ownership SQLite database (default: configured corpus path)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary_parser = subparsers.add_parser("summary", help="summarize models and solver outcomes")
    summary_parser.add_argument("--output", default="-", help="JSON destination, or - for stdout")

    export_parser = subparsers.add_parser("export-jsonl", help="export lossless observation records")
    export_parser.add_argument("--output", default="-", help="JSONL destination, or - for stdout")

    sensitivity_parser = subparsers.add_parser(
        "sensitivity", help="rank same-board near-duplicate coherent snapshots"
    )
    sensitivity_parser.add_argument("--output", default="-", help="JSON destination, or - for stdout")
    sensitivity_parser.add_argument("--max-pairs", type=int, default=1000)
    sensitivity_parser.add_argument("--max-snapshots-per-group", type=int, default=200)
    sensitivity_parser.add_argument("--margin-threshold", type=float, default=0.05)

    args = parser.parse_args(argv)
    try:
        rows = load_archive(args.db)
        if args.command == "summary":
            _write_json(args.output, build_summary(rows))
        elif args.command == "export-jsonl":
            _write_jsonl(args.output, rows)
        elif args.command == "sensitivity":
            _write_json(
                args.output,
                build_sensitivity(
                    rows,
                    max_pairs=args.max_pairs,
                    margin_threshold=args.margin_threshold,
                    max_snapshots_per_group=args.max_snapshots_per_group,
                ),
            )
        else:
            parser.error(f"unknown command: {args.command}")
    except (OSError, sqlite3.Error, ValueError) as exc:
        parser.exit(2, f"ownership corpus analysis failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
