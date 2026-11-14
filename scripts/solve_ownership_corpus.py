#!/usr/bin/env python3
"""Offline iterative-deepening solver campaign for ownership evidence archives.

The source SQLite corpus is read-only. Campaign progress and final witnesses
are written to separate JSON files so a long run can be interrupted and
resumed without changing the live evidence archive.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from scripts.analyze_ownership_corpus import (
    ArchiveRow,
    _coherent_config,
    _default_database_path,
    _evidence,
    _objective_key,
    _reference_index,
    load_archive,
)
from transformers.stencil_cover.stencil_ownership import OwnershipEvidence
from transformers.stencil_cover.stencil_ownership_coherent import (
    CoherentOwnershipPolicy,
)
from transformers.stencil_cover.stencil_ownership_graphcut import ownership_graph
from transformers.stencil_cover.stencil_ownership_repair import ownership_energy
from transformers.stencil_cover.stencil_ownership_solver_api import (
    OwnershipLabeling,
    OwnershipSolverStatus,
)


_CHECKPOINT_FORMAT = "kataproxy.ownership-bulk-solve-checkpoint.v1"
_RESULTS_FORMAT = "kataproxy.ownership-bulk-solve-results.v1"
_DEFAULT_LEVELS_SECONDS = (10.0, 20.0, 40.0, 80.0, 160.0)
_DEFAULT_SLICE_SECONDS = 10.0
_NUMERIC_TOLERANCE = 1e-6


@dataclass(frozen=True)
class CampaignItem:
    task_id: str
    evidence_id: str
    objective_key: str
    evidence: OwnershipEvidence
    source_row: ArchiveRow
    policy_ids: tuple[str, ...]
    config: dict[str, float]
    eligible_points: tuple[int, ...]
    edges: tuple[tuple[int, int], ...]
    initial_labels: dict[int, int] | None
    initial_energy: float | None
    initial_lower_bound: float | None
    initial_reference_source: str | None


@dataclass(frozen=True)
class Campaign:
    items: tuple[CampaignItem, ...]
    unique_problem_count: int
    already_proven_count: int
    no_optimization_required_count: int
    omitted_by_limit_count: int = 0


@dataclass
class TaskProgress:
    spent_wall_ms: float = 0.0
    attempt_count: int = 0
    best_labels: dict[int, int] | None = None
    best_energy: float | None = None
    best_lower_bound: float | None = None
    optimal_proven: bool = False
    last_status: str = "not_started"
    last_error: str | None = None


@dataclass(frozen=True)
class SliceOutcome:
    elapsed_wall_ms: float
    labels: dict[int, int] | None
    energy: float | None
    lower_bound: float | None
    status: str
    error: str | None


SliceSolver = Callable[[CampaignItem, TaskProgress, int], SliceOutcome]


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be a finite number")
    return result


def _optional_finite_number(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    return _finite_number(value, field_name)


def _aggregate_interval(
    evidence: OwnershipEvidence,
    score_tolerance: float,
) -> tuple[int, int]:
    expected_net = sum(evidence.ownership[point] for point in evidence.empty_points)
    count = len(evidence.empty_points)
    low = max(-count, math.ceil(expected_net - score_tolerance - 1e-9))
    high = min(count, math.floor(expected_net + score_tolerance + 1e-9))
    if low > high:
        raise ValueError("ownership objective has no attainable aggregate interval")
    return low, high


def _evaluate_labels(
    evidence: OwnershipEvidence,
    config: dict[str, float],
    eligible_points: tuple[int, ...],
    edges: tuple[tuple[int, int], ...],
    labels: dict[int, int],
) -> float:
    if tuple(sorted(labels)) != eligible_points:
        raise ValueError("labeling does not cover the exact eligible-point set")
    if any(
        isinstance(label, bool) or label not in (-1, 0, 1)
        for label in labels.values()
    ):
        raise ValueError("labeling contains an invalid ternary label")
    if any(labels[point] != 1 for point in evidence.dead_black):
        raise ValueError("labeling disagrees with fixed dead-Black points")
    if any(labels[point] != -1 for point in evidence.dead_white):
        raise ValueError("labeling disagrees with fixed dead-White points")
    low, high = _aggregate_interval(evidence, config["score_tolerance"])
    empty_net = sum(labels[point] for point in evidence.empty_points)
    if not low <= empty_net <= high:
        raise ValueError("labeling violates the aggregate interval")
    return float(ownership_energy(
        labels,
        evidence,
        evidence.empty_points,
        edges,
        config["kappa"],
        config["beta"],
    ))


def _labels_for_reference(
    row: ArchiveRow,
    source: str,
) -> dict[int, int] | None:
    if source == "optimizer_incumbent":
        if row.attempt is None:
            return None
        labels = row.attempt.get("optimizer_labels")
        return None if labels is None else dict(labels)
    if source == "initial_heuristic":
        if row.attempt is None:
            return None
        labels = row.attempt.get("initial_labels")
        return None if labels is None else dict(labels)
    return dict(row.selected_labels)


def build_campaign(rows: Sequence[ArchiveRow]) -> Campaign:
    """Deduplicate unresolved coherent problems by evidence and objective."""
    references = _reference_index(rows)
    grouped: dict[tuple[str, str], list[ArchiveRow]] = defaultdict(list)
    for row in rows:
        config = _coherent_config(row)
        if (
            row.policy_id not in {"coherent", "coherent-cut"}
            or config is None
            or row.ownership_stdev is None
        ):
            continue
        key = (row.evidence_id, _objective_key(row, config))
        grouped[key].append(row)

    by_observation = {row.observation_id: row for row in rows}
    items: list[CampaignItem] = []
    proven_count = 0
    no_optimization_count = 0

    for key, problem_rows in sorted(grouped.items()):
        reference = references.get(key)
        if reference is not None and bool(reference["optimal_proven"]):
            proven_count += 1
            continue

        ordered_rows = sorted(
            problem_rows,
            key=lambda row: (row.observed_at_utc, row.observation_id),
        )
        source_row = ordered_rows[0]
        config = _coherent_config(source_row)
        if config is None:
            raise ValueError("coherent objective configuration disappeared")
        evidence = _evidence(source_row)

        if not evidence.empty_points:
            no_optimization_count += 1
            continue

        eligible = tuple(sorted(
            set(evidence.empty_points)
            | set(evidence.dead_black)
            | set(evidence.dead_white)
        ))
        _neighbours, edges = ownership_graph(
            eligible,
            evidence.width,
            evidence.height,
        )

        initial_labels: dict[int, int] | None = None
        initial_energy: float | None = None
        initial_source: str | None = None
        if reference is not None:
            reference_id = str(reference["observation_id"])
            reference_row = by_observation.get(reference_id)
            if reference_row is None:
                raise ValueError(
                    f"reference observation {reference_id} is absent from the archive"
                )
            initial_source = str(reference["source"])
            candidate = _labels_for_reference(reference_row, initial_source)
            if candidate is None:
                raise ValueError("reference row does not contain its selected witness")
            initial_energy = _evaluate_labels(
                evidence, config, eligible, edges, candidate
            )
            recorded_energy = _finite_number(
                reference["energy"],
                "reference energy",
            )
            if abs(initial_energy - recorded_energy) > _NUMERIC_TOLERANCE:
                raise ValueError("reference witness energy does not reproduce")
            initial_labels = candidate

        lower_bounds: list[float] = []
        for row in problem_rows:
            if row.attempt is None:
                continue
            bound = _optional_finite_number(
                row.attempt.get("lower_bound"),
                "archived lower bound",
            )
            if bound is not None:
                lower_bounds.append(bound)
        initial_lower_bound = max(lower_bounds) if lower_bounds else None
        if (
            initial_lower_bound is not None
            and initial_energy is not None
            and initial_lower_bound > initial_energy + _NUMERIC_TOLERANCE
        ):
            raise ValueError(
                "archived lower bound exceeds the best feasible witness for "
                f"evidence {source_row.evidence_id}"
            )

        identity_payload = json.dumps(
            [key[0], key[1]],
            separators=(",", ":"),
            ensure_ascii=True,
        )
        task_id = hashlib.sha256(identity_payload.encode("utf-8")).hexdigest()
        items.append(CampaignItem(
            task_id=task_id,
            evidence_id=source_row.evidence_id,
            objective_key=key[1],
            evidence=evidence,
            source_row=source_row,
            policy_ids=tuple(sorted({row.policy_id for row in problem_rows})),
            config=config,
            eligible_points=eligible,
            edges=edges,
            initial_labels=initial_labels,
            initial_energy=initial_energy,
            initial_lower_bound=initial_lower_bound,
            initial_reference_source=initial_source,
        ))

    items.sort(key=lambda item: (item.evidence_id, item.objective_key))
    return Campaign(
        items=tuple(items),
        unique_problem_count=len(grouped),
        already_proven_count=proven_count,
        no_optimization_required_count=no_optimization_count,
    )


def _initial_progress(item: CampaignItem) -> TaskProgress:
    return TaskProgress(
        best_labels=(
            None if item.initial_labels is None else dict(item.initial_labels)
        ),
        best_energy=item.initial_energy,
        best_lower_bound=item.initial_lower_bound,
        last_status=(
            "archive_incumbent"
            if item.initial_labels is not None
            else "no_feasible_archive_witness"
        ),
    )


def campaign_fingerprint(
    campaign: Campaign,
    levels_ms: Sequence[int],
    slice_ms: int,
) -> str:
    payload = {
        "checkpoint_format": _CHECKPOINT_FORMAT,
        "unique_problem_count": campaign.unique_problem_count,
        "already_proven_count": campaign.already_proven_count,
        "no_optimization_required_count": campaign.no_optimization_required_count,
        "levels_ms": list(levels_ms),
        "slice_ms": slice_ms,
        "items": [
            {
                "task_id": item.task_id,
                "evidence_id": item.evidence_id,
                "objective_key": item.objective_key,
                "initial_labels": (
                    None if item.initial_labels is None
                    else [[point, label] for point, label in sorted(item.initial_labels.items())]
                ),
                "initial_energy": item.initial_energy,
                "initial_lower_bound": item.initial_lower_bound,
                "initial_reference_source": item.initial_reference_source,
            }
            for item in campaign.items
        ],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _decode_checkpoint_labels(value: Any) -> dict[int, int] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("checkpoint labeling must be a list")
    result: dict[int, int] = {}
    for entry in value:
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError("checkpoint labeling entries must be point/label pairs")
        point, label = entry
        if (
            isinstance(point, bool)
            or not isinstance(point, int)
            or isinstance(label, bool)
            or not isinstance(label, int)
        ):
            raise ValueError("checkpoint labeling entries must contain integers")
        if point in result:
            raise ValueError("checkpoint labeling contains duplicate points")
        result[point] = label
    if tuple(sorted(result.items())) != tuple(
        (entry[0], entry[1]) for entry in value
    ):
        raise ValueError("checkpoint labeling points must be sorted")
    return result


def load_checkpoint(
    path: Path,
    campaign: Campaign,
    fingerprint: str,
) -> dict[str, TaskProgress]:
    """Load and validate a checkpoint, or seed progress from archive witnesses."""
    progress = {
        item.task_id: _initial_progress(item)
        for item in campaign.items
    }
    if not path.exists():
        return progress

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read campaign checkpoint: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("campaign checkpoint must contain a JSON object")
    if document.get("format") != _CHECKPOINT_FORMAT:
        raise ValueError("unsupported campaign checkpoint format")
    if document.get("campaign_fingerprint") != fingerprint:
        raise ValueError(
            "campaign checkpoint does not match this archive snapshot or budget "
            "schedule; use a new checkpoint path or remove the old checkpoint"
        )
    raw_tasks = document.get("tasks")
    if not isinstance(raw_tasks, dict):
        raise ValueError("campaign checkpoint tasks must be a JSON object")
    unknown_tasks = set(raw_tasks) - set(progress)
    if unknown_tasks:
        raise ValueError("campaign checkpoint contains tasks outside this campaign")

    items_by_id = {item.task_id: item for item in campaign.items}
    for task_id, state in progress.items():
        raw = raw_tasks.get(task_id)
        if raw is None:
            continue
        if not isinstance(raw, dict):
            raise ValueError(f"checkpoint task {task_id} must be an object")
        item = items_by_id[task_id]

        spent = _finite_number(raw.get("spent_wall_ms", 0.0), "spent_wall_ms")
        attempts_value = raw.get("attempt_count", 0)
        if (
            isinstance(attempts_value, bool)
            or not isinstance(attempts_value, int)
            or attempts_value < 0
            or spent < 0
        ):
            raise ValueError("checkpoint time and attempt count must be non-negative")

        labels = _decode_checkpoint_labels(raw.get("best_labels"))
        stored_energy = _optional_finite_number(
            raw.get("best_energy"),
            "checkpoint best_energy",
        )
        if labels is None:
            if stored_energy is not None:
                raise ValueError("checkpoint energy exists without a labeling")
        else:
            calculated_energy = _evaluate_labels(
                item.evidence,
                item.config,
                item.eligible_points,
                item.edges,
                labels,
            )
            if (
                stored_energy is None
                or abs(calculated_energy - stored_energy) > _NUMERIC_TOLERANCE
            ):
                raise ValueError("checkpoint witness energy does not reproduce")
            if state.best_energy is None or calculated_energy < state.best_energy:
                state.best_labels = labels
                state.best_energy = calculated_energy

        bound = _optional_finite_number(
            raw.get("best_lower_bound"),
            "checkpoint best_lower_bound",
        )
        if bound is not None and (
            state.best_lower_bound is None or bound > state.best_lower_bound
        ):
            state.best_lower_bound = bound
        if (
            state.best_lower_bound is not None
            and state.best_energy is not None
            and state.best_lower_bound > state.best_energy + _NUMERIC_TOLERANCE
        ):
            raise ValueError("checkpoint lower bound exceeds its best feasible energy")

        proven = raw.get("optimal_proven", False)
        if not isinstance(proven, bool):
            raise ValueError("checkpoint optimal_proven must be boolean")
        if proven:
            if (
                state.best_energy is None
                or state.best_lower_bound is None
                or state.best_lower_bound < state.best_energy - _NUMERIC_TOLERANCE
            ):
                raise ValueError("checkpoint claims proof without a closed objective gap")
        status = raw.get("last_status", "resumed")
        error = raw.get("last_error")
        if not isinstance(status, str) or (
            error is not None and not isinstance(error, str)
        ):
            raise ValueError("checkpoint status/error fields are invalid")

        state.spent_wall_ms = spent
        state.attempt_count = attempts_value
        state.optimal_proven = proven
        state.last_status = status
        state.last_error = error
    return progress


def _progress_document(
    campaign: Campaign,
    progress: dict[str, TaskProgress],
    fingerprint: str,
    levels_ms: Sequence[int],
    slice_ms: int,
) -> dict[str, Any]:
    return {
        "format": _CHECKPOINT_FORMAT,
        "campaign_fingerprint": fingerprint,
        "levels_ms": list(levels_ms),
        "slice_ms": slice_ms,
        "unique_problem_count": campaign.unique_problem_count,
        "already_proven_count": campaign.already_proven_count,
        "no_optimization_required_count": campaign.no_optimization_required_count,
        "tasks": {
            task_id: {
                "spent_wall_ms": state.spent_wall_ms,
                "attempt_count": state.attempt_count,
                "best_labels": (
                    None if state.best_labels is None
                    else [[point, label] for point, label in sorted(state.best_labels.items())]
                ),
                "best_energy": state.best_energy,
                "best_lower_bound": state.best_lower_bound,
                "optimal_proven": state.optimal_proven,
                "last_status": state.last_status,
                "last_error": state.last_error,
            }
            for task_id, state in sorted(progress.items())
        },
    }


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            json.dump(
                value,
                stream,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def save_checkpoint(
    path: Path,
    campaign: Campaign,
    progress: dict[str, TaskProgress],
    fingerprint: str,
    levels_ms: Sequence[int],
    slice_ms: int,
) -> None:
    _write_json_atomic(
        path,
        _progress_document(campaign, progress, fingerprint, levels_ms, slice_ms),
    )


def _solve_slice(
    item: CampaignItem,
    state: TaskProgress,
    time_limit_ms: int,
) -> SliceOutcome:
    started = time.perf_counter()
    warm_start = (
        None if state.best_labels is None
        else OwnershipLabeling(tuple(sorted(state.best_labels.items())))
    )
    policy = CoherentOwnershipPolicy(
        kappa=item.config["kappa"],
        beta=item.config["beta"],
        score_tolerance=item.config["score_tolerance"],
        time_limit_ms=time_limit_ms,
    )
    try:
        result = policy.classify(item.evidence, initial_labeling=warm_start)
    except Exception as exc:
        elapsed = (time.perf_counter() - started) * 1000.0
        return SliceOutcome(
            elapsed_wall_ms=max(elapsed, 1.0),
            labels=None,
            energy=None,
            lower_bound=None,
            status="error",
            error=str(exc) or type(exc).__name__,
        )

    record = result.solver_evidence
    if record is None:
        elapsed = (time.perf_counter() - started) * 1000.0
        status = str(result.diagnostics.get("solverStatus", "no_attempt"))
        return SliceOutcome(
            elapsed_wall_ms=max(elapsed, 1.0),
            labels=None,
            energy=None,
            lower_bound=None,
            status=status,
            error=None,
        )

    candidates = (
        record.optimizer_incumbent,
        record.selected_labeling,
        record.initial_heuristic,
    )
    best_labels: dict[int, int] | None = None
    best_energy: float | None = None
    for candidate in candidates:
        if candidate is None:
            continue
        decoded = dict(candidate.point_labels)
        energy = _evaluate_labels(
            item.evidence,
            item.config,
            item.eligible_points,
            item.edges,
            decoded,
        )
        if best_energy is None or energy < best_energy:
            best_labels = decoded
            best_energy = energy

    elapsed = (time.perf_counter() - started) * 1000.0
    recorded_elapsed = float(record.elapsed_wall_ns) / 1_000_000.0
    return SliceOutcome(
        elapsed_wall_ms=max(elapsed, recorded_elapsed, 1.0),
        labels=best_labels,
        energy=best_energy,
        lower_bound=record.lower_bound,
        status=record.status.value,
        error=record.error,
    )


def _apply_outcome(
    item: CampaignItem,
    state: TaskProgress,
    outcome: SliceOutcome,
) -> None:
    elapsed = _finite_number(outcome.elapsed_wall_ms, "slice elapsed time")
    if elapsed < 0:
        raise ValueError("slice elapsed time must be non-negative")
    state.spent_wall_ms += max(elapsed, 1.0)
    state.attempt_count += 1
    state.last_status = outcome.status
    state.last_error = outcome.error

    if outcome.labels is not None:
        energy = _evaluate_labels(
            item.evidence,
            item.config,
            item.eligible_points,
            item.edges,
            outcome.labels,
        )
        if outcome.energy is None or abs(energy - outcome.energy) > _NUMERIC_TOLERANCE:
            raise ValueError("solver slice returned a non-reproducible objective")
        if state.best_energy is None or energy < state.best_energy:
            state.best_labels = dict(outcome.labels)
            state.best_energy = energy
    elif outcome.energy is not None:
        raise ValueError("solver slice returned an objective without a labeling")

    if outcome.lower_bound is not None:
        lower_bound = _finite_number(outcome.lower_bound, "solver lower bound")
        if (
            state.best_lower_bound is None
            or lower_bound > state.best_lower_bound
        ):
            state.best_lower_bound = lower_bound

    if outcome.status == OwnershipSolverStatus.OPTIMAL.value:
        if state.best_energy is None:
            raise ValueError("optimal solver status did not produce a feasible witness")
        state.optimal_proven = True
        if (
            state.best_lower_bound is None
            or state.best_lower_bound < state.best_energy - _NUMERIC_TOLERANCE
        ):
            state.best_lower_bound = state.best_energy

    if (
        state.best_lower_bound is not None
        and state.best_energy is not None
        and state.best_lower_bound > state.best_energy + _NUMERIC_TOLERANCE
    ):
        raise ValueError("solver lower bound exceeds the retained feasible energy")


def _state_counts(
    campaign: Campaign,
    progress: dict[str, TaskProgress],
) -> dict[str, int]:
    states = tuple(progress.values())
    return {
        "scheduled_count": len(campaign.items),
        "optimal_proven_count": sum(state.optimal_proven for state in states),
        "approximate_feasible_count": sum(
            state.best_labels is not None and not state.optimal_proven
            for state in states
        ),
        "no_feasible_witness_count": sum(
            state.best_labels is None and not state.optimal_proven
            for state in states
        ),
        "error_count": sum(state.last_status == "error" for state in states),
        "attempt_count": sum(state.attempt_count for state in states),
    }


def _progress_line(
    campaign: Campaign,
    progress: dict[str, TaskProgress],
    level_seconds: float,
    elapsed_seconds: float,
    prefix: str,
) -> None:
    counts = _state_counts(campaign, progress)
    gaps = [
        state.best_energy - state.best_lower_bound
        for state in progress.values()
        if state.best_energy is not None and state.best_lower_bound is not None
    ]
    best_gap = min(gaps) if gaps else None
    worst_gap = max(gaps) if gaps else None
    gap_text = (
        "gap n/a"
        if best_gap is None or worst_gap is None
        else f"known absolute gaps {best_gap:.6g}..{worst_gap:.6g}"
    )
    print(
        f"[{time.strftime('%H:%M:%S')}] {prefix} "
        f"level={level_seconds:g}s elapsed={elapsed_seconds:.1f}s "
        f"optimal={counts['optimal_proven_count']}/"
        f"{counts['scheduled_count']} "
        f"approximate={counts['approximate_feasible_count']} "
        f"no-witness={counts['no_feasible_witness_count']} "
        f"errors={counts['error_count']} attempts={counts['attempt_count']} "
        f"{gap_text}",
        flush=True,
    )


def run_campaign(
    campaign: Campaign,
    progress: dict[str, TaskProgress],
    *,
    levels_ms: Sequence[int],
    slice_ms: int,
    checkpoint_path: Path,
    fingerprint: str,
    progress_interval_seconds: float,
    solver: SliceSolver = _solve_slice,
) -> None:
    """Run level-synchronous rounds, checkpointing after every bounded slice."""
    started = time.monotonic()
    last_report = started
    _progress_line(
        campaign,
        progress,
        0.0,
        0.0,
        "campaign started",
    )

    for target_ms in levels_ms:
        target_seconds = target_ms / 1000.0
        while True:
            pending = [
                item for item in campaign.items
                if (
                    not progress[item.task_id].optimal_proven
                    and progress[item.task_id].spent_wall_ms < target_ms
                )
            ]
            if not pending:
                break

            for item in pending:
                state = progress[item.task_id]
                remaining_ms = target_ms - state.spent_wall_ms
                budget_ms = max(1, min(slice_ms, math.ceil(remaining_ms)))
                outcome = solver(item, state, budget_ms)
                _apply_outcome(item, state, outcome)
                save_checkpoint(
                    checkpoint_path,
                    campaign,
                    progress,
                    fingerprint,
                    levels_ms,
                    slice_ms,
                )

                now = time.monotonic()
                if now - last_report >= progress_interval_seconds:
                    _progress_line(
                        campaign,
                        progress,
                        target_seconds,
                        now - started,
                        "progress",
                    )
                    last_report = now

        _progress_line(
            campaign,
            progress,
            target_seconds,
            time.monotonic() - started,
            "level complete",
        )

    save_checkpoint(
        checkpoint_path,
        campaign,
        progress,
        fingerprint,
        levels_ms,
        slice_ms,
    )


def _results_document(
    database: Path,
    campaign: Campaign,
    progress: dict[str, TaskProgress],
    fingerprint: str,
    levels_ms: Sequence[int],
    slice_ms: int,
    *,
    interrupted: bool,
) -> dict[str, Any]:
    instances: list[dict[str, Any]] = []
    for item in campaign.items:
        state = progress[item.task_id]
        if state.optimal_proven:
            final_status = "optimal_proven"
        elif state.best_labels is not None:
            final_status = "approximate_feasible"
        else:
            final_status = "no_feasible_witness"
        absolute_gap = (
            None
            if state.best_energy is None or state.best_lower_bound is None
            else max(0.0, state.best_energy - state.best_lower_bound)
        )
        relative_gap = (
            None
            if absolute_gap is None
            or state.best_energy is None
            or abs(state.best_energy) <= 1e-12
            else absolute_gap / abs(state.best_energy)
        )
        instances.append({
            "task_id": item.task_id,
            "evidence_id": item.evidence_id,
            "objective_key": item.objective_key,
            "observed_policy_ids": list(item.policy_ids),
            "parameters": item.config,
            "initial_reference_source": item.initial_reference_source,
            "best_energy": state.best_energy,
            "best_lower_bound": state.best_lower_bound,
            "absolute_gap": absolute_gap,
            "relative_gap": relative_gap,
            "status": final_status,
            "last_solver_status": state.last_status,
            "last_error": state.last_error,
            "spent_wall_ms": state.spent_wall_ms,
            "attempt_count": state.attempt_count,
            "best_labels": (
                None if state.best_labels is None
                else [[point, label] for point, label in sorted(state.best_labels.items())]
            ),
        })

    return {
        "format": _RESULTS_FORMAT,
        "database": str(database),
        "campaign_fingerprint": fingerprint,
        "interrupted": interrupted,
        "levels_seconds": [value / 1000.0 for value in levels_ms],
        "slice_seconds": slice_ms / 1000.0,
        "unique_problem_count": campaign.unique_problem_count,
        "already_proven_count": campaign.already_proven_count,
        "no_optimization_required_count": campaign.no_optimization_required_count,
        "omitted_by_limit_count": campaign.omitted_by_limit_count,
        "counts": _state_counts(campaign, progress),
        "instances": instances,
    }


def _parse_levels(value: str) -> tuple[float, ...]:
    try:
        levels = tuple(float(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("levels must be comma-separated seconds") from exc
    if not levels:
        raise argparse.ArgumentTypeError("at least one budget level is required")
    if any(not math.isfinite(level) or level <= 0 for level in levels):
        raise argparse.ArgumentTypeError("budget levels must be finite and positive")
    if any(after <= before for before, after in zip(levels, levels[1:])):
        raise argparse.ArgumentTypeError("budget levels must be strictly increasing")
    return levels


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Iteratively deepen exact coherent MIP solves for unique approximate "
            "ownership evidence problems. The source SQLite archive is read-only."
        )
    )
    parser.add_argument(
        "--db",
        default=str(_default_database_path()),
        help="ownership evidence SQLite database",
    )
    parser.add_argument(
        "--levels-seconds",
        type=_parse_levels,
        default=_DEFAULT_LEVELS_SECONDS,
        help="cumulative per-instance limits (default: 10,20,40,80,160)",
    )
    parser.add_argument(
        "--slice-seconds",
        type=float,
        default=_DEFAULT_SLICE_SECONDS,
        help="maximum duration of one solver call (capped at 10 seconds)",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=10.0,
        help="minimum interval between progress summaries",
    )
    parser.add_argument(
        "--checkpoint",
        default=None,
        help="checkpoint JSON path (default: next to the archive)",
    )
    parser.add_argument(
        "--results",
        default=None,
        help="final results JSON path (default: next to the archive)",
    )
    parser.add_argument(
        "--max-instances",
        type=int,
        default=None,
        help="optional deterministic limit for a trial campaign",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="inspect deduplicated campaign size without solving",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    database = Path(args.db).expanduser().resolve()

    try:
        levels_seconds = tuple(args.levels_seconds)
        slice_seconds = _finite_number(args.slice_seconds, "slice-seconds")
        report_interval = _finite_number(
            args.progress_interval_seconds,
            "progress-interval-seconds",
        )
        if slice_seconds <= 0 or report_interval <= 0:
            raise ValueError("slice and progress intervals must be positive")
        if slice_seconds > _DEFAULT_SLICE_SECONDS:
            raise ValueError("slice-seconds cannot exceed 10 seconds")
        levels_ms = tuple(max(1, round(value * 1000.0)) for value in levels_seconds)
        slice_ms = max(1, round(slice_seconds * 1000.0))

        rows = load_archive(database)
        campaign = build_campaign(rows)
        if args.max_instances is not None:
            if (
                isinstance(args.max_instances, bool)
                or not isinstance(args.max_instances, int)
                or args.max_instances < 1
            ):
                raise ValueError("max-instances must be a positive integer")
            selected = campaign.items[:args.max_instances]
            campaign = Campaign(
                items=selected,
                unique_problem_count=campaign.unique_problem_count,
                already_proven_count=campaign.already_proven_count,
                no_optimization_required_count=campaign.no_optimization_required_count,
                omitted_by_limit_count=len(campaign.items) - len(selected),
            )

        checkpoint_path = (
            Path(args.checkpoint).expanduser()
            if args.checkpoint is not None
            else database.with_name(database.name + ".bulk-solve.checkpoint.json")
        )
        results_path = (
            Path(args.results).expanduser()
            if args.results is not None
            else database.with_name(database.name + ".bulk-solve.results.json")
        )
        fingerprint = campaign_fingerprint(campaign, levels_ms, slice_ms)

        if args.plan_only:
            print(json.dumps({
                "database": str(database),
                "unique_problem_count": campaign.unique_problem_count,
                "already_proven_count": campaign.already_proven_count,
                "no_optimization_required_count": campaign.no_optimization_required_count,
                "scheduled_count": len(campaign.items),
                "omitted_by_limit_count": campaign.omitted_by_limit_count,
                "budget_levels_seconds": levels_seconds,
                "slice_seconds": slice_ms / 1000.0,
                "campaign_fingerprint": fingerprint,
            }, indent=2, sort_keys=True))
            return 0

        progress = load_checkpoint(checkpoint_path, campaign, fingerprint)
        try:
            run_campaign(
                campaign,
                progress,
                levels_ms=levels_ms,
                slice_ms=slice_ms,
                checkpoint_path=checkpoint_path,
                fingerprint=fingerprint,
                progress_interval_seconds=report_interval,
            )
        except KeyboardInterrupt:
            save_checkpoint(
                checkpoint_path,
                campaign,
                progress,
                fingerprint,
                levels_ms,
                slice_ms,
            )
            _write_json_atomic(
                results_path,
                _results_document(
                    database,
                    campaign,
                    progress,
                    fingerprint,
                    levels_ms,
                    slice_ms,
                    interrupted=True,
                ),
            )
            print(
                f"Interrupted safely. Checkpoint: {checkpoint_path}. "
                f"Resume with the same command and budget schedule.",
                file=sys.stderr,
                flush=True,
            )
            return 130

        _write_json_atomic(
            results_path,
            _results_document(
                database,
                campaign,
                progress,
                fingerprint,
                levels_ms,
                slice_ms,
                interrupted=False,
            ),
        )
        counts = _state_counts(campaign, progress)
        print(
            f"Campaign complete: optimal={counts['optimal_proven_count']}, "
            f"approximate={counts['approximate_feasible_count']}, "
            f"no-witness={counts['no_feasible_witness_count']}. "
            f"Results: {results_path}",
            flush=True,
        )
        return 0
    except (OSError, ValueError) as exc:
        parser.exit(2, f"ownership bulk solve failed: {exc}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
