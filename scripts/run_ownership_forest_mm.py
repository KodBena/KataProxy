#!/usr/bin/env python3
"""Run the offline cardinality-aware forest/MM ownership experiment.

The evidence archive is opened by the existing read-only analyzer. Results are
written to a separate JSON artifact; this command never writes solver witnesses
or experiment state into the source archive.
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.analyze_ownership_corpus import (
    ArchiveRow,
    COHERENT_POLICIES,
    _coherent_config,
    _distribution,
    _evidence,
    _objective_key,
    _reference_candidates,
    _reference_index,
    _default_database_path,
    load_archive,
)
from transformers.stencil_cover.stencil_ownership_forest import (
    budget_exact_marginal_labeling,
    build_reduced_problem,
    forest_majorize_minimize,
    full_energy_from_reduction,
    solve_forest_dp,
    spanning_forest_edges,
)
from transformers.stencil_cover.stencil_ownership_graphcut import ownership_graph
from transformers.stencil_cover.stencil_ownership_repair import ownership_energy


RESULT_FORMAT = "kataproxy.ownership-forest-mm-results.v1"
ALGORITHM_VERSION = "cardinality-forest-mm-v1"
DEFAULT_SPLIT_SEED = "ownership-forest-mm-holdout-v1"
DEFAULT_MAX_PASSES = 8


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _board_signature(row: ArchiveRow) -> str:
    header = (
        f"{row.width}:{row.height}:{row.coordinate_convention}:"
    ).encode("ascii")
    return hashlib.sha256(header + row.board_blob).hexdigest()


def _split_for_board(signature: str, seed: str) -> str:
    digest = hashlib.sha256(f"{seed}:{signature}".encode("utf-8")).digest()
    bucket = int.from_bytes(digest[:8], "big") % 100
    if bucket < 70:
        return "train"
    if bucket < 85:
        return "validation"
    return "test"


def _reference_labels(
    row: ArchiveRow,
    reference_source: str,
) -> dict[int, int]:
    for labels, _proven, source in _reference_candidates(row):
        if source == reference_source:
            return dict(labels)
    raise ValueError(
        f"reference source {reference_source!r} is absent from observation "
        f"{row.observation_id}"
    )


def _strongest_lower_bound(rows: Sequence[ArchiveRow]) -> float | None:
    bounds: list[float] = []
    for row in rows:
        if row.attempt is None:
            continue
        value = row.attempt.get("lower_bound")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        numeric = float(value)
        if math.isfinite(numeric):
            bounds.append(numeric)
    return None if not bounds else max(bounds)


def _archive_labeling(
    empty_labels: Mapping[int, int],
    row: ArchiveRow,
) -> dict[int, int]:
    return {
        **{point: 1 for point in row.dead_black},
        **{point: -1 for point in row.dead_white},
        **dict(empty_labels),
    }


def _encode_labeling(labels: Mapping[int, int]) -> str:
    ordered = tuple(sorted(labels.items()))
    payload = struct.pack("<I", len(ordered)) + b"".join(
        struct.pack("<Ib", point, label)
        for point, label in ordered
    )
    return base64.b64encode(payload).decode("ascii")


def _boundary_edges(
    labels: Mapping[int, int],
    edges: Sequence[tuple[int, int]],
) -> set[tuple[int, int]]:
    return {
        edge for edge in edges
        if labels[edge[0]] != labels[edge[1]]
    }


def _method_metrics(
    candidate: Mapping[int, int],
    candidate_energy: float,
    reference: Mapping[int, int],
    reference_energy: float,
    reference_is_optimal: bool,
    reference_lower_bound: float | None,
    empty_points: Sequence[int],
    all_edges: Sequence[tuple[int, int]],
) -> dict[str, Any]:
    empty_changes = sum(
        candidate[point] != reference[point]
        for point in empty_points
    )
    reference_boundaries = _boundary_edges(reference, all_edges)
    candidate_boundaries = _boundary_edges(candidate, all_edges)
    result: dict[str, Any] = {
        "full_energy": float(candidate_energy),
        "energy_difference_vs_reference": float(candidate_energy - reference_energy),
        "reference_is_proven_optimal": bool(reference_is_optimal),
        "empty_hamming_count": int(empty_changes),
        "empty_hamming_fraction": (
            0.0 if not empty_points else empty_changes / len(empty_points)
        ),
        "boundary_symmetric_difference": len(
            reference_boundaries ^ candidate_boundaries
        ),
    }
    if reference_is_optimal:
        result["additive_optimality_gap"] = max(
            0.0, float(candidate_energy - reference_energy),
        )
    else:
        result["difference_vs_unproven_incumbent"] = float(
            candidate_energy - reference_energy
        )
    if reference_lower_bound is not None:
        result["reference_lower_bound"] = float(reference_lower_bound)
        result["additive_gap_upper_bound"] = max(
            0.0, float(candidate_energy - reference_lower_bound),
        )
    return result


def _distribution_summary(values: Sequence[float]) -> dict[str, Any]:
    return _distribution(values)


def build_results(
    rows: Sequence[ArchiveRow],
    *,
    split: str,
    split_seed: str,
    maximum_passes: int,
    limit: int | None,
) -> dict[str, Any]:
    """Evaluate each unique evidence/objective problem once."""
    references = _reference_index(rows)
    rows_by_observation = {row.observation_id: row for row in rows}
    grouped: dict[tuple[str, str], list[ArchiveRow]] = defaultdict(list)
    for row in rows:
        config = _coherent_config(row)
        if (
            row.policy_id not in COHERENT_POLICIES
            or config is None
            or row.ownership_stdev is None
        ):
            continue
        grouped[(row.evidence_id, _objective_key(row, config))].append(row)

    prepared: list[tuple[tuple[str, str], list[ArchiveRow], dict[str, Any], str]] = []
    excluded_split_counts: Counter[str] = Counter()
    missing_reference_count = 0
    for key, problem_rows in sorted(grouped.items()):
        reference = references.get(key)
        if reference is None:
            missing_reference_count += 1
            continue
        representative = min(
            problem_rows,
            key=lambda row: (row.observed_at_utc, row.observation_id),
        )
        board_signature = _board_signature(representative)
        board_split = _split_for_board(board_signature, split_seed)
        excluded_split_counts[board_split] += 1
        if split != "all" and split != board_split:
            continue
        prepared.append((key, problem_rows, reference, board_split))

    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        prepared = prepared[:limit]

    records: list[dict[str, Any]] = []
    method_times: dict[str, list[float]] = {
        "marginal_initializer": [],
        "forest_dp": [],
        "forest_mm_total": [],
    }
    method_gaps: dict[str, list[float]] = {
        "marginal_initializer": [],
        "forest_dp": [],
        "forest_mm": [],
    }
    reference_kind_counts: Counter[str] = Counter()
    board_group_signatures: set[str] = set()

    for key, problem_rows, reference, board_split in prepared:
        reference_observation = str(reference["observation_id"])
        reference_row = rows_by_observation.get(reference_observation)
        if reference_row is None:
            raise ValueError(
                f"reference observation {reference_observation} is absent"
            )
        representative = min(
            problem_rows,
            key=lambda row: (row.observed_at_utc, row.observation_id),
        )
        config = _coherent_config(representative)
        if config is None:
            raise ValueError("coherent objective configuration disappeared")
        evidence = _evidence(representative)
        problem = build_reduced_problem(
            evidence,
            kappa=config["kappa"],
            beta=config["beta"],
            score_tolerance=config["score_tolerance"],
        )
        eligible = tuple(sorted(
            set(representative.empty_points)
            | set(representative.dead_black)
            | set(representative.dead_white)
        ))
        _neighbours, all_edges = ownership_graph(
            eligible,
            representative.width,
            representative.height,
        )
        reference_labels = _reference_labels(
            reference_row,
            str(reference["source"]),
        )
        if tuple(sorted(reference_labels)) != eligible:
            raise ValueError("reference labeling does not cover the eligible graph")
        reference_energy = float(reference["energy"])
        lower_bound = _strongest_lower_bound(problem_rows)
        reference_is_proven = bool(reference["optimal_proven"])
        if lower_bound is not None and lower_bound > reference_energy + 1e-6:
            raise ValueError("archived lower bound exceeds best feasible reference energy")

        start = time.perf_counter_ns()
        marginal_empty = budget_exact_marginal_labeling(problem)
        marginal_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        marginal_full = _archive_labeling(marginal_empty, representative)
        marginal_energy = ownership_energy(
            marginal_full,
            evidence,
            representative.empty_points,
            all_edges,
            config["kappa"],
            config["beta"],
        )
        if abs(
            marginal_energy - full_energy_from_reduction(problem, marginal_empty)
        ) > 1e-6:
            raise ArithmeticError("marginal candidate failed full-energy cross-check")

        forest = spanning_forest_edges(problem)
        start = time.perf_counter_ns()
        forest_result = solve_forest_dp(problem, forest)
        forest_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        forest_empty = dict(forest_result.labeling)
        forest_full = _archive_labeling(forest_empty, representative)
        forest_energy = ownership_energy(
            forest_full,
            evidence,
            representative.empty_points,
            all_edges,
            config["kappa"],
            config["beta"],
        )
        if abs(
            forest_energy - full_energy_from_reduction(problem, forest_empty)
        ) > 1e-6:
            raise ArithmeticError("forest candidate failed full-energy cross-check")

        start = time.perf_counter_ns()
        mm_result = forest_majorize_minimize(
            problem,
            forest,
            maximum_passes=maximum_passes,
            initial_labeling=marginal_empty,
        )
        mm_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        mm_empty = dict(mm_result.labeling)
        mm_full = _archive_labeling(mm_empty, representative)
        mm_energy = ownership_energy(
            mm_full,
            evidence,
            representative.empty_points,
            all_edges,
            config["kappa"],
            config["beta"],
        )
        if abs(mm_energy - full_energy_from_reduction(problem, mm_empty)) > 1e-6:
            raise ArithmeticError("MM candidate failed full-energy cross-check")
        if mm_energy > marginal_energy + 1e-7:
            raise ArithmeticError("MM result is worse than its feasible initializer")

        for name, elapsed in (
            ("marginal_initializer", marginal_ms),
            ("forest_dp", forest_ms),
            ("forest_mm_total", marginal_ms + mm_ms),
        ):
            method_times[name].append(elapsed)
        method_results = {
            "marginal_initializer": (marginal_full, marginal_energy),
            "forest_dp": (forest_full, forest_energy),
            "forest_mm": (mm_full, mm_energy),
        }
        methods: dict[str, Any] = {}
        for name, (labels, energy) in method_results.items():
            metrics = _method_metrics(
                labels,
                energy,
                reference_labels,
                reference_energy,
                reference_is_proven,
                lower_bound,
                representative.empty_points,
                all_edges,
            )
            metrics["labeling_codec"] = "point-u32le-label-i8-v1"
            metrics["labeling_base64"] = _encode_labeling(labels)
            methods[name] = metrics
            if reference_is_proven:
                gap = metrics.get("additive_optimality_gap")
                if isinstance(gap, (int, float)):
                    method_gaps[name].append(float(gap))

        reference_kind = (
            "proven_optimum" if reference_is_proven
            else "best_feasible_unproven_incumbent"
        )
        reference_kind_counts[reference_kind] += 1
        board_group = _board_signature(representative)
        board_group_signatures.add(board_group)
        records.append({
            "task_id": hashlib.sha256(
                json.dumps([key[0], key[1]], separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
            "evidence_id": key[0],
            "objective_key": key[1],
            "board_signature": board_group,
            "split": board_split,
            "board_width": representative.width,
            "board_height": representative.height,
            "empty_point_count": len(representative.empty_points),
            "eligible_point_count": len(eligible),
            "empty_edge_count": len(problem.edges),
            "forest_edge_count": len(forest),
            "omitted_edge_count": len(problem.edges) - len(forest),
            "objective_parameters": {
                "kappa": config["kappa"],
                "beta": config["beta"],
                "score_tolerance": config["score_tolerance"],
            },
            "aggregate_interval": [
                problem.aggregate_lower,
                problem.aggregate_upper,
            ],
            "reference": {
                "kind": reference_kind,
                "observation_id": reference_observation,
                "source": reference["source"],
                "energy": reference_energy,
                "lower_bound": lower_bound,
            },
            "timing_ms": {
                "marginal_initializer": marginal_ms,
                "forest_dp": forest_ms,
                "forest_mm_passes": mm_ms,
                "forest_mm_total_including_initializer": marginal_ms + mm_ms,
            },
            "forest_dp": {
                "surrogate_energy": forest_result.surrogate_energy,
                "selected_sum": forest_result.selected_sum,
                "component_count": forest_result.component_count,
                "dp_state_count": forest_result.dp_state_count,
            },
            "forest_mm": {
                "initial_energy": mm_result.initial_energy,
                "final_energy": mm_result.energy,
                "pass_count": len(mm_result.iterations),
                "iterations": [
                    {
                        "pass": item.pass_number,
                        "surrogate_energy": item.surrogate_energy,
                        "surrogate_at_reference": item.surrogate_at_reference,
                        "original_energy_before": item.original_energy_before,
                        "original_energy_after": item.original_energy_after,
                        "dp_state_count": item.dp_state_count,
                    }
                    for item in mm_result.iterations
                ],
                "omitted_edge_tv_energy": mm_result.omitted_edge_tv_energy,
            },
            "methods": methods,
        })

    summary: dict[str, Any] = {
        "format": RESULT_FORMAT,
        "algorithm_version": ALGORITHM_VERSION,
        "split_seed": split_seed,
        "requested_split": split,
        "split_assignment": {
            "train": "sha256(seed + board_signature) mod 100 in [0, 69]",
            "validation": "sha256(seed + board_signature) mod 100 in [70, 84]",
            "test": "sha256(seed + board_signature) mod 100 in [85, 99]",
            "grouping": "board dimensions + coordinate convention + exact board-state bytes",
        },
        "maximum_mm_passes": maximum_passes,
        "source_observation_count": len(rows),
        "unique_coherent_problem_count": len(grouped),
        "problems_missing_feasible_reference": missing_reference_count,
        "problem_count_by_split_before_filter": dict(sorted(excluded_split_counts.items())),
        "evaluated_problem_count": len(records),
        "evaluated_board_group_count": len(board_group_signatures),
        "reference_kind_counts": dict(sorted(reference_kind_counts.items())),
        "method_wall_time_ms": {
            name: _distribution_summary(values)
            for name, values in method_times.items()
        },
        "proven_reference_additive_gap": {
            name: _distribution_summary(values)
            for name, values in method_gaps.items()
        },
        "records": records,
    }
    return summary


def _default_output_path(database: Path) -> Path:
    return database.with_name(database.name + ".forest-mm.results.json")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=_default_database_path(),
        help="read-only ownership evidence SQLite database",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="separate JSON results path (refuses to overwrite by default)",
    )
    parser.add_argument(
        "--split",
        choices=("train", "validation", "test", "all"),
        default="test",
        help="board-grouped evaluation split (default: held-out test)",
    )
    parser.add_argument(
        "--split-seed",
        default=DEFAULT_SPLIT_SEED,
        help="stable board-group split seed; changing it changes the holdout",
    )
    parser.add_argument(
        "--max-passes",
        type=int,
        default=DEFAULT_MAX_PASSES,
        help="maximum number of monotone MM passes",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="optional deterministic cap for a small smoke run",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an existing result file after validating path separation",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    database = args.database.expanduser().resolve()
    output = (
        _default_output_path(database)
        if args.output is None
        else args.output.expanduser().resolve()
    )
    if database == output:
        raise ValueError("results output must be separate from the source archive")
    if output.exists() and not args.overwrite:
        raise FileExistsError(
            f"results file already exists; select another path or pass --overwrite: {output}"
        )
    if args.max_passes < 0:
        raise ValueError("--max-passes must be non-negative")
    if not args.split_seed:
        raise ValueError("--split-seed must be non-empty")

    rows = load_archive(database)
    report = build_results(
        rows,
        split=args.split,
        split_seed=args.split_seed,
        maximum_passes=args.max_passes,
        limit=args.limit,
    )
    report["source_archive"] = {
        "sha256": _sha256_file(database),
        "database_schema_version": 1,
        "opened_read_only": True,
    }
    report["execution"] = {
        "source_database": str(database),
        "result_path": str(output),
        "source_archive_modified": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output.parent,
        prefix=output.name + ".",
        suffix=".tmp",
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)
        json.dump(
            report,
            temporary,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        temporary.write("\n")
        temporary.flush()
        os.fsync(temporary.fileno())
    if output.exists() and not args.overwrite:
        temporary_path.unlink(missing_ok=True)
        raise FileExistsError(f"results file appeared during execution: {output}")
    os.replace(temporary_path, output)
    print(json.dumps({
        "output": str(output),
        "split": args.split,
        "evaluated_problem_count": report["evaluated_problem_count"],
        "reference_kind_counts": report["reference_kind_counts"],
        "source_archive_sha256": report["source_archive"]["sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
