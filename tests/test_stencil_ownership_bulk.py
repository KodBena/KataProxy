from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.analyze_ownership_corpus import load_archive
from scripts.solve_ownership_corpus import (
    CampaignItem,
    SliceOutcome,
    TaskProgress,
    _initial_progress,
    build_campaign,
    campaign_fingerprint,
    load_checkpoint,
    run_campaign,
)
from transformers.stencil_cover.stencil_ownership import (
    OwnershipEvidence,
    OwnershipRegions,
    make_ownership_evidence,
)
from transformers.stencil_cover.stencil_ownership_coherent import (
    CoherentOwnershipPolicy,
)
from transformers.stencil_cover.stencil_ownership_cut import (
    CoherentCutOwnershipPolicy,
)
from transformers.stencil_cover.stencil_ownership_store import SQLiteOwnershipStore


def _evidence() -> OwnershipEvidence:
    return make_ownership_evidence(
        {
            "ownership": [0.8, -0.7, 0.2, -0.3],
            "ownershipStdev": [0.05, 0.05, 0.05, 0.05],
        },
        [None, None, None, None],
        2,
        2,
        {},
        requires_ownership_stdev=True,
    )


def _record(
    store: SQLiteOwnershipStore,
    policy: CoherentOwnershipPolicy | CoherentCutOwnershipPolicy,
    evidence: OwnershipEvidence,
    positive: tuple[int, ...],
    negative: tuple[int, ...],
) -> None:
    regions = OwnershipRegions(
        positive=positive,
        negative=negative,
        policy_id=policy.policy_id,
        diagnostics={"solverStatus": "unit_test"},
    )
    store.record(
        evidence,
        regions,
        policy,
        turn_number=1,
        is_during_search=True,
    )


def test_campaign_deduplicates_repeated_observations_and_policy_variants(
    tmp_path: Path,
) -> None:
    database = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(database)
    evidence = _evidence()
    mip = CoherentOwnershipPolicy()
    cut = CoherentCutOwnershipPolicy()

    _record(store, mip, evidence, (0,), (1,))
    _record(store, mip, evidence, (2,), (1,))
    _record(store, cut, evidence, (0,), (1,))

    campaign = build_campaign(load_archive(database))

    assert campaign.unique_problem_count == 1
    assert campaign.already_proven_count == 0
    assert len(campaign.items) == 1
    assert campaign.items[0].policy_ids == ("coherent", "coherent-cut")
    assert campaign.items[0].initial_labels is not None
    assert campaign.items[0].initial_energy is not None


def test_campaign_rounds_every_item_through_cumulative_levels_and_resumes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(database)
    evidence = _evidence()
    policy = CoherentOwnershipPolicy()
    _record(store, policy, evidence, (0,), (1,))

    campaign = build_campaign(load_archive(database))
    item = campaign.items[0]
    progress = {item.task_id: _initial_progress(item)}
    levels_ms = (1000, 2000)
    slice_ms = 500
    fingerprint = campaign_fingerprint(campaign, levels_ms, slice_ms)
    checkpoint = tmp_path / "bulk-checkpoint.json"
    budgets: list[int] = []

    def fake_solver(
        campaign_item: CampaignItem,
        task_progress: TaskProgress,
        time_limit_ms: int,
    ) -> SliceOutcome:
        budgets.append(time_limit_ms)
        if task_progress.best_labels is None:
            labels = campaign_item.initial_labels
            energy = campaign_item.initial_energy
        else:
            labels = task_progress.best_labels
            energy = task_progress.best_energy
        assert labels is not None
        assert energy is not None
        return SliceOutcome(
            elapsed_wall_ms=float(time_limit_ms),
            labels=dict(labels),
            energy=float(energy),
            lower_bound=None,
            status="feasible",
            error=None,
        )

    run_campaign(
        campaign,
        progress,
        levels_ms=levels_ms,
        slice_ms=slice_ms,
        checkpoint_path=checkpoint,
        fingerprint=fingerprint,
        progress_interval_seconds=3600.0,
        solver=fake_solver,
    )

    state = progress[item.task_id]
    assert budgets == [500, 500, 500, 500]
    assert state.attempt_count == 4
    assert state.spent_wall_ms == 2000.0
    assert state.optimal_proven is False

    resumed = load_checkpoint(checkpoint, campaign, fingerprint)
    assert resumed[item.task_id].attempt_count == 4
    assert resumed[item.task_id].spent_wall_ms == 2000.0
    assert resumed[item.task_id].best_labels == state.best_labels

    with pytest.raises(ValueError, match="does not match"):
        load_checkpoint(checkpoint, campaign, "different-campaign")


def test_campaign_rejects_checkpoint_witness_outside_eligible_graph(
    tmp_path: Path,
) -> None:
    database = tmp_path / "ownership.sqlite3"
    store = SQLiteOwnershipStore(database)
    evidence = _evidence()
    _record(store, CoherentOwnershipPolicy(), evidence, (0,), (1,))

    campaign = build_campaign(load_archive(database))
    item = campaign.items[0]
    progress = {item.task_id: _initial_progress(item)}
    levels_ms = (1000,)
    fingerprint = campaign_fingerprint(campaign, levels_ms, 500)
    checkpoint = tmp_path / "bulk-checkpoint.json"

    def fake_solver(
        _task: CampaignItem,
        state: TaskProgress,
        time_limit_ms: int,
    ) -> SliceOutcome:
        labels = state.best_labels
        energy = state.best_energy
        assert labels is not None and energy is not None
        return SliceOutcome(
            elapsed_wall_ms=1.0,
            labels=dict(labels),
            energy=energy,
            lower_bound=None,
            status="feasible",
            error=None,
        )

    run_campaign(
        campaign,
        progress,
        levels_ms=levels_ms,
        slice_ms=500,
        checkpoint_path=checkpoint,
        fingerprint=fingerprint,
        progress_interval_seconds=3600.0,
        solver=fake_solver,
    )
    document = json.loads(checkpoint.read_text(encoding="utf-8"))
    document["tasks"][item.task_id]["best_labels"] = [[999, 1]]
    checkpoint.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="eligible-point set"):
        load_checkpoint(checkpoint, campaign, fingerprint)
