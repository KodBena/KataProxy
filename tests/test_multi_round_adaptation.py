"""
tests/test_multi_round_adaptation.py — Regression tests for v1.0.24.

Pins the multi-round adaptation + budget abstraction contract introduced
per `proxy/docs/roadmap-multi-round-adaptation.md`. Covers six
categories aligned with the roadmap's commit-7 test plan:

  1. AdaptiveState mutation + query surface — observe / record_round /
     record_round_scores_* / record_visits / record_wall_clock and
     the corresponding read accessors.
  2. Framework-default metric trajectories — `worst_selector_value`
     and `worst_set_jaccard_to_previous` populated by record_round.
  3. ConvergenceCheck — absolute / relative scale, lookback semantics,
     short-trajectory non-firing.
  4. CombinedConvergence — `all_of` / `any_of` combinators.
  5. Budget — each of the four constraint shapes individually, AND-
     composition across multiple constraints, has_capacity exhaustion.
  6. `_parse_budget` — default (absent), profile-name, raw-object
     shapes; the three pre-curated profiles; the budget_invalid
     refusal surface (§11.4 cost-asymmetry calibration).

A coroutine-level integration test pins the multi-round wire shape:
under `max_rounds=2` the coroutine spawns twice, observes spawn
finals as previews, and finalizes one authoritative per turn at
end-of-loop.

Run from the proxy directory: `pytest tests/test_multi_round_adaptation.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Tuple

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import ClientId  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    Color,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MoveIndex,
    TurnIndex,
)
from middleware.adaptive_reevaluate import (  # noqa: E402
    AdaptiveConfigurationError,
    AdaptiveState,
    Budget,
    CombinedConvergence,
    ConvergenceCheck,
    _BUDGET_PROFILES,
    _parse_budget,
    adaptive_reevaluate,
)
from middleware.session_middleware import SessionCapabilities  # noqa: E402


# ---------------------------------------------------------------------------
# Synthetic packet helpers
# ---------------------------------------------------------------------------


def _packet(turn: int, *, delta: float = 0.0, marker: str = "") -> AnalyzeResponse:
    """Final AnalyzeResponse with optional bad-delta payload + marker."""
    opaque: dict[str, Any] = {
        "moveInfos": [],
        "extra": {
            "black": {"deltas": {str(turn): delta}},
            "white": {"deltas": {str(turn): delta}},
        },
    }
    if marker:
        opaque["marker"] = marker
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn,
        opaque=opaque,
    )


def _neutral_packet(turn: int, *, marker: str = "") -> AnalyzeResponse:
    """Final AnalyzeResponse without extra.deltas — invisible to the
    default move-axis selector. Used for turns that should not enter
    the worst-set."""
    opaque: dict[str, Any] = {"moveInfos": []}
    if marker:
        opaque["marker"] = marker
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn,
        opaque=opaque,
    )


# ===========================================================================
# 1. AdaptiveState
# ===========================================================================


class TestAdaptiveState:

    def test_observe_records_latest_per_turn(self) -> None:
        s = AdaptiveState()
        first = _packet(0, delta=-1.0, marker="first")
        second = _packet(0, delta=-0.5, marker="second")
        s.observe(first)
        s.observe(second)
        latest = s.last_packet(TurnIndex(0))
        assert latest is not None
        assert latest.opaque["marker"] == "second"

    def test_last_packet_absent_returns_none(self) -> None:
        s = AdaptiveState()
        assert s.last_packet(TurnIndex(7)) is None

    def test_selector_history_move_accumulates(self) -> None:
        s = AdaptiveState()
        s.record_round_scores_move([
            ("black", MoveIndex(0), -1.0),
            ("white", MoveIndex(1), -0.5),
        ])
        s.record_round_scores_move([
            ("black", MoveIndex(0), -0.8),
        ])
        assert s.selector_history_move("black", MoveIndex(0)) == [-1.0, -0.8]
        assert s.selector_history_move("white", MoveIndex(1)) == [-0.5]
        # Unseen pair → empty.
        assert s.selector_history_move("black", MoveIndex(99)) == []

    def test_selector_history_turn_accumulates(self) -> None:
        s = AdaptiveState()
        s.record_round_scores_turn([(TurnIndex(2), -1.5), (TurnIndex(3), -0.2)])
        s.record_round_scores_turn([(TurnIndex(2), -1.4)])
        assert s.selector_history_turn(TurnIndex(2)) == [-1.5, -1.4]
        assert s.selector_history_turn(TurnIndex(3)) == [-0.2]

    def test_record_round_increments_counter_and_deepened_counts(self) -> None:
        s = AdaptiveState()
        s.record_round(
            worst_pairs=[("black", MoveIndex(4)), ("white", MoveIndex(5))],
            deepening_turns={TurnIndex(3), TurnIndex(4), TurnIndex(5)},
        )
        assert s.rounds_completed == 1
        assert s.deepened_count_turn(TurnIndex(3)) == 1
        assert s.deepened_count_turn(TurnIndex(4)) == 1
        assert s.deepened_count_move("black", MoveIndex(4)) == 1
        assert s.deepened_count_move("white", MoveIndex(5)) == 1
        # Round 2 — re-deepen turn 3 only; counts diverge.
        s.record_round(
            worst_pairs=[("black", MoveIndex(4))],
            deepening_turns={TurnIndex(3)},
        )
        assert s.rounds_completed == 2
        assert s.deepened_count_turn(TurnIndex(3)) == 2
        assert s.deepened_count_turn(TurnIndex(4)) == 1
        assert s.deepened_count_move("black", MoveIndex(4)) == 2

    def test_record_visits_accumulates(self) -> None:
        s = AdaptiveState()
        s.record_visits(800)
        s.record_visits(200)
        assert s.total_visits_spent == 1000

    def test_record_wall_clock_sets_elapsed(self) -> None:
        s = AdaptiveState()
        s.record_wall_clock(1.5)
        assert s.wall_clock_elapsed_s == 1.5
        # record_wall_clock SETS (cumulative), not adds.
        s.record_wall_clock(3.2)
        assert s.wall_clock_elapsed_s == 3.2


# ===========================================================================
# 2. Framework-default metric trajectories
# ===========================================================================


class TestFrameworkDefaultMetrics:

    def test_worst_selector_value_appended_each_round(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)},
            worst_selector_value=-1.0,
        )
        s.record_round(
            deepening_turns={TurnIndex(1)},
            worst_selector_value=-0.7,
        )
        assert s.metric_trajectory("worst_selector_value") == [-1.0, -0.7]

    def test_worst_selector_value_omitted_when_none(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)},
            worst_selector_value=None,
        )
        assert s.metric_trajectory("worst_selector_value") == []

    def test_jaccard_populates_from_round_two(self) -> None:
        s = AdaptiveState()
        s.record_round(deepening_turns={TurnIndex(0), TurnIndex(1)})
        # Round 1 has no previous round → no jaccard entry.
        assert s.metric_trajectory("worst_set_jaccard_to_previous") == []
        # Round 2: identical set → jaccard=1.0.
        s.record_round(deepening_turns={TurnIndex(0), TurnIndex(1)})
        assert s.metric_trajectory("worst_set_jaccard_to_previous") == [1.0]
        # Round 3: completely different set → jaccard=0.0.
        s.record_round(deepening_turns={TurnIndex(7), TurnIndex(8)})
        assert s.metric_trajectory("worst_set_jaccard_to_previous") == [1.0, 0.0]
        # Round 4: half overlap → 1 / 3.
        s.record_round(deepening_turns={TurnIndex(7), TurnIndex(9)})
        traj = s.metric_trajectory("worst_set_jaccard_to_previous")
        assert traj[:2] == [1.0, 0.0]
        assert traj[2] == pytest.approx(1.0 / 3.0)

    def test_jaccard_empty_sets_treated_as_identical(self) -> None:
        """Two consecutive empty deepening sets — union is empty;
        the framework returns 1.0 (vacuously identical) to avoid
        division-by-zero. This case won't arise in production (the
        multi-round loop terminates on empty deepening), but the
        invariant is pinned for the unit-level surface."""
        s = AdaptiveState()
        s.record_round(deepening_turns=set())
        s.record_round(deepening_turns=set())
        assert s.metric_trajectory("worst_set_jaccard_to_previous") == [1.0]


# ===========================================================================
# 3. ConvergenceCheck
# ===========================================================================


class TestConvergenceCheck:

    def test_short_trajectory_not_converged(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)},
            worst_selector_value=-1.0,
        )
        # Only one entry; lookback=1 needs at least 2 → False.
        check = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1, lookback=1,
        )
        assert check.is_converged(s) is False

    def test_absolute_scale_converged_when_delta_below_tolerance(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-1.00,
        )
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-0.95,
        )
        # |(-0.95) - (-1.00)| = 0.05 < 0.10 → converged.
        check = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.10, scale="absolute",
        )
        assert check.is_converged(s) is True

    def test_absolute_scale_not_converged_when_delta_above_tolerance(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-1.0,
        )
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-0.5,
        )
        # |(-0.5) - (-1.0)| = 0.5 > 0.1 → not converged.
        check = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1, scale="absolute",
        )
        assert check.is_converged(s) is False

    def test_relative_scale(self) -> None:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-100.0,
        )
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-95.0,
        )
        # |Δ| / |prior| = 5 / 100 = 0.05.
        check_loose = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1, scale="relative",
        )
        check_tight = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.01, scale="relative",
        )
        assert check_loose.is_converged(s) is True
        assert check_tight.is_converged(s) is False

    def test_lookback_compares_to_n_steps_back(self) -> None:
        s = AdaptiveState()
        # Trajectory: -1.0, -0.9, -0.5. lookback=2 compares last to
        # first → |Δ|=0.5; lookback=1 compares last two → |Δ|=0.4.
        for v in (-1.0, -0.9, -0.5):
            s.record_round(
                deepening_turns={TurnIndex(0)}, worst_selector_value=v,
            )
        check_lb1 = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.45, lookback=1,
        )
        check_lb2 = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.45, lookback=2,
        )
        assert check_lb1.is_converged(s) is True   # 0.4 < 0.45
        assert check_lb2.is_converged(s) is False  # 0.5 > 0.45

    def test_jaccard_metric_path(self) -> None:
        """ConvergenceCheck against worst_set_jaccard_to_previous —
        the metric used by the range-generous / loop-aggressive
        budget profiles. Delta-of-jaccard interpretation: once the
        jaccard stops moving (whether near 1 or anywhere else),
        the dynamics have settled."""
        s = AdaptiveState()
        s.record_round(deepening_turns={TurnIndex(0), TurnIndex(1)})
        s.record_round(deepening_turns={TurnIndex(0), TurnIndex(1)})  # jaccard 1.0
        s.record_round(deepening_turns={TurnIndex(0), TurnIndex(1)})  # jaccard 1.0
        check = ConvergenceCheck(
            metric="worst_set_jaccard_to_previous", tolerance=0.1, lookback=1,
        )
        # Trajectory = [1.0, 1.0]; delta=0 < 0.1 → converged.
        assert check.is_converged(s) is True


# ===========================================================================
# 4. CombinedConvergence
# ===========================================================================


class TestCombinedConvergence:

    @staticmethod
    def _stalled_state() -> AdaptiveState:
        s = AdaptiveState()
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-1.00,
        )
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-0.99,
        )
        return s

    def test_all_of_requires_every_check(self) -> None:
        s = self._stalled_state()
        tight = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.001,
        )  # |Δ|=0.01 > 0.001 → False
        loose = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1,
        )    # |Δ|=0.01 < 0.1 → True
        combo = CombinedConvergence(mode="all_of", checks=(tight, loose))
        assert combo.is_converged(s) is False
        combo_both_loose = CombinedConvergence(
            mode="all_of", checks=(loose, loose),
        )
        assert combo_both_loose.is_converged(s) is True

    def test_any_of_requires_one_check(self) -> None:
        s = self._stalled_state()
        tight = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.001,
        )
        loose = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1,
        )
        combo = CombinedConvergence(mode="any_of", checks=(tight, loose))
        assert combo.is_converged(s) is True


# ===========================================================================
# 5. Budget
# ===========================================================================


class TestBudget:

    def test_unconstrained_budget_always_has_capacity(self) -> None:
        b = Budget()
        s = AdaptiveState()
        assert b.has_capacity(s) is True
        # State mutates — still has capacity (no constraint set).
        s.rounds_completed = 100
        s.total_visits_spent = 1_000_000
        s.wall_clock_elapsed_s = 3600.0
        assert b.has_capacity(s) is True

    def test_max_rounds_constraint(self) -> None:
        b = Budget(max_rounds=3)
        s = AdaptiveState()
        assert b.has_capacity(s) is True
        s.rounds_completed = 2
        assert b.has_capacity(s) is True
        s.rounds_completed = 3
        assert b.has_capacity(s) is False

    def test_total_extra_visits_constraint(self) -> None:
        b = Budget(total_extra_visits=2000)
        s = AdaptiveState()
        s.total_visits_spent = 1500
        assert b.has_capacity(s) is True
        s.total_visits_spent = 2000
        assert b.has_capacity(s) is False

    def test_wall_clock_constraint(self) -> None:
        b = Budget(wall_clock_seconds=10.0)
        s = AdaptiveState()
        s.wall_clock_elapsed_s = 9.99
        assert b.has_capacity(s) is True
        s.wall_clock_elapsed_s = 10.0
        assert b.has_capacity(s) is False

    def test_convergence_constraint(self) -> None:
        check = ConvergenceCheck(
            metric="worst_selector_value", tolerance=0.1,
        )
        b = Budget(convergence=check)
        s = AdaptiveState()
        # No data → not converged → has_capacity.
        assert b.has_capacity(s) is True
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-1.0,
        )
        s.record_round(
            deepening_turns={TurnIndex(0)}, worst_selector_value=-0.95,
        )
        # |Δ|=0.05 < 0.1 → converged → no capacity.
        assert b.has_capacity(s) is False

    def test_and_composition_terminates_when_any_exhausts(self) -> None:
        """Multiple constraints AND-compose: has_capacity is True iff
        every constraint still has room."""
        b = Budget(max_rounds=5, total_extra_visits=1000)
        s = AdaptiveState()
        s.rounds_completed = 2
        s.total_visits_spent = 999
        assert b.has_capacity(s) is True
        # Exhaust the visits cap — max_rounds still under, but the
        # budget terminates because ANY constraint exhausts.
        s.total_visits_spent = 1000
        assert b.has_capacity(s) is False
        # Reset visits, exhaust rounds — same property.
        s.total_visits_spent = 0
        s.rounds_completed = 5
        assert b.has_capacity(s) is False

    def test_visits_for_round_returns_per_round_extra(self) -> None:
        b = Budget(per_round_extra_visits=1234)
        assert b.visits_for_round() == 1234


# ===========================================================================
# 6. _parse_budget
# ===========================================================================


class TestParseBudget:

    def test_absent_budget_defaults_to_single_round(self) -> None:
        b = _parse_budget({})
        assert b.max_rounds == 1
        assert b.per_round_extra_visits == 800

    def test_extra_visits_threads_to_per_round_default(self) -> None:
        b = _parse_budget({"extra_visits": 1500})
        assert b.per_round_extra_visits == 1500
        assert b.max_rounds == 1  # default single-round still applies

    def test_profile_name_review_tight(self) -> None:
        b = _parse_budget({"budget": "review-tight"})
        assert b.max_rounds == 1
        assert b.total_extra_visits is None
        assert b.wall_clock_seconds is None
        assert b.convergence is None

    def test_profile_name_range_generous(self) -> None:
        b = _parse_budget({"budget": "range-generous"})
        assert b.max_rounds == 5
        assert b.total_extra_visits == 3000
        assert isinstance(b.convergence, ConvergenceCheck)
        assert b.convergence.metric == "worst_set_jaccard_to_previous"

    def test_profile_name_loop_aggressive(self) -> None:
        b = _parse_budget({"budget": "loop-aggressive"})
        assert b.max_rounds == 20
        assert b.total_extra_visits == 10000
        assert b.wall_clock_seconds == 60.0

    def test_profile_overrides_per_round_extra_visits(self) -> None:
        b = _parse_budget({"budget": "review-tight", "extra_visits": 1234})
        assert b.per_round_extra_visits == 1234

    def test_raw_object_max_rounds_only(self) -> None:
        b = _parse_budget({"budget": {"max_rounds": 7}})
        assert b.max_rounds == 7
        assert b.total_extra_visits is None
        assert b.wall_clock_seconds is None
        assert b.convergence is None

    def test_raw_object_full_shape(self) -> None:
        b = _parse_budget({"budget": {
            "max_rounds": 3,
            "total_extra_visits": 2000,
            "wall_clock_seconds": 30.0,
            "convergence": {
                "metric": "worst_selector_value",
                "tolerance": 0.05,
                "lookback": 2,
                "scale": "relative",
            },
        }})
        assert b.max_rounds == 3
        assert b.total_extra_visits == 2000
        assert b.wall_clock_seconds == 30.0
        assert isinstance(b.convergence, ConvergenceCheck)
        assert b.convergence.metric == "worst_selector_value"
        assert b.convergence.tolerance == 0.05
        assert b.convergence.lookback == 2
        assert b.convergence.scale == "relative"

    def test_raw_object_combinator_convergence(self) -> None:
        b = _parse_budget({"budget": {
            "convergence": {
                "all_of": [
                    {"metric": "worst_selector_value", "tolerance": 0.05},
                    {"metric": "worst_set_jaccard_to_previous", "tolerance": 0.1},
                ],
            },
        }})
        assert isinstance(b.convergence, CombinedConvergence)
        assert b.convergence.mode == "all_of"
        assert len(b.convergence.checks) == 2

    # ─── Refusal surface (§11.4 cost-asymmetry calibration) ───

    def test_unknown_profile_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": "no-such-profile"})
        assert exc.value.code == "budget_invalid"

    def test_wrong_budget_type_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": 42})
        assert exc.value.code == "budget_invalid"

    def test_unknown_budget_field_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"max_rounds": 3, "unknown_field": "x"}})
        assert exc.value.code == "budget_invalid"

    def test_negative_max_rounds_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"max_rounds": -1}})
        assert exc.value.code == "budget_invalid"

    def test_zero_max_rounds_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"max_rounds": 0}})
        assert exc.value.code == "budget_invalid"

    def test_bool_max_rounds_refuses(self) -> None:
        """Python's bool is a subclass of int; the parser explicitly
        excludes it to avoid `max_rounds: True` parsing as 1."""
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"max_rounds": True}})
        assert exc.value.code == "budget_invalid"

    def test_invalid_wall_clock_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"wall_clock_seconds": -1.0}})
        assert exc.value.code == "budget_invalid"

    def test_convergence_missing_metric_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"convergence": {"tolerance": 0.1}}})
        assert exc.value.code == "budget_invalid"

    def test_convergence_invalid_scale_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"convergence": {
                "metric": "worst_selector_value",
                "tolerance": 0.1,
                "scale": "nonsense",
            }}})
        assert exc.value.code == "budget_invalid"

    def test_convergence_dual_combinator_refuses(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_budget({"budget": {"convergence": {
                "all_of": [],
                "any_of": [],
            }}})
        assert exc.value.code == "budget_invalid"


# ===========================================================================
# 7. Curated profile shapes (smoke)
# ===========================================================================


class TestProfileShapes:

    def test_three_profiles_present(self) -> None:
        assert set(_BUDGET_PROFILES.keys()) == {
            "review-tight", "range-generous", "loop-aggressive",
        }

    def test_review_tight_is_single_round(self) -> None:
        p = _BUDGET_PROFILES["review-tight"]
        assert p.max_rounds == 1

    def test_range_generous_has_compute_caps_and_convergence(self) -> None:
        p = _BUDGET_PROFILES["range-generous"]
        assert p.max_rounds == 5
        assert p.total_extra_visits == 3000
        assert isinstance(p.convergence, ConvergenceCheck)

    def test_loop_aggressive_has_wall_clock_cap(self) -> None:
        p = _BUDGET_PROFILES["loop-aggressive"]
        assert p.wall_clock_seconds == 60.0
        assert p.max_rounds == 20


# ===========================================================================
# 8. Coroutine-level multi-round wire shape
# ===========================================================================


class TestMultiRoundCoroutine:
    """End-to-end pinning of the multi-round wire shape:
    spawn fires multiple times under max_rounds > 1; each round's
    spawn finals stream as previews; one authoritative per turn
    emerges at end-of-loop (finalization)."""

    @staticmethod
    def _make_caps() -> Tuple[Any, SessionCapabilities]:
        class _Caps:
            submitted: List[Tuple[ClientId, KataGoQuery]] = []
            terminated: List[ClientId] = []
            synthetic_sends: List[Tuple[ClientId, KataGoResponse]] = []

            async def submit(self, oid: ClientId, q: KataGoQuery) -> None:
                self.submitted.append((oid, q))

            async def terminate(self, oid: ClientId) -> None:
                self.terminated.append(oid)

            async def send(self, oid: ClientId, r: KataGoResponse) -> None:
                self.synthetic_sends.append((oid, r))

        c = _Caps()
        c.submitted = []
        c.terminated = []
        c.synthetic_sends = []
        return c, SessionCapabilities(
            submit_query=c.submit,
            terminate_query=c.terminate,
            send_response=c.send,
        )

    @staticmethod
    async def _drive_response(
        m: Any, orig_id: ClientId, response: KataGoResponse,
    ) -> List[Tuple[ClientId, KataGoResponse]]:
        """Drive handle_response and harvest orchestration emissions
        for the orig_id from caps.send_response (the push-based output
        channel)."""
        import asyncio
        sc = getattr(m, "_caps", None)
        fake = getattr(getattr(sc, "send_response", None), "__self__", None) if sc else None
        pre = len(fake.synthetic_sends) if fake is not None else 0
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for oid, resp in m.handle_response(orig_id, response, None):
            out.append((oid, resp))
        if fake is not None:
            await asyncio.sleep(0.01)
            out.extend(fake.synthetic_sends[pre:])
        return out

    @staticmethod
    async def _wait_for_spawn_count(
        caps: Any, n: int, timeout_s: float = 1.0,
    ) -> bool:
        import asyncio
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            if len(caps.submitted) >= n:
                return True
            await asyncio.sleep(0.005)
        return False

    @pytest.mark.asyncio
    async def test_max_rounds_two_spawns_twice_and_finalizes(self) -> None:
        """A multi-round budget with `max_rounds=2` runs the
        select-and-deepen loop twice (when the per-round dispatch
        keeps producing a non-empty deepening set), then finalizes
        one authoritative per turn at end-of-loop."""
        c, caps = self._make_caps()
        m = adaptive_reevaluate(
            worst_quantile=0.25, extra_visits=400, window_size=1,
        )()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                "capabilities": {
                    "adaptive_reevaluate": {
                        "budget": {"max_rounds": 2},
                        "worst_quantile": 0.25,
                        "extra_visits": 400,
                    },
                },
            },
        )
        m.on_query(ClientId("eid-1"), q)

        # Drive 6 originals — turn 0 has a bad delta, others neutral
        # (no extra.deltas → invisible to the default move selector).
        all_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in range(6):
            resp = (
                _packet(0, delta=-1.0) if turn == 0
                else _neutral_packet(turn)
            )
            all_yields += await self._drive_response(m, ClientId("eid-1"), resp)

        # Round-1 spawn fires.
        assert await self._wait_for_spawn_count(c, 1), "round-1 spawn missing"
        spawn1_oid, spawn1_q = c.submitted[0]
        # worst-set on the move axis with the bad-delta-at-turn-0
        # pattern is {0, 1, 2} (window_size=1 → no expansion).
        assert sorted(spawn1_q.analyze_turns) == [0, 1, 2]

        # Drive round-1 spawn finals (still bad-delta → next round's
        # worst-set is also {0, 1, 2}; with default jaccard-style
        # convergence absent in this budget, the loop continues to
        # round 2 per max_rounds=2).
        for turn in (0, 1, 2):
            all_yields += await self._drive_response(
                m, spawn1_oid, _packet(turn, delta=-1.0, marker="round1"),
            )

        # Round-2 spawn fires.
        assert await self._wait_for_spawn_count(c, 2), "round-2 spawn missing"
        spawn2_oid, _spawn2_q = c.submitted[1]

        # Drive round-2 spawn finals.
        for turn in (0, 1, 2):
            all_yields += await self._drive_response(
                m, spawn2_oid, _packet(turn, delta=-1.0, marker="round2"),
            )

        # Budget exhausted (max_rounds=2 reached) — no further spawn.
        assert len(c.submitted) == 2

        # Finalization invariant: exactly one authoritative per
        # analyzed turn.
        auth_emissions = [
            r for _, r in all_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        ]
        auth_turns = sorted(r.turn_number for r in auth_emissions)
        assert auth_turns == [0, 1, 2, 3, 4, 5], (
            f"finalization should emit exactly one authoritative per "
            f"analyzed turn; got auth_turns={auth_turns}"
        )

        # Latest-payload provenance: deepened turns carry the round-2
        # marker (most recent observation); non-deepened turns carry
        # no marker (original packet).
        by_turn = {r.turn_number: r for r in auth_emissions}
        for turn in (0, 1, 2):
            assert by_turn[turn].opaque.get("marker") == "round2", (
                f"deepened turn {turn}'s finalization payload should be "
                f"the round-2 spawn packet; got opaque={by_turn[turn].opaque}"
            )
        for turn in (3, 4, 5):
            assert "marker" not in by_turn[turn].opaque, (
                f"non-deepened turn {turn}'s finalization should carry "
                f"the original payload; got opaque={by_turn[turn].opaque}"
            )

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_convergence_terminates_before_max_rounds(self) -> None:
        """When the convergence metric stabilises, the multi-round
        loop terminates early — even with `max_rounds` set generously.

        Construction: max_rounds=5 + jaccard tolerance=0.1, lookback=1.
        Each round's spawn returns identical-content responses, so the
        next round's worst-set matches the prior round's; jaccard
        trajectory becomes [1.0, 1.0] after rounds 2 and 3; |Δ|=0 <
        0.1 triggers convergence. The loop ends at round 3 — not 5."""
        c, caps = self._make_caps()
        m = adaptive_reevaluate(
            worst_quantile=0.25, extra_visits=400, window_size=1,
        )()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                "capabilities": {
                    "adaptive_reevaluate": {
                        "budget": {
                            "max_rounds": 5,
                            "convergence": {
                                "metric": "worst_set_jaccard_to_previous",
                                "tolerance": 0.1,
                                "lookback": 1,
                                "scale": "absolute",
                            },
                        },
                        "worst_quantile": 0.25,
                        "extra_visits": 400,
                    },
                },
            },
        )
        m.on_query(ClientId("eid-1"), q)

        # Drive originals.
        for turn in range(6):
            await self._drive_response(
                m, ClientId("eid-1"),
                _packet(0, delta=-1.0) if turn == 0
                else _neutral_packet(turn),
            )

        # Drive identical round spawns until convergence terminates
        # the loop or the safety-cap fires. Convergence: jaccard
        # trajectory needs 2+ entries (rounds 2 and 3) and |Δ|<0.1.
        round_idx = 0
        while round_idx < 5:
            if not await self._wait_for_spawn_count(c, round_idx + 1):
                break
            spawn_oid, _ = c.submitted[round_idx]
            for turn in (0, 1, 2):
                await self._drive_response(
                    m, spawn_oid, _packet(turn, delta=-1.0),
                )
            round_idx += 1

        # Convergence terminates at round 3 (jaccard=[1.0, 1.0]
        # after rounds 2 and 3; |Δ|=0 < 0.1). The loop must NOT
        # have spawned 5 times.
        assert len(c.submitted) < 5, (
            f"convergence should terminate the loop before max_rounds=5; "
            f"got {len(c.submitted)} spawns"
        )
        assert len(c.submitted) == 3, (
            f"expected exactly 3 rounds (jaccard trajectory of length "
            f"2 from rounds 2 and 3 triggers convergence at round-4 "
            f"capacity check); got {len(c.submitted)}"
        )

        m.on_session_end()
