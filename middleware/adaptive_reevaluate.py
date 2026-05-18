"""
middleware/adaptive_reevaluate.py — Adaptive re-evaluation as an
orchestration coroutine.

(Refactored in v1.0.16 from the manual-state-machine SessionMiddleware
shape to an orchestration coroutine using the framework primitives in
middleware/orchestration.py. Subsequently refactored in v1.0.20 to
stream original finals as previews — see Design below.)

Design
──────
The coroutine expresses adaptive re-evaluation as sequential async/await
code:

  1. Forward partials immediately. For each original final that arrives,
     buffer it for the worst-quantile decision AND emit it immediately
     with is_during_search=True patched (a "preview"), so the SPA can
     render the turn's data without waiting for the entire range to
     drain.
  2. When all originals have arrived, identify the worst-quantile
     turns by mean policy delta.
  3. Promote previews to authoritative: emit the buffered final with
     is_during_search=False for every turn NOT in the deepen set.
     Deepened turns get their authoritative emission from Stage 4.
  4. Spawn a single deeper-analysis sub-query targeting the worst
     turns at original_max_visits + extra_visits; yield its
     responses (which the framework auto-relabels onto the parent's
     orig_id). The deeper query's authoritative is_during_search=False
     responses replace the previews for the deepened turns.

The framework owns: parent-query lifetime, sub-query parent-pointer
tracking, response routing into the spawn iterator, cancellation
propagation, cleanup. This middleware owns: when to deepen, what to
spawn, how to label.

Streaming-previews rationale (v1.0.20)
──────────────────────────────────────
The pre-v1.0.20 shape buffered every original final on the demand edge
until original_stream exhausted, then released them all in Stage 3
with `is_during_search=True` patched on deepening turns and unchanged
on the rest. On range queries with auto-engage adaptive, this held
each turn's authoritative-quality data on the proxy for up to several
seconds (the gap from KataGo emitting turn 0's final to the last
turn's QUERY_COMPLETE), with no observable signal on the wire — the
operator-visible symptom was "ranges feel batchy". The v1.0.20 shape
streams each final immediately as `is_during_search=True`, then
emits the authoritative `is_during_search=False` (or relays the
deeper query's, for deepened turns) once the worst-quantile decision
is in. Wire bandwidth doubles for non-deepened turns (one preview +
one authoritative); the SPA's existing partial→final transition
handles the promotion idempotently.

Per-query metadata schema
─────────────────────────
The coroutine reads `capabilities.adaptive_reevaluate.worst_quantile`
and `capabilities.adaptive_reevaluate.extra_visits` from the parent's
opaque payload (Phase 1 capability negotiation, v1.0.14). Absent
fields fall back to the constructor defaults captured by closure.

`extra_visits` stays an *increment*: the deeper query's
`maxVisits = original_maxVisits + extra_visits` so KataGo's NN cache
continues the search from where the original left off rather than
restarting.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import ast
import asyncio
import logging
import time
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import numpy as np

from katago import (
    AnalyzeResponse,
    Color,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
    MoveIndex,
    TurnIndex,
    move_to_turn_pair,
)
from middleware.allocation import (
    AllocationAlgorithm,
    _parse_allocation_algorithm,
    _registered_algorithm_names,
)
from middleware.orchestration import (
    OrchestrationContext,
    OrchestrationMiddleware,
    orchestration_middleware,
)
from middleware.visit_scaling import (
    VisitScalingModel,
    _parse_visit_scaling_model,
    _registered_model_names,
)
from proxy_logging import Event, get_proxy_logger
from registry_interpreter import RegistryInterpreter

logger = logging.getLogger("kataproxy." + __name__)
_log = get_proxy_logger(__name__)


# ---------------------------------------------------------------------------
# Configuration-consistency error class (v1.0.23)
# ---------------------------------------------------------------------------
#
# Hard-refusal at the adaptive dispatch site per the cost-asymmetry
# calibration in docs/roadmap-adaptive-selector-pluggability.md §11.4:
# executing an expensive range-based query with conflated intent is
# more harmful than refusing clearly. The orchestration framework's
# exception handler synthesises a structured error response when the
# coroutine raises (see middleware/orchestration.py); the operator's
# log carries the full `code` + `detail` context via the exception's
# string representation.

class AdaptiveConfigurationError(RuntimeError):
    """Raised when adaptive_reevaluate's per-query configuration is
    inconsistent.

    See docs/roadmap-adaptive-selector-pluggability.md §11.4 for the
    cost-asymmetry principle. The `code` values across v1.0.23-v1.0.25:

      v1.0.23 (selector pluggability):
        - `ambiguous_axis`
        - `axis_binding_mismatch`
        - `policy_axis_mismatch`
        - `policy_parameters_invalid`

      v1.0.24 (multi-round + budget):
        - `budget_invalid`

      v1.0.25 (info-theoretic allocation):
        - `allocation_invalid`
    """

    def __init__(self, *, code: str, detail: dict[str, Any]) -> None:
        super().__init__(f"adaptive_reevaluate: {code} {detail}")
        self.code = code
        self.detail = detail


# ---------------------------------------------------------------------------
# Selector substrate — views and selection policies (v1.0.23 commit 1)
#
# The views (MoveView, TurnView) are the per-unit objects passed to
# user-authored selector expressions bound via analysis_config.bindings's
# `move_selector_fn` / `turn_selector_fn` roles. The selection-policy
# primitives operate on typed scored lists and return the worst-set; the
# dispatch wiring that resolves which policy to call per query lands in
# commit 4. See docs/roadmap-adaptive-selector-pluggability.md.
# ---------------------------------------------------------------------------


# Typed colour iteration constant — used by per-color selection policies and
# by the pure helpers below.
_COLORS: tuple[Color, Color] = ("black", "white")


# Per-unit history surfaced to user-authored selectors via the
# round_history field on MoveView / TurnView. Defined here (before the
# views that reference them) for forward-reference cleanliness.
# Populated by `_build_move_views` / `_build_turn_views` from the
# active AdaptiveState (commit 3); empty/zero in round 1 and at the
# legacy single-round dispatch entry. See
# docs/roadmap-multi-round-adaptation.md §2.3.

@dataclass(frozen=True)
class MoveRoundHistory:
    """Per-move history surfaced to a `move_selector_fn` via
    `x.round_history`.

    Empty in round 1 (selector_values=[], deepened=0,
    previous_packet=None, rounds_completed=0); populated as
    rounds progress. User selectors that don't access
    round_history are unaffected by the field's presence.
    """

    selector_values: list[float]
    deepened: int
    previous_packet: Optional[AnalyzeResponse]
    rounds_completed: int


@dataclass(frozen=True)
class TurnRoundHistory:
    """Per-turn history surfaced to a `turn_selector_fn` via
    `x.round_history`. Same shape as MoveRoundHistory.
    """

    selector_values: list[float]
    deepened: int
    previous_packet: Optional[AnalyzeResponse]
    rounds_completed: int


def _empty_move_round_history() -> MoveRoundHistory:
    """Default round_history for newly constructed MoveViews — empty
    selector_values list, zero deepened count, no previous packet,
    zero rounds completed. Matches what `_build_move_views` would
    construct from an empty AdaptiveState.
    """
    return MoveRoundHistory(
        selector_values=[],
        deepened=0,
        previous_packet=None,
        rounds_completed=0,
    )


def _empty_turn_round_history() -> TurnRoundHistory:
    """Default round_history for newly constructed TurnViews."""
    return TurnRoundHistory(
        selector_values=[],
        deepened=0,
        previous_packet=None,
        rounds_completed=0,
    )


@dataclass(frozen=True)
class MoveView:
    """The per-move view a `move_selector_fn` binding receives.

    Carries the brand (color + move_index) plus the per-arrival policy
    deltas for this move and references to the before/after analyze
    packets so the user expression can compute transition-shaped
    metrics (policy delta aggregations, score-lead drop, played-policy
    divergence, etc.).
    """

    color: Color
    move_index: MoveIndex
    deltas: list[float]
    before: AnalyzeResponse
    after: AnalyzeResponse
    round_history: MoveRoundHistory = field(
        default_factory=_empty_move_round_history,
    )


@dataclass(frozen=True)
class TurnView:
    """The per-turn view a `turn_selector_fn` binding receives.

    Carries the position index, the side-to-play at this position, and
    the analyze response. Turn-based metrics (policy entropy, score
    variance via state_fns precomputation, ownership flux) operate on
    a single packet without transition context.
    """

    turn_index: TurnIndex
    to_play: Color
    packet: AnalyzeResponse
    round_history: TurnRoundHistory = field(
        default_factory=_empty_turn_round_history,
    )


# Selection-policy primitives — move-axis
#
# Each takes a typed scored list and returns the worst-set as
# (Color, MoveIndex) tuples. Parameters are keyword-only; the dispatch
# in commit 4 supplies them from capability metadata.
#
# The threshold-based quantile primitives match the existing
# _find_worst_turns behaviour bit-for-bit: threshold = scalar at the
# quantile-index after sorting ascending; inclusion criterion is
# `scalar <= threshold`; ties are admitted. This is what makes the
# commit-3 refactor's "default-path preserves behaviour exactly"
# guarantee honest.

def _select_per_color_quantile_move(
    scored: list[tuple[Color, MoveIndex, float]],
    *,
    worst_quantile: float,
) -> list[tuple[Color, MoveIndex]]:
    """Per-color bottom-quantile selection.

    Both colors contribute Q% of their items independently; the union
    is returned. Matches the v1.0.22 `_find_worst_turns` per-color
    quantile shape exactly (threshold-based with `<=`).
    """
    worst: list[tuple[Color, MoveIndex]] = []
    for color in _COLORS:
        color_scored = [(m, s) for c, m, s in scored if c == color]
        if not color_scored:
            continue
        sorted_scalars = sorted(s for _, s in color_scored)
        threshold = sorted_scalars[int(len(color_scored) * worst_quantile)]
        worst.extend((color, m) for m, s in color_scored if s <= threshold)
    return worst


def _select_pooled_quantile_move(
    scored: list[tuple[Color, MoveIndex, float]],
    *,
    worst_quantile: float,
) -> list[tuple[Color, MoveIndex]]:
    """Pooled bottom-quantile selection across both colors."""
    if not scored:
        return []
    sorted_scalars = sorted(s for _, _, s in scored)
    threshold = sorted_scalars[int(len(scored) * worst_quantile)]
    return [(c, m) for c, m, s in scored if s <= threshold]


def _select_per_color_threshold_move(
    scored: list[tuple[Color, MoveIndex, float]],
    *,
    black_threshold: float,
    white_threshold: float,
) -> list[tuple[Color, MoveIndex]]:
    """Per-color absolute thresholds — scalar <= color's threshold."""
    threshold: dict[Color, float] = {
        "black": black_threshold,
        "white": white_threshold,
    }
    return [(c, m) for c, m, s in scored if s <= threshold[c]]


def _select_top_k_move(
    scored: list[tuple[Color, MoveIndex, float]],
    *,
    top_k: int,
) -> list[tuple[Color, MoveIndex]]:
    """Bottom-K worst items across both colors, pooled."""
    sorted_scored = sorted(scored, key=lambda x: x[2])
    return [(c, m) for c, m, _ in sorted_scored[:top_k]]


# Selection-policy primitives — turn-axis
#
# Each takes a typed scored list and returns the worst-set as
# TurnIndex values. Per-color partitioning does not apply on the
# turn axis (positions are color-agnostic for selection purposes;
# the to_play field on TurnView is the user's to consult).

def _select_pooled_quantile_turn(
    scored: list[tuple[TurnIndex, float]],
    *,
    worst_quantile: float,
) -> list[TurnIndex]:
    """Pooled bottom-quantile selection over turns."""
    if not scored:
        return []
    sorted_scalars = sorted(s for _, s in scored)
    threshold = sorted_scalars[int(len(scored) * worst_quantile)]
    return [t for t, s in scored if s <= threshold]


def _select_top_k_turn(
    scored: list[tuple[TurnIndex, float]],
    *,
    top_k: int,
) -> list[TurnIndex]:
    """Bottom-K worst turns."""
    sorted_scored = sorted(scored, key=lambda x: x[1])
    return [t for t, _ in sorted_scored[:top_k]]


# Default move-axis selector — used when no `move_selector_fn` binding
# is present in the active `analysis_config`. Lower returned scalar is
# worse (smaller mean policy delta indicates the actual move's quality
# was lower). The signature is deltas-only rather than MoveView-typed
# because the default path bypasses view construction (it does not
# need the before/after AnalyzeResponse references); user-authored
# selectors receive a full MoveView from the dispatch site in
# commit 4.

def _default_move_selector(deltas: list[float]) -> float:
    """The hardcoded default move-axis selector: mean of per-arrival
    policy deltas for the move. Used when no `move_selector_fn`
    binding is active.
    """
    return float(np.mean(deltas))


# ---------------------------------------------------------------------------
# Adaptive state — across-iteration accumulator (v1.0.24 commit 1)
#
# Framework-owned object accumulating per-round per-unit data across the
# multi-round loop. Read-only from selectors / value functions / budget
# objects; populated by the coroutine and the framework via the
# `observe` / `record_*` methods.
#
# Consumers (introduced in subsequent commits):
#   - The finalization stage at end-of-loop reads `last_packet(turn)` to
#     emit each turn's authoritative is_during_search=False response
#     (commit 4).
#   - The round_history field on MoveView / TurnView reads
#     `selector_history_*` and `deepened_count_*` to surface per-unit
#     history to user-authored selectors (commit 3).
#   - Budget objects read `rounds_completed`, `total_visits_spent`,
#     `wall_clock_elapsed_s`, and `metric_trajectory(name)` to evaluate
#     termination (commit 2 introduces Budget; commit 6 wires wall
#     clock).
#   - Framework-default metric trajectories (worst_selector_value,
#     worst_set_jaccard_to_previous) populated by `record_round` in
#     commit 5.
#
# See docs/roadmap-multi-round-adaptation.md §2.2 for the contract.
# ---------------------------------------------------------------------------


@dataclass
class AdaptiveState:
    """Across-iteration accumulator for adaptive's multi-round loop.

    Lifetime: one per parent query, constructed at coroutine entry,
    discarded at coroutine completion. The state is the source of
    truth for:

    - **Latest per-turn observed response** (`last_packet`). Updated
      by `observe(resp)` for every KataGo final the proxy receives
      (originals in Stage 1 and deeper-query responses in the
      multi-round loop). Consumed by the finalization stage to
      emit each turn's authoritative.
    - **Per-unit history** (`selector_history_*`,
      `deepened_count_*`). Updated by `record_round_scores_*` and
      `record_round`. Surfaced to selectors via
      MoveView/TurnView's round_history field.
    - **Round-level aggregates** (`rounds_completed`,
      `total_visits_spent`, `wall_clock_elapsed_s`). Consumed by
      Budget objects via `has_capacity(state)`.
    - **Named metric trajectories** (`metric_trajectory(name)`).
      Populated by `record_round` for framework defaults
      (worst_selector_value, worst_set_jaccard_to_previous) in
      commit 5. Consumed by ConvergenceCheck objects.
    """

    rounds_completed: int = 0
    total_visits_spent: int = 0
    wall_clock_elapsed_s: float = 0.0

    # Per-turn latest observed final (populated by observe()).
    _last_packet_by_turn: dict[TurnIndex, AnalyzeResponse] = field(
        default_factory=dict,
    )

    # Per-unit selector histories (populated by
    # record_round_scores_*).
    _selector_history_move: dict[tuple[Color, MoveIndex], list[float]] = field(
        default_factory=dict,
    )
    _selector_history_turn: dict[TurnIndex, list[float]] = field(
        default_factory=dict,
    )

    # Per-unit deepened counts (populated by record_round).
    _deepened_counts_move: dict[tuple[Color, MoveIndex], int] = field(
        default_factory=dict,
    )
    _deepened_counts_turn: dict[TurnIndex, int] = field(
        default_factory=dict,
    )

    # Per-round deepening turn-sets (kept for jaccard-style
    # trajectories that compare to the previous round).
    _round_deepen_sets: list[set[TurnIndex]] = field(
        default_factory=list,
    )

    # Named metric trajectories. Framework-default trajectories
    # populated by record_round in commit 5.
    _metric_trajectories: dict[str, list[float]] = field(
        default_factory=dict,
    )

    # ─── Framework-side mutation methods ───

    def observe(self, resp: AnalyzeResponse) -> None:
        """Record a KataGo final as the latest for its turn.

        Called for every is_during_search=False response observed by
        the coroutine (originals from Stage 1 and deeper-query
        responses from the multi-round loop). The finalization stage
        at end-of-loop reads `last_packet(turn)` to emit each turn's
        authoritative emission with is_during_search=False edited in.
        """
        self._last_packet_by_turn[TurnIndex(resp.turn_number)] = resp

    def record_round_scores_move(
        self,
        scored: list[tuple[Color, MoveIndex, float]],
    ) -> None:
        """Append per-move selector scores for the current round.

        Move-axis only. The score list reflects every move scored in
        this round (not just the worst-set); selector_history_move
        returns the full per-round trajectory of selector values per
        (color, move) for selectors that want it.
        """
        for color, m, scalar in scored:
            self._selector_history_move.setdefault((color, m), []).append(scalar)

    def record_round_scores_turn(
        self,
        scored: list[tuple[TurnIndex, float]],
    ) -> None:
        """Append per-turn selector scores for the current round.

        Turn-axis only; symmetric to record_round_scores_move.
        """
        for t, scalar in scored:
            self._selector_history_turn.setdefault(t, []).append(scalar)

    def record_round(
        self,
        *,
        worst_pairs: Optional[list[tuple[Color, MoveIndex]]] = None,
        deepening_turns: set[TurnIndex],
        worst_selector_value: Optional[float] = None,
    ) -> None:
        """Finalize a round: increment round counters, update per-unit
        deepened counts, and populate framework-default metric
        trajectories.

        For move-axis rounds: pass `worst_pairs` (the move-level
        worst-set before window expansion) to update per-move
        deepened counts. For turn-axis rounds: omit `worst_pairs`
        (per-turn deepened counts track via `deepening_turns`).

        `deepening_turns` is the round's full deepening turn-set
        (post-window-expansion on move-axis); used to update
        per-turn deepened counts and to record the round's deepening
        set for jaccard-style trajectories.

        `worst_selector_value` is the minimum selector value across
        this round's worst-set entries (None if not provided —
        unit-level callers that don't compute scores can omit it).
        Appended to `_metric_trajectories["worst_selector_value"]`
        for convergence checks.

        `_metric_trajectories["worst_set_jaccard_to_previous"]` is
        populated from round 2 onwards (Jaccard similarity of this
        round's `deepening_turns` against the prior round's). Round
        1 has no previous round, so the trajectory grows from round
        2; convergence checks against this metric with lookback=1
        thus require at least 3 rounds to fire (2 jaccard entries
        needed for a single delta).
        """
        self.rounds_completed += 1
        self._round_deepen_sets.append(set(deepening_turns))
        for turn in deepening_turns:
            self._deepened_counts_turn[turn] = (
                self._deepened_counts_turn.get(turn, 0) + 1
            )
        if worst_pairs is not None:
            for color, m in worst_pairs:
                self._deepened_counts_move[(color, m)] = (
                    self._deepened_counts_move.get((color, m), 0) + 1
                )
        if worst_selector_value is not None:
            self._metric_trajectories.setdefault(
                "worst_selector_value", [],
            ).append(worst_selector_value)
        if len(self._round_deepen_sets) >= 2:
            prev_set = self._round_deepen_sets[-2]
            curr_set = self._round_deepen_sets[-1]
            union = prev_set | curr_set
            inter = prev_set & curr_set
            jaccard = (len(inter) / len(union)) if union else 1.0
            self._metric_trajectories.setdefault(
                "worst_set_jaccard_to_previous", [],
            ).append(jaccard)

    def record_visits(self, visits: int) -> None:
        """Increment `total_visits_spent` by this round's deeper-query
        extra_visits (the amount the round added to KataGo's
        maxVisits over the original)."""
        self.total_visits_spent += visits

    def record_wall_clock(self, elapsed_s: float) -> None:
        """Update `wall_clock_elapsed_s` to the cumulative elapsed
        time since coroutine entry. Populated by the coroutine
        sampling time.monotonic() at round boundaries; consumed by
        wall-clock budget shapes (commit 6)."""
        self.wall_clock_elapsed_s = elapsed_s

    # ─── Queryable surface (read-only from external callers) ───

    def last_packet(self, t: TurnIndex) -> Optional[AnalyzeResponse]:
        """Most recent KataGo final observed for this turn, or
        None if the proxy has not yet observed any final for this
        turn. Consumed by the finalization stage to emit each turn's
        authoritative."""
        return self._last_packet_by_turn.get(t)

    def selector_history_move(
        self, color: Color, m: MoveIndex,
    ) -> list[float]:
        """Per-move selector scalars across rounds (move-axis).

        Returns a copy of the trajectory so callers can't mutate
        framework-owned state by side-effect.
        """
        return list(self._selector_history_move.get((color, m), []))

    def selector_history_turn(self, t: TurnIndex) -> list[float]:
        """Per-turn selector scalars across rounds (turn-axis)."""
        return list(self._selector_history_turn.get(t, []))

    def deepened_count_move(self, color: Color, m: MoveIndex) -> int:
        """Number of rounds this move was in the worst-set (move-axis)."""
        return self._deepened_counts_move.get((color, m), 0)

    def deepened_count_turn(self, t: TurnIndex) -> int:
        """Number of rounds this turn was in the deepening set."""
        return self._deepened_counts_turn.get(t, 0)

    def metric_trajectory(self, name: str) -> list[float]:
        """Named metric's per-round values. Returns an empty list if
        the metric isn't tracked."""
        return list(self._metric_trajectories.get(name, []))


# ---------------------------------------------------------------------------
# Budget abstraction (v1.0.24 commit 2)
#
# Composable per-query budget with four constraint shapes:
#   - max_rounds            (terminate after N rounds)
#   - total_extra_visits    (terminate when cumulative deeper-query
#                            extras exhaust)
#   - wall_clock_seconds    (terminate after elapsed time)
#   - convergence           (terminate when a named metric trajectory
#                            stabilises per the four-form tolerance shape)
#
# Multiple constraints AND-compose: terminate when ANY exhausts. A
# Budget consisting only of convergence (no compute cap) is valid —
# the loop runs until the metric stabilises.
#
# Three context profiles (`review-tight`, `range-generous`,
# `loop-aggressive`) provide named per-context tuning; resolved by
# `_parse_budget` at coroutine entry. Per-query `extra_visits` overrides
# `per_round_extra_visits` on profile lookup.
#
# Configuration-consistency refusal: `_parse_budget` raises
# `AdaptiveConfigurationError(code="budget_invalid")` on malformed
# input per §11.4's cost-asymmetry calibration.
#
# See docs/roadmap-multi-round-adaptation.md §3 for the full contract.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConvergenceCheck:
    """Single tolerance-style convergence check on a named metric.

    Four standard tolerance forms map to the (metric, scale,
    lookback) tuple:
      - Absolute on iterate: scale="absolute", metric=an iterate
        trajectory.
      - Relative on iterate: scale="relative".
      - Absolute on objective: scale="absolute", metric=a named
        objective trajectory.
      - Patience (no improvement for N rounds): lookback=N,
        metric=a best-observed trajectory.

    `is_converged(state)` returns True when the metric's trajectory
    has stabilised within `tolerance` over the last `lookback`
    rounds. Returns False when the trajectory is too short to
    evaluate (< lookback + 1 entries).
    """

    metric: str
    tolerance: float
    lookback: int = 1
    scale: Literal["absolute", "relative"] = "absolute"

    def is_converged(self, state: AdaptiveState) -> bool:
        history = state.metric_trajectory(self.metric)
        if len(history) < self.lookback + 1:
            return False
        current = history[-1]
        prior = history[-1 - self.lookback]
        delta = abs(current - prior)
        if self.scale == "absolute":
            return delta < self.tolerance
        # scale == "relative"
        return delta / max(abs(prior), 1e-9) < self.tolerance


@dataclass(frozen=True)
class CombinedConvergence:
    """`all_of` / `any_of` combinator over multiple ConvergenceChecks.

    `all_of`: converged when every check is converged.
    `any_of`: converged when any check is converged.

    Nested combinators (CombinedConvergence within CombinedConvergence)
    are out of scope for v1.0.24; checks must be ConvergenceCheck.
    """

    mode: Literal["all_of", "any_of"]
    checks: tuple[ConvergenceCheck, ...]

    def is_converged(self, state: AdaptiveState) -> bool:
        if self.mode == "all_of":
            return all(c.is_converged(state) for c in self.checks)
        return any(c.is_converged(state) for c in self.checks)


@dataclass(frozen=True)
class Budget:
    """Per-query budget with up to four AND-composable constraints.

    `has_capacity(state)` returns True iff every non-None
    constraint still has room. The multi-round loop terminates
    when has_capacity returns False (any constraint exhausted) OR
    when the per-round dispatch returns an empty deepening set
    (no adaptation warranted).
    """

    max_rounds: Optional[int] = None
    total_extra_visits: Optional[int] = None
    wall_clock_seconds: Optional[float] = None
    convergence: Optional[ConvergenceCheck | CombinedConvergence] = None
    # Per-round extra-visits — added to the deeper-query's maxVisits
    # each round. Defaults to the per-query `extra_visits` field
    # (legacy v1.0.23 knob). Phase 3 may replace this with an
    # allocation-algorithm-driven per-round visit count.
    per_round_extra_visits: int = 800

    def has_capacity(self, state: AdaptiveState) -> bool:
        if self.max_rounds is not None and state.rounds_completed >= self.max_rounds:
            return False
        if (
            self.total_extra_visits is not None
            and state.total_visits_spent >= self.total_extra_visits
        ):
            return False
        if (
            self.wall_clock_seconds is not None
            and state.wall_clock_elapsed_s >= self.wall_clock_seconds
        ):
            return False
        if self.convergence is not None and self.convergence.is_converged(state):
            return False
        return True

    def visits_for_round(self) -> int:
        """Visits to add to the deeper-query's maxVisits this round.

        Defaults to per_round_extra_visits. Phase 3 may replace this
        with allocation-algorithm-driven per-round visit counts.
        """
        return self.per_round_extra_visits


# Three context profiles — named per-context tuning over Budget.
# Looked up by string name in `_parse_budget` when the wire field
# `capabilities.adaptive_reevaluate.budget` is a string.

_BUDGET_PROFILES: dict[str, Budget] = {
    "review-tight": Budget(max_rounds=1),
    "range-generous": Budget(
        max_rounds=5,
        total_extra_visits=3000,
        convergence=ConvergenceCheck(
            metric="worst_set_jaccard_to_previous",
            tolerance=0.1,
            lookback=1,
            scale="absolute",
        ),
    ),
    "loop-aggressive": Budget(
        max_rounds=20,
        total_extra_visits=10000,
        wall_clock_seconds=60.0,
        convergence=ConvergenceCheck(
            metric="worst_set_jaccard_to_previous",
            tolerance=0.1,
            lookback=1,
            scale="absolute",
        ),
    ),
}


_VALID_BUDGET_FIELDS: frozenset[str] = frozenset({
    "max_rounds", "total_extra_visits", "wall_clock_seconds", "convergence",
})
_VALID_CONVERGENCE_FIELDS: frozenset[str] = frozenset({
    "metric", "tolerance", "lookback", "scale",
})


def _parse_budget(cap_meta: dict[str, Any]) -> Budget:
    """Parse `capabilities.adaptive_reevaluate.budget` into a Budget.

    Accepts a profile name (string) or a raw object with the four
    constraint fields. The `extra_visits` field on the capability
    metadata flows through as `per_round_extra_visits`.

    Raises AdaptiveConfigurationError(code="budget_invalid") on:
      - Unknown profile name.
      - Wrong type for `budget` (neither string nor object).
      - Unknown field in the budget object.
      - Wrong type / out-of-range value for any constraint.
      - Malformed convergence shape (missing required fields,
        invalid scale value, conflicting combinator usage).
    """
    raw = cap_meta.get("budget")
    extra_visits = int(cap_meta.get("extra_visits", 800))

    # Absent budget → default to single-round (matches v1.0.23 semantic).
    if raw is None:
        return Budget(max_rounds=1, per_round_extra_visits=extra_visits)

    if isinstance(raw, str):
        profile = _BUDGET_PROFILES.get(raw)
        if profile is None:
            raise AdaptiveConfigurationError(
                code="budget_invalid",
                detail={
                    "budget": raw,
                    "valid_profiles": sorted(_BUDGET_PROFILES.keys()),
                },
            )
        return replace(profile, per_round_extra_visits=extra_visits)

    if not isinstance(raw, dict):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={
                "budget": raw,
                "expected": "string profile name or budget object",
            },
        )

    return _parse_budget_dict(raw, extra_visits)


def _parse_budget_dict(raw: dict[str, Any], extra_visits: int) -> Budget:
    """Parse a raw budget object into a Budget. Validates every field."""
    unknown = set(raw.keys()) - _VALID_BUDGET_FIELDS
    if unknown:
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={
                "unknown_fields": sorted(unknown),
                "valid_fields": sorted(_VALID_BUDGET_FIELDS),
            },
        )

    max_rounds = raw.get("max_rounds")
    if max_rounds is not None and (
        not isinstance(max_rounds, int) or isinstance(max_rounds, bool) or max_rounds < 1
    ):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"max_rounds": max_rounds, "expected": "positive int"},
        )

    total_extra_visits = raw.get("total_extra_visits")
    if total_extra_visits is not None and (
        not isinstance(total_extra_visits, int)
        or isinstance(total_extra_visits, bool)
        or total_extra_visits < 1
    ):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={
                "total_extra_visits": total_extra_visits,
                "expected": "positive int",
            },
        )

    wall_clock_seconds_raw = raw.get("wall_clock_seconds")
    wall_clock_seconds: Optional[float] = None
    if wall_clock_seconds_raw is not None:
        if (
            not isinstance(wall_clock_seconds_raw, (int, float))
            or isinstance(wall_clock_seconds_raw, bool)
            or wall_clock_seconds_raw <= 0
        ):
            raise AdaptiveConfigurationError(
                code="budget_invalid",
                detail={
                    "wall_clock_seconds": wall_clock_seconds_raw,
                    "expected": "positive number",
                },
            )
        wall_clock_seconds = float(wall_clock_seconds_raw)

    convergence_raw = raw.get("convergence")
    convergence: Optional[ConvergenceCheck | CombinedConvergence] = None
    if convergence_raw is not None:
        convergence = _parse_convergence(convergence_raw)

    return Budget(
        max_rounds=max_rounds,
        total_extra_visits=total_extra_visits,
        wall_clock_seconds=wall_clock_seconds,
        convergence=convergence,
        per_round_extra_visits=extra_visits,
    )


def _parse_convergence(
    raw: Any,
) -> ConvergenceCheck | CombinedConvergence:
    """Parse a convergence sub-object into ConvergenceCheck or
    CombinedConvergence. Raises on malformed shapes."""
    if not isinstance(raw, dict):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence": raw, "expected": "object"},
        )

    has_all_of = "all_of" in raw
    has_any_of = "any_of" in raw

    if has_all_of and has_any_of:
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence": "cannot have both all_of and any_of"},
        )

    if has_all_of or has_any_of:
        mode: Literal["all_of", "any_of"] = "all_of" if has_all_of else "any_of"
        checks_raw = raw[mode]
        if not isinstance(checks_raw, list):
            raise AdaptiveConfigurationError(
                code="budget_invalid",
                detail={f"convergence.{mode}": "expected list of checks"},
            )
        # Disallow extra fields alongside the combinator.
        extra = set(raw.keys()) - {mode}
        if extra:
            raise AdaptiveConfigurationError(
                code="budget_invalid",
                detail={
                    f"convergence.{mode}": f"unexpected sibling fields: {sorted(extra)}",
                },
            )
        checks = tuple(_parse_single_convergence(c) for c in checks_raw)
        return CombinedConvergence(mode=mode, checks=checks)

    return _parse_single_convergence(raw)


def _parse_single_convergence(raw: Any) -> ConvergenceCheck:
    """Parse a single convergence check object into ConvergenceCheck."""
    if not isinstance(raw, dict):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence check": raw, "expected": "object"},
        )

    unknown = set(raw.keys()) - _VALID_CONVERGENCE_FIELDS
    if unknown:
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={
                "convergence_check_unknown_fields": sorted(unknown),
                "valid_fields": sorted(_VALID_CONVERGENCE_FIELDS),
            },
        )

    metric = raw.get("metric")
    if not isinstance(metric, str) or not metric:
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence.metric": metric, "expected": "non-empty string"},
        )

    tolerance = raw.get("tolerance")
    if (
        not isinstance(tolerance, (int, float))
        or isinstance(tolerance, bool)
        or tolerance <= 0
    ):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence.tolerance": tolerance, "expected": "positive number"},
        )

    lookback = raw.get("lookback", 1)
    if not isinstance(lookback, int) or isinstance(lookback, bool) or lookback < 1:
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={"convergence.lookback": lookback, "expected": "positive int"},
        )

    scale = raw.get("scale", "absolute")
    if scale not in ("absolute", "relative"):
        raise AdaptiveConfigurationError(
            code="budget_invalid",
            detail={
                "convergence.scale": scale,
                "expected": "'absolute' or 'relative'",
            },
        )

    return ConvergenceCheck(
        metric=metric,
        tolerance=float(tolerance),
        lookback=lookback,
        scale=scale,
    )


# ---------------------------------------------------------------------------
# Pure helpers (runtime behaviour unchanged since the pre-v1.0.16 imperative
# impl; signatures brand-threaded in v1.0.22; refactored in v1.0.23 to use
# the selector substrate above while preserving the legacy-path behaviour
# bit-for-bit — see docs/roadmap-adaptive-selector-pluggability.md).
# ---------------------------------------------------------------------------


def _collect_per_move_deltas(
    responses: List[AnalyzeResponse],
) -> dict[Color, dict[MoveIndex, list[float]]]:
    """Collect per-color per-move policy deltas from final responses.

    Reads `extra.<color>.deltas` (the per-move per-arrival policy
    deltas emitted by the analysis_enricher transformer) and groups
    them into a typed per-color, per-move-index map. Consumed by
    both the default-path scoring in `_dispatch_deepening_set` and
    the user-axis `_build_move_views`.
    """
    turn_maps: dict[Color, dict[MoveIndex, list[float]]] = {
        "black": defaultdict(list),
        "white": defaultdict(list),
    }
    for resp in responses:
        for color in _COLORS:
            deltas = resp.opaque.get("extra", {}).get(color, {}).get("deltas")
            if isinstance(deltas, dict):
                for t, d in deltas.items():
                    turn_maps[color][MoveIndex(int(t))].append(float(d))
    return turn_maps


def _expand_window_same_color(
    worst_pairs: list[tuple[Color, MoveIndex]],
    all_turns: set[TurnIndex],
    window_size: int,
) -> set[TurnIndex]:
    """Same-color predecessor window in move-space.

    For each worst (color, move) pair, includes that move's
    (before, after) turn pair PLUS the same pairs for its (window_size
    - 1) same-color predecessors. Default `window_size=1` — re-evaluate
    only the bad moves themselves (each move is two positions). The
    windowing infrastructure is in place for users who want to include
    context, but the semantic default is "re-evaluate exactly what was
    flagged as worst, nothing else."

    Replaces the pre-v1.0.23 `_expand_window` (symmetric turn-space
    ±half), which crossed into opposite-color neighbouring moves
    whose badness is independent of the selected move. The new
    expansion stays within the move's color, matching the per-color
    selection semantics. See docs/roadmap-adaptive-selector-pluggability.md
    §6 and §11.4's rationale.

    Out-of-range predecessors (negative MoveIndex) and turns whose
    TurnIndex is not in `all_turns` (game edges, range not analyzed)
    are dropped.
    """
    expanded: set[TurnIndex] = set()
    for color, m in worst_pairs:
        m_int = int(m)
        for offset in range(window_size):
            pred_int = m_int - offset
            if pred_int < 0:
                break
            pred = MoveIndex(pred_int)
            before, after = move_to_turn_pair(color, pred)
            if before in all_turns:
                expanded.add(before)
            if after in all_turns:
                expanded.add(after)
    return expanded


def _build_deeper_query(
    orig: KataGoQuery, turns: list[TurnIndex], extra_visits: int,
) -> KataGoQuery:
    """Build a deeper-analysis query derived from the original.

    `extra_visits` is per-orig_id. Increment-not-absolute: the deeper
    query's maxVisits = original_maxVisits + extra_visits so KataGo's
    NN cache continues the search from where the original left off
    rather than restarting.

    The capabilities field stays in the deeper opaque so the
    orchestration framework treats the synthetic deeper query
    consistently with the parent on the wire-strip side. The central
    wire-strip in katago/katago_proxy.py:translate_query_to_wire
    ensures it never reaches KataGo regardless.
    """
    new_opaque = dict(orig.opaque)
    new_opaque["maxVisits"] = (
        new_opaque.get("maxVisits", 1000) + extra_visits
    )
    # Strip client-side cache flags — the injected query is internal.
    new_opaque.pop("cache", None)
    new_opaque.pop("lookup_cache", None)
    new_opaque.pop("replay_final_only", None)
    # NOTE (ADR-0002 Rule 2): KataGoQuery.analyze_turns is declared
    # Optional[list[int]] at the wire-types level. Adaptive's internal
    # surface threads list[TurnIndex] (runtime-equal to list[int]; the
    # brand is a typecheck-only distinction). The wider migration that
    # would narrow the wire-types field is deferred per
    # docs/roadmap-adaptive-type-branding.md §7.2; the cast is the one
    # documented seam between the branded internal world and the
    # un-branded wire-types declaration.
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=cast(list[int], turns),
        opaque=new_opaque,
    )


# ---------------------------------------------------------------------------
# Dispatch helpers (v1.0.23 commit 4)
#
# These wire user-authored selectors and the curated selection policies
# into the per-query path. They consume:
#
#   - `analysis_config` (parent.opaque['analysis_config']) — the
#     authoritative source of `move_selector_fn` / `turn_selector_fn`
#     bindings, via RegistryInterpreter's Optional-returning accessors
#     introduced in v1.0.23 commit 2;
#   - `cap_meta` (parent.opaque['capabilities']['adaptive_reevaluate'])
#     — the disambiguator (`selector_axis`), the selection policy
#     name + parameters (`selection_policy`, `worst_quantile`,
#     `top_k`, `black_threshold`, `white_threshold`), and the
#     legacy scalar knobs (`worst_quantile`, `extra_visits`,
#     `window_size`).
#
# Configuration inconsistencies hard-refuse with
# AdaptiveConfigurationError per the cost-asymmetry calibration in
# docs/roadmap-adaptive-selector-pluggability.md §11.4.
# ---------------------------------------------------------------------------


def _try_build_interpreter(
    analysis_config: Optional[dict[str, Any]],
) -> Optional[RegistryInterpreter]:
    """Build a RegistryInterpreter from analysis_config, or None.

    Returns None when analysis_config is absent or fails to compile
    (matches analysis_enricher's "warn and skip enrichment" posture
    for compile failures). Adaptive's dispatch treats this as
    "no user-authored selectors" and falls back to the hardcoded
    default path.

    This is the same fallback discipline as analysis_enricher.on_query;
    §11.4's hard-refusal applies to ADAPTIVE-SPECIFIC inconsistencies
    (axis/policy/parameter conflicts), not to analysis_config-level
    compile failures that would also break the rest of the analysis
    pipeline.
    """
    if not analysis_config:
        return None
    try:
        return RegistryInterpreter(analysis_config)
    except (RuntimeError, TypeError, ValueError) as e:
        _log.warning(
            Event.DIAGNOSTIC,
            msg=(
                f"adaptive: RegistryInterpreter setup failed: {e}; "
                f"selectors disabled (falling back to hardcoded default)"
            ),
        )
        return None


def _resolve_axis_and_selector(
    interpreter: Optional[RegistryInterpreter],
    cap_meta: dict[str, Any],
) -> tuple[Literal["move", "turn"], Optional[Callable[[Any], Any]]]:
    """Resolve the effective selector axis and (optional) user-authored callable.

    Returns:
      ("move", None)        — default path; no user binding for move axis.
      ("move", callable)    — user-authored move selector.
      ("turn", callable)    — user-authored turn selector (turn axis
                              has no hardcoded default — a user
                              binding is required for the turn axis).

    Raises AdaptiveConfigurationError on the inconsistency cases
    enumerated in §11.4:
      - `ambiguous_axis`: both bindings present, no `selector_axis`.
      - `axis_binding_mismatch`: `selector_axis` names an absent
        binding or an invalid axis value.
    """
    declared_axis = cap_meta.get("selector_axis")

    if declared_axis is not None and declared_axis not in ("move", "turn"):
        raise AdaptiveConfigurationError(
            code="axis_binding_mismatch",
            detail={
                "selector_axis": declared_axis,
                "remedy": "selector_axis must be 'move' or 'turn'",
            },
        )

    move_selector = (
        interpreter.get_move_selector_fn() if interpreter is not None else None
    )
    turn_selector = (
        interpreter.get_turn_selector_fn() if interpreter is not None else None
    )

    if declared_axis is None:
        if move_selector is not None and turn_selector is not None:
            raise AdaptiveConfigurationError(
                code="ambiguous_axis",
                detail={
                    "remedy": (
                        "both move_selector_fn and turn_selector_fn are "
                        "bound; set capabilities.adaptive_reevaluate."
                        "selector_axis to 'move' or 'turn' to disambiguate"
                    ),
                },
            )
        if turn_selector is not None:
            return ("turn", turn_selector)
        # move_selector is either a callable or None (no binding,
        # default path engages).
        return ("move", move_selector)

    if declared_axis == "move":
        if move_selector is None:
            raise AdaptiveConfigurationError(
                code="axis_binding_mismatch",
                detail={
                    "selector_axis": "move",
                    "remedy": (
                        "bind move_selector_fn in "
                        "analysis_config.bindings, or omit selector_axis "
                        "to use the hardcoded default selector"
                    ),
                },
            )
        return ("move", move_selector)

    # declared_axis == "turn"
    if turn_selector is None:
        raise AdaptiveConfigurationError(
            code="axis_binding_mismatch",
            detail={
                "selector_axis": "turn",
                "remedy": "bind turn_selector_fn in analysis_config.bindings",
            },
        )
    return ("turn", turn_selector)


def _apply_selection_policy_move(
    scored: list[tuple[Color, MoveIndex, float]],
    cap_meta: dict[str, Any],
) -> list[tuple[Color, MoveIndex]]:
    """Apply the named selection policy to move-axis scored data.

    Raises AdaptiveConfigurationError on policy_axis_mismatch (unknown
    policy name, or turn-only policy named on move axis) or
    policy_parameters_invalid (required parameter missing).
    """
    policy_name = cap_meta.get("selection_policy", "per_color_quantile")

    if policy_name == "per_color_quantile":
        return _select_per_color_quantile_move(
            scored,
            worst_quantile=float(cap_meta.get("worst_quantile", 0.25)),
        )
    if policy_name == "pooled_quantile":
        return _select_pooled_quantile_move(
            scored,
            worst_quantile=float(cap_meta.get("worst_quantile", 0.25)),
        )
    if policy_name == "per_color_threshold":
        missing = [
            k for k in ("black_threshold", "white_threshold")
            if k not in cap_meta
        ]
        if missing:
            raise AdaptiveConfigurationError(
                code="policy_parameters_invalid",
                detail={
                    "selection_policy": "per_color_threshold",
                    "missing": missing,
                },
            )
        return _select_per_color_threshold_move(
            scored,
            black_threshold=float(cap_meta["black_threshold"]),
            white_threshold=float(cap_meta["white_threshold"]),
        )
    if policy_name == "top_k":
        if "top_k" not in cap_meta:
            raise AdaptiveConfigurationError(
                code="policy_parameters_invalid",
                detail={
                    "selection_policy": "top_k",
                    "missing": ["top_k"],
                },
            )
        return _select_top_k_move(scored, top_k=int(cap_meta["top_k"]))
    raise AdaptiveConfigurationError(
        code="policy_axis_mismatch",
        detail={
            "selection_policy": policy_name,
            "axis": "move",
            "valid": [
                "per_color_quantile", "pooled_quantile",
                "per_color_threshold", "top_k",
            ],
        },
    )


def _apply_selection_policy_turn(
    scored: list[tuple[TurnIndex, float]],
    cap_meta: dict[str, Any],
) -> list[TurnIndex]:
    """Apply the named selection policy to turn-axis scored data.

    Per-color policies (per_color_quantile, per_color_threshold) are
    move-only and raise policy_axis_mismatch when named on the turn
    axis.
    """
    policy_name = cap_meta.get("selection_policy", "pooled_quantile")

    if policy_name == "pooled_quantile":
        return _select_pooled_quantile_turn(
            scored,
            worst_quantile=float(cap_meta.get("worst_quantile", 0.25)),
        )
    if policy_name == "top_k":
        if "top_k" not in cap_meta:
            raise AdaptiveConfigurationError(
                code="policy_parameters_invalid",
                detail={
                    "selection_policy": "top_k",
                    "missing": ["top_k"],
                },
            )
        return _select_top_k_turn(scored, top_k=int(cap_meta["top_k"]))
    raise AdaptiveConfigurationError(
        code="policy_axis_mismatch",
        detail={
            "selection_policy": policy_name,
            "axis": "turn",
            "valid_for_turn": ["pooled_quantile", "top_k"],
        },
    )


def _build_move_views(
    finals: List[AnalyzeResponse],
    turn_maps: dict[Color, dict[MoveIndex, list[float]]],
    state: AdaptiveState,
) -> list[MoveView]:
    """Construct a MoveView for each (color, move) with deltas, when
    both endpoint AnalyzeResponses are available.

    Moves at game edges (or any move whose before/after turn isn't in
    the final-response set) are skipped — the user selector cannot
    operate on them without complete view data.

    The `round_history` field on each view is constructed from the
    active AdaptiveState — selector_history_move per (color, m),
    deepened_count_move per (color, m), the move's after-position's
    latest observed packet via state.last_packet, and the global
    rounds_completed counter. In round 1 (or when the dispatch is
    state-empty), the fields are empty/zero.
    """
    by_turn: dict[TurnIndex, AnalyzeResponse] = {
        TurnIndex(f.turn_number): f for f in finals
    }
    views: list[MoveView] = []
    for color in _COLORS:
        for m, deltas in turn_maps[color].items():
            before_idx, after_idx = move_to_turn_pair(color, m)
            before_packet = by_turn.get(before_idx)
            after_packet = by_turn.get(after_idx)
            if before_packet is None or after_packet is None:
                continue
            round_history = MoveRoundHistory(
                selector_values=state.selector_history_move(color, m),
                deepened=state.deepened_count_move(color, m),
                previous_packet=state.last_packet(after_idx),
                rounds_completed=state.rounds_completed,
            )
            views.append(MoveView(
                color=color,
                move_index=m,
                deltas=list(deltas),
                before=before_packet,
                after=after_packet,
                round_history=round_history,
            ))
    return views


def _build_turn_views(
    finals: List[AnalyzeResponse],
    state: AdaptiveState,
) -> list[TurnView]:
    """Construct a TurnView for each final AnalyzeResponse.

    Side-to-play at turn t: Black at even turns (0, 2, 4, …), White
    at odd turns. The convention follows KataGo's analyze_turns
    indexing where turn 0 is the root.

    The `round_history` field is constructed from the active
    AdaptiveState — selector_history_turn, deepened_count_turn,
    state.last_packet(turn), and rounds_completed.
    """
    views: list[TurnView] = []
    for f in finals:
        turn = TurnIndex(f.turn_number)
        to_play: Color = "black" if int(turn) % 2 == 0 else "white"
        round_history = TurnRoundHistory(
            selector_values=state.selector_history_turn(turn),
            deepened=state.deepened_count_turn(turn),
            previous_packet=state.last_packet(turn),
            rounds_completed=state.rounds_completed,
        )
        views.append(TurnView(
            turn_index=turn,
            to_play=to_play,
            packet=f,
            round_history=round_history,
        ))
    return views


def _dispatch_deepening_round(
    finals: List[AnalyzeResponse],
    state: AdaptiveState,
    cap_meta: dict[str, Any],
    analysis_config: Optional[dict[str, Any]],
    window_size: int,
    all_turns: set[TurnIndex],
) -> tuple[
    set[TurnIndex],
    Optional[list[tuple[Color, MoveIndex]]],
    Optional[float],
]:
    """Run one round's select-and-deepen dispatch against the current
    state and return the deepening turn-set + move-level worst-set
    + worst-set's minimum selector value.

    Returns (deepening_turns, worst_pairs, worst_selector_value):
      - deepening_turns: the set of turns to deepen this round
        (after window expansion for move-axis; identity-with-
        all_turns-mask for turn-axis).
      - worst_pairs: move-level worst-set for move-axis (before
        window expansion); None for turn-axis. The caller passes
        worst_pairs to state.record_round to update per-move
        deepened counts.
      - worst_selector_value: the minimum selector value across the
        round's worst-set entries (worst-of-the-worst, lower=worse
        convention). None when the worst-set is empty. Threaded into
        state.record_round for the framework's `worst_selector_value`
        metric trajectory.

    Side effect: records this round's per-unit scoring into state
    via state.record_round_scores_move/turn so subsequent rounds'
    view round_history reflects this round's selector values.
    """
    interpreter = _try_build_interpreter(analysis_config)
    axis, user_selector = _resolve_axis_and_selector(interpreter, cap_meta)

    if axis == "move":
        turn_maps = _collect_per_move_deltas(finals)
        worst_pairs: list[tuple[Color, MoveIndex]]
        scored: list[tuple[Color, MoveIndex, float]]
        if user_selector is None:
            scored = [
                (color, m, _default_move_selector(ds))
                for color in _COLORS
                for m, ds in turn_maps[color].items()
            ]
            worst_pairs = _select_per_color_quantile_move(
                scored,
                worst_quantile=float(cap_meta.get("worst_quantile", 0.25)),
            )
        else:
            views_m = _build_move_views(finals, turn_maps, state)
            scored = [
                (v.color, v.move_index, float(user_selector(v)))
                for v in views_m
            ]
            worst_pairs = _apply_selection_policy_move(scored, cap_meta)
        state.record_round_scores_move(scored)
        worst_pair_set = set(worst_pairs)
        worst_value_move: Optional[float] = min(
            (s for (c, m, s) in scored if (c, m) in worst_pair_set),
            default=None,
        )
        deepening = _expand_window_same_color(
            worst_pairs, all_turns, window_size,
        )
        return deepening, worst_pairs, worst_value_move

    # axis == "turn" — user binding required (axis resolution enforces).
    assert user_selector is not None
    views_t = _build_turn_views(finals, state)
    scored_t: list[tuple[TurnIndex, float]] = [
        (v.turn_index, float(user_selector(v))) for v in views_t
    ]
    state.record_round_scores_turn(scored_t)
    worst_turns_list = _apply_selection_policy_turn(scored_t, cap_meta)
    worst_turn_set = set(worst_turns_list)
    worst_value_turn: Optional[float] = min(
        (s for (t, s) in scored_t if t in worst_turn_set),
        default=None,
    )
    deepening = set(worst_turns_list) & all_turns
    return deepening, None, worst_value_turn


def _dispatch_deepening_set(
    finals: List[AnalyzeResponse],
    cap_meta: dict[str, Any],
    analysis_config: Optional[dict[str, Any]],
    window_size: int,
    all_turns: set[TurnIndex],
) -> set[TurnIndex]:
    """Single-round convenience wrapper around _dispatch_deepening_round.

    Constructs a fresh AdaptiveState, observes finals, and dispatches
    one round. Returns the deepening turn-set. Preserved for the
    v1.0.23-style tests that exercise dispatch in isolation; the
    multi-round coroutine uses _dispatch_deepening_round directly
    with a persistent state.
    """
    state = AdaptiveState()
    for f in finals:
        state.observe(f)
    deepening, _worst_pairs, _worst_value = _dispatch_deepening_round(
        finals=finals,
        state=state,
        cap_meta=cap_meta,
        analysis_config=analysis_config,
        window_size=window_size,
        all_turns=all_turns,
    )
    return deepening


# ---------------------------------------------------------------------------
# Phase 3 — information-theoretic allocation dispatch (v1.0.25 commit 5)
#
# Engagement: `capabilities.adaptive_reevaluate.allocation_algorithm`
# present in the per-query metadata. When engaged, the per-round
# dispatch (after _dispatch_deepening_round identifies the candidate
# set) routes through `_allocate_visits` to produce a per-turn visit
# budget, then spawns N parallel sub-queries (one per candidate),
# streaming responses as previews via _stream_parallel_spawns. When
# absent, the v1.0.24 single-spawn worst-quantile dispatch holds.
#
# See `proxy/docs/roadmap-info-theoretic-allocation.md` §§3.6, 5, 6.
# ---------------------------------------------------------------------------


# Attribute names whose access in a user-authored value-function
# expression requires opting in via the corresponding `include*` flag
# on the parent analyze query. The mapping is closed for v1.0.25;
# future KataGo additions extend this dict.

_GATED_ATTR_TO_INCLUDE_FLAG: dict[str, str] = {
    "policy": "includePolicy",
    "ownership": "includeOwnership",
    "ownershipStdev": "includeOwnershipStdev",
    "pvVisits": "includePVVisits",
    "noResultValue": "includeNoResultValue",
}


def _required_include_flags(expression_str: str) -> set[str]:
    """Walk the AST of a value-function expression and collect the set
    of `include*` flags the expression's field references would
    require on the parent query.

    Heuristic on the moves-vs-root variant: if the expression
    references `moveInfos` anywhere AND reads `.ownership` /
    `.ownershipStdev`, both the root-level and the
    moves-* variants are required. False-positives over-require flags
    (benign: payload grows slightly); false-negatives (missing a
    required flag) is what the eager check exists to prevent — the
    AST walk's blanket "require both variants on moveInfos+ownership"
    keeps the false-negative side clean.

    A SyntaxError in the expression returns an empty set; the
    interpreter raises at evaluation time with its own diagnostics.
    """
    try:
        tree = ast.parse(expression_str, mode="eval")
    except SyntaxError:
        return set()

    has_moveinfos_ref = False
    attrs_seen: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "moveInfos":
            has_moveinfos_ref = True
        elif isinstance(node, ast.Attribute):
            if node.attr == "moveInfos":
                has_moveinfos_ref = True
            attrs_seen.add(node.attr)

    required: set[str] = set()
    if "policy" in attrs_seen:
        required.add("includePolicy")
    if "noResultValue" in attrs_seen:
        required.add("includeNoResultValue")
    if "pvVisits" in attrs_seen:
        required.add("includePVVisits")
    if "ownership" in attrs_seen:
        required.add("includeOwnership")
        if has_moveinfos_ref:
            required.add("includeMovesOwnership")
    if "ownershipStdev" in attrs_seen:
        required.add("includeOwnership")
        required.add("includeOwnershipStdev")
        if has_moveinfos_ref:
            required.add("includeMovesOwnership")
            required.add("includeMovesOwnershipStdev")
    return required


def _is_phase3_engaged(cap_meta: dict[str, Any]) -> bool:
    """True when `allocation_algorithm` is set in capability metadata.

    The single engagement signal per §6 of the roadmap: presence of
    `allocation_algorithm` indicates the user has opted into Phase 3.
    Co-fields (`visit_scaling_model`, `value_binding`) are validated
    by `_engage_phase3` only when engagement is signalled — absent
    signal leaves v1.0.24 dispatch entirely untouched.
    """
    return cap_meta.get("allocation_algorithm") is not None


def _engage_phase3(
    cap_meta: dict[str, Any],
    analysis_config: Optional[dict[str, Any]],
    parent_opaque: dict[str, Any],
) -> tuple[AllocationAlgorithm, VisitScalingModel, Callable[[Any], float]]:
    """Parse all three Phase 3 plug points OR refuse with
    `AdaptiveConfigurationError(code="allocation_invalid")`.

    Returns the resolved `(algorithm, visit_scaling_model, value_fn)`
    triple. Called once at coroutine entry (not per round) so the
    refusal happens before any compute is spent.

    Eager validation per §3.6.4 / §11.10: the value-function
    expression's AST is scanned for opt-in-gated field references;
    if any required `include*` flag is absent on the parent query,
    refuse with the missing-flags list in `detail`.
    """
    algo = _parse_allocation_algorithm(cap_meta)

    model_name = cap_meta.get("visit_scaling_model")
    if not isinstance(model_name, str) or not model_name:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "visit_scaling_model": model_name,
                "expected": "non-empty string (name of a curated model)",
                "valid": _registered_model_names(),
            },
        )
    model = _parse_visit_scaling_model(model_name)

    value_binding = cap_meta.get("value_binding")
    if not isinstance(value_binding, str) or not value_binding:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "value_binding": value_binding,
                "expected": (
                    "non-empty string naming a value_fn symbol in "
                    "analysis_config.symbols"
                ),
            },
        )

    interp = _try_build_interpreter(analysis_config)
    if interp is None:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "value_binding": value_binding,
                "issue": (
                    "Phase 3 requires analysis_config with a "
                    "value_fn binding; analysis_config is absent or "
                    "malformed"
                ),
            },
        )

    # Verify value_binding consistency: analysis_config.bindings.value_fn
    # must point to the symbol named in capability metadata. The mismatch
    # is a configuration error (SPA-side wire shape disagreed with itself).
    bindings = analysis_config.get("bindings") if analysis_config else None
    bindings_value_fn = (
        bindings.get("value_fn") if isinstance(bindings, dict) else None
    )
    if bindings_value_fn != value_binding:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "value_binding": value_binding,
                "analysis_config.bindings.value_fn": bindings_value_fn,
                "issue": (
                    "capability metadata's value_binding must match "
                    "analysis_config.bindings.value_fn"
                ),
            },
        )

    value_fn = interp.get_value_fn()
    if value_fn is None:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "value_binding": value_binding,
                "issue": (
                    "value_fn binding does not resolve in "
                    "analysis_config; check that the named symbol "
                    "exists in analysis_config.symbols"
                ),
            },
        )

    # Eager include-flag validation (§3.6.4 / §11.10).
    symbols = analysis_config.get("symbols") if analysis_config else None
    expression_src = (
        symbols.get(value_binding) if isinstance(symbols, dict) else None
    )
    if isinstance(expression_src, str):
        required_flags = _required_include_flags(expression_src)
        missing = sorted(
            flag for flag in required_flags
            if not parent_opaque.get(flag)
        )
        if missing:
            raise AdaptiveConfigurationError(
                code="allocation_invalid",
                detail={
                    "value_binding": value_binding,
                    "missing_includes": missing,
                    "remedy": (
                        "set these include* flags to true on the "
                        "parent analyze query so the value function "
                        "can read the fields it references"
                    ),
                },
            )

    # Cast for the type-checker: get_value_fn returns Callable[[Any], Any];
    # the allocation algorithm consumes Callable[[TurnView], float] — the
    # float coercion happens at the call site in the allocation code.
    return algo, model, cast(Callable[[Any], float], value_fn)


async def _stream_parallel_spawns(
    ctx: OrchestrationContext,
    queries: List[KataGoQuery],
) -> AsyncIterator[KataGoResponse]:
    """Stream responses from N parallel sub-queries, interleaved as
    they arrive from upstream.

    The orchestration framework's `ctx.parallel(*queries)` gathers
    each sub-query's responses into a list and returns once all are
    complete — which would buffer N rounds' worth of intermediate
    previews against v1.0.20's no-buffering discipline. This helper
    merges N async iterators into a single flat stream, yielding each
    response as it arrives.

    Each yield is one response from one sub-query; the response's
    `turn_number` identifies which candidate the response belongs to.
    All sub-queries' responses interleave; the caller demultiplexes
    by `turn_number` if needed.

    Cancellation safety: if the caller stops iterating, the pump
    tasks are cancelled in the `finally` block; the orchestration
    framework's own cancellation path handles sub-query teardown.
    """
    _Sentinel = object
    sentinel: object = _Sentinel()
    queue: asyncio.Queue[Union[KataGoResponse, object]] = asyncio.Queue()

    async def pump(query: KataGoQuery) -> None:
        try:
            async for resp in ctx.spawn(query):
                await queue.put(resp)
        finally:
            await queue.put(sentinel)

    tasks = [asyncio.create_task(pump(q)) for q in queries]
    pending = len(tasks)
    try:
        while pending > 0:
            item = await queue.get()
            if item is sentinel:
                pending -= 1
                continue
            # The type check above narrows `item` away from the
            # sentinel object, leaving KataGoResponse.
            assert isinstance(item, (AnalyzeResponse, MetadataResponse))
            yield item
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


def _turn_view_for_allocation(
    turn: TurnIndex, state: AdaptiveState,
) -> Optional[TurnView]:
    """Build a TurnView for the allocation algorithm from the state's
    latest observed packet for this turn.

    Returns None when the state has no packet for this turn — the
    coroutine should not call _allocate_visits with a turn that
    hasn't been observed. The Phase 3 dispatch composes with Phase 2:
    the dispatch's `deepen` set comes from candidates whose finals
    were already observed in Stage 1.
    """
    packet = state.last_packet(turn)
    if packet is None:
        return None
    to_play: Color = "black" if int(turn) % 2 == 0 else "white"
    return TurnView(
        turn_index=turn,
        to_play=to_play,
        packet=packet,
    )


def _allocate_visits(
    deepen: set[TurnIndex],
    state: AdaptiveState,
    algo: AllocationAlgorithm,
    model: VisitScalingModel,
    value_fn: Callable[[Any], float],
    budget_visits: int,
) -> dict[TurnIndex, int]:
    """Run the Phase 3 allocation algorithm over the round's candidate set.

    `deepen` is the per-round candidate set from
    `_dispatch_deepening_round`; each candidate becomes a TurnView
    constructed from state.last_packet. Turns absent from state
    (defensive — shouldn't happen in normal dispatch) are skipped.

    Returns the per-turn visit allocation. The empty dict when the
    candidate set is empty after filtering OR when the algorithm
    returns an empty allocation (e.g., budget_visits = 0).
    """
    candidates: list[TurnView] = []
    for turn in sorted(deepen):
        view = _turn_view_for_allocation(turn, state)
        if view is not None:
            candidates.append(view)
    if not candidates:
        return {}
    allocation = algo.allocate(
        candidates=candidates,
        value_fn=value_fn,
        visit_scaling_model=model,
        budget_visits=budget_visits,
    )
    return allocation


# ---------------------------------------------------------------------------
# adaptive_reevaluate factory (orchestration-shaped)
# ---------------------------------------------------------------------------

def adaptive_reevaluate(
    worst_quantile: float = 0.25,
    extra_visits: int = 800,
    window_size: int = 1,
) -> Callable[[], OrchestrationMiddleware]:
    """Return a factory that produces an OrchestrationMiddleware
    expressing adaptive re-evaluation.

    The constructor parameters become the per-query defaults: a
    parent query that opts in to `adaptive_reevaluate` without
    overriding metadata uses these values. Per-query overrides via
    `capabilities.adaptive_reevaluate.{worst_quantile,extra_visits}`
    take precedence.

    Caller pattern (mirrors the SELECTOR / capability_gate factories):

        base = CapabilityGatedMiddleware(
            "adaptive_reevaluate",
            adaptive_reevaluate(
                worst_quantile=0.25,
                extra_visits=800,
                window_size=1,
            )(),  # () to invoke the factory
        )

    The trailing `()` is the only API change vs. the pre-v1.0.16
    shape (which returned the middleware directly). The wrapping
    pattern is otherwise identical.
    """

    @orchestration_middleware(name="adaptive_reevaluate")
    async def coro(
        parent: KataGoQuery, ctx: OrchestrationContext,
    ) -> AsyncIterator[KataGoResponse]:
        # Non-analyze queries pass through unchanged.
        if parent.action != KataGoAction.ANALYZE:
            async for resp in ctx.original_stream():
                yield resp
            return

        # Per-query metadata overrides (Phase 1 capability schema);
        # closure-captured defaults are the fallback.
        cap_meta = (
            (parent.opaque.get("capabilities") or {})
            .get("adaptive_reevaluate") or {}
        )
        q_quantile = cap_meta.get("worst_quantile", worst_quantile)
        q_extra = cap_meta.get("extra_visits", extra_visits)

        # Mirror closure defaults into cap_meta so dispatch + budget
        # parsing read uniformly. cap_meta_for_dispatch is the single
        # source for per-query knobs across the multi-round loop.
        cap_meta_for_dispatch: dict[str, Any] = dict(cap_meta)
        cap_meta_for_dispatch.setdefault("worst_quantile", q_quantile)
        cap_meta_for_dispatch.setdefault("extra_visits", q_extra)

        # v1.0.24: AdaptiveState constructed at coroutine entry; lives
        # for the duration of the query. Tracks each turn's latest
        # observed final (for the finalization stage) plus per-unit
        # history and round-level aggregates for selectors / budget.
        state = AdaptiveState()
        # Wall-clock origin for the wall_clock_seconds budget shape;
        # sampled at coroutine entry (Stage 1 + finalization both count
        # toward the elapsed total per §3.1's "from coroutine entry to
        # has_capacity check" calibration). Updated after each round
        # so budget.has_capacity reads the most recent elapsed time.
        wall_clock_origin = time.monotonic()

        # v1.0.25: Phase 3 engagement check. When `allocation_algorithm`
        # is named in capability metadata, all three plug points are
        # resolved + validated eagerly at coroutine entry — refusal
        # happens before Stage 1's compute cost is committed. When
        # absent, phase3 stays None and v1.0.24 dispatch holds.
        raw_config = parent.opaque.get("analysis_config")
        analysis_config: Optional[dict[str, Any]] = (
            raw_config if isinstance(raw_config, dict) else None
        )
        phase3: Optional[tuple[
            AllocationAlgorithm, VisitScalingModel, Callable[[Any], float],
        ]] = None
        if _is_phase3_engaged(cap_meta_for_dispatch):
            phase3 = _engage_phase3(
                cap_meta=cap_meta_for_dispatch,
                analysis_config=analysis_config,
                parent_opaque=parent.opaque,
            )

        # Stage 1: forward partials + metadata immediately; record
        # each original final into state AND emit a preview to the
        # client. The original packet (is_during_search=False from
        # KataGo) is edited to a preview (is_during_search=True) on
        # the wire; the finalization stage at end-of-loop emits the
        # authoritative is_during_search=False per turn.
        finals: List[AnalyzeResponse] = []
        async for resp in ctx.original_stream():
            if isinstance(resp, MetadataResponse):
                yield resp
                continue
            if resp.is_during_search:
                yield resp
                continue
            finals.append(resp)
            state.observe(resp)
            yield replace(resp, is_during_search=True)

        if not finals:
            return

        # Stage 2: budget parsing + multi-round adaptive loop.
        # analysis_config was extracted at coroutine entry (above) so
        # the Phase 3 engagement check could consume it. Reused here.
        all_turns: set[TurnIndex] = {TurnIndex(f.turn_number) for f in finals}

        budget = _parse_budget(cap_meta_for_dispatch)

        # Multi-round loop. Each iteration:
        #   1. Compute this round's worst-set from current state
        #      (can re-include already-deepened turns; can include
        #      newly-worst turns whose state shifted).
        #   2. Spawn deeper query; each KataGo final is recorded in
        #      state AND emitted as a preview (is_during_search=True).
        #   3. record_round finalizes the round (counters, deepening
        #      sets, per-unit deepened counts).
        # Termination: budget exhausted (any constraint) OR empty
        # worst-set ("no more adaptation warranted").
        while budget.has_capacity(state):
            deepen, worst_pairs, worst_value = _dispatch_deepening_round(
                finals=finals,
                state=state,
                cap_meta=cap_meta_for_dispatch,
                analysis_config=analysis_config,
                window_size=window_size,
                all_turns=all_turns,
            )
            if not deepen:
                break

            _log.info(
                Event.DIAGNOSTIC,
                cid=ctx.parent_id,
                msg=(
                    f"adaptive: orig_id={ctx.parent_id!r} "
                    f"round={state.rounds_completed + 1} "
                    f"deepening turns={sorted(deepen)} "
                    f"quantile={q_quantile} extra_visits={q_extra} "
                    f"phase3={'on' if phase3 is not None else 'off'}"
                ),
            )

            if phase3 is not None:
                # v1.0.25 — Phase 3 dispatch: allocate per-turn visits,
                # spawn N parallel sub-queries (one per candidate),
                # stream responses interleaved as previews.
                algo, model, value_fn = phase3
                allocation = _allocate_visits(
                    deepen=deepen,
                    state=state,
                    algo=algo,
                    model=model,
                    value_fn=value_fn,
                    budget_visits=budget.visits_for_round(),
                )
                if not allocation:
                    # Allocation collapsed to empty (e.g., zero budget);
                    # nothing to spawn this round. Record the round
                    # so the budget bookkeeping advances.
                    state.record_round(
                        worst_pairs=worst_pairs,
                        deepening_turns=deepen,
                        worst_selector_value=worst_value,
                    )
                    state.record_visits(budget.visits_for_round())
                    state.record_wall_clock(time.monotonic() - wall_clock_origin)
                    continue
                sub_queries = [
                    _build_deeper_query(parent, [turn], visits)
                    for turn, visits in allocation.items()
                ]
                async for resp in _stream_parallel_spawns(ctx, sub_queries):
                    if isinstance(resp, MetadataResponse):
                        yield resp
                        continue
                    if resp.is_during_search:
                        yield resp
                        continue
                    state.observe(resp)
                    yield replace(resp, is_during_search=True)
            else:
                # v1.0.24 dispatch: single deeper query covering the
                # whole deepening set under one maxVisits envelope.
                deeper = _build_deeper_query(
                    parent, sorted(deepen), budget.visits_for_round(),
                )
                async for resp in ctx.spawn(deeper):
                    if isinstance(resp, MetadataResponse):
                        yield resp
                        continue
                    if resp.is_during_search:
                        yield resp
                        continue
                    state.observe(resp)
                    yield replace(resp, is_during_search=True)

            state.record_round(
                worst_pairs=worst_pairs,
                deepening_turns=deepen,
                worst_selector_value=worst_value,
            )
            state.record_visits(budget.visits_for_round())
            state.record_wall_clock(time.monotonic() - wall_clock_origin)

        # Stage 3 — finalization. Emit each turn's latest observed
        # response with is_during_search=False. The single
        # authoritative emission per turn the KataGo protocol contract
        # requires. Duplicates the latest preview emission for that
        # turn modulo the flag (per §8.3 of the roadmap); the SPA's
        # rendering of analysis data from previews handles the
        # intermediate state.
        for f in finals:
            turn = TurnIndex(f.turn_number)
            latest = state.last_packet(turn) or f
            yield replace(latest, is_during_search=False)

    return coro
