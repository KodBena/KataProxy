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

import logging
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
from middleware.orchestration import (
    OrchestrationContext,
    OrchestrationMiddleware,
    orchestration_middleware,
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
    principle and the four `code` values: `ambiguous_axis`,
    `axis_binding_mismatch`, `policy_axis_mismatch`,
    `policy_parameters_invalid`.
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
    ) -> None:
        """Finalize a round: increment round counters and update
        per-unit deepened counts.

        For move-axis rounds: pass `worst_pairs` (the move-level
        worst-set before window expansion) to update per-move
        deepened counts. For turn-axis rounds: omit `worst_pairs`
        (per-turn deepened counts track via `deepening_turns`).

        `deepening_turns` is the round's full deepening turn-set
        (post-window-expansion on move-axis); used to update
        per-turn deepened counts and to record the round's deepening
        set for jaccard-style trajectories.

        Framework-default metric trajectories (worst_selector_value,
        worst_set_jaccard_to_previous) are populated here in
        commit 5.
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
) -> list[MoveView]:
    """Construct a MoveView for each (color, move) with deltas, when
    both endpoint AnalyzeResponses are available.

    Moves at game edges (or any move whose before/after turn isn't in
    the final-response set) are skipped — the user selector cannot
    operate on them without complete view data.
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
            views.append(MoveView(
                color=color,
                move_index=m,
                deltas=list(deltas),
                before=before_packet,
                after=after_packet,
            ))
    return views


def _build_turn_views(
    finals: List[AnalyzeResponse],
) -> list[TurnView]:
    """Construct a TurnView for each final AnalyzeResponse.

    Side-to-play at turn t: Black at even turns (0, 2, 4, …), White
    at odd turns. The convention follows KataGo's analyze_turns
    indexing where turn 0 is the root.
    """
    views: list[TurnView] = []
    for f in finals:
        turn = TurnIndex(f.turn_number)
        to_play: Color = "black" if int(turn) % 2 == 0 else "white"
        views.append(TurnView(turn_index=turn, to_play=to_play, packet=f))
    return views


def _dispatch_deepening_set(
    finals: List[AnalyzeResponse],
    cap_meta: dict[str, Any],
    analysis_config: Optional[dict[str, Any]],
    window_size: int,
    all_turns: set[TurnIndex],
) -> set[TurnIndex]:
    """Resolve dispatch and produce the deepening-turn set.

    Move-axis path: worst-set is expanded via `_expand_window`
    (turn-space symmetric; same-color-predecessor in commit 5).
    Turn-axis path: worst-set IS the deepening set (no framework
    window expansion in v1.0.23; selector authors any cross-turn
    aggregation via `apply_window` in the expression substrate).
    """
    interpreter = _try_build_interpreter(analysis_config)
    axis, user_selector = _resolve_axis_and_selector(interpreter, cap_meta)

    if axis == "move":
        # Score the per-color moves — either via the hardcoded default
        # (no user binding) or the user-authored selector. The default
        # path preserves the legacy "mean policy delta + per-color
        # quantile" shape; the window correction (move-space same-color
        # predecessor expansion) is the one wire-visible behaviour
        # change post-v1.0.23 on this path.
        turn_maps = _collect_per_move_deltas(finals)
        worst_pairs: list[tuple[Color, MoveIndex]]
        if user_selector is None:
            scored_default: list[tuple[Color, MoveIndex, float]] = [
                (color, m, _default_move_selector(ds))
                for color in _COLORS
                for m, ds in turn_maps[color].items()
            ]
            worst_pairs = _select_per_color_quantile_move(
                scored_default,
                worst_quantile=float(cap_meta.get("worst_quantile", 0.25)),
            )
        else:
            views_m = _build_move_views(finals, turn_maps)
            scored_m: list[tuple[Color, MoveIndex, float]] = [
                (v.color, v.move_index, float(user_selector(v)))
                for v in views_m
            ]
            worst_pairs = _apply_selection_policy_move(scored_m, cap_meta)
        return _expand_window_same_color(worst_pairs, all_turns, window_size)

    # axis == "turn" — user binding required (axis resolution enforces).
    assert user_selector is not None
    views_t = _build_turn_views(finals)
    scored_t: list[tuple[TurnIndex, float]] = [
        (v.turn_index, float(user_selector(v))) for v in views_t
    ]
    worst_turns_list = _apply_selection_policy_turn(scored_t, cap_meta)
    return set(worst_turns_list) & all_turns


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

        # Stage 1: forward partials and metadata immediately. For each
        # original final that arrives, buffer it for Stage 2's worst-
        # quantile decision AND emit it immediately as a preview
        # (is_during_search=True). The preview lets the SPA render the
        # turn's data the moment KataGo finishes it; the authoritative
        # is_during_search=False follows in Stage 3 (non-deepened turns)
        # or Stage 4 (deepened turns, via the spawn sub-query).
        # The framework signals end-of-stream via original_stream()
        # exhaustion once all expected finals have arrived.
        finals: List[AnalyzeResponse] = []
        async for resp in ctx.original_stream():
            if isinstance(resp, MetadataResponse):
                # adaptive is analyze-shaped end-to-end, but metadata
                # responses (e.g., error responses) can still arrive
                # for analyze queries; pass them through.
                yield resp
                continue
            if resp.is_during_search:
                yield resp
                continue
            finals.append(resp)
            yield replace(resp, is_during_search=True)

        if not finals:
            return

        # Stage 2: decide on adaptation. Dispatch into the
        # selector + selection-policy substrate; either the
        # hardcoded default (no user binding) or the user-authored
        # selector path runs, with configuration inconsistencies
        # hard-refusing per AdaptiveConfigurationError. The cap_meta
        # already carries worst_quantile / window_size / etc.; the
        # coroutine's closure-default scalars are present as keys via
        # the legacy-default scaffolding so the dispatch reads
        # uniformly.
        all_turns: set[TurnIndex] = {TurnIndex(f.turn_number) for f in finals}
        raw_config = parent.opaque.get("analysis_config")
        analysis_config: Optional[dict[str, Any]] = (
            raw_config if isinstance(raw_config, dict) else None
        )
        # Ensure cap_meta carries the resolved scalar defaults
        # (closure capture + per-query overrides). The dispatch
        # helpers read cap_meta as the single source for these
        # scalars; mirror them in so the call site is uniform.
        cap_meta_for_dispatch: dict[str, Any] = dict(cap_meta)
        cap_meta_for_dispatch.setdefault("worst_quantile", q_quantile)
        deepen = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta_for_dispatch,
            analysis_config=analysis_config,
            window_size=window_size,
            all_turns=all_turns,
        )

        if not deepen:
            # No adaptation warranted; promote each preview to the
            # authoritative final (is_during_search=False).
            for f in finals:
                yield f
            return

        _log.info(
            Event.DIAGNOSTIC,
            cid=ctx.parent_id,
            msg=(
                f"adaptive: orig_id={ctx.parent_id!r} "
                f"deepening turns={sorted(deepen)} "
                f"quantile={q_quantile} extra_visits={q_extra}"
            ),
        )

        # Stage 3: promote previews to authoritative for non-deepened
        # turns only. Deepened turns already streamed as previews in
        # Stage 1; their authoritative is_during_search=False arrives
        # via the spawn sub-query (Stage 4), relabelled onto the
        # parent's orig_id by the orchestration framework.
        for f in finals:
            if TurnIndex(f.turn_number) not in deepen:
                yield f

        # Stage 4: spawn the deeper analysis; yield its responses.
        # The framework auto-relabels them onto the parent's orig_id
        # via the OrchestrationMiddleware's handle_response.
        deeper = _build_deeper_query(parent, sorted(deepen), q_extra)
        async for resp in ctx.spawn(deeper):
            yield resp

    return coro
