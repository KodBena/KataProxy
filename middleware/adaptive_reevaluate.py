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
from dataclasses import replace
from typing import AsyncIterator, Callable, Dict, List, Optional, Set, Tuple

import numpy as np

from katago import (
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
)
from middleware.orchestration import (
    OrchestrationContext,
    OrchestrationMiddleware,
    orchestration_middleware,
)
from proxy_logging import Event, get_proxy_logger

logger = logging.getLogger("kataproxy." + __name__)
_log = get_proxy_logger(__name__)


# ---------------------------------------------------------------------------
# Pure helpers (unchanged signatures from the pre-v1.0.16 imperative impl)
# ---------------------------------------------------------------------------

def _find_worst_turns(
    responses: List[AnalyzeResponse], quantile: float,
) -> List[int]:
    """Return turn numbers whose mean policy delta is in the worst quantile.

    `quantile` is per-orig_id (read from the query's
    capabilities.adaptive_reevaluate metadata, falling back to the
    constructor-time default).
    """
    turn_maps: Dict[str, Dict[int, List[float]]] = {
        "black": defaultdict(list),
        "white": defaultdict(list),
    }
    for resp in responses:
        for color in ("black", "white"):
            deltas = resp.opaque.get("extra", {}).get(color, {}).get("deltas")
            if isinstance(deltas, dict):
                for t, d in deltas.items():
                    turn_maps[color][int(t)].append(float(d))

    worst: List[int] = []
    for displacement, color in [(0, "black"), (1, "white")]:
        tm = turn_maps[color]
        if not tm:
            continue
        avg_deltas = [(t, float(np.mean(ds))) for t, ds in tm.items()]
        threshold = sorted(d for _, d in avg_deltas)[
            int(len(avg_deltas) * quantile)
        ]
        moves = [t for t, d in avg_deltas if d <= threshold]
        turns = sum(
            [[2 * t + displacement, 2 * t + 1 + displacement] for t in moves],
            [],
        )
        worst.extend(turns)

    return worst


def _expand_window(
    worst_turns: List[int], all_turns: Set[int], window_size: int,
) -> Set[int]:
    """Expand each worst turn into a window of neighbouring turns."""
    expanded: Set[int] = set()
    half = window_size // 2
    for t in worst_turns:
        for offset in range(-half, half + 1):
            c = t + offset
            if c in all_turns:
                expanded.add(c)
    return expanded


def _build_deeper_query(
    orig: KataGoQuery, turns: List[int], extra_visits: int,
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
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=turns,
        opaque=new_opaque,
    )


# ---------------------------------------------------------------------------
# adaptive_reevaluate factory (orchestration-shaped)
# ---------------------------------------------------------------------------

def adaptive_reevaluate(
    worst_quantile: float = 0.25,
    extra_visits: int = 800,
    window_size: int = 3,
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
                window_size=3,
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

        # Stage 2: decide on adaptation.
        all_turns: Set[int] = {f.turn_number for f in finals}
        worst = _find_worst_turns(finals, q_quantile)
        deepen = _expand_window(worst, all_turns, window_size)

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
            if f.turn_number not in deepen:
                yield f

        # Stage 4: spawn the deeper analysis; yield its responses.
        # The framework auto-relabels them onto the parent's orig_id
        # via the OrchestrationMiddleware's handle_response.
        deeper = _build_deeper_query(parent, sorted(deepen), q_extra)
        async for resp in ctx.spawn(deeper):
            yield resp

    return coro
