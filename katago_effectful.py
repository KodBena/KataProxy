"""
katago_effectful.py — Adaptive re-evaluation as a SessionMiddleware.

Design
──────
AdaptiveReevaluateMiddleware observes the *final* responses for a KataGo
analyze query (isDuringSearch=False, one per requested turn), identifies the
turns with the worst policy-delta statistics, and submits a deeper analysis
for those turns.

From the client's perspective the stream for a given orig_id stays open:
  • Partial responses (isDuringSearch=True) — forwarded immediately, unchanged.
  • Final responses for turns that will NOT be re-analyzed — forwarded
    immediately with isDuringSearch=False (those turns are done).
  • Final responses for turns that WILL be re-analyzed — forwarded with
    isDuringSearch patched to True (telling the client "still in progress").
  • Deeper-analysis responses arrive later under a synthetic orig_id;
    the middleware re-labels them to the real client orig_id before sending.
    The last final from the deeper pass carries isDuringSearch=False,
    completing those turns from the client's point of view.

ID namespacing
──────────────
Injected queries use a synthetic orig_id that encodes the real client ID.
They go through ClientSession._submit_raw (bypassing the Transformer — no
double-enrichment) and get an independent ProxyLink entry.  The original
query's ProxyLink entry is cleaned up normally by CompletionTracker; there is
no lifecycle interference.

Synthetic ID format:  __adap__<8 hex chars>__<real_orig_id>
"""

from __future__ import annotations

import asyncio
import uuid
from collections import OrderedDict, defaultdict
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

import sproxy_config as cfg
from AbstractProxy.katago_proxy import KataGoAction, KataGoQuery, KataGoResponse
from session_middleware import SessionMiddleware, SubmitQuery, ResponseStream

import logging
logger = logging.getLogger("kataproxy." + __name__)
from copy import deepcopy

# ---------------------------------------------------------------------------
# Synthetic-ID helpers
# ---------------------------------------------------------------------------

_PREFIX = "Q:"


def _make_synthetic_id(real_orig_id: str) -> str:
    return f"{_PREFIX}{uuid.uuid4().hex[:8]}__{real_orig_id}"


def _is_synthetic(orig_id: str) -> bool:
    return orig_id.startswith(_PREFIX)


def _real_id_of(synthetic_id: str) -> str:
    """Extract the real orig_id from a synthetic one."""
    # Format: __adap__<8hex>__<real_id>  (real_id may itself contain __)
    return synthetic_id.split("__", 2)[1]


# ---------------------------------------------------------------------------
# AdaptiveReevaluateMiddleware
# ---------------------------------------------------------------------------

class AdaptiveReevaluateMiddleware(SessionMiddleware):
    """
    Re-analyzes turns with poor policy-delta statistics at higher visit count.

    Parameters
    ----------
    worst_quantile:
        Fraction of turns considered "worst" and eligible for re-analysis.
        0.25 means the bottom 25 % by mean delta are candidates.
    extra_visits:
        Additional visits added on top of the original maxVisits for the
        deeper query.
    window_size:
        Number of turns around each worst turn to include in the deeper query
        (to give the engine positional context).
    """

    def __init__(
        self,
        worst_quantile: float = 1.0,
        extra_visits: int = 800,
        window_size: int = 3,
        max_inflight: Optional[int] = None,
    ) -> None:
        self._worst_quantile = worst_quantile
        self._extra_visits = extra_visits
        self._window_size = window_size
        # Per-session in-flight cap. None or non-positive → unbounded
        # (pre-v1.0.5 semantics, except without the panic-flush behaviour).
        self._max_inflight = (
            max_inflight if max_inflight is not None else cfg.ADAPTIVE_MAX_INFLIGHT
        )

        # orig_id → number of final responses still expected. OrderedDict
        # so insertion order is the canonical eviction order on overflow;
        # _buffered and _orig_queries are kept consistent at every
        # mutation site.
        self._expected: "OrderedDict[str, int]" = OrderedDict()
        # orig_id → buffered (turn_number, response) pairs
        self._buffered: Dict[str, List[Tuple[int, KataGoResponse]]] = {}
        # orig_id → original KataGoQuery (needed to build the deeper query)
        self._orig_queries: Dict[str, KataGoQuery] = {}

    # ------------------------------------------------------------------
    # on_query — register expected response count
    # ------------------------------------------------------------------

    def on_query(self, orig_id: str, query: KataGoQuery) -> None:
        if _is_synthetic(orig_id):
            return  # injected queries are tracked implicitly via handle_response

        if query.action != KataGoAction.ANALYZE:
            return

        turns = query.analyze_turns or []
        self._expected[orig_id] = len(turns) if turns else 1
        self._orig_queries[orig_id] = deepcopy(query)

        # Bounded LRU eviction. The pre-v1.0.5 implementation tripped a
        # panic-flush at 5000 entries that wiped every in-flight query's
        # state — a single bad client could lose every other concurrent
        # query's adaptation context. The ordered eviction here keeps the
        # newest queries' state intact and only loses the oldest, which is
        # the right trade-off when the client is genuinely producing more
        # in-flight queries than the cap allows.
        while (self._max_inflight > 0
               and len(self._expected) > self._max_inflight):
            evicted, _ = self._expected.popitem(last=False)
            self._buffered.pop(evicted, None)
            self._orig_queries.pop(evicted, None)
            logger.warning(
                f"adaptive: per-session in-flight cap {self._max_inflight} "
                f"reached; evicted oldest orig_id={evicted!r}"
            )

    # ------------------------------------------------------------------
    # handle_response — core interception logic
    # ------------------------------------------------------------------

    async def handle_response(
        self,
        orig_id: str,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        # ── Responses from injected deeper queries ──────────────────────
        if _is_synthetic(orig_id):
            # Re-label to the real client ID and pass through.
            # The deeper query's CompletionTracker will emit isDuringSearch=False
            # on its last turn, which is exactly what the client needs to consider
            # those turns complete.
            yield _real_id_of(orig_id), response
            return

        # ── Partial responses from the original query ───────────────────
        if response.is_during_search:
            yield orig_id, response
            return

        # ── Final responses from the original query ─────────────────────
        # Buffer until all expected finals have arrived.
        bucket = self._buffered.setdefault(orig_id, [])
        bucket.append((response.turn_number, response))

        if len(bucket) < self._expected.get(orig_id, 1):
            return  # more finals still expected; hold this one

        # All finals received — decide on adaptation.
        finals = self._buffered.pop(orig_id)
        self._expected.pop(orig_id, None)
        orig_query = self._orig_queries.pop(orig_id, None)

        all_turns: Set[int] = {t for t, _ in finals}
        worst_turns = self._find_worst_turns([r for _, r in finals])
        turns_to_deepen = self._expand_window(worst_turns, all_turns)

        if turns_to_deepen and orig_query is not None:
            logger.info(
                f"adaptive: orig_id={orig_id!r} "
                f"deepening turns={sorted(turns_to_deepen)}"
            )
            # Emit original finals — patched for turns that will be re-analyzed,
            # un-patched for turns that are truly complete now.
            for turn_n, final_r in finals:
                if turn_n in turns_to_deepen:
                    # Signal "still in progress" for this turn.
                    yield orig_id, KataGoResponse(
                        is_during_search=True,
                        turn_number=final_r.turn_number,
                        opaque=final_r.opaque,
                    )
                else:
                    yield orig_id, final_r  # this turn is definitively done

            # Inject deeper query under a fresh synthetic orig_id.
            synthetic_id = _make_synthetic_id(orig_id)
            deeper = self._build_deeper_query(orig_query, sorted(turns_to_deepen))
            # create_task so we don't block the current yield chain.
            asyncio.create_task(submit_query(synthetic_id, deeper))

        else:
            # No adaptation warranted — emit all finals unchanged.
            for _, final_r in finals:
                yield orig_id, final_r

    # ------------------------------------------------------------------
    # Delta-analysis helpers
    # ------------------------------------------------------------------

    def _find_worst_turns(self, responses: List[KataGoResponse]) -> List[int]:
        """Return turn numbers whose mean policy delta is in the worst quantile."""
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
        for displacement, color in [(0,"black"), (1,"white")]:
            tm = turn_maps[color]
            if not tm:
                continue
            avg_deltas = [(t, float(np.mean(ds))) for t, ds in tm.items()]
            # Threshold = value at the worst_quantile-th percentile.
            threshold = sorted(d for _, d in avg_deltas)[
                int(len(avg_deltas) * self._worst_quantile)
            ]
            moves = [t for t,d in avg_deltas if d <= threshold]
            turns = sum([[2*t + displacement, 2*t + 1 + displacement] for t in moves],[])
            worst.extend(turns)

        return worst

    def _expand_window(self, worst_turns: List[int], all_turns: Set[int]) -> Set[int]:
        """Expand each worst turn into a window of neighbouring turns."""
        expanded: Set[int] = set()
        half = self._window_size // 2
        for t in worst_turns:
            for offset in range(-half, half + 1):
                c = t + offset
                if c in all_turns:
                    expanded.add(c)
        return expanded

    def _build_deeper_query(self, orig: KataGoQuery, turns: List[int]) -> KataGoQuery:
        """Build a deeper-analysis query derived from the original."""
        new_opaque = dict(orig.opaque)
        new_opaque["maxVisits"] = new_opaque.get("maxVisits", 1000) + self._extra_visits
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
# Public factory function (matches the existing call-site style)
# ---------------------------------------------------------------------------

def adaptive_reevaluate(
    worst_quantile: float = 0.25,
    extra_visits: int = 800,
    window_size: int = 3,
) -> AdaptiveReevaluateMiddleware:
    """Return a configured AdaptiveReevaluateMiddleware instance."""
    return AdaptiveReevaluateMiddleware(worst_quantile, extra_visits, window_size)
