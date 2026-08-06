"""
transformers/analysis_enricher.py — Transformer factory that wires
DeltaAnalysisState into the proxy's response pipeline.

This is the *proxy-protocol-aware glue* between the wire-level Transformer
extension surface and the protocol-agnostic analysis substance in
delta_analysis.py. It reads ``analysis_config`` off each incoming query's
opaque payload, builds a per-eid DeltaAnalysisState via the
RegistryInterpreter (registry_interpreter.py compiles the user-supplied
analysis expressions against a curated stdlib), and on each response
attaches the analysis result to ``r.opaque['extra']``.

(Renamed and relocated in v1.0.13 from baduk.py at the proxy root.)

``extra_status`` — typed absence for the wire (v1.0.28+)
----------------------------------------------------------
Setup (RegistryInterpreter compile error, DeltaAnalysisState's own
n_moves<2 / invalid-color-token ValueErrors, or a TypeError from the
curated stdlib) can fail at ``on_query`` time, and per-packet
enrichment can raise at ``on_response`` time; per ADR-0002 both cases
log and let the query proceed unenriched. Historically the client had
no way to distinguish "enrichment skipped/failed" from "not
requested" or "no delta yet" — absence of ``r.opaque['extra']`` was
the only signal, and it was ambiguous.

This module is the **single source of truth** for
``r.opaque['extra_status']``, attached to every response of a query
that carried ``analysis_config`` on an ``ANALYZE`` action (i.e. every
response of a query for which enrichment was *in play* — regardless
of whether it went on to succeed). Responses of queries that never
carried ``analysis_config`` on ANALYZE get **no** ``extra_status`` key
at all, so old clients and non-enrichment traffic see no wire change.

Closed vocabulary:

* ``{"state": "computed"}`` — this response's enrichment ran
  ``push_packet`` successfully and ``extra`` is attached.
* ``{"state": "skipped", "reason": <token>}`` — analyzer setup failed
  at ``on_query`` time; this response (and every other response of the
  same query) is unenriched. ``reason`` is one of:

  - ``"config_error"`` — ``RegistryInterpreter`` asteval compile
    failure (syntax error, symbol shadow), or a ``TypeError`` /
    non-moves ``ValueError`` from the curated stdlib's own
    range/dtype/shape checks (malformed ``analysis_config``, not a
    moves-list problem).
  - ``"too_few_moves"`` — the ``on_query`` moves-length gate
    (``len(moves) <= 1``) or ``DeltaAnalysisState``'s own
    ``n_moves must be >= 2`` guard.
  - ``"invalid_moves"`` — ``DeltaAnalysisState``'s invalid-color-token
    guard (a move color that is not one of ``'B'``, ``'b'``, ``'W'``,
    ``'w'``), or a similar per-move data violation.
* ``{"state": "failed", "reason": "enrichment_exception"}`` — setup
  succeeded but *this* response's ``push_packet`` call raised.
* ``{"state": "not_applicable"}`` — enrichment was in play for the
  query, but this particular response cannot be enriched (not an
  ``AnalyzeResponse``, or no ``moveInfos`` — errors / interrupted
  searches, per the existing gate in ``on_response``).

**Coupling invariant** (load-bearing): ``r.opaque['extra']`` is
present on a response **iff** ``r.opaque['extra_status']['state'] ==
"computed"``. Enforced by construction below — ``extra`` and the
``computed`` status are set together in the same success branch, never
independently — and asserted by the tests in
``tests/test_analysis_enricher_extra_status.py``.

``extra_status`` is response-side only: it is written in ``on_response``
and never read back off ``q.opaque`` by any query-building path (the
only opaque-cloning query builder, ``adaptive_reevaluate._build_deeper_query``,
clones a ``KataGoQuery``'s opaque, never a response's), so it cannot
reach KataGo — there is nothing to add to
``katago/katago_proxy.py:_PROXY_ONLY_FIELDS`` for it.

The SPA-side consumer of this field is explicitly out of scope here
and is dispatched separately.
"""

from scipy.stats import entropy

from delta_analysis import DeltaAnalysisState


from typing import Any, Optional, Dict
from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink
from katago import (
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    translate_query_to_wire,
    translate_response_to_wire,
    parse_response_from_wire,
)
import numpy as np
from copy import deepcopy
import logging
from proxy_logging import Event, get_proxy_logger
logger = logging.getLogger("kataproxy." + __name__)
_log = get_proxy_logger(__name__)

def sliding_median(arr: Any, window: int) -> Any:
    return np.median(np.lib.stride_tricks.sliding_window_view(arr, (window,)), axis=1)

# NOTE: The analysis functions previously defined here as module-level lambdas
# (visit_entropy, winrate, default_delta_fn, etc.) are now user-configurable
# via the 'symbols' and 'bindings' sections of the analysis_config passed in
# each query's opaque payload.  See registry_interpreter.py for the stdlib
# helpers that are always available (_visit_entropy, _spread, _visit_ratio,
# _uservisits, …).

from registry_interpreter import RegistryInterpreter


def _classify_setup_error(e: Exception) -> str:
    """Map an on_query enrichment-setup exception to the closed
    ``extra_status`` "skipped" reason vocabulary (see module docstring).

    Honest-mapping posture: only the two named, message-identified
    ``DeltaAnalysisState`` guards get their own dedicated reason token;
    everything else (the ``RegistryInterpreter`` compile-time
    ``RuntimeError``, any other ``ValueError``, and every ``TypeError``
    from the curated stdlib's range/dtype/shape checks) is a malformed-
    ``analysis_config`` problem, not a moves-list problem, so it maps to
    ``"config_error"``.
    """
    if isinstance(e, ValueError):
        msg = str(e)
        if "n_moves must be" in msg:
            return "too_few_moves"
        if "invalid move color token" in msg:
            return "invalid_moves"
    return "config_error"


def analysis_enricher(
    link: ProxyLink[ClientId, InternalId],
) -> Transformer[KataGoQuery, KataGoResponse]:
    request_cache: Dict[ClientId, DeltaAnalysisState] = {}
    # Per-eid skip reason, set at on_query time when enrichment setup
    # failed for a query that was otherwise "in play" (ANALYZE action +
    # analysis_config present). Consumed in on_response to attach
    # extra_status; cleaned up on the same forward(eid)-is-None
    # condition request_cache already uses. eid is never a key in both
    # dicts at once — a given on_query call either lands the analyzer
    # in request_cache (success) or a reason here (failure), not both.
    skip_reasons: Dict[ClientId, str] = {}

    def on_query(eid: ClientId, q: KataGoQuery) -> Optional[KataGoQuery]:
        # Read `analysis_config` non-destructively (v1.0.21). The
        # authoritative line preventing it from reaching KataGo's stdin
        # is the central wire-strip in
        # katago/katago_proxy.py:_PROXY_ONLY_FIELDS; the pre-v1.0.21
        # destructive pop here was belt-and-braces work that the
        # wire-strip already covers, and keeping the field in opaque
        # is load-bearing for sub-queries spawned by
        # OrchestrationMiddleware (e.g., adaptive_reevaluate's deeper
        # query, whose `_build_deeper_query` clones the parent's opaque
        # via `dict(orig.opaque)`). Pre-v1.0.21, the cloned sub-query
        # opaque was stripped of `analysis_config`, the gate below
        # failed, no analyzer was cached for the sub-query's eid, and
        # the sub-query's responses reached the client with `extra`
        # undefined entirely — defeating the SPA's mergeKataExtra
        # policy of preserving populated extra against an empty one.
        #
        # KataGo's analysis-engine protocol does not define this field,
        # and forwarding it produces malformed responses on short /
        # empty queries (no moveInfos / rootInfo on returned packets,
        # observed as a frontend crash on empty-board ponder). The
        # central wire-strip is what closes that exposure.
        config = q.opaque.get('analysis_config')

        # "In play" per the extra_status wire contract (module docstring):
        # ANALYZE action + a truthy analysis_config, independent of
        # whether the moves-length gate or setup below goes on to
        # succeed. Every response of an in-play query gets an
        # extra_status key; queries that are never in play get none.
        in_play = q.action == KataGoAction.ANALYZE and bool(config)

        if in_play:
            moves = q.opaque.get('moves')
            if not moves or len(moves) <= 1:
                skip_reasons[eid] = "too_few_moves"
                return q

            _log.debug(
                Event.DIAGNOSTIC, orig=eid,
                msg=f"analysis_config setup for eid={eid!r}",
            )
            try:
                env = RegistryInterpreter(config)
                delta_fn = env.get_delta_fn()
                summary_fn = env.get_summary_fn()
                state_fns = env.get_state_fns()
                analyzer = DeltaAnalysisState(
                    q.opaque['boardXSize'],
                    q.opaque['moves'],
                    delta_fn=delta_fn,
                    summary_fn=summary_fn,
                    state_fns=state_fns,
                    triangular=True,
                )
            except (RuntimeError, TypeError, ValueError) as e:
                # RegistryInterpreter raises RuntimeError on asteval compile
                # failure (syntax error, parameter/symbol shadow of a curated
                # name, etc.); DeltaAnalysisState can raise ValueError for
                # n_moves<2; range/dtype checks in the curated stdlib raise
                # TypeError or ValueError. None of these justify killing the
                # WebSocket connection — the right posture per ADR-0002 is to
                # log loudly and let the query proceed without enrichment.
                #
                # NOTE: asteval's name resolution inside def-bodies happens
                # lazily at call time, so a body that references a name no
                # longer exposed (e.g. legacy `np.median(x)`) may compile
                # cleanly here and only fail when the pipeline invokes the
                # procedure during a response. Those failures land in the
                # on_response try/except below. The fully-structured wire
                # error path is v1.0.4 H-3 work.
                _log.warning(
                    Event.DIAGNOSTIC, orig=eid,
                    msg=(
                        f"analysis_config setup failed for eid={eid!r}: {e}. "
                        f"Query proceeds without enrichment."
                    ),
                )
                skip_reasons[eid] = _classify_setup_error(e)
                return q
            request_cache[eid] = analyzer
        return q

    def on_response(eid: ClientId, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_analyzer = request_cache.get(eid)
        skip_reason = skip_reasons.get(eid)
        # "In play" per the extra_status wire contract: either a live
        # analyzer (setup succeeded) or a recorded skip reason (setup
        # failed) is cached for this eid. request_cache and skip_reasons
        # are disjoint by construction (on_query populates exactly one
        # of them per eid), so branching on req_analyzer then skip_reason
        # below is exhaustive over the "in play" / "not in play" split.

        # Tighten the gate from "moveInfos in opaque" (which was already
        # an analyze-only check structurally) to the explicit isinstance
        # narrowing the type system needs. moveInfos remains the second
        # gate because not every analyze response carries it (errors,
        # interrupted searches).
        if req_analyzer is not None:
            if isinstance(r, AnalyzeResponse) and "moveInfos" in r.opaque:
                try:
                    analysis = req_analyzer.push_packet(r.turn_number, (r.turn_number, r.opaque))
                    # extra and the "computed" extra_status are set together,
                    # in this branch only — this is what makes the coupling
                    # invariant ("extra present iff extra_status.state ==
                    # computed") true by construction rather than by
                    # convention.
                    r.opaque['extra'] = deepcopy(analysis)
                    r.opaque['extra_status'] = {"state": "computed"}
                except Exception:
                    # asteval defers name resolution inside def-bodies to call
                    # time, so a body that referenced a no-longer-exposed name
                    # (e.g. legacy `np.median(x)`) may compile cleanly in
                    # on_query and only fail here on the first real packet.
                    _log.exception(
                        Event.DIAGNOSTIC, orig=eid,
                        msg=f"enrichment failed for eid={eid!r}",
                    )
                    r.opaque['extra_status'] = {
                        "state": "failed", "reason": "enrichment_exception",
                    }
            else:
                # Enrichment was in play for the query but this particular
                # response isn't enrichable (not an AnalyzeResponse, or no
                # moveInfos — errors / interrupted searches).
                r.opaque['extra_status'] = {"state": "not_applicable"}
        elif skip_reason is not None:
            r.opaque['extra_status'] = {"state": "skipped", "reason": skip_reason}

        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)
            skip_reasons.pop(eid, None)

        return r

    return Transformer(
        name="analysis_transformer",
        on_query=on_query,
        on_response=on_response
    )
