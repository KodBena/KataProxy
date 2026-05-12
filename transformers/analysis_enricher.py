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

def analysis_enricher(
    link: ProxyLink[ClientId, InternalId],
) -> Transformer[KataGoQuery, KataGoResponse]:
    request_cache: Dict[ClientId, DeltaAnalysisState] = {}

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

        if (
            q.action == KataGoAction.ANALYZE
            and config
            and q.opaque.get('moves')
            and len(q.opaque['moves']) > 1
        ):
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
                return q
            request_cache[eid] = analyzer
        return q

    def on_response(eid: ClientId, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_analyzer = request_cache.get(eid)
        # Tighten the gate from "moveInfos in opaque" (which was already
        # an analyze-only check structurally) to the explicit isinstance
        # narrowing the type system needs. moveInfos remains the second
        # gate because not every analyze response carries it (errors,
        # interrupted searches).
        if (
            req_analyzer is not None
            and isinstance(r, AnalyzeResponse)
            and "moveInfos" in r.opaque
        ):
            try:
                analysis = req_analyzer.push_packet(r.turn_number, (r.turn_number, r.opaque))
                r.opaque['extra'] = deepcopy(analysis)
            except Exception:
                # asteval defers name resolution inside def-bodies to call
                # time, so a body that referenced a no-longer-exposed name
                # (e.g. legacy `np.median(x)`) may compile cleanly in
                # on_query and only fail here on the first real packet.
                _log.exception(
                    Event.DIAGNOSTIC, orig=eid,
                    msg=f"enrichment failed for eid={eid!r}",
                )

        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)

        return r

    return Transformer(
        name="analysis_transformer",
        on_query=on_query,
        on_response=on_response
    )
