from scipy.stats import entropy

# ===========================================================================
# Baduk (Go) reactive multi-resolution analysis pipeline
# ===========================================================================


from bsa import BadukAnalysisState


from typing import Optional, Dict
from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ProxyLink
from AbstractProxy.katago_proxy import (
    KataGoAction,
    KataGoQuery, 
    KataGoResponse, 
    translate_query_to_wire, 
    translate_response_to_wire,
    parse_response_from_wire
)
import numpy as np
from copy import deepcopy
import logging
logger = logging.getLogger("kataproxy." + __name__)

def sliding_median(arr, window):
    return np.median(np.lib.stride_tricks.sliding_window_view(arr, (window,)), axis=1)

# NOTE: The analysis functions previously defined here as module-level lambdas
# (visit_entropy, winrate, default_delta_fn, etc.) are now user-configurable
# via the 'symbols' and 'bindings' sections of the analysis_config passed in
# each query's opaque payload.  See reginterp.py for the stdlib helpers that
# are always available (_visit_entropy, _spread, _visit_ratio, _uservisits, …).

from reginterp import RegistryInterpreter

def analysis_enricher(link: ProxyLink) -> Transformer[KataGoQuery, KataGoResponse]:
    request_cache: Dict[str, BadukAnalysisState] = {}

    def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
        # `analysis_config` is consumed by this transformer and must NEVER
        # reach KataGo's stdin — KataGo's analysis-engine protocol does
        # not define this field, and forwarding it produces malformed
        # responses on short / empty queries (no moveInfos / rootInfo on
        # returned packets, observed as a frontend crash on empty-board
        # ponder). The strip is unconditional; analyser setup is gated
        # below because BadukAnalysisState requires ≥2 moves to compute
        # meaningful deltas, but that gate must NOT also gate the strip.
        config = q.opaque.pop('analysis_config', None)

        if (
            q.action == KataGoAction.ANALYZE
            and config
            and q.opaque.get('moves')
            and len(q.opaque['moves']) > 1
        ):
            logger.debug(f"analysis_config = {config}")
            try:
                env = RegistryInterpreter(config)
                delta_fn = env.get_delta_fn()
                summary_fn = env.get_summary_fn()
                state_fns = env.get_state_fns()
                analyzer = BadukAnalysisState(
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
                # name, etc.); BadukAnalysisState can raise ValueError for
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
                logger.warning(
                    f"analysis_config setup failed for eid={eid!r}: {e}. "
                    f"Query proceeds without enrichment."
                )
                return q
            request_cache[eid] = analyzer
        return q

    def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_analyzer = request_cache.get(eid)
        if req_analyzer is not None and "moveInfos" in r.opaque:
            try:
                analysis = req_analyzer.push_packet(r.turn_number, (r.turn_number, r.opaque))
                r.opaque['extra'] = deepcopy(analysis)
            except Exception:
                # asteval defers name resolution inside def-bodies to call
                # time, so a body that referenced a no-longer-exposed name
                # (e.g. legacy `np.median(x)`) may compile cleanly in
                # on_query and only fail here on the first real packet.
                logger.exception("Enrichment failed")

        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)

        return r

    return Transformer(
        name="analysis_transformer",
        on_query=on_query,
        on_response=on_response
    )
