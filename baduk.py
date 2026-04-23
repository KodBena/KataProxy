import traceback
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
        # Guard: only cache for analysis
        if q.action == KataGoAction.ANALYZE and q.opaque and len(q.opaque['moves']) > 1:
            config = q.opaque.get('analysis_config')
            logger.debug(f"analysis_config = {config}")
            if not config:
                return q

            env = RegistryInterpreter(config)
            delta_fn = env.get_delta_fn()
            summary_fn = env.get_summary_fn()
            state_fns = env.get_state_fns()
            analyzer = BadukAnalysisState(
                q.opaque['boardXSize'],
                q.opaque['moves'],
                delta_fn=delta_fn,
                summary_fn=summary_fn,   # was incorrectly hardcoded to default_summary_fn
                state_fns=state_fns,
                triangular=True,
            )
            request_cache[eid] = analyzer
            del q.opaque['analysis_config']
        return q

    def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_analyzer = request_cache.get(eid)
        if req_analyzer is not None and "moveInfos" in r.opaque:
            try:
                analysis = req_analyzer.push_packet(r.turn_number, (r.turn_number, r.opaque))
                r.opaque['extra'] = deepcopy(analysis)
            except Exception as e:
                print(traceback.format_exc())
                logger.error("Enrichment failed: {e}")

        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)

        return r

    return Transformer(
        name="analysis_transformer",
        on_query=on_query,
        on_response=on_response
    )
