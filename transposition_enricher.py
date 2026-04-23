"""
transposition_enricher.py — Optional response enricher for KataGo PVs.

When the native go_transposition module is available, this transformer
post-processes analysis responses to partition the principal-variation
move list, distinguishing transpositional moves (positions reachable by
multiple orderings) from unique continuations.

The native module is OPTIONAL. If it is not importable at startup, this
module emits a single warning and the transformer becomes a no-op pass-
through. The proxy operates normally without enrichment.

See README.md ("The goboard_transposition extension") for build and
distribution notes.
"""
import logging
logger = logging.getLogger("kataproxy." + __name__)
try:
    import go_transposition as _go_transposition  # type: ignore[import]
    _TRANSPOSITION_AVAILABLE = True
except ImportError:
    _go_transposition = None  # type: ignore[assignment]
    _TRANSPOSITION_AVAILABLE = False
if not _TRANSPOSITION_AVAILABLE:
    logging.getLogger("kataproxy.transposition_enricher").warning(
        "go_transposition native module not found; "
        "transposition enrichment disabled. "
        "See README for build instructions."
    )
import json
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


def transposition_enricher(link: ProxyLink) -> Transformer[KataGoQuery, KataGoResponse]:
    """
    Stateful transformer for PV partitioning.
    Monitors the ProxyLink to know when to purge the request cache.
    """
    request_cache: Dict[str, str] = {}

    def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
        # Guard: only cache for analysis
        if q.action == KataGoAction.ANALYZE:
            wire_dict = translate_query_to_wire(q, eid)
            request_cache[eid] = json.dumps(wire_dict)
        return q

    def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_json = request_cache.get(eid)
        if _TRANSPOSITION_AVAILABLE and req_json is not None and "moveInfos" in r.opaque:
            try:
                resp_wire = translate_response_to_wire(r, eid)
                enriched_json = _go_transposition.partition_pv(req_json, json.dumps(resp_wire))
                
                enriched_dict = json.loads(enriched_json)
                _, r = parse_response_from_wire(enriched_dict) # Update 'r'
            except Exception as e:
                logger.error(f"Enrichment failed: {e}")

        # 2. THE MULTI-TURN CLEANUP:
        # Check if the ProxyLink just removed this ID mapping.
        # If link.mapping.forward(eid) is None, it means the CompletionTracker 
        # signaled QUERY_COMPLETE and the Link has finalized the query.
        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)

        return r

    return Transformer(
        name="go_transposition_pv",
        on_query=on_query,
        on_response=on_response
    )

