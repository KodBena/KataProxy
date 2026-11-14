# SPDX-License-Identifier: MIT
"""KataGo response transformer for signed stencil-cover enrichment.

The public entry point composes request tracking, board reconstruction,
ownership-policy selection, and the exact-cover solver. Each concern lives in
its own collaborator under transformers.stencil_cover.
"""

from __future__ import annotations

import logging
from typing import Dict

import sproxy_config as cfg

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink
from katago import AnalyzeResponse, KataGoQuery, KataGoResponse

from transformers.stencil_cover.stencil_board import (
    RequestContext,
    _board_at_turn,
    _request_context,
)
from transformers.stencil_cover.stencil_ownership import OwnershipRegionPolicy
from transformers.stencil_cover.stencil_ownership_policy import DEFAULT_OWNERSHIP_POLICY
from transformers.stencil_cover.stencil_region_solver import _build_stencil_cover
from transformers.stencil_cover.stencil_solver_api import SolverBackend


logger = logging.getLogger("kataproxy.stencil_cover_enricher")


def stencil_cover_enricher(
    link: ProxyLink[ClientId, InternalId],
    solver_backend: SolverBackend | None = None,
    ownership_policy: OwnershipRegionPolicy | None = None,
) -> Transformer[KataGoQuery, KataGoResponse]:
    """Enrich opted-in final packets with the selected ownership policy."""
    selected_policy = (
        DEFAULT_OWNERSHIP_POLICY if ownership_policy is None else ownership_policy
    )
    request_cache: Dict[ClientId, RequestContext] = {}

    def on_query(eid: ClientId, query: KataGoQuery) -> KataGoQuery | None:
        context = _request_context(query)
        if context is not None:
            selected_policy.validate_request_metadata(context.policy_metadata)
            request_cache[eid] = context
            query.opaque["includeOwnership"] = True
            if (
                selected_policy.requires_ownership_stdev
                or cfg.STENCIL_OWNERSHIP_CORPUS_ENABLED
            ):
                query.opaque["includeOwnershipStdev"] = True
        return query

    def on_response(eid: ClientId, response: KataGoResponse) -> KataGoResponse | None:
        try:
            context = request_cache.get(eid)
            if context is not None and isinstance(response, AnalyzeResponse):
                try:
                    board = _board_at_turn(context, response.turn_number)
                    response.opaque["stencilCover"] = _build_stencil_cover(
                        response.opaque,
                        board,
                        context.width,
                        context.height,
                        context.policy_metadata,
                        selected_policy,
                        solver_backend=solver_backend,
                        turn_number=response.turn_number,
                        is_during_search=response.is_during_search,
                        response_context=response.opaque,
                    )
                except Exception:
                    logger.exception(
                        "Could not build stencilCover for query %s; "
                        "forwarding the original KataGo response",
                        eid,
                    )
            return response
        finally:
            if link.mapping.forward(eid) is None:
                request_cache.pop(eid, None)

    return Transformer(
        name="stencil_cover",
        on_query=on_query,
        on_response=on_response,
    )


__all__ = ["stencil_cover_enricher"]
