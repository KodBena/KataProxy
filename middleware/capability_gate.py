"""
middleware/capability_gate.py — Capability-gating wrapper for
SessionMiddleware.

Wraps a SessionMiddleware so the wrapped middleware engages on a given
query iff that query's `capabilities` field opts in to a named
capability. Legacy-compatible: queries with no `capabilities` field
auto-engage (the dispatch sign-off's Q1 answer — preserves wire
compatibility for clients that have not migrated to the
capability-aware contract).

Per-orig_id state recording the engagement decision lives in the
wrapper. on_session_end clears it, mirroring on_session_end's role
across the middleware lifecycle.

Usage at the ProxyServer composition site:

    base = CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(...),
    )

The wrapper preserves the SessionMiddleware contract exactly: every
call surface (on_session_start, on_session_end, on_query,
handle_response) delegates to the wrapped middleware on engagement
and short-circuits to a single passthrough yield otherwise.

Real-cost note: when a query opts out of `adaptive_reevaluate`, the
wrapper bypasses the wrapped middleware entirely. on_query is not
invoked; handle_response yields the response unchanged without
observing it through the wrapped middleware. The wrapped middleware
therefore never identifies positions to re-evaluate, never builds the
deeper query, never calls submit_query. KataGo never receives the
deeper-analysis request — real GPU savings, not compute-and-discard.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from typing import Dict

from katago import KataGoQuery, KataGoResponse
from middleware.session_middleware import (
    ResponseStream,
    SessionCapabilities,
    SessionMiddleware,
    SubmitQuery,
)


class CapabilityGatedMiddleware(SessionMiddleware):
    """SessionMiddleware wrapper that gates engagement on a per-query
    capability opt-in.

    See module docstring for the engagement-decision matrix and the
    real-cost guarantee for adaptive_reevaluate-style middlewares.
    """

    def __init__(self, capability: str, wrapped: SessionMiddleware) -> None:
        self._capability = capability
        self._wrapped = wrapped
        # orig_id → per-query metadata for this capability (or empty
        # dict for legacy auto-engage / opt-in-with-defaults).
        self._engaged: Dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Lifecycle delegation
    # ------------------------------------------------------------------

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self._wrapped.on_session_start(caps)

    def on_session_end(self) -> None:
        self._wrapped.on_session_end()
        self._engaged.clear()

    # ------------------------------------------------------------------
    # Query side: record engagement, conditionally delegate
    # ------------------------------------------------------------------

    def on_query(self, orig_id: str, query: KataGoQuery) -> None:
        opaque_caps = query.opaque.get("capabilities")
        if opaque_caps is None:
            # Legacy auto-engage.
            self._engaged[orig_id] = {}
            self._wrapped.on_query(orig_id, query)
            return
        if isinstance(opaque_caps, dict) and self._capability in opaque_caps:
            md = opaque_caps[self._capability]
            self._engaged[orig_id] = md if isinstance(md, dict) else {}
            self._wrapped.on_query(orig_id, query)
            return
        # Explicit opt-out: capabilities present but does not name this
        # capability. Do not register the query with the wrapped
        # middleware; handle_response will pass responses through.

    # ------------------------------------------------------------------
    # Response side: gate engagement on the recording
    # ------------------------------------------------------------------

    async def handle_response(
        self,
        orig_id: str,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        if orig_id in self._engaged:
            async for out_id, out_resp in self._wrapped.handle_response(
                orig_id, response, submit_query
            ):
                yield out_id, out_resp
        else:
            yield orig_id, response
