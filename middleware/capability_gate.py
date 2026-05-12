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
wrapper bypasses the wrapped middleware's *on_query*. The wrapped
middleware therefore never identifies positions to re-evaluate,
never builds the deeper query, never calls submit_query. KataGo
never receives the deeper-analysis request — real GPU savings,
not compute-and-discard.

Response side: handle_response unconditionally delegates to the
wrapped middleware. The wrapped middleware decides what to do
based on its own state (whether it has a context for this orig_id,
whether the orig_id is a sub-query it spawned, etc.). For opt-out
parents the wrapped's contract is to pass the response through
unchanged; OrchestrationMiddleware's "Not orchestrated; pass
through" branch satisfies this contract. The reason for
unconditional delegation is the orchestration sub-query routing
case: sub-query orig_ids are never registered in this wrapper's
self._engaged dict (sub-queries bypass middleware.on_query at
submit_query time), so a self._engaged-based gate would silently
short-circuit sub-query responses past the wrapped's relabel code
and they would reach the client carrying their synthetic
sub_orig_id rather than the parent's orig_id.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from typing import Any, Dict

from AbstractProxy.proxy_core import ClientId
from katago import KataGoQuery, KataGoResponse
from middleware.session_middleware import (
    ResponseStream,
    SessionCapabilities,
    SessionMiddleware,
    SubmitQuery,
)
from proxy_logging import Event, get_proxy_logger


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
        self._engaged: Dict[ClientId, dict] = {}
        # Structured-logging adapter; refined in on_session_start
        # to bind the session-scoped context.
        self._log: Any = get_proxy_logger("kataproxy.middleware.capability_gate")

    # ------------------------------------------------------------------
    # Lifecycle delegation
    # ------------------------------------------------------------------

    def on_session_start(self, caps: SessionCapabilities) -> None:
        if caps.proxy_log is not None:
            self._log = caps.proxy_log
        self._wrapped.on_session_start(caps)

    def on_session_end(self) -> None:
        self._wrapped.on_session_end()
        self._engaged.clear()

    # ------------------------------------------------------------------
    # Query side: record engagement, conditionally delegate
    # ------------------------------------------------------------------

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        opaque_caps = query.opaque.get("capabilities")
        if opaque_caps is None:
            # Legacy auto-engage.
            self._engaged[orig_id] = {}
            self._wrapped.on_query(orig_id, query)
            self._log.debug(
                Event.MIDDLEWARE_ENGAGE,
                cid=orig_id, orig=orig_id,
                middleware_name=type(self._wrapped).__name__,
                capability=self._capability,
                cause="legacy_auto_engage",
                msg=f"engage {self._capability} (legacy auto-engage)",
            )
            return
        if isinstance(opaque_caps, dict) and self._capability in opaque_caps:
            md = opaque_caps[self._capability]
            self._engaged[orig_id] = md if isinstance(md, dict) else {}
            self._wrapped.on_query(orig_id, query)
            self._log.debug(
                Event.MIDDLEWARE_ENGAGE,
                cid=orig_id, orig=orig_id,
                middleware_name=type(self._wrapped).__name__,
                capability=self._capability,
                cause="opt_in",
                msg=f"engage {self._capability} (per-query opt-in)",
            )
            return
        # Explicit opt-out: capabilities present but does not name this
        # capability. Do not register the query with the wrapped
        # middleware; handle_response will pass responses through.
        self._log.debug(
            Event.MIDDLEWARE_SKIP,
            cid=orig_id, orig=orig_id,
            middleware_name=type(self._wrapped).__name__,
            cause="opt_out",
            msg=f"skip {self._capability} (per-query opt-out)",
        )

    # ------------------------------------------------------------------
    # Response side: gate engagement on the recording
    # ------------------------------------------------------------------

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        # Unconditionally delegate to the wrapped. The wrapped
        # decides what to do based on its own state — engaged parents
        # processed normally, sub-queries relabeled to parent_id,
        # everything else passed through. self._engaged has no entry
        # for sub-query orig_ids (sub-queries bypass middleware.on_query),
        # so a gate based on self._engaged would silently drop sub-query
        # responses past the wrapped's relabel code; the wrapped's own
        # state-based check is the right gate. See the module docstring
        # for the full reasoning.
        async for out_id, out_resp in self._wrapped.handle_response(
            orig_id, response, submit_query
        ):
            yield out_id, out_resp
