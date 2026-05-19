"""
middleware/session_middleware.py — Session-level async middleware for
response interception.

Architecture
────────────
SessionMiddleware operates at the ClientSession level, *above* TransformedChain.
It sees responses in the client's orig_id namespace (after translate_upstream has
already run), and can:

  - Pass responses through unchanged
  - Suppress or modify responses
  - Buffer responses and release them later (with modified payloads)
  - Inject new queries back into the same session pipeline via submit_query

Crucially, because it runs AFTER translate_upstream, the ProxyLink's
CompletionTracker and IdMapping have already been updated normally for the
original query. Injected queries use fresh synthetic orig_ids and go through
_submit_raw (bypassing the Transformer), giving them entirely independent
ProxyLink entries with no lifecycle interference.

Flow
────

  KataGo engine
      ↓
  hub.on_response  → subscriber_internal_id relabelling → _send_queue
      ↓
  _deliver_upstream
      ↓
  chain.translate_upstream   (ProxyLink: si_id → orig_id, tracker advances,
                               mapping possibly cleaned up, Transformer applied)
      ↓
  middleware.handle_response  (orig_id namespace — THIS FILE)
      ↓
  WebSocket.send

Composition
───────────
Middlewares compose via MiddlewareChain. The inner middleware runs first; its
output is fed into the outer one. This mirrors the Transformer.then() convention.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable

from AbstractProxy.proxy_core import ClientId
from katago import KataGoQuery, KataGoResponse

__all__ = [
    "SessionMiddleware",
    "IdentityMiddleware",
    "MiddlewareChain",
    "SessionCapabilities",
    "SubmitQuery",
    "TerminateQuery",
    "SendResponse",
    "ResponseStream",
]

# ---------------------------------------------------------------------------
# Public type aliases
#
# SubmitQuery: the callback signature for injecting analyze queries. Callers
#   pass a synthetic orig_id and a KataGoQuery; the session routes it through
#   the full transformer + hub/router pipeline independently.
#
# TerminateQuery: the callback for terminating an in-flight query by orig_id.
#   Wraps _handle_terminate internally; failure modes (already-completed
#   query, untranslatable orig_id) are logged and return cleanly. Routes
#   through the now-coalescing-aware terminate path, so middleware-initiated
#   terminations respect coalescing transparency without extra work.
#
# SendResponse: the callback for injecting a synthetic response from a
#   middleware that produces output decoupled from incoming wire arrivals
#   (typically the orchestration framework's driver task; see
#   docs/roadmap-orchestration-output-channel.md). Wire-encodes the
#   response under the given orig_id, logs lifecycle.forward, sends on
#   the WebSocket. Bypasses both the transformer chain (the response
#   was already processed upstream of the middleware producing it) and
#   the middleware chain itself (the producing middleware is the one
#   driving the call).
#
# ResponseStream: what every handle_response implementation must return. Using
#   AsyncGenerator (rather than AsyncIterator) aligns the alias with what
#   Python actually produces from an `async def` + `yield` body, eliminating
#   the subtype mismatch that would otherwise require # type: ignore on every
#   concrete class.
# ---------------------------------------------------------------------------

SubmitQuery = Callable[[ClientId, KataGoQuery], Awaitable[None]]
TerminateQuery = Callable[[ClientId], Awaitable[None]]
SendResponse = Callable[[ClientId, KataGoResponse], Awaitable[None]]
ResponseStream = AsyncIterator[tuple[ClientId, KataGoResponse]]


async def _send_response_not_wired(
    orig_id: ClientId, response: KataGoResponse,
) -> None:
    """Default for SessionCapabilities.send_response when a constructor
    site has not been updated to wire the real implementation.

    Raises NotImplementedError per ADR-0002 (fail loudly): any caller
    invoking this default has either (a) constructed
    SessionCapabilities without threading the session's
    `_send_response` method into it, or (b) constructed it in a test
    harness that exercises the push-based output channel without
    providing a stub. Either way, silent no-op would mask a wiring
    bug; the explicit raise surfaces it.
    """
    raise NotImplementedError(
        "SessionCapabilities.send_response was not wired by the "
        f"constructor (called for orig_id={orig_id!r}). The push-based "
        "output channel introduced by the orchestration output-channel "
        "arc (see proxy/docs/roadmap-orchestration-output-channel.md) "
        "requires ClientSession-side wiring or a test-harness stub."
    )


# ---------------------------------------------------------------------------
# SessionCapabilities — the lifetime-of-the-session callback bundle
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SessionCapabilities:
    """Callbacks the session exposes to middleware for the session's lifetime.

    Constructed once per session; passed to `SessionMiddleware.on_session_start`
    so middleware can stash references for use from session-scoped tasks.
    Frozen to make the contract clear: middleware cannot mutate or extend
    the capability surface.

    ``send_response`` is the push-based output channel: a middleware
    that produces a response decoupled from incoming-wire arrivals
    (the orchestration framework's driver task is the worked example)
    calls this to push the response onto the session's WebSocket. The
    default raises NotImplementedError per ADR-0002 — constructor
    sites that don't wire a real implementation get a loud error
    rather than a silent drop. See
    ``proxy/docs/roadmap-orchestration-output-channel.md`` for the
    rationale.

    The ``proxy_log`` field is the session-bound structured-logging
    adapter (ProxyLogger). Middleware that emits structured records
    stashes it from on_session_start and refines via .bind() as
    needed. ``None`` is permitted for tests / harnesses that haven't
    threaded a proxy_log through; middleware is responsible for
    falling back to a no-bind module-level logger in that case.
    Typed as ``Any`` to avoid a runtime import dependency on
    proxy_logging from this module (would create a fragile import
    cycle at module load).
    """
    submit_query: SubmitQuery
    terminate_query: TerminateQuery
    send_response: SendResponse = _send_response_not_wired
    proxy_log: Any = None


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class SessionMiddleware(ABC):
    """
    Intercepts the outgoing response stream at the session level.

    Implemented as an async generator per response so that a single incoming
    response can produce zero, one, or many outgoing (orig_id, response) pairs,
    enabling buffering, suppression, fan-out, and re-labelling.

    Thread-safety: ClientSession is single-threaded (asyncio event loop), so
    implementations need not be thread-safe.
    """

    def on_session_start(self, caps: SessionCapabilities) -> None:
        """Called once after instantiation, before any on_query/handle_response.

        Override to stash capabilities for later use (e.g., from a
        session-scoped asyncio task) or to spawn such a task. The default
        implementation is a no-op.

        Called from within the session's event loop, so async task creation
        (`asyncio.create_task`) is safe.
        """

    def on_session_end(self) -> None:
        """Called once during session cleanup, after the hub.unsubscribe loop
        and after any orphan-termination calls.

        Override to cancel session-scoped tasks and release resources. The
        default implementation is a no-op.
        """

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        """Called synchronously when a client query is received, before routing.

        Use to record expected response counts or annotate per-query state.
        The default implementation is a no-op; override selectively.
        """

    @abstractmethod
    def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        """Process one translated response and yield the responses to send.

        Parameters
        ----------
        orig_id:
            The client-visible ID of the query this response belongs to.
        response:
            The translated (and Transformer-processed) response payload.
        submit_query:
            Injects a new query into the session pipeline under a synthetic
            orig_id, giving it an independent ProxyLink entry.
        """


# ---------------------------------------------------------------------------
# Concrete implementations
# ---------------------------------------------------------------------------

class IdentityMiddleware(SessionMiddleware):
    """Pass every response through unchanged. The do-nothing default."""

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        yield orig_id, response


class MiddlewareChain(SessionMiddleware):
    """Compose two middlewares: inner first, outer second.

    Each (orig_id, response) pair yielded by inner is fed into outer.
    The final stream is everything outer yields, across all inner outputs.

    Multi-orchestration guard: at most one OrchestrationMiddleware
    (from middleware/orchestration.py) may be present anywhere in
    the chain. Chained orchestration is implementable but not
    algebraically composable under the coroutine substrate; lifting
    the limit requires lifting the abstraction's algebraic floor
    (a true effects system). Until then, the limit is the honest
    scope of the abstraction. Violations raise
    MiddlewareChainConfigurationError at construction. See
    proxy/docs/roadmap-orchestration-middleware.md ("On chained
    orchestration: an algebraic-laws note") for the full reasoning.
    """

    def __init__(self, inner: SessionMiddleware, outer: SessionMiddleware) -> None:
        self._inner = inner
        self._outer = outer
        self._guard_orchestration_count(inner, outer)

    @staticmethod
    def _guard_orchestration_count(
        inner: SessionMiddleware, outer: SessionMiddleware
    ) -> None:
        """Raise MiddlewareChainConfigurationError on multi-orchestration chains.

        Counts OrchestrationMiddleware instances across both branches,
        recursing into nested MiddlewareChains so the guard is robust
        against multi-level composition.
        """
        # Lazy import: orchestration imports session_middleware, so a
        # top-level import here would cycle. Imported on construction
        # only (cheap; chain construction is rare).
        from middleware.orchestration import (
            MiddlewareChainConfigurationError,
            OrchestrationMiddleware,
        )

        def count(m: SessionMiddleware) -> int:
            if isinstance(m, MiddlewareChain):
                return count(m._inner) + count(m._outer)
            return 1 if isinstance(m, OrchestrationMiddleware) else 0

        total = count(inner) + count(outer)
        if total > 1:
            raise MiddlewareChainConfigurationError(
                f"MiddlewareChain contains {total} OrchestrationMiddleware "
                f"instances; at most one is permitted per chain. Chained "
                f"orchestration is not algebraically composable under the "
                f"coroutine substrate (see roadmap-orchestration-middleware.md, "
                f"\"On chained orchestration\")."
            )

    def on_session_start(self, caps: SessionCapabilities) -> None:
        # Inner first, outer second — same convention as on_query.
        self._inner.on_session_start(caps)
        self._outer.on_session_start(caps)

    def on_session_end(self) -> None:
        # Outer first, inner second — reverse-of-construction is the safer
        # teardown convention (outer's tasks may depend on inner's state).
        self._outer.on_session_end()
        self._inner.on_session_end()

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        self._inner.on_query(orig_id, query)
        self._outer.on_query(orig_id, query)

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        async for mid_id, mid_resp in self._inner.handle_response(
            orig_id, response, submit_query
        ):
            async for out_id, out_resp in self._outer.handle_response(
                mid_id, mid_resp, submit_query
            ):
                yield out_id, out_resp
