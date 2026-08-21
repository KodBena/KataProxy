"""
middleware/orchestration.py — Orchestration middleware as the third
extension surface alongside Transformer (sync per-message) and
SessionMiddleware (async per-stream).

Lets the author express orchestration of a parent query and its
spawned sub-queries as a single async coroutine using framework-
provided primitives:

  ctx.spawn(query)              — submit a sub-query; iterate responses
  ctx.parallel(*queries)        — gather-style fork-join over N sub-queries
  ctx.original_stream()         — iterate the parent's own responses
  ctx.discard_originals()       — signal "I don't want the originals"

The coroutine's `yield` emits a response on the parent's response
stream (the framework labels it with the parent's orig_id so
downstream middlewares — KeepAliveMiddleware, the WebSocket send
loop — see a coherent per-orig_id stream).

Composition with non-orchestration middlewares (Transformer,
SessionMiddleware, CapabilityGatedMiddleware) is clean. Composition
*with itself* — chaining two OrchestrationMiddlewares in a single
MiddlewareChain — is forbidden (raises MiddlewareChainConfigurationError
at chain construction). The reason is algebraic, not operational; see
proxy/docs/roadmap-orchestration-middleware.md ("On chained
orchestration: an algebraic-laws note") for the full reasoning.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from typing import Any, AsyncIterator, Callable, Optional, Union

import sproxy_config as cfg
from AbstractProxy.proxy_core import ClientId
from katago import (
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
    structured_error_wire,
)
from middleware.session_middleware import (
    ResponseStream,
    SessionCapabilities,
    SessionMiddleware,
    SubmitQuery,
)
from proxy_logging import Event, get_proxy_logger

logger = logging.getLogger("kataproxy." + __name__)
# Module-level structured-fields adapter. Used for diagnostics that
# fire from OrchestrationContext (which doesn't hold a reference to
# the per-session ProxyLogger). The session-bound logger lives on
# OrchestrationMiddleware._log; OrchestrationContext-internal
# diagnostics use this module-level one.
_log = get_proxy_logger(__name__)

__all__ = [
    "MiddlewareChainConfigurationError",
    "OrchestrationContext",
    "OrchestrationMiddleware",
    "orchestration_middleware",
]


class MiddlewareChainConfigurationError(RuntimeError):
    """Raised at MiddlewareChain construction when more than one
    OrchestrationMiddleware is present in the chain.

    The single-orchestration-per-chain limit is structural: chained
    orchestration is implementable but not algebraically composable
    under the coroutine substrate. See the roadmap for the reasoning.
    """


# ---------------------------------------------------------------------------
# Sentinels
# ---------------------------------------------------------------------------
#
# Distinguishable from any KataGoResponse / (orig_id, response) tuple by
# class identity, so a single class with a singleton instance suffices.

class _Sentinel:
    """Stream-end marker placed in queues to signal completion."""

_SENTINEL = _Sentinel()


# ---------------------------------------------------------------------------
# Per-sub-query bookkeeping
# ---------------------------------------------------------------------------

class _SubQueryRecord:
    """Per-sub-query state owned by the spawning OrchestrationContext.

    The expected/received counters mirror the ones the existing
    CompletionTracker uses on the LeafRouter / RelayRouter side; the
    framework owns these here because the orchestration coroutine
    consumes the spawn iterator before any other completion signal
    would reach it.
    """

    def __init__(self, expected_finals: int) -> None:
        self.queue: asyncio.Queue[Union[KataGoResponse, _Sentinel]] = asyncio.Queue()
        self.expected_finals = expected_finals
        self.received_finals = 0
        self.completed = False


# ---------------------------------------------------------------------------
# OrchestrationContext
# ---------------------------------------------------------------------------

class OrchestrationContext:
    """Per-parent orchestration state passed to the coroutine.

    Lifetime: one per client-originated parent query. Created when
    OrchestrationMiddleware.on_query fires; destroyed when the
    coroutine completes (return / raise / cancellation).
    """

    def __init__(
        self,
        *,
        parent_id: ClientId,
        parent_query: KataGoQuery,
        session_capabilities: SessionCapabilities,
        middleware: "OrchestrationMiddleware",
    ) -> None:
        self._parent_id = parent_id
        self._parent_query = parent_query
        self._caps = session_capabilities
        self._middleware = middleware
        # Original-stream state.
        self._original_queue: asyncio.Queue[Union[KataGoResponse, _Sentinel]] = asyncio.Queue()
        self._original_completed = False
        self._original_discarded = False
        self._original_expected = self._compute_expected_finals(parent_query)
        self._original_received = 0
        # Per-sub-query state. orig_id → _SubQueryRecord.
        self._sub_queries: dict[ClientId, _SubQueryRecord] = {}
        # NOTE: output delivery is push-based via SessionCapabilities.
        # send_response (see proxy/docs/roadmap-orchestration-output-
        # channel.md). The driver task in _drive_coroutine calls
        # caps.send_response for each yield from the coroutine; there
        # is no per-context output queue and no drain race.

    # ---------------- Static helpers ----------------

    @staticmethod
    def _compute_expected_finals(query: KataGoQuery) -> int:
        """Mirrors LeafRouter._register_query's count derivation.

        For non-analyze actions and analyze with no analyzeTurns, the
        engine emits exactly one final response. For analyze with
        explicit analyzeTurns, one final per turn.
        """
        if query.action != KataGoAction.ANALYZE:
            return 1
        if query.analyze_turns:
            return len(query.analyze_turns)
        return 1

    @staticmethod
    def _is_final(response: KataGoResponse) -> bool:
        """A response is 'final' iff it is metadata or analyze-not-during-search."""
        if isinstance(response, MetadataResponse):
            return True
        return not response.is_during_search

    # ---------------- Public read-only properties ----------------

    @property
    def parent_id(self) -> str:
        """The parent's orig_id (client namespace)."""
        return self._parent_id

    @property
    def parent_query(self) -> KataGoQuery:
        """The parent query as parsed from the client wire."""
        return self._parent_query

    @property
    def session_capabilities(self) -> SessionCapabilities:
        """Underlying session capabilities (terminate_query, etc.).

        Exposed for orchestration coroutines that need lower-level
        access — typical use is rare; the orchestration primitives
        below cover the common cases.
        """
        return self._caps

    @property
    def original_completed(self) -> bool:
        """True iff the parent query has reached its expected finals.

        Useful for coroutines that called discard_originals() but
        still need to know when the upstream is done with the parent.
        """
        return self._original_completed

    # ---------------- Spawn primitives ----------------

    async def spawn(
        self, query: KataGoQuery
    ) -> AsyncIterator[KataGoResponse]:
        """Submit a sub-query and iterate its responses.

        Yields each response as it arrives from the upstream. The
        iterator completes when the sub-query's expected number of
        finals (1 for metadata; len(analyze_turns) for analyze with
        explicit turns; 1 otherwise) has been received.

        The framework tracks the sub-query's identity under the hood
        (the parent-pointer registry); responses arrive at the
        orchestration middleware's handle_response and are routed back
        here. When this iterator completes, the registry entry is
        cleaned up automatically.

        Cancellation of the orchestration coroutine cancels all
        outstanding sub-queries via session_capabilities.terminate_query.
        """
        sub_orig_id: ClientId = ClientId(f"__orch__{uuid.uuid4().hex[:12]}")
        record = _SubQueryRecord(
            expected_finals=self._compute_expected_finals(query),
        )
        self._sub_queries[sub_orig_id] = record
        self._middleware._register_sub_query(sub_orig_id, self._parent_id)
        # Lifecycle: orchestration coroutine spawned a sub-query.
        # parent cid is on the bind chain via the parent's
        # subscribe; surface sub_orig + name explicitly.
        self._middleware._log.info(
            Event.ORCHESTRATION_SPAWN,
            cid=self._parent_id,
            sub_orig=sub_orig_id,
            orch_name=self._middleware.name,
            msg=(
                f"orchestration[{self._middleware.name}] spawn "
                f"sub={sub_orig_id} parent={self._parent_id}"
            ),
        )
        try:
            await self._caps.submit_query(sub_orig_id, query)
            while True:
                item = await record.queue.get()
                if isinstance(item, _Sentinel):
                    return
                yield item
        finally:
            self._middleware._unregister_sub_query(sub_orig_id)
            self._sub_queries.pop(sub_orig_id, None)

    async def parallel(
        self, *queries: KataGoQuery
    ) -> list[list[KataGoResponse]]:
        """Spawn N sub-queries; gather; return per-query response lists.

        Convenience over

            await asyncio.gather(*[
                _collect(self.spawn(q)) for q in queries
            ])

        where `_collect` collects an async iterator into a list. Any
        sub-query raising propagates as an exception (matches
        asyncio.gather's default return_exceptions=False semantic).
        Use spawn() directly with explicit error handling if a
        per-sub-query error policy is needed.
        """
        async def collect(q: KataGoQuery) -> list[KataGoResponse]:
            return [r async for r in self.spawn(q)]
        return await asyncio.gather(*[collect(q) for q in queries])

    # ---------------- Original-stream primitives ----------------

    async def original_stream(self) -> AsyncIterator[KataGoResponse]:
        """Iterate the parent query's own responses.

        Yields each response as it arrives from the upstream. The
        iterator completes when the parent's expected finals have
        been received. Coroutines that want to forward originals
        unchanged write `async for resp in ctx.original_stream(): yield resp`;
        coroutines that want to filter or modify originals do so in
        the loop body.

        If the coroutine never iterates this stream, originals are
        buffered (bounded by cfg.ORCHESTRATION_BUFFER_MAX) until the
        coroutine completes; on overflow the oldest are dropped with
        a WARNING. Coroutines that don't want originals at all should
        call discard_originals() instead — it releases the buffer
        immediately and silences the overflow warning.
        """
        while True:
            item = await self._original_queue.get()
            if isinstance(item, _Sentinel):
                return
            yield item

    async def discard_originals(self) -> None:
        """Signal that the coroutine will not iterate original_stream.

        Releases the original-stream buffer immediately. Useful for
        coroutines that fully replace the original (e.g., a hypothetical
        jsd_compare that wants only the JSD-annotated derived
        responses, not the per-model originals). The parent query
        still runs upstream and its responses still arrive at the
        framework — they're dropped silently after this call. The
        parent's natural completion (the last expected final) still
        flips ctx.original_completed for coroutines that want to
        observe the upstream's progress without consuming it.
        """
        self._original_discarded = True
        # Drain any buffered originals.
        while not self._original_queue.empty():
            try:
                self._original_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    # ---------------- Framework-internal pushers ----------------

    async def _push_original(self, response: KataGoResponse) -> None:
        """Push a parent-original response into the stream (or count and drop)."""
        is_final = self._is_final(response)
        if is_final:
            self._original_received += 1
            if self._original_received >= self._original_expected:
                self._original_completed = True
        if self._original_discarded:
            return
        # Bounded buffer: drop oldest with WARNING on overflow.
        if (
            cfg.ORCHESTRATION_BUFFER_MAX > 0
            and self._original_queue.qsize() >= cfg.ORCHESTRATION_BUFFER_MAX
        ):
            try:
                self._original_queue.get_nowait()
                _log.warning(
                    Event.DIAGNOSTIC,
                    cid=self._parent_id,
                    msg=(
                        f"orchestration[{self._parent_id}]: original_stream "
                        f"buffer overflow ({cfg.ORCHESTRATION_BUFFER_MAX}); "
                        f"dropped oldest. The coroutine should iterate "
                        f"ctx.original_stream() or call ctx.discard_originals()."
                    ),
                )
            except asyncio.QueueEmpty:
                pass
        await self._original_queue.put(response)
        if self._original_completed:
            await self._original_queue.put(_SENTINEL)

    async def _push_sub_response(
        self, sub_orig_id: ClientId, response: KataGoResponse
    ) -> None:
        """Route a sub-query response to its spawn iterator."""
        record = self._sub_queries.get(sub_orig_id)
        if record is None:
            _log.info(
                Event.DIAGNOSTIC,
                cid=self._parent_id, sub_orig=sub_orig_id,
                msg=(
                    f"orchestration[{self._parent_id}]: stray response for "
                    f"sub_orig_id={sub_orig_id!r}; coroutine no longer iterating"
                ),
            )
            return
        is_final = self._is_final(response)
        await record.queue.put(response)
        if is_final:
            record.received_finals += 1
            if record.received_finals >= record.expected_finals:
                record.completed = True
                await record.queue.put(_SENTINEL)


# ---------------------------------------------------------------------------
# OrchestrationMiddleware
# ---------------------------------------------------------------------------

class OrchestrationMiddleware(SessionMiddleware):
    """SessionMiddleware that drives one orchestration coroutine per
    parent query.

    See OrchestrationContext for the per-parent state and the
    framework-provided primitives the coroutine receives. See the
    decorator orchestration_middleware below for the typical
    construction shape.
    """

    def __init__(
        self,
        coro_factory: Callable[
            [KataGoQuery, OrchestrationContext], AsyncIterator[KataGoResponse]
        ],
        *,
        name: str,
    ) -> None:
        self._coro_factory = coro_factory
        self.name = name  # exposed for chain-debug logs and the chain guard
        self._caps: Optional[SessionCapabilities] = None
        # parent_id → context
        self._contexts: dict[ClientId, OrchestrationContext] = {}
        # parent_id → coroutine driver task
        self._tasks: dict[ClientId, asyncio.Task[None]] = {}
        # sub_orig_id → parent_id  (for response routing)
        self._sub_to_parent: dict[ClientId, ClientId] = {}
        # Structured-logging adapter; refined in on_session_start.
        self._log: Any = get_proxy_logger("kataproxy.middleware.orchestration")

    # ---------------- Lifecycle ----------------

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self._caps = caps
        if caps.proxy_log is not None:
            self._log = caps.proxy_log

    def on_session_end(self) -> None:
        # Cancel all live orchestration tasks; their finally blocks run
        # the per-parent cleanup (terminate sub-queries, drop
        # registry entries).
        for task in list(self._tasks.values()):
            if not task.done():
                task.cancel()
        # The tasks' finally blocks will pop their entries from
        # self._contexts / self._tasks / self._sub_to_parent; clear
        # everything here as belt-and-braces.
        self._contexts.clear()
        self._tasks.clear()
        self._sub_to_parent.clear()

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        """Spawn an orchestration coroutine for this parent query.

        Sub-queries (submitted via SessionCapabilities.submit_query
        from a coroutine's ctx.spawn) bypass middleware on_query by
        the existing submit_query → _handle_query path, so this
        method is only ever called for client-originated parents.
        """
        if self._caps is None:
            self._log.error(
                Event.DIAGNOSTIC,
                cid=orig_id, orig=orig_id,
                msg=(
                    f"orchestration[{self.name}]: on_query before "
                    f"on_session_start; ignoring orig_id={orig_id!r}"
                ),
            )
            return
        if orig_id in self._contexts:
            self._log.warning(
                Event.DIAGNOSTIC,
                cid=orig_id, orig=orig_id,
                msg=(
                    f"orchestration[{self.name}]: duplicate on_query for "
                    f"orig_id={orig_id!r}; cancelling prior context"
                ),
            )
            old_task = self._tasks.pop(orig_id, None)
            if old_task is not None and not old_task.done():
                old_task.cancel()
            self._contexts.pop(orig_id, None)
        # v1.0.26 (Phase 3.5 follow-up) — snapshot the query's opaque
        # dict before the Hub's post-subscribe strip can mutate it
        # (pubsub_hub.py:494 pops `capabilities`). The coro runs
        # async-scheduled, so by the time it reads
        # `parent.opaque["capabilities"]` the Hub has already
        # processed and stripped. Without this snapshot, every
        # capability-metadata-aware orchestration middleware (notably
        # adaptive_reevaluate's worst_quantile / extra_visits / Phase 3
        # fields) reads stale-empty cap_meta and silently falls back
        # to closure defaults. Discovered 2026-05-18 when the SPA's
        # learned-VF dropdown's `value_binding` / `allocation_algorithm`
        # never reached the substrate.
        import copy as _copy
        query_for_coro = KataGoQuery(
            action=query.action,
            analyze_turns=query.analyze_turns,
            opaque=_copy.deepcopy(query.opaque),
        )
        ctx = OrchestrationContext(
            parent_id=orig_id,
            parent_query=query_for_coro,
            session_capabilities=self._caps,
            middleware=self,
        )
        self._contexts[orig_id] = ctx
        coro = self._coro_factory(query_for_coro, ctx)
        task = asyncio.create_task(
            self._drive_coroutine(orig_id, ctx, coro),
            name=f"orchestration:{self.name}:{orig_id}",
        )
        self._tasks[orig_id] = task

    async def _drive_coroutine(
        self,
        parent_id: ClientId,
        ctx: OrchestrationContext,
        coro: AsyncIterator[KataGoResponse],
    ) -> None:
        """Iterate the user's coroutine; push each yield via
        SessionCapabilities.send_response.

        Output is push-based — each yield from the coroutine is
        delivered directly onto the session's WebSocket via
        caps.send_response. This decouples output delivery from the
        timing of incoming wire arrivals, closing the drain/driver
        race in the prior output-queue + handle_response.drain design.
        See proxy/docs/roadmap-orchestration-output-channel.md.

        On normal completion: nothing further is required; the
        coroutine's emissions have all reached the wire.

        On exception (other than CancelledError): synthesise a
        structured error response (per ADR-0002) and push it via
        caps.send_response so the client sees the failure rather than
        hanging. The error path uses the same channel as normal yields,
        so trailing error responses are NOT stranded either.

        On cancellation: re-raise so the asyncio task is genuinely
        cancelled. The finally block runs in either case to clean up
        in-flight sub-queries and remove registry entries.
        """
        outcome = "normal"
        try:
            async for resp in coro:
                if self._caps is not None:
                    await self._caps.send_response(parent_id, resp)
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception as e:
            outcome = "error"
            self._log.exception(
                Event.DIAGNOSTIC,
                cid=parent_id,
                msg=(
                    f"orchestration[{self.name}]: coroutine raised for "
                    f"parent_id={parent_id!r}: {e}"
                ),
            )
            # structured_error_wire is the single writer of the
            # client-facing error shape; as the response's opaque it
            # contributes the "error" key while the envelope id comes
            # from translate_response_to_wire's parent_id relabelling.
            err_response = MetadataResponse(
                opaque=structured_error_wire(
                    f"orchestration error in {self.name}: {e}",
                ),
            )
            if self._caps is not None:
                try:
                    await self._caps.send_response(parent_id, err_response)
                except Exception:
                    self._log.exception(
                        Event.DIAGNOSTIC,
                        cid=parent_id,
                        msg=(
                            f"orchestration[{self.name}]: send_response "
                            f"failed while delivering error response for "
                            f"parent_id={parent_id!r}"
                        ),
                    )
        finally:
            # Lifecycle: orchestration coroutine completed (one of
            # three outcomes). Logged once per parent query at INFO
            # level — operators tracing one cid through the orch
            # framework see the spawn at the start, the done at the
            # end, with the outcome surfaced.
            self._log.info(
                Event.ORCHESTRATION_DONE,
                cid=parent_id, orch_name=self.name, outcome=outcome,
                msg=f"orchestration[{self.name}] {outcome} for {parent_id}",
            )
            # Cancel any still-in-flight sub-queries.
            for sub_orig_id in list(ctx._sub_queries.keys()):
                try:
                    if self._caps is not None:
                        await self._caps.terminate_query(sub_orig_id)
                except Exception:
                    self._log.debug(
                        Event.DIAGNOSTIC,
                        cid=parent_id, sub_orig=sub_orig_id,
                        msg=(
                            f"orchestration[{self.name}]: cleanup terminate "
                            f"of sub_orig_id={sub_orig_id!r} failed (likely "
                            f"already cleaned up)"
                        ),
                    )
            # Drop registry entries.
            self._contexts.pop(parent_id, None)
            self._tasks.pop(parent_id, None)
            for sub_orig_id in list(ctx._sub_queries.keys()):
                self._sub_to_parent.pop(sub_orig_id, None)

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        """Route the response to the right consumer.

        Routing rules:
          - orig_id is a known parent (in _contexts) → push to that
            parent's original_stream. The orchestration coroutine
            consumes the response asynchronously; its yields reach
            the wire via caps.send_response, not via this
            handle_response's yields.
          - orig_id is a known sub-query (in _sub_to_parent) → push
            to the parent's spawn iterator for that sub-query. Same
            asynchronous flow: yields go via caps.send_response.
          - else → not orchestrated; pass through unchanged.

        Yields nothing for orchestrated orig_ids. The orchestration
        coroutine's output flows through caps.send_response directly
        to the WebSocket, bypassing the drain race that the
        prior output-queue design suffered.
        """
        ctx = self._contexts.get(orig_id)
        if ctx is not None:
            await ctx._push_original(response)
            return
        parent_id = self._sub_to_parent.get(orig_id)
        if parent_id is None:
            # Not orchestrated; pass through.
            yield orig_id, response
            return
        ctx = self._contexts.get(parent_id)
        if ctx is None:
            # Race: parent's coroutine completed between
            # _sub_to_parent registration and this response
            # arrival. Drop silently (the framework's cleanup
            # path has already cancelled the sub-query).
            self._log.debug(
                Event.DIAGNOSTIC,
                cid=parent_id, sub_orig=orig_id,
                msg=(
                    f"orchestration[{self.name}]: sub-query response for "
                    f"orig_id={orig_id!r} arrived after parent "
                    f"{parent_id!r} cleaned up; dropping"
                ),
            )
            return
        await ctx._push_sub_response(orig_id, response)

    # ---------------- Framework-internal hooks for OrchestrationContext ----

    def _register_sub_query(self, sub_orig_id: ClientId, parent_id: ClientId) -> None:
        self._sub_to_parent[sub_orig_id] = parent_id

    def _unregister_sub_query(self, sub_orig_id: ClientId) -> None:
        self._sub_to_parent.pop(sub_orig_id, None)


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def orchestration_middleware(
    *, name: str
) -> Callable[
    [Callable[[KataGoQuery, OrchestrationContext], AsyncIterator[KataGoResponse]]],
    Callable[[], OrchestrationMiddleware],
]:
    """Wrap an async coroutine into an orchestration middleware factory.

    Usage:

        def adaptive_reevaluate(
            worst_quantile: float = 0.25,
            extra_visits: int = 800,
            window_size: int = 3,
        ) -> Callable[[], OrchestrationMiddleware]:

            @orchestration_middleware(name="adaptive_reevaluate")
            async def coro(parent: KataGoQuery, ctx: OrchestrationContext):
                # Closure captures the parameters above.
                async for resp in ctx.original_stream():
                    yield resp
                # ... etc.

            return coro

    The decorator returns a *factory* (callable taking no arguments
    and returning an OrchestrationMiddleware) so callers register it
    in proxy_server.py's _make_middleware exactly the way they
    register existing middleware factories — adapter wrappers like
    CapabilityGatedMiddleware compose with the factory's product
    transparently.
    """
    def decorator(
        coro: Callable[
            [KataGoQuery, OrchestrationContext],
            AsyncIterator[KataGoResponse],
        ],
    ) -> Callable[[], OrchestrationMiddleware]:
        @functools.wraps(coro)
        def factory() -> OrchestrationMiddleware:
            return OrchestrationMiddleware(coro_factory=coro, name=name)
        return factory
    return decorator
