"""
tests/test_orchestration_middleware.py — Phase 1 (v1.0.16) tests for
the orchestration middleware primitive.

Covers:

  - OrchestrationContext primitives in isolation: spawn / parallel
    iteration semantics; original_stream / discard_originals
    behaviour; original_completed signal; bounded buffer overflow
    with WARNING.
  - Coroutine lifecycle: spawn-on-on_query, yields routed to the
    output queue, completion signals handle_response, cancellation
    on session end, structured error response on unhandled exception.
  - Composition with CapabilityGatedMiddleware: per-query opt-out
    short-circuits before the orchestration coroutine instantiates.
  - Composition with MiddlewareChain: single orchestration in a
    chain works; two raise MiddlewareChainConfigurationError at
    construction; nested MiddlewareChain composition still trips
    the guard.

Run from the proxy directory:
  pytest tests/test_orchestration_middleware.py

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

import sproxy_config as cfg  # noqa: E402
from AbstractProxy.proxy_core import ClientId  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
)
from middleware.capability_gate import CapabilityGatedMiddleware  # noqa: E402
from middleware.orchestration import (  # noqa: E402
    MiddlewareChainConfigurationError,
    OrchestrationContext,
    OrchestrationMiddleware,
    orchestration_middleware,
)
from middleware.session_middleware import (  # noqa: E402
    IdentityMiddleware,
    MiddlewareChain,
    SessionCapabilities,
    SessionMiddleware,
)


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

def _make_analyze_query(
    *, model: Optional[str] = None,
    capabilities: Optional[Dict[str, Any]] = None,
    analyze_turns: Optional[list[int]] = None,
) -> KataGoQuery:
    opaque: Dict[str, Any] = {
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": [["B", "Q4"]],
    }
    if model is not None:
        opaque["model"] = model
    if capabilities is not None:
        opaque["capabilities"] = capabilities
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=analyze_turns,
        opaque=opaque,
    )


def _final_analyze(turn: int = 0) -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=False, turn_number=turn, opaque={"moveInfos": []},
    )


def _partial_analyze(turn: int = 0) -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=True, turn_number=turn, opaque={},
    )


class _FakeSessionCapabilities:
    """Records submit_query / terminate_query calls; lets tests drive
    sub-query response delivery via the orchestration middleware's
    handle_response path.

    Mirrors the real SessionCapabilities surface but does not actually
    submit anything to a router — tests inject responses directly via
    the middleware's handle_response method below.
    """

    def __init__(self) -> None:
        self.submitted: List[Tuple[ClientId, KataGoQuery]] = []
        self.terminated: List[ClientId] = []

    async def submit_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        self.submitted.append((orig_id, query))

    async def terminate_query(self, orig_id: ClientId) -> None:
        self.terminated.append(orig_id)

    def as_session_capabilities(self) -> SessionCapabilities:
        return SessionCapabilities(
            submit_query=self.submit_query,
            terminate_query=self.terminate_query,
        )


async def _drive_response(
    middleware: OrchestrationMiddleware,
    orig_id: ClientId,
    response: KataGoResponse,
) -> List[Tuple[ClientId, KataGoResponse]]:
    """Helper: invoke handle_response and collect its yields into a list."""
    out: List[Tuple[ClientId, KataGoResponse]] = []
    assert middleware._caps is not None
    async for oid, resp in middleware.handle_response(
        orig_id, response, middleware._caps.submit_query
    ):
        out.append((oid, resp))
    return out


async def _wait_for(
    predicate: Callable[[], bool], timeout_s: float = 1.0, interval_s: float = 0.005,
) -> bool:
    """Poll predicate; return True once it's true, False on timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_s)
    return False


# ===========================================================================
# OrchestrationContext primitives in isolation
# ===========================================================================


@pytest.mark.asyncio
class TestContextOriginalStream:
    async def test_original_stream_yields_pushed_responses(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro,
            name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0, 1]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )

        responses_seen: list[KataGoResponse] = []

        async def consumer() -> None:
            async for r in ctx.original_stream():
                responses_seen.append(r)

        task = asyncio.create_task(consumer())
        # Push two finals (matches expected_finals=2 for analyze_turns=[0, 1]).
        await ctx._push_original(_final_analyze(turn=0))
        await ctx._push_original(_final_analyze(turn=1))
        await asyncio.wait_for(task, timeout=1.0)
        assert len(responses_seen) == 2

    async def test_original_completed_flips_on_last_final(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0, 1]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        await ctx._push_original(_final_analyze(turn=0))
        assert not ctx.original_completed
        await ctx._push_original(_final_analyze(turn=1))
        assert ctx.original_completed

    async def test_partials_dont_count_toward_completion(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        await ctx._push_original(_partial_analyze(turn=0))
        assert not ctx.original_completed
        await ctx._push_original(_final_analyze(turn=0))
        assert ctx.original_completed

    async def test_discard_originals_drops_buffer(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        await ctx._push_original(_partial_analyze(turn=0))
        assert ctx._original_queue.qsize() > 0
        await ctx.discard_originals()
        assert ctx._original_queue.qsize() == 0

    async def test_discard_originals_still_tracks_completion(self) -> None:
        # The coroutine may discard originals but still want to know
        # when the upstream is done with the parent.
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        await ctx.discard_originals()
        await ctx._push_original(_final_analyze(turn=0))
        assert ctx.original_completed

    async def test_buffer_overflow_drops_oldest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cfg, "ORCHESTRATION_BUFFER_MAX", 3)
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(analyze_turns=[0, 1, 2, 3, 4]),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        # Push 5 partials with the buffer cap at 3 — first two should
        # be dropped (oldest-first).
        for i in range(5):
            await ctx._push_original(_partial_analyze(turn=i))
        assert ctx._original_queue.qsize() == 3


# ===========================================================================
# OrchestrationContext spawn primitive
# ===========================================================================


@pytest.mark.asyncio
class TestContextSpawn:
    async def test_spawn_submits_via_session_capabilities(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )

        sub_query = _make_analyze_query(model="strong")
        responses: list[KataGoResponse] = []

        async def consumer() -> None:
            async for r in ctx.spawn(sub_query):
                responses.append(r)

        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.01)  # let consumer reach the first await

        # submit_query was called.
        assert len(caps.submitted) == 1
        sub_orig_id, submitted_query = caps.submitted[0]
        assert sub_orig_id.startswith("__orch__")
        assert submitted_query is sub_query
        # The sub-query is registered.
        assert middleware._sub_to_parent[sub_orig_id] == "p1"

        # Push a final response to the sub-query.
        await ctx._push_sub_response(sub_orig_id, _final_analyze(turn=0))
        await asyncio.wait_for(task, timeout=1.0)
        assert len(responses) == 1
        # Cleanup happened.
        assert sub_orig_id not in middleware._sub_to_parent

    async def test_spawn_completes_on_expected_finals(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )
        responses: list[KataGoResponse] = []

        async def consumer() -> None:
            sub = _make_analyze_query(analyze_turns=[0, 1])
            async for r in ctx.spawn(sub):
                responses.append(r)

        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.01)
        sub_orig_id = caps.submitted[0][0]
        # Push partials first — should not complete.
        await ctx._push_sub_response(sub_orig_id, _partial_analyze(turn=0))
        await ctx._push_sub_response(sub_orig_id, _partial_analyze(turn=1))
        await asyncio.sleep(0.01)
        assert not task.done()
        # Push two finals.
        await ctx._push_sub_response(sub_orig_id, _final_analyze(turn=0))
        await ctx._push_sub_response(sub_orig_id, _final_analyze(turn=1))
        await asyncio.wait_for(task, timeout=1.0)
        assert len(responses) == 4

    async def test_parallel_returns_per_query_response_lists(self) -> None:
        caps = _FakeSessionCapabilities()
        middleware = OrchestrationMiddleware(
            coro_factory=_identity_coro, name="t",
        )
        middleware.on_session_start(caps.as_session_capabilities())
        ctx = OrchestrationContext(
            parent_id=ClientId("p1"),
            parent_query=_make_analyze_query(),
            session_capabilities=caps.as_session_capabilities(),
            middleware=middleware,
        )

        result_holder: list[list[list[KataGoResponse]]] = []

        async def driver() -> None:
            r = await ctx.parallel(
                _make_analyze_query(model="strong"),
                _make_analyze_query(model="weak"),
            )
            result_holder.append(r)

        task = asyncio.create_task(driver())
        await asyncio.sleep(0.01)
        assert len(caps.submitted) == 2
        # Identify which submitted orig_id corresponds to which model.
        sub_strong = next(o for o, q in caps.submitted if q.opaque["model"] == "strong")
        sub_weak = next(o for o, q in caps.submitted if q.opaque["model"] == "weak")

        await ctx._push_sub_response(sub_strong, _final_analyze(turn=0))
        await ctx._push_sub_response(sub_weak, _final_analyze(turn=0))
        await asyncio.wait_for(task, timeout=1.0)

        result = result_holder[0]
        assert len(result) == 2
        assert all(len(per_q) == 1 for per_q in result)


# ===========================================================================
# Coroutine helpers (used in tests above and below)
# ===========================================================================


async def _identity_coro(
    parent: KataGoQuery, ctx: OrchestrationContext,
) -> AsyncIterator[KataGoResponse]:
    """Test fixture: forward originals unchanged; emit no derived.

    Not decorated because some tests pass the raw coroutine into
    ``OrchestrationMiddleware`` directly. See
    ``_identity_middleware_factory`` for the decorated form.
    """
    async for resp in ctx.original_stream():
        yield resp


@orchestration_middleware(name="identity")
async def _identity_middleware_factory(
    parent: KataGoQuery, ctx: OrchestrationContext,
) -> AsyncIterator[KataGoResponse]:
    """Decorated form of ``_identity_coro`` — a factory that returns an
    ``OrchestrationMiddleware``. Tests that need the lifecycle hooks
    (on_session_start / on_query / handle_response / on_session_end)
    call ``_identity_middleware_factory()`` to get the middleware
    instance.
    """
    async for resp in ctx.original_stream():
        yield resp


# ===========================================================================
# OrchestrationMiddleware lifecycle
# ===========================================================================


@pytest.mark.asyncio
class TestCoroutineLifecycle:
    async def test_on_query_spawns_coroutine(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()  # factory call returns OrchestrationMiddleware
        m.on_session_start(caps.as_session_capabilities())
        m.on_query(ClientId("p1"), _make_analyze_query(analyze_turns=[0]))
        assert ClientId("p1") in m._contexts
        assert ClientId("p1") in m._tasks
        # Cleanup.
        m.on_session_end()

    async def test_handle_response_routes_original_to_stream(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        m.on_session_start(caps.as_session_capabilities())
        m.on_query(ClientId("p1"), _make_analyze_query(analyze_turns=[0]))
        # Push a final via handle_response; the identity coroutine
        # should yield it back.
        out = await _drive_response(m, ClientId("p1"), _final_analyze(turn=0))
        # The coroutine completes; the output is the same response
        # under the parent's orig_id.
        assert any(oid == ClientId("p1") for oid, _ in out)
        m.on_session_end()

    async def test_unrelated_response_passes_through(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        m.on_session_start(caps.as_session_capabilities())
        # No on_query for "unknown"; handle_response should pass through.
        out = await _drive_response(m, ClientId("unknown"), _final_analyze(turn=0))
        assert len(out) == 1
        assert out[0][0] == ClientId("unknown")
        m.on_session_end()

    async def test_session_end_cancels_live_tasks(self) -> None:
        caps = _FakeSessionCapabilities()

        @orchestration_middleware(name="long_running")
        async def long_running(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            # Wait forever.
            await asyncio.Event().wait()
            yield _final_analyze()  # never reached; satisfies generator shape

        m = long_running()
        m.on_session_start(caps.as_session_capabilities())
        m.on_query(ClientId("p1"), _make_analyze_query(analyze_turns=[0]))
        task = m._tasks[ClientId("p1")]
        assert not task.done()
        m.on_session_end()
        # Task should be cancelled; its finally block runs.
        await asyncio.sleep(0.05)
        assert task.done()

    async def test_unhandled_exception_synthesises_error_response(self) -> None:
        caps = _FakeSessionCapabilities()

        @orchestration_middleware(name="raises")
        async def raises(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            raise ValueError("simulated coroutine bug")
            yield _final_analyze()  # never reached; satisfies generator shape

        m = raises()
        m.on_session_start(caps.as_session_capabilities())
        m.on_query(ClientId("p1"), _make_analyze_query(analyze_turns=[0]))
        # The coroutine raises immediately; the error response is queued.
        # Push a partial to trigger the drain in handle_response.
        out = await _drive_response(m, ClientId("p1"), _partial_analyze(turn=0))
        # Should yield exactly one error response (the structured one
        # from the coroutine driver's exception handler).
        assert len(out) >= 1
        oid, resp = out[-1]
        assert oid == ClientId("p1")
        assert isinstance(resp, MetadataResponse)
        assert "error" in resp.opaque
        assert "raises" in resp.opaque["error"]
        m.on_session_end()


# ===========================================================================
# Composition with CapabilityGatedMiddleware
# ===========================================================================


@pytest.mark.asyncio
class TestCompositionWithCapabilityGate:
    async def test_opt_in_engages_orchestration(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        gated = CapabilityGatedMiddleware("identity", m)
        gated.on_session_start(caps.as_session_capabilities())
        gated.on_query(
            ClientId("p1"),
            _make_analyze_query(
                capabilities={"identity": {}}, analyze_turns=[0],
            ),
        )
        # Orchestration's on_query should have been called via the gate.
        assert ClientId("p1") in m._contexts
        gated.on_session_end()

    async def test_opt_out_skips_orchestration(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        gated = CapabilityGatedMiddleware("identity", m)
        gated.on_session_start(caps.as_session_capabilities())
        # capabilities present but does NOT name "identity" → skip.
        gated.on_query(
            ClientId("p1"),
            _make_analyze_query(
                capabilities={"other": {}}, analyze_turns=[0],
            ),
        )
        # Orchestration's on_query should NOT have been called.
        assert ClientId("p1") not in m._contexts
        gated.on_session_end()

    async def test_opt_out_passthrough_handle_response(self) -> None:
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        gated = CapabilityGatedMiddleware("identity", m)
        gated.on_session_start(caps.as_session_capabilities())
        gated.on_query(
            ClientId("p1"),
            _make_analyze_query(
                capabilities={"other": {}}, analyze_turns=[0],
            ),
        )
        # Send a response — gate delegates to orchestration, which
        # has no context for "p1" (on_query was skipped by the gate's
        # opt-out branch) and falls through to "Not orchestrated;
        # pass through". Output is the original response unchanged.
        resp = _final_analyze(turn=0)
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for oid, r in gated.handle_response(
            ClientId("p1"), resp, caps.submit_query
        ):
            out.append((oid, r))
        assert out == [(ClientId("p1"), resp)]
        gated.on_session_end()

    async def test_sub_query_response_relabels_through_gate(self) -> None:
        """Regression: sub-query responses must be relabeled to the
        parent's orig_id even when the orchestration middleware is
        wrapped behind a CapabilityGatedMiddleware.

        Bug shape (pre-fix): CapabilityGatedMiddleware short-circuited
        on the response side for orig_ids it had not registered in
        self._engaged. Only parent orig_ids land in self._engaged
        (via on_query), but sub-queries spawned by the orchestration
        framework carry synthetic orig_ids that bypass
        middleware.on_query entirely (submit_query is the spawn path,
        not the gated query path). The gate's short-circuit yielded
        the sub-query response unchanged with the synthetic orig_id,
        never reaching the wrapped orchestration's auto-relabel-to-
        parent code. Downstream consumers (e.g., the SPA's WebSocket
        subscriber map keyed by parent orig_id) silently dropped the
        response.

        Symptom in production: adaptive_reevaluate's deeper-analysis
        responses arrived at the SPA carrying ``__orch__<hex>`` ids
        and were dropped; the SPA's review session timed out at 30s
        when adaptive engaged on legacy auto-engage queries
        (PROXY_ADVERTISE_CAPABILITIES=false), because the original
        responses were patched is_during_search=True for deepening
        turns and the deeper's is_during_search=False responses never
        reached the waitForAnalysis subscriber.

        Fix: CapabilityGatedMiddleware.handle_response unconditionally
        delegates to wrapped. The wrapped's state-based check
        (self._contexts / self._sub_to_parent / pass-through) is the
        right gate for orchestration's three-way response routing.
        """
        caps = _FakeSessionCapabilities()

        # Coroutine that spawns one sub-query and yields its responses
        # — mirrors adaptive_reevaluate's stage-4 shape (spawn +
        # forward) without the buffering/decision logic.
        @orchestration_middleware(name="forwarder")
        async def forwarder(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            sub = _make_analyze_query(analyze_turns=[0])
            async for resp in ctx.spawn(sub):
                yield resp

        m = forwarder()
        gated = CapabilityGatedMiddleware("forwarder", m)
        gated.on_session_start(caps.as_session_capabilities())

        # Parent opts in to the gated capability — orchestration
        # engages, coroutine starts, ctx.spawn fires.
        gated.on_query(ClientId("p1"), _make_analyze_query(
            capabilities={"forwarder": {}}, analyze_turns=[0],
        ))
        # Let the coroutine reach ctx.spawn → submit_query.
        ok = await _wait_for(lambda: len(caps.submitted) >= 1)
        assert ok, "spawn should have submitted a sub-query"
        sub_orig_id, _sub_query = caps.submitted[0]
        assert sub_orig_id.startswith("__orch__"), (
            "sub-query orig_id should carry the orchestration's "
            "synthetic prefix"
        )

        # Sub-query response arrives at the gate (this is the path
        # that mimics deliver_upstream's middleware.handle_response
        # call). The gate must delegate to wrapped so the wrapped's
        # _sub_to_parent lookup re-routes the response under the
        # parent's orig_id.
        sub_response = _final_analyze(turn=0)
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for oid, resp in gated.handle_response(
            sub_orig_id, sub_response, caps.submit_query
        ):
            out.append((oid, resp))

        # Critical: the response must be relabeled to "p1" (the
        # parent's orig_id). The pre-fix bug had this output carrying
        # the synthetic sub_orig_id, which the SPA's subscriber map
        # would silently drop.
        assert any(oid == ClientId("p1") for oid, _ in out), (
            f"sub-query response did not reach the orchestration's "
            f"auto-relabel; out={[(oid, type(r).__name__) for oid, r in out]}. "
            f"Regression: CapabilityGatedMiddleware short-circuited "
            f"the response without delegating to the wrapped "
            f"OrchestrationMiddleware."
        )
        # Symmetric: the synthetic id must NOT leak through to the
        # gate's output (wire-side leakage causes the SPA-side drop).
        assert not any(oid == sub_orig_id for oid, _ in out), (
            f"synthetic sub_orig_id leaked through to gate output; "
            f"the SPA's subscriber map is keyed by parent orig_id, "
            f"so this would cause silent response drops on the wire"
        )

        gated.on_session_end()


# ===========================================================================
# Composition with MiddlewareChain
# ===========================================================================


class TestCompositionWithMiddlewareChain:
    def test_single_orchestration_in_chain_works(self) -> None:
        m = _identity_middleware_factory()
        chain = MiddlewareChain(inner=m, outer=IdentityMiddleware())
        assert chain is not None  # construction succeeded

    def test_two_orchestrations_in_chain_raises(self) -> None:
        with pytest.raises(MiddlewareChainConfigurationError, match="2 OrchestrationMiddleware"):
            MiddlewareChain(inner=_identity_middleware_factory(), outer=_identity_middleware_factory())

    def test_nested_chain_with_two_orchestrations_raises(self) -> None:
        # Construct a nested chain where the inner is another chain
        # containing an orchestration; the outer chain has another
        # orchestration. The guard should recurse.
        inner_chain = MiddlewareChain(
            inner=_identity_middleware_factory(),
            outer=IdentityMiddleware(),
        )
        with pytest.raises(MiddlewareChainConfigurationError, match="2 OrchestrationMiddleware"):
            MiddlewareChain(inner=inner_chain, outer=_identity_middleware_factory())

    def test_nested_chain_with_one_orchestration_works(self) -> None:
        # Single orchestration anywhere in nested chains is fine.
        inner_chain = MiddlewareChain(
            inner=IdentityMiddleware(),
            outer=IdentityMiddleware(),
        )
        outer = MiddlewareChain(inner=inner_chain, outer=_identity_middleware_factory())
        assert outer is not None

    def test_chain_with_no_orchestrations_works(self) -> None:
        chain = MiddlewareChain(
            inner=IdentityMiddleware(),
            outer=IdentityMiddleware(),
        )
        assert chain is not None


# ===========================================================================
# Sub-query response routing through handle_response
# ===========================================================================


@pytest.mark.asyncio
class TestSubQueryRouting:
    async def test_sub_query_response_routes_to_spawn_iterator(self) -> None:
        caps = _FakeSessionCapabilities()

        # Coroutine that spawns one sub-query and yields its responses.
        @orchestration_middleware(name="forwarder")
        async def forwarder(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            sub = _make_analyze_query(model="strong", analyze_turns=[0])
            async for resp in ctx.spawn(sub):
                yield resp

        m = forwarder()
        m.on_session_start(caps.as_session_capabilities())
        m.on_query(ClientId("p1"), _make_analyze_query(analyze_turns=[0]))
        # Coroutine started; let it reach the spawn submit.
        await asyncio.sleep(0.01)
        assert len(caps.submitted) == 1
        sub_orig_id = caps.submitted[0][0]
        # Sub-query response arrives at handle_response.
        out = await _drive_response(m, sub_orig_id, _final_analyze(turn=0))
        # The coroutine yields it under the parent's orig_id.
        assert any(oid == ClientId("p1") for oid, _ in out)
        m.on_session_end()

    async def test_unrelated_sub_query_response_after_parent_cleanup_drops(
        self,
    ) -> None:
        # Race scenario: parent's coroutine completes (cleanup happens),
        # then a stray sub-query response arrives. Should drop silently.
        caps = _FakeSessionCapabilities()
        m = _identity_middleware_factory()
        m.on_session_start(caps.as_session_capabilities())
        # Manually populate _sub_to_parent without a context (simulates
        # the post-cleanup race).
        m._sub_to_parent[ClientId("__orch__abc")] = ClientId("p1")
        out = await _drive_response(m, ClientId("__orch__abc"), _final_analyze(turn=0))
        assert out == []  # nothing yielded; response dropped
        m.on_session_end()


# ===========================================================================
# Decorator ergonomics
# ===========================================================================


class TestDecorator:
    def test_decorator_returns_factory(self) -> None:
        @orchestration_middleware(name="x")
        async def coro(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            yield _final_analyze()  # never reached

        # The decorator returns a factory (callable).
        assert callable(coro)
        # Calling the factory returns an OrchestrationMiddleware.
        m = coro()
        assert isinstance(m, OrchestrationMiddleware)
        assert m.name == "x"

    def test_factory_returns_fresh_instance_each_call(self) -> None:
        @orchestration_middleware(name="y")
        async def coro(
            parent: KataGoQuery, ctx: OrchestrationContext,
        ) -> AsyncIterator[KataGoResponse]:
            yield _final_analyze()  # never reached

        m1 = coro()
        m2 = coro()
        assert m1 is not m2
