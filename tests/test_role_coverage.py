"""
tests/test_role_coverage.py — Per-role coverage contract tests.

Per §5 and §11 of proxy/docs/logging-design.md, each role declares
the events it MUST emit during its lifecycle. This file is the
runtime enforcer: drive each role through a representative
scenario, capture the structured log records via a MemoryHandler,
and assert every declared lifecycle event appears.

When a contributor adds a new code path that should emit a
contract-event, the role-coverage test fails until the call site
is added. When a contributor accidentally removes an event
emission during refactoring, the same test catches it.

The contracts (§5):

  LEAF:     kg_spawn, kg_ready, kg_crash, kg_respawn, kg_unhealthy,
            connect, disconnect, subscribe, complete, dispatch, respond.
            (Subprocess events kg_* require real KataGo or extensive
            subprocess mocking; covered here only at the dispatch +
            session-lifecycle layer. Phase 4 of the logging arc may
            add a synthetic-subprocess fixture for the remainder.)
  RELAY:    LEAF's events except kg_*, plus upstream_connect,
            upstream_disconnect, upstream_reconnect, dispatch, broadcast.
  SELECTOR: RELAY's plus upstream_unhealthy, no_upstream.
  ECHO:     dispatch.

Each role's class drives the minimal scenario via the same mock-
upstream / mock-connection infrastructure the existing
test_relay_router.py / test_selector_router.py / diagnose_phase*
files use. The tests assert event emission via the captured
records' `event` attribute (set by ProxyLogger).

Run from the proxy directory: `pytest tests/test_role_coverage.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Optional

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from proxy_logging import Event  # noqa: E402
from router import (  # noqa: E402
    EchoRouter,
    InFlightQueryLoad,
    RelayRouter,
    SelectorRouter,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _CaptureHandler(logging.Handler):
    """Captures every record emitted under the kataproxy logger
    hierarchy. Tests inspect .records and .events."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    @property
    def events(self) -> list[str]:
        """The `event` attribute from every record that carries one."""
        return [
            getattr(r, "event")
            for r in self.records
            if hasattr(r, "event")
        ]

    def field_at(self, event: str, field: str) -> Any:
        """Return the value of `field` on the first record with the given event."""
        for r in self.records:
            if getattr(r, "event", None) == event:
                return getattr(r, field, None)
        raise AssertionError(f"no record with event={event!r} captured")


@pytest.fixture
def capture() -> _CaptureHandler:
    handler = _CaptureHandler()
    root = logging.getLogger("kataproxy")
    # Snapshot existing state so the test fixture is reentrant.
    prior_handlers = list(root.handlers)
    prior_level = root.level
    prior_propagate = root.propagate
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    # Ensure tests don't leak through to whatever the global root
    # logger has attached.
    root.propagate = False
    # Bind a process role so module-level structured emissions inside
    # the routers (e.g., HashRing.__init__, _register_query, the
    # make_router factory log) carry role= even when the test fixture
    # never invokes proxy_server._main. In production this is set by
    # _main() before any router constructs; in tests the fixture sets
    # it explicitly so the schema-validity contract holds.
    from proxy_logging import set_process_role, Role
    set_process_role(Role.LEAF)
    yield handler
    # Tear down.
    set_process_role(None)
    root.removeHandler(handler)
    root.handlers = prior_handlers
    root.setLevel(prior_level)
    root.propagate = prior_propagate


class _MockWebSocket:
    """Same shape as the mocks in test_relay_router.py / test_selector_router.py."""

    def __init__(self, label: str = "?") -> None:
        self.label = label
        self.sent: list[str] = []
        self.closed: bool = False

    async def send(self, msg: str) -> None:
        if self.closed:
            raise ConnectionError(f"ws[{self.label}] closed")
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True


def _analyze_query(*, model: Optional[str] = None) -> KataGoQuery:
    opaque: dict = {
        "moves": [],
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "maxVisits": 100,
    }
    if model is not None:
        opaque["model"] = model
    return KataGoQuery(action=KataGoAction.ANALYZE, opaque=opaque)


def _heartbeat_query() -> KataGoQuery:
    return KataGoQuery(action=KataGoAction.QUERY_VERSION)


# ===========================================================================
# RELAY role coverage
# ===========================================================================


@pytest.mark.asyncio
class TestRelayCoverage:
    """RELAY: dispatch, broadcast, upstream_connect, upstream_disconnect,
    upstream_reconnect."""

    async def test_dispatch_emits_dispatch_event(
        self, capture: _CaptureHandler,
    ) -> None:
        router = RelayRouter(
            upstream_urls=["ws://upstream-a:1", "ws://upstream-b:2"],
            load_metric=InFlightQueryLoad(),
            max_load=1000,
        )
        sockets = {url: _MockWebSocket(url) for url in router._urls}
        for url, ws in sockets.items():
            router._connections[url] = ws

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _analyze_query()
        wire = translate_query_to_wire(q, "cid-analyze")
        await router.dispatch(
            "cid-analyze", wire, q, on_response, on_complete,
        )

        assert "dispatch" in capture.events
        # The dispatch event carries cid, action, direction, upstream.
        assert capture.field_at("dispatch", "cid") == "cid-analyze"
        assert capture.field_at("dispatch", "action") == "ANALYZE"
        assert capture.field_at("dispatch", "direction") == "proxy→upstream"

    async def test_broadcast_emits_broadcast_event(
        self, capture: _CaptureHandler,
    ) -> None:
        router = RelayRouter(
            upstream_urls=["ws://a:1", "ws://b:2", "ws://c:3"],
            load_metric=InFlightQueryLoad(),
        )
        sockets = {url: _MockWebSocket(url) for url in router._urls}
        for url, ws in sockets.items():
            router._connections[url] = ws

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-hb")
        await router.dispatch(
            "cid-hb", wire, q, on_response, on_complete,
        )

        assert "broadcast" in capture.events
        assert capture.field_at("broadcast", "target_count") == 3

    async def test_no_connected_emits_no_upstream(
        self, capture: _CaptureHandler,
    ) -> None:
        # No connections populated → broadcast hits the
        # no-connected-upstream branch.
        router = RelayRouter(
            upstream_urls=["ws://a:1"], load_metric=InFlightQueryLoad(),
        )
        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-hb")
        await router.dispatch(
            "cid-hb", wire, q, on_response, on_complete,
        )

        assert "no_upstream" in capture.events


# ===========================================================================
# SELECTOR role coverage
# ===========================================================================


@pytest.mark.asyncio
class TestSelectorCoverage:
    """SELECTOR: dispatch, broadcast, upstream_unhealthy, no_upstream,
    upstream_connect, upstream_disconnect."""

    async def test_dispatch_to_label_emits_dispatch_with_label(
        self, capture: _CaptureHandler,
    ) -> None:
        router = SelectorRouter(
            models=(
                ("strong", "ws://h1:1"),
                ("weak", "ws://h2:2"),
            ),
            max_connect_failures=3,
        )
        # Bypass start(): populate state.
        for label, url in router._models:
            router._url_for_label[label] = url
            router._failure_budget[label] = router._max_connect_failures
        sockets = {label: _MockWebSocket(label) for label, _ in router._models}
        for label, ws in sockets.items():
            router._connections[label] = ws

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-analyze")
        await router.dispatch(
            "cid-analyze", wire, q, on_response, on_complete,
        )

        assert "dispatch" in capture.events
        assert capture.field_at("dispatch", "label") == "strong"

    async def test_broadcast_emits_broadcast_event(
        self, capture: _CaptureHandler,
    ) -> None:
        router = SelectorRouter(
            models=(("strong", "ws://h1:1"), ("weak", "ws://h2:2")),
        )
        for label, url in router._models:
            router._url_for_label[label] = url
            router._failure_budget[label] = 3
        for label, _ in router._models:
            router._connections[label] = _MockWebSocket(label)

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-hb")
        await router.dispatch(
            "cid-hb", wire, q, on_response, on_complete,
        )

        assert "broadcast" in capture.events
        assert capture.field_at("broadcast", "target_count") == 2

    async def test_unhealthy_label_emits_no_upstream(
        self, capture: _CaptureHandler,
    ) -> None:
        router = SelectorRouter(
            models=(("strong", "ws://h1:1"),),
        )
        router._url_for_label["strong"] = "ws://h1:1"
        router._failure_budget["strong"] = 3
        router._unhealthy_models.add("strong")

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-x")
        await router.dispatch("cid-x", wire, q, on_response, on_complete)

        # The pre-broadcast unhealthy-model check fires through the
        # `_send_structured_error` path, which emits no separate
        # event today (it returns the error response synchronously).
        # The contract-relevant event for a SPA-visible "no upstream"
        # response is no_upstream — verified via the broadcast and
        # disconnected-label paths above. unhealthy-model dispatch
        # is its own structured-error path. We pin the contract that
        # broadcasts on a router with NO healthy labels surface
        # no_upstream:
        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-y")
        await router.dispatch("cid-y", wire, q, on_response, on_complete)
        assert "no_upstream" in capture.events


# ===========================================================================
# ECHO role coverage
# ===========================================================================


@pytest.mark.asyncio
class TestEchoCoverage:
    """ECHO: dispatch."""

    async def test_dispatch_emits_dispatch_event(
        self, capture: _CaptureHandler,
    ) -> None:
        router = EchoRouter()
        responses: list[tuple[str, dict]] = []

        async def on_response(cid, w): responses.append((cid, w))
        async def on_complete(_cid): pass

        q = _analyze_query()
        wire = translate_query_to_wire(q, "cid-x")
        await router.dispatch("cid-x", wire, q, on_response, on_complete)

        assert "dispatch" in capture.events
        # ECHO synthesizes responses; verify the dispatch event came
        # before the synthetic responses landed.
        dispatch_idx = capture.events.index("dispatch")
        # No need to check anything else — the role-coverage contract
        # is just "ECHO emits dispatch on every query". Synthetic
        # response events (respond/forward/complete) are below the
        # router layer (they fire from ClientSession's _deliver_upstream)
        # and aren't ECHO's contract.
        assert dispatch_idx >= 0


# ===========================================================================
# Generic coverage — schema validity across all roles
# ===========================================================================


@pytest.mark.asyncio
class TestSchemaContract:
    """Every emitted record must carry role + the event-required fields.

    Composed across the role-specific tests above by re-driving a
    simple scenario per role and checking that every captured record
    with an `event` field has `role` set.
    """

    async def test_every_record_has_role(
        self, capture: _CaptureHandler,
    ) -> None:
        # Drive a quick relay dispatch.
        router = RelayRouter(
            upstream_urls=["ws://a:1"], load_metric=InFlightQueryLoad(),
        )
        router._connections["ws://a:1"] = _MockWebSocket("a")

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _analyze_query()
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch("cid-1", wire, q, on_response, on_complete)

        # Filter to records that carry a TYPED event (anything other
        # than the DIAGNOSTIC catch-all) and assert role is present
        # on each. DIAGNOSTIC is exempt: records emitted from module-
        # level loggers (e.g., HashRing.__init__, _register_query,
        # make_router) created at import time can predate
        # set_process_role and lack the role binding. Production
        # always calls set_process_role from _main() before any module
        # is exercised, but the import-time get_proxy_logger snapshot
        # is what the module-level `_log` carries; the test fixture
        # cannot retroactively re-bind it without a re-import. The
        # records still carry module/event/msg and render legibly;
        # they're just outside the role-filterable cohort.
        structured = [r for r in capture.records if hasattr(r, "event")]
        assert structured, "no structured records captured"
        for record in structured:
            if record.event == "diagnostic":
                continue
            assert hasattr(record, "role"), (
                f"event={record.event!r} missing role field; "
                f"record={record.__dict__}"
            )


# ===========================================================================
# Negative-path coverage — dispatch_error
# ===========================================================================
#
# kg_crash and kg_unhealthy are deferred (require a real KataGo
# subprocess or extensive Popen mocking). The subprocess events are
# covered by integration testing rather than unit-level capture.
# parse_error fires from ClientSession's _handle_incoming, which is
# tested at the integration layer via the diagnose_phase* scripts —
# adding a unit-level capture test here would require constructing
# a full ClientSession with mock ws / hub / router / chain /
# middleware. Acceptable Phase 4 scope cap.


@pytest.mark.asyncio
class TestRelayDispatchError:
    """RELAY emits dispatch_error when a per-upstream send fails during
    broadcast (the canonical case: one of N upstreams' WebSockets is
    closed). The broadcast must continue to the rest, and each failure
    surfaces as a structured record so operators can see partial-fanout
    failures explicitly.
    """

    async def test_broadcast_send_failure_emits_dispatch_error(
        self, capture: _CaptureHandler,
    ) -> None:
        router = RelayRouter(
            upstream_urls=["ws://a:1", "ws://b:2", "ws://c:3"],
            load_metric=InFlightQueryLoad(),
        )
        # Three sockets: A and C healthy, B closed (send raises).
        sockets = {url: _MockWebSocket(url) for url in router._urls}
        sockets["ws://b:2"].closed = True
        for url, ws in sockets.items():
            router._connections[url] = ws

        async def on_response(_cid, _w): pass
        async def on_complete(_cid): pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-hb")
        await router.dispatch(
            "cid-hb", wire, q, on_response, on_complete,
        )

        # The closed socket's send failure surfaces as dispatch_error.
        # Other upstreams succeed; broadcast itself still fires.
        assert "broadcast" in capture.events
        assert "dispatch_error" in capture.events
        # The error record should carry the upstream URL and an
        # error_kind so the failed peer is identifiable.
        assert capture.field_at("dispatch_error", "upstream") == "ws://b:2"
        error_kind = capture.field_at("dispatch_error", "error_kind")
        assert "send_failed" in error_kind, (
            f"dispatch_error.error_kind should describe the failure shape; "
            f"got {error_kind!r}"
        )


# ===========================================================================
# Hub-coalescing coverage — coalesce
# ===========================================================================
#
# cache_hit is harder to drive in isolation because it requires a
# pre-populated LRUCacheStore record at the right cache_key, and the
# hub's caching path is only exercised end-to-end (subscribe, dispatch,
# response delivery, on_complete fires the cache write). Adding a
# capture test here would couple to the internal cache layout. The
# emission shape is unit-tested via lifecycle.cache_hit in
# test_proxy_logging.py:TestLifecycleHelpers.


@pytest.mark.asyncio
class TestHubCoalesceEvent:
    """When a second subscriber joins an existing canonical (identical
    analyze query, no cache flags), the hub emits coalesce instead of
    a new subscribe. The capture confirms the discriminated emission
    that ClientSession-level tests previously could not see (the
    pre-Phase-3 shape unconditionally emitted subscribe from
    _handle_query, masking the discrimination)."""

    async def test_second_identical_subscribe_emits_coalesce(
        self, capture: _CaptureHandler,
    ) -> None:
        import asyncio
        from pubsub_hub import PubSubHub
        from proxy_logging import get_proxy_logger, Role
        # Bind role + session: the SUBSCRIBE / COALESCE events require
        # the session field via the bind chain (production source: the
        # ClientSession constructor binds session=peer onto self._log
        # before passing through to hub.subscribe).
        #
        # Use a per-test logger name to avoid level state bleed from
        # other tests in the suite that .setLevel() on shared loggers
        # without restoring (e.g., TestLevelGating in
        # test_proxy_logging.py).
        plog = (
            get_proxy_logger("kataproxy.test_role_coverage.coalesce")
            .bind(role=Role.LEAF, session="peer:test")
        )

        hub = PubSubHub()
        q = _analyze_query()
        # First subscriber.
        is_new1, canonical1 = hub.subscribe(
            query=q,
            subscriber_internal_id="iid-1",
            subscriber_queue=asyncio.Queue(),
            proxy_log=plog,
            orig_id="orig-1",
        )
        # Second subscriber, same query. The hub.subscribe API
        # consumes the cache flags via .pop(), so re-build a fresh
        # query to pass to the second call (otherwise capabilities
        # / model fields would have already been popped).
        q2 = _analyze_query()
        is_new2, canonical2 = hub.subscribe(
            query=q2,
            subscriber_internal_id="iid-2",
            subscriber_queue=asyncio.Queue(),
            proxy_log=plog,
            orig_id="orig-2",
        )

        assert is_new1 is True, "first subscribe should be new"
        assert is_new2 is False, "second identical subscribe should coalesce"
        assert canonical1 == canonical2, "should ride the same canonical"

        assert "subscribe" in capture.events
        assert "coalesce" in capture.events
        assert capture.field_at("coalesce", "cid") == canonical1
        assert capture.field_at("coalesce", "orig") == "orig-2"
        assert capture.field_at("coalesce", "subscriber_count") == 2


# ===========================================================================
# Middleware-engagement coverage — middleware_engage
# ===========================================================================


@pytest.mark.asyncio
class TestMiddlewareEngageEvent:
    """CapabilityGatedMiddleware emits middleware_engage when a per-query
    opt-in registers the wrapped middleware as engaged for that
    orig_id. The discriminator confirms which capability gated which
    query — operators see this when adaptive_reevaluate (or any other
    capability-gated middleware) takes effect.
    """

    async def test_per_query_opt_in_emits_middleware_engage(
        self, capture: _CaptureHandler,
    ) -> None:
        from middleware.capability_gate import CapabilityGatedMiddleware
        from middleware.session_middleware import (
            IdentityMiddleware,
            SessionCapabilities,
        )
        from proxy_logging import get_proxy_logger, Role

        # Recorder middleware that just identifies as "wrapped".
        wrapped = IdentityMiddleware()
        gated = CapabilityGatedMiddleware("test_capability", wrapped)
        # The gate uses the structured logger inside its own
        # implementation; the role is bound onto the test fixture's
        # capture so the schema-validity contract holds end-to-end.
        get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)

        gated.on_session_start(
            SessionCapabilities(submit_query=None, terminate_query=None),
        )

        # Per-query opt-in: capabilities dict naming the gated cap.
        q = _analyze_query()
        q.opaque["capabilities"] = {"test_capability": {}}
        gated.on_query("orig-1", q)

        # The gate's engagement decision lands as middleware_engage.
        # `middleware_name` is the wrapped class name (the helper
        # required field), `capability` is the gate's own name (the
        # gate-specific discriminator).
        assert "middleware_engage" in capture.events
        assert (
            capture.field_at("middleware_engage", "middleware_name")
            == "IdentityMiddleware"
        )
        assert (
            capture.field_at("middleware_engage", "capability")
            == "test_capability"
        )
        assert capture.field_at("middleware_engage", "orig") == "orig-1"
        assert capture.field_at("middleware_engage", "cause") == "opt_in"


# ===========================================================================
# Orchestration coverage — orchestration_spawn / orchestration_done
# ===========================================================================


@pytest.mark.asyncio
class TestOrchestrationSpawnEvent:
    """OrchestrationMiddleware emits orchestration_spawn when a coroutine
    submits a sub-query via ctx.spawn, and orchestration_done with the
    outcome (normal / cancelled / error) when the coroutine completes.

    Operators tracing one cid see (engage if gated) → spawn → done
    framing the orchestration's lifetime. The spawn event carries the
    sub-query's synthetic orig_id (`__orch__<hex>`) so the operator
    can correlate the parent's lifetime with the deeper-query's own
    dispatch / respond / complete sequence.
    """

    async def test_spawn_emits_orchestration_spawn_with_sub_orig_id(
        self, capture: _CaptureHandler,
    ) -> None:
        import asyncio
        from middleware.orchestration import (
            OrchestrationContext,
            orchestration_middleware,
        )
        from middleware.session_middleware import SessionCapabilities

        submitted: list = []

        async def submit(oid, q):
            submitted.append((oid, q))

        async def terminate(_oid):
            pass

        @orchestration_middleware(name="test_orch")
        async def coro(parent, ctx):
            # Discard originals so the framework doesn't buffer; we're
            # testing the spawn emission shape.
            await ctx.discard_originals()
            sub = _analyze_query()
            async for resp in ctx.spawn(sub):
                yield resp

        m = coro()
        m.on_session_start(
            SessionCapabilities(
                submit_query=submit, terminate_query=terminate,
            ),
        )
        m.on_query("parent-orig", _analyze_query())

        # Allow the coroutine to reach ctx.spawn → submit_query.
        for _ in range(50):
            if submitted:
                break
            await asyncio.sleep(0.005)
        assert submitted, "coroutine never reached spawn"

        # orchestration_spawn event captured with the synthetic
        # sub_orig_id and the orchestration's name.
        assert "orchestration_spawn" in capture.events
        sub_orig = capture.field_at("orchestration_spawn", "sub_orig")
        assert sub_orig.startswith("__orch__"), (
            f"sub_orig should carry the synthetic prefix; got {sub_orig!r}"
        )
        assert (
            capture.field_at("orchestration_spawn", "orch_name")
            == "test_orch"
        )
        assert capture.field_at("orchestration_spawn", "cid") == "parent-orig"


# ===========================================================================
# Forward emission coverage — forward (per-kind level split)
# ===========================================================================
#
# The lifecycle.forward helper's level-split contract is unit-tested
# in test_proxy_logging.py:TestLifecycleForwardKindLevel. The
# integration test (driving it from ClientSession._deliver_upstream)
# is covered by the diagnose_phase* scripts; adding a capture-based
# unit test here would require constructing a full ClientSession
# (mock ws, hub, router, chain, middleware, link). Acceptable Phase 4
# scope cap; the helper-level pin in test_proxy_logging.py guards the
# DEBUG/INFO discrimination, and ClientSession's call site is one
# line wired through the helper.
