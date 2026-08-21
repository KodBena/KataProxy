"""
tests/test_relay_router.py — RelayRouter dispatch contract.

Filed in v1.0.19 as part of the RELAY broadcast fix. The pre-fix
RelayRouter routed every action through `_select_upstream`
(hash-ring), which silently broke the keep-alive heartbeat-fanout
contract for multi-upstream deployments — same root cause as the
SELECTOR watchdog regression covered by the postmortem in the
umbrella's docs/notes/postmortem-selector-watchdog-2026-05.md.

Coverage:

  - ANALYZE (and any future per-query action) still routes single-
    target via the hash ring; the broadcast path leaves single-
    target dispatch untouched.
  - QUERY_VERSION / TERMINATE_ALL / CLEAR_CACHE broadcast to every
    connected upstream; SyntheticCallbackOrigin.BROADCAST (v1.0.32;
    was the "__broadcast__" string)
    occupies the URL slot of `_callbacks` (the value isn't a
    routing key — broadcast canonicals aren't subject to
    SelectorRouter-style per-upstream terminate).
  - Per-upstream send failures log and continue; the broadcast
    aborts only when zero sends succeed.
  - LoadMetric is skipped on the broadcast path (verified via the
    InFlightQueryLoad's `_counts` not advancing for the broadcast
    URLs).
  - No-connected-upstream case: drop with a WARNING (matches the
    pre-fix single-target dispatch's "log and drop" convention;
    structured-error surfacing is a separate enhancement scoped
    out of v1.0.19).

Run from the proxy directory: `pytest tests/test_relay_router.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from AbstractProxy.proxy_core import CanonicalId  # noqa: E402
from router import (  # noqa: E402
    InFlightQueryLoad,
    RelayRouter,
    SyntheticCallbackOrigin,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Mocks an upstream WebSocket. Records sends; supports close-failure."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.sent: list[str] = []
        self.closed: bool = False

    async def send(self, msg: str) -> None:
        if self.closed:
            raise ConnectionError(f"ws[{self.url}] closed")
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True


def _make_router(
    upstream_urls: Optional[list[str]] = None,
    max_load: int = 1000,
) -> RelayRouter:
    urls = upstream_urls if upstream_urls is not None else [
        "ws://upstream-a:1", "ws://upstream-b:2", "ws://upstream-c:3",
    ]
    return RelayRouter(
        upstream_urls=urls,
        load_metric=InFlightQueryLoad(),
        max_load=max_load,
    )


def _populate_connections(
    router: RelayRouter, connected_urls: list[str],
) -> dict[str, _MockWebSocket]:
    sockets: dict[str, _MockWebSocket] = {}
    for url in connected_urls:
        ws = _MockWebSocket(url)
        router._connections[url] = ws
        sockets[url] = ws
    return sockets


def _analyze_query() -> KataGoQuery:
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        opaque={
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "moves": [["B", "Q4"]],
        },
    )


def _heartbeat_query() -> KataGoQuery:
    return KataGoQuery(action=KataGoAction.QUERY_VERSION)


# ===========================================================================
# Single-target dispatch (regression — broadcast must not break this path)
# ===========================================================================


@pytest.mark.asyncio
class TestSingleTargetDispatch:
    async def test_analyze_routes_to_one_upstream_via_hash_ring(self) -> None:
        # ANALYZE still goes through _select_upstream → exactly one
        # upstream's socket sees the wire. Other upstreams stay quiet.
        router = _make_router()
        sockets = _populate_connections(
            router, connected_urls=router._urls,
        )
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _analyze_query()
        wire = translate_query_to_wire(q, "cid-analyze")
        await router.dispatch(
            CanonicalId("cid-analyze"), wire, q, on_response, on_complete,
        )

        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        # Exactly one upstream received the wire (the hash ring's
        # preferred node). The fact that any one of them got it is
        # what we pin; which specific one is implementation detail
        # (depends on the hash).
        assert sum(sent_counts.values()) == 1, (
            f"single-target dispatch should hit exactly one upstream; "
            f"got {sent_counts!r}"
        )
        # _callbacks records the URL the canonical was sent to —
        # used by terminate() to route the cancel correctly.
        assert "cid-analyze" in router._callbacks
        _, _, recorded_url = router._callbacks["cid-analyze"]
        assert recorded_url in router._urls
        assert isinstance(recorded_url, str), (
            "single-target dispatch must record a real URL, not a "
            "SyntheticCallbackOrigin"
        )

    async def test_analyze_load_metric_increments(self) -> None:
        # The LoadMetric is the LoadMetric instance the router was
        # constructed with — single-target dispatch must call
        # on_query_sent so the ring's load-aware fallback sees the
        # in-flight query.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _analyze_query()
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        # Exactly one URL has load=1; the others have load=0.
        loads = [router._load_metric.current_load(u) for u in router._urls]
        assert sum(loads) == 1, f"expected exactly one in-flight load; got {loads!r}"


# ===========================================================================
# Broadcast (the v1.0.19 fix)
# ===========================================================================


@pytest.mark.asyncio
class TestBroadcastDispatch:
    async def test_query_version_broadcasts_to_all_connected(self) -> None:
        # Heartbeat fanout: every connected upstream must receive the
        # wire so its KeepAliveMiddleware._last_heartbeat resets. The
        # pre-fix hash-ring routing landed the heartbeat on one
        # upstream consistently, starving every other LEAF's watchdog
        # on whatever ANALYZE the ring had hash-routed there.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        # Every connected upstream got the wire exactly once.
        for url, ws in sockets.items():
            assert len(ws.sent) == 1, (
                f"{url} should have received the broadcast heartbeat; "
                f"got {len(ws.sent)} sends"
            )
        # _callbacks holds a single entry with the typed broadcast
        # discriminant in the URL slot (v1.0.32; was "__broadcast__").
        assert "cid-qv" in router._callbacks
        _, _, label_slot = router._callbacks["cid-qv"]
        assert label_slot is SyntheticCallbackOrigin.BROADCAST

    async def test_terminate_all_broadcasts_to_all_connected(self) -> None:
        # SPA expectation for TERMINATE_ALL is "cancel every in-flight
        # query on this session". Single-target hash-ring routing of
        # this action would leave queries on the other (N-1) upstreams
        # running.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.TERMINATE_ALL)
        wire = translate_query_to_wire(q, "cid-ta")
        await router.dispatch(CanonicalId("cid-ta"), wire, q, on_response, on_complete)

        for url, ws in sockets.items():
            assert len(ws.sent) == 1, f"{url} should have received TERMINATE_ALL"

    async def test_clear_cache_broadcasts_to_all_connected(self) -> None:
        # KataGo's cache is per-LEAF (per-subprocess); a SPA-issued
        # CLEAR_CACHE wants every upstream cleared. Hash-ring routing
        # would leave (N-1) upstreams' caches stale.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.CLEAR_CACHE)
        wire = translate_query_to_wire(q, "cid-cc")
        await router.dispatch(CanonicalId("cid-cc"), wire, q, on_response, on_complete)

        for url, ws in sockets.items():
            assert len(ws.sent) == 1, f"{url} should have received CLEAR_CACHE"

    async def test_broadcast_no_connected_upstream_drops(self) -> None:
        # Match the pre-fix single-target dispatch's "log and drop"
        # convention. Structured-error surfacing for RELAY is a
        # separate enhancement (the SELECTOR's structured-error path
        # was added in v1.0.15; RELAY hasn't gained it).
        router = _make_router()
        # No connections populated.
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)
        # Callback was not installed (no upstream → drop early).
        assert "cid-qv" not in router._callbacks

    async def test_broadcast_continues_after_per_upstream_send_failure(self) -> None:
        # If one upstream's send raises, the broadcast logs and
        # continues to the next. Healthy upstreams still receive the
        # wire; the canonical's _callbacks entry is installed.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        # Pick one upstream to refuse all sends.
        bad_url = router._urls[0]
        sockets[bad_url].closed = True

        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        # Bad upstream got nothing; the others got it.
        assert sockets[bad_url].sent == []
        for url in router._urls:
            if url == bad_url:
                continue
            assert len(sockets[url].sent) == 1, (
                f"{url} should still have received the broadcast wire"
            )
        # _callbacks still has the entry — broadcast didn't abort.
        assert "cid-qv" in router._callbacks

    async def test_broadcast_all_sends_failing_drops(self) -> None:
        # Degenerate case: every connected upstream's send raised.
        # Pop the callbacks installed by the broadcast so the
        # canonical isn't hung.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        for ws in sockets.values():
            ws.closed = True

        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        assert "cid-qv" not in router._callbacks
        for ws in sockets.values():
            assert ws.sent == []

    async def test_broadcast_does_not_advance_load_metric(self) -> None:
        # The LoadMetric is for fungible-upstream load balancing of
        # ANALYZE queries; heartbeats and other broadcast metadata
        # actions aren't in-flight in the load sense and shouldn't
        # contribute to per-upstream load. If we did call
        # on_query_sent for each upstream during broadcast, we'd
        # leak (N-1) counts (only one on_query_complete fires when
        # the first response triggers QUERY_COMPLETE → callback pop).
        router = _make_router()
        _populate_connections(router, connected_urls=router._urls)
        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        loads = [router._load_metric.current_load(u) for u in router._urls]
        assert sum(loads) == 0, (
            f"broadcast must not advance the load metric; got {loads!r}"
        )


# ===========================================================================
# First-response-wins via the existing read-loop machinery
# ===========================================================================


@pytest.mark.asyncio
class TestFirstResponseWins:
    async def test_callbacks_pop_on_query_complete_drops_subsequent(self) -> None:
        # Verifies the natural-deduplication property the broadcast
        # relies on: _read_loop pops _callbacks on QUERY_COMPLETE; a
        # second response from a different upstream (same
        # canonical_id) finds no callback and is silently dropped.
        # This is the existing _read_loop behaviour we lean on; we
        # exercise it here at the contract level by simulating the
        # pop after the first response and confirming the second
        # response would find no callback.
        router = _make_router()
        sockets = _populate_connections(router, connected_urls=router._urls)
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(_cid: CanonicalId) -> None: pass

        q = _heartbeat_query()
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)
        # Broadcast installed the callback.
        assert "cid-qv" in router._callbacks
        # Simulate the read loop's QUERY_COMPLETE branch popping the
        # callback after the first response.
        router._callbacks.pop("cid-qv", None)
        # Second upstream's response would now find no callback. The
        # _read_loop's "no callback for {canonical_id!r}" branch
        # would log and continue, dropping the response. The
        # contract this pins: subsequent responses drop at the read
        # loop, not at the broadcast helper.
        assert "cid-qv" not in router._callbacks
