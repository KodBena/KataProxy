"""
diagnose_watchdog_relay.py — Verify the RELAY + multi-LEAF deployment
topology preserves the keep-alive contract.

Topology under test (analog of diagnose_watchdog_selector.py for
RELAY):

       SPA (mocked: this script's loop)
        │   QUERY_VERSION every HEARTBEAT_CADENCE
        ▼
    RelayRouter (in-process, three upstream connections via _MockWebSocket)
        │           │           │
        ▼           ▼           ▼
    LEAF "A"      LEAF "B"     LEAF "C"
    (phantom: a   (phantom)    (phantom)
     KeepAlive-
     Middleware)

The pre-fix bug: RelayRouter.dispatch routed every action through
`_select_upstream(canonical_id)` — a hash-ring lookup. QUERY_VERSION
queries with consistent canonical_ids hashed to one upstream
deterministically; ANALYZE queries with different content hashed to
(potentially) different upstreams. Any LEAF the heartbeat didn't
land on never saw a `query_version` to reset its
`_last_heartbeat`; whatever ANALYZE the ring routed to that LEAF
fired its watchdog after `idle_timeout`. Same root cause as the
SELECTOR watchdog regression (see the postmortem in the umbrella's
docs/notes/), different routing mechanism (hash-ring rather than
first-healthy).

The fix (proxy v1.0.19): RelayRouter broadcasts QUERY_VERSION /
TERMINATE_ALL / CLEAR_CACHE to every connected upstream; first
response wins. Mirrors the v1.0.18 SELECTOR fix.

Two phases, one scenario:

  Phase A — heartbeat broadcast under load
    1. RelayRouter with three connected upstream LEAFs.
    2. ANALYZE dispatched (single-target via the hash ring; lands
       on whichever upstream the ring picks). The LEAF that
       receives it tracks the analyze in its phantom
       KeepAliveMiddleware._in_flight.
    3. For each tick: send QUERY_VERSION through router.dispatch;
       assert every LEAF received the wire on its mock socket;
       feed the parsed query to each phantom's
       KeepAliveMiddleware.on_query.
    4. After 5x idle_timeout of heartbeats, assert NO phantom
       LEAF's watchdog has fired.

  Phase B — heartbeats stop
    5. Stop sending heartbeats. Wait > idle_timeout + check_interval.
    6. Assert the phantom LEAF carrying the in-flight ANALYZE fires
       its watchdog. Other LEAFs (no in-flight) stay quiet (their
       empty-flight self-reset branch keeps them quiet).

A future change to RelayRouter.dispatch that re-introduces single-
target routing for QUERY_VERSION breaks Phase A on tick 1 with a
regression-naming error message.

Run from the proxy directory:
    python -m tests.diagnose_watchdog_relay

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from middleware.keep_alive import KeepAliveMiddleware  # noqa: E402
from middleware.session_middleware import SessionCapabilities  # noqa: E402
from router import (  # noqa: E402
    InFlightQueryLoad,
    RelayRouter,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Test fixtures (matches diagnose_watchdog_selector.py idioms)
# ---------------------------------------------------------------------------


class _MockWebSocket:
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


@dataclass
class _PhantomLeaf:
    """A LEAF as observed at the keep-alive-contract layer.

    Each LEAF in the production topology runs its own ClientSession
    with its own KeepAliveMiddleware. The RELAY forwards queries to
    that session's WebSocket; the session's _handle_incoming parses
    the wire and calls middleware.on_query. Here we elide the
    session machinery and feed parsed queries directly to the
    middleware. The phantom records terminate_query calls so
    watchdog firing is observable.
    """
    url: str
    socket: _MockWebSocket
    keep_alive: KeepAliveMiddleware
    terminated_query_ids: list[str]

    def feed(self, query: KataGoQuery, orig_id: str) -> None:
        self.keep_alive.on_query(orig_id, query)


def _build_phantom_leaf(
    url: str, *, idle_timeout_seconds: float, check_interval_seconds: float,
) -> _PhantomLeaf:
    socket = _MockWebSocket(url)
    keep_alive = KeepAliveMiddleware(
        idle_timeout_seconds=idle_timeout_seconds,
        check_interval_seconds=check_interval_seconds,
    )
    terminated: list[str] = []

    async def submit_query(_orig_id: str, _query: KataGoQuery) -> None:
        pass

    async def terminate_query(orig_id: str) -> None:
        terminated.append(orig_id)

    keep_alive.on_session_start(
        SessionCapabilities(
            submit_query=submit_query,
            terminate_query=terminate_query,
        )
    )
    return _PhantomLeaf(
        url=url, socket=socket, keep_alive=keep_alive,
        terminated_query_ids=terminated,
    )


def _populate_relay(
    router: RelayRouter, phantoms: dict[str, _PhantomLeaf],
) -> None:
    """Bypass RelayRouter.start() and inject phantom upstream connections."""
    for url, phantom in phantoms.items():
        router._connections[url] = phantom.socket


def _analyze_query() -> KataGoQuery:
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0],
        opaque={
            "moves": [],
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "maxVisits": 100000,
        },
    )


def _heartbeat_query() -> KataGoQuery:
    return KataGoQuery(action=KataGoAction.QUERY_VERSION)


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

async def run_scenario() -> bool:
    print()
    print("=" * 70)
    print("RELAY-watchdog diagnostic — heartbeat broadcast across upstreams")
    print("=" * 70)

    IDLE_TIMEOUT = 0.5
    CHECK_INTERVAL = 0.1
    HEARTBEAT_CADENCE = 0.1
    HEARTBEAT_DURATION = 2.5  # 5x idle_timeout

    # Three upstream LEAFs — enough to make the hash-ring vs.
    # broadcast distinction observable (a 1-LEAF RELAY would route
    # everything to one place by definition; a 2-LEAF RELAY's
    # behaviour depends on hash distribution; 3 makes the
    # broadcast property unambiguous regardless of which upstream
    # the ANALYZE happens to hash to).
    upstream_urls = [
        "ws://upstream-a:1",
        "ws://upstream-b:2",
        "ws://upstream-c:3",
    ]
    router = RelayRouter(
        upstream_urls=upstream_urls,
        load_metric=InFlightQueryLoad(),
        max_load=1000,
    )
    phantoms = {
        url: _build_phantom_leaf(
            url,
            idle_timeout_seconds=IDLE_TIMEOUT,
            check_interval_seconds=CHECK_INTERVAL,
        )
        for url in upstream_urls
    }
    _populate_relay(router, phantoms)

    sink_responses: list[tuple[str, dict]] = []

    async def on_response(cid: str, wire: dict) -> None:
        sink_responses.append((cid, wire))

    async def on_complete(cid: str) -> None:
        pass

    # ----------------------------------------------------------------
    # Phase A — analyze on whichever LEAF the ring picks + heartbeats
    # ----------------------------------------------------------------
    print("\n--- Step 1: dispatch the ANALYZE (hash-ring routes to one LEAF) ---")
    analyze = _analyze_query()
    analyze_wire = translate_query_to_wire(analyze, "cid-analyze")
    await router.dispatch(
        "cid-analyze", analyze_wire, analyze, on_response, on_complete,
    )

    # Identify which upstream the ring picked.
    analyze_target_url: str | None = None
    for url, phantom in phantoms.items():
        if phantom.socket.sent:
            analyze_target_url = url
            break
    if analyze_target_url is None:
        print("  FAIL: ANALYZE did not reach any upstream")
        return False
    print(f"  hash-ring routed ANALYZE to {analyze_target_url}")

    # Production: that upstream's session sees the analyze first
    # (via _handle_incoming → middleware.on_query). Simulate.
    phantoms[analyze_target_url].feed(analyze, "cid-analyze")
    print(
        f"  {analyze_target_url}.keep_alive._in_flight: "
        f"{phantoms[analyze_target_url].keep_alive._in_flight}"
    )
    if "cid-analyze" not in phantoms[analyze_target_url].keep_alive._in_flight:
        print(
            f"  FAIL: analyze did not reach {analyze_target_url}'s "
            f"KeepAliveMiddleware._in_flight"
        )
        return False

    print(
        f"\n--- Step 2: heartbeats every {HEARTBEAT_CADENCE}s for "
        f"{HEARTBEAT_DURATION}s (idle_timeout={IDLE_TIMEOUT}s) ---"
    )
    elapsed = 0.0
    seq = 0
    while elapsed < HEARTBEAT_DURATION:
        await asyncio.sleep(HEARTBEAT_CADENCE)
        seq += 1
        elapsed += HEARTBEAT_CADENCE
        hb = _heartbeat_query()
        cid = f"cid-hb-{seq}"
        hb_wire = translate_query_to_wire(hb, cid)
        prev_counts = {
            url: len(p.socket.sent) for url, p in phantoms.items()
        }
        await router.dispatch(cid, hb_wire, hb, on_response, on_complete)
        # Every connected upstream must have received the wire.
        for url, phantom in phantoms.items():
            new = len(phantom.socket.sent)
            if new != prev_counts[url] + 1:
                print(
                    f"  FAIL: upstream {url!r} did not receive heartbeat "
                    f"seq={seq} (sends: {prev_counts[url]} → {new}). "
                    f"REGRESSION: RelayRouter.dispatch is not broadcasting "
                    f"QUERY_VERSION to every connected upstream. See the "
                    f"SELECTOR watchdog postmortem in the umbrella's "
                    f"docs/notes/ and the heartbeat-fanout-contract section "
                    f"of proxy/CLAUDE.md."
                )
                return False
        # Production: each LEAF's read loop parses the wire and calls
        # middleware.on_query.
        for phantom in phantoms.values():
            phantom.feed(hb, cid)

    print(f"  sent {seq} heartbeats; all {len(phantoms)} upstreams received every one")

    for url, phantom in phantoms.items():
        if phantom.terminated_query_ids:
            print(
                f"\n  FAIL (Phase A): upstream {url!r}'s watchdog fired "
                f"and terminated {phantom.terminated_query_ids!r} during "
                f"the heartbeat phase. Regression: KeepAliveMiddleware did "
                f"not see the heartbeats."
            )
            return False
    print(
        f"  PASS (Phase A): all {len(phantoms)} upstreams received every "
        f"heartbeat; no watchdog fired"
    )

    # ----------------------------------------------------------------
    # Phase B — heartbeats stop; only the LEAF with in-flight fires
    # ----------------------------------------------------------------
    print(
        f"\n--- Step 3: stop heartbeats; wait > idle_timeout "
        f"({IDLE_TIMEOUT}s) ---"
    )
    await asyncio.sleep(IDLE_TIMEOUT + CHECK_INTERVAL + 0.3)

    # The analyze-target upstream has _in_flight; its watchdog must fire.
    target_phantom = phantoms[analyze_target_url]
    if "cid-analyze" not in target_phantom.terminated_query_ids:
        print(
            f"\n  FAIL (Phase B): {analyze_target_url!r}'s watchdog did "
            f"not terminate the in-flight analyze. "
            f"terminated_query_ids={target_phantom.terminated_query_ids!r}"
        )
        return False
    # The other upstreams have no in-flight; their watchdogs stay quiet.
    for url, phantom in phantoms.items():
        if url == analyze_target_url:
            continue
        if phantom.terminated_query_ids:
            print(
                f"\n  FAIL (Phase B): {url!r}'s watchdog terminated "
                f"{phantom.terminated_query_ids!r} despite no in-flight."
            )
            return False
    print(
        f"  PASS (Phase B): {analyze_target_url!r}'s watchdog fired on "
        f"its in-flight analyze; the other {len(phantoms) - 1} "
        f"upstream(s) stayed quiet"
    )

    # Cleanup.
    for phantom in phantoms.values():
        phantom.keep_alive.on_session_end()
    await asyncio.sleep(0.05)
    return True


def main() -> int:
    success = asyncio.run(run_scenario())
    print()
    print("=" * 70)
    print(f"  Result: {'PASS' if success else 'FAIL'}")
    print("=" * 70)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
