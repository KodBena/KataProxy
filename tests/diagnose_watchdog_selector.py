"""
diagnose_watchdog_selector.py — Verify the SELECTOR + multi-LEAF
deployment topology preserves the keep-alive contract.

Topology under test:

       SPA (mocked: this script's loop)
        │   QUERY_VERSION every HEARTBEAT_CADENCE
        ▼
    SelectorRouter (in-process, two LEAF connections via _MockWebSocket)
        │           │
        ▼           ▼
    LEAF "strong"  LEAF "weak"
    (phantom: a    (phantom: a
     KeepAlive-     KeepAlive-
     Middleware)    Middleware)

The bug this test guards against was diagnosed in the SELECTOR
watchdog postmortem (umbrella docs/notes/postmortem-selector-
watchdog-2026-05.md). Pre-fix, SelectorRouter.dispatch routed
QUERY_VERSION to `_first_healthy_label()` only. The LEAF that the
SPA was actually analyzing on (selected by `model`) never received
heartbeats while its analyze was in-flight; its KeepAliveMiddleware
fired after `idle_timeout` and terminated the in-flight query. The
v1.0.17 KEEP_ALIVE_IDLE_TIMEOUT_SECONDS 25→250 band-aid widened the
window without addressing the cause.

The fix (proxy v1.0.18, this commit's fix): SelectorRouter
broadcasts QUERY_VERSION (and TERMINATE_ALL, CLEAR_CACHE) to every
healthy upstream. First response wins; subsequent responses for the
same canonical drop at the read loop's "no callback" branch.

This test exercises the topology-level property:

  Phase A — heartbeat broadcast under load
    1. SELECTOR with two healthy LEAFs.
    2. ANALYZE targeted at LEAF "weak" via `model`. The phantom
       LEAF's KeepAliveMiddleware tracks the analyze in _in_flight.
    3. For each tick: send a QUERY_VERSION through SELECTOR.dispatch;
       assert both phantom LEAFs received the wire on their mock
       sockets; feed the parsed query to each phantom's
       KeepAliveMiddleware.on_query.
    4. After 5x idle_timeout of heartbeats, assert NO phantom LEAF's
       watchdog has fired (terminate_query sentinel records empty).

  Phase B — heartbeats stop
    5. Stop sending heartbeats. Wait > idle_timeout + check_interval.
    6. Assert the phantom LEAF carrying the in-flight ANALYZE fires
       its watchdog and records the terminate. Assert the OTHER
       phantom LEAF (no in-flight) does NOT fire (its self-reset
       branch keeps last_heartbeat fresh because _in_flight is
       empty).

Both phases together specify the contract: heartbeats reach every
LEAF; absence trips only the LEAFs that have in-flight queries. A
future change to `SelectorRouter.dispatch` that re-introduces
single-target routing for QUERY_VERSION breaks Phase A immediately.

Run from the proxy directory:
    python -m tests.diagnose_watchdog_selector

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

# Make the proxy root importable when running as a script.
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
from router import SelectorRouter  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Mock the SELECTOR-to-LEAF WebSocket. Records sends; supports close."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.sent: list[str] = []
        self.closed: bool = False
        self._inbox: asyncio.Queue[str] = asyncio.Queue()

    async def send(self, msg: str) -> None:
        if self.closed:
            raise ConnectionError(f"ws[{self.label}] closed")
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if self.closed:
            raise StopAsyncIteration
        return await self._inbox.get()


@dataclass
class _PhantomLeaf:
    """A LEAF as observed at the keep-alive-contract layer.

    Each LEAF in the production topology runs its own ClientSession with
    its own KeepAliveMiddleware. The SELECTOR forwards queries to that
    session's WebSocket; the session's _handle_incoming parses the wire
    and calls middleware.on_query. Here we elide the session machinery
    (already covered by diagnose_watchdog.py for the single-LEAF
    middleware contract) and feed parsed queries directly to the
    middleware. The phantom records terminate_query calls so the
    watchdog firing is observable.
    """
    label: str
    socket: _MockWebSocket
    keep_alive: KeepAliveMiddleware
    terminated_query_ids: list[str]

    def feed(self, query: KataGoQuery, orig_id: str) -> None:
        """Simulate this phantom LEAF's session observing one query.

        Mirrors what proxy_server.py's _handle_incoming does: parse
        the wire (already done — `query` is parsed), call
        middleware.on_query. The keep-alive middleware reads the
        action and updates its bookkeeping accordingly.
        """
        self.keep_alive.on_query(orig_id, query)


def _build_phantom_leaf(label: str, *, idle_timeout_seconds: float,
                        check_interval_seconds: float) -> _PhantomLeaf:
    """Construct a phantom LEAF with its own KeepAliveMiddleware."""
    socket = _MockWebSocket(label)
    keep_alive = KeepAliveMiddleware(
        idle_timeout_seconds=idle_timeout_seconds,
        check_interval_seconds=check_interval_seconds,
    )
    terminated: list[str] = []

    async def submit_query(_orig_id: str, _query: KataGoQuery) -> None:
        # The keep-alive middleware does not call submit_query —
        # only terminate_query — but the SessionCapabilities dataclass
        # requires both. Empty implementation suffices.
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
        label=label, socket=socket, keep_alive=keep_alive,
        terminated_query_ids=terminated,
    )


def _populate_selector(
    router: SelectorRouter, phantoms: dict[str, _PhantomLeaf],
) -> None:
    """Bypass SelectorRouter.start() and inject phantom LEAF connections."""
    for label, _url in router._models:
        router._url_for_label[label] = f"ws://phantom-{label}:0"
        router._failure_budget[label] = router._max_connect_failures
    for label, phantom in phantoms.items():
        router._connections[label] = phantom.socket


def _analyze_query(model: str) -> KataGoQuery:
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
            "model": model,
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
    print("SELECTOR-watchdog diagnostic — heartbeat broadcast across LEAFs")
    print("=" * 70)

    IDLE_TIMEOUT = 0.5
    CHECK_INTERVAL = 0.1
    HEARTBEAT_CADENCE = 0.1
    HEARTBEAT_DURATION = 2.5  # 5x idle_timeout

    # SELECTOR with two named upstreams.
    router = SelectorRouter(
        models=(
            ("strong", "ws://upstream-strong:0"),
            ("weak", "ws://upstream-weak:0"),
        ),
        max_connect_failures=3,
    )
    phantoms = {
        "strong": _build_phantom_leaf(
            "strong",
            idle_timeout_seconds=IDLE_TIMEOUT,
            check_interval_seconds=CHECK_INTERVAL,
        ),
        "weak": _build_phantom_leaf(
            "weak",
            idle_timeout_seconds=IDLE_TIMEOUT,
            check_interval_seconds=CHECK_INTERVAL,
        ),
    }
    _populate_selector(router, phantoms)

    # Sink callbacks for SELECTOR.dispatch — the SPA-side. The bug
    # under test is upstream of these (router routing); the SPA
    # callbacks just absorb whatever the SELECTOR emits.
    sink_responses: list[tuple[str, dict]] = []

    async def on_response(cid: str, wire: dict) -> None:
        sink_responses.append((cid, wire))

    async def on_complete(cid: str) -> None:
        pass

    # ----------------------------------------------------------------
    # Phase A — analyze on "weak" + heartbeats; no LEAF's watchdog fires
    # ----------------------------------------------------------------
    print(
        "\n--- Step 1: ANALYZE routed to 'weak' via `model` field ---"
    )
    analyze = _analyze_query(model="weak")
    analyze_wire = translate_query_to_wire(analyze, "cid-analyze")
    await router.dispatch(
        "cid-analyze", analyze_wire, analyze, on_response, on_complete,
    )
    # Production: the SELECTOR's read loop on the 'weak' upstream
    # would parse responses and forward them through the SELECTOR's
    # own ClientSession. The 'weak' LEAF's ClientSession sees the
    # analyze first (via _handle_incoming → middleware.on_query).
    # We simulate that step:
    phantoms["weak"].feed(analyze, "cid-analyze")
    print(f"  router.dispatched (canonicals): {[k for k in router._callbacks]}")
    print(f"  weak.keep_alive._in_flight: {phantoms['weak'].keep_alive._in_flight}")
    print(f"  strong.keep_alive._in_flight: {phantoms['strong'].keep_alive._in_flight}")
    if "cid-analyze" not in phantoms["weak"].keep_alive._in_flight:
        print("  FAIL: analyze did not reach 'weak's KeepAliveMiddleware._in_flight")
        return False
    if phantoms["strong"].keep_alive._in_flight:
        print("  FAIL: 'strong' has spurious _in_flight; analyze should have routed to 'weak' only")
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
        # Snapshot pre-broadcast counts so we can verify *this* tick's
        # broadcast hit every LEAF.
        prev_counts = {
            label: len(p.socket.sent) for label, p in phantoms.items()
        }
        await router.dispatch(cid, hb_wire, hb, on_response, on_complete)
        # Verify both phantom LEAFs received the heartbeat wire on
        # their mock sockets — this is the regression-specific
        # property: pre-fix only the first-healthy LEAF received it.
        for label, phantom in phantoms.items():
            new = len(phantom.socket.sent)
            if new != prev_counts[label] + 1:
                print(
                    f"  FAIL: LEAF {label!r} did not receive heartbeat "
                    f"seq={seq} (sends: {prev_counts[label]} → {new}). "
                    f"REGRESSION: SelectorRouter.dispatch is not "
                    f"broadcasting QUERY_VERSION to every healthy "
                    f"upstream. See the SELECTOR watchdog postmortem "
                    f"in the umbrella's docs/notes/."
                )
                return False
        # Production: each LEAF's read loop parses the wire and calls
        # middleware.on_query. Simulate.
        for phantom in phantoms.values():
            phantom.feed(hb, cid)

    print(f"  sent {seq} heartbeats; both LEAFs received every one")

    # No phantom LEAF's watchdog should have fired.
    for label, phantom in phantoms.items():
        if phantom.terminated_query_ids:
            print(
                f"\n  FAIL (Phase A): LEAF {label!r}'s watchdog fired and "
                f"terminated {phantom.terminated_query_ids!r} during the "
                f"heartbeat phase. Regression: KeepAliveMiddleware did "
                f"not see the heartbeats (chain composition broken on "
                f"the LEAF side?)."
            )
            return False
    print(
        "  PASS (Phase A): both LEAFs received every heartbeat; "
        "no watchdog fired"
    )

    # ----------------------------------------------------------------
    # Phase B — heartbeats stop; only the LEAF with in-flight fires
    # ----------------------------------------------------------------
    print(
        f"\n--- Step 3: stop heartbeats; wait > idle_timeout "
        f"({IDLE_TIMEOUT}s) ---"
    )
    await asyncio.sleep(IDLE_TIMEOUT + CHECK_INTERVAL + 0.3)

    # 'weak' has an in-flight analyze; its watchdog must fire.
    if "cid-analyze" not in phantoms["weak"].terminated_query_ids:
        print(
            f"\n  FAIL (Phase B): 'weak's watchdog did not terminate "
            f"the in-flight analyze after heartbeats stopped. "
            f"terminated_query_ids={phantoms['weak'].terminated_query_ids!r}"
        )
        return False
    # 'strong' has no in-flight; its watchdog's empty-flight self-reset
    # branch must keep last_heartbeat fresh and avoid spurious
    # terminations. (It might still fire once and find empty
    # _in_flight; the contract is that it doesn't terminate anything,
    # not that the watchdog never wakes.)
    if phantoms["strong"].terminated_query_ids:
        print(
            f"\n  FAIL (Phase B): 'strong's watchdog terminated "
            f"{phantoms['strong'].terminated_query_ids!r} despite having "
            f"no in-flight queries. The empty-flight self-reset branch "
            f"is broken."
        )
        return False
    print(
        "  PASS (Phase B): 'weak's watchdog fired on its in-flight "
        "analyze; 'strong's stayed quiet"
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
