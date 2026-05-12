"""
diagnose_watchdog.py — Verify the keep-alive watchdog stays quiet under
SPA-cadence heartbeats AND fires when heartbeats stop, exercising the
PRODUCTION middleware composition (KeepAliveMiddleware outer, wrapping
CapabilityGatedMiddleware around the v1.0.16 orchestration-shaped
adaptive_reevaluate).

Complement of diagnose_phase3, which exercises the watchdog with a
*bare* KeepAliveMiddleware as the only middleware. That coverage is
necessary but not sufficient: the production chain has two layers
between _handle_incoming's middleware.on_query call and KeepAlive's
heartbeat-recognition logic — CapabilityGatedMiddleware (which can
short-circuit per-query opt-out) and OrchestrationMiddleware (which
spawns a per-query coroutine task). Either layer changing in a way
that fails to propagate query_version through to KeepAlive.on_query
would silently break the heartbeat reset and let the watchdog fire
during legitimate long-running analyses.

Two phases, one scenario:

  Phase A — heartbeats flowing → watchdog must NOT fire
    1. Build production chain (KeepAlive(outer) wraps
       CapabilityGated('adaptive_reevaluate') around adaptive_reevaluate).
       Idle timeout 0.5s, check interval 0.1s for a fast loop.
    2. Send a long-running ANALYZE through the chain (on_query for
       middleware bookkeeping; _handle_query for router dispatch).
    3. Send heartbeats (action=query_version) every 0.1s for 2.5s
       (5x the idle timeout — many chances for a false fire).
    4. Verify: router.terminated is empty; KeepAlive._in_flight still
       carries the analyze.

  Phase B — heartbeats stop → watchdog MUST fire
    5. Stop heartbeats; sleep idle_timeout + check_interval + margin.
    6. Verify: router.terminated has the analyze's canonical;
       KeepAlive._in_flight is empty.

The two halves together specify the contract precisely. Either
half failing is a regression worth surfacing audibly.

Background — the regression this test guards against:

The v1.0.17 band-aid commit (cfb976a, "chore(keep-alive): bump
KEEP_ALIVE_IDLE_TIMEOUT_SECONDS default 25s -> 250s") was sized
for a real symptom — long range queries terminated by the watchdog
mid-stream — but the root-cause analysis was deferred. This test
is the contract-level check: as long as middleware.on_query is
reached for each heartbeat, the chain MUST propagate it to
KeepAlive.on_query and KeepAlive MUST reset last_heartbeat. If
this test passes, the failure mode lives upstream of the chain
(SPA-side: setInterval throttled by background tab; JS event-loop
starvation under heavy work) or in the receive loop's
await self._router.dispatch (network-latency-induced backpressure
under SELECTOR/RELAY) — not in middleware composition.

Run from the proxy directory:
    python -m tests.diagnose_watchdog

Exit 0 on PASS, 1 on FAIL. Prints all observed events.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Make the proxy root importable when running as a script.
_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import ClientId  # noqa: E402
from katago import KataGoAction, KataGoQuery  # noqa: E402
from middleware.adaptive_reevaluate import adaptive_reevaluate  # noqa: E402
from middleware.capability_gate import CapabilityGatedMiddleware  # noqa: E402
from middleware.keep_alive import KeepAliveMiddleware  # noqa: E402
from middleware.session_middleware import (  # noqa: E402
    MiddlewareChain,
    SessionMiddleware,
)
from proxy_server import ClientSession  # noqa: E402
from pubsub_hub import PubSubHub  # noqa: E402

from tests.synthetic_backend import SyntheticPonderingRouter  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Test fixtures (matches diagnose_phase{1,2,3}.py idioms)
# ---------------------------------------------------------------------------

class MockWebSocket:
    """Minimal WS mock for ClientSession construction."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.remote_address = ("127.0.0.1", 0)
        self.sent: list[str] = []

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        pass


def _ponder_query() -> KataGoQuery:
    """Long-running analyze (matches phase3's shape)."""
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
    """SPA's watchdog heartbeat shape: action=query_version, no opaque.

    Mirrors `analysisService.startWatchdog`'s
    `client.sendCommand({ action: 'query_version', ... })` — which is
    the wire form KeepAliveMiddleware._is_query_version expects.
    """
    return KataGoQuery(action=KataGoAction.QUERY_VERSION)


def _make_production_middleware(
    *,
    idle_timeout_seconds: float,
    check_interval_seconds: float,
) -> tuple[SessionMiddleware, KeepAliveMiddleware]:
    """Build the production middleware chain.

    Mirrors proxy_server._make_middleware exactly, except the
    KEEP_ALIVE_IDLE_TIMEOUT_SECONDS / check interval are parameterised
    here for fast test loops. The composition is the regression-
    detection point: if proxy_server.py's _make_middleware changes
    its layering, this builder must be updated in lockstep, and the
    diff is what surfaces the change for review.

    Returns (chain, keep_alive) so callers can both drive the chain
    and inspect KeepAliveMiddleware's internal _in_flight set.
    """
    base = CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=3,
        )(),  # () to invoke the v1.0.16 factory (returns OrchestrationMiddleware).
    )
    keep_alive = KeepAliveMiddleware(
        idle_timeout_seconds=idle_timeout_seconds,
        check_interval_seconds=check_interval_seconds,
    )
    chain = MiddlewareChain(inner=base, outer=keep_alive)
    return chain, keep_alive


async def _drain_send_queue(session: ClientSession, max_drains: int = 10000) -> int:
    """Drop pending wires from the session's send queue without sending.

    Matches diagnose_phase{1,2}.py. The synthetic backend keeps emitting
    intermediates onto the hub fan-out, which puts to each subscriber's
    queue; without periodic draining, memory grows during the long
    heartbeat phase. Manual drain bounds it.
    """
    drained = 0
    for _ in range(max_drains):
        try:
            session._send_queue.get_nowait()
            drained += 1
        except asyncio.QueueEmpty:
            return drained
    return drained


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

async def run_scenario() -> bool:
    print()
    print("=" * 70)
    print("Watchdog diagnostic — heartbeats keep watchdog quiet; absence trips it")
    print("=" * 70)

    # Aggressive timing for a fast test loop. Ratio mirrors production:
    # SPA's 5s watchdog cadence vs. v1.0.10's 25s idle timeout = 0.2;
    # 0.1s heartbeat cadence vs. 0.5s idle timeout = 0.2 here.
    IDLE_TIMEOUT = 0.5
    CHECK_INTERVAL = 0.1
    HEARTBEAT_CADENCE = 0.1
    HEARTBEAT_DURATION = 2.5  # 5x idle timeout — many chances for a false fire

    hub = PubSubHub()
    router = SyntheticPonderingRouter(emit_interval_s=0.05)
    await router.start()

    middleware, keep_alive = _make_production_middleware(
        idle_timeout_seconds=IDLE_TIMEOUT,
        check_interval_seconds=CHECK_INTERVAL,
    )

    ws = MockWebSocket("A")

    # ClientSession.__init__ calls middleware.on_session_start, which
    # (via the chain's inner→outer order) starts the orchestration
    # framework's bookkeeping and KeepAliveMiddleware's watchdog task.
    # We are inside an event loop (asyncio.run wraps run_scenario), so
    # the task scheduling is safe.
    session = ClientSession(
        ws, "A", hub, router,
        transformer_factory=None,
        middleware=middleware,
        rate_limit=None,
    )

    # ----------------------------------------------------------------
    # Phase A — long analyze + heartbeats; watchdog must NOT fire
    # ----------------------------------------------------------------
    print("\n--- Step 1: dispatch the long-running analyze ---")
    # _handle_query alone does NOT call middleware.on_query (that's
    # _handle_incoming's job). For this test we mimic _handle_incoming
    # by calling on_query manually so the chain's bookkeeping runs.
    middleware.on_query(ClientId("orig_A"), _ponder_query())
    await session._handle_query(ClientId("orig_A"), _ponder_query())
    if not router.dispatched:
        print("  FAIL: synthetic router never received the dispatch")
        await router.stop()
        return False
    canonical = router.dispatched[0]
    print(f"  router.dispatched: {router.dispatched}")
    print(f"  keep_alive._in_flight: {keep_alive._in_flight}")
    if "orig_A" not in keep_alive._in_flight:
        print(
            "  FAIL: the analyze did not reach KeepAliveMiddleware._in_flight. "
            "The chain is not propagating ANALYZE on_query to the outer "
            "KeepAliveMiddleware. Either MiddlewareChain.on_query order "
            "(inner-first, outer-second) has changed, or an inner layer "
            "raised silently."
        )
        await router.stop()
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
        # Heartbeat enters the chain via on_query. CapabilityGate
        # auto-engages (no `capabilities` opaque field on a heartbeat),
        # delegates to the wrapped OrchestrationMiddleware. KeepAlive
        # (outer) then observes query_version and resets last_heartbeat.
        # We deliberately do NOT call _handle_query for heartbeats —
        # the keep-alive contract depends only on the on_query side;
        # dispatching adds response-side noise without exercising
        # additional contract.
        middleware.on_query(ClientId(f"hb-{seq}"), _heartbeat_query())
        # Bound queue growth from the synthetic router's intermediates.
        await _drain_send_queue(session)

    print(f"  sent {seq} heartbeats over ~{elapsed:.1f}s")
    print(f"  router.terminated: {router.terminated}")
    print(f"  keep_alive._in_flight: {keep_alive._in_flight}")

    if router.terminated:
        print()
        print(
            f"  FAIL (Phase A): watchdog terminated canonical {canonical!r} "
            f"despite heartbeats arriving every {HEARTBEAT_CADENCE}s "
            f"(idle_timeout={IDLE_TIMEOUT}s). Either the production "
            f"middleware chain is not propagating heartbeats to "
            f"KeepAliveMiddleware.on_query, or KeepAliveMiddleware is not "
            f"resetting last_heartbeat on query_version. THIS IS THE "
            f"REGRESSION the v1.0.17 band-aid (cfb976a, 25s->250s) papered "
            f"over without diagnosing."
        )
        await router.stop()
        return False
    if "orig_A" not in keep_alive._in_flight:
        print()
        print(
            "  FAIL (Phase A): the analyze 'orig_A' was dropped from "
            "KeepAliveMiddleware._in_flight during the heartbeat phase. "
            "The watchdog cannot terminate it in Phase B; the contract is "
            "structurally unsound. Likely cause: AnalyzeResponse "
            "is_during_search=False has been observed for a partial "
            "(adaptive_reevaluate's deepening pattern, or a stray "
            "metadata) and the discard logic is over-eager."
        )
        await router.stop()
        return False
    print(f"  PASS (Phase A): watchdog stayed quiet under heartbeat flow")

    # ----------------------------------------------------------------
    # Phase B — heartbeats stop; watchdog MUST fire
    # ----------------------------------------------------------------
    print(
        f"\n--- Step 3: stop heartbeats; wait > idle_timeout "
        f"({IDLE_TIMEOUT}s) ---"
    )
    # Last heartbeat reset last_heartbeat ~HEARTBEAT_CADENCE ago. Sleep
    # idle_timeout + check_interval + margin so the watchdog has at
    # least one tick where idle > timeout.
    await asyncio.sleep(IDLE_TIMEOUT + CHECK_INTERVAL + 0.3)
    print(f"  router.terminated: {router.terminated}")
    print(f"  keep_alive._in_flight: {keep_alive._in_flight}")

    if canonical not in router.terminated:
        print()
        print(
            f"  FAIL (Phase B): watchdog did NOT terminate canonical "
            f"{canonical!r} after heartbeats stopped for > idle_timeout. "
            f"The watchdog is not firing under the production chain "
            f"composition. Likely cause: chain.on_session_start did not "
            f"start KeepAliveMiddleware's watchdog task (the outer's "
            f"on_session_start was skipped), or KeepAliveMiddleware._caps "
            f"was not populated and the terminate_query call inside "
            f"_watchdog raised."
        )
        await router.stop()
        return False
    if keep_alive._in_flight:
        print()
        print(
            f"  FAIL (Phase B): keep_alive still tracks _in_flight="
            f"{keep_alive._in_flight!r} after the watchdog fired. The "
            f"watchdog clears _in_flight on terminate (keep_alive.py:186)."
        )
        await router.stop()
        return False
    print(f"  PASS (Phase B): watchdog fired after heartbeats stopped")

    # Cleanup. Chain on_session_end runs outer-first (cancels the
    # watchdog task) then inner (cancels orchestration coroutines for
    # the heartbeats and the analyze).
    middleware.on_session_end()
    await asyncio.sleep(0.1)  # let cancellations propagate
    await router.stop()
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
