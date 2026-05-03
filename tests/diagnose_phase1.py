"""
diagnose_phase1.py — Reproduce the multi-subscriber-then-both-disconnect
scenario at the Python level using SyntheticPonderingRouter.

Scenario:
  1. Construct hub + synthetic router.
  2. Construct two ClientSessions with mock WSes (no real network).
  3. Each session sends an identical ANALYZE query (so they coalesce
     onto the same canonical at the hub).
  4. Synthetic router emits a few intermediate isDuringSearch=True
     responses (simulating a long-running ponder).
  5. Each session's _cleanup is invoked directly (simulating WS
     disconnect after the run-loop ends).
  6. Verify: synthetic router observed exactly one terminate dispatch
     for the coalesced canonical (the one fired by the second session,
     after it became the sole-and-then-orphaned subscriber).

Run from repo root: `python3 -m tests.diagnose_phase1`
Or from anywhere: `python3 tests/diagnose_phase1.py`

Prints all observed events. Exit code 0 on PASS, 1 on FAIL.
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

from AbstractProxy.katago_proxy import KataGoAction, KataGoQuery  # noqa: E402
from proxy_server import ClientSession  # noqa: E402
from pubsub_hub import PubSubHub  # noqa: E402

from tests.synthetic_backend import SyntheticPonderingRouter  # noqa: E402


# Verbose diagnostic logging — every layer's log is interesting here.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


class MockWebSocket:
    """Minimal WS mock for ClientSession construction.

    Provides .remote_address (used by the per-IP rate limiter at construction
    time) and .send (used by _send_loop, if it runs). We don't iterate this
    object — the diagnostic drives sessions by calling _handle_query directly,
    bypassing _receive_loop.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.remote_address = ("127.0.0.1", 0)
        self.sent: list[str] = []

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        pass


def _ponder_query() -> KataGoQuery:
    """A representative empty-board ponder query (high maxVisits)."""
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


async def _drain_send_queue(session: ClientSession, max_drains: int = 1000) -> int:
    """Empty a session's send_queue without WS sending. Returns # drained.

    The synthetic backend's emit loop will keep adding to the hub fan-out,
    which puts to each subscriber's queue. If we never drain, the queues
    grow unboundedly. Manual drain keeps memory in check during the
    intermediate-emission window.
    """
    drained = 0
    for _ in range(max_drains):
        try:
            session._send_queue.get_nowait()
            drained += 1
        except asyncio.QueueEmpty:
            return drained
    return drained


async def run_scenario() -> bool:
    print()
    print("=" * 70)
    print("Phase 1 diagnostic — multi-subscriber-then-both-disconnect")
    print("=" * 70)

    hub = PubSubHub()
    router = SyntheticPonderingRouter(emit_interval_s=0.05)
    await router.start()

    ws_a = MockWebSocket("A")
    ws_b = MockWebSocket("B")

    session_a = ClientSession(
        ws_a, "A", hub, router, transformer_factory=None, middleware=None, rate_limit=None,
    )
    session_b = ClientSession(
        ws_b, "B", hub, router, transformer_factory=None, middleware=None, rate_limit=None,
    )

    print("\n--- Step 1: A subscribes to ponder ---")
    await session_a._handle_query("orig_A", _ponder_query())
    print(f"  session_a._active_queries: {dict(session_a._active_queries)}")
    print(f"  router.dispatched: {router.dispatched}")

    print("\n--- Step 2: B subscribes to identical ponder (should coalesce) ---")
    await session_b._handle_query("orig_B", _ponder_query())
    print(f"  session_b._active_queries: {dict(session_b._active_queries)}")
    print(f"  router.dispatched: {router.dispatched}")
    if len(router.dispatched) != 1:
        print(f"  UNEXPECTED: dispatched should be 1, got {len(router.dispatched)}")
        return False

    canonical = router.dispatched[0]
    print(f"  coalesced canonical_id: {canonical}")

    print("\n--- Step 3: let synthetic backend emit a few intermediates ---")
    await asyncio.sleep(0.2)
    drained_a = await _drain_send_queue(session_a)
    drained_b = await _drain_send_queue(session_b)
    print(f"  router.live: {router.live}")
    print(f"  drained from session_a queue: {drained_a}")
    print(f"  drained from session_b queue: {drained_b}")

    print("\n--- Step 4: A disconnects (cleanup runs) ---")
    await session_a._cleanup()
    print(f"  router.terminated so far: {router.terminated}")
    print(f"  router.live: {router.live}")
    if canonical in router.terminated:
        print("  UNEXPECTED: terminate fired after A's cleanup; B is still subscribed!")
        return False

    await asyncio.sleep(0.1)

    print("\n--- Step 5: B disconnects (cleanup runs) ---")
    await session_b._cleanup()

    # Allow the router's terminate task / cancellation to settle.
    await asyncio.sleep(0.1)

    print("\n--- Step 6: final state ---")
    print(f"  router.dispatched: {router.dispatched}")
    print(f"  router.terminated: {router.terminated}")
    print(f"  router.live: {router.live}")

    await router.stop()

    if canonical not in router.terminated:
        print()
        print(f"  FAIL: canonical {canonical!r} was never terminated.")
        print(f"        The ponder would still run on a real LEAF.")
        return False

    if router.live:
        print()
        print(f"  FAIL: router still has live canonicals: {router.live}")
        return False

    print()
    print(f"  PASS: canonical {canonical!r} was terminated exactly once.")
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
