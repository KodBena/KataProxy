"""
diagnose_phase2.py — Verify v1.0.8's coalescing-transparent explicit
terminate. The bug v1.0.8 fixed: when client A explicitly terminates a
query that's currently coalesced with client B's identical query, the
pre-v1.0.8 _handle_terminate fired router.terminate(canonical) at the
LEAF, silently ending B's analysis stream. The fix branches on the
was_last contract: A's terminate synthesizes a local ack and leaves
the LEAF running; only when the last subscriber leaves does
router.terminate fire.

Scenario:
  1. Two ClientSessions (A, B) subscribe to an identical ponder
     query — they coalesce onto the same canonical at the hub.
  2. Synthetic backend emits a few intermediate isDuringSearch=True
     responses to both subscribers.
  3. Session A sends an explicit TERMINATE for its ponder.
  4. Verify:
       - router.terminated is empty (no LEAF terminate dispatched).
       - router.live still has the canonical (still pondering).
       - A's send_queue contains a synthesized terminate ack
         ({"action": "terminate"}).
  5. Wait. B's intermediates keep flowing — verify the synthetic
     backend's emit count for the canonical increases.
  6. Session B sends an explicit TERMINATE for its ponder.
  7. Verify:
       - router.terminated now has the canonical (real LEAF
         terminate dispatched).
       - router.live is empty.

Run from repo root: `python -m tests.diagnose_phase2`
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.katago_proxy import KataGoAction, KataGoQuery  # noqa: E402
from proxy_server import ClientSession  # noqa: E402
from pubsub_hub import PubSubHub  # noqa: E402

from tests.synthetic_backend import SyntheticPonderingRouter  # noqa: E402


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname).1s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


class MockWebSocket:
    def __init__(self, name: str) -> None:
        self.name = name
        self.remote_address = ("127.0.0.1", 0)
        self.sent: list[str] = []

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        pass


def _ponder_query() -> KataGoQuery:
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


def _terminate_query(target_orig_id: str) -> KataGoQuery:
    return KataGoQuery(
        action=KataGoAction.TERMINATE,
        terminate_id=target_orig_id,
    )


async def _drain_queue(session: ClientSession, max_drains: int = 1000) -> list:
    """Drain a session's send_queue, returning all wires."""
    wires = []
    for _ in range(max_drains):
        try:
            wires.append(session._send_queue.get_nowait())
        except asyncio.QueueEmpty:
            return wires
    return wires


def _find_terminate_ack(wires: list) -> dict | None:
    for w in wires:
        if isinstance(w, dict) and w.get("action") == "terminate":
            return w
    return None


async def run_scenario() -> bool:
    print()
    print("=" * 70)
    print("Phase 2 diagnostic — coalescing-transparent explicit terminate")
    print("=" * 70)

    hub = PubSubHub()
    router = SyntheticPonderingRouter(emit_interval_s=0.05)
    await router.start()

    ws_a = MockWebSocket("A")
    ws_b = MockWebSocket("B")
    session_a = ClientSession(
        ws_a, "A", hub, router,
        transformer_factory=None, middleware=None, rate_limit=None,
    )
    session_b = ClientSession(
        ws_b, "B", hub, router,
        transformer_factory=None, middleware=None, rate_limit=None,
    )

    print("\n--- Step 1: A and B subscribe to identical ponder (coalesce) ---")
    await session_a._handle_query("orig_A", _ponder_query())
    await session_b._handle_query("orig_B", _ponder_query())
    if len(router.dispatched) != 1:
        print(f"  UNEXPECTED: dispatched should be 1, got {len(router.dispatched)}")
        return False
    canonical = router.dispatched[0]
    print(f"  coalesced canonical_id: {canonical}")

    print("\n--- Step 2: let synthetic backend emit a few intermediates ---")
    await asyncio.sleep(0.2)
    a_wires_pre = await _drain_queue(session_a)
    b_wires_pre = await _drain_queue(session_b)
    emits_pre_a_terminate = router.live.get(canonical, 0)
    print(f"  A received {len(a_wires_pre)} intermediates")
    print(f"  B received {len(b_wires_pre)} intermediates")
    print(f"  router emit count (canonical): {emits_pre_a_terminate}")

    print("\n--- Step 3: A explicitly terminates its ponder ---")
    await session_a._handle_terminate("orig_A_term", _terminate_query("orig_A"))

    print("\n--- Step 4: verify multi-subscriber path ---")
    if router.terminated:
        print(f"  FAIL: router.terminated={router.terminated}; "
              f"A's terminate should NOT have hit the LEAF (B still subscribed)")
        return False
    if canonical not in router.live:
        print(f"  FAIL: router.live={router.live}; "
              f"canonical {canonical!r} should still be pondering")
        return False
    a_wires_post = await _drain_queue(session_a)
    print(f"  A received {len(a_wires_post)} additional wires after terminate")
    ack = _find_terminate_ack(a_wires_post)
    if ack is None:
        print(f"  FAIL: no synthesized terminate ack on A's send_queue. "
              f"Wires drained: {a_wires_post}")
        return False
    print(f"  A's synthesized terminate ack: {ack}")

    print("\n--- Step 5: B's analysis continues — verify more intermediates ---")
    await asyncio.sleep(0.2)
    b_wires_continued = await _drain_queue(session_b)
    emits_post_a_terminate = router.live.get(canonical, 0)
    print(f"  B received {len(b_wires_continued)} additional intermediates")
    print(f"  router emit count (canonical): {emits_post_a_terminate}")
    if len(b_wires_continued) == 0:
        print(f"  FAIL: B's analysis stream stopped — coalescing isolation broken")
        return False
    if emits_post_a_terminate <= emits_pre_a_terminate:
        print(f"  FAIL: emit count did not advance "
              f"({emits_pre_a_terminate} → {emits_post_a_terminate})")
        return False

    print("\n--- Step 6: B explicitly terminates its ponder (now sole subscriber) ---")
    await session_b._handle_terminate("orig_B_term", _terminate_query("orig_B"))

    # Allow a tick for the router's terminate task and synthetic ack to settle.
    await asyncio.sleep(0.1)

    print("\n--- Step 7: verify sole-subscriber path ---")
    if canonical not in router.terminated:
        print(f"  FAIL: router.terminated={router.terminated}; "
              f"B's terminate (last subscriber) should have hit the LEAF")
        return False
    if router.live:
        print(f"  FAIL: router.live={router.live}; should be empty after last terminate")
        return False
    print(f"  router.terminated: {router.terminated}")
    print(f"  router.live: {router.live}")

    await router.stop()

    print()
    print(f"  PASS: A's terminate stayed local (synthesized ack, no LEAF terminate)")
    print(f"        B's stream continued through A's terminate")
    print(f"        B's terminate (sole subscriber) fired LEAF terminate")
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
