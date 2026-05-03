"""
diagnose_phase3.py — Verify the keep-alive watchdog terminates a stranded
ANALYZE query when no heartbeat arrives within the idle timeout.

Scenario:
  1. Construct hub + synthetic router + a single ClientSession with a
     KeepAliveMiddleware configured for very short idle/check intervals.
  2. Send one ponder query (sole subscriber, so was_last semantics are
     simple — we are testing the watchdog, not coalescing).
  3. Send NO heartbeat queries.
  4. Wait for the watchdog interval to lapse.
  5. Verify: synthetic router observed exactly one terminate; the live
     set is empty; the terminate fired BEFORE _cleanup ran (we never
     call _cleanup in this scenario, so any terminate must have come
     from the middleware).

Run from repo root: `python -m tests.diagnose_phase3`
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
from keep_alive import KeepAliveMiddleware  # noqa: E402
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


async def run_scenario() -> bool:
    print()
    print("=" * 70)
    print("Phase 3 diagnostic — keep-alive watchdog terminates stranded query")
    print("=" * 70)

    hub = PubSubHub()
    router = SyntheticPonderingRouter(emit_interval_s=0.05)
    await router.start()

    # Aggressive idle timeout / check interval for a fast test loop.
    keep_alive = KeepAliveMiddleware(
        idle_timeout_seconds=0.5,
        check_interval_seconds=0.1,
    )

    ws = MockWebSocket("A")

    # ClientSession.__init__ now calls middleware.on_session_start, which
    # spawns the keep-alive watchdog task immediately. We are inside an
    # event loop here (asyncio.run wraps run_scenario), so the task
    # scheduling is safe.
    session = ClientSession(
        ws, "A", hub, router,
        transformer_factory=None,
        middleware=keep_alive,
        rate_limit=None,
    )

    print("\n--- Step 1: A sends a ponder query (no heartbeat ever) ---")
    # _handle_query alone does NOT call middleware.on_query (that's
    # _handle_incoming's job). For this test we mimic _handle_incoming by
    # calling on_query manually so the keep-alive's _in_flight is populated.
    keep_alive.on_query("orig_A", _ponder_query())
    await session._handle_query("orig_A", _ponder_query())
    print(f"  router.dispatched: {router.dispatched}")
    print(f"  keep_alive in-flight: {keep_alive._in_flight}")

    canonical = router.dispatched[0]

    print("\n--- Step 2: let the synthetic backend emit a few intermediates ---")
    await asyncio.sleep(0.2)
    print(f"  router.live: {router.live}")
    print(f"  router.terminated so far: {router.terminated}")
    if router.terminated:
        print("  UNEXPECTED: terminate fired before idle timeout elapsed")
        return False

    print("\n--- Step 3: wait past the idle timeout (watchdog should fire) ---")
    # Total elapsed: 0.2s + 0.6s = 0.8s, exceeding the 0.5s idle timeout
    # by enough margin to allow a check_interval (0.1s) tick to detect it.
    await asyncio.sleep(0.6)
    print(f"  router.terminated: {router.terminated}")
    print(f"  router.live: {router.live}")
    print(f"  keep_alive in-flight: {keep_alive._in_flight}")

    if canonical not in router.terminated:
        print()
        print(f"  FAIL: canonical {canonical!r} was never terminated by watchdog.")
        return False
    if router.live:
        print()
        print(f"  FAIL: router still has live canonicals: {router.live}")
        return False
    if keep_alive._in_flight:
        print()
        print(f"  FAIL: keep_alive still tracks in-flight: {keep_alive._in_flight}")
        return False

    # Cleanup: cancel the watchdog and stop the synthetic router.
    keep_alive.on_session_end()
    await asyncio.sleep(0.05)  # let the cancel propagate
    await router.stop()

    print()
    print(f"  PASS: watchdog terminated canonical {canonical!r} on idle timeout.")
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
