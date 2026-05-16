"""
tests/test_relay_coalescing.py — PubSubHub + RelayRouter coalescing contract.

The plan note `docs/notes/proxy-topology-testing-plan.md` §1.2 names
RELAY coalescing across multiple subscribers as currently-unexercised
in any in-process test. `test_relay_router.py` covers the router's
single-target and broadcast dispatch contracts in isolation; nothing
exercises the hub+router boundary the SPA actually drives in
production. This file fills that gap.

Two subscribers each issuing the SAME analyze query result in:

  1. The hub coalesces them onto one canonical_id (one InFlightEntry,
     two Subscribers). Second `subscribe()` returns
     `(False, same_canonical_id)`.
  2. The router dispatches the canonical exactly once, to one upstream
     (the hash ring's preferred node for that canonical_id).
  3. When the upstream's response arrives, the hub fans it out to both
     subscribers, each with their own subscriber_internal_id
     substituted in.

A regression that breaks (1) — e.g., a `CoalescingPolicy.capturing_fields`
change that includes a per-client field without intent — surfaces as
`is_new_2 == True`. A regression that breaks (2) — e.g., a hub change
that double-dispatches — surfaces as multiple upstream sends. A
regression that breaks (3) — e.g., subscriber-list mishandling on
response — surfaces as missing queue entries.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import CanonicalId, InternalId  # noqa: E402
from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from pubsub_hub import PubSubHub  # noqa: E402
from router import InFlightQueryLoad, RelayRouter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Same shape as `test_relay_router.py`'s `_MockWebSocket`. Inlined
    rather than promoted to `tests/_fixtures.py` per the plan §4.1
    convention: shared fixtures graduate when a third file needs them."""

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


def _make_relay(urls: list[str]) -> RelayRouter:
    return RelayRouter(
        upstream_urls=urls,
        load_metric=InFlightQueryLoad(),
        max_load=1000,
    )


def _populate_relay(
    router: RelayRouter, urls: list[str],
) -> dict[str, _MockWebSocket]:
    """Bypass `RelayRouter.start()` (which would open real WebSockets)
    and inject mock connections directly into `_connections`."""
    sockets = {url: _MockWebSocket(url) for url in urls}
    router._connections.update(sockets)
    return sockets


def _identical_analyze_query() -> KataGoQuery:
    """An analyze query two subscribers can issue identically.

    Uses only fields in the default `CoalescingPolicy.capturing_fields`
    (rules, komi, boardX/YSize, moves) plus `analyze_turns` (always
    captured). No `capabilities` / `model` / cache flags so the default-
    shape SPA query path is what's tested. `maxVisits` is intentionally
    excluded from the policy's hash, so two subscribers with different
    visit budgets would still coalesce — but that's a different test;
    this one keeps both visits identical too so the coalescing reason
    is unambiguously the policy fields."""
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0],
        opaque={
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "moves": [["B", "Q4"]],
            "maxVisits": 100,
        },
    )


# ---------------------------------------------------------------------------
# Hub + router integration: coalescing → single dispatch → fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCoalescingFanout:
    async def test_two_identical_subscribers_coalesce_one_dispatch_two_fanouts(
        self,
    ) -> None:
        upstream_urls = [
            "ws://leaf-a:1", "ws://leaf-b:2", "ws://leaf-c:3",
        ]
        router = _make_relay(upstream_urls)
        sockets = _populate_relay(router, upstream_urls)
        hub = PubSubHub()

        # Two synthetic subscribers; ClientSession constructs these in
        # production, one per WebSocket.
        sub1_id = InternalId("internal-1")
        sub1_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        sub2_id = InternalId("internal-2")
        sub2_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        # Two distinct query OBJECTS with identical content. Queries are
        # passed by reference and `subscribe()` pops fields off opaque,
        # so reusing the same object across two subscribes would test
        # object identity, not content identity.
        q1 = _identical_analyze_query()
        q2 = _identical_analyze_query()

        # First subscribe: hub creates a new slot.
        is_new_1, cid_1 = hub.subscribe(q1, sub1_id, sub1_q)
        assert is_new_1, "first subscribe should create a new in-flight slot"

        # Second subscribe: hub coalesces onto the same slot.
        is_new_2, cid_2 = hub.subscribe(q2, sub2_id, sub2_q)
        assert not is_new_2, (
            "second identical-content subscribe should coalesce; "
            f"is_new={is_new_2!r}"
        )
        assert cid_1 == cid_2, (
            f"coalesced subscribers must share canonical_id; "
            f"first={cid_1!r} second={cid_2!r}"
        )

        # Mirror the production ClientSession's discipline: only
        # dispatch to the router on `is_new=True`. The second subscribe
        # yields no router call.
        wire = translate_query_to_wire(q1, cid_1)
        await router.dispatch(
            cid_1, wire, q1, hub.on_response, hub.on_complete,
        )

        # Exactly one upstream socket received exactly one send.
        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        assert sum(sent_counts.values()) == 1, (
            f"single coalesced canonical should hit exactly one upstream; "
            f"got {sent_counts!r}"
        )

        # Now simulate the upstream's response. Production path:
        # `_read_loop` parses the wire → fires `on_response`. We bypass
        # `_read_loop` (which would require a stateful mock yielding
        # messages) and call `hub.on_response` directly with the wire
        # the read loop would have built.
        synthetic_response: Dict[str, Any] = {
            "id": cid_1,  # canonical-namespace id; hub will relabel
            "moveInfos": [],
            "rootInfo": {"visits": 100},
            "isDuringSearch": False,
            "turnNumber": 0,
        }
        await hub.on_response(cid_1, synthetic_response)

        # Both subscribers received the relabelled response on their
        # own queues.
        assert sub1_q.qsize() == 1, (
            "subscriber 1 missed the fan-out (queue empty)"
        )
        assert sub2_q.qsize() == 1, (
            "subscriber 2 missed the fan-out (queue empty)"
        )

        sub1_msg = sub1_q.get_nowait()
        sub2_msg = sub2_q.get_nowait()

        # Each subscriber's `id` field carries THEIR OWN internal_id,
        # not the canonical_id. This is the relabelling that lets
        # Layer 1 operate without coalescing knowledge.
        assert sub1_msg["id"] == sub1_id, (
            f"sub1 should see its own internal id; got {sub1_msg['id']!r}"
        )
        assert sub2_msg["id"] == sub2_id, (
            f"sub2 should see its own internal id; got {sub2_msg['id']!r}"
        )

        # Non-id content is identical between fan-outs (shallow-copy
        # relabelling shares nested object references; the test asserts
        # value equality rather than object identity since either is
        # acceptable from the consumer's perspective).
        assert sub1_msg["rootInfo"] == sub2_msg["rootInfo"]
        assert sub1_msg["isDuringSearch"] == sub2_msg["isDuringSearch"]
        assert sub1_msg["moveInfos"] == sub2_msg["moveInfos"]

    async def test_distinct_queries_do_not_coalesce(self) -> None:
        """Negative control: two queries differing on a captured field
        (`moves`) get distinct canonical_ids and distinct dispatches.
        Without this, a coalescing test that always passes (e.g., a
        broken policy that returns one hash for everything) would still
        look like it's working."""
        upstream_urls = [
            "ws://leaf-a:1", "ws://leaf-b:2", "ws://leaf-c:3",
        ]
        router = _make_relay(upstream_urls)
        sockets = _populate_relay(router, upstream_urls)
        hub = PubSubHub()

        sub1_id = InternalId("internal-1")
        sub1_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        sub2_id = InternalId("internal-2")
        sub2_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        q1 = _identical_analyze_query()
        q2 = _identical_analyze_query()
        q2.opaque["moves"] = [["B", "K10"]]  # different position → different hash

        is_new_1, cid_1 = hub.subscribe(q1, sub1_id, sub1_q)
        is_new_2, cid_2 = hub.subscribe(q2, sub2_id, sub2_q)

        assert is_new_1 and is_new_2, (
            "distinct-content queries should both create new slots; "
            f"got is_new=({is_new_1}, {is_new_2})"
        )
        assert cid_1 != cid_2, (
            f"distinct queries should get distinct canonical_ids; "
            f"got {cid_1!r} == {cid_2!r}"
        )

        wire1 = translate_query_to_wire(q1, cid_1)
        wire2 = translate_query_to_wire(q2, cid_2)
        await router.dispatch(
            cid_1, wire1, q1, hub.on_response, hub.on_complete,
        )
        await router.dispatch(
            cid_2, wire2, q2, hub.on_response, hub.on_complete,
        )

        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        assert sum(sent_counts.values()) == 2, (
            f"two distinct canonicals should produce two dispatches; "
            f"got {sent_counts!r}"
        )
