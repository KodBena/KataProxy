"""
tests/test_relay_load_distribution.py — RelayRouter hash-ring distribution.

The plan note `docs/notes/proxy-topology-testing-plan.md` §1.2 names
"stuck-on-one-LEAF behaviour" as a class of bug currently catchable
only via the operator's KataGo GPU-utilisation dashboard. The
existing `test_relay_router.py::test_analyze_routes_to_one_upstream_via_hash_ring`
asserts single-target dispatch hits exactly one upstream PER query;
this file extends that to the property the operator actually cares
about: a sample of N distinct canonical_ids spreads across the ring
rather than starving (N-1) of N upstreams.

The hash ring uses 150 replicas per node by default
(`cfg.HASH_RING_REPLICAS`). The expected distribution is roughly
uniform but not exactly so; this test asserts no upstream is starved
(each gets at least one query) and no upstream is strongly skewed
(>75% of total). The bound is loose enough to survive ordinary hash
variance over N=30 samples and tight enough to catch a regression
that defeats the ring (e.g., a stable-sort bug that collapses every
canonical to the first node).

A complementary load-aware-fallback test pins the second branch of
`_select_upstream`: when the preferred upstream is at `max_load`,
the dispatch walks past it.

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

from AbstractProxy.proxy_core import CanonicalId  # noqa: E402
from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from router import InFlightQueryLoad, RelayRouter  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Same shape as `test_relay_router.py`'s `_MockWebSocket`; inlined
    per the plan §4.1 promote-when-third-consumer convention."""

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


def _populate_relay(
    router: RelayRouter, urls: list[str],
) -> dict[str, _MockWebSocket]:
    sockets = {url: _MockWebSocket(url) for url in urls}
    router._connections.update(sockets)
    return sockets


def _analyze_query() -> KataGoQuery:
    """An analyze query whose canonical_id will be used as the hash key.

    The hash ring keys on canonical_id, not on query content, so the
    query content can stay constant across this test file's dispatches
    — only the canonical_id needs to vary."""
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0],
        opaque={
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "moves": [],
        },
    )


# ---------------------------------------------------------------------------
# Hash-ring distribution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestHashRingDistribution:
    async def test_distinct_canonicals_distribute_across_three_upstreams(
        self,
    ) -> None:
        """N distinct canonical_ids should hit all three upstreams,
        with no single upstream taking a strongly skewed share."""
        upstream_urls = [
            "ws://leaf-a:1", "ws://leaf-b:2", "ws://leaf-c:3",
        ]
        router = RelayRouter(
            upstream_urls=upstream_urls,
            load_metric=InFlightQueryLoad(),
            # max_load high enough that the load-aware fallback never
            # trips — this test pins ring distribution, not fallback.
            max_load=10_000,
        )
        sockets = _populate_relay(router, upstream_urls)

        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None:
            pass

        async def on_complete(_cid: CanonicalId) -> None:
            pass

        # 30 samples gives the 150-replica ring enough room to look
        # roughly uniform; small enough to keep the test fast.
        N = 30
        q = _analyze_query()
        for i in range(N):
            cid = CanonicalId(f"cid-{i:04d}")
            wire = translate_query_to_wire(q, cid)
            await router.dispatch(cid, wire, q, on_response, on_complete)

        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        total = sum(sent_counts.values())
        assert total == N, (
            f"expected {N} dispatches; got {total} (counts={sent_counts!r})"
        )

        # No starvation — every upstream got at least one query.
        for url, count in sent_counts.items():
            assert count > 0, (
                f"upstream {url!r} got 0 queries of {N}; hash-ring "
                f"distribution appears stuck-on-others. "
                f"counts={sent_counts!r}"
            )

        # No strong skew — no upstream took >75% of total. Ordinary
        # hash variance on N=30 / 3 nodes / 150 replicas should keep
        # the max share well under this; the bound is loose enough to
        # tolerate variance and tight enough to catch ring-bypass
        # regressions.
        max_share = max(sent_counts.values()) / total
        assert max_share <= 0.75, (
            f"hash-ring distribution is skewed: max share is "
            f"{max_share:.1%} of {N} queries. counts={sent_counts!r}"
        )


# ---------------------------------------------------------------------------
# Load-aware fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestLoadAwareFallback:
    async def test_dispatch_walks_past_saturated_preferred_upstream(
        self,
    ) -> None:
        """When the hash ring's preferred upstream is at `max_load`,
        `_select_upstream` walks the ring to the next under-loaded node.

        Setup: artificially saturate one upstream's load metric, then
        find a canonical_id whose hash-ring preference (in isolation)
        is that upstream, and dispatch it. Assert the dispatch landed
        on a DIFFERENT upstream — the fallback was exercised."""
        upstream_urls = [
            "ws://leaf-a:1", "ws://leaf-b:2", "ws://leaf-c:3",
        ]
        load_metric = InFlightQueryLoad()
        router = RelayRouter(
            upstream_urls=upstream_urls,
            load_metric=load_metric,
            max_load=2,
        )
        sockets = _populate_relay(router, upstream_urls)

        # Find a canonical_id whose hash-ring TOP preference is leaf-a.
        # We probe the ring directly (load-independent) to identify
        # such a key, so the fallback assertion below is unambiguous
        # about what was being walked past.
        target_url = "ws://leaf-a:1"
        chosen_cid: CanonicalId | None = None
        for i in range(1000):
            candidate = CanonicalId(f"probe-{i:04d}")
            ordered = router._ring.ordered_nodes_for(candidate)
            if ordered and ordered[0] == target_url:
                chosen_cid = candidate
                break
        assert chosen_cid is not None, (
            "could not find a canonical_id preferring leaf-a in 1000 "
            "samples — hash-ring construction or probing logic broken"
        )

        # Saturate leaf-a's load metric to `max_load`. The router's
        # `_select_upstream` now skips leaf-a for any candidate whose
        # ring preference is leaf-a.
        for i in range(router._max_load):
            load_metric.on_query_sent(
                target_url, CanonicalId(f"saturate-{i}"),
            )
        assert load_metric.current_load(target_url) == router._max_load

        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None:
            pass

        async def on_complete(_cid: CanonicalId) -> None:
            pass

        # Dispatch the chosen canonical. Ring would prefer leaf-a;
        # load-aware fallback should route it elsewhere.
        q = _analyze_query()
        wire = translate_query_to_wire(q, chosen_cid)
        await router.dispatch(chosen_cid, wire, q, on_response, on_complete)

        # leaf-a got nothing (skipped by fallback); one of the other
        # two upstreams got the dispatch.
        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        assert sent_counts[target_url] == 0, (
            f"saturated upstream {target_url!r} should have been "
            f"skipped; got {sent_counts!r}"
        )
        assert sum(sent_counts.values()) == 1, (
            f"exactly one upstream should have dispatched; "
            f"got {sent_counts!r}"
        )

    async def test_all_upstreams_saturated_falls_back_to_least_loaded(
        self,
    ) -> None:
        """When every connected upstream is at `max_load`, dispatch
        falls back to the least-loaded one rather than dropping. This
        is the worst-case branch of `_select_upstream`'s fallback."""
        upstream_urls = [
            "ws://leaf-a:1", "ws://leaf-b:2", "ws://leaf-c:3",
        ]
        load_metric = InFlightQueryLoad()
        router = RelayRouter(
            upstream_urls=upstream_urls,
            load_metric=load_metric,
            max_load=2,
        )
        sockets = _populate_relay(router, upstream_urls)

        # Saturate every upstream past max_load, with leaf-c the
        # least-loaded.
        for i in range(router._max_load + 5):
            load_metric.on_query_sent(
                "ws://leaf-a:1", CanonicalId(f"sat-a-{i}"),
            )
        for i in range(router._max_load + 3):
            load_metric.on_query_sent(
                "ws://leaf-b:2", CanonicalId(f"sat-b-{i}"),
            )
        for i in range(router._max_load + 1):
            load_metric.on_query_sent(
                "ws://leaf-c:3", CanonicalId(f"sat-c-{i}"),
            )
        # Sanity: all over max_load, leaf-c is the least.
        assert load_metric.current_load("ws://leaf-a:1") > router._max_load
        assert load_metric.current_load("ws://leaf-b:2") > router._max_load
        assert load_metric.current_load("ws://leaf-c:3") > router._max_load
        assert (
            load_metric.current_load("ws://leaf-c:3")
            < load_metric.current_load("ws://leaf-b:2")
            < load_metric.current_load("ws://leaf-a:1")
        )

        async def on_response(_cid: CanonicalId, _w: Dict[str, Any]) -> None:
            pass

        async def on_complete(_cid: CanonicalId) -> None:
            pass

        # Any canonical now: should land on least-loaded (leaf-c).
        q = _analyze_query()
        cid = CanonicalId("cid-overflow")
        wire = translate_query_to_wire(q, cid)
        await router.dispatch(cid, wire, q, on_response, on_complete)

        sent_counts = {url: len(s.sent) for url, s in sockets.items()}
        assert sent_counts["ws://leaf-c:3"] == 1, (
            f"least-loaded upstream should have received the dispatch; "
            f"got {sent_counts!r}"
        )
        assert sum(sent_counts.values()) == 1
