"""
tests/diagnose_relay_load_distribution_e2e.py — Multi-process RELAY
hash-ring distribution diagnostic against pre-existing upstream LEAFs.

Spawns a RELAY-under-test pointing at three pre-existing KataGo
endpoints (default ws://192.168.122.1:1235-1237; override via
`PROXY_TOPOLOGY_DIAG_UPSTREAMS`), issues N distinct analyze queries
sequentially, and parses the RELAY's structured JSON log to assert
the distribution contract:

  - All N analyze queries produced N dispatch events (no coalescing,
    distinct content).
  - The dispatches spread across upstreams via the hash ring — no
    starvation (every upstream got ≥1 dispatch) and no strong skew
    (no upstream got >75% of total).

The companion Tier 2 test `test_relay_load_distribution.py` pins the
same property against mocked upstreams in-process; this script pins
it through the full multi-process wire surface, observing via
structured logs rather than mock-socket inspection.

Queries are deliberately distinct (different move per query) so
the hub does NOT coalesce them. KataGo's per-LEAF cache isn't a
confounder here — the queries are all different positions, no
cache reuse opportunity — so `clear_cache` isn't needed.

Run from the proxy directory:

    /home/bork/w/vdc/venvs/kataproxy/bin/python3 \\
        -m tests.diagnose_relay_load_distribution_e2e

Exit 0 on PASS, 1 on FAIL.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import websockets

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from tests.topology_runner import (  # noqa: E402
    NodeSpec,
    ProxyRole,
    TopologyRunner,
    TopologySpec,
    _allocate_free_port,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


_DEFAULT_UPSTREAMS = (
    "ws://192.168.122.1:1235",
    "ws://192.168.122.1:1236",
    "ws://192.168.122.1:1237",
)


def _upstreams_from_env() -> tuple[str, ...]:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_UPSTREAMS")
    if not raw:
        return _DEFAULT_UPSTREAMS
    return tuple(u.strip() for u in raw.split(",") if u.strip())


# 12 distinct queries over 3 upstreams gives the ring enough room to
# distribute; small enough that the test completes quickly even at
# real-KataGo cost per query (low maxVisits keeps each fast).
_N_QUERIES = 12


# ---------------------------------------------------------------------------
# Distinct test queries
# ---------------------------------------------------------------------------


_DISTINCT_FIRST_MOVES = [
    ("B", "Q4"),  ("B", "D4"),  ("B", "Q16"), ("B", "D16"),
    ("B", "K10"), ("B", "Q10"), ("B", "K4"),  ("B", "D10"),
    ("B", "K16"), ("B", "Q3"),  ("B", "D3"),  ("B", "Q17"),
]
assert len(_DISTINCT_FIRST_MOVES) >= _N_QUERIES


def _distinct_analyze_query(seq: int) -> Dict[str, Any]:
    """An analyze query distinct from every other in this script's
    sequence, so the hub treats each as a separate canonical
    (no coalescing)."""
    move = _DISTINCT_FIRST_MOVES[seq]
    return {
        "id": f"client-{seq:02d}",
        "action": "analyze",
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": [move],
        "analyzeTurns": [0],
        "maxVisits": 50,
    }


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------


async def _send_and_drain(
    url: str, query: Dict[str, Any], *, timeout: float = 30.0,
) -> Dict[str, Any]:
    """Open a fresh WebSocket, send `query`, drain until isDuringSearch
    is False, return the final response."""
    async with websockets.connect(url, open_timeout=10) as ws:
        await ws.send(json.dumps(query))
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(
                    f"no final response for {query['id']!r} within "
                    f"{timeout}s"
                )
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            data = json.loads(raw)
            if data.get("isDuringSearch") is False:
                return data


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------


def _read_events(log_path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not log_path.exists():
        return events
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _events_for_action(
    events: List[Dict[str, Any]], event_name: str, action: str,
) -> List[Dict[str, Any]]:
    return [
        e for e in events
        if e.get("event") == event_name and e.get("action") == action
    ]


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------


async def run_scenario() -> bool:
    print()
    print("=" * 72)
    print("RELAY load-distribution diagnostic (Tier 3 — real upstream LEAFs)")
    print("=" * 72)

    upstream_urls = _upstreams_from_env()
    print(f"\nupstreams: {upstream_urls}")
    print(f"queries:   {_N_QUERIES} distinct")

    upstream_nodes = tuple(
        NodeSpec(
            label=f"leaf-{i}", role=ProxyRole.LEAF,
            pre_existing_url=url,
        )
        for i, url in enumerate(upstream_urls)
    )
    relay_node = NodeSpec(
        label="relay", role=ProxyRole.RELAY,
        upstreams=tuple(n.label for n in upstream_nodes),
    )
    client_port = _allocate_free_port("127.0.0.1")
    spec = TopologySpec(
        nodes=upstream_nodes + (relay_node,),
        client_label="relay",
        client_port=client_port,
    )

    log_dir = Path(tempfile.mkdtemp(prefix="kataproxy-distrib-"))
    print(f"log_dir:   {log_dir}")
    runner = TopologyRunner(spec, log_dir=log_dir)

    await runner.start()
    print(f"RELAY listening on {runner.client_url}")

    try:
        # Step 1: drive N distinct queries sequentially. Sequential
        # rather than parallel: this test is about ring distribution
        # over a population of canonicals, not about concurrency, and
        # serialising keeps the operator log simpler to read.
        print(f"\n--- Step 1: send {_N_QUERIES} distinct analyze queries ---")
        for seq in range(_N_QUERIES):
            query = _distinct_analyze_query(seq)
            result = await _send_and_drain(runner.client_url, query)
            visits = result.get("rootInfo", {}).get("visits", "?")
            print(
                f"  [{seq + 1:>2}/{_N_QUERIES}] {query['id']!r} → "
                f"final visits={visits}"
            )

        # Settle: flush the last query's dispatch + forward events.
        await asyncio.sleep(0.5)

        # Step 2: parse the RELAY's log and assert distribution.
        print("\n--- Step 2: parse log and assert distribution ---")
        log_path = log_dir / "relay.jsonl"
        events = _read_events(log_path)
        print(f"  parsed {len(events)} structured events from {log_path}")

        # Coalesce safety check: no analyze should have coalesced
        # (queries are distinct). If any did, our distinct-set is
        # accidentally collapsing — bug in the test, not the system.
        coalesces = _events_for_action(events, "coalesce", "ANALYZE")
        if coalesces:
            print(
                f"\n  FAIL: {len(coalesces)} coalesce(ANALYZE) event(s) "
                f"observed — distinct queries are coalescing, meaning "
                f"the test fixtures are not as distinct as intended."
            )
            return False

        # Distinct subscribes: should equal N.
        subscribes = _events_for_action(events, "subscribe", "ANALYZE")
        if len(subscribes) != _N_QUERIES:
            print(
                f"\n  FAIL: expected {_N_QUERIES} subscribe(ANALYZE); "
                f"got {len(subscribes)}. Some queries didn't reach the "
                f"hub or coalesced unexpectedly."
            )
            return False

        # Dispatches: should equal N, distributed across upstreams.
        dispatches = _events_for_action(events, "dispatch", "ANALYZE")
        if len(dispatches) != _N_QUERIES:
            print(
                f"\n  FAIL: expected {_N_QUERIES} dispatch(ANALYZE); "
                f"got {len(dispatches)}. Hub coalesced + router "
                f"dispatched don't agree on cardinality."
            )
            return False

        per_upstream = Counter(d.get("upstream") for d in dispatches)
        print(f"  per-upstream dispatch counts:")
        for url in upstream_urls:
            count = per_upstream.get(url, 0)
            print(f"    {url}: {count}")

        # Starvation check.
        for url in upstream_urls:
            if per_upstream.get(url, 0) == 0:
                print(
                    f"\n  FAIL: upstream {url!r} received 0 of "
                    f"{_N_QUERIES} dispatches — hash ring is stuck."
                )
                return False

        # Skew check: no upstream above 75%. With 12 queries / 3
        # upstreams the expected share is ~33% per upstream; the
        # 75% bound is loose enough to tolerate variance and tight
        # enough to catch a ring-bypass regression.
        max_share = max(per_upstream.values()) / _N_QUERIES
        if max_share > 0.75:
            top_url = max(per_upstream, key=lambda u: per_upstream[u])
            print(
                f"\n  FAIL: upstream {top_url!r} received "
                f"{max_share:.1%} of dispatches "
                f"({per_upstream[top_url]}/{_N_QUERIES}) — distribution "
                f"is strongly skewed."
            )
            return False

        print(
            f"\n  PASS: {_N_QUERIES} queries distributed across "
            f"{len(upstream_urls)} upstreams; "
            f"max share = {max_share:.1%}, no starvation."
        )
        return True

    finally:
        await runner.stop()
        print(f"\nlog directory preserved at: {log_dir}")


def main() -> int:
    try:
        success = asyncio.run(run_scenario())
    except KeyboardInterrupt:
        print("\n(interrupted)")
        return 130
    print()
    print("=" * 72)
    print(f"  Result: {'PASS' if success else 'FAIL'}")
    print("=" * 72)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
