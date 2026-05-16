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
_DEFAULT_N_QUERIES = 12
_DEFAULT_CONCURRENCY = 1   # sequential
_DEFAULT_VISITS = 50


def _upstreams_from_env() -> tuple[str, ...]:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_UPSTREAMS")
    if not raw:
        return _DEFAULT_UPSTREAMS
    return tuple(u.strip() for u in raw.split(",") if u.strip())


def _n_queries_from_env() -> int:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_QUERIES")
    return int(raw) if raw else _DEFAULT_N_QUERIES


def _concurrency_from_env() -> int:
    """Maximum in-flight queries at any moment.

    Default 1 (sequential) for the cheap smoke run. Higher values
    bound by a semaphore — useful for heavier runs where the wall-
    clock would otherwise be dominated by per-query KataGo latency.
    Don't crank too high: each in-flight query is one slot of
    RelayRouter.max_load on the chosen upstream; saturating that
    forces the load-aware fallback, which still passes the
    distribution test but exercises a different branch than the
    pure hash-ring property."""
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_CONCURRENCY")
    n = int(raw) if raw else _DEFAULT_CONCURRENCY
    if n < 1:
        raise ValueError(
            f"PROXY_TOPOLOGY_DIAG_CONCURRENCY must be ≥1; got {n}"
        )
    return n


def _visits_from_env() -> int:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_VISITS")
    return int(raw) if raw else _DEFAULT_VISITS


# ---------------------------------------------------------------------------
# Distinct test queries — generator scales to ~130k queries
# ---------------------------------------------------------------------------


# KataGo's column vocabulary: 19 letters, skipping I (operator
# convention — easy to confuse with 1 / J on a board).
_COLS = "ABCDEFGHJKLMNOPQRST"
_BOARD_SIZE = 19
_FIRST_MOVE_SPACE = _BOARD_SIZE * _BOARD_SIZE  # 361


def _coord(idx: int) -> str:
    """Map 0..360 → board coord ('A1'..'T19')."""
    col = _COLS[idx % _BOARD_SIZE]
    row = (idx // _BOARD_SIZE) + 1
    return f"{col}{row}"


def _distinct_analyze_query(seq: int, visits: int) -> Dict[str, Any]:
    """An analyze query distinct from every other in this script's
    sequence, so the hub treats each as a separate canonical (no
    coalescing). The opening-move space is 361; for seq >= 361 we
    encode the overflow as a second move (W on the next-indexed
    coordinate), which gives ~130k distinct sequences before
    repetition."""
    first_idx = seq % _FIRST_MOVE_SPACE
    second_seq = (seq // _FIRST_MOVE_SPACE)
    first = _coord(first_idx)
    moves = [["B", first]]
    if second_seq > 0:
        second_idx = second_seq % _FIRST_MOVE_SPACE
        if second_idx == first_idx:
            # Same point would be illegal; bump by one.
            second_idx = (second_idx + 1) % _FIRST_MOVE_SPACE
        moves.append(["W", _coord(second_idx)])
    return {
        "id": f"client-{seq:04d}",
        "action": "analyze",
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": _BOARD_SIZE,
        "boardYSize": _BOARD_SIZE,
        "moves": moves,
        "analyzeTurns": [0],
        "maxVisits": visits,
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
    n_queries = _n_queries_from_env()
    concurrency = _concurrency_from_env()
    visits = _visits_from_env()
    print(f"\nupstreams:   {upstream_urls}")
    print(f"queries:     {n_queries} distinct")
    print(f"concurrency: {concurrency}")
    print(f"maxVisits:   {visits}")

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
        # Step 1: drive N distinct queries with bounded concurrency.
        # Concurrency=1 is sequential (good for low-N smoke runs and
        # readable operator output); higher concurrency bounds the
        # wall-clock when N is in the hundreds. Each in-flight query
        # consumes one slot of RelayRouter.max_load on its routed
        # upstream — too-high concurrency forces the load-aware
        # fallback (a different branch than what we're pinning here),
        # so the default stays conservative.
        print(
            f"\n--- Step 1: send {n_queries} distinct analyze queries "
            f"(concurrency {concurrency}) ---"
        )
        sem = asyncio.Semaphore(concurrency)
        completed = 0
        progress_every = max(1, n_queries // 20)  # ~20 progress lines

        async def run_one(seq: int) -> tuple[int, Dict[str, Any]]:
            nonlocal completed
            async with sem:
                query = _distinct_analyze_query(seq, visits)
                result = await _send_and_drain(runner.client_url, query)
                completed += 1
                if completed % progress_every == 0 or completed == n_queries:
                    rv = result.get("rootInfo", {}).get("visits", "?")
                    print(
                        f"  [{completed:>4}/{n_queries}] last: "
                        f"{query['id']!r} → visits={rv}"
                    )
                return seq, result

        t0 = asyncio.get_event_loop().time()
        await asyncio.gather(*(run_one(i) for i in range(n_queries)))
        elapsed = asyncio.get_event_loop().time() - t0
        print(f"  all queries complete in {elapsed:.1f}s")

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
        if len(subscribes) != n_queries:
            print(
                f"\n  FAIL: expected {n_queries} subscribe(ANALYZE); "
                f"got {len(subscribes)}. Some queries didn't reach the "
                f"hub or coalesced unexpectedly."
            )
            return False

        # Dispatches: should equal N, distributed across upstreams.
        dispatches = _events_for_action(events, "dispatch", "ANALYZE")
        if len(dispatches) != n_queries:
            print(
                f"\n  FAIL: expected {n_queries} dispatch(ANALYZE); "
                f"got {len(dispatches)}. Hub coalesced + router "
                f"dispatched don't agree on cardinality."
            )
            return False

        per_upstream = Counter(d.get("upstream") for d in dispatches)
        # Skew bound is scale-aware: tight at large N (where binomial
        # variance compresses), loose at small N (where 12 queries
        # can defensibly land 8/2/2 by chance). Bound chosen as
        # 1/3 + 5σ at the binomial std dev to stay well clear of
        # ordinary variance while still catching real skew.
        #
        # Cap at 0.75 so the bound is meaningful at small N (where
        # the formula's 5σ term blows past 1.0). At N=12 the cap
        # binds → 0.75. At N=100 the formula gives ~0.57 → used
        # directly. At N=500 ~0.43, at N=1000 ~0.40 — both tight
        # enough to catch real hash bias.
        n = n_queries
        sigma = (n * (1 / 3) * (2 / 3)) ** 0.5
        skew_bound = min(0.75, (n / 3 + 5 * sigma) / n)

        print(f"  per-upstream dispatch counts (expected mean: {n/3:.1f}):")
        for url in upstream_urls:
            count = per_upstream.get(url, 0)
            share = count / n if n else 0.0
            print(f"    {url}: {count} ({share:.1%})")
        print(
            f"  binomial σ: {sigma:.2f}; skew bound: {skew_bound:.1%} "
            f"(mean + 5σ)"
        )

        # Starvation check.
        for url in upstream_urls:
            if per_upstream.get(url, 0) == 0:
                print(
                    f"\n  FAIL: upstream {url!r} received 0 of "
                    f"{n} dispatches — hash ring is stuck."
                )
                return False

        max_share = max(per_upstream.values()) / n
        if max_share > skew_bound:
            top_url = max(per_upstream, key=lambda u: per_upstream[u])
            print(
                f"\n  FAIL: upstream {top_url!r} received "
                f"{max_share:.1%} of dispatches "
                f"({per_upstream[top_url]}/{n}) — exceeds skew bound "
                f"{skew_bound:.1%}. Hash-ring distribution looks "
                f"biased (or N is too small for the bound — try a "
                f"larger PROXY_TOPOLOGY_DIAG_QUERIES)."
            )
            return False

        print(
            f"\n  PASS: {n} queries distributed across "
            f"{len(upstream_urls)} upstreams; "
            f"max share = {max_share:.1%} (bound {skew_bound:.1%}), "
            f"no starvation."
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
