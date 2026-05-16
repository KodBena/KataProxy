"""
tests/diagnose_relay_coalescing_e2e.py — Multi-process RELAY coalescing
diagnostic against pre-existing upstream LEAFs.

Spawns a RELAY-under-test pointing at three pre-existing KataGo
endpoints (default ws://192.168.122.1:1235-1237; override via
`PROXY_TOPOLOGY_DIAG_UPSTREAMS`), opens two WebSocket clients to the
RELAY, issues identical analyze queries near-simultaneously, and
parses the RELAY's structured JSON log to assert the coalescing
contract end-to-end:

  - The hub emits exactly one `subscribe` and one `coalesce` event
    for our analyze queries (two subscribers, one in-flight slot).
  - The router emits exactly one `dispatch` event for our analyze
    queries (one upstream LEAF received the canonical, not two).

Issues a `clear_cache` broadcast before the test queries to neutralise
KataGo's per-LEAF cache as a confounder — per the plan note's §5
debugging-discipline section, a confounder should be named and
neutralised, not just hoped-against. Otherwise a second test run
could appear to "coalesce" merely because KataGo's cache served
both queries from the same upstream — distinct mechanism, same
wire-level outcome.

Run from the proxy directory (any cwd works; the script discovers
its own location):

    /home/bork/w/vdc/venvs/kataproxy/bin/python3 \\
        -m tests.diagnose_relay_coalescing_e2e

Or as a script. Exit 0 on PASS, 1 on FAIL — same convention as
`diagnose_phase{1,2,3}.py` and `diagnose_watchdog_*.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

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
_DEFAULT_N_CLIENTS = 2
_DEFAULT_VISITS = 50


def _upstreams_from_env() -> tuple[str, ...]:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_UPSTREAMS")
    if not raw:
        return _DEFAULT_UPSTREAMS
    return tuple(u.strip() for u in raw.split(",") if u.strip())


def _n_clients_from_env() -> int:
    """Number of simultaneous clients issuing the identical query.

    Default 2 is the minimum to observe coalescing. Setting higher
    (e.g. 50) exercises the hub's subscriber-list management and
    per-response fan-out at realistic scale; the assertion stays
    `1 subscribe + (N-1) coalesce + 1 dispatch` regardless of N
    because coalescing is N-independent at the hub."""
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_CLIENTS")
    if not raw:
        return _DEFAULT_N_CLIENTS
    n = int(raw)
    if n < 2:
        raise ValueError(
            f"PROXY_TOPOLOGY_DIAG_CLIENTS must be ≥2 to observe "
            f"coalescing; got {n}"
        )
    return n


def _visits_from_env() -> int:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_VISITS")
    return int(raw) if raw else _DEFAULT_VISITS


# ---------------------------------------------------------------------------
# Test queries
# ---------------------------------------------------------------------------


def _identical_analyze_query(client_id: str, visits: int) -> Dict[str, Any]:
    """An analyze query N clients can issue identically (modulo their
    distinct client-side `id` fields — the hub's coalescing operates
    on content_hash, not on the client `id`)."""
    return {
        "id": client_id,
        "action": "analyze",
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": [["B", "Q4"]],
        "analyzeTurns": [0],
        "maxVisits": visits,
    }


def _clear_cache_query() -> Dict[str, Any]:
    return {
        "id": f"diag-clear-{secrets.token_hex(4)}",
        "action": "clear_cache",
    }


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------


async def _run_client(
    url: str, query: Dict[str, Any], *, ready_event: asyncio.Event,
    go_event: asyncio.Event,
) -> List[Dict[str, Any]]:
    """Open a WebSocket to `url`, signal ready, wait for the launch
    signal, send the query, collect responses until isDuringSearch=False.

    The ready/go gate exists so both clients have their WebSocket
    sessions established before either sends — otherwise the first
    client's analyze could be in flight (and not yet coalesceable —
    actually the hub coalesces whenever the second subscribe happens
    while the first is still in-flight, which is from subscribe time
    to QUERY_COMPLETE; so timing matters less than I thought, but
    the gate still tightens the race so the test is deterministic
    rather than relying on the analyze being slow enough).
    """
    responses: List[Dict[str, Any]] = []
    async with websockets.connect(url, open_timeout=10) as ws:
        ready_event.set()
        await go_event.wait()
        await ws.send(json.dumps(query))
        async for raw in ws:
            data = json.loads(raw)
            responses.append(data)
            # Final response for this client's query.
            if data.get("isDuringSearch") is False:
                break
    return responses


async def _clear_cache(url: str) -> None:
    """Issue a clear_cache to the RELAY (broadcasts to all upstreams)."""
    async with websockets.connect(url, open_timeout=10) as ws:
        await ws.send(json.dumps(_clear_cache_query()))
        # clear_cache returns one response (the relay-fanout's
        # first-response-wins picks one upstream's ack).
        await asyncio.wait_for(ws.recv(), timeout=5.0)


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------


def _read_events(log_path: Path) -> List[Dict[str, Any]]:
    """Read the JSONL log; return parsed dicts. Tolerates trailing
    incomplete lines (the proxy may still be flushing at read time)."""
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
            # Partial last line during a flush; skip rather than fail.
            continue
    return events


def _events_for_action(
    events: List[Dict[str, Any]], event_name: str, action: str,
) -> List[Dict[str, Any]]:
    """Filter to events of a given type AND a given KataGo action."""
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
    print("RELAY coalescing diagnostic (Tier 3 — real upstream LEAFs)")
    print("=" * 72)

    upstream_urls = _upstreams_from_env()
    n_clients = _n_clients_from_env()
    visits = _visits_from_env()
    print(f"\nupstreams:  {upstream_urls}")
    print(f"clients:    {n_clients}")
    print(f"maxVisits:  {visits}")

    # Build the topology: N pre-existing LEAFs + a spawned RELAY.
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

    log_dir = Path(tempfile.mkdtemp(prefix="kataproxy-coalesce-"))
    print(f"log_dir: {log_dir}")
    runner = TopologyRunner(spec, log_dir=log_dir)

    await runner.start()
    print(f"RELAY listening on {runner.client_url}")

    try:
        # Step 1: clear KataGo's cache on all upstreams so a previous
        # run's response isn't served from cache (which would short-
        # circuit the test in an interesting but off-target way).
        print("\n--- Step 1: clear_cache broadcast to all upstreams ---")
        await _clear_cache(runner.client_url)
        # Brief settle for the broadcast events to land in the log.
        await asyncio.sleep(0.5)

        # Step 2: N clients issue identical analyze queries
        # near-simultaneously via a ready/go gate.
        print(
            f"\n--- Step 2: {n_clients} clients send identical "
            f"analyze queries ---"
        )
        ready_events = [asyncio.Event() for _ in range(n_clients)]
        go = asyncio.Event()

        async def launch() -> None:
            await asyncio.gather(*(e.wait() for e in ready_events))
            go.set()

        launch_task = asyncio.create_task(launch())
        client_tasks = [
            asyncio.create_task(
                _run_client(
                    runner.client_url,
                    _identical_analyze_query(f"client-{i:03d}", visits),
                    ready_event=ready_events[i], go_event=go,
                )
            )
            for i in range(n_clients)
        ]

        # Timeout scales loosely with N; 30s is the floor for small
        # N, plus 0.5s per additional client to cover WS-setup
        # overhead at scale (50 clients on a single loop adds real
        # connect-time).
        timeout = 30.0 + max(0, n_clients - 2) * 0.5
        all_responses = await asyncio.wait_for(
            asyncio.gather(*client_tasks),
            timeout=timeout,
        )
        await launch_task

        final_visits = [
            r[-1].get("rootInfo", {}).get("visits", "?")
            for r in all_responses
        ]
        # Distinct final-visit counts would mean different upstreams
        # answered different clients — a coalescing failure caught at
        # the response shape rather than at the dispatch event.
        unique_visits = set(final_visits)
        print(
            f"  all {n_clients} clients received final responses; "
            f"final-visit values: {sorted(unique_visits)}"
        )

        # Settle: give the proxy a moment to flush log records for the
        # analyze events (broadcast events from step 1 are already
        # there; we want to be sure step-2's analyze events have
        # landed).
        await asyncio.sleep(0.5)

        # Step 3: parse the RELAY's log and assert the coalescing
        # contract.
        print("\n--- Step 3: parse log and assert coalescing ---")
        log_path = log_dir / "relay.jsonl"
        events = _read_events(log_path)
        print(f"  parsed {len(events)} structured events from {log_path}")

        subscribes = _events_for_action(events, "subscribe", "ANALYZE")
        coalesces = _events_for_action(events, "coalesce", "ANALYZE")
        dispatches = _events_for_action(events, "dispatch", "ANALYZE")

        print(f"  subscribe(ANALYZE) events: {len(subscribes)}")
        print(f"  coalesce(ANALYZE)  events: {len(coalesces)}")
        print(f"  dispatch(ANALYZE)  events: {len(dispatches)}")

        # Contract:
        #   1 subscribe   — first client created the slot
        #   N-1 coalesces — every subsequent client joined the slot
        #   1 dispatch    — the single canonical went to one upstream
        expected_coalesces = n_clients - 1
        if len(subscribes) != 1:
            print(
                f"\n  FAIL: expected exactly 1 subscribe(ANALYZE); "
                f"got {len(subscribes)}. The first client didn't "
                f"create a fresh slot, or multiple clients created "
                f"slots that didn't share a content_hash."
            )
            return False
        if len(coalesces) != expected_coalesces:
            print(
                f"\n  FAIL: expected exactly {expected_coalesces} "
                f"coalesce(ANALYZE); got {len(coalesces)}. "
                f"Some clients didn't join the existing slot — likely "
                f"a hash-policy regression or a timing problem the "
                f"ready/go gate was supposed to prevent."
            )
            return False
        if len(dispatches) != 1:
            print(
                f"\n  FAIL: expected exactly 1 dispatch(ANALYZE); "
                f"got {len(dispatches)}. Coalescing should result in "
                f"a single upstream send, not {len(dispatches)}."
            )
            return False

        # Cross-check: subscribe, all coalesces, and dispatch should
        # share the same canonical_id.
        sub_cid = subscribes[0].get("cid")
        disp_cid = dispatches[0].get("cid")
        co_cids = {c.get("cid") for c in coalesces}
        if disp_cid != sub_cid:
            print(
                f"\n  FAIL: subscribe.cid={sub_cid!r} != "
                f"dispatch.cid={disp_cid!r}"
            )
            return False
        if co_cids != {sub_cid}:
            print(
                f"\n  FAIL: coalesce events carry mismatched cids: "
                f"subscribe.cid={sub_cid!r} coalesce.cids={co_cids!r}"
            )
            return False

        upstream_target = dispatches[0].get("upstream")
        print(
            f"\n  PASS: coalescing observed at N={n_clients}. "
            f"canonical={sub_cid!r} → upstream={upstream_target!r}; "
            f"{expected_coalesces} subscribers joined the slot."
        )
        return True

    finally:
        await runner.stop()
        # The temp dir stays for inspection; print path so the
        # operator knows where to look.
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
