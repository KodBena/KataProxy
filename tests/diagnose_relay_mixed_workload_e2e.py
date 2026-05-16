"""
tests/diagnose_relay_mixed_workload_e2e.py — Multi-process RELAY
mixed-workload diagnostic.

Drives a real-world-shaped workload through a spawned RELAY against
pre-existing KataGo endpoints, then aggregates a comprehensive set
of statistics on coalescing, dispatch distribution, load-aware
fallback, latency (per class), and throughput.

The workload deliberately exercises all three router/hub concerns
simultaneously (the "multiparadigm" shape the design discussion
called out as the realistic institutional case):

  - **Coalescing**: H hot positions × K clients each, fired as
    near-simultaneous bursts so the hub's content-hash collision is
    near-deterministic. Hot positions use slow (high-maxVisits)
    queries so the canonical stays in-flight long enough for all
    K clients to coalesce.
  - **Hash-ring distribution + load-aware fallback**: D distinct
    positions, fired at bounded concurrency. RELAY_MAX_LOAD is set
    low (default 2) so the load-aware fallback walk in
    `_select_upstream` actually fires under real concurrent load
    rather than only at unit-test scale.
  - **Asymmetric query cost**: a configurable fraction of distinct
    queries use slow (high-maxVisits) visits to create per-upstream
    busy-time asymmetry, which is what makes load-balancing
    decisions interesting in the first place.

The hot-bursts task and the distinct-flow task run concurrently
(via `asyncio.gather`), so the RELAY sees mixed traffic the way a
real institutional deployment would.

Run with the kataproxy venv:

    /home/bork/w/vdc/venvs/kataproxy/bin/python3 \\
        -m tests.diagnose_relay_mixed_workload_e2e

All sizes are env-var-parameterised; the defaults are tuned to
exercise the system meaningfully without overstaying their welcome
(see CONFIGURATION below). Override for heavier or lighter runs.

Exit 0 on PASS (workload completed, all contracts held), 1 on
FAIL (some contract violated — typically a missing dispatch for a
sent query, or per-upstream load exceeded the substrate's
reasonableness bound). The report is printed regardless, so even
on FAIL the operator sees what happened.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import statistics
import sys
import tempfile
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import websockets

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from router import HashRing  # noqa: E402

from tests.topology_runner import (  # noqa: E402
    NodeSpec,
    ProxyRole,
    TopologyRunner,
    TopologySpec,
    _allocate_free_port,
)


# ---------------------------------------------------------------------------
# CONFIGURATION (env-var-parameterised)
# ---------------------------------------------------------------------------


_DEFAULT_UPSTREAMS = (
    "ws://192.168.122.1:1235",
    "ws://192.168.122.1:1236",
    "ws://192.168.122.1:1237",
)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def _upstreams_from_env() -> tuple[str, ...]:
    raw = os.environ.get("PROXY_TOPOLOGY_DIAG_UPSTREAMS")
    if not raw:
        return _DEFAULT_UPSTREAMS
    return tuple(u.strip() for u in raw.split(",") if u.strip())


# Visit-distribution presets (visits, weight) — distinct queries
# sample from one of these (or a custom one via env). The "balanced"
# preset mimics a realistic Go-study workload: lots of quick "first
# look" queries, fewer deeper queries, occasional very-deep dives.
_VISIT_DIST_PRESETS: Dict[str, Tuple[Tuple[int, int], ...]] = {
    "uniform_fast": ((50, 1),),
    "fast_slow": ((50, 80), (500, 20)),
    "balanced": (
        (50, 50), (200, 30), (500, 15), (1500, 4), (5000, 1),
    ),
    "heavy": (
        (50, 30), (200, 30), (500, 20), (1500, 15), (5000, 5),
    ),
}


def _parse_visit_dist(raw: str) -> Tuple[Tuple[int, int], ...]:
    """Parse 'V1:W1,V2:W2,...' into ((V1,W1),(V2,W2),...)."""
    out: List[Tuple[int, int]] = []
    for part in raw.split(","):
        v, _, w = part.strip().partition(":")
        out.append((int(v), int(w)))
    return tuple(out)


@dataclass(frozen=True)
class Config:
    upstreams: tuple[str, ...]
    hot_positions: int                          # H
    clients_per_hot: int                        # K
    distinct_queries: int                       # D
    concurrency: int                            # max in-flight distinct queries
    max_load: int                               # RELAY_MAX_LOAD env override
    hot_visits: int                             # visits per hot query
    visit_dist: Tuple[Tuple[int, int], ...]    # (visits, weight) for distinct
    visit_dist_name: str                        # for reporting
    rng_seed: int                               # for reproducible shuffles

    @classmethod
    def from_env(cls) -> "Config":
        # Visit distribution: explicit > preset > default-preset.
        explicit = os.environ.get("PROXY_TOPOLOGY_DIAG_VISIT_DIST")
        preset_name = os.environ.get(
            "PROXY_TOPOLOGY_DIAG_VISIT_PRESET", "balanced",
        )
        if explicit:
            dist = _parse_visit_dist(explicit)
            dist_name = f"explicit({explicit})"
        else:
            if preset_name not in _VISIT_DIST_PRESETS:
                raise ValueError(
                    f"Unknown visit preset {preset_name!r}; "
                    f"available: {sorted(_VISIT_DIST_PRESETS)}"
                )
            dist = _VISIT_DIST_PRESETS[preset_name]
            dist_name = preset_name
        return cls(
            upstreams=_upstreams_from_env(),
            hot_positions=_env_int(
                "PROXY_TOPOLOGY_DIAG_HOT_POSITIONS", 5,
            ),
            clients_per_hot=_env_int(
                "PROXY_TOPOLOGY_DIAG_CLIENTS_PER_HOT", 10,
            ),
            distinct_queries=_env_int(
                "PROXY_TOPOLOGY_DIAG_DISTINCT", 150,
            ),
            concurrency=_env_int(
                "PROXY_TOPOLOGY_DIAG_CONCURRENCY", 20,
            ),
            max_load=_env_int(
                "PROXY_TOPOLOGY_DIAG_MAX_LOAD", 2,
            ),
            hot_visits=_env_int(
                "PROXY_TOPOLOGY_DIAG_HOT_VISITS", 500,
            ),
            visit_dist=dist,
            visit_dist_name=dist_name,
            rng_seed=_env_int(
                "PROXY_TOPOLOGY_DIAG_RNG_SEED", 42,
            ),
        )

    @property
    def mean_distinct_visits(self) -> float:
        total_w = sum(w for _, w in self.visit_dist)
        return sum(v * w for v, w in self.visit_dist) / total_w

    @property
    def expected_distinct_visits(self) -> int:
        return int(self.distinct_queries * self.mean_distinct_visits)

    @property
    def total_clients(self) -> int:
        return self.hot_positions * self.clients_per_hot + self.distinct_queries

    @property
    def expected_subscribes(self) -> int:
        """If coalescing works perfectly, every distinct content_hash
        gets exactly one subscribe event."""
        return self.hot_positions + self.distinct_queries

    @property
    def expected_coalesces(self) -> int:
        """Each hot position has (clients_per_hot - 1) coalesces if the
        timing works out. Distinct queries don't coalesce."""
        return self.hot_positions * (self.clients_per_hot - 1)


# ---------------------------------------------------------------------------
# Workload generation
# ---------------------------------------------------------------------------


_COLS = "ABCDEFGHJKLMNOPQRST"   # KataGo board columns (no I)
_BOARD = 19


def _coord(idx: int) -> str:
    return f"{_COLS[idx % _BOARD]}{(idx // _BOARD) + 1}"


def _hot_position_query(
    hot_idx: int, client_idx: int, visits: int,
) -> Dict[str, Any]:
    """Hot positions are first H board points; every client for a
    given hot_idx sends the SAME `moves`, so the hub's content_hash
    collides → coalescing."""
    return {
        "id": f"hot-{hot_idx:02d}-c{client_idx:02d}",
        "action": "analyze",
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": _BOARD,
        "boardYSize": _BOARD,
        "moves": [["B", _coord(hot_idx)]],
        "analyzeTurns": [0],
        "maxVisits": visits,
    }


def _distinct_position_query(seq: int, visits: int) -> Dict[str, Any]:
    """Distinct positions offset past the hot block so they never
    collide with a hot canonical's content_hash."""
    # Hot positions occupy 0..hot_positions-1; offset by 100 to leave
    # ample room even at large H. Then encode seq as a two-move shape
    # for guaranteed distinctness up to ~130k.
    base = 100 + seq
    first_idx = base % (_BOARD * _BOARD)
    second_seq = base // (_BOARD * _BOARD)
    moves = [["B", _coord(first_idx)]]
    if second_seq > 0:
        second_idx = second_seq % (_BOARD * _BOARD)
        if second_idx == first_idx:
            second_idx = (second_idx + 1) % (_BOARD * _BOARD)
        moves.append(["W", _coord(second_idx)])
    return {
        "id": f"distinct-{seq:04d}",
        "action": "analyze",
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": _BOARD,
        "boardYSize": _BOARD,
        "moves": moves,
        "analyzeTurns": [0],
        "maxVisits": visits,
    }


# ---------------------------------------------------------------------------
# Per-query result record
# ---------------------------------------------------------------------------


@dataclass
class QueryResult:
    client_id: str             # the wire-level client id we sent
    kind: str                  # "hot" or "distinct"
    requested_visits: int      # what the query asked for
    submit_t: float            # wall time at send
    final_t: float             # wall time at last response
    n_responses: int
    achieved_visits: Optional[int]   # what the final response reported
    error: Optional[str] = None

    @property
    def latency_ms(self) -> float:
        return (self.final_t - self.submit_t) * 1000.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "client_id": self.client_id,
            "kind": self.kind,
            "requested_visits": self.requested_visits,
            "submit_t": self.submit_t,
            "final_t": self.final_t,
            "latency_ms": self.latency_ms,
            "n_responses": self.n_responses,
            "achieved_visits": self.achieved_visits,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# WebSocket client driver
# ---------------------------------------------------------------------------


async def _drive_query(
    url: str, query: Dict[str, Any], kind: str, requested_visits: int,
) -> QueryResult:
    submit_t = time.monotonic()
    n_responses = 0
    achieved_visits: Optional[int] = None
    try:
        async with websockets.connect(url, open_timeout=10) as ws:
            await ws.send(json.dumps(query))
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=120.0)
                data = json.loads(raw)
                n_responses += 1
                if data.get("isDuringSearch") is False:
                    achieved_visits = data.get("rootInfo", {}).get("visits")
                    break
        return QueryResult(
            client_id=str(query["id"]),
            kind=kind,
            requested_visits=requested_visits,
            submit_t=submit_t,
            final_t=time.monotonic(),
            n_responses=n_responses,
            achieved_visits=achieved_visits,
        )
    except Exception as exc:
        return QueryResult(
            client_id=str(query["id"]),
            kind=kind,
            requested_visits=requested_visits,
            submit_t=submit_t,
            final_t=time.monotonic(),
            n_responses=n_responses,
            achieved_visits=None,
            error=f"{type(exc).__name__}: {exc}",
        )


async def _hot_bursts_task(
    url: str, cfg: Config,
) -> List[QueryResult]:
    """For each hot position, fire all `clients_per_hot` clients
    near-simultaneously (asyncio.gather), then proceed to the next
    hot position. Bursts are sequential between positions; within a
    burst, all clients race the same content_hash to the hub so
    coalescing is near-deterministic."""
    results: List[QueryResult] = []
    for hot_idx in range(cfg.hot_positions):
        # Hot queries all use `hot_visits` so the canonical stays
        # in-flight long enough for all clients in the burst to land
        # while it's still coalesceable.
        burst_queries = [
            _hot_position_query(hot_idx, c, cfg.hot_visits)
            for c in range(cfg.clients_per_hot)
        ]
        burst_results = await asyncio.gather(*(
            _drive_query(url, q, "hot", cfg.hot_visits)
            for q in burst_queries
        ))
        results.extend(burst_results)
    return results


def _sample_visits(rng: random.Random, dist: Tuple[Tuple[int, int], ...]) -> int:
    """Weighted-choice from a (visits, weight) discrete distribution."""
    visits_list = [v for v, _ in dist]
    weights = [w for _, w in dist]
    return rng.choices(visits_list, weights=weights, k=1)[0]


async def _distinct_flow_task(
    url: str, cfg: Config,
) -> List[QueryResult]:
    """Distinct queries at bounded concurrency. Visit count per query
    is sampled from `cfg.visit_dist`, deterministic per RNG seed so
    re-runs are reproducible."""
    rng = random.Random(cfg.rng_seed)
    visits_per_query: List[int] = [
        _sample_visits(rng, cfg.visit_dist)
        for _ in range(cfg.distinct_queries)
    ]

    sem = asyncio.Semaphore(cfg.concurrency)

    async def run_one(seq: int) -> QueryResult:
        async with sem:
            v = visits_per_query[seq]
            q = _distinct_position_query(seq, v)
            return await _drive_query(url, q, "distinct", v)

    return await asyncio.gather(*(
        run_one(i) for i in range(cfg.distinct_queries)
    ))


# ---------------------------------------------------------------------------
# Structured-log parsing
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


def _filter(
    events: List[Dict[str, Any]], event_name: str,
    action: Optional[str] = None,
) -> List[Dict[str, Any]]:
    out = [e for e in events if e.get("event") == event_name]
    if action is not None:
        out = [e for e in out if e.get("action") == action]
    return out


# ---------------------------------------------------------------------------
# Per-upstream peak in-flight tracking
# ---------------------------------------------------------------------------


def _peak_in_flight_per_upstream(
    dispatches: List[Dict[str, Any]],
    completes: List[Dict[str, Any]],
    cid_to_upstream: Dict[str, str],
) -> Dict[str, int]:
    """Reconstruct per-upstream in-flight count over time from
    dispatch (+1) and complete (-1) events; return the peak per
    upstream.

    Complete events lack `upstream`; we look it up from the
    cid→upstream map built from dispatches.
    """
    # (timestamp, upstream, delta)
    events: List[Tuple[str, str, int]] = []
    for d in dispatches:
        ts = d.get("ts", "")
        up = d.get("upstream")
        if up:
            events.append((ts, str(up), +1))
    for c in completes:
        cid = c.get("cid")
        if cid is None:
            continue
        up = cid_to_upstream.get(str(cid))
        if up is None:
            # Broadcast completes have no per-upstream tracking; skip.
            continue
        ts = c.get("ts", "")
        events.append((ts, up, -1))

    events.sort(key=lambda t: t[0])
    current: Dict[str, int] = defaultdict(int)
    peak: Dict[str, int] = defaultdict(int)
    for _, up, delta in events:
        current[up] += delta
        if current[up] > peak[up]:
            peak[up] = current[up]
    return dict(peak)


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * pct / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return sorted_values[int(k)]
    return (
        sorted_values[lo] * (hi - k)
        + sorted_values[hi] * (k - lo)
    )


def _latency_summary(results: List[QueryResult]) -> Dict[str, float]:
    latencies = sorted(r.latency_ms for r in results if r.error is None)
    if not latencies:
        return {"count": 0}
    return {
        "count": len(latencies),
        "p50": _percentile(latencies, 50),
        "p95": _percentile(latencies, 95),
        "p99": _percentile(latencies, 99),
        "max": latencies[-1],
        "mean": statistics.mean(latencies),
    }


def _format_lat(summary: Dict[str, float]) -> str:
    if summary["count"] == 0:
        return "  (no successful queries)"
    return (
        f"    n={summary['count']:>4}  "
        f"p50={summary['p50']:>6.1f}  "
        f"p95={summary['p95']:>6.1f}  "
        f"p99={summary['p99']:>6.1f}  "
        f"max={summary['max']:>6.1f}  "
        f"mean={summary['mean']:>6.1f}  (ms)"
    )


def _binomial_sigma(n: int, p: float) -> float:
    return math.sqrt(n * p * (1 - p))


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------


async def run_scenario() -> bool:
    cfg = Config.from_env()

    print()
    print("=" * 78)
    print(
        "RELAY mixed-workload diagnostic (Tier 3 — real upstream LEAFs, "
        "RELAY_MAX_LOAD override)"
    )
    print("=" * 78)
    print(f"\nupstreams:           {cfg.upstreams}")
    print(
        f"hot:                 {cfg.hot_positions} positions × "
        f"{cfg.clients_per_hot} clients = "
        f"{cfg.hot_positions * cfg.clients_per_hot} hot queries "
        f"(@{cfg.hot_visits}v each)"
    )
    print(
        f"distinct:            {cfg.distinct_queries} queries; "
        f"visit dist = {cfg.visit_dist_name}"
    )
    print(f"  visit composition: {cfg.visit_dist}")
    print(
        f"  mean distinct visits: {cfg.mean_distinct_visits:.1f}  "
        f"(→ expected distinct compute: "
        f"{cfg.expected_distinct_visits:,} visits)"
    )
    print(f"concurrency:         {cfg.concurrency}")
    print(f"RELAY_MAX_LOAD:      {cfg.max_load}")
    print(f"RNG seed:            {cfg.rng_seed}")
    print(f"total client queries: {cfg.total_clients}")

    # Build topology: pre-existing upstreams + spawned RELAY with the
    # max_load override.
    upstream_nodes = tuple(
        NodeSpec(
            label=f"leaf-{i}", role=ProxyRole.LEAF,
            pre_existing_url=url,
        )
        for i, url in enumerate(cfg.upstreams)
    )
    relay_node = NodeSpec(
        label="relay", role=ProxyRole.RELAY,
        upstreams=tuple(n.label for n in upstream_nodes),
        extra_env={"RELAY_MAX_LOAD": str(cfg.max_load)},
    )
    client_port = _allocate_free_port("127.0.0.1")
    spec = TopologySpec(
        nodes=upstream_nodes + (relay_node,),
        client_label="relay",
        client_port=client_port,
    )

    log_dir_override = os.environ.get("PROXY_TOPOLOGY_DIAG_LOG_DIR")
    if log_dir_override:
        log_dir = Path(log_dir_override)
        log_dir.mkdir(parents=True, exist_ok=True)
    else:
        log_dir = Path(tempfile.mkdtemp(prefix="kataproxy-mixed-"))
    print(f"log_dir:             {log_dir}")
    runner = TopologyRunner(spec, log_dir=log_dir)

    await runner.start()
    print(f"RELAY listening on   {runner.client_url}")

    success = True
    try:
        # Step 1: drive the mixed workload (hot bursts + distinct flow
        # in parallel).
        print(
            "\n--- Step 1: drive mixed workload "
            "(hot bursts || distinct flow) ---"
        )
        t0 = time.monotonic()
        hot_results, distinct_results = await asyncio.gather(
            _hot_bursts_task(runner.client_url, cfg),
            _distinct_flow_task(runner.client_url, cfg),
        )
        elapsed = time.monotonic() - t0
        all_results = hot_results + distinct_results
        print(
            f"  wall elapsed: {elapsed:.1f}s  "
            f"({cfg.total_clients} client queries)"
        )

        errors = [r for r in all_results if r.error is not None]
        if errors:
            print(f"  ERRORS: {len(errors)} queries failed")
            for e in errors[:5]:
                print(f"    {e.client_id}: {e.error}")
            if len(errors) > 5:
                print(f"    ... and {len(errors) - 5} more")

        # Settle: flush last events.
        await asyncio.sleep(0.5)

        # Step 2: parse log + aggregate stats.
        print("\n--- Step 2: aggregate statistics from structured log ---")
        log_path = log_dir / "relay.jsonl"
        events = _read_events(log_path)
        print(f"  parsed {len(events)} structured events from {log_path}")

        subscribes = _filter(events, "subscribe", "ANALYZE")
        coalesces = _filter(events, "coalesce", "ANALYZE")
        dispatches = _filter(events, "dispatch", "ANALYZE")
        completes = _filter(events, "complete")

        cid_to_upstream: Dict[str, str] = {
            str(d["cid"]): str(d["upstream"])
            for d in dispatches if d.get("cid") and d.get("upstream")
        }

        # COALESCING
        print("\nCoalescing")
        print(
            f"  subscribe events:     {len(subscribes):>4}  "
            f"(expected if perfect: {cfg.expected_subscribes})"
        )
        print(
            f"  coalesce events:      {len(coalesces):>4}  "
            f"(expected if perfect: {cfg.expected_coalesces})"
        )
        total_client_subs = len(subscribes) + len(coalesces)
        if total_client_subs > 0:
            coalesce_rate = len(coalesces) / total_client_subs
            print(
                f"  coalescing rate:      {coalesce_rate:.1%}  "
                f"(coalesces / (subscribes + coalesces))"
            )

        # Per-hot-position coalescing breakdown.
        hot_coalesces: Counter[str] = Counter()
        for c in coalesces:
            orig = c.get("orig", "")
            if isinstance(orig, str) and orig.startswith("hot-"):
                # Hot client IDs are "hot-NN-cMM"; group by "hot-NN".
                parts = orig.split("-")
                if len(parts) >= 2:
                    hot_coalesces[f"{parts[0]}-{parts[1]}"] += 1
        if cfg.hot_positions > 0:
            fully_coalesced = sum(
                1 for n in hot_coalesces.values()
                if n == cfg.clients_per_hot - 1
            )
            print(
                f"  hot positions fully coalesced: "
                f"{fully_coalesced}/{cfg.hot_positions}  "
                f"(all {cfg.clients_per_hot} clients shared canonical)"
            )

        # DISTRIBUTION
        print("\nDistribution (across upstreams)")
        per_upstream = Counter(d.get("upstream") for d in dispatches)
        total_disp = sum(per_upstream.values())
        mean = total_disp / len(cfg.upstreams) if cfg.upstreams else 0
        sigma = _binomial_sigma(total_disp, 1 / len(cfg.upstreams))
        print(
            f"  total dispatches:     {total_disp:>4}  "
            f"(expected mean per upstream: {mean:.1f}; σ {sigma:.2f})"
        )
        for url in cfg.upstreams:
            count = per_upstream.get(url, 0)
            share = count / total_disp if total_disp else 0
            z = (count - mean) / sigma if sigma > 0 else 0
            print(
                f"    {url}:  {count:>4}  ({share:.1%}, "
                f"{z:+.2f}σ from mean)"
            )

        # LOAD-AWARE FALLBACK
        print("\nLoad-aware fallback")
        # Local hash ring with default replicas to compute preferences.
        ring = HashRing(list(cfg.upstreams), replicas=150)
        fallback_count = 0
        no_preference = 0
        for d in dispatches:
            cid = d.get("cid")
            actual = d.get("upstream")
            if not cid or not actual:
                continue
            preference = ring.ordered_nodes_for(str(cid))
            if not preference:
                no_preference += 1
                continue
            if str(actual) != preference[0]:
                fallback_count += 1
        if total_disp > 0:
            fallback_rate = fallback_count / total_disp
            print(
                f"  fallback dispatches:  {fallback_count:>4}  "
                f"(dispatch.upstream != HashRing preference)"
            )
            print(
                f"  fallback rate:        {fallback_rate:.1%}  "
                f"(0% = no load fallback triggered; >0% confirms the "
                f"load-aware walk fired under this workload)"
            )

        peak = _peak_in_flight_per_upstream(
            dispatches, completes, cid_to_upstream,
        )
        print(
            f"  per-upstream peak in-flight (max_load={cfg.max_load}):"
        )
        for url in cfg.upstreams:
            print(f"    {url}:  {peak.get(url, 0)}")
        print(
            f"  (peak can exceed max_load only on the all-saturated "
            f"least-loaded fallback path)"
        )

        # LATENCY (bucketed by requested_visits since cost varies)
        print("\nLatency (client-side, ms; buckets are by requested visits)")
        print(f"  All:                  ")
        print(_format_lat(_latency_summary(all_results)))
        print(f"  Hot (coalesced):      ")
        print(_format_lat(_latency_summary(
            [r for r in all_results if r.kind == "hot"]
        )))
        for label, lo, hi in [
            ("Distinct quick (≤100v):  ", 0, 100),
            ("Distinct medium (≤500v): ", 101, 500),
            ("Distinct deep (≤2000v):  ", 501, 2000),
            ("Distinct very deep:      ", 2001, 10_000_000),
        ]:
            bucket = [
                r for r in all_results
                if r.kind == "distinct"
                and lo <= r.requested_visits <= hi
            ]
            if bucket:
                print(f"  {label}")
                print(_format_lat(_latency_summary(bucket)))

        # THROUGHPUT — visits/second is the right unit when query cost
        # is variable (queries/second misses the work asymmetry).
        print("\nThroughput")
        # Sum achieved visits across all completed queries. Hot
        # queries' visits are counted once per ACHIEVED canonical
        # (i.e., the coalesced run), not per client subscriber.
        achieved_per_cid: Dict[str, int] = {}
        # Match client orig_id → cid via subscribe + coalesce events,
        # then attribute the canonical's achieved visits (from the
        # client's response that was the actual subscriber) to that cid.
        orig_to_cid: Dict[str, str] = {}
        for ev in subscribes + coalesces:
            o = ev.get("orig")
            c = ev.get("cid")
            if o and c:
                orig_to_cid[str(o)] = str(c)
        for r in all_results:
            if r.achieved_visits is None:
                continue
            c = orig_to_cid.get(r.client_id)
            if c is None:
                continue
            # If multiple clients coalesced onto the same canonical
            # they each report the same achieved_visits; take the
            # max as the canonical's visit count (KataGo may have run
            # slightly fewer or more).
            achieved_per_cid[c] = max(
                achieved_per_cid.get(c, 0), r.achieved_visits,
            )
        total_visits = sum(achieved_per_cid.values())
        per_upstream_visits: Dict[str, int] = defaultdict(int)
        for c, v in achieved_per_cid.items():
            up = cid_to_upstream.get(c)
            if up:
                per_upstream_visits[up] += v

        vps_total = total_visits / elapsed if elapsed else 0
        qps_client = cfg.total_clients / elapsed if elapsed else 0
        print(
            f"  client queries:  {cfg.total_clients} in {elapsed:.1f}s "
            f"= {qps_client:.2f} client-qps  "
            f"(post-coalescing actual KataGo work below)"
        )
        print(
            f"  total visits:    {total_visits:,} in {elapsed:.1f}s "
            f"= {vps_total:,.0f} visits/sec  "
            f"(actual GPU work delivered)"
        )
        print("  per-upstream visits/sec (achieved):")
        for url in cfg.upstreams:
            v = per_upstream_visits.get(url, 0)
            vps = v / elapsed if elapsed else 0
            print(f"    {url}:  {v:>10,} visits  =  {vps:>7,.0f} v/s")

        # PASS/FAIL CHECKS
        print("\n--- Sanity checks ---")
        # No client queries lost.
        if errors:
            print(f"  FAIL: {len(errors)} client queries errored")
            success = False
        else:
            print(f"  ok: all {cfg.total_clients} client queries returned")

        # Hub accounting balances: (subscribes + coalesces) per ANALYZE
        # should equal total client analyze queries.
        if total_client_subs != cfg.total_clients:
            print(
                f"  WARN: subscribes + coalesces = {total_client_subs}, "
                f"but {cfg.total_clients} client queries were sent. "
                f"Difference may indicate dropped events or a hub "
                f"accounting gap."
            )
        else:
            print(
                f"  ok: subscribes + coalesces ({total_client_subs}) "
                f"matches client queries sent"
            )

        # Per dispatch, every subscribe should pair with a dispatch.
        if len(dispatches) != len(subscribes):
            print(
                f"  WARN: {len(subscribes)} subscribes vs "
                f"{len(dispatches)} dispatches. Each new slot should "
                f"trigger exactly one dispatch."
            )
        else:
            print(
                f"  ok: subscribes == dispatches ({len(subscribes)}); "
                f"every new canonical was dispatched once"
            )

        # JSON OUTPUT — write everything the plotting / analysis side
        # needs so a single run dir is self-contained.
        summary_path = log_dir / "summary.json"
        summary = {
            "config": {
                "upstreams": list(cfg.upstreams),
                "hot_positions": cfg.hot_positions,
                "clients_per_hot": cfg.clients_per_hot,
                "distinct_queries": cfg.distinct_queries,
                "concurrency": cfg.concurrency,
                "max_load": cfg.max_load,
                "hot_visits": cfg.hot_visits,
                "visit_dist": list(cfg.visit_dist),
                "visit_dist_name": cfg.visit_dist_name,
                "rng_seed": cfg.rng_seed,
            },
            "elapsed_sec": elapsed,
            "queries": [r.to_dict() for r in all_results],
            "events": {
                "subscribes": subscribes,
                "coalesces": coalesces,
                "dispatches": dispatches,
                "completes": completes,
            },
            "cid_to_upstream": cid_to_upstream,
            "summary_stats": {
                "subscribes": len(subscribes),
                "coalesces": len(coalesces),
                "dispatches": len(dispatches),
                "completes": len(completes),
                "coalesce_rate": (
                    len(coalesces) / total_client_subs
                    if total_client_subs else 0
                ),
                "fallback_count": fallback_count,
                "fallback_rate": (
                    fallback_count / total_disp if total_disp else 0
                ),
                "per_upstream_dispatch_count": dict(per_upstream),
                "per_upstream_peak_in_flight": peak,
                "total_visits_achieved": total_visits,
                "per_upstream_visits_achieved": dict(per_upstream_visits),
                "visits_per_sec_total": vps_total,
                "client_qps": qps_client,
            },
        }
        summary_path.write_text(json.dumps(summary, indent=2))
        print(f"\nsummary JSON written to: {summary_path}")

        return success

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
    print("=" * 78)
    print(f"  Result: {'PASS' if success else 'FAIL'}")
    print("=" * 78)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
