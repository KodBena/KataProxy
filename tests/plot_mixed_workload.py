"""
tests/plot_mixed_workload.py — Render publication-quality charts
from one or more mixed-workload diagnostic runs.

Consumes the `summary.json` files produced by
`diagnose_relay_mixed_workload_e2e.py` (one per run; the diagnostic
writes it into `log_dir`).

Two chart families:

  Headline (single run) — a 2×2 panel figure summarising one
  comprehensive mixed-workload run:
    1. Per-upstream in-flight load over time (the "smoothness"
       demonstration) — reconstructed from dispatch (+1) and
       complete (-1) events sorted by timestamp.
    2. Latency CDF by visit-bucket (the "responsiveness"
       demonstration).
    3. Dispatch distribution with binomial CI overlay + fallback
       fraction annotated (the "fairness" demonstration).
    4. Coalescing efficiency — subscribers per canonical histogram
       + total work saved (the "shared infrastructure"
       demonstration).

  Scaling sweep (multiple runs) — a 2-panel figure:
    1. Achieved total visits/sec vs concurrency.
    2. p50 / p95 / p99 latency vs concurrency.

Usage:

    # Headline only
    python -m tests.plot_mixed_workload \\
        --headline /tmp/kp-headline \\
        --output-dir /tmp/kp-charts

    # Headline + sweep
    python -m tests.plot_mixed_workload \\
        --headline /tmp/kp-headline \\
        --sweep /tmp/kp-sweep-c3 /tmp/kp-sweep-c6 /tmp/kp-sweep-c12 \\
                /tmp/kp-sweep-c24 /tmp/kp-sweep-c48 \\
        --output-dir /tmp/kp-charts

The output dir gets `headline.png` and (if --sweep is set) `sweep.png`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import seaborn as sns


# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------


sns.set_theme(style="whitegrid", context="paper", font_scale=1.05)

# A consistent palette so the same upstream gets the same colour
# across all panels. Operators learning to read these charts at a
# glance benefit from the consistency.
_UPSTREAM_PALETTE = sns.color_palette("Set2", n_colors=8)


def _color_for(idx: int) -> Any:
    return _UPSTREAM_PALETTE[idx % len(_UPSTREAM_PALETTE)]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "summary.json"
    if not p.exists():
        raise FileNotFoundError(
            f"{p} not found; did the diagnostic write it?"
        )
    # json.loads is typed as Any; cast preserves the structural
    # contract this function advertises (the diagnostic always
    # writes a top-level dict).
    return cast(Dict[str, Any], json.loads(p.read_text()))


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Derived series
# ---------------------------------------------------------------------------


def _in_flight_timeseries(
    summary: Dict[str, Any],
) -> Dict[str, List[Tuple[float, int]]]:
    """Reconstruct per-upstream in-flight count over time.

    Returns {upstream_url: [(t_relative_sec, count), ...]} where
    each entry is a step-function point: at time t the count
    becomes `count` and stays there until the next entry.

    Per-upstream timelines are independent (only that upstream's
    dispatches and completes affect its count).
    """
    cid_to_upstream: Dict[str, str] = summary["cid_to_upstream"]
    per_up: Dict[str, List[Tuple[datetime, int]]] = defaultdict(list)
    for d in summary["events"]["dispatches"]:
        ts = d.get("ts")
        up = d.get("upstream")
        if ts and up:
            per_up[str(up)].append((_parse_ts(ts), +1))
    # The lifecycle `complete` event fires per session_complete (once
    # per SUBSCRIBER), not per CANONICAL. For coalesced canonicals
    # that gives N decrement events for 1 dispatch — pushing the
    # in-flight count negative. Dedupe by cid (keep the first
    # complete per cid) to recover the canonical-completion shape.
    seen: set[str] = set()
    for c in summary["events"]["completes"]:
        ts = c.get("ts")
        cid = c.get("cid")
        if not ts or not cid or cid in seen:
            continue
        seen.add(cid)
        up = cid_to_upstream.get(str(cid))
        if up:
            per_up[up].append((_parse_ts(ts), -1))
    if not per_up:
        return {}
    # Global t0 = earliest event across all upstreams (so all series
    # share an origin).
    t0 = min(e[0] for evs in per_up.values() for e in evs)
    out: Dict[str, List[Tuple[float, int]]] = {}
    for url in summary["config"]["upstreams"]:
        evs = sorted(per_up.get(url, []), key=lambda e: e[0])
        timeline = [(0.0, 0)]
        count = 0
        for ts, delta in evs:
            count += delta
            timeline.append(((ts - t0).total_seconds(), count))
        out[url] = timeline
    return out


def _sample_step(
    timeline: List[Tuple[float, int]], sample_times: np.ndarray,
) -> np.ndarray:
    """Step-interpolate timeline at sample_times: at each sample t,
    take the value from the largest timeline entry with ts ≤ t."""
    out = np.zeros(len(sample_times), dtype=float)
    j = 0
    for i, t in enumerate(sample_times):
        while j + 1 < len(timeline) and timeline[j + 1][0] <= t:
            j += 1
        out[i] = timeline[j][1]
    return out


def _subscribers_per_canonical(
    summary: Dict[str, Any],
) -> List[int]:
    """For each canonical_id that had at least one subscribe, count
    1 (the subscribe itself) plus all subsequent coalesces onto it.
    Returns the distribution as a list of integers."""
    per_cid: Counter[str] = Counter()
    for s in summary["events"]["subscribes"]:
        cid = s.get("cid")
        if cid:
            per_cid[str(cid)] += 1
    for c in summary["events"]["coalesces"]:
        cid = c.get("cid")
        if cid:
            per_cid[str(cid)] += 1
    return list(per_cid.values())


# ---------------------------------------------------------------------------
# Headline figure
# ---------------------------------------------------------------------------


def render_headline(summary: Dict[str, Any], out_path: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    cfg = summary["config"]
    stats = summary["summary_stats"]

    # ---- Panel 1: per-upstream in-flight over time ----
    ax1 = axes[0, 0]
    series = _in_flight_timeseries(summary)
    elapsed = summary["elapsed_sec"]
    # Sample at ~400 evenly-spaced points across the run for a clean
    # line. Step-interpolation preserves the actual count at each
    # sample (no biased averaging).
    sample_times = np.linspace(0.0, elapsed, 600)
    # Per-upstream means for the legend annotation — these are the
    # number an operator actually cares about (is load balanced
    # across upstreams? compare these).
    mean_loads: Dict[str, float] = {}
    for idx, url in enumerate(cfg["upstreams"]):
        timeline = series.get(url, [])
        if len(timeline) < 2:
            continue
        sampled = _sample_step(timeline, sample_times)
        mean_loads[url] = float(sampled.mean())
        # Smoothed via box filter (~5% of run window) for readability;
        # raw is plotted underneath at low alpha so the actual
        # variance is still visible.
        window = max(5, len(sampled) // 25)
        smoothed = np.convolve(
            sampled, np.ones(window) / window, mode="same",
        )
        ax1.plot(
            sample_times, sampled,
            color=_color_for(idx), linewidth=0.6, alpha=0.25,
        )
        ax1.plot(
            sample_times, smoothed,
            color=_color_for(idx),
            label=f"{_short_url(url)}  (mean {mean_loads[url]:.1f})",
            linewidth=1.8, alpha=0.95,
        )
    ax1.axhline(
        cfg["max_load"], color="grey", linestyle=":", linewidth=1.2,
        label=f"RELAY_MAX_LOAD = {cfg['max_load']}",
    )
    # Annotate the steady-state band (5th-95th percentile of
    # in-flight across the run, averaged across upstreams) so the
    # operator can see at a glance where the system spends its time.
    all_samples = np.concatenate([
        _sample_step(series[u], sample_times)
        for u in cfg["upstreams"] if len(series.get(u, [])) >= 2
    ])
    if len(all_samples) > 0:
        p05, p95 = np.percentile(all_samples, [5, 95])
        ax1.axhspan(
            p05, p95, color="grey", alpha=0.10,
            label=f"steady-state 5–95% band ({p05:.0f}–{p95:.0f})",
        )
    ax1.set_xlabel("seconds")
    ax1.set_ylabel("in-flight queries (per upstream)")
    ax1.set_title(
        "Load smoothness — per-upstream in-flight over time",
        fontweight="bold",
    )
    ax1.legend(loc="upper right", fontsize=8, framealpha=0.85)
    ax1.set_ylim(bottom=0)

    # ---- Panel 2: Latency CDF by visit bucket ----
    ax2 = axes[0, 1]
    from typing import Callable
    BucketPred = Callable[[Dict[str, Any]], bool]
    buckets: List[Tuple[str, BucketPred]] = [
        ("hot (coalesced)", lambda r: bool(r["kind"] == "hot")),
        ("quick (≤100v)", lambda r: bool(r["kind"] == "distinct" and r["requested_visits"] <= 100)),
        ("medium (101-500v)", lambda r: bool(r["kind"] == "distinct" and 100 < r["requested_visits"] <= 500)),
        ("deep (501-2000v)", lambda r: bool(r["kind"] == "distinct" and 500 < r["requested_visits"] <= 2000)),
        ("very deep (>2000v)", lambda r: bool(r["kind"] == "distinct" and r["requested_visits"] > 2000)),
    ]
    # seaborn lacks type stubs; cast to a concrete colour-list type so
    # the downstream `ax2.plot(color=color, ...)` calls don't get
    # tainted with the call's Any return.
    bucket_colors = cast(
        List[Any], sns.color_palette("viridis", n_colors=len(buckets)),
    )
    for (label, pred), color in zip(buckets, bucket_colors):
        latencies = sorted(
            r["latency_ms"] for r in summary["queries"]
            if r["error"] is None and pred(r)
        )
        if not latencies:
            continue
        # CDF: y = (1..N)/N
        ys = np.arange(1, len(latencies) + 1) / len(latencies)
        ax2.plot(
            latencies, ys, color=color,
            label=f"{label}  (n={len(latencies)})",
            linewidth=1.8,
        )
    ax2.set_xlabel("client-observed latency (ms)")
    ax2.set_ylabel("cumulative fraction")
    ax2.set_title(
        "Latency CDF — by requested visits bucket",
        fontweight="bold",
    )
    ax2.set_xscale("log")
    ax2.legend(loc="lower right", fontsize=8, framealpha=0.85)
    ax2.grid(True, which="both", alpha=0.3)

    # ---- Panel 3: Dispatch distribution + fallback rate ----
    ax3 = axes[1, 0]
    upstreams = cfg["upstreams"]
    counts = [
        stats["per_upstream_dispatch_count"].get(u, 0) for u in upstreams
    ]
    total = sum(counts)
    visits = [
        stats["per_upstream_visits_achieved"].get(u, 0) for u in upstreams
    ]
    short_labels = [_short_url(u) for u in upstreams]
    x = np.arange(len(upstreams))
    width = 0.4
    colors = [_color_for(i) for i in range(len(upstreams))]

    # Two side-by-side bars per upstream: dispatch count (left), visits-achieved scaled (right)
    bars1 = ax3.bar(
        x - width / 2, counts, width,
        color=colors, edgecolor="black", linewidth=0.5,
        label="dispatches",
    )
    # Annotate each bar with the share %.
    for i, (count, bar) in enumerate(zip(counts, bars1)):
        share = count / total if total else 0
        ax3.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(counts) * 0.01,
            f"{share:.1%}",
            ha="center", va="bottom", fontsize=9, fontweight="bold",
        )
    # Reference line: ideal uniform share
    mean = total / len(upstreams) if upstreams else 0
    ax3.axhline(
        mean, color="grey", linestyle="--", linewidth=1,
        label=f"ideal uniform = {mean:.0f}",
    )
    # Binomial 95% CI band (mean ± 1.96σ)
    if total and len(upstreams) > 1:
        sigma = math.sqrt(total * (1 / len(upstreams)) * (1 - 1 / len(upstreams)))
        ax3.axhspan(
            mean - 1.96 * sigma, mean + 1.96 * sigma,
            color="grey", alpha=0.1, label=f"95% CI (±{1.96 * sigma:.1f})",
        )
    ax3.set_xticks(x)
    ax3.set_xticklabels(short_labels, rotation=0)
    ax3.set_ylabel("dispatch count")
    fb_rate = stats.get("fallback_rate", 0)
    ax3.set_title(
        f"Distribution — uniform under load (fallback rate: "
        f"{fb_rate:.1%} of dispatches)",
        fontweight="bold",
    )
    ax3.legend(loc="lower right", fontsize=8, framealpha=0.85)
    ax3.set_ylim(bottom=0)

    # ---- Panel 4: Coalescing efficiency ----
    ax4 = axes[1, 1]
    subs_per_canonical = _subscribers_per_canonical(summary)
    if subs_per_canonical:
        max_n = max(subs_per_canonical)
        bins = range(1, max_n + 2)
        ax4.hist(
            subs_per_canonical, bins=bins, align="left",
            color=_UPSTREAM_PALETTE[3], edgecolor="black", linewidth=0.5,
        )
    n_singletons = sum(1 for n in subs_per_canonical if n == 1)
    n_coalesced = sum(1 for n in subs_per_canonical if n > 1)
    total_clients = sum(subs_per_canonical)
    total_canonicals = len(subs_per_canonical)
    work_saved = total_clients - total_canonicals
    save_pct = work_saved / total_clients if total_clients else 0
    ax4.set_xlabel("subscribers per canonical")
    ax4.set_ylabel("count (number of canonicals)")
    ax4.set_title(
        f"Coalescing — {save_pct:.0%} of client queries shared "
        f"existing work ({work_saved} of {total_clients} subscribers "
        f"coalesced)",
        fontweight="bold",
    )
    ax4.set_yscale("symlog", linthresh=2)
    ax4.text(
        0.97, 0.95,
        f"canonicals: {total_canonicals}\n"
        f"  singletons: {n_singletons}\n"
        f"  coalesced:  {n_coalesced}\n"
        f"client queries: {total_clients}",
        transform=ax4.transAxes,
        ha="right", va="top",
        fontsize=9,
        bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.8},
    )

    # ---- Header / summary ----
    elapsed = summary["elapsed_sec"]
    vps = stats.get("visits_per_sec_total", 0)
    total_visits = stats.get("total_visits_achieved", 0)
    fig.suptitle(
        f"KataProxy RELAY under realistic mixed workload — "
        f"{cfg['hot_positions']} hot × {cfg['clients_per_hot']} + "
        f"{cfg['distinct_queries']} distinct  ·  "
        f"{total_visits:,} visits in {elapsed:.1f}s = "
        f"{vps:,.0f} visits/sec  ·  "
        f"3 LEAFs, RELAY_MAX_LOAD={cfg['max_load']}, "
        f"concurrency={cfg['concurrency']}",
        fontsize=12, fontweight="bold", y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def _short_url(url: str) -> str:
    """ws://192.168.122.1:1235 → LEAF :1235."""
    if "://" in url:
        url = url.split("://", 1)[1]
    if ":" in url:
        return f"LEAF :{url.rsplit(':', 1)[1]}"
    return url


# ---------------------------------------------------------------------------
# Sweep figure
# ---------------------------------------------------------------------------


def render_maxload_sweep(sweep_dirs: List[Path], out_path: Path) -> None:
    """Render the operator-facing `RELAY_MAX_LOAD` tuning chart.

    Four panels, each as a function of `max_load`:

      Top-left:  load-aware fallback rate — the fraction of
                 dispatches that landed on a non-preferred upstream
                 because the hash-ring preference was saturated.
                 Should fall as max_load increases (more capacity →
                 fewer fallback decisions).
      Top-right: per-upstream peak in-flight queries — shows how
                 much capacity each upstream actually used.
      Bottom-left: dispatch distribution — per-upstream share.
                 The system holds distribution near uniform across
                 max_load values; this panel confirms it.
      Bottom-right: latency percentiles — characterises how the
                 trade-off plays out for clients.
    """
    rows: List[Dict[str, Any]] = []
    for d in sweep_dirs:
        s = _load(d)
        latencies = sorted(
            r["latency_ms"] for r in s["queries"] if r["error"] is None
        )
        if not latencies:
            continue
        ts_total = sum(s["summary_stats"]["per_upstream_dispatch_count"].values())
        rows.append({
            "max_load": s["config"]["max_load"],
            "fallback_rate": s["summary_stats"]["fallback_rate"],
            "per_upstream_peak": s["summary_stats"]["per_upstream_peak_in_flight"],
            "per_upstream_count": s["summary_stats"]["per_upstream_dispatch_count"],
            "total_dispatches": ts_total,
            "p50": _percentile(latencies, 50),
            "p95": _percentile(latencies, 95),
            "p99": _percentile(latencies, 99),
            "vps": s["summary_stats"]["visits_per_sec_total"],
            "upstreams": s["config"]["upstreams"],
            "concurrency": s["config"]["concurrency"],
        })
    if not rows:
        print("(no max_load sweep data)")
        return
    rows.sort(key=lambda r: r["max_load"])
    max_loads = [r["max_load"] for r in rows]
    upstreams = rows[0]["upstreams"]
    concurrency = rows[0]["concurrency"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Top-left: fallback rate
    ax = axes[0, 0]
    fb = [r["fallback_rate"] * 100 for r in rows]
    ax.plot(
        max_loads, fb, marker="o", linewidth=2.2,
        color=_UPSTREAM_PALETTE[3],
    )
    for ml, v in zip(max_loads, fb):
        ax.text(
            ml, v + 2, f"{v:.1f}%",
            ha="center", va="bottom", fontsize=9,
        )
    ax.set_xlabel("RELAY_MAX_LOAD")
    ax.set_ylabel("load-aware fallback rate (%)")
    ax.set_title(
        "Fallback rate — how often the load-aware walk fires",
        fontweight="bold",
    )
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.set_xticks(max_loads)
    ax.set_xticklabels([str(m) for m in max_loads])
    ax.set_ylim(bottom=0)
    ax.grid(True, which="both", alpha=0.3)

    # Top-right: per-upstream peak in-flight
    ax = axes[0, 1]
    for idx, url in enumerate(upstreams):
        peaks = [r["per_upstream_peak"].get(url, 0) for r in rows]
        ax.plot(
            max_loads, peaks, marker="o", linewidth=1.8,
            color=_color_for(idx),
            label=_short_url(url),
        )
    # Reference: the max_load itself (i.e., the threshold the fallback respects).
    ax.plot(
        max_loads, max_loads,
        color="grey", linestyle=":", linewidth=1.2,
        label="max_load (admission threshold)",
    )
    ax.set_xlabel("RELAY_MAX_LOAD")
    ax.set_ylabel("peak in-flight (per upstream)")
    ax.set_title(
        "Peak per-upstream in-flight — capacity used",
        fontweight="bold",
    )
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.set_xticks(max_loads)
    ax.set_xticklabels([str(m) for m in max_loads])
    ax.legend(loc="upper left", fontsize=8, framealpha=0.85)
    ax.grid(True, which="both", alpha=0.3)

    # Bottom-left: per-upstream dispatch share
    ax = axes[1, 0]
    ideal_share = 100 / len(upstreams)
    for idx, url in enumerate(upstreams):
        shares = [
            r["per_upstream_count"].get(url, 0) / r["total_dispatches"] * 100
            for r in rows
        ]
        ax.plot(
            max_loads, shares, marker="o", linewidth=1.8,
            color=_color_for(idx),
            label=_short_url(url),
        )
    ax.axhline(
        ideal_share, color="grey", linestyle="--", linewidth=1.2,
        label=f"ideal uniform ({ideal_share:.1f}%)",
    )
    ax.set_xlabel("RELAY_MAX_LOAD")
    ax.set_ylabel("dispatch share (%)")
    ax.set_title(
        "Dispatch distribution — should stay near-uniform",
        fontweight="bold",
    )
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.set_xticks(max_loads)
    ax.set_xticklabels([str(m) for m in max_loads])
    ax.legend(loc="lower right", fontsize=8, framealpha=0.85)
    ax.grid(True, which="both", alpha=0.3)

    # Bottom-right: latency percentiles
    ax = axes[1, 1]
    for pct, color in [
        ("p50", _UPSTREAM_PALETTE[0]),
        ("p95", _UPSTREAM_PALETTE[1]),
        ("p99", _UPSTREAM_PALETTE[3]),
    ]:
        ys = [r[pct] for r in rows]
        ax.plot(
            max_loads, ys, marker="o", linewidth=1.8,
            color=color, label=pct,
        )
    ax.set_xlabel("RELAY_MAX_LOAD")
    ax.set_ylabel("latency (ms)")
    ax.set_title(
        "Latency percentiles — client-side observed",
        fontweight="bold",
    )
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.set_xticks(max_loads)
    ax.set_xticklabels([str(m) for m in max_loads])
    ax.legend(loc="upper right", fontsize=9, framealpha=0.85)
    ax.grid(True, which="both", alpha=0.3)

    fig.suptitle(
        f"KataProxy RELAY tuning — max_load sweep at "
        f"concurrency={concurrency}",
        fontsize=12, fontweight="bold", y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def render_sweep(sweep_dirs: List[Path], out_path: Path) -> None:
    rows: List[Dict[str, Any]] = []
    for d in sweep_dirs:
        s = _load(d)
        latencies = sorted(
            r["latency_ms"] for r in s["queries"] if r["error"] is None
        )
        if not latencies:
            continue
        rows.append({
            "concurrency": s["config"]["concurrency"],
            "vps": s["summary_stats"]["visits_per_sec_total"],
            "p50": _percentile(latencies, 50),
            "p95": _percentile(latencies, 95),
            "p99": _percentile(latencies, 99),
            "max_load": s["config"]["max_load"],
            "n_queries": len(latencies),
        })
    if not rows:
        print("(no sweep data)")
        return
    rows.sort(key=lambda r: r["concurrency"])
    concurrencies = [r["concurrency"] for r in rows]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left: throughput vs concurrency
    ax1 = axes[0]
    vps = [r["vps"] for r in rows]
    ax1.plot(
        concurrencies, vps,
        marker="o", linewidth=2, color=_UPSTREAM_PALETTE[2],
    )
    for c, v in zip(concurrencies, vps):
        ax1.text(
            c, v * 1.02, f"{v:,.0f}",
            ha="center", va="bottom", fontsize=9,
        )
    ax1.set_xlabel("client concurrency (max in-flight)")
    ax1.set_ylabel("achieved throughput (visits/sec)")
    ax1.set_title(
        "Throughput scaling — visits/sec vs client concurrency",
        fontweight="bold",
    )
    ax1.set_xscale("log", base=2)
    ax1.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax1.set_xticks(concurrencies)
    ax1.set_xticklabels([str(c) for c in concurrencies])
    ax1.grid(True, which="both", alpha=0.3)
    ax1.set_ylim(bottom=0)

    # Right: latency vs concurrency
    ax2 = axes[1]
    for pct, color in [
        ("p50", _UPSTREAM_PALETTE[0]),
        ("p95", _UPSTREAM_PALETTE[1]),
        ("p99", _UPSTREAM_PALETTE[3]),
    ]:
        ys = [r[pct] for r in rows]
        ax2.plot(
            concurrencies, ys, marker="o",
            linewidth=2, color=color, label=pct,
        )
    ax2.set_xlabel("client concurrency (max in-flight)")
    ax2.set_ylabel("latency (ms)")
    ax2.set_title(
        "Latency scaling — percentiles vs client concurrency",
        fontweight="bold",
    )
    ax2.set_xscale("log", base=2)
    ax2.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax2.set_xticks(concurrencies)
    ax2.set_xticklabels([str(c) for c in concurrencies])
    ax2.set_yscale("log")
    ax2.legend(loc="upper left", fontsize=10)
    ax2.grid(True, which="both", alpha=0.3)

    max_load = rows[0]["max_load"]
    fig.suptitle(
        f"KataProxy RELAY scaling — concurrency sweep at "
        f"RELAY_MAX_LOAD={max_load}",
        fontsize=12, fontweight="bold", y=1.02,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


# ---------------------------------------------------------------------------
# Utilities
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--headline", type=Path, default=None,
        help="run dir for the headline 2×2 figure",
    )
    parser.add_argument(
        "--sweep", type=Path, nargs="*", default=None,
        help="run dirs for the concurrency-sweep figure",
    )
    parser.add_argument(
        "--maxload-sweep", type=Path, nargs="*", default=None,
        help="run dirs for the max_load-sweep figure",
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True,
        help="directory to write chart PNGs into",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.headline:
        s = _load(args.headline)
        render_headline(s, args.output_dir / "headline.png")
    if args.sweep:
        render_sweep(list(args.sweep), args.output_dir / "sweep.png")
    if args.maxload_sweep:
        render_maxload_sweep(
            list(args.maxload_sweep),
            args.output_dir / "maxload-sweep.png",
        )
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
