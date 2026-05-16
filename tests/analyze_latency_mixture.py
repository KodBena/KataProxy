"""
tests/analyze_latency_mixture.py — Quantitative analysis of latency
multimodality for one or more mixed-workload diagnostic runs.

Investigates the claim that observed latency distributions are
mixtures of per-regime sub-distributions, using:

  * Hartigans' dip test (via the `diptest` package) — tests the
    null hypothesis that a sample is drawn from a unimodal
    distribution; small p-value rejects unimodality.
  * Gaussian Mixture Models (via `sklearn.mixture`) fit in
    log-latency space (more Gaussian for queueing latencies),
    compared via BIC across k=1..4 components.
  * Time-window split — partitions a single run's samples by
    submit time and tests each window for unimodality.
  * Kolmogorov-Smirnov two-sample test — compares two empirical
    distributions for distributional equality.
  * Mixture-decomposition chart rendering — multi-panel figure
    showing the per-regime sub-distributions reconstructing the
    full-run aggregate.

Companion to `tests/plot_mixed_workload.py`. Where that script
focuses on operator-facing summary charts, this one focuses on
the statistical-validation analysis that supports the benchmark
write-up's mixture-of-regimes claim.

Usage:

    # Single-run analysis: dip test + GMM fit per single-visit-count
    # sub-population
    python -m tests.analyze_latency_mixture \\
        --run /tmp/headline-run

    # Two-run triangulation: headline + a clean steady-only run,
    # plus the mixture-decomposition chart
    python -m tests.analyze_latency_mixture \\
        --run /tmp/headline-run \\
        --steady-only /tmp/steady-only-run \\
        --visit-class 50 \\
        --chart /tmp/mixture-decomposition.png

The CLI reads `summary.json` from each run directory (written by
`diagnose_relay_mixed_workload_e2e.py`).

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import numpy as np


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load(run_dir: Path) -> Dict[str, Any]:
    p = run_dir / "summary.json"
    if not p.exists():
        raise FileNotFoundError(f"{p} not found")
    return cast(Dict[str, Any], json.loads(p.read_text()))


def _distinct_lats(
    summary: Dict[str, Any], visit_class: Optional[int] = None,
) -> np.ndarray:
    """Latencies (ms) for distinct queries; optionally filtered to a
    specific requested-visits value."""
    return np.array([
        q["latency_ms"] for q in summary["queries"]
        if q["error"] is None and q["kind"] == "distinct"
        and (visit_class is None or q["requested_visits"] == visit_class)
    ])


def _time_window_split(
    summary: Dict[str, Any], visit_class: int, t_lo_sec: float, t_hi_sec: float,
) -> np.ndarray:
    t0 = min(q["submit_t"] for q in summary["queries"])
    return np.array([
        q["latency_ms"] for q in summary["queries"]
        if q["error"] is None and q["kind"] == "distinct"
        and q["requested_visits"] == visit_class
        and t_lo_sec <= (q["submit_t"] - t0) < t_hi_sec
    ])


# ---------------------------------------------------------------------------
# Dip test
# ---------------------------------------------------------------------------


def dip_test(arr: np.ndarray) -> tuple[float, float]:
    """Hartigans' dip test. Returns (dip_statistic, p_value).

    Null hypothesis: arr is sampled from a unimodal distribution.
    Small p-value = evidence against unimodality.
    """
    import diptest  # lazy import; not all callers need this
    # diptest lacks type stubs; coerce the documented (float, float)
    # return so the function's typed contract holds.
    dip, p = diptest.diptest(arr)
    return float(dip), float(p)


# ---------------------------------------------------------------------------
# GMM fitting
# ---------------------------------------------------------------------------


def fit_gmm_log_space(
    lats: np.ndarray, k_range: range = range(1, 5), n_init: int = 20,
    random_state: int = 42,
) -> List[Dict[str, Any]]:
    """Fit GMM in log-latency space for each k; return BIC, AIC, and
    component params per fit. Caller compares BICs across k."""
    from sklearn.mixture import GaussianMixture
    log_lats = np.log(lats).reshape(-1, 1)
    results: List[Dict[str, Any]] = []
    for k in k_range:
        gmm = GaussianMixture(
            n_components=k, n_init=n_init, random_state=random_state,
            tol=1e-5, max_iter=500,
        )
        gmm.fit(log_lats)
        means = np.exp(gmm.means_.flatten())
        sigmas = np.sqrt(gmm.covariances_.flatten())
        weights = gmm.weights_
        order = np.argsort(means)
        results.append({
            "k": k,
            "bic": gmm.bic(log_lats),
            "aic": gmm.aic(log_lats),
            "means_ms": means[order].tolist(),
            "sigmas_log": sigmas[order].tolist(),
            "weights": weights[order].tolist(),
        })
    return results


# ---------------------------------------------------------------------------
# Per-run analysis (dip test + GMM at each single-visit-count)
# ---------------------------------------------------------------------------


def analyze_run(
    summary: Dict[str, Any], visit_classes: tuple[int, ...] = (50, 200, 500, 1500, 5000),
) -> None:
    """Run dip test + GMM fit per single-visit-count sub-population in a
    single run, printing a formatted table."""
    print()
    print(f"{'class':>6} {'n':>6} {'dip':>8} {'p_dip':>10} "
          f"{'best k':>8} {'ΔBIC(k=1)':>12} {'components (μ_ms × weight)':>40}")
    print("-" * 100)
    for v in visit_classes:
        lats = _distinct_lats(summary, visit_class=v)
        if len(lats) < 50:
            continue
        dip, p = dip_test(lats)
        fits = fit_gmm_log_space(lats)
        best = min(fits, key=lambda f: f["bic"])
        k1 = next(f for f in fits if f["k"] == 1)
        delta = k1["bic"] - best["bic"]
        comp_str = "  ".join(
            f"{m:.0f}×{w:.2f}"
            for m, w in zip(best["means_ms"], best["weights"])
        )
        print(f"{v:>6} {len(lats):>6} {dip:>8.4f} {p:>10.3g} "
              f"{best['k']:>8} {delta:>12.0f} {comp_str:>40}")


# ---------------------------------------------------------------------------
# Two-sample KS test
# ---------------------------------------------------------------------------


def ks_compare(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    """Kolmogorov-Smirnov two-sample test. Returns (statistic, p_value).

    Null: a and b are from the same distribution. Small p = different.
    """
    from scipy.stats import ks_2samp
    stat, p = ks_2samp(a, b)
    return float(stat), float(p)


# ---------------------------------------------------------------------------
# Mixture-decomposition chart
# ---------------------------------------------------------------------------


def render_mixture_chart(
    headline: Dict[str, Any], steady_only: Dict[str, Any],
    visit_class: int, out_path: Path,
    regime_split_sec: float = 100.0,
) -> None:
    """3-panel mixture-decomposition figure for a single visit-class.

    Panel 1: histograms of headline full / post-split steady / pre-split burst.
    Panel 2: empirical CDFs of all of the above + independent steady-only run
             + GMM(k=3) fit.
    Panel 3: GMM(k=3) component density curves over the full-run histogram.
    """
    import matplotlib.pyplot as plt
    import seaborn as sns
    from scipy.stats import norm

    sns.set_theme(style="whitegrid", context="paper", font_scale=1.05)
    pal = sns.color_palette("Set2", n_colors=8)

    t0 = min(q["submit_t"] for q in headline["queries"])
    hl_full = _distinct_lats(headline, visit_class=visit_class)
    hl_burst = np.array([
        q["latency_ms"] for q in headline["queries"]
        if q["error"] is None and q["kind"] == "distinct"
        and q["requested_visits"] == visit_class
        and (q["submit_t"] - t0) < regime_split_sec
    ])
    hl_steady = np.array([
        q["latency_ms"] for q in headline["queries"]
        if q["error"] is None and q["kind"] == "distinct"
        and q["requested_visits"] == visit_class
        and (q["submit_t"] - t0) >= regime_split_sec
    ])
    clean_steady = _distinct_lats(steady_only, visit_class=visit_class)

    # GMM k=3 in log space
    fits = fit_gmm_log_space(hl_full)
    fit3 = next(f for f in fits if f["k"] == 3)
    gmm_means = np.array(fit3["means_ms"])
    gmm_sigmas = np.array(fit3["sigmas_log"])
    gmm_weights = np.array(fit3["weights"])

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    bins = np.logspace(np.log10(max(50, hl_full.min() * 0.9)),
                       np.log10(hl_full.max() * 1.05), 60)

    # Panel 1: histograms
    ax = axes[0]
    ax.hist(hl_full, bins=bins, alpha=0.30, color=pal[7],
            density=True, label=f"headline FULL  (n={len(hl_full)})",
            edgecolor="black", linewidth=0.3)
    ax.hist(hl_steady, bins=bins, alpha=0.55, color=pal[2],
            density=True,
            label=f"headline post-{regime_split_sec:.0f}s steady  "
                  f"(n={len(hl_steady)})",
            edgecolor="black", linewidth=0.3)
    ax.hist(hl_burst, bins=bins, alpha=0.55, color=pal[3],
            density=True,
            label=f"headline 0-{regime_split_sec:.0f}s burst  "
                  f"(n={len(hl_burst)})",
            edgecolor="black", linewidth=0.3)
    ax.set_xscale("log")
    ax.set_xlabel("client-observed latency (ms, log)")
    ax.set_ylabel("density")
    ax.set_title(f"Headline {visit_class}v decomposes into two regimes",
                 fontweight="bold")
    ax.legend(loc="upper left", fontsize=8, framealpha=0.92)

    # Panel 2: CDFs + GMM fit
    ax = axes[1]

    def cdf_xy(a: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        s = np.sort(a)
        y = np.arange(1, len(s) + 1) / len(s)
        return s, y

    for arr, color, lbl in [
        (hl_full, pal[7], f"headline FULL  (n={len(hl_full)})"),
        (hl_steady, pal[2],
         f"headline post-{regime_split_sec:.0f}s  (n={len(hl_steady)})"),
        (clean_steady, pal[0],
         f"INDEPENDENT clean steady-only run  (n={len(clean_steady)})"),
        (hl_burst, pal[3],
         f"headline 0-{regime_split_sec:.0f}s  (n={len(hl_burst)})"),
    ]:
        x, y = cdf_xy(arr)
        ax.plot(x, y, color=color, linewidth=1.8, label=lbl)
    x_grid = np.logspace(np.log10(bins[0]), np.log10(bins[-1]), 500)
    cdf_mix = np.zeros_like(x_grid)
    for m, sig, w in zip(gmm_means, gmm_sigmas, gmm_weights):
        cdf_mix += w * norm.cdf(np.log(x_grid), loc=np.log(m), scale=sig)
    ax.plot(x_grid, cdf_mix, color="black", linewidth=1.5,
            linestyle="--", label="GMM(k=3) fit to headline full")
    ax.set_xscale("log")
    ax.set_xlabel("client-observed latency (ms, log)")
    ax.set_ylabel("cumulative fraction")
    ax.set_title("Empirical CDFs — independent measurements triangulate",
                 fontweight="bold")
    ax.legend(loc="lower right", fontsize=8, framealpha=0.92)

    # Panel 3: GMM component density curves
    ax = axes[2]
    ax.hist(hl_full, bins=bins, alpha=0.20, color="grey",
            density=True, edgecolor="black", linewidth=0.3)
    x_dense = np.logspace(np.log10(bins[0]), np.log10(bins[-1]), 1000)
    log_x = np.log(x_dense)
    total = np.zeros_like(x_dense)
    comp_colors = [pal[0], pal[2], pal[3]]
    for i, (m, sig, w) in enumerate(zip(gmm_means, gmm_sigmas, gmm_weights)):
        pdf = w * norm.pdf(log_x, loc=np.log(m), scale=sig) / x_dense
        ax.plot(x_dense, pdf, color=comp_colors[i % 3], linewidth=2,
                label=f"comp {i + 1}: μ={m:.0f}ms, σ_log={sig:.2f}, w={w:.3f}")
        total += pdf
    ax.plot(x_dense, total, color="black", linewidth=1.5,
            linestyle="--", label="sum of components")
    ax.set_xscale("log")
    ax.set_xlabel("client-observed latency (ms, log)")
    ax.set_ylabel("density")
    ax.set_title("GMM k=3 components vs observed histogram",
                 fontweight="bold")
    ax.legend(loc="upper left", fontsize=8, framealpha=0.92)

    fig.suptitle(
        f"Latency multimodality decomposed — {visit_class}v queries from "
        f"the headline run",
        fontsize=13, fontweight="bold", y=1.02,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"wrote {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True,
                        help="run dir to analyse (must contain summary.json)")
    parser.add_argument("--steady-only", type=Path, default=None,
                        help="optional second run dir (clean steady-state)"
                             " for triangulation + chart")
    parser.add_argument("--visit-class", type=int, default=50,
                        help="single-visit-count value to focus the chart on")
    parser.add_argument("--regime-split-sec", type=float, default=100.0,
                        help="time-window split point for the chart")
    parser.add_argument("--chart", type=Path, default=None,
                        help="if set, write mixture-decomposition chart here")
    args = parser.parse_args()

    headline = _load(args.run)
    print(f"=== per-class dip test + GMM fit ({args.run}) ===")
    analyze_run(headline)

    if args.steady_only:
        steady = _load(args.steady_only)
        print(f"\n=== triangulation: clean steady-only run ({args.steady_only}) ===")
        clean = _distinct_lats(steady, visit_class=args.visit_class)
        dip, p = dip_test(clean)
        print(f"  {args.visit_class}v: n={len(clean)}, mean={clean.mean():.0f}ms,"
              f" dip={dip:.4f}, p={p:.3g}")

        hl_steady = np.array([
            q["latency_ms"] for q in headline["queries"]
            if q["error"] is None and q["kind"] == "distinct"
            and q["requested_visits"] == args.visit_class
            and (q["submit_t"] - min(qq["submit_t"] for qq in headline["queries"])) >= args.regime_split_sec
        ])
        if len(hl_steady) > 0:
            stat, p_ks = ks_compare(clean, hl_steady)
            print(f"  KS test (clean vs headline post-{args.regime_split_sec:.0f}s):"
                  f" stat={stat:.4f}, p={p_ks:.3g}")

        if args.chart:
            args.chart.parent.mkdir(parents=True, exist_ok=True)
            render_mixture_chart(
                headline, steady, args.visit_class, args.chart,
                regime_split_sec=args.regime_split_sec,
            )

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
