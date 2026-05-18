"""middleware/allocation.py — AllocationAlgorithm substrate (v1.0.25).

Per-round allocation algorithms for the Phase 3 information-theoretic
allocation arc. See `proxy/docs/roadmap-info-theoretic-allocation.md`
§3.3 (Protocol contract + the four curated algorithms).

An `AllocationAlgorithm` consumes a candidate set (the Phase 1
selector's worst-quantile slice, materialised as a list of `TurnView`),
a user-authored value function (`Callable[[TurnView], float]`,
higher=more valuable), a `VisitScalingModel` (predicts marginal info
gain from extra visits), and a per-round visit budget. It returns a
`dict[TurnIndex, int]` allocation summing to the budget (modulo integer
rounding).

Four curated implementations:

  - `greedy_eig` — sort by current EIG-per-visit; allocate one visit at
    a time to the max-EIG candidate; recompute. Deterministic; no
    exploration.
  - `knowledge_gradient` — incremental KG: each step picks the
    candidate whose visit-effect would most improve the argmax over
    the candidate-value space. Deterministic; exploration through
    "could-overtake" logic.
  - `thompson_sampling` — Gaussian-posterior Thompson sampling.
    Stochastic; per-query `ts_seed` exposed via `allocation_params`
    for reproducibility (§11.8 prescribed).
  - `ucb` — upper-confidence-bound with Beale-Welford-shifted
    exploration term `√(log T / (n(c) + 1))` (§11.6 prescribed).
    Deterministic given κ.

Closed registry for v1.0.25; user-authored allocation policies are
Phase 4 territory per the umbrella design note's §6.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import math
import random
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from katago import TurnIndex
    from middleware.adaptive_reevaluate import TurnView
    from middleware.visit_scaling import VisitScalingModel


# Type aliases for readability. The TYPE_CHECKING-only-ness applies
# at the qualified-name level; Python doesn't see these at runtime
# inside annotations after `from __future__ import annotations`.
ValueFn = Callable[["TurnView"], float]
Allocation = dict["TurnIndex", int]


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class AllocationAlgorithm(Protocol):
    """Allocates a fixed visit budget across a candidate set.

    `allocate` returns a dict `{turn_index: visits}` whose values sum
    to `budget_visits` (or as close as integer rounding allows).
    Candidates with zero allocation are absent from the returned dict
    — callers must treat absence as zero. `rng` is consumed only by
    stochastic algorithms (`thompson_sampling`); deterministic
    algorithms ignore it.
    """

    def allocate(
        self,
        candidates: list["TurnView"],
        value_fn: ValueFn,
        visit_scaling_model: "VisitScalingModel",
        budget_visits: int,
        rng: Optional[random.Random] = None,
    ) -> Allocation:
        ...


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_current_visits(turn: "TurnView") -> int:
    """Read `rootInfo.visits` from the turn's packet; fall back to 1.

    The visit-scaling model treats `current_visits = 0` as 1 (the NN-
    prior baseline of "one visit's worth of evidence"). The allocation
    algorithm uses the same convention so the model's gain estimates
    line up with the per-candidate visit accounting.
    """
    opaque = turn.packet.opaque
    if not isinstance(opaque, dict):
        return 1
    root = opaque.get("rootInfo")
    if not isinstance(root, dict):
        return 1
    v = root.get("visits")
    if isinstance(v, bool):
        return 1
    if isinstance(v, int) and v >= 1:
        return v
    if isinstance(v, float) and v >= 1:
        return int(v)
    return 1


def _filter_positive(alloc: dict["TurnIndex", int]) -> Allocation:
    """Drop zero / negative entries; the Allocation contract is positive-only."""
    return {t: v for t, v in alloc.items() if v > 0}


# ---------------------------------------------------------------------------
# greedy_eig
# ---------------------------------------------------------------------------


class GreedyEIGAlgorithm:
    """Greedy expected-information-gain allocation.

    At each step, allocate one visit to the candidate with the
    largest `value_fn(c) × scaling.expected_gain(c, V_current_c, 1)`.
    Update that candidate's running visit count; recompute. Repeat
    until budget exhausted.

    Deterministic; no exploration term. The natural baseline; when
    `value_fn` is constant and `scaling` is uniform across candidates,
    this reduces to round-robin allocation (which matches v1.0.24's
    uniform-extras shape).
    """

    def allocate(
        self,
        candidates: list["TurnView"],
        value_fn: ValueFn,
        visit_scaling_model: "VisitScalingModel",
        budget_visits: int,
        rng: Optional[random.Random] = None,
    ) -> Allocation:
        if budget_visits <= 0 or not candidates:
            return {}
        values: dict[Any, float] = {c.turn_index: value_fn(c) for c in candidates}
        base_visits: dict[Any, int] = {
            c.turn_index: _get_current_visits(c) for c in candidates
        }
        allocated: dict[Any, int] = {c.turn_index: 0 for c in candidates}
        for _ in range(budget_visits):
            best = max(
                candidates,
                key=lambda c: (
                    values[c.turn_index]
                    * visit_scaling_model.expected_gain(
                        c,
                        base_visits[c.turn_index] + allocated[c.turn_index],
                        1,
                    )
                ),
            )
            allocated[best.turn_index] += 1
        return _filter_positive(allocated)


# ---------------------------------------------------------------------------
# knowledge_gradient (incremental)
# ---------------------------------------------------------------------------


class KnowledgeGradientAlgorithm:
    """Incremental knowledge-gradient allocation.

    At each step, compute for each candidate `c`:

        KG(c) = max(max_before, μ(c) + visit-effect(c)) − max_before

    where `μ(c) = value_fn(c)`, `visit-effect(c) = scaling.expected_
    gain(c, V_current_c + allocated_c, 1)` (interpreted as the
    expected positive shift in `μ` from one more visit), and
    `max_before = max(μ(c') for c')`.

    Properties:
      - The current leader's KG equals its visit-effect (the lead
        grows by the gain magnitude).
      - A non-leader's KG is positive iff its gain would lift it
        past `max_before` — encoding "exploration toward a candidate
        that might overtake."
      - Tie-breaking: when KGs are equal (e.g. all zero on the first
        step before any visit-effect dominates), falls back to the
        greedy-EIG ranking.

    Deterministic; the exploration property emerges from the
    `could-overtake` logic, not from stochasticity.
    """

    def allocate(
        self,
        candidates: list["TurnView"],
        value_fn: ValueFn,
        visit_scaling_model: "VisitScalingModel",
        budget_visits: int,
        rng: Optional[random.Random] = None,
    ) -> Allocation:
        if budget_visits <= 0 or not candidates:
            return {}
        values: dict[Any, float] = {c.turn_index: value_fn(c) for c in candidates}
        base_visits: dict[Any, int] = {
            c.turn_index: _get_current_visits(c) for c in candidates
        }
        allocated: dict[Any, int] = {c.turn_index: 0 for c in candidates}

        for _ in range(budget_visits):
            max_before = max(values[c.turn_index] for c in candidates)

            def kg_score(c: "TurnView") -> tuple[float, float]:
                gain = visit_scaling_model.expected_gain(
                    c,
                    base_visits[c.turn_index] + allocated[c.turn_index],
                    1,
                )
                new_value = values[c.turn_index] + gain
                kg = max(new_value, max_before) - max_before
                # Tie-break by EIG for the all-zero-KG case.
                eig_tiebreak = values[c.turn_index] * gain
                return (kg, eig_tiebreak)

            best = max(candidates, key=kg_score)
            allocated[best.turn_index] += 1
        return _filter_positive(allocated)


# ---------------------------------------------------------------------------
# thompson_sampling
# ---------------------------------------------------------------------------


class ThompsonSamplingAlgorithm:
    """Gaussian-posterior Thompson sampling allocation.

    At each step:
      1. For each candidate `c`, sample
         `θ_c ~ N(μ(c), σ²(c))` where σ(c) is the visit-scaling
         model's per-visit gain magnitude for `c` at its current
         visit count (interpreted as posterior std).
      2. Allocate one visit to `argmax θ_c`.
      3. Repeat.

    Stochastic. `rng` controls the per-sample noise; tests pass a
    seeded `random.Random` for reproducibility. The substrate
    constructs the RNG from `allocation_params.ts_seed` at parse
    time when one is supplied; otherwise the algorithm consumes the
    caller-supplied `rng` or constructs a fresh non-seeded one.

    The Gaussian assumption is a simplifying choice: value functions
    can return any scalar, and the visit-scaling model's gain is
    interpreted as a 1-σ width (not literal variance). This is the
    standard pragmatic TS substrate; the algorithm's exploration-
    exploitation balance is empirical even if the math isn't
    rigorously Bayesian.
    """

    def __init__(self, *, default_rng: Optional[random.Random] = None) -> None:
        self._default_rng = default_rng

    def allocate(
        self,
        candidates: list["TurnView"],
        value_fn: ValueFn,
        visit_scaling_model: "VisitScalingModel",
        budget_visits: int,
        rng: Optional[random.Random] = None,
    ) -> Allocation:
        if budget_visits <= 0 or not candidates:
            return {}
        # Precedence: caller-supplied rng > algorithm's default rng >
        # a fresh non-seeded Random.
        active_rng = rng or self._default_rng or random.Random()
        values: dict[Any, float] = {c.turn_index: value_fn(c) for c in candidates}
        base_visits: dict[Any, int] = {
            c.turn_index: _get_current_visits(c) for c in candidates
        }
        allocated: dict[Any, int] = {c.turn_index: 0 for c in candidates}

        for _ in range(budget_visits):
            best: Optional["TurnView"] = None
            best_theta = -math.inf
            for c in candidates:
                sigma = visit_scaling_model.expected_gain(
                    c,
                    base_visits[c.turn_index] + allocated[c.turn_index],
                    1,
                )
                theta = active_rng.gauss(values[c.turn_index], max(sigma, 0.0))
                if theta > best_theta:
                    best_theta = theta
                    best = c
            assert best is not None  # candidates non-empty (early-return above)
            allocated[best.turn_index] += 1
        return _filter_positive(allocated)


# ---------------------------------------------------------------------------
# ucb
# ---------------------------------------------------------------------------


class UCBAlgorithm:
    """Upper-confidence-bound allocation with Beale-Welford shift.

    At each step:

        UCB(c) = μ(c) + κ × √(log(max(T, 1)) / (n(c) + 1))

    where `μ(c) = value_fn(c)`, `T` is the total visits-spent-this-
    round-so-far, and `n(c)` is the visits allocated to `c` this
    round. The `n(c) + 1` denominator (Beale-Welford shifted form,
    §11.6 prescribed) avoids the classic `n(c) = 0` div-by-zero on
    the first allocation step.

    The first step (T=0, treated as 1) has `log(1) = 0` so the
    exploration term vanishes; candidates rank by `μ` alone. From
    step 2 onward, the `log(T)` numerator grows and unallocated
    candidates (`n(c) = 0`) accumulate exploration bonus relative to
    already-allocated ones.

    Deterministic given `κ`. Default `κ = 1.0`; configurable via
    `allocation_params.ucb_kappa` at parse time.

    UCB does not consume the visit-scaling model — exploration is
    purely log-T-driven, not gain-driven. The model is required by
    the substrate's Protocol (other algorithms consume it) but UCB
    ignores it. This is documented at §3.4 of the roadmap.
    """

    def __init__(self, *, kappa: float = 1.0) -> None:
        self.kappa = kappa

    def allocate(
        self,
        candidates: list["TurnView"],
        value_fn: ValueFn,
        visit_scaling_model: "VisitScalingModel",
        budget_visits: int,
        rng: Optional[random.Random] = None,
    ) -> Allocation:
        if budget_visits <= 0 or not candidates:
            return {}
        values: dict[Any, float] = {c.turn_index: value_fn(c) for c in candidates}
        allocated: dict[Any, int] = {c.turn_index: 0 for c in candidates}

        for step in range(budget_visits):
            total = max(step, 1)  # Beale-Welford safety; log(1)=0.
            log_t = math.log(total)
            best = max(
                candidates,
                key=lambda c: (
                    values[c.turn_index]
                    + self.kappa * math.sqrt(log_t / (allocated[c.turn_index] + 1))
                ),
            )
            allocated[best.turn_index] += 1
        return _filter_positive(allocated)


# ---------------------------------------------------------------------------
# Curated registry + factory
# ---------------------------------------------------------------------------


# Closed set of algorithm names. The factory's branch on name produces
# the instance with the right parameter bindings; the registry below
# captures the "what names exist" question for error messages.

_REGISTERED_ALGORITHM_NAMES: frozenset[str] = frozenset({
    "greedy_eig",
    "knowledge_gradient",
    "thompson_sampling",
    "ucb",
})


_VALID_ALLOCATION_PARAM_FIELDS: dict[str, frozenset[str]] = {
    "greedy_eig": frozenset(),
    "knowledge_gradient": frozenset(),
    "thompson_sampling": frozenset({"ts_seed"}),
    "ucb": frozenset({"ucb_kappa"}),
}


def _parse_allocation_algorithm(cap_meta: dict[str, Any]) -> AllocationAlgorithm:
    """Resolve the named algorithm + its parameters from capability metadata.

    Reads `allocation_algorithm: str` (required) and `allocation_params:
    dict[str, Any]` (optional; algorithm-specific keys). Returns an
    instance bound with the parsed parameters.

    Raises `AdaptiveConfigurationError(code="allocation_invalid")` on:

      - Unknown algorithm name.
      - `allocation_params` not a dict.
      - Unknown parameter keys for the named algorithm.
      - Out-of-range / wrong-type parameter values.

    The cost-asymmetry calibration (§7) applies — silent fallback on
    parameter errors could burn a multi-round allocation arc on the
    wrong shape.
    """
    # Local import to avoid the module-load cycle with adaptive_reevaluate.
    from middleware.adaptive_reevaluate import AdaptiveConfigurationError

    name = cap_meta.get("allocation_algorithm")
    if not isinstance(name, str) or name not in _REGISTERED_ALGORITHM_NAMES:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "allocation_algorithm": name,
                "valid": sorted(_REGISTERED_ALGORITHM_NAMES),
            },
        )

    raw_params = cap_meta.get("allocation_params", {})
    if not isinstance(raw_params, dict):
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "allocation_params": raw_params,
                "expected": "object (dict)",
            },
        )

    valid_keys = _VALID_ALLOCATION_PARAM_FIELDS[name]
    unknown_keys = set(raw_params.keys()) - valid_keys
    if unknown_keys:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "allocation_algorithm": name,
                "unknown_params": sorted(unknown_keys),
                "valid_params": sorted(valid_keys),
            },
        )

    if name == "greedy_eig":
        return GreedyEIGAlgorithm()
    if name == "knowledge_gradient":
        return KnowledgeGradientAlgorithm()
    if name == "thompson_sampling":
        seed = raw_params.get("ts_seed")
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise AdaptiveConfigurationError(
                code="allocation_invalid",
                detail={
                    "ts_seed": seed,
                    "expected": "int or absent",
                },
            )
        default_rng = random.Random(seed) if seed is not None else None
        return ThompsonSamplingAlgorithm(default_rng=default_rng)
    if name == "ucb":
        kappa = raw_params.get("ucb_kappa", 1.0)
        if (
            isinstance(kappa, bool)
            or not isinstance(kappa, (int, float))
            or kappa <= 0
        ):
            raise AdaptiveConfigurationError(
                code="allocation_invalid",
                detail={
                    "ucb_kappa": kappa,
                    "expected": "positive number",
                },
            )
        return UCBAlgorithm(kappa=float(kappa))

    # Defensive: reachable only if _REGISTERED_ALGORITHM_NAMES drifts
    # from the branch set above. The membership check at the top
    # protects against this, but the static type-checker doesn't
    # know that.
    raise AdaptiveConfigurationError(
        code="allocation_invalid",
        detail={"allocation_algorithm": name, "internal": "no branch matched"},
    )


def _registered_algorithm_names() -> list[str]:
    return sorted(_REGISTERED_ALGORITHM_NAMES)


__all__ = [
    "AllocationAlgorithm",
    "Allocation",
    "ValueFn",
    "GreedyEIGAlgorithm",
    "KnowledgeGradientAlgorithm",
    "ThompsonSamplingAlgorithm",
    "UCBAlgorithm",
    "_REGISTERED_ALGORITHM_NAMES",
    "_VALID_ALLOCATION_PARAM_FIELDS",
    "_parse_allocation_algorithm",
    "_registered_algorithm_names",
]
