# Information-theoretic allocation — design roadmap

- **Status:** `design-note: planned` (per umbrella ADR-0005 Rule 8
  doc-graph genre vocabulary)
- **Date:** 2026-05-18
- **Scope:** `proxy/middleware/adaptive_reevaluate.py` (Phase 3
  substrate + allocation-driven dispatch path),
  `proxy/registry_interpreter.py` (value-function binding accessor).
  Builds on the v1.0.24 multi-round / budget substrate and the
  v1.0.23 selector-pluggability substrate.
- **Origin:** Lands Phase 3 of the umbrella's adaptive-widening
  design note (`LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`)
  — the principled information-theoretic allocation arc. The umbrella
  note's §6 names three plug points (visit-scaling model, value
  function, allocation algorithm) and prescribes a curated set of
  algorithms with `registry_interpreter` as the escape hatch for
  user-authored policies in Phase 4.
- **Authoritative for the `feat/info-theoretic-allocation` branch;**
  superseded by the v1.0.25 release notes once tagged.

---

## TL;DR

This arc widens `adaptive_reevaluate` from a worst-quantile +
uniform-extra-visits dispatch into an **acquisition-function-
driven allocation** over a candidate set. Three plug points
collaborate to produce a per-turn visit allocation `dict[TurnIndex,
int]` each round; the coroutine spawns N parallel deeper queries
(one per candidate, each with its allocated visit budget); the
v1.0.24 finalization stage emits each turn's authoritative as
before.

The three plug points:

1. **`VisitScalingModel`** — predicts the expected information
   gain from adding V visits to a given per-turn state.
   Curated registry of named models (`monte_carlo_sqrt` —
   the canonical `1/√V` variance scaling; `diminishing_returns_log`
   — logarithmic), pluggable via wire `visit_scaling_model: str`.
   No calibrated empirical default; the naive analytic baselines
   are useful for the substrate to exercise, calibrated models
   land in a follow-on research arc.

2. **Value function** — per-turn scalar measuring "how valuable
   would clarifying this turn's uncertainty be" to the user/research
   workflow. Authored as an expression bound to `value_fn` in
   `analysis_config.bindings`, same substrate as v1.0.23's
   selector bindings. No hardcoded default — user must supply,
   following v1.0.23's turn-axis selector discipline (explicit
   semantic intent > silent fallback).

3. **`AllocationAlgorithm`** — curated set of four bandit/BO
   primitives that consume `(candidates, value_fn,
   visit_scaling_model, budget_visits)` and produce
   `dict[TurnIndex, int]` allocations:
   - `greedy_eig` — sort by expected information gain per visit;
     greedy allocation. Deterministic; no exploration.
   - `knowledge_gradient` — KG-style: for each candidate, compute
     expected improvement in `max value_fn` from allocating
     V visits to it. Pick max-KG repeatedly. Gaussian-posterior
     assumption.
   - `thompson_sampling` — sample from each candidate's posterior;
     allocate one visit to the sampled-max; repeat. Stochastic;
     natural exploration.
   - `ucb` — upper-confidence-bound; canonical bandit shape.
     Deterministic; balances exploitation and exploration.

Plus:

- A **sixth `AdaptiveConfigurationError` code**: `allocation_invalid`,
  same fail-loud calibration as v1.0.23/v1.0.24's prior five.
- An **allocation-driven dispatch path** in the multi-round
  coroutine, conditional on `allocation_algorithm` being named in
  capability metadata. When absent, v1.0.24's worst-quantile +
  uniform-visits dispatch holds — Phase 3 is **opt-in**, not the
  new default.
- **`ctx.parallel(*queries)`** integration: each round now spawns
  N sub-queries in parallel instead of v1.0.24's one bundled
  deeper query. KataGo's analyze action has no per-turn maxVisits
  field, so per-turn allocation requires per-turn spawning.

Phases 4 (user-authored adaptation policies) and the empirical
visit-scaling-model calibration are out of scope for v1.0.25.
Phase 4 extends `RegistryInterpreter` to program-shaped bindings;
the calibrated visit-scaling model is a research arc that lands
as a `VisitScalingModel` implementation in a follow-on PR (or as
an out-of-tree plugin once the substrate accepts external
registrations).

Pattern parallels v1.0.24 in shape (focused multi-commit arc on
a feature branch) but is bigger in substrate surface — three new
Protocol classes, a curated four-algorithm set with non-trivial
math, a new dispatch path. The wire shape gains four
capability-metadata fields. Legacy clients (no
`allocation_algorithm` field) get v1.0.24's behaviour unchanged.

---

## 1. The current state (post-v1.0.24)

The Phase-2 coroutine's per-round dispatch is uniform:

```python
# Per-round dispatch (v1.0.24, simplified)
deepen, worst_pairs, worst_value = _dispatch_deepening_round(
    finals, state, cap_meta, analysis_config, window_size, all_turns,
)
if not deepen:
    break
deeper = _build_deeper_query(parent, sorted(deepen), budget.visits_for_round())
async for resp in ctx.spawn(deeper):
    state.observe(resp)
    yield replace(resp, is_during_search=True)
state.record_round(...)
```

`deepen: set[TurnIndex]` is the worst-set (after window expansion
on move-axis). The deeper query carries the entire worst-set under
a single `maxVisits = original_maxVisits + extra_visits`. Every
deepened turn gets the same per-turn visit budget regardless of
how much that turn benefits from extra visits and how much the
user cares about that turn's uncertainty being resolved.

This is the "worst-quantile + uniform extras" shape v1.0.23
established and v1.0.24 wrapped in a multi-round loop. It works
when:

- Every worst-set member is roughly equally valuable to clarify.
- Visit-scaling is roughly uniform across worst-set members.
- The user has no per-turn information-need preference.

These assumptions hold for some workflows (a casual review of a
recent game) but break for others (autonomous SR loop with a
fixed GPU-minutes budget per card, where high-leverage turns
deserve more compute than tail-end turns).

The umbrella design note's §6 names this as the Phase 3 arc:
"each potential deepening has a *value* (expected information
gain); the budget allocation is a constrained-optimization /
multi-armed-bandit problem; the selector 'worst-quantile' rule
is a crude proxy for the underlying acquisition function."

This roadmap operationalises that arc as a substrate plus four
algorithms, with the substrate composing with Phase 2's
multi-round loop and Phase 1's selector pluggability.

---

## 2. Why this exists

Three orthogonal concerns the v1.0.24 substrate does not
distinguish:

1. **Which turns are candidates for deepening?** v1.0.24 uses
   the selector + selection-policy substrate (worst-quantile,
   per-color quantile, top-k, etc.). This identifies the
   *candidate set*.

2. **How valuable would resolving each candidate's uncertainty
   be?** v1.0.24 treats every candidate identically (uniform
   `extra_visits`). Phase 3 introduces a *value function* —
   per-turn scalar measuring user-side priority for clarifying
   that turn.

3. **How much does adding V visits actually reduce a turn's
   uncertainty?** v1.0.24 doesn't model this — every candidate
   gets the same V. Phase 3 introduces a *visit-scaling model*
   — empirical/analytic prediction of expected information gain
   per visit, per turn.

The Phase 3 *acquisition function* composes the value function
and the visit-scaling model: roughly,

```
EIG(turn, V_extra) ≈ value_fn(turn) × visit_scaling_model.gain(turn, V_extra)
```

(The exact composition depends on the algorithm — KG and TS
have their own forms; greedy_eig is the closest match to the
product form above.)

The *allocation algorithm* solves the constrained optimization:
given a candidate set, a value function, a visit-scaling model,
and a per-round visit budget, produce an allocation
`dict[TurnIndex, int]` that maximises (or stochastically
approximates) total expected information gain.

This decomposition cleanly separates:

- **What the user wants clarified** (value function — authored).
- **How KataGo's visit-scaling behaves** (visit-scaling model —
  pluggable, calibration is research).
- **How to spend a finite budget rationally** (allocation
  algorithm — curated bandit primitives).

The three plug points are independent — a user with no preference
between algorithms can pick `greedy_eig` and focus on authoring
the value function; a user who wants Bayesian exploration picks
`thompson_sampling`; a researcher calibrating KataGo's visit-
scaling provides their own `VisitScalingModel` and uses
`greedy_eig` to drive it.

---

## 3. Substrate design

### 3.1 `VisitScalingModel`

```python
class VisitScalingModel(Protocol):
    """Predicts expected information gain from adding visits to a turn.

    Implementations are free to consume any per-turn state via
    the `TurnView` argument (raw policy logits, score variance,
    LCB spread, etc.). The substrate accepts pluggable models;
    calibrated models against KataGo data ship as their own arc.
    """

    def expected_gain(
        self,
        turn: TurnView,
        current_visits: int,
        extra_visits: int,
    ) -> float:
        """Expected info gain from adding extra_visits to a turn
        that has already accumulated current_visits.

        Convention: higher = more gain. Units are arbitrary as
        long as they're comparable across turns within a single
        round (the allocation algorithm consumes ratios, not
        absolute magnitudes)."""
```

Curated registry:

- **`monte_carlo_sqrt`** — Monte Carlo variance scaling.
  Standard deviation of an MC estimator scales as `1/√N`, so
  the variance reduction from adding `V_extra` visits to a turn
  with `V_current` current visits is:

  ```
  gain(V_current, V_extra) = 1/√V_current − 1/√(V_current + V_extra)
  ```

  The canonical analytic baseline; appropriate when no
  calibrated model is available.

- **`diminishing_returns_log`** — Logarithmic. Models the
  empirically-observed property that successive doublings of
  visits yield roughly constant utility improvements (up to a
  saturation point). Form:

  ```
  gain(V_current, V_extra) = log(1 + V_extra / max(V_current, 1))
  ```

  Useful for sanity-checking algorithms against a
  qualitatively-different visit-scaling shape than
  `monte_carlo_sqrt`.

Both are stateless; the registry is a `dict[str,
VisitScalingModel]` constant. Pluggable models from external
callers (Phase 4 territory) extend the registry via a future
registration API; v1.0.25 ships the closed set.

`current_visits` comes from the turn's `rootInfo.visits`
field if present, else defaults to the parent query's
`maxVisits` (the visit budget KataGo finished on at the original
analysis). The `TurnView` argument carries the per-turn state
so model authors can read `rootInfo`, deltas, policy entropy,
etc., — same access pattern as v1.0.23's selector bindings.

### 3.2 Value function binding

The value function is authored as an expression in
`analysis_config.symbols` and bound under `value_fn` in
`analysis_config.bindings`. Same substrate as v1.0.23's
selector bindings; `RegistryInterpreter` gains a
`get_value_fn()` accessor returning `Optional[Callable[[TurnView], float]]`.

Convention: **higher = more valuable**. The user authors the
expression to score per-turn priority for the workflow at hand.
Examples:

```yaml
# Author 1: "I care about turns near forks in score trajectory."
bindings:
  value_fn: score_variance
symbols:
  score_variance: |
    var([m.scoreLead for m in extra.before.moveInfos[:5]])

# Author 2: "I want to clarify positions with ambiguous policy."
bindings:
  value_fn: policy_entropy
symbols:
  policy_entropy: |
    entropy([m.prior for m in extra.before.moveInfos])

# Author 3: "Pure exploration — every candidate equal-weighted."
bindings:
  value_fn: constant_one
symbols:
  constant_one: 1.0
```

The binding is **required** when `allocation_algorithm` is named.
Phase 3 is opt-in; engaging the allocation path without a value
function refuses with `AdaptiveConfigurationError(code=
"allocation_invalid")` (the symmetric discipline to v1.0.23's
turn-axis requirement).

The value function operates on a `TurnView` (v1.0.23's per-turn
view dataclass). The move-axis is not natively supported by
Phase 3 — allocation is over turns (KataGo allocates visits to
positions, not to move transitions). Move-axis value functions
would require a turn-axis aggregation step (e.g., sum value
across both endpoints of a move); v1.0.25 ships turn-axis only,
move-axis aggregation as a future refinement.

### 3.3 `AllocationAlgorithm` — four canonical algorithms

```python
class AllocationAlgorithm(Protocol):
    """Allocates a fixed visit budget across a candidate set."""

    def allocate(
        self,
        candidates: list[TurnView],
        value_fn: Callable[[TurnView], float],
        visit_scaling_model: VisitScalingModel,
        budget_visits: int,
        rng: Optional[Random] = None,
    ) -> dict[TurnIndex, int]:
        """Return per-candidate visit allocations summing to
        budget_visits (or as close as integer rounding allows).

        Candidates with zero allocation are simply absent from
        the returned dict — callers must treat absence as zero.

        rng is consumed only by stochastic algorithms (thompson
        _sampling); deterministic algorithms ignore it."""
```

The four curated implementations:

#### `greedy_eig`

Sort candidates by `EIG_per_visit(turn, 1)`. Allocate one visit
at a time to the current max-EIG candidate; recompute its
`EIG_per_visit` (after the visit, its `current_visits` increases,
so the gain-per-visit decreases per the scaling model). Repeat
until budget exhausted.

```python
def allocate(candidates, value_fn, scaling, budget, rng=None):
    visits_so_far: dict[TurnIndex, int] = {c.turn_index: 0 for c in candidates}
    values = {c.turn_index: value_fn(c) for c in candidates}
    current_visits = {c.turn_index: _get_current_visits(c) for c in candidates}
    for _ in range(budget):
        # Pick candidate with current-max EIG per visit.
        best = max(
            candidates,
            key=lambda c: values[c.turn_index] * scaling.expected_gain(
                c, current_visits[c.turn_index] + visits_so_far[c.turn_index], 1,
            ),
        )
        visits_so_far[best.turn_index] += 1
    return {t: v for t, v in visits_so_far.items() if v > 0}
```

Properties: deterministic, monotonic (each visit goes to highest
marginal value), no exploration. The natural baseline; the closest
analogue to v1.0.24's uniform allocation when value_fn is constant
and visit-scaling is uniform across candidates.

#### `knowledge_gradient`

For each candidate, compute the *knowledge gradient* — the
expected improvement in `max value` if we allocate the budget to
that candidate. Pick the candidate with the max KG; allocate the
full budget there (single-spend KG) OR allocate one visit and
repeat (incremental KG — closer to greedy but with the KG
formulation).

v1.0.25 ships the **incremental** form (one visit at a time,
recompute KG, repeat). The single-spend form is degenerate when
the budget is large compared to per-candidate visit-scaling.

KG formulation (sketch):

```
KG(c) = E[max_{c'} (μ(c') + visit-effect(c', if visits added to c=c))]
        − max_{c'} μ(c')
```

where `μ(c) = value_fn(c)` and `visit-effect` models the
expected change in `μ` after visits — for Phase 3's substrate,
this is the visit-scaling-model's `expected_gain` value. The KG
of a candidate is positive iff adding visits to it might shift
the argmax; otherwise zero.

Implementation: see §3.4 for the math. The substrate ships this
algorithm; calibration is the implementation's responsibility.

#### `thompson_sampling`

Treat each candidate's `μ(c) = value_fn(c)` as the mean of a
posterior; sample one `θ ~ N(μ(c), σ²(c))` per candidate, where
`σ²(c)` is informed by the visit-scaling model's expected
variance reduction. Allocate one visit to `argmax θ`; update the
sampled candidate's posterior (its `σ²` shrinks by the model's
prediction); repeat.

The per-candidate posterior maintenance is the algorithm's
internal state; v1.0.25 ships it without exposing the posterior
to user inspection. Stochastic — `rng` matters; the algorithm
ships with a deterministic-seed option for reproducibility in
tests.

#### `ucb`

Upper-confidence-bound. Score each candidate by

```
UCB(c) = μ(c) + κ × √(2 log T / n(c))
```

where `T` is the total visits-spent-this-round and `n(c)` is
candidate `c`'s allocated visits so far. Allocate one visit to
argmax UCB; repeat. `κ` is a tunable exploration-exploitation
parameter (default `κ = 1.0`; configurable via algorithm
parameters in capability metadata).

Properties: deterministic given `κ`; the classic bandit shape.
Suitable when the user wants principled exploration without
the stochasticity of Thompson sampling.

### 3.4 The acquisition function — how the three compose

Each algorithm has its own composition of the three plug points;
the substrate doesn't enforce a single shape. But the canonical
decomposition is the **expected information gain (EIG)**:

```
EIG(c, V) = value_fn(c) × visit_scaling_model.expected_gain(c, n(c), V)
```

`greedy_eig` consumes `EIG(c, 1)` to pick which candidate gets
each additional visit. `knowledge_gradient` uses
`visit_scaling_model.expected_gain(c, n(c), V_budget_remaining)`
as the expected `μ` shift; the `argmax` shift estimation is
the KG-specific bit. `thompson_sampling` uses
`visit_scaling_model.expected_gain(...)` to drive posterior
variance updates. `ucb` uses `value_fn(c)` directly as the mean
and a `√(log T / n(c))` exploration bonus independent of the
visit-scaling model.

So `value_fn` is consumed by all four; `visit_scaling_model` is
consumed by three (UCB ignores it — the exploration bonus has
its own shape). This is intentional: the substrate exposes the
plug points; the algorithms compose them as their math
prescribes. Substrate documentation in
`adaptive_reevaluate.py` names each algorithm's composition
explicitly.

### 3.5 The candidate set — composition with Phase 1 selector

Phase 3's allocation operates on a *candidate set*. Phase 1's
selector + selection-policy substrate provides this set: the
v1.0.23 `_dispatch_deepening_round` returns a deepening turn-set
which becomes Phase 3's candidates.

Two composition shapes are possible:

**(A) Phase 3 reuses Phase 1's candidate set.** The selector
identifies the worst-quantile slice; the allocation algorithm
spends the visit budget across that slice. Phase 1's `worst_
quantile` parameter controls candidate-set size; Phase 3's
`allocation_algorithm` controls intra-candidate spending. This
is the natural composition and v1.0.25 ships this shape.

**(B) Phase 3 operates on the full turn-set.** The allocation
algorithm decides candidate-set membership itself (via UCB-style
exploration or KG-style "argmax-shifting" filtering). Phase 1's
selector is bypassed.

Shape (A) is cleaner and composes with v1.0.24's per-color
quantile / pooled quantile / top-k mechanics. Shape (B) is more
ambitious but reduces to (A) when the allocation algorithm
includes "filter to top-k by `value_fn`" as a first step. v1.0.25
implements (A); (B) is achievable in user-authored shape via the
selector binding setting `value_fn` as the selector function
(reusing the same expression).

The candidate set's *size* (controlled by Phase 1's `worst_
quantile`, `top_k`, etc.) determines the allocation problem's
scale. A `top_k=10` selector produces 10 candidates; the
allocation algorithm spends `budget.visits_for_round()` across
those 10. The per-candidate average is `budget / 10` visits —
this is the relevant comparison to v1.0.24's uniform-extras
shape.

### 3.6 KataGo field grounding — what the response actually carries

The substrate sketched in §§3.1-3.5 is abstract: a value function
returns a per-turn scalar; a visit-scaling model returns expected
gain from V extra visits. Both interfaces consume a `TurnView`
whose `packet` field is the latest KataGo `AnalyzeResponse` for
that turn. This section pins down what information that packet
actually carries — what fields are available, which are
opt-in-gated, and how they ground concrete value functions and
visit-scaling models.

The data here was collected by submitting a probe analysis (one
SGF, four turns, `maxVisits=200`, every information-rich optional
field enabled) against a live KataGo SELECTOR. Field shapes are
recorded verbatim from the response.

#### 3.6.1 Field census

The response is a dict with five top-level information-bearing
keys, gated by per-query opt-in flags. Quantities marked
**always-on** ship without opt-in; **opt-in** require the named
field on the parent query.

```
rootInfo  (always-on, ~18 keys)
  currentPlayer            str        "B" | "W"
  winrate                  float      Post-search winrate estimate.
  rawWinrate               float      Pre-search (NN one-shot) winrate.
  scoreLead                float      Post-search expected score margin.
  rawLead                  float      Pre-search expected lead.
  scoreSelfplay            float      Score-from-selfplay-utility.
  rawScoreSelfplay         float      Pre-search version.
  scoreStdev               float      Post-search score standard deviation.
  rawScoreSelfplayStdev    float      Pre-search score stdev (NN's own SE).
  rawStScoreError          float      NN's one-shot score-estimate SE.
  rawStWrError             float      NN's one-shot winrate-estimate SE.
  rawNoResultValue         float      Pre-search no-result probability.
  rawVarTimeLeft           float      Expected game length variance.
  utility                  float      Composite KataGo utility.
  utilityLcb               float      LCB on utility.
  visits                   int        Total visits this position got.
  weight                   float      Aggregated visit-weight (soft).
  symHash, thisHash        str        For transposition-equivalence checks.

moveInfos  (always-on, list[dict], one per candidate move)
  move                     str        GTP-style (e.g. "Q16", "pass").
  order                    int        Rank by playSelectionValue (0=best).
  prior                    float      NN policy prior for this move.
  visits / edgeVisits      int        Visits / weighted-edge visits.
  weight / edgeWeight      float      Soft visit aggregates.
  playSelectionValue       float      KataGo's own move-selection score.
  winrate / scoreLead      float      Searched estimates for this move.
  scoreMean / scoreStdev   float      Per-move score moments.
  scoreSelfplay            float      Selfplay-utility score.
  utility / utilityLcb     float      Composite utility + LCB.
  lcb                      float      Winrate LCB.
  noResultValue            float      No-result probability under this move.
  pv                       list[str]  Principal variation, up to ~6 moves.
  pvVisits / pvEdgeVisits  list[int]  Visit counts at each PV depth.
  ownership                list[361]  Opt-in via includeMovesOwnership.
  ownershipStdev           list[361]  Opt-in via includeMovesOwnershipStdev.

ownership          (opt-in: includeOwnership)
  list[float, 361]            Per-board-point E[ownership in [-1, +1]].

ownershipStdev     (opt-in: includeOwnershipStdev)
  list[float, 361]            Per-board-point search-pooled stdev.

policy             (opt-in: includePolicy)
  list[float, 362]            NN raw policy distribution (361 board + pass).
```

The probe's observed numerics at turn 0 of a 19×19 game,
`maxVisits=200`:

```
rootInfo.scoreStdev               = 14.53     (post-search)
rootInfo.rawScoreSelfplayStdev    = 16.03     (pre-search, NN one-shot)
rootInfo.rawStScoreError          = 0.63      (NN's own SE estimate)
rootInfo.winrate                  = 0.5215
rootInfo.rawWinrate               = 0.5188
rootInfo.visits                   = 215       (KataGo overshoots maxVisits)
ownershipStdev                    sum=36.99, mean=0.103, max=0.54  (361 points)
policy                            len=362, Shannon entropy = 4.19 bits
moveInfos                         len=362, top-4 visits = [43, 43, 43, 43]
moveInfos[0].pvVisits             = [43, 19, 7, 3, 2, 1]
```

The visit distribution at turn 0 is unusual (the four corner
moves are symmetric under board hash, so visits split exactly
evenly across them — verifiable via `symHash == thisHash` and
the `pv` first-move pattern). Real mid-game turns produce more
skewed distributions; the substrate handles both.

#### 3.6.2 Concrete value-function expressions

Three natural information measures the user can author against
KataGo's field surface:

**(a) Policy entropy** — `H(policy)` over the 362-dim distribution.
Directly answers "is the NN uncertain about the best move?"

```yaml
bindings:
  value_fn: policy_entropy
symbols:
  policy_entropy: |
    sum([-p * log2(p) for p in extra.policy if p > 0])
```

Requires `includePolicy=true` on the parent query. Cheap to
compute; the 362-float list adds ~3KB per turn to the response.

**(b) Total ownership uncertainty** — sum of per-point stdev.
Answers "how unresolved is the territorial picture?"

```yaml
bindings:
  value_fn: ownership_total_uncertainty
symbols:
  ownership_total_uncertainty: |
    sum(extra.ownershipStdev)
```

Requires `includeOwnership=true` and `includeOwnershipStdev=true`.
Same byte cost as policy_entropy (~3KB per turn for the stdev
list).

**(c) LCB-spread top-K** — variance of the top-K candidates'
`utilityLcb` values. Answers "do the top candidates disagree on
who's best?"

```yaml
bindings:
  value_fn: top5_lcb_spread
symbols:
  top5_lcb_spread: |
    max(m.utilityLcb for m in moveInfos[:5])
      - min(m.utilityLcb for m in moveInfos[:5])
```

Always-on (no opt-in needed). The classic decision-uncertainty
proxy; this is also what informs the v1.0.23 default selector's
intuition.

Composite value functions naturally combine these:

```yaml
bindings:
  value_fn: composite_uncertainty
symbols:
  composite_uncertainty: |
    0.5 * sum([-p * log2(p) for p in extra.policy if p > 0])
      + 0.3 * sum(extra.ownershipStdev) / 361
      + 0.2 * (max(m.utilityLcb for m in moveInfos[:5])
              - min(m.utilityLcb for m in moveInfos[:5]))
```

The proxy substrate doesn't prescribe which measure to use —
each captures a different facet of "what does the user want
clarified." The user authoring against their workflow names the
relevant blend.

#### 3.6.3 Concrete visit-scaling model grounding

The naive `monte_carlo_sqrt` model from §3.1 uses `1/√V` scaling
with an unspecified prefactor. KataGo's response gives that
prefactor empirically: `rootInfo.scoreStdev` is the
search-aggregated stdev at the current visit count. The natural
visit-scaling model is then:

```
gain(turn, V_current, V_extra)
  = packet.rootInfo.scoreStdev * (1/√V_current − 1/√(V_current + V_extra))
```

Translation: the stdev *across MCTS samples* at the current
position is `scoreStdev`; the standard error of the mean (the
proxy's actual handle on "where this turn's score will settle")
scales as `scoreStdev / √V`. Adding `V_extra` reduces the SEM
from `scoreStdev/√V_current` to `scoreStdev/√(V_current + V_extra)`;
the gain is the difference.

This is **`monte_carlo_sqrt` parametrised per-turn from KataGo's
own variance estimate** — no calibration arc needed, no
empirical curve-fit. The substrate ships this as the curated
`monte_carlo_sqrt` model's implementation.

A secondary anchor: `rootInfo.rawStScoreError` (the NN's
one-shot pre-search SE) provides the V=1 baseline. Comparing
`scoreStdev/√V_current` against `rawStScoreError` tells the
operator whether the search has already reduced variance below
the NN-prior baseline (the typical state for any well-searched
position) — informative for diagnostics but not directly needed
by the model.

The ownership analogue: `ownershipStdev` per-point also scales
as 1/√V. A territory-uncertainty-oriented visit-scaling model
sums per-point variance reduction:

```
gain(turn, V_current, V_extra)
  = sum(packet.ownershipStdev) * (1/√V_current − 1/√(V_current + V_extra))
```

Same shape; the prefactor is the integrated ownership stdev
instead of `scoreStdev`. Useful when the user's value function
is ownership-driven (case (b) above) — the prefactor matches
the value-function units.

The substrate ships **`monte_carlo_sqrt`** (with `scoreStdev`
prefactor; the default) and **`diminishing_returns_log`** (the
non-`1/√V` baseline for sanity-checking). A future research arc
calibrates an empirical model against the gap between
`scoreStdev/√V_current` and the actual fluctuation observed
when V is bumped to `V_current + V_extra` — this is the kind of
question the substrate makes answerable but doesn't itself answer.

#### 3.6.4 Wire-shape implications

The optional `include*` flags are KataGo-native — they pass
through the proxy unchanged via the existing wire-shape strip
mechanics. The Phase 3 substrate doesn't need to introduce
proxy-side equivalents; it just consumes the fields when present.

But: **a value function authored against an opt-in field requires
the parent query to opt in.** A `value_fn` reading
`extra.ownershipStdev` against a query without
`includeOwnershipStdev=true` produces a runtime error in the
binding evaluation. Two design options for handling this:

- **Cheap option** — let the binding fail at evaluation time;
  the failure surfaces via the registry interpreter's own error
  path.
- **Eager option** — at `_is_phase3_engaged` check time, parse
  the value-function expression for field references and verify
  the parent query's opt-in flags match. Refuse with
  `AdaptiveConfigurationError(code="allocation_invalid",
  detail={"missing_includes": [...]})` if not.

The eager option matches the cost-asymmetry calibration (§7) —
better to refuse at construction with a structured error than
to spawn a multi-round adaptive run that fails on the first
round's value evaluation. **The roadmap prescribes the eager
option** (commit 5's `_is_phase3_engaged` validation gains
this check); it's a substrate decision worth pinning in the
design phase.

The cost of opt-in is non-trivial: `includeMovesOwnership` adds
`361 * len(moveInfos)` floats per response — at a typical
`maxVisits=1000` with 60-80 candidate moves, this is ~25K
floats ≈ 200KB per turn. Range queries multiply by N turns;
multi-round multiplies again by K rounds. The user opting into
`includeMovesOwnership` is opting into a real per-query byte
cost. The substrate doesn't enforce a budget on this (KataGo's
own protocol does the work); the roadmap surfaces the cost as
a documentation note rather than a substrate restriction.

#### 3.6.5 Summary — fields the substrate consumes

For the canonical Phase 3 path (the three plug points operating
on `TurnView.packet`):

- **Visit-scaling model** consumes
  `packet.rootInfo.scoreStdev` (or `ownershipStdev` sum,
  depending on the model), `packet.rootInfo.visits` (current
  visit count anchor).
- **Value function** consumes whatever fields its
  user-authored expression names — typically `policy`,
  `ownershipStdev`, `moveInfos[*].utilityLcb`,
  `moveInfos[*].prior`, or `rootInfo.scoreStdev`.
- **Allocation algorithm** is field-agnostic — it consumes only
  the model + value function outputs.

The substrate is field-aware only at the visit-scaling-model
implementation level and at the eager-validation step in
`_is_phase3_engaged`. The `AllocationAlgorithm` Protocol stays
agnostic; user-authored value functions are free to read any
field the parent query has opted into.

---

## 4. Wire shape

Capability metadata gains four new fields under
`capabilities.adaptive_reevaluate`:

```json
{
  "capabilities": {
    "adaptive_reevaluate": {
      "allocation_algorithm": "greedy_eig",
      "visit_scaling_model": "monte_carlo_sqrt",
      "value_binding": "expected_score_variance",
      "allocation_params": { "ucb_kappa": 1.5 },

      "worst_quantile": 0.25,
      "extra_visits": 800,
      "window_size": 1,
      "selection_policy": "per_color_quantile",
      "selector_axis": "turn",
      "budget": "range-generous"
    }
  }
}
```

Where:

- **`allocation_algorithm: str`** — name of the algorithm from
  the curated set. Engagement signal: when present, the Phase 3
  dispatch path engages; when absent, v1.0.24's worst-quantile +
  uniform-visits dispatch holds.
- **`visit_scaling_model: str`** — name of the model from the
  curated registry. Required when `allocation_algorithm` is named.
- **`value_binding: str`** — name of the symbol in
  `analysis_config.symbols` bound to `value_fn`. Required when
  `allocation_algorithm` is named.
- **`allocation_params: dict[str, Any]`** — algorithm-specific
  parameters (e.g., `ucb_kappa` for UCB, `ts_seed` for Thompson
  sampling). Optional; algorithm-specific.

The wire shape is **additive** — every v1.0.24 client continues
to work unchanged. The `allocation_algorithm` field is the
single engagement signal; absent means v1.0.24 semantics, present
means Phase 3.

---

## 5. Allocation-driven dispatch

### 5.1 The dispatch path

When `allocation_algorithm` is named, the multi-round loop's
per-round dispatch changes:

```python
while budget.has_capacity(state):
    # Phase 1 still gates: identify candidates.
    deepen, worst_pairs, worst_value = _dispatch_deepening_round(
        finals, state, cap_meta, analysis_config, window_size, all_turns,
    )
    if not deepen:
        break

    if _is_phase3_engaged(cap_meta):
        # Phase 3: allocate visits across candidates.
        allocation = _allocate_visits(
            candidates=[_turn_view(state, t) for t in deepen],
            cap_meta=cap_meta,
            analysis_config=analysis_config,
            budget_visits=budget.visits_for_round(),
        )
        # Spawn N parallel sub-queries, one per candidate.
        sub_queries = [
            _build_deeper_query(parent, [turn], visits)
            for turn, visits in allocation.items()
        ]
        results = await ctx.parallel(*sub_queries)
        for r_list in results:
            for resp in r_list:
                if isinstance(resp, AnalyzeResponse) and not resp.is_during_search:
                    state.observe(resp)
                    yield replace(resp, is_during_search=True)
        state.record_round(
            worst_pairs=worst_pairs,
            deepening_turns=deepen,
            worst_selector_value=worst_value,
        )
        state.record_visits(budget.visits_for_round())
    else:
        # Phase 2: v1.0.24 path (unchanged).
        deeper = _build_deeper_query(parent, sorted(deepen), budget.visits_for_round())
        async for resp in ctx.spawn(deeper):
            ...
```

`_allocate_visits` parses the capability metadata, resolves the
named algorithm + visit-scaling model + value-function binding,
and returns the per-turn allocation dict.

### 5.2 Per-turn parallel spawning

KataGo's analyze action has no per-turn `maxVisits` field —
`maxVisits` is a query-level scalar applied uniformly across all
`analyze_turns`. To achieve per-turn allocation, the Phase 3
dispatch spawns N sub-queries in parallel, each with a single
turn in `analyze_turns` and its allocated visits as `maxVisits`.

This composes with the v1.0.21 orchestration framework's
`OrchestrationContext.parallel(*queries)` primitive:

```python
async def parallel(self, *queries: KataGoQuery) -> list[list[KataGoResponse]]:
    """Spawn N sub-queries; gather; return per-query response lists."""
```

The `parallel` primitive is already implemented (see
`middleware/orchestration.py:260`); Phase 3 is the first
production-side consumer. Each sub-query gets its own
`sub_orig_id`; the orchestration framework routes responses back
to the parent's spawn iterator.

The N-spawn cost is bounded: candidate-set sizes are small
(`worst_quantile=0.25` over a 10-turn range produces ~3
candidates; `top_k=10` produces 10). KataGo's analyze pipeline
handles per-query overhead well; the marginal cost of N parallel
single-turn queries vs 1 N-turn query is typically dominated by
visit cost, not protocol overhead.

### 5.3 Composition with Phase 2 multi-round

Multi-round allocation: each round's allocation reads the *current*
state. Visits spent in round R update each candidate's
`current_visits` (via `state.observe`); round R+1's allocation
sees the updated state. This is the natural composition — the
allocation algorithm operates on the per-round snapshot.

The budget abstraction's `total_extra_visits` and
`wall_clock_seconds` constraints apply across the whole multi-
round arc; `budget.visits_for_round()` returns the per-round slice
the allocation algorithm consumes. Convergence-based termination
operates on the round's worst-selector-value or jaccard trajectory
as in v1.0.24.

A subtle composition point: under Phase 3, the worst-set may
shift round-to-round (a turn that received allocation in round 1
may have its uncertainty resolved and drop out of round 2's
worst-set; a previously-uninteresting turn may rise into the
worst-set if its state shifted). The v1.0.24 `worst_set_jaccard_
to_previous` metric continues to make sense; convergence triggers
when the worst-set stabilises (whether or not the allocation
algorithm continues spending budget on the same candidates).

---

## 6. Defaults and engagement

**Engagement:** Phase 3 is opt-in. The single engagement signal
is the presence of `allocation_algorithm` in capability metadata.
v1.0.24 clients omitting the field see no behaviour change.

**Required co-fields when engaged:** `visit_scaling_model` and
`value_binding`. The substrate refuses with
`AdaptiveConfigurationError(code="allocation_invalid")` if either
is missing.

**No `allocation_algorithm` default.** The user explicitly opts
in to Phase 3 by naming an algorithm. There is no path where
the substrate engages the allocation dispatch silently. Matches
the v1.0.23 turn-axis selector discipline.

**Algorithm parameter defaults:** Algorithm-specific.
`ucb_kappa=1.0` for UCB (canonical balance);
`ts_seed=None` for Thompson sampling (stochastic; tests pass an
explicit seed for reproducibility).

**Visit-scaling-model default within Phase 3:** No default. The
user names a model explicitly. The curated set ships two
(`monte_carlo_sqrt`, `diminishing_returns_log`); user-authored
models are Phase 4 territory.

---

## 7. `allocation_invalid` — the sixth `AdaptiveConfigurationError` code

Extending v1.0.24's five codes (`ambiguous_axis`,
`axis_binding_mismatch`, `policy_axis_mismatch`,
`policy_parameters_invalid`, `budget_invalid`):

`allocation_invalid` raises on:

- **Unknown algorithm name.** `allocation_algorithm:
  "no_such_algo"` → refuse with the curated set in `detail`.
- **Unknown visit-scaling-model name.** Symmetric refusal.
- **Missing `visit_scaling_model`** when `allocation_algorithm`
  is named.
- **Missing `value_binding`** when `allocation_algorithm` is
  named.
- **Missing or wrong-shape value-function binding** in
  `analysis_config` (e.g., named binding doesn't resolve to a
  callable).
- **Missing required algorithm parameter.** E.g., a future
  algorithm requires `param_x` and it's absent.
- **Wrong type or out-of-range algorithm parameter.** E.g.,
  `ucb_kappa: -1.0` is invalid.
- **`allocation_params` is not a dict.**

Same cost-asymmetry calibration (§11.4 in the
selector-pluggability roadmap):
adaptive_reevaluate operations are expensive (range queries,
N-parallel sub-queries per round, multi-round repetition).
Silent fallback to a default algorithm or model would burn
compute on a wrong-shape analysis without surfacing the
configuration error. Hard-refuse with structured `detail`.

The `detail` dict carries the violated field, the offending
value, and (where applicable) the valid alternatives. The
SPA's error surface consumes this for user-facing
configuration validation.

---

## 8. Sub-arc design discussions

### 8.1 Why per-turn parallel sub-queries (not batching)

KataGo's analyze action's `maxVisits` is query-level, not
per-turn. Three options for per-turn allocation:

- **(A) N parallel single-turn sub-queries.** Each with its
  allocated visits as `maxVisits`. Composes with `ctx.parallel`.
  Substrate-supported.
- **(B) One big batched sub-query with `maxVisits = max
  allocation`.** Then suppress responses early per non-maximally-
  allocated turn. Wasted compute; fragile.
- **(C) Multiple rounds with single-turn batches.** Spawn one
  turn per round-fragment. Defeats the point of multi-round
  (each round's worst-set determination would be polluted by
  one-turn-at-a-time scope).

Option (A) is the only clean shape. The orchestration
framework already supports it (`ctx.parallel` from v1.0.21).
Phase 3 is the first production caller.

### 8.2 Why turn-axis only (no move-axis allocation)

KataGo allocates visits to *positions*, not to *moves*. A
"move" in v1.0.23's sense is a per-color sequence index spanning
two positions (before, after). Allocating visits to a "move"
would mean allocating to both endpoints, which doubles the
candidate count (or requires move-to-position aggregation).

Phase 3 simplifies: candidates are positions (turns); the value
function is per-turn; the allocation produces a per-turn dict.
Users who want move-axis prioritisation aggregate at the
value-function level — e.g., `value_fn(turn) = max(score_loss_
of_moves_ending_at_turn)`. The aggregation is one expression
in the user's binding, not a substrate complication.

### 8.3 Why curated algorithm set (not full registry)

Allocation algorithms span theory and practice. Phase 4's
program-shaped binding substrate would expose the
`AllocationAlgorithm` Protocol directly to user authoring, but
v1.0.25 ships a closed set. Rationale:

- **Correctness.** Each curated algorithm has well-defined math;
  user-authored algorithms risk subtle bugs (e.g., a
  knowledge-gradient implementation that doesn't update its
  posterior correctly will allocate poorly without the user
  realising).
- **Composition with state.** The algorithms read v1.0.24's
  `AdaptiveState` for per-round per-turn visits-so-far; the
  substrate hides this state-passing from user code.
- **Performance.** The curated implementations are
  performance-tuned for the typical candidate-set size (3-20);
  user-authored algorithms via the registry interpreter would
  pay interpretation overhead per visit-allocation step.

The closed-set discipline mirrors v1.0.23's curated
selection-policy set (per-color quantile / pooled quantile /
threshold / top-k). Phase 4 adds the escape hatch; Phase 3
gets the four-algorithm coverage.

### 8.4 `current_visits` source — `rootInfo` or `maxVisits`

The visit-scaling model needs each candidate's `current_visits`
to predict marginal information gain. Two sources:

- **KataGo's `rootInfo.visits`** — the actual visit count
  KataGo reported in the most recent observation for this turn.
  Authoritative; comes from the latest `state.last_packet(turn)`.
- **Query's `maxVisits`** — the visit budget the query
  authorised. KataGo may stop short (if a terminator triggers)
  but typically reaches `maxVisits`.

`rootInfo.visits` is authoritative when available;
`maxVisits` is the fallback. The implementation prefers
`state.last_packet(turn).opaque.get("rootInfo", {}).get("visits",
parent.opaque.get("maxVisits", 1))` with sensible nesting
fallback. Documented in the `VisitScalingModel` Protocol's
contract; both `monte_carlo_sqrt` and `diminishing_returns_log`
consume it via the same helper.

### 8.5 Acquisition function vs algorithm — why the substrate exposes both

A simpler design would absorb the visit-scaling model + value
function into a single "acquisition function" exposed as one
plug point. Three reasons the three-plug-point design wins:

- **Separation of concerns.** The value function expresses
  user intent ("I care about this kind of turn"); the visit-
  scaling model expresses empirical KataGo behaviour ("V visits
  buy this much information"). Conflating them obscures whether
  a configuration choice is a research preference or an
  empirical claim about KataGo.
- **Reusability.** A user authoring a value function for one
  workflow can reuse it across allocation algorithms;
  swapping `greedy_eig → knowledge_gradient` doesn't require
  rewriting the value expression.
- **Composability with Phase 4.** When Phase 4 lands user-
  authored allocation policies, the value function and visit-
  scaling model are natural injectable components — the
  user-authored allocation reads them via the curated registry,
  not by re-deriving them.

### 8.6 The selector vs the value function — a structural-overlap note

A keen-eyed reader notices that v1.0.23's selector binding and
Phase 3's value function are structurally identical: both are
per-turn (or per-move) scalars consumed by the dispatch path.

They are deliberately separate because their *semantic roles*
differ:

- **Selector** (Phase 1) — "**bad** turn?" (lower=worse).
  Identifies candidates from the full turn set.
- **Value function** (Phase 3) — "**valuable to clarify?**"
  (higher=more valuable). Prioritises allocation among
  candidates.

A user can reuse the same expression for both (binding the same
symbol to `move_selector_fn` for v1.0.23 and `value_fn` for
v1.0.25, modulo sign): the candidate set is the worst (by
selector); the allocation prioritises the most-valuable (by
value-function). When the selector and value function are
sign-flipped versions of the same expression, Phase 3 behaves
like "allocate proportionally to how bad the candidate is" —
a sensible default for the "I want to investigate the worst
turns most thoroughly" workflow.

This is intentional substrate composition, not redundancy.

---

## 9. Migration shape — eight-commit arc

The implementation rolls out incrementally, each commit adding
substrate or wiring without breaking v1.0.24 behaviour:

### Commit 1 — Documentation

- **File:** `proxy/docs/roadmap-info-theoretic-allocation.md`
  (this file).
- Lands the design note. Reviewed before substrate work begins.

### Commit 2 — `VisitScalingModel` substrate

- **Protocol class** `VisitScalingModel` in
  `middleware/adaptive_reevaluate.py` (or a new file
  `middleware/visit_scaling.py` if size warrants).
- **Curated registry** `_VISIT_SCALING_MODELS: dict[str,
  VisitScalingModel]` with `monte_carlo_sqrt` and
  `diminishing_returns_log` implementations.
- **`_parse_visit_scaling_model(name: str) → VisitScalingModel`**
  factory raising `AdaptiveConfigurationError(code=
  "allocation_invalid")` on unknown names.
- **No consumers yet** — additive substrate only.
- **Tests:** `tests/test_visit_scaling.py` — unit-level
  exercise of both models against known inputs.

### Commit 3 — `AllocationAlgorithm` substrate

- **Protocol class** `AllocationAlgorithm`.
- **Curated registry** with the four implementations
  (`greedy_eig`, `knowledge_gradient`, `thompson_sampling`,
  `ucb`).
- **`_parse_allocation_algorithm(cap_meta) → AllocationAlgorithm`**
  factory, threading `allocation_params` through.
- **No consumers yet** — additive substrate.
- **Tests:** `tests/test_allocation_algorithms.py` — unit-level
  exercise of each algorithm against synthetic candidate sets.

### Commit 4 — Value-function binding

- **`RegistryInterpreter.get_value_fn()`** Optional-returning
  accessor in `registry_interpreter.py`. Same shape as v1.0.23's
  `get_move_selector_fn()`/`get_turn_selector_fn()`.
- **`AdaptiveConfigurationError(code="allocation_invalid")`**
  raises when `allocation_algorithm` is named without a
  resolvable `value_fn` binding.
- **No consumers yet** — accessor available but no dispatch
  consumes it.
- **Tests:** extension of
  `tests/test_adaptive_selector_pluggability.py` with
  `value_fn` resolution cases.

### Commit 5 — Allocation-driven dispatch path

- **`_is_phase3_engaged(cap_meta)`** — checks for
  `allocation_algorithm` presence and validates co-fields.
- **`_allocate_visits(candidates, cap_meta, analysis_config,
  budget_visits)`** — resolves algorithm + model + value
  function and produces the allocation dict.
- **Coroutine integration** — multi-round loop's per-round
  dispatch branches on `_is_phase3_engaged`:
  - Phase 3 engaged → `_allocate_visits` → `ctx.parallel(*sub_
    queries)` → emit results as previews.
  - Phase 3 absent → v1.0.24 single-spawn path (unchanged).
- **Tests:** `tests/test_info_theoretic_allocation.py` (new) —
  end-to-end coroutine-level tests covering both dispatch
  paths.

### Commit 6 — Finalization composition

- **No code change expected** — v1.0.24's finalization stage
  already emits each turn's latest observed packet. The
  N-parallel-sub-query spawning observes each result; the
  finalization stage handles N-source state uniformly.
- **Tests:** finalization wire-shape pinning under Phase 3 —
  one authoritative per turn regardless of N-source.

### Commit 7 — Refusal-surface tests

- **Tests:** ten-plus `allocation_invalid` refusal cases —
  unknown algorithm, unknown model, missing
  `visit_scaling_model`, missing `value_binding`, malformed
  algorithm parameters, etc.
- Mirrors v1.0.24's `budget_invalid` refusal coverage.

### Commit 8 — Documentation closure

- **Update** the umbrella's adaptive-widening design note
  (`LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`)
  to mark Phase 3 as landed; reference the v1.0.25 tag.
- **Update** `proxy/CLAUDE.md` if Phase 3 introduces a
  new heartbeat / fanout / etc. contract worth surfacing
  (likely not — Phase 3 is internal to adaptive's coroutine).
- **Cross-references** between this roadmap, the
  selector-pluggability roadmap, the multi-round roadmap, and
  the umbrella design note are completed.

mypy --strict must remain clean at every commit boundary;
pytest passes at every commit. Same discipline as v1.0.22-24.

---

## 10. Composition with existing precedents

- **Same substrate** (`RegistryInterpreter`) for the value-
  function binding. The `get_value_fn` accessor follows the same
  Optional-returning shape as `get_move_selector_fn` /
  `get_turn_selector_fn`.
- **Same gating mechanics** (`CapabilityGatedMiddleware`).
  Phase 3 engagement signal (`allocation_algorithm` present)
  layers within the existing `adaptive_reevaluate` capability
  gate.
- **Same wire-strip discipline** (`_PROXY_ONLY_FIELDS`). The
  four new capability-metadata fields ride within the existing
  `capabilities` strip envelope.
- **Same orchestration framework** (`ctx.parallel` for N
  sub-queries per round; `ctx.spawn` semantics for each).
- **Same streaming-preview semantics** (v1.0.20) — each
  sub-query's responses stream as previews; finalization emits
  authoritatives.
- **Same multi-round budget** (v1.0.24) — the four budget
  shapes apply uniformly across Phase 3 and v1.0.24 dispatch.
- **Same `AdaptiveConfigurationError` shape** — sixth code
  joins the existing five with the same cost-asymmetry
  calibration.
- **Same `AdaptiveState` accumulator** — Phase 3's
  `current_visits` consumption reads from `state.last_packet`;
  each round's per-turn allocation updates state via
  `state.observe` on the sub-query response.

---

## 11. Open questions for user review

A short list of points worth deciding before substrate work
begins. Each names the proposed default and the alternative.

### 11.1 Value-function default

**Question:** Should Phase 3 ship a default value function
(e.g., `policy_entropy` or `score_variance`)?

**Proposed default:** No. The user must supply `value_binding`
when `allocation_algorithm` is named. Symmetric with v1.0.23's
turn-axis selector discipline (explicit > silent fallback).

**Alternative:** Provide one canonical default (e.g., constant=1,
which makes allocation reduce to "spread visits evenly across
candidates, weighted by visit-scaling-model").

### 11.2 Visit-scaling-model registry — closed vs extensible

**Question:** Should the registry of named models be a closed
constant or accept runtime registrations from external callers?

**Proposed default:** Closed for v1.0.25 (the two canonical
models). Extension API in Phase 4 alongside user-authored
allocation policies.

**Alternative:** Open registry from the start — define a
plugin-registration API (`register_visit_scaling_model(name,
impl)`) that future calibration arcs can consume.

### 11.3 Allocation-algorithm registry — same question

**Question:** Same as 11.2 for `AllocationAlgorithm`.

**Proposed default:** Closed for v1.0.25 (the four canonical
algorithms).

**Alternative:** Same — open registry from the start.

### 11.4 Algorithm parameters wire surface

**Question:** Should algorithm-specific parameters live in
`allocation_params: dict[str, Any]` (sketched in §4), or as
top-level capability fields (e.g., `ucb_kappa: 1.5` directly)?

**Proposed default:** `allocation_params` dict. Keeps the
top-level shape stable across algorithms; algorithm-specific
keys nest under the dict.

**Alternative:** Top-level fields with algorithm-aware
validation. More discoverable; risks namespace conflict if
two algorithms have parameters with the same name.

### 11.5 Knowledge-gradient — single-spend vs incremental

**Question:** Should `knowledge_gradient` allocate the whole
budget to the max-KG candidate (single-spend) or one visit at
a time, recomputing KG (incremental)?

**Proposed default:** Incremental. More natural composition with
budgets that span much more than one candidate's saturation
point; single-spend KG is degenerate when the budget is large.

**Alternative:** Single-spend. Faster to compute; matches the
canonical KG formulation in the BO literature.

### 11.6 UCB exploration bonus form

**Question:** Should UCB's exploration term use `√(log T /
n(c))` (classic) or `√(log T / (n(c) + 1))` (Beale-Welford
shifted form for division-by-zero safety)?

**Proposed default:** Beale-Welford shifted form. Avoids the
`n(c) = 0` div-by-zero on the first allocation step.

**Alternative:** Classic form with explicit first-pass
initialisation (each candidate gets one mandatory visit before
the UCB loop engages).

### 11.7 Move-axis allocation — out of scope or future

**Question:** Should v1.0.25 surface a path for move-axis value
functions, with per-move allocation aggregated into per-turn?

**Proposed default:** Out of scope for v1.0.25. Move-axis
authoring goes via Phase 1's selector binding; Phase 3
turn-axis-only.

**Alternative:** Add `value_axis: "move" | "turn"` symmetric to
v1.0.23's `selector_axis`, with a move-to-turn aggregation step
(sum / max / mean across endpoints).

### 11.8 Thompson sampling — deterministic option for tests

**Question:** Tests need deterministic Thompson sampling
output. Should the `rng` argument be wire-exposable (e.g.,
`allocation_params.ts_seed`)?

**Proposed default:** Yes — tests are first-class consumers,
and reproducible exploration is valuable for research workflows.

**Alternative:** Internal `rng` only, with a test-only hook
(`set_test_seed`) outside the production wire.

### 11.9 Per-turn parallel — visibility of N sub-queries

**Question:** Should the SPA see N preview emissions (one per
sub-query) or one per turn? Currently each KataGo response
streams as a preview, so N parallel sub-queries → N preview
streams interleaved.

**Proposed default:** N preview emissions, as currently. The
SPA already handles multi-source previews via v1.0.20's
state-fn substrate; the SPA's rendering of "this turn's analysis"
reads `state.last_packet(turn)`, which absorbs the latest
emission from any source.

**Alternative:** Coalesce per-turn previews — only emit the
latest preview per turn, suppressing intermediate updates.
Adds buffering; v1.0.20's "no buffering" discipline forbids.

### 11.10 Eager vs lazy field-availability checks

**Question:** Should the substrate parse the user's value-function
expression at engagement time and verify that the parent query's
`include*` flags match the fields the expression reads
(eager), or let the binding evaluation fail at first use
(lazy)?

**Proposed default:** Eager. The cost-asymmetry argument (§7)
applies — a malformed Phase 3 configuration can burn many
rounds × many candidates of compute before lazy evaluation
surfaces the issue. Eager validation matches v1.0.23's
`AdaptiveConfigurationError` discipline.

**Alternative:** Lazy. Simpler substrate (no expression parsing);
defers field-reference detection to the user's authoring
discipline.

The §3.6.4 prose currently prescribes eager; this open question
makes the choice explicit for user review.

### 11.11 Visit-scaling model — `scoreStdev` prefactor vs constant

**Question:** Should `monte_carlo_sqrt` use
`packet.rootInfo.scoreStdev` as its prefactor (per-turn
empirical, per §3.6.3), or a constant magnitude prefactor
(value-function-relative)?

**Proposed default:** `scoreStdev` prefactor. The natural
empirical grounding from KataGo's own variance estimate; means
the value-function output is in score-equivalent units.

**Alternative:** Constant prefactor (e.g., 1.0). The
visit-scaling model's output becomes a dimensionless gain factor
that the allocation algorithm scales by the value function. The
total EIG units become value-function-dependent (whatever the
user's expression returns). More flexibility for unusual
value-function units (e.g., bit-valued policy entropy) at the
cost of less-natural empirical grounding.

If `monte_carlo_sqrt` consumes `scoreStdev`, users with
information-theoretic value functions (e.g., policy entropy in
bits) get a mixed-units EIG. Workable — the allocation
algorithm consumes ratios, not absolutes — but worth being
explicit about.

---

## 12. Defaults / backwards compat

- **No `allocation_algorithm`** → v1.0.24 dispatch unchanged
  (worst-quantile + uniform extras + single deeper spawn).
- **`allocation_algorithm` present without `visit_scaling_model`
  or `value_binding`** → `AdaptiveConfigurationError(code=
  "allocation_invalid")`. Hard-refuse.
- **`allocation_algorithm` present, all co-fields valid** →
  Phase 3 dispatch engages. N parallel sub-queries per round;
  per-turn allocation per the named algorithm.
- **Multi-round budget** — unchanged. Phase 3 composes with
  v1.0.24's Budget; the per-round `visits_for_round()` becomes
  the allocation's `budget_visits`.

---

## 13. Scope boundaries (out of scope across all Phase 3)

- **Calibrated visit-scaling models.** The substrate accepts
  pluggable models; calibrated `katago_lcb_spread` /
  `katago_value_variance` models land as their own arc with
  offline curve-fitting against KataGo data.
- **User-authored allocation algorithms.** Phase 4 territory;
  requires `RegistryInterpreter` extension to program-shaped
  bindings.
- **Move-axis allocation.** Substrate is turn-axis; move-axis
  authoring composes via Phase 1's selector binding.
- **SPA-side authoring surface** (palette-editor extensions for
  value-function authoring). Proxy ships substrate; the SPA
  exposes editing in a separate frontend arc.
- **Cross-query / cross-session allocation.** Allocation
  decisions reset at each parent query's start. Autonomous-SR-
  loop territory for cross-session reasoning.
- **Live re-allocation within a round.** Round R's allocation
  is computed at round start; if a candidate's allocation is
  100 visits and that turn's response arrives early, the
  remaining visits are not redirected. Future refinement
  (early-stopping per candidate within a round) is out of scope.

---

## 14. References

- **`proxy/middleware/adaptive_reevaluate.py`** — file the
  Phase 3 dispatch modifies.
- **`proxy/middleware/orchestration.py`** — substrate for the
  `ctx.parallel` per-round N-spawn pattern.
- **`proxy/registry_interpreter.py`** — substrate the
  `value_fn` binding rides on.
- **`proxy/docs/roadmap-adaptive-selector-pluggability.md`** —
  v1.0.23 establishes the binding-role / selection-policy
  substrate Phase 3 composes with.
- **`proxy/docs/roadmap-multi-round-adaptation.md`** — v1.0.24
  establishes the multi-round loop, `AdaptiveState`, and
  `Budget` substrate Phase 3 composes with.
- **`proxy/docs/roadmap-adaptive-type-branding.md`** — v1.0.22
  establishes `MoveIndex` / `TurnIndex` brands.
- **`proxy/docs/roadmap-capability-negotiation.md`** —
  capability-metadata pattern Phase 3's wire shape extends.
- **`LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`**
  — umbrella design note's §6 names Phase 3's plug points and
  curated-set discipline.

---

## 15. End-to-end check

A worked example exercising the substrate, to verify the design
hangs together. The user's wire payload:

```json
{
  "action": "analyze",
  "analyze_turns": [10, 11, 12, 13, 14, 15],
  "rules": "tromp-taylor",
  "boardXSize": 19,
  "maxVisits": 1000,
  "analysis_config": {
    "bindings": {
      "value_fn": "score_variance"
    },
    "symbols": {
      "score_variance": "var([m.scoreLead for m in extra.before.moveInfos[:5]])"
    }
  },
  "capabilities": {
    "adaptive_reevaluate": {
      "worst_quantile": 0.5,
      "extra_visits": 2000,
      "window_size": 1,
      "selection_policy": "pooled_quantile",
      "selector_axis": "turn",
      "allocation_algorithm": "greedy_eig",
      "visit_scaling_model": "monte_carlo_sqrt",
      "value_binding": "value_fn",
      "budget": {"max_rounds": 3}
    }
  }
}
```

Coroutine flow:

1. **Stage 1:** Six originals stream as previews. State observes
   each. (v1.0.24 behaviour, unchanged.)

2. **Stage 2 round 1:**
   - `_dispatch_deepening_round` resolves to the turn axis (per
     `selector_axis`); identifies 3 worst-quantile turns from the
     6 candidates (`worst_quantile=0.5` → top half by selector,
     but the selector is `score_variance`... actually it depends
     on the user's binding for `move_selector_fn` /
     `turn_selector_fn`). Suppose turns {11, 13, 14} are the
     candidates.
   - `_is_phase3_engaged(cap_meta)` returns `True`.
   - `_allocate_visits(candidates=[view_11, view_13, view_14],
     algo=greedy_eig, model=monte_carlo_sqrt,
     value_fn=resolved, budget_visits=2000)` computes per-turn
     allocations. Suppose `{11: 800, 13: 600, 14: 600}`.
   - `ctx.parallel(deeper_for_11_800, deeper_for_13_600,
     deeper_for_14_600)` spawns 3 sub-queries.
   - Each sub-query's final streams as a preview;
     `state.observe` updates each turn's `last_packet`.
   - `state.record_round(...)` finalises round 1.

3. **Stage 2 round 2:**
   - Candidate set may shift (turn 14's resolved uncertainty
     may have moved it out of the worst-quantile; turn 12 may
     have shifted in). Allocation re-runs with the updated
     state.

4. **Stage 2 round 3:** Same shape. After round 3, `budget.has_
   capacity` returns False (`max_rounds=3`).

5. **Stage 3 finalisation:** Each turn's latest observed packet
   emitted with `is_during_search=False`. 6 authoritatives.

Composition holds: v1.0.24's multi-round loop, v1.0.23's
selector + selection-policy substrate, v1.0.22's
move/turn brands, and v1.0.21's orchestration framework
(`ctx.parallel`) all compose to produce the Phase 3 wire
behaviour.

---
