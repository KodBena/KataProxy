# Multi-round adaptation + budget abstraction — design roadmap

- **Status:** `design-note: planned` (per umbrella ADR-0005 Rule 8
  doc-graph genre vocabulary)
- **Date:** 2026-05-18
- **Scope:** `proxy/middleware/adaptive_reevaluate.py` (multi-round
  coroutine + budget + AdaptiveState), with small additive surface
  in the view dataclasses. Builds on the v1.0.23 selector-
  pluggability substrate.
- **Origin:** Lands Phase 2 of the umbrella's adaptive-widening
  design note (`LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`)
  — the multi-round arc plus the across-iteration substrate that
  the design note's later revision recognised as Phase-2-coupled.
- **Authoritative for the `feat/multi-round-adaptation` branch;**
  superseded by the v1.0.24 release notes once tagged.

---

## TL;DR

This arc widens `adaptive_reevaluate` from a single-shot
select-and-deepen into a multi-round loop with a typed budget
abstraction and an across-iteration state object. v1.0.23's
selector pluggability + window correction stay in place; this arc
wraps them in a loop and exposes round-history to user selectors
via the view object's `round_history` field.

Five new pieces:

1. **The multi-round loop** in the coroutine's Stage 2+. Each
   iteration is one round of select-and-deepen. Budget exhaustion,
   convergence, or "no more turns to deepen" terminates the loop.
2. **`AdaptiveState`** object — accumulates per-round per-unit
   data across iterations. Framework-owned, queryable from
   selectors / value functions / budget objects.
3. **`round_history`** field on `MoveView` / `TurnView` —
   selectors can read prior-round selector values, deepened
   counts, and the prior round's analyze packet.
4. **Budget abstraction** — composable shapes: fixed K rounds,
   total extra-visits, wall-clock / GPU-time, convergence-driven.
   Multiple constraints on one budget terminate on whichever
   exhausts first.
5. **Curated termination policies** — built-in convergence
   metrics (`worst_selector_value`, `worst_set_jaccard_to_previous`)
   plus the four-form tolerance shape (`metric`, `tolerance`,
   `lookback`, `scale`). Composable via `all_of` / `any_of` per
   §11.4-style consistency.

Plus context-dependent budget profiles (`review-tight` /
`range-generous` / `loop-aggressive`) layered over raw budget
shapes for ergonomic per-context tuning.

Pattern parallels v1.0.23 in shape (focused multi-commit arc on
a feature branch) but is bigger in substrate surface. The wire
shape gains a `budget` field on capability metadata. Legacy
clients (no `budget` field) get K=1 by default — exactly
v1.0.23's single-shot behaviour, bit-for-bit.

Phases 3 (info-theoretic primitives) and 4 (user-authored
policies) are out of scope for v1.0.24. Phase 3 inherits this
arc's `AdaptiveState` substrate; Phase 4 builds on the
program-shaped binding surface that would extend the
registry interpreter — not in scope here.

---

## 1. The current state (post-v1.0.23)

The coroutine's Stage 2 is single-shot:

```python
# Stage 2: decide on adaptation (single round)
all_turns = {TurnIndex(f.turn_number) for f in finals}
deepen = _dispatch_deepening_set(
    finals, cap_meta, analysis_config, window_size, all_turns,
)
if not deepen:
    for f in finals: yield f  # promote previews
    return
for f in finals:
    if TurnIndex(f.turn_number) not in deepen:
        yield f
deeper = _build_deeper_query(parent, sorted(deepen), q_extra)
async for resp in ctx.spawn(deeper):
    yield resp
```

One call to `_dispatch_deepening_set`, one `ctx.spawn`, done.

The orchestration substrate already supports sequential
`ctx.spawn`s within one coroutine — the framework's depth bound
(`max_depth=4`) governs *nested* orchestration, not sequential
same-depth spawns. Phase 2 leans on this supported-but-unused
capability.

---

## 2. The multi-round substrate

### 2.1 The loop

```python
async def coro(parent, ctx):
    # Stage 1: original finals + preview streaming (unchanged from v1.0.23).
    finals = []
    async for resp in ctx.original_stream():
        # ... process resp, append to finals, emit preview ...

    if not finals: return

    # Stage 2: initialize state + budget.
    all_turns = {TurnIndex(f.turn_number) for f in finals}
    state = AdaptiveState(originals=finals)
    budget = parse_budget(cap_meta)

    # Multi-round loop.
    deepened_so_far: set[TurnIndex] = set()
    while budget.has_capacity(state):
        deepen = _dispatch_deepening_round(
            state, cap_meta, analysis_config, window_size, all_turns,
        )
        if not deepen:
            break  # no more adaptation warranted

        # Spawn deeper query for this round's deepening set.
        deeper = _build_deeper_query(
            parent, sorted(deepen), budget.visits_for_round(),
        )
        async for resp in ctx.spawn(deeper):
            state.observe_response(resp)
            yield resp

        deepened_so_far.update(deepen)
        state.record_round(round_deepen=deepen)

    # Stage 3: emit non-deepened originals as authoritative.
    for f in finals:
        if TurnIndex(f.turn_number) not in deepened_so_far:
            yield f
```

Notes on the shape:

- **Each round computes its own worst-set** from current `state`,
  which includes the prior rounds' deeper-query observations.
  Already-deepened turns may re-enter the worst-set if their
  deeper analysis didn't move them out — KataGo's cache
  continuation means re-deepening adds further visits efficiently.
- **`state.observe_response(resp)`** updates the state as deeper-
  query responses arrive. The next round's selector reads the
  updated state.
- **Stage 3 emits non-deepened originals AFTER the loop terminates.**
  This is a UX-vs-cleanliness trade-off: previews linger for
  non-deepened turns until the multi-round sequence completes,
  but each turn has exactly one authoritative emission (no
  duplicate finals). The v1.0.20 streaming-previews refactor
  established this preview-then-final pattern as the SPA-side
  expectation; lingering previews are within that contract.
- **The single-shot semantics are recovered at K=1** — the budget
  terminates after one round, the loop body runs once, Stage 3
  emits non-deepened originals exactly as v1.0.23 did.

### 2.2 `AdaptiveState` contract

The state object accumulates across-iteration data and exposes a
defined query surface to selectors, value functions, and budget
objects. Framework-owned: `observe_*` / `record_round` are
private; consumers only read.

```python
@dataclass
class AdaptiveState:
    """Across-iteration accumulator for adaptive's multi-round loop.

    Lifetime: one per parent query, constructed at coroutine entry,
    discarded at coroutine completion. Selectors / value functions
    / budget objects read it; the coroutine and the framework
    populate it as rounds progress.
    """
    originals: list[AnalyzeResponse]
    rounds_completed: int = 0
    total_visits_spent: int = 0
    wall_clock_elapsed_s: float = 0.0

    # Per-unit history.
    _selector_history: dict[tuple[Color, MoveIndex], list[float]] = ...
    _selector_history_turn: dict[TurnIndex, list[float]] = ...
    _deepened_counts_move: dict[tuple[Color, MoveIndex], int] = ...
    _deepened_counts_turn: dict[TurnIndex, int] = ...
    _last_packet_by_turn: dict[TurnIndex, AnalyzeResponse] = ...

    # Per-round trajectories of named metrics (worst_selector_value,
    # worst_set_jaccard_to_previous, plus user-authored metrics if
    # present).
    _metric_trajectories: dict[str, list[float]] = ...

    # Per-round deepening sets (for jaccard-like trajectories).
    _round_deepen_sets: list[set[TurnIndex]] = ...

    # ─── Queryable surface (read-only from external callers) ───

    def selector_history_move(
        self, color: Color, m: MoveIndex,
    ) -> list[float]:
        """Selector scalars for this move across rounds in which it
        was scored."""

    def selector_history_turn(self, t: TurnIndex) -> list[float]:
        """Selector scalars for this turn across rounds."""

    def deepened_count_move(self, color: Color, m: MoveIndex) -> int:
        """Number of rounds this move was deepened."""

    def deepened_count_turn(self, t: TurnIndex) -> int:
        """Number of rounds this turn was deepened (turn-axis only)."""

    def last_packet(self, t: TurnIndex) -> Optional[AnalyzeResponse]:
        """Most recent analyze response observed for this turn (either
        original or from a deeper-query response)."""

    def metric_trajectory(self, name: str) -> list[float]:
        """Named metric's value history, one entry per completed round."""
```

The `_selector_history*` and `_metric_trajectories` are populated
by the framework AFTER each round's scoring/selection — selectors
in round K see the trajectory up through round K-1.

### 2.3 `round_history` exposure on views

Selectors author across-iteration logic via a new `round_history`
field on `MoveView` / `TurnView`:

```python
@dataclass(frozen=True)
class MoveRoundHistory:
    """Per-unit history surfaced to a move_selector_fn."""
    selector_values: list[float]   # past rounds' selector values
    deepened: int                  # number of rounds deepened
    previous_packet: Optional[AnalyzeResponse]  # last packet observed
    rounds_completed: int          # global round counter


@dataclass(frozen=True)
class TurnRoundHistory:
    """Per-unit history surfaced to a turn_selector_fn."""
    selector_values: list[float]
    deepened: int
    previous_packet: Optional[AnalyzeResponse]
    rounds_completed: int


@dataclass(frozen=True)
class MoveView:
    color: Color
    move_index: MoveIndex
    deltas: list[float]
    before: AnalyzeResponse
    after: AnalyzeResponse
    round_history: MoveRoundHistory  # NEW in v1.0.24


@dataclass(frozen=True)
class TurnView:
    turn_index: TurnIndex
    to_play: Color
    packet: AnalyzeResponse
    round_history: TurnRoundHistory  # NEW in v1.0.24
```

User selectors author across-iteration logic naturally:

```
# Tighten attention on units already deepened multiple times
selector_fn(x) = base_metric(x) * (1 + 0.3 * x.round_history.deepened)

# Stability-decay: deprioritise units whose selector value moved
# little last round
selector_fn(x) = base_metric(x) * stability_decay(x.round_history.selector_values)
```

In round 1, `round_history.selector_values` is empty and
`deepened` is 0 — selectors that depend on history degrade
gracefully (a `[]`-handling sentinel returns the base metric).

Backwards compatibility: v1.0.23 user expressions that don't
access `round_history` are unaffected. The field is additive; no
breakage.

---

## 3. Budget abstraction

### 3.1 The four shapes

```python
@dataclass
class Budget:
    """Per-query budget; admits multiple simultaneous constraints.

    Terminates the multi-round loop when ANY constraint is
    exhausted. A budget consisting only of a convergence policy
    (no compute cap) is valid — the loop runs until the
    convergence metric stabilises.
    """
    max_rounds: Optional[int] = None
    total_extra_visits: Optional[int] = None
    wall_clock_seconds: Optional[float] = None
    convergence: Optional[ConvergenceCheck | CombinedConvergence] = None
    per_round_extra_visits: int = 800  # default; carries v1.0.23's value

    def has_capacity(self, state: AdaptiveState) -> bool:
        if self.max_rounds is not None and state.rounds_completed >= self.max_rounds:
            return False
        if self.total_extra_visits is not None and state.total_visits_spent >= self.total_extra_visits:
            return False
        if self.wall_clock_seconds is not None and state.wall_clock_elapsed_s >= self.wall_clock_seconds:
            return False
        if self.convergence is not None and self.convergence.is_converged(state):
            return False
        return True

    def visits_for_round(self) -> int:
        """Visits to add to the deeper-query's maxVisits this round.

        Defaults to per_round_extra_visits. Phase 3 may replace this
        with allocation-algorithm-driven per-round visit counts.
        """
        return self.per_round_extra_visits
```

The four constraints are AND-composable: any non-None constraint
that exhausts terminates the loop. A budget with only `max_rounds`
set behaves identically across rounds with no other limits.

### 3.2 Convergence checks

```python
@dataclass(frozen=True)
class ConvergenceCheck:
    """Single tolerance-style convergence check on a named metric."""
    metric: str          # name in AdaptiveState.metric_trajectory
    tolerance: float     # threshold for "moved less than"
    lookback: int = 1    # rounds back to compare against
    scale: Literal["absolute", "relative"] = "absolute"

    def is_converged(self, state: AdaptiveState) -> bool:
        history = state.metric_trajectory(self.metric)
        if len(history) < self.lookback + 1:
            return False
        current = history[-1]
        prior = history[-1 - self.lookback]
        delta = abs(current - prior)
        if self.scale == "absolute":
            return delta < self.tolerance
        # scale == "relative"
        return delta / max(abs(prior), 1e-9) < self.tolerance


@dataclass(frozen=True)
class CombinedConvergence:
    """`all_of` or `any_of` combinator over multiple checks."""
    mode: Literal["all_of", "any_of"]
    checks: list[ConvergenceCheck]

    def is_converged(self, state: AdaptiveState) -> bool:
        if self.mode == "all_of":
            return all(c.is_converged(state) for c in self.checks)
        return any(c.is_converged(state) for c in self.checks)
```

The standard tolerance forms map to the four `ConvergenceCheck`
fields:

| Form | Configuration |
|---|---|
| Absolute on iterate \|x_k - x_{k-1}\| < ε | metric=trajectory, scale="absolute" |
| Relative on iterate \|x_k - x_{k-1}\| / \|x_{k-1}\| < ε | metric=trajectory, scale="relative" |
| Absolute on objective \|f(x_k) - f(x_{k-1})\| < ε | metric=objective-named trajectory, scale="absolute" |
| Patience (no improvement for N rounds) | lookback=N, metric=best-observed |

### 3.3 Framework-default metric trajectories

Two built-in metrics that the framework computes per round, no
user authoring required:

- **`worst_selector_value`** — the minimum selector value in
  this round's worst-set (or the worst-quantile threshold value
  when applicable). Stabilising = the "worst" stops getting
  worse round-over-round.
- **`worst_set_jaccard_to_previous`** — Jaccard similarity of
  this round's deepening set vs the prior round's. Approaching 1
  = the same turns keep getting picked → adaptation has stalled.

User-authored metric trajectories (via `state_fns`-shaped
bindings evaluated per round) are out of scope for v1.0.24;
the framework defaults cover the common convergence shapes.

### 3.4 Context profiles

Three named profiles for ergonomic per-context tuning:

| Profile | Shape |
|---|---|
| `review-tight` | `max_rounds=1`, no other constraints — preserves single-round semantics for review-session queries (turn-locked timing). |
| `range-generous` | `max_rounds=5`, `total_extra_visits=3000`, default convergence (`worst_set_jaccard_to_previous` ≥ 0.9 over lookback=1). Range-based analysis queries from the toolbar. |
| `loop-aggressive` | `wall_clock_seconds=60`, `total_extra_visits=10000`, default convergence. Autonomous-SR-loop scenarios with GPU minutes to spend per card. |

The profile-to-raw expansion happens proxy-side at coroutine
entry; the SPA may author the raw shape when ad-hoc profiles
are needed.

---

## 4. Wire shape

### 4.1 `capabilities.adaptive_reevaluate.budget`

Additive optional field. String value names a profile; object
value carries a raw `Budget` shape.

```json
"capabilities": {
  "adaptive_reevaluate": {
    "worst_quantile": 0.25,
    "extra_visits": 800,
    "budget": "range-generous"
  }
}
```

Or raw shape:

```json
"capabilities": {
  "adaptive_reevaluate": {
    "worst_quantile": 0.25,
    "extra_visits": 800,
    "budget": {
      "max_rounds": 5,
      "total_extra_visits": 3000,
      "convergence": {
        "metric": "worst_set_jaccard_to_previous",
        "tolerance": 0.1,
        "lookback": 1,
        "scale": "absolute"
      }
    }
  }
}
```

Combined convergence:

```json
"budget": {
  "max_rounds": 10,
  "convergence": {
    "all_of": [
      {"metric": "worst_selector_value", "tolerance": 0.05},
      {"metric": "worst_set_jaccard_to_previous", "tolerance": 0.1}
    ]
  }
}
```

### 4.2 Existing fields unchanged

`worst_quantile`, `extra_visits`, `window_size`, `selection_policy`,
`selector_axis`, `move_selector_fn` / `turn_selector_fn` bindings
in `analysis_config.bindings` — all unchanged. Per-round
behaviour: same selector + same selection policy applied to each
round's worst-set computation.

### 4.3 Configuration-consistency refusals extend naturally

The `AdaptiveConfigurationError` family established in v1.0.23
gains a new code:

- `budget_invalid` — budget shape is malformed (unknown profile
  name, invalid constraint type, convergence metric not in
  framework defaults, invalid combinator). Detail names the
  specific issue.

Per the §11.4 cost-asymmetry calibration: a malformed budget
would trigger expensive compute on conflated intent → refuse
clearly.

---

## 5. Defaults and backwards compatibility

When no `budget` field is set, the budget defaults to
`max_rounds=1` with no other constraints — exact v1.0.23
single-shot semantics. Wire-compatible in both directions:
legacy clients see today's behaviour bit-for-bit.

When a `budget` profile string names an unknown profile, the
dispatch refuses with `budget_invalid`. Per §11.4: silent
fallback to a default would mask the configuration error.

The `round_history` field on views is always present (in round
1 it carries empty lists / zero counts). User selectors that
read `round_history` get sensible defaults; user selectors
that don't access `round_history` are unaffected.

### Behaviour matrix

| Configuration | Effective shape |
|---|---|
| No `budget` field | `max_rounds=1`; equivalent to v1.0.23 single-shot |
| `budget: "review-tight"` | Same as above (K=1 named explicitly) |
| `budget: "range-generous"` | K up to 5, total visits ≤ 3000, default convergence |
| `budget: "loop-aggressive"` | Wall-clock ≤ 60s, total visits ≤ 10000, default convergence |
| `budget: {max_rounds: 3}` | Three rounds, no other limits |
| `budget: {convergence: {metric: ..., tolerance: ...}}` | Convergence-driven only; no compute cap |
| `budget: "unknown-profile"` | **Refused** — `AdaptiveConfigurationError(code="budget_invalid")` |

---

## 6. The migration arc

Seven commits on `feat/multi-round-adaptation`, ordered so each
preserves `mypy --strict` + the existing test suite green.

### Commit 1 — `AdaptiveState` class (additive)

New module-level dataclass in
`middleware/adaptive_reevaluate.py`:

- `AdaptiveState` with the queryable surface from §2.2.
- `MoveRoundHistory` / `TurnRoundHistory` frozen dataclasses.
- Framework-side population methods (`observe_originals`,
  `observe_response`, `record_round`).
- No consumers yet; additive only.

### Commit 2 — Budget abstraction (additive)

- `Budget` dataclass with the four-constraint shape from §3.1.
- `ConvergenceCheck` / `CombinedConvergence` per §3.2.
- `_parse_budget(cap_meta)` factory parsing profile strings or
  raw shapes; raises `AdaptiveConfigurationError(code="budget_invalid")`
  on malformed input.
- The three profile constants (`review-tight`, `range-generous`,
  `loop-aggressive`).
- No consumers yet; additive only.

### Commit 3 — `round_history` field on views (additive)

- `MoveView` / `TurnView` gain a `round_history` field of type
  `MoveRoundHistory` / `TurnRoundHistory`.
- `_build_move_views` / `_build_turn_views` updated to construct
  the history field from `state` (always present in v1.0.24;
  pre-Phase-2 the state was implicit / unused).
- Existing v1.0.23 selector test fixtures get a small update —
  views now require the history field. The construction sites
  in tests are updated.
- No behaviour change in production (the dispatch's view
  construction is purely additive on the wire); existing v1.0.23
  user selectors that don't read `round_history` see no change.

### Commit 4 — Multi-round coroutine refactor

The substantive integration commit. The `coro` Stage 2+ refactors
to the multi-round loop from §2.1:

- `AdaptiveState` constructed at coroutine entry, populated by
  `observe_originals(finals)`.
- `Budget` parsed from `cap_meta`.
- `while budget.has_capacity(state)`: round loop.
- Inside the loop: `_dispatch_deepening_round` (a thin wrapper
  around `_dispatch_deepening_set` that threads `state` for
  the round_history construction); deeper query spawn; state
  observation; round recording.
- Stage 3 emits non-deepened originals after the loop.
- K=1 default behaviour preserved bit-for-bit (the loop body
  runs once; the deepening set is computed against the
  initial state; the spawn matches v1.0.23's exact shape).

### Commit 5 — Framework-default metric trajectories

- `state.record_round` populates `metric_trajectories` with the
  two framework defaults: `worst_selector_value`,
  `worst_set_jaccard_to_previous`.
- Convergence checks against these metrics now have data to
  consult.
- Wire-shape verification: a convergence-based budget terminates
  the loop when the metric stops moving.

### Commit 6 — Wall-clock budget plumbing

- `state.wall_clock_elapsed_s` populated by the coroutine
  (`time.monotonic()` at coroutine entry, sampled at round
  boundaries).
- Wall-clock convergence becomes operational.

This commit is small but lands in a discrete piece because the
wall-clock plumbing touches the coroutine's timing surface;
keeping it separate from Commit 4's structural refactor makes
review cleaner.

### Commit 7 — Tests

New `tests/test_multi_round_adaptation.py` covering:

- `AdaptiveState` queryable surface — fields, history methods,
  metric trajectory accumulation.
- `Budget` parsing — raw shapes, profile strings, malformed
  shape refusal with `budget_invalid` code.
- `ConvergenceCheck` / `CombinedConvergence` — convergence
  detection under various trajectories.
- Multi-round dispatch — synthetic finals + state + budget;
  assert per-round worst-set computation; assert state
  accumulation across rounds.
- K=1 default — single-round behaviour bit-for-bit equivalent
  to v1.0.23's `_dispatch_deepening_set`.
- Convergence-based termination — synthetic state with a
  stabilising metric trajectory; assert the loop terminates at
  the expected round.
- Round-history exposure — a user selector that reads
  `x.round_history.deepened` produces the expected per-unit
  score modulation across rounds.
- Wall-clock budget — fast-time-elapse via mocked
  `time.monotonic`; assert termination.

Plus a small extension to existing `test_adaptive_selector_pluggability.py`'s
`TestDispatchEndToEnd` if needed to verify the new state-aware
dispatch is compatible with v1.0.23's user-selector test fixtures.

---

## 7. Test discipline

Per the v1.0.21 / v1.0.22 / v1.0.23 precedent:

1. **Existing suite must remain green at every commit.**
2. **The mypy CI gate enforces typing.** No new `# type: ignore`.
3. **Regression tests in `tests/test_multi_round_adaptation.py`.**
4. **Default-path tests in `tests/test_adaptive_selector_pluggability.py`
   and `tests/test_adaptive_cache_matrix.py` continue to pass
   unchanged** — verifying K=1 single-round preserves v1.0.23's
   behaviour.
5. **Bit-for-bit preservation at K=1** is the validation gate
   for Commit 4. The dispatch path with no `budget` field set
   produces the same deepening turns and the same spawn
   sequence as the pre-refactor `_dispatch_deepening_set`.

---

## 8. Design calls

### 8.1 `AdaptiveState` is framework-owned

Selectors / value functions / budget objects read it; they do
not write it. Mutation methods (`observe_originals`,
`observe_response`, `record_round`) are private to the
coroutine and the framework's dispatch helpers. This mirrors the
v1.0.23 dispatch's discipline: framework owns lifecycle and
mutation; user code reads and authors expressions.

### 8.2 Per-round behaviour is uniform across rounds

The same selector, same selection policy, same window-size
apply to every round. Multi-round = "do this scoring +
selection K times with state updating between rounds." A
future Phase 4 user-authored policy could vary the selection
shape across rounds; v1.0.24 doesn't admit this.

### 8.3 Stage 3 emits non-deepened originals AFTER the loop

The v1.0.20 streaming-previews refactor established that
previews linger for un-finalised turns; this arc extends the
"preview lingering" through the entire multi-round sequence
for non-deepened turns. UX trade-off: previews linger longer
for never-deepened turns vs the simpler "each turn has
exactly one authoritative emission" property. Recommendation:
accept the trade-off; the lingering is bounded by the budget
which the user explicitly authored.

### 8.4 Re-deepening a previously-deepened turn is allowed

If a turn enters the worst-set in round 1 (deepened) and again
in round 2 (still worst even after deepening), the round-2
deeper query includes it. KataGo's cache continuation
(maxVisits picks up from prior count) makes this efficient.
Each round's deeper query has its own `extra_visits` added on
top of `original_maxVisits` — so re-deepening accumulates
visits across rounds.

### 8.5 Framework-default convergence metrics only

v1.0.24 ships two framework-default metrics
(`worst_selector_value`, `worst_set_jaccard_to_previous`)
populated by `state.record_round`. User-authored metric
trajectories via `state_fns`-equivalent bindings are out of
scope — the framework defaults cover the natural convergence
shapes; richer authoring is Phase 3+ territory.

### 8.6 `budget_invalid` is the fifth `AdaptiveConfigurationError` code

Extending v1.0.23's four codes (`ambiguous_axis`,
`axis_binding_mismatch`, `policy_axis_mismatch`,
`policy_parameters_invalid`) with `budget_invalid`. Same
hard-refusal discipline per §11.4: a malformed budget would
trigger expensive compute on conflated intent → refuse with
structured error.

### 8.7 Standard submodule release arc

Per `proxy/CLAUDE.md`'s "Submodule release arc" section.
Branch + PR + tag v1.0.24. The umbrella pointer bump remains
deferred per the umbrella's 2026-05-18 decision until the full
adaptive-widening arc lands (Phase 3 territory remains).

---

## 9. Sunsetting

This memo is `design-note: planned`. When the seven commits land
and v1.0.24 is tagged, the memo transitions to
`design-note: implemented` with implementation notes inline
(format per the v1.0.21 / v1.0.22 / v1.0.23 post-implementation
annotation pattern).

Phase 3 (information-theoretic primitives) builds on this arc's
`AdaptiveState` substrate. Phase 4 (user-authored policies)
builds on the substrate plus a registry-interpreter extension
for program-shaped bindings — both subsequent arcs.

---

## 10. Related documents

- `LengYue:docs/notes/adaptive-reevaluate-widening-plan.md` —
  the umbrella design note this roadmap implements Phase 2 of.
  Sections "Phase 2 — Multi-round adaptation + budget
  abstraction" and "Across-iteration policies" together
  describe what this arc delivers.
- `proxy/docs/roadmap-adaptive-selector-pluggability.md`
  (v1.0.23) — the selector-pluggability predecessor; this arc
  inherits its substrate (MoveView / TurnView, selection
  policies, AdaptiveConfigurationError discipline, the
  `_dispatch_deepening_set` shape that gets wrapped in a loop).
- `proxy/docs/roadmap-adaptive-type-branding.md` (v1.0.22) —
  the type-branding foundation; MoveIndex / TurnIndex /
  `move_to_turn_pair` continue to underpin every reference here.
- `proxy/docs/roadmap-orchestration-middleware.md` (v1.0.16) —
  the substrate `ctx.spawn` runs on; sequential same-depth
  spawning that v1.0.24 leans on is supported by the framework
  but unused pre-Phase-2.
- `proxy/middleware/adaptive_reevaluate.py` — the file the
  migration modifies.
- `proxy/CLAUDE.md` §"Type-driven design (within Python's
  limits)" — the existing discipline this arc extends.
