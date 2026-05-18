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
abstraction, an across-iteration state object, and a finalization
stage that emits each turn's authoritative final at end-of-loop.
v1.0.23's selector pluggability + window correction stay in
place; this arc wraps them in a loop and exposes round-history
to user selectors via the view object's `round_history` field.

Six new pieces:

1. **The multi-round loop** in the coroutine's Stage 2+. Each
   iteration is one round of select-and-deepen. Each round's
   worst-set is computed from current state and can vary across
   rounds (re-deepening already-deepened turns, or picking up
   newly-worst turns as the state shifts).
2. **`AdaptiveState`** object — accumulates per-round per-unit
   data, including each turn's latest observed response.
   Framework-owned, queryable from selectors / value functions /
   budget objects.
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
6. **Finalization stage** — at end-of-loop (whatever the
   termination cause), the proxy emits each turn's latest
   observed response with `is_during_search=False`. This is the
   single authoritative emission per turn that the KataGo
   protocol contract requires; during the multi-round loop
   every emission carries `is_during_search=True` (preview).
   The finalization-at-end mechanism makes the protocol
   contract hold uniformly under any budget shape and allows
   the worst-set to vary across rounds without violation.

Plus context-dependent budget profiles (`review-tight` /
`range-generous` / `loop-aggressive`) layered over raw budget
shapes for ergonomic per-context tuning.

Pattern parallels v1.0.23 in shape (focused multi-commit arc on
a feature branch) but is bigger in substrate surface. The wire
shape gains a `budget` field on capability metadata. Legacy
clients (no `budget` field) get K=1 by default. The wire shape
at K=1 differs slightly from v1.0.23 single-shot by +1 preview
emission per deepened turn — see §5 for details.

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
    # Stage 1: original finals + preview streaming. Each KataGo
    # final is recorded in state (as the initial "latest" per
    # turn) AND emitted to the client as a preview. Partials and
    # metadata pass through unchanged.
    state = AdaptiveState()
    finals: list[AnalyzeResponse] = []
    async for resp in ctx.original_stream():
        if isinstance(resp, MetadataResponse):
            yield resp
            continue
        if resp.is_during_search:
            yield resp
            continue
        finals.append(resp)
        state.observe(resp)
        yield replace(resp, is_during_search=True)

    if not finals: return

    # Stage 2: budget + multi-round loop.
    budget = parse_budget(cap_meta)
    all_turns = {TurnIndex(f.turn_number) for f in finals}

    while budget.has_capacity(state):
        # Compute this round's worst-set from current state. The
        # worst-set can vary across rounds: re-include
        # already-deepened turns (re-deepening at higher visit
        # counts) or include newly-worst turns whose state
        # shifted relative to peers in prior rounds.
        deepen = _dispatch_deepening_round(
            state, cap_meta, analysis_config, window_size, all_turns,
        )
        if not deepen:
            break  # no more adaptation warranted

        # Spawn deeper query for this round's deepening set. Each
        # KataGo final from the deeper query is recorded in state
        # (overwriting the prior "latest" for that turn) AND
        # emitted to the client as a PREVIEW (is_during_search=True).
        # Partials pass through. No buffering: each KataGo
        # response is immediately emitted to the client with
        # appropriate field edits.
        deeper = _build_deeper_query(
            parent, sorted(deepen), budget.visits_for_round(),
        )
        async for resp in ctx.spawn(deeper):
            if isinstance(resp, MetadataResponse):
                yield resp
                continue
            if resp.is_during_search:
                yield resp
                continue
            state.observe(resp)
            yield replace(resp, is_during_search=True)

        state.record_round(round_deepen=deepen)

    # Stage 3 — finalization. Emit each turn's latest observed
    # response with is_during_search=False. This is the single
    # authoritative emission per turn the KataGo protocol contract
    # requires (exactly one is_during_search=False per turn per
    # query). The data duplicates the latest preview emission for
    # that turn modulo the flag; acknowledged-fine per §8.3.
    for f in finals:
        turn = TurnIndex(f.turn_number)
        latest = state.last_packet(turn) or f
        yield replace(latest, is_during_search=False)
```

Notes on the shape:

- **Each round computes its own worst-set** from current `state`,
  which carries the latest observed response per turn (from
  Stage 1's originals or any prior round's deeper-query
  responses). Already-deepened turns may re-enter the worst-set
  if their deeper analysis didn't move them out — KataGo's
  cache continuation makes re-deepening at progressively higher
  maxVisits efficient. Turns not deepened in round 1 may enter
  later rounds' worst-sets as the state shifts.
- **`state.observe(resp)`** is called for every KataGo final the
  proxy receives (originals in Stage 1 and deeper-query
  responses in the loop). Records `resp` as the latest for
  `TurnIndex(resp.turn_number)`. The next round's selector
  reads the updated state via the per-turn / per-move
  accessors; the finalization stage reads
  `state.last_packet(turn)` to emit the authoritative.
- **Finalization at end-of-loop emits each turn's latest with
  `is_during_search=False`.** This is the single authoritative
  emission per turn the protocol contract requires. The
  finalization runs regardless of why the loop terminated
  (budget exhausted, convergence, "no more to deepen") — the
  protocol contract holds uniformly. The "duplicate modulo
  is_during_search" pattern is named-and-acknowledged in §8.3.
- **No buffering of KataGo responses.** Each KataGo response is
  emitted to the client immediately (with `is_during_search`
  edited to True). The finalization emission is a NEW emission
  per turn — duplicating the preview's data modulo the flag.
  `state.last_packet` provides a reference to the latest data
  for the finalization stage; this is short-term retention for
  the field-edit decision, not delayed emission.

**Per-turn emission accounting:**

A turn deepened in K rounds:
- Stage 1: 1 preview (from KataGo's original final)
- Per round in worst-set: 1 preview (from deeper-query final)
- Finalization: 1 emission with `is_during_search=False`
- Total: K + 2 proxy emissions; exactly one `is_during_search=False`. ✓

A turn never deepened:
- Stage 1: 1 preview
- Finalization: 1 emission with `is_during_search=False`
- Total: 2 proxy emissions; exactly one `is_during_search=False`. ✓

Protocol contract holds uniformly.

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
`max_rounds=1` with no other constraints. The coroutine runs
one round of select-and-deepen and then enters the finalization
stage. Each turn receives the wire shape characteristic of K=1
multi-round (preview during the round; one finalization emission
with `is_during_search=False`).

**Vs v1.0.23 single-shot, wire-shape diff:** The wire shape
differs from v1.0.23 by +1 preview emission per deepened turn.
v1.0.23 emitted each deepened turn's deeper-query final directly
with `is_during_search=False` (one emission per deepened turn,
serving both as preview-completion and authoritative-final).
v1.0.24 emits the deeper-query final as a preview
(`is_during_search=True`), then re-emits at the finalization
stage with `is_during_search=False`. Per turn, two emissions
where v1.0.23 had one. Non-deepened turns are unchanged
(one preview from Stage 1, one finalization emission — matches
v1.0.23 exactly).

The +1 preview per deepened turn is the "duplicate modulo
`is_during_search`" pattern named in §8.3; protocol-conformant
and acknowledged client-acceptable. The release annotation
names this divergence.

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
- Framework-side population methods: `observe(resp)` records a
  KataGo final as the latest for its turn (called from Stage 1
  for originals and from the loop for deeper-query responses);
  `record_round(round_deepen)` increments round counters and
  records the round's deepening set for metric-trajectory
  computation.
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

- `AdaptiveState` constructed at coroutine entry. Stage 1's
  per-original `state.observe(resp)` calls record originals as
  initial "latest per turn"; the per-round loop's
  `state.observe(resp)` calls overwrite the latest as deeper
  responses arrive.
- `Budget` parsed from `cap_meta`.
- `while budget.has_capacity(state)`: round loop.
- Inside the loop: `_dispatch_deepening_round` (a thin wrapper
  around `_dispatch_deepening_set` that threads `state` for
  the round_history construction); deeper query spawn; each
  KataGo final from the spawn emitted as a preview
  (`is_during_search=True`) and recorded via `state.observe`;
  round recording.
- After the loop: finalization stage — for each turn in
  `finals`, yield `replace(state.last_packet(turn) or
  original_final, is_during_search=False)`. The single
  authoritative emission per turn per the protocol contract;
  duplicates the latest preview emission modulo the flag.
- K=1 default behaviour: one round of select-and-deepen, then
  finalization. Wire shape differs from v1.0.23 by +1 preview
  emission per deepened turn (per §5 / §8.3).

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
- K=1 default — one round of select-and-deepen + finalization;
  wire shape carries +1 preview emission per deepened turn vs
  v1.0.23, with one finalization emission per turn carrying
  `is_during_search=False`. Assert the per-turn emission
  accounting matches §2.1's table.
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
not write it. Mutation methods (`observe`, `record_round`)
are private to the
coroutine and the framework's dispatch helpers. This mirrors the
v1.0.23 dispatch's discipline: framework owns lifecycle and
mutation; user code reads and authors expressions.

### 8.2 Per-round behaviour is uniform across rounds

The same selector, same selection policy, same window-size
apply to every round. Multi-round = "do this scoring +
selection K times with state updating between rounds." A
future Phase 4 user-authored policy could vary the selection
shape across rounds; v1.0.24 doesn't admit this.

### 8.3 Finalization stage emits each turn's latest at end-of-loop

Each KataGo response is emitted immediately to the client as a
preview (`is_during_search=True`); the proxy retains a
reference to each turn's latest observed response in
`AdaptiveState`. When the multi-round loop terminates
(whatever the termination cause), the finalization stage
re-emits each turn's latest response with
`is_during_search=False` — the single authoritative emission
per turn the KataGo protocol contract requires.

The finalization emission produces a "duplicate modulo
`is_during_search`" wire pattern: the SPA receives the same
data once as a preview during the loop and once as the
authoritative at the end. The duplicate is acknowledged-fine on
protocol-correctness grounds — the protocol mandates exactly
one `is_during_search=False` per turn per query, which the
finalization stage provides uniformly.

What the finalization-at-end design enables:

- **Worst-set varies per round.** A turn deepened in round 1
  can re-enter round 2's worst-set for further deepening; a
  turn not deepened in round 1 can be picked up in round 2 as
  the state shifts. No protocol violation arises because the
  in-loop emissions are previews; only the finalization emits
  finals.
- **All four budget shapes work uniformly.** Convergence and
  wall-clock no longer need special-case handling — the
  finalization happens at end-of-loop regardless of why the
  loop ended.
- **No buffering of KataGo responses.** Each KataGo response
  flows immediately to the client; the `state.last_packet`
  retention is short-term reference for the finalization
  emission, not delayed pass-through.

Wire-shape divergence vs v1.0.23 at K=1 is +1 preview emission
per deepened turn (the deeper-query response is now emitted
as a preview rather than directly as a final, with the
finalization emission providing the authoritative). See §5 for
the full per-turn accounting.

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
