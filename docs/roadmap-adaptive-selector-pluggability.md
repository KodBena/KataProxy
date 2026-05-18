# `adaptive_reevaluate` selector pluggability + window correction — design roadmap

- **Status:** `design-note: planned` (per umbrella ADR-0005 Rule 8
  doc-graph genre vocabulary)
- **Date:** 2026-05-18
- **Scope:** `proxy/middleware/adaptive_reevaluate.py`,
  `proxy/registry_interpreter.py` (new binding-role resolution),
  and a small selection-policies surface (curated set of four).
  Builds on the v1.0.22 branded substrate.
- **Origin:** Lands the behavioural piece of the umbrella's
  adaptive-widening design note
  (`LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`)
  on top of v1.0.22's branded substrate. The umbrella note's
  open questions resolved to their proposed defaults (per the
  2026-05-18 review).
- **Authoritative for the `feat/adaptive-selector-pluggability`
  branch;** superseded by the v1.0.23 release notes once tagged.

---

## TL;DR

This arc replaces `adaptive_reevaluate`'s hardcoded
"mean-of-policy-deltas with per-color quantile selection" policy
with a pluggable substrate that exposes two co-equal first-class
axes:

- **Move-based selectors** (`move_selector_fn`) — operate on a
  transition between two positions. Per-move-per-color scalar;
  natural for move-loss metrics. The recovered default.
- **Turn-based selectors** (`turn_selector_fn`) — operate on a
  single position. Per-turn scalar; natural for policy entropy,
  score variance, ownership flux.

Both authored as expressions in `analysis_config.symbols`, bound
under the new role names in `analysis_config.bindings`. Selection
policy (per-color quantile / pooled quantile / per-color threshold
/ top-k) is named separately in capability metadata; defaults
match each axis's most natural pairing.

The window correction lands as a bundled change: replace the
current symmetric turn-space `±half` expansion with a same-color
predecessor expansion in move-space, applied to move-based
selectors only. Hard-flipped (no opt-in metadata) per the
umbrella note's recommendation.

Pattern parallels v1.0.22 in shape (a focused multi-commit arc
on a feature branch) but is bigger in surface — new view
dataclasses, new selection policies, registry-interpreter
extension, coroutine dispatch refactor. The wire shape stays
additive: legacy clients (no new bindings) see exactly the
current adaptive behaviour modulo the window-correction flip.

Phase 2+ widenings of the umbrella note (multi-round adaptation,
budget abstraction, info-theoretic primitives, user-authored
policies) are out of scope for v1.0.23.

---

## 1. The current state (post-v1.0.22)

`_find_worst_turns` is hardcoded to compute mean policy delta per
move-per-color, then apply per-color quantile selection. Three
brand-threaded helpers live in `middleware/adaptive_reevaluate.py`:

```python
def _find_worst_turns(
    responses: List[AnalyzeResponse], quantile: float,
) -> list[TurnIndex]:
    # Per-color deltas → per-color quantile → flat turn list
    # via move_to_turn_pair.
    ...

def _expand_window(
    worst_turns: list[TurnIndex],
    all_turns: set[TurnIndex],
    window_size: int,
) -> set[TurnIndex]:
    # Symmetric turn-space ±half expansion (color-blind).
    ...

def _build_deeper_query(
    orig: KataGoQuery, turns: list[TurnIndex], extra_visits: int,
) -> KataGoQuery:
    # Build sub-query at original_max + extra_visits.
    ...
```

v1.0.23 keeps `_build_deeper_query` unchanged, refactors
`_find_worst_turns` into a selector-dispatch shape (with the
hardcoded policy recovered as the default), and replaces
`_expand_window` with a move-space same-color-predecessor
expansion that applies to move-based selectors only.

`RegistryInterpreter` (in `registry_interpreter.py`) compiles
user-authored expressions and resolves named bindings. The three
existing binding roles (`delta_fn`, `summary_fn`, `state_fns`)
gain two siblings: `move_selector_fn` and `turn_selector_fn`.

---

## 2. The two selector contracts

### 2.1 The view dataclasses

User-authored selectors receive a typed view of the current unit:

```python
@dataclass(frozen=True)
class MoveView:
    """The per-move view a move_selector_fn receives.

    Carries the brand (color + move_index) plus the per-arrival
    deltas and references to the before/after analyze packets so
    the user expression can compute transition-shaped metrics
    (policy delta aggregations, score-lead drop, played-policy
    divergence, etc.).
    """
    color: Color
    move_index: MoveIndex
    deltas: list[float]  # per-arrival policy deltas for this move
    before: AnalyzeResponse  # state at the position before the move
    after: AnalyzeResponse  # state at the position after the move


@dataclass(frozen=True)
class TurnView:
    """The per-turn view a turn_selector_fn receives.

    Carries the position index, the side-to-play at this position,
    and the analyze response. Turn-based metrics (policy entropy,
    score variance via state_fns precomputation, ownership flux)
    operate on a single packet without transition context.
    """
    turn_index: TurnIndex
    to_play: Color  # whose turn it is at this position
    packet: AnalyzeResponse
```

The views are constructed by the framework from `finals` (the
accumulated original analyze responses) and the per-move deltas
extracted from `extra.<color>.deltas`. The user expression
consumes one field-set; the framework owns the construction.

### 2.2 The selector signatures

In `RegistryInterpreter` terms:

```python
# Bound under analysis_config.bindings.move_selector_fn:
def move_selector_fn(x: MoveView) -> float: ...
# Bound under analysis_config.bindings.turn_selector_fn:
def turn_selector_fn(x: TurnView) -> float: ...
```

Lower returned scalar = worse. The framework collects scalars,
applies the selection policy, and constructs the worst-set.

### 2.3 Default selector when no binding is named

The recovered default (legacy / no-metadata path): move-based,
mean-of-deltas, per-color quantile. Wire-compatible — clients
that don't author a selector binding see exactly the v1.0.22
behaviour (modulo the window correction).

The default lives as a framework-internal selector (not authored
via `analysis_config.symbols`); call site uses it when neither
binding is set.

---

## 3. Selection policies (curated set)

Four named policies are first-class metadata choices. The
default depends on the selector axis:

| Policy name | Operates on | Parameter | Move-based | Turn-based |
|---|---|---|---|---|
| `per_color_quantile` | (Color, MoveIndex, scalar) tuples | `worst_quantile` | **default** | N/A |
| `pooled_quantile` | (T, scalar) tuples (T = unit type) | `worst_quantile` | available | **default** |
| `per_color_threshold` | (Color, MoveIndex, scalar) tuples | `black_threshold`, `white_threshold` | available | N/A |
| `top_k` | (T, scalar) tuples | `top_k` (int) | available | available |

The wire-shape: `capabilities.adaptive_reevaluate.selection_policy`
names one of the four; parameter fields (`worst_quantile`,
`top_k`, `black_threshold`, `white_threshold`) ride alongside.
Absent → axis-default. `worst_quantile` keeps its existing wire
field name for backward-compat.

The four are deliberately a curated set, not an open vocabulary:
they cover the use cases the design space surveys cleanly. The
escape hatch for "I want a different selection rule" is the
Phase 4 user-authored-policy substrate (out of scope here).

---

## 4. `RegistryInterpreter` extension

Two new binding-role accessors on `RegistryInterpreter`:

```python
def get_move_selector_fn(self) -> Optional[Callable[[Any], Any]]:
    return self.resolve_binding("move_selector_fn") if "move_selector_fn" in self.bindings else None

def get_turn_selector_fn(self) -> Optional[Callable[[Any], Any]]:
    return self.resolve_binding("turn_selector_fn") if "turn_selector_fn" in self.bindings else None
```

`Optional` shape — `None` when the binding is absent, allowing
the adaptive coroutine to fall back to the hardcoded default
without raising. The pattern differs from the existing
`get_delta_fn` / `get_summary_fn` (which return the
zero-stub fallback on missing binding); adaptive needs to
distinguish "not bound" from "bound to something that returns
zero" because the dispatch logic is axis-conditional. The
explicit `Optional` makes that distinction visible.

The expression-substrate security boundary (curated stdlib, no
arbitrary callables, refused-dtype gates, the
`apply_window` higher-order combinator restricted to
asteval-compiled procedures) holds unchanged. The two new
binding roles ride the same audited surface; no new attack
surface.

---

## 5. Dispatch in the coroutine

The Stage 2 logic in `coro` refactors. Today:

```python
# Stage 2: decide on adaptation.
all_turns: set[TurnIndex] = {TurnIndex(f.turn_number) for f in finals}
worst = _find_worst_turns(finals, q_quantile)
deepen = _expand_window(worst, all_turns, window_size)
```

Post-v1.0.23 (sketched — concrete shape lands in implementation):

```python
# Stage 2a: resolve the selector axis and the selection policy.
selector_axis, selector_fn = _resolve_selector(analysis_config, cap_meta)
selection_policy = _resolve_selection_policy(cap_meta, selector_axis)

# Stage 2b: build the views and score each unit.
if selector_axis == "move":
    move_views = _build_move_views(finals, deltas_by_color_and_move)
    scored = [(v.color, v.move_index, selector_fn(v)) for v in move_views]
    worst_units: list[tuple[Color, MoveIndex]] = selection_policy.apply_move(scored)
    deepen = _expand_window_same_color(worst_units, all_turns, q_window_size)
else:
    # selector_axis == "turn"
    turn_views = _build_turn_views(finals)
    scored = [(v.turn_index, selector_fn(v)) for v in turn_views]
    worst_units: list[TurnIndex] = selection_policy.apply_turn(scored)
    deepen = set(worst_units)  # no window for turn-based in v1.0.23
```

The framework owns: view construction, the selector invocation,
the selection policy application, and the move↔turn translation
(via `move_to_turn_pair`). The user owns: the selector
expression itself and the choice of selection policy / parameters
via metadata.

The default path (no selector binding set) skips the view
construction and uses the hardcoded mean-of-deltas selector
directly — preserves v1.0.22's perf on the legacy path.

---

## 6. Window correction (move-space)

Replace `_expand_window` with `_expand_window_same_color` for
move-based selectors. For window size N, each worst move
contributes its own (before, after) turn pair plus the same
pair for its (N-1) same-color predecessors.

```python
def _expand_window_same_color(
    worst_pairs: list[tuple[Color, MoveIndex]],
    all_turns: set[TurnIndex],
    window_size: int,
) -> set[TurnIndex]:
    """Same-color predecessor expansion in move-space.

    For window_size N: each worst move m contributes turns
    move_to_turn_pair(color, m), plus the same for moves
    m-1, m-2, ..., m-(N-1) of the same color. Out-of-range
    predecessors (negative move-index, or turn-index not in
    all_turns) are dropped. Default window_size=2 — the move
    plus its immediate same-color predecessor.
    """
    expanded: set[TurnIndex] = set()
    for color, m in worst_pairs:
        m_int = int(m)
        for offset in range(window_size):
            pred_int = m_int - offset
            if pred_int < 0:
                break
            pred = MoveIndex(pred_int)
            before, after = move_to_turn_pair(color, pred)
            if before in all_turns:
                expanded.add(before)
            if after in all_turns:
                expanded.add(after)
    return expanded
```

Default `window_size=2` (the move plus its immediate same-color
predecessor) replaces the v1.0.22 default of `window_size=3`
(symmetric ±1 turn-space). The defaults differ because the
semantics differ — the size-3 in turn-space and size-2 in
move-space deliberately don't map 1:1; size-2 in move-space is
the smaller and more honest neighborhood.

The pre-correction `_expand_window` is removed entirely. Turn-
based selectors don't get a window in v1.0.23 (selectors author
any cross-turn aggregation they want via `apply_window` in the
expression substrate); the function isn't kept around as
turn-space window machinery.

Behavior change visible at the wire for clients with adaptive
engaged. The release annotation names the change.

---

## 7. Wire shape

### 7.1 `analysis_config.bindings`

Two new binding roles, both optional:

```json
"analysis_config": {
  "bindings": {
    "delta_fn": "default_delta_fn",
    "state_fns": {...},
    "summary_fn": "default_summary_fn",
    "move_selector_fn": "my_score_drop_metric"
  },
  "symbols": {
    "my_score_drop_metric": "score_lead(x.after) - score_lead(x.before)"
  }
}
```

(`turn_selector_fn` is the parallel role for turn-based.)

### 7.2 `capabilities.adaptive_reevaluate`

Three new optional fields:

```json
"capabilities": {
  "adaptive_reevaluate": {
    "worst_quantile": 0.25,
    "extra_visits": 800,
    "window_size": 2,
    "selection_policy": "per_color_quantile",
    "selector_axis": "move"
  }
}
```

`window_size` keeps its name but its semantics shifts: it is now
the same-color-predecessor-count (move-space) rather than the
symmetric turn-space half-window. Default changes from 3 to 2.

`selection_policy` (optional) names one of the four curated
strategies. Absent → axis default.

`selector_axis` (optional) disambiguates when both
`move_selector_fn` and `turn_selector_fn` are bound (rare).
Absent → the axis whose binding is present wins; if neither,
hardcoded default; if both without disambiguator, the
implementation logs a warning and falls back to move-based.

### 7.3 Top-k and threshold parameters

For `selection_policy: "top_k"`:

```json
"capabilities": {
  "adaptive_reevaluate": {
    "selection_policy": "top_k",
    "top_k": 5
  }
}
```

For `selection_policy: "per_color_threshold"`:

```json
"capabilities": {
  "adaptive_reevaluate": {
    "selection_policy": "per_color_threshold",
    "black_threshold": -0.05,
    "white_threshold": -0.05
  }
}
```

Per-policy parameters are present alongside `selection_policy`;
the dispatch matches policy-name to expected parameter set and
raises on mismatch (per ADR-0002 — silent ignoring of malformed
metadata would be a fail-loud violation).

---

## 8. Defaults and backwards compatibility

Behaviour matrix for absent / present metadata combinations:

| `move_selector_fn` | `turn_selector_fn` | `selection_policy` | Effective shape |
|---|---|---|---|
| absent | absent | absent | Hardcoded default: move-based, mean-of-deltas, per-color quantile. |
| present | absent | absent | User move selector, per-color quantile (move-default). |
| absent | present | absent | User turn selector, pooled quantile (turn-default). |
| present | absent | named | User move selector, named policy. |
| absent | present | named | User turn selector, named policy. |
| present | present | (any) | Disambiguator (`selector_axis`) required; warn + move-based fallback. |

The one behavioural change visible at the wire for all clients
is the window correction (move-space replaces turn-space). The
default `window_size` value changes (3 → 2) to match the new
semantics; clients that explicitly set `window_size=3` get
"three same-color predecessors" post-v1.0.23, which is wider
than the pre-v1.0.23 symmetric window. The release annotation
names this divergence; clients tuning `window_size` should
revisit their value.

---

## 9. The migration arc

Six commits on `feat/adaptive-selector-pluggability`, ordered so
each preserves `mypy --strict` + the existing test suite green.

### Commit 1 — Substrate: view dataclasses + selection policies

New module-level declarations in
`middleware/adaptive_reevaluate.py`:

- `MoveView` and `TurnView` frozen dataclasses (§2.1).
- Four selection-policy functions (`_per_color_quantile_select`,
  `_pooled_quantile_select`, `_per_color_threshold_select`,
  `_top_k_select`) operating on `list[tuple[..., float]]` and
  returning the worst-set.
- A tiny dispatcher mapping policy name + axis to the
  appropriate `Callable` (`_resolve_selection_policy`).

Additive; no consumers yet; `mypy --strict` + tests pass.

### Commit 2 — `RegistryInterpreter` extension

Add `get_move_selector_fn` and `get_turn_selector_fn` on
`RegistryInterpreter`. Return `Optional[Callable[[Any], Any]]`
— `None` on absent binding to distinguish from the existing
zero-stub fallback discipline.

Additive; no consumers yet.

### Commit 3 — Default-selector extraction (refactor)

Refactor `_find_worst_turns`: extract the body's per-color
quantile + mean-of-deltas logic into a hardcoded default
selector function (`_default_move_selector`) plus a selection-
policy application. The function now has two stages: score each
move using the default selector; apply per-color quantile via
the new policy machinery from Commit 1.

Behaviour preserved bit-for-bit on the legacy path (no
selector binding); the existing tests pass unchanged.

### Commit 4 — Dispatch: user selectors + axis resolution

Wire the new binding roles into the coroutine:

- Resolve selector axis from `analysis_config.bindings` +
  `capabilities.adaptive_reevaluate.selector_axis`.
- For move axis: build `MoveView` per (color, move), call
  user selector or default, apply selection policy.
- For turn axis: build `TurnView` per turn, call user selector,
  apply selection policy.
- The Stage 2 block of `coro` consumes the new dispatch
  helpers and produces the deepening set as before.

Behaviour preserved on the legacy path; new wire-shape fields
unlock new behaviours per the matrix in §8.

### Commit 5 — Window correction (move-space)

Replace `_expand_window` (turn-space, symmetric) with
`_expand_window_same_color` (move-space, same-color
predecessor). Default `window_size` shifts from 3 to 2.
Behaviour change visible at the wire on adaptive-engaged
queries.

This commit's diff is the smallest of the substantive ones but
has the largest wire-visible behaviour shift. Release notes
name it explicitly.

### Commit 6 — Tests

New tests in `tests/test_adaptive_selector_pluggability.py`:

- View construction: `MoveView` and `TurnView` carry the
  expected fields from a synthetic `finals` list.
- Selection policies: each of the four returns the expected
  worst-set on synthetic scored lists.
- `RegistryInterpreter` resolution: `get_move_selector_fn` /
  `get_turn_selector_fn` return `None` on absent bindings,
  the user-authored callable on present.
- Dispatch axis resolution: each row of §8's matrix is
  exercised with a synthetic `analysis_config` + `cap_meta`.
- Window correction: same-color-predecessor expansion produces
  the documented turn-set for known move-color inputs.
- Wire-shape integration: a full adaptive coroutine run with
  `move_selector_fn` bound produces the expected deepening
  turns (mocked sub-query); ditto for `turn_selector_fn`.

The existing `tests/test_capability_negotiation.py` suite's
adaptive sections pass unchanged on the legacy path.

---

## 10. Test discipline

Per the v1.0.21 / v1.0.22 precedent:

1. **Existing suite must remain green at every commit.** Each of
   the six commits is internally consistent; tests track.
2. **The mypy CI gate enforces typing.** New surface stays under
   the existing `mypy --strict` gate; no new `# type: ignore`.
3. **Regression tests in
   `tests/test_adaptive_selector_pluggability.py`.** Covers the
   six categories above.
4. **The default-path tests in
   `tests/test_capability_negotiation.py` and
   `tests/test_adaptive_cache_matrix.py` continue to pass
   unchanged** — verifying the legacy path's behaviour is
   preserved bit-for-bit.
5. **One documented cast** allowed if implementation surfaces a
   structural-but-true contract that resists encoding (per
   ADR-0002 Rule 2); none anticipated by the design.

---

## 11. Design calls

### 11.1 View dataclasses live in `middleware/adaptive_reevaluate.py`

The `MoveView` and `TurnView` dataclasses live in the same module
as the adaptive coroutine. Rationale: they are adaptive-internal
surface; no other middleware or transformer uses them today, and
the umbrella note's autonomous-SR-loop generalisation is Phase 4
territory (separate substrate work). If a future arc surfaces a
second consumer, splitting into `middleware/adaptive_views.py`
is a small refactor.

### 11.2 Selection policies are a curated set, not user-authored

The four named policies (`per_color_quantile`, `pooled_quantile`,
`per_color_threshold`, `top_k`) cover the use cases the design
space survey identified. User-authored selection policies are
Phase 4 territory and require the program-shaped binding
substrate that doesn't exist in v1.0.23.

### 11.3 Turn-based selectors get no framework window

For v1.0.23, turn-based selectors emit a scalar per turn and the
framework selects the worst-set. No turn-space window expansion.
Rationale: turn-based metrics naturally aggregate via `state_fns`
+ `apply_window` in the expression substrate; a framework-side
window adds noise. If a future arc surfaces a use case, adding
an optional `turn_window_size` field in capability metadata is
backwards-compatible.

### 11.4 `selector_axis` disambiguator is the warn-and-fallback path

If both `move_selector_fn` and `turn_selector_fn` are bound AND
no `selector_axis` disambiguator is set, the implementation logs
a warning and falls back to move-based. Rationale: silent
inversion of intent is the failure mode ADR-0002 forbids; a
warning surface lets the user see and correct. Hard-raising
would punish a configuration mistake more loudly than warranted
for a recoverable shape.

### 11.5 Window correction is hard-flipped

No opt-in metadata for the symmetric-vs-predecessor window. The
release annotation names the change; clients tuning `window_size`
should revisit. Rationale: per the umbrella note, the proxy's
primary consumer is the umbrella SPA (coordinated); institutional
consumers can pin to v1.0.22 if the window change is disruptive.

### 11.6 Standard submodule release arc

Branch + PR + tag v1.0.23. The umbrella pointer bump is
deliberately deferred until the full adaptive-widening arc lands
per the umbrella's 2026-05-18 decision (proxy main runs ahead of
the umbrella pin for the duration of the widening).

---

## 12. Sunsetting

This memo is `design-note: planned`. When the six commits land
and v1.0.23 is tagged, the memo transitions to
`design-note: implemented` with implementation notes inline
(format per `roadmap-identity-type-branding.md` §5 Phase 3 and
`roadmap-adaptive-type-branding.md`'s post-implementation
annotation pattern).

Phase 2+ widenings (multi-round, budget, info-theoretic
allocation, user-authored policies) from the umbrella note are
separate arcs in subsequent proxy releases; v1.0.23 establishes
the substrate they compose on but does not pre-empt their design.

---

## 13. Related documents

- `LengYue:docs/notes/adaptive-reevaluate-widening-plan.md` —
  the umbrella design note this roadmap implements Phase 1 of.
- `proxy/docs/roadmap-adaptive-type-branding.md` (v1.0.22) —
  the substrate-piece predecessor; this arc builds on its
  branded `MoveIndex` / `TurnIndex` / `move_to_turn_pair`
  vocabulary.
- `proxy/docs/roadmap-identity-type-branding.md` (v1.0.21) —
  the original `NewType` discipline and post-implementation
  annotation pattern this roadmap inherits.
- `proxy/registry_interpreter.py` — the expression substrate
  extended with two new binding roles in Commit 2.
- `proxy/middleware/adaptive_reevaluate.py` — the file most of
  the migration modifies.
- `proxy/CLAUDE.md` §"Type-driven design (within Python's
  limits)" — the existing discipline this arc extends.
