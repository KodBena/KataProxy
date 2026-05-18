# `adaptive_reevaluate` type branding — design roadmap

- **Status:** `design-note: planned` (per umbrella ADR-0005 Rule 8
  doc-graph genre vocabulary)
- **Date:** 2026-05-18
- **Scope:** `proxy/middleware/adaptive_reevaluate.py` and the
  brand/seam declarations in `proxy/katago/katago_proxy.py`.
  Narrow surface — adaptive's internal arithmetic only.
- **Origin:** Closes the substrate-level type-branding gap
  surfaced in the umbrella's
  `LengYue:docs/notes/adaptive-reevaluate-widening-plan.md`. This
  roadmap is the v1.0.22 arc that lands the substrate piece
  before the v1.0.23 selector-pluggability arc rides on top.
- **Authoritative for the `feat/adaptive-type-branding` branch;**
  superseded by the v1.0.22 release notes once tagged.

---

## TL;DR

`adaptive_reevaluate` currently mixes two distinct integer concepts
into the same `int` type: a **per-color move index** (the position
of a move within one color's move sequence — what
`extra.<color>.deltas` is keyed by) and a **per-position turn
index** (the overall position number, root = 0 — what
`KataGoQuery.analyze_turns` carries). The conversion lives
open-coded inside `_find_worst_turns` as
`2*t + displacement` arithmetic, with `displacement = 0` for Black
and `1` for White.

This arc brands the two concepts as `MoveIndex` and `TurnIndex`
(`NewType` aliases over `int`), introduces one named translation
seam `move_to_turn_pair(color, m) -> (TurnIndex, TurnIndex)` that
captures the existing arithmetic, and migrates adaptive's three
internal helpers (`_find_worst_turns`, `_expand_window`,
`_build_deeper_query`) to thread the brands through their
signatures. Zero behaviour change; `mypy --strict` enforces
non-confusion at the typecheck level.

The pattern parallels the v1.0.21 identity-type-branding arc
(`roadmap-identity-type-branding.md`) — same `NewType` discipline
applied to a different axis. The two arcs compose: identity
branding catches namespace confusion at protocol boundaries;
move/turn branding catches game-tree-indexing confusion within
adaptive's loop.

The wider migration (typing `KataGoQuery.analyze_turns` as
`list[TurnIndex]`, propagating brands through
`analysis_enricher` and `delta_analysis`) is **deferred**. v1.0.22
establishes the substrate at adaptive's seam; v1.0.23+ surfaces
concrete needs that may motivate the wider propagation.

---

## 1. The current state

Three functions in `proxy/middleware/adaptive_reevaluate.py`
carry the magic-constant arithmetic the brands discipline.

### 1.1 `_find_worst_turns` (the mix happens here)

```python
def _find_worst_turns(
    responses: List[AnalyzeResponse], quantile: float,
) -> List[int]:
    turn_maps: Dict[str, Dict[int, List[float]]] = {
        "black": defaultdict(list),
        "white": defaultdict(list),
    }
    for resp in responses:
        for color in ("black", "white"):
            deltas = resp.opaque.get("extra", {}).get(color, {}).get("deltas")
            if isinstance(deltas, dict):
                for t, d in deltas.items():
                    turn_maps[color][int(t)].append(float(d))

    worst: List[int] = []
    for displacement, color in [(0, "black"), (1, "white")]:
        tm = turn_maps[color]
        if not tm:
            continue
        avg_deltas = [(t, float(np.mean(ds))) for t, ds in tm.items()]
        threshold = sorted(d for _, d in avg_deltas)[
            int(len(avg_deltas) * quantile)
        ]
        moves = [t for t, d in avg_deltas if d <= threshold]
        turns = sum(
            [[2 * t + displacement, 2 * t + 1 + displacement] for t in moves],
            [],
        )
        worst.extend(turns)

    return worst
```

The function consumes per-color move indices from
`extra.<color>.deltas` dict keys (these are `MoveIndex`-shaped),
processes them by color, and emits a flat list of overall turn
indices (`TurnIndex`-shaped) via the `2*t + displacement`
arithmetic. The collapsed `int` type doesn't catch a confusion
of move and turn — both are runtime-equal `int`.

### 1.2 `_expand_window` (purely turn-space)

```python
def _expand_window(
    worst_turns: List[int], all_turns: Set[int], window_size: int,
) -> Set[int]:
    expanded: Set[int] = set()
    half = window_size // 2
    for t in worst_turns:
        for offset in range(-half, half + 1):
            c = t + offset
            if c in all_turns:
                expanded.add(c)
    return expanded
```

Operates entirely in turn-space. Branding threads `TurnIndex`
through input and output; internal logic unchanged.

(The v1.0.23 arc replaces this function's symmetric turn-space
expansion with a same-color predecessor expansion in move-space.
v1.0.22 only brands the existing shape.)

### 1.3 `_build_deeper_query` (turn-space → KataGoQuery)

```python
def _build_deeper_query(
    orig: KataGoQuery, turns: List[int], extra_visits: int,
) -> KataGoQuery:
    new_opaque = dict(orig.opaque)
    new_opaque["maxVisits"] = (
        new_opaque.get("maxVisits", 1000) + extra_visits
    )
    new_opaque.pop("cache", None)
    new_opaque.pop("lookup_cache", None)
    new_opaque.pop("replay_final_only", None)
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=turns,
        opaque=new_opaque,
    )
```

`turns` is `TurnIndex`-shaped post-branding. The
`KataGoQuery.analyze_turns: Optional[list[int]]` field stays
`list[int]` at the wire-types level (see §8.2 for the scope
decision); the cast at this boundary is documented per
ADR-0002 Rule 2.

---

## 2. The two namespaces

| Namespace | What it identifies | Range | Cardinality |
|---|---|---|---|
| `MoveIndex` | A move within one color's move sequence | 0-indexed within that color | One sequence per color |
| `TurnIndex` | A position in the overall game | 0 = root, 1 = post-first-move, etc. | Single sequence over all positions |

The relationship: a move at `MoveIndex` M played by Black takes
the game from `TurnIndex(2*M)` to `TurnIndex(2*M+1)`; a move at
`MoveIndex` M played by White takes the game from
`TurnIndex(2*M+1)` to `TurnIndex(2*M+2)`. Both indices are
0-indexed within their respective sequences.

Both types are `NewType` aliases over `int` — runtime-equal
`int`s, type-distinct under `mypy --strict`. Same discipline
v1.0.21 applied to the four identity namespaces; same
strengths and limitations (`§3` below cross-references the full
discussion in `roadmap-identity-type-branding.md`).

A third small piece of vocabulary lands alongside:

```python
Color = Literal["black", "white"]
```

`Color` is a `Literal` type alias rather than a NewType. The
existing string literals (`"black"`, `"white"`) at consumer sites
satisfy it directly without explicit construction; mypy still
enforces that only those two values are admissible. The
NewType-over-str alternative would require `Color("black")` at
every literal site for no additional safety beyond what Literal
gives — Literal is the more ergonomic shape for a 2-valued
domain.

---

## 3. Python's `NewType` semantics

Cross-reference `roadmap-identity-type-branding.md` §3 for the
full discussion of `typing.NewType`'s strength/weakness profile.
The summary applicable to v1.0.22 unchanged:

- Nominal types at typecheck time; identity at runtime.
- Zero JSON-serialisation impact (brands serialise as their
  base type).
- Grep-friendly (`grep -rn 'MoveIndex' proxy/` returns the
  actual move-index-flowing surface).
- Discipline is as strong as the codebase's type-coverage; an
  untyped cast or `# type: ignore` can launder one brand into
  another.

The v1.0.21 arc established **`# type: ignore` is forbidden in
the proxy's brand-touched surface** (per the discipline recorded
inline in `roadmap-identity-type-branding.md` §3 and §6 Rule 4).
v1.0.22 inherits this. Any cast required is documented inline
with the structural reasoning per ADR-0002 Rule 2.

---

## 4. The translation seam

One named function captures the move-space → turn-space
arithmetic:

```python
def move_to_turn_pair(
    color: Color, m: MoveIndex,
) -> tuple[TurnIndex, TurnIndex]:
    """Translate a per-color move index to its (before, after) turn pair.

    For Black's m-th move (MoveIndex m): turns (2m, 2m+1).
    For White's m-th move (MoveIndex m): turns (2m+1, 2m+2).

    The 'before' turn is the position the moving side faces; the
    'after' turn is the position resulting from playing the move.
    """
    displacement = 0 if color == "black" else 1
    t = int(m)
    return TurnIndex(2 * t + displacement), TurnIndex(2 * t + 1 + displacement)
```

This is the **only** open-coded location for the
`2*t + displacement` arithmetic post-migration. Every consumer
that previously wrote `2 * t + displacement` (in `_find_worst_turns`
today; in the v1.0.23 same-color-predecessor expansion later)
calls this seam.

The seam returns a tuple rather than a flat 2-list because the
two TurnIndex values are structurally distinguishable (before /
after) and tuple unpacking is idiomatic at consumer sites. The
current code's flat-list output is reshaped at the consumer side
when it joins the worst-turn collection.

---

## 5. The migration arc

Single arc, four commits on `feat/adaptive-type-branding`.

### Commit 1 — Declare brands and seam

`proxy/katago/katago_proxy.py` gains the three new declarations
near the existing wire-type definitions:

```python
from typing import Literal, NewType

# Per-color move index (0-indexed within a single color's move sequence).
MoveIndex = NewType("MoveIndex", int)

# Per-position turn index (0 = root, 1 = post-first-move, etc.).
TurnIndex = NewType("TurnIndex", int)

# Side-to-play / color of a move.
Color = Literal["black", "white"]


def move_to_turn_pair(
    color: Color, m: MoveIndex,
) -> tuple[TurnIndex, TurnIndex]:
    """[docstring per §4]"""
    displacement = 0 if color == "black" else 1
    t = int(m)
    return TurnIndex(2 * t + displacement), TurnIndex(2 * t + 1 + displacement)
```

No consumers yet; additive declarations. `mypy --strict` passes;
test suite passes unchanged.

### Commit 2 — Migrate `_find_worst_turns`

```python
def _find_worst_turns(
    responses: List[AnalyzeResponse], quantile: float,
) -> list[TurnIndex]:
    turn_maps: dict[Color, dict[MoveIndex, list[float]]] = {
        "black": defaultdict(list),
        "white": defaultdict(list),
    }
    for resp in responses:
        for color in ("black", "white"):
            deltas = resp.opaque.get("extra", {}).get(color, {}).get("deltas")
            if isinstance(deltas, dict):
                for t, d in deltas.items():
                    turn_maps[color][MoveIndex(int(t))].append(float(d))

    worst: list[TurnIndex] = []
    for color in ("black", "white"):
        tm = turn_maps[color]
        if not tm:
            continue
        avg_deltas = [(m, float(np.mean(ds))) for m, ds in tm.items()]
        threshold = sorted(d for _, d in avg_deltas)[
            int(len(avg_deltas) * quantile)
        ]
        worst_moves = [m for m, d in avg_deltas if d <= threshold]
        for m in worst_moves:
            before, after = move_to_turn_pair(color, m)
            worst.append(before)
            worst.append(after)

    return worst
```

The `displacement` constant and the
`[2 * t + d, 2 * t + 1 + d]` list-comp disappear; the seam owns
the conversion. The `for displacement, color in [(0, "black"), (1, "white")]`
loop becomes a simple per-color loop since color alone suffices
(the seam handles displacement internally).

Construction sites for the brands: `MoveIndex(int(t))` brands
the per-color delta dict keys (which arrive on the wire as
strings from JSON-parse, converted to `int` by the existing
code, then branded). The `Color` literal `"black"` / `"white"`
satisfies the `Color` type natively.

### Commit 3 — Migrate `_expand_window` and `_build_deeper_query`

```python
def _expand_window(
    worst_turns: list[TurnIndex],
    all_turns: set[TurnIndex],
    window_size: int,
) -> set[TurnIndex]:
    expanded: set[TurnIndex] = set()
    half = window_size // 2
    for t in worst_turns:
        for offset in range(-half, half + 1):
            c = TurnIndex(int(t) + offset)
            if c in all_turns:
                expanded.add(c)
    return expanded
```

Pure brand-threading; internal logic unchanged. The
`TurnIndex(int(t) + offset)` re-brand at the inner construction
site is idiomatic — arithmetic on a NewType returns its base
type, so the brand has to be re-applied at the construction
site.

```python
def _build_deeper_query(
    orig: KataGoQuery, turns: list[TurnIndex], extra_visits: int,
) -> KataGoQuery:
    new_opaque = dict(orig.opaque)
    new_opaque["maxVisits"] = (
        new_opaque.get("maxVisits", 1000) + extra_visits
    )
    new_opaque.pop("cache", None)
    new_opaque.pop("lookup_cache", None)
    new_opaque.pop("replay_final_only", None)
    # NOTE (ADR-0002 Rule 2): KataGoQuery.analyze_turns is typed
    # list[int] at the wire-types level (out of scope for the
    # v1.0.22 brand migration; wider migration deferred per the
    # roadmap §8.2). The cast here is safe-by-construction:
    # TurnIndex is a NewType over int, runtime-equal.
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=list(turns),  # list[TurnIndex] → list[int] (NewType base)
        opaque=new_opaque,
    )
```

The call sites in the orchestration coroutine (`coro` in
`adaptive_reevaluate`) thread `list[TurnIndex]` end-to-end.
`_find_worst_turns` → `set(...)` → `_expand_window` →
`sorted(...)` → `_build_deeper_query` keeps `TurnIndex` typing
across the chain.

### Commit 4 — Tests

Add `proxy/tests/test_adaptive_type_branding.py` mirroring
`test_identity_types.py`'s shape:

```python
# This file uses mypy's assert_type machinery to pin the
# move/turn brand contract. It runs in the typecheck step.
from typing import assert_type
from katago.katago_proxy import (
    Color, MoveIndex, TurnIndex, move_to_turn_pair,
)


def test_brands_distinct() -> None:
    m = MoveIndex(3)
    t = TurnIndex(7)
    assert_type(m, MoveIndex)
    assert_type(t, TurnIndex)
    # The following would be a typecheck error:
    # x: TurnIndex = m  # error: expected TurnIndex, got MoveIndex


def test_translation_seam_signature() -> None:
    color: Color = "black"
    m = MoveIndex(2)
    pair = move_to_turn_pair(color, m)
    assert_type(pair, tuple[TurnIndex, TurnIndex])


def test_translation_seam_runtime() -> None:
    before, after = move_to_turn_pair("black", MoveIndex(2))
    assert int(before) == 4
    assert int(after) == 5
    before, after = move_to_turn_pair("white", MoveIndex(2))
    assert int(before) == 5
    assert int(after) == 6
```

Plus runtime parity checks: the existing adaptive test suite
(`tests/test_adaptive_cache_matrix.py` and any adjacent files
exercising adaptive's wire behaviour) passes unchanged. Wire
behaviour is preserved bit-for-bit; the migration is type-only.

---

## 6. Test discipline

Per the v1.0.21 precedent:

1. **Existing suite must remain green at every commit.** Each
   of the four commits is mechanical brand-threading; tests
   track 1:1.
2. **The mypy CI gate at `.github/workflows/typecheck.yml`
   (v1.0.21) enforces the discipline.** v1.0.22 extends the
   surface the existing gate covers; no new gate added.
3. **Regression tests in `tests/test_adaptive_type_branding.py`.**
   `assert_type` pinning per the v1.0.21 precedent.
4. **No `# type: ignore` introduced.** Inherits v1.0.21's
   discipline. The one documented cast in
   `_build_deeper_query` (the `list[TurnIndex] → list[int]` at
   the `KataGoQuery.analyze_turns` boundary) carries an
   ADR-0002 Rule 2 comment naming the structural-but-true
   contract Python's type system cannot encode without the
   wider migration scoped out per §8.2.

---

## 7. Design calls

### 7.1 Brands live in `katago/katago_proxy.py`

The three declarations (`MoveIndex`, `TurnIndex`, `Color`) and
the `move_to_turn_pair` seam live in
`proxy/katago/katago_proxy.py` near the existing KataGo wire-type
definitions.

**Rationale:** the move/turn distinction is structural to KataGo's
game-tree protocol — a non-game protocol using this framework
would not have it. This contrasts with v1.0.21's identity-type
NewTypes (`ClientId` etc.) which live in `AbstractProxy/proxy_core.py`
because every protocol the framework supports has namespace
boundaries to brand. The two placements coexist consistently:
framework-universal abstractions in core, protocol-specific
abstractions in `katago/`.

If a future second game-tree protocol (chess, shogi) is
implemented, `MoveIndex` / `TurnIndex` are candidates to
generalise (along with `Color`, which becomes
`Literal["black", "white"] | Literal["white", "black"] | ...`
or similar). The decision to generalise belongs to that
implementation arc, not v1.0.22.

### 7.2 Scope is `adaptive_reevaluate.py`-internal

`KataGoQuery.analyze_turns: Optional[list[int]]` stays as
`Optional[list[int]]` post-v1.0.22. The wider migration
(narrowing the wire-types field to `Optional[list[TurnIndex]]`,
propagating brands through `analysis_enricher` and
`delta_analysis`'s state machine) is **deferred**.

**Rationale:** v1.0.22 establishes the substrate for v1.0.23's
selector pluggability arc, which is the immediately-blocking
work. Wider propagation of the brands is parallel-arc work that
can land after v1.0.23 surfaces concrete needs — or stay
deferred indefinitely if no concrete need emerges. The one
documented cast at the `_build_deeper_query` boundary is the
admission of the deferred-wider-migration; ADR-0002 Rule 2's
inline-comment discipline names it visibly.

The cast is safe-by-construction: `TurnIndex` is a `NewType`
over `int`, so `list[TurnIndex]` is runtime-equal to `list[int]`
and the `KataGoQuery` constructor accepts the value unchanged.
The typecheck-visible drop happens at one named site; future
narrowing of the wire-types field would remove the cast in one
edit.

### 7.3 `Color` is a `Literal` type alias, not a NewType

`Color = Literal["black", "white"]`. NewType over `str` would
require explicit construction at every literal site
(`Color("black")`); `Literal` admits the existing string
literals naturally and gives the same mypy discipline. The
2-valued domain doesn't benefit from NewType's nominal
distinction.

### 7.4 Standard submodule release arc

Per `proxy/CLAUDE.md`'s "Submodule release arc" section.
Branch + PR + tag v1.0.22 in the proxy repo; umbrella pointer
bump in a separate small PR on the umbrella side.

v1.0.21's §7.4 noted that pure-type-only migrations could
technically skip the tag and bump-to-commit directly. v1.0.22
keeps the tag for tracking parity with v1.0.21 (the umbrella's
adaptive-widening arc spans multiple proxy bumps; consistent
tagging keeps the bump-arc legible).

---

## 8. Sunsetting

This memo is `design-note: planned`. When the four commits land
and v1.0.22 is tagged, the memo transitions to
`design-note: implemented` with implementation notes inline
(format per `roadmap-identity-type-branding.md` §5 Phase 3's
post-implementation annotation).

If implementation surfaces something the design did not
anticipate, the memo is re-marked `design-note: revised` per
umbrella ADR-0005 Rule 8 with the revision noted at the change
site.

---

## 9. Related documents

- `LengYue:docs/notes/adaptive-reevaluate-widening-plan.md` —
  the umbrella design note this roadmap implements the
  type-branding substrate piece of. The widening's "type
  branding at the move/turn seam" substrate-level concern is
  what v1.0.22 closes.
- `proxy/docs/roadmap-identity-type-branding.md` — the closest
  precedent (v1.0.21). Shares the `NewType` discipline applied
  to a different axis; cross-referenced throughout this memo
  for the `NewType` semantics, the `# type: ignore`
  prohibition, and the implementation-notes-inline pattern.
- `proxy/CLAUDE.md` §"Type-driven design (within Python's
  limits)" — the existing discipline this migration extends.
- `proxy/ARCHITECTURE.md` §"Where this falls short" — names
  the broader class of "protocol abstraction leaks at the
  edges" that the v1.0.21 and v1.0.22 arcs progressively close.
- `proxy/middleware/adaptive_reevaluate.py` — the file the
  migration modifies; the three helper functions in §1 are
  the touch surface.
- `proxy/katago/katago_proxy.py` — where the brand declarations
  and the translation seam land.
