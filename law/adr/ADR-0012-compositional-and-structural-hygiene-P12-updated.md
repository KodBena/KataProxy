<!-- The 2026-07-15 doc-table-mechanization exempt marker that stood here is struck per its
     own closing sentence ("A future PROSE edit to this file should get a real attestation
     regardless of this marker's literal wholesale scope"): the 2026-07-27 P11 amendment is
     that prose edit, and its real +A:B:C attestation (two blind rounds, escalated per the
     2-round cap, adjudicated) is recorded in attestations/doc-legibility-attestations.jsonl
     for this file's current content hash. -->

# ADR-0012: Compositional and Structural Hygiene

- **Status:** Proposed
- **Genre:** Tenet (cross-cutting structural-design discipline) — the ninth
  tenet, and the structural counterpart to the *authoring*-discipline family
  (ADR-0002/0005/0007/0009) and the *corrective*-discipline tenet (ADR-0011).
  Where ADR-0011 says *a recurrence converts to a mechanism*, this tenet says
  *new structure is born in the shape an audit's mechanisms would otherwise
  have to enforce after the fact* — so the conversion ADR-0011 mandates is
  rarely needed, because the rot never forms. It is the **positive inverse of
  an architectural audit's "architectural cancer" taxonomy**: each disease the
  audit named gets the structural rule whose presence makes that disease
  impossible to author.
- **Date:** 2026-06-15 (original); refactored for portability 2026-07-13 (below).
- **Provenance:** Native to this tenet's originating codebase, not transferred.
  Its source substrate is a named, dated architectural audit of that codebase
  (not held in this repository) and a forward-looking design note for its
  incoming C++ component (see the worked instance in
  [`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md)).
  The audit's verdict — *"the bones are sound; the connective tissue is
  rotting … the right idea applied once and not propagated"* — is this tenet's
  reason to exist: the disciplines were known (a dependency-inversion seam, a
  live per-call tunable, zero-drift derived dimensions) and proven, but were
  not the **default shape new code is born in**. This ADR makes them the
  default, written ahead of that codebase's incoming C++ runner and future
  async actor-learner loop, precisely so that those — the next large bodies of
  new code — are born clean rather than audited dirty.
- **Scope:** All **new** structure across a hosting project's own codebase and
  any new-language component that joins it — this tenet's originating instance
  is an incoming C++ search/sim runner, then a future async actor-learner loop.
  It binds at design and authoring time. Per ADR-0004's incremental-retrofit
  posture it mandates **no retroactive sweep** of existing code; a project's
  own remediation roadmap (not this ADR) sequences the cleanup of what already
  exists.

*Refactored for cross-project portability on 2026-07-13 under
[`design/MAINT-ADR-PORTABILITY-SPEC.md`](../../design/MAINT-ADR-PORTABILITY-SPEC.md)
(tracker `adr-portability-refactor`, maintainer-ratified 2026-07-13). The
pre-refactor text stands verbatim at commit `0f7b3e4`; extracted records live in
[`history/0012-p9-worked-examples.md`](history/0012-p9-worked-examples.md),
[`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md), and
[`history/0012-cross-device-bench-saga.md`](history/0012-cross-device-bench-saga.md),
and are not retro-edited. The anti-pattern checklist, all nine principles' rule
statements, the Self-application declarations, and both 2026-07-02 amendments
stand unedited in substance; each principle's worked example is condensed to
its one-to-three-sentence lesson, the extracted material's own dated Amendment
headings are renamed to the corpus's standing `Amendment — <date>: <title>`
form (a labeling fix, not a content change, made so this refactor's own A1
gate shields their Provenance fields correctly), and the header fields above
are re-instanced generically per this refactor's own precedent (ADR-0009,
ADR-0011). Dated amendments below are preserved verbatim from the original.*

## Context

An architectural audit of this tenet's originating codebase diagnosed eight
recurring "architectural cancers" (anti-patterns A–H, verified line-by-line
against the live tree) and named the remediation as *"overwhelmingly
subtraction and relocation … the codebase finishing a sentence it started
correctly."* The deepest finding is that **the codebase had already proved it
knows the right answer**, in specific places — a live per-call tunable
threaded through the hot path and owned by one loop; a derived dimension
computed from the instance with zero drift; a dependency-inversion seam
honored to the letter — *and then applied that discipline once and stopped.*
The cancers are not wrong ideas; they are the **right idea not propagated.**

This tenet's job is propagation by default. It states, as **checkable rules**,
the compositional and structural hygiene an audit's remediation roadmap would
otherwise have to re-derive by hand, so a contributor (human or LLM) authoring
new code can self-check against a closed list rather than rediscovering each
lesson. It is deliberately **anti-pattern-first**: the cancer is the
load-bearing motivation, so each rule is anchored to the specific disease its
absence permits.

This tenet **composes with — and does not restate —** its siblings, which own
adjacent concerns:

- **[ADR-0002](0002-fail-loudly.md) (fail loudly)** owns *error/diagnosis
  surfacing*. Principle 5 below cites it; it does not re-derive the loudness
  hierarchy.
- **[ADR-0004](0004-minimal-touch-edits-to-partially-visible-files.md)
  (minimal-touch)** owns *editing under partial visibility*. This tenet's
  no-retroactive-sweep scoping defers to it.
- **[ADR-0005](0005-documentation-discipline.md) (documentation discipline)**
  owns *how facts are documented*. Principle 1's SSOT is the **structural**
  twin of ADR-0005 Rule 1's single-source-of-truth-per-handle (documentation
  register); they cite each other, neither restates the other.
- **[ADR-0007](0007-file-size-and-information-density.md) (file size /
  information density)** owns *file budgets*. Principle 3 (no god-objects)
  produces small files as a byproduct but is justified on one-owner grounds,
  not line count; the budget is ADR-0007's.
- **[ADR-0009](0009-performance-investigation-discipline.md)
  (perf/equivalence investigation discipline)** owns *substantiating perf and
  equivalence claims*. Principle 6 composes with it directly and imports its
  two-tier (bit-exact vs aggregate-behavioral) bar wholesale rather than
  redefining it.
- **[ADR-0011](0011-mechanization-discipline.md) (mechanization discipline)**
  owns *converting a recurrence to a mechanism*. This tenet is upstream of it:
  structure born clean is structure ADR-0011 never has to convert. The
  mechanisms ADR-0011 mints — `FeatureLayout` (a single ordered feature-block
  table with a fail-loud contiguous-partition check) and `BeliefRefs` (a single
  owner for a set of duplicated reference rates), both defined there — and the
  equivalence tests, are this tenet's worked examples.

## Decision

We adopt **Compositional and Structural Hygiene** as a codebase-wide tenet for
new structure. It is stated in two registers: first **the anti-pattern
checklist** (each audit cancer → the rule that prevents it — the index a
contributor scans before authoring), then **the nine principles** (each a
checkable rule, with a worked example from this codebase and the cancer it
prevents), then a **dedicated concrete section for a new-language (C++)
component**.

### The anti-pattern checklist (cancer → preventing rule)

This is the audit's §2 disposition table, inverted: read it before authoring
new structure, and again at review. Each row is "if your new code can exhibit
this shape, the named principle forbids it."

| Audit cancer (§2) | The shape to never author | Preventing rule |
| --- | --- | --- |
| **A** — Config frozen at construction; ownership lives nowhere | a tunable swept across a run captured once in `__init__`/`Namespace` with no per-call or per-iteration read | **P4** (live, not frozen) — heat is decided by *where the value lives*; a value that changes within a run is a live cell, not a ctor invariant |
| **B** — SSOT dissolved; same knowledge re-encoded in N places | a second hand-maintained copy of a fact (belief math, the C(N,K) prior, the feature layout, K, the reference rates) | **P1** (single source of truth / derive-don't-duplicate) — every fact has one home; derived quantities are computed, never re-typed |
| **C** — Hidden global state keyed by object identity | a module-global cache keyed on `id(env)` (or any value-less identity) instead of owned on the object | **P2** (seam/port discipline) — derived data lives on the object whose lifetime it shares; no module global keyed by address |
| **D** — Copy-paste programs instead of one parameterized runner | the Nth bespoke `main()`/driver differing only in one literal | **P3** (no god-objects → one parameterized collaborator) + **P1** (one definition of the metric) |
| **E** — Abstraction built then abandoned beside a live inline copy | a fully-built type sitting unused next to the hand-inlined path that is actually live; a parameter the receiver ignores | **P5** (remove the root cause) — adopt or delete; **P2** — a parameter the receiver cannot honor is not in the signature |
| **F** — Magic constants strewn as bare literals | a shared invariant (the episode horizon, UCB `c`, a λ-tolerance) typed at each use site | **P1** — one owner, referenced; not re-typed and trusted to agree |
| **G** — Load-bearing knowledge offloaded to unenforceable prose | a convention that lives only in a comment/doc the code cannot check or that does not resolve | **P5** + **ADR-0011** — encode in code or a real registry; cite the derivation, not volatile prose (ADR-0011 owns the mechanization) |
| **H** — Defensive band-aids stacked against a hostile substrate | a new mitigation layered on an un-diagnosed cause; a reliability strategy that *is* a stack of patches | **P5** (fail loud; remove the root cause) — distinguish a justified guard from a band-aid masking an undiagnosed cause |
| **(new, cross-language)** — two writers of one cross-boundary truth | a hand-mirrored type, offset, key, or codec on the far side of the language boundary that re-authors a fact the near side already defines (a hardcoded weight offset; a second result-blob codec) | **P7** (cross-language wire discipline) — a cross-boundary fact has exactly one authoritative definition and every side *derives* its view (reads it at runtime or generates it at build time), never re-authors it by hand; mechanically enforced at the strongest feasible level (generate/compile-from-one-source > build-time lint > runtime parity backstop). Schema-driven codegen (one schema → N derived readers) is SSOT and is encouraged, not banned |
| **(new, call-boundary)** — a contract carried only by an unenforced or dishonest signature | an untyped function/method/dataclass signature (the contract lives nowhere checkable), or a *lying* one whose body does not honor its annotation (`hp: AdamHParams = None` whose body accepts `None`; `lr/b1/b2/eps: float` populated with jax `Array`s) | **P8** (typed signatures are the contract's SSOT) — the signature is the single source of truth of the input/output contract, honored by the body, at the **strict-where-achievable** bar, with each relaxation a named stub-gap (not a convenience); mechanically enforced by the mypy `--strict` CI gate ratcheting a monotonically-decreasing baseline (ADR-0011 Rule 1) |
| **(new, compiled-component)** — an untyped-effectful-void / black-box mutation in a compiled (C++/new-language) component | a function taking raw pointers (`const float*`, a `T*, size_t` pair) and returning `void` while writing its result through an output parameter or mutating hidden/global state (`void matvec_bias(const float* in, …, std::vector<float>& out)`; `void require_matrix(…, int& rows, int& cols, std::vector<float>& out)`) — a black box you cannot unit-test (it mutates rather than returns), cannot compose (it is not a value-function to chain), and whose contract is invisible (the `void` + raw-pointer signature names neither the bounds, the const-ness, nor what it mutates); or **signaling failure by throwing an exception** (an untyped control-flow escape absent from the signature, which the caller is not forced to handle); or **signaling a legitimately-absent result with a nullable raw pointer or a sentinel** (`const char* opt(…)` returning `nullptr` for "not found"; a `-1` / `""` magic return) — an **untyped optional** whose absence is invisible in the type, so a missed null-check is undefined behavior, the C++ form of ADR-0002's sentinel-instead-of-raise red flag; or, more generally, **a reliquary anti-pattern where a designed-replacement modern feature exists, used out of habit** (a raw `new`/`delete` where RAII / a smart pointer fits; a C-style cast where a named cast says which conversion; a `#define` constant where `constexpr` does; `strcmp`/`strcpy` where `std::string_view` does; `NULL` where `nullptr` does; a `typedef` where `using` does; an unscoped `enum` where `enum class` does) — the modern feature is the standard answer to that construct's hazard at zero runtime cost, so the legacy form needs a *measured* justification, never a habit one | **P9** (functional core, imperative shell) — a computation is a pure function of typed, bounds-carrying, const-correct inputs **and outputs** (`std::span<const T>` / `std::string_view` over a raw `T*`, in *either* direction) **returning its result by value**; every effect is named in the signature, the only sanctioned hidden mutation is a measured hot-path buffer-reuse routed through an explicitly-typed `Workspace`/`Context&` parameter, **a legitimately-absent result is a `[[nodiscard]] std::optional<T>`** and **a failure is a `[[nodiscard]] std::expected<T, Error>`, never a sentinel, a nullable pointer, or a throw** (a throwing ctor becomes a `create(…) -> std::expected` factory). P9 is the **modern-C++ discipline**: these five rules are the catalog, not the whole — for any reliquary construct, prefer the standard (C++11–23) feature designed to ameliorate it at zero runtime cost, the legacy form forbidden absent a measured reason (a profile or a real, named constraint), never habit. The compiled-component form of B (a second/hidden writer), of P2 (hidden state / a lying signature), and of P8 (an untyped/dishonest contract), enforced by the compiler (`-Wall -Wextra`, the nodiscard warning treated as an error) and a future `clang-tidy` config — its `modernize-*` family the purpose-built net for the reliquary→modern substitutions (ADR-0011 Rule 1) |

### The nine principles

#### P1 — Single source of truth / derive-don't-duplicate

**Rule (checkable).** Every fact has exactly **one** home. A *derived*
quantity — a dimension, a layout, a count, the feature/weight layout, the
"keep" set of a sub-instance, a reference rate — is **computed from its source
at the point of use (or cached on the object that owns the source)**, never
hand-copied as a literal or re-encoded in a second place. The check: *grep the
tree for the value; if it appears as an independent literal in two places that
must agree, the rule is violated.* (P8 is this same single-home rule at the
call boundary: a function's contract has one home — its typed signature.)

**Worked example (this tenet's originating codebase).** Two derived dimensions had **zero drift**
because they were computed from the instance at every use; `FeatureLayout`
([ADR-0011](0011-mechanization-discipline.md)'s worked proof) turned a
three-writer duplicated table into one ordered, by-name-addressed structure
with a fail-loud contiguous-partition check, and a same-shaped single-owner
structure (`BeliefRefs`; and `WeightContainer`, described in
[`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md)) did
the same for a set of reference rates and a weight layout, respectively.

**Cancer prevented: B (SSOT dissolved), and F (magic constants).** The
originating audit proved the fuse is already lit: one hand-copied numeric
constant had already drifted from its sibling, and the three-writer feature
layout (one writer untested) would have *silently mislabeled feature-
importance rows* on a reorder. This rule is the structural form of ADR-0005
Rule 1 (single-source-of-truth-per-handle, documentation register); they are
twins, not duplicates.

#### P2 — Seam / port discipline (dependency inversion)

**Rule (checkable).** A boundary between two concerns is an **explicit port
with its dependency injected**, not an import-coupling or a reach into the
other side's internals. The template is the env↔Policy seam: **a new capability
is a new `Policy` subclass with ZERO core edits.** A Port/ACL boundary — a
Port/[anti-corruption-layer](../../GLOSSARY.md#anti-corruption-layer) boundary
(expansion added 2026-07-27 under [ADR-0017](0017-the-zero-context-reader.md) Rule 4,
which migrates a document to the legibility standard when it is touched; the dated
reader's glosses in [ADR-0000](0000-the-alpha-and-the-omega-type-driven-design.md)
already point to this P2 for the definition) —
**translates-and-validates** — it decodes the foreign representation into the
native one and rejects what it cannot honor; it does **not** coerce a
malformed input into a plausible one (a strict-decode boundary — see
[ADR-0011](0011-mechanization-discipline.md)'s own `hp` registry example — is
the exemplar). The checks: *(a) does a new method/capability require editing the
core, or only adding a subclass/impl behind the seam? (b) does the boundary
reject what it cannot honor, or silently accept it? (c) is any derived state
owned on the object whose lifetime it shares, or on a module global keyed by
identity?*

**Worked example (this tenet's originating codebase).** A core module imported no solver; the
environment↔policy contract was an injected method, so adding a new capability
was a new subclass with zero core edits. A strict boundary decode
translated-and-validated rather than coercing (refusing a conflicting
mid-run value change, naming both values — ADR-0002). A module-global cache
keyed on raw object identity was re-keyed to a `WeakKeyDictionary` on the
owning object, tying the cache entry's lifetime to the object's rather than to
its address.

**Cancer prevented: C (hidden global state keyed by identity) and the leaky-
boundary half of E.** The original identity-keyed cache was keyed on the
*least value-stable key possible* — masked only because every instance
happened to share one layout, it would hand back the **wrong cached value with
no error** the moment two instances diverged (and leak one never-evicted entry
per instance). A parameter the receiver silently ignores is a *lying
signature* — P2 forbids it: **a parameter the receiver cannot honor is not in
the signature** (and P8 at the call boundary: an annotation the body does not
honor is the same lie surfaced at the type layer).

#### P3 — No god-objects

**Rule (checkable).** Orthogonal concerns are split into **one-owner
collaborators**, each owning exactly one axis of the problem. The check: *can
you name, in one clause, the single concern this object owns? If naming its
responsibility requires "and," it is two collaborators wearing one class.* This
produces small files, but the justification is single-ownership, not the line
budget (that is ADR-0007's).

**Worked example (this tenet's originating codebase).** A transport/pool/task split separated *how
bytes travel* from *what runs them* and *what one worker computes*; a weight
container split out of a transport's former second encoder; a many-flag
argument-namespace god-object was dissolved into nested per-concern
configuration objects.

**Cancer prevented: D (copy-paste programs) and the split-brain-encoder half of
B.** A god-object forces every consumer to re-thread its whole state, which is
why one orchestration got re-typed across eight near-identical entry points and
a weight layout was *split-brained* between two owners. One parameterized
collaborator replaces N copies.

#### P4 — Live, not frozen, where it should breathe

**Rule (checkable).** A value that is **tuned mid-run or swept across runs** is
**read at the point of use from the live source**, not baked at construction.
A value's *heat is decided by where it lives, not by intentions*:
a knob assigned to `self.X` in `__init__` is cold no matter how often you mean
to sweep it; the same knob arriving as a per-call argument or read from a live
registry is hot for free. The check — the originating audit's own litmus test:
*if the value changes during a run or across a sweep, it is a live cell, not a
constructor invariant.* Apply a facet discipline like [ADR-0011](0011-mechanization-discipline.md)'s
originating codebase's `hp` (hyperparameter) registry: classify each tunable as
**HOT** (read per-use, e.g. per-iteration), **RESTART** (changed only across a
restart, with a loud drift refusal mid-run — ADR-0002), or **INSTANCE** (a true
Tier-1 geometry invariant), and place it accordingly. Bake only the INSTANCE
facet; never bake what is HOT.

**Worked example (this tenet's originating codebase).** One tunable was the gold standard — owned by
one loop, threaded as a live per-call argument to ~100 sites, with a dependent
policy rebuilding its own tables whenever the value moved. A learning-rate
knob baked into a jit'd optimizer closure at construction was the frozen
counter-example: the remediation makes it live per-iteration instead, unblocking
an experiment that previously required killing and restarting the process.

**Cancer prevented: A (config frozen at construction; ownership lives
nowhere).** The originating audit's verdict: *"of the project's
experimentation levers, exactly one was live. Every other dial was welded
shut."* The frozen-at-construction failure was **biting the project in
production, on its own roadmap** — an anneal experiment could not run without
a process restart.

#### P5 — Fail loud; remove the root cause, never band-aid

**Rule (checkable).** A stall or error surfaces as a **loud, diagnosable
failure** (this defers wholesale to **ADR-0002** for the loudness hierarchy —
construction-time raise > test/CI failure > runtime exception > logged
diagnostic > silent fallback-only-when-genuinely-right). And: when a defect's
**root cause** is found, you **remove the cause**, not add another mitigation.
The check distinguishing a *guard* from a *band-aid*: **a justified defensive
guard is re-justified on orthogonal merit and kept; a band-aid masks an
un-diagnosed cause and is one of a growing stack.** Ask: *is this layer fixing a
symptom of the previous layer's fight, and would the whole stack disappear if
the substrate conflict were removed at the root?* If yes, it is a band-aid;
remove the root instead.

**Worked example (this tenet's originating codebase).** A deadlock's root cause (a compiled inner loop
sharing a process with a JIT compiler's own thread pool) was removed at the
source — giving the affected workers a different entrypoint — rather than
stacking an eighth mitigation onto seven prior deadlock band-aids. Contrast the
**kept** guard in the same codebase: a bounded socket timeout, re-justified on
orthogonal merit as a genuine safety net rather than a patch on an undiagnosed
cause. The originating audit's own verdict: *"when the reliability strategy
becomes a stack of patches, the substrate is the bug."*

**Cancer prevented: H (defensive band-aids stacked against a hostile
substrate) and the silent-fallback half of A/G.** A subsystem whose correctness
test can only assert "fails loud" rather than "works" is fragile by
construction; the fix is to remove the substrate conflict so the bands become
unnecessary.

#### P6 — Substantiate equivalence/perf claims (composes with ADR-0009)

**Rule (checkable).** A perf, regression, null-result, or equivalence claim is
honest only with its substantiation attached — this **composes with ADR-0009**
and imports its **two-tier bar wholesale**, it does not restate it. The
ML-specific calibration this tenet underlines, because the C++ parity work
(P7) rests on it: **behavioral float32-equivalence is the bar, NOT byte-
identity** — float32 is not associative, so a reordered or cross-language
reimplementation of the same math *will* move the float and may flip a
near-tied argmax / Sequential-Halving choice. The check: *(a) is the quantity a
logic invariant (illegal-slot mass, a legality mask) → assert bit-exactly
(`== 0.0`); (b) is it a float-sensitive numeric (a rate under float32+numba,
or a cross-language forward) → hold to aggregate behavioral equivalence
(statistically indistinguishable rate / E[T] / action distribution over N≥300
episodes, ≥2 seeds, within Monte-Carlo CI); (c) claim bit-identity ONLY where
it is free and proven* (the three bit-exactness contracts the audit names: the
distance memo, the `ABS_TOL=1e-4` forward equivalence test, the value-target
MC-limit identity).

**Worked example (this tenet's originating codebase).** One harness held a float32 numeric path to
aggregate behavioral equivalence; another held mixed-precision forwards to a
tight absolute tolerance (the bit-near-identity that made a later forward
consolidation safe to attempt); a logic invariant was asserted bit-exactly. A
reproduced stale-weight numeric divergence is exactly the silent equivalence
failure an un-run check misses.

**Cancer prevented: unsubstantiated "equivalent"/"faster" claims** — the
ADR-0008/0009 closed-vocabulary failure in the perf/equivalence register, and
specifically the category error of pinning a float-sensitive quantity bit-
exactly (which forbids a legitimate optimization *and* a legitimate cross-
language port).

#### P7 — Cross-language wire discipline (the new material)

**Rule (checkable).** A **cross-boundary fact** — a layout, a key, a byte
format — has exactly **one authoritative definition**; every side **derives**
its view from that one definition (reading it at runtime, *or* generating it
from it at build time) and **never re-authors it by hand**. The violation is
**two writers of one truth** — a hand-mirrored type or a hand-written codec
that can drift from the authority — *regardless of representation*. This is
**P1 applied across the language boundary**: shared types are not the sin
(schema-driven codegen — one schema → N generated/derived readers — is SSOT
and is **encouraged**); a second hand-author of the same truth is.

The rule is **mechanically enforced at the strongest feasible level**, against
ADR-0011/ADR-0002's own enforcement hierarchy: **generate-or-compile-from-one-
source > build-time lint > runtime parity test.** A runtime parity test is a
**backstop, not the contract** — it catches drift only if it runs, with the
right fixtures, *after* the drift already exists. Where the contract is
**static** (a fixed layout with known dtypes/shapes) it should be generated or
compiled from one schema, or at minimum **build-time linted so a Python/C++
format disagreement fails the build** — not left to two hand-written codecs
joined only by a runtime test. This is cancer **G** (load-bearing knowledge in
unenforceable convention) plus **ADR-0011** (mechanize at the strongest
feasible surface) applied across the language boundary. *Never* justify
settling for a weaker mechanism with a scale / minimality / "one X" / "for now"
/ "unnecessary here" / YAGNI argument — that argument shape is itself the tell
this tenet exists to reject (the discipline applied once at small scale is
exactly how the cancers grew).

Separate the **serialization contract** from the **transport/coordination
mechanism** — the durable rule is mechanism-independent. A shared bytes-store
(redis today) holds **state/payloads** (the current weight snapshot, late-join,
large blobs); a **messaging fabric** (ZeroMQ or a broker — the originating
codebase's own forward-looking design note names this an explicit alternate
shape, kept as history in
[`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md))
carries **events/coordination/streaming**. **Never** use
a shared bytes-store as a synchronization/coordination primitive (polling a
key; pub-sub-on-a-store as the backbone) — that is an architectural smell.
Today the synchronous loop coordinates via the OS process pool with redis as a
**pure bytes-store** (no sync-via-store is committed); the C++ worker and the
async actor-learner introduce an explicit fabric for coordination while the
bytes contract is unchanged. Do not enshrine redis as "the contract," and do
not enshrine ZeroMQ as "the one way" either — they are instances of
(bytes-store) and (messaging fabric).

The asymmetry between the two payloads matters: the **weight** payload is a
**dynamic** layout (residual-block toggles, instance-derived dims), so a
**self-describing manifest read at runtime is a legitimate mechanism** there —
the C++ derives `(offset, len, shape, dtype)` each run, so a layout change is
**absorbed, not drifted**; its residual gap is only that the manifest's *own*
schema is still two hand-written (de)serializers. The **result** format is
**static** (four blocks X/PI/M/Y, known dtypes/shapes) — exactly what a
generated/compiled contract is for, and exactly what is left to hand-codecs
plus a runtime test today; **this is where codegen/lint is warranted.**

A new-language component then: **(1)** mirrors the env↔Policy seam with a
**composable Policy interface** in its own language (`RandomPolicy` today, a
search/MLP policy later); **(2)** **derives** its read/write of the keys and
byte layouts from the one authority `transport.py` spells — reading the manifest
at runtime for the dynamic weight layout, and (the floor) a build-time lint
that fails on a format-constant disagreement for the static result layout —
authoring **no second hand codec**; **(3)** **reimplements the surface behind
the seam** (belief mechanics, `forward_core`) against the wire, not by
translating Python objects; **(4)** is **validated by parity** under the **P6
behavioral-equivalence bar** (matched-seed aggregate-stat comparison vs the
Python reference) — as the **backstop**, not the primary guarantee. The full
concrete contract is the dedicated section below.

**Implementation guidance (examples, not mandate).** For raw float blobs on a
hot path a **zero-copy IDL** (FlatBuffers / Cap'n Proto) fits better than
protobuf's parse-and-copy; the **floor** is a build-time lint that fails on a
format-constant disagreement. The MVP's runtime parity test stays — as the
backstop.

**Cancer prevented: the cross-language form of B (two writers of one truth —
a hand-mirrored type or codec that drifts from its authority, across the
hardest boundary to audit, where the drift is silent) and G (load-bearing
format knowledge left in an unenforceable runtime-only convention instead of
generated/compiled/linted from one source) and C (shared mutable state across
processes).** A cross-boundary fact has one authoritative home; every side
derives its view and none re-authors it.

#### P8 — Typed signatures are the single source of truth of a function's contract

**Rule (checkable).** A function, method, or dataclass **signature is the
single source of truth of its input/output contract** — the call-boundary twin
of P1 (one home per fact) and of ADR-0002's no-lying-signature, P2's "a
parameter the receiver cannot honor is not in the signature." An annotation the
body does not honor is a **lying signature** — the type-layer form of the same
lie: `hp: AdamHParams = None` whose body proves `None` is an accepted value;
`lr/b1/b2/eps: float` fields populated with traced jax `Array`s (the exact two
defects the originating codebase's from-scratch strict-typing assessment — a
dated artifact of that project, not held in this repository — surfaced). The
bar is
**strict-where-achievable**: `mypy --strict`-clean at the maximal real
strictness a module can reach, where array internals annotated `NDArray[Any]` /
`Any` *satisfy* strict without any relaxation (they are honest types, not
escapes). The check: *(a) does every function/method/dataclass field carry a
param+return annotation? (b) does the body honor each — no value the annotation
forbids reaches a consumer? (c) does the module pass the strict gate, or is it
a documented backlog entry on the way in (see Self-application)?*

**Named-relaxation posture (constraint, not excuse).** A per-module
`ignore_missing_imports` is legitimate **only** for a genuine stub-gap — a
library that ships **no** `py.typed` and no stubs (numba's `@njit` erases the
decorated signature; optax's `GradientTransformation`; tensorboardX's logging
sink). A library that **is** typed (jax ships `py.typed`) must **not** be
blanket-ignored — silencing a checkable library is a convenience-relaxation, so
its friction is instead a **commented `Any` at the use site**, visible in the
diff, distinguishing constraint from excuse in the source itself. Each escape
stays honest by `warn_unused_ignores` (a relaxation that stops being needed
fails CI). And — **reusing P7's no-scale-excuse rule verbatim** — *never*
justify a weaker bar with a scale / "one maintainer" / "for now" / minimality /
YAGNI argument; that argument shape is the tell P7 already named and rejected
(the discipline applied once at small scale is exactly how the cancers grew). A
weaker bar is justified only by a named, verified stub-gap, never by extent.

**Worked example (this tenet's originating codebase).** The single genuine bug cluster a from-scratch
strict typecheck found was a default value proving an annotation false (a
parameter typed as non-optional whose body accepted the omitted case) and a
tuple type declaring plain-float fields that a constructor actually populated
with a different numeric-array type — both P2/ADR-0002 **lying signatures**
surfaced at the type layer. The contrasting clean case: a genuinely
backend-polymorphic parameter typed `Any` at a real dispatch seam, honest
because the type really is "either backend," not a relaxation.

**Cancer prevented: untyped / lying-signature contracts.** A contract carried
only by an unenforced signature lives nowhere checkable (the call-boundary form
of G — load-bearing knowledge in unenforceable convention); a contract carried
by a *dishonest* signature is worse — it asserts a guarantee the body breaks,
the call-boundary form of B's two-writers (the signature says one thing, the
body another) and of ADR-0002's silently-accepted lie. P8 makes the signature
the one honored authority, checked by the gate below.

#### P9 — Functional core, imperative shell (the compiled-component contract)

**Rule (checkable).** In a **compiled (C++/new-language) component**, a
computation is a **pure function of typed inputs that returns its result by
value**; effects (I/O, the redis transport, the episode/inference loop, buffer
lifetimes, absence, and failure) live in a thin **imperative shell** that calls
the pure core. This is **P8's typed-contract rule carried into the compiled
component** and **P2's no-hidden-state rule sharpened by C++**, where a raw `T*`
erases — whichever way it points — the bounds and const an input depends on, or
the nullability an output depends on. The discipline costs **no
performance**: it is built on **zero-cost abstractions** (a `std::span<const T>`
compiles to the same pointer+length a hand-rolled pair would; **guaranteed copy
elision / (N)RVO** makes return-by-value free), so the honest signature is not a
tax paid for cleanliness — it is the same machine code with the contract
restored.

**The general posture (P9 is the modern-C++ discipline).** The five rules below
are specific instances of one general principle, and P9 states that principle
explicitly so the discipline extends past the enumerated five. For any **legacy
/ C-with-classes / pre-modern construct** — a *reliquary* form carried forward
out of habit — there is usually a **standard C++ (11–23) feature designed
precisely to make it safer, clearer, or both at zero runtime cost**, and that
feature is **preferred**; the reliquary form is **forbidden absent a measured
reason**. *Check (general): for the construct under review, is there a standard
modern feature designed to replace it? If so, the legacy form needs a measured
justification — a profile showing the modern form costs runtime here, or a real,
named constraint (a fixed C ABI to interoperate with, a toolchain that genuinely
lacks the feature) — never a habit one.* The carve-out is exact: **only a
profile or a real, named constraint** licenses the reliquary form. *That's how
it's always been done* / *it's how I learned C++* / *the old way is fine here* is
**habit, not a reason** — the same lazy-argument shape as the scale-excuse P7/P8
already reject (the discipline declined "just this once" is precisely how the
cancers grew), and P9 rejects it in the same words: a *measured* reason justifies
the legacy form, a habit one never does.

The five rules below are **the current catalog of this principle, not an
exhaustive list** — the principle is general and extensible, and a reliquary
construct outside the five is governed by the general check above just as the
five are governed by their specific forms. A **representative (explicitly
non-exhaustive) catalog** of the same move beyond the five — in each row the rule
is "use the designed replacement," and the list is only its illustration: raw
`new`/`delete` → RAII / value semantics / smart pointers (`std::unique_ptr`,
`std::make_unique`); C-style casts → named casts (`static_cast` /
`reinterpret_cast`, which say *which* conversion and are greppable); `#define`
constants and function-like macros → `constexpr` / `consteval` / templates
(typed, scoped, debuggable); C-string functions (`strcmp` / `strcpy` / `strlen`)
→ `std::string_view` / `std::string` (bounds-carrying, no manual terminator);
`NULL` → `nullptr` (a typed null, no `int`-conversion ambiguity); `typedef` →
`using` (reads left-to-right, and aliases templates); unscoped `enum` →
`enum class` (scoped, no implicit-int decay); a hand-rolled index loop →
range-based-`for` / `<algorithm>` / ranges where clearer; a manual
acquire/release resource pair → an RAII handle whose destructor releases. In
every row the modern feature is the standard library's or language's answer to
the exact hazard the legacy form leaves open, at no runtime cost — so the
reliquary form, not the modern one, is what carries the burden of a measured
justification.

Five **checkable rules a reviewer enforces yes/no from the signature
alone** — the catalog of the general principle at the call/computation
boundary:

1. **Inputs *and outputs* are typed, bounds-carrying, const-correct — no raw
   or nullable pointer crosses the signature in either direction.** Favor
   `std::span<const T>` (or a typed view) over a raw `T*` or a `T*, size_t`
   pair — the span carries the extent and prevents the out-of-bounds the raw
   pointer silently invites; a non-trivial read-only input is `const&`. The ban
   is **directional-symmetric**: a raw-pointer or nullable-pointer **output** is
   as forbidden as a raw-pointer input — a returned string is a
   `std::string_view`, a returned-or-absent string a
   `std::optional<std::string_view>`, never a `const char*` that may be
   `nullptr`. A raw/nullable pointer erases the same contract whichever way it
   points: as an input it erases the extent and const-ness; as an output it
   erases the **nullability** (the `T*` return says nothing about whether
   `nullptr` is a sanctioned value), so a missed null-check is undefined
   behavior the type did not warn against. *Check: does any signature take **or
   return** a raw `T*` / nullable pointer where a `std::span<const T>` /
   `std::string_view` (always present) or a `std::optional<…>` (legitimately
   absent — rule 5) would carry the contract? Is every read-only input `const`?*
2. **Outputs are returned by value.** A function that computes a value
   **returns** it — exploiting guaranteed copy elision / (N)RVO so the return is
   free — not a `void f(…, Out& out)` that writes through an output parameter.
   *Check: does the function return what it computes, or mutate an
   out-parameter? A primary result delivered through an out-parameter is
   forbidden.*
3. **The signature declares every effect.** A function mutates only what its
   signature names — an explicit non-`const` `&` parameter that **is** the
   declared purpose, or `this`; never a global/static, never a parameter whose
   mutation is not the function's stated job. *Check: can a reviewer name, from
   the signature alone, every piece of state the function mutates? An invisible
   mutation is forbidden.*
4. **The ML hot-path exception is explicit and typed.** When **measured**
   allocation overhead on a hot path (an inference / episode loop) genuinely
   requires reusing buffers, the mutable scratch is isolated into an
   explicitly-typed **`Workspace`/`Context`** struct passed as an explicit
   `Workspace&` parameter — so the reuse is a **declared, typed requirement of
   the computation**, not a hidden side-effect. The core stays otherwise pure:
   it reads its typed inputs, uses the `Workspace` as named scratch, and
   **still returns its result by value**. *Check: is every hot-path
   buffer-reuse mutation routed through an explicitly-typed `Workspace`/
   `Context` parameter, with the result still returned by value, and is that the
   only mutation?* **Reusing the P7/P8 no-excuse posture verbatim:** a hidden
   mutation is justified **only** by a measured, named hot-path requirement
   expressed as a typed `Workspace` — *never* by a scale / minimality / "it's
   faster" / "for now" / "unnecessary here" / YAGNI argument. That argument
   shape is the tell P7 and P8 already named and reject; "faster" with no
   measured allocation profile and no typed `Workspace` is a hand-wave, and the
   hand-wave is exactly how the cancers grew.
5. **Absence and failure are BOTH typed return values — `optional` for the
   legitimately-absent, `expected` for the fallible — never a sentinel or a
   nullable pointer.** A result that may be **legitimately absent** is a
   `[[nodiscard]] std::optional<T>` returned by value; a result that may
   **fail** is a `[[nodiscard]] std::expected<T, Error>` returned by value. The
   distinction is drawn precisely and is **not interchangeable**: `optional` =
   "there might be nothing, and that *is* a valid, expected outcome the caller
   chooses what to do with" (a CLI flag the user did not pass, a lookup with no
   match — no error has occurred); `expected` = "it might **fail**, and the
   caller must handle a named `Error`" (a malformed payload, an unreachable
   redis — something went wrong). Choosing `optional` where the absence is
   actually a failure throws away the diagnosis; choosing `expected` where the
   absence is routine fabricates an error category. What is forbidden in **both**
   cases is the same: a **nullable raw pointer** (`const char*` that may be
   `nullptr`) or any **sentinel** (`nullptr`, `-1`, `""`, an empty-but-valid
   value standing for "not found"). A nullable raw pointer is the **worst of
   both** — the absence is invisible in the type (a `T*` return declares nothing
   about whether `nullptr` is a sanctioned value) so a missed check is
   **undefined behavior**, *and* if the absence was really a failure the error
   carries no diagnosis. This is **ADR-0002's sentinel-instead-of-raise red flag
   in the C++ register** — a nullable pointer or magic return is the C++
   sentinel ADR-0002 names — lifted from convention to **compile-enforcement**:
   `[[nodiscard]]` on the `optional`/`expected` return makes *ignoring* the
   absence-or-error a **compile error** (compile-time > runtime in the loudness
   hierarchy P5 defers to), where a nullable pointer's missed check compiles
   silently. (And — composing with **P8** — a `T*` return that may be `nullptr`
   is a **dishonest contract**: the type does not carry the nullability the body
   relies on, the call-boundary lie P8 forbids, here in the compiled register.)

   **Failure, specifically, is never an exception either.** Where the result is
   the `expected` kind, a fallible computation reports failure as that **typed
   value** — never by throwing. An exception is the purest untyped effect: a
   control-flow escape that appears **nowhere in the signature**, that the
   caller is not forced to handle, and that makes the function no longer a total
   value-function of its inputs (the rule-2/rule-3 violation in the one register
   the other four do not cover) — the **control-flow** twin of the nullable
   pointer's **value** sentinel, both of them absences/failures the signature
   hides. `std::expected` makes the error path a **declared part of the return
   type**, and `[[nodiscard]]` makes *ignoring* it a **compile error** — lifting
   ADR-0002's fail-loud to its strongest surface. The **functional core is
   total** — pure arithmetic over already-validated inputs, it neither throws
   nor returns `expected`; the error surface lives entirely in the **imperative
   shell**, at its boundaries (I/O, parsing, construction). A throwing
   **constructor** (which cannot return a value) becomes a static factory:
   `T::create(…) -> std::expected<T, Error>` with a private `noexcept` ctor.
   *Check: does any function signal an absent result with a sentinel / nullable
   pointer instead of a `[[nodiscard]] std::optional<T>`, or a failure by
   throwing (or by a sentinel) instead of returning a `[[nodiscard]]
   std::expected<T, Error>`? Is the absent-or-error path in the return type and
   forced on the caller? Is the core total (throw-free)?* The one thing
   `std::expected` does **not** absorb:
   a genuine **invariant violation** — a state the code's own logic guarantees
   impossible, i.e. a bug — is an `assert`/contract abort, not an `expected`;
   `expected` is reserved for the *recoverable, expected* boundary conditions
   (a missing redis payload, a malformed manifest) an operator or upstream
   causes and a caller can report. The two are categorically distinct: a
   `std::expected` value is for what the world legitimately hands you and the
   program must handle; an abort is for what your own invariants say can never
   happen and a return value would only paper over. (`std::expected` is C++23;
   the toolchain — GCC 15.2 — provides it, so the compiled components build at
   `-std=c++23`.)

> **Extracted record — P9's three worked examples**
> (moved verbatim to
> [`history/0012-p9-worked-examples.md`](history/0012-p9-worked-examples.md)):
> three worked diagnoses of an as-merged, pre-P9 C++ MLP forward-pass
> component against the five checkable rules above. The anchor example shows a
> `predict` entry point that returns by value (rule 2 honored) but takes a raw
> unbounded pointer (rule 1 violated), backed by internals that are pure
> untyped-effectful `void` functions writing through out-parameters instead of
> returning a value — the compliant form threads `std::span`/`std::optional`
> throughout and moves the one measured hot-path buffer reuse into a typed
> `Workspace` parameter. The error-axis example shows a total, throw-free
> compute core whose *boundary* functions throw `std::runtime_error` instead of
> returning `std::expected` — the compliant form makes every boundary failure a
> typed, `[[nodiscard]]`-enforced return value, reserving `assert`/abort for a
> genuine invariant violation rather than a recoverable boundary condition. The
> optionality-axis example is a CLI flag parser returning a nullable
> `const char*` for "flag absent" — the C++ sentinel ADR-0002 names — with the
> compliant `std::optional<std::string_view>` form and the one sanctioned
> untyped→typed decode at the boundary (`main`'s `argv` translation).

**Cancer prevented: untestable, uncomposable black-box mutations.** A
`void`-returning, raw-pointer-taking, out-parameter-writing function in a
compiled component is the compiled form of three cancers at once — **B** (a
second/hidden writer of state the signature does not name), **P2's hidden state
/ lying signature** (a contract the signature does not carry), and **P8's
untyped contract** (no bounds, no const, no return declared) — sharpened by C++,
where the raw `T*` erases the very bounds and const-ness the contract needs. The
functional core makes each computation a value you can test in isolation and
compose, and confines every effect to the named, typed imperative shell.

---

## Concrete guidance for a new-language (C++) component

A new-language component (a C++ or similarly compiled search/sim runner, in
this tenet's originating instance) applies P1–P2, P6, and P7 together at one
real cross-language boundary: **(1)** it mirrors the existing seam pattern with
a composable Policy-shaped interface in its own language — zero edits to the
core to add a capability; **(2)** it **derives** its read/write of every
cross-boundary key and byte layout from the one authoritative definition the
host language already owns, never re-authoring a second hand codec — a
*dynamic* layout (residual/optional fields, instance-derived dimensions) is
legitimately read from a self-describing runtime manifest, while a *static*
layout (fixed, known dtypes/shapes) is the case codegen or a build-time lint
is for; **(3)** it reimplements the domain surface *behind* the seam, against
the wire, not by translating host-language objects; and **(4)** it is
validated by parity under the P6 behavioral-equivalence bar (matched-seed
aggregate-statistic comparison against the reference implementation) as the
**backstop**, never the primary guarantee — the primary guarantee is the
generated/compiled/linted contract of step (2).

> **Extracted record — the concrete C++ wire contract**
> (moved verbatim to
> [`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md)):
> the fully worked instance of the four steps above against a real system — the
> actual redis key formats, weight-manifest layout, and four named result
> blocks a Gumbel-AZ search worker's C++ port derives from its Python
> reference, plus the parity-validation plan (logic invariants asserted
> bit-exactly; float-sensitive numerics held to aggregate behavioral
> equivalence over N≥300 episodes across ≥2 seeds). It is the fullest worked
> demonstration of applying P1/P2/P6/P7 together at one cross-language
> boundary, kept as dated evidence rather than restated as general guidance —
> another project's wire contract has its own keys, payload shapes, and
> reference implementation to derive from.

## Self-application (ADR-0011 Rule 1 — enforcement surface)

Per ADR-0011 Rule 1, this tenet declares **how each principle is enforced**,
against ADR-0011's closed vocabulary (construction-time / test-CI gate /
write-time data constraint / run-time invariant / review-only):

- **P1 (SSOT):** mostly **run-time invariant + test/CI gate** where mechanized
  (`FeatureLayout`'s contiguous-partition assertion; the equivalence tests; a
  `feature_names` test); **review-only** for new facts until their mechanism is
  minted (ADR-0011 Rule 2 is the conversion trigger).
- **P2 (seam/port):** **review-only at design**, with the ACL's strict decode a
  **construction/import-time** raise where a boundary exists (see
  [ADR-0011](0011-mechanization-discipline.md)'s `hp` registry decode).
- **P3 (no god-objects):** **review-only** (a one-clause-responsibility
  judgment), composing with ADR-0007's review-only file budget.
- **P4 (live, not frozen):** **construction-time + run-time** where the registry
  facet discipline applies (the loud RESTART-drift refusal is a construction/
  run-time raise); **review-only** for placing a new tunable in its tier.
- **P5 (fail loud / root cause):** inherits **ADR-0002's full loudness
  hierarchy**; the guard-vs-band-aid distinction is **review-only**.
- **P6 (substantiate):** inherits **ADR-0009's** surface (test/CI gate for the
  bit-exact and forward-`ABS_TOL` parts; review-only-with-explicit-absence for
  the behavioral part).
- **P7 (cross-language wire):** enforced at the **strongest feasible level** —
  the **static** result format wants a **generate/compile-from-one-schema or
  build-time-lint gate** (a Python/C++ format-constant disagreement fails the
  build); the **dynamic** weight layout is enforced by the **runtime manifest**
  the C++ derives `(offset, len, shape, dtype)` from (its residual gap: the
  manifest's own schema is still two hand-written codecs). Below those sits the
  **runtime parity test/CI gate** (matched-seed aggregate comparison) as the
  **backstop**, plus the **construction-time** loud failure on a missing/
  malformed payload (`read_weights`' `RuntimeError`). Until the static-format
  codegen/lint is minted, that gap is **review-only** — but settling for the
  runtime-test-only backstop is *not* justified by a scale / minimality / "for
  now" argument (that argument shape is the tell P7 rejects).
- **P8 (typed signatures):** **test/CI gate** — the **mypy `--strict` CI gate**
  (`pyproject.toml` `[tool.mypy]` + `tests/test_mypy_strict.py`) is the
  ADR-0011 Rule-1 mechanism that converts "typed signatures" from review-only
  prose into an enforced contract. It runs `mypy --strict` against an explicit
  `STRICT_CLEAN` set and asserts zero errors, ratcheting a
  **monotonically-decreasing baseline module-by-module** (a staged rollout the
  same strict-typing assessment cited under P8 planned in full): a module
  joins the gated set as it is typed, and a regression in
  any gated module's annotations fails CI. A module is **review-only** until it
  joins that set — and that join is the ADR-0011 Rule-2 conversion trigger (the
  recurrence that converts review-only prose to a mechanism), here a scheduled
  monotonic rollout rather than a defect. `warn_unused_ignores` keeps each
  named relaxation honest at the same gate.
- **P9 (functional core, imperative shell):** a **mix** — the error axis
  (rule 5) is **partly compile-enforced**, the other four (input/output/mutation)
  are **review-only**. `[[nodiscard]]` on every `std::expected`-returning
  boundary function, **with the nodiscard warning treated as an error**, makes an
  **unhandled error a build failure** — a strictly stronger surface than the
  review-only structural rules, and the same compile-time-over-runtime move ADR-
  0011 Rule 1 ranks highest. Rules 1–4 are policed against their checkable form
  at C++ review, with the **compiler (`-Wall -Wextra`)** as the standing floor
  and a **future `clang-tidy` config** as the mechanization surface. The compiler
  already raises some of the relevant signals (an unused parameter, a
  const-violation); the `clang-tidy` config is the ADR-0011 Rule-2 conversion
  trigger — **when a P9 violation recurs after this record, mint the `clang-tidy`
  check** that catches it (e.g. a check against out-parameters or raw-pointer
  arithmetic where a `std::span` belongs, or `-Werror` on a thrown exception
  escaping a function the contract says should return `expected`) rather than
  re-stating the rule in prose. The **general modern-C++ posture** has a
  concrete, purpose-built mechanization surface: clang-tidy's **`modernize-*`
  check family** exists *precisely* to catch the reliquary→modern substitutions,
  so the ADR-0011 Rule-2 trigger for the general principle is to **enable the
  `modernize-*` (or `cppcoreguidelines-*`) check that catches the recurring
  reliquary form** — `modernize-use-nullptr` (`NULL` → `nullptr`),
  `modernize-use-using` (`typedef` → `using`), `modernize-avoid-c-arrays`,
  `modernize-loop-convert`, `modernize-make-unique`/`-make-shared` (raw `new` →
  smart pointers), and `cppcoreguidelines-pro-bounds-pointer-arithmetic` /
  `-pro-type-cstyle-cast` for the bounds/cast rows — each the standing answer to
  one catalog row. Until the check for a given recurrence is enabled, that
  construct is review-policed against the **general check** (is there a designed
  modern replacement?), with the compiler the floor. Until those recurrences
  fire, rules 1–4 and the general posture are review-only — and settling for
  review-only is *not* justified by a scale / "one compiled component" / "for
  now" / "that's how it's always been done" argument (that argument shape is the
  tell P7/P8 reject); it is the honest ADR-0011 Rule-1 level for a discipline
  whose recurrence has not yet fired.

This tenet's own Rule-1 declaration: **review-and-audit-policed**, with the
architectural audit as the absence-detector — exactly as ADR-0011 declares for
itself. Its protection is the structure it shapes at authoring time, not its
prose.

## Consequences

### Positive

- **New code is born clean.** The incoming C++ runner and the future async loop
  are authored against a closed checklist of the exact diseases the audit found,
  so the audit's "subtraction and relocation" remediation is never needed for
  them — they never accrete the rot. This is the whole point: propagation by
  default of disciplines the codebase already proved (the env↔Policy seam, live
  λ, derived dimensions).
- **The cancer taxonomy becomes a forward-looking checklist, not just a
  diagnosis.** The audit is point-in-time and not retro-edited (ADR-0005 Rule
  8); this ADR carries its lessons forward as authoring rules so the next
  contributor scans a list rather than re-deriving the lessons.
- **Every cross-language fact has exactly one authoritative definition.** P7
  gives each cross-boundary layout/key/format one home from which every side
  derives — separating the serialization contract from the transport/
  coordination mechanism (a bytes-store holds state, a messaging fabric carries
  coordination) — so "swap the worker for C++" stays a drop-in and a second
  hand-author of one truth cannot form across the language boundary.
- **The compiled component is value-functional, not a black box.** P9 makes each
  C++ computation a pure function of typed, bounds-carrying inputs returning its
  result by value, so the compiled core is unit-testable and composable rather
  than an untyped-effectful void — at zero performance cost (zero-cost
  abstractions: a `std::span` is a pointer+length, return-by-value is free under
  (N)RVO), with the one hot-path buffer-reuse exception declared as a typed
  `Workspace` parameter rather than hidden. Absence and failure, too, are
  **typed return values** — a legitimately-absent result a `[[nodiscard]]
  std::optional<T>`, a failure a `[[nodiscard]] std::expected<T, Error>`, never
  a nullable pointer or sentinel whose absence is invisible in the type (the C++
  form of ADR-0002's sentinel red flag, a P8 dishonest contract) and never an
  untyped thrown escape — so the absence/error path is declared in the return
  type, ignoring it is a **compile error** (ADR-0002 fail-loud at its strongest
  surface), and the core stays total while that surface lives at the shell's
  boundaries.

### Negative

- **Per-authoring overhead.** Each new structure carries a checklist pass; most
  principles are review-only (ADR-0011 Rule 1), so they are policed by attention
  until a recurrence mints a mechanism (ADR-0011 Rule 2). This is the same
  policy-vs-mechanism cost ADR-0003–0009 carry.
- **Some rules are judgments, not measurements.** "No god-object" (one-clause
  responsibility) and the guard-vs-band-aid distinction are calibrated at
  review, like ADR-0007's density heuristic and ADR-0008's severity. ADR-0008's
  substitution test (calibrate to the worst case the shape could apply to, not
  the observed instance) calibrates the cost honestly.

### Neutral

- **No retroactive sweep** (ADR-0004's incremental-retrofit posture). Existing
  code is cleaned by the audit's own remediation roadmap (its numbered "R"
  items) on its own schedule, not by this ADR; this ADR binds **new**
  structure. Existing rules retrofit on touch.
- **No new infrastructure mandated beyond what that remediation roadmap
  already names.** The worked mechanisms (`FeatureLayout`, `BeliefRefs`,
  `WeightContainer`, the wire authority described in
  [`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md))
  are the audit's, surfaced here as this tenet's examples — not new builds
  this ADR commissions.

## Revisit when…

1. **A principle introduces its own failure mode.** Flag the offending rule
   here by dated amendment (ADR-0005 Rule 8).
2. **The C++ runner lands and the wire contract proves incomplete.** If the
   parity work surfaces a wire detail P7 under-specifies (an endianness
   ambiguity, a manifest field the C++ side cannot reconstruct), record the
   clarification here and repoint the contract — `transport.py`'s docstring is
   the live SSOT, this section the rationale.
3. **A new-language component beyond C++ joins** (a Rust core, a GPU service).
   P7 is stated over "a new-language component," not C++ specifically — and a
   third reader of a static layout is **exactly** the recurrence at which
   generating/compiling the serialization contract from one schema becomes the
   right move (ADR-0011 Rule 2). Confirm the one-authoritative-definition /
   derive-don't-re-author rule survives the new component's constraints, and
   that the static formats are mechanized at the strongest feasible level
   rather than gaining a third hand codec; amend if not.
4. **A principle's review-only enforcement recurs into a defect** (ADR-0011
   Rule 2). The recurrence converts the principle to a mechanism at the
   strongest feasible-and-proportionate surface; record the mechanism here.
5. **The async actor-learner restructure lands** (the originating codebase's
   forward-looking design note's third deployment shape). It relaxes the
   aggregate bit-determinism P6 records as a deliberate trade; confirm the
   trade is still the right one and that per-episode exactness held.

## Related

- **[ADR-0002](0002-fail-loudly.md) (fail loudly).** P5 defers to it wholesale
  for the loudness hierarchy; the missing-weight-payload `RuntimeError` and the
  RESTART-drift refusal are its mechanisms in the wire/registry register.
- **[ADR-0004](0004-minimal-touch-edits-to-partially-visible-files.md)
  (minimal-touch).** Owns the no-retroactive-sweep posture this tenet's
  scoping defers to; new structure is born clean, existing structure is
  retrofitted on touch.
- **[ADR-0005](0005-documentation-discipline.md) (documentation discipline).**
  Rule 1 (single-source-of-truth-per-handle) is P1's documentation twin; this
  tenet is its structural form. Rule 8 (amend point-in-time records by append)
  governs how the audit is cited without retro-editing it.
- **[ADR-0007](0007-file-size-and-information-density.md) (file size /
  information density).** P3 (no god-objects) produces small files; ADR-0007
  owns the budget and the density heuristic. They reinforce; neither restates
  the other.
- **[ADR-0009](0009-performance-investigation-discipline.md) (perf/equivalence
  investigation discipline).** P6 composes with it directly and imports its
  two-tier (bit-exact vs aggregate-behavioral) bar; the cross-language parity
  of P7 is that bar applied across the language boundary.
- **[ADR-0011](0011-mechanization-discipline.md) (mechanization discipline).** This tenet is upstream of it:
  structure born clean is structure ADR-0011 never converts. ADR-0011's worked
  mechanisms (`FeatureLayout`, `BeliefRefs`, the param-registry serializer, and
  the **mypy `--strict` CI gate** that backs P8) are this tenet's worked
  examples; its Rule 1 governs this tenet's enforcement-surface declaration
  above, and its Rule 2 (recurrence → mechanism) is the trigger by which a
  module joins P8's gated set.
- **This tenet's originating architectural audit** (a dated artifact of the
  source project, not held in this repository). Every anti-pattern A–H here
  inverts one of its cancers, and its own remediation roadmap is the cleanup
  of existing code this ADR's forward-looking rules make unnecessary for new
  code.
- **This tenet's originating cross-language seam design** (see
  [`history/0012-cpp-wire-contract.md`](history/0012-cpp-wire-contract.md)).
  The four-seam composition and deployment shapes the C++ section
  operationalizes; P7's concrete wire contract is cited there against that
  design's own worked instance.

## Amendments

*Per ADR-0005 Rule 8 (amend point-in-time records by append; never silently
rewrite), each entry is dated and additive. The original Decision (the
anti-pattern checklist table and the nine principles above) stands unedited;
amendments extend it.*

### Amendment — 2026-06-20: P7 lifted from the cross-LANGUAGE wire to the cross-DEVICE boundary: gratuitous host↔device transfers

*(Provenance: ADR-0011 Rule 2 / this ADR's Revisit #4 — a review-only
principle's enforcement recurring into a *measured* cost, on this tenet's
originating codebase. See the Extracted record below for the full benchmark
narrative.)* P7's "derive-don't-re-author across a boundary" was stated and
enforced for the cross-*language* wire. The same compositional sin — a
boundary crossing scattered across N call sites instead of isolated at *one*
auditable home — recurs at the cross-*device* (host↔device, e.g. numpy↔jax)
boundary, and the recurrence was measured, not hypothetical: a per-call cost
of tens of microseconds turned out to be nothing but a repeated host→device
parameter transfer that a naive dispatch redoes every call, and a production
inference path paid an even larger cost on its input/output transfer. A
recurrence with a measured cost is exactly the ADR-0011 Rule-2 trigger that
converts a review-only principle to a mechanism.

**New anti-pattern row (extends the anti-pattern checklist table — appended,
not edited into it):**

| Audit cancer / boundary | The shape to never author | Preventing rule |
| --- | --- | --- |
| **(new, cross-DEVICE)** — gratuitous host↔device transfer | a host↔device crossing (a framework-specific host→device stage, or a blocking device→host pull) scattered at an arbitrary call-site instead of isolated at one designated boundary, so a per-call re-stage or a redundant pull hides a real, measurable cost no one site owns | **P7** lifted from the cross-LANGUAGE wire to the cross-DEVICE boundary (composing with **P1** one-home/derive-don't-duplicate and **P2** seam/Port-ACL): a host↔device crossing has **one authoritative, auditable home** — a *designated boundary* — from which the hot path stages once and consolidates, never N scattered re-stages; mechanically enforced at the strongest feasible-and-proportionate level (an AST gate + ratcheting baseline, ADR-0011 Rule 1, mirroring a strict-typing CI gate), so a NEW transfer outside a boundary fails CI |

**The rule (the cross-DEVICE register of P7/P1/P2).** A host↔device transfer
**CALL-SITE is allowed only at a designated boundary**; anywhere else is a
violation. This is **P1** (the crossing has *one* home, not a literal re-typed
at N sites), **P2** (the boundary is an *explicit* port, not a reach scattered
through the hot loop), and **P7** (a cross-boundary fact — here the *device*
boundary, not the *language* wire — is isolated and derived-from-one-home,
never re-authored ad hoc). Isolating the crossings is what makes a
consolidation (stage once; one pull per call) a *local* edit at the boundary
rather than a tree-wide hunt — the same way the SSOT wire makes "swap the
worker for a new language" a drop-in.

**The mechanism (ADR-0011 Rule 1 — this principle's enforcement surface, now
upgraded for the cross-device boundary from review-only to a test/CI gate).**
A pure-AST walker flags transfer call-sites by name pattern and asserts each
sits at an inline-marked or whitelisted boundary, against a ratcheting
baseline (today's non-boundary transfers grandfathered, keyed structurally
over the class of crossing, never a churning line number — ADR-0011 Rule 4);
a new, non-baselined transfer outside a boundary fails CI, and the checker
carries a negative/mutation self-check proving a synthetic new transfer fails
and a boundary marker passes.

> **Extracted record — the cross-device bench saga**
> (moved verbatim to
> [`history/0012-cross-device-bench-saga.md`](history/0012-cross-device-bench-saga.md)):
> the full measurement narrative this amendment's rule and mechanism rest on
> — the originating micro-benchmark that found the per-call cost, the AST
> lint's concrete implementation and adoption-time baseline, a same-day
> decomposition that isolated parameter staging (not the input/output
> transfer) as the dominant lever, the production consolidation that landed
> and its measured fixed-cost reduction, and a follow-up assessment that
> refuted the remaining input/output crossing as a further local win. The
> rule, the anti-pattern row, and the mechanism above are unchanged by any of
> it; the saga is the dated proof they were derived from a real measurement,
> not asserted.

**Scope (ADR-0004 no-retroactive-sweep, unchanged).** This amendment adds a
mechanism and grandfathers the existing crossings; it sweeps *nothing*. The
enforcement-surface declaration in §"Self-application" for **P7** is hereby
extended: P7 is now mechanized at the **test/CI-gate** level for the
**cross-DEVICE** boundary (the AST lint + ratcheting baseline above), in
addition to its existing cross-LANGUAGE surfaces (the runtime manifest for
the dynamic weight layout, a parity test as the backstop for the static
result format).

### Amendment — 2026-07-02: The corrective diff IS new structure (closing the scope gap the fixes walked through)

*(Reader's gloss, added 2026-07-27 as a dated append — the amendment text below stands
verbatim per ADR-0005 Rule 8: "CB-NN" codes here and in the later amendments are the
fact-mining recidivism study's own internal canonical-bug identifiers ("CB" = canonical
bug), from a project outside this repository; the study's source ledger is
`experiments/fact-mining/docs/recidivism-study/canonical_ledger.json` in the
originating codebase, as
[ADR-0000](0000-the-alpha-and-the-omega-type-driven-design.md)'s own 2026-07-02
amendment records. "OBS-2" is the same study's observation-numbering.)*

*(Provenance: the fact-mining recidivism study. CB-21: a pass-2 fix placed
`recv_bounded()` outside the guarded try, re-minting the exact
outside-the-guard shape pass 1 had found and fixed as CB-18 — two independent
executors, same nail, one pass apart. CB-31: a pass-3 fix ran its ceiling
screen as a separate pre-pass, doubling the warm preprocess — a P1
two-producers violation introduced BY the fix, in a pass whose commission
quoted P1. CB-32: a recovery fix put a blocking backoff and a
client-timing-reachable process exit on the hot serve thread.)*

This tenet's Scope binds "all **new** structure." The study proved the
reading that defeats it: a fix does not present to its author as new
structure — it presents as the discharge of an obligation — so the checklist
is never run against the very diffs most likely to be authored under
pressure, in code just proven hazardous. The scope clause is therefore made
explicit:

**A corrective diff is new structure.** Before a fix is claimed done, it is
passed against the anti-pattern checklist exactly as a green-field module
would be, and additionally against **the ledger of defect classes already on
record for the stack it touches** — a fix that re-mints a shape the record
already names (CB-21 re-minting CB-18) is the enumeration-fails-open failure
of ADR-0011 Rule 4, committed against the project's own history. Where a
defect ledger exists (a canonical-bug ledger, an audit's cancer table, this
checklist), consulting it is part of the fix, not part of some later audit.

New anti-pattern rows (appended, per this section's convention):

| Audit cancer / boundary | The shape to never author | Preventing rule |
| --- | --- | --- |
| **(new, corrective)** — a fix exempted from the checklist | a corrective diff authored and claimed done without a checklist pass or a consult of the stack's defect ledger, so the fix re-mints a cataloged cancer (an outside-the-guard refusal, CB-21; a second producer of one intermediate, CB-31; blocking work on the hot thread, CB-32) | **the scope clause above** — a corrective diff is new structure; checklist + ledger pass before "fixed" is claimed |
| **(new, proxy bound)** — a ceiling denominated in the wrong currency | a bound expressed in a unit OTHER than the resource that detonates (a char cap protecting a token budget, CB-08; a per-frame byte cap protecting aggregate memory, then a frame-count cap protecting wall time, CB-17/CB-15/CB-29; a round-number time literal orders above the warm envelope it protects, CB-30) | **P1 (derive-don't-duplicate) in the bound register** — a bound is denominated in, and derived from, the detonating resource (ADR-0000 Specimen 3's byte-budgeted high-water-mark is the worked form); each independent axis of an ingress surface (bytes, count, magnitude, time) carries its own bound, and a foreclosure claim names the axes it does not cover |

### Amendment — 2026-07-02: P2 extended to the COMPOSED system: a boundary advertises what it will refuse (refusals are backstops, not the interface)

*(Provenance: maintainer ground truth OBS-2
(`recidivism-study/maintainer_observations.md`): after three hardening
passes, every daemon boundary refused over-ceiling input with a correct
typed refusal — and the maintainer ruled the refusal a SYSTEM-level failure,
because no ceiling was ever advertised to the client before its first
request, so a legal user request failed against a limit it could not know.
Every Port individually satisfied this tenet's letter; the composed system
violated its spirit.)*

P2 commands that a boundary *translates-and-validates and refuses what it
cannot honor*. That rule is per-Port; this amendment states its
composed-system half:

**Within one system — services and the client libraries that are part of it —
a capability ceiling a boundary will enforce is ADVERTISED to the caller
before the caller must comply with it, from the same single source of truth
the enforcing gate reads (P1), over a pre-use surface (readiness/info); and
the system's own client libraries honor the advertisement transparently, so
a legal end-user request never fails against an unadvertised internal limit.
The typed refusal remains — as the backstop, never as the interface.** A
ceiling knowable only downstream — derivable only from a computed model
output (call it a "P" quantity), not from anything present in the request
itself (a "K" quantity) — is exempt from advertisement but not from honesty:
its disposition is stated explicitly at the earliest knowable point, not
discovered by detonation.

The discriminator against over-reading: this binds *inside* one system's own
composition. A hostile or third-party client still meets the refusal; the
rule exists so the system's own components never make an end user pay for a
contract two internal parties failed to exchange. The worked instance is the
OBS-2 remediation (`AdvertisedLimits` built at boot from the gate SSOT,
served pre-inference, one client-side planner, refusals as backstops), which
survived an out-of-frame hack-rationalization audit
(`recidivism-study/obs2_hack_audit.md`).

*Enforcement surface: run-time invariant where mechanized (the advertisement
is constructed FROM the gate constants, so gate/advert drift is
unrepresentable — P1); test/CI gate for the client-planner round-trip;
review-only for the judgment that a new ceiling is advertisable.*

### Amendment — 2026-07-18: The interpreter boundary — a value never crosses as program text

*(Provenance: witnessed 2026-07-18, ledger row 1637 — an operator verb spliced a
caller-supplied name into statement text handed to a command interpreter, at a
resolve stage and a teardown stage sharing one pattern. The instance was fixed
under commission; this amendment is the constitutional half, paired with
ADR-0000's same-day amendment: that record binds the reflex, this one owns the
shape.)*

New anti-pattern row (appended, per this section's convention):

| Audit cancer / boundary | The shape to never author | Preventing rule |
| --- | --- | --- |
| **(new, interpreter boundary)** — a value spliced into program text | any construction where data reaches a second evaluator (a command line, a query, a template, markup, a path/glob expression, a config fragment) by concatenation or interpolation into the text that evaluator parses and executes, so a value can alter the utterance's STRUCTURE instead of remaining a value | **P2 + P8 at the interpreter boundary** — data crosses an interpreter boundary as DATA, via the typed value-carrier the evaluating interpreter itself provides (a bound placeholder, an argument vector, a builder API); where no carrier exists, a strict validation to a closed alphabet at the Port, which refuses what it cannot honor and never rewrites an input into plausibility |

**The rule (checkable).** An **interpreter boundary** is any point where this
codebase constructs text that a second evaluator will parse and execute. The
checks: *(a) find every construction site whose result is handed to an
evaluator (concatenation, interpolation, template substitution into
evaluator-bound text); each such site is either a value-carrier call or a
validated-to-closed-alphabet construction with the validator adjacent, total
over its input type, and refusing on failure; (b) the validator REFUSES — it
never escapes, rewrites, or coerces the input into an acceptable form.*
Hand-escaping as the primary mechanism is banned — it is cancer **H**'s
band-aid in this register, betting on an encoder's completeness against an
open alphabet; at most it is defense-in-depth *behind* a carrier, never
instead of one. "The input is trusted here" does not exempt a site: the
carrier costs nothing, and the trust claim is exactly the in-the-moment
optimism P7/P8 already reject as an argument shape.

**Cancer prevented:** the composed form of **G** (load-bearing structure
carried in unenforceable text), **P2**'s coercing boundary, and **P8**'s lying
contract — a parameter received as type "name" that the construction site
actually treats as type "program fragment" is a dishonest contract between the
verb and its caller.

*Enforcement surface (ADR-0011 Rule 1, honest): review-only at authoring
today, with the corrective-diff clause above already binding every fix that
touches such a site. The ADR-0011 Rule-2 trigger is a single recurrence: the
mechanism to mint is a lint over verb/scaffold sources that flags expansion or
concatenation inside evaluator-bound text absent an adjacent closed-alphabet
validation or carrier call.*

### Amendment — 2026-07-22: P10 — data is not code (a tenth principle, deliberately not subsumed)

*(Provenance: the 2026-07-19 setup-TUI field test and its 2026-07-21 four-class
investigation (project ledger rows 1844–1850). Witnessed substrate: three modules of
one operator-facing package — the setup wizard, `tools/setup_tui/` — carried 44–63%
authored prose and configuration as Python literals, and a fourth interleaved
user-facing copy sentence-by-sentence through control flow; four fresh-context
ADR-compliance reviews of that same package surfaced SSOT and interpreter-boundary
defects and never the co-location, because no principle named it.
Maintainer-instructed: "data is not code; factor out the prompts." Drafted and
twice fresh-context-reviewed as
[design/CONSULT-ADR-0012-DATA-IS-NOT-CODE-AMENDMENT.md](../../design/CONSULT-ADR-0012-DATA-IS-NOT-CODE-AMENDMENT.md),
which proposed it as a P1 extension; at ratification (maintainer, 2026-07-22) it was
deliberately promoted to a standalone tenth principle instead, on the maintainer's
own reasoning: the failure mode is nasty precisely because it survived active
ADR-discipline — "one would think a brief glance at ADR-0012 would be enough that
such code smells never rear their heads, but look what happened, despite our
discipline" — so it earns a clause a reader cannot skim past, not a register-note
inside P1. The Decision section's "nine principles" framing stands unedited per
ADR-0005 Rule 8; this amendment adds the tenth by dated append.)*

New anti-pattern row (appended, per this section's convention):

| Audit cancer / boundary | The shape to never author | Preventing rule |
| --- | --- | --- |
| **(new, content boundary)** — authored content embedded as program text | operator-facing copy, teaching prose, prompt/screen text, feature catalogs, rules/config tables, or any other content whose edits are judged by reading it as *writing or configuration*, authored as literals inside a logic module — block-form (a hundred-line dict of prose) or interleaved (copy threaded through control flow) | **P10** — content has one home, and that home is a *data artifact* (a data-only module, a structured file, a keyed registry) the logic loads and renders; logic modules contain the strings that ARE logic (raised errors, log diagnostics, wire/protocol constants) and no others |

#### P10 — data is not code

**Rule (checkable).** A file answers to one editor identity. The check: *(a) for
each sizable literal, ask whose act a change to it is — a writing/config edit, or a
logic edit; a writing-edit literal inside a logic module is the violation; (b) a
module that is majority content by volume is a data artifact wearing a logic file's
name — split it so the data artifact is declared as such and the residual logic
imports it; (c) content is addressed by key/identity from logic (the renderer
receives typed content, it does not concatenate prose fragments inline).* The
discriminator against over-reading: error messages, log lines, docstrings, SQL/wire
constants, and format strings for internal state are the logic's own contract and
stay. A one-line label does not trigger the rule; a paragraph does; between them the
edit-identity question decides, calibrated at review like P3's one-clause test.

**Why a principle of its own, and not P1's:** the relation to P1 is real — this is
derive-don't-duplicate's sibling failure, not two homes for one fact but one home
serving two masters — yet the witnessed substrate proves subsumption is how it
hides: co-located content defeats both audiences at once (the prose reviewer cannot
see the copy as a document; the code reviewer wades through pages carrying no
decisions — ADR-0007's density red flag), every content edit lands as a logic diff
(maximizing ADR-0004's partial-visibility hazard for zero-logic changes), and
reviews aimed at P1/P2/P8 walked straight past it four times. It also composes with
the 2026-07-18 interpreter-boundary amendment above: content rendered by
interpolation into program-adjacent text is one refactor away from the splicing that
amendment bans, whereas content addressed as typed data is not.

**Cancer prevented:** the content-register form of **B** (a logic file becomes the
second, unacknowledged home of what is really a document), of **G** (load-bearing
operator guidance living where no writing review will ever read it), and the root
cause of chronic ADR-0007 violation in operator-facing modules (the witnessed
substrate: files at 1.2–3.6× the ceiling whose overage was majority content).

*Enforcement surface (ADR-0011 Rule 1, honest): review-only at authoring — the
edit-identity question is a judgment; `gates/max_lines.py` (ADR-0007's
mechanization, minted from this same field test) is the blunt backstop that forces
the question when a file grows past budget. The ADR-0011 Rule-2 trigger: if a
majority-content logic module recurs after this record, mint the measured check (a
literal-volume-fraction lint over the affected package) rather than re-stating the
rule in prose. No retroactive sweep: the witnessed offenders are already queued
under their own remediation track; existing files elsewhere retrofit on touch
(ADR-0004). The Self-application section's P1–P9 enforcement catalog likewise
stands unedited per ADR-0005 Rule 8: P10's enforcement-surface declaration lives
in this paragraph, in the amendment that mints the principle — the same
disposition the 2026-06-20 amendment used when it extended P7's declaration from
its own text.*

### Anecdote — 2026-07-27: P1's "home" is read at the unit of CHANGE, not the unit of topic (the monolithic-validator misreading)

*(Dated append per ADR-0005 Rule 8; recorded in
[ADR-0000](0000-the-alpha-and-the-omega-type-driven-design.md)'s anecdote register —
the facts carry it. Provenance: the s61 kernel regression of 2026-07-26, project ledger
rows 1429/1430 (the witnessed four-branch drop), 1432 (the three remedies), and the
maintainer's own question of 2026-07-27 — "the monolithic validators seem a violation
of ADR-0012 to begin with?" — which this append answers on the record. A "validator"
here is a kernel-side `BEFORE INSERT` trigger function that refuses an illegal ledger
write with a teaching message; the specimen is `validate_supersession_target` in
[`kernel/lineage/`](../../kernel/lineage/).)*

**The misreading, caught in the act.** P1 says every fact has exactly one home. The
project's kernel used that rule to justify keeping ALL supersession discipline —
belief holder-only revision, three missive retraction rules, signature symmetry — as
branches of one PL/pgSQL function, and the build record praised the shape in P1's own
vocabulary ("validate_supersession_target's single home"). The 2026-07-26 incident
showed what that reading missed: those are not one fact but FIVE, and in PL/pgSQL the
unit of change is the whole function body (`CREATE OR REPLACE` replaces everything),
so amending any one fact forces re-stating every other from whatever copy the author
has at hand. One re-issue (`s61`) worked from a stale copy and silently deleted four
refusal branches while correctly adding its own — and the SQL/ASP differential was
structurally blind to the drop (a refused write never becomes a row), so nothing
mechanized caught it.

**The rule, sharpened.** P1's "home" is read at the **unit of change**, never the unit
of topic: a home whose replace-unit forces you to restate facts your change does not
touch is not one home — it is N homes trading under one name, each exposed to every
other's re-issues. The topic-level single home the original reading wanted is
legitimate, but it belongs to the COMPOSITION — a thin dispatcher that names which
per-fact leaf functions apply — while each fact keeps its own change-unit-sized home.
(This is the same granularity move `FeatureLayout` made for the duplicated table in
P1's worked example, applied to code instead of data.)

**The tell, checkable in review.** A diff that RESTATES rules it does not change is
the violation surfacing: if your re-issue's body contains the verbatim text of
refusals your delta has no business touching, the fact-granularity and the
change-granularity have diverged, and the two ADR-0000 questions fire on sight.

**Scope, honestly.** This bites wherever a language's replace-unit is coarser than the
fact granularity it carries — SQL function bodies, configuration blocks, template
files — and it bites hardest where no compiler holds the compositional line (PL/pgSQL
has no mypy). There the discipline is carried by gates: the project's existing
precedent is `s35`'s decomposition of an earlier validator family into per-fact leaf
functions policed by a byte-identity gate, plus the prior-body hash binding
(`gates/lineage_reissue_lineage.py`) that makes a stale base unrepresentable in any
re-issue. The specimen monolith's own decomposition is filed as project ledger row
1432's third remedy, not yet executed at this writing.

**The time-travel claim, stated falsifiably.** Had this append existed before
2026-07-26, it had three chances to fire: when `s53` added the belief branch to an
already-two-concern function, when `s58` added three more, and when `s61`'s re-issue
diff visibly restated four branches it did not intend to change. Any one of the three
would have prevented the incident; that is why this is recorded as a lesson and not
only as a repair.

### Amendment — 2026-07-27: P11 — absence carries a typed reason (NULL is never a meaning-carrier)

*(Provenance: the s67 merge-hold ruling, this project's ledger rows 1539/1541 — the
"ledger" being this project's append-only decision/audit log, a database read via the
`./autoharn led` verb rather than a file in this tree, and "s67" one of its numbered
kernel lineage deltas (`kernel/lineage/sNN-*.sql`),
held un-merged while the maintainer ruled on its shape. That kernel
delta proposed recording a refusal-journal digest — the refusal journal being the
kernel's audit record of refused ledger writes, rows of kind `write_refused` carrying
a hash (digest) of the refused payload — as NULL when the refused payload
exceeded a size bound — NULL's meaning, "not computed because over-bound," living in a
comment and a function body. The fresh-context review argued containment (no code
consumes the digest today, and sibling columns of the same journal already use NULL
the same way); the maintainer refused the shape itself, near-verbatim: "NULL as an
implicit sentinel/meaning-carrier is something I cannot condone here, no matter what the
no-consumer test is saying — it's a drift hazard," invoking the industry's own name for
the failure family, the billion-dollar mistake. The economy argument was the wrong test:
representation explicitness governs, because drift hazards are about future readers, not
present consumers. Numbered P11 by Consult — the maintainer's primary AI-collaborator
authoring model, per the orchestration section of this repository's
[AGENTS.md](../../AGENTS.md) — at the
maintainer's direction to "add this in the appropriate register"; the P10 precedent —
maintainer promotion to a standalone principle at ratification — is followed
deliberately.)*

**Rule (checkable).** When a value can be absent and the absence has a REASON any
reader, gate, or investigator could ever need, that reason is a **representable, typed
fact** — a disposition column with a closed vocabulary and a coupling constraint, a sum
type, a tagged option — never a bare NULL/None/sentinel whose meaning lives in
documentation. The check: *find every nullable column, Optional field, or sentinel
value whose comment or spec assigns a meaning to the absence; each is a violation
unless the meaning is also carried by a typed, constraint-coupled companion the
storage layer itself can enforce.* A NULL with a documented meaning is a bare value
carrying a contract-by-comment — the exact shape the standing no-bare-types rule bans
at value granularity (project ledger row 1105, the maintainer's 2026-07-22 commission:
every value construction goes through one single-source-of-truth home that checks a
contract appropriate to the value's use case — no bare ints, no bare strings), stated
here structurally.

**The coupling half, load-bearing.** The typed reason must be TIED to the absence by a
machine-checkable constraint (the s67 repair's shape:
`(disposition = 'computed') = (value IS NOT NULL)`), so an accidental future writer
that produces the absence without declaring the reason is caught structurally. A
declaration column without the coupling is decoration; the constraint is the rule.
What the constraint cannot buy — and this is stated, not hidden — is truthfulness of
the declaration itself: a writer that lies can still satisfy the coupling constraint
by producing a false disposition token alongside the matching absent-or-present value,
so the constraint catches accidents, never a consistent lie. That residue is
[ADR-0002](0002-fail-loudly.md)/[ADR-0011](0011-mechanization-discipline.md) territory
(loudness and mechanized cross-checks), not representation.

**What this does NOT ban.** Absence with genuinely NO meaning beyond "not applicable
to this row's kind" is not a sentinel (the kernel's kind-shape idiom: a wide ledger
table where each row-kind licenses its own column family, so a column is NULL on every
row of a kind that simply does not carry it) — no reader needs a reason there beyond
the kind itself, which IS the typed companion. The rule bites exactly when two or more causes of absence exist or can
arrive, or when one cause is a fact someone might act on.

**Cancer prevented: F (magic constants) generalized to magic ABSENCES, and the P8
lying-signature family at the value level.** Composes with
[ADR-0008](0008-classification-discipline.md)'s same-day (2026-07-27)
vocabulary-register amendment: 0008 governs the vocabulary (absence-reasons are named
tokens in a closed set); this principle governs the structure (the token is
constraint-coupled to the absence). Known pre-existing instances at adoption, named
per ADR-0008 Rule 3 rather than silently grandfathered: two sibling columns of the
same refusal journal — `refusal_attempted_kind` (delta s65, recording which act kind a
refused write attempted) and the attempted-actor family (deltas s43/s49, recording who
attempted it) — both of whose NULLs today mean "not extractable" by documentation
alone. Their disposition awaits a separate maintainer decision, tracked on the project
ledger (row 1541; the backlogged codebase audit is work item null-sentinel-audit,
row 1542).

### Amendment — 2026-11-04: P12 — Eager imports are the module's dependency contract (lazy imports are banned)

*(Provenance: the maintainer's original 2026-07-02 edict, supplied again
on 2026-11-04 and ratified in spirit by the maintainer on that date. The rule
originated after deferred imports interfered with benchmarking and
performance-sensitive code in an unrelated project. The maintainer identifies
the omission from ADR-0012 as an executive lapse. The ADR corpus currently has
no single canonical home; this is the supplied ADR-0012 copy amended for the
maintainer to transfer, and does not claim to resolve that separate corpus-level
SSOT lapse.)*

New anti-pattern row (appended to the checklist, per this amendment's
structural scope):

| Audit cancer / boundary | The shape to never author | Preventing rule |
| --- | --- | --- |
| **(new, module boundary)** — a deferred dependency whose cost and footprint are hidden from importers | an `import` / `from ... import` inside a function or method body, so the importing module's top-level dependency footprint omits a dependency whose first-use cost is paid by whichever caller reaches that branch | **P12** — every runtime dependency is imported at module/class definition time; if some importers should not pay it, split the module at the dependency boundary and compose explicitly |

**Rule (checkable).** Every runtime Python `import` or `from ... import`
statement executes at module import time. Import statements inside a function
or method body — including nested functions and methods — are **forbidden,
without an allowlist**. A deferred import transfers work to whichever caller
first reaches that branch, hides the module's actual dependency footprint, and
makes that footprint a module-scale lying contract.

If some importers of a module should not pay for one of its dependencies, the
module is mis-factored. **Split the module at the dependency boundary** so each
module has one honest, complete top-of-file dependency footprint; have an
explicit composition root import and inject the collaborator where concerns
must be combined. Do not defer an import to preserve a monolithic module or to
make an optional backend appear absent from the importing module's contract.
An optional backend belongs in its own module, which consumers import
explicitly when they choose that backend.

**The narrow exemptions are execution semantics, not allowlists.** An import
inside an `if TYPE_CHECKING:` block is exempt because that block never executes
at runtime. Module-level imports nested under `if`, `try`, or another module
scope construct are permitted when they execute during module import. Imports
in a class body are also permitted because the class body executes when the
class is defined. No function-body or method-body import is permitted merely
because it is guarded, optional, rarely reached, or convenient.

**Mechanization.** `gates/no_lazy_imports.py` parses live Python source and
reports every `Import` / `ImportFrom` statement nested in a function or method
body. In a Git checkout it reads staged blobs by default; `--tree` selects the
working tree. It excludes only historical snapshots and generated, vendored,
or environment trees that are not live project source. The required result is
**zero violations**. There is no per-module baseline, suppressions list, or
allowlist. A parse/read failure is itself a gate failure because the gate cannot
certify the file.

**Cancer prevented.** Hidden dependency edges, delayed first-use cost, and
module import footprints that lie about what the module depends on. This
composes with P2 (explicit ports and dependency inversion), P3 (one-owner
collaborators), and P8 (the signature is the contract's single source of truth):
a module's imports are its dependency contract, and the module boundary must
honor it honestly.

**Self-application / adoption status.** The first scan after introducing the
gate reported existing violations in the live tree. They are not grandfathered
or allowlisted; the gate remains red until the owning modules are decomposed
and the count reaches zero. The current inventory is recorded in `BACKLOG.md`.
Ratification of this rule does not claim that adding the gate alone discharges
it; the gate is the net, and zero violations is the required result.

## License

Public Domain (The Unlicense).
