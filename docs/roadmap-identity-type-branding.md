# Identity-type branding — design roadmap

- **Status:** planned (per the umbrella ADR-0005 Rule 8 doc-graph
  genre vocabulary; this memo is `design-note: planned`)
- **Date:** 2026-05-12
- **Scope:** `proxy/AbstractProxy/proxy_core.py` and every
  consumer that flows identity strings through it. Specifically
  the four ID namespaces named in `ARCHITECTURE.md`'s
  "ID namespaces and translation" section, plus the
  orchestration framework's sub-query orig-id family.
- **Origin:** `ARCHITECTURE.md`'s "Where this falls short"
  section flags the **`str` constraint on identity types** as
  one of three known shortcomings ("a second protocol
  implementation would surface these leaks quickly"). The
  2026-05-12 umbrella session prioritised closing it ahead of
  the substrate work in
  `LengYue:docs/notes/proxy-topology-testing-plan.md`.

---

## TL;DR

The framework is *already partially typed for namespace
distinction* — `IdMapping`, `ProxyLink`, `Envelope`, and
`Translation` are all generic over an `I` type variable bound
to `Hashable`. The collapse to plain `str` happens at exactly
one instantiation site in the KataGo concrete layer
(`katago/katago_proxy.py:make_katago_link`). Three things stop
this from being a single-line fix:

1. `IdMapping[I]`, `Translation[I]`, and `ProxyLink[I]` use one
   type variable for *both* upstream and downstream IDs. To
   distinguish (the actual goal — a `ClientId` shouldn't be
   confusable with the `InternalId` it maps to), the framework
   needs to split `I` into `I_up` and `I_down` per link.
2. Python's `NewType` produces *nominal* types only at type-
   checker time; at runtime they're identical to the base type.
   This is enough for `mypy --strict` to catch confusions but
   does not provide runtime isolation. The discipline lands as
   a typecheck contract, not a runtime guarantee.
3. The orchestration framework's `__orch__<hex>` sub-query
   orig-ids share the orig-id namespace with parent-client
   orig-ids; they need to remain assignable to the same NewType
   or distinguishable, and the choice between those shapes is
   the memo's main design decision.

The memo proposes a phased migration: framework split (Phase 1),
KataGo-instantiation branding (Phase 2), consumer-call-site
propagation (Phase 3), with a no-regression typecheck contract
at each phase boundary.

---

## 1. The current state

### 1.1 What the framework already has

`AbstractProxy/proxy_core.py` declares the identity-typed
classes with a `Hashable`-bound type variable:

```python
I = TypeVar("I", bound=Hashable)  # identity representation

class IdMapping(Generic[I]):
    def __init__(self, generator: IdGenerator[I]) -> None:
        self._fwd: dict[I, I] = {}   # upstream → downstream
        self._rev: dict[I, I] = {}   # downstream → upstream
    def register(self, upstream_id: I) -> I: ...
    def forward(self, upstream_id: I) -> Optional[I]: ...
    def reverse(self, downstream_id: I) -> Optional[I]: ...

class Translation(Generic[I]):
    upstream: I
    downstream: I

class Envelope(Generic[I, P]):
    id: I
    payload: P

class ProxyLink(Generic[I]):
    mapping: IdMapping[I]
    def translate_downstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]: ...
    def translate_upstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]: ...
```

### 1.2 Where the collapse happens

One site instantiates `I = str`:

```python
# katago/katago_proxy.py
def make_katago_link(tracker: CompletionTracker) -> ProxyLink[str]:
    mapping: IdMapping[str] = IdMapping(generator=katago_id_generator)
    ...
```

Every consumer downstream of this — `proxy_server.ClientSession`,
the transformer chain, the middleware chain, the routers — sees
`ProxyLink[str]` and treats every ID as a bare `str`.

### 1.3 What the type variable doesn't catch

`IdMapping[I]._fwd: dict[I, I]` uses the same `I` for keys and
values. The type system can't say "this dict's keys are
upstream IDs and its values are downstream IDs"; both are `I`.
`mapping.register(some_id)` returns the same type it accepted.
A caller that's confused about which namespace `some_id` lives
in can't be caught by mypy.

Similarly, `Translation[I].upstream` and `.downstream` are both
`I`. A test asserting `t.upstream == some_other_id` doesn't
know whether `some_other_id` is upstream-shaped or
downstream-shaped — they're both the same type.

**This is the framework limitation the memo's design must
address.** Pure NewType-at-instantiation (Phase 2 alone) gives
us `ClientId` everywhere a `str` was, but doesn't distinguish
`ClientId` from `InternalId` *at the boundary that maps between
them*.

---

## 2. The four namespaces

From `ARCHITECTURE.md`'s "ID namespaces and translation":

```
client_id  --[ProxyLink in ClientSession]-->  internal_id
internal_id  --[PubSubHub coalescing]-->      canonical_id
canonical_id  --[BackendRouter dispatch]-->   wire_id (sent to engine)

wire_id (from engine)  --[BackendRouter]-->   canonical_id
canonical_id  --[Hub fans out, relabels]-->   internal_id (one per subscriber)
internal_id  --[ProxyLink reverse]-->         client_id
```

The framework distinguishes four namespaces:

| Namespace | What it identifies | Lives in | Generator |
|---|---|---|---|
| `ClientId` | A query as the client named it on the wire | The `id` field on incoming wire messages | The wire (not generated) |
| `InternalId` | A query inside one client session | `ClientSession`'s ProxyLink mapping | `IdGenerator` for client_id → internal_id |
| `CanonicalId` | A coalesced semantic-equivalence class | `PubSubHub`'s in-flight entries | Content-hash + dedup |
| `WireId` | A query sent to the actual engine | What `BackendRouter` writes to upstream | Per-router (LEAF mints fresh, RELAY uses upstream's namespace) |

There's a fifth de-facto namespace the framework's TypeVar
doesn't currently account for:

| Namespace | What it identifies | Lives in | Notes |
|---|---|---|---|
| `OrigId` | A query at the transformer/middleware layer (after Layer 1 translation, before Hub) | `Transformer.on_query(orig_id, q)`, `SessionMiddleware.on_query(orig_id, q)`, `OrchestrationMiddleware._sub_to_parent` | Equal to `InternalId` in code today; logically the client-namespace post-Layer-1-receive |

Actually `orig_id` IS `client_id` (or close to it) at the
transformer layer, post-receive but pre-downstream-translation.
The framework's current shape conflates them at the type
level; the audit during implementation (Phase 1) will resolve
whether they need separate NewTypes or whether one suffices.

There's also a synthesised-id family the orchestration
framework produces:

| Sub-family | Pattern | Lives in | Notes |
|---|---|---|---|
| Sub-query orig-id | `__orch__<hex>` | `OrchestrationContext.spawn`, `OrchestrationMiddleware._sub_to_parent` | Distinguished by lexical prefix at runtime; could be a sub-type of `OrigId` for documentation, but the framework doesn't depend on the distinction |

---

## 3. Python's `NewType` semantics

`typing.NewType('Foo', str)` produces a callable that mypy
treats as a distinct type at type-check time. At runtime, the
callable is identity — `Foo("abc")` returns the string `"abc"`,
no wrapping. The runtime overhead is one function call (at
construction sites only); the runtime memory is bare `str`.

**Strengths:**

- mypy + IDE catch namespace confusions at edit time.
- No runtime cost on the hot path (after construction).
- Zero JSON-serialisation impact — NewTypes serialise as their
  base type.
- Grep-friendly. `grep -rn 'ClientId' proxy/` returns the
  actual ID-flowing surface; `grep -rn ': str' proxy/` returns
  everything.

**Limitations:**

- No runtime guarantee. An untyped cast or `# type: ignore`
  can launder one NewType into another. The discipline is as
  weak as the codebase's type-coverage.
- NewType cannot be inherited from (mypy refuses
  `class FancyClientId(ClientId)`). Sub-namespacing (e.g.,
  `__orch__`-prefixed orig-ids as a sub-type of `OrigId`)
  requires a different mechanism — likely a `Literal`-typed
  prefix discriminator or a `Protocol` instead of a strict
  sub-type. Worth noting; not a blocker.
- The construction call sites need to be explicit. Reading a
  `str` from JSON and wanting it as a `ClientId` requires
  `ClientId(json["id"])` somewhere. Mypy won't add these
  automatically.

**Comparison with alternatives:**

- `dataclass`-wrapped IDs (`@dataclass(frozen=True) class ClientId: value: str`):
  give runtime distinction (`isinstance` works) but add
  per-construction cost, complicate JSON serialisation, and
  fight the `dict[I, I]` shape the framework already uses.
  Rejected for proxy-perf reasons.
- `enum.StrEnum` per-namespace: nonsensical (the IDs are
  generated, not enumerated).
- Stub-file-only types: works for read-only declarations but
  doesn't propagate through actual call sites. Rejected for
  ergonomics.

NewType is the right shape. The strength/weakness profile fits
the proxy's "discipline-enforced-by-type-checker" posture per
`proxy/CLAUDE.md`'s "Type-driven design (within Python's
limits)" section.

---

## 4. The split: from `I` to `I_up` + `I_down`

### 4.1 The shape

The framework's identity-typed classes need to become
two-parameter:

```python
# Before
class IdMapping(Generic[I]):
    _fwd: dict[I, I]
    def register(self, upstream_id: I) -> I: ...
    def forward(self, upstream_id: I) -> Optional[I]: ...
    def reverse(self, downstream_id: I) -> Optional[I]: ...

# After
U = TypeVar("U", bound=Hashable)  # upstream identity
D = TypeVar("D", bound=Hashable)  # downstream identity

class IdMapping(Generic[U, D]):
    _fwd: dict[U, D]
    def register(self, upstream_id: U) -> D: ...
    def forward(self, upstream_id: U) -> Optional[D]: ...
    def reverse(self, downstream_id: D) -> Optional[U]: ...

class Translation(Generic[U, D]):
    upstream: U
    downstream: D

class Envelope(Generic[I, P]):  # unchanged — envelope has ONE id
    id: I
    payload: P

class ProxyLink(Generic[U, D]):
    mapping: IdMapping[U, D]
    def translate_downstream(self, envelope: Envelope[U, Any]) -> Envelope[D, Any]: ...
    def translate_upstream(self, envelope: Envelope[D, Any]) -> Envelope[U, Any]: ...
```

Note the asymmetry: `Envelope` stays generic over a single
identity (an envelope has *one* id; it doesn't simultaneously
hold an upstream and downstream form). The `Envelope[U, Any]`
vs `Envelope[D, Any]` distinction in `ProxyLink`'s signature is
the typechecker's record of "this envelope is upstream-shaped"
vs "this envelope is downstream-shaped" — caught at the call
site, not stored in the envelope itself.

### 4.2 Per-link instantiation

Each link in the chain has its own `(U, D)`:

```python
# In katago/katago_proxy.py
ClientId = NewType("ClientId", str)
InternalId = NewType("InternalId", str)
CanonicalId = NewType("CanonicalId", str)
WireId = NewType("WireId", str)

# The client-facing link maps client → internal
def make_client_link(tracker) -> ProxyLink[ClientId, InternalId]:
    mapping: IdMapping[ClientId, InternalId] = IdMapping(
        generator=client_to_internal_generator,
    )
    ...

# The hub layer maps internal → canonical (via PubSubHub's own
# IdMapping[InternalId, CanonicalId])
# The router layer maps canonical → wire (via per-router IdMapping[CanonicalId, WireId])
```

The chain composition guarantees namespace continuity at each
boundary. A misuse like passing a `ClientId` to a function
expecting `InternalId` becomes a typecheck error at the call
site, with mypy pointing at the precise mismatch.

### 4.3 What this gets us

Three concrete bug classes the typechecker would catch
post-migration:

1. **Cross-link confusion.** A function that operates on
   `ClientId` is called with `InternalId` (or vice versa)
   somewhere in the codebase. Caught at the boundary.
2. **`_sub_to_parent` namespace mistakes.** The
   `OrchestrationMiddleware._sub_to_parent` dict maps
   `sub_orig_id → parent_orig_id`. If we type it as
   `dict[OrigId, OrigId]`, mypy enforces that both keys and
   values are orig-namespace. A future refactor that
   accidentally stores a `CanonicalId` would be caught.
3. **Translation invariants in tests.** Test assertions like
   `assert link.mapping.forward(some_id) == some_other_id` are
   currently structurally valid for any string pair. Post-
   migration, the type system enforces that the lookup-key type
   matches the mapping's upstream type and the result type
   matches the mapping's downstream type.

---

## 5. The migration arc

Three phases, each shippable independently and each preserving
suite green-state.

### Phase 1 — Framework split

**Scope:** `AbstractProxy/proxy_core.py` only. No KataGo-side
or consumer changes.

**Deliverable:** `IdMapping`, `Translation`, `ProxyLink`, and
`Envelope` (where applicable) gain a second type parameter per
§4.1. Existing single-type instantiations become trivially
re-expressible: a `ProxyLink[str]` becomes
`ProxyLink[str, str]` until Phase 2 brands the parameters.

**Backward compatibility:** Single-type aliases for the
transition window:

```python
# Provided in proxy_core.py during the migration window.
# Removed at the end of Phase 3.
IdMappingHomogeneous = IdMapping  # Generic[T] = Generic[T, T]
```

Actually Python's typing doesn't let us alias generics with
parameter-count reduction cleanly. The pragmatic approach:
existing call sites that instantiate `IdMapping[str]` get
updated to `IdMapping[str, str]` in Phase 1; the Phase 2
branding swaps `str` for `ClientId`/`InternalId`/etc.

**Test discipline:** The existing `test_protocol_parser.py` and
adjacent suite must remain green. The migration is type-only;
no runtime behaviour changes in Phase 1.

### Phase 2 — KataGo-side branding

**Scope:** `katago/katago_proxy.py`, specifically
`make_katago_link`. Plus `proxy_server.py` (ClientSession owns
the link), `pubsub_hub.py` (the Hub owns the InternalId →
CanonicalId mapping), and `router.py` (per-router CanonicalId
→ WireId mappings).

**Deliverable:** Define the four NewTypes in a canonical
location (proposed: `AbstractProxy/proxy_core.py` for the
framework-aware ones — actually no, NewTypes are
protocol-specific; they belong in `katago/katago_proxy.py` or
a sibling. The framework stays protocol-agnostic at the type
level, which is the existing discipline). Wire the
instantiations to use the brands.

**Touch surface inventory** (preliminary; Phase 1 audit
confirms):

- `katago/katago_proxy.py`: `make_katago_link` becomes
  `ProxyLink[ClientId, InternalId]`; the `KataGoQuery.opaque`
  fields that hold IDs (`terminate_id` on terminate queries)
  get typed.
- `proxy_server.py`: `ClientSession._active_queries` and
  related dicts; the `eid: str` parameter on
  `_handle_query` and friends becomes `eid: OrigId`.
- `pubsub_hub.py`: `PubSubHub`'s `_canonicals` and subscriber
  maps; the cache_key type (`str` today) stays `str` —
  cache_key is a CONTENT hash, not an identity, and shouldn't
  be branded as one.
- `router.py`: per-router mappings, particularly RELAY's
  upstream-pool hashmap and SELECTOR's labelled pool.
- `transformers/*.py`: `analysis_enricher`, `capability_gate`,
  `transposition_enricher`, `capabilities_advertiser` — all
  receive `eid: OrigId` rather than `eid: str` once the
  Transformer type is generalised.
- `middleware/*.py`: `orchestration`, `capability_gate`,
  `keep_alive`, `adaptive_reevaluate` — the same eid-typing
  cascade.

**Audit-during-implementation question:** does `orig_id` at the
transformer layer warrant its own NewType, or is it identical
to `ClientId` in the typed world? I believe identical —
Phase 1's `ProxyLink[ClientId, InternalId]` shape implies the
transformer-side (which runs BEFORE `translate_downstream` for
queries and AFTER `translate_upstream` for responses) operates
in the ClientId namespace. If so, `orig_id: ClientId`. The
implementation audit confirms.

### Phase 3 — Consumer call-site propagation

**Scope:** Every test file and the umbrella's SPA-side
references. The proxy submodule changes ship in Phase 2; the
umbrella PR bumps the pointer and updates SPA-side type
imports if the new NewTypes get re-exported (likely
unnecessary — the SPA already has its own branded types via
`frontend/src/types.ts`'s `BoardId`, `NodeId`, etc.; the
proxy's NewTypes don't cross the wire boundary because they
serialise as plain strings).

**Deliverable:** Tests in `proxy/tests/` get type-annotated
correctly. `mypy --strict` (or the equivalent project-wide
type-check command — proxy uses pyright or similar; the
implementation arc confirms) passes on the proxy's full
surface. CI integration of the typecheck as a gate, if not
already present.

---

## 6. Test discipline for the migration

The migration is type-level; runtime behaviour is unchanged.
The right test discipline is:

1. **Existing suite must remain green at every commit.** Each
   phase's diff is mechanical (rename + retype); test
   modifications track 1:1.
2. **New type-checking gate.** Phase 3 adds (or enables) a
   `mypy --strict` (or pyright) CI step. The gate fails on
   any namespace mismatch introduced by the migration or by
   future code.
3. **Regression tests for namespace contract.** A small set of
   typed-assertion tests in `tests/test_identity_types.py`
   demonstrating that mypy catches mismatches that would
   otherwise have shipped. Example shape:

   ```python
   # This file uses mypy's reveal_type / assert-type machinery
   # to pin the contract. It does NOT run in pytest; it runs
   # in the typecheck step.
   from typing import assert_type
   from katago import ClientId, InternalId
   from AbstractProxy import IdMapping

   def test_client_internal_brands_distinct() -> None:
       mapping: IdMapping[ClientId, InternalId] = ...
       cid = ClientId("abc")
       iid = mapping.register(cid)
       assert_type(iid, InternalId)
       # The following would be a type error and is verified
       # via reveal_type or a sibling mypy-error-expecting test:
       # mapping.register(iid)  # error: expected ClientId, got InternalId
   ```

4. **No `# type: ignore` introduced.** Every type-ignore comment
   in the migration is a hidden bug; the typechecker is the
   point.

---

## 7. Open questions

### 7.1 Where do the NewTypes live?

Two candidates:

- `AbstractProxy/proxy_core.py` — framework-level. Argument
  for: the four namespace names are framework concepts; they
  exist independently of the KataGo protocol. Argument
  against: NewTypes are protocol-specific concretions; the
  framework is protocol-agnostic.
- `katago/katago_proxy.py` — protocol-level. Argument for:
  matches the existing pattern (the KataGo concrete layer
  instantiates the framework's TypeVars). Argument against:
  the four namespace names are universal across whatever
  protocol; defining them here makes a second-protocol port
  redefine them.

**Lean:** framework-level for the namespace *names* (e.g.,
`ClientId` is a universal concept), but the concrete `NewType`
instantiations could live in protocol-specific modules with
the framework declaring `ClientId` as a generic placeholder via
`TypeAlias` or `Protocol`. This nuance worth confirming during
the Phase 1 design review.

### 7.2 Does `OrigId` deserve its own NewType?

`orig_id` at the transformer layer is logically the same as
`client_id` at the wire-receive point. The framework's
ProxyLink boundary translates `ClientId → InternalId` on the
downstream side. Transformers run BEFORE this translation for
queries (per `TransformedChain.translate_downstream` in
`AbstractProxy/protocol_transformer.py`), so transformer-layer
`eid` is `ClientId`-shaped.

But: the `OrchestrationMiddleware._sub_to_parent` dict maps
sub-query orig-ids (synthetic `__orch__<hex>`) to parent
orig-ids (client-supplied). Both are in the orig namespace,
but the sub-queries are framework-minted, not client-supplied.

**Option A:** `orig_id: ClientId`. Sub-query synthetic ids are
just `ClientId` values that happen to have a prefix. Type
system doesn't enforce the prefix; runtime convention does.

**Option B:** `OrigId = NewType('OrigId', str)` with `ClientId`
as a sub-namespace and `SubOrigId` as another (both unifiable
to `OrigId`). Type system distinguishes; harder to model in
Python (NewType doesn't compose nicely).

**Lean:** Option A. The prefix discipline lives in runtime
checks (the `__orch__` prefix is already a convention; no need
to hoist it into the type system). Phase 2 confirms.

### 7.3 What about `wire_id` per upstream connection?

Multi-upstream routers (RELAY, SELECTOR) maintain separate
WireId namespaces per upstream LEAF. The framework's current
shape has each router holding its own IdMapping(s). Should
WireId be parameterised by upstream identity (e.g.,
`WireId[upstream_label]`)? The router's existing API doesn't
expose the per-upstream distinction; the discipline lives in
the router's internal partitioning.

**Lean:** Don't parameterise WireId by upstream. The
per-upstream isolation is a router-internal concern; the
NewType represents "this is a downstream-of-router id" and
that's enough for the consumer-facing typecheck.

### 7.4 Phase 2's submodule release arc

Phase 2 ships as a proxy-side PR + tag + umbrella pointer
bump, per the standard arc documented in
`proxy/CLAUDE.md`'s "Submodule release arc" section and the
umbrella's `CLAUDE.md` "On the proxy submodule" section. No
deviations from the standard pattern anticipated.

---

## 8. Sunsetting

This memo is `design-note: planned`. The migration arc in §5
transitions it to `design-note: implemented` when Phase 3 lands
(per ADR-0005 Rule 8 in the umbrella). The Phase 1 design
review should be a separate proxy-side PR with the framework
diff alone; this memo's open questions in §7 get answered
through that review, and the answers update §7 in-place before
the design is considered settled (in the umbrella's vocabulary,
`design-note: revised` if the answers materially change the
proposal; in-place update if they just specify what was open).

If a §7 question's answer warrants a tenet-level discipline
(e.g., "no `# type: ignore` in the proxy" graduating from §6's
test-discipline note to a CLAUDE.md rule), the appropriate
landing is `proxy/CLAUDE.md`'s "Type-driven design" section,
co-located with the existing "a cast() or # type: ignore needs
a justification in a comment or it doesn't ship" rule.

---

## 9. Related documents

- `proxy/ARCHITECTURE.md` §"ID namespaces and translation" —
  the canonical description of the four namespaces this memo
  brands. §"Where this falls short" names the `str` constraint
  as the shortcoming this memo addresses.
- `proxy/CLAUDE.md` §"Type-driven design (within Python's
  limits)" — the existing discipline this migration extends.
- `proxy/FRAMEWORK.md` — the layer model the four namespaces
  pass through.
- `LengYue:docs/notes/postmortem-adaptive-deeper-enrichment-2026-05.md`
  — the investigation that originally surfaced the umbrella's
  prioritisation of this work, alongside the topology-testing
  arc.
- `LengYue:docs/notes/proxy-topology-testing-plan.md` — the
  adjacent design that the umbrella user prioritised behind this
  one. The two arcs are independent at the implementation level
  but compose on the discipline side (typed identities make
  topology-tests' assertions stronger).
- `LengYue:docs/wire-schemas.md` §7 — the
  `_PROXY_ONLY_FIELDS` invariant, which this memo's NewType
  branding would let us express at the type level (a
  proxy-only field's value type could be branded with
  `ProxyOnly` if it's worth the ceremony; lower priority than
  the identity namespaces).
