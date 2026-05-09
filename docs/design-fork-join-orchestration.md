# Design Note — Fork-Join Orchestration as a Middleware Primitive

A design exploration for a recurring pattern that the proxy's
`SessionMiddleware` abstraction can express but does not directly
support: middleware that derives N sub-queries from one parent
query, awaits some or all of them, joins their responses, and emits
a derived result back under the parent's `orig_id`. Written
2026-05-10 against proxy v1.0.15. Not a roadmap: not committed to a
release, no branch open. Filed as architectural prior thinking that
a future implementation arc can draw from.

The motivating concrete cases the design serves:

- `adaptive_reevaluate` (already exists): a degenerate fork-join with
  fan-out-of-1 (one deeper-analysis follow-up query per parent).
- A hypothetical `jsd_compare` middleware (the trigger for this
  note): given two model labels, fan out one ANALYZE per model,
  await both, compute Jensen-Shannon divergence on the policy
  distributions, emit annotated responses.
- N-way generalisations of either: the multi-weights or
  LLM-at-seat experiments the umbrella's
  `docs/notes/autonomous-srs-loop.md` sketches.

The user's framing — "Man muss immer generalisieren" — accepts the
generalisation as intrinsically valuable, not contingent on having
N concrete consumers in hand. This note proceeds on that premise.

Cross-references: `proxy/ARCHITECTURE.md` (the layer model and
extension surfaces this design extends); `proxy/FRAMEWORK.md` §1
(the Transformer/Middleware vocabulary); `proxy/docs/roadmap-
capability-negotiation.md` and `proxy/docs/roadmap-selector-
router.md` (Phases 1 and 2+3 of the capability/SELECTOR work that
established the surrounding context); `proxy/middleware/
adaptive_reevaluate.py` (the closest existing example of the
pattern this design generalises); `proxy/reactive_pipeline/`
(experimental related work — see *Related work* below).

---

## What exists today

### The clean abstractions

The proxy's bottom and top layers are well-shaped:

- **`Transformer` (sync, stateless, per-message).** Algebraically a
  literal endofunctor: `Transformer[Q, R]` with `then` as
  composition. The `Optional`-as-suppression gives it a Maybe-like
  effect. `TransformedChain` keeps identity translation orthogonal
  to content transformation. This is the most categorical part of
  the codebase and it stays out of this design.
- **`ProxyLink` and `ReferentialField`.** ID-namespace translation
  declared once on a policy, applied uniformly by
  `translate_referentials`. Composable via `ProxyChain`. Honest.
  Unaffected by this design.
- **`PubSubHub` and `CoalescingPolicy`.** Two-hashes (`content_hash`
  for coalescing, `cache_key` for replay) with the strip-before-
  hash discipline. Phase 1 added `capabilities` to `capturing_fields`
  and the central wire-strip; Phase 2+3 added `model` similarly.
  Unaffected by this design.

### The pragmatic abstraction (where the gap lives)

`SessionMiddleware` is the abstraction that this design touches:

- It is an async generator over the response stream:
  `handle_response(orig_id, response, submit_query) -> ResponseStream`.
- It composes via `MiddlewareChain` (sequential: outer wraps inner).
- It receives a `SessionCapabilities` bundle exposing `submit_query`
  (inject a query into the proxy pipeline) and `terminate_query`
  (cancel an in-flight query by orig_id).
- Per-query state is the implementer's responsibility:
  `_per_orig_id_state: dict[str, T]` with manual cleanup on
  completion or session-end.

Every stateful middleware that uses `submit_query` today reimplements
the same pattern:

1. **Synthesise a parent-pointer in a synthetic orig_id.**
   `adaptive_reevaluate` uses `Q:<8hex>__<real_orig_id>`;
   `keep_alive` uses `__keepalive_term_<hex>`. Each invents its own
   string-marshalling convention. The framework does not track
   parent-child relationships.
2. **Buffer responses per parent in instance dicts.** Counts of
   expected finals, lists of buffered responses, snapshots of the
   parent query. Cleanup on completion; LRU eviction on overflow.
3. **Detect "all sub-queries complete" via hand-rolled counters.**
   `adaptive_reevaluate` knows the original is complete when
   `len(bucket) >= self._expected.get(orig_id, 1)`; it knows a
   deeper query is complete by intercepting its synthetic-id
   responses and using `_real_id_of` to recover the parent.
4. **Emit derived responses under the parent orig_id.** With manual
   cleanup of all the bookkeeping.

This works. `adaptive_reevaluate` proves it. It is also entirely
imperative: every step is spelled out; the framework offers no
combinators that capture "parent-child relationship", "join when N
complete", "automatic state cleanup on completion". A second
fork-join middleware (jsd_compare) would re-implement the same
machinery from scratch with its own variable names. A third would
make the missing primitive obvious.

### What `submit_query` is and is not

`submit_query` is the escape hatch that gives middleware access to
the proxy pipeline as a callable. It is power: middleware can
spawn arbitrary sub-queries, including recursive cascades. It is
also unstructured: the spawned query's lineage is invisible to the
framework, and any ordering or joining semantics live entirely in
the calling middleware's state machine.

The hypothesis this design rests on: most uses of `submit_query`
are instances of fan-out + (optional join) + emit-derived. Capturing
that pattern as a typed primitive lets the framework manage the
parent-child tracking, the completion detection, and the cleanup —
freeing the middleware author to write only the fork specification
and the join algebra.

---

## The design space

Five candidate shapes for the missing primitive, walked through in
increasing order of expressive power (and engineering cost). The
recommendation lands on the third.

### Option A — `FanOutMiddleware` factory (closure-based)

The conservative starting point. A factory function that takes a
`fork_fn` and a `join_fn`; the framework handles state machinery.

```python
def fan_out(
    *,
    fork: Callable[[KataGoQuery], list[KataGoQuery]],
    join: Callable[[list[list[KataGoResponse]]], KataGoResponse],
    name: str,
) -> SessionMiddleware:
    """Spawn N sub-queries from one parent; await all; emit join(results)."""
    ...
```

JSD becomes:

```python
jsd_middleware = fan_out(
    fork=lambda q: [q.with_model(m) for m in COMPARE_MODELS],
    join=lambda response_lists: compute_jsd_annotation(response_lists),
    name="jsd_compare",
)
```

`adaptive_reevaluate` refactors to a degenerate fan-out-of-1:

```python
adaptive = fan_out(
    fork=lambda q: [build_deeper_query(q)] if should_deepen(q) else [],
    join=lambda response_lists: response_lists[0][-1],
    name="adaptive_reevaluate",
)
```

**Strengths.** Smallest framework change. Captures the most common
shape. Recognisable from existing code. No paradigm shift.

**Weaknesses.** Limited to fan-out → join → single-emit. Can't
express partial joins (emit some responses immediately, hold others
until all branches complete — which is what `adaptive_reevaluate`
actually does today with its `isDuringSearch` patching). Can't
express conditional flows ("if response from branch A satisfies X,
short-circuit and ignore branch B"). Can't express multi-step
derivations (fork → process → fork-again). The factory's signature
is a forced-fit for anything more complex than the simplest case.

**Verdict.** Useful but not the right ceiling. Choosing this would
satisfy JSD specifically, but the third use case (whatever it is)
would push past it.

### Option B — Combinator DSL

Express orchestration as algebraic combinators on a `Flow` type.

```python
flow = (
    parallel(
        spawn(lambda q: q.with_model("strong")),
        spawn(lambda q: q.with_model("weak")),
    )
    .then(zip_responses)
    .then(annotate(jsd))
    .then(emit_under_parent)
)

middleware = flow_to_middleware(flow, name="jsd_compare")
```

**Strengths.** Algebraically clean. Composes by design. Captures
parallel, sequential, join, partition as first-class operations.
The flow object is inspectable (you can pretty-print the
orchestration plan).

**Weaknesses.** Heavy learning curve. Debugging a combinator
expression is harder than debugging an imperative state machine
when something goes wrong (no stack frame; the combinator graph
is the program). Requires substantial framework infrastructure
(an interpreter for the Flow type, error propagation through
combinators, lifecycle integration). The Python ergonomics fight
the abstraction: no operator overloading for natural composition,
verbose lambda syntax, no type inference for the response types
flowing through combinators.

**Verdict.** Beautiful in a Haskell-shaped language; clunky in
Python. The codebase already gestures at this in
`reactive_pipeline/` for value-flow computation; pulling it up to
query orchestration would compound the experimental-vs-mainline
distinction. Pass.

### Option C — Generator-style orchestration coroutines

The middleware author writes orchestration as an async coroutine
that uses framework-provided primitives to spawn sub-queries and
await their completion. Reads like sequential code; the framework
schedules the parent-child lifecycle under the hood.

```python
@orchestration_middleware(name="jsd_compare")
async def jsd_compare(
    parent: KataGoQuery,
    ctx: OrchestrationContext,
) -> AsyncIterator[KataGoResponse]:
    """Fan out one ANALYZE per model; emit JSD-annotated joined results."""
    a, b = await ctx.parallel(
        parent.with_model("strong"),
        parent.with_model("weak"),
    )
    # `a` and `b` are lists of responses (one per turn); compute JSD
    # turn-wise and yield a new response per turn under the parent's
    # orig_id.
    for turn_a, turn_b in zip(a, b):
        jsd = compute_jsd(turn_a.opaque, turn_b.opaque)
        yield ctx.derive(turn_a, extra={"jsd": jsd})
```

`adaptive_reevaluate` refactors to:

```python
@orchestration_middleware(name="adaptive_reevaluate")
async def adaptive(
    parent: KataGoQuery,
    ctx: OrchestrationContext,
) -> AsyncIterator[KataGoResponse]:
    # Stage 1: yield original responses as they arrive, buffering
    # them for the worst-quantile decision.
    finals = []
    async for resp in ctx.original_stream():
        if resp.is_during_search:
            yield resp
            continue
        finals.append(resp)
        # Patch is_during_search=True for turns we will deepen,
        # so the client knows the turn isn't done yet.
        ...
    # Stage 2: decide on adaptation; spawn deeper if warranted.
    worst = find_worst_turns(finals, quantile=quantile_for(parent))
    if worst:
        deeper = build_deeper_query(parent, worst)
        async for resp in ctx.spawn(deeper):
            yield resp  # already re-labelled to parent's orig_id
```

The framework provides on `ctx`:
- `ctx.spawn(query) -> AsyncIterator[Response]` — submit a single
  sub-query; iterate its responses (re-labelled to the parent's
  orig_id automatically).
- `ctx.parallel(*queries) -> Awaitable[list[list[Response]]]` —
  spawn N sub-queries in parallel; await all; return per-query
  response lists.
- `ctx.original_stream() -> AsyncIterator[Response]` — iterate the
  parent query's own responses.
- `ctx.derive(template_response, extra=...) -> Response` — synthesise
  a derived response (helper, not orchestration primitive).

**Strengths.** Reads like sequential code. Captures fan-out, fan-in,
multi-stage derivations, conditional flows uniformly. The Python
async/await machinery does the heavy lifting; the framework only
needs to provide the context object and the parent-child tracking.
Each middleware's logic is in one place, top-to-bottom. Stack
frames work for debugging. Type checkers can mostly follow it.

**Weaknesses.** Requires a real framework lift: per-orig_id
orchestration context object, framework-tracked parent-pointers
(replacing the synthetic-id encoding hack), an async dispatcher
that drives the orchestration coroutine and resumes it when
sub-queries complete. The per-stream coroutine must integrate with
the existing `MiddlewareChain` composition model. Cancellation
(client disconnects mid-orchestration) needs careful handling —
the orchestration coroutine must be cancellable cleanly, with all
in-flight sub-queries terminated.

**Verdict.** This is the one. It generalises `Option A` (fan-out is
just `await ctx.parallel(...)`) and captures every use case
`adaptive_reevaluate` exercises today, plus everything jsd_compare
needs, plus the multi-step variants the autonomous-srs-loop note
sketches. It's a substantial design lift but the resulting
primitive is durable and matches Python's idioms.

### Option D — Reactive streams (rxpy or similar)

Treat the response stream as a first-class observable. Compose via
operators: `merge`, `combine_latest`, `zip`, `partition`, `window`.

**Strengths.** Mature ecosystem in some languages. Powerful
combinators for stream-shaped problems.

**Weaknesses.** Brings in a paradigm shift the rest of the proxy
doesn't share. Existing middleware would either need to be
rewritten in stream-operator style or coexist awkwardly with the
new style. The Python `rxpy` library is reasonable but not
idiomatic; the cognitive load to follow a stream pipeline is high
relative to imperative code with framework primitives.

**Verdict.** Wrong paradigm fit. The proxy's middleware is not
stream-shaped at the semantic level — it's query-shaped, with
streams as the implementation detail. Pass.

### Option E — Algebraic effects

Define orchestration as a set of effects (`Spawn`, `Await`,
`Emit`) and let the framework interpret them. Pure functional;
trivially mockable; perfect typing.

**Strengths.** Beautiful. Inspectable. Mockable in tests with no
framework involvement.

**Weaknesses.** Python has no native algebraic-effects support.
Simulating effects via generators that `yield` effect objects and
have an interpreter pump them is workable but reinvents the
async/await machinery the language already provides.

**Verdict.** The async/await alternative (Option C) gets 80% of
the elegance with 20% of the implementation. Pass.

---

## Recommendation: Option C, generator-style coroutines

The opinionated take is C. Rationale:

1. **It generalises every existing use.** `adaptive_reevaluate`
   re-expresses cleanly. `keep_alive` doesn't fit the orchestration
   pattern at all (no sub-queries, just a watchdog) and stays as
   a plain `SessionMiddleware`. `jsd_compare` lands as ~10 lines.
   N-way fan-outs land as a `for model in models: queries.append(...)`
   plus an `await ctx.parallel(*queries)`.
2. **It captures the orchestration concern at the right level.**
   The framework owns lifecycle (parent-child tracking, cleanup,
   cancellation); the middleware owns *what* gets spawned and *how
   results are joined*. The split matches the responsibility
   boundary.
3. **Python async/await is the right substrate.** `await` is the
   natural notation for "wait for sub-query completion."
   `AsyncIterator` is the natural notation for "stream of derived
   responses." No custom interpreter to debug.
4. **Stack frames work for debugging.** When `jsd_compare` raises
   on a malformed response, the traceback shows the line in
   `jsd_compare`'s body where the failure happened, not a generic
   "combinator step 5" pointer.
5. **It composes with `CapabilityGatedMiddleware` unchanged.** The
   gate wraps the whole orchestration; per-query opt-out short-
   circuits the entire coroutine without engaging it. `adaptive
   _reevaluate`'s capability gating ports over without redesign.

The cost is real:

- **A new framework class:** `OrchestrationMiddleware` (or a
  decorator-shaped factory) plus an `OrchestrationContext` exposing
  the primitives.
- **Framework-tracked parent-pointers:** replacing the synthetic-id
  encoding (`Q:<hex>__<real>`, `__keepalive_term_<hex>`) with a
  typed parent-id field on the spawned query's lifecycle record.
  The Hub doesn't need to know; the change is in Layer 1's session
  state.
- **A per-orig_id orchestration scheduler:** the coroutine runs
  inside the session's event loop; the framework provides the
  context, drives the coroutine, and resumes it when sub-queries
  complete. Cancellation cleanly terminates the coroutine and any
  outstanding sub-queries.

Estimated implementation footprint: ~600-800 lines of new framework
code (the orchestration class, the context, the scheduler, the
parent-pointer machinery), ~150 lines for the
`adaptive_reevaluate` refactor, ~100 lines of tests covering the
primitive itself. Comparable to the SELECTOR arc.

---

## Implementation sketch

The framework needs four new pieces:

### 1. `OrchestrationContext`

Per-orig_id object the orchestration coroutine receives. Owns the
per-parent state: in-flight sub-queries, response buffers,
cancellation flag.

```python
class OrchestrationContext:
    """Per-orig_id orchestration state passed to the coroutine."""

    @property
    def parent_id(self) -> str: ...

    @property
    def parent_query(self) -> KataGoQuery: ...

    async def spawn(
        self, query: KataGoQuery
    ) -> AsyncIterator[KataGoResponse]:
        """Submit a sub-query; iterate its responses as they arrive.

        Responses are pre-labelled to the parent's orig_id from the
        coroutine's perspective; the framework tracks the actual
        sub-query's id under the hood. The iterator completes when
        the sub-query reaches QUERY_COMPLETE.
        """

    async def parallel(
        self, *queries: KataGoQuery
    ) -> list[list[KataGoResponse]]:
        """Spawn N sub-queries; gather; return per-query response lists.

        Convenience over `asyncio.gather([list(spawn(q)) async for ...])`.
        Cancellation of the parent cancels all in-flight sub-queries.
        """

    def original_stream(self) -> AsyncIterator[KataGoResponse]:
        """Iterate the parent query's own responses.

        For middlewares that want to observe the parent's stream
        before deciding what to spawn (the `adaptive_reevaluate`
        pattern: wait for original finals, then decide on deepening).
        """

    def derive(
        self,
        template: KataGoResponse,
        *,
        extra: Optional[dict] = None,
        opaque_overrides: Optional[dict] = None,
    ) -> KataGoResponse:
        """Helper: synthesise a derived response from a template."""
```

### 2. `OrchestrationMiddleware` (decorator)

```python
def orchestration_middleware(name: str) -> Callable:
    """Decorator: wrap an async coroutine into a SessionMiddleware.

    The coroutine has signature
    `(parent_query, ctx) -> AsyncIterator[KataGoResponse]`.
    The framework instantiates one coroutine per parent query,
    drives it inside the session's event loop, and emits its
    yielded responses on the parent's response stream.

    Cancellation of the parent (client disconnect, terminate query,
    session end) cancels the coroutine and all in-flight sub-queries.
    """
```

### 3. Parent-pointer machinery

Today, when `adaptive_reevaluate` calls `submit_query(synthetic_id,
deeper)`, the synthetic_id encodes the parent in a string. A typed
replacement: a per-orig_id record in the session's state that maps
sub-orig_id → parent-orig_id. The orchestration scheduler consults
this record to route sub-query completions back to the right parent
context.

The `ClientSession`'s `_active_queries` dict is the natural home
for the parent-pointer; extend its tuple to carry an optional
`parent_orig_id` field. `submit_query` takes an optional `parent`
argument; orchestration sub-queries set it; client queries don't.

### 4. The scheduler

Lives inside `ClientSession` (or a per-session helper). Responsible
for:

- Instantiating the orchestration coroutine per parent query.
- Resuming the coroutine when sub-queries complete (driven by the
  upstream response routing).
- Forwarding the coroutine's yielded responses onto the parent's
  response stream (via the existing send-loop machinery).
- Cancellation: parent terminate or session end → cancel the
  coroutine → cancel all in-flight sub-queries via
  `terminate_query`.

The scheduler is the trickiest piece: it must integrate with the
existing `MiddlewareChain` composition (orchestration middlewares
are still middlewares; they participate in the chain) and with the
session lifecycle. The cleanest shape is probably an orchestration
coroutine running as an `asyncio.Task` per parent, with the
framework owning the task's lifetime and the cancellation chain.

---

## Migration path

1. **Land the framework piece.** New module
   `middleware/orchestration.py` exposing `OrchestrationContext`,
   `OrchestrationMiddleware`, `orchestration_middleware`. Tests
   covering the primitive in isolation (mock sub-query completion,
   verify cancellation, verify cleanup, verify parallel/sequential
   compositions).
2. **Refactor `adaptive_reevaluate`.** Re-express as an
   orchestration coroutine. The existing tests should pass
   unchanged (the observable behaviour is identical). The diff
   should *shrink* the file substantively — most of the buffer-
   and-state-machine code is replaced by `await ctx.spawn(...)`
   and `async for resp in ctx.original_stream(): yield resp`.
   This is the proof that the abstraction was correctly identified.
3. **Land `jsd_compare` (or whatever the first new use case is).**
   Demonstrates the abstraction's power on a previously-unsupported
   use case. New middleware factory; new tests.
4. **Document the pattern.** A new section in `FRAMEWORK.md` on
   "Orchestration middleware" alongside the existing
   Transformer/Middleware split. The orchestration pattern is the
   third extension surface.

This is a v1.0.16-or-later arc. It deliberately does not bundle
with any other proxy work; it's a focused architectural change
that wants its own release window for clean attribution.

---

## Risks and open questions

**Cancellation semantics.** When the parent query is terminated
(client disconnect, explicit terminate, keep-alive watchdog
timeout), the orchestration coroutine and all its in-flight
sub-queries must be cancelled cleanly. The Python async-cancel
machinery is well-defined but the framework needs to be careful
that:

- Cancellation propagates from the parent to the coroutine to all
  spawned sub-queries.
- The Hub is notified that orphaned canonicals from cancelled
  sub-queries can be terminated (the existing orphan-canonical
  cleanup contract from the keep-alive arc; should compose
  unchanged).
- Cleanup of the per-parent state is automatic on cancellation.
- A coroutine that ignores `CancelledError` (e.g. catches all
  exceptions in a try/except) should still be torn down — perhaps
  via `task.cancel()` followed by a bounded wait.

**Coroutine error handling.** A bug in the orchestration coroutine
(say, KeyError on a malformed response) should:

- Surface as a structured error response to the client (per
  ADR-0002), not silently drop the parent query into the void.
- Cancel any in-flight sub-queries.
- Log the traceback at ERROR with the parent orig_id for triage.

The framework needs a top-level try/except around the coroutine
that does these things, comparable to how `_deliver_upstream`
handles middleware errors today.

**Composition with `CapabilityGatedMiddleware`.** The orchestration
middleware is still a `SessionMiddleware`, so it composes with
`CapabilityGatedMiddleware` via the existing wrapper. The
question: when a query opts out of the orchestration capability,
the wrapper short-circuits before any state is created — which is
what we want. Verify this composes cleanly in a test.

**Reentrancy.** A sub-query spawned by an orchestration coroutine
goes through the full proxy pipeline, including the
`MiddlewareChain`. If the chain includes another orchestration
middleware, the sub-query could trigger its own orchestration.
This is presumably correct behaviour (orchestration is composable)
but needs explicit testing — and a guard against unbounded
recursion (if a middleware spawns a sub-query that spawns another
sub-query that spawns…, the budget for "depth of orchestration
nesting" should be finite, with a structured error on overflow).

**Performance.** Each orchestration coroutine is one
`asyncio.Task` per parent query. For a session with many concurrent
parent queries (the analysis-tab range case can issue dozens), the
task count grows linearly. This is fine — `asyncio` handles
thousands of tasks routinely — but worth noting that a
pathological client could explode task counts. The existing
per-session ratelimit and message-size caps bound this in practice.

**Synthetic-id migration.** The synthetic-id encoding pattern
currently in `adaptive_reevaluate` and `keep_alive` is cited as a
hack this design replaces. But not every middleware needs the
orchestration primitive — `keep_alive`'s `__keepalive_term_<hex>`
is for synthesised TERMINATE queries that are not orchestration
sub-queries; they're spawned to cancel something. Either keep
that pattern (it's a small, contained string) or extend the
orchestration context to support synthesised terminate queries
too (`ctx.terminate(orig_id)`). The latter is cleaner; either is
defensible.

---

## Out of scope (for this design)

Deliberately not addressed in this note:

- **Refactoring `Transformer` to share orchestration mechanics.**
  Transformers are sync per-message; they don't need orchestration.
  The split between Transformer and Middleware is the right
  factoring (Phase 1's roadmap reaffirmed this). Orchestration
  lives entirely on the Middleware side.
- **A typed wire schema for sub-query responses.** Sub-queries
  return the same wire shapes parent queries do. The orchestration
  context exposes them as `KataGoResponse` (the existing
  discriminated union); no new wire type.
- **Backpressure across sub-queries.** If one sub-query is
  producing responses faster than the orchestration coroutine can
  process them, asyncio's natural backpressure (the queue between
  router and session) handles it. No special framework support
  needed.
- **Distributed orchestration across multiple proxy instances.**
  Out of scope. Orchestration is per-session, lives in one Python
  process.
- **Replacing `submit_query` entirely.** `submit_query` remains as
  the lower-level escape hatch for middlewares that don't fit the
  orchestration pattern (like a hypothetical middleware that fires
  a one-shot announce query without caring about the response).
  Orchestration is the layer above; `submit_query` is the layer
  below.

---

## Related work

### `reactive_pipeline/` (the experimental subpackage)

The proxy already explored DSL-shaped architecture in
`reactive_pipeline/core.py` (renamed from `rxp/rxp.py` in v1.0.13).
The DSL there is for *value-flow*: a computation graph over indexed
values where each node has a topology (which inputs it depends on)
and an algebra (Map/Fold/ZipWith). Used internally by
`DeltaAnalysisState` for incremental multi-resolution analysis.

It targets a different layer than the orchestration middleware
this note proposes: `reactive_pipeline` is for derived computations
*over* a fixed value space (Go-game-state arrays, policy heads);
orchestration middleware is for derived *queries* that the proxy
sends to upstream LEAFs. The two could conceivably interact (an
orchestration coroutine could use a `Pipeline` internally to
process the joined responses), but they are not substitutes.

The `reactive_pipeline` package's `__init__.py` is explicit:

> If you are looking for the proxy's response-transformation
> extension points, see AbstractProxy/protocol_transformer.py
> (Transformer) and session_middleware.py (SessionMiddleware)
> instead.

The orchestration primitive proposed here is the third extension
surface that note's framing implicitly invites: alongside
Transformer (sync content) and SessionMiddleware (async stream),
the new surface is OrchestrationMiddleware (async coroutine over
sub-query lifecycle).

### Prior `submit_query` usage as evidence

`adaptive_reevaluate` (line 247): `asyncio.create_task(submit_query(synthetic_id, deeper))`.

`keep_alive` (`KeepAliveMiddleware`): uses `caps.terminate_query`
(not `submit_query`) — closer to a one-shot RPC than orchestration.
Stays as a plain `SessionMiddleware` even after this design lands.

That's the entire current evidence base — two middlewares, one of
which is orchestration-shaped and one of which isn't. Hence the
"third use case would make it obvious" framing earlier; this
design lands ahead of that evidence on the user's invocation of
Jacobi's principle (generalising before the third instance forces
it). The risk is the standard one for designing-ahead-of-evidence:
the abstraction may need refinement once the second orchestration
middleware (jsd_compare) is actually built. The mitigation is to
land the framework piece first, refactor `adaptive_reevaluate` as
the validation, and let `jsd_compare` use the abstraction with
real eyes on it before stamping the design as final.

— end design note —
