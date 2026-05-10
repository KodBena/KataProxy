# Roadmap — Orchestration Middleware (v1.0.16)

A planning artifact for the proxy-side implementation of the
orchestration primitive whose design space was explored in
`docs/design-fork-join-orchestration.md` (merged via PR #23). Sibling
to `roadmap-capability-negotiation.md` (Phase 1, v1.0.14) and
`roadmap-selector-router.md` (Phase 2+3, v1.0.15) in shape and
discipline. Written 2026-05-10 against proxy v1.0.15. Authoritative
for the `feat/orchestration-middleware` branch; superseded by the
v1.0.16 release notes once tagged.

This document is **scoped to the proxy submodule**. No wire-shape
changes; no new env var; no new role; no client-side work required.
The implementation introduces a third extension surface alongside
`Transformer` (sync per-message) and `SessionMiddleware` (async per-
stream): `OrchestrationMiddleware` (async coroutine over the parent-
plus-sub-query lifecycle).

The recommendation locked down here is **Option C** from the design
note: generator-style orchestration coroutines that receive a
context object and use framework-provided primitives (`ctx.spawn`,
`ctx.parallel`, `ctx.original_stream`) to express orchestration as
sequential async/await code. The framework owns parent-child
tracking, lifecycle, cleanup, cancellation; the middleware owns
*what* gets spawned and *how results are joined*.

Cross-references: `docs/design-fork-join-orchestration.md` (the
design exploration this roadmap implements); `proxy/ARCHITECTURE.md`
(Layer 1's extension surfaces this work extends); `proxy/middleware/
adaptive_reevaluate.py` (the existing orchestration-shaped middleware
that the implementation refactors as the validation criterion);
`proxy/middleware/session_middleware.py` (the abstraction
orchestration sits alongside, not replaces); `proxy/reactive_pipeline/`
(experimental related work at a different layer — value-flow over
indexed arrays, not query orchestration).

---

## Why this exists

Three converging facts from the design note:

1. **Every stateful middleware that uses `submit_query` today
   reimplements the same pattern.** Synthesise a synthetic-id
   encoding the parent-pointer; buffer responses per parent in an
   instance dict; hand-roll a counter for "all sub-queries
   complete"; emit derived responses under the parent's orig_id;
   manual cleanup of all the bookkeeping. `adaptive_reevaluate` is
   the worked example.

2. **The pattern is fork-join + emit-derived.** Spawn N sub-queries
   from one parent; await some or all of them; combine their
   responses; emit something on the parent's response stream.
   `adaptive_reevaluate` is the degenerate fan-out-of-1; future
   middlewares (the `jsd_compare` use case the user cited; the
   N-way comparisons the autonomous-srs-loop note sketches) are
   fan-out-of-N.

3. **The right primitive is async/await over a context object.**
   The design note's Option C, accepted by the user with an
   acknowledged personal bias toward Option E (algebraic effects)
   that Python lacks the substrate for. Coroutines in Python are
   close enough to algebraic effects in practice — the
   yield-suspend-resume mechanics of `await` are effect operations
   in disguise — that a future migration to a true effects system
   (if Python ever gets one) would be tractable from this baseline.

This v1.0.16 release lands the framework piece, refactors
`adaptive_reevaluate` to use it (the validation that the abstraction
was correctly identified), and ships the third extension-surface
documented as a peer of Transformer and SessionMiddleware.

---

## Scope

**In scope (v1.0.16):**

- New module `middleware/orchestration.py` exposing
  `OrchestrationContext`, `OrchestrationMiddleware`, and the
  `orchestration_middleware` decorator.
- Per-orig_id orchestration scheduler in `ClientSession`
  (`proxy_server.py`): sub-query parent-pointer registry, response
  routing, cancellation propagation, completion detection.
- Refactor of `middleware/adaptive_reevaluate.py` to express its
  logic as an orchestration coroutine. Behaviour preserved exactly;
  the diff shrinks the file substantively (the validation criterion).
- Composition with `CapabilityGatedMiddleware` (existing) — the
  gate wraps the orchestration; per-query opt-out short-circuits
  before the orchestration coroutine runs.
- Tests: orchestration primitive in isolation (mocked sub-queries),
  composition with capability gating, cancellation semantics, error
  propagation. Plus the existing `adaptive_reevaluate` tests
  unchanged (must pass against the refactored implementation).
- Documentation: a new section in `FRAMEWORK.md` on Orchestration
  middleware as the third extension surface.

**Out of scope (deferrals):**

- **No new use case beyond the `adaptive_reevaluate` refactor.**
  `jsd_compare` and any other concrete orchestration middleware are
  follow-on arcs (v1.0.17+). v1.0.16's job is the framework piece
  plus the proof that the abstraction fits the pattern it was
  designed for.
- **No `keep_alive` refactor.** `KeepAliveMiddleware` is a watchdog
  with no fork-join shape; it stays as a plain `SessionMiddleware`.
  Its `__keepalive_term_<hex>` synthetic-id pattern stays — small,
  contained, and not a fork-join concern.
- **No multi-orchestration chains.** v1.0.16 supports at most one
  orchestration middleware in a `MiddlewareChain`. The reason is
  algebraic, not operational. Chaining two orchestration coroutines
  is *implementable* — ownership-aware routing in `MiddlewareChain`
  plus per-middleware sub-query registries, ~150 lines of bounded
  framework code. But implementability is not composability: the
  abstraction lacks algebraic laws strong enough for chained
  orchestration to be true composition rather than operational glue.
  See *Risks and design decisions → On chained orchestration* below
  for the full reasoning.
- **No `Transformer` changes.** Orchestration is exclusively a
  middleware-side concept. Transformers are sync per-message; they
  don't need the lifecycle machinery orchestration provides.
- **No wire-protocol changes.** No new env var, no new wire field,
  no role addition. v1.0.16 is purely an internal framework lift.
- **No new operator-opt-in gate.** Orchestration is structural,
  not behaviour-on-the-wire. The `PROXY_ADVERTISE_CAPABILITIES`
  gate from Phase 1 covers all wire-visible behaviour.

**Wire-compatibility:** v1.0.16 with default config is byte-
identical on the wire to v1.0.15. The `adaptive_reevaluate` refactor
preserves observable behaviour exactly (the existing tests, which
verify wire-shape behaviour, are the regression net).

---

## The API

### `orchestration_middleware` decorator

```python
def orchestration_middleware(
    *,
    name: str,
    max_depth: Optional[int] = None,
) -> Callable[
    [Callable[[KataGoQuery, "OrchestrationContext"], AsyncIterator[KataGoResponse]]],
    Callable[[], "OrchestrationMiddleware"],
]:
    """Decorator: wrap an async coroutine into an orchestration middleware factory.

    The decorated coroutine has signature
    `(parent_query, ctx) -> AsyncIterator[KataGoResponse]`.

    The decorator returns a *factory* (callable taking no arguments
    and returning an `OrchestrationMiddleware` instance) so callers
    can register it as a middleware factory in `proxy_server.py`'s
    `_make_middleware`, mirroring how `adaptive_reevaluate(...)`
    today returns a `SessionMiddleware`.

    Parameters
    ----------
    name:
        Human-readable name for logs and the `Transformer.name`-style
        identifier the chain composer uses. Required.
    max_depth:
        Maximum orchestration nesting depth. None → use
        `cfg.ORCHESTRATION_MAX_DEPTH` (default 4). Sub-queries spawned
        via `ctx.spawn` increment depth by 1; depth-overflow raises
        a structured error.
    """
```

Usage pattern (factory-with-config closure):

```python
def adaptive_reevaluate(
    worst_quantile: float = 0.25,
    extra_visits: int = 800,
    window_size: int = 3,
) -> Callable[[], OrchestrationMiddleware]:
    @orchestration_middleware(name="adaptive_reevaluate")
    async def coro(parent: KataGoQuery, ctx: OrchestrationContext):
        # closure captures worst_quantile, extra_visits, window_size
        ...
    return coro
```

The factory shape is identical to existing middleware factories
(`adaptive_reevaluate(...)`, `keep_alive(...)`) so wiring in
`_make_middleware` doesn't change shape — `CapabilityGatedMiddleware`
wraps the factory's product the same way.

### `OrchestrationContext`

Per-orig_id object passed to the coroutine. Owns the orchestration
state for one parent query.

```python
class OrchestrationContext:
    """Per-orig_id orchestration state passed to the coroutine.

    Lifetime: one per parent query, created when the orchestration
    middleware engages on the parent's on_query, destroyed when the
    coroutine completes or is cancelled.
    """

    @property
    def parent_id(self) -> str:
        """The parent's orig_id (client namespace)."""

    @property
    def parent_query(self) -> KataGoQuery:
        """The parent query as parsed from the client wire."""

    @property
    def depth(self) -> int:
        """Orchestration nesting depth (0 for client-originated parent)."""

    @property
    def session_capabilities(self) -> SessionCapabilities:
        """Underlying session capabilities (terminate_query, etc.).

        Exposed for orchestration coroutines that need lower-level
        access — typical use is rare. The orchestration primitives
        below cover the common cases.
        """

    async def spawn(
        self, query: KataGoQuery
    ) -> AsyncIterator[KataGoResponse]:
        """Submit a sub-query and iterate its responses.

        Yields each response as it arrives from the upstream. The
        iterator completes when the sub-query reaches QUERY_COMPLETE
        (analogous to the parent's natural completion).

        From the coroutine's perspective the responses arrive
        labelled with the sub-query's identity; the framework tracks
        the actual sub-query orig_id under the hood for response
        routing. To emit a response on the *parent's* stream, the
        coroutine `yield`s it (which goes through the orchestration
        middleware's outer envelope).

        Sub-queries inherit the parent's session and traverse the
        full proxy pipeline (transformers, hub coalescing, router
        dispatch). They are subject to the same cancellation
        semantics as the parent: cancelling the orchestration
        coroutine cancels all its in-flight sub-queries.

        Sub-queries also inherit the parent's depth + 1; on
        depth-overflow (`> max_depth`), spawn raises
        `OrchestrationDepthError`. The coroutine should not catch
        this exception — it is part of the framework's loud-failure
        surface per ADR-0002.
        """

    async def parallel(
        self, *queries: KataGoQuery
    ) -> list[list[KataGoResponse]]:
        """Spawn N sub-queries; gather; return per-query response lists.

        Convenience over

            await asyncio.gather(*[
                _collect(self.spawn(q)) for q in queries
            ])

        where `_collect` is `async def _collect(it): return [r async for r in it]`.

        Any sub-query raising propagates as an exception (the parallel
        await re-raises the first exception per asyncio.gather's
        default `return_exceptions=False` semantic). Use `spawn`
        directly with explicit error handling if you need
        per-sub-query error policies.
        """

    def original_stream(self) -> AsyncIterator[KataGoResponse]:
        """Iterate the parent query's own responses.

        Returns an async iterator over the responses the parent
        query receives from the upstream. The iterator completes
        when the parent's QUERY_COMPLETE arrives.

        If the coroutine never iterates this stream, the responses
        are buffered (bounded by `cfg.ORCHESTRATION_BUFFER_MAX`,
        default 1024) until the coroutine completes; on overflow
        the oldest are dropped with a WARNING. This is a guard
        against leaks; well-formed coroutines either iterate the
        stream or explicitly `await ctx.discard_originals()`
        (see below) to signal "I don't want them."
        """

    async def discard_originals(self) -> None:
        """Signal that the coroutine will not iterate original_stream.

        Releases the original-stream buffer immediately. Useful for
        coroutines that fully replace the original (like a
        hypothetical `jsd_compare` that wants only the JSD-annotated
        derived responses, not the per-model originals).

        After calling this, the parent query's own responses are
        dropped silently (not buffered, not yielded). The parent's
        QUERY_COMPLETE is still observed and triggers an internal
        flag the coroutine can poll via `ctx.original_completed`.
        """

    @property
    def original_completed(self) -> bool:
        """True iff the parent query has reached QUERY_COMPLETE.

        Useful for coroutines that need to know whether all original
        responses have arrived without iterating original_stream
        (typically after `discard_originals`).
        """
```

### Composition with `CapabilityGatedMiddleware`

Orchestration middlewares are still `SessionMiddleware`s. The
existing `CapabilityGatedMiddleware` wraps them transparently:

```python
gated = CapabilityGatedMiddleware(
    "adaptive_reevaluate",
    adaptive_reevaluate(worst_quantile=0.25, extra_visits=800)(),
    # note: () because adaptive_reevaluate now returns a factory,
    # called once to instantiate the middleware
)
```

When a query opts out, the gate's `on_query` and `handle_response`
short-circuit before the orchestration coroutine is instantiated.
No state is created; no sub-queries are spawned; no GPU cost.

### Composition with `MiddlewareChain`

For v1.0.16, **only one orchestration middleware per chain** is
supported. The chain composer (`MiddlewareChain.handle_response`)
detects multiple orchestration middlewares and raises at chain-
construction time:

```python
class MiddlewareChainConfigurationError(RuntimeError):
    """Raised at chain construction when multiple orchestration
    middlewares are present in the same chain."""
```

The current `_make_middleware` chain has at most one orchestration
candidate (`adaptive_reevaluate`); chaining a second is the trigger
for revisiting the multi-orchestration design.

---

## Framework changes by file

### New: `middleware/orchestration.py` (~400 lines)

Module containing:
- `OrchestrationContext` class.
- `OrchestrationMiddleware` class extending `SessionMiddleware`.
- `orchestration_middleware` decorator.
- `OrchestrationDepthError` and `MiddlewareChainConfigurationError`
  exception classes.

The `OrchestrationMiddleware` class implements `SessionMiddleware`
methods by:

- `on_session_start(caps)`: store the SessionCapabilities for
  passing into spawned contexts.
- `on_session_end()`: cancel any orchestration tasks still alive,
  releasing all per-parent state.
- `on_query(orig_id, query)`: instantiate `OrchestrationContext` for
  this orig_id; spawn the coroutine as an `asyncio.Task` driven by
  the context.
- `handle_response(orig_id, response, submit_query)`: route the
  response either to the parent's `original_stream` (if `orig_id`
  matches the parent) or to the appropriate sub-query's `spawn`
  iterator (if the response belongs to a sub-query whose parent is
  this context). Yield the orchestration coroutine's emitted
  responses to the chain's outer middleware.

### New: per-session orchestration registry (in `ClientSession`)

Modest extension to `ClientSession` in `proxy_server.py`:

- New attribute: `_orchestration_contexts: dict[str, OrchestrationContext]`
  mapping parent-orig_id to context. Populated by
  `OrchestrationMiddleware.on_query`.
- New attribute: `_sub_to_parent: dict[str, str]` mapping sub-query
  orig_id to parent orig_id. Populated by `OrchestrationContext.spawn`
  before submitting the sub-query through `_handle_query`.
- Modified: `_handle_query` accepts an optional `parent_orig_id`
  argument (None for client-originated queries; set for
  orchestration-spawned sub-queries). When set, registers the
  parent-pointer in `_sub_to_parent` and tracks the depth.
- Modified: `_deliver_upstream` (response delivery path) checks
  `_sub_to_parent`; if the response's orig_id is a known sub-query,
  routes to the parent's orchestration context's spawn iterator
  instead of through the regular middleware chain.

The `_active_queries` dict gains an optional `parent_orig_id` slot
in its tuple shape: `(subscriber_internal_id, canonical_id,
parent_orig_id_or_None)`.

### New: `cfg.ORCHESTRATION_MAX_DEPTH`, `cfg.ORCHESTRATION_BUFFER_MAX`

Two new config entries in `sproxy_config.py`:

```python
# Maximum orchestration nesting depth. A sub-query spawned by an
# orchestration coroutine that itself triggers an orchestration
# middleware increments depth; depth-overflow raises
# OrchestrationDepthError. Default 4 — enough for non-trivial
# nesting (a top-level orchestration that uses a derived helper
# orchestration that uses one more level of derivation), bounded
# enough to prevent unbounded recursion.
ORCHESTRATION_MAX_DEPTH: int = int(
    os.environ.get("PROXY_ORCHESTRATION_MAX_DEPTH", "4")
)

# Maximum number of original responses buffered for an orchestration
# coroutine that hasn't iterated original_stream() yet. On overflow
# the oldest are dropped with a WARNING (the coroutine is presumably
# misbehaving — it should either iterate original_stream or call
# discard_originals).
ORCHESTRATION_BUFFER_MAX: int = int(
    os.environ.get("PROXY_ORCHESTRATION_BUFFER_MAX", "1024")
)
```

### Refactor: `middleware/adaptive_reevaluate.py`

Existing imperative implementation replaced with an orchestration
coroutine. Behaviour preserved exactly; the existing test suite
(`tests/test_capability_negotiation.py::TestAdaptiveReevaluateMetadata`
and any integration tests against adaptive's wire behaviour) must
pass unchanged.

The refactored shape (sketch — exact code lands in implementation):

```python
def adaptive_reevaluate(
    worst_quantile: float = 0.25,
    extra_visits: int = 800,
    window_size: int = 3,
) -> Callable[[], OrchestrationMiddleware]:

    @orchestration_middleware(name="adaptive_reevaluate")
    async def coro(parent: KataGoQuery, ctx: OrchestrationContext):
        # Read per-query metadata overrides from the capabilities dict;
        # fall back to closure-captured defaults.
        cap_meta = (
            (parent.opaque.get("capabilities") or {})
            .get("adaptive_reevaluate") or {}
        )
        q_quantile = cap_meta.get("worst_quantile", worst_quantile)
        q_extra = cap_meta.get("extra_visits", extra_visits)

        if parent.action != KataGoAction.ANALYZE:
            # Not an analyze query: pass everything through and exit.
            async for resp in ctx.original_stream():
                yield resp
            return

        # Stage 1: collect originals.
        finals: list[AnalyzeResponse] = []
        async for resp in ctx.original_stream():
            if isinstance(resp, MetadataResponse):
                yield resp
                continue
            if resp.is_during_search:
                yield resp
                continue
            finals.append(resp)

        # Stage 2: decide on adaptation.
        all_turns = {f.turn_number for f in finals}
        worst = _find_worst_turns(finals, q_quantile)
        deepen = _expand_window(worst, all_turns, window_size)

        if not deepen:
            # No adaptation: emit originals as-is.
            for f in finals:
                yield f
            return

        # Stage 3: emit originals with isDuringSearch patched for
        # turns that will be re-analyzed.
        for f in finals:
            if f.turn_number in deepen:
                yield replace(f, is_during_search=True)
            else:
                yield f

        # Stage 4: spawn deeper query; re-emit its responses.
        deeper = _build_deeper_query(parent, sorted(deepen), q_extra)
        async for resp in ctx.spawn(deeper):
            yield resp

    return coro
```

The diff against the existing implementation should:
- **Remove**: `_expected`, `_buffered`, `_orig_queries`,
  `_per_query_quantile`, `_per_query_extra_visits` instance dicts;
  the LRU eviction loop; the synthetic-id encoding helpers
  (`_make_synthetic_id`, `_is_synthetic`, `_real_id_of`); the
  manual on_query/handle_response state machine.
- **Keep**: `_find_worst_turns`, `_expand_window`,
  `_build_deeper_query` (pure helpers, unchanged signatures).
- **Net change**: ~150 line reduction (the file shrinks from ~324
  to ~170 lines; most of the savings come from deleting the manual
  state machine).

If the refactor *grows* the file, the abstraction is wrong and the
roadmap needs revision. This is the validation criterion.

### `middleware/keep_alive.py`: untouched

Watchdog middleware; no fork-join shape; no `submit_query`. Stays
as plain `SessionMiddleware`. Its synthetic-id encoding
(`__keepalive_term_<hex>`) is local to the synthesised TERMINATE
queries it issues via `caps.terminate_query`; not an orchestration
concern.

### `middleware/session_middleware.py`: untouched API; new sibling

The `SessionMiddleware` ABC is unchanged. `OrchestrationMiddleware`
is a *concrete* subclass that fits the existing chain composition;
it doesn't redefine the ABC.

`MiddlewareChain` gains the multi-orchestration-detection guard at
construction (raises `MiddlewareChainConfigurationError` if more
than one orchestration middleware is present). The detection is by
isinstance(`OrchestrationMiddleware`).

### `proxy_server.py:_make_middleware` rewiring

Trivial. The factory call changes shape:

```python
def _make_middleware() -> SessionMiddleware:
    base = CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=3,
        )(),  # NEW: () because adaptive_reevaluate now returns a factory
    )
    if cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS <= 0:
        return base
    return MiddlewareChain(
        inner=base,
        outer=KeepAliveMiddleware(
            idle_timeout_seconds=cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS,
        ),
    )
```

The single-character change (`)(` becomes `)())`) is the only
caller-visible API impact. This is a trivial migration any future
external consumer of `adaptive_reevaluate` would do once.

---

## Test plan

KataGo-free per the existing tests/diagnose_phase{1,2,3}.py
precedent.

### New: `tests/test_orchestration_middleware.py` (~30 tests)

- **OrchestrationContext primitive tests** (10):
  - `spawn` yields responses in arrival order.
  - `spawn` iterator completes on QUERY_COMPLETE.
  - `parallel` returns per-query response lists in input order.
  - `parallel` raises if any sub-query raises.
  - `original_stream` yields the parent's responses.
  - `original_stream` buffers up to `ORCHESTRATION_BUFFER_MAX`,
    drops oldest with WARNING on overflow.
  - `discard_originals` releases buffer; subsequent originals are
    dropped.
  - `original_completed` flips to True on parent's QUERY_COMPLETE.
  - `parent_id` and `parent_query` properties expose the right
    values.
  - `depth` is 0 for client-originated parent.

- **Coroutine lifecycle** (8):
  - Coroutine starts on parent's on_query.
  - Coroutine's yields are emitted on the parent's response stream.
  - Coroutine completion (StopAsyncIteration) triggers parent
    QUERY_COMPLETE.
  - Cancellation (parent terminate) cancels the coroutine.
  - Cancellation cancels all in-flight sub-queries.
  - Unhandled exception in coroutine → structured error response
    to client + logged at ERROR.
  - Coroutine that ignores CancelledError still gets torn down via
    bounded wait.
  - on_session_end cancels all live orchestration tasks.

- **Reentrancy** (3):
  - Sub-query spawned by orchestration coroutine that itself
    triggers an orchestration middleware → nested orchestration.
  - Depth tracking: nested context's `depth` is parent's + 1.
  - `OrchestrationDepthError` raised on overflow; structured error
    delivered to root client.

- **Composition with CapabilityGatedMiddleware** (4):
  - Per-query opt-out short-circuits before coroutine instantiation.
  - Per-query opt-in instantiates and runs the coroutine.
  - Capability metadata passed to the coroutine via the parent
    query's opaque (the existing pattern).
  - Cleanup on session end works with gating wrapper.

- **Composition with MiddlewareChain** (3):
  - Single orchestration middleware in chain: works.
  - Two orchestration middlewares in chain: raises
    `MiddlewareChainConfigurationError` at construction.
  - Orchestration + non-orchestration (e.g., KeepAliveMiddleware)
    in chain: works; orchestration's yields flow through to the
    outer middleware as expected.

- **Configuration** (2):
  - `cfg.ORCHESTRATION_MAX_DEPTH` parsed correctly; respected by
    spawn.
  - `cfg.ORCHESTRATION_BUFFER_MAX` parsed correctly; respected by
    original_stream buffer.

### Existing tests must pass unchanged

- `tests/test_capability_negotiation.py::TestAdaptiveReevaluateMetadata`
  (7 tests) — verifies adaptive's per-orig_id parameter shift; must
  pass against the refactored implementation.
- `tests/test_protocol_parser.py` (52 tests) — unaffected.
- `tests/test_capability_negotiation.py` (other classes, 34 tests) —
  unaffected.
- `tests/test_selector_router.py` (40 tests) — unaffected.
- KataGo-free diagnostic suite
  (`tests/diagnose_phase{1,2,3}.py`) — must still PASS.

Total expected: 133 existing + ~30 new = ~163 tests. The
adaptive-refactor PR validation is "all existing tests still pass"
(no behaviour change) plus "the file shrinks substantively" (the
abstraction was correctly identified).

---

## Migration ordering on the branch

The implementation lands as multiple commits on
`feat/orchestration-middleware` so the diff is reviewable in pieces:

1. **This roadmap commit** (already on the branch as you read this).
2. **The framework piece, no integration**:
   - New `middleware/orchestration.py` with `OrchestrationContext`,
     `OrchestrationMiddleware`, `orchestration_middleware` decorator,
     exception classes.
   - `cfg.ORCHESTRATION_MAX_DEPTH` and `cfg.ORCHESTRATION_BUFFER_MAX`
     in `sproxy_config.py`; documented in `.env.example`.
   - Tests for the primitive in isolation (with mock SessionCapabilities).
3. **Session-level integration**:
   - `ClientSession` extensions for parent-pointer registry and
     sub-query response routing.
   - `_handle_query` parent_orig_id parameter.
   - `_deliver_upstream` sub-query routing branch.
   - `MiddlewareChain` multi-orchestration guard.
4. **Refactor `adaptive_reevaluate`**:
   - Replace the manual state machine with an orchestration coroutine.
   - Update `_make_middleware` for the factory-shape change
     (`)()`).
   - The existing capability-negotiation tests pass unchanged.
5. **Documentation**:
   - New section in `FRAMEWORK.md` on "Orchestration middleware"
     as the third extension surface (after Transformer and
     SessionMiddleware).
   - Update `ARCHITECTURE.md`'s Layer 1 description to mention the
     new module.
6. **Release**:
   - Bump version to 1.0.16 in pyproject.toml.
   - Tag with annotated changelog.

Each commit is independently reviewable. Commit (2) lands the
abstraction with no caller; commit (3) wires it into the session
machinery; commit (4) is the validation; commits (5)–(6) are the
release ceremony.

---

## Risks and design decisions

### Decision: Coroutine vs. class for the middleware author

**Chosen: coroutine via decorator.** A coroutine is a function that
the decorator wraps into the SessionMiddleware shape. The author
writes ordinary async/await code; the framework provides the
context.

Alternative: an abstract class the author subclasses with
`async def run(self, parent, ctx) -> AsyncIterator[Response]`.

The decorator wins on ergonomics: no `self`, no `def __init__`,
no class boilerplate. The closure pattern (factory function +
inner decorated coroutine) handles configuration cleanly. The
class form is one indirection too many for what is essentially
"a function with a context."

### Decision: `original_stream` as opt-in iteration

**Chosen: original responses are buffered for the coroutine; the
coroutine `async for`s `ctx.original_stream()` to consume them.**
If the coroutine doesn't iterate, originals are buffered (bounded)
until the coroutine completes; on overflow, oldest dropped with
WARNING.

Alternative A: original responses are *automatically* emitted to
the parent's response stream; the coroutine adds derived responses
on top. Simpler; can't express adaptive's "patch isDuringSearch"
behaviour without breaking the model.

Alternative B: original responses are dropped silently unless the
coroutine "claims" the parent stream via an explicit hook.
Cleaner separation but requires a separate hook for the common
"I want to forward originals" case.

The chosen shape lets the coroutine do exactly what it wants: full
control over which originals go through, with what modifications.
Bounded buffering is the safety net against leaks. The
`discard_originals` escape hatch handles the "I want to fully
replace originals" case efficiently.

### Decision: `parallel` re-raises first exception (gather-style)

**Chosen: `parallel(*queries)` re-raises the first sub-query
exception.** Mirrors `asyncio.gather(*, return_exceptions=False)`'s
default; matches what most orchestration code wants.

Alternative: `parallel(*queries, return_exceptions=True)` returns
`list[Result | Exception]`. Easy to add later if a use case wants it.

### Decision: depth bound default 4

**Chosen: `ORCHESTRATION_MAX_DEPTH = 4`.** Allows non-trivial
nesting (a top-level orchestration that uses a derived helper
orchestration that uses one more level) while bounding unbounded
recursion. Configurable via env var. On overflow, structured error
to the root client per ADR-0002.

The number is somewhat arbitrary; 4 is "a few" without being
prodigal. If a real use case wants more, raise the env var.

### Decision: only one orchestration middleware per chain (v1.0.16)

**Chosen: enforce at construction; raise
`MiddlewareChainConfigurationError`.** This is the *algebraic*
position, not the MVP-focus position. See *On chained orchestration*
below.

### On chained orchestration: an algebraic-laws note

The original framing of this limit was operational ("no use case
yet; defer the composition complexity"). On reflection that framing
is weak — it's exactly the kind of utilitarian-evidence argument
that Jacobi's principle ("Man muss immer generalisieren") calls
out. The honest reason for the limit is algebraic.

The orchestration coroutine has access to `ctx.parent_query`,
`ctx.spawn(...)`, `ctx.original_stream()`, etc. — all of which
presuppose a single, well-defined "parent query" with a single
response stream. When two orchestration middlewares O1 and O2
chain (O2 outer, O1 inner), the question becomes: what does O2's
context refer to?

- If `ctx.parent_query` for O2 is the *original* client query, then
  `ctx.spawn(parent.with_model("strong"))` is a sub-query of the
  *root* parent, not of O1's emissions. O1 would see this sub-query
  flowing back through the chain, possibly try to claim it (it has
  the parent's `model` field set, after all). The chain has to
  decide ownership; the answer isn't compositional.
- If `ctx.parent_query` for O2 is the conceptual "parent stream
  coming in", that isn't a `KataGoQuery` at all — it's a stream of
  responses. `with_model` doesn't apply; `spawn` has no template.
  The abstraction's API doesn't admit this case.
- Either way, O1's spawned sub-queries' responses arrive at the
  chain. Whether O2 sees them is a per-pair decision: if O1 spawns
  a deeper query for adaptive analysis and O2 is JSD-comparing,
  should O2 process the deeper-query response (which is the same
  position the parent asked about) as a parent response? Operational
  routing can decide; algebra can't.

Each of these resolutions is implementable. None of them composes
in the laws-shaped sense. Two coroutines chained operate on each
other's emissions through side-effects on the parent context that
must be reasoned about per-pair — which is precisely the failure
mode that suggests the abstraction is doing something other than
what it advertises.

The honest single-orchestration limit reflects what the abstraction
actually provides: a coroutine over a single parent's lifecycle,
which composes cleanly with non-orchestration middleware
(Transformer, plain SessionMiddleware, CapabilityGatedMiddleware
wrappers) but does not compose with itself the way functor
composition would demand.

A future migration to a true effects system (Option E from the
design exploration: `docs/design-fork-join-orchestration.md`) would
make orchestration composition laws-mechanical — handlers stack,
effects compose, the laws hold by construction. Python lacks the
substrate for that today; the closest available primitive
(coroutines + context) gets us most of the ergonomics with none of
the algebraic guarantees. The single-orchestration limit is an
honest admission of where the abstraction's actual scope ends.

The `MiddlewareChainConfigurationError` raised at construction
names this explicitly so the next reader doesn't think the limit is
accidental — it's structural, and lifting it without first lifting
the abstraction's algebraic floor would be operational glue
pretending to compose.

### Decision: factory-shape change for adaptive_reevaluate

**Chosen: `adaptive_reevaluate(...)` returns a factory; calling site
adds `()`.** Single-character API change for the only known
caller (`_make_middleware`). Trivial migration.

Alternative: keep the old shape, have the decorator instantiate the
middleware on first call. More magic; harder to reason about
construction timing.

### Risk: cancellation semantics

The orchestration coroutine runs as an `asyncio.Task` per parent
query. When the parent is cancelled (terminate, disconnect, session
end), the task is cancelled. The coroutine's CancelledError handling
runs; spawn iterators raise CancelledError into the coroutine's
`async for` loops; in-flight sub-queries are terminated via the
framework's cleanup path.

Risk: a coroutine that catches `Exception` (including
`CancelledError` since it's an Exception) without re-raising could
swallow cancellation and leak sub-queries.

Mitigation: the framework's cleanup path runs in a `try/finally`
around the task that explicitly iterates the context's spawn
registry and calls `terminate_query` on each in-flight sub-query.
This runs even if the coroutine swallows CancelledError. The task
is also subject to a bounded shutdown timeout
(`ORCHESTRATION_SHUTDOWN_TIMEOUT_S`, default 5s) after which it's
forcibly cancelled.

### Risk: coroutine error masking

If the coroutine raises an exception, the framework synthesises a
structured error response. But what if the exception is from a
sub-query that already returned a structured error (which the
coroutine then re-raised by accessing a field that didn't exist)?
The client sees a generic orchestration error instead of the more
specific sub-query error.

Mitigation: the framework's exception handler logs the original
exception (with traceback) at ERROR; the client error response
includes a generic "orchestration failed" message but the operator's
log has the full context for triage. This matches how
`_deliver_upstream` handles middleware errors today.

### Risk: response-routing complexity in `_deliver_upstream`

The session's response delivery path gains a new branch:
"if response is a sub-query of an orchestration, route to its
parent context's spawn iterator." This adds a conditional in a
hot path.

Mitigation: the conditional is a single dict lookup
(`_sub_to_parent.get(orig_id)`), structurally negligible. The
existing `_active_queries` lookup is the same shape.

---

## Wire-compatibility posture

**v1.0.16 with default config is byte-identical on the wire to
v1.0.15.** The orchestration primitive is purely an internal
implementation detail; no client ever sees a difference in the
wire protocol because of it.

The only observable change is: when `adaptive_reevaluate` is engaged
(per Phase 1's capability gate, default legacy auto-engage), its
wire behaviour is unchanged — same partial responses with patched
isDuringSearch, same deeper-query injection, same per-orig_id
parameter overrides via capability metadata.

The validation: the existing `tests/test_capability_negotiation.py
::TestAdaptiveReevaluateMetadata` and any wire-shape regression
tests continue to pass against the refactored implementation. If
they don't, the refactor is incorrect and gets pulled back before
the v1.0.16 tag.

---

## Documentation updates

- `proxy/FRAMEWORK.md`: new section on "Orchestration middleware"
  alongside Transformer and SessionMiddleware. Walks through:
  - When to use orchestration vs plain middleware.
  - The OrchestrationContext API.
  - The decorator pattern.
  - The composition with CapabilityGatedMiddleware.
  - The migration story from manual state machines.
- `proxy/ARCHITECTURE.md`: Layer 1 section gains a sub-bullet on
  `middleware/orchestration.py` as the third extension surface.
- `proxy/docs/design-fork-join-orchestration.md`: cross-reference
  this roadmap as the implementation arc.
- `.env.example`: new section for `PROXY_ORCHESTRATION_MAX_DEPTH`
  and `PROXY_ORCHESTRATION_BUFFER_MAX`.

---

## Branch and tag

- Branch: `feat/orchestration-middleware` (this one).
- Tag: `v1.0.16` (minor bump per the established cadence).
- Source branch base: current `main` (post PR #23 merge,
  commit `2d87ef8`).
- Subsequent PRs (`v1.0.17+`) for follow-on use cases (`jsd_compare`,
  the autonomous-srs-loop's multi-weights middleware, etc.).

---

## A note on the personal-bias concession

The design note recorded the user's preference for Option E
(algebraic effects) and the practical choice of Option C
(coroutines). Worth restating here: the coroutine implementation
makes a future migration to a true effects system tractable. The
yields and awaits in an orchestration coroutine are effect
operations in disguise; if Python ever gains native algebraic
effects (PEP 654-flavored exception groups are a half-step in that
direction; nothing more concrete is on the horizon), the
orchestration coroutines could be re-interpreted as effect handlers
without changing their author-facing shape.

In the meantime, async/await is the substrate Python actually has,
and the abstraction sits on it cleanly. The `OrchestrationContext`
methods are effect-shaped (each `await ctx.spawn(...)` is an
effect operation; the framework is the handler); the structure is
algebraic in spirit even if not in form.

— end roadmap —
