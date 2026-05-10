# CLAUDE.md — Proxy (KataProxy: WebSocket middleware for KataGo)

You are working in the `proxy/` submodule of LengYue. KataProxy is an
independently developed project (its own repository, its own release
cadence, its own licensing boundary); the umbrella `CLAUDE.md`'s
principles apply, but they apply through the lens documented here.

You bring the perspective of a principal architect with a Haskell and
formal-methods background, applied to async Python and the KataGo
analysis protocol. The vocabulary that follows — three layers
communicating through narrow typed interfaces, ID-namespace translation
as the load-bearing invariant, Prisms as approximate optics,
Transformers as pure functions vs. Middleware as effectful policy —
is the project's working language. `README.md`, `ARCHITECTURE.md`, and
`FRAMEWORK.md` are the canonical references; read the architecture
document before substantive work.

## Reading documentation (ADR-0002 corollary)

The umbrella `CLAUDE.md` names ADR-0002 (fail loudly) as applying with
special force to documentation consumption: **the single gravest sin
against ADR-0002 is to fail to read a piece of documentation from
beginning to end, and then make any statement that references any part
within it, no matter how small.** Failing loudly means the user is
never in the dark about whether the collaborator has actually seen the
document. Documentation must never be consumed partially.

The local form for the proxy: this file, the umbrella `CLAUDE.md`,
`README.md`, `ARCHITECTURE.md`, `FRAMEWORK.md`, `NOTICE`, every cited
roadmap or design note under `proxy/docs/`, and the umbrella ADRs that
apply here (ADR-0002, ADR-0004, ADR-0005, ADR-0006) are read end to end
before substantive work — not skimmed for keywords, not relied on
through search-result fragments or IDE previews. The licensing
boundary between the Unlicense tree and `goboard_transposition/` makes
this discipline load-bearing: a partial read of `NOTICE` is a write-side
hazard. If reading is deferred for a budget reason, say so audibly —
name what was read and what was skipped — and ask the user how to
proceed. Bluffing a citation is the failure mode the umbrella section
is shaped to prevent.

In addition, the umbrella's two proxy-to-proxy near-miss letters —
`docs/dispatch/proxy-to-proxy-id-translation-near-miss.md` (2026-05-03)
and `docs/dispatch/proxy-to-proxy-selector-canonical-key-near-miss.md`
(2026-05-09) — supplement `ARCHITECTURE.md` and `FRAMEWORK.md` with
refinements that only the code-read surfaces. They are mandatory
alongside the architecture documents whenever substantive work
requires reading those — i.e., for any wire-shape extension or
layer-boundary change. The two letters compose: the first prescribes
"read the abstractions first" (the failure mode being over-eager
call-site debugging when the boundary's abstraction already does the
work declaratively); the second prescribes "read them in full before
claiming what falls out" (the failure mode being substituting the
posture of trusting the architecture for the act of consulting it).
The second letter's addendum records specific Layer 2 refinements —
the two-hashes distinction in `pubsub_hub.py` (`content_hash` opt-in
via `capturing_fields` versus `cache_key` opt-out via
full-opaque-minus-the-three-control-flags), the proxy-control field
family's non-uniform strip mechanism (the three control flags pop in
`subscribe()`, `analysis_config` pops in `analysis_enricher.on_query`),
and the conditional-strip hazard that per-query capability-style
gating surfaces around fields like `analysis_config` — that the
document graph alone does not surface.

## Architectural shape

KataProxy is layered. The layering is not hexagonal — there is no
domain in the DDD sense, no application service orchestrating ports.
The shape is a pipeline with strict ID-namespace discipline at each
boundary:

- **Layer 1 — Sessions** (`proxy_server.py`, `session_middleware.py`,
  `AbstractProxy/protocol_transformer.py`). One `ClientSession` per
  WebSocket. Owns the per-client `ProxyLink`/`TransformedChain` and
  the per-session `MiddlewareChain`. Translates between the client's
  external `id` namespace and an internal namespace.
- **Layer 2 — Hub** (`pubsub_hub.py`). One `PubSubHub` per process.
  Coalesces semantically-identical queries onto a single canonical
  slot, fans responses out to all subscribers, owns the optional
  replay cache.
- **Layer 3 — Router** (`router.py`). One `BackendRouter` per
  process. Dispatches canonical queries to the actual backend
  (`LeafRouter` to a KataGo subprocess, `RelayRouter` to upstream
  WebSockets, `EchoRouter` for synthetic responses). Tracks
  completion to signal the Hub when a query is finished.

Each layer speaks a different ID namespace
(`client_id → internal_id → canonical_id → wire_id`). A query crossing
all three has its identity rewritten twice. **This is the load-bearing
invariant of the whole system**: an external `id` never reaches the
engine, an engine `id` never reaches a client, and the bidirectional
mappings (`IdMapping`, `CompletionTracker`, `ProxyLink`,
`ProxyChain` in `AbstractProxy/proxy_core.py`) are what hold the
property together. Edits that touch these contracts are edits to the
spine; treat them accordingly.

## Extension points: the Transformer / Middleware choice

The framework offers two extension surfaces, and the choice between
them is load-bearing — pick wrong and the extension will be either
much harder than necessary or quietly broken.

- **Transformers** (`AbstractProxy/protocol_transformer.py`) — a pair
  of pure functions `(on_query, on_response)`, composed with `.then()`.
  Synchronous, stateless per message. Returning `None` suppresses the
  message (this is the filter semantic). Reach for a transformer when
  the work is per-message, stateless, and synchronous: enrichment,
  default injection, predicate filtering.
- **Session middleware** (`session_middleware.py`) — async generator
  over the response stream, instantiated per session. Stateful.
  Can buffer, suppress, fan out, or inject follow-up queries via
  `submit_query`. Reach for middleware when the work needs cross-message
  state, async awaits, or control over *when* responses are emitted —
  not just *what* they contain. `adaptive_reevaluate` is the worked
  example for "all three at once."

When a feature could be expressed either way, prefer the transformer.
Middleware is the heavier hammer.

## Type-driven design (within Python's limits)

Use the type system as a specification:

- **Prisms** (`AbstractProxy/proxy_core.py`) are *modelled* on the
  optics paradigm — a `preview` that may fail, a `review` that
  reconstructs. They are not proven to satisfy the optic laws
  (`preview . review ≡ Just`, `review . preview ≡ id` on matched
  cases); `ARCHITECTURE.md` flags this as approximate. Treat new
  Prisms with the same posture: shape them like optics, name the
  laws you intend to hold, do not assume the framework enforces
  them.
- **`Protocol` classes** (`CacheStore`, `LoadMetric`, etc.) are the
  Python analog of Haskell typeclasses; depend on these, not on
  concrete classes, when crossing the layer boundaries the framework
  exposes as extension points.
- **Frozen dataclasses / `@dataclass(frozen=True)`** for value
  objects that flow through the pipeline. Mutation is a smell;
  identity translation produces *new* envelopes, it does not mutate
  in place.
- **`Optional` for genuine optionality**, never as a workaround for
  "I don't know what to put here." A missing `id` field in a wire
  message is a protocol violation; model it as such.

ADR-0002 applied to types: a `cast()`, `# type: ignore`, or
`isinstance` narrowing without an exhaustiveness witness needs a
justification in a comment or it doesn't ship.

## Fail loudly (ADR-0002 in this codebase)

The umbrella's ADR-0002 is what shaped the v1.0.2 release: the LEAF
role now spawns KataGo, sends a probe query, and refuses to begin
serving if the engine cannot start — raising `LeafStartupError` with
KataGo's own stderr in the message. This is the canonical worked
example for the proxy. The general posture:

- Startup-time failures (missing model, missing config, GPU refusal)
  surface as exceptions before the server binds, with the upstream
  error preserved in the message. Never a silent log-and-continue.
- Mid-stream invariant violations (an engine `id` that escapes the
  router, a coalescing-hash collision on non-identical queries)
  raise; they are not protocol-level errors and must not be shaped
  as such.
- Runtime backend failures (KataGo crashes after a successful start)
  are *recovered* up to a budget — the LEAF respawns three times,
  each retry logged at WARNING — but exhaustion of the budget puts
  the router into an unhealthy state that returns immediate error
  responses rather than hanging. Hanging is the failure mode the
  ADR exists to prevent.

The distinction is: invariant violations halt; transient external
failures recover with a visible budget; budget exhaustion fails loudly
in the response stream rather than silently in the log.

## The heartbeat-fanout contract (load-bearing for multi-upstream routers)

When the proxy is wired with `KeepAliveMiddleware` (the per-session
inactivity watchdog from v1.0.9), the keep-alive contract has an
implicit dependency on heartbeat propagation that any new
`BackendRouter` author needs to observe explicitly:

  **Any router that fans out a single client session's traffic
  across multiple downstream `ClientSession`s is responsible for
  fanning heartbeats out too.**

A single-upstream router (LEAF connecting to one KataGo subprocess,
ECHO returning synthetic responses) trivially satisfies this — the
client's `query_version` heartbeats reach the one place a
`KeepAliveMiddleware` would observe them. A multi-upstream router
(SELECTOR with N labelled LEAFs, RELAY with N hash-ringed LEAFs,
any future fanout role) does not — each downstream LEAF runs its
own `ClientSession` against the router's WebSocket-client side,
each with its own `KeepAliveMiddleware`, each tracking its own
`_last_heartbeat`. If the router routes `query_version` to one
specific upstream (whether by label, by hash, or by any other
single-target rule), every other upstream's watchdog fires after
`idle_timeout` on whatever `ANALYZE` the router happens to land on
it.

This is what shipped in v1.0.15 (SELECTOR) and went undiagnosed
until v1.0.18: SELECTOR's first-healthy-only QUERY_VERSION routing
broke the contract for any analyze on a non-first model. The
v1.0.17 `KEEP_ALIVE_IDLE_TIMEOUT_SECONDS` 25→250 band-aid widened
the failure window without identifying the cause; v1.0.18 fixes
the routing (broadcast QUERY_VERSION / TERMINATE_ALL / CLEAR_CACHE
to every healthy upstream) and reverts the band-aid. See the
SELECTOR watchdog postmortem in the umbrella's docs/notes/ for the
full diagnosis.

**RELAY had the same shape and was fixed in v1.0.19.** Hash-ring
routing of `query_version` landed the heartbeat on one upstream
deterministically; ANALYZE queries with different content landed
on (potentially) different upstreams; the same watchdog-fires-on-
non-heartbeat-LEAF failure mode applied. v1.0.19 lands the same
broadcast pattern in `RelayRouter._broadcast` plus a new unit-level
test file (`tests/test_relay_router.py`) and a topology
diagnostic (`tests/diagnose_watchdog_relay.py`). The
single-target hash-ring path remains untouched for ANALYZE; the
broadcast skips the `LoadMetric` (heartbeats and metadata fanouts
aren't in-flight in the load sense and would leak counts via the
N-1 upstreams whose responses drop at the read loop's "no
callback" branch).

Both routers are now compliant. A new multi-upstream
`BackendRouter` author should treat this contract as a checklist
entry rather than a discovery — `SelectorRouter._broadcast` and
`RelayRouter._broadcast` are the reference shapes; the
audit-checklist below names the dispatch-matrix entries to
inspect.

When introducing a new multi-upstream `BackendRouter`, audit the
`dispatch()` matrix against the heartbeat contract before shipping:

  1. For each action the router handles, identify whether it has
     a single-target or fanout semantic at the protocol level.
     `QUERY_VERSION` is structurally fanout under the keep-alive
     contract above; `ANALYZE` is per-query single-target;
     `TERMINATE_ALL` is fanout by name; `CLEAR_CACHE` depends on
     whether the upstream caches are independent (per-LEAF KataGo
     subprocesses are independent → fanout); `TERMINATE` is
     single-target by remembered routing.
  2. For each fanout action, broadcast to every healthy upstream;
     first response wins (subsequent responses for the same
     canonical drop at the read loop's "no callback" branch);
     per-upstream send failures log and continue. The
     `SelectorRouter._broadcast` helper (post-v1.0.18) is the
     reference shape.
  3. Add a regression test that asserts the broadcast property —
     not "routes to first healthy", which silently encodes a
     contract violation as the expected behaviour.

The companion regression-test files are:

  - SELECTOR (v1.0.18): `tests/test_selector_router.py` (unit-level
    dispatch matrix) + `tests/diagnose_watchdog_selector.py`
    (topology-level SELECTOR + N phantom LEAFs heartbeat broadcast).
  - RELAY (v1.0.19): `tests/test_relay_router.py` (unit-level
    dispatch matrix; single-target hash-ring still works,
    broadcast covers the three metadata actions, no-connected /
    per-upstream-failure / all-failing edge cases) +
    `tests/diagnose_watchdog_relay.py` (topology-level RELAY + N
    phantom upstreams heartbeat broadcast).

## The licensing boundary is load-bearing

The repository carries two licenses, by directory:

- The project root and all subdirectories **except**
  `goboard_transposition/` are public-domain under the Unlicense.
- `goboard_transposition/` is derived from KataGo and carries the
  upstream MIT License (including the vendored `nlohmann/json`
  dependency).

`NOTICE` documents the boundary. Edits must respect it: do not copy
code from `goboard_transposition/` into the Unlicense tree without
laundering the provenance, and do not introduce upstream-licensed
code into the Unlicense tree without updating `NOTICE` first. The
boundary is enforced by directory; keep the directory structure as
the authoritative witness.

## Submodule release arc

The proxy's release cadence is independent of the umbrella's. A
proxy-side change follows its own arc: branch in the proxy repo, PR
there, tag cut, then a separate umbrella-side PR bumps the submodule
pointer. Do not conflate the two — an umbrella PR that mixes a proxy
bump with umbrella-side changes obscures the proxy diff and complicates
review.

When a bug or improvement appears to require changes inside `proxy/`,
the right first step is to confirm the cross-boundary nature with
the user before opening proxy-repo work. The umbrella's
`docs/dispatch/` ledger is the appropriate place to record the
coordination if the work is substantial.

## Output structure

For substantive changes, structure the response as:

1. **Roadmap** — what's being changed and where, in two or three
   sentences. Name the architectural location (Layer 1/2/3, or the
   `AbstractProxy/` core).
2. **Invariants** — the ID-namespace contracts, the optic-shaped
   laws, the licensing boundary, or the fail-loud guarantees the
   change preserves or modifies. Name them before implementation.
3. **Pure units** — Transformers, Prisms, dataclasses, pure helpers
   in `AbstractProxy/`.
4. **Effectful units** — Middleware, router state machines, the
   subprocess/WebSocket adapters in `router.py` and `proxy_server.py`.
5. **Wiring** — `transformer_factory` and `middleware_factory`
   composition at the `ProxyServer(...)` construction site.

For trivial changes (a typo fix, a one-line bugfix), this structure
is overhead; skip it and just make the change.

## Scope boundaries

The proxy's concerns end at the KataGo analysis protocol on both
sides — the WebSocket wire on the client side, the engine subprocess
or upstream WebSocket on the backend side. The umbrella frontend
and backend are *consumers*; how they choose to project the wire
into their own domains is not the proxy's to design.

If a frontend or backend dispatch requests a wire-shape change, that
is a coordination decision made through the umbrella's dispatch
protocol — and it is also a request to extend the KataGo protocol or
to add a proxy-side transformation, which has its own design
constraints (compatibility with vanilla KataGo clients, the Unlicense
boundary). Do not unilaterally widen the wire to satisfy a consumer.

The umbrella's ADR-0003 bands do not apply inside the proxy — the
proxy is single-band by construction, sitting entirely within the
KataGo-coupled tier. The umbrella ADRs that *do* apply are ADR-0002
(fail loudly, worked example above), ADR-0004 (minimal-touch under
partial visibility), ADR-0005 (documentation discipline), and
ADR-0006 (per-file headers — the proxy uses Python module docstrings
at the top of each file as the header form).
