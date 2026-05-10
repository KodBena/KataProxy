### Modular Proxy Framework: Architectural Documentation

This documentation outlines the architecture of the modular proxy framework, its layered design, and the extension patterns for middleware and backend integration.

---

## 1. High-Level Architecture

The framework is organized into three distinct layers of abstraction. This decoupling ensures that identity management, message coalescing, and network dispatching are strictly isolated.

### Layer 1: Protocol & Identity Translation
* **Core Components:** `ProxyLink`, `Prism`, `IdMapping`, `TransformedChain`.
* **Purpose:** This layer manages the "View" of the protocol. It translates incoming client-side IDs into internal canonical IDs and back again.
* **Middleware Entry Point:** This is where `Transformers` reside. Since transformers run here, they operate on data that has already been "relabeled" into the client's namespace, making them ideal for UI-centric logic (e.g., filtering or data enrichment).

### Layer 2: The PubSub Hub (Coalescing & Fan-out)
* **Core Components:** `PubSubHub`, `CoalescingPolicy`.
* **Purpose:** The Hub acts as a traffic controller. It hashes the semantic content of queries to deduplicate work. If five clients request the same analysis, the Hub ensures only one request hits the backend, then fans out the resulting stream to all five subscribers.
* **State:** The Hub tracks "In-Flight" queries by their `content_hash`.

### Layer 3: Backend Dispatch (The Router)
* **Core Components:** `BackendRouter`, `LeafRouter`, `RelayRouter`.
* **Purpose:** The physical execution layer. It handles process management (stdin/stdout) or upstream WebSocket connections. It is entirely unaware of identity translation or client sessions.

---

## 2. Extending the Framework

### Implementing a New Protocol
To support a protocol other than KataGo, you must define the structural "Prisms" that the proxy uses to peer into the messages:
1.  **Define `ReferentialField`s:** Identify which keys in your JSON represent IDs that need translation (e.g., `id`, `parentId`, `callbackId`).
2.  **Create a `Prism`:** A prism defines how to "preview" a raw dictionary into a structured object and how to "review" it back into a dictionary for the wire.
3.  **Instantiate a `ProxyLink`:** Pass your prisms and an `IdPolicy` (which defines when an ID is considered "done" and can be purged from memory) into a new link.

### Assembling Middleware (Transformers)
Middleware is implemented via the `Transformer` class. These are bidirectional mutators:
* **`on_query`**: Modifies the request before it reaches the Hub (e.g., injecting default parameters).
* **`on_response`**: Modifies the backend output before it reaches the client (e.g., stripping noise or calculating deltas).

Transformers are **composable**. You can chain them using the `.then()` method to create a processing pipeline:
```python
# Example: Creating a specialized analysis pipeline
pipeline = (
    inject_defaults(maxVisits=1000)
    .then(min_visits_filter(20))
    .then(score_delta_calculator())
)
```

---

## 3. Caching Strategy for Online Parameter Tuning

To support **online tuning of protocol transformers**, caching must be implemented in **Layer 2 (The Hub)**. This placement allows the system to bypass the expensive backend (Layer 3) while still flowing data through the transformation logic (Layer 1).

### The "Replay" Mechanism
When a client sends a query with a control flag like `{"cached": true}`, the system follows this workflow:

1.  **Intercept & Strip:** Layer 1 identifies the `cached` flag, notes it, and strips it from the payload so it doesn't affect the `content_hash`.
2.  **Hub Lookup:** The `PubSubHub` computes the hash of the query.
3.  **Short-Circuit:**
    * If a cache hit occurs, the Hub **does not** dispatch the query to the Router.
    * The Hub initiates a **Replay Engine** task.
4.  **Multi-Turn Replay:** The Replay Engine retrieves the stored sequence of raw responses (the JSONL stream) associated with that hash.
5.  **Relabeling:** For each cached message, the Hub swaps the original "canonical ID" with the current subscriber's expected ID.
6.  **Sequence Injection:** The Hub pushes these messages into the subscriber's `asyncio.Queue` in the exact order they were originally received.

### Why this enables Tuning
Because the cache stores the **raw backend response** and injects it *below* the Transformer layer:
* The `TransformedChain.translate_upstream()` method still executes for every replayed message.
* The `Transformer.on_response()` logic runs on the cached data exactly as if it were coming from a live GPU.
* **Result:** You can modify your transformer parameters (e.g., changing a `min_visits` threshold from 10 to 50) and re-request the query with `cached: true`. The proxy will instantly "replay" the game through the new filters, allowing for immediate observation of the parameter effects without re-running the backend compute.

---

## 4. Component Interaction Map

| Component | Scope | Primary Task |
| :--- | :--- | :--- |
| **ProxyLink** | Identity | Maps client IDs to internal IDs. |
| **Transformer** | Content | Mutates payloads (downstream/upstream). |
| **PubSubHub** | Logic | Deduplicates queries; manages the Cache. |
| **Router** | Network | Manages the life cycle of the backend process. |

---

## 5. Implementation Notes for the Replay Engine

When implementing the replay in `pubsub_hub.py`, ensure the following:
* **Order Preservation:** Use a List or an ordered Document DB to store response sequences.
* **Completion Signal:** The replay must conclude by sending a `QUERY_COMPLETE` signal (or equivalent) to the subscriber's queue to ensure Layer 1 cleans up its `IdMapping`.
* **Concurrency:** Replays should run in their own `asyncio.Task` to avoid blocking the Hub's main coordination loop.

---

## 6. Orchestration Middleware (v1.0.16) — the Third Extension Surface

In addition to **Transformers** (sync per-message; §2 above) and
**SessionMiddleware** (async per-stream; described in
`ARCHITECTURE.md` and `middleware/session_middleware.py`), the
proxy provides a third extension surface:
**OrchestrationMiddleware** (`middleware/orchestration.py`).

### When to reach for it

Use OrchestrationMiddleware when the policy needs to:

* **Spawn one or more derived sub-queries** from a parent query and
  combine their responses (fork-join).
* **Express the orchestration as sequential async/await code** rather
  than as a manual state machine over per-orig_id buffers.
* **Have parent-child relationships, sub-query lifecycle, and
  cancellation cleanup** managed by the framework rather than
  reimplemented per-policy.

The canonical example is `adaptive_reevaluate` (refactored in v1.0.16
to use this surface): it observes original responses, decides whether
to deepen worst-quantile turns, and spawns a single deeper-analysis
sub-query whose responses are auto-relabelled onto the parent's
orig_id.

If the policy is purely per-message (no state, no sub-queries), use
a Transformer. If the policy needs state or async but no sub-queries
(e.g., a watchdog like `KeepAliveMiddleware`), use a plain
SessionMiddleware. If it does fork-join over sub-queries, use
OrchestrationMiddleware.

### The shape

```python
from middleware.orchestration import (
    OrchestrationContext,
    orchestration_middleware,
)

@orchestration_middleware(name="my_policy")
async def coro(parent: KataGoQuery, ctx: OrchestrationContext):
    # Iterate the parent's own responses.
    async for resp in ctx.original_stream():
        yield resp

    # Spawn a sub-query; iterate its responses (auto-relabelled
    # onto the parent's orig_id when yielded below).
    sub = build_some_derived_query(parent)
    async for resp in ctx.spawn(sub):
        yield resp

    # Or fork-join over N sub-queries:
    response_lists = await ctx.parallel(
        build_query_a(parent),
        build_query_b(parent),
    )
    yield combine(response_lists)
```

The decorator returns a *factory*; call it (`coro()`) at the
ProxyServer construction site to instantiate. Wrap with
`CapabilityGatedMiddleware` for per-query opt-in; compose with other
middlewares via `MiddlewareChain` (subject to the algebraic-laws
limit below).

### Single-orchestration-per-chain (algebraic-laws limit)

`MiddlewareChain` enforces at most one OrchestrationMiddleware per
chain. The reason is algebraic, not operational: chained
orchestration is implementable but not algebraically composable
under the coroutine substrate — `ctx.parent_query`, `ctx.spawn`, and
`ctx.original_stream` presuppose a single, well-defined parent, and
chaining two orchestrations leaves the outer's parent semantics
underdetermined (the original client query? the inner's emissions?).
Either resolution works operationally; neither composes in the
laws-shaped sense.

A future migration to a true effects system would make orchestration
composition laws-mechanical; until then, the limit is the honest
scope of the abstraction. `MiddlewareChainConfigurationError` is
raised at chain construction so the limit is explicit.

See `proxy/docs/roadmap-orchestration-middleware.md` (§*On chained
orchestration: an algebraic-laws note*) and
`proxy/docs/design-fork-join-orchestration.md` (the broader design
exploration that landed Option C, generator-style coroutines, over
Options A/B/D/E) for the full rationale.

### Configuration

* `PROXY_ORCHESTRATION_BUFFER_MAX` (default 1024) bounds the
  per-context original-stream buffer for misbehaving coroutines that
  don't iterate `ctx.original_stream()` or call
  `ctx.discard_originals()`.

---
