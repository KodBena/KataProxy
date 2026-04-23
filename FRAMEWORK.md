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
