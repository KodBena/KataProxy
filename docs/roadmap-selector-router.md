# Roadmap — SELECTOR Router (Phase 2+3, v1.0.15)

A planning artifact for the proxy-side implementation of Phase 2+3 of
the two-sided capability-negotiation contract. Ships the SELECTOR
Layer 3 role (Ask 3 of the umbrella's
`docs/dispatch/frontend-to-proxy-selector-and-capabilities.md`) plus
the `selector` capability advertisement (Ask 2). Written 2026-05-09
against proxy v1.0.14. Authoritative for the `feat/selector-router`
branch; superseded by the v1.0.15 release notes once tagged.

This document is **scoped to the proxy submodule**. Wire-shape
additions: a new optional `model: string` field on the analysis
query (proxy-interpreted, never reaches engine, joins
`_PROXY_ONLY_FIELDS`); `query_models` response on SELECTOR
synthesises the union of configured upstream labels; `selector` joins
the advertised capability set when in SELECTOR role and
`PROXY_ADVERTISE_CAPABILITIES=true`.

Cross-references: `ARCHITECTURE.md` (Layer 3 router contracts);
`router.py` (the existing `LeafRouter` / `RelayRouter` / `EchoRouter`
peers SELECTOR slots in among); `proxy/docs/roadmap-capability-
negotiation.md` (Phase 1, which SELECTOR's advertisement extends);
the umbrella's
`docs/dispatch/proxy-to-frontend-selector-and-capabilities-status.md`
(the proxy-side sign-off naming the per-upstream failure-budget shape
and operator-declared label sourcing decisions this roadmap
implements).

---

## Why this exists

Two distinct user needs converge on the same routing primitive:

1. **Multi-network model selection.** A user with multiple KataGo
   networks running on the same machine — multiple LEAFs on multiple
   ports — has no in-app way to switch between them today. The SPA
   connects to one URL, reads `query_models` to learn what model that
   proxy is serving, and that's the totality of model knowledge.
   Editing the registry URL and reconnecting is not a model selector.

2. **Real-time model-vs-model.** Strong network reviewing weak
   network's move; two networks alternating in self-play; the
   multi-weights and LLM-at-seat policies the autonomous-SR-loop
   note sketches — all want the same primitive (route this query to
   model X, that query to model Y, on one connection) and none of
   them have it.

A new proxy role (SELECTOR) that lets clients name which upstream a
query targets, plus the `selector` capability advertisement that
gates the SPA's model-dropdown UI. This is Phase 2+3 of the
dispatch's three-phase plan; Phase 1 (capability protocol +
delta_analysis / transposition / adaptive_reevaluate gating, plus the
operator-opt-in advertisement gate) shipped as v1.0.14.

---

## Scope

This roadmap covers **Phase 2+3 only**. The `capabilities`
infrastructure, the wire-strip discipline, and the
`PROXY_ADVERTISE_CAPABILITIES` operator-opt-in gate all ship in
v1.0.14 and are inherited unchanged.

Out of scope (deferrals):

- **Per-model recovery** (bring an unhealthy model back to healthy
  without a proxy restart). The dispatch's Q3 answer was
  terminal-until-restart, mirroring `LeafRouter`'s posture; revisit
  if operational experience surfaces a need.
- **Heterogeneous load metrics on SELECTOR.** SELECTOR's invariant is
  named (distinguishable) upstreams, not a fungible pool; load
  balancing is structurally inapplicable.
- **Probing upstreams at startup for their actual model identifiers.**
  Q2's answer was operator-declared `(URL, label)` tuples; probing
  would couple SELECTOR's startup to upstream availability and fight
  ADR-0002's startup-time loud-failure posture. Operators may add
  richer per-label metadata to a future configuration shape if a
  consumer needs it.
- **Broadcast semantics for `clear_cache` / `terminate_all`.**
  SELECTOR routes them to one upstream (the first healthy) for MVP.
  Documented as a known limitation; revisit if a consumer needs true
  broadcast.

---

## Architectural location

Phase 2+3 is concentrated in Layer 3 plus thin extensions of the
Phase 1 spine:

- **Layer 3 — `router.py`.** New `SelectorRouter` class, peer to
  `LeafRouter` / `RelayRouter` / `EchoRouter`. Maintains per-label
  upstream WebSocket connections; dispatch is dictionary lookup by
  `model` field; per-upstream failure budget mirroring
  `LeafRouter._MAX_RESTARTS`; structured errors for unknown model /
  dead upstream / startup label collisions; synthesised
  `query_models` response. New `SelectorStartupError` class at
  the same register as `LeafStartupError`.
- **Layer 2 — `pubsub_hub.py`.** `model` joins
  `CoalescingPolicy.capturing_fields` (so different models route to
  different canonicals). The hub-side post-hash pop also picks up
  `model`.
- **Layer 1 wire emission — `katago/katago_proxy.py`.**
  `_PROXY_ONLY_FIELDS` gains `"model"`.
- **Layer 1 advertisement — `proxy_server.py`.** When
  `cfg.ROLE == "SELECTOR"`, `_build_advertised_capabilities` adds
  `selector: {}` to the advertised set.
- **Configuration — `sproxy_config.py`.** New `SELECTOR_MODELS`
  env var: `label1=ws://host1:port1,label2=ws://host2:port2`.
  Parsed as an ordered `tuple[tuple[str, str], ...]`; SELECTOR
  rejects empty list at startup; SELECTOR rejects duplicate labels
  at startup.
- **Factory — `make_router` in `router.py`.** New `SELECTOR` role
  branch.

---

## Invariants

**Preserved** — every Phase 1 invariant composes unchanged:

- ID-namespace contracts (`client_id → internal_id → canonical_id →
  wire_id`). The new router participates as Layer 3; the chain is
  unaffected.
- Coalescing-transparent terminate. SELECTOR's `terminate()` mirrors
  `RelayRouter.terminate()`: route to the right upstream by
  remembered label, synthesise an ack if the upstream is gone.
- Orphan-canonical cleanup. Hub's `unsubscribe` returning `bool`
  flows through unchanged; SELECTOR honours the orphan-terminate
  signal at the dispatched-to upstream.
- Wire-strip discipline. Adding `model` to `_PROXY_ONLY_FIELDS`
  keeps the central enforcement intact; `model` never reaches
  KataGo.
- `capabilities` field per-query gating from Phase 1.
- Operator opt-in for the advertisement
  (`PROXY_ADVERTISE_CAPABILITIES`). SELECTOR's `selector`
  advertisement also gates on this.
- The optic-shaped Prism / ReferentialField laws. `model` lives in
  opaque, not as a typed field on `KataGoQuery`; no new
  ReferentialField (model is not an ID, not translated across
  namespaces).
- Fail-loud (ADR-0002): unknown model raises a structured error
  response; dead upstream raises a structured error response;
  startup configuration violations (empty `SELECTOR_MODELS`,
  duplicate labels) raise `SelectorStartupError` before the server
  binds.

**Modified:**

- `model` is now part of query identity for coalescing purposes
  (mirrors the `capabilities` decision from Phase 1).
- A new role exists: `PROXY_ROLE=SELECTOR`. Distinct env-var
  configuration (`SELECTOR_MODELS` instead of `UPSTREAM_URLS`) — the
  existing `UPSTREAM_URLS` semantic stays untouched for `RELAY` /
  `REDIRECT` operators.

---

## Pure units

### CoalescingPolicy.capturing_fields gains "model" (`pubsub_hub.py`)

One-line tuple extension, alongside the Phase 1 `"capabilities"`
addition. The existing per-field opt-in machinery already handles
per-field inclusion via `query.opaque.get(field_name, None)`.

### `_PROXY_ONLY_FIELDS` gains "model" (`katago/katago_proxy.py`)

One-line frozenset extension. The wire builder is the central
authoritative line for the "never reaches KataGo" discipline;
`model` joins `cache`, `lookup_cache`, `replay_final_only`,
`analysis_config`, `capabilities`.

### Hub-side post-hash pop (`pubsub_hub.subscribe`)

`capabilities` continues to pop after both hashes (Phase 1
behaviour). `model` does **not** pop — SelectorRouter reads it from
`query.opaque` in `dispatch()` to choose the upstream, so it must
survive Layer 2. The central wire-strip in `translate_query_to_wire`
(see *Pure units → `_PROXY_ONLY_FIELDS`*) is what guarantees `model`
is excluded from the wire emitted to upstream LEAFs.

The asymmetry between `capabilities` (pops in subscribe) and `model`
(does not pop in subscribe) reflects the layer where each is
consumed: `capabilities` is consumed by Layer 1 (transformer +
middleware gates) before subscribe runs, so by the time subscribe
sees the query the field has done its job; `model` is consumed by
Layer 3 (SelectorRouter) after subscribe, so it must remain in the
opaque dict for the router to read.

### `SELECTOR_MODELS` parser (`sproxy_config.py`)

Produces `tuple[tuple[str, str], ...]` of `(label, url)` pairs from
the env var `SELECTOR_MODELS=label1=ws://host1,label2=ws://host2`.
Returns empty tuple on absent or blank env var. Trims whitespace
around each entry and around `label` / `url`. Rejects entries with
no `=` separator at parse time (raises `ValueError` with a clear
message naming the malformed entry); SELECTOR's startup gate also
rejects empty configuration and duplicate labels (see
`SelectorRouter.start`).

---

## Effectful units

### `SelectorRouter` class (`router.py`)

Standalone — does not inherit `RelayRouter`. The dispatch's Q3
framing makes the per-upstream invariant opposite to relay's
interchangeable-pool invariant; sharing a class would put the
structural distinction in dispatch branches, which is the wrong
shape per the status reply. Some connection-management code is
deliberately duplicated between the two routers; future cleanup
could extract a shared `WebSocketUpstreamConnections` helper, but
that is out of Phase 2+3 scope.

State:

```python
self._models: tuple[tuple[str, str], ...]   # ordered (label, url) pairs
self._url_for_label: dict[str, str]         # for dispatch lookup
self._connections: dict[str, Any]           # label → websocket
self._reader_tasks: dict[str, asyncio.Task] # label → reader task
self._reconnect_tasks: set[asyncio.Task]    # for stop()
self._failure_budget: dict[str, int]        # label → remaining retries
self._unhealthy_models: set[str]            # labels whose budget exhausted
self._tracker: CompletionTracker
self._callbacks: dict[str, tuple[OnResponse, OnComplete, str]]
                                            # canonical_id → (cbs, label)
```

`start()`:
- Reject empty `self._models` with `SelectorStartupError`.
- Reject duplicate labels with `SelectorStartupError` (names the
  duplicate).
- Connect to all upstreams in parallel. Initial connect failures
  decrement that label's budget; if exhausted at startup, the model
  is marked unhealthy. The server still binds — operators with one
  broken upstream out of many can still serve the others.
- Log the disposition at INFO level (which models are healthy /
  unhealthy at startup).

`dispatch(canonical_id, wire_dict, query, on_response, on_complete)`:

Action-routing matrix (different from relay's hash-ring uniform
dispatch):

- **`ANALYZE`**: read `query.opaque.get("model")`. Missing →
  structured error (`{id, error: "missing model field for SELECTOR
  routing", field: "model"}`). Unknown label → structured error
  naming the requested label and the available labels. Healthy →
  forward to that label's upstream WebSocket; record
  `_callbacks[canonical_id] = (on_response, on_complete, label)`.
  Unhealthy → structured error naming the unavailable model.
- **`TERMINATE`**: routed by the remembered label for the targeted
  canonical_id (mirrors `RelayRouter.terminate()`'s pattern).
- **`QUERY_MODELS`**: synthesised — no upstream traffic. Response
  shape: `{"id": canonical_id, "models": [{"label": l} for l, _ in
  self._models]}`. The list-of-dicts shape leaves room for future
  enrichment; the SPA uses `entry.label` as the routing key.
- **`QUERY_VERSION`**: forwarded to the first healthy upstream. The
  capabilities_advertiser at Layer 1 enriches the response on the
  way back. If no upstream is healthy, structured error.
- **`CLEAR_CACHE` / `TERMINATE_ALL`**: routed to the first healthy
  upstream as an MVP limitation. Logged at WARNING. True broadcast
  semantics are deferred (see *Out of scope*).

`terminate(canonical_id, ...)`: look up label from `_callbacks`,
route terminate to that upstream's WebSocket. Synthesise ack on
upstream gone — exact mirror of `RelayRouter.terminate()`.

`stop()`: cancel all reader and reconnect tasks; close all
connections.

Per-label connection management (`_connect`, `_read_loop`,
`_reconnect_with_backoff`): mirrors `RelayRouter`'s structure but
keys state by label (not URL), and the reconnect path checks the
failure budget before scheduling. Budget exhaustion transitions the
label to unhealthy and stops the reconnect loop.

### `SelectorStartupError`

New exception class peer to `LeafStartupError`, raised by
`SelectorRouter.start()` on configuration violations (empty
`SELECTOR_MODELS`, duplicate labels). Same register as
`LeafStartupError`: ADR-0002's startup-time loud-failure posture.

### Capability advertisement extension (`proxy_server.py`)

`_build_advertised_capabilities` adds `selector: {}` when
`cfg.ROLE == "SELECTOR"`. The advertisement still gates on
`PROXY_ADVERTISE_CAPABILITIES`; SELECTOR with the env var disabled
serves the role's traffic but does not advertise.

---

## Wiring

`make_router` factory gains a `SELECTOR` branch:

```python
if role_upper == "SELECTOR":
    return SelectorRouter(models=cfg.SELECTOR_MODELS)
```

`proxy_server.py:_main` and `_make_middleware` are unchanged
beyond what Phase 1 wired — Layer 1 (transformer/middleware chains)
is router-agnostic; SELECTOR composes with the existing
capability-gated chain transparently.

---

## Wire-compatibility posture

SELECTOR is a new role; existing operators are unaffected.

- **`model` is a new optional query field.** Old proxies (v1.0.14
  and earlier) don't parse it. If a SELECTOR-aware client sends
  `model` to a non-SELECTOR proxy, the field flows through to
  KataGo as an unknown opaque field — KataGo's analysis-engine
  protocol ignores unknown fields, so this is wire-safe.
- **`SELECTOR_MODELS` is a new dedicated env var.** Existing
  `UPSTREAM_URLS` stays untouched for `RELAY` / `REDIRECT`.
- **The `selector` advertisement only appears when**
  `cfg.ROLE == "SELECTOR"` AND `PROXY_ADVERTISE_CAPABILITIES=true`.
  A fresh-installed v1.0.15 with `PROXY_ROLE=LEAF` and default
  config is byte-identical on the wire to v1.0.14.
- **Coalescing-key inclusion of `model`** has no effect on legacy
  queries (which don't carry a `model` field; the policy reads
  `query.opaque.get("model", None)` and gets `None` uniformly).

---

## Test plan

KataGo-free per the existing precedent. New tests in
`tests/test_selector_router.py`:

**SELECTOR_MODELS parser**:
- Well-formed entries parse to `(label, url)` tuples.
- Whitespace around entries / `label` / `url` is trimmed.
- Absent env var → empty tuple.
- Malformed entry (no `=`) raises `ValueError` naming the entry.

**CoalescingPolicy with model**:
- Two queries identical except for `model` produce different
  `content_hash`.
- Absent vs. empty-string `model` differs from a populated `model`.
- Existing `capabilities`-affects-hash tests still hold.

**`_PROXY_ONLY_FIELDS` includes model**:
- `translate_query_to_wire` strips `model` from the emitted wire.

**`SelectorRouter.start`**:
- Empty `models` → `SelectorStartupError`.
- Duplicate labels → `SelectorStartupError` naming the duplicate.

**`SelectorRouter.dispatch` matrix**:
- ANALYZE without `model` → structured error.
- ANALYZE with unknown model → structured error naming the
  unavailable label and the available labels.
- ANALYZE with healthy model → forwarded to the right upstream
  (verified with mock WebSocket).
- ANALYZE with unhealthy model → structured error.
- QUERY_MODELS → synthesised response, no upstream traffic.
- QUERY_VERSION → forwarded to first healthy upstream.

**`SelectorRouter.terminate`**:
- Routes to the label remembered for the canonical_id.
- Synthesises ack on dead upstream.

**Failure budget**:
- Per-upstream connect failures decrement budget.
- Budget exhaustion marks unhealthy.
- Unhealthy queries fail loudly.
- Other upstreams continue normally.

**Capabilities advertiser includes `selector`**:
- When `cfg.ROLE == "SELECTOR"` and advertisement enabled,
  `selector: {}` appears in the advertisement.
- Other roles do not advertise `selector`.

---

## Release

Tag: `v1.0.15` (minor bump per the keep-alive arc and Phase 1
precedent). PR in the proxy repo against `main`; separate
umbrella-side pointer-bump PR follows.

— end roadmap —
