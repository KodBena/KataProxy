# Roadmap — Two-sided capability negotiation (Phase 1)

A planning artifact for the proxy-side implementation of the dispatch
filed at the umbrella's
`docs/dispatch/frontend-to-proxy-selector-and-capabilities.md` (the
proxy-side sign-off lives at
`docs/dispatch/proxy-to-frontend-selector-and-capabilities-status.md`).
Written 2026-05-09 against proxy v1.0.13. Authoritative for the
`feat/capability-negotiation` branch; superseded by the v1.0.14
release notes once tagged.

This document is **scoped to the proxy submodule**. The wire shape
gains two new optional fields (a `capabilities` dict on both
`query_version` responses and analysis queries); existing wire-shape
contracts are preserved exactly.

Cross-references: `ARCHITECTURE.md` (the three-layer model and the
two extension surfaces this work modifies); `FRAMEWORK.md` §3 (the
strip-before-hash discipline for the existing proxy-control field
family, which this work extends); the umbrella's
`docs/dispatch/proxy-to-proxy-selector-canonical-key-near-miss.md`
(the addendum's two-hashes / non-uniform-strip / conditional-strip
refinements ground the implementation choices below).

---

## Why this exists

Three separate but composable problems on one shared mechanism:

1. **A pre-existing fail-loud violation.** The SPA's registry has a
   "use transposition" toggle that assumes the proxy has
   `goboard_transposition` compiled and the `transposition_enricher`
   Transformer wired. There is no probe verifying the assumption;
   when the module is missing the toggle silently controls nothing.
   That is exactly the silent-fallback failure mode ADR-0002 forbids.
2. **The transposition_enricher pays an unconditional cost.** When
   the Transformer is wired it runs on every analyze packet,
   crossing the Python↔C++ boundary whether the user has the toggle
   on or off. Architectural hygiene: the proxy should do work in
   proportion to what's asked.
3. **`adaptive_reevaluate`'s mid-turn follow-ups break review-session
   timing on shared connections.** The SPA's review-session queries
   are turn-locked; range-based analysis queries are not. Today both
   share the same connection and the same uniformly-engaged
   middleware chain, which means adaptive_reevaluate's deeper
   queries (the dominant GPU cost on long games for weaker players)
   fire on review-session queries where they corrupt timing.

The unifying mechanism — a two-sided capability-negotiation protocol
where the server advertises what it *can* do and the client opts in
per-query for what it *wants done* — closes all three. Phase 1 ships
the protocol plus initial capabilities (`delta_analysis`,
`transposition`, `adaptive_reevaluate`).

---

## Scope

This roadmap covers **Phase 1 only**. Phase 2 + 3 (SELECTOR role and
the `selector` capability advertisement) ship as a subsequent arc on
a separate branch; they depend on Phase 1's capability advertisement
machinery but are otherwise architecturally independent.

The dispatch's open questions were answered in the status reply:
legacy auto-engage when no `capabilities` field present; metadata
schema formalised per capability (Phase 1 defines the
`adaptive_reevaluate` schema); startup-time advertisement.

---

## Architectural location

Phase 1 touches all three layers but the spine work is concentrated
in Layers 1 and 2:

- **Layer 2 — `pubsub_hub.py`'s `CoalescingPolicy`.** Add
  `"capabilities"` to `capturing_fields`. Pop `capabilities` from
  opaque in `subscribe()` after both hashes are computed.
- **Layer 1 wire emission — `katago/katago_proxy.py`'s
  `translate_query_to_wire`.** Centralise the "never reaches KataGo"
  discipline in a closed `_PROXY_ONLY_FIELDS` frozenset.
- **Layer 1 extension surfaces — `AbstractProxy/protocol_transformer.py`
  and `middleware/session_middleware.py`.** Two new wrapper types:
  `CapabilityGatedTransformer` (factory-shaped) and
  `CapabilityGatedMiddleware`.
- **Layer 1 wired chains — `transformers/analysis_enricher.py`,
  `transformers/transposition_enricher.py`,
  `middleware/adaptive_reevaluate.py`.** Wrapped at composition time;
  `adaptive_reevaluate` shifts its two parameters (`worst_quantile`,
  `extra_visits`) from constructor-time to per-orig_id reads from
  capability metadata.
- **Layer 1 advertisement — new module
  `transformers/capabilities_advertiser.py`.** A new always-on
  Transformer that adds `capabilities` to `query_version` responses;
  the advertised set is constructed at server startup based on what
  is wired.
- **Wiring — `proxy_server.py`'s `_main` and `_make_middleware`.**
  Compose the gates and the advertiser into the existing chain.

---

## Invariants

**Preserved:**

- ID-namespace contracts (`client_id → internal_id → canonical_id →
  wire_id`). The new wrappers and the metadata field never observe
  internal IDs.
- The optic-shaped `Prism` / `ReferentialField` laws. `capabilities`
  lives in opaque, not as a typed field on `KataGoQuery`; no new
  `ReferentialField` (capabilities is not an ID, not translated
  across namespaces).
- Coalescing-transparent terminate. Untouched.
- Orphan-canonical cleanup. Untouched.
- Fail-loud (ADR-0002): the wrappers do not swallow errors; the
  wrapped transformer / middleware surface their own errors as
  today.
- Wire compatibility in both directions: legacy clients (no
  `capabilities` field) get today's auto-engage-when-wired
  behaviour; new clients connecting to legacy proxies see no
  `capabilities` advertisement and fall through to legacy.

**Modified:**

- The CoalescingPolicy's `content_hash` now includes `capabilities`.
  Two queries differing only in their opt-in set are different
  canonicals. New invariant: capability opt-in is part of query
  identity for coalescing purposes.
- The "never reaches KataGo" discipline is now expressed centrally
  in `translate_query_to_wire` rather than scattered across
  consumer pops. Pre-existing pops (the three control flags in
  `subscribe()`, `analysis_config` in `analysis_enricher.on_query`)
  become belt-and-braces with the central enforcement; legacy
  semantics unchanged, but the central strip is the new
  authoritative line.
- The Transformer / Middleware factory composition gains a wrapping
  idiom. Composition semantics unchanged; the wrappers preserve the
  `Transformer` and `SessionMiddleware` contracts exactly.

---

## Pure units

### CoalescingPolicy extension (`pubsub_hub.py`)

Add `"capabilities"` to `CoalescingPolicy.capturing_fields`. The
existing per-field opt-in machinery already handles per-field
inclusion via `query.opaque.get(field_name, None)`; the dict-shape
`capabilities` value serializes deterministically under the policy's
existing `json.dumps(fields, sort_keys=True, default=str)`.

### Wire-strip set (`katago/katago_proxy.py`)

Introduce module-level `_PROXY_ONLY_FIELDS` frozenset:

```python
_PROXY_ONLY_FIELDS = frozenset({
    "cache",
    "lookup_cache",
    "replay_final_only",
    "analysis_config",
    "capabilities",
})
```

Extend `translate_query_to_wire` to filter the final dict against
this set before returning. Strictly additive: no existing call site
loses information, every existing pre-pop becomes redundant-but-safe.
Adding a future proxy-only field becomes a one-line tuple extension
to a single known location instead of a search-the-codebase
exercise.

### CapabilityGatedTransformer (`AbstractProxy/protocol_transformer.py`)

Factory-shaped wrapper. Per-eid `engaged: dict[str, dict]` state
recording the capability metadata for each query that opted in.

```python
def capability_gate(
    name: str,
    wrapped_factory: Callable[[ProxyLink], Transformer],
) -> Callable[[ProxyLink], Transformer]:
    def factory(link: ProxyLink) -> Transformer:
        wrapped = wrapped_factory(link)
        engaged: dict[str, dict] = {}

        def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
            opaque_caps = q.opaque.get("capabilities")
            # Legacy auto-engage when capabilities field absent.
            if opaque_caps is None:
                engaged[eid] = {}
                return wrapped.on_query(eid, q)
            # Explicit opt-in when name present.
            if isinstance(opaque_caps, dict) and name in opaque_caps:
                metadata = opaque_caps[name] if isinstance(opaque_caps[name], dict) else {}
                engaged[eid] = metadata
                return wrapped.on_query(eid, q)
            # Otherwise skip.
            return q

        def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
            if eid in engaged:
                result = wrapped.on_response(eid, r)
            else:
                result = r
            if link.mapping.forward(eid) is None:
                engaged.pop(eid, None)
            return result

        return Transformer(
            name=f"gated:{name}:{wrapped.name}",
            on_query=on_query,
            on_response=on_response,
        )
    return factory
```

Cleanup mirrors the existing per-transformer pattern in
`analysis_enricher` and `transposition_enricher` (`request_cache.pop(eid, None)`
when `link.mapping.forward(eid) is None`).

### CapabilityGatedMiddleware (`middleware/session_middleware.py`)

Same logic applied to the middleware abstraction. `on_query`,
`handle_response`, `on_session_start`, `on_session_end` all
delegated to the wrapped middleware; `handle_response` short-circuits
to a single passthrough yield when not engaged for the orig_id.
Cleanup of per-orig_id state on `on_session_end`.

```python
class CapabilityGatedMiddleware(SessionMiddleware):
    def __init__(self, capability: str, wrapped: SessionMiddleware) -> None:
        self._capability = capability
        self._wrapped = wrapped
        self._engaged: dict[str, dict] = {}

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self._wrapped.on_session_start(caps)

    def on_session_end(self) -> None:
        self._wrapped.on_session_end()
        self._engaged.clear()

    def on_query(self, orig_id: str, query: KataGoQuery) -> None:
        opaque_caps = query.opaque.get("capabilities")
        if opaque_caps is None:
            self._engaged[orig_id] = {}
            self._wrapped.on_query(orig_id, query)
            return
        if isinstance(opaque_caps, dict) and self._capability in opaque_caps:
            md = opaque_caps[self._capability]
            self._engaged[orig_id] = md if isinstance(md, dict) else {}
            self._wrapped.on_query(orig_id, query)

    async def handle_response(
        self,
        orig_id: str,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        if orig_id in self._engaged:
            async for out_id, out_resp in self._wrapped.handle_response(
                orig_id, response, submit_query
            ):
                yield out_id, out_resp
        else:
            yield orig_id, response
```

The synthetic-id case for `adaptive_reevaluate` (deeper queries
submitted via `submit_query`) inherits engagement naturally:
`_build_deeper_query` copies the original opaque (`new_opaque =
dict(orig.opaque)`), so the synthetic query's `capabilities` matches
the original's, and the wrapper engages on the synthetic id too.

---

## Effectful units

### `adaptive_reevaluate` parameter shift

Two parameters move from constructor-time fields to per-orig_id
state read from capability metadata:

```python
self._per_query_quantile: dict[str, float] = {}
self._per_query_extra_visits: dict[str, int] = {}
```

`on_query` reads `(query.opaque.get('capabilities') or {}).get('adaptive_reevaluate') or {}`
and stashes per-orig_id values, falling back to constructor-time
defaults when a key is absent:

```python
def on_query(self, orig_id: str, query: KataGoQuery) -> None:
    if _is_synthetic(orig_id):
        return
    if query.action != KataGoAction.ANALYZE:
        return

    cap_meta = (query.opaque.get('capabilities') or {}).get('adaptive_reevaluate') or {}
    self._per_query_quantile[orig_id] = cap_meta.get('worst_quantile', self._worst_quantile)
    self._per_query_extra_visits[orig_id] = cap_meta.get('extra_visits', self._extra_visits)

    # ... existing turn registration ...
```

`_find_worst_turns` and `_build_deeper_query` consult per-orig_id
state. Cleanup mirrors the existing `_buffered.pop(orig_id)` site.

`extra_visits` stays an *increment*, not an absolute. The deeper
query's `maxVisits = original_maxVisits + extra_visits` so KataGo's
NN cache continues the search from where the original left off
rather than restarting; switching to absolute target visits would
surprise users about what they actually get when the cache picks up
mid-search.

`window_size` (currently constructor-time, default 3) stays
proxy-side. Easy to add to the schema later if demand surfaces;
preserves a small surface for Phase 1.

### `query_version_capabilities_advertiser` (new module: `transformers/capabilities_advertiser.py`)

A new Transformer factory. The advertised dict is constructed at
server startup and passed to the factory. The Transformer's
`on_query` is identity (it is a response-side transformer);
`on_response` checks for `MetadataResponse` whose `opaque`
corresponds to a `query_version` reply and adds `capabilities` to it.

**Wiring is gated by `PROXY_ADVERTISE_CAPABILITIES`** (default
false); see *Operator opt-in* below for the wire-compatibility
rationale.

```python
def capabilities_advertiser(
    advertised: dict[str, dict],
) -> Callable[[ProxyLink], Transformer]:
    def factory(_link: ProxyLink) -> Transformer:
        def on_query(_eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
            return q

        def on_response(_eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
            if isinstance(r, MetadataResponse) and "version" in r.opaque:
                new_opaque = dict(r.opaque)
                new_opaque["capabilities"] = deepcopy(advertised)
                return MetadataResponse(opaque=new_opaque)
            return r

        return Transformer(
            name="capabilities_advertiser",
            on_query=on_query,
            on_response=on_response,
        )
    return factory
```

Per-query cost is one `isinstance` check and one `"version" in opaque`
check on every response that flows through; structurally negligible.
The advertisement is opt-in by Transformer presence — and the
Transformer itself is opt-in via `PROXY_ADVERTISE_CAPABILITIES`
(see *Operator opt-in* below).

---

## Wiring

`proxy_server.py:_main`:

```python
async def _main() -> None:
    chain = (
        Contextual(capability_gate("delta_analysis", analysis_enricher))
        .then(capability_gate("transposition", transposition_enricher))
    )
    if cfg.ADVERTISE_CAPABILITIES:
        advertised_caps = _build_advertised_capabilities()
        chain = chain.then(capabilities_advertiser(advertised_caps))

    server = ProxyServer(
        transformer_factory=chain,
        middleware_factory=_make_middleware,
    )
    ...
```

`_build_advertised_capabilities()` is called only when the env var
is enabled; the function itself constructs the set unconditionally
(`delta_analysis` and `adaptive_reevaluate` always; `transposition`
iff the native module is importable).

`_make_middleware`:

```python
def _make_middleware() -> SessionMiddleware:
    base = CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(worst_quantile=0.25, extra_visits=800, window_size=3),
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

`KeepAliveMiddleware` is *not* capability-gated — it is a watchdog
that should always run regardless of per-query opt-in.

`pubsub_hub.subscribe()`:

```python
# Existing pops:
cache_flag = bool(query.opaque.pop("cache", False))
lookup_cache_flag = bool(query.opaque.pop("lookup_cache", False))
replay_final_only_flag = bool(query.opaque.pop("replay_final_only", False))

# Compute hashes (capabilities still in opaque so it participates):
content_hash = self._policy.query_hash(query)
cache_key = self._compute_cache_key(query)

# NEW: strip capabilities after both hashes are computed.
query.opaque.pop("capabilities", None)
```

---

## Operator opt-in

KataProxy is in live use beyond the LengYue umbrella — Go schools,
online services, research groups sharing analysis machines (per the
README). The `query_version` advertisement is the one wire-shape
extension Phase 1 introduces, and even though it is strictly
additive (a new key in the response opaque), any client that
strictly validates `query_version`'s schema would see a breaking
change. To keep a v1.0.13 → v1.0.14 update fully wire-safe by
default, the advertiser is gated behind `PROXY_ADVERTISE_CAPABILITIES`.

**Default off (`false`).** `query_version` responses pass through
unchanged. Per-query capability gating still works on the proxy
side; legacy clients (no `capabilities` field) auto-engage all
wired transformers/middleware as in v1.0.13. Behaviour is byte-
identical on the wire to v1.0.13.

**Opt in (`true`/`1`/`yes`/`on`).** Operators set the env var when
they have capability-aware clients ready to engage the new contract.
The advertisement appears on `query_version` responses; the SPA
(and any other capability-aware client) feature-detects it and
sends `capabilities` per query. Legacy clients on the same
deployment continue to work unchanged via auto-engage.

The env var controls *advertisement only*, never gating. A future
release may flip the default to `true` once the wire-shape extension
has been broadly absorbed; the lifetime of the env var is
indefinite (operators may always want explicit control).

---

## Test plan

KataGo-free per the existing `tests/diagnose_phase{1,2,3}.py`
precedent. New tests:

**Unit (CoalescingPolicy)**: two queries identical except for
`capabilities` produce different `content_hash`; `capabilities: {}`
differs from absent-`capabilities`; dict-key order in `capabilities`
does not affect the hash.

**Unit (CapabilityGatedTransformer)**:
- Query without `capabilities` field → wrapped runs (legacy
  auto-engage).
- Query with `capabilities: {}` → wrapped does not run.
- Query with capability named → wrapped runs; metadata recorded.
- Cleanup: `engaged.pop` happens when `link.mapping.forward(eid)` is
  None.

**Unit (CapabilityGatedMiddleware)**: same engagement matrix; verify
response stream passes through unchanged when not engaged; verify
`submit_query` is never called from the wrapped middleware when not
engaged.

**Unit (AdaptiveReevaluate metadata)**:
- Per-orig_id `worst_quantile` overrides default.
- Per-orig_id `extra_visits` overrides default.
- Absent metadata uses constructor defaults.
- Synthetic-id deeper queries inherit the parent's metadata via the
  `dict(orig.opaque)` copy.

**Unit (`translate_query_to_wire`)**: every field in
`_PROXY_ONLY_FIELDS` is excluded from the emitted wire, including
when those fields were never popped by upstream consumers.

**Integration (KataGo-free, via `SyntheticPonderingRouter`)**:
- Query with `capabilities: {transposition: {}}` reaches
  `transposition_enricher` but not `analysis_enricher`.
- Query with `capabilities: {}` reaches neither.
- Query with no `capabilities` field reaches both (legacy).
- `query_version` response includes `capabilities` advertisement
  matching what's wired.
- Two queries with different `capabilities` opt-in sets get distinct
  canonical_ids (do not coalesce).

---

## Out of scope (Phase 2 + 3)

The SELECTOR role and the `selector` capability advertisement ship
on a separate branch (`feat/selector-router`) after Phase 1 is
tagged. Phase 2 + 3 work depends on Phase 1's capability
advertisement machinery being in place but is otherwise
architecturally independent — a new `BackendRouter` peer in
`router.py`, plus `_PROXY_ONLY_FIELDS` extended with `model`, plus
the `selector` entry in the advertisement when in SELECTOR role.
The dispatch's open question 2 (label sourcing for SELECTOR) and
question 3 (per-upstream failure budget) settle in that arc.

---

## Release

Tag: `v1.0.14` (minor bump per the keep-alive arc precedent of
v1.0.7 through v1.0.11). PR in the proxy repo against `main`;
separate umbrella-side PR bumps the submodule pointer.

— end roadmap —
