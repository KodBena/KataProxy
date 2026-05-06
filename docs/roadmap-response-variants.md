# Roadmap — Response variants (v1.0.13)

A planning artifact for the proxy-side refactor that splits
`KataGoResponse` into a discriminated union of two structurally
distinct variants. Written 2026-05-06 against proxy v1.0.12.
Authoritative for the `refactor/response-variants` branch; superseded
by the v1.0.13 release notes once tagged.

> **Filename note (post-v1.0.14).** This document references files by
> their pre-rename names (`baduk.py`, `bsa.py`, `reginterp.py`, `rxp/`).
> In v1.0.14 these renamed to `analysis_enricher.py`, `delta_analysis.py`,
> `registry_interpreter.py`, and `reactive_pipeline/` respectively, and
> the class `BadukAnalysisState` renamed to `DeltaAnalysisState`. The
> historical names are preserved here as the durable record of what the
> v1.0.13 work touched; `git log --follow` traces the renames.

This document is **scoped to the proxy submodule**. The wire shape
emitted to clients does not change for analyze responses; metadata
responses (the bug subject) gain transparency by no longer being
polluted with synthetic `isDuringSearch`/`turnNumber` fields. No
umbrella-side changes are required beyond the eventual pointer bump.

Cross-references: `ARCHITECTURE.md` ("Protocol abstraction leaks at
the edges"), `FRAMEWORK.md` (the Layer 1 vocabulary this change
operates within), and the umbrella's `docs/TODO.md` entry on the
silent-coercion-at-protocol-boundaries audit (this refactor is the
worked example for the response side of that pattern).

---

## Why this exists

The KataGo analysis-engine wire protocol carries **two structurally
distinct response variants**, discriminated by the originating action:

- **Analyze responses** — partial (mid-search update,
  `isDuringSearch=true`) and final (per-turn completion,
  `isDuringSearch=false`, including the last turn for queries with
  implicit `analyzeTurns`). Carry `id`, `isDuringSearch: bool`,
  `turnNumber: int`, plus payload (`moveInfos`, `rootInfo`, …).
- **Metadata responses** — `query_version`, `query_models`,
  `clear_cache` ack, `terminate` ack, error responses for
  non-analyze queries. Carry `id` plus payload only. KataGo does
  **not** include `isDuringSearch` or `turnNumber` on these.

The proxy's internal model (`AbstractProxy/katago_proxy.py:78-83` at
v1.0.12) collapses both into one shape with `is_during_search` and
`turn_number` as **required** fields. The wire reality is then bridged
with synthesis on both sides:

- `parse_response_from_wire` does `wire.get("isDuringSearch", False)`
  and `wire.get("turnNumber", 0)` — fabricating defaults for the
  metadata variant on every parse.
- `translate_response_to_wire` unconditionally writes both keys on
  the way out.

Net effect on v1.0.12: an inbound `{"id":"x","version":"1.13.0",...}`
from KataGo round-trips out to the client as
`{"id":"x","isDuringSearch":false,"turnNumber":0,"version":"1.13.0",...}`.
Wire transparency violated. The frontend's status-bar tooltip displays
the synthetic fields verbatim, which surfaced the bug.

This is the same shape as the silent-coercion bug fixed on the query
side in v1.0.12: a closed-set protocol shape parsed with an open-set
fallback (`.get(field, default)`) that fabricates values not present
on the wire, then a downstream code path emits those fabricated
values. ADR-0002 forbids the silent fabrication; the umbrella's
`docs/TODO.md` audit entry names the pattern.

`ARCHITECTURE.md` already flags the response-side conflation
indirectly under "Protocol abstraction leaks at the edges":

> The intent of `AbstractProxy/proxy_core.py` is to be
> protocol-agnostic, with all KataGo specifics confined to
> `katago_proxy.py`. The intent is mostly realised, but several
> places have KataGo assumptions baked into supposedly generic code
> (the `str` constraint on identity types, the assumption that turn
> numbers are integers).

The "turn numbers are integers" assumption baked into the response
type is what this refactor addresses on the response side. (The
remaining items — the `str` constraint, the integer assumption in
generic code — stay open after this lands.)

---

## Roadmap

The change lives entirely in `AbstractProxy/katago_proxy.py` (the
protocol-types core) plus narrow consumer adjustments. Replace the
single `KataGoResponse` dataclass with a discriminated union of
`AnalyzeResponse` and `MetadataResponse`; let every consumer narrow
the union explicitly. The wire shape for analyze responses is
unchanged. The wire shape for metadata responses gains transparency
(stops carrying fabricated keys).

The completion-tracking abstraction stays load-bearing. The policy
bridges "metadata response → synthetic `(0, False)` signal" in one
named place.

---

## Invariants

### Preserved

- **ID-namespace translation.** `RESPONSE_TERMINATE_ID_FIELD` and the
  rest of the `ProxyLink` machinery work on `KataGoResponse` (the
  union); the `set` callback uses `dataclasses.replace` to preserve
  the variant. No identity invariant changes.
- **Completion contract.** `register_query_completion` still registers
  `[turns]` for analyze and `[0]` for non-analyze. The removal
  predicate translates an `AnalyzeResponse` into
  `signal(turn_number, is_during_search)` and a `MetadataResponse`
  into `signal(0, False)` — same downstream effect, structurally
  honest input.
- **Audit-H-3 (per-connection-survive).** Receive loops in `router.py`
  and `proxy_server.py` keep their existing try/except envelopes; the
  parser's new "half-present fields" raise (see below) propagates
  exactly as the existing `wire["id"]` `KeyError` does — log-and-
  continue at the loop's outer guard.
- **Wire compatibility with vanilla KataGo.** Externally-observable
  wire bytes for analyze responses are byte-for-byte identical to
  v1.0.12. Metadata responses *lose* the synthetic
  `isDuringSearch`/`turnNumber` keys (the bug fix).

### Modified

- **Internal response shape.** One dataclass → discriminated union.
  Consumers narrow with `isinstance` (or a type-guard helper).

### Newly added

- **Half-present fields are a protocol violation.** A wire dict
  containing exactly one of `isDuringSearch`/`turnNumber` (KataGo
  would never emit this; it would be malformed) raises `ValueError`
  per ADR-0002. The two-fields-or-zero-fields invariant becomes
  explicit at the parser.

---

## The data model

```python
@dataclass(frozen=True)
class AnalyzeResponse:
    """A response to an `analyze` action.

    The wire shape carries `isDuringSearch` and `turnNumber`. Per the
    KataGo analysis-engine protocol, every analyze response — partial
    (mid-search update) and final (per-turn completion, including the
    last turn for queries with implicit `analyzeTurns`) — includes
    both fields.
    """
    is_during_search: bool
    turn_number: int
    opaque: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetadataResponse:
    """A response to a non-analyze action.

    Covers `query_version`, `query_models`, `clear_cache` ack,
    `terminate` ack, and error responses for non-analyze queries.
    KataGo does not include `isDuringSearch` or `turnNumber` on these
    wire shapes; the proxy must not synthesise them on emission.
    """
    opaque: dict[str, Any] = field(default_factory=dict)


KataGoResponse = AnalyzeResponse | MetadataResponse
```

The `frozen=True` posture matches the rest of `AbstractProxy/`'s
value objects. The variants share `opaque` rather than inheriting
from a base because the union *is* the discriminator — there's no
shared behaviour to factor out, and a base class would invite
"helper that takes the base and reads the missing fields" antibodies
the refactor exists to prevent.

---

## Pure units — parser, translator, functional update

### `parse_response_from_wire`

```python
def parse_response_from_wire(wire: dict[str, Any]) -> tuple[str, KataGoResponse]:
    envelope_id: str = wire["id"]
    has_search = "isDuringSearch" in wire
    has_turn = "turnNumber" in wire

    # Half-present is a structural protocol violation per ADR-0002.
    # KataGo emits both fields together (analyze responses) or neither
    # (metadata responses); a wire with exactly one is a KataGo bug or
    # an upstream-relay corruption and must surface, not coerce.
    if has_search != has_turn:
        raise ValueError(
            f"response wire has exactly one of "
            f"isDuringSearch/turnNumber: keys={sorted(wire.keys())}"
        )

    known_keys = {"id", "isDuringSearch", "turnNumber"}
    opaque = {k: v for k, v in wire.items() if k not in known_keys}

    response: KataGoResponse
    if has_search:
        response = AnalyzeResponse(
            is_during_search=wire["isDuringSearch"],
            turn_number=wire["turnNumber"],
            opaque=opaque,
        )
    else:
        response = MetadataResponse(opaque=opaque)
    return envelope_id, response
```

### `translate_response_to_wire`

```python
def translate_response_to_wire(
    response: KataGoResponse, envelope_id: str
) -> dict[str, Any]:
    wire: dict[str, Any] = {"id": envelope_id}
    if isinstance(response, AnalyzeResponse):
        wire["isDuringSearch"] = response.is_during_search
        wire["turnNumber"] = response.turn_number
    wire.update(response.opaque)
    return wire
```

The `isinstance` here is the *only* runtime discriminator on
emission; opaque pass-through composes for both variants. With
`mypy --strict`, the union narrows exhaustively at this site.

### `_response_with_terminate_id`

`dataclasses.replace` preserves the variant:

```python
from dataclasses import replace

def _response_with_terminate_id(r: KataGoResponse, new_id: str) -> KataGoResponse:
    new_opaque = dict(r.opaque)
    new_opaque["terminateId"] = new_id
    return replace(r, opaque=new_opaque)
```

The terminate-ack ID translation already runs against the metadata
variant in practice (terminate acks are metadata-shaped); this just
makes the type system agree.

---

## Effectful units — completion policy, transformers, middleware

### `make_katago_removal_predicate` (`katago_proxy.py`)

The single named place where the variant gets bridged to the
tracker's discriminator-based contract:

```python
def make_katago_removal_predicate(
    tracker: CompletionTracker[str, int],
) -> Callable[[str, KataGoResponse], bool]:
    def should_remove(downstream_id: str, response: KataGoResponse) -> bool:
        disc, is_partial = response_completion_signal(response)
        sig = tracker.signal(
            query_id=downstream_id,
            discriminator=disc,
            is_partial=is_partial,
        )
        return sig == CompletionSignal.QUERY_COMPLETE
    return should_remove
```

`response_completion_signal` is a small public helper (used in three
places — see below):

```python
def response_completion_signal(response: KataGoResponse) -> tuple[int, bool]:
    """Translate a KataGoResponse to the (discriminator, is_partial)
    tuple that CompletionTracker.signal expects.

    Metadata responses are single-shot; the synthetic (0, False)
    pairs with the `[0]` discriminator set that
    register_query_completion installs for non-analyze queries.
    """
    if isinstance(response, AnalyzeResponse):
        return response.turn_number, response.is_during_search
    return 0, False
```

### `router.py` direct `tracker.signal` calls

Two sites call `tracker.signal` outside the policy: `router.py:657`
(LEAF receive) and `router.py:987` (RELAY receive). Both currently
read `response.turn_number` / `response.is_during_search` directly.
Replace with `response_completion_signal(response)`. Three consumers,
one helper, the variant-to-signal bridge spelled once.

---

## Consumer migration table

Read sites that need narrowing. Most are already structurally guarded
(by `"moveInfos" in opaque` or analyze-specific predicates); the
isinstance narrowing tightens the type-check while preserving runtime
behaviour.

| File | Site | Pattern after |
|---|---|---|
| `keep_alive.py:143` | `if not response.is_during_search:` (final-of-turn discard) | `if isinstance(response, MetadataResponse) or not response.is_during_search:` — metadata responses are also "finals" for the keep-alive's purposes (any response is a non-partial heartbeat-equivalent) |
| `baduk.py:101` | `req_analyzer.push_packet(r.turn_number, ...)` gated by `"moveInfos" in r.opaque` | Tighten the gate: `if isinstance(r, AnalyzeResponse) and "moveInfos" in r.opaque:` — the `moveInfos` check was already the structural intent |
| `transposition_enricher.py:188-194` | reads `wire_dict.get("turnNumber", 0)` (operates on wire dict, not the dataclass) | No change needed — this code is already wire-level; left alone |
| `katago_transformers.py:31-32, 68-69` | `IdentityTransformer` copies `r.is_during_search`/`r.turn_number` through a fresh constructor | Use `dataclasses.replace(r, opaque=...)` (or just return `r` since it's frozen) — preserves the variant without isinstance |
| `katago_effectful.py:177, 184, 209-210` | `adaptive_reevaluate` reads/synthesises analyze fields | Add isinstance narrowing at the middleware's entry: `if isinstance(response, MetadataResponse): yield response; return` — adaptive only operates on analyze |
| `pubsub_hub.py:359` | `wire.get("isDuringSearch") is True` for replay-final-only | Wire-level, no change |
| `proxy_server.py:579, 619, 525` | parse + translate + the v1.0.8 terminate-ack synthesis | Synthesis site at `:525` constructs a `KataGoResponse(is_during_search=False, turn_number=0, opaque=...)` for the terminate ack; switch to `MetadataResponse(opaque=...)` (this is the *positive* change — synthesised wire stops carrying spurious analyze fields) |

---

## Tests

`tests/test_protocol_parser.py` extended (add to the existing file —
do not fork):

- **Parse: analyze response.** Wire with `id` + `isDuringSearch` +
  `turnNumber` + payload → `AnalyzeResponse(is_during_search=...,
  turn_number=..., opaque=payload)`.
- **Parse: metadata response.** Wire with `id` + payload only →
  `MetadataResponse(opaque=payload)`. No `isDuringSearch` or
  `turnNumber` reach `opaque`.
- **Parse: half-present raises.** Wire with `id` + only
  `isDuringSearch` (or only `turnNumber`) raises `ValueError` whose
  message names the keys.
- **Translate: analyze round-trip.** `AnalyzeResponse → wire →
  AnalyzeResponse` is identity; the wire carries both fields.
- **Translate: metadata round-trip.** `MetadataResponse → wire →
  MetadataResponse` is identity; the wire carries *neither* field.
  Regression-test pin for the v1.0.12 transparency bug.
- **Translate: query_version / query_models cases.** Concrete wire
  dicts (`{"id":"x","version":"1.13.0"}`,
  `{"id":"x","models":[...]}`) round-trip as `MetadataResponse` and
  emit identical wire bytes back. Direct regression tests for the
  user-visible bug.
- **Completion signal helper.**
  `response_completion_signal(AnalyzeResponse(...)) == (turn,
  is_partial)`;
  `response_completion_signal(MetadataResponse(...)) == (0, False)`.
  Pinned in isolation since three call sites depend on it.
- **`_response_with_terminate_id` preserves variant.** Apply to an
  `AnalyzeResponse` → still `AnalyzeResponse`; apply to a
  `MetadataResponse` → still `MetadataResponse`.
- **Vocabulary-completeness adjacent.** Existing
  `test_every_enum_member_has_a_wire_string` already pins the action
  side; add a parametrised test that asserts each non-analyze action
  parses its sample response wire as `MetadataResponse` (concrete
  sample wire dicts per action).

`tests/diagnose_phase{1,2,3}.py` — these construct wire dicts in
`synthetic_backend.py` and round-trip them through the live
router/hub/session. The current synthetic emits
`isDuringSearch`/`turnNumber` for every response (it models analyze
ponder), so they parse as `AnalyzeResponse` and the diagnostics stay
valid without changes. Verify with a clean re-run; add a synthetic
metadata-emit path only if a future diagnostic needs it.

---

## Edge cases and risks

1. **`pubsub_hub.py` cache + replay.** The replay cache stores raw
   wire (per FRAMEWORK.md §3 — "the cache stores the raw backend
   response"). Wire-level storage is invariant under the refactor.
   Worth a fresh confirming read of `pubsub_hub.py`'s cache write
   path before implementation, but the design is wire-out, wire-in
   and the variant change does not touch it.

2. **Receive-loop survival.** The new `ValueError` in
   `parse_response_from_wire` (half-present fields) could fire in
   `router.py:651` or `:981`. Confirm the existing `try/except`
   envelope around the receive-loop dispatch logs+continues rather
   than terminating the loop, mirroring the audit-H-3 query-side
   pattern. If it doesn't, wrap the parse in the receive-loop body.

3. **Coalescing-transparent terminate (v1.0.8) synthesis.**
   `_handle_terminate` synthesises a terminate-ack response for the
   multi-subscriber case (`proxy_server.py:525`-ish). Currently
   constructs a `KataGoResponse(is_during_search=False, turn_number=0,
   opaque={...})`. After the refactor, construct
   `MetadataResponse(opaque={...})` — terminate acks are
   metadata-shaped. This is the *positive* change: the synthesised
   wire stops carrying the spurious analyze fields, which is what
   ARCHITECTURE.md's terminate-ack section was implicitly assuming
   all along.

4. **`_response_with_terminate_id` and frozen dataclasses.**
   `dataclasses.replace` on a frozen dataclass returns a new instance
   of the same class — documented Python behaviour. Adding
   `frozen=True` to the variants (the v1.0.12 `KataGoResponse` is
   `@dataclass`, not `@dataclass(frozen=True)` — worth checking and
   consistency-fixing as part of the refactor) is a small adjacent
   improvement that prevents accidental mid-pipeline mutation.

5. **Mypy `--strict` impact.** The union introduces
   `Union[AnalyzeResponse, MetadataResponse]` everywhere. Most
   consumer sites narrow naturally with `isinstance`; a few may need
   `assert isinstance(response, AnalyzeResponse)` after a structural
   predicate (e.g., the `"moveInfos" in opaque` guard already implies
   analyze; mypy can't infer that without a type-guard helper). A
   small `is_analyze(r) -> TypeGuard[AnalyzeResponse]` helper in
   `katago_proxy.py` cleans this up; optional, add only if the
   isinstance noise is significant.

6. **Frontend / consumer impact.** Zero. The wire shape is unchanged
   for analyze responses, and the wire shape gains transparency for
   metadata responses (drops fields that shouldn't have been there).
   No coordination dispatch needed; no umbrella-side changes beyond
   the eventual pointer bump.

---

## Sequencing

Single proxy PR (one branch, one merge). Small enough not to warrant
phase splits but big enough that the consumer migration is the bulk
of the diff:

1. **Branch `refactor/response-variants` off `main`** (this branch).
2. **First commit — this roadmap.** Lands as `docs/roadmap-response-variants.md`.
3. **Define the variants and the helper.** `AnalyzeResponse`,
   `MetadataResponse`, `KataGoResponse = … | …`,
   `response_completion_signal`. Update the `__all__` list.
4. **Rewrite `parse_response_from_wire` and
   `translate_response_to_wire`.** Update `_response_with_terminate_id`
   to use `replace`. Migrate every `KataGoResponse(...)` constructor
   call site (now there's no constructor — must pick a variant).
5. **Update `make_katago_removal_predicate`.** Use
   `response_completion_signal`.
6. **Update `router.py:657-661, 987-991`.** Use
   `response_completion_signal`.
7. **Update consumer call sites per the migration table.** Each site
   is a small edit; commit them together since they're all the same
   shape ("narrow before reading").
8. **Add response-side tests to `tests/test_protocol_parser.py`.** All
   cases from the Tests section.
9. **Run full pytest + diagnose phases.** All green.
10. **Optional: confirm wire-level transparency manually** by spinning
    up a LEAF and tailing one connection's traffic on both sides for
    `analyze`, `query_version`, `query_models`, `terminate`.
11. **Open PR.** Description references this roadmap doc.

---

## Versioning + umbrella arc

- **Proxy v1.0.13** — `chore(release)` commit on proxy `main` after
  the refactor PR merges; tag annotation summarises "Honest
  treatment of KataGo's two response variants — eliminates the v1.0.12
  transparency bug for non-analyze responses, and addresses
  ARCHITECTURE.md's 'Protocol abstraction leaks at the edges' on the
  response side."
- **Umbrella PR `chore/bump-proxy-1.0.13`** off umbrella main —
  pointer bump only, single-line diff. Description references the
  proxy tag and the user-visible improvement (status-bar tooltip
  stops showing fabricated `isDuringSearch: false, turnNumber: 0` on
  the `query_models` response payload).

---

## Doc updates per ADR-0005

- **`ARCHITECTURE.md`** — the "Protocol abstraction leaks at the
  edges" section should be updated to say the response-side leak is
  closed in v1.0.13; the remaining items (the `str` constraint on
  identity types, integer-typed turn numbers in supposedly generic
  code) stay as open items. ADR-0005's "Revisit when…" trigger.
- **`FRAMEWORK.md`** — no change needed; the framework's vocabulary
  (Transformers, ProxyLink, PubSubHub) operates above the
  response-shape level.
- **Module docstring on `katago_proxy.py`** — add a short paragraph
  on the variant model right after the existing intro: "Responses are
  modelled as a discriminated union (`AnalyzeResponse |
  MetadataResponse`) reflecting the wire protocol's two shapes; the
  parser discriminates structurally on the presence of
  `isDuringSearch`/`turnNumber`, the bridge to the completion-tracker
  abstraction lives in `response_completion_signal`."
- **This roadmap** — survives in `docs/` after the PR merges as the
  durable record of why the variant split exists. ADR-0006-shaped
  posture: future contributors reading `katago_proxy.py` who wonder
  "why two variants?" find their answer here.

---

## Estimated diff size

Order-of-magnitude estimate (will vary):

- `katago_proxy.py`: +60 / −30 (variants + parser/translator + helper
  + docstring)
- `router.py`: ~6 small edits (helper substitutions)
- `keep_alive.py`, `baduk.py`, `katago_effectful.py`: 1–3 lines each
- `katago_transformers.py`: 4 lines (constructor → replace)
- `proxy_server.py`: ~1 line at the v1.0.8 terminate-ack synthesis
  site
- `tests/test_protocol_parser.py`: +120 lines
- `ARCHITECTURE.md`: +6 / −3
- `docs/roadmap-response-variants.md`: this file (~600 lines)

Net: ~250 lines of code moved (excluding the roadmap), scoped to one
PR, with the consumer migration table doubling as the reviewer's
checklist.
