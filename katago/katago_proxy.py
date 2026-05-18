"""
katago/katago_proxy.py — KataGo analysis engine protocol, assembled from
AbstractProxy.proxy_core.

This module contains *only* KataGo-specific definitions. It imports the
reusable abstractions and instantiates them for KataGo's wire protocol.

No JSON parsing occurs here beyond wire-level dict access. The types represent
the *structure* of KataGo messages as the proxy sees them — envelope ID,
referential fields, and completion signals. Everything else is opaque
pass-through.

Responses are modelled as a discriminated union (`AnalyzeResponse |
MetadataResponse`) reflecting the wire protocol's two structurally distinct
shapes: analyze responses carry `isDuringSearch` and `turnNumber`, metadata
responses (query_version, query_models, clear_cache ack, terminate ack,
error responses for non-analyze queries) carry neither. The parser
discriminates structurally on the presence of those keys; the bridge to
the completion-tracker abstraction lives in `response_completion_signal`.
See `docs/roadmap-response-variants.md` for the design rationale.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from typing import Any, Callable, Literal, NewType, Optional, Sequence, cast

from AbstractProxy.proxy_core import (
    ClientId,
    CompletionSignal,
    CompletionTracker,
    Envelope,
    IdMapping,
    IdPolicy,
    InternalId,
    ProxyChain,
    ProxyLink,
    ReferentialField,
    Prism,
    translate_referentials,
)

__all__ = [
    "KataGoAction",
    "KataGoQuery",
    "AnalyzeResponse",
    "MetadataResponse",
    "KataGoResponse",
    "response_completion_signal",
    "make_katago_link",
    "make_katago_chain",
    "parse_query_from_wire",
    "parse_response_from_wire",
    "translate_query_to_wire",
    "translate_response_to_wire",
    "register_query_completion",
    "KATAGO_QUERY_PRISMS",
    "CompletionTracker",
    # Game-tree indexing brands (v1.0.22; see
    # docs/roadmap-adaptive-type-branding.md).
    "MoveIndex",
    "TurnIndex",
    "Color",
    "move_to_turn_pair",
]


# ---------------------------------------------------------------------------
# Game-tree indexing — MoveIndex / TurnIndex / Color
# ---------------------------------------------------------------------------
#
# KataGo's analysis protocol mixes two distinct integer concepts at the
# game-tree level: per-color move indices (the position of a move within
# one color's move sequence — what `extra.<color>.deltas` is keyed by)
# and per-position turn indices (the overall position number, root = 0
# — what `KataGoQuery.analyze_turns` carries). The brands below make
# them type-distinct under `mypy --strict` while staying runtime-equal
# `int`s. The single named translation seam (`move_to_turn_pair`) owns
# the move-space → turn-space arithmetic.
#
# v1.0.22 introduces these brands and threads them through
# `middleware/adaptive_reevaluate.py`'s internal helpers. The wider
# migration (typing `KataGoQuery.analyze_turns` as `list[TurnIndex]`,
# propagating brands through `analysis_enricher` and `delta_analysis`)
# is deferred — see `docs/roadmap-adaptive-type-branding.md` §7.2.
#
# Pattern parallels the v1.0.21 identity-type branding arc
# (`AbstractProxy/proxy_core.py`'s ClientId / InternalId / CanonicalId
# / WireId); see `docs/roadmap-identity-type-branding.md` §3 for the
# full NewType semantics discussion.

MoveIndex = NewType("MoveIndex", int)
"""Per-color move index. 0-indexed within one color's move sequence.

The Nth Black move has `MoveIndex(N)`; the Nth White move has
`MoveIndex(N)`. The brand carries no color context itself — color
must be supplied alongside (e.g., via `move_to_turn_pair`'s `color`
argument) when the brand is consumed.
"""

TurnIndex = NewType("TurnIndex", int)
"""Per-position turn index. 0 = root (empty board), 1 = position after
the first move, 2 = position after the first response, and so on.

The wire field `KataGoQuery.analyze_turns` carries `TurnIndex` values
post-migration. v1.0.22 keeps the wire-types field declared as
`Optional[list[int]]` (see roadmap §7.2); the brand discipline lives
in `adaptive_reevaluate`'s internal arithmetic.
"""

Color = Literal["black", "white"]
"""Side-to-play / color of a move. A `Literal` type alias rather than
a NewType: the 2-valued domain admits the existing string literals
natively, and mypy enforces that only `"black"` or `"white"` flows
through `Color`-typed sites without requiring explicit construction
at every literal.
"""


def move_to_turn_pair(
    color: Color, m: MoveIndex,
) -> tuple[TurnIndex, TurnIndex]:
    """Translate a per-color move index to its (before, after) turn pair.

    For Black's m-th move (`MoveIndex(m)`): returns `(TurnIndex(2m), TurnIndex(2m+1))`.
    For White's m-th move (`MoveIndex(m)`): returns `(TurnIndex(2m+1), TurnIndex(2m+2))`.

    The "before" turn is the position the moving side faces; the
    "after" turn is the position resulting from playing the move.

    This is the one open-coded location for the move-space → turn-space
    arithmetic in the proxy. Every consumer that previously wrote
    `2 * t + displacement` (in `_find_worst_turns`; in v1.0.23+'s
    same-color-predecessor expansion) calls this seam.
    """
    displacement = 0 if color == "black" else 1
    t = int(m)
    return TurnIndex(2 * t + displacement), TurnIndex(2 * t + 1 + displacement)


# ---------------------------------------------------------------------------
# KataGo query action types
# ---------------------------------------------------------------------------

class KataGoAction(Enum):
    ANALYZE = auto()
    TERMINATE = auto()
    TERMINATE_ALL = auto()
    QUERY_VERSION = auto()
    QUERY_MODELS = auto()
    CLEAR_CACHE = auto()


# ---------------------------------------------------------------------------
# KataGo message types (structural, not serialization)
# ---------------------------------------------------------------------------

@dataclass
class KataGoQuery:
    """The proxy-relevant fields of a KataGo query.

    `opaque` carries everything else (rules, komi, moves, boardXSize, …)
    as a pass-through dict. The proxy never inspects it.
    """
    action: KataGoAction
    terminate_id: Optional[str] = None
    analyze_turns: Optional[list[int]] = None
    opaque: dict[str, Any] = field(default_factory=dict)


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


# A KataGo response on the wire is one of two structurally distinct
# variants discriminated by the originating action. The parser decides
# the variant from the presence/absence of `isDuringSearch`/`turnNumber`
# in the wire dict; consumers that read those fields must narrow the
# union with `isinstance` first. See `response_completion_signal` below
# for the canonical bridge from the variant to the CompletionTracker
# discriminator contract.
KataGoResponse = AnalyzeResponse | MetadataResponse


def response_completion_signal(response: KataGoResponse) -> tuple[int, bool]:
    """Translate a KataGoResponse to the (discriminator, is_partial)
    tuple that CompletionTracker.signal expects.

    Metadata responses are single-shot; the synthetic (0, False) pairs
    with the `[0]` discriminator set that `register_query_completion`
    installs for non-analyze queries. This is the one named place where
    the variant model meets the completion-tracking abstraction;
    `make_katago_removal_predicate` and the two `tracker.signal` call
    sites in `router.py` all delegate here so the bridge is spelled
    once.
    """
    if isinstance(response, AnalyzeResponse):
        return response.turn_number, response.is_during_search
    return 0, False


# ---------------------------------------------------------------------------
# ReferentialField definitions
# ---------------------------------------------------------------------------

def _with_terminate_id(q: KataGoQuery, new_id: str) -> KataGoQuery:
    """Functional update — returns a new KataGoQuery with terminate_id replaced."""
    return KataGoQuery(
        action=q.action,
        terminate_id=new_id,
        analyze_turns=q.analyze_turns,
        opaque=q.opaque,
    )


def _response_with_terminate_id(r: KataGoResponse, new_id: str) -> KataGoResponse:
    """Functional update — returns a new response with terminateId in opaque replaced.

    `dataclasses.replace` preserves the variant: an `AnalyzeResponse`
    in produces an `AnalyzeResponse` out, and likewise for
    `MetadataResponse`. In practice the terminate ack is metadata-shaped
    so this nearly always operates on `MetadataResponse`, but the
    contract is variant-preserving regardless.
    """
    new_opaque = dict(r.opaque)
    new_opaque["terminateId"] = new_id
    return replace(r, opaque=new_opaque)


TERMINATE_ID_FIELD: ReferentialField[KataGoQuery, str] = ReferentialField(
    name="terminateId",
    get=lambda q: q.terminate_id,
    set=_with_terminate_id,
)

RESPONSE_TERMINATE_ID_FIELD: ReferentialField[KataGoResponse, str] = ReferentialField(
    name="terminateId",
    get=lambda r: r.opaque.get("terminateId"),
    set=_response_with_terminate_id,
)


# ---------------------------------------------------------------------------
# ID generator
# ---------------------------------------------------------------------------

def katago_id_generator(upstream_id: ClientId) -> InternalId:
    """Mint a downstream ID that is unlinkable to the upstream ID.

    The 'kg_' prefix aids debugging without leaking upstream identity.
    The (ClientId → InternalId) signature reflects the per-session
    `ProxyLink` boundary this generator services: ClientSession's
    link translates the client's wire-namespace ids into a session-
    scoped internal namespace per `proxy/ARCHITECTURE.md` § "ID
    namespaces and translation".
    """
    return InternalId(f"kg_{uuid.uuid4().hex[:12]}")


# ---------------------------------------------------------------------------
# Completion integration
# ---------------------------------------------------------------------------

def make_katago_removal_predicate(
    tracker: CompletionTracker[InternalId, int],
) -> Callable[[InternalId, KataGoResponse], bool]:
    """Build the should_remove predicate for KataGo responses.

    Returns True only when all expected turns (analyze) or the single
    expected metadata response have arrived. Variant discrimination
    runs through `response_completion_signal` so the analyze /
    metadata bridge is spelled once.
    """
    def should_remove(downstream_id: InternalId, response: KataGoResponse) -> bool:
        disc, is_partial = response_completion_signal(response)
        sig = tracker.signal(
            query_id=downstream_id,
            discriminator=disc,
            is_partial=is_partial,
        )
        return sig == CompletionSignal.QUERY_COMPLETE

    return should_remove


# ---------------------------------------------------------------------------
# Policy assembly
# ---------------------------------------------------------------------------

def make_katago_query_policy(
    tracker: CompletionTracker[InternalId, int],
) -> IdPolicy[KataGoQuery, ClientId]:
    """Query-direction policy for KataGo.

    All query types get registered. The single referential field is
    terminateId. Queries never trigger mapping removal — only responses do.

    The `I` parameter is `ClientId` because the query-direction policy's
    referential field is read pre-translation, i.e., in the client-facing
    namespace. The lambda's `_did` parameter is annotated `ClientId` to
    match.
    """
    return IdPolicy(
        should_register=lambda _q: True,
        # Cast: TERMINATE_ID_FIELD's `I` is the raw `str` because a single
        # ReferentialField cannot encode the get-namespace / set-namespace
        # asymmetry of the translate path (the field reads ClientId
        # pre-translation and writes InternalId post-translation, or
        # vice-versa on the response side). The Phase 2 design memo
        # acknowledges this; a future field-shape refactor (per §7 future
        # work) would close the cast.
        referential_fields=cast(
            "Sequence[ReferentialField[KataGoQuery, ClientId]]",
            [TERMINATE_ID_FIELD],
        ),
        should_remove=lambda _did, _q: False,
    )


def make_katago_response_policy(
    tracker: CompletionTracker[InternalId, int],
) -> IdPolicy[KataGoResponse, InternalId]:
    """Response-direction policy for KataGo.

    The `I` parameter is `InternalId` because the response-direction
    policy's referential field is read pre-translation, i.e., in the
    session-internal namespace before being lensed back to the
    client-facing namespace. The `should_remove` predicate's first
    argument lands in this namespace too.
    """
    return IdPolicy(
        should_register=lambda _r: True,
        # Cast: see the symmetric comment in make_katago_query_policy.
        referential_fields=cast(
            "Sequence[ReferentialField[KataGoResponse, InternalId]]",
            [RESPONSE_TERMINATE_ID_FIELD],
        ),
        should_remove=make_katago_removal_predicate(tracker),
    )


# ---------------------------------------------------------------------------
# Registration hook
# ---------------------------------------------------------------------------

def register_query_completion(
    tracker: CompletionTracker[InternalId, int],
    downstream_id: InternalId,
    query: KataGoQuery,
) -> None:
    """After translating a query downstream, register its expected turns.

    Must be called for every analyze query so the tracker knows when all
    sub-tasks are done. For non-analyze queries, a single synthetic turn
    is registered so the mapping cleans up on the first (and only) response.
    """
    if query.action == KataGoAction.ANALYZE:
        turns = query.analyze_turns if query.analyze_turns is not None else [-1]
        tracker.register(downstream_id, turns)
    else:
        tracker.register(downstream_id, [0])


# ---------------------------------------------------------------------------
# Full link and chain assembly
# ---------------------------------------------------------------------------

def make_katago_link(
    tracker: Optional[CompletionTracker[InternalId, int]] = None,
) -> ProxyLink[ClientId, InternalId]:
    """Assemble a complete KataGo proxy link from reusable components.

    The `ProxyLink[ClientId, InternalId]` annotation completes Phase 2
    of the identity-type-branding migration (see
    `proxy/docs/roadmap-identity-type-branding.md`). The per-session
    link translates from the client's wire-namespace (`ClientId`,
    what the client wrote in `query.id`) to a session-scoped internal
    namespace (`InternalId`, generated by `katago_id_generator` above).
    Downstream of this link, the Hub coalesces InternalIds onto
    CanonicalIds and the routers translate CanonicalIds to WireIds;
    those translations happen in `pubsub_hub.PubSubHub` and
    `router.*Router` respectively (their own IdMapping instances,
    independent of this link's).

    The `tracker`'s type parameters reflect the same branding: it
    tracks completion per `InternalId` (the link's downstream
    namespace) with `int` discriminators (KataGo's turn numbers).
    """
    if tracker is None:
        tracker = CompletionTracker[InternalId, int]()

    mapping: IdMapping[ClientId, InternalId] = IdMapping(
        generator=katago_id_generator,
    )

    # The policy factories now return branded variants directly:
    # `IdPolicy[KataGoQuery, ClientId]` and `IdPolicy[KataGoResponse,
    # InternalId]`. The ReferentialField asymmetry (a field reads in
    # one namespace and writes in another after translation) is
    # absorbed by casts inside each factory, leaving the link
    # composition site clean.
    return ProxyLink(
        mapping=mapping,
        query_policy=make_katago_query_policy(tracker),
        response_policy=make_katago_response_policy(tracker),
    )


def make_katago_chain(depth: int = 1) -> ProxyChain[ClientId]:
    """Build a chain of `depth` independent KataGo proxy links.

    Note: `ProxyChain` is homogeneous in a single namespace `I`; the
    KataGo link is `ProxyLink[ClientId, InternalId]`, which is two
    namespaces. A multi-link KataGo chain is only well-formed for
    depth=1 at present; higher depths would require renormalising
    each link's downstream to the next link's upstream (`InternalId
    == ClientId` of the next), which is out of Phase 2's scope.
    """
    links = [make_katago_link() for _ in range(depth)]
    # Cast: see docstring — ProxyChain's single-I generic cannot express
    # the per-link two-namespace shape. Runtime behaviour is correct;
    # the typecheck approximation is the cast.
    return cast(ProxyChain[ClientId], ProxyChain(links))


# ---------------------------------------------------------------------------
# Wire-level translation
# ---------------------------------------------------------------------------

# Closed-set vocabulary of supported KataGo wire action strings. The map is
# the single source of truth: parse_query_from_wire raises on a string that
# is not a key, and the dispatch prism (_action_preview below) gates on
# membership before delegating to the parser. Adding a new action means
# adding a member to KataGoAction *and* a key here — and the type checker
# will not catch a missed half. See the module-level note above the prisms
# for why the receive-loop side has to gate-not-raise.
_KATAGO_WIRE_ACTIONS: dict[str, KataGoAction] = {
    "analyze": KataGoAction.ANALYZE,
    "terminate": KataGoAction.TERMINATE,
    "terminate_all": KataGoAction.TERMINATE_ALL,
    "query_version": KataGoAction.QUERY_VERSION,
    "query_models": KataGoAction.QUERY_MODELS,
    "clear_cache": KataGoAction.CLEAR_CACHE,
}


# Proxy-control fields that the proxy interprets but the engine must
# never see. The wire builder is the single authoritative line for the
# "never reaches KataGo" discipline; per-consumer pops elsewhere
# (pubsub_hub.subscribe pops the three cache-control flags;
# transformers/analysis_enricher.on_query pops analysis_config;
# pubsub_hub.subscribe pops capabilities post-hash) become belt-and-
# braces with this central enforcement, and adding a future proxy-only
# field is a one-line tuple extension to a single known location.
#
# Without this central strip, per-query capability gating would create
# a regression hazard: a query opting out of delta_analysis while
# still carrying analysis_config in opaque would let analysis_config
# survive past the (now-skipped) analysis_enricher.on_query and reach
# KataGo, crashing it as in the empty-board-ponder failure mode that
# v1.0.13's analysis_config-strip fix closed. See the umbrella's
# docs/dispatch/proxy-to-proxy-selector-canonical-key-near-miss.md
# (addendum, "downstream hazard" section) for the full rationale.
_PROXY_ONLY_FIELDS: frozenset[str] = frozenset({
    "cache",
    "lookup_cache",
    "replay_final_only",
    "analysis_config",
    "capabilities",
    # SELECTOR (v1.0.15): the `model` field is read by SelectorRouter
    # to choose which upstream LEAF receives the query, then stripped
    # by this central wire-strip before forwarding. Vanilla KataGo does
    # not understand the field; this discipline prevents leakage into
    # the engine regardless of which router serves the role. The field
    # is intentionally NOT popped in pubsub_hub.subscribe (unlike
    # `capabilities`), so SelectorRouter can read it from query.opaque
    # to choose the upstream; the central strip handles the wire side.
    "model",
})


def translate_query_to_wire(query: KataGoQuery, envelope_id: str) -> dict[str, Any]:
    """Serialise a KataGoQuery to a wire-format dict.

    Excludes any key in `_PROXY_ONLY_FIELDS` — those fields are
    proxy-interpreted and must never reach KataGo. See the module-level
    comment on `_PROXY_ONLY_FIELDS` for the rationale and the
    belt-and-braces relationship with per-consumer pops elsewhere.
    """
    wire: dict[str, Any] = {"id": envelope_id}
    if query.action != KataGoAction.ANALYZE:
        wire["action"] = query.action.name.lower()
    if query.terminate_id is not None:
        wire["terminateId"] = query.terminate_id
    if query.analyze_turns is not None:
        wire["analyzeTurns"] = query.analyze_turns
    wire.update(query.opaque)
    return {
        k: v for k, v in wire.items()
        if v is not None and k not in _PROXY_ONLY_FIELDS
    }


def parse_query_from_wire(wire: dict[str, Any]) -> tuple[str, KataGoQuery]:
    """Extract envelope ID and structured query from a wire-format dict.

    Per ADR-0002, an `action` key whose value is not in the closed-set
    vocabulary is a protocol violation and raises ValueError — silently
    coercing to ANALYZE was the v1.0.11-and-earlier shape that masked the
    `query_models` regression. A missing `action` key still defaults to
    ANALYZE for vanilla-KataGo wire compatibility (where the analyze
    action is implicit).

    Receive-loop callers must gate on `_KATAGO_WIRE_ACTIONS` membership
    before invoking this parser to preserve the audit-H-3
    per-connection-survive property; the prism layer below does that.
    """
    envelope_id: str = wire["id"]

    if "action" in wire:
        action_str = wire["action"]
        if action_str not in _KATAGO_WIRE_ACTIONS:
            raise ValueError(
                f"unknown KataGo wire action: {action_str!r}; "
                f"expected one of {sorted(_KATAGO_WIRE_ACTIONS)}"
            )
        action = _KATAGO_WIRE_ACTIONS[action_str]
    else:
        action = KataGoAction.ANALYZE

    known_keys = {"id", "action", "terminateId", "analyzeTurns"}
    opaque = {k: v for k, v in wire.items() if k not in known_keys}

    query = KataGoQuery(
        action=action,
        terminate_id=wire.get("terminateId"),
        analyze_turns=wire.get("analyzeTurns"),
        opaque=opaque,
    )
    return envelope_id, query


def parse_response_from_wire(wire: dict[str, Any]) -> tuple[str, KataGoResponse]:
    """Extract envelope ID and structured response from a wire-format dict.

    Discriminates the response variant structurally on the presence of
    `isDuringSearch`/`turnNumber`: both keys present → `AnalyzeResponse`,
    both absent → `MetadataResponse`. The two-fields-or-zero-fields
    invariant is load-bearing — KataGo emits both together (analyze) or
    neither (metadata, including terminate ack and error responses); a
    wire with exactly one is a structural protocol violation per
    ADR-0002 and raises ValueError. The receive-loop call sites in
    `router.py` and `proxy_server.py` wrap this in try/except so the
    raise lands in a structured ERROR log without tearing down the
    receive loop (audit-H-3 posture).
    """
    envelope_id: str = wire["id"]
    has_search = "isDuringSearch" in wire
    has_turn = "turnNumber" in wire

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


def translate_response_to_wire(
    response: KataGoResponse, envelope_id: str
) -> dict[str, Any]:
    """Reconstruct a wire-format dict from a structured response and translated ID.

    Emits `isDuringSearch`/`turnNumber` only for `AnalyzeResponse`;
    `MetadataResponse` round-trips with neither field, preserving wire
    transparency to the originating KataGo response shape.
    """
    wire: dict[str, Any] = {"id": envelope_id}
    if isinstance(response, AnalyzeResponse):
        wire["isDuringSearch"] = response.is_during_search
        wire["turnNumber"] = response.turn_number
    wire.update(response.opaque)
    return wire


# ---------------------------------------------------------------------------
# Prism definitions for the Dispatcher
# ---------------------------------------------------------------------------
#
# Each preview returns None — the Prism contract's "doesn't match this
# shape" signal — for any payload that is not a dict-with-id. The
# alternative (raise on missing structural fields) was the pre-v1.0.4
# behaviour and let a single malformed wire message tear down the receive
# loop with a stack trace; the per-connection-DoS surface noted in audit
# H-3. The Dispatcher's no-match path remains the loud surface (an
# operator-visible ERROR log in proxy_server's _handle_incoming), so
# returning None here doesn't sacrifice ADR-0002 visibility.


def _terminate_preview(d: Any) -> Optional[tuple[str, KataGoQuery]]:
    if not isinstance(d, dict) or "id" not in d:
        return None
    if d.get("action") != "terminate":
        return None
    return (d["id"], parse_query_from_wire(d)[1])


def _action_preview(d: Any) -> Optional[tuple[str, KataGoQuery]]:
    if not isinstance(d, dict) or "id" not in d:
        return None
    action = d.get("action")
    if action is None or action == "terminate":
        return None
    # Gate on the closed-set vocabulary so unknown actions fall through
    # to the dispatcher's no-match path, where proxy_server emits the
    # structured "malformed protocol message" ERROR (ADR-0002 loud
    # surface) without raising into the receive loop (audit H-3).
    if action not in _KATAGO_WIRE_ACTIONS:
        return None
    return (d["id"], parse_query_from_wire(d)[1])


def _analyze_preview(d: Any) -> Optional[tuple[str, KataGoQuery]]:
    if not isinstance(d, dict) or "id" not in d:
        return None
    if "action" in d:
        return None
    return (d["id"], parse_query_from_wire(d)[1])


TERMINATE_PRISM: Prism[dict[str, Any], KataGoQuery] = Prism(
    name="terminate",
    preview=_terminate_preview,
    review=translate_query_to_wire,
)

ACTION_PRISM: Prism[dict[str, Any], KataGoQuery] = Prism(
    name="action",
    preview=_action_preview,
    review=translate_query_to_wire,
)

ANALYZE_PRISM: Prism[dict[str, Any], KataGoQuery] = Prism(
    name="analyze",
    preview=_analyze_preview,
    review=translate_query_to_wire,
)

# Order matters: most specific match first.
KATAGO_QUERY_PRISMS: list[Prism[dict[str, Any], KataGoQuery]] = [
    TERMINATE_PRISM,
    ACTION_PRISM,
    ANALYZE_PRISM,
]
