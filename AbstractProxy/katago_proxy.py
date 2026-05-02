"""
katago_proxy.py — KataGo analysis engine protocol, assembled from proxy_core.

This module contains *only* KataGo-specific definitions. It imports the
reusable abstractions and instantiates them for KataGo's wire protocol.

No JSON parsing occurs here beyond wire-level dict access. The types represent
the *structure* of KataGo messages as the proxy sees them — envelope ID,
referential fields, and completion signals. Everything else is opaque
pass-through.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

from .proxy_core import (
    CompletionSignal,
    CompletionTracker,
    Envelope,
    IdMapping,
    IdPolicy,
    ProxyChain,
    ProxyLink,
    ReferentialField,
    Prism,
    translate_referentials,
)

__all__ = [
    "KataGoAction",
    "KataGoQuery",
    "KataGoResponse",
    "make_katago_link",
    "make_katago_chain",
    "parse_query_from_wire",
    "parse_response_from_wire",
    "translate_query_to_wire",
    "translate_response_to_wire",
    "register_query_completion",
    "KATAGO_QUERY_PRISMS",
    "CompletionTracker",
]


# ---------------------------------------------------------------------------
# KataGo query action types
# ---------------------------------------------------------------------------

class KataGoAction(Enum):
    ANALYZE = auto()
    TERMINATE = auto()
    TERMINATE_ALL = auto()
    QUERY_VERSION = auto()
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


@dataclass
class KataGoResponse:
    """The proxy-relevant fields of a KataGo response."""
    is_during_search: bool
    turn_number: int
    opaque: dict[str, Any] = field(default_factory=dict)


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
    """Functional update — returns a new KataGoResponse with terminateId in opaque replaced."""
    new_opaque = dict(r.opaque)
    new_opaque["terminateId"] = new_id
    return KataGoResponse(
        is_during_search=r.is_during_search,
        turn_number=r.turn_number,
        opaque=new_opaque,
    )


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

def katago_id_generator(upstream_id: str) -> str:
    """Mint a downstream ID that is unlinkable to the upstream ID.

    The 'kg_' prefix aids debugging without leaking upstream identity.
    """
    return f"kg_{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Completion integration
# ---------------------------------------------------------------------------

def make_katago_removal_predicate(
    tracker: CompletionTracker[str, int],
) -> Callable[[str, KataGoResponse], bool]:
    """Build the should_remove predicate for KataGo responses.

    Returns True only when all expected turns for a query have delivered
    their final (isDuringSearch=False) response.
    """
    def should_remove(downstream_id: str, response: KataGoResponse) -> bool:
        sig = tracker.signal(
            query_id=downstream_id,
            discriminator=response.turn_number,
            is_partial=response.is_during_search,
        )
        return sig == CompletionSignal.QUERY_COMPLETE

    return should_remove


# ---------------------------------------------------------------------------
# Policy assembly
# ---------------------------------------------------------------------------

def make_katago_query_policy(
    tracker: CompletionTracker[str, int],
) -> IdPolicy[KataGoQuery, str]:
    """Query-direction policy for KataGo.

    All query types get registered. The single referential field is
    terminateId. Queries never trigger mapping removal — only responses do.
    """
    return IdPolicy(
        should_register=lambda _q: True,
        referential_fields=[TERMINATE_ID_FIELD],
        should_remove=lambda _did, _q: False,
    )


def make_katago_response_policy(
    tracker: CompletionTracker[str, int],
) -> IdPolicy[KataGoResponse, str]:
    """Response-direction policy for KataGo."""
    return IdPolicy(
        should_register=lambda _r: True,
        referential_fields=[RESPONSE_TERMINATE_ID_FIELD],
        should_remove=make_katago_removal_predicate(tracker),
    )


# ---------------------------------------------------------------------------
# Registration hook
# ---------------------------------------------------------------------------

def register_query_completion(
    tracker: CompletionTracker[str, int],
    downstream_id: str,
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
    tracker: Optional[CompletionTracker[str, int]] = None,
) -> ProxyLink[str]:
    """Assemble a complete KataGo proxy link from reusable components."""
    if tracker is None:
        tracker = CompletionTracker[str, int]()

    mapping: IdMapping[str] = IdMapping(generator=katago_id_generator)

    return ProxyLink(
        mapping=mapping,
        query_policy=make_katago_query_policy(tracker),
        response_policy=make_katago_response_policy(tracker),
    )


def make_katago_chain(depth: int = 1) -> ProxyChain[str]:
    """Build a chain of `depth` independent KataGo proxy links."""
    links = [make_katago_link() for _ in range(depth)]
    return ProxyChain(links)


# ---------------------------------------------------------------------------
# Wire-level translation
# ---------------------------------------------------------------------------

def translate_query_to_wire(query: KataGoQuery, envelope_id: str) -> dict[str, Any]:
    """Serialise a KataGoQuery to a wire-format dict."""
    wire: dict[str, Any] = {"id": envelope_id}
    if query.action != KataGoAction.ANALYZE:
        wire["action"] = query.action.name.lower()
    if query.terminate_id is not None:
        wire["terminateId"] = query.terminate_id
    if query.analyze_turns is not None:
        wire["analyzeTurns"] = query.analyze_turns
    wire.update(query.opaque)
    return {k: v for k, v in wire.items() if v is not None}


def parse_query_from_wire(wire: dict[str, Any]) -> tuple[str, KataGoQuery]:
    """Extract envelope ID and structured query from a wire-format dict."""
    envelope_id: str = wire["id"]
    action_str: str = wire.get("action", "analyze")

    action_map: dict[str, KataGoAction] = {
        "analyze": KataGoAction.ANALYZE,
        "terminate": KataGoAction.TERMINATE,
        "terminate_all": KataGoAction.TERMINATE_ALL,
        "query_version": KataGoAction.QUERY_VERSION,
        "clear_cache": KataGoAction.CLEAR_CACHE,
    }
    action = action_map.get(action_str, KataGoAction.ANALYZE)

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
    """Extract envelope ID and structured response from a wire-format dict."""
    envelope_id: str = wire["id"]
    known_keys = {"id", "isDuringSearch", "turnNumber"}
    opaque = {k: v for k, v in wire.items() if k not in known_keys}

    response = KataGoResponse(
        is_during_search=wire.get("isDuringSearch", False),
        turn_number=wire.get("turnNumber", 0),
        opaque=opaque,
    )
    return envelope_id, response


def translate_response_to_wire(
    response: KataGoResponse, envelope_id: str
) -> dict[str, Any]:
    """Reconstruct a wire-format dict from a structured response and translated ID."""
    wire: dict[str, Any] = {
        "id": envelope_id,
        "isDuringSearch": response.is_during_search,
        "turnNumber": response.turn_number,
    }
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
