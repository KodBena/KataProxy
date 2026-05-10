"""
proxy_logging/events.py — The Event enum and per-event required-field
schemas.

The Event vocabulary is closed and reviewable. Adding an event is a
PR; that PR also adds the event's REQUIRED_FIELDS entry. New
events are added by the same code path as new commits, so the set
stays auditable.

Each event has two sibling declarations:

  - A TypedDict in EVENT_FIELDS describing the field shape (used
    for IDE help and documentation).
  - A frozenset in EVENT_REQUIRED_FIELDS naming the keys that must
    be present at log-emission time (consumed by the runtime
    validator).

Why two declarations: TypedDict's PEP 655 Required / NotRequired
markers need Python 3.11+ (or typing_extensions). The proxy
targets `>=3.10` per pyproject.toml. The frozenset is the runtime-
enforceable source of truth; the TypedDict is documentation.

The pairing is colocated entry-by-entry in this file so a reader
inspecting one always sees the other. Tests pin both at once.

Event groups (matching §4 of proxy/docs/logging-design.md):

  4.1 Connection lifecycle    — connect, disconnect, …
  4.2 Wire-side ingress       — recv, parse, parse_error
  4.3 Hub-coalescing          — subscribe, coalesce, cache_*
  4.4 Dispatch                — dispatch, broadcast, dispatch_error,
                                no_upstream
  4.5 Response                — respond, forward, respond_dropped,
                                complete
  4.6 Terminate               — terminate_recv, terminate_dispatch,
                                terminate_synthesized,
                                terminate_complete
  4.7 Keep-alive              — keepalive_reset, keepalive_check,
                                keepalive_fired
  4.8 KataGo subprocess       — kg_spawn, kg_ready, kg_unready,
                                kg_crash, kg_respawn, kg_unhealthy
  4.9 Upstream connection     — upstream_connect, upstream_disconnect,
                                upstream_reconnect, upstream_unhealthy
  4.10 Middleware/transformer — middleware_engage, middleware_skip,
                                transformer_apply, transformer_drop,
                                orchestration_spawn, orchestration_done

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import enum
from typing import TypedDict


class Event(str, enum.Enum):
    """Closed event vocabulary. Each value is a stable wire-shape
    string consumed by aggregators. New entries land via the same
    code path as new commits.
    """

    # 4.1 Connection lifecycle
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    CONNECT_REFUSED = "connect_refused"
    RATE_LIMITED = "rate_limited"

    # 4.2 Wire-side ingress
    RECV = "recv"
    PARSE = "parse"
    PARSE_ERROR = "parse_error"

    # 4.3 Hub-coalescing
    SUBSCRIBE = "subscribe"
    COALESCE = "coalesce"
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"
    UNSUBSCRIBE = "unsubscribe"

    # 4.4 Dispatch
    DISPATCH = "dispatch"
    BROADCAST = "broadcast"
    DISPATCH_ERROR = "dispatch_error"
    NO_UPSTREAM = "no_upstream"

    # 4.5 Response
    RESPOND = "respond"
    FORWARD = "forward"
    RESPOND_DROPPED = "respond_dropped"
    COMPLETE = "complete"

    # 4.6 Terminate
    TERMINATE_RECV = "terminate_recv"
    TERMINATE_DISPATCH = "terminate_dispatch"
    TERMINATE_SYNTHESIZED = "terminate_synthesized"
    TERMINATE_COMPLETE = "terminate_complete"

    # 4.7 Keep-alive
    KEEPALIVE_RESET = "keepalive_reset"
    KEEPALIVE_CHECK = "keepalive_check"
    KEEPALIVE_FIRED = "keepalive_fired"

    # 4.8 KataGo subprocess (LEAF role)
    KG_SPAWN = "kg_spawn"
    KG_READY = "kg_ready"
    KG_UNREADY = "kg_unready"
    KG_CRASH = "kg_crash"
    KG_RESPAWN = "kg_respawn"
    KG_UNHEALTHY = "kg_unhealthy"

    # 4.9 Upstream connection (RELAY / SELECTOR roles)
    UPSTREAM_CONNECT = "upstream_connect"
    UPSTREAM_DISCONNECT = "upstream_disconnect"
    UPSTREAM_RECONNECT = "upstream_reconnect"
    UPSTREAM_UNHEALTHY = "upstream_unhealthy"

    # 4.10 Middleware and transformer
    MIDDLEWARE_ENGAGE = "middleware_engage"
    MIDDLEWARE_SKIP = "middleware_skip"
    TRANSFORMER_APPLY = "transformer_apply"
    TRANSFORMER_DROP = "transformer_drop"
    ORCHESTRATION_SPAWN = "orchestration_spawn"
    ORCHESTRATION_DONE = "orchestration_done"

    # 4.11 Catch-all for diagnostic records that don't fit a specific
    # lifecycle event. Use sparingly — preferred shape is a typed
    # event + structured fields. DIAGNOSTIC exists for warnings /
    # errors / one-off info that don't have a dedicated category
    # (e.g., transformer-internal computational failures, hub
    # cache-eviction notes). Records with this event still go
    # through the structured envelope (ts/level/role/module/msg);
    # the event is just a wildcard category.
    DIAGNOSTIC = "diagnostic"

    def __str__(self) -> str:
        return self.value


# ---------------------------------------------------------------------------
# Per-event required-field schema. Source of truth for the validator.
# ---------------------------------------------------------------------------
#
# Each entry maps an Event to the frozenset of field names that MUST
# be present in the merged (bind-chain + call-site) field dict at log-
# emission time. `role` is required on every event but is owned by
# the bind chain (set in ClientSession / router constructors); it's
# not re-listed per event.
#
# Optional fields are documented in the matching TypedDict below
# (EVENT_FIELDS) but are not enforced.

EVENT_REQUIRED_FIELDS: dict[Event, frozenset[str]] = {
    # 4.1
    Event.CONNECT: frozenset({"session", "peer_ip"}),
    Event.DISCONNECT: frozenset({"session"}),
    Event.CONNECT_REFUSED: frozenset({"session", "peer_ip", "cause"}),
    Event.RATE_LIMITED: frozenset({"session", "peer_ip"}),
    # 4.2
    Event.RECV: frozenset({"session", "raw_size_bytes"}),
    Event.PARSE: frozenset({"session", "cid", "orig", "action"}),
    Event.PARSE_ERROR: frozenset({"session", "error_kind"}),
    # 4.3
    Event.SUBSCRIBE: frozenset({"session", "cid", "orig", "action"}),
    Event.COALESCE: frozenset(
        {"session", "cid", "orig", "action", "subscriber_count"}
    ),
    Event.CACHE_HIT: frozenset({"session", "cid", "orig", "action"}),
    Event.CACHE_MISS: frozenset({"session", "cid", "orig", "action"}),
    Event.UNSUBSCRIBE: frozenset({"session", "cid", "orig", "was_last"}),
    # 4.4
    Event.DISPATCH: frozenset({"cid", "orig", "action", "direction"}),
    Event.BROADCAST: frozenset(
        {"cid", "orig", "action", "target_count"}
    ),
    Event.DISPATCH_ERROR: frozenset({"cid", "orig", "error_kind"}),
    Event.NO_UPSTREAM: frozenset({"cid", "orig", "action"}),
    # 4.5
    Event.RESPOND: frozenset({"cid", "orig", "kind", "direction"}),
    Event.FORWARD: frozenset({"cid", "orig", "kind", "direction"}),
    Event.RESPOND_DROPPED: frozenset({"cid", "orig", "cause"}),
    Event.COMPLETE: frozenset({"cid", "orig"}),
    # 4.6
    Event.TERMINATE_RECV: frozenset({"session", "cid", "orig"}),
    Event.TERMINATE_DISPATCH: frozenset({"cid", "orig", "direction"}),
    Event.TERMINATE_SYNTHESIZED: frozenset({"cid", "orig", "cause"}),
    Event.TERMINATE_COMPLETE: frozenset({"cid", "orig"}),
    # 4.7
    Event.KEEPALIVE_RESET: frozenset({"session"}),
    Event.KEEPALIVE_CHECK: frozenset(
        {"session", "idle_seconds", "in_flight_count"}
    ),
    Event.KEEPALIVE_FIRED: frozenset(
        {"session", "idle_seconds", "terminated_cids"}
    ),
    # 4.8
    Event.KG_SPAWN: frozenset({"kg_pid", "kg_cmd"}),
    Event.KG_READY: frozenset({"kg_pid", "startup_seconds"}),
    Event.KG_UNREADY: frozenset({"kg_pid", "cause"}),
    Event.KG_CRASH: frozenset({"kg_pid", "exit_code"}),
    Event.KG_RESPAWN: frozenset({"kg_pid_new", "attempt", "budget_remaining"}),
    Event.KG_UNHEALTHY: frozenset({"cause"}),
    # 4.9
    Event.UPSTREAM_CONNECT: frozenset(set()),  # bound: upstream OR label
    Event.UPSTREAM_DISCONNECT: frozenset({"cause"}),
    Event.UPSTREAM_RECONNECT: frozenset({"attempt", "delay_seconds"}),
    Event.UPSTREAM_UNHEALTHY: frozenset({"label", "budget_remaining"}),
    # 4.10
    Event.MIDDLEWARE_ENGAGE: frozenset({"cid", "orig", "middleware_name"}),
    Event.MIDDLEWARE_SKIP: frozenset(
        {"cid", "orig", "middleware_name", "cause"}
    ),
    Event.TRANSFORMER_APPLY: frozenset(
        {"cid", "orig", "transformer_name", "direction"}
    ),
    Event.TRANSFORMER_DROP: frozenset({"cid", "orig", "transformer_name"}),
    Event.ORCHESTRATION_SPAWN: frozenset({"cid", "sub_orig", "name"}),
    Event.ORCHESTRATION_DONE: frozenset({"cid", "name", "outcome"}),
    # 4.11 — DIAGNOSTIC is a catch-all; only the always-present
    # fields (role/module/ts/level/msg) are required, none of the
    # event-specific fields. Useful when a record needs the
    # structured envelope but doesn't fit a domain event.
    Event.DIAGNOSTIC: frozenset(),
}


# ---------------------------------------------------------------------------
# Per-event TypedDict shapes. Documentation aid + IDE help.
# ---------------------------------------------------------------------------
#
# These TypedDicts list every field an event may carry (required +
# optional). They're not enforced at runtime — the EVENT_REQUIRED_FIELDS
# frozenset above is the runtime contract. The TypedDicts are exposed
# for callers / reviewers who want to see "what fields does this
# event carry" without reading the validator's data.
#
# Usage from a call site:
#
#   logger.info(Event.DISPATCH, **DispatchFields(
#       cid=canonical_id, orig=orig_id, action=action.name,
#       direction=Direction.PROXY_TO_UPSTREAM, label=label,
#   ))
#
# The TypedDict's `total=False` semantics means optional fields can
# be omitted; the runtime validator catches missing required fields.

class DispatchFields(TypedDict, total=False):
    cid: str
    orig: str
    action: str
    direction: str
    upstream: str  # optional — RELAY's URL
    label: str     # optional — SELECTOR's label
    duration_ms: int


class BroadcastFields(TypedDict, total=False):
    cid: str
    orig: str
    action: str
    target_count: int
    targets: list[str]


class RespondFields(TypedDict, total=False):
    cid: str
    orig: str
    kind: str  # partial | final | metadata | error
    direction: str
    upstream: str
    label: str


class CompleteFields(TypedDict, total=False):
    cid: str
    orig: str
    total_responses: int
    duration_ms: int


class KeepaliveFiredFields(TypedDict, total=False):
    session: str
    idle_seconds: float
    terminated_cids: list[str]
    in_flight_count: int


class KgSpawnFields(TypedDict, total=False):
    kg_pid: int
    kg_cmd: str


class KgCrashFields(TypedDict, total=False):
    kg_pid: int
    exit_code: int
    stderr_tail: str


class UpstreamConnectFields(TypedDict, total=False):
    upstream: str  # RELAY's URL
    label: str     # SELECTOR's label (one of the two should be present
                   # via bind; both may be present for SELECTOR after
                   # the upstream URL is also bound)


# Full lookup so callers can `EVENT_FIELDS[Event.DISPATCH]` for help.
EVENT_FIELDS: dict[Event, type] = {
    Event.DISPATCH: DispatchFields,
    Event.BROADCAST: BroadcastFields,
    Event.RESPOND: RespondFields,
    Event.FORWARD: RespondFields,  # same shape
    Event.COMPLETE: CompleteFields,
    Event.KEEPALIVE_FIRED: KeepaliveFiredFields,
    Event.KG_SPAWN: KgSpawnFields,
    Event.KG_CRASH: KgCrashFields,
    Event.UPSTREAM_CONNECT: UpstreamConnectFields,
    # Other events use a generic TypedDict with the keys named in
    # EVENT_REQUIRED_FIELDS; the lookup falls back to a "fields are
    # str-keyed Any-valued" interpretation when an entry is absent.
}
