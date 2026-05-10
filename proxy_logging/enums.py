"""
proxy_logging/enums.py — Closed enums and the LogContractError type.

Three small enums (Role, Direction) plus a single exception class
(LogContractError) live here. They're the smallest typed surface
the rest of the package builds on. The Event enum is in a sibling
module (events.py) because it's substantially larger and carries
its own per-event field schemas.

Role: which kind of proxy process is emitting the record. PROXY_ROLE
env var maps to one of these at startup; the LeafRouter, RelayRouter,
SelectorRouter, EchoRouter, and RedirectSession constructors set
it on their session-scoped loggers via .bind().

Direction: which way a wire frame crossed when the event fired.
The five values are named for their endpoints; aggregator queries
filter on direction= reliably.

LogContractError: raised at the call site when ProxyLogger.log() is
asked to emit a record that violates the per-event contract — an
unknown event, a missing required field, a wrongly-typed field.
It's a fail-loudly response per ADR-0002: an institutional
operator depending on the schema gets an immediate, located
exception rather than a silently-malformed record that breaks
their dashboard six months later.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import enum


class Role(str, enum.Enum):
    """Proxy process role; closed set."""

    LEAF = "LEAF"
    RELAY = "RELAY"
    SELECTOR = "SELECTOR"
    ECHO = "ECHO"
    REDIRECT = "REDIRECT"

    def __str__(self) -> str:
        # `str(Role.LEAF)` returns "LEAF", not "Role.LEAF". Keeps
        # rendering compact and matches the wire-shape value.
        return self.value


class Direction(str, enum.Enum):
    """Wire-frame direction at the moment the event fired.

    Five values cover every wire-crossing event in the proxy:

      RECV               — client wrote a frame to the proxy.
      FORWARD            — proxy wrote a frame to the client.
      PROXY_TO_UPSTREAM  — proxy wrote a frame to an upstream
                           (LeafRouter's subprocess stdin counts as
                           "upstream" for direction purposes;
                           RelayRouter / SelectorRouter's WebSocket
                           sends are the literal case).
      UPSTREAM_TO_PROXY  — upstream sent a frame back to the proxy.
      INTERNAL           — no wire crossing (e.g., the keep-alive
                           watchdog firing, a transformer applying,
                           an orchestration coroutine spawning).
    """

    RECV = "recv"
    FORWARD = "forward"
    PROXY_TO_UPSTREAM = "proxy→upstream"
    UPSTREAM_TO_PROXY = "upstream→proxy"
    INTERNAL = "internal"

    def __str__(self) -> str:
        return self.value


class LogContractError(Exception):
    """Raised when a ProxyLogger.log() call violates the per-event contract.

    Three failure modes:

      - Unknown event. `event=` is not a member of the Event enum
        and not a string equal to one of its values.
      - Missing required field. The event's REQUIRED_FIELDS frozenset
        names a field that's neither in the bind chain nor in the
        call-site kwargs.
      - Wrongly-typed field. (Reserved for future per-field type
        validation; not enforced in Phase 1.)

    The exception carries the call site's intended event + the set
    of fields it could see, so the operator's traceback names what
    was passed and what was missing.

    Per ADR-0002 (fail loudly): malformed records are caught at the
    emission site, not at the aggregator's parsing pipeline. The
    cost is one exception in development; the saving is no
    silent-malformed-records in production.
    """
