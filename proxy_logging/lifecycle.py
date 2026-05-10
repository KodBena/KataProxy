"""
proxy_logging/lifecycle.py — Convenience helpers for the most common
event sequences.

Each helper wraps a ProxyLogger.<level>() call with the right Event
and a sensible default `msg=`, so call sites don't repeat the same
boilerplate at every dispatch / forward / respond / etc. Keeps the
log emission code legible at the call site.

The helpers are thin: they exist for ergonomics, not for hiding the
contract. A call site that needs unusual fields can always drop to
the underlying `.info()` / `.debug()` / etc. directly.

Coverage:

  - lifecycle.connect, lifecycle.disconnect
  - lifecycle.subscribe, lifecycle.coalesce, lifecycle.cache_hit
  - lifecycle.dispatch, lifecycle.broadcast, lifecycle.no_upstream
  - lifecycle.respond, lifecycle.forward, lifecycle.complete
  - lifecycle.terminate_recv, lifecycle.terminate_synthesized,
    lifecycle.terminate_complete
  - lifecycle.keepalive_reset, lifecycle.keepalive_fired
  - lifecycle.upstream_connect, lifecycle.upstream_disconnect

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from typing import Any, Optional

from proxy_logging.adapter import ProxyLogger
from proxy_logging.enums import Direction
from proxy_logging.events import Event


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------

def connect(logger: ProxyLogger, *, peer_ip: str) -> None:
    """Client WebSocket accepted."""
    logger.info(
        Event.CONNECT,
        peer_ip=peer_ip,
        msg=f"connected from {peer_ip}",
    )


def disconnect(
    logger: ProxyLogger,
    *,
    code: Optional[int] = None,
    reason: Optional[str] = None,
) -> None:
    """Client WebSocket closed."""
    logger.info(
        Event.DISCONNECT,
        code=code if code is not None else 0,
        reason=reason or "",
        msg=f"disconnected (code={code} reason={reason!r})",
    )


# ---------------------------------------------------------------------------
# Hub-coalescing
# ---------------------------------------------------------------------------

def subscribe(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
    summary: str = "",
) -> None:
    """New subscription registered. ``summary`` is the
    summarize_query() result; included in the human-readable msg."""
    msg = f"subscribe {action}"
    if summary:
        msg = f"subscribe {summary}"
    logger.info(
        Event.SUBSCRIBE,
        cid=cid, orig=orig, action=action,
        msg=msg,
    )


def coalesce(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
    subscriber_count: int,
) -> None:
    """Joined an existing canonical."""
    logger.info(
        Event.COALESCE,
        cid=cid, orig=orig, action=action,
        subscriber_count=subscriber_count,
        msg=f"coalesced (now {subscriber_count} subscriber(s))",
    )


def cache_hit(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
    cache_key: str,
) -> None:
    """Replay-cache short-circuit."""
    logger.info(
        Event.CACHE_HIT,
        cid=cid, orig=orig, action=action, cache_key=cache_key,
        msg=f"cache hit (key={cache_key[:12]}…)",
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def dispatch(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
    upstream: Optional[str] = None,
    label: Optional[str] = None,
) -> None:
    """Single-target dispatch. Either upstream or label MUST be present
    (LeafRouter's dispatch passes neither — for LEAF the bind chain
    already carries the kg_pid context, and the implicit "the only
    upstream is this LEAF's KataGo subprocess" is the operator's
    mental model)."""
    extras: dict[str, Any] = {}
    if upstream is not None:
        extras["upstream"] = upstream
    if label is not None:
        extras["label"] = label
    target = label or upstream or "(local)"
    logger.info(
        Event.DISPATCH,
        cid=cid, orig=orig, action=action,
        direction=Direction.PROXY_TO_UPSTREAM,
        msg=f"→ {action} to {target}",
        **extras,
    )


def broadcast(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
    targets: list[str],
) -> None:
    """Fanout to every connected upstream."""
    logger.info(
        Event.BROADCAST,
        cid=cid, orig=orig, action=action,
        target_count=len(targets), targets=list(targets),
        msg=f"⤖ {action} to {len(targets)} upstream(s)",
    )


def no_upstream(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    action: str,
) -> None:
    """No healthy upstream available. ERROR level."""
    logger.error(
        Event.NO_UPSTREAM,
        cid=cid, orig=orig, action=action,
        msg=f"{action} dropped — no healthy upstream",
    )


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------

def respond(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    kind: str,
) -> None:
    """Response received from upstream. DEBUG level (high volume)."""
    logger.debug(
        Event.RESPOND,
        cid=cid, orig=orig, kind=kind,
        direction=Direction.UPSTREAM_TO_PROXY,
        msg=f"↓ {kind}",
    )


def forward(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    kind: str,
) -> None:
    """Forwarded to client. DEBUG level."""
    logger.debug(
        Event.FORWARD,
        cid=cid, orig=orig, kind=kind,
        direction=Direction.FORWARD,
        msg=f"← {kind}",
    )


def complete(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    total_responses: Optional[int] = None,
    duration_ms: Optional[int] = None,
) -> None:
    """Query lifecycle ended."""
    extras: dict[str, Any] = {}
    if total_responses is not None:
        extras["total_responses"] = total_responses
    if duration_ms is not None:
        extras["duration_ms"] = duration_ms
    detail = ""
    if duration_ms is not None:
        detail = f" ({duration_ms}ms)"
    logger.info(
        Event.COMPLETE,
        cid=cid, orig=orig,
        msg=f"complete{detail}",
        **extras,
    )


# ---------------------------------------------------------------------------
# Terminate
# ---------------------------------------------------------------------------

def terminate_recv(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
) -> None:
    """Client TERMINATE received."""
    logger.info(
        Event.TERMINATE_RECV,
        cid=cid, orig=orig,
        msg="terminate ⤺",
    )


def terminate_synthesized(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
    cause: str,
) -> None:
    """Synthetic ack returned to client (coalesced / already-completed /
    no-upstream)."""
    logger.info(
        Event.TERMINATE_SYNTHESIZED,
        cid=cid, orig=orig, cause=cause,
        msg=f"terminate synthesized (cause={cause})",
    )


def terminate_complete(
    logger: ProxyLogger,
    *,
    cid: str,
    orig: str,
) -> None:
    """Terminate ack delivered."""
    logger.info(
        Event.TERMINATE_COMPLETE,
        cid=cid, orig=orig,
        msg="terminate complete",
    )


# ---------------------------------------------------------------------------
# Keep-alive
# ---------------------------------------------------------------------------

def keepalive_reset(logger: ProxyLogger, *, session: str) -> None:
    """Heartbeat observed; timer reset. DEBUG level."""
    logger.debug(
        Event.KEEPALIVE_RESET,
        session=session,
        msg="heartbeat",
    )


def keepalive_fired(
    logger: ProxyLogger,
    *,
    session: str,
    idle_seconds: float,
    terminated_cids: list[str],
    in_flight_count: Optional[int] = None,
) -> None:
    """Watchdog terminated stranded queries. WARNING level."""
    extras: dict[str, Any] = {}
    if in_flight_count is not None:
        extras["in_flight_count"] = in_flight_count
    logger.warning(
        Event.KEEPALIVE_FIRED,
        session=session,
        idle_seconds=round(idle_seconds, 2),
        terminated_cids=list(terminated_cids),
        msg=(
            f"keep-alive fired: idle={idle_seconds:.1f}s "
            f"terminated {len(terminated_cids)} query(ies)"
        ),
        **extras,
    )


# ---------------------------------------------------------------------------
# Upstream connection (RELAY / SELECTOR)
# ---------------------------------------------------------------------------

def upstream_connect(
    logger: ProxyLogger,
    *,
    upstream: Optional[str] = None,
    label: Optional[str] = None,
) -> None:
    """Connected to an upstream."""
    extras: dict[str, Any] = {}
    if upstream is not None:
        extras["upstream"] = upstream
    if label is not None:
        extras["label"] = label
    target = label or upstream or "?"
    logger.info(
        Event.UPSTREAM_CONNECT,
        msg=f"upstream connected: {target}",
        **extras,
    )


def upstream_disconnect(
    logger: ProxyLogger,
    *,
    cause: str,
    upstream: Optional[str] = None,
    label: Optional[str] = None,
) -> None:
    """Lost an upstream connection."""
    extras: dict[str, Any] = {}
    if upstream is not None:
        extras["upstream"] = upstream
    if label is not None:
        extras["label"] = label
    target = label or upstream or "?"
    logger.warning(
        Event.UPSTREAM_DISCONNECT,
        cause=cause,
        msg=f"upstream lost: {target} ({cause})",
        **extras,
    )
