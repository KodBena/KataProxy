"""
middleware/keep_alive.py — Per-session inactivity watchdog as a
SessionMiddleware.

KeepAliveMiddleware catches the case v1.0.7's orphan-cleanup cannot:
when the WebSocket stays nominally open but the client is silent.
Concrete scenarios:

  - Frontend HMR-orphaned singleton holds an old WebSocket via a dead
    closure; the proxy never sees a disconnect.
  - A network freeze wedges the connection without ever sending TCP
    RST; the proxy's recv loop blocks indefinitely.
  - A frontend bug stops sending heartbeats without disconnecting.

In all of these, `_cleanup` never runs, and the canonical query keeps
computing for a client that no longer exists. The watchdog is the
safety net.

Mechanism
─────────
The middleware tracks the most recent timestamp at which a configurable
heartbeat query (default: `action == query_version`) was observed on
the session. A session-scoped asyncio task wakes every `check_interval`
and compares now − last_heartbeat against `idle_timeout`. On exceedance,
it terminates every ANALYZE query currently in-flight on this session
via the `terminate_query` capability (which routes through the
coalescing-aware `_handle_terminate` from v1.0.8 — middleware-initiated
terminations respect coalescing transparency without extra work).

The watchdog logs at WARNING per ADR-0002 when it fires; operators get
visibility into how often the safety net triggers.

Lifecycle integration
─────────────────────
Implemented via the lifecycle hooks added to SessionMiddleware in
v1.0.10:

  - `on_session_start(caps)`: stash capabilities, spawn the watchdog task.
  - `on_query(orig_id, q)`: if heartbeat → reset; if ANALYZE → track.
  - `handle_response(...)`: on the final response, untrack.
  - `on_session_end()`: cancel the watchdog task.

Out-of-scope asymmetry: queries injected by other middleware (e.g.,
AdaptiveReevaluateMiddleware's deeper-analysis follow-ups) bypass
`_handle_incoming` and so are invisible to KeepAliveMiddleware.on_query.
This is a pre-existing asymmetry in the SessionMiddleware contract,
flagged as a separate concern in the keep-alive dispatch.
"""

from __future__ import annotations

import asyncio
import logging
from time import monotonic
from typing import Callable, Optional

from AbstractProxy.proxy_core import ClientId
from katago import (
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
)
from middleware.session_middleware import (
    ResponseStream,
    SessionCapabilities,
    SessionMiddleware,
    SubmitQuery,
)
from proxy_logging import Event, get_proxy_logger, lifecycle


logger = logging.getLogger("kataproxy." + __name__)


__all__ = ["KeepAliveMiddleware"]


def _is_query_version(query: KataGoQuery) -> bool:
    """Default heartbeat predicate: gogui's `query_version` action."""
    return query.action == KataGoAction.QUERY_VERSION


class KeepAliveMiddleware(SessionMiddleware):
    """Per-session inactivity watchdog. See module docstring."""

    def __init__(
        self,
        *,
        idle_timeout_seconds: float,
        check_interval_seconds: Optional[float] = None,
        is_heartbeat: Optional[Callable[[KataGoQuery], bool]] = None,
    ) -> None:
        if idle_timeout_seconds <= 0:
            # The factory should omit this middleware when disabled, but
            # guard against direct construction with a degenerate value.
            raise ValueError(
                "idle_timeout_seconds must be positive; "
                "set KEEP_ALIVE_IDLE_TIMEOUT_SECONDS<=0 in the env to "
                "disable keep-alive entirely."
            )
        self._idle_timeout = idle_timeout_seconds
        self._check_interval = (
            check_interval_seconds
            if check_interval_seconds is not None
            else max(0.5, idle_timeout_seconds / 5)
        )
        self._is_heartbeat = is_heartbeat or _is_query_version
        self._last_heartbeat: float = monotonic()
        self._in_flight: set[ClientId] = set()
        self._caps: Optional[SessionCapabilities] = None
        self._task: Optional[asyncio.Task] = None
        # Structured-logging adapter. Falls back to a module-bare
        # ProxyLogger if the SessionCapabilities didn't carry one
        # (test harnesses, diagnose scripts that bypass ClientSession).
        # on_session_start refines via .bind() once the session_log is
        # in hand.
        self._log = get_proxy_logger("kataproxy.middleware.keep_alive")

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self._caps = caps
        # If the session passed a session-bound proxy_log, adopt it
        # so heartbeat / watchdog records carry session context. The
        # ClientSession.__init__ → SessionCapabilities path always
        # supplies one in production; the fallback covers
        # diagnose-script direct construction.
        if caps.proxy_log is not None:
            self._log = caps.proxy_log
        self._last_heartbeat = monotonic()
        self._task = asyncio.create_task(
            self._watchdog(), name="keep-alive-watchdog"
        )

    def on_session_end(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()

    # ------------------------------------------------------------------
    # Per-message hooks
    # ------------------------------------------------------------------

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        if self._is_heartbeat(query):
            self._last_heartbeat = monotonic()
            # Lifecycle: heartbeat observed; timer reset. DEBUG-level
            # because this is per-tick high-volume. session field
            # comes from the bind chain on self._log.
            try:
                # Read session from the bound context if present so
                # the schema's required field is satisfied; fall back
                # to "?" for diagnose-script direct constructions
                # that don't bind session.
                session = self._log._bound.get("session", "?")
                self._log.debug(
                    Event.KEEPALIVE_RESET,
                    session=session,
                    msg="heartbeat",
                )
            except Exception:
                # The lifecycle event is diagnostic-only; never let
                # a logging failure interfere with the actual reset.
                pass
            return
        if query.action == KataGoAction.ANALYZE:
            self._in_flight.add(orig_id)

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: SubmitQuery,
    ) -> ResponseStream:
        # Final-of-turn discards. is_during_search=False on a turn means
        # "this turn is done"; for a single-turn analyze that is also the
        # signal that the whole orig_id is done. AdaptiveReevaluateMiddleware
        # patches is_during_search=True on finals it intends to re-analyze,
        # so a False here is genuinely the last response for this orig_id.
        # Metadata responses (query_version, query_models, terminate ack)
        # never enter `_in_flight` (only analyze queries are tracked there
        # — see on_query above), so the discard is structurally limited
        # to AnalyzeResponse.
        if isinstance(response, AnalyzeResponse) and not response.is_during_search:
            self._in_flight.discard(orig_id)
        yield orig_id, response

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    async def _watchdog(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._check_interval)
                idle = monotonic() - self._last_heartbeat
                if idle <= self._idle_timeout:
                    continue
                stranded = list(self._in_flight)
                if not stranded:
                    # Nothing to terminate; reset to suppress log spam on a
                    # genuinely-idle session that's not stranding compute.
                    self._last_heartbeat = monotonic()
                    continue
                session = self._log._bound.get("session", "?")
                lifecycle.keepalive_fired(
                    self._log,
                    session=session,
                    idle_seconds=idle,
                    terminated_cids=list(stranded),
                    in_flight_count=len(stranded),
                )
                assert self._caps is not None  # set in on_session_start
                for orig_id in stranded:
                    try:
                        await self._caps.terminate_query(orig_id)
                    except Exception:
                        self._log.exception(
                            Event.DIAGNOSTIC,
                            msg=f"keep-alive terminate failed: orig_id={orig_id!r}",
                        )
                self._in_flight.clear()
                self._last_heartbeat = monotonic()
        except asyncio.CancelledError:
            raise
