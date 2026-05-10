"""
router.py — Layer 3: Backend dispatch and load balancing.

The router receives queries already in the proxy's internal namespace
(canonical_id) and delivers results back via two async callbacks:

  on_response(canonical_id, wire_dict)  — called once per response message
                                          (partial and final alike)
  on_complete(canonical_id)             — called exactly once, after the
                                          last final response for a query

The router is entirely unaware of clients, original IDs, and coalescing.
It is handed canonical_ids and gives them back.

Roles
─────
  LEAF    — KataGo subprocess via stdin/stdout (newline-delimited JSON).
  RELAY   — Upstream SovereignProxy nodes via WebSocket; uses HashRing for
             stable routing with load-aware fallback.
  ECHO    — Synthetic immediate responses; for fuzzing and unit tests.

REDIRECT is handled in proxy_server.py because it requires direct access
to the client WebSocket before any query reaches the Hub or the router.

LoadMetric
──────────
Load measurement is separated from routing via a LoadMetric ABC.  The
default is InFlightQueryLoad (count of dispatched-but-unfinished queries).
Swapping in a different metric (byte throughput, latency-weighted score, …)
requires only implementing the three-method ABC.
"""

from __future__ import annotations

import asyncio
import bisect
import hashlib
import json
import logging
import secrets
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Optional

from AbstractProxy.proxy_core import CompletionSignal, CompletionTracker
from katago import (
    KataGoAction,
    KataGoQuery,
    parse_response_from_wire,
    response_completion_signal,
)
from proxy_json import loads_bounded, JsonDepthExceededError
import sproxy_config as cfg

from logging_config import filter_dict, log_safe

logger = logging.getLogger("kataproxy.router")

__all__ = [
    "WireDict",
    "OnResponse",
    "OnComplete",
    "LoadMetric",
    "InFlightQueryLoad",
    "HashRing",
    "BackendRouter",
    "LeafRouter",
    "LeafStartupError",
    "RelayRouter",
    "SelectorRouter",
    "SelectorStartupError",
    "EchoRouter",
    "make_router",
]


class LeafStartupError(RuntimeError):
    """KataGo subprocess failed to reach a healthy state during start().

    Raised by ``LeafRouter.start()`` when the subprocess exits, fails to
    respond to the startup probe, or exceeds the startup timeout. The
    captured stderr tail is included in the message because that is
    where KataGo records the actual cause (missing config, missing model,
    GPU initialisation failure, etc.).
    """


class SelectorStartupError(RuntimeError):
    """SELECTOR configuration violation prevents the router from starting.

    Raised by ``SelectorRouter.start()`` on configuration violations:
    empty ``SELECTOR_MODELS`` (the SELECTOR role requires at least one
    labelled upstream); duplicate labels (each label must be unique so
    routing is unambiguous). Peer to ``LeafStartupError``: ADR-0002's
    startup-time loud-failure register — a misconfigured SELECTOR
    refuses to bind, with the specific violation named in the
    exception message.
    """

# ---------------------------------------------------------------------------
# Type aliases (the router-to-hub contract)
# ---------------------------------------------------------------------------

# A wire-format KataGo message: opaque JSON-shaped dict with at least an "id".
WireDict = dict[str, Any]

# Async callback types for router → hub communication.
#
# We use Awaitable rather than Coroutine because Awaitable is the broader
# contract: any object with __await__ satisfies it, including async def
# functions, async lambdas, and wrapped tasks. The contract must constrain
# the *result* (must be awaitable, must yield None), not the implementation.
OnResponse = Callable[[str, WireDict], Awaitable[None]]
OnComplete = Callable[[str], Awaitable[None]]

_READER_LIMIT = 64 * 1024 * 1024
_WS_MAX_SIZE = _READER_LIMIT


# ---------------------------------------------------------------------------
# Completion registration helper
# ---------------------------------------------------------------------------

def _register_query(
    tracker: CompletionTracker[str, int], qid: str, query: KataGoQuery
) -> None:
    """Register the expected number of final responses for qid in tracker.

    This avoids the -1 sentinel in the upstream register_query_completion:
    when analyzeTurns is absent, KataGo analyses only the final position and
    emits exactly one final response, so register_count(qid, 1) is correct.
    """
    if query.action != KataGoAction.ANALYZE:
        tracker.register_count(qid, 1)
    elif query.analyze_turns:
        tracker.register(qid, query.analyze_turns)
    else:
        tracker.register_count(qid, 1)
    logger.debug(
        f"qid={qid} action={query.action.name} "
        f"turns={query.analyze_turns}"
    )


# ---------------------------------------------------------------------------
# LoadMetric — pluggable load measurement
# ---------------------------------------------------------------------------

class LoadMetric(ABC):
    """Measures the 'load' on an upstream node.

    The interface is intentionally minimal so implementations can measure
    anything: in-flight query count, byte throughput, latency-weighted
    score, etc.  RelayRouter calls these three methods and never inspects
    the internal state of the metric.
    """

    @abstractmethod
    def on_query_sent(self, url: str, canonical_id: str) -> None:
        """Called immediately after a query is dispatched to url."""

    @abstractmethod
    def on_query_complete(self, url: str, canonical_id: str) -> None:
        """Called when the router receives QUERY_COMPLETE for this canonical_id."""

    @abstractmethod
    def current_load(self, url: str) -> int:
        """Return the current load value for url.  Lower is preferred."""


class InFlightQueryLoad(LoadMetric):
    """Load = number of queries dispatched but not yet QUERY_COMPLETE."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}          # url → count
        self._assignments: dict[str, str] = {}     # canonical_id → url

    def on_query_sent(self, url: str, canonical_id: str) -> None:
        self._counts[url] = self._counts.get(url, 0) + 1
        self._assignments[canonical_id] = url
        logger.info(f"url={url} load={self._counts[url]}")

    def on_query_complete(self, url: str, canonical_id: str) -> None:
        self._counts[url] = max(0, self._counts.get(url, 0) - 1)
        self._assignments.pop(canonical_id, None)
        logger.info(f"url={url} load={self._counts[url]}")

    def current_load(self, url: str) -> int:
        return self._counts.get(url, 0)

    def url_for(self, canonical_id: str) -> Optional[str]:
        """Convenience: which upstream owns this in-flight query?"""
        return self._assignments.get(canonical_id)


# ---------------------------------------------------------------------------
# HashRing — consistent upstream selection for RELAY
# ---------------------------------------------------------------------------

class HashRing:
    """Consistent hash ring for stable upstream routing.

    Each upstream gets `replicas` virtual nodes.  Routing `canonical_id`
    through the ring means the same query always prefers the same upstream
    (cache-friendly), while different queries spread roughly uniformly.

    `ordered_nodes_for` returns all upstreams in preference order so the
    caller can walk down the list when the preferred node is over-loaded or
    disconnected.
    """

    def __init__(self, nodes: list[str], replicas: int = 150) -> None:
        self._ring: list[tuple[int, str]] = []
        for node in nodes:
            for i in range(replicas):
                h = int(hashlib.md5(f"{node}:{i}".encode()).hexdigest(), 16)
                self._ring.append((h, node))
        self._ring.sort(key=lambda t: t[0])
        # Preserve insertion order, deduplicate.
        seen: set[str] = set()
        self._unique_nodes: list[str] = []
        for node in nodes:
            if node not in seen:
                seen.add(node)
                self._unique_nodes.append(node)
        logger.info(
            f"{len(self._unique_nodes)} node(s) × {replicas} replicas "
            f"= {len(self._ring)} ring entries"
        )

    def ordered_nodes_for(self, key: str) -> list[str]:
        """Return all unique nodes in preference order for this key."""
        if not self._ring:
            return []
        h = int(hashlib.md5(key.encode()).hexdigest(), 16)
        # bisect_left requires a comparable tuple; use ("", 0) structure
        idx = bisect.bisect_left(self._ring, (h, ""))
        seen: set[str] = set()
        result: list[str] = []
        n = len(self._ring)
        for i in range(n):
            _, node = self._ring[(idx + i) % n]
            if node not in seen:
                seen.add(node)
                result.append(node)
                if len(result) == len(self._unique_nodes):
                    break
        return result


# ---------------------------------------------------------------------------
# BackendRouter ABC
# ---------------------------------------------------------------------------

class BackendRouter(ABC):
    """Sends queries to a backend; delivers responses via async callbacks.

    on_complete is called exactly once per query, after all sub-tasks finish.
    on_response may be called many times (partial + final per turn).
    terminate may be called at any time to cancel an in-flight query.
    """

    @abstractmethod
    async def start(self) -> None:
        """Initialise (open connections, start subprocess, etc.)."""

    @abstractmethod
    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Schedule query for backend execution (fire-and-forget).

        Callbacks will be invoked asynchronously as responses arrive.
        """

    @abstractmethod
    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Ask the backend to cancel this in-flight query.

        on_response and on_complete are called when the backend's acknowledge
        response arrives, following the same contract as dispatch().
        """

    @abstractmethod
    async def stop(self) -> None:
        """Graceful shutdown."""


# ---------------------------------------------------------------------------
# LeafRouter
# ---------------------------------------------------------------------------

class LeafRouter(BackendRouter):
    """Routes queries to a local KataGo analysis subprocess.

    KataGo speaks newline-delimited JSON on stdin/stdout.  A single asyncio
    background task reads stdout and dispatches responses to registered
    callbacks by canonical_id; a sibling task drains stderr to the logger
    so KataGo diagnostics are visible to operators.

    Lifecycle, in three phases:

      1. ``start()`` spawns the subprocess and runs a *health gate*: it
         sends a 1-visit probe query and waits for either the response,
         the subprocess exiting, or a configurable timeout. A failed gate
         raises :class:`LeafStartupError` with the captured stderr tail —
         the proxy refuses to advertise itself as ready when the engine
         is non-functional.

      2. After the gate clears, the router is *healthy*. The reader loop
         processes responses and calls registered callbacks. If KataGo
         crashes mid-flight, the reader respawns it within a bounded
         budget; each restart is logged at WARNING level.

      3. After the restart budget is exhausted, the router enters an
         *unhealthy* terminal state. ``dispatch()`` and ``terminate()``
         synthesise an error response and complete immediately, so
         clients learn rather than wait forever; in-flight queries at
         the moment of transition are also failed loudly.

    This shape is the application of ADR-0002 (fail loudly) to the LEAF
    role: a config that fails to start KataGo will keep failing to start
    KataGo, so silent retry would mask a deterministic problem; a
    runtime crash *may* be transient, so a bounded retry is acceptable,
    but the bound is finite and operator visibility is non-negotiable.
    """

    _RESTART_DELAY_S = 2.0
    _MAX_RESTARTS = 3
    _STDERR_TAIL_LINES = 200
    _STARTUP_PROBE_ID = "_kataproxy_startup_probe"
    _ENGINE_DEAD_ERROR = "KataGo engine is not running"

    def __init__(
        self,
        cmd: list[str],
        startup_timeout_s: float = 60.0,
        max_restarts: Optional[int] = None,
    ) -> None:
        self._cmd = cmd
        self._startup_timeout_s = startup_timeout_s
        self._max_restarts = (
            self._MAX_RESTARTS if max_restarts is None else max_restarts
        )
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._stderr_task: Optional[asyncio.Task[None]] = None
        self._tracker: CompletionTracker[str, int] = CompletionTracker()
        # canonical_id → (on_response, on_complete)
        self._callbacks: dict[str, tuple[OnResponse, OnComplete]] = {}
        # Bounded ring of recent stderr lines, included in startup-failure
        # exception messages and visible in operator-facing logs.
        self._stderr_tail: Deque[str] = deque(maxlen=self._STDERR_TAIL_LINES)
        # Shutdown gate. False initially; True from start() until stop().
        # The reader exits when False; the EOF-respawn path also disengages.
        self._running = False
        # True only after the startup probe has been acknowledged. Gates
        # dispatch/terminate, and controls reader EOF semantics (pre-gate
        # EOF lets the gate raise; post-gate EOF triggers bounded restart).
        self._healthy = False
        # Restart budget for runtime crashes (post-startup).
        self._restart_budget = self._max_restarts
        # Set by the read loop when the probe response arrives; awaited by
        # the startup gate.
        self._probe_event: Optional[asyncio.Event] = None

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn KataGo, attach stderr drainer, and clear the startup gate.

        Raises :class:`LeafStartupError` if KataGo exits before responding
        to the probe, or if the probe doesn't return within
        ``startup_timeout_s``.
        """
        await self._spawn()
        # _running gates the read loop. We set it True before starting the
        # reader so the loop can deliver the probe response. The reader's
        # EOF branch checks _healthy to decide whether to respawn.
        self._running = True
        self._reader_task = asyncio.create_task(
            self._read_loop(), name="leaf-katago-reader"
        )

        try:
            await self._await_startup_or_fail()
        except LeafStartupError:
            # Roll back: tear down tasks and the proc before re-raising,
            # so the ProxyServer doesn't end up advertising a router whose
            # background tasks are still attached to a dead subprocess.
            self._running = False
            await self._teardown_subprocess()
            raise

        self._healthy = True
        assert self._proc is not None
        logger.info(
            f"KataGo pid={self._proc.pid}; startup gate cleared, router healthy"
        )

    async def _spawn(self) -> None:
        """Spawn the KataGo subprocess and (re)attach the stderr drainer.

        The stdout reader is *not* started here — it lives for the
        LeafRouter's lifetime, regardless of how many KataGo instances
        come and go through respawn. The stderr drainer, by contrast, is
        bound to a specific Process object, so it must be replaced on
        every spawn.
        """
        logger.info(f"launching: {self._cmd}")
        self._proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_READER_LIMIT,
        )

        # Replace any prior drainer with one bound to the new process.
        if self._stderr_task is not None and not self._stderr_task.done():
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._stderr_task = asyncio.create_task(
            self._stderr_drain(self._proc), name="leaf-katago-stderr"
        )

        logger.info(f"KataGo spawned, pid={self._proc.pid}")

    async def _stderr_drain(self, proc: asyncio.subprocess.Process) -> None:
        """Forward KataGo's stderr to the operator's logs.

        Without an active drainer, ``stderr=PIPE`` fills its kernel buffer
        and a long-running KataGo can deadlock on its own log writes. With
        the drainer, every KataGo diagnostic — startup banner, model-load
        progress, fatal errors — is visible at WARNING level under
        ``kataproxy.router``. The most recent lines are also kept in a
        bounded ring so startup-failure exceptions can include them.
        """
        if proc.stderr is None:
            return
        try:
            while True:
                raw = await proc.stderr.readline()
                if not raw:
                    return
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                self._stderr_tail.append(line)
                logger.warning(f"katago[pid={proc.pid}]: {line}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"stderr drainer for pid={proc.pid}: {e}")

    async def _await_startup_or_fail(self) -> None:
        """Send a probe and race against proc-exit and timeout.

        Sends a minimal 1-visit empty-board analyze query; waits for the
        first response on that id. *Any* response counts as liveness — the
        probe is intercepted in the read loop before parsing, so even a
        protocol-level error from KataGo (which still demonstrates the
        engine is up and reading stdin) clears the gate. Pre-gate EOF on
        stdout is not handled here; the read loop sees it, exits, and
        proc_task resolves below with the exit code.
        """
        assert self._proc is not None
        if self._proc.stdin is None:
            raise LeafStartupError(
                "KataGo stdin pipe unavailable; cannot send startup probe.\n"
                + self._stderr_tail_block()
            )

        self._probe_event = asyncio.Event()

        probe_wire: WireDict = {
            "id": self._STARTUP_PROBE_ID,
            "moves": [],
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "analyzeTurns": [0],
            "maxVisits": 1,
        }

        try:
            self._proc.stdin.write((json.dumps(probe_wire) + "\n").encode())
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as e:
            # KataGo exited between spawn and our write — almost always a
            # config/model error. Give the drainer a moment to flush, then
            # raise with whatever stderr it caught.
            await asyncio.sleep(0.2)
            raise LeafStartupError(
                f"KataGo stdin closed during startup probe: {e}.\n"
                f"command: {self._cmd}\n"
                + self._stderr_tail_block()
            ) from e

        probe_task = asyncio.create_task(self._probe_event.wait())
        proc_task = asyncio.create_task(self._proc.wait())
        try:
            done, _pending = await asyncio.wait(
                {probe_task, proc_task},
                timeout=self._startup_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (probe_task, proc_task):
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass

        if probe_task in done and probe_task.exception() is None:
            return

        if proc_task in done and proc_task.exception() is None:
            rc = proc_task.result()
            # Brief settle so the stderr drainer flushes residual buffer
            # from the now-dead process before we sample the tail.
            await asyncio.sleep(0.2)
            raise LeafStartupError(
                f"KataGo exited with code {rc} before responding to the "
                f"startup probe.\n"
                f"command: {self._cmd}\n"
                + self._stderr_tail_block()
            )

        # Timeout: the proc is still alive but unresponsive. Don't sleep
        # — the drainer is concurrent, and any stderr already captured is
        # already in the tail.
        raise LeafStartupError(
            f"KataGo did not respond to the startup probe within "
            f"{self._startup_timeout_s:.0f}s.\n"
            f"command: {self._cmd}\n"
            + self._stderr_tail_block()
        )

    def _stderr_tail_block(self) -> str:
        if not self._stderr_tail:
            return "stderr tail: (no output captured)"
        lines = "\n".join(f"  {line}" for line in self._stderr_tail)
        return (
            f"stderr tail (last {len(self._stderr_tail)} line(s)):\n{lines}"
        )

    async def _teardown_subprocess(self) -> None:
        """Cancel reader+drainer tasks and stop the subprocess.

        Used both on startup failure (rollback) and on stop(). Idempotent:
        every step guards against missing or already-finished state.
        """
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._stderr_task is not None and not self._stderr_task.done():
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if self._proc is not None and self._proc.returncode is None:
            try:
                self._proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await self._proc.wait()
            except Exception:
                pass

    # -----------------------------------------------------------------------
    # Reader loop
    # -----------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Continuously read KataGo stdout; dispatch responses by canonical_id."""
        while self._running:
            if self._proc is None or self._proc.stdout is None:
                logger.warning("no proc/stdout; waiting before retry")
                await asyncio.sleep(self._RESTART_DELAY_S)
                continue

            try:
                raw = await self._proc.stdout.readline()
            except asyncio.LimitOverrunError as e:
                # Line exceeded reader buffer — log and discard, do NOT restart.
                logger.debug(f"line too long ({e.consumed} bytes); discarding")
                # Drain the remainder of the overlong line before continuing.
                try:
                    await self._proc.stdout.readuntil(b"\n")
                except Exception:
                    pass
                continue
            except Exception as e:
                logger.error(f"read error: {e}")
                raw = b""

            if not raw:
                if not await self._handle_eof():
                    return
                continue

            line = raw.decode().strip()

            try:
                wire: WireDict = loads_bounded(line, max_depth=cfg.JSON_MAX_DEPTH)
            except JsonDepthExceededError as e:
                logger.error(f"refused depth-bombed line from KataGo: {e}")
                continue
            except json.JSONDecodeError as e:
                logger.error(f"JSON error: {e}  raw={log_safe(line)}")
                continue

            logger.debug(f"stdout: {json.dumps(filter_dict(wire))}")

            canonical_id = wire.get("id")
            if canonical_id is None:
                logger.warning("response missing 'id', skipping")
                continue

            # Startup-probe short-circuit. Any response on the probe id
            # counts as liveness — even a KataGo protocol error, since
            # that still proves the engine is reading stdin and writing
            # stdout. We don't try to parse the response.
            if canonical_id == self._STARTUP_PROBE_ID:
                if self._probe_event is not None:
                    self._probe_event.set()
                continue

            cbs = self._callbacks.get(canonical_id)
            if cbs is None:
                logger.info(f"no callback for canonical_id={canonical_id!r}")
                continue
            on_response, on_complete = cbs

            try:
                _, response = parse_response_from_wire(wire)
            except Exception as e:
                logger.error(f"parse error: {e}  wire={wire}")
                continue

            disc, is_partial = response_completion_signal(response)
            sig = self._tracker.signal(canonical_id, disc, is_partial)
            logger.info(
                f"canonical_id={canonical_id} "
                f"turn={disc} during_search={is_partial} "
                f"sig={sig.name}"
            )

            await on_response(canonical_id, wire)

            if sig == CompletionSignal.QUERY_COMPLETE:
                self._callbacks.pop(canonical_id, None)
                await on_complete(canonical_id)

    async def _handle_eof(self) -> bool:
        """React to a stdout EOF; return True to keep looping, False to exit.

        Branches:

          * **During startup gate** (``_healthy is False``): exit. The
            startup gate is concurrently awaiting ``proc.wait()`` and will
            raise ``LeafStartupError`` carrying the stderr tail.
          * **Shutting down** (``_running is False``): exit.
          * **Restart budget exhausted**: log at error, mark unhealthy,
            fail in-flight queries loudly, exit. Subsequent dispatches
            fail at the unhealthy-state check.
          * **Within budget**: decrement, log at warning, sleep, respawn.
            On respawn failure, transition to unhealthy and exit.
        """
        if not self._healthy:
            logger.info(
                "KataGo stdout EOF before startup gate cleared; "
                "reader exiting (gate will raise)"
            )
            return False
        if not self._running:
            return False
        if self._restart_budget <= 0:
            logger.error(
                "KataGo crashed and the restart budget is exhausted; "
                "router is now UNHEALTHY. Subsequent queries will fail "
                "with an explicit error response."
            )
            self._healthy = False
            await self._fail_inflight()
            return False
        self._restart_budget -= 1
        logger.warning(
            f"KataGo crashed; respawning in {self._RESTART_DELAY_S:.1f}s. "
            f"restart budget remaining: {self._restart_budget}"
        )
        await asyncio.sleep(self._RESTART_DELAY_S)
        try:
            await self._spawn()
        except Exception as e:
            logger.error(
                f"KataGo respawn failed: {e}; router is now UNHEALTHY"
            )
            self._healthy = False
            await self._fail_inflight()
            return False
        return True

    async def _fail_inflight(self) -> None:
        """Synthesise error completions for every in-flight query.

        Called on the transition to unhealthy. Without this, queries
        dispatched before the crash sit in ``_callbacks`` forever and
        their clients hang. Each gets one error response and a
        completion, in the same order they were registered.
        """
        in_flight = list(self._callbacks.items())
        self._callbacks.clear()
        for canonical_id, (on_response, on_complete) in in_flight:
            self._tracker.cancel(canonical_id)
            error_wire: WireDict = {
                "id": canonical_id,
                "error": self._ENGINE_DEAD_ERROR,
            }
            try:
                await on_response(canonical_id, error_wire)
                await on_complete(canonical_id)
            except Exception as e:
                logger.error(
                    f"failed to deliver engine-dead notice for "
                    f"{canonical_id!r}: {e}"
                )

    # -----------------------------------------------------------------------
    # Dispatch / terminate / stop
    # -----------------------------------------------------------------------

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"canonical_id={canonical_id} action={query.action.name}")

        if (
            not self._healthy
            or self._proc is None
            or self._proc.stdin is None
        ):
            logger.error(
                f"KataGo unavailable (healthy={self._healthy}); "
                f"failing query {canonical_id!r} loudly"
            )
            error_wire: WireDict = {
                "id": canonical_id,
                "error": self._ENGINE_DEAD_ERROR,
            }
            await on_response(canonical_id, error_wire)
            await on_complete(canonical_id)
            return

        _register_query(self._tracker, canonical_id, query)
        self._callbacks[canonical_id] = (on_response, on_complete)

        line = json.dumps(wire_dict) + "\n"
        try:
            self._proc.stdin.write(line.encode())
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as e:
            # Lost the engine between the health check and the write.
            # Roll back local state and notify the caller loudly.
            logger.error(
                f"KataGo stdin write failed for {canonical_id!r}: {e}"
            )
            self._tracker.cancel(canonical_id)
            self._callbacks.pop(canonical_id, None)
            error_wire = {
                "id": canonical_id,
                "error": self._ENGINE_DEAD_ERROR,
            }
            await on_response(canonical_id, error_wire)
            await on_complete(canonical_id)
            return
        logger.debug(f"wrote to stdin: {line}")

    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"canonical_id={canonical_id}")
        # Cancel the analyze query's tracking state and remove its callback.
        self._tracker.cancel(canonical_id)
        self._callbacks.pop(canonical_id, None)

        async def _send_synthetic_ack() -> None:
            term_wire_id = f"kg_{secrets.token_hex(6)}"
            synthetic: WireDict = {
                "id": term_wire_id,
                "action": "terminate",
                "terminateId": canonical_id,
            }
            await on_response(term_wire_id, synthetic)
            await on_complete(term_wire_id)

        if (
            not self._healthy
            or self._proc is None
            or self._proc.stdin is None
        ):
            logger.warning(
                f"KataGo unavailable (healthy={self._healthy}); "
                f"synthesising terminate ack for {canonical_id!r}"
            )
            await _send_synthetic_ack()
            return

        # Mint a fresh wire id for the terminate query itself.
        term_wire_id = f"kg_{secrets.token_hex(6)}"
        term_wire: WireDict = {
            "id": term_wire_id,
            "action": "terminate",
            "terminateId": canonical_id,
        }

        # Register callback so the read loop dispatches KataGo's ack normally.
        self._tracker.register_count(term_wire_id, 1)
        self._callbacks[term_wire_id] = (on_response, on_complete)

        try:
            self._proc.stdin.write((json.dumps(term_wire) + "\n").encode())
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as e:
            logger.warning(
                f"KataGo stdin write failed for terminate of "
                f"{canonical_id!r}: {e}"
            )
            self._tracker.cancel(term_wire_id)
            self._callbacks.pop(term_wire_id, None)
            await _send_synthetic_ack()
            return
        logger.debug(f"sent {term_wire}")

    async def stop(self) -> None:
        self._running = False
        self._healthy = False
        await self._teardown_subprocess()
        logger.info("done")


# ---------------------------------------------------------------------------
# RelayRouter
# ---------------------------------------------------------------------------

class RelayRouter(BackendRouter):
    """Routes queries to upstream SovereignProxy nodes via WebSocket.

    Selection policy (single-target, per-query actions):
      1. Hash canonical_id through the ring → preferred upstream.
      2. Walk the ring until a connected node with load < max_load is found.
      3. If all are over max_load, use the least-loaded connected node.

    Action-routing matrix:

      ANALYZE        → single-target via _select_upstream (above policy).
      TERMINATE      → single-target via remembered routing for the
                       canonical_id (label in _callbacks).
      QUERY_VERSION  → broadcast to every connected upstream. First
      CLEAR_CACHE      response wins; subsequent responses drop at
      TERMINATE_ALL    _read_loop's "no callback" branch. Heartbeat
                       fanout (QUERY_VERSION) is load-bearing for
                       downstream LEAF KeepAliveMiddleware — without
                       it, every upstream the hash ring doesn't route
                       the heartbeat to fires its watchdog after
                       idle_timeout on whatever ANALYZE the ring
                       lands on it. CLEAR_CACHE fanout follows from
                       KataGo's per-LEAF cache; TERMINATE_ALL fanout
                       follows from "every in-flight query, regardless
                       of which upstream it was hash-routed to". See
                       proxy/CLAUDE.md's heartbeat-fanout-contract
                       section and the SELECTOR watchdog postmortem in
                       the umbrella's docs/notes/.

    Each upstream connection has its own asyncio reader task.  Disconnections
    trigger a reconnect loop with exponential back-off.

    The LoadMetric is called around dispatch and completion for the
    single-target path; it does not participate in any other logic and
    is skipped for the broadcast path (heartbeats and metadata fanouts
    aren't in-flight in the load sense).
    """

    def __init__(
        self,
        upstream_urls: list[str],
        load_metric: LoadMetric,
        max_load: int = cfg.RELAY_MAX_LOAD,
        ring_replicas: int = cfg.HASH_RING_REPLICAS,
    ) -> None:
        self._urls = upstream_urls
        self._load_metric = load_metric
        self._max_load = max_load
        self._ring = HashRing(upstream_urls, ring_replicas)
        # ws is typed as Any because the websockets library has no stable
        # type stubs across major versions; the runtime contract (.send,
        # async iteration) is enforced behaviourally.
        self._connections: dict[str, Any] = {}            # url → websocket
        self._reader_tasks: dict[str, asyncio.Task[None]] = {}  # url → task
        # Reconnect tasks scheduled by _read_loop's finally block. Tracked
        # so stop() can cancel them; without tracking, a flapping upstream
        # accumulates one orphan task per disconnect cycle indefinitely
        # (audit H-2). The set is self-pruning via the done-callback in
        # _schedule_reconnect.
        self._reconnect_tasks: set[asyncio.Task[None]] = set()
        self._tracker: CompletionTracker[str, int] = CompletionTracker()
        # canonical_id → (on_response, on_complete, url)
        self._callbacks: dict[str, tuple[OnResponse, OnComplete, str]] = {}

    async def start(self) -> None:
        logger.info(f"connecting to {len(self._urls)} upstream(s)")
        await asyncio.gather(
            *(self._connect(url) for url in self._urls),
            return_exceptions=True,
        )

    async def _connect(self, url: str) -> None:
        import websockets
        logger.info(f"→ {url}")
        try:
            ws = await websockets.connect(url, max_size=_WS_MAX_SIZE)
            self._connections[url] = ws
            task = asyncio.create_task(
                self._read_loop(url, ws), name=f"relay-reader:{url}"
            )
            self._reader_tasks[url] = task
            logger.info(f"connected: {url}")
        except Exception as e:
            logger.error(f"FAILED {url}: {e}")

    async def _reconnect_with_backoff(self, url: str) -> None:
        delay = 2.0
        while True:
            await asyncio.sleep(delay)
            logger.info(f"attempt for {url}, delay was {delay:.1f}s")
            try:
                await self._connect(url)
                return
            except Exception as e:
                logger.error(f"still failing {url}: {e}")
                delay = min(delay * 2.0, 60.0)

    async def _read_loop(self, url: str, ws: Any) -> None:
        """Read responses from one upstream; dispatch callbacks by canonical_id."""
        try:
            async for raw_msg in ws:
                # Log the raw message via log_safe rather than re-parsing —
                # sidesteps both the cost of a second json.loads on every
                # message AND the (theoretical) RecursionError that would
                # have fired here for a depth-bombed payload before the
                # depth-bound check below could refuse it.
                logger.debug(f"url={url} raw={log_safe(raw_msg)}")
                try:
                    wire: WireDict = loads_bounded(raw_msg, max_depth=cfg.JSON_MAX_DEPTH)
                except JsonDepthExceededError as e:
                    logger.error(f"refused depth-bombed message from {url}: {e}")
                    continue
                except json.JSONDecodeError as e:
                    logger.error(f"JSON error from {url}: {e}")
                    continue

                # Upstream may send proxy_meta (e.g., another redirect).
                # Log and ignore — relaying redirects is not supported.
                if "proxy_meta" in wire:
                    logger.info(f"proxy_meta from upstream {url}: {wire['proxy_meta']}")
                    continue

                canonical_id = wire.get("id")
                if canonical_id is None:
                    logger.warning(f"response missing 'id' from {url}")
                    continue

                cb = self._callbacks.get(canonical_id)
                if cb is None:
                    logger.warning(f"no callback for {canonical_id!r}")
                    continue
                on_response, on_complete, assigned_url = cb

                try:
                    _, response = parse_response_from_wire(wire)
                except Exception as e:
                    logger.error(f"parse error: {e}")
                    continue

                disc, is_partial = response_completion_signal(response)
                sig = self._tracker.signal(canonical_id, disc, is_partial)
                logger.debug(
                    f"canonical_id={canonical_id} "
                    f"turn={disc} during={is_partial} "
                    f"sig={sig.name}"
                )

                await on_response(canonical_id, wire)

                if sig == CompletionSignal.QUERY_COMPLETE:
                    self._callbacks.pop(canonical_id, None)
                    self._load_metric.on_query_complete(assigned_url, canonical_id)
                    await on_complete(canonical_id)

        except Exception as e:
            logger.error(f"connection lost for {url}: {e}")
        finally:
            self._connections.pop(url, None)
            self._reader_tasks.pop(url, None)
            self._schedule_reconnect(url)

    def _schedule_reconnect(self, url: str) -> None:
        """Spawn a reconnect-with-backoff task and track it for cancellation."""
        logger.info(f"scheduling reconnect for {url}")
        task = asyncio.create_task(
            self._reconnect_with_backoff(url),
            name=f"relay-reconnect:{url}",
        )
        self._reconnect_tasks.add(task)
        # Self-prune on completion so stop()'s set scan stays bounded
        # under sustained successful reconnects.
        task.add_done_callback(self._reconnect_tasks.discard)

    def _select_upstream(self, canonical_id: str) -> Optional[str]:
        """Walk the ring in preference order; return first under max_load."""
        candidates = self._ring.ordered_nodes_for(canonical_id)
        logger.info(f"candidates for {canonical_id}: {candidates}")

        connected = [u for u in candidates if u in self._connections]
        if not connected:
            logger.info("no connected upstreams")
            return None

        for url in connected:
            load = self._load_metric.current_load(url)
            logger.info(f"{url} load={load} max={self._max_load}")
            if load < self._max_load:
                return url

        # All over limit — use least-loaded to avoid complete stall.
        best = min(connected, key=lambda u: self._load_metric.current_load(u))
        logger.info(f"all over max_load; using least-loaded {best}")
        return best

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        action = query.action
        # Broadcast the metadata-shaped fanout actions. See the class
        # docstring's action-routing matrix and _broadcast below.
        if action in (
            KataGoAction.QUERY_VERSION,
            KataGoAction.CLEAR_CACHE,
            KataGoAction.TERMINATE_ALL,
        ):
            await self._broadcast(
                canonical_id, wire_dict, query, on_response, on_complete,
            )
            return

        # Single-target path (ANALYZE and any future per-query action).
        url = self._select_upstream(canonical_id)
        if url is None:
            logger.warning(f"no upstream available for {canonical_id!r}; dropping")
            return
        logger.info(f"canonical_id={canonical_id} → {url}")
        _register_query(self._tracker, canonical_id, query)
        self._callbacks[canonical_id] = (on_response, on_complete, url)
        self._load_metric.on_query_sent(url, canonical_id)
        ws = self._connections[url]
        await ws.send(json.dumps(wire_dict))
        logger.debug(f"sent: {json.dumps(wire_dict)}")

    async def _broadcast(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Forward wire_dict to every currently-connected upstream.

        Used for the actions whose semantic is "reach every backend":

          - QUERY_VERSION — heartbeat fanout. Every downstream LEAF
            runs its own KeepAliveMiddleware against the RELAY's
            connection; without per-upstream heartbeat propagation
            the LEAFs the hash ring doesn't route the heartbeat to
            fire their watchdogs on whatever ANALYZE the ring lands
            on them. The same root cause was first surfaced on
            SELECTOR (see umbrella's
            docs/notes/postmortem-selector-watchdog-2026-05.md);
            RELAY's hash-ring routing differs in mechanism but
            shares the structural failure mode.
          - TERMINATE_ALL — cancel every in-flight query the
            session holds, regardless of which upstream the ring
            routed each to.
          - CLEAR_CACHE — KataGo's analysis cache is per-subprocess;
            a SPA-issued clear_cache wants every upstream cleared.

        First response wins. Each upstream emits an independent
        response for the same canonical_id; the first one fires
        on_response and on_complete (the latter pops self._callbacks),
        subsequent responses land at _read_loop's "no callback for
        canonical_id" branch and are silently dropped. The SPA sees
        exactly one response.

        The LoadMetric is skipped — heartbeats and metadata fanouts
        aren't in-flight in the load sense, and tracking N
        on_query_sent calls against a single on_query_complete (the
        first response that pops _callbacks) would leak counts on
        the (N-1) upstreams that never paired through. The synthetic
        "__broadcast__" sentinel in the URL slot of the _callbacks
        entry is what _read_loop sees on the first-response path —
        it's a no-op for terminate (broadcast actions are not
        targets of per-query SPA terminate) and is benign through
        InFlightQueryLoad.on_query_complete (max(0, ...) keeps the
        synthetic count at 0).

        Per-upstream send failures log at error and continue. The
        broadcast aborts only when zero sends succeed; in that case
        the callback is popped and the canonical is silently dropped
        (matching the single-target dispatch's no-upstream-available
        behaviour — RELAY's existing convention).
        """
        connected = list(self._connections.keys())
        if not connected:
            logger.warning(
                f"{query.action.name} ({canonical_id}): no connected "
                f"upstream available; dropping"
            )
            return

        _register_query(self._tracker, canonical_id, query)
        self._callbacks[canonical_id] = (
            on_response, on_complete, "__broadcast__",
        )

        sent_to: list[str] = []
        for url in connected:
            ws = self._connections.get(url)
            if ws is None:
                continue
            try:
                await ws.send(json.dumps(wire_dict))
            except Exception as e:
                logger.error(
                    f"broadcast send failed for {url} "
                    f"({canonical_id}, {query.action.name}): {e}"
                )
                continue
            sent_to.append(url)

        if not sent_to:
            self._callbacks.pop(canonical_id, None)
            self._tracker.cancel(canonical_id)
            logger.error(
                f"broadcast {query.action.name} ({canonical_id}) sent to "
                f"zero of {len(connected)} connected upstream(s)"
            )
            return

        logger.debug(
            f"broadcast {query.action.name} ({canonical_id}) sent to "
            f"{len(sent_to)}/{len(connected)} upstream(s): {sent_to}"
        )

    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        cb = self._callbacks.pop(canonical_id, None)

        # Helper to synthesize an ack so the client doesn't freeze
        async def _send_synthetic_ack() -> None:
            term_wire_id = f"kg_{secrets.token_hex(6)}"
            synthetic_ack: WireDict = {
                "id": term_wire_id,
                "action": "terminate",
                "terminateId": canonical_id,
            }
            await on_response(term_wire_id, synthetic_ack)
            await on_complete(term_wire_id)

        if cb is None:
            logger.info(f"no in-flight entry for {canonical_id!r}")
            await _send_synthetic_ack()
            return

        _, _, url = cb
        self._tracker.cancel(canonical_id)
        self._load_metric.on_query_complete(url, canonical_id)

        ws = self._connections.get(url)
        if ws is None:
            logger.warning(f"upstream {url} disconnected; cannot send terminate")
            await _send_synthetic_ack()
            return

        term_wire_id = f"kg_{secrets.token_hex(6)}"
        term_wire: WireDict = {
            "id": term_wire_id,
            "action": "terminate",
            "terminateId": canonical_id,
        }

        self._tracker.register_count(term_wire_id, 1)
        self._callbacks[term_wire_id] = (on_response, on_complete, url)

        logger.debug(f"→ {url}: {term_wire}")
        await ws.send(json.dumps(term_wire))

    async def stop(self) -> None:
        for task in list(self._reader_tasks.values()):
            task.cancel()
        # Cancel any in-flight reconnect-with-backoff tasks so stop() truly
        # stops; otherwise they would continue retrying indefinitely after
        # the router was meant to shut down.
        for task in list(self._reconnect_tasks):
            task.cancel()
        for ws in list(self._connections.values()):
            try:
                await ws.close()
            except Exception:
                pass
        logger.info("done")


# ---------------------------------------------------------------------------
# SelectorRouter
# ---------------------------------------------------------------------------

class SelectorRouter(BackendRouter):
    """Routes queries by `model` field to labelled upstream LEAFs.

    Distinct from ``RelayRouter`` even though both manage WebSocket
    upstreams: SELECTOR's invariant is that upstreams are *named*
    (distinguishable), not *interchangeable*. Dispatch is a labelled-
    dictionary lookup, not hash-ring routing; there is no LoadMetric
    (the upstreams are not fungible) and no fallback (each model is
    unique, so a query for model X cannot be served by model Y).

    Per-upstream failure budget mirrors ``LeafRouter._MAX_RESTARTS``:
    each label has up to ``max_connect_failures`` reconnect attempts
    after a disconnect (the initial connect attempt does not count, by
    analogy with LeafRouter where the initial spawn does not count
    against the restart budget). Budget exhaustion marks the label
    UNHEALTHY; queries to an unhealthy model fail loudly with a
    structured error response naming the unavailable model. Other
    labels continue to serve normally. Recovery is restart-only —
    matching LeafRouter's posture, where the proxy is the unit of
    operational restart.

    Action-routing matrix (different from RelayRouter's uniform
    hash-ring dispatch):

      ANALYZE        → routed by ``query.opaque['model']`` to the
                       labelled upstream
      TERMINATE      → routed via the dedicated ``terminate()`` method
                       by remembered label for the canonical_id
      QUERY_MODELS   → synthesised from the configured label set; no
                       upstream traffic. Wire shape:
                       ``{"id": ..., "models": [{"label": l, "healthy": b}, ...]}``
                       — list-of-dicts; the SPA reads ``entry.label`` as
                       the routing key and ``entry.healthy`` to gate the
                       model-selector dropdown's enabled state. Old
                       SPAs that read only ``entry.label`` continue to
                       work unchanged.
      QUERY_VERSION  → broadcast to every healthy upstream. First
      CLEAR_CACHE      response wins; subsequent responses for the
      TERMINATE_ALL    same canonical drop at the read loop's "no
                       callback" branch (entry was popped on
                       QUERY_COMPLETE). Heartbeat fanout
                       (QUERY_VERSION) is load-bearing for downstream
                       LEAF KeepAliveMiddleware: any LEAF that doesn't
                       receive heartbeats fires its watchdog after
                       ``idle_timeout`` on whatever ANALYZE the
                       SELECTOR routed to it by ``model``. See the
                       SELECTOR watchdog postmortem in the umbrella's
                       docs/notes/ for the full diagnosis. CLEAR_CACHE
                       fanout follows from KataGo's per-LEAF cache;
                       TERMINATE_ALL fanout follows from "every
                       in-flight query, regardless of which LEAF it
                       was routed to". All three responses are single-
                       message metadata, so first-response-wins is
                       correct (no aggregation required).
    """

    _MAX_CONNECT_FAILURES = 3  # mirrors LeafRouter._MAX_RESTARTS
    _RECONNECT_INITIAL_DELAY_S = 2.0
    _RECONNECT_MAX_DELAY_S = 60.0
    _MISSING_MODEL_ERROR = "missing 'model' field for SELECTOR routing"
    _UNKNOWN_MODEL_ERROR_TEMPLATE = (
        "unknown model {requested!r}; available models: {available}"
    )
    _UNHEALTHY_MODEL_ERROR_TEMPLATE = (
        "model {label!r} is currently unavailable "
        "(reconnect budget exhausted; restart the proxy to retry)"
    )
    _DISCONNECTED_MODEL_ERROR_TEMPLATE = (
        "model {label!r} is temporarily disconnected; please retry"
    )
    _NO_HEALTHY_UPSTREAM_ERROR = (
        "no healthy upstream available to serve this action"
    )

    def __init__(
        self,
        models: tuple[tuple[str, str], ...],
        max_connect_failures: Optional[int] = None,
    ) -> None:
        self._models = models
        self._max_connect_failures = (
            self._MAX_CONNECT_FAILURES
            if max_connect_failures is None
            else max_connect_failures
        )
        # Label → upstream URL. Populated in start() after duplicate-
        # label validation; an explicit dict makes the dispatch lookup
        # explicit rather than scanning the ordered tuple every query.
        self._url_for_label: dict[str, str] = {}
        # Label → live websocket. Absent → not currently connected
        # (either reconnecting within budget or marked unhealthy).
        # ws is typed as Any because the websockets library has no
        # stable type stubs across major versions; the runtime contract
        # (.send, async iteration) is enforced behaviourally.
        self._connections: dict[str, Any] = {}
        # Per-label reader task and reconnect tasks; tracked so stop()
        # can cancel everything.
        self._reader_tasks: dict[str, asyncio.Task[None]] = {}
        self._reconnect_tasks: set[asyncio.Task[None]] = set()
        # Per-label remaining reconnect budget. Decremented by failed
        # reconnect attempts (not by the initial connect attempt).
        # Hits zero → label is added to _unhealthy_models and the
        # reconnect loop exits.
        self._failure_budget: dict[str, int] = {}
        # Labels whose budget has been exhausted. Terminal until a
        # proxy restart, mirroring LeafRouter's unhealthy state.
        self._unhealthy_models: set[str] = set()
        # Completion-tracker shared with the read loop; the wire-id-
        # uniqueness invariant is the same as LeafRouter / RelayRouter.
        self._tracker: CompletionTracker[str, int] = CompletionTracker()
        # canonical_id → (on_response, on_complete, label). The label is
        # the routing record consulted by terminate() to send the
        # cancel to the correct upstream.
        self._callbacks: dict[
            str, tuple[OnResponse, OnComplete, str]
        ] = {}

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def start(self) -> None:
        """Validate configuration; connect to all upstreams in parallel.

        Raises ``SelectorStartupError`` on empty ``SELECTOR_MODELS`` or
        duplicate labels — the routing table must be unambiguous before
        the server binds. Initial connect failures do not block startup;
        the affected labels schedule reconnect-with-backoff in the
        background. The disposition is logged at INFO so operators can
        see which models are healthy at startup.
        """
        if not self._models:
            raise SelectorStartupError(
                "SELECTOR role requires at least one entry in "
                "SELECTOR_MODELS (format: "
                "label1=ws://host1:port1,label2=ws://host2:port2)"
            )
        seen: set[str] = set()
        for label, url in self._models:
            if label in seen:
                raise SelectorStartupError(
                    f"duplicate label {label!r} in SELECTOR_MODELS; "
                    f"each label must be unique so the routing table "
                    f"is unambiguous"
                )
            seen.add(label)
            self._url_for_label[label] = url
            self._failure_budget[label] = self._max_connect_failures

        labels = list(self._url_for_label.keys())
        logger.info(
            f"connecting to {len(labels)} labelled upstream(s): {labels}"
        )
        await asyncio.gather(
            *(self._connect(label) for label in labels),
            return_exceptions=True,
        )
        self._log_health_disposition()

    def _log_health_disposition(self) -> None:
        """Emit a single INFO line summarising per-label connect status."""
        healthy: list[str] = []
        retrying: list[str] = []
        unhealthy: list[str] = []
        for label in self._url_for_label:
            if label in self._unhealthy_models:
                unhealthy.append(label)
            elif label in self._connections:
                healthy.append(label)
            else:
                retrying.append(label)
        logger.info(
            f"SELECTOR ready: healthy={healthy} "
            f"reconnecting={retrying} unhealthy={unhealthy}"
        )

    async def _connect(self, label: str) -> None:
        """Initial connect attempt for a label; on failure, schedule retry.

        The initial attempt does not consume the reconnect budget — by
        analogy with ``LeafRouter`` where the initial spawn doesn't
        count against the restart budget. Only failed reconnect
        attempts decrement the budget.
        """
        import websockets
        url = self._url_for_label[label]
        logger.info(f"label={label!r} → {url}")
        try:
            ws = await websockets.connect(url, max_size=_WS_MAX_SIZE)
        except Exception as e:
            logger.error(f"connect failed for label={label!r} ({url}): {e}")
            self._schedule_reconnect(label)
            return
        self._connections[label] = ws
        task = asyncio.create_task(
            self._read_loop(label, ws), name=f"selector-reader:{label}"
        )
        self._reader_tasks[label] = task
        logger.info(f"connected: label={label!r} ({url})")

    def _schedule_reconnect(self, label: str) -> None:
        """Spawn a reconnect-with-backoff task and track it for cancellation.

        No-op if the label is already unhealthy (terminal state) — the
        reconnect loop has nothing to do once budget is exhausted.
        """
        if label in self._unhealthy_models:
            return
        logger.info(f"scheduling reconnect for label={label!r}")
        task = asyncio.create_task(
            self._reconnect_with_backoff(label),
            name=f"selector-reconnect:{label}",
        )
        self._reconnect_tasks.add(task)
        # Self-prune on completion so stop()'s set scan stays bounded.
        task.add_done_callback(self._reconnect_tasks.discard)

    async def _reconnect_with_backoff(self, label: str) -> None:
        """Retry connecting to an upstream until success or budget exhausted.

        Exponential backoff capped at ``_RECONNECT_MAX_DELAY_S``.
        Each failed attempt decrements ``_failure_budget[label]``; on
        exhaustion the label is added to ``_unhealthy_models`` and the
        loop exits. On success the new websocket and reader task
        replace any prior entries.
        """
        import websockets
        delay = self._RECONNECT_INITIAL_DELAY_S
        url = self._url_for_label[label]
        while label not in self._unhealthy_models:
            await asyncio.sleep(delay)
            try:
                ws = await websockets.connect(url, max_size=_WS_MAX_SIZE)
            except Exception as e:
                self._failure_budget[label] -= 1
                if self._failure_budget[label] <= 0:
                    logger.error(
                        f"label={label!r} reconnect budget exhausted; "
                        f"marking UNHEALTHY. Queries to this model "
                        f"will fail loudly until the proxy is restarted."
                    )
                    self._unhealthy_models.add(label)
                    return
                logger.warning(
                    f"reconnect still failing for label={label!r}: {e}; "
                    f"reconnect attempts remaining: "
                    f"{self._failure_budget[label]}"
                )
                delay = min(delay * 2.0, self._RECONNECT_MAX_DELAY_S)
                continue
            self._connections[label] = ws
            task = asyncio.create_task(
                self._read_loop(label, ws),
                name=f"selector-reader:{label}",
            )
            self._reader_tasks[label] = task
            logger.info(f"reconnected: label={label!r} ({url})")
            return

    # -----------------------------------------------------------------------
    # Reader loop (per-label)
    # -----------------------------------------------------------------------

    async def _read_loop(self, label: str, ws: Any) -> None:
        """Read responses from one labelled upstream; dispatch by canonical_id.

        Mirrors ``RelayRouter._read_loop`` in shape; the only structural
        difference is that connections are keyed by label rather than
        URL. On connection loss, the finally block schedules a
        reconnect (subject to the budget).
        """
        try:
            async for raw_msg in ws:
                logger.debug(f"label={label!r} raw={log_safe(raw_msg)}")
                try:
                    wire: WireDict = loads_bounded(
                        raw_msg, max_depth=cfg.JSON_MAX_DEPTH
                    )
                except JsonDepthExceededError as e:
                    logger.error(
                        f"refused depth-bombed message from label={label!r}: {e}"
                    )
                    continue
                except json.JSONDecodeError as e:
                    logger.error(f"JSON error from label={label!r}: {e}")
                    continue

                if "proxy_meta" in wire:
                    logger.info(
                        f"proxy_meta from label={label!r}: {wire['proxy_meta']}"
                    )
                    continue

                canonical_id = wire.get("id")
                if canonical_id is None:
                    logger.warning(
                        f"response missing 'id' from label={label!r}"
                    )
                    continue

                cb = self._callbacks.get(canonical_id)
                if cb is None:
                    logger.info(
                        f"no callback for {canonical_id!r} (already cleaned up?)"
                    )
                    continue
                on_response, on_complete, _assigned_label = cb

                try:
                    _, response = parse_response_from_wire(wire)
                except Exception as e:
                    logger.error(f"parse error from label={label!r}: {e}")
                    continue

                disc, is_partial = response_completion_signal(response)
                sig = self._tracker.signal(canonical_id, disc, is_partial)
                logger.debug(
                    f"canonical_id={canonical_id} "
                    f"turn={disc} during={is_partial} sig={sig.name}"
                )

                await on_response(canonical_id, wire)

                if sig == CompletionSignal.QUERY_COMPLETE:
                    self._callbacks.pop(canonical_id, None)
                    await on_complete(canonical_id)

        except Exception as e:
            logger.error(f"connection lost for label={label!r}: {e}")
        finally:
            self._connections.pop(label, None)
            self._reader_tasks.pop(label, None)
            if label not in self._unhealthy_models:
                self._schedule_reconnect(label)

    # -----------------------------------------------------------------------
    # Routing helpers
    # -----------------------------------------------------------------------

    def _healthy_labels(self) -> list[str]:
        """Return all currently-connected, non-budget-exhausted labels.

        Used both by the broadcast dispatch path (QUERY_VERSION /
        CLEAR_CACHE / TERMINATE_ALL) and by the QUERY_MODELS
        synthesised response that surfaces per-label availability to
        the SPA.
        """
        return [
            label for label, _url in self._models
            if label in self._connections
            and label not in self._unhealthy_models
        ]

    async def _send_synthetic_response(
        self,
        canonical_id: str,
        opaque: dict[str, Any],
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Synthesise a single MetadataResponse-shaped wire and complete."""
        wire: WireDict = {"id": canonical_id, **opaque}
        await on_response(canonical_id, wire)
        await on_complete(canonical_id)

    async def _send_structured_error(
        self,
        canonical_id: str,
        message: str,
        on_response: OnResponse,
        on_complete: OnComplete,
        field: Optional[str] = None,
    ) -> None:
        """Synthesise a structured error response in the proxy's error shape.

        Wire shape: ``{"id": canonical_id, "error": message[, "field": field]}``.
        Single response, immediate completion. Mirrors the structured
        errors the LeafRouter emits when KataGo is unavailable.
        """
        wire: WireDict = {"id": canonical_id, "error": message}
        if field is not None:
            wire["field"] = field
        await on_response(canonical_id, wire)
        await on_complete(canonical_id)

    # -----------------------------------------------------------------------
    # Dispatch (action-routing matrix)
    # -----------------------------------------------------------------------

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        action = query.action
        logger.debug(
            f"canonical_id={canonical_id} action={action.name}"
        )

        # QUERY_MODELS: synthesise the union of configured labels.
        # No upstream traffic — operators with all upstreams down still
        # get a meaningful enumeration. Each entry carries `healthy`
        # so the SPA's model-selector dropdown can gate its enabled
        # state per-label (a label that's advertised but currently
        # disconnected, or whose reconnect budget is exhausted, should
        # not be a selectable analyze target). Old SPAs that read only
        # `entry.label` continue to work — the addition is wire-
        # compatible.
        if action == KataGoAction.QUERY_MODELS:
            healthy = set(self._healthy_labels())
            await self._send_synthetic_response(
                canonical_id,
                {"models": [
                    {"label": label, "healthy": label in healthy}
                    for label, _ in self._models
                ]},
                on_response,
                on_complete,
            )
            return

        # ANALYZE: route by `model` field.
        if action == KataGoAction.ANALYZE:
            requested = query.opaque.get("model")
            if requested is None:
                logger.warning(
                    f"ANALYZE without `model` field; failing loudly "
                    f"({canonical_id})"
                )
                await self._send_structured_error(
                    canonical_id,
                    self._MISSING_MODEL_ERROR,
                    on_response,
                    on_complete,
                    field="model",
                )
                return
            if requested not in self._url_for_label:
                available = sorted(self._url_for_label.keys())
                err = self._UNKNOWN_MODEL_ERROR_TEMPLATE.format(
                    requested=requested,
                    available=available,
                )
                logger.warning(
                    f"unknown model {requested!r} ({canonical_id}); "
                    f"available: {available}"
                )
                await self._send_structured_error(
                    canonical_id,
                    err,
                    on_response,
                    on_complete,
                    field="model",
                )
                return
            if requested in self._unhealthy_models:
                err = self._UNHEALTHY_MODEL_ERROR_TEMPLATE.format(
                    label=requested
                )
                logger.warning(
                    f"model {requested!r} unhealthy; failing loudly "
                    f"({canonical_id})"
                )
                await self._send_structured_error(
                    canonical_id, err, on_response, on_complete,
                    field="model",
                )
                return
            await self._forward(
                canonical_id, wire_dict, query, requested,
                on_response, on_complete,
            )
            return

        # QUERY_VERSION / CLEAR_CACHE / TERMINATE_ALL: broadcast to
        # every healthy upstream. First response wins; subsequent
        # responses for the same canonical drop at the read loop's
        # "no callback" branch (the entry was popped on the first
        # QUERY_COMPLETE). Heartbeat fanout is load-bearing — see
        # _broadcast and the class docstring.
        if action in (
            KataGoAction.QUERY_VERSION,
            KataGoAction.CLEAR_CACHE,
            KataGoAction.TERMINATE_ALL,
        ):
            await self._broadcast(
                canonical_id, wire_dict, query, on_response, on_complete,
            )
            return

        # TERMINATE is dispatched via the dedicated terminate() method,
        # not via dispatch(); landing here is a misuse upstream of
        # SELECTOR. Fail loudly so the misroute is visible.
        logger.error(
            f"unexpected action {action.name} in dispatch() "
            f"({canonical_id}); SELECTOR routes terminate via the "
            f"dedicated terminate() method"
        )
        await self._send_structured_error(
            canonical_id,
            f"unsupported action for SELECTOR.dispatch: {action.name}",
            on_response,
            on_complete,
        )

    async def _forward(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        label: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Forward wire_dict to the labelled upstream's WebSocket.

        ``wire_dict`` already has ``model`` stripped by the central
        wire-strip in ``translate_query_to_wire`` (the ``model`` field
        is in ``_PROXY_ONLY_FIELDS``). Vanilla KataGo on the upstream
        side never sees the field.

        Disconnected (within retry budget) → structured error: the
        operator may have a transient blip; the SPA can retry. We don't
        queue the query to wait for reconnect — that would conflict
        with the fail-loud posture and the per-query timing semantics
        the SPA expects.
        """
        ws = self._connections.get(label)
        if ws is None:
            logger.warning(
                f"label={label!r} disconnected (within retry budget); "
                f"failing query {canonical_id!r} loudly"
            )
            await self._send_structured_error(
                canonical_id,
                self._DISCONNECTED_MODEL_ERROR_TEMPLATE.format(label=label),
                on_response,
                on_complete,
                field="model",
            )
            return
        _register_query(self._tracker, canonical_id, query)
        self._callbacks[canonical_id] = (on_response, on_complete, label)
        try:
            await ws.send(json.dumps(wire_dict))
        except Exception as e:
            logger.error(
                f"send failed for label={label!r} ({canonical_id}): {e}"
            )
            self._tracker.cancel(canonical_id)
            self._callbacks.pop(canonical_id, None)
            await self._send_structured_error(
                canonical_id,
                self._DISCONNECTED_MODEL_ERROR_TEMPLATE.format(label=label),
                on_response,
                on_complete,
                field="model",
            )
            return
        logger.debug(f"sent to label={label!r}: {json.dumps(wire_dict)}")

    async def _broadcast(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Forward wire_dict to every currently-healthy upstream.

        Used for the actions whose semantic is "reach every backend":

          - QUERY_VERSION — heartbeat fanout. Every downstream LEAF runs
            its own KeepAliveMiddleware against the SELECTOR's
            connection; that middleware's _last_heartbeat resets on
            on_query of a query_version. Without fanout, only the first
            healthy LEAF sees heartbeats, and any other LEAF the
            SELECTOR routes ANALYZE to (by `model`) fires its watchdog
            after idle_timeout on the in-flight query. The SELECTOR
            watchdog postmortem in the umbrella's docs/notes/ records
            the real-deployment failure mode this fanout closes.
          - TERMINATE_ALL — cancel every in-flight query the session
            holds, regardless of which LEAF carries it.
          - CLEAR_CACHE — KataGo's analysis cache is per-LEAF
            (per-subprocess); a SPA-issued clear_cache wants every
            LEAF cleared.

        First response wins. Each upstream emits an independent
        response for the same canonical_id; the first one fires
        on_response and on_complete (the latter pops self._callbacks),
        subsequent responses land at _read_loop's "no callback for
        canonical_id" branch and are silently dropped. The SPA sees
        exactly one response. Aggregation across upstreams isn't
        required for any of the three actions: QUERY_VERSION's
        response is metadata (any healthy LEAF's version answers the
        SPA's identity probe; capabilities_advertiser at Layer 1
        enriches), TERMINATE_ALL/CLEAR_CACHE responses are acks.

        Per-upstream send failures log at error and continue to the
        remaining upstreams. The broadcast aborts only when zero
        sends succeed (no healthy upstream, OR every healthy
        upstream's send raised); a structured error is returned in
        that case rather than a hung canonical.
        """
        healthy = self._healthy_labels()
        if not healthy:
            logger.error(
                f"{query.action.name} requested ({canonical_id}) but no "
                f"healthy upstream available"
            )
            await self._send_structured_error(
                canonical_id,
                self._NO_HEALTHY_UPSTREAM_ERROR,
                on_response,
                on_complete,
            )
            return

        _register_query(self._tracker, canonical_id, query)
        # _callbacks holds a single (on_response, on_complete) tuple.
        # The first upstream's response triggers QUERY_COMPLETE and
        # pops the entry; subsequent responses find no callback and
        # are dropped. The "label" slot carries a synthetic sentinel
        # — broadcast queries are not the target of a
        # SelectorRouter.terminate (the SPA never terminates a
        # query_version, and TERMINATE_ALL is itself the
        # terminate-shaped action).
        self._callbacks[canonical_id] = (
            on_response, on_complete, "__broadcast__",
        )

        sent_to: list[str] = []
        for label in healthy:
            ws = self._connections[label]
            try:
                await ws.send(json.dumps(wire_dict))
            except Exception as e:
                logger.error(
                    f"broadcast send failed for label={label!r} "
                    f"({canonical_id}, {query.action.name}): {e}"
                )
                continue
            sent_to.append(label)

        if not sent_to:
            # All upstreams refused the send. Pop the callbacks we
            # just installed and surface a structured error rather
            # than a hung canonical.
            self._callbacks.pop(canonical_id, None)
            self._tracker.cancel(canonical_id)
            logger.error(
                f"broadcast {query.action.name} ({canonical_id}) "
                f"could not be sent to any of {len(healthy)} "
                f"healthy upstream(s)"
            )
            await self._send_structured_error(
                canonical_id,
                self._NO_HEALTHY_UPSTREAM_ERROR,
                on_response,
                on_complete,
            )
            return

        logger.debug(
            f"broadcast {query.action.name} ({canonical_id}) sent to "
            f"{len(sent_to)}/{len(healthy)} upstream(s): {sent_to}"
        )

    # -----------------------------------------------------------------------
    # Terminate (label-routed)
    # -----------------------------------------------------------------------

    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        """Cancel an in-flight query at its routed-to upstream.

        Looks up the label remembered for ``canonical_id`` in
        ``_callbacks``; routes the terminate to that upstream's
        WebSocket. If the upstream is gone or no in-flight entry
        exists, synthesises a terminate ack so the client doesn't
        freeze. Mirrors ``RelayRouter.terminate()``.
        """
        cb = self._callbacks.pop(canonical_id, None)

        async def _send_synthetic_ack() -> None:
            term_wire_id = f"kg_{secrets.token_hex(6)}"
            synthetic_ack: WireDict = {
                "id": term_wire_id,
                "action": "terminate",
                "terminateId": canonical_id,
            }
            await on_response(term_wire_id, synthetic_ack)
            await on_complete(term_wire_id)

        if cb is None:
            logger.info(
                f"no in-flight entry for {canonical_id!r}; "
                f"synthesising terminate ack"
            )
            await _send_synthetic_ack()
            return

        _, _, label = cb
        self._tracker.cancel(canonical_id)

        ws = self._connections.get(label)
        if ws is None:
            logger.warning(
                f"label={label!r} disconnected; cannot send terminate "
                f"for {canonical_id!r}; synthesising ack"
            )
            await _send_synthetic_ack()
            return

        term_wire_id = f"kg_{secrets.token_hex(6)}"
        term_wire: WireDict = {
            "id": term_wire_id,
            "action": "terminate",
            "terminateId": canonical_id,
        }

        self._tracker.register_count(term_wire_id, 1)
        self._callbacks[term_wire_id] = (on_response, on_complete, label)

        try:
            await ws.send(json.dumps(term_wire))
        except Exception as e:
            logger.warning(
                f"send-terminate failed for label={label!r} "
                f"({canonical_id!r}): {e}"
            )
            self._tracker.cancel(term_wire_id)
            self._callbacks.pop(term_wire_id, None)
            await _send_synthetic_ack()
            return
        logger.debug(f"→ label={label!r}: {term_wire}")

    async def stop(self) -> None:
        """Cancel reader/reconnect tasks and close all upstream connections."""
        for task in list(self._reader_tasks.values()):
            task.cancel()
        for task in list(self._reconnect_tasks):
            task.cancel()
        for ws in list(self._connections.values()):
            try:
                await ws.close()
            except Exception:
                pass
        logger.info("done")


# ---------------------------------------------------------------------------
# EchoRouter
# ---------------------------------------------------------------------------

class EchoRouter(BackendRouter):
    """Returns a synthetic final response immediately.

    Useful for integration tests and protocol fuzzing — the entire proxy
    stack can be exercised without a live KataGo instance or network.
    """

    async def start(self) -> None:
        logger.info("echo mode active")

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"canonical_id={canonical_id}")
        turns = query.analyze_turns if query.analyze_turns else [0]
        for turn in turns:
            synthetic: WireDict = {
                "id": canonical_id,
                "isDuringSearch": False,
                "turnNumber": turn,
                "moveInfos": [],
                "rootInfo": {"scoreLead": 0.0, "visits": 1},
            }
            logger.debug(f"emitting synthetic response turn={turn}")
            await on_response(canonical_id, synthetic)
        await on_complete(canonical_id)

    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        synthetic: WireDict = {
            "id": canonical_id,
            "isDuringSearch": False,
            "turnNumber": 0,
            "action": "terminate",
        }
        logger.debug(f"synthetic ack for canonical_id={canonical_id}")
        await on_response(canonical_id, synthetic)
        await on_complete(canonical_id)

    async def stop(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_router(
    role: str,
    upstream_urls: list[str],
    load_metric: Optional[LoadMetric] = None,
) -> BackendRouter:
    """Construct the appropriate BackendRouter for the given ROLE."""
    role_upper = role.upper()
    logger.info(f"role={role_upper} upstream_urls={upstream_urls}")

    if role_upper == "LEAF":
        return LeafRouter(
            cmd=cfg.KATAGO_CMD,
            startup_timeout_s=cfg.KATAGO_STARTUP_TIMEOUT_S,
        )

    if role_upper == "RELAY":
        if not upstream_urls:
            raise ValueError("RELAY role requires at least one UPSTREAM_URL")
        if load_metric is None:
            load_metric = InFlightQueryLoad()
        return RelayRouter(upstream_urls, load_metric)

    if role_upper == "SELECTOR":
        # SelectorRouter consumes its own dedicated env var
        # (SELECTOR_MODELS) rather than UPSTREAM_URLS — the role's
        # invariant (named, distinguishable upstreams) is structurally
        # different from RELAY's (interchangeable pool), and the env
        # var puts the structural difference in configuration space.
        # Empty configuration / duplicate labels raise
        # SelectorStartupError at start() per ADR-0002.
        return SelectorRouter(models=cfg.SELECTOR_MODELS)

    if role_upper == "ECHO":
        return EchoRouter()

    if role_upper in ("REDIRECT", "DELEGATE"):
        raise ValueError(
            f"Role {role!r} is handled in proxy_server.py, not by make_router"
        )

    raise ValueError(f"Unknown role: {role!r}")
