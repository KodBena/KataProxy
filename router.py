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
from AbstractProxy.katago_proxy import (
    KataGoAction,
    KataGoQuery,
    parse_response_from_wire,
)
import sproxy_config as cfg

from flt import filter_dict

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
                wire: WireDict = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"JSON error: {e}  raw={line}")
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

            sig = self._tracker.signal(
                canonical_id, response.turn_number, response.is_during_search
            )
            logger.info(
                f"canonical_id={canonical_id} "
                f"turn={response.turn_number} during_search={response.is_during_search} "
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

    Selection policy:
      1. Hash canonical_id through the ring → preferred upstream.
      2. Walk the ring until a connected node with load < max_load is found.
      3. If all are over max_load, use the least-loaded connected node.

    Each upstream connection has its own asyncio reader task.  Disconnections
    trigger a reconnect loop with exponential back-off.

    The LoadMetric is called around dispatch and completion; it does not
    participate in any other logic.
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
                logger.debug(
                    f"url={url} raw="
                    f"{json.dumps(filter_dict(json.loads(str(raw_msg))))}"
                )
                try:
                    wire: WireDict = json.loads(raw_msg)
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

                sig = self._tracker.signal(
                    canonical_id, response.turn_number, response.is_during_search
                )
                logger.debug(
                    f"canonical_id={canonical_id} "
                    f"turn={response.turn_number} during={response.is_during_search} "
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
            logger.info(f"scheduling reconnect for {url}")
            asyncio.create_task(self._reconnect_with_backoff(url))

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

    if role_upper == "ECHO":
        return EchoRouter()

    if role_upper in ("REDIRECT", "DELEGATE"):
        raise ValueError(
            f"Role {role!r} is handled in proxy_server.py, not by make_router"
        )

    raise ValueError(f"Unknown role: {role!r}")
