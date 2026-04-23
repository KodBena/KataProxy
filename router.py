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
from typing import Any, Awaitable, Callable, Optional

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
    "RelayRouter",
    "EchoRouter",
    "make_router",
]

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
    callbacks by canonical_id.  If KataGo exits unexpectedly, the reader
    schedules a restart after a brief delay.
    """

    _RESTART_DELAY_S = 2.0

    def __init__(self, cmd: list[str]) -> None:
        self._cmd = cmd
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._tracker: CompletionTracker[str, int] = CompletionTracker()
        # canonical_id → (on_response, on_complete)
        self._callbacks: dict[str, tuple[OnResponse, OnComplete]] = {}
        self._running = False

    async def start(self) -> None:
        self._running = True
        await self._spawn()
        self._reader_task = asyncio.create_task(
            self._read_loop(), name="leaf-katago-reader"
        )
        assert self._proc is not None  # _spawn guarantees this
        logger.info(f"KataGo pid={self._proc.pid}; reader task started")

    async def _spawn(self) -> None:
        logger.info(f"launching: {self._cmd}")
        self._proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_READER_LIMIT,
        )
        logger.info(f"KataGo started, pid={self._proc.pid}")

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
                logger.info("KataGo stdout EOF; scheduling restart")
                if self._running:
                    await asyncio.sleep(self._RESTART_DELAY_S)
                    try:
                        await self._spawn()
                    except Exception as e:
                        logger.error(f"restart failed: {e}")
                continue

            line = raw.decode().strip()
            logger.debug(f"stdout: {json.dumps(filter_dict(json.loads(line)))}")

            try:
                wire: WireDict = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"JSON error: {e}  raw={line}")
                continue

            canonical_id = wire.get("id")
            if canonical_id is None:
                logger.warning("response missing 'id', skipping")
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

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"canonical_id={canonical_id} action={query.action.name}")
        _register_query(self._tracker, canonical_id, query)
        self._callbacks[canonical_id] = (on_response, on_complete)

        if self._proc is None or self._proc.stdin is None:
            logger.error(f"no subprocess; dropping {canonical_id!r}")
            self._tracker.cancel(canonical_id)
            self._callbacks.pop(canonical_id, None)
            return

        line = json.dumps(wire_dict) + "\n"
        self._proc.stdin.write(line.encode())
        await self._proc.stdin.drain()
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

        if self._proc is None or self._proc.stdin is None:
            logger.warning("no subprocess; cannot send terminate")
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

        self._proc.stdin.write((json.dumps(term_wire) + "\n").encode())
        await self._proc.stdin.drain()
        logger.debug(f"sent {term_wire}")

    async def stop(self) -> None:
        self._running = False
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._proc:
            self._proc.terminate()
            await self._proc.wait()
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
        return LeafRouter(cmd=cfg.KATAGO_CMD)

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
