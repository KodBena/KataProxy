"""
proxy_server.py — Layer 1: Client sessions and WebSocket server.

One ClientSession is created per accepted WebSocket connection.  It owns:
  - A ProxyLink wrapped in a TransformedChain — the per-client identity
    translator.  When no Transformer is configured, Transformer.identity()
    is used so the code path is always the same.
  - A CompletionTracker that is shared with the ProxyLink (via the tracker
    argument of make_katago_link), so that register_query_completion and the
    link's internal should_remove predicate operate on the same state.
  - An asyncio.Queue: the Hub puts relabelled wire dicts here; the send loop
    drains it to the WebSocket.
  - A receive loop: parses incoming JSON, calls translate_downstream, and
    hands the result to the Hub.
  - An optional SessionMiddleware: intercepts translated responses (in
    orig_id namespace) to enable stateful async policies such as adaptive
    re-evaluation.  Injected queries go through _handle_query (the full
    Transformer + hub/router pipeline), giving them the same enrichment as
    client-originated queries.

Data flow (downstream, client → engine):
  WebSocket.recv()
    → parse_query_from_wire()           [structural parse]
    → middleware.on_query()             [middleware bookkeeping]
    → chain.translate_downstream()      [orig_id → subscriber_internal_id]
    → register_query_completion()       [populate shared tracker]
    → hub.subscribe()                   [coalescing; may short-circuit]
    → router.dispatch()                 [only if hub.is_new_query]

Data flow (upstream, engine → client):
  router calls hub.on_response(canonical_id, wire)
    → hub relabels wire["id"] = subscriber_internal_id per subscriber
    → wire placed on subscriber.queue
  ClientSession._send_loop dequeues:
    → parse_response_from_wire()        [structural parse]
    → chain.translate_upstream()        [subscriber_internal_id → orig_id,
                                         CompletionTracker advances,
                                         mapping entry removed when done,
                                         Transformer applied on response]
    → middleware.handle_response()      [async policy; may buffer/inject]
    → WebSocket.send() × N             [one send per yielded (orig_id, resp)]
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from collections import OrderedDict
from typing import Callable, Dict, List, Optional

import numpy as np
import websockets
from sortedcontainers import SortedList
from websockets.exceptions import ConnectionClosed

from logging_config import get_logger, log_safe
logger = get_logger("kataproxy")


import sproxy_config as cfg
from AbstractProxy.katago_proxy import (
    KataGoAction,
    KataGoQuery,
    make_katago_link,
    parse_query_from_wire,
    parse_response_from_wire,
    register_query_completion,
    translate_query_to_wire,
    translate_response_to_wire,
    KATAGO_QUERY_PRISMS,
)
from AbstractProxy.proxy_core import CompletionTracker, Envelope, TranslationError, Dispatcher, ProxyLink
from AbstractProxy.protocol_transformer import TransformedChain, Transformer
from pubsub_hub import PubSubHub, LRUCacheStore
from proxy_json import loads_bounded, JsonDepthExceededError
from router import BackendRouter, InFlightQueryLoad, make_router
from session_middleware import (
    IdentityMiddleware,
    MiddlewareChain,
    SessionCapabilities,
    SessionMiddleware,
)

from flt import filter_dict


# Process-wide JSONEncoder.default extension. The body references SortedList
# (from BSA's enrichment output), numpy scalars (also BSA), and Python NaN
# (from edge-case KataGo responses); pre-v1.0.6 the imports were missing and
# the monkeypatched default would NameError on any of those types reaching
# json.dumps. Fixed by adding the imports above (audit L-2). This patch is
# duplicated by bsa.py for the same reasons and survives whichever module
# loads last; consolidating into one place is a future cleanup.

original_default = json.JSONEncoder.default

def global_extended_encoder(self, obj):
    if isinstance(obj, SortedList):
        return list(obj)
    if isinstance(obj, (np.floating, np.integer)):
        if isinstance(obj, np.floating) and np.isnan(obj):
            return None
        return obj.item()
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return original_default(self, obj)

json.JSONEncoder.default = global_extended_encoder


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

TransformerFactory = Callable[[ProxyLink], Transformer]
"""A callable that receives the session's ProxyLink and returns a Transformer.
Called once per ClientSession, so each session gets its own Transformer instance."""

MiddlewareFactory = Callable[[], SessionMiddleware]
"""A callable that returns a fresh SessionMiddleware for each ClientSession.
Must produce a new instance per call; middleware is stateful per session."""


# ---------------------------------------------------------------------------
# _PerIpRateLimit — token bucket keyed by peer IP
# ---------------------------------------------------------------------------

class _PerIpRateLimit:
    """Per-IP token-bucket rate limiter for inbound messages.

    Constructed with a queries-per-minute budget; each :meth:`allow`
    call consumes one token if available and refills at
    ``rate_per_minute / 60`` tokens per second up to a ceiling equal to
    ``rate_per_minute``. A budget of 0 (or any non-positive integer)
    disables the limiter entirely; :meth:`allow` always returns True.

    State is bounded: at most ``max_ips`` peer-IP entries are tracked,
    with LRU eviction when the cap is exceeded. Eviction is a known
    correctness weakness (an evicted IP gets a fresh full bucket on its
    next message) but it bounds memory under sustained scanning. The
    cap is not intended to be hit in normal operation; an operator
    seeing it hit should investigate the traffic shape.
    """

    def __init__(self, rate_per_minute: int, *, max_ips: int = 10000) -> None:
        self._rate_per_sec = rate_per_minute / 60.0  # tokens per second
        self._capacity = max(1, rate_per_minute)
        self._max_ips = max_ips
        # OrderedDict keyed by IP; value is (tokens, last_seen_monotonic).
        self._buckets: "OrderedDict[str, tuple[float, float]]" = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._rate_per_sec > 0

    def allow(self, ip: str) -> bool:
        if not self.enabled:
            return True
        import time as _time
        now = _time.monotonic()
        entry = self._buckets.get(ip)
        if entry is None:
            tokens = float(self._capacity)
        else:
            tokens, last = entry
            elapsed = now - last
            tokens = min(float(self._capacity),
                         tokens + elapsed * self._rate_per_sec)

        if tokens < 1.0:
            self._buckets[ip] = (tokens, now)
            self._buckets.move_to_end(ip)
            return False

        self._buckets[ip] = (tokens - 1.0, now)
        self._buckets.move_to_end(ip)
        # LRU-evict the oldest entry if state has grown past the cap.
        while len(self._buckets) > self._max_ips:
            self._buckets.popitem(last=False)
        return True


# ---------------------------------------------------------------------------
# ClientSession
# ---------------------------------------------------------------------------

class ClientSession:
    """Manages one client WebSocket connection end-to-end.

    Constructed once per accepted connection.  run() drives the full
    lifecycle: concurrent (receive | send) → cleanup.
    """

    def __init__(
        self,
        ws,
        peer: str,
        hub: PubSubHub,
        router: BackendRouter,
        transformer_factory: Optional[TransformerFactory] = None,
        middleware: Optional[SessionMiddleware] = None,
        rate_limit: Optional[_PerIpRateLimit] = None,
    ):
        self._ws = ws
        self._peer = peer
        # Extract IP for the per-IP rate limiter. ws.remote_address is a
        # (host, port) tuple from the websockets library; falls back to the
        # full peer string if the tuple shape isn't available.
        self._peer_ip = (
            ws.remote_address[0]
            if isinstance(getattr(ws, "remote_address", None), tuple)
            else peer
        )
        self._hub = hub
        self._router = router
        self._rate_limit = rate_limit
        self._dispatcher = Dispatcher(KATAGO_QUERY_PRISMS)

        # One tracker per client, shared with the ProxyLink so that
        # register_query_completion and the link's should_remove predicate
        # both operate on the same CompletionTracker instance.
        self._tracker: CompletionTracker = CompletionTracker()
        self._link = make_katago_link(tracker=self._tracker)

        transformer = transformer_factory and transformer_factory(self._link)
        effective_transformer = (
            transformer if transformer is not None else Transformer.identity()
        )

        # Queue: hub puts relabelled wire dicts here; _send_loop drains it.
        self._send_queue: asyncio.Queue = asyncio.Queue()

        # TransformedChain is always pure/synchronous.  Async policy lives
        # exclusively in SessionMiddleware, above this layer.
        self._chain = TransformedChain(self._link, effective_transformer)

        # SessionMiddleware: intercepts translated responses (orig_id namespace).
        self._middleware: SessionMiddleware = middleware or IdentityMiddleware()

        # Maps orig_id → (subscriber_internal_id, canonical_id) for cleanup.
        self._active_queries: Dict[str, tuple] = {}

        logger.debug(
            f"peer={peer} "
            f"transformer={effective_transformer.name!r} "
            f"middleware={type(self._middleware).__name__!r}"
        )

        # Lifecycle hook: middleware sees the capability bundle once,
        # before any queries arrive. Constructed here so the middleware can
        # spawn session-scoped tasks (e.g., the keep-alive watchdog) inside
        # the running event loop. ClientSession is always constructed within
        # _handle_connection (an async coroutine), so an event loop exists.
        caps = SessionCapabilities(
            submit_query=self._handle_query,
            terminate_query=self._terminate_query,
        )
        self._middleware.on_session_start(caps)

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def run(self) -> None:
        logger.debug(f"peer={self._peer} connection accepted")

        recv_task = asyncio.create_task(
            self._receive_loop(), name=f"recv:{self._peer}"
        )
        send_task = asyncio.create_task(
            self._send_loop(), name=f"send:{self._peer}"
        )

        try:
            done, pending = await asyncio.wait(
                [recv_task, send_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            logger.debug(
                f"peer={self._peer} "
                f"one loop finished; cancelling sibling"
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        except Exception:
            logger.exception(f"peer={self._peer} unexpected error in run loop")
        finally:
            await self._cleanup()

    # -----------------------------------------------------------------------
    # Receive (downstream)
    # -----------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        logger.info(f"peer={self._peer} started")
        try:
            async for raw_msg in self._ws:
                # log_safe defends against (a) log injection — a peer
                # cannot insert newlines that forge log lines from inside
                # the formatted record — and (b) unbounded log-line growth
                # for multi-megabyte messages. Default truncation is 256
                # chars, configurable via PROXY_LOG_TRUNCATE.
                logger.debug(
                    f"peer={self._peer} raw={log_safe(raw_msg)}"
                )
                await self._handle_incoming(raw_msg)
        except ConnectionClosed as e:
            logger.info(f"peer={self._peer} closed: {e}")
        except Exception:
            logger.exception(f"peer={self._peer} error in receive loop")

    async def _handle_incoming(self, raw_msg: str) -> None:
        # Per-IP rate limit. Off when the limiter is disabled (the default),
        # so this is a single attribute read on the hot path otherwise.
        if self._rate_limit is not None and not self._rate_limit.allow(self._peer_ip):
            logger.warning(
                f"peer={self._peer_ip} rate limit exceeded; dropping message"
            )
            return

        try:
            outer = loads_bounded(raw_msg, max_depth=cfg.JSON_MAX_DEPTH)
        except JsonDepthExceededError as e:
            logger.error(
                f"peer={self._peer} refused depth-bombed payload: {e} "
                f"raw={log_safe(raw_msg)}"
            )
            return
        except json.JSONDecodeError:
            return

        result = self._dispatcher.match(outer)
        if not result:
            # Differentiate "looks like a near-valid query but malformed"
            # from "fully alien JSON" so a buggy-client signal isn't lost
            # in the bot-noise floor. Both surfaces are ERROR per
            # ADR-0002's loudness hierarchy; the message specificity is
            # what changes.
            if isinstance(outer, dict) and ("action" in outer or "id" in outer):
                logger.error(
                    f"peer={self._peer} malformed protocol message "
                    f"(looks like a query but no prism matched): "
                    f"keys={sorted(outer.keys())} "
                    f"action={log_safe(outer.get('action'))} "
                    f"id_present={'id' in outer} "
                    f"raw={log_safe(raw_msg)}"
                )
            else:
                logger.error(
                    f"peer={self._peer} unknown protocol branch: "
                    f"raw={log_safe(raw_msg)}"
                )
            return

        prism, orig_id, query = result

        if prism.name == "terminate":
            await self._handle_terminate(orig_id, query)
        else:
            # Notify middleware BEFORE routing so it can record expected turn count.
            self._middleware.on_query(orig_id, query)
            await self._handle_query(orig_id, query)

    async def _handle_query(self, orig_id: str, query: KataGoQuery) -> None:
        """Translate and submit a query through the full Transformer + hub/router pipeline.

        Used for both client-originated queries and middleware-injected queries.
        Middleware passes this method as the submit_query callback, so injected
        follow-up queries receive the same enrichment as the originals.
        """
        try:
            env = self._chain.translate_downstream(Envelope(id=orig_id, payload=query))
        except TranslationError as e:
            logger.error(f"translation error: {e}")
            return

        if env is None:
            logger.debug(f"transformer suppressed query {orig_id!r}")
            return

        subscriber_internal_id: str = env.id
        translated_query = env.payload

        register_query_completion(self._tracker, subscriber_internal_id, translated_query)

        is_new, canonical_id = self._hub.subscribe(
            query=translated_query,
            subscriber_internal_id=subscriber_internal_id,
            subscriber_queue=self._send_queue,
        )

        self._active_queries[orig_id] = (subscriber_internal_id, canonical_id)
        logger.debug(
            f"orig={orig_id!r} internal={subscriber_internal_id!r} "
            f"canonical={canonical_id!r} is_new={is_new}"
        )

        if is_new:
            wire = translate_query_to_wire(translated_query, canonical_id)
            await self._router.dispatch(
                canonical_id=canonical_id,
                wire_dict=wire,
                query=translated_query,
                on_response=self._hub.on_response,
                on_complete=self._hub.on_complete,
            )

    async def _handle_terminate(self, orig_id: str, query) -> None:
        try:
            env = self._chain.translate_downstream(Envelope(id=orig_id, payload=query))
        except TranslationError as e:
            logger.warn(
                f"cannot translate terminateId: {e} "
                f"(query may have already completed)"
            )
            return

        if env is None:
            return

        terminate_internal_id: str = env.id
        translated_query = env.payload
        target_internal_id = translated_query.terminate_id

        if target_internal_id is None:
            logger.error(f"terminate missing terminateId after translation")
            return

        canonical_id = self._internal_to_canonical(target_internal_id)
        if canonical_id is None:
            logger.warn(
                f"no canonical_id for "
                f"internal={target_internal_id!r}; query may have already completed"
            )
            return

        was_last = self._hub.unsubscribe(target_internal_id, canonical_id)
        self._active_queries = {
            oid: pair
            for oid, pair in self._active_queries.items()
            if pair[1] != canonical_id
        }

        register_query_completion(self._tracker, terminate_internal_id, translated_query)

        send_queue = self._send_queue

        if was_last:
            # Sole subscriber on this canonical: terminate at the LEAF and
            # forward the real KataGo ack via relabelling. Existing flow.

            async def on_terminate_response(wire_id: str, wire: dict) -> None:
                relabelled = dict(wire)
                relabelled["id"] = terminate_internal_id
                if relabelled.get("terminateId") == canonical_id:
                    relabelled["terminateId"] = target_internal_id
                logger.debug(
                    f"on_terminate_response "
                    f"wire_id={wire_id!r} → terminate_internal={terminate_internal_id!r}"
                )
                await send_queue.put(relabelled)

            async def on_terminate_complete(wire_id: str) -> None:
                logger.debug(
                    f"on_terminate_complete "
                    f"wire_id={wire_id!r}"
                )

            await self._router.terminate(
                canonical_id,
                on_response=on_terminate_response,
                on_complete=on_terminate_complete,
            )
            logger.debug(
                f"canonical={canonical_id!r} "
                f"dispatched for peer={self._peer}"
            )
        else:
            # Other subscribers remain on this canonical. Terminating the
            # LEAF would silently end their analysis (the canonical's
            # response stream would just stop). Synthesize the ack the
            # originating client would have received as a sole subscriber:
            # the KataGo protocol guarantees the ack is a verbatim echo of
            # the terminate query's fields, so the synthesis is
            # deterministic. Both id-fields here are in the internal
            # namespace; _deliver_upstream's translate_upstream pass
            # rewrites them to the client's namespace via the response
            # policy's referential fields (RESPONSE_TERMINATE_ID_FIELD).
            synthesized_ack: dict = {
                "id": terminate_internal_id,
                "action": "terminate",
                "terminateId": target_internal_id,
            }
            logger.debug(
                f"coalescing-transparent terminate: canonical={canonical_id!r} "
                f"retained other subscriber(s); synthesizing ack for "
                f"orig={orig_id!r}"
            )
            await send_queue.put(synthesized_ack)

    async def _terminate_query(self, target_orig_id: str) -> None:
        """Terminate an in-flight ANALYZE query by its client-namespace orig_id.

        Surfaced to middleware via SessionCapabilities.terminate_query so
        session-scoped tasks (the keep-alive watchdog, etc.) can cancel
        stranded queries without needing a synthetic terminate query
        constructed by the client. Wraps _handle_terminate; the synthetic
        wrapper-id follows the `__keepalive_term_<hex>` convention next
        to katago_effectful's `_make_synthetic_id` (audit L-4 preserved
        the `__` separator across both consumers).

        Routes through v1.0.8's coalescing-aware terminate path, so a
        middleware-initiated termination on a coalesced canonical only
        stops this session's view; other subscribers continue.
        """
        synthetic_id = f"__keepalive_term_{uuid.uuid4().hex[:12]}"
        term_query = KataGoQuery(
            action=KataGoAction.TERMINATE,
            terminate_id=target_orig_id,
        )
        await self._handle_terminate(synthetic_id, term_query)

    def _internal_to_canonical(self, subscriber_internal_id: str) -> Optional[str]:
        """Reverse lookup: subscriber_internal_id → canonical_id."""
        for _orig, (iid, cid) in self._active_queries.items():
            if iid == subscriber_internal_id:
                return cid
        return None

    # -----------------------------------------------------------------------
    # Send (upstream)
    # -----------------------------------------------------------------------

    async def _send_loop(self) -> None:
        logger.info(f"peer={self._peer} started")
        try:
            while True:
                wire = await self._send_queue.get()
                logger.debug(
                    f"peer={self._peer} "
                    f"dequeued id={wire.get('id')!r}"
                )
                await self._deliver_upstream(wire)
        except asyncio.CancelledError:
            logger.debug(f"peer={self._peer} cancelled")
            raise
        except ConnectionClosed as e:
            logger.info(f"peer={self._peer} ws closed: {e}")
        except Exception:
            logger.exception(f"peer={self._peer} error in send loop")

    async def _deliver_upstream(self, wire: dict) -> None:
        """Translate one relabelled response to client namespace and send.

        wire["id"] is already subscriber_internal_id (relabelled by the hub).

        Steps:
          1. Parse the structural response from the wire dict.
          2. chain.translate_upstream: subscriber_internal_id → orig_id,
             CompletionTracker advances (mapping entry removed if done),
             Transformer.on_response applied.
          3. middleware.handle_response: async policy; may buffer/inject/re-label.
          4. One WebSocket send per (orig_id, response) pair yielded.
        """
        subscriber_internal_id = wire.get("id")
        logger.debug(
            f"peer={self._peer} "
            f"internal_id={subscriber_internal_id!r}"
        )

        try:
            _, response = parse_response_from_wire(wire)
        except Exception as e:
            logger.error(f"parse error: {e}")
            return

        try:
            env = Envelope(id=subscriber_internal_id, payload=response)
            translated_env = self._chain.translate_upstream(env)
        except TranslationError as e:
            logger.error(
                f"translate_upstream failed: {e} "
                f"(already cleaned up, or duplicate delivery?)"
            )
            return

        if translated_env is None:
            logger.debug(f"transformer suppressed response")
            return

        # Drop the per-session _active_queries entry as soon as the
        # underlying ProxyLink considers the query done. The link's
        # response policy purges the mapping on the QUERY_COMPLETE final,
        # so `forward(orig_id) is None` is the canonical signal that no
        # further translate_upstream calls for this orig_id will happen.
        # Without this cleanup the entry leaked for the session's lifetime
        # — most acutely on the lookup_cache=true replay path, where the
        # orphaned entry was the only consequence of a successful cached
        # delivery (audit M-4).
        completed_orig_id = translated_env.id
        if self._link.mapping.forward(completed_orig_id) is None:
            self._active_queries.pop(completed_orig_id, None)

        # Pass through the middleware.  It yields zero or more (orig_id, response)
        # pairs; each becomes one WebSocket frame.
        try:
            async for out_id, out_resp in self._middleware.handle_response(
                translated_env.id,
                translated_env.payload,
                self._handle_query,        # full transformer + hub/router path
            ):
                out_wire = translate_response_to_wire(out_resp, out_id)
                out_json = json.dumps(out_wire)
                logger.debug(
                    f"peer={self._peer} "
                    f"sending orig_id={out_id!r} "
                    f"out={json.dumps(filter_dict(out_wire))}"
                )
                await self._ws.send(out_json)
        except Exception:
            logger.exception(f"peer={self._peer} middleware error in deliver_upstream")

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------

    async def _cleanup(self) -> None:
        logger.debug(
            f"peer={self._peer} "
            f"unsubscribing {len(self._active_queries)} active query(ies)"
        )

        async def _drop_response(_wid: str, _wire: dict) -> None:
            pass

        async def _drop_complete(_wid: str) -> None:
            pass

        for _orig_id, (iid, cid) in list(self._active_queries.items()):
            was_last = self._hub.unsubscribe(iid, cid)
            if was_last:
                # Sole subscriber departed. The canonical has no consumer
                # left and would otherwise run on the LEAF until natural
                # completion (cheap on bounded analyze, expensive on
                # ponder). Terminate at the router; the WS is already
                # closed, so the ack is dropped.
                try:
                    await self._router.terminate(
                        cid,
                        on_response=_drop_response,
                        on_complete=_drop_complete,
                    )
                except Exception:
                    logger.exception(
                        f"orphan terminate failed: canonical={cid!r}"
                    )
        self._active_queries.clear()

        # Lifecycle hook: middleware releases session-scoped resources
        # (cancels watchdog tasks, etc.). Called after orphan-termination
        # so any middleware that depends on _active_queries observing the
        # cleanup sees the post-cleanup state.
        self._middleware.on_session_end()


# ---------------------------------------------------------------------------
# RedirectSession — REDIRECT / DELEGATE role
# ---------------------------------------------------------------------------

class RedirectSession:
    """Handles the REDIRECT (formerly DELEGATE) role.

    Selects an upstream using round-robin via a
    shared counter owned by ProxyServer (so rotation is server-wide, not
    per-connection), sends a proxy_meta redirect message, and closes.
    """

    def __init__(
        self,
        ws,
        peer: str,
        upstream_urls: List[str],
        rr_state: dict,
    ):
        self._ws = ws
        self._peer = peer
        self._urls = upstream_urls
        self._rr_state = rr_state

    async def run(self) -> None:

        if not self._urls:
            logger.info(
                f"no UPSTREAM_URLS configured; "
                f"closing {self._peer}"
            )
            await self._ws.close(1011, "no upstream configured")
            return

        idx = self._rr_state["counter"] % len(self._urls)
        self._rr_state["counter"] += 1
        target = self._urls[idx]

        redirect_msg = json.dumps({
            "proxy_meta": {"type": "redirect", "url": target}
        })
        logger.info(f"redirecting {self._peer} → {target} (idx={idx})")
        await self._ws.send(redirect_msg)
        await self._ws.close(1000, "redirect issued")


# ---------------------------------------------------------------------------
# ProxyServer
# ---------------------------------------------------------------------------

class ProxyServer:
    """Top-level server: owns the Hub, Router, and session factory."""

    def __init__(
        self,
        transformer_factory: Optional[TransformerFactory] = None,
        middleware_factory: Optional[MiddlewareFactory] = None,
    ):
        self._transformer_factory = transformer_factory
        self._middleware_factory = middleware_factory
        # Hub replay-cache: bounded LRU by default (audit H-2). The
        # LRUCacheStore implementation degrades to a plain dict when its
        # maxsize is non-positive, so PROXY_HUB_CACHE_MAX=0 restores
        # pre-v1.0.4 unbounded semantics for operators who explicitly want
        # them.
        self._hub_cache = LRUCacheStore(maxsize=cfg.HUB_CACHE_MAX)
        self._hub = PubSubHub(cache_store=self._hub_cache)
        self._router: Optional[BackendRouter] = None
        self._rr_state: dict = {"counter": 0}
        # Concurrent-session bookkeeping (audit M-1). Capped via
        # PROXY_MAX_SESSIONS; a non-positive value disables the cap.
        self._active_sessions: int = 0
        # Per-IP rate limiter (audit M-1). Disabled when
        # PROXY_RATELIMIT_PER_IP <= 0; that is the default so deployments
        # behind a reverse proxy do not throttle every user as one IP.
        self._rate_limit = _PerIpRateLimit(cfg.RATELIMIT_PER_IP)

    async def start(self) -> None:

        role = cfg.ROLE.upper()
        if role not in ("REDIRECT", "DELEGATE"):
            self._router = make_router(
                role=role,
                upstream_urls=cfg.UPSTREAM_URLS,
                load_metric=InFlightQueryLoad(),
            )
            await self._router.start()
            logger.info(f"router started for role={role}")

        logger.info(
            f"listening on ws://{cfg.HOST}:{cfg.PORT} role={role} "
            f"max_size={cfg.MAX_MESSAGE_SIZE} max_sessions={cfg.MAX_SESSIONS} "
            f"ratelimit_per_ip={cfg.RATELIMIT_PER_IP}"
        )
        async with websockets.serve(
            self._handle_connection,
            cfg.HOST,
            cfg.PORT,
            max_size=cfg.MAX_MESSAGE_SIZE,
        ):
            await asyncio.Future()  # run forever

    async def _handle_connection(self, ws) -> None:
        peer = str(ws.remote_address)

        # Concurrent-session cap (audit M-1). Refused connections close with
        # WebSocket code 1013 ("try again later"), which the websockets
        # library translates appropriately for the client.
        if cfg.MAX_SESSIONS > 0 and self._active_sessions >= cfg.MAX_SESSIONS:
            logger.warning(
                f"refusing {peer}: session cap reached "
                f"({self._active_sessions} >= {cfg.MAX_SESSIONS})"
            )
            await ws.close(code=1013, reason="server too busy")
            return

        logger.info(f"accepted {peer}")
        self._active_sessions += 1
        try:
            role = cfg.ROLE.upper()
            if role in ("REDIRECT", "DELEGATE"):
                session = RedirectSession(
                    ws=ws,
                    peer=peer,
                    upstream_urls=cfg.UPSTREAM_URLS,
                    rr_state=self._rr_state,
                )
            else:
                # Each session gets its own middleware instance (middleware
                # is stateful).
                middleware = (
                    self._middleware_factory() if self._middleware_factory else None
                )
                session = ClientSession(
                    ws=ws,
                    peer=peer,
                    hub=self._hub,
                    router=self._router,
                    transformer_factory=self._transformer_factory,
                    middleware=middleware,
                    rate_limit=self._rate_limit,
                )

            await session.run()
            logger.info(f"{peer} disconnected")
        finally:
            self._active_sessions -= 1

    async def stop(self) -> None:
        if self._router is not None:
            await self._router.stop()
        logger.info(f"done")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
from contextual import Contextual
from transposition_enricher import transposition_enricher
from baduk import analysis_enricher
from katago_effectful import adaptive_reevaluate
from keep_alive import KeepAliveMiddleware


def _make_middleware() -> SessionMiddleware:
    """Per-session middleware factory.

    Composes the adaptive re-evaluation policy (inner) with the keep-alive
    inactivity watchdog (outer). When KEEP_ALIVE_IDLE_TIMEOUT_SECONDS is
    set to 0 or negative, the watchdog is omitted and the chain degrades
    to bare adaptive_reevaluate.
    """
    base = adaptive_reevaluate(
        worst_quantile=0.25,
        extra_visits=800,
        window_size=3,
    )
    if cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS <= 0:
        return base
    return MiddlewareChain(
        inner=base,
        outer=KeepAliveMiddleware(
            idle_timeout_seconds=cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS,
        ),
    )


async def _main() -> None:
    server = ProxyServer(
        # Pure synchronous content transformations (enrichment, filtering).
        transformer_factory=Contextual(analysis_enricher).then(transposition_enricher),
        # Stateful async policy: adaptive re-evaluation chained with the
        # keep-alive inactivity watchdog. A fresh instance per session
        # because middleware holds per-query state.
        middleware_factory=_make_middleware,
    )
    try:
        await server.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"shutting down")
    finally:
        await server.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
