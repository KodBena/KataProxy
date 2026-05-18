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
from time import monotonic
from typing import Any, Awaitable, Callable, Dict, List, Optional

import numpy as np
import websockets
from sortedcontainers import SortedList
from websockets.exceptions import ConnectionClosed

from logging_config import filter_dict, get_logger, log_safe  # noqa: E402
logger = get_logger("kataproxy")

from proxy_logging import (
    Direction,
    Event,
    Role,
    get_proxy_logger,
    lifecycle,
)

_log = get_proxy_logger(__name__)


import sproxy_config as cfg
from katago import (
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
    make_katago_link,
    parse_query_from_wire,
    parse_response_from_wire,
    register_query_completion,
    translate_query_to_wire,
    translate_response_to_wire,
    KATAGO_QUERY_PRISMS,
)
from AbstractProxy.proxy_core import (
    CanonicalId,
    ClientId,
    CompletionTracker,
    Dispatcher,
    Envelope,
    InternalId,
    ProxyLink,
    TranslationError,
)
from AbstractProxy.protocol_transformer import TransformedChain, Transformer
from pubsub_hub import PubSubHub, LRUCacheStore
from proxy_json import loads_bounded, JsonDepthExceededError
from router import BackendRouter, InFlightQueryLoad, make_router
from middleware.session_middleware import (
    IdentityMiddleware,
    MiddlewareChain,
    SessionCapabilities,
    SessionMiddleware,
)



# Process-wide JSONEncoder.default extension. The body references SortedList
# (from delta_analysis enrichment output), numpy scalars (also delta_analysis),
# and Python NaN (from edge-case KataGo responses); pre-v1.0.6 the imports
# were missing and the monkeypatched default would NameError on any of those
# types reaching json.dumps. Fixed by adding the imports above (audit L-2).
# This patch is duplicated by delta_analysis.py for the same reasons and
# survives whichever module loads last; consolidating into one place is a
# future cleanup.

original_default = json.JSONEncoder.default

def global_extended_encoder(self: json.JSONEncoder, obj: Any) -> Any:
    if isinstance(obj, SortedList):
        return list(obj)
    if isinstance(obj, (np.floating, np.integer)):
        if isinstance(obj, np.floating) and np.isnan(obj):
            return None
        return obj.item()
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return original_default(self, obj)

# Method-assign on the stdlib JSON encoder is the deliberate
# monkey-patch shape the encoder hooks expect; the assignment is
# load-bearing for SortedList / np.floating handling and intentional.
# Use setattr to express the dynamic-rebind intent so mypy doesn't
# flag a closed-class method-assignment.
setattr(json.JSONEncoder, "default", global_extended_encoder)


# ---------------------------------------------------------------------------
# Role resolution for the structured logger
# ---------------------------------------------------------------------------

def _resolve_role() -> Role:
    """Map cfg.ROLE (env var) to the Role enum.

    Defaults to LEAF when the env var is missing or unrecognised —
    LEAF is the most permissive bind in the structured-logging
    contract (no upstream / label fields required) and the most
    common deployment shape. REDIRECT / DELEGATE roles route via
    RedirectSession, not ClientSession, so this helper is only
    consulted by ClientSession-side code paths.
    """
    try:
        return Role(cfg.ROLE.upper())
    except ValueError:
        return Role.LEAF


# ---------------------------------------------------------------------------
# Response-kind classifier (for lifecycle.forward emission)
# ---------------------------------------------------------------------------

def _classify_response_kind(resp: KataGoResponse) -> str:
    """Classify a KataGo response for the structured ``forward`` event.

    The kind drives the level inside ``lifecycle.forward``: partials are
    DEBUG (high-volume mid-search updates), authoritative responses are
    INFO (one per turn, or one per non-analyze query). Errors are
    distinguished from regular metadata so operators can spot them at
    INFO without having to filter on opaque payload contents.
    """
    if isinstance(resp, MetadataResponse):
        if resp.opaque.get("error") is not None:
            return "error"
        return "metadata"
    return "partial" if resp.is_during_search else "final"


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

TransformerFactory = Callable[
    [ProxyLink[ClientId, InternalId]],
    Transformer[KataGoQuery, KataGoResponse],
]
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
        ws: Any,
        peer: str,
        hub: PubSubHub,
        router: BackendRouter,
        transformer_factory: Optional[TransformerFactory] = None,
        middleware: Optional[SessionMiddleware] = None,
        rate_limit: Optional[_PerIpRateLimit] = None,
        proxy_log: Any = None,
    ) -> None:
        self._ws = ws
        self._peer = peer
        # Structured-logging adapter: bound to role + session for
        # every record this ClientSession emits. Production callers
        # (ProxyServer._handle_connection) pass an already-bound
        # adapter so connect/disconnect emit through the same
        # instance; tests / diagnose scripts construct ClientSession
        # directly and let it bind its own.
        if proxy_log is None:
            proxy_log = get_proxy_logger("kataproxy.proxy_server").bind(
                role=_resolve_role(), session=peer,
            )
        self._log = proxy_log
        # Per-query start-time tracking for the `complete` event's
        # duration_ms field. Keyed by orig_id.
        self._query_started_at: dict[ClientId, float] = {}
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
        self._tracker: CompletionTracker[InternalId, int] = CompletionTracker()
        self._link = make_katago_link(tracker=self._tracker)

        transformer = transformer_factory and transformer_factory(self._link)
        effective_transformer = (
            transformer if transformer is not None else Transformer.identity()
        )

        # Queue: hub puts relabelled wire dicts here; _send_loop drains it.
        self._send_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        # TransformedChain is always pure/synchronous.  Async policy lives
        # exclusively in SessionMiddleware, above this layer.
        self._chain = TransformedChain(self._link, effective_transformer)

        # SessionMiddleware: intercepts translated responses (orig_id namespace).
        self._middleware: SessionMiddleware = middleware or IdentityMiddleware()

        # Maps orig_id → (subscriber_internal_id, canonical_id) for cleanup.
        # Keys are ClientId-namespace (the client's wire-id) per the
        # identity-type-branding migration; values are the
        # (InternalId, CanonicalId) tuple the link + hub produced.
        self._active_queries: Dict[ClientId, tuple[InternalId, CanonicalId]] = {}

        self._log.debug(
            Event.DIAGNOSTIC,
            msg=(
                f"peer={peer} "
                f"transformer={effective_transformer.name!r} "
                f"middleware={type(self._middleware).__name__!r}"
            ),
        )

        # Lifecycle hook: middleware sees the capability bundle once,
        # before any queries arrive. Constructed here so the middleware can
        # spawn session-scoped tasks (e.g., the keep-alive watchdog) inside
        # the running event loop. ClientSession is always constructed within
        # _handle_connection (an async coroutine), so an event loop exists.
        # proxy_log is the session-bound structured-logging adapter;
        # middleware that emits structured records refines via .bind()
        # for sub-contexts (e.g., per-orig_id orchestration coroutines).
        caps = SessionCapabilities(
            submit_query=self._handle_query,
            terminate_query=self._terminate_query,
            proxy_log=self._log,
        )
        self._middleware.on_session_start(caps)

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def run(self) -> None:
        self._log.debug(
            Event.DIAGNOSTIC,
            msg=f"peer={self._peer} connection accepted",
        )

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
            self._log.debug(
                Event.DIAGNOSTIC,
                msg=(
                    f"peer={self._peer} "
                    f"one loop finished; cancelling sibling"
                ),
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        except Exception:
            self._log.exception(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} unexpected error in run loop",
            )
        finally:
            await self._cleanup()

    # -----------------------------------------------------------------------
    # Receive (downstream)
    # -----------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        self._log.info(
            Event.DIAGNOSTIC,
            msg=f"peer={self._peer} started",
        )
        try:
            async for raw_msg in self._ws:
                # log_safe defends against (a) log injection — a peer
                # cannot insert newlines that forge log lines from inside
                # the formatted record — and (b) unbounded log-line growth
                # for multi-megabyte messages. Default truncation is 256
                # chars, configurable via PROXY_LOG_TRUNCATE.
                self._log.debug(
                    Event.DIAGNOSTIC,
                    msg=f"peer={self._peer} raw={log_safe(raw_msg)}",
                )
                await self._handle_incoming(raw_msg)
        except ConnectionClosed as e:
            self._log.info(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} closed: {e}",
            )
        except Exception:
            self._log.exception(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} error in receive loop",
            )

    async def _handle_incoming(self, raw_msg: str) -> None:
        # Per-IP rate limit. Off when the limiter is disabled (the default),
        # so this is a single attribute read on the hot path otherwise.
        if self._rate_limit is not None and not self._rate_limit.allow(self._peer_ip):
            self._log.warning(
                Event.RATE_LIMITED,
                peer_ip=self._peer_ip,
                msg=f"rate limit exceeded for peer={self._peer_ip}",
            )
            return

        try:
            outer = loads_bounded(raw_msg, max_depth=cfg.JSON_MAX_DEPTH)
        except JsonDepthExceededError as e:
            self._log.error(
                Event.PARSE_ERROR,
                error_kind="depth_bomb",
                raw_excerpt=log_safe(raw_msg),
                msg=f"refused depth-bombed payload: {e}",
            )
            return
        except json.JSONDecodeError:
            # Silently drop alien JSON to keep the bot-noise floor low;
            # the malformed-but-near-valid case below is the louder one.
            return

        result = self._dispatcher.match(outer)
        if not result:
            # Differentiate "looks like a near-valid query but malformed"
            # from "fully alien JSON" so a buggy-client signal isn't lost
            # in the bot-noise floor. Both surfaces are ERROR per
            # ADR-0002's loudness hierarchy; the message specificity is
            # what changes.
            if isinstance(outer, dict) and ("action" in outer or "id" in outer):
                self._log.error(
                    Event.PARSE_ERROR,
                    error_kind="malformed_protocol",
                    raw_excerpt=log_safe(raw_msg),
                    keys=sorted(outer.keys()),
                    msg=(
                        f"malformed protocol message "
                        f"(looks like a query but no prism matched)"
                    ),
                )
            else:
                self._log.error(
                    Event.PARSE_ERROR,
                    error_kind="unknown_protocol",
                    raw_excerpt=log_safe(raw_msg),
                    msg="unknown protocol branch",
                )
            return

        prism, raw_orig_id, query = result
        # Construction site: the prism extracted a raw `str` from the
        # wire's `id` field, which is in the client's wire-namespace.
        # Brand it as ClientId here so the downstream call chain
        # (_handle_query, _handle_terminate, middleware.on_query,
        # hub.subscribe, …) is typecheck-coherent end-to-end. The
        # framework's Prism.preview signature returns `tuple[str, A]`
        # generically; this is the protocol-aware brand assignment.
        orig_id: ClientId = ClientId(raw_orig_id)

        if prism.name == "terminate":
            await self._handle_terminate(orig_id, query)
        else:
            # Notify middleware BEFORE routing so it can record expected turn count.
            self._middleware.on_query(orig_id, query)
            await self._handle_query(orig_id, query)

    async def _handle_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        """Translate and submit a query through the full Transformer + hub/router pipeline.

        Used for both client-originated queries and middleware-injected queries.
        Middleware passes this method as the submit_query callback, so injected
        follow-up queries receive the same enrichment as the originals.
        """
        try:
            env = self._chain.translate_downstream(Envelope(id=orig_id, payload=query))
        except TranslationError as e:
            self._log.error(
                Event.DIAGNOSTIC,
                orig=orig_id,
                msg=f"translation error: {e}",
            )
            return

        if env is None:
            self._log.debug(
                Event.DIAGNOSTIC,
                orig=orig_id,
                msg=f"transformer suppressed query {orig_id!r}",
            )
            return

        subscriber_internal_id: InternalId = env.id
        translated_query = env.payload

        register_query_completion(self._tracker, subscriber_internal_id, translated_query)

        is_new, canonical_id = self._hub.subscribe(
            query=translated_query,
            subscriber_internal_id=subscriber_internal_id,
            subscriber_queue=self._send_queue,
            proxy_log=self._log,
            orig_id=orig_id,
        )

        self._active_queries[orig_id] = (subscriber_internal_id, canonical_id)
        # The hub emits the discriminated subscribe / coalesce /
        # cache_hit event itself (Phase 3 migration; was emitted
        # unconditionally as `subscribe` from this site in Phase 2).
        # Per-orig_id timing is still tracked here because the
        # complete event fires from _deliver_upstream and reads back
        # this map.
        self._query_started_at[orig_id] = monotonic()
        self._log.debug(
            Event.DIAGNOSTIC,
            cid=canonical_id, orig=orig_id,
            msg=(
                f"orig={orig_id!r} internal={subscriber_internal_id!r} "
                f"canonical={canonical_id!r} is_new={is_new}"
            ),
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

    async def _handle_terminate(self, orig_id: ClientId, query: KataGoQuery) -> None:
        try:
            env = self._chain.translate_downstream(Envelope(id=orig_id, payload=query))
        except TranslationError as e:
            self._log.warning(
                Event.DIAGNOSTIC,
                orig=orig_id,
                msg=(
                    f"cannot translate terminateId: {e} "
                    f"(query may have already completed)"
                ),
            )
            return

        if env is None:
            return

        terminate_internal_id: InternalId = env.id
        translated_query = env.payload
        target_internal_id = translated_query.terminate_id

        if target_internal_id is None:
            self._log.error(
                Event.DIAGNOSTIC,
                orig=orig_id,
                msg="terminate missing terminateId after translation",
            )
            return

        canonical_id = self._internal_to_canonical(target_internal_id)
        if canonical_id is None:
            self._log.warning(
                Event.DIAGNOSTIC,
                orig=orig_id,
                msg=(
                    f"no canonical_id for "
                    f"internal={target_internal_id!r}; query may have already completed"
                ),
            )
            return

        # Lifecycle emission: terminate received. cid points at the
        # target query's canonical; orig is the terminate request's
        # own id (same convention as subscribe/dispatch — the "what
        # message is being processed right now" identifier).
        lifecycle.terminate_recv(self._log, cid=canonical_id, orig=orig_id)

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
            self._log.info(
                Event.TERMINATE_DISPATCH,
                cid=canonical_id, orig=orig_id,
                direction=Direction.PROXY_TO_UPSTREAM,
                msg=f"terminate → upstream (canonical={canonical_id})",
            )

            async def on_terminate_response(wire_id: str, wire: Dict[str, Any]) -> None:
                relabelled = dict(wire)
                relabelled["id"] = terminate_internal_id
                if relabelled.get("terminateId") == canonical_id:
                    relabelled["terminateId"] = target_internal_id
                self._log.debug(
                    Event.DIAGNOSTIC,
                    cid=canonical_id, orig=orig_id,
                    msg=(
                        f"on_terminate_response "
                        f"wire_id={wire_id!r} → terminate_internal={terminate_internal_id!r}"
                    ),
                )
                await send_queue.put(relabelled)

            async def on_terminate_complete(wire_id: str) -> None:
                # Lifecycle: terminate ack received from upstream and
                # delivered to the client. Fires once per terminate
                # round-trip; coalesced with the wire_id-keyed
                # callback shape.
                lifecycle.terminate_complete(
                    self._log, cid=canonical_id, orig=orig_id,
                )

            await self._router.terminate(
                canonical_id,
                on_response=on_terminate_response,
                on_complete=on_terminate_complete,
            )
            self._log.debug(
                Event.DIAGNOSTIC,
                cid=canonical_id, orig=orig_id,
                msg=(
                    f"canonical={canonical_id!r} "
                    f"dispatched for peer={self._peer}"
                ),
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
            synthesized_ack: Dict[str, Any] = {
                "id": terminate_internal_id,
                "action": "terminate",
                "terminateId": target_internal_id,
            }
            lifecycle.terminate_synthesized(
                self._log, cid=canonical_id, orig=orig_id, cause="coalesced",
            )
            await send_queue.put(synthesized_ack)

    async def _terminate_query(self, target_orig_id: ClientId) -> None:
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
        synthetic_id = ClientId(f"__keepalive_term_{uuid.uuid4().hex[:12]}")
        term_query = KataGoQuery(
            action=KataGoAction.TERMINATE,
            terminate_id=target_orig_id,
        )
        await self._handle_terminate(synthetic_id, term_query)

    def _internal_to_canonical(
        self, subscriber_internal_id: InternalId,
    ) -> Optional[CanonicalId]:
        """Reverse lookup: subscriber_internal_id → canonical_id."""
        for _orig, (iid, cid) in self._active_queries.items():
            if iid == subscriber_internal_id:
                return cid
        return None

    # -----------------------------------------------------------------------
    # Send (upstream)
    # -----------------------------------------------------------------------

    async def _send_loop(self) -> None:
        self._log.info(
            Event.DIAGNOSTIC,
            msg=f"peer={self._peer} started",
        )
        try:
            while True:
                wire = await self._send_queue.get()
                self._log.debug(
                    Event.DIAGNOSTIC,
                    msg=(
                        f"peer={self._peer} "
                        f"dequeued id={wire.get('id')!r}"
                    ),
                )
                await self._deliver_upstream(wire)
        except asyncio.CancelledError:
            self._log.debug(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} cancelled",
            )
            raise
        except ConnectionClosed as e:
            self._log.info(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} ws closed: {e}",
            )
        except Exception:
            self._log.exception(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} error in send loop",
            )

    async def _deliver_upstream(self, wire: Dict[str, Any]) -> None:
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
        raw_internal_id = wire.get("id")
        self._log.debug(
            Event.DIAGNOSTIC,
            msg=(
                f"peer={self._peer} "
                f"internal_id={raw_internal_id!r}"
            ),
        )

        if raw_internal_id is None:
            self._log.warning(
                Event.DIAGNOSTIC,
                msg="response missing 'id', skipping",
            )
            return
        # The hub relabels wire["id"] to the subscriber_internal_id
        # for this session before posting onto _send_queue (see
        # pubsub_hub.PubSubHub.fanout); the brand is justified by that
        # contract.
        subscriber_internal_id = InternalId(raw_internal_id)

        try:
            _, response = parse_response_from_wire(wire)
        except Exception as e:
            self._log.error(
                Event.DIAGNOSTIC,
                msg=f"parse error: {e}",
            )
            return

        try:
            env = Envelope(id=subscriber_internal_id, payload=response)
            translated_env = self._chain.translate_upstream(env)
        except TranslationError as e:
            self._log.error(
                Event.DIAGNOSTIC,
                msg=(
                    f"translate_upstream failed: {e} "
                    f"(already cleaned up, or duplicate delivery?)"
                ),
            )
            return

        if translated_env is None:
            self._log.debug(
                Event.DIAGNOSTIC,
                msg="transformer suppressed response",
            )
            return

        # Capture cid (canonical_id) before the pop below; lifecycle.forward
        # emissions inside the middleware loop need it after _active_queries
        # has been cleared on the terminal response.
        parent_orig_id = translated_env.id
        parent_active = self._active_queries.get(parent_orig_id)
        parent_cid = parent_active[1] if parent_active is not None else parent_orig_id

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
            active_entry = self._active_queries.pop(completed_orig_id, None)
            # Lifecycle emission: query lifecycle ended cleanly.
            # Duration is the gap from subscribe to completion (the
            # `_query_started_at` ts was set in _handle_query). For
            # injected sub-queries (orchestration's spawn path) that
            # bypass _handle_query the ts is absent and we omit
            # duration_ms.
            started_at = self._query_started_at.pop(completed_orig_id, None)
            duration_ms = (
                int((monotonic() - started_at) * 1000.0)
                if started_at is not None
                else None
            )
            if active_entry is not None:
                cid = active_entry[1]
                lifecycle.complete(
                    self._log,
                    cid=cid, orig=completed_orig_id,
                    duration_ms=duration_ms,
                )

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
                self._log.debug(
                    Event.DIAGNOSTIC,
                    cid=parent_cid, orig=out_id,
                    msg=(
                        f"peer={self._peer} "
                        f"sending orig_id={out_id!r} "
                        f"out={json.dumps(filter_dict(out_wire))}"
                    ),
                )
                # Lifecycle emission: demand-edge timestamp. Kind drives
                # the level (partial → DEBUG, final/metadata/error → INFO)
                # inside lifecycle.forward; see the helper for rationale.
                lifecycle.forward(
                    self._log,
                    cid=parent_cid, orig=out_id,
                    kind=_classify_response_kind(out_resp),
                )
                await self._ws.send(out_json)
        except Exception:
            self._log.exception(
                Event.DIAGNOSTIC,
                msg=f"peer={self._peer} middleware error in deliver_upstream",
            )

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------

    async def _cleanup(self) -> None:
        self._log.debug(
            Event.DIAGNOSTIC,
            msg=(
                f"peer={self._peer} "
                f"unsubscribing {len(self._active_queries)} active query(ies)"
            ),
        )

        async def _drop_response(_wid: str, _wire: Dict[str, Any]) -> None:
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
                    self._log.exception(
                        Event.DIAGNOSTIC,
                        cid=cid,
                        msg=f"orphan terminate failed: canonical={cid!r}"
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
        ws: Any,
        peer: str,
        upstream_urls: List[str],
        rr_state: Dict[str, Any],
    ) -> None:
        self._ws = ws
        self._peer = peer
        self._urls = upstream_urls
        self._rr_state = rr_state

    async def run(self) -> None:

        if not self._urls:
            _log.info(
                Event.DIAGNOSTIC,
                msg=(
                    f"no UPSTREAM_URLS configured; "
                    f"closing {self._peer}"
                ),
            )
            await self._ws.close(1011, "no upstream configured")
            return

        idx = self._rr_state["counter"] % len(self._urls)
        self._rr_state["counter"] += 1
        target = self._urls[idx]

        redirect_msg = json.dumps({
            "proxy_meta": {"type": "redirect", "url": target}
        })
        _log.info(
            Event.DIAGNOSTIC,
            msg=f"redirecting {self._peer} → {target} (idx={idx})",
        )
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
        self._rr_state: Dict[str, Any] = {"counter": 0}
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
            _log.info(
                Event.DIAGNOSTIC,
                msg=f"router started for role={role}",
            )

        _log.info(
            Event.DIAGNOSTIC,
            msg=(
                f"listening on ws://{cfg.HOST}:{cfg.PORT} role={role} "
                f"max_size={cfg.MAX_MESSAGE_SIZE} max_sessions={cfg.MAX_SESSIONS} "
                f"ratelimit_per_ip={cfg.RATELIMIT_PER_IP}"
            ),
        )
        async with websockets.serve(
            self._handle_connection,
            cfg.HOST,
            cfg.PORT,
            max_size=cfg.MAX_MESSAGE_SIZE,
        ):
            await asyncio.Future()  # run forever

    async def _handle_connection(self, ws: Any) -> None:
        peer = str(ws.remote_address)
        peer_ip = (
            ws.remote_address[0]
            if isinstance(getattr(ws, "remote_address", None), tuple)
            else peer
        )

        # Session-scoped structured-log adapter. Bound to role + session
        # at the connection boundary so connect / connect_refused /
        # disconnect emit through the same instance, and the same
        # adapter is forwarded into ClientSession (for in-session
        # events like parse_error / subscribe / dispatch / terminate /
        # complete) so everything in one connection's stream shares
        # the same context.
        conn_log = get_proxy_logger("kataproxy.proxy_server").bind(
            role=_resolve_role(), session=peer,
        )

        # Concurrent-session cap (audit M-1). Refused connections close with
        # WebSocket code 1013 ("try again later"), which the websockets
        # library translates appropriately for the client.
        if cfg.MAX_SESSIONS > 0 and self._active_sessions >= cfg.MAX_SESSIONS:
            conn_log.warning(
                Event.CONNECT_REFUSED,
                peer_ip=peer_ip, cause="max_sessions",
                msg=(
                    f"refused {peer}: session cap reached "
                    f"({self._active_sessions} >= {cfg.MAX_SESSIONS})"
                ),
            )
            await ws.close(code=1013, reason="server too busy")
            return

        lifecycle.connect(conn_log, peer_ip=peer_ip)
        self._active_sessions += 1
        try:
            role = cfg.ROLE.upper()
            session: "RedirectSession | ClientSession"
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
                # _router is None pre-start(), set non-None in start().
                # Accepting connections without start() being called is a
                # construction-order bug — assert rather than degrade.
                assert self._router is not None, (
                    "ProxyServer._accept_connection called before start()"
                )
                session = ClientSession(
                    ws=ws,
                    peer=peer,
                    hub=self._hub,
                    router=self._router,
                    transformer_factory=self._transformer_factory,
                    middleware=middleware,
                    rate_limit=self._rate_limit,
                    proxy_log=conn_log,
                )

            await session.run()
            lifecycle.disconnect(conn_log)
        finally:
            self._active_sessions -= 1

    async def stop(self) -> None:
        if self._router is not None:
            await self._router.stop()
        _log.info(
            Event.DIAGNOSTIC,
            msg="done",
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
from contextual import Contextual
from transformers.transposition_enricher import transposition_enricher
from transformers.analysis_enricher import analysis_enricher
from transformers.capability_gate import capability_gate
from transformers.capabilities_advertiser import capabilities_advertiser
from middleware.adaptive_reevaluate import adaptive_reevaluate
from middleware.capability_gate import CapabilityGatedMiddleware
from middleware.keep_alive import KeepAliveMiddleware


def _make_middleware() -> SessionMiddleware:
    """Per-session middleware factory.

    Composes the adaptive re-evaluation policy (inner) with the keep-alive
    inactivity watchdog (outer). adaptive_reevaluate is wrapped in a
    CapabilityGatedMiddleware so per-query opt-out of `adaptive_reevaluate`
    bypasses the middleware entirely (no observation, no submit_query, no
    GPU cycles spent on deeper analysis). KeepAliveMiddleware is *not*
    capability-gated: it is a watchdog that should always run.

    When KEEP_ALIVE_IDLE_TIMEOUT_SECONDS is set to 0 or negative, the
    watchdog is omitted and the chain degrades to bare
    CapabilityGatedMiddleware around adaptive_reevaluate.
    """
    base = CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=1,
        )(),  # () because adaptive_reevaluate now returns a factory
              # (refactored to use the orchestration primitive in
              # v1.0.16; see roadmap-orchestration-middleware.md).
              # window_size=1 reflects v1.0.23's semantic default —
              # re-evaluate only the moves flagged as worst, no
              # contextual siblings. The windowing infrastructure
              # (move-space, same-color predecessor) is in place for
              # users wanting context. See
              # docs/roadmap-adaptive-selector-pluggability.md.
    )
    if cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS <= 0:
        return base
    return MiddlewareChain(
        inner=base,
        outer=KeepAliveMiddleware(
            idle_timeout_seconds=cfg.KEEP_ALIVE_IDLE_TIMEOUT_SECONDS,
        ),
    )


def _build_advertised_capabilities() -> Dict[str, Dict[str, Any]]:
    """Construct the server's capability advertisement at startup.

    delta_analysis and adaptive_reevaluate are unconditionally wired
    below in _main, so they are unconditionally advertised. transposition
    is advertised iff the native go_transposition module is importable;
    this mirrors the runtime check in
    transformers/transposition_enricher.py and keeps the advertisement
    honest about what the proxy can actually do.

    selector (Phase 2+3) is advertised iff cfg.ROLE == "SELECTOR" — the
    role-gated routing capability that tells SPA clients to render the
    model dropdown. Unlike the behavioural capabilities, `selector` is
    not engaged per-query via the capabilities field; routing flows
    through the dedicated `model` field on the analysis query. The
    advertisement is presence-as-signal so the SPA can feature-detect
    and decide whether to render the model UI.

    All capabilities ship with empty metadata in Phase 1+2+3 — the
    metadata-schema-formalisation per the dispatch's Q4 answer is
    on the *query* side (e.g. adaptive_reevaluate's worst_quantile /
    extra_visits overrides), not the advertisement side.
    """
    advertised: Dict[str, Dict[str, Any]] = {
        "delta_analysis": {},
        "adaptive_reevaluate": {},
    }
    # v1.0.26 — enumerate proxy-hosted learned-VF versions for the SPA
    # to discover and offer in its value-function dropdown. Empty
    # (or absent if lightgbm isn't installed) means the proxy has
    # no learned predictors; SPA hides the option. See
    # docs/dispatch/proxy-to-frontend-learned-vf.md for the wire shape.
    try:
        from middleware.learned_value_fn import get_registry
        versions = get_registry().available_versions()
        if versions:
            advertised["adaptive_reevaluate"]["available_value_bindings"] = versions
    except Exception:
        # Registry construction failure (e.g., directory permissions
        # at startup) should not block server startup. The learned VF
        # is simply unavailable for this run; advertisement omits it.
        pass
    try:
        import go_transposition  # noqa: F401
        advertised["transposition"] = {}
    except ImportError:
        pass
    if cfg.ROLE.upper() == "SELECTOR":
        advertised["selector"] = {}
    return advertised


async def _main() -> None:
    # Install the structured-logging handler at startup so the
    # ProxyLogger emissions from ClientSession / routers / etc. land
    # on the chosen formatter (console by default when stderr is a
    # tty; logfmt otherwise). Idempotent — calling twice is a no-op.
    # See proxy_logging.formatters.configure_logging_from_env() and
    # proxy/docs/logging-design.md §6 / §8 for the env-var matrix.
    from proxy_logging import configure_logging_from_env, set_process_role
    configure_logging_from_env()
    # Bind the process role onto every module-level get_proxy_logger
    # call. ClientSession's per-session log refines further; bare
    # module-level loggers (transformers, hub, etc.) inherit role
    # from this single set point.
    set_process_role(_resolve_role())

    # Per-query capability gating is always wired — legacy clients (no
    # capabilities field on the query) trigger auto-engage on the gate
    # side, so all transformers/middleware run as in v1.0.13. The
    # advertiser is gated by PROXY_ADVERTISE_CAPABILITIES so a
    # v1.0.13 → v1.0.14 update is byte-identical on the wire by
    # default; operators opt in to advertisement explicitly when
    # they're ready for capability-aware clients to engage the new
    # per-query contract.
    chain = (
        Contextual(capability_gate("delta_analysis", analysis_enricher))
        .then(capability_gate("transposition", transposition_enricher))
    )
    if cfg.ADVERTISE_CAPABILITIES:
        advertised_caps = _build_advertised_capabilities()
        _log.info(
            Event.DIAGNOSTIC,
            msg=(
                f"advertising capabilities: {sorted(advertised_caps.keys())} "
                f"(PROXY_ADVERTISE_CAPABILITIES enabled)"
            ),
        )
        chain = chain.then(capabilities_advertiser(advertised_caps))
    else:
        _log.info(
            Event.DIAGNOSTIC,
            msg=(
                "PROXY_ADVERTISE_CAPABILITIES is disabled (default); "
                "query_version responses pass through unchanged. Set "
                "PROXY_ADVERTISE_CAPABILITIES=true to advertise per-query "
                "capabilities to capability-aware clients. Per-query gating "
                "remains active on the proxy side regardless."
            ),
        )

    server = ProxyServer(
        transformer_factory=chain,
        # Stateful async policy: capability-gated adaptive re-evaluation
        # chained with the keep-alive inactivity watchdog. A fresh
        # instance per session because middleware holds per-query state.
        middleware_factory=_make_middleware,
    )
    try:
        await server.start()
    except (KeyboardInterrupt, asyncio.CancelledError):
        _log.info(
            Event.DIAGNOSTIC,
            msg="shutting down",
        )
    finally:
        await server.stop()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
