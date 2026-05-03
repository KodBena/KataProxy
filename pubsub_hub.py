"""
pubsub_hub.py — Layer 2: Query coalescing and response fan-out.

The Hub sits between the client sessions (Layer 1) and the backend router
(Layer 3).  It knows nothing about clients, original IDs, or ProxyLinks.
It speaks exclusively in canonical_ids.

Coalescing semantics
────────────────────
A CoalescingPolicy maps a (canonical_id, KataGoQuery) pair to a hash key.
The key determines whether an incoming query will reuse an in-flight backend
query or become a new one.

  "Capturing" keys: fields *included* in the hash identify the query so that
  two queries with identical values on those fields share a backend slot.
  This is the default, and the correct mental model is:
      "these fields, if equal, mean 'same query'".

  The default policy hashes: rules, komi, board size, moves, and
  analyzeTurns (sorted).  `maxVisits` is NOT included by default, which
  means two clients requesting different visit counts will coalesce — the
  hub does not attempt to reconcile visit-count differences.  This is
  the conservative choice; a more aggressive policy could include maxVisits
  and still coalesce when values match exactly.

  Action queries (terminate, clear_cache, etc.) are always un-coalesceable:
  they get a unique random suffix so they never share a backend slot.  This
  is enforced unconditionally regardless of what the policy returns for the
  content hash.

Cache semantics (analysis-level query cache)
────────────────────────────────────────────
Separate from the coalescing content_hash.  The replay cache keys on the
FULL query payload (action + analyzeTurns + every opaque field) except the
client-specific "id" and the three control flags (cache, lookup_cache,
replay_final_only).  This guarantees that a replayed stream exactly matches
the parameters that would have been sent to the backend.

  • lookup_cache=True  → short-circuit to replay (if exact match in cache)
  • cache=True         → record the live backend stream under the full key
  • replay_final_only  → during replay, drop any isDuringSearch=True messages

Return-path design
──────────────────
When the hub fans out a response for a canonical query (canonical_id), it
must present each subscriber with a wire dict whose "id" field carries
*that subscriber's own internal id* (subscriber_internal_id), not the
canonical one.  This is the relabelling step.  Each client's ProxyLink then
calls translate_upstream on a wire whose "id" is already in its own
namespace, so Layer 1 can operate without any knowledge of coalescing.

Subscription records
────────────────────
Each subscriber is a (subscriber_internal_id, asyncio.Queue) pair.  The
queue carries already-relabelled wire dicts.  client_session.py's send loop
drains its queue and writes to the WebSocket.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from AbstractProxy.katago_proxy import KataGoAction, KataGoQuery

logger = logging.getLogger("kataproxy." + __name__)

__all__ = [
    "CoalescingPolicy",
    "DEFAULT_POLICY",
    "Subscriber",
    "InFlightEntry",
    "PubSubHub",
    "CacheStore",
    "LRUCacheStore",
    "WireDict",
]


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

# A wire-format KataGo message: opaque JSON-shaped dict with at least an "id".
WireDict = dict[str, Any]


# ---------------------------------------------------------------------------
# CacheStore — structural contract for the replay cache backend
# ---------------------------------------------------------------------------

class CacheStore(Protocol):
    """Structural contract for any cache backend the hub will accept.

    Any object implementing these three operations satisfies the contract —
    a plain dict, an LRU wrapper, a Redis adapter, a SQLite-backed store.
    No inheritance required. This is the Interface Segregation Principle:
    the hub depends only on the operations it actually calls.
    """
    def __contains__(self, key: str) -> bool: ...
    def __getitem__(self, key: str) -> list[WireDict]: ...
    def __setitem__(self, key: str, value: list[WireDict]) -> None: ...


class LRUCacheStore:
    """A bounded LRU cache implementing the CacheStore Protocol.

    Eviction order is "least recently set or read" — both ``__setitem__``
    and ``__getitem__`` move the affected key to the most-recent position.
    On overflow the least-recently-used entry is evicted (popitem(last=False)).

    A maxsize of 0 or negative is interpreted as "unbounded": the LRU
    machinery is bypassed entirely and the wrapped store behaves as a plain
    dict. Operators who explicitly want unbounded growth set
    ``PROXY_HUB_CACHE_MAX=0`` (or any non-positive value); the conservative
    default is the finite maxsize wired by ``ProxyServer``.
    """

    def __init__(self, maxsize: int) -> None:
        self._store: "OrderedDict[str, list[WireDict]]" = OrderedDict()
        self._maxsize = maxsize  # ≤ 0 means unbounded

    def __contains__(self, key: object) -> bool:
        return key in self._store

    def __getitem__(self, key: str) -> list[WireDict]:
        value = self._store[key]
        if self._maxsize > 0:
            self._store.move_to_end(key)
        return value

    def __setitem__(self, key: str, value: list[WireDict]) -> None:
        if self._maxsize > 0 and key in self._store:
            self._store.move_to_end(key)
        self._store[key] = value
        if self._maxsize > 0 and len(self._store) > self._maxsize:
            evicted_key, _ = self._store.popitem(last=False)
            logger.debug(f"LRU evicted cache_key={evicted_key[:24]}…")

    def __len__(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# CoalescingPolicy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CoalescingPolicy:
    """Determines whether two queries share a backend slot.

    Fields listed in `capturing_fields` are extracted from a query's opaque
    dict and hashed together with the structural fields (action, turns, …).
    If two queries produce the same hash they are coalesced.

    "Capturing" means: include these fields in the equality test.  Two queries
    are considered the same query if and only if all capturing fields agree.
    Fields absent from this list are invisible to the coalescing logic — they
    neither prevent nor enable coalescing.

    Example: if capturing_fields=("rules", "komi", "moves"), then two queries
    with identical rules/komi/moves but different maxVisits WILL coalesce.
    Add "maxVisits" to capturing_fields if you want separate backend slots for
    different visit budgets.
    """
    capturing_fields: tuple[str, ...] = (
        "rules",
        "komi",
        "boardXSize",
        "boardYSize",
        "moves",
    )

    def query_hash(self, query: KataGoQuery) -> str:
        """Return a stable hex digest identifying this query's coalescing slot.

        analyzeTurns is always included and sorted so [0,5,10] == [10,0,5].
        Action queries always receive a random suffix; this method is called
        before the hub applies the action-query exemption so the caller can
        still see the content hash for logging.  The hub itself appends the
        random suffix unconditionally for action queries.

        The hash is built from a JSON-serialised structured form (a dict
        with action / analyzeTurns / capturing fields), not from
        ``|``-joined ``repr``-quoted strings — the structured form is
        stable across Python releases (json.dumps semantics are specified;
        repr() escaping rules are not). The digest is 128 bits, well
        beyond practical birthday collision in the in-flight pool. See
        audit M-2 for the rationale.
        """
        fields: dict[str, Any] = {
            "action": query.action.name,
            "analyzeTurns": (
                sorted(query.analyze_turns) if query.analyze_turns else []
            ),
        }
        for field_name in self.capturing_fields:
            # `default=str` lets us include any non-JSON-native value the
            # opaque might carry without raising; the resulting string form
            # is still deterministic for any given input.
            fields[field_name] = query.opaque.get(field_name, None)

        payload = json.dumps(fields, sort_keys=True, default=str)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
        logger.debug(f"payload={payload} digest={digest}")
        return digest


DEFAULT_POLICY = CoalescingPolicy()


# ---------------------------------------------------------------------------
# Subscriber record
# ---------------------------------------------------------------------------

@dataclass
class Subscriber:
    """One client's subscription to an in-flight backend query.

    subscriber_internal_id: the canonical_id in *this client's* ProxyLink
        namespace.  The hub writes wire dicts with "id" already set to this
        value before putting them on the queue.
    queue: the asyncio.Queue the client's send loop drains.
    """
    subscriber_internal_id: str
    queue: asyncio.Queue[WireDict]


# ---------------------------------------------------------------------------
# InFlightEntry
# ---------------------------------------------------------------------------

@dataclass
class InFlightEntry:
    """State for one live backend query slot."""
    canonical_id: str             # the id sent to the backend router
    content_hash: str             # for coalescing / logging
    cache_key: str                # FULL analysis-level key for replay cache
    subscribers: list[Subscriber] = field(default_factory=list)
    # Replay-cache support
    record_cache: bool = False
    response_record: list[WireDict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# PubSubHub
# ---------------------------------------------------------------------------

class PubSubHub:
    """Coalesces duplicate queries and fans responses out to subscribers.

    Thread-safety: this class is NOT thread-safe.  It is designed to run
    entirely on one asyncio event loop.  All public methods are synchronous
    (no awaits) except for the two async callbacks passed to the router.
    """

    def __init__(
        self,
        policy: CoalescingPolicy = DEFAULT_POLICY,
        canonical_id_generator: Optional[Callable[[], str]] = None,
        cache_store: Optional[CacheStore] = None,
    ) -> None:
        self._policy = policy
        self._gen = canonical_id_generator or _default_canonical_id
        self._cache_store = cache_store
        # content_hash → InFlightEntry
        self._by_hash: dict[str, InFlightEntry] = {}
        # canonical_id → InFlightEntry (same objects, indexed two ways)
        self._by_canonical: dict[str, InFlightEntry] = {}

    # -----------------------------------------------------------------------
    # Internal cache helpers (future-proof for Redis / external stores)
    # -----------------------------------------------------------------------

    def _get_record(self, cache_key: str) -> Optional[list[WireDict]]:
        """Retrieve cached response stream (raw wire dicts) or None."""
        if self._cache_store is None or cache_key not in self._cache_store:
            return None
        try:
            return self._cache_store[cache_key]
        except Exception as exc:
            logger.warning(
                f"[PubSubHub._get_record] cache lookup failed for key={cache_key[:24]}: {exc}"
            )
            return None

    def _save_record(self, cache_key: str, record: list[WireDict]) -> None:
        """Persist the full response stream under the analysis-level cache key."""
        if self._cache_store is None:
            return
        try:
            # Deep copy isolates the cache from any later mutation of the
            # InFlightEntry list (no mutability leaks).
            self._cache_store[cache_key] = deepcopy(record)
        except Exception as exc:
            logger.warning(
                f"[PubSubHub._save_record] failed to save key={cache_key[:24]}: {exc}"
            )

    # -----------------------------------------------------------------------
    # Full query hash for analysis-level replay cache
    # -----------------------------------------------------------------------

    def _compute_cache_key(self, query: KataGoQuery) -> str:
        """Compute stable hash of the *entire* query (analysis-level cache key).

        Includes: action, analyzeTurns (sorted), and every field in opaque
        EXCEPT the client-specific "id" and the three control flags
        (already stripped in subscribe()).
        This is deliberately different from the coalescing content_hash.
        """
        data: dict[str, Any] = {
            "action": query.action.name,
        }
        if query.analyze_turns:
            data["analyzeTurns"] = sorted(query.analyze_turns)

        # All opaque fields, dropping client "id" if present
        for k, v in query.opaque.items():
            if k != "id":
                data[k] = v

        # Stable JSON (sort_keys guarantees same hash for semantically equal queries)
        payload = json.dumps(data, sort_keys=True)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
        logger.debug(f"full-cache-key={digest[:12]}… action={query.action.name}")
        return digest

    # -----------------------------------------------------------------------
    # Replay engine (short-circuit path)
    # -----------------------------------------------------------------------

    async def _replay_task(
        self,
        subscriber_internal_id: str,
        subscriber_queue: asyncio.Queue[WireDict],
        cached_record: list[WireDict],
        replay_final_only: bool,
    ) -> None:
        """Replay a cached stream into the subscriber's queue.

        Performs all the same relabelling and filtering that a live
        response path would experience, so Layer 1 Transformers still
        execute exactly as they would on fresh backend data.
        """
        logger.debug(
            f"starting replay for {subscriber_internal_id!r} "
            f"({len(cached_record)} messages)"
        )

        for wire in cached_record:
            if replay_final_only and wire.get("isDuringSearch") is True:
                continue

            # CRITICAL: fresh dict for every message – never mutate cached objects
            relabelled = deepcopy(wire)
            relabelled["id"] = subscriber_internal_id

            logger.debug(
                f"→ queue for {subscriber_internal_id!r} "
                f"(qsize={subscriber_queue.qsize()})"
            )
            await subscriber_queue.put(relabelled)
            await asyncio.sleep(0)  # yield to event loop

        logger.debug(
            f"completed replay for {subscriber_internal_id!r}"
        )

    # -----------------------------------------------------------------------
    # Public: subscribe
    # -----------------------------------------------------------------------

    def subscribe(
        self,
        query: KataGoQuery,
        subscriber_internal_id: str,
        subscriber_queue: asyncio.Queue[WireDict],
    ) -> tuple[bool, str]:
        """Register a subscriber for this query.

        Returns (is_new_query, canonical_id).

        is_new_query=True  → caller must dispatch the query to the backend
                             using canonical_id.
        is_new_query=False → an identical backend query is already in flight
                             OR a cache replay was started.
                             Caller should NOT send anything to the backend.

        Action queries bypass coalescing unconditionally.
        Cache-control flags are stripped before hashing so they never affect
        coalescing semantics.
        """
        # -------------------------------------------------------------------
        # 1. Extract + strip client cache flags (must happen before any hash)
        # -------------------------------------------------------------------
        cache_flag = bool(query.opaque.pop("cache", False))
        lookup_cache_flag = bool(query.opaque.pop("lookup_cache", False))
        replay_final_only_flag = bool(query.opaque.pop("replay_final_only", False))

        # -------------------------------------------------------------------
        # 2. Compute coalescing content_hash (partial fields only)
        # -------------------------------------------------------------------
        content_hash = self._policy.query_hash(query)

        if query.action != KataGoAction.ANALYZE:
            content_hash = f"{content_hash}:{secrets.token_hex(8)}"
            logger.debug(
                f"action query {query.action.name} — "
                f"unique hash {content_hash[:32]}"
            )

        # -------------------------------------------------------------------
        # 3. Compute analysis-level cache key (FULL query, separate from content_hash)
        # -------------------------------------------------------------------
        cache_key = self._compute_cache_key(query)

        # -------------------------------------------------------------------
        # 4. Replay-cache short-circuit (analysis-level exact match)
        # -------------------------------------------------------------------
        if lookup_cache_flag and query.action == KataGoAction.ANALYZE:
            cached_record = self._get_record(cache_key)
            if cached_record is not None:
                logger.debug(
                    f"CACHE HIT (analysis-level) for {subscriber_internal_id!r} "
                    f"key={cache_key[:24]}"
                )
                dummy_canonical_id = f"replay_{secrets.token_hex(8)}"
                asyncio.create_task(
                    self._replay_task(
                        subscriber_internal_id=subscriber_internal_id,
                        subscriber_queue=subscriber_queue,
                        cached_record=cached_record,
                        replay_final_only=replay_final_only_flag,
                    )
                )
                return False, dummy_canonical_id

        # -------------------------------------------------------------------
        # 5. Normal coalescing path
        # -------------------------------------------------------------------
        sub = Subscriber(
            subscriber_internal_id=subscriber_internal_id,
            queue=subscriber_queue,
        )

        if content_hash in self._by_hash:
            entry = self._by_hash[content_hash]
            entry.subscribers.append(sub)
            logger.debug(
                f"COALESCED {subscriber_internal_id!r} "
                f"onto canonical={entry.canonical_id!r} "
                f"(now {len(entry.subscribers)} subscriber(s))"
            )
            return False, entry.canonical_id

        # New slot – record whether THIS run should be cached
        canonical_id = self._gen()
        entry = InFlightEntry(
            canonical_id=canonical_id,
            content_hash=content_hash,
            cache_key=cache_key,
            subscribers=[sub],
            record_cache=(cache_flag and query.action == KataGoAction.ANALYZE),
        )
        self._by_hash[content_hash] = entry
        self._by_canonical[canonical_id] = entry
        logger.debug(
            f"NEW slot canonical={canonical_id!r} "
            f"hash={content_hash[:24]} cache_key={cache_key[:24]} for {subscriber_internal_id!r}"
        )
        return True, canonical_id

    # -----------------------------------------------------------------------
    # Public: unsubscribe (on client disconnect before query completes)
    # -----------------------------------------------------------------------

    def unsubscribe(self, subscriber_internal_id: str, canonical_id: str) -> bool:
        """Remove a subscriber from an in-flight slot.

        Returns True iff this call left the subscriber list empty — i.e., the
        canonical is now orphaned and the caller is responsible for terminating
        it at the router. Returns False otherwise: either other subscribers
        remain on the canonical, or the entry was already gone (on_complete
        racing this call). The hub stays free of router dependencies; the
        orphan-cleanup signal is a return value, not a callback.
        """
        entry = self._by_canonical.get(canonical_id)
        if entry is None:
            logger.info(f"no entry for canonical={canonical_id!r}")
            return False

        before = len(entry.subscribers)
        entry.subscribers = [
            s for s in entry.subscribers
            if s.subscriber_internal_id != subscriber_internal_id
        ]

        # Remove from the hash map so new queries cannot coalesce onto
        # this canonical_id. Once a query is targeted for unsubscription /
        # termination, it is no longer valid for "new business".
        if entry.content_hash in self._by_hash:
            # Only pop if it's the same entry (prevents race conditions
            # where a restarted query reclaimed the hash).
            if self._by_hash[entry.content_hash].canonical_id == canonical_id:
                self._by_hash.pop(entry.content_hash)

        logger.debug(
            f"canonical={canonical_id!r} "
            f"{before} → {len(entry.subscribers)} subscriber(s). "
            f"Hash {entry.content_hash[:24]} removed from coalescing pool."
        )

        return len(entry.subscribers) == 0

    # -----------------------------------------------------------------------
    # Router callbacks (async, called from the router's read loop)
    # -----------------------------------------------------------------------

    async def on_response(self, canonical_id: str, wire: WireDict) -> None:
        """Fan out a wire dict to all subscribers of this canonical_id.

        Each subscriber receives a copy with "id" replaced by their own
        subscriber_internal_id.  This is the relabelling step that allows
        each client's ProxyLink to work in its own ID namespace.
        """
        entry = self._by_canonical.get(canonical_id)
        if entry is None:
            logger.info(f"no entry for canonical={canonical_id!r} — discarding")
            return

        # Record raw backend response if this query was marked for caching.
        # We store a fresh copy so the cache never shares mutable objects
        # with the live response path.
        if entry.record_cache:
            entry.response_record.append(deepcopy(wire))

        logger.debug(
            f"canonical={canonical_id!r} "
            f"→ {len(entry.subscribers)} subscriber(s)"
        )

        for sub in list(entry.subscribers):
            relabelled: WireDict = dict(wire)
            relabelled["id"] = sub.subscriber_internal_id
            logger.debug(
                f"→ queue for {sub.subscriber_internal_id!r} "
                f"(qsize={sub.queue.qsize()})"
            )
            await sub.queue.put(relabelled)

    async def on_complete(self, canonical_id: str) -> None:
        """Clean up the hub entry after all responses have been delivered.

        If the query was being recorded, the full response stream is now
        committed to the cache_store under the analysis-level cache_key.
        """
        entry = self._by_canonical.pop(canonical_id, None)
        if entry is None:
            logger.info(f"no entry for canonical={canonical_id!r} (already cleaned?)")
            return

        # Commit recording to the external cache (if any)
        if entry.record_cache and entry.response_record and entry.cache_key:
            self._save_record(entry.cache_key, entry.response_record)
            logger.info(
                f"cached {len(entry.response_record)} responses "
                f"for cache_key={entry.cache_key[:24]}"
            )

        # Only pop if it's the exact same entry to prevent race conditions
        # where a restarted query reclaimed the hash.
        if entry.content_hash in self._by_hash:
            if self._by_hash[entry.content_hash].canonical_id == canonical_id:
                self._by_hash.pop(entry.content_hash)

        logger.info(
            f"cleaned up canonical={canonical_id!r} "
            f"hash={entry.content_hash[:24]}"
        )


# ---------------------------------------------------------------------------
# Canonical ID generator
# ---------------------------------------------------------------------------

def _default_canonical_id() -> str:
    """Generate a short opaque canonical ID for hub-internal use."""
    return f"hub_{secrets.token_hex(10)}"
