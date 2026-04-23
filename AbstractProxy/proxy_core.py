"""
proxy_core.py — Reusable abstractions for identity-translating protocol proxies.

These components are protocol-agnostic. They implement the structural pattern:
messages carry an envelope identity; some messages carry referential identities
in their payload that name other messages. A proxy translates both kinds.

The abstractions are:

    Prism               — bidirectional lens between wire and abstract payloads
    Dispatcher          — first-match router over a sequence of Prisms
    ReferentialField    — a named lens into a payload's cross-reference slot
    IdMapping           — bidirectional identity map with pluggable generation
    CompletionTracker   — tracks multi-part completion (e.g. multiple turns)
    IdPolicy            — decides registration and removal lifecycle
    ProxyLink           — one boundary: mapping + policy + translation logic
    ProxyChain          — flat sequence of links, fold in each direction
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Generic,
    Hashable,
    Iterable,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
)

import logging

logger = logging.getLogger("kataproxy.proxy_core")

__all__ = [
    "Prism",
    "Dispatcher",
    "Direction",
    "ReferentialField",
    "Translation",
    "TranslationError",
    "IdGenerator",
    "IdMapping",
    "CompletionKey",
    "CompletionSignal",
    "CompletionTracker",
    "IdPolicy",
    "translate_referentials",
    "Envelope",
    "ProxyLink",
    "ProxyChain",
]

# ---------------------------------------------------------------------------
# Type variables
# ---------------------------------------------------------------------------

I = TypeVar("I", bound=Hashable)  # identity representation
P = TypeVar("P")                   # payload
T = TypeVar("T", bound=Hashable)  # sub-task discriminator (e.g. turn number)
A = TypeVar("A")                   # abstract-level payload (e.g. KataGoQuery)


# ---------------------------------------------------------------------------
# Prism — bidirectional lens between wire format and abstract payload
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Prism(Generic[P, A]):
    """
    A bidirectional lens between a protocol-level payload P (e.g. a raw wire
    dict) and an abstract payload A (e.g. KataGoQuery).

    preview: P -> Optional[(envelope_id, A)]
        Attempt to match and parse a wire payload. Returns None if this Prism
        does not handle the given payload shape.

    review: (A, str) -> P
        Reconstruct the wire payload from an abstract payload and its
        envelope_id. Note: envelope_id is the second argument, following the
        convention of translate_query_to_wire(payload, id).
    """
    name: str
    preview: Callable[[P], Optional[tuple[str, A]]]
    review: Callable[[A, str], P]


# ---------------------------------------------------------------------------
# Dispatcher — first-match router over a Prism sequence
# ---------------------------------------------------------------------------

class Dispatcher(Generic[P]):
    """Routes an incoming wire payload to the first matching Prism."""

    def __init__(self, prisms: Iterable[Prism[P, Any]]) -> None:
        self._prisms: list[Prism[P, Any]] = list(prisms)

    def match(self, raw_payload: P) -> Optional[tuple[Prism[P, Any], str, Any]]:
        """Return (matched_prism, envelope_id, abstract_payload) or None."""
        for prism in self._prisms:
            result = prism.preview(raw_payload)
            if result is not None:
                orig_id, abstract_payload = result
                return prism, orig_id, abstract_payload
        return None


# ---------------------------------------------------------------------------
# Direction
# ---------------------------------------------------------------------------

class Direction(Enum):
    DOWNSTREAM = auto()  # client → engine
    UPSTREAM = auto()    # engine → client


# ---------------------------------------------------------------------------
# ReferentialField
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReferentialField(Generic[P, I]):
    """A lens focusing on a cross-referencing identity inside a payload.

    Some messages name *other* messages by ID. The proxy must translate these
    inner references in addition to the envelope ID. Factoring this out as a
    first-class object means:

      1. The proxy core never parses protocol-specific payloads.
      2. Protocol definitions declaratively list their cross-references.
      3. Fanout routers can inspect which backend owns the referenced ID.

    Attributes:
        name:  human-readable label (e.g. "terminateId") for logging/audit.
        get:   extract the referenced ID, or None if absent.
        set:   return a new payload with the referenced ID replaced.
    """
    name: str
    get: Callable[[P], Optional[I]]
    set: Callable[[P, I], P]


# ---------------------------------------------------------------------------
# Translation pair
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Translation(Generic[I]):
    """A recorded pair: one upstream ID ↔ one downstream ID."""
    upstream: I
    downstream: I


# ---------------------------------------------------------------------------
# TranslationError
# ---------------------------------------------------------------------------

class TranslationError(Exception):
    """Raised when an ID cannot be resolved during translation."""

    def __init__(self, kind: str, identity: Any) -> None:
        self.kind = kind
        self.identity = identity
        super().__init__(f"{kind}: {identity!r}")


# ---------------------------------------------------------------------------
# IdGenerator — pluggable ID minting strategy
# ---------------------------------------------------------------------------

class IdGenerator(Protocol[I]):
    """Strategy for generating downstream IDs.

    Swapping this is how you get:
      - opaque UUIDs (isolation)
      - prefixed IDs (debuggability)
      - deterministic IDs (reproducibility in tests)
    """
    def __call__(self, upstream_id: I) -> I: ...


# ---------------------------------------------------------------------------
# IdMapping
# ---------------------------------------------------------------------------

class IdMapping(Generic[I]):
    """Thread-safe bidirectional identity map.

    This is the *state* of a proxy link: two dicts protected by one lock.

    The `generator` parameter decouples ID-minting policy from storage,
    enabling load-balancing (tagged IDs) and fanout (prefixed IDs)
    without touching this class.
    """

    def __init__(self, generator: IdGenerator[I]) -> None:
        self._gen = generator
        self._fwd: dict[I, I] = {}   # upstream → downstream
        self._rev: dict[I, I] = {}   # downstream → upstream
        self._lock = threading.Lock()

    def register(self, upstream_id: I) -> I:
        """Register an upstream ID, returning the (possibly cached) downstream ID."""
        with self._lock:
            if upstream_id in self._fwd:
                return self._fwd[upstream_id]
            downstream_id = self._gen(upstream_id)
            self._fwd[upstream_id] = downstream_id
            self._rev[downstream_id] = upstream_id
            return downstream_id

    def forward(self, upstream_id: I) -> Optional[I]:
        """Upstream → downstream lookup."""
        with self._lock:
            return self._fwd.get(upstream_id)

    def reverse(self, downstream_id: I) -> Optional[I]:
        """Downstream → upstream lookup."""
        with self._lock:
            return self._rev.get(downstream_id)

    def remove_by_upstream(self, upstream_id: I) -> Optional[Translation[I]]:
        with self._lock:
            downstream_id = self._fwd.pop(upstream_id, None)
            if downstream_id is None:
                return None
            self._rev.pop(downstream_id, None)
            return Translation(upstream_id, downstream_id)

    def remove_by_downstream(self, downstream_id: I) -> Optional[Translation[I]]:
        with self._lock:
            upstream_id = self._rev.pop(downstream_id, None)
            if upstream_id is None:
                return None
            self._fwd.pop(upstream_id, None)
            return Translation(upstream_id, downstream_id)

    def snapshot(self) -> list[Translation[I]]:
        with self._lock:
            return [Translation(u, d) for u, d in self._fwd.items()]


# ---------------------------------------------------------------------------
# CompletionTracker
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CompletionKey(Generic[I, T]):
    """Identifies one sub-task within a multi-part query."""
    query_id: I
    discriminator: T


class CompletionSignal(Enum):
    PARTIAL = auto()        # mid-search update; ignore
    TURN_COMPLETE = auto()  # one sub-task done
    QUERY_COMPLETE = auto() # all sub-tasks done; safe to remove mapping


class CompletionTracker(Generic[I, T]):
    """Tracks how many sub-tasks remain for each query.

    For KataGo: a query with analyzeTurns=[0,3,7] registers three outstanding
    turns. Each final (isDuringSearch=False) response for a turn decrements
    the count. When zero, QUERY_COMPLETE is returned.

    Two registration modes:

    register(query_id, discriminators)
        Discriminator-based: each final response must carry a known
        discriminator value (e.g. turn number). Use when analyzeTurns is
        explicit and exact turn numbers are known in advance.

    register_count(query_id, n)
        Count-based: any n final responses complete the query, regardless of
        discriminator value. Use for single-turn queries (analyzeTurns absent)
        and action queries where the response turn number is not known ahead
        of time.
    """

    def __init__(self) -> None:
        self._outstanding: dict[I, set[T]] = {}
        self._counts: dict[I, int] = {}
        self._lock = threading.Lock()

    def register(self, query_id: I, discriminators: Iterable[T]) -> None:
        with self._lock:
            self._outstanding[query_id] = set(discriminators)

    def register_count(self, query_id: I, n: int) -> None:
        """Register exactly n expected final responses, ignoring discriminator values."""
        logger.debug(f"query_id={query_id!r} n={n}")
        with self._lock:
            self._counts[query_id] = n

    def signal(
        self, query_id: I, discriminator: T, is_partial: bool
    ) -> CompletionSignal:
        if is_partial:
            return CompletionSignal.PARTIAL
        with self._lock:
            if query_id in self._counts:
                self._counts[query_id] -= 1
                remaining = self._counts[query_id]
                logger.debug(
                    f"count-mode query_id={query_id!r} "
                    f"discriminator={discriminator!r} remaining={remaining}"
                )
                if remaining <= 0:
                    del self._counts[query_id]
                    return CompletionSignal.QUERY_COMPLETE
                return CompletionSignal.TURN_COMPLETE

            remaining_set = self._outstanding.get(query_id)
            if remaining_set is None:
                logger.warning(
                    f"unknown query_id={query_id!r}; returning QUERY_COMPLETE"
                )
                return CompletionSignal.QUERY_COMPLETE
            remaining_set.discard(discriminator)
            logger.debug(
                f"disc-mode query_id={query_id!r} "
                f"discriminator={discriminator!r} remaining={remaining_set!r}"
            )
            if not remaining_set:
                del self._outstanding[query_id]
                return CompletionSignal.QUERY_COMPLETE
            return CompletionSignal.TURN_COMPLETE

    def cancel(self, query_id: I) -> None:
        """Remove tracking state for a query that will never receive a response.

        Safe to call if the query was never registered or has already completed.
        """
        logger.debug(f"query_id={query_id!r}")
        with self._lock:
            self._outstanding.pop(query_id, None)
            self._counts.pop(query_id, None)

    def outstanding(self, query_id: I) -> set[T]:
        with self._lock:
            return set(self._outstanding.get(query_id, ()))


# ---------------------------------------------------------------------------
# IdPolicy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IdPolicy(Generic[P, I]):
    """Lifecycle rules for ID mappings, parameterised by payload type.

    The proxy core consults this object; it never touches payload content
    directly. All protocol-specific logic lives in the policy.

    Attributes:
        should_register:    should we allocate a mapping for this payload?
        referential_fields: lenses into cross-referencing ID slots.
        should_remove:      given a downstream_id and response, should we
                            remove the mapping? (Wraps the CompletionTracker
                            for multi-part completion.)
    """
    should_register: Callable[[P], bool]
    referential_fields: Sequence[ReferentialField[P, I]]
    should_remove: Callable[[I, P], bool]


# ---------------------------------------------------------------------------
# translate_referentials — the generic workhorse
# ---------------------------------------------------------------------------

def translate_referentials(
    fields: Sequence[ReferentialField[P, I]],
    lookup: Callable[[I], Optional[I]],
    payload: P,
) -> P:
    """Translate every populated referential field in a payload.

    Raises TranslationError if a referenced ID is not found in the mapping.
    Returns a new payload (or the same object for mutable types — caller's
    concern). This function is intentionally pure: it has no side effects.
    """
    for f in fields:
        ref_id = f.get(payload)
        if ref_id is not None:
            translated = lookup(ref_id)
            if translated is None:
                raise TranslationError(f"unknown_{f.name}", ref_id)
            payload = f.set(payload, translated)
    return payload


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------

@dataclass
class Envelope(Generic[I, P]):
    """A message with its envelope identity and payload."""
    id: I
    payload: P


# ---------------------------------------------------------------------------
# ProxyLink
# ---------------------------------------------------------------------------

class ProxyLink(Generic[I]):
    """One translation boundary: mapping + query policy + response policy.

    A link knows nothing about chains, other links, or its position.
    It translates messages crossing *one* namespace boundary.
    """

    def __init__(
        self,
        mapping: IdMapping[I],
        query_policy: IdPolicy[Any, I],
        response_policy: IdPolicy[Any, I],
    ) -> None:
        self.mapping = mapping
        self.query_policy = query_policy
        self.response_policy = response_policy

    def translate_downstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]:
        """Translate a query flowing toward the engine."""
        policy = self.query_policy
        downstream_id = self.mapping.register(envelope.id)
        translated_payload = translate_referentials(
            policy.referential_fields,
            self.mapping.forward,
            envelope.payload,
        )
        return Envelope(id=downstream_id, payload=translated_payload)

    def translate_upstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]:
        """Translate a response flowing toward the client."""
        policy = self.response_policy
        upstream_id = self.mapping.reverse(envelope.id)
        if upstream_id is None:
            raise TranslationError("unknown_downstream_id", envelope.id)

        translated_payload = translate_referentials(
            policy.referential_fields,
            lambda did: self.mapping.reverse(did),
            envelope.payload,
        )

        if policy.should_remove(envelope.id, translated_payload):
            self.mapping.remove_by_downstream(envelope.id)

        return Envelope(id=upstream_id, payload=translated_payload)


# ---------------------------------------------------------------------------
# ProxyChain
# ---------------------------------------------------------------------------

class ProxyChain(Generic[I]):
    """A flat, ordered sequence of proxy links.

    Downstream: fold left  (link[0] → link[1] → … → link[n]).
    Upstream:   fold right (link[n] → link[n-1] → … → link[0]).

    No recursion. No nesting. Each link is independent.
    """

    def __init__(self, links: Sequence[ProxyLink[I]]) -> None:
        if not links:
            raise ValueError("ProxyChain requires at least one link")
        self._links: list[ProxyLink[I]] = list(links)

    def translate_downstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]:
        for link in self._links:
            envelope = link.translate_downstream(envelope)
        return envelope

    def translate_upstream(self, envelope: Envelope[I, Any]) -> Envelope[I, Any]:
        for link in reversed(self._links):
            envelope = link.translate_upstream(envelope)
        return envelope

    def __len__(self) -> int:
        return len(self._links)

    def extend(self, other: ProxyChain[I]) -> ProxyChain[I]:
        """Horizontal composition: concatenate two chains."""
        return ProxyChain(self._links + other._links)
