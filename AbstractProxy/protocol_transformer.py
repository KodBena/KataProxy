"""
protocol_transformer.py — Content transformations composed with proxy translation.

A Transformer is a pair of functions on payloads, one per direction.
It composes horizontally with ProxyChain via TransformedChain, which applies
identity translation first, then content transformation.

Transformers compose vertically (stacking) via ordinary function composition,
surfaced as the `then` combinator. Composition reads left-to-right:
    a.then(b) means "a first on the way down, b first on the way back up".

Message flow through TransformedChain:

    Client → [transformer.on_query] → [chain.translate_downstream] → Engine
    Engine → [chain.translate_upstream] → [transformer.on_response] → Client

The transformer sees messages in the client's ID namespace — it never
encounters internal proxy IDs. This is the correct composition order
because post-processing (enrichment, filtering, aggregation) is a
client-facing concern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, TypeVar

from .proxy_core import Direction, Envelope, ProxyChain

__all__ = [
    "Transformer",
    "TransformedChain",
]

Q = TypeVar("Q")  # query payload type
R = TypeVar("R")  # response payload type
I = TypeVar("I", bound=str)  # envelope IDs are always str in this system


# ---------------------------------------------------------------------------
# Transformer: the primitive
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Transformer(Generic[Q, R]):
    """A bidirectional content transformation on message payloads.

    on_query:    (envelope_id, query)    -> Optional[query]
    on_response: (envelope_id, response) -> Optional[response]

    Returning None from either function suppresses the message — it will not
    be forwarded in that direction. This is the filtering semantic.
    """
    name: str
    on_query: Callable[[str, Q], Optional[Q]]
    on_response: Callable[[str, R], Optional[R]]

    @staticmethod
    def identity(name: str = "identity") -> Transformer[Any, Any]:
        """The do-nothing transformer. Left and right unit of `then`."""
        def _passthrough_query(_id: str, q: Any) -> Any:
            return q

        def _passthrough_response(_id: str, r: Any) -> Any:
            return r

        return Transformer(
            name=name,
            on_query=_passthrough_query,
            on_response=_passthrough_response,
        )

    def then(self, other: Transformer[Q, R]) -> Transformer[Q, R]:
        """Compose two transformers: self first downstream, other first upstream.

        Composition is asymmetric by design:
          - Downstream (query):  self → other
          - Upstream (response): other → self

        This preserves the "closest to client runs last on the way back"
        invariant, which is the correct semantic for layered enrichment.
        """
        left = self
        right = other

        def composed_query(eid: str, q: Q) -> Optional[Q]:
            q1 = left.on_query(eid, q)
            if q1 is None:
                return None
            return right.on_query(eid, q1)

        def composed_response(eid: str, r: R) -> Optional[R]:
            r1 = right.on_response(eid, r)
            if r1 is None:
                return None
            return left.on_response(eid, r1)

        return Transformer(
            name=f"{left.name} >> {right.name}",
            on_query=composed_query,
            on_response=composed_response,
        )


# ---------------------------------------------------------------------------
# TransformedChain: composition of proxy chain + transformer
# ---------------------------------------------------------------------------

class TransformedChain(Generic[I]):
    """A proxy chain with a transformer applied on the client-facing side.

    This is the primary composition point between identity translation
    (ProxyChain) and content transformation (Transformer). They are kept
    separate because they are orthogonal concerns:

      - ProxyChain  : knows about IDs, knows nothing about content
      - Transformer : knows about content, knows nothing about IDs
    """

    def __init__(
        self,
        chain: ProxyChain[I],
        transformer: Transformer[Any, Any],
    ) -> None:
        self._chain = chain
        self._transformer = transformer

    def translate_downstream(
        self, envelope: Envelope[I, Any]
    ) -> Optional[Envelope[I, Any]]:
        """Client → Engine: transform content first, then translate identity."""
        transformed_payload = self._transformer.on_query(
            envelope.id, envelope.payload
        )
        if transformed_payload is None:
            return None
        return self._chain.translate_downstream(
            Envelope(id=envelope.id, payload=transformed_payload)
        )

    def translate_upstream(
        self, envelope: Envelope[I, Any]
    ) -> Optional[Envelope[I, Any]]:
        """Engine → Client: translate identity first, then transform content."""
        translated = self._chain.translate_upstream(envelope)
        transformed_payload = self._transformer.on_response(
            translated.id, translated.payload
        )
        if transformed_payload is None:
            return None
        return Envelope(id=translated.id, payload=transformed_payload)

    @property
    def chain(self) -> ProxyChain[I]:
        return self._chain

    @property
    def transformer(self) -> Transformer[Any, Any]:
        return self._transformer
