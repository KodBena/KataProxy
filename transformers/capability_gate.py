"""
transformers/capability_gate.py — Capability-gating wrapper for KataGo
Transformers.

Wraps a Transformer factory so the wrapped transformer engages on a
given query iff that query's `capabilities` field opts in to a named
capability. Legacy-compatible: queries with no `capabilities` field
auto-engage (the dispatch sign-off's Q1 answer — preserves wire
compatibility for clients that have not migrated to the
capability-aware contract).

Per-eid state recording the engagement decision lives in the wrapper;
cleanup mirrors the per-transformer pattern used by analysis_enricher
and transposition_enricher (drop the eid entry when
link.mapping.forward(eid) is None).

Usage at the ProxyServer composition site:

    transformer_factory = (
        Contextual(capability_gate("delta_analysis", analysis_enricher))
        .then(capability_gate("transposition", transposition_enricher))
    )

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from typing import Callable, Dict, Optional

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ProxyLink
from katago import KataGoQuery, KataGoResponse


def capability_gate(
    name: str,
    wrapped_factory: Callable[
        [ProxyLink], Transformer
    ],
) -> Callable[[ProxyLink], Transformer]:
    """Wrap a Transformer factory so the wrapped transformer engages
    only when the query's `capabilities` field opts in to ``name``.

    Engagement decision (mirrors CapabilityGatedMiddleware in
    middleware/capability_gate.py):
      - `capabilities` field absent on the query → engage (legacy
        auto-engage; the dispatch's Q1 answer).
      - `capabilities` is a dict containing ``name`` → engage with
        the per-query metadata as the capability's metadata.
      - Otherwise → skip; the wrapped transformer's on_query and
        on_response are not invoked for this eid, and the response
        flows through unchanged.

    The wrapped factory and gate share a single ProxyLink at the
    factory call site, so the wrapped transformer's existing cleanup
    (`request_cache.pop(eid, None)` when `link.mapping.forward(eid)
    is None`) and the gate's own cleanup share the same lifecycle.

    The ``capabilities`` field itself is *not* popped here — the gate
    only reads. The post-hash pop happens in pubsub_hub.subscribe(),
    and the central wire-strip discipline in
    katago/katago_proxy.py:translate_query_to_wire is the
    authoritative line ensuring it never reaches KataGo.
    """

    def factory(link: ProxyLink) -> Transformer:
        wrapped = wrapped_factory(link)
        engaged: Dict[str, dict] = {}

        def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
            opaque_caps = q.opaque.get("capabilities")
            if opaque_caps is None:
                # Legacy auto-engage (no capabilities field present).
                engaged[eid] = {}
                return wrapped.on_query(eid, q)
            if isinstance(opaque_caps, dict) and name in opaque_caps:
                md = opaque_caps[name]
                engaged[eid] = md if isinstance(md, dict) else {}
                return wrapped.on_query(eid, q)
            # Explicit opt-out: capabilities present but does not
            # name this capability. Pass the query through; on_response
            # will short-circuit too.
            return q

        def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
            if eid in engaged:
                result = wrapped.on_response(eid, r)
            else:
                result = r
            # Mirror the existing per-transformer cleanup pattern.
            if link.mapping.forward(eid) is None:
                engaged.pop(eid, None)
            return result

        return Transformer(
            name=f"gated:{name}:{wrapped.name}",
            on_query=on_query,
            on_response=on_response,
        )

    return factory
