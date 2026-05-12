"""
transformers/capabilities_advertiser.py — Always-on Transformer that
adds the proxy's capability advertisement to query_version responses.

The advertised set is constructed at server startup based on what is
wired (which middleware factories are present, native module
availability for transposition) and frozen for the server's lifetime.
The Transformer's on_query is identity; on_response intercepts
metadata responses that carry a "version" key (KataGo's idiomatic
discriminator for query_version replies) and adds a "capabilities"
entry to the opaque.

Deliberately not capability-gated: the advertisement is what tells
clients what they may opt into. The dispatch sign-off's startup-time
shape (Q5) makes this a server-scoped configuration rather than a
per-query concern.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, Optional

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, ProxyLink
from katago import KataGoQuery, KataGoResponse, MetadataResponse


def capabilities_advertiser(
    advertised: Dict[str, Dict[str, Any]],
) -> Callable[[ProxyLink], Transformer]:
    """Return a Transformer factory that adds the capability advertisement
    to query_version responses.

    Parameters
    ----------
    advertised:
        Server-scoped capability advertisement. Each key is a
        capability name; each value is the metadata dict to advertise
        for that capability (empty dict for capabilities without
        metadata, per the dispatch's Q4 answer that formalises
        metadata only as user-meaningful knobs are identified).

    Implementation notes:
      - The advertisement dict is captured (deep-copied) at factory
        call time so subsequent mutations of the input don't leak
        into emitted responses.
      - Discrimination is by "version" key presence in the opaque
        rather than by tracking the originating query type, because
        the response variant alone (MetadataResponse) is shared with
        query_models, clear_cache ack, terminate ack, and error
        responses for non-analyze queries; "version" is KataGo's
        idiomatic field on query_version replies.
      - The Transformer is *always on* — the gating decision (which
        capabilities to advertise at all) lives in the wiring at
        ProxyServer construction.
    """
    advertised_snapshot: Dict[str, Dict[str, Any]] = deepcopy(advertised)

    def factory(_link: ProxyLink) -> Transformer:
        def on_query(_eid: ClientId, q: KataGoQuery) -> Optional[KataGoQuery]:
            return q

        def on_response(_eid: ClientId, r: KataGoResponse) -> Optional[KataGoResponse]:
            if isinstance(r, MetadataResponse) and "version" in r.opaque:
                new_opaque = dict(r.opaque)
                new_opaque["capabilities"] = deepcopy(advertised_snapshot)
                return MetadataResponse(opaque=new_opaque)
            return r

        return Transformer(
            name="capabilities_advertiser",
            on_query=on_query,
            on_response=on_response,
        )

    return factory
