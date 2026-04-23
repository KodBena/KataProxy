"""
AbstractProxy — Protocol-agnostic proxy abstractions and KataGo bindings.

The package is split into two layers:

  proxy_core, protocol_transformer
      Reusable, protocol-agnostic abstractions: IdMapping, ProxyLink,
      ProxyChain, Prism, Transformer, TransformedChain. Nothing in this
      layer knows about KataGo.

  katago_proxy, katago_transformers
      KataGo-specific instantiations of the above. KataGo wire format,
      query/response types, and concrete transformers live here.

Extenders adding a second protocol (e.g. an alternative analysis engine)
should mirror the katago_* pattern: define the wire types, the prisms,
the ProxyLink factory, and a transformer module.

See ARCHITECTURE.md for the full layer model and rough-edges discussion.
"""

from . import katago_proxy
from . import katago_transformers
from . import protocol_transformer
from . import proxy_core
