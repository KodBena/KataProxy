"""
AbstractProxy — Protocol-agnostic proxy abstractions.

This package holds the reusable, protocol-agnostic core of the proxy
framework. Two modules:

  proxy_core
      ID translation and identity contracts: ``IdMapping``,
      ``ProxyLink``, ``ProxyChain``, ``Prism``, ``Dispatcher``,
      ``CompletionTracker``, ``CompletionSignal``, ``Envelope``,
      ``ReferentialField``.

  protocol_transformer
      Synchronous content-transformation extension surface:
      ``Transformer`` and ``TransformedChain``. Generic over query
      and response types.

KataGo-specific instantiations of these abstractions live in sibling
top-level packages (post-v1.0.13):

  katago/                     — KataGo wire types, prisms, parsers,
                                completion bridge.
  transformers/               — concrete Transformer factories
                                (KataGo post-processing, analysis
                                enricher, transposition enricher).
  middleware/                 — concrete SessionMiddleware extensions
                                (keep-alive, adaptive-reevaluate)
                                plus the middleware ABC + chain.

Extenders adding a second protocol mirror this pattern: a sibling
package for the wire types, plus protocol-specific transformer/middleware
modules under ``transformers/`` and ``middleware/``.

See ARCHITECTURE.md for the full layer model.
"""

from . import protocol_transformer
from . import proxy_core
