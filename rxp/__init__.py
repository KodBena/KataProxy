"""
rxp — Reactive pipeline (experimental, narrowly used).

This subpackage is an experimental reactive-pipeline implementation. It is
NOT integrated with the main proxy message flow. It is currently used only
by bsa.py and exposes a deliberately narrow
public surface.

Public exports:
  CompiledNode, CompiledPipeline, MonitoredNode, MissingData, Pipeline

If you are looking for the proxy's response-transformation extension points,
see AbstractProxy/protocol_transformer.py (Transformer) and
session_middleware.py (SessionMiddleware) instead.
"""

from .rxp import (
    CompiledNode,
    CompiledPipeline,
    MonitoredNode,
    MissingData,
    Pipeline,
)
