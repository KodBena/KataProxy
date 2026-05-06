"""
reactive_pipeline — Reactive pipeline DSL (experimental, narrowly used).

This subpackage is an experimental reactive-pipeline implementation. It is
NOT integrated with the main proxy message flow. It is currently used only
by ``delta_analysis.py`` (the DeltaAnalysisState multi-resolution analysis
manager) and exposes a deliberately narrow public surface.

Public exports:
  CompiledNode, CompiledPipeline, MonitoredNode, MissingData, Pipeline

If you are looking for the proxy's response-transformation extension points,
see AbstractProxy/protocol_transformer.py (Transformer) and
session_middleware.py (SessionMiddleware) instead.

(Renamed in v1.0.14 from rxp/. The ``core.py`` module was previously
``rxp/rxp.py``.)
"""

from .core import (
    CompiledNode,
    CompiledPipeline,
    MonitoredNode,
    MissingData,
    Pipeline,
)
