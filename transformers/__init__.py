"""
transformers — Layer 1 transformer extensions for the proxy pipeline.

A `Transformer` (defined in `AbstractProxy.protocol_transformer`) is a
synchronous, stateless-per-message pair `(on_query, on_response)`
composed with `.then()`. Reach for a transformer when the work is
per-message, stateless, and can be expressed as a pure function;
returning `None` from either callback suppresses the message (the
filter semantic).

Modules in this package:

  - ``katago``                   — KataGo response post-processing
                                   factories (min_visits_filter,
                                   add_score_delta, final_only,
                                   inject_defaults,
                                   standard_postprocessing).
  - ``analysis_enricher``        — proxy-protocol-aware glue around
                                   ``DeltaAnalysisState`` + the
                                   user-supplied ``analysis_config``.
  - ``transposition_enricher``   — optional PV-partitioning enricher
                                   backed by the native
                                   ``go_transposition`` extension.

The middleware extension surface (``SessionMiddleware``) lives next
door in ``middleware/``. The choice between the two is documented in
``ARCHITECTURE.md``: pick the transformer when the work is per-message
and stateless; reach for middleware when you need cross-message state,
async awaits, or control over *when* responses are emitted.
"""
