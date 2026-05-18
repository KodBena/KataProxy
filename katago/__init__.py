"""
katago — KataGo-specific protocol types and parsers.

This package holds the wire-protocol surface for the KataGo analysis
engine: the action enum, the query/response dataclasses (response is a
discriminated union of `AnalyzeResponse | MetadataResponse` per
v1.0.13), the parsers and translators, the prisms used by the
dispatcher, the referential-field definitions, and the completion-
tracker bridge.

Lives outside `AbstractProxy/` because it is *KataGo-specific*. The
protocol-agnostic core (`IdMapping`, `CompletionTracker`, `ProxyLink`,
`Prism`, `Dispatcher`, `Transformer`) stays in `AbstractProxy/`. Only
this package and `transformers/katago.py` (KataGo-specific transformer
factories) carry KataGo wire knowledge.

Public exports re-mirror the module's `__all__` for ergonomic imports
(`from katago import KataGoQuery, AnalyzeResponse`).
"""

from .katago_proxy import (
    KATAGO_QUERY_PRISMS,
    AnalyzeResponse,
    Color,
    CompletionTracker,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
    MoveIndex,
    TurnIndex,
    make_katago_chain,
    make_katago_link,
    move_to_turn_pair,
    parse_query_from_wire,
    parse_response_from_wire,
    register_query_completion,
    response_completion_signal,
    translate_query_to_wire,
    translate_response_to_wire,
)

__all__ = [
    "KATAGO_QUERY_PRISMS",
    "AnalyzeResponse",
    "Color",
    "CompletionTracker",
    "KataGoAction",
    "KataGoQuery",
    "KataGoResponse",
    "MetadataResponse",
    "MoveIndex",
    "TurnIndex",
    "make_katago_chain",
    "make_katago_link",
    "move_to_turn_pair",
    "parse_query_from_wire",
    "parse_response_from_wire",
    "register_query_completion",
    "response_completion_signal",
    "translate_query_to_wire",
    "translate_response_to_wire",
]
