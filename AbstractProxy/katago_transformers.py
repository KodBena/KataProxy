"""
katago_transformers.py — Transformers specific to KataGo analysis post-processing.

These are assembled from the generic Transformer primitive. Each is
independently testable and composable via `then`.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from .protocol_transformer import Transformer
from .katago_proxy import KataGoQuery, KataGoResponse


# ---------------------------------------------------------------------------
# Filter: suppress responses below a visit threshold
# ---------------------------------------------------------------------------

def min_visits_filter(threshold: int) -> Transformer[KataGoQuery, KataGoResponse]:
    """Drop moveInfos entries with fewer than `threshold` visits."""

    def filter_response(_eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        if "moveInfos" in r.opaque:
            filtered = [
                m for m in r.opaque["moveInfos"]
                if m.get("visits", 0) >= threshold
            ]
            new_opaque = {**r.opaque, "moveInfos": filtered}
            return KataGoResponse(
                is_during_search=r.is_during_search,
                turn_number=r.turn_number,
                opaque=new_opaque,
            )
        return r

    return Transformer(
        name=f"min_visits({threshold})",
        on_query=lambda _eid, q: q,
        on_response=filter_response,
    )


# ---------------------------------------------------------------------------
# Enrich: add computed fields to responses
# ---------------------------------------------------------------------------

def add_score_delta() -> Transformer[KataGoQuery, KataGoResponse]:
    """Compute score deltas between consecutive moves.

    Stateful: maintains a per-query running score to compute deltas.
    Demonstrates that transformers can carry state without violating
    the proxy chain's invariants (the chain is identity-only; state
    lives in the transformer's closure).
    """
    scores: dict[str, float] = {}  # eid → last known scoreLead

    def enrich_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        root_info = r.opaque.get("rootInfo", {})
        current_score = root_info.get("scoreLead")
        if current_score is not None:
            previous = scores.get(eid)
            scores[eid] = current_score
            if previous is not None:
                delta = current_score - previous
                new_opaque = {**r.opaque, "scoreDelta": delta}
                return KataGoResponse(
                    is_during_search=r.is_during_search,
                    turn_number=r.turn_number,
                    opaque=new_opaque,
                )
        return r

    return Transformer(
        name="score_delta",
        on_query=lambda _eid, q: q,
        on_response=enrich_response,
    )


# ---------------------------------------------------------------------------
# Suppress mid-search noise
# ---------------------------------------------------------------------------

def final_only() -> Transformer[KataGoQuery, KataGoResponse]:
    """Suppress all isDuringSearch=True responses.

    The client sees only final results. Useful when the proxy requests
    reportDuringSearchEvery for internal monitoring but the end client
    doesn't want the noise.
    """
    return Transformer(
        name="final_only",
        on_query=lambda _eid, q: q,
        on_response=lambda _eid, r: None if r.is_during_search else r,
    )


# ---------------------------------------------------------------------------
# Inject query parameters
# ---------------------------------------------------------------------------

def inject_defaults(**defaults: Any) -> Transformer[KataGoQuery, KataGoResponse]:
    """Inject default opaque fields into queries if not already present.

    Useful for enforcing analysis parameters (e.g., maxVisits, komi)
    without requiring the client to specify them.
    """
    def augment_query(_eid: str, q: KataGoQuery) -> KataGoQuery:
        merged = {**defaults, **q.opaque}  # client's values win
        return KataGoQuery(
            action=q.action,
            terminate_id=q.terminate_id,
            analyze_turns=q.analyze_turns,
            opaque=merged,
        )

    return Transformer(
        name=f"defaults({list(defaults.keys())})",
        on_query=augment_query,
        on_response=lambda _eid, r: r,
    )


# ---------------------------------------------------------------------------
# Composition example
# ---------------------------------------------------------------------------

def standard_postprocessing() -> Transformer[KataGoQuery, KataGoResponse]:
    """A composed transformer for typical analysis post-processing.

    Reads left to right as a pipeline:
    1. Inject sensible defaults into queries.
    2. On responses, drop low-visit moves.
    3. Compute score deltas.
    4. Suppress mid-search chatter.
    """
    return (
        inject_defaults(maxVisits=1000, komi=7.5)
        .then(min_visits_filter(10))
        .then(add_score_delta())
        .then(final_only())
    )
