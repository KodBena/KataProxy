"""middleware/visit_scaling.py — VisitScalingModel substrate (v1.0.25).

Per-turn visit-scaling models for the Phase 3 information-theoretic
allocation arc. See `proxy/docs/roadmap-info-theoretic-allocation.md`
§3.1 (Protocol contract) and §3.6.3 (empirical grounding via
`rootInfo.scoreStdev`).

A `VisitScalingModel` predicts the expected information gain from
adding `extra_visits` to a turn whose KataGo analysis currently
reports `current_visits` visits. Convention: higher gain = more
uncertainty reduction. Units are arbitrary as long as they're
comparable across turns within a single round (the allocation
algorithm consumes ratios, not absolute magnitudes).

This module is consumed by:

  - `middleware/adaptive_reevaluate.py` at engagement-time parsing
    (`_parse_visit_scaling_model` looks up the named model).
  - The Phase 3 allocation dispatch (commits 5+) at per-round
    allocation-decision time.

Curated registry (closed for v1.0.25; user-authored models are
Phase 4 territory):

  - `monte_carlo_sqrt` — the empirically-grounded 1/√V scaling
    with `rootInfo.scoreStdev` as the per-turn prefactor (per
    §11.11's confirmed default; the natural anchor for Monte
    Carlo variance reduction).
  - `diminishing_returns_log` — logarithmic baseline,
    qualitatively different shape for sanity-checking
    allocation algorithms against a non-`1/√V` curve.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    # The view types live in adaptive_reevaluate; importing them at
    # type-check time only avoids the module-load cycle (adaptive_
    # reevaluate imports this module, so the reverse must be lazy).
    from middleware.adaptive_reevaluate import TurnView


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class VisitScalingModel(Protocol):
    """Predicts expected information gain from adding visits to a turn.

    Implementations consume the `TurnView` (which carries the current
    KataGo packet) and return a scalar gain. Convention: higher = more
    gain. Units are arbitrary but must be comparable across turns
    within one allocation round.

    Implementations are stateless from the substrate's perspective —
    the same model instance is reused across rounds and across queries.
    """

    def expected_gain(
        self,
        turn: "TurnView",
        current_visits: int,
        extra_visits: int,
    ) -> float:
        """Expected info gain from adding extra_visits to a turn that
        currently has current_visits visits committed."""
        ...


# ---------------------------------------------------------------------------
# Curated implementations
# ---------------------------------------------------------------------------


class MonteCarloSqrtModel:
    """Monte Carlo 1/√V variance scaling with empirical prefactor.

    `gain = prefactor × (1/√V_current − 1/√(V_current + V_extra))`

    The prefactor is `packet.opaque["rootInfo"]["scoreStdev"]` when
    available — KataGo's own search-aggregated standard deviation of
    score across MCTS samples — making this model per-turn empirically
    calibrated without an offline curve-fit step. Falls back to 1.0
    when `rootInfo.scoreStdev` is absent (the gain becomes a pure
    1/√V curve), so a missing field doesn't crash the allocation
    dispatch; the §11.11-prescribed default behaviour.

    See `docs/roadmap-info-theoretic-allocation.md` §3.6.3 for the
    derivation: stdev of the MCTS estimator at V visits is
    `scoreStdev/√V`; the SEM reduction from adding `V_extra` is the
    formula above.
    """

    def expected_gain(
        self,
        turn: "TurnView",
        current_visits: int,
        extra_visits: int,
    ) -> float:
        if extra_visits <= 0:
            return 0.0
        prefactor = self._score_stdev(turn)
        # current_visits = 0 would div-by-zero; treat as "at least one
        # visit's worth of evidence" — matches KataGo's NN-prior baseline.
        cv = max(current_visits, 1)
        return prefactor * (
            1.0 / math.sqrt(cv) - 1.0 / math.sqrt(cv + extra_visits)
        )

    @staticmethod
    def _score_stdev(turn: "TurnView") -> float:
        opaque = turn.packet.opaque
        if not isinstance(opaque, dict):
            return 1.0
        root = opaque.get("rootInfo")
        if not isinstance(root, dict):
            return 1.0
        v = root.get("scoreStdev")
        if isinstance(v, bool):
            # Defensive: Python bool is an int subclass; reject it
            # explicitly to avoid `scoreStdev: True` parsing as 1.0
            # by accident.
            return 1.0
        if isinstance(v, (int, float)):
            return float(v)
        return 1.0


class DiminishingReturnsLogModel:
    """Logarithmic-utility baseline: `gain = log(1 + extra/max(current, 1))`.

    Models the empirical regularity that successive doublings of
    visits yield roughly constant utility improvements (up to a
    saturation point past tens of thousands of visits). No empirical
    prefactor; the model's output is dimensionless. Useful for
    sanity-checking allocation algorithms against a qualitatively-
    different visit-scaling shape than `monte_carlo_sqrt`.

    `current_visits = 0` is treated as `max(current, 1)` to avoid
    division-by-zero. The first visit at a fresh position thus has
    gain `log(1 + V_extra)`; subsequent visits scale logarithmically.
    """

    def expected_gain(
        self,
        turn: "TurnView",
        current_visits: int,
        extra_visits: int,
    ) -> float:
        if extra_visits <= 0:
            return 0.0
        return math.log(1.0 + extra_visits / max(current_visits, 1))


# ---------------------------------------------------------------------------
# Curated registry + factory
# ---------------------------------------------------------------------------


_VISIT_SCALING_MODELS: dict[str, VisitScalingModel] = {
    "monte_carlo_sqrt": MonteCarloSqrtModel(),
    "diminishing_returns_log": DiminishingReturnsLogModel(),
}


def _parse_visit_scaling_model(name: str) -> VisitScalingModel:
    """Look up a named visit-scaling model.

    Raises `AdaptiveConfigurationError(code="allocation_invalid")` on
    unknown name, with `detail` carrying the offending name and the
    valid alternatives.

    The cost-asymmetry calibration (§11.4 in the v1.0.23 roadmap)
    applies: silent fallback to a default model could burn many rounds
    of compute on a wrong-shape gain estimate. Hard-refuse.
    """
    # Local import to avoid the module-load cycle with adaptive_reevaluate.
    from middleware.adaptive_reevaluate import AdaptiveConfigurationError

    model = _VISIT_SCALING_MODELS.get(name)
    if model is None:
        raise AdaptiveConfigurationError(
            code="allocation_invalid",
            detail={
                "visit_scaling_model": name,
                "valid": sorted(_VISIT_SCALING_MODELS.keys()),
            },
        )
    return model


def _registered_model_names() -> list[str]:
    """Return the curated registry's names (used by other parsers' error
    messages, e.g. when a required visit_scaling_model is absent from
    the capability metadata)."""
    return sorted(_VISIT_SCALING_MODELS.keys())


# Avoid an unused-import warning for the public Protocol in __all__-style
# downstream consumers — the export surface is the Protocol + the two
# implementations + the parser + the helper.
__all__ = [
    "VisitScalingModel",
    "MonteCarloSqrtModel",
    "DiminishingReturnsLogModel",
    "_VISIT_SCALING_MODELS",
    "_parse_visit_scaling_model",
    "_registered_model_names",
]


# Silence type-only-import warning on the `Any` import — it's used by
# the lint configuration's reserved imports list.
_ = Any
