"""tests/test_visit_scaling.py — VisitScalingModel substrate (v1.0.25).

Unit-level regression coverage for the Phase 3 visit-scaling substrate
per `docs/roadmap-info-theoretic-allocation.md` §3.1 and §3.6.3.

Three test classes:

  1. `TestMonteCarloSqrtModel` — pin the scoreStdev-prefactor formula
     (§3.6.3), the V=0 / V_extra=0 edge cases, and the absent-rootInfo
     fallback (the §3.6.3 "fallback to 1.0" rule).
  2. `TestDiminishingReturnsLogModel` — pin the log formula and
     V=0 / V_extra=0 edges.
  3. `TestRegistryParse` — `_parse_visit_scaling_model` returns
     instances for known names; raises
     `AdaptiveConfigurationError(code="allocation_invalid")` for
     unknown.

Run from the proxy directory: `pytest tests/test_visit_scaling.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import AnalyzeResponse, TurnIndex  # noqa: E402
from middleware.adaptive_reevaluate import (  # noqa: E402
    AdaptiveConfigurationError,
    TurnView,
)
from middleware.visit_scaling import (  # noqa: E402
    DiminishingReturnsLogModel,
    MonteCarloSqrtModel,
    _parse_visit_scaling_model,
    _registered_model_names,
)


def _turn_view(
    turn: int = 5,
    *,
    score_stdev: Any = 12.0,
    include_root: bool = True,
) -> TurnView:
    """Build a synthetic TurnView whose packet carries (or omits)
    `rootInfo.scoreStdev` for the model under test."""
    opaque: dict[str, Any] = {"moveInfos": []}
    if include_root:
        root: dict[str, Any] = {}
        if score_stdev is not None:
            root["scoreStdev"] = score_stdev
        opaque["rootInfo"] = root
    return TurnView(
        turn_index=TurnIndex(turn),
        to_play="black" if turn % 2 == 0 else "white",
        packet=AnalyzeResponse(
            is_during_search=False, turn_number=turn, opaque=opaque,
        ),
    )


# ===========================================================================
# 1. MonteCarloSqrtModel
# ===========================================================================


class TestMonteCarloSqrtModel:

    def test_basic_formula(self) -> None:
        """gain = scoreStdev × (1/√V_current − 1/√(V_current + V_extra))."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        expected = 10.0 * (1.0 / math.sqrt(100) - 1.0 / math.sqrt(400))
        assert gain == pytest.approx(expected)

    def test_uses_score_stdev_as_prefactor(self) -> None:
        """Two views with different scoreStdev produce proportional gains."""
        m = MonteCarloSqrtModel()
        v1 = _turn_view(score_stdev=10.0)
        v2 = _turn_view(score_stdev=20.0)
        g1 = m.expected_gain(v1, current_visits=100, extra_visits=300)
        g2 = m.expected_gain(v2, current_visits=100, extra_visits=300)
        # Twice the prefactor → twice the gain (same V, V_extra).
        assert g2 == pytest.approx(2 * g1)

    def test_extra_visits_zero_returns_zero(self) -> None:
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        assert m.expected_gain(v, current_visits=100, extra_visits=0) == 0.0

    def test_extra_visits_negative_returns_zero(self) -> None:
        """Defensive: a negative budget step shouldn't produce a gain
        (it would be nonsensical for a visit-scaling model)."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        assert m.expected_gain(v, current_visits=100, extra_visits=-50) == 0.0

    def test_current_visits_zero_treated_as_one(self) -> None:
        """V_current=0 would div-by-zero; the model treats it as 1
        (the NN-prior baseline of "one visit's worth of evidence")."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        gain = m.expected_gain(v, current_visits=0, extra_visits=100)
        expected = 10.0 * (1.0 / math.sqrt(1) - 1.0 / math.sqrt(101))
        assert gain == pytest.approx(expected)

    def test_current_visits_negative_treated_as_one(self) -> None:
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        gain = m.expected_gain(v, current_visits=-5, extra_visits=100)
        expected = 10.0 * (1.0 / math.sqrt(1) - 1.0 / math.sqrt(101))
        assert gain == pytest.approx(expected)

    def test_absent_root_info_falls_back_to_unit_prefactor(self) -> None:
        """When rootInfo is absent, the prefactor is 1.0 — the gain
        becomes a pure 1/√V curve. The substrate doesn't crash; the
        allocation algorithm sees a non-empirical signal."""
        m = MonteCarloSqrtModel()
        v = _turn_view(include_root=False)
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        expected = 1.0 * (1.0 / math.sqrt(100) - 1.0 / math.sqrt(400))
        assert gain == pytest.approx(expected)

    def test_absent_score_stdev_falls_back_to_unit_prefactor(self) -> None:
        """rootInfo present but no scoreStdev field → fallback to 1.0."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=None)
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        expected = 1.0 * (1.0 / math.sqrt(100) - 1.0 / math.sqrt(400))
        assert gain == pytest.approx(expected)

    def test_non_numeric_score_stdev_falls_back(self) -> None:
        """A non-numeric scoreStdev (e.g. a string from a corrupted
        packet) doesn't crash; the fallback is the unit prefactor."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev="not a number")
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        expected = 1.0 * (1.0 / math.sqrt(100) - 1.0 / math.sqrt(400))
        assert gain == pytest.approx(expected)

    def test_bool_score_stdev_rejected(self) -> None:
        """Python's bool is an int subclass; `scoreStdev: True` should
        NOT silently parse as a 1.0 prefactor — that would be a wrong-
        type bug masquerading as the absent-field fallback. The model
        rejects bool explicitly and falls back to the unit prefactor."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=True)
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        expected = 1.0 * (1.0 / math.sqrt(100) - 1.0 / math.sqrt(400))
        assert gain == pytest.approx(expected)

    def test_gain_is_monotonically_decreasing_in_current_visits(self) -> None:
        """Adding 300 visits to a turn at V=10 buys more than at V=1000
        (diminishing-returns property of 1/√V scaling)."""
        m = MonteCarloSqrtModel()
        v = _turn_view(score_stdev=10.0)
        g_small = m.expected_gain(v, current_visits=10, extra_visits=300)
        g_large = m.expected_gain(v, current_visits=1000, extra_visits=300)
        assert g_small > g_large > 0.0


# ===========================================================================
# 2. DiminishingReturnsLogModel
# ===========================================================================


class TestDiminishingReturnsLogModel:

    def test_basic_formula(self) -> None:
        """gain = log(1 + V_extra / max(V_current, 1))."""
        m = DiminishingReturnsLogModel()
        v = _turn_view()
        gain = m.expected_gain(v, current_visits=100, extra_visits=300)
        assert gain == pytest.approx(math.log(4.0))  # 1 + 300/100 = 4

    def test_extra_visits_zero_returns_zero(self) -> None:
        m = DiminishingReturnsLogModel()
        v = _turn_view()
        assert m.expected_gain(v, current_visits=100, extra_visits=0) == 0.0

    def test_extra_visits_negative_returns_zero(self) -> None:
        m = DiminishingReturnsLogModel()
        v = _turn_view()
        assert m.expected_gain(v, current_visits=100, extra_visits=-50) == 0.0

    def test_current_visits_zero_treated_as_one(self) -> None:
        m = DiminishingReturnsLogModel()
        v = _turn_view()
        gain = m.expected_gain(v, current_visits=0, extra_visits=100)
        assert gain == pytest.approx(math.log(101.0))

    def test_does_not_read_score_stdev(self) -> None:
        """The log model's output is dimensionless; rootInfo content
        doesn't affect the gain."""
        m = DiminishingReturnsLogModel()
        v1 = _turn_view(score_stdev=10.0)
        v2 = _turn_view(score_stdev=100.0)
        v3 = _turn_view(include_root=False)
        g1 = m.expected_gain(v1, current_visits=50, extra_visits=200)
        g2 = m.expected_gain(v2, current_visits=50, extra_visits=200)
        g3 = m.expected_gain(v3, current_visits=50, extra_visits=200)
        assert g1 == g2 == g3 == pytest.approx(math.log(5.0))

    def test_diminishing_returns_property(self) -> None:
        """Same V_extra; gain decreases as V_current grows (the
        log-ratio shape)."""
        m = DiminishingReturnsLogModel()
        v = _turn_view()
        g_small = m.expected_gain(v, current_visits=10, extra_visits=100)
        g_large = m.expected_gain(v, current_visits=10000, extra_visits=100)
        assert g_small > g_large > 0.0


# ===========================================================================
# 3. Registry parse
# ===========================================================================


class TestRegistryParse:

    def test_monte_carlo_sqrt_resolves(self) -> None:
        m = _parse_visit_scaling_model("monte_carlo_sqrt")
        assert isinstance(m, MonteCarloSqrtModel)

    def test_diminishing_returns_log_resolves(self) -> None:
        m = _parse_visit_scaling_model("diminishing_returns_log")
        assert isinstance(m, DiminishingReturnsLogModel)

    def test_unknown_name_raises_allocation_invalid(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_visit_scaling_model("no_such_model")
        assert exc.value.code == "allocation_invalid"
        assert "visit_scaling_model" in exc.value.detail
        assert exc.value.detail["visit_scaling_model"] == "no_such_model"
        # Valid alternatives surfaced for the SPA's error rendering.
        valid = exc.value.detail.get("valid")
        assert isinstance(valid, list)
        assert "monte_carlo_sqrt" in valid
        assert "diminishing_returns_log" in valid

    def test_empty_name_raises_allocation_invalid(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_visit_scaling_model("")
        assert exc.value.code == "allocation_invalid"

    def test_registered_model_names(self) -> None:
        names = _registered_model_names()
        assert set(names) == {"monte_carlo_sqrt", "diminishing_returns_log"}
        assert names == sorted(names)  # deterministic order
