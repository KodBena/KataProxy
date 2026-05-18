"""tests/test_allocation_algorithms.py — AllocationAlgorithm substrate (v1.0.25).

Unit-level regression coverage for the Phase 3 allocation algorithms per
`docs/roadmap-info-theoretic-allocation.md` §3.3 and §11.5/§11.6/§11.8.

Five test classes:

  1. `TestGreedyEIG` — pin the EIG-per-visit ranking; verify the
     "value × gain" composition; budget conservation.
  2. `TestKnowledgeGradient` — pin the incremental KG formula
     (§11.5 prescribed: incremental, not single-spend); leader-vs-
     non-leader cases; budget conservation.
  3. `TestThompsonSampling` — pin determinism under a seeded
     `random.Random`; verify the algorithm consumes the seed; pin
     budget conservation.
  4. `TestUCB` — pin the Beale-Welford-shifted exploration term
     (§11.6 prescribed: `n(c) + 1` denominator); verify κ
     parameterisation; budget conservation.
  5. `TestParseAllocationAlgorithm` — factory branches for each
     of the four; refusal surface for unknown names, malformed
     params, wrong-type values.

Run from the proxy directory: `pytest tests/test_allocation_algorithms.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import random
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
from middleware.allocation import (  # noqa: E402
    GreedyEIGAlgorithm,
    KnowledgeGradientAlgorithm,
    ThompsonSamplingAlgorithm,
    UCBAlgorithm,
    _parse_allocation_algorithm,
    _registered_algorithm_names,
)
from middleware.visit_scaling import (  # noqa: E402
    DiminishingReturnsLogModel,
    MonteCarloSqrtModel,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _view(turn: int, *, visits: int = 100, score_stdev: float = 10.0) -> TurnView:
    """Construct a synthetic TurnView whose packet carries the visit
    count and score stdev the visit-scaling model reads."""
    return TurnView(
        turn_index=TurnIndex(turn),
        to_play="black" if turn % 2 == 0 else "white",
        packet=AnalyzeResponse(
            is_during_search=False, turn_number=turn,
            opaque={
                "moveInfos": [],
                "rootInfo": {"visits": visits, "scoreStdev": score_stdev},
            },
        ),
    )


def _const_value(v: float) -> Any:
    """Value function returning a constant — independent of turn."""
    def vf(_turn: TurnView) -> float:
        return v
    return vf


def _identity_turn_value() -> Any:
    """Value function returning `float(turn_index)` — predictable per-turn ordering."""
    def vf(turn: TurnView) -> float:
        return float(int(turn.turn_index))
    return vf


# ===========================================================================
# 1. GreedyEIG
# ===========================================================================


class TestGreedyEIG:

    def test_empty_candidates_returns_empty(self) -> None:
        a = GreedyEIGAlgorithm()
        result = a.allocate([], _const_value(1.0), MonteCarloSqrtModel(), 100)
        assert result == {}

    def test_zero_budget_returns_empty(self) -> None:
        a = GreedyEIGAlgorithm()
        candidates = [_view(0), _view(1)]
        result = a.allocate(candidates, _const_value(1.0), MonteCarloSqrtModel(), 0)
        assert result == {}

    def test_budget_conservation(self) -> None:
        """Sum of allocations equals the budget."""
        a = GreedyEIGAlgorithm()
        candidates = [_view(0), _view(1), _view(2)]
        result = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 100,
        )
        assert sum(result.values()) == 100

    def test_uniform_inputs_produce_uniform_allocation(self) -> None:
        """When every candidate has identical value AND identical
        scaling, the greedy step ranks them stably; the result is
        round-robin (modulo ties broken by Python's stable max)."""
        a = GreedyEIGAlgorithm()
        candidates = [_view(0, visits=100), _view(1, visits=100), _view(2, visits=100)]
        result = a.allocate(
            candidates, _const_value(1.0), MonteCarloSqrtModel(), 99,
        )
        # 99 visits across 3 candidates with identical state → 33 each
        # (the greedy step always picks the same lex-min turn on ties,
        # but the EIG strictly decreases after each visit goes to it,
        # so the next visit prefers a different candidate).
        assert sum(result.values()) == 99
        # Each candidate gets ~33 visits; allow small variance from
        # the greedy ranking's tie-breaking sequence.
        for turn in (0, 1, 2):
            assert 30 <= result.get(TurnIndex(turn), 0) <= 36

    def test_higher_value_attracts_more_visits(self) -> None:
        """A candidate with 10x the value attracts more visits."""
        a = GreedyEIGAlgorithm()
        c_high = _view(0)
        c_low = _view(1)

        def vf(turn: TurnView) -> float:
            return 10.0 if int(turn.turn_index) == 0 else 1.0

        result = a.allocate(
            [c_high, c_low], vf, MonteCarloSqrtModel(), 100,
        )
        assert result.get(TurnIndex(0), 0) > result.get(TurnIndex(1), 0)

    def test_higher_score_stdev_attracts_more_visits(self) -> None:
        """A candidate with larger scoreStdev prefactor attracts more
        visits under MonteCarloSqrtModel (the gain is larger)."""
        a = GreedyEIGAlgorithm()
        c_loud = _view(0, score_stdev=20.0)
        c_quiet = _view(1, score_stdev=2.0)
        result = a.allocate(
            [c_loud, c_quiet], _const_value(1.0),
            MonteCarloSqrtModel(), 100,
        )
        assert result.get(TurnIndex(0), 0) > result.get(TurnIndex(1), 0)


# ===========================================================================
# 2. KnowledgeGradient (incremental)
# ===========================================================================


class TestKnowledgeGradient:

    def test_empty_candidates_returns_empty(self) -> None:
        a = KnowledgeGradientAlgorithm()
        result = a.allocate([], _const_value(1.0), MonteCarloSqrtModel(), 100)
        assert result == {}

    def test_budget_conservation(self) -> None:
        a = KnowledgeGradientAlgorithm()
        candidates = [_view(0), _view(1), _view(2)]
        result = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 50,
        )
        assert sum(result.values()) == 50

    def test_strong_leader_keeps_visits_when_lead_is_large(self) -> None:
        """When the leader's lead is wider than any per-visit gain
        could overtake, KG keeps allocating to the leader (its KG =
        its visit-effect; non-leaders have KG=0). Demonstrates the
        incremental KG's leader-stay property."""
        a = KnowledgeGradientAlgorithm()
        leader = _view(0)
        # huge value gap: leader=1000, follower=1. Score-stdev visits
        # could plausibly buy ~10 in gain magnitude; the lead is 999.
        c_follower = _view(1)

        def vf(turn: TurnView) -> float:
            return 1000.0 if int(turn.turn_index) == 0 else 1.0

        result = a.allocate(
            [leader, c_follower], vf, MonteCarloSqrtModel(), 20,
        )
        assert result.get(TurnIndex(0), 0) == 20
        assert TurnIndex(1) not in result or result[TurnIndex(1)] == 0

    def test_close_competitor_can_attract_visits(self) -> None:
        """When values are close AND the per-visit gain is large
        enough to bridge the gap, KG sends some visits to the
        non-leader — the "could-overtake" exploration property.

        Construction: visits=1 (low → first-visit gain is huge under
        1/√V scaling), scoreStdev=10 (prefactor is the magnitude),
        value gap 1.0 vs 0.5 (gain ≈ 10*(1 - 1/√2) ≈ 2.9 easily
        bridges this)."""
        a = KnowledgeGradientAlgorithm()
        c0 = _view(0, visits=1, score_stdev=10.0)
        c1 = _view(1, visits=1, score_stdev=10.0)

        def vf(turn: TurnView) -> float:
            return 1.0 if int(turn.turn_index) == 0 else 0.5

        result = a.allocate([c0, c1], vf, MonteCarloSqrtModel(), 20)
        # Both candidates get some allocation — c1's gain bridges
        # the value gap, so KG(c1) > 0 occasionally and the
        # round-robin-flavoured allocator picks c1 too.
        assert result.get(TurnIndex(0), 0) > 0
        assert result.get(TurnIndex(1), 0) > 0


# ===========================================================================
# 3. ThompsonSampling
# ===========================================================================


class TestThompsonSampling:

    def test_empty_candidates_returns_empty(self) -> None:
        a = ThompsonSamplingAlgorithm()
        result = a.allocate([], _const_value(1.0), MonteCarloSqrtModel(), 100,
                            rng=random.Random(0))
        assert result == {}

    def test_budget_conservation_seeded(self) -> None:
        a = ThompsonSamplingAlgorithm()
        candidates = [_view(0), _view(1), _view(2)]
        result = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 50,
            rng=random.Random(42),
        )
        assert sum(result.values()) == 50

    def test_deterministic_under_seeded_rng(self) -> None:
        """Same seed → same allocation. §11.8 prescribed."""
        a = ThompsonSamplingAlgorithm()
        candidates = [_view(0), _view(1), _view(2)]

        def vf(turn: TurnView) -> float:
            return float(int(turn.turn_index))

        result_a = a.allocate(
            candidates, vf, MonteCarloSqrtModel(), 100,
            rng=random.Random(123),
        )
        result_b = a.allocate(
            candidates, vf, MonteCarloSqrtModel(), 100,
            rng=random.Random(123),
        )
        assert result_a == result_b

    def test_different_seeds_can_diverge(self) -> None:
        """Different seeds produce different allocations (with
        non-trivial probability for non-degenerate inputs)."""
        a = ThompsonSamplingAlgorithm()
        candidates = [_view(0, score_stdev=10.0), _view(1, score_stdev=10.0)]

        def vf(turn: TurnView) -> float:
            return 1.0  # equal values, stochastic-only differentiation

        results = [
            a.allocate(candidates, vf, MonteCarloSqrtModel(), 50,
                       rng=random.Random(s))
            for s in (1, 2, 3, 4, 5)
        ]
        # At least one pair differs — TS is genuinely stochastic.
        all_same = all(r == results[0] for r in results)
        assert not all_same

    def test_default_rng_threading(self) -> None:
        """Algorithm constructed with a default_rng uses it when caller
        doesn't supply one (the factory path for `ts_seed`)."""
        rng = random.Random(7)
        a = ThompsonSamplingAlgorithm(default_rng=rng)
        candidates = [_view(0), _view(1)]
        result = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 30,
        )
        assert sum(result.values()) == 30

    def test_seeded_via_factory_is_deterministic(self) -> None:
        """The factory's `ts_seed` parameter constructs an internal
        Random(seed) — two parse calls produce identical allocations
        under identical inputs."""
        algo_a = _parse_allocation_algorithm({
            "allocation_algorithm": "thompson_sampling",
            "allocation_params": {"ts_seed": 999},
        })
        algo_b = _parse_allocation_algorithm({
            "allocation_algorithm": "thompson_sampling",
            "allocation_params": {"ts_seed": 999},
        })
        candidates = [_view(0), _view(1), _view(2)]
        r_a = algo_a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 30,
        )
        r_b = algo_b.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 30,
        )
        assert r_a == r_b


# ===========================================================================
# 4. UCB
# ===========================================================================


class TestUCB:

    def test_empty_candidates_returns_empty(self) -> None:
        a = UCBAlgorithm()
        result = a.allocate([], _const_value(1.0), MonteCarloSqrtModel(), 100)
        assert result == {}

    def test_budget_conservation(self) -> None:
        a = UCBAlgorithm()
        candidates = [_view(0), _view(1), _view(2)]
        result = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 50,
        )
        assert sum(result.values()) == 50

    def test_first_step_picks_max_value(self) -> None:
        """Step 1: T=1, log(T)=0, exploration bonus=0 for all — pure
        μ ranking. The highest-value candidate gets the first visit."""
        a = UCBAlgorithm(kappa=1.0)
        candidates = [_view(0), _view(1), _view(2)]

        def vf(turn: TurnView) -> float:
            return float(int(turn.turn_index))  # 0 < 1 < 2

        # Allocate just 1 visit; the winner should be turn 2 (max value).
        result = a.allocate(candidates, vf, MonteCarloSqrtModel(), 1)
        assert result == {TurnIndex(2): 1}

    def test_higher_kappa_explores_more(self) -> None:
        """A larger κ gives more weight to the exploration bonus,
        so under-visited candidates accumulate more visits relative
        to UCB with κ=1."""
        kappa_high = UCBAlgorithm(kappa=10.0)
        kappa_low = UCBAlgorithm(kappa=0.01)
        candidates = [_view(0), _view(1), _view(2)]

        def vf(turn: TurnView) -> float:
            return 10.0 if int(turn.turn_index) == 0 else 0.0

        result_high = kappa_high.allocate(
            candidates, vf, MonteCarloSqrtModel(), 30,
        )
        result_low = kappa_low.allocate(
            candidates, vf, MonteCarloSqrtModel(), 30,
        )
        # Low κ: most visits go to the leader.
        assert result_low.get(TurnIndex(0), 0) > result_high.get(TurnIndex(0), 0)
        # High κ: more visits go to non-leaders.
        total_non_leader_high = (
            result_high.get(TurnIndex(1), 0) + result_high.get(TurnIndex(2), 0)
        )
        total_non_leader_low = (
            result_low.get(TurnIndex(1), 0) + result_low.get(TurnIndex(2), 0)
        )
        assert total_non_leader_high > total_non_leader_low

    def test_does_not_consume_visit_scaling_model(self) -> None:
        """UCB ignores the visit-scaling model (per §3.4 of the
        roadmap). Two different models produce identical allocations."""
        a = UCBAlgorithm(kappa=1.0)
        candidates = [_view(0), _view(1)]
        r1 = a.allocate(
            candidates, _identity_turn_value(), MonteCarloSqrtModel(), 20,
        )
        r2 = a.allocate(
            candidates, _identity_turn_value(), DiminishingReturnsLogModel(), 20,
        )
        assert r1 == r2

    def test_beale_welford_shift_no_div_by_zero(self) -> None:
        """`n(c) + 1` denominator: no candidate ever has division
        by zero, even at step 1 when no visits are allocated yet."""
        a = UCBAlgorithm(kappa=1.0)
        candidates = [_view(0), _view(1)]
        # Allocating 1 visit shouldn't raise.
        result = a.allocate(
            candidates, _const_value(0.0), MonteCarloSqrtModel(), 1,
        )
        assert sum(result.values()) == 1


# ===========================================================================
# 5. _parse_allocation_algorithm — factory + refusal surface
# ===========================================================================


class TestParseAllocationAlgorithm:

    def test_greedy_eig_resolves(self) -> None:
        a = _parse_allocation_algorithm({"allocation_algorithm": "greedy_eig"})
        assert isinstance(a, GreedyEIGAlgorithm)

    def test_knowledge_gradient_resolves(self) -> None:
        a = _parse_allocation_algorithm({"allocation_algorithm": "knowledge_gradient"})
        assert isinstance(a, KnowledgeGradientAlgorithm)

    def test_thompson_sampling_resolves(self) -> None:
        a = _parse_allocation_algorithm({"allocation_algorithm": "thompson_sampling"})
        assert isinstance(a, ThompsonSamplingAlgorithm)

    def test_ucb_resolves_default_kappa(self) -> None:
        a = _parse_allocation_algorithm({"allocation_algorithm": "ucb"})
        assert isinstance(a, UCBAlgorithm)
        assert a.kappa == 1.0

    def test_ucb_resolves_custom_kappa(self) -> None:
        a = _parse_allocation_algorithm({
            "allocation_algorithm": "ucb",
            "allocation_params": {"ucb_kappa": 2.5},
        })
        assert isinstance(a, UCBAlgorithm)
        assert a.kappa == 2.5

    def test_unknown_algorithm_raises_allocation_invalid(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({"allocation_algorithm": "no_such_algo"})
        assert exc.value.code == "allocation_invalid"
        assert exc.value.detail["allocation_algorithm"] == "no_such_algo"
        valid = exc.value.detail.get("valid")
        assert isinstance(valid, list)
        assert set(valid) == {
            "greedy_eig", "knowledge_gradient", "thompson_sampling", "ucb",
            "learned_piecewise",
        }

    def test_missing_algorithm_name_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({})
        assert exc.value.code == "allocation_invalid"

    def test_non_string_algorithm_name_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({"allocation_algorithm": 42})
        assert exc.value.code == "allocation_invalid"

    def test_non_dict_params_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "ucb",
                "allocation_params": "not a dict",
            })
        assert exc.value.code == "allocation_invalid"

    def test_unknown_param_key_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "ucb",
                "allocation_params": {"ucb_kappa": 1.0, "no_such_param": True},
            })
        assert exc.value.code == "allocation_invalid"
        assert "no_such_param" in exc.value.detail["unknown_params"]

    def test_ts_seed_non_int_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "thompson_sampling",
                "allocation_params": {"ts_seed": "abc"},
            })
        assert exc.value.code == "allocation_invalid"

    def test_ts_seed_bool_rejected(self) -> None:
        """Python's bool is an int subclass; ts_seed=True must NOT
        silently parse as seed=1."""
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "thompson_sampling",
                "allocation_params": {"ts_seed": True},
            })
        assert exc.value.code == "allocation_invalid"

    def test_ucb_kappa_negative_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "ucb",
                "allocation_params": {"ucb_kappa": -1.0},
            })
        assert exc.value.code == "allocation_invalid"

    def test_ucb_kappa_zero_raises(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "ucb",
                "allocation_params": {"ucb_kappa": 0.0},
            })
        assert exc.value.code == "allocation_invalid"

    def test_ucb_kappa_bool_rejected(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "ucb",
                "allocation_params": {"ucb_kappa": True},
            })
        assert exc.value.code == "allocation_invalid"

    def test_greedy_eig_disallows_params(self) -> None:
        """greedy_eig has no parameters; any allocation_params key
        is rejected."""
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _parse_allocation_algorithm({
                "allocation_algorithm": "greedy_eig",
                "allocation_params": {"some_param": 1},
            })
        assert exc.value.code == "allocation_invalid"

    def test_registered_algorithm_names_helper(self) -> None:
        names = _registered_algorithm_names()
        assert set(names) == {
            "greedy_eig", "knowledge_gradient", "thompson_sampling", "ucb",
            "learned_piecewise",
        }
        assert names == sorted(names)
