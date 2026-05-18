"""
tests/test_adaptive_selector_pluggability.py — Regression tests for v1.0.23.

Pins the selector-pluggability + window-correction contract introduced
per `proxy/docs/roadmap-adaptive-selector-pluggability.md`. Covers six
categories aligned with the roadmap's commit-6 test plan:

  1. View construction (`MoveView`, `TurnView`, `_build_move_views`,
     `_build_turn_views`).
  2. Selection-policy primitives — the four move-axis policies plus
     the two turn-axis policies, on synthetic scored lists.
  3. `RegistryInterpreter` selector accessors — `Optional` return
     shape distinguishing absent / bound / broken bindings.
  4. Axis resolution (valid shapes from §8.1 of the roadmap).
  5. Configuration-consistency refusal (§8.2's four
     `AdaptiveConfigurationError` codes).
  6. Window correction — same-color-predecessor expansion in
     move-space; default-`window_size=2` semantics.

End-to-end integration at the dispatcher level (TestDispatchEndToEnd)
exercises `_dispatch_deepening_set` with a real `RegistryInterpreter`
constructed from synthetic `analysis_config`, exercising the full
composition: interpreter setup, axis resolution, view construction,
user-authored selector invocation, selection-policy application,
and window expansion. Full-coroutine-level integration (with mocked
spawn, response streaming, etc.) is left to a follow-on arc — the
dispatcher-level tests pin the dispatch composition's behaviour
end-to-end; the coroutine wrapper is a thin transformer that does
not add semantically distinct surface.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import (  # noqa: E402
    AnalyzeResponse,
    Color,
    MoveIndex,
    TurnIndex,
)
from middleware.adaptive_reevaluate import (  # noqa: E402
    AdaptiveConfigurationError,
    AdaptiveState,
    MoveView,
    TurnView,
    _apply_selection_policy_move,
    _apply_selection_policy_turn,
    _build_move_views,
    _build_turn_views,
    _collect_per_move_deltas,
    _default_move_selector,
    _dispatch_deepening_set,
    _expand_window_same_color,
    _resolve_axis_and_selector,
    _select_per_color_quantile_move,
    _select_pooled_quantile_move,
    _select_pooled_quantile_turn,
    _select_per_color_threshold_move,
    _select_top_k_move,
    _select_top_k_turn,
)
from registry_interpreter import RegistryInterpreter  # noqa: E402


def _make_packet(turn_number: int) -> AnalyzeResponse:
    """Synthetic AnalyzeResponse at the given turn (no per-turn payload)."""
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn_number,
        opaque={"moveInfos": [], "rootInfo": {}, "extra": {}},
    )


def _build_interpreter(
    bindings: dict[str, Any],
    symbols: dict[str, str],
    parameters: dict[str, Any] | None = None,
) -> RegistryInterpreter:
    """Convenience constructor for a RegistryInterpreter with a small config."""
    return RegistryInterpreter({
        "bindings": bindings,
        "symbols": symbols,
        "parameters": parameters or {},
    })


# ---------------------------------------------------------------------------
# 1. View construction
# ---------------------------------------------------------------------------


class TestViewConstruction:

    def test_move_view_carries_fields(self) -> None:
        before = _make_packet(0)
        after = _make_packet(1)
        view = MoveView(
            color="black",
            move_index=MoveIndex(0),
            deltas=[0.1, 0.2],
            before=before,
            after=after,
        )
        assert view.color == "black"
        assert int(view.move_index) == 0
        assert view.deltas == [0.1, 0.2]
        assert view.before is before
        assert view.after is after

    def test_turn_view_carries_fields(self) -> None:
        packet = _make_packet(3)
        view = TurnView(
            turn_index=TurnIndex(3),
            to_play="white",
            packet=packet,
        )
        assert int(view.turn_index) == 3
        assert view.to_play == "white"
        assert view.packet is packet

    def test_build_move_views_constructs_from_finals(self) -> None:
        # Black's MoveIndex(0) bridges turns 0→1; White's MoveIndex(0)
        # bridges turns 1→2. Finals at turns 0, 1, 2 are needed.
        finals = [_make_packet(0), _make_packet(1), _make_packet(2)]
        turn_maps: dict[Color, dict[MoveIndex, list[float]]] = {
            "black": {MoveIndex(0): [0.1, 0.15]},
            "white": {MoveIndex(0): [0.2]},
        }
        views = _build_move_views(finals, turn_maps, AdaptiveState())
        assert len(views) == 2
        black_view = next(v for v in views if v.color == "black")
        assert int(black_view.move_index) == 0
        assert int(black_view.before.turn_number) == 0
        assert int(black_view.after.turn_number) == 1
        white_view = next(v for v in views if v.color == "white")
        assert int(white_view.move_index) == 0
        assert int(white_view.before.turn_number) == 1
        assert int(white_view.after.turn_number) == 2

    def test_build_move_views_skips_when_endpoints_missing(self) -> None:
        # Turn 1 absent — Black's MoveIndex(0) (which needs turns 0 and 1)
        # and White's MoveIndex(0) (which needs turns 1 and 2) are
        # both un-constructable.
        finals = [_make_packet(0), _make_packet(2)]
        turn_maps: dict[Color, dict[MoveIndex, list[float]]] = {
            "black": {MoveIndex(0): [0.1]},
            "white": {MoveIndex(0): [0.2]},
        }
        views = _build_move_views(finals, turn_maps, AdaptiveState())
        assert views == []

    def test_build_turn_views_assigns_to_play(self) -> None:
        finals = [_make_packet(0), _make_packet(1), _make_packet(2)]
        views = _build_turn_views(finals, AdaptiveState())
        assert len(views) == 3
        by_turn = {int(v.turn_index): v for v in views}
        assert by_turn[0].to_play == "black"  # Black plays from turn 0
        assert by_turn[1].to_play == "white"  # White responds at turn 1
        assert by_turn[2].to_play == "black"  # Black plays again


# ---------------------------------------------------------------------------
# 2. Selection-policy primitives
# ---------------------------------------------------------------------------


class TestSelectionPoliciesMove:

    def test_per_color_quantile_picks_bottom_each_color(self) -> None:
        scored: list[tuple[Color, MoveIndex, float]] = [
            ("black", MoveIndex(0), 0.1),
            ("black", MoveIndex(1), 0.5),
            ("black", MoveIndex(2), 0.9),
            ("black", MoveIndex(3), 0.4),
            ("white", MoveIndex(0), 0.2),
            ("white", MoveIndex(1), 0.7),
            ("white", MoveIndex(2), 0.3),
            ("white", MoveIndex(3), 0.8),
        ]
        # Per-color (4 items each): threshold_idx = int(4 * 0.25) = 1.
        # Black sorted: [0.1, 0.4, 0.5, 0.9] → threshold = 0.4
        #   picks {(black, 0), (black, 3)}.
        # White sorted: [0.2, 0.3, 0.7, 0.8] → threshold = 0.3
        #   picks {(white, 0), (white, 2)}.
        result = _select_per_color_quantile_move(scored, worst_quantile=0.25)
        assert set(result) == {
            ("black", MoveIndex(0)),
            ("black", MoveIndex(3)),
            ("white", MoveIndex(0)),
            ("white", MoveIndex(2)),
        }

    def test_pooled_quantile_picks_pooled_bottom(self) -> None:
        scored: list[tuple[Color, MoveIndex, float]] = [
            ("black", MoveIndex(0), 0.9),
            ("black", MoveIndex(1), 0.1),
            ("white", MoveIndex(0), 0.5),
            ("white", MoveIndex(1), 0.2),
        ]
        # Pooled, 4 items, threshold_idx = int(4 * 0.5) = 2.
        # Sorted scalars: [0.1, 0.2, 0.5, 0.9] → threshold = 0.5.
        # Picks scalar <= 0.5: 0.1, 0.2, 0.5.
        result = _select_pooled_quantile_move(scored, worst_quantile=0.5)
        assert set(result) == {
            ("black", MoveIndex(1)),
            ("white", MoveIndex(0)),
            ("white", MoveIndex(1)),
        }

    def test_per_color_threshold(self) -> None:
        scored: list[tuple[Color, MoveIndex, float]] = [
            ("black", MoveIndex(0), -0.1),
            ("black", MoveIndex(1), 0.5),
            ("white", MoveIndex(0), -0.2),
            ("white", MoveIndex(1), 0.3),
        ]
        result = _select_per_color_threshold_move(
            scored, black_threshold=0.0, white_threshold=-0.1,
        )
        assert set(result) == {
            ("black", MoveIndex(0)),  # -0.1 <= 0.0
            ("white", MoveIndex(0)),  # -0.2 <= -0.1
        }

    def test_top_k_picks_bottom_k(self) -> None:
        scored: list[tuple[Color, MoveIndex, float]] = [
            ("black", MoveIndex(0), 0.5),
            ("black", MoveIndex(1), 0.1),
            ("white", MoveIndex(0), 0.3),
            ("white", MoveIndex(1), 0.7),
        ]
        result = _select_top_k_move(scored, top_k=2)
        # Bottom-2 pooled: scalars 0.1, 0.3.
        assert set(result) == {
            ("black", MoveIndex(1)),
            ("white", MoveIndex(0)),
        }


class TestSelectionPoliciesTurn:

    def test_pooled_quantile_turn(self) -> None:
        scored = [
            (TurnIndex(0), 0.9),
            (TurnIndex(1), 0.1),
            (TurnIndex(2), 0.5),
            (TurnIndex(3), 0.3),
        ]
        # threshold_idx = int(4 * 0.5) = 2; sorted [0.1, 0.3, 0.5, 0.9]
        # → threshold = 0.5; picks <= 0.5.
        result = _select_pooled_quantile_turn(scored, worst_quantile=0.5)
        assert set(result) == {TurnIndex(1), TurnIndex(2), TurnIndex(3)}

    def test_top_k_turn(self) -> None:
        scored = [
            (TurnIndex(0), 0.5),
            (TurnIndex(1), 0.1),
            (TurnIndex(2), 0.7),
        ]
        result = _select_top_k_turn(scored, top_k=1)
        assert result == [TurnIndex(1)]


# ---------------------------------------------------------------------------
# 3. RegistryInterpreter selector accessors
# ---------------------------------------------------------------------------


class TestRegistryInterpreterSelectors:

    def test_returns_none_when_binding_absent(self) -> None:
        interp = _build_interpreter(bindings={}, symbols={})
        assert interp.get_move_selector_fn() is None
        assert interp.get_turn_selector_fn() is None

    def test_returns_callable_when_binding_present(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "my_metric"},
            symbols={"my_metric": "mean(x.deltas)"},
        )
        fn = interp.get_move_selector_fn()
        assert fn is not None
        assert callable(fn)

    def test_returns_none_when_named_symbol_missing(self) -> None:
        # Binding names a symbol that wasn't defined in `symbols`.
        interp = _build_interpreter(
            bindings={"move_selector_fn": "no_such_symbol"},
            symbols={},
        )
        assert interp.get_move_selector_fn() is None

    def test_turn_selector_independent_of_move(self) -> None:
        interp = _build_interpreter(
            bindings={"turn_selector_fn": "tm"},
            symbols={"tm": "1.0"},
        )
        assert interp.get_move_selector_fn() is None
        assert interp.get_turn_selector_fn() is not None


# ---------------------------------------------------------------------------
# 4. Axis resolution (valid shapes)
# ---------------------------------------------------------------------------


class TestAxisResolutionValid:

    def test_no_interpreter_defaults_to_move(self) -> None:
        axis, sel = _resolve_axis_and_selector(None, {})
        assert axis == "move"
        assert sel is None

    def test_move_binding_alone(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "m"},
            symbols={"m": "1.0"},
        )
        axis, sel = _resolve_axis_and_selector(interp, {})
        assert axis == "move"
        assert sel is not None

    def test_turn_binding_alone(self) -> None:
        interp = _build_interpreter(
            bindings={"turn_selector_fn": "t"},
            symbols={"t": "2.0"},
        )
        axis, sel = _resolve_axis_and_selector(interp, {})
        assert axis == "turn"
        assert sel is not None

    def test_both_bindings_with_move_disambiguator(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "m", "turn_selector_fn": "t"},
            symbols={"m": "1.0", "t": "2.0"},
        )
        axis, sel = _resolve_axis_and_selector(
            interp, {"selector_axis": "move"},
        )
        assert axis == "move"
        assert sel is not None

    def test_both_bindings_with_turn_disambiguator(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "m", "turn_selector_fn": "t"},
            symbols={"m": "1.0", "t": "2.0"},
        )
        axis, sel = _resolve_axis_and_selector(
            interp, {"selector_axis": "turn"},
        )
        assert axis == "turn"
        assert sel is not None


# ---------------------------------------------------------------------------
# 5. Configuration-consistency refusal
# ---------------------------------------------------------------------------


class TestConfigurationRefusal:

    def test_ambiguous_axis_both_bindings_no_disambiguator(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "m", "turn_selector_fn": "t"},
            symbols={"m": "1.0", "t": "2.0"},
        )
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _resolve_axis_and_selector(interp, {})
        assert exc.value.code == "ambiguous_axis"

    def test_axis_binding_mismatch_move_axis_no_move_binding(self) -> None:
        interp = _build_interpreter(
            bindings={"turn_selector_fn": "t"},
            symbols={"t": "1.0"},
        )
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _resolve_axis_and_selector(interp, {"selector_axis": "move"})
        assert exc.value.code == "axis_binding_mismatch"
        assert exc.value.detail.get("selector_axis") == "move"

    def test_axis_binding_mismatch_turn_axis_no_turn_binding(self) -> None:
        interp = _build_interpreter(
            bindings={"move_selector_fn": "m"},
            symbols={"m": "1.0"},
        )
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _resolve_axis_and_selector(interp, {"selector_axis": "turn"})
        assert exc.value.code == "axis_binding_mismatch"
        assert exc.value.detail.get("selector_axis") == "turn"

    def test_axis_binding_mismatch_invalid_selector_axis_value(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _resolve_axis_and_selector(None, {"selector_axis": "foo"})
        assert exc.value.code == "axis_binding_mismatch"

    def test_policy_axis_mismatch_per_color_on_turn(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _apply_selection_policy_turn(
                [(TurnIndex(0), 0.1)],
                {"selection_policy": "per_color_quantile"},
            )
        assert exc.value.code == "policy_axis_mismatch"

    def test_policy_axis_mismatch_unknown_policy_name_move(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _apply_selection_policy_move(
                [("black", MoveIndex(0), 0.1)],
                {"selection_policy": "this_policy_does_not_exist"},
            )
        assert exc.value.code == "policy_axis_mismatch"

    def test_policy_parameters_invalid_top_k_missing_move(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _apply_selection_policy_move(
                [("black", MoveIndex(0), 0.1)],
                {"selection_policy": "top_k"},
            )
        assert exc.value.code == "policy_parameters_invalid"
        assert exc.value.detail.get("missing") == ["top_k"]

    def test_policy_parameters_invalid_top_k_missing_turn(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _apply_selection_policy_turn(
                [(TurnIndex(0), 0.1)],
                {"selection_policy": "top_k"},
            )
        assert exc.value.code == "policy_parameters_invalid"
        assert exc.value.detail.get("missing") == ["top_k"]

    def test_policy_parameters_invalid_threshold_missing(self) -> None:
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _apply_selection_policy_move(
                [("black", MoveIndex(0), 0.1)],
                {"selection_policy": "per_color_threshold"},
            )
        assert exc.value.code == "policy_parameters_invalid"
        # Both thresholds missing.
        missing = exc.value.detail.get("missing")
        assert isinstance(missing, list)
        assert "black_threshold" in missing
        assert "white_threshold" in missing


# ---------------------------------------------------------------------------
# 6. Window correction — same-color predecessor expansion
# ---------------------------------------------------------------------------


class TestExpandWindowSameColor:

    def test_window_1_just_the_move_itself(self) -> None:
        # Black's MoveIndex(2) → turns (4, 5). Window 1 = just this move.
        worst: list[tuple[Color, MoveIndex]] = [("black", MoveIndex(2))]
        all_turns = {TurnIndex(i) for i in range(10)}
        result = _expand_window_same_color(worst, all_turns, window_size=1)
        assert result == {TurnIndex(4), TurnIndex(5)}

    def test_window_2_includes_self_and_same_color_predecessor(self) -> None:
        # Black's MoveIndex(3) → (6, 7). Predecessor Black MoveIndex(2)
        # → (4, 5). Window 2 unions both pairs.
        worst: list[tuple[Color, MoveIndex]] = [("black", MoveIndex(3))]
        all_turns = {TurnIndex(i) for i in range(10)}
        result = _expand_window_same_color(worst, all_turns, window_size=2)
        assert result == {
            TurnIndex(4), TurnIndex(5),
            TurnIndex(6), TurnIndex(7),
        }

    def test_window_3_includes_two_predecessors(self) -> None:
        worst: list[tuple[Color, MoveIndex]] = [("black", MoveIndex(2))]
        all_turns = {TurnIndex(i) for i in range(10)}
        result = _expand_window_same_color(worst, all_turns, window_size=3)
        # Black moves 2, 1, 0 → turns (4,5), (2,3), (0,1).
        assert result == {
            TurnIndex(0), TurnIndex(1),
            TurnIndex(2), TurnIndex(3),
            TurnIndex(4), TurnIndex(5),
        }

    def test_window_stops_at_move_0(self) -> None:
        # MoveIndex(-1) doesn't exist; expansion stops cleanly.
        worst: list[tuple[Color, MoveIndex]] = [("black", MoveIndex(1))]
        all_turns = {TurnIndex(i) for i in range(10)}
        result = _expand_window_same_color(worst, all_turns, window_size=10)
        # Only move 1 and move 0 produce valid pairs.
        assert result == {
            TurnIndex(0), TurnIndex(1),
            TurnIndex(2), TurnIndex(3),
        }

    def test_window_white_uses_white_predecessors(self) -> None:
        # White's MoveIndex(2) → (5, 6). White MoveIndex(1) → (3, 4).
        worst: list[tuple[Color, MoveIndex]] = [("white", MoveIndex(2))]
        all_turns = {TurnIndex(i) for i in range(10)}
        result = _expand_window_same_color(worst, all_turns, window_size=2)
        assert result == {
            TurnIndex(3), TurnIndex(4),
            TurnIndex(5), TurnIndex(6),
        }

    def test_window_drops_out_of_range_turns(self) -> None:
        # If a predecessor's turn is not in all_turns, drop it.
        worst: list[tuple[Color, MoveIndex]] = [("black", MoveIndex(3))]
        all_turns = {TurnIndex(4), TurnIndex(5), TurnIndex(6), TurnIndex(7)}
        result = _expand_window_same_color(worst, all_turns, window_size=3)
        # Move 3 → (6,7), move 2 → (4,5), move 1 → (2,3) excluded.
        assert result == {
            TurnIndex(4), TurnIndex(5),
            TurnIndex(6), TurnIndex(7),
        }


# ---------------------------------------------------------------------------
# Adjacent unit coverage: default selector + delta-collection helper
# ---------------------------------------------------------------------------


class TestDefaultSelectorAndCollect:

    def test_default_move_selector_is_mean_of_deltas(self) -> None:
        assert _default_move_selector([0.1, 0.2, 0.3]) == pytest.approx(0.2)
        assert _default_move_selector([0.5]) == pytest.approx(0.5)

    def test_collect_per_move_deltas_groups_by_color_and_move(self) -> None:
        # Two responses each carrying per-color deltas at different moves.
        r0 = AnalyzeResponse(
            is_during_search=False, turn_number=0,
            opaque={"extra": {
                "black": {"deltas": {"0": 0.1, "1": 0.3}},
                "white": {"deltas": {"0": 0.2}},
            }},
        )
        r1 = AnalyzeResponse(
            is_during_search=False, turn_number=1,
            opaque={"extra": {
                "black": {"deltas": {"0": 0.15}},  # repeat — accumulates
                "white": {"deltas": {"0": 0.25}},
            }},
        )
        turn_maps = _collect_per_move_deltas([r0, r1])
        assert turn_maps["black"][MoveIndex(0)] == [0.1, 0.15]
        assert turn_maps["black"][MoveIndex(1)] == [0.3]
        assert turn_maps["white"][MoveIndex(0)] == [0.2, 0.25]


# ---------------------------------------------------------------------------
# 7. End-to-end dispatch integration
# ---------------------------------------------------------------------------
#
# Exercises `_dispatch_deepening_set` with a real `RegistryInterpreter`
# built from synthetic `analysis_config`. Covers the full composition:
# interpreter setup → axis resolution → view construction →
# user-authored selector invocation (asteval-compiled, dataclass
# attribute access) → selection-policy application → window
# expansion. The unit tests above pin each step in isolation; these
# tests pin the composition.


def _build_finals_with_per_move_deltas() -> list[AnalyzeResponse]:
    """6 turns (0..5) with per-move deltas embedded in turn 0's opaque.

    Black moves 0, 1, 2 carry deltas 0.1, 0.5, 0.9 (move 0 worst).
    White moves 0, 1 carry deltas 0.2, 0.7 (move 0 worst).
    Each move has a single per-arrival delta — the user selector's
    output equals the embedded delta directly.
    """
    deltas_payload = {
        "moveInfos": [],
        "rootInfo": {},
        "extra": {
            "black": {"deltas": {"0": 0.1, "1": 0.5, "2": 0.9}},
            "white": {"deltas": {"0": 0.2, "1": 0.7}},
        },
    }
    return [
        AnalyzeResponse(
            is_during_search=False,
            turn_number=i,
            opaque=deltas_payload if i == 0 else {
                "moveInfos": [], "rootInfo": {}, "extra": {},
            },
        )
        for i in range(6)
    ]


class TestDispatchEndToEnd:

    def test_move_axis_user_selector_drives_per_color_quantile(self) -> None:
        """User binds move_selector_fn = mean(x.deltas); per_color_quantile."""
        finals = _build_finals_with_per_move_deltas()
        analysis_config: dict[str, Any] = {
            "bindings": {"move_selector_fn": "my_metric"},
            "symbols": {"my_metric": "mean(x.deltas)"},
            "parameters": {},
        }
        cap_meta: dict[str, Any] = {
            "selection_policy": "per_color_quantile",
            "worst_quantile": 0.34,
        }
        all_turns = {TurnIndex(i) for i in range(6)}
        result = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta,
            analysis_config=analysis_config,
            window_size=1,
            all_turns=all_turns,
        )
        # Per-color quantile 0.34:
        #   Black scores [0.1, 0.5, 0.9]: threshold_idx=int(3*0.34)=1,
        #     threshold=0.5; worst <= 0.5: m=0, m=1.
        #     Pairs: (0,1), (2,3).
        #   White scores [0.2, 0.7]: threshold_idx=int(2*0.34)=0,
        #     threshold=0.2; worst <= 0.2: m=0.
        #     Pair: (1,2).
        # Window_size=1 — no predecessor expansion.
        # Union: {0,1,2,3} ∪ {1,2} = {0,1,2,3}.
        assert result == {TurnIndex(0), TurnIndex(1), TurnIndex(2), TurnIndex(3)}

    def test_move_axis_user_selector_with_top_k_selection(self) -> None:
        """User selector + top_k=1 picks the single worst pooled move."""
        finals = _build_finals_with_per_move_deltas()
        analysis_config: dict[str, Any] = {
            "bindings": {"move_selector_fn": "m"},
            "symbols": {"m": "mean(x.deltas)"},
            "parameters": {},
        }
        cap_meta: dict[str, Any] = {
            "selection_policy": "top_k",
            "top_k": 1,
        }
        all_turns = {TurnIndex(i) for i in range(6)}
        result = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta,
            analysis_config=analysis_config,
            window_size=1,
            all_turns=all_turns,
        )
        # Pooled bottom-1: 0.1 (Black m=0). Pair: (0,1).
        assert result == {TurnIndex(0), TurnIndex(1)}

    def test_move_axis_user_selector_using_before_after_packets(self) -> None:
        """Selector reads x.before and x.after to compute a transition metric.

        Tests nested attribute + dict access in the asteval-compiled
        expression — the seam that lets users author cross-position
        move-loss metrics.
        """
        # Embed scoreLead per turn so the selector can read it.
        score_leads = {0: 0.0, 1: -0.1, 2: -0.05, 3: -0.4, 4: -0.35, 5: -0.45}
        finals = []
        for i in range(6):
            payload: dict[str, Any] = {
                "moveInfos": [],
                "rootInfo": {"scoreLead": score_leads[i]},
                "extra": {},
            }
            if i == 0:
                payload["extra"] = {
                    "black": {"deltas": {"0": 0.0, "1": 0.0, "2": 0.0}},
                    "white": {"deltas": {"0": 0.0, "1": 0.0}},
                }
            finals.append(AnalyzeResponse(
                is_during_search=False, turn_number=i, opaque=payload,
            ))
        # Selector: score-lead drop across the move (after - before).
        # Lower (more negative) = worse from this color's perspective.
        analysis_config: dict[str, Any] = {
            "bindings": {"move_selector_fn": "drop"},
            "symbols": {
                "drop": (
                    "x.after.opaque['rootInfo']['scoreLead'] - "
                    "x.before.opaque['rootInfo']['scoreLead']"
                ),
            },
            "parameters": {},
        }
        # top_k=1 picks the worst single move across both colors.
        cap_meta: dict[str, Any] = {
            "selection_policy": "top_k",
            "top_k": 1,
        }
        all_turns = {TurnIndex(i) for i in range(6)}
        result = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta,
            analysis_config=analysis_config,
            window_size=1,
            all_turns=all_turns,
        )
        # Per-move score-lead drops:
        #   Black m=0 (turns 0→1): -0.1 - 0.0 = -0.10
        #   White m=0 (turns 1→2): -0.05 - -0.1 = +0.05
        #   Black m=1 (turns 2→3): -0.4 - -0.05 = -0.35  ← worst (lowest)
        #   White m=1 (turns 3→4): -0.35 - -0.4 = +0.05
        #   Black m=2 (turns 4→5): -0.45 - -0.35 = -0.10
        # top_k=1: Black m=1 (drop -0.35). Pair: (2,3).
        assert result == {TurnIndex(2), TurnIndex(3)}

    def test_turn_axis_user_selector_pooled_quantile(self) -> None:
        """User binds turn_selector_fn; pooled_quantile selection."""
        finals = [
            AnalyzeResponse(
                is_during_search=False, turn_number=i,
                opaque={"moveInfos": [], "rootInfo": {}, "extra": {}},
            )
            for i in range(6)
        ]
        # Selector returns x.turn_index (lower index = worse).
        # Verifies attribute access on TurnView (NewType-wrapped int).
        analysis_config: dict[str, Any] = {
            "bindings": {"turn_selector_fn": "tm"},
            "symbols": {"tm": "x.turn_index"},
            "parameters": {},
        }
        cap_meta: dict[str, Any] = {
            "selection_policy": "top_k",
            "top_k": 3,
        }
        all_turns = {TurnIndex(i) for i in range(6)}
        result = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta,
            analysis_config=analysis_config,
            window_size=1,  # ignored on turn axis (no framework window)
            all_turns=all_turns,
        )
        # Scores: 0, 1, 2, 3, 4, 5. Bottom-3: {0, 1, 2}.
        assert result == {TurnIndex(0), TurnIndex(1), TurnIndex(2)}

    def test_default_path_no_binding_uses_hardcoded_selector(self) -> None:
        """No analysis_config / no binding → hardcoded default path."""
        finals = _build_finals_with_per_move_deltas()
        cap_meta: dict[str, Any] = {"worst_quantile": 0.34}
        all_turns = {TurnIndex(i) for i in range(6)}
        result = _dispatch_deepening_set(
            finals=finals,
            cap_meta=cap_meta,
            analysis_config=None,
            window_size=1,
            all_turns=all_turns,
        )
        # Default path: per-color quantile on mean-of-deltas. With
        # single-arrival deltas, scores = the embedded delta values.
        # Same expected set as the equivalent user-selector test above.
        assert result == {TurnIndex(0), TurnIndex(1), TurnIndex(2), TurnIndex(3)}

    def test_ambiguous_axis_raises_in_dispatch(self) -> None:
        """End-to-end: dispatch raises AdaptiveConfigurationError on
        ambiguous axis. Verifies the refusal propagates through the
        composition (not just at the helper level)."""
        finals = _build_finals_with_per_move_deltas()
        analysis_config: dict[str, Any] = {
            "bindings": {
                "move_selector_fn": "m",
                "turn_selector_fn": "t",
            },
            "symbols": {
                "m": "mean(x.deltas)",
                "t": "x.turn_index",
            },
            "parameters": {},
        }
        cap_meta: dict[str, Any] = {}  # no selector_axis disambiguator
        all_turns = {TurnIndex(i) for i in range(6)}
        with pytest.raises(AdaptiveConfigurationError) as exc:
            _dispatch_deepening_set(
                finals=finals,
                cap_meta=cap_meta,
                analysis_config=analysis_config,
                window_size=1,
                all_turns=all_turns,
            )
        assert exc.value.code == "ambiguous_axis"
