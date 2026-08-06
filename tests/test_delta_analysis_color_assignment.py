"""
tests/test_delta_analysis_color_assignment.py — Regression coverage for
DeltaAnalysisState's per-color delta index assignment.

Pins the fix for the defect where `DeltaAnalysisState.__init__` derived
per-color delta buckets from a hardcoded `black_first=True` PARITY rule
(black owns odd global delta indices, white owns even) instead of from
the actual move colors carried in the `moves` list it is handed. Any
non-strictly-alternating-black-first game (white-first, handicap,
consecutive same-color moves, setup sequences) put deltas in the wrong
color bucket under the old rule, desynchronizing color-local indices
against a client that counts actual move colors.

The fix derives `black_delta_indices` / `white_delta_indices` from
`moves[j - 1][0]` for each global delta index `j` (delta[j] is caused
by 0-indexed move j-1), normalizing 'B'/'b' and 'W'/'w', and raising
loudly (ADR-0002) on any other color token.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from delta_analysis import DeltaAnalysisState  # noqa: E402


def _make_packet(score: float) -> Any:
    """A minimal opaque KataGo-shaped packet: no moveInfos, just a score
    field for the test delta_fn to read."""
    return {"moveInfos": [], "score": score}


def _delta_fn(pair: List[Any]) -> Any:
    # pair is the two-element [prev, cur] window produced by Window(-1, 0);
    # each element is the *preprocessed* opaque packet dict.
    prev, cur = pair
    return cur["score"] - prev["score"]


def _push_all(state: DeltaAnalysisState, n_moves: int) -> None:
    # Domain is n_moves + 1 packet slots (0..n_moves); deltas fire at
    # global indices 1..n_moves as each consecutive pair lands.
    for i in range(n_moves + 1):
        state.push_packet(i, (i, _make_packet(score=float(i))))


def _make_state(moves: List[List[str]]) -> DeltaAnalysisState:
    return DeltaAnalysisState(
        board_size=19,
        moves=moves,
        delta_fn=_delta_fn,
        triangular=False,
    )


class TestColorIndexDerivation:
    def test_black_first_alternating_regression(self) -> None:
        """Standard alternating black-first game: locks the pre-fix
        parity result for the ordinary case (black odd, white even)."""
        moves = [["B", "Q16"], ["W", "D17"], ["B", "D4"], ["W", "Q4"]]
        state = _make_state(moves)

        assert state._black_delta_indices == [1, 3]
        assert state._white_delta_indices == [2, 4]

        _push_all(state, n_moves=4)
        # delta[j] = score[j] - score[j-1] = 1.0 for every j with our
        # linear score fn; only the bucket assignment is under test.
        assert state.black_deltas == [1.0, 1.0]
        assert state.white_deltas == [1.0, 1.0]

    def test_white_first_lands_in_correct_buckets(self) -> None:
        """A white-first (handicap-style) game must NOT be forced into
        the black-owns-odd-indices parity rule."""
        moves = [["W", "Q16"], ["B", "D17"], ["W", "D4"], ["B", "Q4"]]
        state = _make_state(moves)

        # delta j is caused by moves[j-1]: j=1 -> moves[0]=W, j=2 -> B,
        # j=3 -> W, j=4 -> B. Inverted from the black-first case.
        assert state._white_delta_indices == [1, 3]
        assert state._black_delta_indices == [2, 4]

        _push_all(state, n_moves=4)
        assert state.white_deltas == [1.0, 1.0]
        assert state.black_deltas == [1.0, 1.0]

    def test_consecutive_same_color_moves_follow_actual_colors(self) -> None:
        """Non-alternating sequence (e.g. a setup/consecutive-color run):
        the parity rule would misassign every delta after the first
        break; the derived rule must track actual colors throughout."""
        moves = [["B", "Q16"], ["B", "D17"], ["W", "D4"], ["W", "Q4"]]
        state = _make_state(moves)

        # j=1 -> moves[0]=B, j=2 -> moves[1]=B, j=3 -> moves[2]=W,
        # j=4 -> moves[3]=W.
        assert state._black_delta_indices == [1, 2]
        assert state._white_delta_indices == [3, 4]

        _push_all(state, n_moves=4)
        assert state.black_deltas == [1.0, 1.0]
        assert state.white_deltas == [1.0, 1.0]

    def test_invalid_color_token_fails_loudly(self) -> None:
        """An unrecognized color token must raise rather than be guessed
        at (ADR-0002: fail loudly)."""
        moves = [["X", "Q16"], ["W", "D17"]]
        with pytest.raises(ValueError):
            _make_state(moves)

    def test_lowercase_color_tokens_are_normalized(self) -> None:
        moves = [["b", "Q16"], ["w", "D17"], ["b", "D4"], ["w", "Q4"]]
        state = _make_state(moves)

        assert state._black_delta_indices == [1, 3]
        assert state._white_delta_indices == [2, 4]
