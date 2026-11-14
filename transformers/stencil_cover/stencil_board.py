# SPDX-License-Identifier: MIT
"""KataGo request parsing and board-position reconstruction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from katago import KataGoAction, KataGoQuery
from .stencil_solver_geometry import MAX_COMPONENT_DIMENSION as _MAX_BOARD_SIZE


_MAX_INITIAL_STONES = 1_000
_MAX_REPLAY_MOVES = 1_000
_MAX_COORDINATE_LENGTH = 16


@dataclass(frozen=True)
class RequestContext:
    width: int
    height: int
    initial_stones: Tuple[Tuple[str, Optional[int]], ...]
    moves: Tuple[Tuple[str, Optional[int]], ...]
    policy_metadata: Dict[str, Any]


def _board_dimensions(opaque: Dict[str, Any]) -> Tuple[int, int]:
    width = opaque.get("boardXSize", 19)
    height = opaque.get("boardYSize", width)
    for name, value in (("boardXSize", width), ("boardYSize", height)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if not 1 <= value <= _MAX_BOARD_SIZE:
            raise ValueError(f"{name} must be in [1, {_MAX_BOARD_SIZE}]")
    return width, height


def _coordinate_index(
    coordinate: str, width: int, height: int, *, allow_pass: bool,
) -> Optional[int]:
    """Parse KataGo GTP-style or explicit (x,y) coordinates to top-row indices."""
    value = coordinate.strip()
    lowered = value.lower()
    if lowered in ("pass", "..") or (lowered == "tt" and max(width, height) <= 19):
        if allow_pass:
            return None
        raise ValueError("initialStones cannot contain a pass")

    numeric = re.fullmatch(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)", value)
    if numeric is not None:
        x, y = int(numeric.group(1)), int(numeric.group(2))
    else:
        gtp = re.fullmatch(r"([A-HJ-Z])([1-9][0-9]*)", value, re.IGNORECASE)
        if gtp is None:
            raise ValueError(f"invalid KataGo coordinate: {coordinate!r}")
        column = ord(gtp.group(1).upper()) - ord("A")
        if column > 8:
            column -= 1
        x = column
        y = height - int(gtp.group(2))

    if not 0 <= x < width or not 0 <= y < height:
        raise ValueError(
            f"coordinate {coordinate!r} is outside the {width}x{height} board"
        )
    return y * width + x


def _parse_position_entries(
    value: Any, field: str, width: int, height: int, *, allow_pass: bool,
) -> Tuple[Tuple[str, Optional[int]], ...]:
    if value is None:
        value = []
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    limit = _MAX_REPLAY_MOVES if allow_pass else _MAX_INITIAL_STONES
    if len(value) > limit:
        raise ValueError(f"{field} has more than {limit} entries")

    entries: List[Tuple[str, Optional[int]]] = []
    for i, item in enumerate(value):
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError(f"{field}[{i}] must be a [player, coordinate] pair")
        player, coordinate = item
        if player not in ("B", "W"):
            raise ValueError(f"{field}[{i}][0] must be 'B' or 'W'")
        if (
            not isinstance(coordinate, str)
            or not coordinate
            or len(coordinate) > _MAX_COORDINATE_LENGTH
        ):
            raise ValueError(f"{field}[{i}][1] must be a short coordinate string")
        point = _coordinate_index(
            coordinate, width, height, allow_pass=allow_pass,
        )
        entries.append((player, point))
    return tuple(entries)


def _neighbours(index: int, width: int, height: int) -> List[int]:
    x, y = index % width, index // width
    result: List[int] = []
    if x > 0:
        result.append(index - 1)
    if x + 1 < width:
        result.append(index + 1)
    if y > 0:
        result.append(index - width)
    if y + 1 < height:
        result.append(index + width)
    return result


def _chain_and_liberties(
    board: Sequence[Optional[str]], start: int, width: int, height: int,
) -> Tuple[set[int], set[int]]:
    """Return a same-colour chain and its distinct empty neighbouring points."""
    colour = board[start]
    if colour is None:
        return set(), set()
    stones: set[int] = set()
    liberties: set[int] = set()
    pending = [start]
    while pending:
        point = pending.pop()
        if point in stones:
            continue
        stones.add(point)
        for neighbour in _neighbours(point, width, height):
            adjacent = board[neighbour]
            if adjacent is None:
                liberties.add(neighbour)
            elif adjacent == colour and neighbour not in stones:
                pending.append(neighbour)
    return stones, liberties


def _board_at_turn(context: RequestContext, turn_number: int) -> List[Optional[str]]:
    """Replay the submitted position to the response's turnNumber.

    Moves are assumed to be KataGo-valid, just as in transposition_enricher.
    Captures and self-captures are applied; ko legality need not be rechecked
    because the source move sequence has already passed KataGo's rules engine.
    """
    if isinstance(turn_number, bool) or not isinstance(turn_number, int) or turn_number < 0:
        raise ValueError("response turnNumber must be a non-negative integer")
    if turn_number > len(context.moves):
        raise ValueError(
            f"response turnNumber {turn_number} exceeds the {len(context.moves)} submitted moves"
        )

    board: List[Optional[str]] = [None] * (context.width * context.height)
    for colour, point in context.initial_stones:
        assert point is not None
        if board[point] is not None:
            raise ValueError(f"overlapping initial stones at flat point {point}")
        board[point] = colour

    visited: set[int] = set()
    for point, board_colour in enumerate(board):
        if board_colour is None or point in visited:
            continue
        chain, liberties = _chain_and_liberties(
            board, point, context.width, context.height,
        )
        visited.update(chain)
        if not liberties:
            raise ValueError("initialStones contains a chain without liberties")

    for colour, point in context.moves[:turn_number]:
        if point is None:
            continue
        if board[point] is not None:
            raise ValueError(f"move plays on occupied point {point}")
        board[point] = colour
        opponent = "W" if colour == "B" else "B"

        checked_opponent_chains: set[int] = set()
        for neighbour in _neighbours(point, context.width, context.height):
            if board[neighbour] != opponent or neighbour in checked_opponent_chains:
                continue
            chain, liberties = _chain_and_liberties(
                board, neighbour, context.width, context.height,
            )
            checked_opponent_chains.update(chain)
            if not liberties:
                for captured in chain:
                    board[captured] = None

        own_chain, own_liberties = _chain_and_liberties(
            board, point, context.width, context.height,
        )
        if not own_liberties:
            for captured in own_chain:
                board[captured] = None
    return board


def _request_context(query: KataGoQuery) -> Optional[RequestContext]:
    if query.action != KataGoAction.ANALYZE:
        return None

    capabilities = query.opaque.get("capabilities")
    if not isinstance(capabilities, dict) or "stencil_cover" not in capabilities:
        return None

    policy_metadata = capabilities["stencil_cover"]
    if not isinstance(policy_metadata, dict):
        raise ValueError("stencil_cover capability metadata must be an object")

    width, height = _board_dimensions(query.opaque)
    initial_stones = _parse_position_entries(
        query.opaque.get("initialStones", []),
        "initialStones",
        width,
        height,
        allow_pass=False,
    )
    moves = _parse_position_entries(
        query.opaque.get("moves", []),
        "moves",
        width,
        height,
        allow_pass=True,
    )
    return RequestContext(
        width=width,
        height=height,
        initial_stones=initial_stones,
        moves=moves,
        policy_metadata=dict(policy_metadata),
    )
