# SPDX-License-Identifier: MIT
"""
KataGo response enricher for signed ownership stencil covers.

A query opts in with:
    capabilities: {"stencil_cover": {"truncation_threshold": 0.6}}

Only final analysis responses are processed. The response annotation is
written to opaque.stencilCover and contains independently solved positive-
and negative-ownership exact covers. Empty points use the configured ownership
cutoff; living chains are omitted and dead chains are assigned to their
opponent's territory.

Importing this module requires HiGHS and a valid proxy-owned stencil family.
Both are loaded eagerly: a missing dependency or invalid family prevents the
transformer from being composed.

See NOTICE for the file-specific MIT exception and KataGo attribution.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re
import threading
from typing import Any, Dict, List, Optional, Sequence, Tuple

import highspy
import numpy as np

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink
from katago import AnalyzeResponse, KataGoAction, KataGoQuery, KataGoResponse
from transformers.stencil_cache_storage import (
    redis_persistent_cache,
    register_cache_invalidator,
)

Stencil = Tuple[str, Tuple[Tuple[int, int], ...]]


@dataclass(frozen=True)
class RequestContext:
    width: int
    height: int
    threshold: float
    initial_stones: Tuple[Tuple[str, Optional[int]], ...]
    moves: Tuple[Tuple[str, Optional[int]], ...]


_MAX_BOARD_SIZE = 25
_SOLVER_TIME_LIMIT_MS = 250
# lru_cache is entry-bounded, not byte-bounded. Results are compact binary
# placement lists (<= about 2.5 KB even for a 25x25 all-singleton cover),
# never candidate matrices. 90,000 entries is roughly a 256 MB-scale cache
# including ordinary Python LRU/dict overhead; typical components are much smaller.
_SOLVER_CACHE_MAX_ENTRIES = 90_000
_OWNERSHIP_EPSILON = 1e-5
# Match KataGo PlayUtils::computeAnticipatedStatusesWithOwnership. These
# classify occupied chains; truncation_threshold remains the confidence cutoff
# for empty intersections only.
_STONE_AVG_LIVENESS_THRESHOLD = 0.2
_STONE_WORST_LIVENESS_THRESHOLD = 0.6
_MAX_INITIAL_STONES = 1_000
_MAX_REPLAY_MOVES = 1_000
_MAX_COORDINATE_LENGTH = 16


def _parse_stencil_family(source: str) -> Tuple[Stencil, ...]:
    """Parse the small name:/ASCII-row family format kept beside this module."""
    lines = source.splitlines()
    family: List[Stencil] = []
    names = set()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line:
            continue
        if not line.startswith("name:"):
            raise ValueError(f"expected 'name:' at family line {i}")
        name = line.partition(":")[2].strip()
        if not name or name in names:
            raise ValueError(f"empty or duplicate stencil name at family line {i}")
        rows: List[str] = []
        while i < len(lines) and lines[i].strip():
            row = lines[i].strip()
            if any(char not in "#." for char in row):
                raise ValueError(f"invalid stencil row at family line {i + 1}")
            rows.append(row)
            i += 1
        if not rows or not any("#" in row for row in rows):
            raise ValueError(f"stencil {name!r} has no occupied cells")
        width = max(map(len, rows))
        if any(len(row) != width for row in rows):
            raise ValueError(f"stencil {name!r} has ragged rows")
        cells = tuple(
            (y, x)
            for y, row in enumerate(rows)
            for x, char in enumerate(row)
            if char == "#"
        )
        # The family is intentionally made of connected stencils; this also
        # prevents a single placement from jumping between regions.
        pending = {cells[0]}
        reached = set()
        cell_set = set(cells)
        while pending:
            y, x = pending.pop()
            if (y, x) in reached:
                continue
            reached.add((y, x))
            pending.update(
                (ny, nx)
                for ny, nx in ((y - 1, x), (y + 1, x), (y, x - 1), (y, x + 1))
                if (ny, nx) in cell_set and (ny, nx) not in reached
            )
        if reached != cell_set:
            raise ValueError(f"stencil {name!r} is not 4-connected")
        family.append((name, cells))
        names.add(name)
    if not family:
        raise ValueError("stencil family is empty")
    return tuple(family)


STENCIL_FAMILY: Tuple[Stencil, ...] = _parse_stencil_family(
    Path(__file__).with_name("stencil_family.txt").read_text(encoding="utf-8")
)


def _threshold_from_metadata(metadata: Any) -> float:
    if not isinstance(metadata, dict):
        raise ValueError("stencil_cover capability metadata must be an object")
    value = metadata.get("truncation_threshold")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("truncation_threshold must be a finite number in [0, 1]")
    threshold = float(value)
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
        raise ValueError("truncation_threshold must be a finite number in [0, 1]")
    return threshold


def _board_dimensions(opaque: Dict[str, Any]) -> Tuple[int, int]:
    width = opaque.get("boardXSize", 19)
    height = opaque.get("boardYSize", width)
    for name, value in (("boardXSize", width), ("boardYSize", height)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if not 1 <= value <= _MAX_BOARD_SIZE:
            raise ValueError(f"{name} must be in [1, {_MAX_BOARD_SIZE}]")
    return width, height


def _numeric_array(value: Any, field: str) -> np.ndarray:
    raw = np.asarray(value)
    if raw.dtype.kind not in "iuf":
        raise ValueError(f"{field} must contain only numbers")
    result = raw.astype(np.float64, copy=False).reshape(-1)
    if not np.isfinite(result).all():
        raise ValueError(f"{field} contains a non-finite value")
    return result


def _extract_maps(
    opaque: Dict[str, Any], width: int, height: int,
) -> np.ndarray:
    """Extract KataGo's signed white-ownership map.

    The policy map is intentionally not consulted here: -1 means that a move
    is illegal, not that the intersection is absent from territorial ownership.
    Board occupancy and chain liveness are reconstructed from the request.
    """
    area = width * height
    ownership_value = opaque.get("ownership")
    if ownership_value is None:
        raise ValueError("response has no ownership map")
    ownership = _numeric_array(ownership_value, "ownership")
    if ownership.size != area:
        raise ValueError(f"ownership has {ownership.size} values; expected {area}")
    if ((ownership < -1.0 - _OWNERSHIP_EPSILON)
            | (ownership > 1.0 + _OWNERSHIP_EPSILON)).any():
        raise ValueError("ownership values must lie in [-1, 1]")
    return ownership


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
            column -= 1  # GTP skips the letter I.
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
        if not isinstance(coordinate, str) or not coordinate or len(coordinate) > _MAX_COORDINATE_LENGTH:
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

    # A valid setup position cannot contain a chain without liberties.
    visited: set[int] = set()
    for point, colour in enumerate(board):
        if colour is None or point in visited:
            continue
        chain, liberties = _chain_and_liberties(
            board, point, context.width, context.height,
        )
        visited.update(chain)
        if not liberties:
            raise ValueError("initialStones contains a chain without liberties")

    for colour, point in context.moves[:turn_number]:
        if point is None:  # pass
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
            # This occurs only under rules that permit suicide in a valid
            # KataGo move sequence; the self-captured stones leave the board.
            for captured in own_chain:
                board[captured] = None
    return board


def _dead_stone_owners(
    ownership: Sequence[float],
    board: Sequence[Optional[str]],
    width: int,
    height: int,
) -> Tuple[List[int], List[int]]:
    """Return dead Black stones (White-owned) and dead White stones (Black-owned).

    This mirrors KataGo's chain-level heuristic in
    PlayUtils::computeAnticipatedStatusesWithOwnership:
    White lives if avg > 0.2 and min > -0.6; Black lives if avg < -0.2
    and max < 0.6. A dead chain belongs to the opponent as a whole.
    """
    if len(ownership) != width * height or len(board) != width * height:
        raise ValueError("ownership and board maps must match the board dimensions")

    dead_black: List[int] = []
    dead_white: List[int] = []
    visited: set[int] = set()
    for point, colour in enumerate(board):
        if colour is None or point in visited:
            continue
        chain, _liberties = _chain_and_liberties(board, point, width, height)
        visited.update(chain)
        values = [float(ownership[cell]) for cell in chain]
        average = sum(values) / len(values)
        minimum = min(values)
        maximum = max(values)

        if colour == "W":
            alive = (
                average > _STONE_AVG_LIVENESS_THRESHOLD
                and minimum > -_STONE_WORST_LIVENESS_THRESHOLD
            )
        else:
            alive = (
                average < -_STONE_AVG_LIVENESS_THRESHOLD
                and maximum < _STONE_WORST_LIVENESS_THRESHOLD
            )
        if not alive:
            (dead_black if colour == "B" else dead_white).extend(chain)

    return sorted(dead_black), sorted(dead_white)


def _ownership_regions(
    ownership: Sequence[float],
    board: Sequence[Optional[str]],
    width: int,
    height: int,
    threshold: float,
) -> Tuple[List[int], List[int]]:
    """Build signed territory masks from empty points plus dead opposing stones.

    The truncation threshold applies to empty intersections. Living stones are
    excluded, while each dead chain is included in the opponent's territory
    regardless of the ownership value at an individual stone.
    """
    dead_black, dead_white = _dead_stone_owners(
        ownership, board, width, height,
    )
    positive = set(dead_black)  # positive ownership is White
    negative = set(dead_white)  # negative ownership is Black

    for index, value in enumerate(ownership):
        if board[index] is not None:
            continue  # occupied stones were handled chain-by-chain above
        # At zero, neutral ownership belongs to neither side. For non-zero
        # thresholds the comparison is inclusive, so threshold 1 keeps only
        # completely-owned empty points.
        if threshold == 0.0:
            if value > 0.0:
                positive.add(index)
            elif value < 0.0:
                negative.add(index)
        elif value >= threshold:
            positive.add(index)
        elif value <= -threshold:
            negative.add(index)

    return sorted(positive), sorted(negative)


def _candidate_placements(
    region: Sequence[int], width: int, height: int, family: Sequence[Stencil],
) -> List[Tuple[str, Tuple[int, ...]]]:
    allowed = set(region)
    found: Dict[Tuple[int, ...], str] = {}
    for name, cells in family:
        stencil_height = max(y for y, _ in cells) + 1
        stencil_width = max(x for _, x in cells) + 1
        if stencil_width > width or stencil_height > height:
            continue
        for y0 in range(height - stencil_height + 1):
            for x0 in range(width - stencil_width + 1):
                placement = tuple(sorted(
                    (y0 + dy) * width + x0 + dx for dy, dx in cells
                ))
                if all(point in allowed for point in placement):
                    found.setdefault(placement, name)
    # Single points guarantee that every region has a feasible exact cover,
    # even if the family is later changed to remove its 1x1 stencil.
    for point in region:
        found.setdefault((point,), "1x1")
    return [(name, cells) for cells, name in found.items()]


def _connected_components(
    region: Sequence[int], width: int, height: int,
) -> List[Tuple[int, ...]]:
    """Split a flat row-major mask into 4-connected components.

    Stencil-family members are 4-connected, so no legal placement can span
    two such components. Solving them together only multiplies the exact-cover
    search space; every component is therefore an independent solver input.
    """
    area = width * height
    remaining = {int(point) for point in region}
    if any(point < 0 or point >= area for point in remaining):
        raise ValueError("region contains a point outside the board")
    components: List[Tuple[int, ...]] = []
    while remaining:
        start = min(remaining)
        remaining.remove(start)
        pending = [start]
        component = [start]
        while pending:
            point = pending.pop()
            x = point % width
            neighbours = []
            if x > 0:
                neighbours.append(point - 1)
            if x + 1 < width:
                neighbours.append(point + 1)
            if point >= width:
                neighbours.append(point - width)
            if point + width < area:
                neighbours.append(point + width)
            for neighbour in neighbours:
                if neighbour in remaining:
                    remaining.remove(neighbour)
                    pending.append(neighbour)
                    component.append(neighbour)
        components.append(tuple(sorted(component)))
    return components


# D4 is represented as integer matrices acting on (row, column). The
# transforms are intentionally shared by canonicalisation and recovery.
_D4_MATRICES = (
    (1, 0, 0, 1),    # identity
    (0, 1, -1, 0),   # quarter turn
    (-1, 0, 0, -1),  # half turn
    (0, -1, 1, 0),   # three-quarter turn
    (-1, 0, 0, 1),   # horizontal-axis reflection
    (1, 0, 0, -1),   # vertical-axis reflection
    (0, 1, 1, 0),    # main-diagonal reflection
    (0, -1, -1, 0),  # anti-diagonal reflection
)
_D4_INVERSE = (0, 3, 2, 1, 4, 5, 6, 7)


@dataclass(frozen=True)
class ComponentFrame:
    """Map between a board component and its canonical D4 frame."""

    board_width: int
    transform: int
    offset_y: int
    offset_x: int
    canonical_width: int
    canonical_height: int


def _transform_cell(y: int, x: int, transform: int) -> Tuple[int, int]:
    a, b, c, d = _D4_MATRICES[transform]
    return a * y + b * x, c * y + d * x


def _normalise_shape(
    cells: Sequence[Tuple[int, int]], transform: int = 0,
) -> Tuple[Tuple[int, int], ...]:
    mapped = [_transform_cell(int(y), int(x), transform) for y, x in cells]
    if not mapped:
        raise ValueError("a stencil shape must contain at least one cell")
    min_y = min(y for y, _ in mapped)
    min_x = min(x for _, x in mapped)
    return tuple(sorted((y - min_y, x - min_x) for y, x in mapped))


def _family_dihedral_group(family: Sequence[Stencil]) -> Tuple[int, ...]:
    """Return exactly the D4 maps that preserve the family's shape set."""
    shapes = {_normalise_shape(cells) for _name, cells in family}
    group = tuple(
        transform for transform in range(8)
        if {
            _normalise_shape(cells, transform)
            for _name, cells in family
        } == shapes
    )
    if not group or group[0] != 0:
        raise RuntimeError("the stencil family's symmetry group lost the identity")
    return group


def _component_shape_key(
    component: Sequence[int],
    board_width: int,
    family: Sequence[Stencil] = STENCIL_FAMILY,
) -> Tuple[bytes, ComponentFrame]:
    """Return the least translation/D4 representative allowed by the family.

    The key is bytes([height, width]) followed by an LSB-first packed bitmap.
    The frame records the chosen map from caller coordinates into that
    canonical rectangle, so cached placements can be mapped back exactly.
    Only symmetries preserving the complete stencil shape set are admitted.
    """
    if isinstance(board_width, bool) or not isinstance(board_width, int) or board_width <= 0:
        raise ValueError("board_width must be a positive integer")
    points = tuple(sorted({int(point) for point in component}))
    if not points:
        raise ValueError("cannot canonicalise an empty component")
    if points[0] < 0:
        raise ValueError("component points must be non-negative board indices")

    coordinates = [(point // board_width, point % board_width) for point in points]
    best_key: Optional[bytes] = None
    best_frame: Optional[ComponentFrame] = None

    for transform in _family_dihedral_group(family):
        mapped = [
            _transform_cell(y, x, transform)
            for y, x in coordinates
        ]
        offset_y = min(y for y, _ in mapped)
        offset_x = min(x for _, x in mapped)
        local = [(y - offset_y, x - offset_x) for y, x in mapped]
        canonical_height = max(y for y, _ in local) + 1
        canonical_width = max(x for _, x in local) + 1
        if canonical_height > 255 or canonical_width > 255:
            raise ValueError("canonical component dimensions exceed the key format")

        bits = bytearray((canonical_width * canonical_height + 7) // 8)
        for y, x in local:
            index = y * canonical_width + x
            bits[index >> 3] |= 1 << (index & 7)
        candidate_key = bytes((canonical_height, canonical_width)) + bytes(bits)

        # Iteration is in transform order, so the first transform wins ties.
        if best_key is None or candidate_key < best_key:
            best_key = candidate_key
            best_frame = ComponentFrame(
                board_width=board_width,
                transform=transform,
                offset_y=offset_y,
                offset_x=offset_x,
                canonical_width=canonical_width,
                canonical_height=canonical_height,
            )

    assert best_key is not None and best_frame is not None
    return best_key, best_frame


def _decode_component_shape_key(shape_key: bytes) -> Tuple[int, int, Tuple[int, ...]]:
    if len(shape_key) < 2:
        raise ValueError("component shape key is truncated")
    height, width = shape_key[0], shape_key[1]
    area = height * width
    expected_bytes = (area + 7) // 8
    if not 1 <= width <= _MAX_BOARD_SIZE or not 1 <= height <= _MAX_BOARD_SIZE:
        raise ValueError("component shape dimensions are invalid")
    if len(shape_key) != 2 + expected_bytes:
        raise ValueError("component shape key has an invalid bitset length")
    packed = shape_key[2:]
    cells = tuple(
        point for point in range(area)
        if packed[point >> 3] & (1 << (point & 7))
    )
    return width, height, cells


def _encode_cached_placements(
    placements: Sequence[Tuple[str, Sequence[int]]],
    family: Sequence[Stencil],
) -> bytes:
    """Compact binary encoding: count, then (family-index, size, cells...)."""
    if len(placements) > 0xFFFF:
        raise ValueError("too many placements for compact cache encoding")
    name_to_index = {name: index for index, (name, _) in enumerate(family)}
    encoded = bytearray(len(placements).to_bytes(2, "little"))
    for name, points in placements:
        if len(points) > 0xFF:
            raise ValueError("stencil placement is too large for compact cache encoding")
        # 255 is reserved for the guaranteed singleton fallback when a
        # future family omits its explicit 1x1 stencil.
        stencil_index = name_to_index.get(name, 255 if name == "1x1" else -1)
        if not 0 <= stencil_index <= 255:
            raise ValueError(f"unknown stencil name in solver result: {name!r}")
        encoded.extend((stencil_index, len(points)))
        for point in points:
            point = int(point)
            if not 0 <= point <= 0xFFFF:
                raise ValueError("component point index exceeds compact cache encoding")
            encoded.extend(point.to_bytes(2, "little"))
    return bytes(encoded)


def _decode_cached_placements(
    encoded: bytes,
    family: Sequence[Stencil],
    frame: ComponentFrame,
) -> List[Dict[str, Any]]:
    """Recover a canonical solution into board coordinates and stencil names.

    A D4 map can rotate a rectangle into a differently named family member
    (for example, canonical 1x2 becomes caller-facing 2x1). Both points and
    stencil geometry are therefore transformed back together.
    """
    if len(encoded) < 2:
        raise ValueError("cached stencil solution is truncated")
    count = int.from_bytes(encoded[:2], "little")
    offset = 2
    inverse = _D4_INVERSE[frame.transform]
    names_by_shape: Dict[Tuple[Tuple[int, int], ...], str] = {}
    for name, cells in family:
        names_by_shape.setdefault(_normalise_shape(cells), name)

    placements: List[Dict[str, Any]] = []
    for _ in range(count):
        if offset + 2 > len(encoded):
            raise ValueError("cached stencil placement header is truncated")
        stencil_index, point_count = encoded[offset], encoded[offset + 1]
        offset += 2
        if offset + point_count * 2 > len(encoded):
            raise ValueError("cached stencil placement points are truncated")

        if stencil_index == 255:
            stencil_name = "1x1"
        elif stencil_index < len(family):
            canonical_stencil = family[stencil_index][1]
            recovered_shape = _normalise_shape(canonical_stencil, inverse)
            stencil_name = names_by_shape.get(recovered_shape)
            if stencil_name is None:
                raise RuntimeError(
                    "the active family is not closed under the component's recovery transform"
                )
        else:
            raise ValueError("cached stencil index is outside the active family")

        points: List[List[int]] = []
        for _ in range(point_count):
            local = int.from_bytes(encoded[offset:offset + 2], "little")
            offset += 2
            if local >= frame.canonical_width * frame.canonical_height:
                raise ValueError("cached stencil point is outside the canonical component frame")
            canonical_y, canonical_x = divmod(local, frame.canonical_width)
            transformed_y = canonical_y + frame.offset_y
            transformed_x = canonical_x + frame.offset_x
            original_y, original_x = _transform_cell(
                transformed_y, transformed_x, inverse,
            )
            if original_y < 0 or not 0 <= original_x < frame.board_width:
                raise ValueError("recovered stencil point is outside the caller's board frame")
            points.append([original_x, original_y])

        points.sort(key=lambda point: (point[1], point[0]))
        placements.append({"stencil": stencil_name, "points": points})

    if offset != len(encoded):
        raise ValueError("cached stencil solution has trailing bytes")
    return placements


def _verify_component_placements(
    component: Sequence[int],
    board_width: int,
    placements: Sequence[Dict[str, Any]],
    family: Sequence[Stencil],
) -> None:
    """Reject corrupt or incorrectly recovered covers before they are served."""
    allowed = set(int(point) for point in component)
    names = {name: _normalise_shape(cells) for name, cells in family}
    covered: set[int] = set()
    for placement in placements:
        name = placement.get("stencil")
        points = placement.get("points")
        if (name not in names and name != "1x1") or not isinstance(points, list) or not points:
            raise RuntimeError("cached stencil placement has an unknown shape or no points")
        coordinates: List[Tuple[int, int]] = []
        flattened: List[int] = []
        for point in points:
            if (
                not isinstance(point, list) or len(point) != 2
                or any(isinstance(value, bool) or not isinstance(value, int) for value in point)
            ):
                raise RuntimeError("cached stencil placement contains a malformed coordinate")
            x, y = point
            if x < 0 or not 0 <= x < board_width or y < 0:
                raise RuntimeError("cached stencil placement is outside the board")
            coordinates.append((y, x))
            flattened.append(y * board_width + x)
        expected_shape = names.get(name)
        if expected_shape is None and name == "1x1" and len(coordinates) == 1:
            # Explicitly preserve the solver's guaranteed singleton fallback
            # for a family that omits a named singleton stencil.
            expected_shape = ((0, 0),)
        if expected_shape is None or _normalise_shape(coordinates) != expected_shape:
            raise RuntimeError("cached stencil name does not match its recovered geometry")
        for point in flattened:
            if point not in allowed or point in covered:
                raise RuntimeError("cached stencil placements do not form a disjoint component cover")
            covered.add(point)
    if covered != allowed:
        raise RuntimeError("cached stencil placements leave component cells uncovered")


def _solve_component_uncached(
    shape_key: bytes,
    time_limit_ms: int,
    family: Tuple[Stencil, ...],
) -> Tuple[bool, int, bytes]:
    """Solve one canonical component and cache only its compact placement list.

    Status codes: 0 = proven optimal, 1 = feasible incumbent, 2 = singleton
    fallback. The time limit and family are cache-key dimensions, so changing
    either cannot accidentally reuse a result produced under another contract.
    """
    if isinstance(time_limit_ms, bool) or time_limit_ms <= 0:
        raise ValueError("solver time limit must be a positive number of milliseconds")
    width, height, region = _decode_component_shape_key(shape_key)
    candidates = _candidate_placements(region, width, height, family)
    point_to_columns: Dict[int, List[int]] = {point: [] for point in region}
    for column, (_, points) in enumerate(candidates):
        for point in points:
            point_to_columns[point].append(column)
    if any(not columns for columns in point_to_columns.values()):
        raise RuntimeError("stencil family failed to provide a feasible singleton cover")

    highs = highspy.Highs()
    highs.setOptionValue("log_to_console", False)
    highs.setOptionValue("time_limit", time_limit_ms / 1000.0)
    variable_count = len(candidates)
    highs.addVars(
        variable_count,
        np.zeros(variable_count, dtype=np.float64),
        np.ones(variable_count, dtype=np.float64),
    )
    indices = np.arange(variable_count, dtype=np.int32)
    highs.changeColsCost(
        variable_count, indices, np.ones(variable_count, dtype=np.float64)
    )
    integrality = np.full(
        variable_count, int(highspy.HighsVarType.kInteger), dtype=np.uint8
    )
    highs.changeColsIntegrality(variable_count, indices, integrality)

    for point in region:
        columns = np.asarray(point_to_columns[point], dtype=np.int32)
        highs.addRow(
            1.0, 1.0, int(len(columns)), columns,
            np.ones(len(columns), dtype=np.float64),
        )
    highs.run()

    optimal = highs.getModelStatus() == highspy.HighsModelStatus.kOptimal
    solution = highs.getSolution()
    values = getattr(solution, "col_value", None)
    chosen = (
        [i for i, value in enumerate(values) if float(value) > 0.5]
        if values is not None and len(values) == variable_count
        else []
    )

    def is_exact_cover(selection: Sequence[int]) -> bool:
        counts = {point: 0 for point in region}
        for column in selection:
            for point in candidates[column][1]:
                counts[point] += 1
        return all(count == 1 for count in counts.values())

    fallback = not chosen or not is_exact_cover(chosen)
    if fallback:
        singleton_columns = {
            points[0]: index
            for index, (_, points) in enumerate(candidates)
            if len(points) == 1
        }
        chosen = [singleton_columns[point] for point in region]
        optimal = False

    selected = [candidates[column] for column in chosen]
    encoded = _encode_cached_placements(selected, family)
    return bool(optimal), 2 if fallback else (0 if optimal else 1), encoded


# The request path uses a large local LRU and a Redis-backed miss handler.
# The bulk worker calls _solve_component_uncached directly with its larger
# budget. Pub/Sub notifications invalidate this local LRU after a worker
# upgrades a Redis row; generation checking closes the race where an
# in-flight request might otherwise reinsert a stale incumbent after clear().
_SOLVER_CACHE_NAMESPACE = "solver-component-d4-v3"
_solver_component_persistent = redis_persistent_cache(
    namespace=_SOLVER_CACHE_NAMESPACE,
)(_solve_component_uncached)
_solver_component_lru = lru_cache(maxsize=_SOLVER_CACHE_MAX_ENTRIES)(
    _solver_component_persistent
)
_solver_cache_lock = threading.RLock()
_solver_cache_generation = 0


def _invalidate_component_cache() -> None:
    global _solver_cache_generation
    with _solver_cache_lock:
        _solver_cache_generation += 1
        _solver_component_lru.cache_clear()


def _solve_component_cached(
    shape_key: bytes,
    time_limit_ms: int,
    family: Tuple[Stencil, ...],
) -> Tuple[bool, int, bytes]:
    while True:
        with _solver_cache_lock:
            generation = _solver_cache_generation
        result = _solver_component_lru(shape_key, time_limit_ms, family)
        with _solver_cache_lock:
            if generation == _solver_cache_generation:
                return result
            # An invalidation raced with computation and may have happened
            # before functools.lru_cache inserted that computation's value.
            _solver_component_lru.cache_clear()


_solve_component_cached.cache_info = _solver_component_lru.cache_info  # type: ignore[attr-defined]
_solve_component_cached.cache_clear = _invalidate_component_cache  # type: ignore[attr-defined]
register_cache_invalidator(
    _SOLVER_CACHE_NAMESPACE, _invalidate_component_cache,
)


def _solve_region(
    region: Sequence[int], width: int, height: int,
    family: Sequence[Stencil],
    time_limit_ms: int = _SOLVER_TIME_LIMIT_MS,
) -> Dict[str, Any]:
    """Decompose, expose, and solve exactly the connected components handed to HiGHS."""
    if not region:
        return {
            "components": [],
            "placements": [],
            "tileCount": 0,
            "optimal": True,
            "status": "empty",
        }
    family_key = tuple(family)
    components: List[Dict[str, Any]] = []
    placements: List[Dict[str, Any]] = []
    statuses: List[int] = []
    for component in _connected_components(region, width, height):
        shape_key, frame = _component_shape_key(component, width, family)
        optimal, status, encoded = _solve_component_cached(
            shape_key, time_limit_ms, family_key,
        )
        component_placements = _decode_cached_placements(
            encoded, family_key, frame,
        )
        _verify_component_placements(
            component, width, component_placements, family_key,
        )
        placements.extend(component_placements)
        statuses.append(status)
        if status == 0:
            component_status = "optimal"
        elif status == 1:
            component_status = "feasible"
        elif status == 2:
            component_status = "fallback"
        else:
            raise RuntimeError(f"unknown cached stencil solver status: {status!r}")
        components.append({
            # Wire cells are [column, row_from_top], directly derived from
            # KataGo's row-major flat index. The SPA owns the one conversion
            # to its internal bottom-origin Point.
            # Keep these exact solver-input cells even for fallback results.
            "cells": [
                [point % width, point // width]
                for point in component
            ],
            "tileCount": len(component_placements),
            "optimal": bool(optimal and status == 0),
            "status": component_status,
        })
    fallback = any(status == 2 for status in statuses)
    feasible = any(status == 1 for status in statuses)
    optimal = all(status == 0 for status in statuses)
    return {
        "components": components,
        "placements": placements,
        "tileCount": len(placements),
        "optimal": optimal,
        "status": "fallback" if fallback else ("feasible" if feasible else "optimal"),
    }

def _build_stencil_cover(
    opaque: Dict[str, Any],
    board: Sequence[Optional[str]],
    width: int,
    height: int,
    threshold: float,
) -> Dict[str, Any]:
    ownership = _extract_maps(opaque, width, height)
    positive_cells, negative_cells = _ownership_regions(
        ownership, board, width, height, threshold,
    )
    positive = _solve_region(positive_cells, width, height, STENCIL_FAMILY)
    negative = _solve_region(negative_cells, width, height, STENCIL_FAMILY)
    return {
        "version": 2,
        "threshold": threshold,
        "width": width,
        "height": height,
        "positive": positive,
        "negative": negative,
        "optimal": positive["optimal"] and negative["optimal"],
    }


def _request_context(query: KataGoQuery) -> Optional[RequestContext]:
    if query.action != KataGoAction.ANALYZE:
        return None

    capabilities = query.opaque.get("capabilities")
    if not isinstance(capabilities, dict) or "stencil_cover" not in capabilities:
        return None

    width, height = _board_dimensions(query.opaque)
    threshold = _threshold_from_metadata(capabilities["stencil_cover"])
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
        threshold=threshold,
        initial_stones=initial_stones,
        moves=moves,
    )


def stencil_cover_enricher(
    link: ProxyLink[ClientId, InternalId],
) -> Transformer[KataGoQuery, KataGoResponse]:
    """Enrich explicitly opted-in final analysis packets with signed covers."""
    request_cache: Dict[ClientId, RequestContext] = {}

    def on_query(eid: ClientId, query: KataGoQuery) -> Optional[KataGoQuery]:
        context = _request_context(query)
        if context is not None:
            request_cache[eid] = context
        return query

    def on_response(eid: ClientId, response: KataGoResponse) -> Optional[KataGoResponse]:
        try:
            context = request_cache.get(eid)
            if (
                context is not None
                and isinstance(response, AnalyzeResponse)
                and not response.is_during_search
            ):
                board = _board_at_turn(context, response.turn_number)
                response.opaque["stencilCover"] = _build_stencil_cover(
                    response.opaque,
                    board,
                    context.width,
                    context.height,
                    context.threshold,
                )
            return response
        finally:
            if link.mapping.forward(eid) is None:
                request_cache.pop(eid, None)

    return Transformer(
        name="stencil_cover",
        on_query=on_query,
        on_response=on_response,
    )
