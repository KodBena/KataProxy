"""Solver-independent geometry and compact legacy-result codecs."""
from __future__ import annotations

from typing import Sequence, TypeAlias

Cell: TypeAlias = tuple[int, int]
Stencil: TypeAlias = tuple[str, tuple[Cell, ...]]
Placement: TypeAlias = tuple[str, tuple[Cell, ...]]

D4_MATRICES: tuple[tuple[int, int, int, int], ...] = (
    (1, 0, 0, 1), (0, 1, -1, 0), (-1, 0, 0, -1), (0, -1, 1, 0),
    (-1, 0, 0, 1), (1, 0, 0, -1), (0, 1, 1, 0), (0, -1, -1, 0),
)
D4_INVERSE: tuple[int, ...] = (0, 3, 2, 1, 4, 5, 6, 7)


def transform_cell(y: int, x: int, transform: int) -> Cell:
    if isinstance(transform, bool) or not 0 <= transform < len(D4_MATRICES):
        raise ValueError("D4 transform id must be in [0, 7]")
    a, b, c, d = D4_MATRICES[transform]
    return a * y + b * x, c * y + d * x


def normalise_shape(cells: Sequence[Cell], transform: int = 0) -> tuple[Cell, ...]:
    mapped = [transform_cell(int(y), int(x), transform) for y, x in cells]
    if not mapped:
        raise ValueError("a stencil shape must contain at least one cell")
    min_y = min(y for y, _ in mapped)
    min_x = min(x for _, x in mapped)
    return tuple(sorted((y - min_y, x - min_x) for y, x in mapped))


def decode_component_shape_key(
    shape_key: bytes, *, max_dimension: int = 25,
) -> tuple[int, int, tuple[int, ...]]:
    """Decode the canonical height/width + row-major LSB-first bitmap."""
    if not isinstance(shape_key, bytes) or len(shape_key) < 2:
        raise ValueError("component shape key is truncated")
    height, width = shape_key[0], shape_key[1]
    area = height * width
    expected_bytes = (area + 7) // 8
    if (
        not 1 <= width <= max_dimension or not 1 <= height <= max_dimension
        or len(shape_key) != 2 + expected_bytes
    ):
        raise ValueError("component shape key has invalid dimensions or bitset length")
    packed = shape_key[2:]
    cells = tuple(
        point for point in range(area)
        if packed[point >> 3] & (1 << (point & 7))
    )
    if not cells:
        raise ValueError("canonical component key cannot encode an empty component")
    return width, height, cells


def encode_cached_placements(
    placements: Sequence[tuple[str, Sequence[int]]],
    family: Sequence[Stencil],
) -> bytes:
    """Preserve the existing Redis placement payload (separate from shape keys)."""
    if len(placements) > 0xFFFF:
        raise ValueError("too many placements for compact cache encoding")
    name_to_index = {name: index for index, (name, _) in enumerate(family)}
    encoded = bytearray(len(placements).to_bytes(2, "little"))
    for name, points in placements:
        if len(points) > 0xFF:
            raise ValueError("stencil placement is too large for compact cache encoding")
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


def verify_canonical_placements(
    component: Sequence[Cell],
    placements: Sequence[Placement],
    family: Sequence[Stencil],
) -> None:
    """Validate family membership, pairwise disjointness, and exact coverage."""
    allowed = set(component)
    family_shapes = {name: normalise_shape(cells) for name, cells in family}
    covered: set[Cell] = set()
    for name, cells in placements:
        if not cells:
            raise ValueError("solver placement must not be empty")
        expected = family_shapes.get(name)
        if expected is None and name == "1x1" and len(cells) == 1:
            expected = ((0, 0),)
        if expected is None:
            raise ValueError(f"solver returned unknown stencil member {name!r}")
        if normalise_shape(cells) != expected:
            raise ValueError(f"solver placement {name!r} does not match family geometry")
        for cell in cells:
            if cell not in allowed or cell in covered:
                raise ValueError("solver placements are not a disjoint subset of the component")
            covered.add(cell)
    if covered != allowed:
        raise ValueError("solver placements do not cover the component exactly")


def verify_component_placements(
    component: Sequence[int],
    board_width: int,
    placements: Sequence[dict[str, object]],
    family: Sequence[Stencil],
) -> None:
    """Reject corrupt or incorrectly recovered covers before they are served."""
    allowed = set(int(point) for point in component)
    names = {name: normalise_shape(cells) for name, cells in family}
    covered: set[int] = set()
    for placement in placements:
        name = placement.get("stencil")
        points = placement.get("points")
        if (name not in names and name != "1x1") or not isinstance(points, list) or not points:
            raise RuntimeError("cached stencil placement has an unknown shape or no points")
        coordinates: list[Cell] = []
        flattened: list[int] = []
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
        expected_shape = names.get(str(name))
        if expected_shape is None and name == "1x1" and len(coordinates) == 1:
            expected_shape = ((0, 0),)
        if expected_shape is None or normalise_shape(coordinates) != expected_shape:
            raise RuntimeError("cached stencil name does not match its recovered geometry")
        for point in flattened:
            if point not in allowed or point in covered:
                raise RuntimeError("cached stencil placements do not form a disjoint component cover")
            covered.add(point)
    if covered != allowed:
        raise RuntimeError("cached stencil placements leave component cells uncovered")
