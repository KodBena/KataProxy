"""Solver-independent geometry and compact legacy-result codecs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence, TypeAlias

Cell: TypeAlias = tuple[int, int]
Stencil: TypeAlias = tuple[str, tuple[Cell, ...]]
Placement: TypeAlias = tuple[str, tuple[Cell, ...]]

MAX_COMPONENT_DIMENSION = 25

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
    shape_key: bytes, *, max_dimension: int = MAX_COMPONENT_DIMENSION,
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
    placements: Sequence[Mapping[str, object]],
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


@dataclass(frozen=True)
class ComponentFrame:
    """Reversible mapping from caller board coordinates into canonical D4 frame."""

    board_width: int
    transform: int
    offset_y: int
    offset_x: int
    canonical_width: int
    canonical_height: int


def family_dihedral_group(family: Sequence[Stencil]) -> tuple[int, ...]:
    """Return exactly the D4 maps preserving the active family's shape set."""
    shapes = {normalise_shape(cells) for _name, cells in family}
    group = tuple(
        transform for transform in range(len(D4_MATRICES))
        if {
            normalise_shape(cells, transform)
            for _name, cells in family
        } == shapes
    )
    if not group or group[0] != 0:
        raise RuntimeError("the stencil family's symmetry group lost the identity")
    return group


def component_shape_key(
    component: Sequence[int],
    board_width: int,
    family: Sequence[Stencil],
) -> tuple[bytes, ComponentFrame]:
    """Canonicalize translations and only the symmetries preserving the family.

    Encoding v1 is bytes([height, width]) plus a row-major bitmap packed
    least-significant-bit first. The returned frame reverses the chosen D4 map.
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

    for transform in family_dihedral_group(family):
        mapped = [transform_cell(y, x, transform) for y, x in coordinates]
        offset_y = min(y for y, _ in mapped)
        offset_x = min(x for _, x in mapped)
        local = [(y - offset_y, x - offset_x) for y, x in mapped]
        canonical_height = max(y for y, _ in local) + 1
        canonical_width = max(x for _, x in local) + 1
        if canonical_height > 255 or canonical_width > 255:
            raise ValueError("canonical component dimensions exceed the key format")

        bits = bytearray((canonical_width * canonical_height + 7) // 8)
        for y, x in local:
            bit_index = y * canonical_width + x
            bits[bit_index >> 3] |= 1 << (bit_index & 7)
        candidate_key = bytes((canonical_height, canonical_width)) + bytes(bits)
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


def decode_cached_placements(
    encoded: bytes,
    family: Sequence[Stencil],
    frame: ComponentFrame,
) -> list[dict[str, Any]]:
    """Recover a cached canonical cover into the caller's board frame."""
    if len(encoded) < 2:
        raise ValueError("cached stencil solution is truncated")
    count = int.from_bytes(encoded[:2], "little")
    offset = 2
    inverse = D4_INVERSE[frame.transform]
    names_by_shape: dict[tuple[Cell, ...], str] = {}
    for name, cells in family:
        names_by_shape.setdefault(normalise_shape(cells), name)

    placements: list[dict[str, Any]] = []
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
            recovered_shape = normalise_shape(canonical_stencil, inverse)
            stencil_name = names_by_shape.get(recovered_shape)
            if stencil_name is None:
                raise RuntimeError(
                    "the active family is not closed under the component's recovery transform"
                )
        else:
            raise ValueError("cached stencil index is outside the active family")

        points: list[list[int]] = []
        for _ in range(point_count):
            local = int.from_bytes(encoded[offset:offset + 2], "little")
            offset += 2
            if local >= frame.canonical_width * frame.canonical_height:
                raise ValueError("cached stencil point is outside the canonical component frame")
            canonical_y, canonical_x = divmod(local, frame.canonical_width)
            transformed_y = canonical_y + frame.offset_y
            transformed_x = canonical_x + frame.offset_x
            original_y, original_x = transform_cell(
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
