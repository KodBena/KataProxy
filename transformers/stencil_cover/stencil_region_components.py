# SPDX-License-Identifier: MIT
"""Board-grid component geometry for stencil-cover exact tiling."""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

from .stencil_family import STENCIL_FAMILY
from .stencil_solver_geometry import (
    ComponentFrame,
    Stencil,
    component_shape_key as _component_shape_key_impl,
)


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



def _component_shape_key(
    component: Sequence[int],
    board_width: int,
    family: Sequence[Stencil] = STENCIL_FAMILY,
) -> tuple[bytes, ComponentFrame]:
    """Compatibility entry point; canonical geometry is owned by solver_geometry."""
    return _component_shape_key_impl(component, board_width, family)


_PreparedComponent = Tuple[Tuple[int, ...], bytes, ComponentFrame]


def _prepare_region_components(
    region: Sequence[int],
    width: int,
    height: int,
    family: Sequence[Stencil],
) -> List[_PreparedComponent]:
    """Canonicalize a region's components before any solver-cache lookups."""
    return [
        (tuple(component), *_component_shape_key(component, width, family))
        for component in _connected_components(region, width, height)
    ]


