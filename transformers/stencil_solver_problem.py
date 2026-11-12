"""Canonical solver-problem construction; independent of cache and transport."""
from __future__ import annotations

from typing import Sequence

from transformers.stencil_solver_api import (
    CanonicalProblem,
    STENCIL_CANONICALIZATION_VERSION,
)
from transformers.stencil_solver_geometry import Stencil, decode_component_shape_key
from transformers.stencil_solver_identity import stable_digest


def canonical_problem_from_key(
    shape_key: bytes,
    family: Sequence[Stencil],
) -> CanonicalProblem:
    """Translate canonical key bytes and the active family into solver inputs."""
    width, height, flat_cells = decode_component_shape_key(shape_key)
    occupied_cells = tuple((point // width, point % width) for point in flat_cells)
    family_value: tuple[Stencil, ...] = tuple(
        (name, tuple((int(y), int(x)) for y, x in cells))
        for name, cells in family
    )
    return CanonicalProblem(
        width=width,
        height=height,
        occupied_cells=occupied_cells,
        stencil_family=family_value,
        family_fingerprint=stable_digest(family_value),
        canonicalization_version=STENCIL_CANONICALIZATION_VERSION,
    )
