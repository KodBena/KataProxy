# SPDX-License-Identifier: MIT
"""Typed ownership evidence and the shared chain-liveness interpretation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Tuple

import numpy as np

from .stencil_board import _chain_and_liberties


_OWNERSHIP_EPSILON = 1e-5
_STONE_AVG_LIVENESS_THRESHOLD = 0.2
_STONE_WORST_LIVENESS_THRESHOLD = 0.6


@dataclass(frozen=True)
class OwnershipEvidence:
    ownership: Tuple[float, ...]
    ownership_stdev: Optional[Tuple[float, ...]]
    board: Tuple[Optional[str], ...]
    width: int
    height: int
    empty_points: Tuple[int, ...]
    dead_black: Tuple[int, ...]
    dead_white: Tuple[int, ...]
    policy_metadata: Mapping[str, Any]


@dataclass(frozen=True)
class OwnershipRegions:
    positive: Tuple[int, ...]
    negative: Tuple[int, ...]
    policy_id: str
    threshold: Optional[float] = None
    diagnostics: Dict[str, int | float | str | bool] = field(default_factory=dict)


class OwnershipRegionPolicy(Protocol):
    """One selectable rule for mapping ownership evidence to signed board regions."""

    policy_id: str
    requires_ownership_stdev: bool

    def validate_request_metadata(self, metadata: Mapping[str, Any]) -> None: ...

    def classify(self, evidence: OwnershipEvidence) -> OwnershipRegions: ...


def _numeric_array(value: Any, field_name: str) -> np.ndarray:
    raw = np.asarray(value)
    if raw.dtype.kind not in "iuf":
        raise ValueError(f"{field_name} must contain only numbers")
    result = raw.astype(np.float64, copy=False).reshape(-1)
    if not np.isfinite(result).all():
        raise ValueError(f"{field_name} contains a non-finite value")
    return result


def _dead_stone_owners(
    ownership: Sequence[float],
    board: Sequence[Optional[str]],
    width: int,
    height: int,
) -> Tuple[Tuple[int, ...], Tuple[int, ...]]:
    """Return dead Black stones (White-owned) and dead White stones (Black-owned).

    This mirrors KataGo's chain-level heuristic in
    PlayUtils::computeAnticipatedStatusesWithOwnership.
    """
    if len(ownership) != width * height or len(board) != width * height:
        raise ValueError("ownership and board maps must match the board dimensions")

    dead_black: list[int] = []
    dead_white: list[int] = []
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

    return tuple(sorted(dead_black)), tuple(sorted(dead_white))


def make_ownership_evidence(
    opaque: Mapping[str, Any],
    board: Sequence[Optional[str]],
    width: int,
    height: int,
    policy_metadata: Mapping[str, Any],
    *,
    requires_ownership_stdev: bool,
) -> OwnershipEvidence:
    area = width * height
    ownership_value = opaque.get("ownership")
    if ownership_value is None:
        raise ValueError("response has no ownership map")
    ownership = _numeric_array(ownership_value, "ownership")
    if ownership.size != area:
        raise ValueError(f"ownership has {ownership.size} values; expected {area}")
    if (
        (ownership < -1.0 - _OWNERSHIP_EPSILON)
        | (ownership > 1.0 + _OWNERSHIP_EPSILON)
    ).any():
        raise ValueError("ownership values must lie in [-1, 1]")

    stdev_values: Optional[Tuple[float, ...]] = None
    if requires_ownership_stdev:
        raw_stdev = opaque.get("ownershipStdev")
        if raw_stdev is None:
            raise ValueError("response has no ownershipStdev map required by the selected policy")
        stdev = _numeric_array(raw_stdev, "ownershipStdev")
        if stdev.size != area:
            raise ValueError(
                f"ownershipStdev has {stdev.size} values; expected {area}"
            )
        if ((stdev < -_OWNERSHIP_EPSILON) | (stdev > 1.0 + _OWNERSHIP_EPSILON)).any():
            raise ValueError("ownershipStdev values must lie in [0, 1]")
        stdev_values = tuple(float(value) for value in stdev)

    board_values = tuple(board)
    if len(board_values) != area:
        raise ValueError("board map must match the board dimensions")
    dead_black, dead_white = _dead_stone_owners(
        ownership, board_values, width, height,
    )
    empty_points = tuple(
        point for point, colour in enumerate(board_values) if colour is None
    )
    return OwnershipEvidence(
        ownership=tuple(float(value) for value in ownership),
        ownership_stdev=stdev_values,
        board=board_values,
        width=width,
        height=height,
        empty_points=empty_points,
        dead_black=dead_black,
        dead_white=dead_white,
        policy_metadata=dict(policy_metadata),
    )
