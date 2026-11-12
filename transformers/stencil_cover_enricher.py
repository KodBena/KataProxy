# SPDX-License-Identifier: MIT
"""
KataGo response enricher for signed ownership stencil covers.

A query opts in with:
    capabilities: {"stencil_cover": {"truncation_threshold": 0.6}}

The transformer automatically adds KataGo's includeOwnership flag for opted-in
queries. Only final analysis responses are processed. The response annotation is
written to opaque.stencilCover and contains independently solved positive-
and negative-ownership exact covers. Empty points use the configured ownership
cutoff; living chains are omitted and dead chains are assigned to their
opponent's territory.

The default composition selects HiGHS eagerly and requires a valid
proxy-owned stencil family. Optional solver packages are imported only when
their adapters are explicitly selected at the composition boundary.

See NOTICE for the file-specific MIT exception and KataGo attribution.
"""
from __future__ import annotations

import logging
import math
from collections import OrderedDict
from dataclasses import dataclass
import re
import threading
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple, cast

import numpy as np

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink
from katago import AnalyzeResponse, KataGoAction, KataGoQuery, KataGoResponse
from transformers.stencil_cache_storage import (
    SolverCacheCandidate,
    redis_persistent_cache,
    register_cache_invalidator,
)
from transformers.stencil_solver_api import (
    NoNativeEffortLimit, OutcomeStatus, SolverBackend, SolverBudget, SolverFailure,
)
from transformers.stencil_cache_storage import (
    SolverWorkProfile, make_solver_work_profile,
)
from transformers.stencil_solver_factory import create_solver_backend
from transformers.stencil_family import (
    STENCIL_FAMILY,
    parse_stencil_family as _parse_stencil_family,
)
from transformers.stencil_solver_geometry import (
    MAX_COMPONENT_DIMENSION as _MAX_BOARD_SIZE,
    D4_INVERSE as _D4_INVERSE,
    D4_MATRICES as _D4_MATRICES,
    ComponentFrame,
    Stencil,
    component_shape_key as _component_shape_key_impl,
    decode_cached_placements as _decode_cached_placements,
    decode_component_shape_key as _decode_component_shape_key,
    encode_cached_placements as _encode_cached_placements,
    family_dihedral_group as _family_dihedral_group,
    normalise_shape as _normalise_shape,
    transform_cell as _transform_cell,
    verify_component_placements as _verify_component_placements,
)
from transformers.stencil_solver_runtime import solve_component as _solve_with_backend
from transformers.stencil_corpus_publisher import publish_instance_seen

logger = logging.getLogger("kataproxy.stencil_cover_enricher")


@dataclass(frozen=True)
class RequestContext:
    width: int
    height: int
    threshold: float
    initial_stones: Tuple[Tuple[str, Optional[int]], ...]
    moves: Tuple[Tuple[str, Optional[int]], ...]


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
    ownership: Any,
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



def _component_shape_key(
    component: Sequence[int],
    board_width: int,
    family: Sequence[Stencil] = STENCIL_FAMILY,
) -> tuple[bytes, ComponentFrame]:
    """Compatibility entry point; canonical geometry is owned by solver_geometry."""
    return _component_shape_key_impl(component, board_width, family)


def _solve_component_uncached(
    shape_key: bytes,
    time_limit_ms: int,
    family: Tuple[Stencil, ...],
    backend: SolverBackend,
    work_profile: SolverWorkProfile | None = None,
) -> SolverCacheCandidate:
    """Invoke the selected backend and retain its provenance for Redis."""
    if isinstance(time_limit_ms, bool) or time_limit_ms <= 0:
        raise ValueError("solver time limit must be a positive number of milliseconds")
    budget = SolverBudget(
        wall_time_limit_ms=int(time_limit_ms),
        native_effort_limit=NoNativeEffortLimit(),
        policy_id=f"foreground-{int(time_limit_ms)}ms",
    )
    expected_profile = make_solver_work_profile(backend.identity, budget)
    if work_profile is not None and work_profile != expected_profile:
        raise ValueError("foreground work profile disagrees with backend or solver budget")
    execution = _solve_with_backend(
        backend,
        shape_key,
        family,
        budget,
    )
    # The shared runtime converts adapter exceptions to a typed error outcome.
    if execution.outcome.status is OutcomeStatus.ERROR:
        error = execution.outcome.error
        message = error.message if isinstance(error, SolverFailure) else "backend invocation failed"
        logger.error(
            "Stencil solver backend %s failed: %s",
            execution.identity.fingerprint,
            message,
        )
    return SolverCacheCandidate(execution.cache_result, execution.result_provenance)


class _SolverCacheInfo(NamedTuple):
    hits: int
    misses: int
    maxsize: int
    currsize: int


class _InvalidatableSolverCache:
    """Entry-bounded LRU with efficient per-component invalidation.

    The generation counters ensure that a solver call already in flight when
    an update notification arrives cannot put its stale result back into the
    cache after the invalidation.
    """

    def __init__(
        self,
        function: Any,
        maxsize: int,
    ) -> None:
        self._function = function
        self._maxsize = maxsize
        self._cache: OrderedDict[
            Tuple[bytes, int, Tuple[Stencil, ...], str],
            Tuple[bool, int, bytes],
        ] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._global_generation = 0
        self._generations: Dict[bytes, int] = {}
        self._inflight: Dict[bytes, int] = {}

    def __call__(
        self,
        shape_key: bytes,
        time_limit_ms: int,
        family: Tuple[Stencil, ...],
        backend: SolverBackend | None = None,
    ) -> Tuple[bool, int, bytes]:
        backend = DEFAULT_SOLVER_BACKEND if backend is None else backend
        budget = SolverBudget(
            wall_time_limit_ms=int(time_limit_ms),
            native_effort_limit=NoNativeEffortLimit(),
            policy_id=f"foreground-{int(time_limit_ms)}ms",
        )
        work_profile = make_solver_work_profile(backend.identity, budget)
        cache_key = (shape_key, time_limit_ms, family, work_profile.fingerprint)
        while True:
            with self._lock:
                cached = self._cache.get(cache_key)
                if cached is not None:
                    self._hits += 1
                    self._cache.move_to_end(cache_key)
                    return cached
                self._misses += 1
                generation = (
                    self._global_generation,
                    self._generations.get(shape_key, 0),
                )
                self._inflight[shape_key] = self._inflight.get(shape_key, 0) + 1

            try:
                result = cast(
                    Tuple[bool, int, bytes],
                    self._function(
                        shape_key, time_limit_ms, family, backend, work_profile,
                    ),
                )
            except Exception:
                with self._lock:
                    self._leave_inflight(shape_key)
                raise

            with self._lock:
                current_generation = (
                    self._global_generation,
                    self._generations.get(shape_key, 0),
                )
                changed_during_solve = generation != current_generation
                self._leave_inflight(shape_key)
                if changed_during_solve:
                    # The cache miss may have computed against an older Redis
                    # row. Retry through the persistent layer before caching.
                    continue

                cached = self._cache.get(cache_key)
                if cached is not None:
                    self._cache.move_to_end(cache_key)
                    return cached
                self._cache[cache_key] = result
                if len(self._cache) > self._maxsize:
                    self._cache.popitem(last=False)
                return result

    def _leave_inflight(self, shape_key: bytes) -> None:
        remaining = self._inflight[shape_key] - 1
        if remaining:
            self._inflight[shape_key] = remaining
        else:
            del self._inflight[shape_key]
            self._generations.pop(shape_key, None)

    def invalidate(self, shape_key: bytes | None = None) -> None:
        with self._lock:
            if not shape_key:
                self._global_generation += 1
                self._cache.clear()
                return

            for cache_key in tuple(self._cache):
                if cache_key[0] == shape_key:
                    del self._cache[cache_key]
            if self._inflight.get(shape_key, 0):
                self._generations[shape_key] = (
                    self._generations.get(shape_key, 0) + 1
                )

    def cache_clear(self) -> None:
        self.invalidate()

    def cache_info(self) -> _SolverCacheInfo:
        with self._lock:
            return _SolverCacheInfo(
                self._hits,
                self._misses,
                self._maxsize,
                len(self._cache),
            )


# The foreground path uses an entry-bounded local LRU and a Redis-backed miss
# handler. Foreground and bulk both call stencil_solver_runtime.solve_component;
# the worker bypasses only the foreground LRU. Pub/Sub invalidates every local
# backend-profile entry for the changed canonical component.
DEFAULT_SOLVER_BACKEND: SolverBackend = create_solver_backend("highs")

_SOLVER_CACHE_NAMESPACE = "solver-component-d4-v4"
_solver_component_persistent = redis_persistent_cache(
    namespace=_SOLVER_CACHE_NAMESPACE,
)(_solve_component_uncached)
_solver_component_lru = _InvalidatableSolverCache(
    _solver_component_persistent,
    _SOLVER_CACHE_MAX_ENTRIES,
)


def _invalidate_component_cache(shape_key: bytes = b"") -> None:
    _solver_component_lru.invalidate(shape_key or None)


_solve_component_cached = _solver_component_lru
register_cache_invalidator(
    _SOLVER_CACHE_NAMESPACE, _invalidate_component_cache,
)


def _solve_region(
    region: Sequence[int], width: int, height: int,
    family: Sequence[Stencil],
    time_limit_ms: int = _SOLVER_TIME_LIMIT_MS,
    solver_backend: SolverBackend = DEFAULT_SOLVER_BACKEND,
) -> Dict[str, Any]:
    """Decompose, expose, and solve components through the selected solver port."""
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
        # A component sighting is recorded before either the local LRU or
        # shared Redis result cache can return early. A hit is not an attempt.
        publish_instance_seen(shape_key, family_key, "foreground")
        optimal, status, encoded = _solve_component_cached(
            shape_key, time_limit_ms, family_key, solver_backend,
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
    solver_backend: SolverBackend = DEFAULT_SOLVER_BACKEND,
) -> Dict[str, Any]:
    ownership = _extract_maps(opaque, width, height)
    positive_cells, negative_cells = _ownership_regions(
        ownership, board, width, height, threshold,
    )
    positive = _solve_region(positive_cells, width, height, STENCIL_FAMILY, solver_backend=solver_backend)
    negative = _solve_region(negative_cells, width, height, STENCIL_FAMILY, solver_backend=solver_backend)
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
    solver_backend: SolverBackend | None = None,
) -> Transformer[KataGoQuery, KataGoResponse]:
    """Enrich opted-in final packets; the backend is injected at composition."""
    selected_backend = DEFAULT_SOLVER_BACKEND if solver_backend is None else solver_backend
    request_cache: Dict[ClientId, RequestContext] = {}

    def on_query(eid: ClientId, query: KataGoQuery) -> Optional[KataGoQuery]:
        context = _request_context(query)
        if context is not None:
            request_cache[eid] = context
            # Ownership is an input to this opt-in enrichment. Ask KataGo for
            # the map here so clients do not need to know this implementation
            # detail or remember a second protocol flag.
            query.opaque["includeOwnership"] = True
        return query

    def on_response(eid: ClientId, response: KataGoResponse) -> Optional[KataGoResponse]:
        try:
            context = request_cache.get(eid)
            if (
                context is not None
                and isinstance(response, AnalyzeResponse)
                and not response.is_during_search
            ):
                try:
                    board = _board_at_turn(context, response.turn_number)
                    response.opaque["stencilCover"] = _build_stencil_cover(
                        response.opaque,
                        board,
                        context.width,
                        context.height,
                        context.threshold,
                        solver_backend=selected_backend,
                    )
                except Exception:
                    # Stencil cover is optional enrichment. A malformed or
                    # incomplete map must not kill the session's send loop or
                    # suppress the original KataGo analysis response.
                    logger.exception(
                        "Could not build stencilCover for query %s; "
                        "forwarding the original KataGo response",
                        eid,
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


# Explicit exports preserve the historical helper surface used by geometry
# tests and the optional bulk worker under strict mypy settings.
__all__ = [
    "stencil_cover_enricher", "STENCIL_FAMILY", "RequestContext", "ComponentFrame",
    "_MAX_BOARD_SIZE", "_D4_INVERSE", "_D4_MATRICES",
    "_decode_component_shape_key", "_decode_cached_placements",
    "_encode_cached_placements", "_normalise_shape", "_transform_cell",
    "_verify_component_placements", "_parse_stencil_family",
    "_threshold_from_metadata", "_board_dimensions", "_numeric_array",
    "_extract_maps", "_coordinate_index", "_parse_position_entries",
    "_neighbours", "_chain_and_liberties", "_board_at_turn",
    "_dead_stone_owners", "_ownership_regions", "_candidate_placements",
    "_connected_components", "_family_dihedral_group", "_component_shape_key",
    "_build_stencil_cover", "_request_context", "_solve_component_uncached",
    "_solve_component_cached", "_invalidate_component_cache",
]
