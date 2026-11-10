"""
Optional KataGo response enricher for signed ownership stencil covers.

A query opts in with:
    capabilities: {"stencil_cover": {"truncation_threshold": 0.6}}

Only final analysis responses are processed. The response annotation is
written to opaque.stencilCover and contains independently solved positive-
and negative-ownership exact covers. Ownership sign is never discarded.

HiGHS is an optional runtime dependency. The capability is advertised only
when both highspy and the proxy-owned stencil family are available.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""
from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink
from katago import AnalyzeResponse, KataGoAction, KataGoQuery, KataGoResponse
from proxy_logging import Event, get_proxy_logger

logger = logging.getLogger("kataproxy." + __name__)
_log = get_proxy_logger(__name__)

try:
    import highspy
except ImportError as exc:
    highspy = None  # type: ignore[assignment]
    _HIGHS_IMPORT_ERROR: Optional[ImportError] = exc
else:
    _HIGHS_IMPORT_ERROR = None

Stencil = Tuple[str, Tuple[Tuple[int, int], ...]]
_MAX_BOARD_SIZE = 25
_SOLVER_TIME_LIMIT_SECONDS = 0.25
_OWNERSHIP_EPSILON = 1e-5
_POLICY_EPSILON = 1e-3


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


try:
    _FAMILY_PATH = Path(__file__).with_name("stencil_family.txt")
    STENCIL_FAMILY: Tuple[Stencil, ...] = _parse_stencil_family(
        _FAMILY_PATH.read_text(encoding="utf-8")
    )
    _FAMILY_ERROR: Optional[Exception] = None
except Exception as exc:
    STENCIL_FAMILY = ()
    _FAMILY_ERROR = exc
    logger.error("stencil-cover family unavailable: %s", exc)

HIGHS_AVAILABLE = highspy is not None
STENCIL_COVER_AVAILABLE = HIGHS_AVAILABLE and bool(STENCIL_FAMILY)
if not HIGHS_AVAILABLE:
    logger.warning(
        "highspy not found; stencil-cover enrichment disabled. "
        "Install the optional highspy dependency to enable it."
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
    try:
        raw = np.asarray(value)
        if raw.dtype.kind not in "iuf":
            raise ValueError(f"{field} must contain only numbers")
        result = raw.astype(np.float64, copy=False).reshape(-1)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a numeric array") from exc
    if not np.isfinite(result).all():
        raise ValueError(f"{field} contains a non-finite value")
    return result


def _extract_maps(
    opaque: Dict[str, Any], width: int, height: int,
) -> Tuple[np.ndarray, np.ndarray]:
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

    root_info = opaque.get("rootInfo")
    policy_value = opaque.get("policy")
    if policy_value is None and isinstance(root_info, dict):
        policy_value = root_info.get("policy")
    if policy_value is None:
        raise ValueError("response has no policy map; legal-point masking is unavailable")
    policy = _numeric_array(policy_value, "policy")
    if policy.size not in (area, area + 1):
        raise ValueError(
            f"policy has {policy.size} values; expected {area} board values "
            f"with an optional pass value"
        )
    policy = policy[:area]
    illegal = policy < -0.5
    if ((illegal & (np.abs(policy + 1.0) > _POLICY_EPSILON))
            | (~illegal & ((policy < 0.0) | (policy > 1.0 + _POLICY_EPSILON)))).any():
        raise ValueError("policy values must be -1 for illegal points or probabilities in [0, 1]")
    legal = ~illegal
    return ownership, legal


def _ownership_regions(
    ownership: Sequence[float],
    legal: Sequence[bool],
    threshold: float,
) -> Tuple[List[int], List[int]]:
    """Return flat row-major cell indices, preserving the two ownership signs."""
    positive: List[int] = []
    negative: List[int] = []
    for index, (value, is_legal) in enumerate(zip(ownership, legal)):
        if not is_legal:
            continue
        # At zero, neutral ownership belongs to neither side. For non-zero
        # thresholds the comparison is inclusive, so a threshold of 1 keeps
        # exactly fully-owned points.
        if threshold == 0.0:
            if value > 0.0:
                positive.append(index)
            elif value < 0.0:
                negative.append(index)
        elif value >= threshold:
            positive.append(index)
        elif value <= -threshold:
            negative.append(index)
    return positive, negative


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


def _solve_region(
    region: Sequence[int], width: int, height: int,
    family: Sequence[Stencil],
) -> Dict[str, Any]:
    if not region:
        return {
            "placements": [], "tileCount": 0, "optimal": True, "status": "empty",
        }
    if highspy is None:
        raise RuntimeError("highspy is not installed")

    candidates = _candidate_placements(region, width, height, family)
    point_to_columns: Dict[int, List[int]] = {point: [] for point in region}
    for column, (_, points) in enumerate(candidates):
        for point in points:
            point_to_columns[point].append(column)
    if any(not columns for columns in point_to_columns.values()):
        raise RuntimeError("stencil family failed to provide a feasible singleton cover")

    highs = highspy.Highs()
    highs.setOptionValue("log_to_console", False)
    highs.setOptionValue("time_limit", _SOLVER_TIME_LIMIT_SECONDS)
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

    status = highs.getModelStatus()
    optimal = status == highspy.HighsModelStatus.kOptimal
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
            points[0]: i
            for i, (_, points) in enumerate(candidates)
            if len(points) == 1
        }
        chosen = [singleton_columns[point] for point in region]
        optimal = False
    elif not optimal:
        # A time-limited incumbent is still a valid exact cover, but it is not
        # labelled as a proven minimum.
        pass

    placements = []
    for column in chosen:
        name, points = candidates[column]
        placements.append({
            "stencil": name,
            "points": [[point % width, point // width] for point in points],
        })
    return {
        "placements": placements,
        "tileCount": len(placements),
        "optimal": bool(optimal),
        "status": "fallback" if fallback else ("optimal" if optimal else "feasible"),
    }


def _build_stencil_cover(
    opaque: Dict[str, Any], width: int, height: int, threshold: float,
) -> Dict[str, Any]:
    ownership, legal = _extract_maps(opaque, width, height)
    positive_cells, negative_cells = _ownership_regions(
        ownership, legal, threshold,
    )
    positive = _solve_region(positive_cells, width, height, STENCIL_FAMILY)
    negative = _solve_region(negative_cells, width, height, STENCIL_FAMILY)
    return {
        "version": 1,
        "threshold": threshold,
        "width": width,
        "height": height,
        "positive": positive,
        "negative": negative,
        "optimal": positive["optimal"] and negative["optimal"],
    }


def stencil_cover_enricher(
    link: ProxyLink[ClientId, InternalId],
) -> Transformer[KataGoQuery, KataGoResponse]:
    """Enrich opted-in final analysis packets with signed exact stencil covers."""
    request_cache: Dict[ClientId, Tuple[int, int, float]] = {}

    def on_query(eid: ClientId, query: KataGoQuery) -> Optional[KataGoQuery]:
        if query.action != KataGoAction.ANALYZE or not STENCIL_COVER_AVAILABLE:
            return query
        capabilities = query.opaque.get("capabilities")
        if not isinstance(capabilities, dict) or "stencil_cover" not in capabilities:
            # capability_gate auto-engages for legacy clients; this feature
            # intentionally requires explicit opt-in to avoid surprise CPU work.
            return query
        try:
            threshold = _threshold_from_metadata(capabilities["stencil_cover"])
            width, height = _board_dimensions(query.opaque)
        except ValueError as exc:
            _log.warning(
                Event.DIAGNOSTIC, orig=eid,
                msg=f"stencil_cover: refusing malformed query metadata: {exc}",
            )
            return query
        request_cache[eid] = (width, height, threshold)
        return query

    def on_response(eid: ClientId, response: KataGoResponse) -> Optional[KataGoResponse]:
        try:
            context = request_cache.get(eid)
            if (
                context is not None
                and isinstance(response, AnalyzeResponse)
                and not response.is_during_search
            ):
                width, height, threshold = context
                try:
                    annotation = _build_stencil_cover(
                        response.opaque, width, height, threshold,
                    )
                    response.opaque["stencilCover"] = annotation
                except ValueError as exc:
                    _log.warning(
                        Event.DIAGNOSTIC, orig=eid,
                        msg=f"stencil_cover: skipping malformed analysis response: {exc}",
                    )
                except Exception as exc:
                    _log.error(
                        Event.DIAGNOSTIC, orig=eid,
                        msg=f"stencil_cover enrichment failed for eid={eid!r}: {exc}",
                    )
        finally:
            if link.mapping.forward(eid) is None:
                request_cache.pop(eid, None)
        return response

    return Transformer(
        name="stencil_cover",
        on_query=on_query,
        on_response=on_response,
    )
