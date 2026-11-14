# SPDX-License-Identifier: MIT
"""Policy-independent exact-cover orchestration for signed board regions."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Sequence

import sproxy_config as cfg

from .stencil_cache_storage import SolverCacheRequest
from .stencil_component_cache import _solve_component_cached
from .stencil_corpus_publisher import publish_instance_seen_many
from .stencil_family import STENCIL_FAMILY
from .stencil_ownership import OwnershipRegionPolicy, make_ownership_evidence
from .stencil_ownership_store import SQLiteOwnershipStore
from .stencil_region_components import (
    _PreparedComponent,
    _prepare_region_components,
)
from .stencil_solver_api import (
    NoNativeEffortLimit, SolverBackend, SolverBudget,
)
from .stencil_solver_factory import DEFAULT_SOLVER_BACKEND
from .stencil_solver_geometry import (
    Stencil,
    decode_cached_placements as _decode_cached_placements,
    verify_component_placements as _verify_component_placements,
)


logger = logging.getLogger("kataproxy.stencil_region_solver")

_SOLVER_TIME_LIMIT_MS = 250

def _solve_prepared_region(
    prepared_components: Sequence[_PreparedComponent],
    width: int,
    family: Sequence[Stencil],
    time_limit_ms: int = _SOLVER_TIME_LIMIT_MS,
    solver_backend: SolverBackend = DEFAULT_SOLVER_BACKEND,
) -> Dict[str, Any]:
    """Solve prepared components; corpus capture has already happened."""
    if not prepared_components:
        return {
            "components": [],
            "placements": [],
            "tileCount": 0,
            "optimal": True,
            "status": "empty",
        }
    family_key = tuple(family)
    budget = SolverBudget(
        wall_time_limit_ms=int(time_limit_ms),
        native_effort_limit=NoNativeEffortLimit(),
        policy_id=f"foreground-{int(time_limit_ms)}ms",
    )
    components: List[Dict[str, Any]] = []
    placements: List[Dict[str, Any]] = []
    statuses: List[int] = []
    for component, shape_key, frame in prepared_components:
        request = SolverCacheRequest(
            shape_key=shape_key,
            family=family_key,
            backend=solver_backend,
            budget=budget,
        )
        optimal, status, encoded = _solve_component_cached(request)
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


def _solve_region(
    region: Sequence[int], width: int, height: int,
    family: Sequence[Stencil],
    time_limit_ms: int = _SOLVER_TIME_LIMIT_MS,
    solver_backend: SolverBackend = DEFAULT_SOLVER_BACKEND,
) -> Dict[str, Any]:
    """Capture this region's component sightings together before solving."""
    if not region:
        return _solve_prepared_region(
            (), width, family, time_limit_ms, solver_backend,
        )
    family_key = tuple(family)
    prepared = _prepare_region_components(region, width, height, family_key)
    publish_instance_seen_many(
        tuple(shape_key for _component, shape_key, _frame in prepared),
        family_key,
        "foreground",
    )
    return _solve_prepared_region(
        prepared, width, family_key, time_limit_ms, solver_backend,
    )

def _build_stencil_cover(
    opaque: Dict[str, Any],
    board: Sequence[str | None],
    width: int,
    height: int,
    policy_metadata: Dict[str, Any],
    ownership_policy: OwnershipRegionPolicy,
    solver_backend: SolverBackend | None = None,
    *,
    turn_number: int = 0,
    is_during_search: bool = False,
    response_context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    evidence = make_ownership_evidence(
        opaque,
        board,
        width,
        height,
        policy_metadata,
        requires_ownership_stdev=ownership_policy.requires_ownership_stdev,
    )
    selected_regions = ownership_policy.classify(evidence)
    if cfg.STENCIL_OWNERSHIP_CORPUS_ENABLED:
        context: dict[str, Any] = {}
        context_keys = (
            "rootInfo", "scoreLead", "scoreStdev", "winrate",
            "visits", "winner", "finalScore",
        )
        for key in context_keys:
            if response_context is not None and key in response_context:
                context[key] = response_context[key]
        try:
            SQLiteOwnershipStore(cfg.STENCIL_OWNERSHIP_CORPUS_DB).record(
                evidence,
                selected_regions,
                ownership_policy,
                turn_number=turn_number,
                is_during_search=is_during_search,
                response_context=context,
            )
        except Exception:
            # Evidence capture must not suppress the interactive stencil-cover
            # response. The diagnostic names a real durability gap.
            logger.exception(
                "Could not persist ownership evidence for turn %s; "
                "the stencil-cover response will continue without this observation",
                turn_number,
            )
    family_key = tuple(STENCIL_FAMILY)
    positive_components = _prepare_region_components(
        selected_regions.positive, width, height, family_key,
    )
    negative_components = _prepare_region_components(
        selected_regions.negative, width, height, family_key,
    )
    sighting_keys = tuple(
        shape_key for _component, shape_key, _frame
        in (*positive_components, *negative_components)
    )
    publish_instance_seen_many(sighting_keys, family_key, "foreground")

    selected_backend = DEFAULT_SOLVER_BACKEND if solver_backend is None else solver_backend
    positive = _solve_prepared_region(
        positive_components, width, family_key, solver_backend=selected_backend,
    )
    negative = _solve_prepared_region(
        negative_components, width, family_key, solver_backend=selected_backend,
    )
    result: Dict[str, Any] = {
        "version": 2,
        "width": width,
        "height": height,
        "positive": positive,
        "negative": negative,
        "optimal": positive["optimal"] and negative["optimal"],
        "ownershipPolicy": selected_regions.policy_id,
        "ownershipDiagnostics": dict(selected_regions.diagnostics),
    }
    if selected_regions.threshold is not None:
        result["threshold"] = selected_regions.threshold
    return result
