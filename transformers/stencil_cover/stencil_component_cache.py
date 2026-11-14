# SPDX-License-Identifier: MIT
"""Invalidatable in-process cache for component solver results."""

from __future__ import annotations

from collections import OrderedDict
import logging
import threading
from typing import Dict, NamedTuple

from .stencil_cache_storage import (
    CacheResult,
    SolverCacheCandidate,
    SolverCacheRequest,
    SolverCacheResultFunction,
    redis_persistent_cache,
    register_cache_invalidator,
)
from .stencil_solver_api import OutcomeStatus, SolverFailure
from .stencil_solver_geometry import Stencil
from .stencil_solver_runtime import solve_component as _solve_with_backend


logger = logging.getLogger("kataproxy.stencil_component_cache")
_SOLVER_CACHE_MAX_ENTRIES = 90_000

def _solve_component_uncached(
    request: SolverCacheRequest,
) -> SolverCacheCandidate:
    """Invoke the backend named by one immutable request and retain provenance."""
    execution = _solve_with_backend(
        request.backend,
        request.shape_key,
        request.family,
        request.budget,
    )
    if execution.outcome.status is OutcomeStatus.ERROR:
        error = execution.outcome.error
        message = error.message if isinstance(error, SolverFailure) else "backend invocation failed"
        logger.error(
            "Stencil solver backend %s failed: %s",
            execution.identity.fingerprint,
            message,
        )
    return SolverCacheCandidate(
        result=execution.cache_result,
        provenance=execution.result_provenance,
    )

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
        function: SolverCacheResultFunction,
        maxsize: int,
    ) -> None:
        self._function = function
        self._maxsize = maxsize
        self._cache: OrderedDict[
            tuple[bytes, int, tuple[Stencil, ...], str],
            CacheResult,
        ] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._global_generation = 0
        self._generations: Dict[bytes, int] = {}
        self._inflight: Dict[bytes, int] = {}

    def __call__(self, request: SolverCacheRequest) -> CacheResult:
        shape_key = request.shape_key
        cache_key = (
            shape_key,
            request.budget.wall_time_limit_ms,
            request.family,
            request.work_profile.fingerprint,
        )
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
                result = self._function(request)
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


