# SPDX-License-Identifier: MIT
"""Optional low-priority worker that upgrades early-exit stencil covers.

The normal foreground stencil-cover path does not require this process.
Run the optional worker with `kataproxy-stencil-bulk-solver` after installing
the project, or from a checkout with
`python -m transformers.stencil_bulk_solver`. One worker processes the Redis
pending set sequentially. Each canonical component receives one 10-second wall-clock solver budget;
a non-optimal outcome is retained as the best result and
accompanied by durable timeout metadata so it is not retried forever.
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Any, Callable, Dict, Sequence, Tuple

from transformers.stencil_cache_storage import (
    RedisStencilStore,
    create_redis_client,
)
from transformers.stencil_cover_enricher import (
    ComponentFrame,
    STENCIL_FAMILY,
    Stencil,
    _decode_cached_placements,
    _decode_component_shape_key,
    _verify_component_placements,
)
from transformers.stencil_solver_api import (
    NoNativeEffortLimit, OutcomeStatus, SolverBackend, SolverBudget,
)
from transformers.stencil_solver_factory import create_solver_backend
from transformers.stencil_solver_runtime import solve_component

logger = logging.getLogger("kataproxy.stencil_bulk_solver")

CACHE_NAMESPACE = "solver-component-d4-v3"
BULK_TIME_LIMIT_MS = 10_000
DEFAULT_POLL_INTERVAL_SECONDS = 5.0


def _validate_bulk_result(
    shape_key: bytes,
    family: Sequence[Stencil],
    result: Tuple[bool, int, bytes],
) -> Tuple[bool, int, bytes]:
    """Validate the candidate cover in its canonical frame before persisting."""
    if not isinstance(result, tuple) or len(result) != 3:
        raise TypeError("bulk solver must return a 3-tuple")
    optimal, status, encoded = result
    if (
        not isinstance(optimal, bool)
        or isinstance(status, bool)
        or not isinstance(status, int)
        or status not in (0, 1, 2)
    ):
        raise ValueError("bulk solver returned an invalid optimal flag or status")
    if optimal != (status == 0) or not isinstance(encoded, bytes):
        raise ValueError("bulk solver result status and payload disagree")

    width, height, cells = _decode_component_shape_key(shape_key)
    canonical_frame = ComponentFrame(
        board_width=width,
        transform=0,
        offset_y=0,
        offset_x=0,
        canonical_width=width,
        canonical_height=height,
    )
    placements = _decode_cached_placements(encoded, family, canonical_frame)
    _verify_component_placements(cells, width, placements, family)
    return optimal, status, encoded


class StencilBulkSolver:
    """Drain unattempted hard components using a longer, uncached solve."""

    def __init__(
        self,
        store: RedisStencilStore,
        family: Tuple[Stencil, ...] = STENCIL_FAMILY,
        solver_backend: SolverBackend | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.family = family
        self.store.initialize_pending_queue(self.family)
        self.solver_backend = solver_backend or create_solver_backend("highs")
        self.clock = clock

    def process_component(self, shape_key: bytes) -> str:
        """Return 'optimal', 'timed_out', or 'already_retired'."""
        if self.store.timeout_metadata(shape_key, self.family) is not None:
            return "already_retired"

        previous = self.store.get_result(shape_key, self.family)
        if previous is None:
            raise RuntimeError("pending stencil component has no persisted incumbent")
        if previous[0] and previous[1] == 0:
            # Repair a stale queue entry should one ever be left by an older
            # version of the storage schema.
            self.store.put_result(shape_key, self.family, previous)
            return "optimal"

        attempted_at = self.clock()
        execution = solve_component(
            self.solver_backend,
            shape_key,
            self.family,
            SolverBudget(
                wall_time_limit_ms=BULK_TIME_LIMIT_MS,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id=f"bulk-{BULK_TIME_LIMIT_MS}ms",
            ),
        )
        if execution.outcome.status is OutcomeStatus.ERROR:
            error = execution.outcome.error
            message = error.message if hasattr(error, "message") else "backend invocation failed"
            raise RuntimeError(f"bulk solver backend failed: {message}")
        result = _validate_bulk_result(
            shape_key, self.family, execution.cache_result,
        )
        self.store.put_result(shape_key, self.family, result)
        best = self.store.get_result(shape_key, self.family)
        if best is None:
            raise RuntimeError("bulk solver result disappeared from Redis")
        if best[0] and best[1] == 0:
            return "optimal"

        # The timeout marker is written only after the best incumbent has
        # been persisted. The Redis script atomically removes it from pending
        # only while it is still hard and pending; a concurrent exact result
        # therefore cannot be accidentally retired.
        if self.store.mark_timed_out(
            shape_key,
            self.family,
            best,
            time_limit_ms=BULK_TIME_LIMIT_MS,
            attempted_at=attempted_at,
        ):
            return "timed_out"
        return "optimal" if best[0] and best[1] == 0 else "already_retired"

    def run_once(self) -> Dict[str, int]:
        """Process the currently pending snapshot and return outcome counts."""
        counts = {"attempted": 0, "optimal": 0, "timed_out": 0, "skipped": 0}
        for shape_key in sorted(self.store.pending_components(self.family)):
            counts["attempted"] += 1
            try:
                outcome = self.process_component(shape_key)
            except Exception:
                # A corrupt/unexpected key must not terminate the daemon and
                # starve later valid entries. It remains pending for operator
                # investigation and will be logged again on the next pass.
                logger.exception(
                    "Bulk stencil solve failed for canonical key %s",
                    shape_key.hex(),
                )
                counts["skipped"] += 1
                continue
            if outcome == "optimal":
                counts["optimal"] += 1
            elif outcome == "timed_out":
                counts["timed_out"] += 1
            else:
                counts["skipped"] += 1
        return counts


def _set_low_priority() -> None:
    """Set this process to nice 19 where the platform exposes POSIX priority."""
    try:
        if hasattr(os, "setpriority") and hasattr(os, "PRIO_PROCESS"):
            os.setpriority(os.PRIO_PROCESS, os.getpid(), 19)
        else:
            os.nice(19)
    except (AttributeError, OSError) as exc:
        logger.warning("Could not set bulk stencil worker nice level to 19 (%s)", exc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        help="seconds to sleep when no pending component is available",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="process the currently pending snapshot, then exit",
    )
    args = parser.parse_args(argv)
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _set_low_priority()

    try:
        store = RedisStencilStore(create_redis_client(), CACHE_NAMESPACE)
    except Exception:
        logger.exception("Could not connect to Redis DB 7 for bulk stencil solving")
        return 1

    try:
        worker = StencilBulkSolver(store)
    except Exception:
        logger.exception("Could not initialize the Redis bulk stencil queue")
        return 1

    try:
        while True:
            counts = worker.run_once()
            if any(counts[key] for key in ("attempted", "optimal", "timed_out")):
                logger.info("Bulk stencil pass: %s", counts)
            if args.once:
                return 0
            if counts["attempted"] == 0 or counts["skipped"] > 0:
                # Avoid a tight error loop if an index entry is malformed or
                # Redis temporarily rejects one component's result.
                time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        logger.info("Bulk stencil worker stopped")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
