"""
reactive_pipeline.py — Composable reactive computation graph with arbitrary topologies.

A Pipeline is an eDSL for declaring how a stream of indexed values should flow through
a series of topology+algebra pairs.  Each combinator sets a topology (which inputs j
depends on) and Map/Fold/ZipWith supplies the algebra (how those inputs are combined).
The compiled result propagates updates incrementally, touching only the outputs that
actually depend on each changed input.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import functools
from functools import lru_cache
import logging
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _are_equal(a: Any, b: Any) -> bool:
    """Safely compare two values, including Pandas and NumPy objects."""
    if a is b:
        return True
    # Pandas Series/DataFrame
#    if hasattr(a, "equals") and callable(a.equals):
#        try:
#            return bool(a.equals(b))
#        except Exception:
#            return False
    # NumPy arrays
    if hasattr(a, "shape") and hasattr(a, "all"):
        import numpy as np
        return bool(np.array_equal(a, b))
    try:
        return bool(a == b)
    except (ValueError, TypeError):
        return False


class MissingData(Exception):
    """Raised when a dependency index is not yet present in memory."""


def extract_indices(shape: Any):
    """Yield every integer leaf in a (possibly nested) shape descriptor."""
    if isinstance(shape, int):
        yield shape
    elif isinstance(shape, (list, tuple)):
        for item in shape:
            yield from extract_indices(item)
    # None / anything else → no indices


def instantiate_shape(shape: Any, memory: Dict[int, Any]) -> Any:
    """Replace every integer leaf in shape with the value stored at that index."""
    if isinstance(shape, int):
        if shape not in memory:
            raise MissingData(f"index {shape} not yet in memory")
        return memory[shape]
    if isinstance(shape, (list, tuple)):
        return [instantiate_shape(x, memory) for x in shape]
    return shape


# ---------------------------------------------------------------------------
# Computation graph nodes
# ---------------------------------------------------------------------------

class CompiledNode:
    """
    A single layer in the pipeline.  Holds:
    - in_mem  : latest received input values
    - out_mem : latest computed output values
    - rev_deps: for each input index i, the set of output indices j that depend on it
    - shapes  : for each output index j, the shape descriptor encoding its dependencies
    """

    def __init__(
        self,
        domain: List[int],
        context_fn: Callable[[int], Any],
        compute_fn: Callable[[Any], Any],
    ) -> None:
        self.compute_fn = compute_fn
        self.rev_deps: Dict[int, Set[int]] = defaultdict(set)
        self.shapes: Dict[int, Any] = {}
        self.in_mem: Dict[int, Any] = {}
        self.out_mem: Dict[int, Any] = {}

        for j in domain:
            shape = context_fn(j)
            if shape is None:
                continue
            self.shapes[j] = shape
            for i in extract_indices(shape):
                self.rev_deps[i].add(j)

    def update(self, idx: int, val: Any) -> List[Tuple[int, Any]]:
        """
        Record a new value for input index idx and return (output_index, new_value)
        pairs for every output that changed as a result.
        """
        if idx in self.in_mem and _are_equal(self.in_mem[idx], val):
            return []

        self.in_mem[idx] = val
        deltas: List[Tuple[int, Any]] = []

        for j in self.rev_deps.get(idx, set()):
            try:
                materialized = instantiate_shape(self.shapes[j], self.in_mem)
                new_out = self.compute_fn(materialized)
            except MissingData:
                continue

            if j not in self.out_mem or not _are_equal(self.out_mem[j], new_out):
                self.out_mem[j] = new_out
                deltas.append((j, new_out))

        return deltas


class MonitoredNode(CompiledNode):
    """
    A throttled variant of CompiledNode.

    After the very first successful emission, subsequent output is suppressed
    until at least ``throttle_ms`` milliseconds have elapsed since the last
    emission.  Inputs that arrive during the quiet window are buffered in
    ``_pending``; on the next open window they are all flushed through the
    parent node logic so the computation sees the *latest* value for every
    input that changed.  The final (deduplicated) outputs are then emitted as
    a single batch.

    The first time a computation becomes possible (all dependencies satisfied
    for the first time) it is emitted immediately, regardless of timing.
    This makes it particularly well-suited to reduction topologies such as
    ``Global()``, where the computation can only fire once every input slot
    has been filled.

    Typical usage in a Pipeline::

        Pipeline(N)
            .Global()
            .Monitor(throttle_ms=500)
            .Map(cwt_fn)
            .compile()
    """

    def __init__(
        self,
        domain: List[int],
        context_fn: Callable[[int], Any],
        compute_fn: Callable[[Any], Any],
        throttle_ms: float,
    ) -> None:
        super().__init__(domain, context_fn, compute_fn)
        self._throttle_s: float = throttle_ms / 1000.0
        # None means "never emitted"; distinguishes first-emission fast-path.
        self._last_emit_time: Optional[float] = None
        # Inputs buffered while the gate is closed.  Later arrivals overwrite
        # earlier ones for the same index, ensuring we never re-process stale
        # values when we finally flush.
        self._pending: Dict[int, Any] = {}

    def update(self, idx: int, val: Any) -> List[Tuple[int, Any]]:
        import time as _time
        now = _time.monotonic()

        # Always buffer the incoming update.
        self._pending[idx] = val

        first_time = self._last_emit_time is None
        gate_open = first_time or (now - self._last_emit_time >= self._throttle_s)

        if not gate_open:
            return []  # throttled — accumulate and wait

        # Gate is open.  Apply all buffered inputs to in_mem in a single pass,
        # collecting the union of outputs that depend on any changed input.
        # Each output is then computed exactly once against the final in_mem
        # state, avoiding redundant calls to compute_fn (critical for
        # expensive reductions such as CWT).
        changed_outputs: Set[int] = set()
        for p_idx, p_val in self._pending.items():
            if p_idx not in self.in_mem or not _are_equal(self.in_mem[p_idx], p_val):
                self.in_mem[p_idx] = p_val
                changed_outputs.update(self.rev_deps.get(p_idx, set()))
        self._pending.clear()

        results: List[Tuple[int, Any]] = []
        for j in changed_outputs:
            try:
                materialized = instantiate_shape(self.shapes[j], self.in_mem)
                new_out = self.compute_fn(materialized)
            except MissingData:
                continue
            if j not in self.out_mem or not _are_equal(self.out_mem[j], new_out):
                self.out_mem[j] = new_out
                results.append((j, new_out))

        if results:
            self._last_emit_time = now

        return results


# ---------------------------------------------------------------------------
# Pipeline eDSL
# ---------------------------------------------------------------------------

class Pipeline:
    def __init__(
        self,
        domain_size: Optional[int] = None,
        *,
        initial_domain_size: Optional[int] = None,
    ) -> None:
        size = domain_size if domain_size is not None else initial_domain_size
        if size is None:
            raise TypeError("Pipeline requires a domain size (positional or keyword)")
        self.domain: List[int] = list(range(size))
        self._nodes: List[Tuple] = []   # 4-tuples: (domain, ctx, comp, throttle|None)
        self._curr_context: Optional[Callable] = None
        # Triangular interval metadata: set by Triangular(), consumed by compile().
        # Stores (s, t) tuples in the *input* coordinate system of that layer.
        self._triangular_intervals: Optional[List[Tuple[int, int]]] = None
        # Pending throttle value, consumed by the next _append_node() call.
        self._monitor_throttle_ms: Optional[float] = None

    # -- Internal node registry ----------------------------------------------

    def _append_node(self, domain: List[int], ctx: Any, comp: Callable) -> None:
        """
        Central choke-point for all node registrations.  Captures and clears
        any pending Monitor throttle so it is attached to exactly this node.
        """
        throttle = self._monitor_throttle_ms
        self._monitor_throttle_ms = None
        self._nodes.append((domain, ctx, comp, throttle))

    # -- Throttle combinator -------------------------------------------------

    def Monitor(self, throttle_ms: float) -> "Pipeline":
        """
        Gate the *next* algebra step (Map, Fold, MapPipeline, ZipWith) behind a
        time-based throttle.

        Semantics
        ---------
        * The very first successful computation is emitted immediately,
          regardless of timing.
        * After that, output is suppressed until at least ``throttle_ms``
          milliseconds have elapsed since the last emission.
        * Inputs that arrive while the gate is closed are buffered.  On the
          next open window all buffered inputs are flushed together, so the
          computation always sees the most recent values.

        This combinator is especially powerful in combination with ``Global()``:
        the Global topology already ensures the computation only fires once
        every input slot has data, and Monitor then throttles subsequent
        recomputations without ever losing an update.

        Example::

            Pipeline(N)
                .Global()
                .Monitor(throttle_ms=500)
                .Map(cwt_fn)
                .compile()
        """
        self._monitor_throttle_ms = throttle_ms
        return self

    # -- Topology combinators ------------------------------------------------

    def ClippedWindow(self, left: int, right: int) -> "Pipeline":
        """Every j gets the intersection of [j+left, j+right] with [0, N)."""
        N = len(self.domain)
        self._curr_context = lambda j: tuple(
            k for k in range(j + left, j + right + 1) if 0 <= k < N
            )
        return self

    def Project(self, new_size: int, context_fn: Callable[[int], Any]) -> "Pipeline":
        """General static domain projection."""
        if new_size < 0:
            raise ValueError("new_size must be non-negative")
        self.domain = list(range(new_size))
        self._curr_context = context_fn
        return self

    def Window(self, left: int, right: int) -> "Pipeline":
        N = len(self.domain)
        def ctx(j: int) -> Optional[List[int]]:
            ks = tuple(range(j + left, j + right + 1))
            return ks if all(0 <= k < N for k in ks) else None
        self._curr_context = ctx
        return self

    def Global(self) -> "Pipeline":
        """
        Reduction topology: output index 0 depends on *all* N inputs.

        The computation at j=0 only fires once every input slot is filled
        (MissingData is raised otherwise and the update is silently skipped).
        This makes Global() the natural topology for reduction-shaped analyses
        such as wavelet transforms over the full delta sequence.

        Pair with Monitor() to throttle subsequent recomputations::

            .Global()
            .Monitor(throttle_ms=500)
            .Map(reduction_fn)
        """
        N = len(self.domain)
        self._curr_context = lambda j: tuple(range(N)) if j == 0 else None
        return self

    def SubStream(self, indices: List[int]) -> "Pipeline":
        """
        Extracts a discrete substream of specific indices, mapping them into a new,
        smaller contiguous domain. Ideal for partitioning players or phases.
        """
        new_size = len(indices)
        self.domain = list(range(new_size))
        self._curr_context = lambda j, _inds=indices: _inds[j]
        # Lock the topology into a node so it isn't overwritten by the next step
        return self.Map(lambda x: x)

    # -- Domain-morphing topology --------------------------------------------

    def ZipWith(self, num_streams: int, func: Callable) -> "Pipeline":
        assert len(self.domain) % num_streams == 0, (
            f"Domain size {len(self.domain)} not divisible by num_streams={num_streams}"
        )
        N = len(self.domain) // num_streams

        ctx = lambda j, _N=N, _ns=num_streams: [j + i * _N for i in range(_ns)]
        comp = lambda vals: func(*vals)

        new_domain = list(range(N))
        self._append_node(new_domain, ctx, comp)

        self.domain = new_domain
        self._curr_context = None
        return self

    def Triangular(self) -> "Pipeline":
        """
        TOPOLOGY: Projects the linear domain of size M into a triangular domain of
        size M*(M+1)//2.  Each output index k corresponds to an interval [s, t]
        (both inclusive, in the input coordinate system), where s <= t < M.

        The ordering is row-major: (0,0), (0,1), ..., (0,M-1), (1,1), ..., (M-1,M-1).

        After calling compile(), the resulting CompiledPipeline exposes:
          .intervals           — list of (s, t) tuples, indexed by k
          .interval_of(k)      — returns the (s, t) tuple for triangular index k
          .endpoints_of(k)     — alias for interval_of
        """
        M = len(self.domain)
        # Build interval table: (s, t) → sorted input indices [s, s+1, ..., t]
        intervals: List[Tuple[int, int]] = []
        for s in range(M):
            for t in range(s, M):
                intervals.append((s, t))

        # Persist metadata so compile() can expose it on CompiledPipeline.
        self._triangular_intervals = intervals

        self.domain = list(range(len(intervals)))
        self._curr_context = lambda j, _ivs=intervals: tuple(range(_ivs[j][0], _ivs[j][1] + 1))
        return self

    def ApplyTriangular(self, aggregate: Optional[Callable] = None) -> "Pipeline":
        """
        CONVENIENCE WRAPPER: Sets triangular topology and applies an aggregate.
        """
        if aggregate is None:
            aggregate = lambda vals: sum(vals) / len(vals)  # Default to Mean
        return self.Triangular().Map(aggregate)

    def Interleave(self, num_branches: int, branch_builder: Callable[[int], "Pipeline"]) -> "Pipeline":
        """
        STRUCTURAL: Splits the current domain into N strides (e.g. Players),
        processes each through a sub-pipeline, and interleaves the results.
        """
        M = len(self.domain)
        sub_size = M // num_branches

        proto_pipe = branch_builder(sub_size).compile()
        branch_out_size = len(proto_pipe.nodes[-1].shapes)

        branches = [branch_builder(sub_size).compile() for _ in range(num_branches)]

        def interleave_compute(j: int, val: Any) -> List[Tuple[int, Any]]:
            branch_id = j % num_branches
            branch_idx = j // num_branches
            branch_updates = branches[branch_id].updateAt(branch_idx, val)
            return [
                (sub_k * num_branches + branch_id, new_v)
                for sub_k, new_v in branch_updates
            ]

        new_domain_size = branch_out_size * num_branches

        def interleave_ctx(k: int) -> int:
            return "ROUTE_SIGNAL"

        self._append_node(list(range(new_domain_size)), interleave_ctx, interleave_compute)
        self.domain = list(range(new_domain_size))
        return self

    # -- Algebra steps -------------------------------------------------------

    def Map(self, func: Callable) -> "Pipeline":
        ctx = self._curr_context or (lambda j: j)
        self._append_node(list(self.domain), ctx, func)
        self._curr_context = None
        return self

    def Fold(self, binary_op: Callable[[Any, Any], Any], initial: Any = None) -> "Pipeline":
        if initial is not None:
            comp_fn = lambda vals: functools.reduce(binary_op, vals, initial)
        else:
            comp_fn = lambda vals: functools.reduce(binary_op, vals)

        ctx = self._curr_context
        if ctx is None:
            raise ValueError("Fold requires a preceding topology combinator")

        self._append_node(list(self.domain), ctx, comp_fn)
        self._curr_context = None
        return self

    def MapPipeline(self, sub_pipeline_builder: Callable[[int], "Pipeline"]) -> "Pipeline":
        def run_sub_pipeline(materialized_shape: List[Any]) -> List[Any]:
            sub_N = len(materialized_shape)
            sub_pipe = sub_pipeline_builder(sub_N).compile()
            sub_pipe.initialize(materialized_shape)
            final_node = sub_pipe.nodes[-1]
            return [final_node.out_mem.get(k) for k in range(sub_N)]

        ctx = self._curr_context
        if ctx is None:
            raise ValueError("MapPipeline requires a preceding topology combinator")

        self._append_node(list(self.domain), ctx, run_sub_pipeline)
        self._curr_context = None
        return self

    # -- Compiler ------------------------------------------------------------

    def compile(self) -> "CompiledPipeline":
        intervals = self._triangular_intervals  # None if Triangular() was never called
        return CompiledPipeline(self._nodes, intervals=intervals)


# ---------------------------------------------------------------------------
# Compiled pipeline
# ---------------------------------------------------------------------------

class CompiledPipeline:
    def __init__(
        self,
        node_specs: List[Tuple],
        *,
        intervals: Optional[List[Tuple[int, int]]] = None,
    ) -> None:
        self.nodes: List[CompiledNode] = []
        for spec in node_specs:
            domain, ctx, comp = spec[0], spec[1], spec[2]
            throttle: Optional[float] = spec[3] if len(spec) > 3 else None
            if throttle is not None:
                self.nodes.append(MonitoredNode(domain, ctx, comp, throttle))
            else:
                self.nodes.append(CompiledNode(domain, ctx, comp))

        # Triangular interval metadata, if the pipeline contained a Triangular() layer.
        # intervals[k] == (s, t) where s and t are in the *input* coordinate system
        # of the Triangular layer (i.e., positions within the color's delta sequence).
        self.intervals: Optional[List[Tuple[int, int]]] = intervals

    # -- Interval endpoint helpers -------------------------------------------

    def interval_of(self, k: int) -> Tuple[int, int]:
        """
        Return the (s, t) endpoints for triangular index k.

        Both s and t are expressed in the coordinate system of the sequence that
        was passed into Triangular() — i.e., positions within the per-color delta
        stream, NOT raw move indices.

        Raises ValueError if this pipeline has no Triangular() layer.
        Raises IndexError if k is out of range.
        """
        if self.intervals is None:
            raise ValueError(
                "interval_of() requires a Triangular() layer in the pipeline"
            )
        return self.intervals[k]

    # Alias for discoverability
    endpoints_of = interval_of

    # -- Execution -----------------------------------------------------------

    def initialize(self, values: List[Any]) -> None:
        for k, v in enumerate(values):
            self.updateAt(k, v)

    def updateAt(self, k: int, new_val: Any) -> List[Tuple[int, Any]]:
        updates: List[Tuple[int, Any]] = [(k, new_val)]
        for node in self.nodes:
            next_updates: List[Tuple[int, Any]] = []
            for idx, val in updates:
                next_updates.extend(node.update(idx, val))
            updates = next_updates
            if not updates:
                break
        return updates
