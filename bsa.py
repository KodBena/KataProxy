import ast
import json
import math
from typing import Any,Callable,Dict,List,Optional,Tuple

import numpy as np
from rxp import CompiledPipeline,Pipeline
from sgfmill.common import format_vertex,move_from_vertex
from sortedcontainers.sortedlist import SortedList

# Process-wide JSONEncoder.default extension. Pre-v1.0.6 the np and math
# references in the body had no corresponding import; the monkeypatched
# default would NameError on any numpy scalar or NaN reaching json.dumps
# (audit L-2). Fixed by the imports above. This patch is duplicated by
# proxy_server.py with the same body; whichever loads last wins. The
# duplication is itself a future-cleanup smell.

original_default = json.JSONEncoder.default

def global_extended_encoder(self, obj):
    # 1. Handle SortedList
    if isinstance(obj, SortedList):
        return list(obj)

    # 2. Handle NumPy scalars (like np.float64, np.int64)
    if isinstance(obj, (np.floating, np.integer)):
        if isinstance(obj, np.floating) and np.isnan(obj):
            return None
        return obj.item() # Converts np types to standard Python types

    # 3. Handle standard Python NaN
    if isinstance(obj, float) and math.isnan(obj):
        return None

    return original_default(self, obj)

# Apply the patch globally
json.JSONEncoder.default = global_extended_encoder

import logging
logger = logging.getLogger("kataproxy." + __name__)


class BadukAnalysisState:
    """
    Reactive manager for two Triangular matrices (one per color) driven by a
    stream of KataGo analysis packets arriving at arbitrary move indices.

    Architecture (built from existing Pipeline components)
    -------------------------------------------------------

                         [move packets]
                              │
              ┌───────────────┼────────────────────────────┐
              │               │                            │
              │   ┌─── Window(-1,0) ───┐         Map(preprocessor)
              │   │   Map(delta_fn)    │         Map(combined_state_fn)
              │   │  (root delta pipe) │         (state_pipe — unwindowed)
              │   └────────┬───────────┘                   │
              │            │ delta updates (broadcast)     │ state updates
              │  ┌─────────┴──────────┐                    ▼
              │  ▼                    ▼             result["state"]
              │ SubStream(B)    SubStream(W)
              │ Triangular()    Triangular()
              │ Map(summary_fn) Map(summary_fn)
              │ (black branch)  (white branch)
              │
              │ (also per color, fed from local delta indices)
              │
              ├── Pipeline(n_black_deltas)         Pipeline(n_white_deltas)
              │   .Global()                        .Global()
              │   .Monitor(cwt_throttle_ms)        .Monitor(cwt_throttle_ms)
              │   .Map(combined_cwt_fn)            .Map(combined_cwt_fn)
              │   (black_cwt_pipe)                 (white_cwt_pipe)
              └─────────────────────────────────────────────┘

    Move-to-color assignment (black_first=True, 0-indexed moves):
      Move 0 = Black, Move 1 = White, Move 2 = Black, ...
      delta[j] = f(packet[j-1], packet[j]) — caused by move j-1.
      delta[1] ← Black, delta[2] ← White, delta[3] ← Black, …

    Parameters
    ----------
    board_size : int
        Board dimensions (for move-coordinate lookup).
    moves : list
        Full move list for the game.
    delta_fn : Callable[[list], Any]
        Extracts a scalar (or any value) from [packet_{j-1}, packet_j].
        Called with a two-element list as produced by Window(-1, 0).
    summary_fn : Callable[[list], Any]
        Aggregates a list of deltas across an interval [s, t].
        Defaults to arithmetic mean.
    black_first : bool
        If True (default), move 0 is Black.
    triangular : bool
        Build the multi-resolution triangular analysis matrices (default True).
    state_fns : dict, optional
        Mapping of ``{name: fn}`` for *unwindowed* per-turn state analyses
        (e.g. entropy, spread).  Each ``fn(preprocessed_packet) -> value``
        runs on the raw packet *before* the Window(-1,0) delta step and is
        therefore available from the very first turn, without needing a
        preceding packet for comparison.  Results appear in
        ``push_packet()[\"state\"][move_idx]`` as ``{name: value}``.
    cwt_fns : dict, optional
        Mapping of ``{name: fn}`` for reduction-shaped analyses applied to
        the full per-color delta sequence once *every* turn has been analysed.
        Each ``fn(list_of_deltas) -> value`` receives the complete ordered
        delta list for one player.  The pipeline uses ``Global().Monitor()``
        so the computation only fires when all delta slots are filled, and
        subsequent recomputations are throttled to at most once per
        ``cwt_throttle_ms`` ms (the very first computation is never delayed).
        Results appear in ``push_packet()[\"black\"][\"cwt\"]`` and
        ``push_packet()[\"white\"][\"cwt\"]`` as ``{name: value}`` dicts.
    cwt_throttle_ms : float
        Throttle window (milliseconds) for the CWT Monitor gate.  Default 500.

    Usage
    -----
    state = BadukAnalysisState(
        board_size=19,
        moves=move_list,
        delta_fn=my_delta,
        summary_fn=my_summary,
        state_fns={"entropy": entropy_fn, "spread": spread_fn},
        cwt_fns={"score_cwt": cwt_fn},
        cwt_throttle_ms=500,
    )

    # As each KataGo response arrives:
    changes = state.push_packet(move_index, katago_packet)
    # changes == {
    #   "black": {
    #     "triangular": {(s, t): new_val, ...},
    #     "deltas":     {local_idx: val, ...},
    #     "cwt":        {"score_cwt": array, ...},   # non-empty when gate open
    #   },
    #   "white": { ... same structure ... },
    #   "state": {move_idx: {"entropy": v, "spread": v}},  # if state_fns given
    # }

    # Full snapshots:
    black_heatmap  = state.black_matrix    # {(s, t): value}
    white_heatmap  = state.white_matrix    # {(s, t): value}
    state_snapshot = state.state_snapshot  # {move_idx: {name: value}}
    """

    def __init__(
        self,
        board_size: int,
        moves: List[Any],
        delta_fn: Callable[[List[Any]], Any],
        summary_fn: Optional[Callable[[List[Any]], Any]] = None,
        *,
        black_first: bool = True,
        triangular: bool = True,
        state_fns: Optional[Dict[str, Callable[[Any], Any]]] = None,
        cwt_fns: Optional[Dict[str, Callable[[List[Any]], Any]]] = None,
        cwt_throttle_ms: float = 500.0,
    ) -> None:
        n_moves = len(moves)
        
        self._board_size = board_size
        self._moves = [ (x[0], x[1]) for x in moves]
        #self._moves = [ (x[0], ast.literal_eval(x[1])) for x in moves]

        self._triangular_enabled = triangular
        if n_moves < 2:
            raise ValueError("n_moves must be >= 2 to form at least one delta")

        if summary_fn is None:
            summary_fn = lambda vals: sum(vals) / len(vals)

        self._n_moves = n_moves
        self._summary_fn = summary_fn

        def preprocessor(packet_input: Any) -> Any:
            turn, opaque = packet_input
            if turn is not None and 0 <= turn < len(self._moves):
                target_coord = self._moves[turn][1]
                logger.debug(f"	preprocessor: target_coord = {target_coord}")
                opaque['userMove'] = target_coord
                move_infos = opaque.get('moveInfos', [])
                opaque['userMoveInfo'] = next(
                    (info for info in move_infos
                     if info.get('move') == target_coord),
                    None
                )
            else:
                opaque['userMoveInfo'] = None
            return opaque

        # ------------------------------------------------------------------
        # Shared root: Window(-1, 0) produces delta[j] from packets[j-1..j].
        # ------------------------------------------------------------------
        self._root: CompiledPipeline = (
            Pipeline(n_moves + 1)
            .Map(preprocessor)
            .Window(-1, 0)
            .Map(delta_fn)
            .compile()
        )

        # ------------------------------------------------------------------
        # Unwindowed state analysis branch.
        #
        # Lives entirely outside Window(-1, 0): state_fns receive the
        # preprocessed packet directly, so a value is available from turn 0
        # without needing a preceding packet.  Each fn is independent; they
        # are combined into a single Map to avoid redundant pipeline nodes.
        # ------------------------------------------------------------------

        if state_fns:
            _state_fns_frozen = dict(state_fns)  # snapshot at construction time
            _combined_state = lambda pkt: {
                name: fn(pkt) for name, fn in _state_fns_frozen.items()
            }
            self._state_pipe: Optional[CompiledPipeline] = (
                Pipeline(n_moves+1)
                .Map(preprocessor)
                .Map(_combined_state)
                .compile()
            )
        else:
            self._state_pipe = None

        # ------------------------------------------------------------------
        # Color index assignment.
        # delta[j] is caused by the play at move j-1:
        #   j-1 even → Black (if black_first) → j odd
        #   j-1 odd  → White (if black_first) → j even (j > 0)
        # ------------------------------------------------------------------
        if black_first:
            black_delta_indices = list(range(1, n_moves + 1, 2))   # j=1,3,5,…
            white_delta_indices = list(range(2, n_moves + 1, 2))   # j=2,4,6,…
        else:
            white_delta_indices = list(range(1, n_moves + 1, 2))
            black_delta_indices = list(range(2, n_moves + 1, 2))

        self._black_delta_indices = black_delta_indices
        self._white_delta_indices = white_delta_indices
        self._black_to_local = {glob: loc for loc, glob in enumerate(black_delta_indices)}
        self._white_to_local = {glob: loc for loc, glob in enumerate(white_delta_indices)}

        # ------------------------------------------------------------------
        # Color branches: SubStream → (optional Triangular) → Map(summary_fn)
        # ------------------------------------------------------------------
        self.black_pipe: CompiledPipeline = self._make_branch(
            n_moves + 1, black_delta_indices, summary_fn, triangular
        )
        self.white_pipe: CompiledPipeline = self._make_branch(
            n_moves + 1, white_delta_indices, summary_fn, triangular
        )

        # ------------------------------------------------------------------
        # CWT reduction branches (one per color).
        #
        # Global() topology: output j=0 depends on every input slot; it only
        # fires once all N delta values are present (MissingData suppresses
        # intermediate firings automatically).  This naturally enforces the
        # "every turn must be analysed" precondition without any explicit
        # bookkeeping.
        #
        # Monitor(cwt_throttle_ms) gates recomputations: the very first
        # emission is immediate; subsequent ones are suppressed until the
        # throttle window has elapsed, with all intermediate inputs buffered
        # so nothing is lost.
        # ------------------------------------------------------------------
        if cwt_fns:
            _cwt_fns_frozen = dict(cwt_fns)
            _combined_cwt = lambda deltas: {
                name: fn(deltas) for name, fn in _cwt_fns_frozen.items()
            }
            self._black_cwt_pipe: Optional[CompiledPipeline] = (
                Pipeline(len(black_delta_indices))
                .Global()
                .Monitor(cwt_throttle_ms)
                .Map(_combined_cwt)
                .compile()
            )
            self._white_cwt_pipe: Optional[CompiledPipeline] = (
                Pipeline(len(white_delta_indices))
                .Global()
                .Monitor(cwt_throttle_ms)
                .Map(_combined_cwt)
                .compile()
            )
        else:
            self._black_cwt_pipe = None
            self._white_cwt_pipe = None

    @staticmethod
    def _make_branch(
        root_domain_size: int,
        color_indices: List[int],
        summary_fn: Callable,
        include_triangular: bool
    ) -> CompiledPipeline:
        """
        Build a compiled branch pipeline that:
          1. Listens on the full root domain (size root_domain_size)
          2. Filters to color_indices via SubStream
          3. Computes a triangular summary matrix
        """
        pipe = Pipeline(root_domain_size).SubStream(color_indices)
        if include_triangular:
            pipe = pipe.Triangular().Map(summary_fn)
        return pipe.compile()

    # ------------------------------------------------------------------
    # Primary reactive interface
    # ------------------------------------------------------------------

    def push_packet(self, move_idx: int, packet: Any) -> Dict[str, Any]:
        """
        Ingest one KataGo packet and return the set of values that changed.

        Return structure
        ----------------
        {
          "black": {
            "triangular": {(s, t): new_val, ...},   # changed Triangular cells
            "deltas":     {local_idx: val, ...},     # changed per-color deltas
            "cwt":        {name: result, ...},       # non-empty when Monitor gate open
          },
          "white": { ... same keys ... },
          "state": {move_idx: {name: value, ...}},   # unwindowed; present if state_fns
        }

        All sub-dicts are empty when nothing changed for that analysis branch,
        preserving the invariant that only genuinely new information is returned.
        """
        delta_updates = self._root.updateAt(move_idx, packet)

        result: Dict[str, Any] = {
            "black": {"triangular": SortedList(), "deltas": {}, "cwt": {}},
            "white": {"triangular": SortedList(), "deltas": {}, "cwt": {}},
            "state": {},
        }

        # ------------------------------------------------------------------
        # 1. Unwindowed state analysis (pointwise, outside Window(-1, 0)).
        #    Runs on the raw packet so results are available from turn 0.
        # ------------------------------------------------------------------
        if self._state_pipe is not None:
            for _, state_val in self._state_pipe.updateAt(move_idx, packet):
                result["state"][move_idx] = state_val

        # ------------------------------------------------------------------
        # 2. Delta-based analyses: triangular matrices and CWT.
        #    Both are driven by the delta stream produced by _root.
        # ------------------------------------------------------------------
        for delta_idx, delta_val in delta_updates:
            for color in ("black", "white"):
                branch   = self.black_pipe       if color == "black" else self.white_pipe
                mapping  = self._black_to_local  if color == "black" else self._white_to_local
                cwt_pipe = self._black_cwt_pipe  if color == "black" else self._white_cwt_pipe

                # 2a. Flat per-color delta.
                if delta_idx in mapping:
                    local_idx = mapping[delta_idx]
                    result[color]["deltas"][local_idx] = delta_val

                    # 2b. CWT reduction (Global -> Monitor -> Map).
                    #     updateAt returns a non-empty list only when the
                    #     Monitor gate is open; otherwise the update is buffered
                    #     inside the MonitoredNode and nothing is emitted here.
                    if cwt_pipe is not None:
                        for _, cwt_val in cwt_pipe.updateAt(local_idx, delta_val):
                            result[color]["cwt"] = cwt_val

                # 2c. Multi-resolution triangular matrix.
                if self._triangular_enabled:
                    tri_updates = branch.updateAt(delta_idx, delta_val)
                    for tri_k, new_val in tri_updates:
                        s, t = branch.interval_of(tri_k)
                        result[color]["triangular"].add(((s,t), new_val))
                        #result[color]["triangular"][(s, t)] = new_val
                else:
                    branch.updateAt(delta_idx, delta_val)

        return result

    # ------------------------------------------------------------------
    # Unwindowed state snapshots
    # ------------------------------------------------------------------

    @property
    def state_snapshot(self) -> Dict[int, Any]:
        """
        Full ``{move_idx: {name: value}}`` snapshot of every unwindowed state
        value computed so far.  Returns an empty dict if no ``state_fns`` were
        provided.
        """
        if self._state_pipe is None:
            return {}
        return dict(self._state_pipe.nodes[-1].out_mem)

    # ------------------------------------------------------------------
    # CWT snapshots
    # ------------------------------------------------------------------

    @property
    def black_cwt_snapshot(self) -> Dict[str, Any]:
        """
        Latest CWT result for Black (``{name: value}``), or ``{}`` if no
        ``cwt_fns`` were provided or the full delta sequence has not yet been
        seen.
        """
        if self._black_cwt_pipe is None:
            return {}
        return self._black_cwt_pipe.nodes[-1].out_mem.get(0, {})

    @property
    def white_cwt_snapshot(self) -> Dict[str, Any]:
        """
        Latest CWT result for White (``{name: value}``), or ``{}`` if no
        ``cwt_fns`` were provided or the full delta sequence has not yet been
        seen.
        """
        if self._white_cwt_pipe is None:
            return {}
        return self._white_cwt_pipe.nodes[-1].out_mem.get(0, {})

    # ------------------------------------------------------------------
    # Per-color delta lists
    # ------------------------------------------------------------------

    @property
    def black_deltas(self) -> List[Any]:
        """Ordered list of all current deltas for Black."""
        # node[0] is the output of SubStream
        mem = self.black_pipe.nodes[0].out_mem
        return [mem[k] for k in range(len(mem))]

    @property
    def white_deltas(self) -> List[Any]:
        """Ordered list of all current deltas for White."""
        mem = self.white_pipe.nodes[0].out_mem
        return [mem[k] for k in range(len(mem))]

    # ------------------------------------------------------------------
    # Full matrix snapshots (for initial hydration of heatmaps)
    # ------------------------------------------------------------------

    def _snapshot(self, branch: CompiledPipeline) -> Dict[Tuple[int, int], Any]:
        node = branch.nodes[-1]
        return {branch.interval_of(k): v for k, v in node.out_mem.items()}

    @property
    def black_matrix(self) -> Dict[Tuple[int, int], Any]:
        """Full {(s, t): value} snapshot of the Black triangular matrix."""
        return self._snapshot(self.black_pipe)

    @property
    def white_matrix(self) -> Dict[Tuple[int, int], Any]:
        """Full {(s, t): value} snapshot of the White triangular matrix."""
        return self._snapshot(self.white_pipe)
