"""
transposition_enricher.py — Optional response enricher for KataGo PVs.

When the native go_transposition module is available, this transformer
post-processes analysis responses to partition the principal-variation
move list, distinguishing transpositional moves (positions reachable by
multiple orderings) from unique continuations.

The native module is OPTIONAL. If it is not importable at startup, this
module emits a single warning and the transformer becomes a no-op pass-
through. The proxy operates normally without enrichment.

Security posture (post v1.0.3, audit finding H-1)
─────────────────────────────────────────────────
The C++ ``partition_pv`` accepts client-derived JSON via the request
(``boardXSize``, ``moves``, ``initialStones``) and the response
(``turnNumber``, ``moveInfos[i].pv``). Several historical paths in that
code reached UB on crafted inputs (empty coord strings → ``s[0]`` UB,
high-bit bytes → signed-char ``std::toupper`` UB, ``boardXSize: 0`` →
abort, ``boardXSize: 50000`` → integer overflow then giant allocation,
deep parent chains → recursive-find stack overflow).

The C++ side has been hardened independently inside
``goboard_transposition/`` (assert→throw, iterative find, parseCoords
empty-/non-ASCII guards). This module is the *primary* defense: a
shape-and-size validation pass on the request wire-dict before it
enters the cache, and on the response wire-dict before
``partition_pv`` is called. Refusal logs at WARNING and skips
enrichment for that query — the analysis still flows; only the
PV-partition annotation is suppressed. Connection survives.

Both layers are deliberate. The Python side is fast and protects
against the most common shapes a wire attacker would try; the C++
side is the backstop for anything that slips through (or anything
that reaches ``partition_pv`` from a path that doesn't go through
this transformer).

See README.md ("The goboard_transposition extension") for build and
distribution notes.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""
import logging
logger = logging.getLogger("kataproxy." + __name__)
try:
    import go_transposition as _go_transposition  # type: ignore[import]
    _TRANSPOSITION_AVAILABLE = True
except ImportError:
    _go_transposition = None  # type: ignore[assignment]
    _TRANSPOSITION_AVAILABLE = False
if not _TRANSPOSITION_AVAILABLE:
    logging.getLogger("kataproxy.transposition_enricher").warning(
        "go_transposition native module not found; "
        "transposition enrichment disabled. "
        "See README for build instructions."
    )
import json
from typing import Any, Optional, Dict
from AbstractProxy.protocol_transformer import Transformer
from AbstractProxy.proxy_core import ProxyLink
from AbstractProxy.katago_proxy import (
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    translate_query_to_wire,
    translate_response_to_wire,
    parse_response_from_wire
)


# ---------------------------------------------------------------------------
# Validation bounds
# ---------------------------------------------------------------------------
# Sized for KataGo's natural envelope plus generous headroom. Anything
# larger is either a wire attacker probing for memory amplification, or a
# legitimate-but-pathological config the operator would want surfaced
# rather than silently fed to the native code. The C++ side mirrors
# these bounds; the duplication is intentional defense-in-depth.

_MAX_BOARD_SIZE = 25
_MAX_MOVES_LIST = 1000
_MAX_INITIAL_STONES = 1000
_MAX_MOVE_INFOS = 1000
_MAX_PV_LENGTH = 1000
_MAX_COORD_LEN = 8
_VALID_PLAYERS = ("B", "W")


def _validate_coord(s: Any, location: str) -> None:
    """Validate that *s* is a non-empty short string the C++ parseCoords can
    safely read. The C++ side does the actual format parse; we only need to
    ensure ``s[0]`` is well-defined and the string isn't pathologically long."""
    if not isinstance(s, str):
        raise ValueError(
            f"{location}: expected string, got {type(s).__name__}"
        )
    if not s:
        raise ValueError(f"{location}: empty coord string")
    if len(s) > _MAX_COORD_LEN:
        raise ValueError(
            f"{location}: coord string length {len(s)} exceeds cap {_MAX_COORD_LEN}"
        )


def _validate_color_coord_pair(p: Any, location: str) -> None:
    """Validate a [color, coord] pair as appears in initialStones / moves."""
    if not isinstance(p, list) or len(p) != 2:
        raise ValueError(
            f"{location}: expected [color, coord] pair, got {type(p).__name__}"
            f" of length {len(p) if hasattr(p, '__len__') else '?'}"
        )
    color, coord = p
    if color not in _VALID_PLAYERS:
        raise ValueError(
            f"{location}[0]: color must be one of {_VALID_PLAYERS}, got {color!r}"
        )
    _validate_coord(coord, f"{location}[1]")


def _validate_partition_pv_request(wire_dict: Dict[str, Any]) -> None:
    """Refuse request payloads the C++ partition_pv cannot safely consume.

    Raises ValueError on any structural or size violation; on success the
    payload is shape-bounded and the C++ side will not encounter the
    historical UB / abort / amplification paths.
    """
    if not isinstance(wire_dict, dict):
        raise ValueError(f"request: expected dict, got {type(wire_dict).__name__}")

    bxs = wire_dict.get("boardXSize", 19)
    if isinstance(bxs, bool) or not isinstance(bxs, int):
        raise ValueError(
            f"request.boardXSize: expected int, got {type(bxs).__name__}"
        )
    if not 1 <= bxs <= _MAX_BOARD_SIZE:
        raise ValueError(
            f"request.boardXSize {bxs} out of range [1, {_MAX_BOARD_SIZE}]"
        )

    bys = wire_dict.get("boardYSize", bxs)
    if isinstance(bys, bool) or not isinstance(bys, int):
        raise ValueError(
            f"request.boardYSize: expected int, got {type(bys).__name__}"
        )
    if not 1 <= bys <= _MAX_BOARD_SIZE:
        raise ValueError(
            f"request.boardYSize {bys} out of range [1, {_MAX_BOARD_SIZE}]"
        )

    initial_stones = wire_dict.get("initialStones")
    if initial_stones is not None:
        if not isinstance(initial_stones, list):
            raise ValueError("request.initialStones: expected list")
        if len(initial_stones) > _MAX_INITIAL_STONES:
            raise ValueError(
                f"request.initialStones: length {len(initial_stones)} "
                f"exceeds cap {_MAX_INITIAL_STONES}"
            )
        for i, stone in enumerate(initial_stones):
            _validate_color_coord_pair(stone, f"request.initialStones[{i}]")

    initial_player = wire_dict.get("initialPlayer")
    if initial_player is not None and initial_player not in _VALID_PLAYERS:
        raise ValueError(
            f"request.initialPlayer: must be one of {_VALID_PLAYERS}, "
            f"got {initial_player!r}"
        )

    moves = wire_dict.get("moves")
    if moves is not None:
        if not isinstance(moves, list):
            raise ValueError("request.moves: expected list")
        if len(moves) > _MAX_MOVES_LIST:
            raise ValueError(
                f"request.moves: length {len(moves)} exceeds cap {_MAX_MOVES_LIST}"
            )
        for i, move in enumerate(moves):
            _validate_color_coord_pair(move, f"request.moves[{i}]")


def _validate_partition_pv_response(wire_dict: Dict[str, Any]) -> None:
    """Refuse response payloads the C++ partition_pv cannot safely consume."""
    if not isinstance(wire_dict, dict):
        raise ValueError(
            f"response: expected dict, got {type(wire_dict).__name__}"
        )

    turn_number = wire_dict.get("turnNumber", 0)
    if isinstance(turn_number, bool) or not isinstance(turn_number, int):
        raise ValueError(
            f"response.turnNumber: expected int, got {type(turn_number).__name__}"
        )
    if turn_number < 0:
        raise ValueError(f"response.turnNumber {turn_number} must be >= 0")

    root_info = wire_dict.get("rootInfo", {})
    if not isinstance(root_info, dict):
        raise ValueError("response.rootInfo: expected dict")
    current_player = root_info.get("currentPlayer")
    # currentPlayer is required by partition_pv (resp["rootInfo"]["currentPlayer"]
    # is dereferenced unconditionally); refuse if absent or malformed.
    if current_player not in _VALID_PLAYERS:
        raise ValueError(
            f"response.rootInfo.currentPlayer: must be one of {_VALID_PLAYERS}, "
            f"got {current_player!r}"
        )

    move_infos = wire_dict.get("moveInfos")
    if move_infos is not None:
        if not isinstance(move_infos, list):
            raise ValueError("response.moveInfos: expected list")
        if len(move_infos) > _MAX_MOVE_INFOS:
            raise ValueError(
                f"response.moveInfos: length {len(move_infos)} "
                f"exceeds cap {_MAX_MOVE_INFOS}"
            )
        for i, mi in enumerate(move_infos):
            if not isinstance(mi, dict):
                raise ValueError(
                    f"response.moveInfos[{i}]: expected dict, "
                    f"got {type(mi).__name__}"
                )
            pv = mi.get("pv")
            if pv is None:
                continue
            if not isinstance(pv, list):
                raise ValueError(f"response.moveInfos[{i}].pv: expected list")
            if len(pv) > _MAX_PV_LENGTH:
                raise ValueError(
                    f"response.moveInfos[{i}].pv: length {len(pv)} "
                    f"exceeds cap {_MAX_PV_LENGTH}"
                )
            for j, m in enumerate(pv):
                _validate_coord(m, f"response.moveInfos[{i}].pv[{j}]")


def transposition_enricher(link: ProxyLink) -> Transformer[KataGoQuery, KataGoResponse]:
    """
    Stateful transformer for PV partitioning.
    Monitors the ProxyLink to know when to purge the request cache.
    """
    request_cache: Dict[str, str] = {}

    def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
        # Guard: only cache for analysis
        if q.action == KataGoAction.ANALYZE:
            wire_dict = translate_query_to_wire(q, eid)
            try:
                _validate_partition_pv_request(wire_dict)
            except ValueError as e:
                # Refuse to cache; on_response will see no entry and skip
                # enrichment. The query itself still goes through to KataGo.
                logger.warning(
                    f"transposition: request for eid={eid!r} fails validation; "
                    f"skipping PV-partition enrichment: {e}"
                )
                return q
            request_cache[eid] = json.dumps(wire_dict)
        return q

    def on_response(eid: str, r: KataGoResponse) -> Optional[KataGoResponse]:
        # 1. Attempt enrichment
        req_json = request_cache.get(eid)
        if _TRANSPOSITION_AVAILABLE and req_json is not None and "moveInfos" in r.opaque:
            resp_wire = translate_response_to_wire(r, eid)
            try:
                _validate_partition_pv_response(resp_wire)
                enriched_json = _go_transposition.partition_pv(req_json, json.dumps(resp_wire))
                enriched_dict = json.loads(enriched_json)
                _, r = parse_response_from_wire(enriched_dict) # Update 'r'
            except ValueError as e:
                # Validation refusal — operator-visible WARNING, no enrichment.
                logger.warning(
                    f"transposition: response for eid={eid!r} fails validation; "
                    f"skipping PV-partition enrichment: {e}"
                )
            except Exception as e:
                # Native-side exception or unexpected failure — ERROR-level.
                # The C++ extension's own bounds checks will land here too,
                # backstopping anything the Python pre-validation missed.
                logger.error(f"transposition enrichment failed: {e}")

        # 2. THE MULTI-TURN CLEANUP:
        # Check if the ProxyLink just removed this ID mapping.
        # If link.mapping.forward(eid) is None, it means the CompletionTracker
        # signaled QUERY_COMPLETE and the Link has finalized the query.
        if link.mapping.forward(eid) is None:
            request_cache.pop(eid, None)

        return r

    return Transformer(
        name="go_transposition_pv",
        on_query=on_query,
        on_response=on_response
    )
