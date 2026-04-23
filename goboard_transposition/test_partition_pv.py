"""
Tests for the partition_pv transposition-clustering logic.

Geometry reminder (9x9, GTP coords, column 'I' is skipped):
  Columns A-H = 0-7, J = 8.
  Row n → array row (size - n).
  So "A1" is bottom-left, "J9" is top-right.

All tests use a 9x9 board for speed; the logic is size-independent.
"""

import json
import pytest

go_transposition = pytest.importorskip(
    "go_transposition",
    reason=(
        "go_transposition extension not compiled — "
        "run: meson compile -C builddir"
    ),
)


# ── helpers ────────────────────────────────────────────────────────────────────


def make_req(
    size=9,
    initial_stones=None,
    initial_player="B",
    moves=None,
):
    d = {
        "boardXSize": size,
        "boardYSize": size,
        "initialPlayer": initial_player,
    }
    if initial_stones:
        d["initialStones"] = initial_stones
    if moves:
        d["moves"] = moves
    return json.dumps(d)


def make_resp(turn_number, current_player, move_infos):
    return json.dumps(
        {
            "turnNumber": turn_number,
            "rootInfo": {"currentPlayer": current_player},
            "moveInfos": move_infos,
        }
    )


def run(req_str, resp_str):
    raw = go_transposition.partition_pv(req_str, resp_str)
    data = json.loads(raw)
    ids = [mi["clusterId"] for mi in data["moveInfos"]]
    print(f"[debug] cluster_ids = {ids}")
    return ids


def same(*ids_at):
    vals = list(ids_at)
    ok = len(set(vals)) == 1
    if not ok:
        print(f"[debug] expected same cluster, got: {vals}")
    return ok


def distinct(*ids_at):
    vals = list(ids_at)
    ok = len(set(vals)) == len(vals)
    if not ok:
        print(f"[debug] expected distinct clusters, got: {vals}")
    return ok


# ── tests ──────────────────────────────────────────────────────────────────────


def test_single_pv_gets_cluster_zero():
    """Baseline: one moveInfo always lands in cluster 0."""
    ids = run(
        make_req(),
        make_resp(0, "B", [{"pv": ["C3", "D4"]}]),
    )
    print(f"[debug] single pv: {ids}")
    assert ids == [0]


def test_disjoint_pvs_get_distinct_clusters():
    """
    Two PVs that visit completely different board regions share no
    intermediate state and must end up in different clusters.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["A1", "B2"]},
            {"pv": ["H9", "J8"]},
        ]),
    )
    print(f"[debug] disjoint pvs: {ids}")
    assert distinct(ids[0], ids[1])


def test_move_order_transposition():
    """
    Two PVs that place the same stones in different orders reach an
    identical board state and must be assigned the same cluster.

    PV0: B@C1, W@D1, B@E1  →  {B:C1,B:E1  W:D1} with W to move
    PV1: B@E1, W@D1, B@C1  →  identical after 3 moves
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["C1", "D1", "E1"]},
            {"pv": ["E1", "D1", "C1"]},
        ]),
    )
    print(f"[debug] move-order transposition: {ids}")
    assert same(ids[0], ids[1])


def test_shared_first_move_merges_at_step_one():
    """
    Two PVs whose first move is identical produce the same board state
    after that move, triggering a merge immediately at step 1 —
    even when they diverge completely from move 2 onward.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["A1", "B2", "C3"]},
            {"pv": ["A1", "H9", "J8"]},
        ]),
    )
    print(f"[debug] shared first move: {ids}")
    assert same(ids[0], ids[1])


def test_three_way_merge_via_transitivity():
    """
    Transitivity through the union-find: PV0 and PV1 share a final state
    so they merge.  Before that merge-and-break, PV1 recorded its own step-1
    state.  PV2 opens with the same first move as PV1, hits that recorded
    state, and cascades into PV0's component — all three end up in one cluster.

    PV0: B@C1, W@D1, B@E1        records all 3 states as index 0
    PV1: B@E1, W@D1, B@C1        step-1 {B:E1} recorded as index 1;
                                  step-3 matches PV0's final state → merge(1,0)
    PV2: B@E1, W@D1, B@F1        step-1 {B:E1} was recorded as index 1
                                  → merge(2, find(1)) = merge(2, 0)
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["C1", "D1", "E1"]},
            {"pv": ["E1", "D1", "C1"]},
            {"pv": ["E1", "D1", "F1"]},
        ]),
    )
    print(f"[debug] three-way transitive merge: {ids}")
    assert same(ids[0], ids[1], ids[2])


def test_two_independent_transposition_pairs():
    """
    When two disjoint pairs of PVs each form their own transposition,
    the result should be exactly two distinct cluster IDs — one per pair.

    Pair A (PV0, PV1): both open with B@A1 → merge immediately
    Pair B (PV2, PV3): both open with B@H9 → merge immediately
    Pairs A and B never touch each other's states.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["A1", "B2"]},
            {"pv": ["A1", "C3"]},
            {"pv": ["H9", "J8"]},
            {"pv": ["H9", "G8"]},
        ]),
    )
    print(f"[debug] two independent pairs: {ids}")
    assert same(ids[0], ids[1])
    assert same(ids[2], ids[3])
    assert distinct(ids[0], ids[2])


def test_missing_pv_key_gets_own_singleton_cluster():
    """
    A moveInfo with no "pv" key is skipped in the union-find loop and
    keeps its own singleton component, getting a cluster ID distinct from
    a normal moveInfo.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"move": "A1"},           # no "pv" key at all
            {"pv": ["C3", "D4"]},
        ]),
    )
    print(f"[debug] missing pv key: {ids}")
    assert distinct(ids[0], ids[1])


def test_empty_pv_gets_own_singleton_cluster():
    """
    A moveInfo with an empty pv list is similarly skipped and must
    not be merged with anything.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": []},
            {"pv": ["C3", "D4"]},
        ]),
    )
    print(f"[debug] empty pv: {ids}")
    assert distinct(ids[0], ids[1])


def test_pass_moves_in_pv():
    """
    Passes are legal in Go and must be handled without crashing.
    Two PVs whose move sequences differ only in order but include
    a pass should still detect the transposition.

    PV0: B@A1, W-pass, B@B1  →  {B:A1,B:B1} W-to-move
    PV1: B@B1, W-pass, B@A1  →  identical position
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["A1", "pass", "B1"]},
            {"pv": ["B1", "pass", "A1"]},
        ]),
    )
    print(f"[debug] pass in pv: {ids}")
    assert same(ids[0], ids[1])


def test_initial_stones_dont_break_transposition_detection():
    """
    Setup stones change the base Zobrist hash but the transposition
    logic must still work correctly over them.  A move-order transposition
    that is valid on an empty board is also valid when setup stones
    are present in an unrelated region.
    """
    ids = run(
        make_req(initial_stones=[["B", "A9"], ["W", "A8"]]),
        make_resp(0, "B", [
            {"pv": ["C1", "D1", "E1"]},
            {"pv": ["E1", "D1", "C1"]},
        ]),
    )
    print(f"[debug] initial stones present: {ids}")
    assert same(ids[0], ids[1])


def test_turn_number_replays_move_history():
    """
    A turnNumber > 0 causes the move history to be replayed into the
    base board before PV evaluation.  This test verifies that the
    replay doesn't crash and that transpositions within the PVs are
    still detected correctly from the resulting base position.
    """
    ids = run(
        make_req(moves=[["B", "A1"], ["W", "B2"]]),
        make_resp(2, "B", [
            {"pv": ["C3", "D4", "E5"]},
            {"pv": ["E5", "D4", "C3"]},  # same position by move order
        ]),
    )
    print(f"[debug] after replaying 2 moves: {ids}")
    assert same(ids[0], ids[1])


def test_all_cluster_ids_are_non_negative_integers():
    """
    Every moveInfo in the output must have a clusterId that is a
    non-negative integer, regardless of whether it has a PV or not.
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["C3", "D4"]},
            {"pv": []},
            {"move": "A1"},
            {"pv": ["H9", "J8"]},
        ]),
    )
    print(f"[debug] all cluster ids: {ids}")
    assert all(isinstance(x, int) and x >= 0 for x in ids), (
        f"unexpected cluster ids: {ids}"
    )


def test_cluster_ids_are_dense_from_zero():
    """
    Cluster IDs are assigned in order of first appearance of each
    canonical root, so they must form a dense range [0, n_clusters).
    """
    ids = run(
        make_req(),
        make_resp(0, "B", [
            {"pv": ["A1", "B2"]},   # cluster 0
            {"pv": ["H9", "J8"]},   # cluster 1
            {"pv": ["A1", "C3"]},   # merges into cluster 0
            {"pv": ["G7", "F6"]},   # cluster 2
        ]),
    )
    print(f"[debug] dense cluster ids: {ids}")
    n_clusters = len(set(ids))
    assert set(ids) == set(range(n_clusters)), (
        f"ids {ids} are not a dense range; n_clusters={n_clusters}"
    )
