# SPDX-License-Identifier: MIT
from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

import transformers.stencil_cover_enricher as stencil
from katago import AnalyzeResponse, KataGoAction, KataGoQuery
from AbstractProxy.proxy_core import ClientId


def test_family_parser_loads_proxy_owned_rectangles() -> None:
    family = stencil._parse_stencil_family("name: dot\n#\n\nname: domino\n##\n")
    assert family == (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )


def test_family_parser_rejects_disconnected_shapes() -> None:
    with pytest.raises(ValueError, match="4-connected"):
        stencil._parse_stencil_family("name: split\n#.#\n")


def test_zero_threshold_splits_signs_and_leaves_neutral_points_unowned() -> None:
    positive, negative = stencil._ownership_regions(
        [0.8, 0.0, -0.7, 0.4],
        [None, None, None, None],
        width=2,
        height=2,
        threshold=0.0,
    )
    assert positive == [0]
    assert negative == [2]


def test_threshold_one_keeps_only_fully_owned_empty_points() -> None:
    positive, negative = stencil._ownership_regions(
        [1.0, 0.999, -1.0, -0.999],
        [None, None, None, None],
        width=2,
        height=2,
        threshold=1.0,
    )
    assert positive == [0]
    assert negative == [2]


def test_coordinate_parser_handles_numeric_and_rectangular_coordinates() -> None:
    assert stencil._coordinate_index("(2, 0)", 3, 2, allow_pass=False) == 2
    assert stencil._coordinate_index("C2", 3, 2, allow_pass=False) == 2
    assert stencil._coordinate_index("A1", 3, 2, allow_pass=False) == 3


def test_dead_chains_join_opponent_territory_and_living_stones_are_excluded() -> None:
    # The black chain at 0-1 and white chain at 3-4 fail KataGo's chain-level
    # liveness heuristic. The black stone at 6 is alive and stays out.
    board = ["B", "B", None, "W", "W", None, "B", None]
    ownership = [0.3, 0.0, 0.95, -0.95, -0.5, -0.9, -0.95, -0.9]

    positive, negative = stencil._ownership_regions(
        ownership, board, width=4, height=2, threshold=0.8,
    )

    # Dead stones are included even though their individual ownership values
    # do not cross 0.8; empty points still use the configured cutoff.
    assert positive == [0, 1, 2]
    assert negative == [3, 4, 5, 7]


def test_board_replay_stops_at_the_requested_turn_and_applies_captures() -> None:
    context = stencil.RequestContext(
        width=3,
        height=3,
        threshold=0.6,
        initial_stones=(
            ("W", stencil._coordinate_index("B2", 3, 3, allow_pass=False)),
            ("B", stencil._coordinate_index("B3", 3, 3, allow_pass=False)),
        ),
        moves=(
            ("B", stencil._coordinate_index("A2", 3, 3, allow_pass=True)),
            ("W", None),
            ("B", stencil._coordinate_index("C2", 3, 3, allow_pass=True)),
            ("W", None),
            ("B", stencil._coordinate_index("B1", 3, 3, allow_pass=True)),
        ),
    )

    before_capture = stencil._board_at_turn(context, 4)
    after_capture = stencil._board_at_turn(context, 5)
    assert before_capture[4] == "W"
    assert after_capture[4] is None
    assert after_capture[1] == "B"


def test_candidates_include_singletons_for_every_region_point() -> None:
    family = (("domino", ((0, 0), (0, 1))),)
    candidates = stencil._candidate_placements([0, 1, 2], 3, 1, family)
    candidate_cells = {points for _, points in candidates}
    assert (0,) in candidate_cells
    assert (1,) in candidate_cells
    assert (2,) in candidate_cells
    assert (0, 1) in candidate_cells
    assert (1, 2) in candidate_cells



def test_components_use_four_connectivity_not_diagonal_connectivity() -> None:
    components = stencil._connected_components(
        [0, 1, 7, 8], width=3, height=3,
    )
    assert components == [(0, 1), (7, 8)]


def test_component_shape_key_is_translation_normalized() -> None:
    first, first_frame = stencil._component_shape_key((0, 1, 3, 4), board_width=3)
    translated, translated_frame = stencil._component_shape_key(
        (7, 8, 10, 11), board_width=3,
    )
    assert first == translated
    assert (
        first_frame.transform,
        first_frame.offset_y,
        first_frame.offset_x,
        first_frame.canonical_width,
        first_frame.canonical_height,
    ) == (0, 0, 0, 2, 2)
    assert (
        translated_frame.transform,
        translated_frame.offset_y,
        translated_frame.offset_x,
        translated_frame.canonical_width,
        translated_frame.canonical_height,
    ) == (0, 2, 1, 2, 2)


def test_rectangle_family_has_the_full_d4_stabilizer() -> None:
    assert stencil._family_dihedral_group(stencil.STENCIL_FAMILY) == tuple(range(8))


def test_component_shape_key_merges_all_eight_dihedral_images() -> None:
    shape = ((0, 0), (1, 0), (1, 1))
    keys: List[bytes] = []
    for transform in range(8):
        mapped = [
            stencil._transform_cell(y, x, transform)
            for y, x in shape
        ]
        min_y = min(y for y, _ in mapped)
        min_x = min(x for _, x in mapped)
        # Embed each orientation at a positive board offset, independently
        # of its rotation/reflection and bounding-box dimensions.
        points = tuple(
            (y - min_y + 4) * 16 + (x - min_x + 4)
            for y, x in mapped
        )
        key, _frame = stencil._component_shape_key(points, board_width=16)
        keys.append(key)
    assert len(set(keys)) == 1


def test_family_symmetry_is_restricted_to_maps_that_preserve_its_shapes() -> None:
    horizontal_only = (
        ("dot", ((0, 0),)),
        ("line", ((0, 0), (0, 1), (0, 2))),
    )
    assert stencil._family_dihedral_group(horizontal_only) == (0, 2, 4, 5)


def test_canonical_solution_is_recovered_with_rotated_stencil_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = stencil.STENCIL_FAMILY
    seen_keys: List[bytes] = []

    def fake_cached(
        shape_key: bytes, time_limit_ms: int, family_key: Any,
    ) -> tuple[bool, int, bytes]:
        seen_keys.append(shape_key)
        local_width, local_height, cells = stencil._decode_component_shape_key(shape_key)
        candidates = stencil._candidate_placements(
            cells, local_width, local_height, family_key,
        )
        complete = next(
            candidate for candidate in candidates
            if len(candidate[1]) == len(cells) and set(candidate[1]) == set(cells)
        )
        return True, 0, stencil._encode_cached_placements([complete], family_key)

    monkeypatch.setattr(stencil, "_solve_component_cached", fake_cached)

    horizontal = stencil._solve_region([0, 1], width=2, height=1, family=family)
    vertical = stencil._solve_region([0, 1], width=1, height=2, family=family)

    assert seen_keys[0] == seen_keys[1]
    assert horizontal["placements"] == [
        {"stencil": "1x2", "points": [[0, 0], [1, 0]]},
    ]
    assert vertical["placements"] == [
        {"stencil": "2x1", "points": [[0, 0], [0, 1]]},
    ]


def test_solution_recovery_is_equivariant_for_all_d4_images(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_shape = ((0, 0), (1, 0), (1, 1))
    unique_shapes: List[tuple[tuple[int, int], ...]] = []
    for transform in range(8):
        candidate = stencil._normalise_shape(base_shape, transform)
        if candidate not in unique_shapes:
            unique_shapes.append(candidate)
    family = (
        ("dot", ((0, 0),)),
        *((f"L{index}", shape) for index, shape in enumerate(unique_shapes)),
    )
    assert stencil._family_dihedral_group(family) == tuple(range(8))

    seen_keys: List[bytes] = []

    def fake_cached(
        shape_key: bytes, time_limit_ms: int, family_key: Any,
    ) -> tuple[bool, int, bytes]:
        seen_keys.append(shape_key)
        local_width, local_height, cells = stencil._decode_component_shape_key(shape_key)
        candidates = stencil._candidate_placements(
            cells, local_width, local_height, family_key,
        )
        complete = next(
            candidate for candidate in candidates
            if len(candidate[1]) == len(cells) and set(candidate[1]) == set(cells)
        )
        return True, 0, stencil._encode_cached_placements([complete], family_key)

    monkeypatch.setattr(stencil, "_solve_component_cached", fake_cached)

    recovered: List[List[Dict[str, Any]]] = []
    for transform in range(8):
        mapped = [
            stencil._transform_cell(y, x, transform)
            for y, x in base_shape
        ]
        min_y = min(y for y, _ in mapped)
        min_x = min(x for _, x in mapped)
        component = tuple(
            (y - min_y + 3) * 10 + (x - min_x + 3)
            for y, x in mapped
        )
        result = stencil._solve_region(
            component, width=10, height=10, family=family,
        )
        recovered.append(result["placements"])

        expected_points = sorted(
            [[point % 10, point // 10] for point in component],
            key=lambda point: (point[1], point[0]),
        )
        assert result["placements"][0]["points"] == expected_points
        assert result["optimal"] is True

    assert len(seen_keys) == 8
    assert len(set(seen_keys)) == 1
    # All eight callers receive a geometrically valid placement in their
    # own frame; the verification inside _solve_region checks the name too.
    assert len(recovered) == 8


def test_disconnected_region_is_solved_as_independent_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = (("1x1", ((0, 0),)),)
    seen_components: List[tuple[int, ...]] = []

    def fake_cached(
        shape_key: bytes, time_limit_ms: int, family_key: Any,
    ) -> tuple[bool, int, bytes]:
        local_width, local_height, cells = stencil._decode_component_shape_key(shape_key)
        # The cache/solver boundary must never receive a disconnected mask.
        assert stencil._connected_components(
            cells, local_width, local_height,
        ) == [tuple(cells)]
        seen_components.append(tuple(cells))
        singleton_cover = [("1x1", (point,)) for point in cells]
        return (
            True,
            0,
            stencil._encode_cached_placements(singleton_cover, family_key),
        )

    monkeypatch.setattr(stencil, "_solve_component_cached", fake_cached)
    result = stencil._solve_region(
        [0, 1, 8], width=3, height=3, family=family,
    )
    assert seen_components == [(0, 1), (0,)]
    assert result["optimal"] is True
    assert result["status"] == "optimal"
    assert result["placements"] == [
        {"stencil": "1x1", "points": [[0, 0]]},
        {"stencil": "1x1", "points": [[1, 0]]},
        {"stencil": "1x1", "points": [[2, 2]]},
    ]
    assert result["components"] == [
        {
            "cells": [[0, 0], [1, 0]],
            "tileCount": 2,
            "optimal": True,
            "status": "optimal",
        },
        {
            "cells": [[2, 2]],
            "tileCount": 1,
            "optimal": True,
            "status": "optimal",
        },
    ]



def test_raw_components_retain_the_cells_and_status_of_each_solver_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = (("1x1", ((0, 0),)),)
    returned_statuses = iter([(False, 1), (False, 2)])

    def fake_cached(
        shape_key: bytes, time_limit_ms: int, family_key: Any,
    ) -> tuple[bool, int, bytes]:
        _optimal, status = next(returned_statuses)
        _width, _height, cells = stencil._decode_component_shape_key(shape_key)
        singleton_cover = [("1x1", (point,)) for point in cells]
        return (
            False,
            status,
            stencil._encode_cached_placements(singleton_cover, family_key),
        )

    monkeypatch.setattr(stencil, "_solve_component_cached", fake_cached)
    result = stencil._solve_region([0, 1, 8], 3, 3, family)

    assert result["status"] == "fallback"
    assert result["optimal"] is False
    assert result["components"] == [
        {
            "cells": [[0, 0], [1, 0]],
            "tileCount": 2,
            "optimal": False,
            "status": "feasible",
        },
        {
            "cells": [[2, 2]],
            "tileCount": 1,
            "optimal": False,
            "status": "fallback",
        },
    ]


def test_empty_region_exposes_an_empty_raw_component_list() -> None:
    result = stencil._solve_region([], 3, 3, (("1x1", ((0, 0),)),))
    assert result == {
        "components": [],
        "placements": [],
        "tileCount": 0,
        "optimal": True,
        "status": "empty",
    }


def test_component_solver_cache_is_large_and_entry_bounded() -> None:
    info = stencil._solve_component_cached.cache_info()
    assert info.maxsize == 90_000

def _mock_link() -> Any:
    return SimpleNamespace(
        mapping=SimpleNamespace(forward=lambda _eid: "internal-id"),
    )


def _query(
    capabilities: Dict[str, Any],
    *,
    initial_stones: List[List[str]] | None = None,
    moves: List[List[str]] | None = None,
) -> KataGoQuery:
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        opaque={
            "boardXSize": 2,
            "boardYSize": 2,
            "initialStones": initial_stones or [],
            "moves": moves or [],
            "capabilities": capabilities,
        },
    )


def _response(*, during_search: bool = False) -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=during_search,
        turn_number=0,
        opaque={
            "ownership": [0.8, 0.7, -0.8, -0.7],
        },
    )


def test_transformer_requires_explicit_stencil_cover_opt_in() -> None:
    transformer = stencil.stencil_cover_enricher(_mock_link())
    query = _query({})
    assert transformer.on_query(ClientId("q1"), query) is query
    response = _response()
    assert transformer.on_response(ClientId("q1"), response) is response
    assert "stencilCover" not in response.opaque


def test_transformer_enriches_only_final_response_and_keeps_signs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[List[int]] = []

    def fake_solve(
        cells: List[int], width: int, height: int, family: Any,
    ) -> Dict[str, Any]:
        calls.append(list(cells))
        components = [
            {
                "cells": [[point % width, point // width] for point in component],
                "tileCount": 1,
                "optimal": True,
                "status": "optimal",
            }
            for component in stencil._connected_components(cells, width, height)
        ]
        return {
            "components": components,
            "placements": [],
            "tileCount": 0,
            "optimal": True,
            "status": "optimal",
        }

    monkeypatch.setattr(stencil, "_solve_region", fake_solve)
    transformer = stencil.stencil_cover_enricher(_mock_link())
    query = _query({
        "stencil_cover": {"truncation_threshold": 0.5},
    })
    transformer.on_query(ClientId("q2"), query)

    partial = _response(during_search=True)
    transformer.on_response(ClientId("q2"), partial)
    assert "stencilCover" not in partial.opaque
    assert calls == []

    final = _response()
    out = transformer.on_response(ClientId("q2"), final)
    assert out is final
    assert calls == [[0, 1], [2, 3]]
    annotation = final.opaque["stencilCover"]
    assert annotation["version"] == 2
    assert annotation["threshold"] == 0.5
    assert annotation["positive"]["components"] == [
        {
            "cells": [[0, 0], [1, 0]],
            "tileCount": 1,
            "optimal": True,
            "status": "optimal",
        },
    ]
    assert annotation["positive"]["optimal"] is True
    assert annotation["negative"]["optimal"] is True
    assert annotation["optimal"] is True


def test_transformer_does_not_need_policy_map_for_territory() -> None:
    transformer = stencil.stencil_cover_enricher(_mock_link())
    transformer.on_query(
        ClientId("q3"),
        _query({"stencil_cover": {"truncation_threshold": 0.5}}),
    )
    response = AnalyzeResponse(
        is_during_search=False,
        turn_number=0,
        opaque={"ownership": [0.8, 0.7, -0.8, -0.7]},
    )
    transformer.on_response(ClientId("q3"), response)
    assert "stencilCover" in response.opaque


def test_highs_returns_a_valid_minimum_exact_cover_for_a_domino_region() -> None:
    family = (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )
    result = stencil._solve_region([0, 1], 2, 1, family)
    assert result["tileCount"] == 1
    assert result["optimal"] is True
    assert result["placements"] == [
        {"stencil": "1x2", "points": [[0, 0], [1, 0]]},
    ]


def test_redis_persistent_cache_round_trips_between_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import transformers.stencil_cache_storage as storage

    persisted: Dict[str, bytes] = {}

    class FakeRedis:
        def __init__(self, **kwargs: Any) -> None:
            assert kwargs["host"] == "127.0.0.1"
            assert kwargs["port"] == 6379
            assert kwargs["db"] == 7

        def ping(self) -> bool:
            return True

        def get(self, key: str) -> bytes | None:
            return persisted.get(key)

        def set(self, key: str, value: bytes) -> bool:
            persisted[key] = value
            return True

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=FakeRedis))
    calls: List[int] = []

    def solve(
        shape_key: bytes,
        time_limit_ms: int,
        family: Any,
    ) -> tuple[bool, int, bytes]:
        calls.append(1)
        return True, 0, b"optimal-solution"

    first_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)
    second_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)
    args = (b"canonical-shape", 250, (("dot", ((0, 0),)),))

    assert first_wrapper(*args) == (True, 0, b"optimal-solution")
    assert second_wrapper(*args) == (True, 0, b"optimal-solution")
    assert calls == [1]


def test_redis_persistent_cache_warns_and_degrades_to_noop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import transformers.stencil_cache_storage as storage

    class OfflineRedis:
        def __init__(self, **kwargs: Any) -> None:
            pass

        def ping(self) -> None:
            raise ConnectionError("Redis is offline")

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=OfflineRedis))
    calls: List[int] = []

    def solve(shape_key: bytes, time_limit_ms: int, family: Any) -> tuple[bool, int, bytes]:
        calls.append(1)
        return False, 2, b"fallback"

    with caplog.at_level("WARNING"):
        decorated = storage.redis_persistent_cache("offline-test")(solve)

    assert decorated(b"shape", 250, ()) == (False, 2, b"fallback")
    assert calls == [1]
    assert "persistent caching disabled" in caplog.text
