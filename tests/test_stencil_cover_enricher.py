# SPDX-License-Identifier: MIT
from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

import transformers.stencil_cover_enricher as stencil
from katago import AnalyzeResponse, KataGoAction, KataGoQuery
from AbstractProxy.proxy_core import ClientId


class _MemoryRedis:
    def __init__(self) -> None:
        self.values: Dict[str, bytes] = {}
        self.sets: Dict[str, set[bytes]] = {}
        self.hashes: Dict[str, Dict[bytes, bytes]] = {}
        self.markers: set[str] = set()

    def get(self, key: str) -> bytes | None:
        return self.values.get(key)

    def smembers(self, key: str) -> set[bytes]:
        return set(self.sets.get(key, set()))

    def hget(self, key: str, field: bytes) -> bytes | None:
        return self.hashes.get(key, {}).get(field)

    def publish(self, _channel: str, _message: bytes) -> int:
        return 1

    def eval(self, script: str, numkeys: int, *args: Any) -> int:
        if "SETNX" in script:
            assert numkeys == 4
            hard_key, pending_key, timeout_key, marker_key = args
            if marker_key in self.markers:
                return 0
            self.markers.add(marker_key)
            pending = self.sets.setdefault(pending_key, set())
            timed_out = self.hashes.get(timeout_key, {})
            queued = 0
            for shape_key in self.sets.get(hard_key, set()):
                if shape_key not in timed_out and shape_key not in pending:
                    pending.add(shape_key)
                    queued += 1
            return queued

        if "SISMEMBER" in script:
            assert numkeys == 4
            hard_key, pending_key, result_key, timeout_key, shape_key, metadata = args
            hard = self.sets.setdefault(hard_key, set())
            pending = self.sets.setdefault(pending_key, set())
            if shape_key not in hard or shape_key not in pending:
                return 0
            raw = self.values.get(result_key)
            if raw is None:
                return 0
            result = json.loads(raw.decode("utf-8"))
            if int(result["status"]) == 0:
                hard.discard(shape_key)
                self.hashes.setdefault(timeout_key, {}).pop(shape_key, None)
                return 0
            metadata_value = (
                json.loads(metadata)
                if isinstance(metadata, str)
                else json.loads(metadata.decode("utf-8"))
            )
            metadata_value["status"] = int(result["status"])
            metadata_value["tile_count"] = int(result["tile_count"])
            self.hashes.setdefault(timeout_key, {})[shape_key] = json.dumps(
                metadata_value, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
            pending.discard(shape_key)
            return 1

        assert numkeys == 4
        (
            result_key, hard_key, pending_key, timeout_key,
            payload, status, tile_count, _schema_version, shape_key,
        ) = args
        new_status = int(status)
        new_cost = int(tile_count)
        should_store = True
        old_raw = self.values.get(result_key)
        if old_raw is not None:
            old = json.loads(old_raw.decode("utf-8"))
            old_status = int(old["status"])
            old_cost = int(old["tile_count"])
            if old_status == 0 and new_status == 0 and old_cost != new_cost:
                return -1
            if new_status > old_status or (
                new_status == old_status and new_cost >= old_cost
            ):
                should_store = False

        if should_store:
            self.values[result_key] = payload
        final = json.loads(self.values[result_key].decode("utf-8"))
        hard = self.sets.setdefault(hard_key, set())
        pending = self.sets.setdefault(pending_key, set())
        timeouts = self.hashes.setdefault(timeout_key, {})
        if int(final["status"]) == 0:
            hard.discard(shape_key)
            pending.discard(shape_key)
            timeouts.pop(shape_key, None)
        else:
            hard.add(shape_key)
            if should_store:
                pending.add(shape_key)
                timeouts.pop(shape_key, None)
        return int(should_store)


class _FakeRedisClient:
    def __init__(self, backend: _MemoryRedis, **kwargs: Any) -> None:
        assert kwargs["host"] == "127.0.0.1"
        assert kwargs["port"] == 6379
        assert kwargs["db"] == 7
        self.backend = backend

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> bytes | None:
        return self.backend.get(key)

    def smembers(self, key: str) -> set[bytes]:
        return self.backend.smembers(key)

    def hget(self, key: str, field: bytes) -> bytes | None:
        return self.backend.hget(key, field)

    def eval(self, *args: Any) -> int:
        return self.backend.eval(*args)

    def publish(self, _channel: str, _message: bytes) -> int:
        return 1



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


def test_stencil_family_contains_complete_d4_orbits_of_s_and_l_pieces() -> None:
    prototypes = (
        ("S-tetromino", ((0, 1), (0, 2), (1, 0), (1, 1)), 4),
        ("L-tetromino", ((0, 0), (1, 0), (2, 0), (2, 1)), 8),
        ("L-tromino", ((0, 0), (1, 0), (1, 1)), 4),
    )
    for prefix, prototype, expected_orbit_size in prototypes:
        expected = {
            stencil._normalise_shape(prototype, transform)
            for transform in range(8)
        }
        actual = {
            cells
            for name, cells in stencil.STENCIL_FAMILY
            if name.startswith(prefix + "-")
        }
        assert len(expected) == expected_orbit_size
        assert actual == expected


def test_each_new_asymmetric_stencil_solves_as_one_exact_tile() -> None:
    new_names = (
        "S-tetromino-",
        "L-tetromino-",
        "L-tromino-",
    )
    new_stencils = [
        (name, cells)
        for name, cells in stencil.STENCIL_FAMILY
        if name.startswith(new_names)
    ]
    assert len(new_stencils) == 16

    for name, cells in new_stencils:
        width = max(x for _, x in cells) + 1
        height = max(y for y, _ in cells) + 1
        region = [y * width + x for y, x in cells]
        result = stencil._solve_region(
            region, width, height, stencil.STENCIL_FAMILY,
        )
        assert result["optimal"] is True
        assert result["tileCount"] == 1
        assert result["placements"] == [{
            "stencil": name,
            "points": [[x, y] for y, x in sorted(cells)],
        }]


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


def test_redis_persistent_cache_round_trips_best_result_across_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    monkeypatch.setitem(
        sys.modules,
        "redis",
        SimpleNamespace(
            Redis=lambda **kwargs: _FakeRedisClient(backend, **kwargs),
        ),
    )
    calls: List[int] = []

    def solve(
        shape_key: bytes,
        time_limit_ms: int,
        family: Any,
    ) -> tuple[bool, int, bytes]:
        calls.append(time_limit_ms)
        return True, 0, b"\x01\x00\x00\x01\x00\x00"

    first_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)
    second_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)
    family = (("dot", ((0, 0),)),)

    assert first_wrapper(b"canonical-shape", 250, family) == (
        True, 0, b"\x01\x00\x00\x01\x00\x00",
    )
    # Budget is intentionally not part of the persistent problem key.
    assert second_wrapper(b"canonical-shape", 10_000, family) == (
        True, 0, b"\x01\x00\x00\x01\x00\x00",
    )
    assert calls == [250]


def test_redis_persistent_cache_keeps_approximation_and_indexes_hard_problem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    monkeypatch.setitem(
        sys.modules,
        "redis",
        SimpleNamespace(
            Redis=lambda **kwargs: _FakeRedisClient(backend, **kwargs),
        ),
    )
    calls: List[int] = []
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    approximation = (
        False, 1,
        stencil._encode_cached_placements([("domino", (0, 1))], family),
    )

    def solve(
        shape_key: bytes,
        time_limit_ms: int,
        family: Any,
    ) -> tuple[bool, int, bytes]:
        calls.append(time_limit_ms)
        return approximation

    wrapped = storage.redis_persistent_cache("approximation-test")(solve)
    shape_key = b"canonical-hard-shape"

    assert wrapped(shape_key, 250, family) == approximation
    # The approximate cover is a useful cached answer, even under a later
    # request with a different budget; it is not thrown away.
    assert wrapped(shape_key, 10_000, family) == approximation
    assert calls == [250]

    store = storage.RedisStencilStore(backend, "approximation-test")
    assert store.get_result(shape_key, family) == approximation
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset({shape_key})

    # Only the canonical problem key and the current best result are stored;
    # there is no serialized HiGHS model, basis, or checkpoint state.
    record = json.loads(next(iter(backend.values.values())).decode("utf-8"))
    assert set(record) == {
        "version", "optimal", "status", "tile_count", "placements",
    }
    assert record["status"] == 1
    assert record["placements"]


def test_redis_stencil_store_only_upgrades_to_better_results() -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "upgrade-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key = b"canonical-component"
    fallback = (
        False, 2,
        stencil._encode_cached_placements(
            [("dot", (0,)), ("dot", (1,))], family,
        ),
    )
    approximation = (
        False, 1,
        stencil._encode_cached_placements([("domino", (0, 1))], family),
    )
    exact = (
        True, 0,
        stencil._encode_cached_placements([("domino", (0, 1))], family),
    )

    assert store.put_result(shape_key, family, fallback) is True
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset({shape_key})
    assert store.put_result(shape_key, family, approximation) is True
    assert store.get_result(shape_key, family) == approximation
    assert store.put_result(shape_key, family, fallback) is False
    assert store.get_result(shape_key, family) == approximation

    assert store.put_result(shape_key, family, exact) is True
    assert store.get_result(shape_key, family) == exact
    assert store.hard_components(family) == frozenset()
    assert store.pending_components(family) == frozenset()
    assert store.timeout_metadata(shape_key, family) is None
    assert store.put_result(shape_key, family, approximation) is False
    assert store.get_result(shape_key, family) == exact

    contradictory_exact = (
        True, 0,
        stencil._encode_cached_placements(
            [("dot", (0,)), ("dot", (1,))], family,
        ),
    )
    with pytest.raises(storage.RedisCacheConflict):
        store.put_result(shape_key, family, contradictory_exact)


def test_redis_stencil_store_isolates_hard_keys_by_family() -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "family-isolation-test")
    first_family = (("dot", ((0, 0),)),)
    second_family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key = b"same-component"
    approximation = (False, 1, b"\x01\x00\x00\x01\x00\x00")

    store.put_result(shape_key, first_family, approximation)
    assert store.hard_components(first_family) == frozenset({shape_key})
    assert store.pending_components(first_family) == frozenset({shape_key})
    assert store.hard_components(second_family) == frozenset()
    assert store.pending_components(second_family) == frozenset()



def test_pending_queue_migrates_existing_hard_components_once() -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "queue-migration-test")
    family = (("dot", ((0, 0),)),)
    shape_key = b"pre-existing-hard-component"
    approximation = (
        False, 1,
        stencil._encode_cached_placements([("dot", (0,))], family),
    )

    store.put_result(shape_key, family, approximation)
    # Simulate a Redis instance that has the Step 3 hard index but predates
    # the Step 4 pending set.
    backend.sets[store._pending_set_key(family)].clear()

    assert store.initialize_pending_queue(family) == 1
    assert store.pending_components(family) == frozenset({shape_key})
    assert store.initialize_pending_queue(family) == 0


def test_timeout_metadata_retires_pending_work_and_better_results_requeue() -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "timeout-lifecycle-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
        ("square", ((0, 0), (0, 1), (1, 0), (1, 1))),
    )
    shape_key = b"hard-square"
    two_dominoes = (
        False, 1,
        stencil._encode_cached_placements(
            [("domino", (0, 1)), ("domino", (2, 3))], family,
        ),
    )
    one_square = (
        False, 1,
        stencil._encode_cached_placements([("square", (0, 1, 2, 3))], family),
    )

    assert store.put_result(shape_key, family, two_dominoes)
    assert store.mark_timed_out(
        shape_key, family, two_dominoes,
        time_limit_ms=10_000, attempted_at=123.5,
    )
    metadata = store.timeout_metadata(shape_key, family)
    assert metadata == {
        "version": 1,
        "reason": "time_limit_reached_without_proving_optimality",
        "time_limit_ms": 10_000,
        "status": 1,
        "tile_count": 2,
        "attempted_at": 123.5,
    }
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset()

    # A later foreground improvement clears the old timeout and queues another
    # bulk attempt, rather than leaving an improved incumbent permanently idle.
    assert store.put_result(shape_key, family, one_square)
    assert store.timeout_metadata(shape_key, family) is None
    assert store.pending_components(family) == frozenset({shape_key})


def test_timeout_metadata_uses_result_current_at_atomic_retirement() -> None:
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "timeout-race-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
        ("square", ((0, 0), (0, 1), (1, 0), (1, 1))),
    )
    shape_key = b"race-component"
    stale = (
        False, 1,
        stencil._encode_cached_placements(
            [("domino", (0, 1)), ("domino", (2, 3))], family,
        ),
    )
    improved = (
        False, 1,
        stencil._encode_cached_placements(
            [("square", (0, 1, 2, 3))], family,
        ),
    )

    assert store.put_result(shape_key, family, stale)
    assert store.put_result(shape_key, family, improved)
    assert store.mark_timed_out(
        shape_key, family, stale,
        time_limit_ms=10_000, attempted_at=321.0,
    )
    metadata = store.timeout_metadata(shape_key, family)
    assert metadata is not None
    assert metadata["tile_count"] == 1
    assert metadata["status"] == 1


def test_bulk_worker_upgrades_to_optimal_and_uses_ten_second_budget() -> None:
    from transformers.stencil_bulk_solver import (
        BULK_TIME_LIMIT_MS,
        StencilBulkSolver,
    )
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "bulk-optimal-test")
    family = (("domino", ((0, 0), (0, 1))),)
    shape_key, _frame = stencil._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    encoded = stencil._encode_cached_placements([("domino", (0, 1))], family)
    approximation = (False, 1, encoded)
    exact = (True, 0, encoded)
    assert store.put_result(shape_key, family, approximation)

    calls: List[int] = []

    def solve(
        key: bytes, time_limit_ms: int, family_key: Any,
    ) -> tuple[bool, int, bytes]:
        assert key == shape_key
        assert family_key == family
        calls.append(time_limit_ms)
        return exact

    worker = StencilBulkSolver(
        store, family=family, solve=solve, clock=lambda: 456.0,
    )
    counts = worker.run_once()

    assert calls == [BULK_TIME_LIMIT_MS]
    assert counts == {
        "attempted": 1, "optimal": 1, "timed_out": 0, "skipped": 0,
    }
    assert store.get_result(shape_key, family) == exact
    assert store.hard_components(family) == frozenset()
    assert store.pending_components(family) == frozenset()
    assert store.timeout_metadata(shape_key, family) is None


def test_bulk_worker_persists_timeout_and_does_not_retry() -> None:
    from transformers.stencil_bulk_solver import (
        BULK_TIME_LIMIT_MS,
        StencilBulkSolver,
    )
    import transformers.stencil_cache_storage as storage

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "bulk-timeout-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key, _frame = stencil._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    encoded = stencil._encode_cached_placements([("domino", (0, 1))], family)
    approximation = (False, 1, encoded)
    assert store.put_result(shape_key, family, approximation)

    calls: List[int] = []

    def solve(
        _key: bytes, time_limit_ms: int, _family: Any,
    ) -> tuple[bool, int, bytes]:
        calls.append(time_limit_ms)
        return approximation

    worker = StencilBulkSolver(
        store, family=family, solve=solve, clock=lambda: 789.0,
    )
    first = worker.run_once()
    second = worker.run_once()

    assert calls == [BULK_TIME_LIMIT_MS]
    assert first == {
        "attempted": 1, "optimal": 0, "timed_out": 1, "skipped": 0,
    }
    assert second == {
        "attempted": 0, "optimal": 0, "timed_out": 0, "skipped": 0,
    }
    metadata = store.timeout_metadata(shape_key, family)
    assert metadata is not None
    assert metadata["time_limit_ms"] == 10_000
    assert metadata["attempted_at"] == 789.0
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset()


def test_bulk_worker_rejects_a_malformed_or_non_covering_solution() -> None:
    from transformers.stencil_bulk_solver import _validate_bulk_result

    family = (("domino", ((0, 0), (0, 1))),)
    shape_key, _frame = stencil._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    malformed = (True, 0, bytes((1, 0, 0, 0)))
    with pytest.raises((ValueError, RuntimeError)):
        _validate_bulk_result(shape_key, family, malformed)


def test_local_solver_cache_invalidation_clears_request_lru() -> None:
    family = (("dot", ((0, 0),)),)
    shape_key, _frame = stencil._component_shape_key(
        (0,), board_width=1, family=family,
    )
    stencil._solve_component_cached.cache_clear()
    stencil._solve_component_cached(shape_key, 100, family)
    assert stencil._solve_component_cached.cache_info().currsize == 1

    stencil._invalidate_component_cache()
    assert stencil._solve_component_cached.cache_info().currsize == 0


def test_component_specific_invalidation_preserves_other_lru_entries() -> None:
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    singleton_key, _ = stencil._component_shape_key(
        (0,), board_width=1, family=family,
    )
    domino_key, _ = stencil._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    stencil._solve_component_cached.cache_clear()
    stencil._solve_component_cached(singleton_key, 1_000, family)
    stencil._solve_component_cached(domino_key, 1_000, family)
    assert stencil._solve_component_cached.cache_info().currsize == 2

    stencil._invalidate_component_cache(singleton_key)
    assert stencil._solve_component_cached.cache_info().currsize == 1
    before_hits = stencil._solve_component_cached.cache_info().hits
    stencil._solve_component_cached(domino_key, 1_000, family)
    assert stencil._solve_component_cached.cache_info().hits == before_hits + 1
    stencil._solve_component_cached.cache_clear()


def test_cache_invalidation_dispatch_passes_the_changed_shape_key() -> None:
    import transformers.stencil_cache_storage as storage

    namespace = "invalidation-dispatch-test"
    seen: List[bytes] = []
    storage._INVALIDATORS[namespace] = [seen.append]
    try:
        storage._dispatch_cache_invalidation(namespace, b"canonical-shape")
    finally:
        storage._INVALIDATORS.pop(namespace, None)
    assert seen == [b"canonical-shape"]


def test_invalidation_payload_skips_self_and_decodes_other_processes() -> None:
    import transformers.stencil_cache_storage as storage

    shape_key = b"canonical-component"
    own_message = storage._invalidation_payload(shape_key)
    assert storage._decode_invalidation_payload(own_message) is None

    other_process = b"0123456789abcdef0123456789abcdef"
    foreign_message = other_process + b":" + __import__("base64").b64encode(shape_key)
    assert storage._decode_invalidation_payload(foreign_message) == shape_key

    # Raw messages remain accepted during a rolling deployment.
    assert storage._decode_invalidation_payload(shape_key) == shape_key


def test_redis_persistent_cache_recovers_after_startup_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import transformers.stencil_cache_storage as storage

    namespace = "startup-recovery-test"
    backend = _MemoryRedis()
    fake_client = _FakeRedisClient(
        backend,
        host="127.0.0.1",
        port=6379,
        db=7,
        socket_connect_timeout=0.25,
        socket_timeout=0.25,
        decode_responses=False,
    )
    monkeypatch.setattr(
        storage,
        "create_redis_client",
        lambda: (_ for _ in ()).throw(ConnectionError("Redis is offline")),
    )
    storage._PERSISTENT_CLIENTS.pop(namespace, None)

    calls: List[int] = []
    family = (("dot", ((0, 0),)),)
    shape_key = b"startup-recovery"
    result = (
        True, 0,
        stencil._encode_cached_placements([("dot", (0,))], family),
    )

    def solve(
        _shape_key: bytes,
        time_limit_ms: int,
        _family: Any,
    ) -> tuple[bool, int, bytes]:
        calls.append(time_limit_ms)
        return result

    wrapped = storage.redis_persistent_cache(namespace)(solve)
    assert wrapped(shape_key, 250, family) == result
    assert calls == [250]

    # The reconnecting invalidation listener publishes its client into this
    # registry; a later LRU miss can adopt it without restarting the proxy.
    storage._PERSISTENT_CLIENTS[namespace] = fake_client
    assert wrapped(shape_key, 10_000, family) == result
    assert wrapped(shape_key, 20_000, family) == result
    assert calls == [250, 10_000]


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
