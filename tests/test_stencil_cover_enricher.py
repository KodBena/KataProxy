# SPDX-License-Identifier: MIT
from __future__ import annotations

import base64
import importlib
import itertools
import hashlib
import json
import os
import threading
import sys
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List

import pytest

import transformers.stencil_cover.stencil_cache_storage as storage
import transformers.stencil_cover.stencil_board as board_position
import transformers.stencil_cover.stencil_family as stencil_family_module
import transformers.stencil_cover.stencil_ownership as ownership
import transformers.stencil_cover.stencil_ownership_policy as ownership_policy_factory
import transformers.stencil_cover.stencil_region_solver as region_solver
import transformers.stencil_cover.stencil_region_components as region_components
import transformers.stencil_cover.stencil_component_cache as component_cache
import transformers.stencil_cover.stencil_solver_factory as solver_factory
import transformers.stencil_cover.stencil_solver_geometry as geometry
import transformers.stencil_cover_enricher as enricher
from transformers.stencil_cover.stencil_ownership_coherent import CoherentOwnershipPolicy
from transformers.stencil_cover.stencil_ownership_threshold import ThresholdOwnershipPolicy
import sproxy_config as cfg
from AbstractProxy.proxy_core import ClientId
from katago import AnalyzeResponse, KataGoAction, KataGoQuery, make_katago_link
from transformers.stencil_cover.stencil_bulk_solver import (
    BULK_TIME_LIMIT_MS,
    StencilBulkSolver,
    _validate_bulk_result,
)
from transformers.stencil_cover.stencil_cache_storage import (
    SolverCacheCandidate,
    SolverCacheRequest,
    SolverWorkProfile,
)
from transformers.stencil_cover.stencil_solver_api import (
    BackendStatusReported,
    Incumbent,
    LowerBoundUnavailable,
    NativeEffortUnavailable,
    NativeEffortLimit,
    NoNativeEffortLimit,
    NoNativeEffortReason,
    NoSolverFailure,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverBudget,
    SolverIdentity,
    SolverOutcome,
)
from transformers.stencil_cover.stencil_solver_geometry import ComponentFrame, Stencil



def _threshold_ownership_regions(
    ownership_map: List[float],
    board_map: List[str | None],
    *,
    width: int,
    height: int,
    threshold: float,
) -> tuple[List[int], List[int]]:
    evidence = ownership.make_ownership_evidence(
        {"ownership": ownership_map},
        board_map,
        width,
        height,
        {"truncation_threshold": threshold},
        requires_ownership_stdev=False,
    )
    selected = ThresholdOwnershipPolicy().classify(evidence)
    return list(selected.positive), list(selected.negative)


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

    def hget(self, key: str, field: bytes | str) -> bytes | None:
        encoded_field = field.encode("utf-8") if isinstance(field, str) else field
        return self.hashes.get(key, {}).get(encoded_field)

    def hset(self, key: str, field: bytes | str, value: bytes | str) -> int:
        encoded_field = field.encode("utf-8") if isinstance(field, str) else field
        encoded_value = value.encode("utf-8") if isinstance(value, str) else value
        values = self.hashes.setdefault(key, {})
        is_new = encoded_field not in values
        values[encoded_field] = encoded_value
        return int(is_new)

    def publish(self, _channel: str, _message: bytes) -> int:
        return 1

    def eval(self, script: str, numkeys: int, *args: Any) -> int:
        if "STENCIL_PROFILE_MIGRATE" in script:
            result_key, hard_key, registry_key, payload, shape_key, family_prefix = args
            if result_key in self.values:
                return 0
            self.values[result_key] = payload
            migrated = json.loads(payload.decode("utf-8"))
            profiles = [
                raw_profile.decode("utf-8")
                for raw_profile in self.hashes.get(registry_key, {})
            ]
            if int(migrated["status"]) != 0:
                self.sets.setdefault(hard_key, set()).add(shape_key)
                for profile_id in profiles:
                    self.sets.setdefault(
                        f"{family_prefix}:pending:{profile_id}", set(),
                    ).add(shape_key)
                    self.hashes.setdefault(
                        f"{family_prefix}:timeouts:{profile_id}", {},
                    ).pop(shape_key, None)
            else:
                self.sets.setdefault(hard_key, set()).discard(shape_key)
                for profile_id in profiles:
                    self.sets.setdefault(
                        f"{family_prefix}:pending:{profile_id}", set(),
                    ).discard(shape_key)
                    self.hashes.setdefault(
                        f"{family_prefix}:timeouts:{profile_id}", {},
                    ).pop(shape_key, None)
            return 1

        if "STENCIL_PROFILE_INITIALIZE" in script:
            hard_key, pending_key, initialized_key, legacy_hard_key, migration_key = args
            if migration_key not in self.markers:
                self.markers.add(migration_key)
                self.sets.setdefault(hard_key, set()).update(
                    self.sets.get(legacy_hard_key, set())
                )
            if initialized_key in self.markers:
                return 0
            self.markers.add(initialized_key)
            pending = self.sets.setdefault(pending_key, set())
            queued = 0
            for shape_key in self.sets.get(hard_key, set()):
                if shape_key not in pending:
                    pending.add(shape_key)
                    queued += 1
            return queued

        if "STENCIL_PROFILE_TIMEOUT" in script or "SISMEMBER" in script:
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
                pending.discard(shape_key)
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

        if "STENCIL_PROFILE_STORE" in script:
            assert numkeys == 3
            (
                result_key, hard_key, registry_key, payload, status, tile_count,
                _schema_version, shape_key, raw_bound, family_prefix, profile_id,
            ) = args
            new_status = int(status)
            new_cost = int(tile_count)
            new_bound = int(raw_bound) if raw_bound not in (None, "") else None
            old_raw = self.values.get(result_key)
            should_store = True
            if old_raw is not None:
                old = json.loads(old_raw.decode("utf-8"))
                old_status = int(old["status"])
                old_cost = int(old["tile_count"])
                old_bound = old.get("quality_lower_bound")
                if old_status == 0 and new_status == 0 and old_cost != new_cost:
                    return -1
                if new_status > old_status or (
                    new_status == old_status and new_cost > old_cost
                ):
                    should_store = False
                elif new_status == old_status and new_cost == old_cost:
                    if new_bound is None and old_bound is None:
                        should_store = False
                    elif new_bound is None:
                        should_store = False
                    elif old_bound is not None and new_bound <= int(old_bound):
                        should_store = False

            if should_store:
                self.values[result_key] = payload
            final = json.loads(self.values[result_key].decode("utf-8"))
            hard = self.sets.setdefault(hard_key, set())
            profiles = [
                field.decode("utf-8")
                for field in self.hashes.get(registry_key, {})
            ]
            if int(final["status"]) == 0:
                hard.discard(shape_key)
                for known in profiles:
                    self.sets.setdefault(
                        f"{family_prefix}:pending:{known}", set(),
                    ).discard(shape_key)
                    self.hashes.setdefault(
                        f"{family_prefix}:timeouts:{known}", {},
                    ).pop(shape_key, None)
            else:
                hard.add(shape_key)
                if should_store:
                    selected = profiles if old_raw is None else [profile_id]
                    for known in selected:
                        self.sets.setdefault(
                            f"{family_prefix}:pending:{known}", set(),
                        ).add(shape_key)
                        self.hashes.setdefault(
                            f"{family_prefix}:timeouts:{known}", {},
                        ).pop(shape_key, None)
            return int(should_store)

        raise AssertionError("unknown Redis Lua script contract")


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

    def hget(self, key: str, field: bytes | str) -> bytes | None:
        return self.backend.hget(key, field)

    def hset(self, key: str, field: bytes | str, value: bytes | str) -> int:
        return self.backend.hset(key, field, value)

    def eval(self, *args: Any) -> int:
        return self.backend.eval(*args)

    def publish(self, _channel: str, _message: bytes) -> int:
        return 1


def _fake_redis_client(backend: _MemoryRedis) -> _FakeRedisClient:
    return _FakeRedisClient(
        backend,
        host="127.0.0.1",
        port=6379,
        db=7,
        socket_connect_timeout=0.25,
        socket_timeout=0.25,
        decode_responses=False,
    )


class _CacheRequestBackend:
    """A minimal identity-bearing port for request/cache-boundary tests."""

    def __init__(self, identity: SolverIdentity | None = None) -> None:
        if identity is None:
            configuration = '{"purpose":"cache-test"}'
            identity = SolverIdentity(
                backend_name="cache-test",
                backend_version="1",
                adapter_version="cache-test-v1",
                configuration_json=configuration,
                configuration_fingerprint=hashlib.sha256(
                    configuration.encode("utf-8"),
                ).hexdigest(),
            )
        self.identity = identity

    def solve(self, _problem: Any, _budget: Any) -> SolverOutcome:
        raise AssertionError("this test backend is only a cache identity")


def _cache_request(
    shape_key: bytes,
    family: tuple[Stencil, ...],
    *,
    wall_ms: int = 250,
    backend: Any = None,
) -> SolverCacheRequest:
    selected = solver_factory.DEFAULT_SOLVER_BACKEND if backend is None else backend
    return SolverCacheRequest(
        shape_key=shape_key,
        family=family,
        backend=selected,
        budget=SolverBudget(
            wall_time_limit_ms=wall_ms,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id=f"cache-test-{wall_ms}ms",
        ),
    )


def test_opt_in_requests_ownership_and_missing_map_does_not_kill_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client_id = ClientId("stencil-cover-test")
    link = make_katago_link()
    transformer = enricher.stencil_cover_enricher(
        link, ownership_policy=CoherentOwnershipPolicy(),
    )
    query = KataGoQuery(
        action=KataGoAction.ANALYZE,
        opaque={
            "capabilities": {
                "stencil_cover": {"truncation_threshold": 0.6},
            },
            "boardXSize": 1,
            "boardYSize": 1,
            "initialStones": [],
            "moves": [],
            "includeOwnership": False,
        },
    )

    assert transformer.on_query(client_id, query) is query
    # Opting in to stencil_cover is sufficient; callers need not separately
    # know that KataGo's ownership map must be requested.
    assert query.opaque["includeOwnership"] is True
    assert query.opaque["includeOwnershipStdev"] is True

    # A missing map can still occur for an upstream error/incomplete response.
    # Optional enrichment must not take down the session send loop.
    response = AnalyzeResponse(
        is_during_search=False,
        turn_number=0,
        opaque={},
    )
    with caplog.at_level(
        "ERROR", logger="kataproxy.stencil_cover_enricher",
    ):
        assert transformer.on_response(client_id, response) is response

    assert "stencilCover" not in response.opaque
    assert "forwarding the original KataGo response" in caplog.text


def test_family_parser_loads_proxy_owned_rectangles() -> None:
    family = stencil_family_module.parse_stencil_family("name: dot\n#\n\nname: domino\n##\n")
    assert family == (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )


def test_family_parser_rejects_disconnected_shapes() -> None:
    with pytest.raises(ValueError, match="4-connected"):
        stencil_family_module.parse_stencil_family("name: split\n#.#\n")


def test_zero_threshold_splits_signs_and_leaves_neutral_points_unowned() -> None:
    positive, negative = _threshold_ownership_regions(
        [0.8, 0.0, -0.7, 0.4],
        [None, None, None, None],
        width=2,
        height=2,
        threshold=0.0,
    )
    assert positive == [0, 3]
    assert negative == [2]


def test_threshold_one_keeps_only_fully_owned_empty_points() -> None:
    positive, negative = _threshold_ownership_regions(
        [1.0, 0.999, -1.0, -0.999],
        [None, None, None, None],
        width=2,
        height=2,
        threshold=1.0,
    )
    assert positive == [0]
    assert negative == [2]


def test_coordinate_parser_handles_numeric_and_rectangular_coordinates() -> None:
    assert board_position._coordinate_index("(2, 0)", 3, 2, allow_pass=False) == 2
    assert board_position._coordinate_index("C2", 3, 2, allow_pass=False) == 2
    assert board_position._coordinate_index("A1", 3, 2, allow_pass=False) == 3


def test_dead_chains_join_opponent_territory_and_living_stones_are_excluded() -> None:
    # The black chain at 0-1 and white chain at 3-4 fail KataGo's chain-level
    # liveness heuristic. The black stone at 6 is alive and stays out.
    board = ["B", "B", None, "W", "W", None, "B", None]
    ownership = [0.3, 0.0, 0.95, -0.95, -0.5, -0.9, -0.95, -0.9]

    positive, negative = _threshold_ownership_regions(
        ownership, board, width=4, height=2, threshold=0.8,
    )

    # Dead stones are included even though their individual ownership values
    # do not cross 0.8; empty points still use the configured cutoff.
    assert positive == [0, 1, 2]
    assert negative == [3, 4, 5, 7]


def test_board_replay_stops_at_the_requested_turn_and_applies_captures() -> None:
    context = board_position.RequestContext(
        width=3,
        height=3,
        initial_stones=(
            ("W", board_position._coordinate_index("B2", 3, 3, allow_pass=False)),
            ("B", board_position._coordinate_index("B3", 3, 3, allow_pass=False)),
        ),
        moves=(
            ("B", board_position._coordinate_index("A2", 3, 3, allow_pass=True)),
            ("W", None),
            ("B", board_position._coordinate_index("C2", 3, 3, allow_pass=True)),
            ("W", None),
            ("B", board_position._coordinate_index("B1", 3, 3, allow_pass=True)),
        ),
        policy_metadata={},
    )

    before_capture = board_position._board_at_turn(context, 4)
    after_capture = board_position._board_at_turn(context, 5)
    assert before_capture[4] == "W"
    assert after_capture[4] is None
    assert after_capture[1] == "B"


def test_candidates_include_singletons_for_every_region_point() -> None:
    family = (("domino", ((0, 0), (0, 1))),)
    candidates = region_components._candidate_placements([0, 1, 2], 3, 1, family)
    candidate_cells = {points for _, points in candidates}
    assert (0,) in candidate_cells
    assert (1,) in candidate_cells
    assert (2,) in candidate_cells
    assert (0, 1) in candidate_cells
    assert (1, 2) in candidate_cells



def test_components_use_four_connectivity_not_diagonal_connectivity() -> None:
    components = region_components._connected_components(
        [0, 1, 7, 8], width=3, height=3,
    )
    assert components == [(0, 1), (7, 8)]


def test_component_shape_key_is_translation_normalized() -> None:
    first, first_frame = region_components._component_shape_key((0, 1, 3, 4), board_width=3)
    translated, translated_frame = region_components._component_shape_key(
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
    assert geometry.family_dihedral_group(stencil_family_module.STENCIL_FAMILY) == tuple(range(8))


def test_stencil_family_contains_complete_d4_orbits_of_s_and_l_pieces() -> None:
    prototypes = (
        ("S-tetromino", ((0, 1), (0, 2), (1, 0), (1, 1)), 4),
        ("L-tetromino", ((0, 0), (1, 0), (2, 0), (2, 1)), 8),
        ("L-tromino", ((0, 0), (1, 0), (1, 1)), 4),
    )
    for prefix, prototype, expected_orbit_size in prototypes:
        expected = {
            geometry.normalise_shape(prototype, transform)
            for transform in range(8)
        }
        actual = {
            cells
            for name, cells in stencil_family_module.STENCIL_FAMILY
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
        for name, cells in stencil_family_module.STENCIL_FAMILY
        if name.startswith(new_names)
    ]
    assert len(new_stencils) == 16

    for name, cells in new_stencils:
        width = max(x for _, x in cells) + 1
        height = max(y for y, _ in cells) + 1
        region = [y * width + x for y, x in cells]
        result = region_solver._solve_region(
            region, width, height, stencil_family_module.STENCIL_FAMILY,
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
            geometry.transform_cell(y, x, transform)
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
        key, _frame = region_components._component_shape_key(points, board_width=16)
        keys.append(key)
    assert len(set(keys)) == 1


def test_family_symmetry_is_restricted_to_maps_that_preserve_its_shapes() -> None:
    horizontal_only = (
        ("dot", ((0, 0),)),
        ("line", ((0, 0), (0, 1), (0, 2))),
    )
    assert geometry.family_dihedral_group(horizontal_only) == (0, 2, 4, 5)


def test_canonical_solution_is_recovered_with_rotated_stencil_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    family = stencil_family_module.STENCIL_FAMILY
    seen_keys: List[bytes] = []

    def fake_cached(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        shape_key = request.shape_key
        family_key = request.family
        assert request.budget.wall_time_limit_ms == 250
        seen_keys.append(shape_key)
        local_width, local_height, cells = geometry.decode_component_shape_key(shape_key)
        candidates = region_components._candidate_placements(
            cells, local_width, local_height, family_key,
        )
        complete = next(
            candidate for candidate in candidates
            if len(candidate[1]) == len(cells) and set(candidate[1]) == set(cells)
        )
        return True, 0, geometry.encode_cached_placements([complete], family_key)

    monkeypatch.setattr(region_solver, "_solve_component_cached", fake_cached)

    horizontal = region_solver._solve_region([0, 1], width=2, height=1, family=family)
    vertical = region_solver._solve_region([0, 1], width=1, height=2, family=family)

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
        candidate = geometry.normalise_shape(base_shape, transform)
        if candidate not in unique_shapes:
            unique_shapes.append(candidate)
    family = (
        ("dot", ((0, 0),)),
        *((f"L{index}", shape) for index, shape in enumerate(unique_shapes)),
    )
    assert geometry.family_dihedral_group(family) == tuple(range(8))

    seen_keys: List[bytes] = []

    def fake_cached(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        shape_key = request.shape_key
        family_key = request.family
        assert request.budget.wall_time_limit_ms == 250
        seen_keys.append(shape_key)
        local_width, local_height, cells = geometry.decode_component_shape_key(shape_key)
        candidates = region_components._candidate_placements(
            cells, local_width, local_height, family_key,
        )
        complete = next(
            candidate for candidate in candidates
            if len(candidate[1]) == len(cells) and set(candidate[1]) == set(cells)
        )
        return True, 0, geometry.encode_cached_placements([complete], family_key)

    monkeypatch.setattr(region_solver, "_solve_component_cached", fake_cached)

    recovered: List[List[Dict[str, Any]]] = []
    for transform in range(8):
        mapped = [
            geometry.transform_cell(y, x, transform)
            for y, x in base_shape
        ]
        min_y = min(y for y, _ in mapped)
        min_x = min(x for _, x in mapped)
        component = tuple(
            (y - min_y + 3) * 10 + (x - min_x + 3)
            for y, x in mapped
        )
        result = region_solver._solve_region(
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

    def fake_cached(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        shape_key = request.shape_key
        family_key = request.family
        assert request.budget.wall_time_limit_ms == 250
        local_width, local_height, cells = geometry.decode_component_shape_key(shape_key)
        # The cache/solver boundary must never receive a disconnected mask.
        assert region_components._connected_components(
            cells, local_width, local_height,
        ) == [tuple(cells)]
        seen_components.append(tuple(cells))
        singleton_cover = [("1x1", (point,)) for point in cells]
        return (
            True,
            0,
            geometry.encode_cached_placements(singleton_cover, family_key),
        )

    monkeypatch.setattr(region_solver, "_solve_component_cached", fake_cached)
    result = region_solver._solve_region(
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

    def fake_cached(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        shape_key = request.shape_key
        family_key = request.family
        assert request.budget.wall_time_limit_ms == 250
        _optimal, status = next(returned_statuses)
        _width, _height, cells = geometry.decode_component_shape_key(shape_key)
        singleton_cover = [("1x1", (point,)) for point in cells]
        return (
            False,
            status,
            geometry.encode_cached_placements(singleton_cover, family_key),
        )

    monkeypatch.setattr(region_solver, "_solve_component_cached", fake_cached)
    result = region_solver._solve_region([0, 1, 8], 3, 3, family)

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
    result = region_solver._solve_region([], 3, 3, (("1x1", ((0, 0),)),))
    assert result == {
        "components": [],
        "placements": [],
        "tileCount": 0,
        "optimal": True,
        "status": "empty",
    }


def test_component_solver_cache_is_large_and_entry_bounded() -> None:
    info = component_cache._solve_component_cached.cache_info()
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
    transformer = enricher.stencil_cover_enricher(_mock_link())
    query = _query({})
    assert transformer.on_query(ClientId("q1"), query) is query
    response = _response()
    assert transformer.on_response(ClientId("q1"), response) is response
    assert "stencilCover" not in response.opaque




def test_legacy_threshold_policy_does_not_require_ownership_stdev() -> None:
    transformer = enricher.stencil_cover_enricher(
        _mock_link(), ownership_policy=ThresholdOwnershipPolicy(),
    )
    query = _query({"stencil_cover": {"truncation_threshold": 0.5}})
    transformer.on_query(ClientId("q3"), query)
    assert query.opaque["includeOwnership"] is True
    assert "includeOwnershipStdev" not in query.opaque
    response = AnalyzeResponse(
        is_during_search=False,
        turn_number=0,
        opaque={"ownership": [0.8, 0.7, -0.8, -0.7]},
    )
    transformer.on_response(ClientId("q3"), response)
    assert "stencilCover" in response.opaque




def test_environment_policy_factory_selects_sibling_implementations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_POLICY", "threshold")
    assert isinstance(ownership_policy_factory.create_ownership_region_policy(),
                      ThresholdOwnershipPolicy)

    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_POLICY", "coherent")
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_CONFIDENCE_KAPPA", 1.75)
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_SPATIAL_BETA", 0.4)
    selected = ownership_policy_factory.create_ownership_region_policy()
    assert isinstance(selected, CoherentOwnershipPolicy)
    assert selected.kappa == 1.75
    assert selected.beta == 0.4


def test_legacy_threshold_policy_validates_its_per_query_cutoff() -> None:
    transformer = enricher.stencil_cover_enricher(
        _mock_link(), ownership_policy=ThresholdOwnershipPolicy(),
    )
    with pytest.raises(ValueError, match="truncation_threshold"):
        transformer.on_query(ClientId("missing-threshold"), _query({"stencil_cover": {}}))


def test_environment_policy_factory_eagerly_loads_a_custom_policy_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = ModuleType("test_stencil_policy_plugin")
    setattr(plugin, "make_policy", lambda: ThresholdOwnershipPolicy())
    monkeypatch.setitem(sys.modules, "test_stencil_policy_plugin", plugin)
    monkeypatch.setattr(
        cfg,
        "STENCIL_OWNERSHIP_POLICY",
        "test_stencil_policy_plugin:make_policy",
    )
    importlib.reload(ownership_policy_factory)
    assert isinstance(
        ownership_policy_factory.DEFAULT_OWNERSHIP_POLICY,
        ThresholdOwnershipPolicy,
    )

    # Restore the real built-in selection for any later test in this module.
    monkeypatch.setattr(cfg, "STENCIL_OWNERSHIP_POLICY", "coherent")
    importlib.reload(ownership_policy_factory)
    assert isinstance(
        ownership_policy_factory.DEFAULT_OWNERSHIP_POLICY,
        CoherentOwnershipPolicy,
    )



def test_coherent_policy_preserves_the_expected_signed_ownership_sum() -> None:
    policy = CoherentOwnershipPolicy(
        kappa=0.0, beta=0.25, score_tolerance=0.5, time_limit_ms=2_000,
    )
    transformer = enricher.stencil_cover_enricher(
        _mock_link(), ownership_policy=policy,
    )
    query = _query({"stencil_cover": {}})
    transformer.on_query(ClientId("coherent"), query)
    assert query.opaque["includeOwnership"] is True
    assert query.opaque["includeOwnershipStdev"] is True

    response = AnalyzeResponse(
        is_during_search=False,
        turn_number=0,
        opaque={
            "ownership": [0.8, 0.7, -0.8, -0.7],
            "ownershipStdev": [0.05, 0.05, 0.05, 0.05],
        },
    )
    transformer.on_response(ClientId("coherent"), response)
    cover = response.opaque["stencilCover"]
    diagnostics = cover["ownershipDiagnostics"]
    assert cover["ownershipPolicy"] == "coherent"
    assert abs(
        diagnostics["targetNetOwnership"] - diagnostics["actualNetOwnership"]
    ) <= policy.score_tolerance
    assert diagnostics["solverStatus"] == "optimal"


def test_coherent_policy_refuses_a_missing_ownership_stdev_map(
    caplog: pytest.LogCaptureFixture,
) -> None:
    policy = CoherentOwnershipPolicy(time_limit_ms=2_000)
    transformer = enricher.stencil_cover_enricher(
        _mock_link(), ownership_policy=policy,
    )
    transformer.on_query(ClientId("missing-stdev"), _query({"stencil_cover": {}}))
    response = AnalyzeResponse(
        is_during_search=False,
        turn_number=0,
        opaque={"ownership": [0.8, 0.7, -0.8, -0.7]},
    )
    with caplog.at_level(
        "ERROR", logger="kataproxy.stencil_cover_enricher",
    ):
        transformer.on_response(ClientId("missing-stdev"), response)
    assert "stencilCover" not in response.opaque
    assert "ownershipStdev" in caplog.text


def test_coherent_policy_uses_ownership_stdev_as_a_confidence_penalty() -> None:
    evidence = ownership.make_ownership_evidence(
        {
            "ownership": [0.7, 0.7, 0.7],
            "ownershipStdev": [0.5, 0.0, 0.0],
        },
        [None, None, None],
        3,
        1,
        {},
        requires_ownership_stdev=True,
    )
    policy = CoherentOwnershipPolicy(
        kappa=2.0, beta=0.0, score_tolerance=0.5, time_limit_ms=2_000,
    )
    selected = policy.classify(evidence)
    assert selected.positive == (1, 2)
    assert selected.negative == ()



def test_coherent_policy_matches_the_explicit_unary_and_spatial_objective() -> None:
    means = (0.75, -0.15, 0.45, 0.05)
    deviations = (0.10, 0.20, 0.05, 0.15)
    evidence = ownership.make_ownership_evidence(
        {
            "ownership": list(means),
            "ownershipStdev": list(deviations),
        },
        [None] * 4,
        2,
        2,
        {},
        requires_ownership_stdev=True,
    )
    policy = CoherentOwnershipPolicy(
        kappa=0.7, beta=0.35, score_tolerance=0.5, time_limit_ms=2_000,
    )
    selected = policy.classify(evidence)
    chosen = tuple(
        1 if point in selected.positive
        else -1 if point in selected.negative
        else 0
        for point in range(4)
    )
    neighbours = ((0, 1), (0, 2), (1, 3), (2, 3))

    def energy(labels: tuple[int, ...]) -> float:
        unary = sum(
            0.0 if label == 0
            else 1.0 - 2.0 * label * mean + 2.0 * policy.kappa * deviation
            for label, mean, deviation in zip(labels, means, deviations)
        )
        spatial = policy.beta * sum(
            abs(labels[first] - labels[second])
            for first, second in neighbours
        )
        return unary + spatial

    expected_net = sum(means)
    feasible = (
        labels for labels in itertools.product((-1, 0, 1), repeat=4)
        if abs(sum(labels) - expected_net) <= policy.score_tolerance
    )
    optimum = min(energy(labels) for labels in feasible)
    assert abs(energy(chosen) - optimum) < 1e-7

def test_coherent_policy_builds_a_connected_region_for_diffuse_ownership() -> None:
    ownership_map = [0.2] * 25
    for point in (6, 7, 8, 11, 12):
        ownership_map[point] = 0.8
    evidence = ownership.make_ownership_evidence(
        {
            "ownership": ownership_map,
            "ownershipStdev": [0.05] * 25,
        },
        [None] * 25,
        5,
        5,
        {},
        requires_ownership_stdev=True,
    )
    selected = CoherentOwnershipPolicy(
        kappa=0.0, beta=0.3, score_tolerance=0.5, time_limit_ms=2_000,
    ).classify(evidence)
    positive = set(selected.positive)
    assert len(positive) == 8
    pending = [min(positive)]
    reached = set()
    while pending:
        point = pending.pop()
        if point in reached:
            continue
        reached.add(point)
        x = point % 5
        adjacent = []
        if x > 0:
            adjacent.append(point - 1)
        if x < 4:
            adjacent.append(point + 1)
        if point >= 5:
            adjacent.append(point - 5)
        if point < 20:
            adjacent.append(point + 5)
        pending.extend(
            neighbour for neighbour in adjacent
            if neighbour in positive and neighbour not in reached
        )
    assert reached == positive

def test_highs_returns_a_valid_minimum_exact_cover_for_a_domino_region() -> None:
    family = (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )
    result = region_solver._solve_region([0, 1], 2, 1, family)
    assert result["tileCount"] == 1
    assert result["optimal"] is True
    assert result["placements"] == [
        {"stencil": "1x2", "points": [[0, 0], [1, 0]]},
    ]


def test_redis_persistent_cache_round_trips_best_result_across_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    backend = _MemoryRedis()
    monkeypatch.setattr(
        storage, "create_redis_client", lambda: _fake_redis_client(backend),
    )
    calls: List[int] = []
    family: tuple[Stencil, ...] = (("dot", ((0, 0),)),)
    result = (True, 0, b"\x01\x00\x00\x01\x00\x00")

    def solve(request: SolverCacheRequest) -> SolverCacheCandidate:
        calls.append(request.budget.wall_time_limit_ms)
        return SolverCacheCandidate(result, {"disposition": "legacy_unknown"})

    first_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)
    second_wrapper = storage.redis_persistent_cache("round-trip-test")(solve)

    assert first_wrapper(_cache_request(b"canonical-shape", family)) == result
    # Budget is intentionally not part of the persistent problem key.
    assert second_wrapper(
        _cache_request(b"canonical-shape", family, wall_ms=10_000),
    ) == result
    assert calls == [250]


def test_foreground_persistent_write_registers_selected_work_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:


    backend = _MemoryRedis()
    monkeypatch.setattr(
        storage, "create_redis_client", lambda: _fake_redis_client(backend),
    )
    family = (("dot", ((0, 0),)),)
    configuration = '{"strategy":"foreground-profile-test"}'
    identity = SolverIdentity(
        backend_name="fake",
        backend_version="1",
        adapter_version="adapter-v1",
        configuration_json=configuration,
        configuration_fingerprint=hashlib.sha256(configuration.encode("utf-8")).hexdigest(),
    )
    budget = SolverBudget(
        wall_time_limit_ms=250,
        native_effort_limit=NoNativeEffortLimit(),
        policy_id="foreground-250ms",
    )
    profile = storage.make_solver_work_profile(identity, budget)
    encoded = geometry.encode_cached_placements([("dot", (0,))], family)
    calls: list[bytes] = []
    selected_backend = _CacheRequestBackend(identity)

    def solve(request: SolverCacheRequest) -> SolverCacheCandidate:
        assert request.budget == budget
        assert request.family == family
        assert request.backend is selected_backend
        assert request.work_profile == profile
        calls.append(request.shape_key)
        return SolverCacheCandidate(
            (True, 0, encoded), {"disposition": "legacy_unknown"},
        )

    wrapped = storage.redis_persistent_cache("foreground-profile-test")(solve)
    shape_key = b"foreground-profile-shape"
    request = SolverCacheRequest(shape_key, family, selected_backend, budget)
    assert wrapped(request) == (True, 0, encoded)
    assert calls == [shape_key]

    store = storage.RedisStencilStore(backend, "foreground-profile-test")
    registered = backend.hget(
        store._profile_registry_key(family), profile.fingerprint,
    )
    assert registered is not None
    assert json.loads(registered.decode("utf-8")) == profile.document


def test_redis_persistent_cache_keeps_approximation_and_indexes_hard_problem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    backend = _MemoryRedis()
    monkeypatch.setattr(
        storage, "create_redis_client", lambda: _fake_redis_client(backend),
    )
    calls: List[int] = []
    family: tuple[Stencil, ...] = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    approximation = (
        False, 1,
        geometry.encode_cached_placements([("domino", (0, 1))], family),
    )

    def solve(request: SolverCacheRequest) -> SolverCacheCandidate:
        calls.append(request.budget.wall_time_limit_ms)
        return SolverCacheCandidate(
            approximation, {"disposition": "legacy_unknown"},
        )

    wrapped = storage.redis_persistent_cache("approximation-test")(solve)
    shape_key = b"canonical-hard-shape"

    request = _cache_request(shape_key, family)
    assert wrapped(request) == approximation
    # The approximate cover is a useful cached answer, even under a later
    # request with a different budget; it is not thrown away.
    assert wrapped(
        _cache_request(shape_key, family, wall_ms=10_000),
    ) == approximation
    assert calls == [250]

    store = storage.RedisStencilStore(backend, "approximation-test")
    assert store.get_result(shape_key, family) == approximation
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family, request.work_profile) == frozenset({shape_key})

    # Only the canonical problem key and the current best result are stored;
    # there is no serialized HiGHS model, basis, or checkpoint state.
    record = json.loads(next(iter(backend.values.values())).decode("utf-8"))
    assert set(record) == {
        "version", "optimal", "status", "tile_count", "placements",
        "quality_lower_bound", "provenance",
    }
    assert record["version"] == 3
    assert record["provenance"] == {"disposition": "legacy_unknown"}
    assert record["status"] == 1
    assert record["placements"]


def test_redis_stencil_store_only_upgrades_to_better_results() -> None:

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "upgrade-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key = b"canonical-component"
    fallback = (
        False, 2,
        geometry.encode_cached_placements(
            [("dot", (0,)), ("dot", (1,))], family,
        ),
    )
    approximation = (
        False, 1,
        geometry.encode_cached_placements([("domino", (0, 1))], family),
    )
    exact = (
        True, 0,
        geometry.encode_cached_placements([("domino", (0, 1))], family),
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
        geometry.encode_cached_placements(
            [("dot", (0,)), ("dot", (1,))], family,
        ),
    )
    with pytest.raises(storage.RedisCacheConflict):
        store.put_result(shape_key, family, contradictory_exact)


def test_redis_stencil_store_isolates_hard_keys_by_family() -> None:

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

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "queue-migration-test")
    family = (("dot", ((0, 0),)),)
    shape_key = b"pre-existing-hard-component"
    approximation = (
        False, 1,
        geometry.encode_cached_placements([("dot", (0,))], family),
    )

    store.put_result(shape_key, family, approximation)
    # Simulate a Redis instance that has the Step 3 hard index but predates
    # the Step 4 pending set.
    backend.sets[store._pending_set_key(family)].clear()

    assert store.initialize_pending_queue(family) == 1
    assert store.pending_components(family) == frozenset({shape_key})
    assert store.initialize_pending_queue(family) == 0


def test_timeout_metadata_retires_pending_work_and_better_results_requeue() -> None:

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
        geometry.encode_cached_placements(
            [("domino", (0, 1)), ("domino", (2, 3))], family,
        ),
    )
    one_square = (
        False, 1,
        geometry.encode_cached_placements([("square", (0, 1, 2, 3))], family),
    )

    assert store.put_result(shape_key, family, two_dominoes)
    assert store.mark_timed_out(
        shape_key, family, two_dominoes,
        time_limit_ms=10_000, attempted_at=123.5,
    )
    metadata = store.timeout_metadata(shape_key, family)
    assert metadata is not None
    assert metadata["version"] == 2
    assert metadata["reason"] == "time_limit_reached_without_proving_optimality"
    assert metadata["time_limit_ms"] == 10_000
    assert metadata["status"] == 1
    assert metadata["tile_count"] == 2
    assert metadata["attempted_at"] == 123.5
    assert metadata["work_profile_fingerprint"] == storage.LEGACY_SOLVER_WORK_PROFILE.fingerprint
    assert metadata["work_profile"] == storage.LEGACY_SOLVER_WORK_PROFILE.document
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset()

    # A later foreground improvement clears the old timeout and queues another
    # bulk attempt, rather than leaving an improved incumbent permanently idle.
    assert store.put_result(shape_key, family, one_square)
    assert store.timeout_metadata(shape_key, family) is None
    assert store.pending_components(family) == frozenset({shape_key})


def test_timeout_metadata_uses_result_current_at_atomic_retirement() -> None:

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
        geometry.encode_cached_placements(
            [("domino", (0, 1)), ("domino", (2, 3))], family,
        ),
    )
    improved = (
        False, 1,
        geometry.encode_cached_placements(
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

    # If an exact foreground result wins the race instead, the atomic
    # retirement check must leave the component out of both hard indexes.
    exact = (True, 0, improved[2])
    assert store.put_result(shape_key, family, exact)
    assert not store.mark_timed_out(
        shape_key, family, stale,
        time_limit_ms=10_000, attempted_at=322.0,
    )
    assert store.timeout_metadata(shape_key, family) is None
    assert store.pending_components(family) == frozenset()
    assert store.hard_components(family) == frozenset()



class _StaticSolverBackend:
    """Small test backend that exposes a legacy compact fixture as a typed outcome."""

    def __init__(self, family: Any, encoded: bytes, *, optimal: bool) -> None:


        config = '{"fixture":"static"}'
        self.identity = SolverIdentity(
            backend_name="test-static",
            backend_version="1",
            adapter_version="test-adapter-v1",
            configuration_json=config,
            configuration_fingerprint=hashlib.sha256(config.encode("utf-8")).hexdigest(),
        )
        self.family = tuple(family)
        self.encoded = encoded
        self.optimal = optimal
        self.calls: List[int] = []

    def solve(self, problem: Any, budget: Any) -> Any:


        self.calls.append(budget.wall_time_limit_ms)
        frame = ComponentFrame(
            board_width=problem.width, transform=0, offset_y=0, offset_x=0,
            canonical_width=problem.width, canonical_height=problem.height,
        )
        recovered = geometry.decode_cached_placements(self.encoded, self.family, frame)
        placements = tuple(
            (item["stencil"], tuple((point[1], point[0]) for point in item["points"]))
            for item in recovered
        )
        tile_count = len(placements)
        return SolverOutcome(
            status=OutcomeStatus.OPTIMAL if self.optimal else OutcomeStatus.FEASIBLE,
            incumbent=Incumbent(tile_count, PlacementWitness(placements)),
            lower_bound=ProvenLowerBound(tile_count) if self.optimal else LowerBoundUnavailable(),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT
            ),
            backend_status=BackendStatusReported("FIXTURE_OPTIMAL" if self.optimal else "FIXTURE_FEASIBLE"),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def test_bulk_worker_upgrades_to_optimal_and_uses_ten_second_budget() -> None:


    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "bulk-optimal-test")
    family = (("domino", ((0, 0), (0, 1))),)
    shape_key, _frame = region_components._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    encoded = geometry.encode_cached_placements([("domino", (0, 1))], family)
    approximation = (False, 1, encoded)
    exact = (True, 0, encoded)
    assert store.put_result(shape_key, family, approximation)

    solver_backend = _StaticSolverBackend(family, encoded, optimal=True)
    worker = StencilBulkSolver(
        store, family=family, solver_backend=solver_backend, clock=lambda: 456.0,
    )
    counts = worker.run_once()

    assert solver_backend.calls == [BULK_TIME_LIMIT_MS]
    assert counts == {
        "attempted": 1, "optimal": 1, "timed_out": 0, "skipped": 0,
    }
    assert store.get_result(shape_key, family) == exact
    assert store.hard_components(family) == frozenset()
    assert store.pending_components(family) == frozenset()
    assert store.timeout_metadata(shape_key, family) is None


def test_bulk_worker_persists_timeout_and_does_not_retry() -> None:


    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "bulk-timeout-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key, _frame = region_components._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    encoded = geometry.encode_cached_placements([("domino", (0, 1))], family)
    approximation = (False, 1, encoded)
    assert store.put_result(shape_key, family, approximation)

    solver_backend = _StaticSolverBackend(family, encoded, optimal=False)
    worker = StencilBulkSolver(
        store, family=family, solver_backend=solver_backend, clock=lambda: 789.0,
    )
    first = worker.run_once()
    second = worker.run_once()

    assert solver_backend.calls == [BULK_TIME_LIMIT_MS]
    assert first == {
        "attempted": 1, "optimal": 0, "timed_out": 1, "skipped": 0,
    }
    assert second == {
        "attempted": 0, "optimal": 0, "timed_out": 0, "skipped": 0,
    }
    metadata = store.timeout_metadata(shape_key, family, worker.work_profile)
    assert metadata is not None
    assert metadata["time_limit_ms"] == 10_000
    assert metadata["attempted_at"] == 789.0
    assert metadata["work_profile_fingerprint"] == worker.work_profile.fingerprint
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family, worker.work_profile) == frozenset()


def test_bulk_worker_rejects_a_malformed_or_non_covering_solution() -> None:

    family = (("domino", ((0, 0), (0, 1))),)
    shape_key, _frame = region_components._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    malformed = (True, 0, bytes((1, 0, 0, 0)))
    with pytest.raises((ValueError, RuntimeError)):
        _validate_bulk_result(shape_key, family, malformed)


def test_local_solver_cache_invalidation_clears_request_lru() -> None:
    family = (("dot", ((0, 0),)),)
    shape_key, _frame = region_components._component_shape_key(
        (0,), board_width=1, family=family,
    )
    component_cache._solve_component_cached.cache_clear()
    component_cache._solve_component_cached(_cache_request(shape_key, family, wall_ms=100))
    assert component_cache._solve_component_cached.cache_info().currsize == 1

    component_cache._invalidate_component_cache()
    assert component_cache._solve_component_cached.cache_info().currsize == 0


def test_component_specific_invalidation_preserves_other_lru_entries() -> None:
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    singleton_key, _ = region_components._component_shape_key(
        (0,), board_width=1, family=family,
    )
    domino_key, _ = region_components._component_shape_key(
        (0, 1), board_width=2, family=family,
    )
    component_cache._solve_component_cached.cache_clear()
    component_cache._solve_component_cached(_cache_request(singleton_key, family, wall_ms=1_000))
    component_cache._solve_component_cached(_cache_request(domino_key, family, wall_ms=1_000))
    assert component_cache._solve_component_cached.cache_info().currsize == 2

    component_cache._invalidate_component_cache(singleton_key)
    assert component_cache._solve_component_cached.cache_info().currsize == 1
    before_hits = component_cache._solve_component_cached.cache_info().hits
    component_cache._solve_component_cached(_cache_request(domino_key, family, wall_ms=1_000))
    assert component_cache._solve_component_cached.cache_info().hits == before_hits + 1
    component_cache._solve_component_cached.cache_clear()


def test_invalidation_during_solver_call_does_not_recache_stale_result() -> None:

    family = (("dot", ((0, 0),)),)
    shape_key = b"component-under-solve"
    stale = (False, 2, b"stale")
    improved = (False, 1, b"improved")
    started = threading.Event()
    release = threading.Event()
    calls: List[int] = []

    def solve(_request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        calls.append(1)
        if len(calls) == 1:
            started.set()
            assert release.wait(timeout=3.0)
            return stale
        return improved

    cache = component_cache._InvalidatableSolverCache(solve, maxsize=4)
    request = _cache_request(shape_key, family)
    results: List[tuple[bool, int, bytes]] = []
    thread = threading.Thread(
        target=lambda: results.append(cache(request)),
    )
    thread.start()
    assert started.wait(timeout=3.0)
    cache.invalidate(shape_key)
    release.set()
    thread.join(timeout=3.0)

    assert not thread.is_alive()
    assert results == [improved]
    assert calls == [1, 1]
    assert cache(request) == improved
    assert calls == [1, 1]


def test_cache_invalidation_dispatch_passes_the_changed_shape_key() -> None:

    namespace = "invalidation-dispatch-test"
    seen: List[bytes] = []
    storage._INVALIDATORS[namespace] = [seen.append]
    try:
        storage._dispatch_cache_invalidation(namespace, b"canonical-shape")
    finally:
        storage._INVALIDATORS.pop(namespace, None)
    assert seen == [b"canonical-shape"]


def test_invalidation_payload_skips_self_and_decodes_other_processes() -> None:

    shape_key = b"canonical-component"
    own_message = storage._invalidation_payload(shape_key)
    assert storage._decode_invalidation_payload(own_message) is None


    # A forked child inherits the module nonce but has a different runtime PID.
    other_process = (
        str(os.getpid() + 1).encode("ascii")
        + b"-"
        + storage._PROCESS_NONCE
    )
    foreign_message = other_process + b":" + base64.b64encode(shape_key)
    assert storage._decode_invalidation_payload(foreign_message) == shape_key

    # Raw messages remain accepted during a rolling deployment.
    assert storage._decode_invalidation_payload(shape_key) == shape_key


def test_redis_persistent_cache_recovers_after_startup_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

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
        geometry.encode_cached_placements([("dot", (0,))], family),
    )

    def solve(request: SolverCacheRequest) -> SolverCacheCandidate:
        calls.append(request.budget.wall_time_limit_ms)
        return SolverCacheCandidate(result, {"disposition": "legacy_unknown"})

    wrapped = storage.redis_persistent_cache(namespace)(solve)
    assert wrapped(_cache_request(shape_key, family)) == result
    assert calls == [250]

    # The reconnecting invalidation listener publishes its client into this
    # registry; a later LRU miss can adopt it without restarting the proxy.
    storage._PERSISTENT_CLIENTS[namespace] = fake_client
    assert wrapped(_cache_request(shape_key, family, wall_ms=10_000)) == result
    assert wrapped(_cache_request(shape_key, family, wall_ms=20_000)) == result
    assert calls == [250, 10_000]


def test_redis_persistent_cache_warns_and_degrades_to_noop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:

    class OfflineRedis:
        def __init__(self, **kwargs: Any) -> None:
            pass

        def ping(self) -> None:
            raise ConnectionError("Redis is offline")

    monkeypatch.setattr(
        storage, "create_redis_client", lambda: OfflineRedis().ping(),
    )
    calls: List[int] = []

    def solve(_request: SolverCacheRequest) -> SolverCacheCandidate:
        calls.append(1)
        return SolverCacheCandidate(
            (False, 2, b"fallback"), {"disposition": "legacy_unknown"},
        )

    with caplog.at_level("WARNING"):
        decorated = storage.redis_persistent_cache("offline-test")(solve)

    family: tuple[Stencil, ...] = (("dot", ((0, 0),)),)
    assert decorated(_cache_request(b"shape", family)) == (False, 2, b"fallback")
    assert calls == [1]
    assert "persistent caching disabled" in caplog.text



def test_v4_reads_v3_rows_as_legacy_unknown_and_ignores_global_timeout() -> None:


    backend = _MemoryRedis()
    family = (("dot", ((0, 0),)),)
    shape_key = b"legacy-v3-shape"
    old_store = storage.RedisStencilStore(backend, "solver-component-d4-v3")
    old_result_key = old_store._problem_key(shape_key, family)
    old_hard_key = old_store._hard_set_key(family)
    old_timeout_key = old_store._timeout_hash_key(family)
    encoded = geometry.encode_cached_placements([("dot", (0,))], family)
    backend.values[old_result_key] = json.dumps({
        "version": 2,
        "optimal": False,
        "status": 1,
        "tile_count": 1,
        "placements": base64.b64encode(encoded).decode("ascii"),
    }, sort_keys=True, separators=(",", ":")).encode("utf-8")
    backend.sets[old_hard_key] = {shape_key}
    backend.hashes[old_timeout_key] = {
        shape_key: b'{"version":1,"time_limit_ms":10000,"status":1,"tile_count":1}'
    }

    store = storage.RedisStencilStore(backend, "solver-component-d4-v4")
    assert store.initialize_pending_queue(family) == 1
    record = store.get_result_record(shape_key, family)

    assert record is not None
    assert record.result == (False, 1, encoded)
    assert record.provenance == {"disposition": "legacy_unknown"}
    assert record.quality_lower_bound is None
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family) == frozenset({shape_key})
    assert store.timeout_metadata(shape_key, family) is None
    assert old_result_key in backend.values


def _result_provenance(
    attempt_id: str, lower_bound: int,
) -> Dict[str, Any]:
    return {
        "disposition": "present",
        "attempt_id": attempt_id,
        "solver_identity": {
            "fingerprint": hashlib.sha256(
                json.dumps({
                    "adapter_version": "test-v1",
                    "backend_name": "test-backend",
                    "backend_version": "1",
                    "configuration_fingerprint": hashlib.sha256(b"{}").hexdigest(),
                }, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            ).hexdigest(),
            "backend_name": "test-backend",
            "backend_version": "1",
            "adapter_version": "test-v1",
            "configuration_json": "{}",
            "configuration_fingerprint": hashlib.sha256(b"{}").hexdigest(),
        },
        "producer_path": "foreground",
        "budget": {
            "wall_time_limit_ms": 250,
            "policy_id": "foreground-250ms",
            "native_effort_limit": {
                "disposition": "not_requested",
                "reason": "not_requested",
            },
        },
        "started_at_utc": "2026-11-12T10:15:30.000000Z",
        "elapsed_wall_ns": 10,
        "outcome": {
            "status": "feasible",
            "optimality_proven": False,
            "incumbent": {"disposition": "present", "tile_count": 1},
            "lower_bound": {"disposition": "reported", "value": lower_bound},
            "native_effort": {"disposition": "not_reported",
                              "reason": "backend_did_not_report_effort"},
            "backend_status": {"disposition": "reported", "value": "TIME_LIMIT"},
            "error": {"disposition": "none"},
            "fallback_disposition": "not_needed",
        },
    }


def test_v3_result_arbitration_uses_certified_bound_for_equal_tile_count() -> None:

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "solver-component-d4-v4")
    family = (("dot", ((0, 0),)),)
    shape_key = b"same-cost-quality-bound"
    result = (
        False, 1,
        geometry.encode_cached_placements([("dot", (0,))], family),
    )
    first = _result_provenance(
        "12345678-1234-5678-1234-567812345678", 0,
    )
    stronger = _result_provenance(
        "22345678-1234-5678-1234-567812345678", 1,
    )

    assert store.put_result(shape_key, family, result, provenance=first) is True
    assert store.put_result(shape_key, family, result, provenance=first) is False
    assert store.put_result(shape_key, family, result, provenance=stronger) is True

    record = store.get_result_record(shape_key, family)
    assert record is not None
    assert record.quality_lower_bound == 1
    assert record.provenance["attempt_id"] == stronger["attempt_id"]



def test_solver_profiles_have_independent_pending_and_timeout_state() -> None:



    def profile(name: str, wall_ms: int = 10_000) -> storage.SolverWorkProfile:
        config = json.dumps({"variant": name}, sort_keys=True, separators=(",", ":"))
        identity = SolverIdentity(
            backend_name="fake",
            backend_version="1",
            adapter_version="adapter-v1",
            configuration_json=config,
            configuration_fingerprint=hashlib.sha256(config.encode()).hexdigest(),
        )
        return storage.make_solver_work_profile(
            identity,
            SolverBudget(
                wall_time_limit_ms=wall_ms,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id="bulk-policy-v1",
            ),
        )

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "profile-isolation-test")
    family = (("dot", ((0, 0),)),)
    shape_key = b"profile-isolated-component"
    result = (False, 1, geometry.encode_cached_placements([("dot", (0,))], family))
    profile_a, profile_b = profile("A"), profile("B")

    store.initialize_pending_queue(family, profile_a)
    store.initialize_pending_queue(family, profile_b)
    assert store.put_result(shape_key, family, result, work_profile=profile_a)
    assert store.pending_components(family, profile_a) == frozenset({shape_key})
    assert store.pending_components(family, profile_b) == frozenset({shape_key})

    assert store.mark_timed_out(
        shape_key, family, result, time_limit_ms=10_000,
        attempted_at=100.0, work_profile=profile_a,
    )
    assert store.pending_components(family, profile_a) == frozenset()
    assert store.pending_components(family, profile_b) == frozenset({shape_key})
    timeout_a = store.timeout_metadata(shape_key, family, profile_a)
    assert timeout_a is not None
    assert timeout_a["work_profile_fingerprint"] == profile_a.fingerprint
    assert store.timeout_metadata(shape_key, family, profile_b) is None

    assert store.mark_timed_out(
        shape_key, family, result, time_limit_ms=10_000,
        attempted_at=200.0, work_profile=profile_b,
    )
    timeout_b = store.timeout_metadata(shape_key, family, profile_b)
    assert timeout_b is not None
    assert timeout_b["work_profile_fingerprint"] == profile_b.fingerprint
    assert store.timeout_metadata(shape_key, family, profile_a) == timeout_a


def test_new_profile_is_seeded_from_hard_index_despite_other_profile_timeouts() -> None:



    def profile(name: str) -> storage.SolverWorkProfile:
        config = json.dumps({"variant": name}, sort_keys=True, separators=(",", ":"))
        identity = SolverIdentity(
            backend_name="fake", backend_version="1", adapter_version="adapter-v1",
            configuration_json=config,
            configuration_fingerprint=hashlib.sha256(config.encode()).hexdigest(),
        )
        return storage.make_solver_work_profile(
            identity,
            SolverBudget(10_000, NoNativeEffortLimit(), "bulk-policy-v1"),
        )

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "profile-seed-test")
    family = (("dot", ((0, 0),)),)
    shape_key = b"seeded-after-retirement"
    result = (False, 1, geometry.encode_cached_placements([("dot", (0,))], family))
    a, b, c = profile("A"), profile("B"), profile("C")
    store.initialize_pending_queue(family, a)
    store.initialize_pending_queue(family, b)
    store.put_result(shape_key, family, result, work_profile=a)
    for selected in (a, b):
        assert store.mark_timed_out(
            shape_key, family, result, time_limit_ms=10_000,
            attempted_at=1.0, work_profile=selected,
        )
    assert store.pending_components(family, a) == frozenset()
    assert store.pending_components(family, b) == frozenset()

    assert store.initialize_pending_queue(family, c) == 1
    assert store.pending_components(family, c) == frozenset({shape_key})
    assert store.timeout_metadata(shape_key, family, c) is None


def test_better_result_requeues_only_its_origin_profile_and_optimum_clears_all() -> None:



    def profile(name: str) -> storage.SolverWorkProfile:
        config = json.dumps({"variant": name}, sort_keys=True, separators=(",", ":"))
        identity = SolverIdentity(
            backend_name="fake", backend_version="1", adapter_version="adapter-v1",
            configuration_json=config,
            configuration_fingerprint=hashlib.sha256(config.encode()).hexdigest(),
        )
        return storage.make_solver_work_profile(
            identity,
            SolverBudget(10_000, NoNativeEffortLimit(), "bulk-policy-v1"),
        )

    backend = _MemoryRedis()
    store = storage.RedisStencilStore(backend, "profile-upgrade-test")
    family = (
        ("dot", ((0, 0),)),
        ("domino", ((0, 0), (0, 1))),
    )
    shape_key = b"global-upgrade"
    two_dots = (False, 2, geometry.encode_cached_placements([("dot", (0,)), ("dot", (1,))], family))
    one_domino = (False, 1, geometry.encode_cached_placements([("domino", (0, 1))], family))
    exact = (True, 0, one_domino[2])
    a, b = profile("A"), profile("B")
    store.initialize_pending_queue(family, a)
    store.initialize_pending_queue(family, b)
    assert store.put_result(shape_key, family, two_dots, work_profile=a)
    for selected in (a, b):
        assert store.mark_timed_out(
            shape_key, family, two_dots, time_limit_ms=10_000,
            attempted_at=1.0, work_profile=selected,
        )

    assert store.put_result(shape_key, family, one_domino, work_profile=a)
    assert store.pending_components(family, a) == frozenset({shape_key})
    assert store.timeout_metadata(shape_key, family, a) is None
    assert store.pending_components(family, b) == frozenset()
    assert store.timeout_metadata(shape_key, family, b) is not None

    assert store.put_result(shape_key, family, exact, work_profile=b)
    assert store.pending_components(family, a) == frozenset()
    assert store.pending_components(family, b) == frozenset()
    assert store.timeout_metadata(shape_key, family, a) is None
    assert store.timeout_metadata(shape_key, family, b) is None
