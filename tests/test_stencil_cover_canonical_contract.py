# SPDX-License-Identifier: MIT
from __future__ import annotations

import pytest

import transformers.stencil_cover.stencil_family as stencil_family_module
import transformers.stencil_cover.stencil_region_components as region_components
import transformers.stencil_cover.stencil_solver_geometry as geometry
from transformers.stencil_cover.stencil_cache_storage import RedisStencilStore
from transformers.stencil_cover.stencil_solver_identity import stable_digest as _stable_digest
from transformers.stencil_cover.stencil_solver_api import (
    BackendStatusUnavailable,
    Incumbent,
    LowerBoundUnavailable,
    NativeEffortUnavailable,
    NoNativeEffortReason,
    NoSolverFailure,
    OutcomeStatus,
    PlacementWitness,
    ProvenLowerBound,
    SolverOutcome,
    STENCIL_CANONICALIZATION_VERSION,
)


_ASYMMETRIC_COMPONENT = (
    (0, 1),
    (1, 0),
    (1, 1),
    (2, 1),
    (2, 2),
    (2, 3),
)


def _board_component(cells: tuple[tuple[int, int], ...], width: int) -> tuple[int, ...]:
    return tuple(sorted(y * width + x for y, x in cells))


def _translated_image(
    cells: tuple[tuple[int, int], ...], transform: int, dy: int, dx: int,
) -> tuple[tuple[int, int], ...]:
    mapped = tuple(geometry.transform_cell(y, x, transform) for y, x in cells)
    min_y = min(y for y, _x in mapped)
    min_x = min(x for _y, x in mapped)
    return tuple(sorted((y - min_y + dy, x - min_x + dx) for y, x in mapped))


def test_canonicalization_protocol_is_explicit_and_key_codec_is_lsb_first() -> None:
    assert STENCIL_CANONICALIZATION_VERSION == "kata-proxy-stencil-d4-lsb-v1"

    family = (("L", ((0, 0), (1, 0), (1, 1))),)
    key, _frame = region_components._component_shape_key((0, 8, 9), 8, family)

    # Header is height,width. Cell indices 0,2,3 set LSB-first -> 0b00001101.
    assert key == bytes((2, 2, 0x0D))
    assert geometry.decode_component_shape_key(key) == (2, 2, (0, 2, 3))


def test_active_stencil_family_is_closed_under_all_eight_d4_maps() -> None:
    expected_shapes = {
        geometry.normalise_shape(cells)
        for _name, cells in stencil_family_module.STENCIL_FAMILY
    }

    assert geometry.family_dihedral_group(stencil_family_module.STENCIL_FAMILY) == tuple(range(8))
    for transform in range(8):
        assert {
            geometry.normalise_shape(cells, transform)
            for _name, cells in stencil_family_module.STENCIL_FAMILY
        } == expected_shapes


def test_all_d4_images_and_translations_share_an_instance_key_and_frame_round_trip() -> None:
    board_width = 31
    expected_key, _ = region_components._component_shape_key(
        _board_component(_ASYMMETRIC_COMPONENT, board_width),
        board_width,
        stencil_family_module.STENCIL_FAMILY,
    )

    for transform in range(8):
        image = _translated_image(
            _ASYMMETRIC_COMPONENT,
            transform,
            dy=transform + 1,
            dx=transform + 2,
        )
        component = _board_component(image, board_width)
        key, frame = region_components._component_shape_key(
            component, board_width, stencil_family_module.STENCIL_FAMILY,
        )
        assert key == expected_key

        canonical_cells = set()
        original_cells = set()
        for point in component:
            y, x = divmod(point, board_width)
            transformed_y, transformed_x = geometry.transform_cell(
                y, x, frame.transform,
            )
            canonical_cells.add((
                transformed_y - frame.offset_y,
                transformed_x - frame.offset_x,
            ))
            original_cells.add((y, x))

        width, _height, flat_cells = geometry.decode_component_shape_key(key)
        decoded_cells = {
            (point // width, point % width)
            for point in flat_cells
        }
        assert canonical_cells == decoded_cells

        recovered_cells = {
            geometry.transform_cell(
                y + frame.offset_y,
                x + frame.offset_x,
                geometry.D4_INVERSE[frame.transform],
            )
            for y, x in canonical_cells
        }
        assert recovered_cells == original_cells


def test_non_closed_family_uses_its_stabilizer_not_the_whole_d4_group() -> None:
    family = (("L-tromino", ((0, 0), (1, 0), (1, 1))),)
    stabilizer = geometry.family_dihedral_group(family)

    # This shape is preserved by the identity and anti-diagonal reflection,
    # but not by a quarter turn. The actual family stabilizer is authoritative.
    assert stabilizer == (0, 7)
    original = _board_component(((0, 0), (1, 0), (1, 1)), 8)
    rotated = _board_component(
        _translated_image(((0, 0), (1, 0), (1, 1)), 1, 2, 2),
        8,
    )
    original_key, _ = region_components._component_shape_key(original, 8, family)
    rotated_key, _ = region_components._component_shape_key(rotated, 8, family)
    assert rotated_key != original_key


def test_family_fingerprint_is_part_of_instance_identity() -> None:
    shape = ((0, 0), (1, 0), (1, 1))
    family_a = (("L-tromino", shape),)
    family_b = (("renamed-L-tromino", shape),)
    component = (0, 8, 9)
    key_a, _ = region_components._component_shape_key(component, 8, family_a)
    key_b, _ = region_components._component_shape_key(component, 8, family_b)

    assert key_a == key_b
    assert _stable_digest(family_a) != _stable_digest(family_b)

    store = RedisStencilStore(client=object(), namespace="phase0-test")
    assert store._problem_key(key_a, family_a) != store._problem_key(key_b, family_b)


def test_solver_outcome_cannot_claim_optimality_without_a_matching_bound() -> None:
    incumbent = Incumbent(
        tile_count=1,
        placement_witness=PlacementWitness((("L", ((0, 0), (1, 0), (1, 1))),)),
    )
    with pytest.raises(ValueError, match="lower_bound == incumbent objective"):
        SolverOutcome(
            status=OutcomeStatus.OPTIMAL,
            incumbent=incumbent,
            lower_bound=ProvenLowerBound(0),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
            ),
            backend_status=BackendStatusUnavailable(),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def test_feasible_outcome_is_not_reported_as_optimal() -> None:
    incumbent = Incumbent(
        tile_count=1,
        placement_witness=PlacementWitness((("L", ((0, 0), (1, 0), (1, 1))),)),
    )
    outcome = SolverOutcome(
        status=OutcomeStatus.FEASIBLE,
        incumbent=incumbent,
        lower_bound=LowerBoundUnavailable(),
        native_effort=NativeEffortUnavailable(
            NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT,
        ),
        backend_status=BackendStatusUnavailable(),
        backend_metrics_json="{}",
        error=NoSolverFailure(),
    )
    assert outcome.optimality_proven is False
