from __future__ import annotations

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
        [True, True, True, False],
        0.0,
    )
    assert positive == [0]
    assert negative == [2]


def test_threshold_one_keeps_only_fully_owned_points() -> None:
    positive, negative = stencil._ownership_regions(
        [1.0, 0.999, -1.0, -0.999],
        [True, True, True, True],
        1.0,
    )
    assert positive == [0]
    assert negative == [2]


def test_candidates_include_singletons_for_every_region_point() -> None:
    family = (("domino", ((0, 0), (0, 1))),)
    candidates = stencil._candidate_placements([0, 1, 2], 3, 1, family)
    candidate_cells = {points for _, points in candidates}
    assert (0,) in candidate_cells
    assert (1,) in candidate_cells
    assert (2,) in candidate_cells
    assert (0, 1) in candidate_cells
    assert (1, 2) in candidate_cells


def _mock_link() -> Any:
    return SimpleNamespace(
        mapping=SimpleNamespace(forward=lambda _eid: "internal-id"),
    )


def _query(capabilities: Dict[str, Any]) -> KataGoQuery:
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        opaque={
            "boardXSize": 2,
            "boardYSize": 2,
            "capabilities": capabilities,
        },
    )


def _response(*, during_search: bool = False) -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=during_search,
        turn_number=0,
        opaque={
            "ownership": [0.8, 0.7, -0.8, -0.7],
            "rootInfo": {"policy": [0.25, 0.25, 0.25, 0.25]},
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
    monkeypatch.setattr(stencil, "STENCIL_COVER_AVAILABLE", True)
    calls: List[List[int]] = []

    def fake_solve(
        cells: List[int], width: int, height: int, family: Any,
    ) -> Dict[str, Any]:
        calls.append(list(cells))
        return {
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
    assert annotation["threshold"] == 0.5
    assert annotation["positive"]["optimal"] is True
    assert annotation["negative"]["optimal"] is True
    assert annotation["optimal"] is True


def test_transformer_skips_when_policy_map_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(stencil, "STENCIL_COVER_AVAILABLE", True)
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
    out = transformer.on_response(ClientId("q3"), response)
    assert out is response
    assert "stencilCover" not in response.opaque


@pytest.mark.skipif(not stencil.HIGHS_AVAILABLE, reason="optional highspy dependency is absent")
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
