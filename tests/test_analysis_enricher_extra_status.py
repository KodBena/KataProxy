"""
tests/test_analysis_enricher_extra_status.py — Pins the ``extra_status``
wire contract that ``transformers/analysis_enricher.py`` attaches to
every response of a query for which enrichment was "in play"
(ANALYZE action + a truthy ``analysis_config``).

Covers every arm of the closed vocabulary — computed / skipped(reason) /
failed / not_applicable — plus the two structural guarantees:

  1. Monotonic wire: a query that never carries ``analysis_config`` on
     ANALYZE gets no ``extra_status`` key on any of its responses.
  2. Coupling invariant: ``r.opaque['extra']`` is present on a response
     iff ``r.opaque['extra_status']['state'] == "computed"``.

Drives the ``analysis_enricher`` Transformer directly — ``on_query`` /
``on_response`` — against a duck-typed ``ProxyLink`` stand-in, following
the pattern in ``tests/test_capability_negotiation.py`` (``_MockLink``
avoids coupling to KataGo's completion tracker; the real contract this
module cares about is ``link.mapping.forward(eid)``).

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional, cast

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink  # noqa: E402
from delta_analysis import DeltaAnalysisState  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    MetadataResponse,
)
from transformers.analysis_enricher import (  # noqa: E402
    _classify_setup_error,
    analysis_enricher,
)

_LinkT = ProxyLink[ClientId, InternalId]


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


class _MockMapping:
    """Mimics the ``IdMapping`` surface ``analysis_enricher`` consumes:
    ``forward(eid)`` returning ``Optional[InternalId]``. An eid is
    "active" (query still in flight) until explicitly marked done, at
    which point ``forward`` returns ``None`` — the same signal
    ``ProxyLink``'s real mapping gives once the last expected response
    for a query has cleared it.
    """

    def __init__(self) -> None:
        self._active: set[ClientId] = set()

    def mark_active(self, eid: ClientId) -> None:
        self._active.add(eid)

    def mark_done(self, eid: ClientId) -> None:
        self._active.discard(eid)

    def forward(self, eid: ClientId) -> Optional[InternalId]:
        return InternalId("internal") if eid in self._active else None


class _MockLink:
    def __init__(self) -> None:
        self.mapping = _MockMapping()


def _make_link() -> _MockLink:
    return _MockLink()


# ---------------------------------------------------------------------------
# Fixtures: a valid analysis_config, and query/response builders
# ---------------------------------------------------------------------------


def _valid_config() -> Dict[str, Any]:
    """A minimal analysis_config that compiles and runs cleanly."""
    return {
        "bindings": {
            "delta_fn": "vd",
            "summary_fn": "ms",
        },
        "parameters": {},
        "symbols": {
            "v": 'x["rootInfo"]["visits"]',
            "vd": 'x[1]["rootInfo"]["visits"] - x[0]["rootInfo"]["visits"]',
            "ms": "float(min(x))",
        },
    }


def _bad_syntax_config() -> Dict[str, Any]:
    """A config whose symbol body fails to compile — RegistryInterpreter
    raises RuntimeError at asteval compile time."""
    return {
        "bindings": {"delta_fn": "vd", "summary_fn": "ms"},
        "parameters": {},
        "symbols": {
            "vd": "x[1][",  # syntax error
            "ms": "float(min(x))",
        },
    }


# Sentinel distinguishing "caller didn't pass this" from "caller passed
# None/[] on purpose" (None means "omit analysis_config"; [] means "an
# empty moves list"). Typed Any so callers can pass either a dict, None,
# a list, or nothing without fighting mypy --strict over the union.
_UNSET: Any = object()


def _query(
    *,
    action: KataGoAction = KataGoAction.ANALYZE,
    config: Any = _UNSET,
    moves: Any = _UNSET,
) -> KataGoQuery:
    """Build a KataGoQuery. Defaults to a well-formed 3-move ANALYZE
    query carrying `_valid_config()`; pass explicit `config`/`moves` to
    override (including `None` to omit `analysis_config` entirely, or
    `[]` / a 1-element list to exercise the moves-length gate)."""
    opaque: Dict[str, Any] = {
        "boardXSize": 19,
        "boardYSize": 19,
        "rules": "tromp-taylor",
        "komi": 7.5,
    }
    opaque["moves"] = (
        [["B", "A1"], ["W", "A2"], ["B", "A3"]] if moves is _UNSET else moves
    )
    resolved_config = _valid_config() if config is _UNSET else config
    if resolved_config is not None:
        opaque["analysis_config"] = resolved_config
    return KataGoQuery(action=action, opaque=opaque)


def _analyze_response(
    *, turn_number: int, is_during_search: bool = False, with_move_infos: bool = True
) -> AnalyzeResponse:
    opaque: Dict[str, Any] = {"rootInfo": {"visits": 10 + turn_number, "scoreLead": 0.5}}
    if with_move_infos:
        opaque["moveInfos"] = [{"move": "A1", "visits": 10}]
    return AnalyzeResponse(
        is_during_search=is_during_search, turn_number=turn_number, opaque=opaque
    )


def _assert_coupling_invariant(r: Any) -> None:
    """The load-bearing half of the contract: 'extra' present on a
    response IFF extra_status.state == 'computed'."""
    status = r.opaque.get("extra_status")
    has_extra = "extra" in r.opaque
    is_computed = status is not None and status.get("state") == "computed"
    assert has_extra == is_computed, (
        f"coupling invariant violated: extra present={has_extra}, "
        f"status={status!r}"
    )


# ---------------------------------------------------------------------------
# 1. Monotonic wire: no analysis_config in play -> no extra_status key ever
# ---------------------------------------------------------------------------


def test_no_analysis_config_yields_no_extra_status_key() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c1")
    link.mapping.mark_active(eid)

    q = _query(config=None)
    out_q = xform.on_query(eid, q)
    assert out_q is q  # untouched

    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert "extra_status" not in out_r.opaque
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


def test_non_analyze_action_with_stale_config_not_in_play() -> None:
    """A TERMINATE query that happens to still carry `analysis_config`
    in its cloned opaque (per the on_query docstring's sub-query
    scenario) must not be treated as 'in play' — the ANALYZE-action
    check is part of the gate."""
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c2")
    link.mapping.mark_active(eid)

    q = _query(action=KataGoAction.TERMINATE, config=_valid_config())
    xform.on_query(eid, q)

    r = MetadataResponse(opaque={"terminateId": "x"})
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert "extra_status" not in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 2. skipped / too_few_moves — the on_query moves-length gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("moves", [[], [["B", "A1"]]], ids=["empty", "single"])
def test_too_few_moves_reason(moves: Any) -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c3")
    link.mapping.mark_active(eid)

    q = _query(moves=moves)
    xform.on_query(eid, q)

    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {
        "state": "skipped",
        "reason": "too_few_moves",
    }
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 3. skipped / config_error — RegistryInterpreter compile failure
# ---------------------------------------------------------------------------


def test_config_error_reason_persists_across_every_response_of_the_query() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c4")
    link.mapping.mark_active(eid)

    q = _query(config=_bad_syntax_config())
    xform.on_query(eid, q)

    for turn in range(3):
        r = _analyze_response(turn_number=turn)
        out_r = xform.on_response(eid, r)
        assert out_r is not None
        assert out_r.opaque["extra_status"] == {
            "state": "skipped",
            "reason": "config_error",
        }
        assert "extra" not in out_r.opaque
        _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 4. skipped / invalid_moves — DeltaAnalysisState's color-token guard
# ---------------------------------------------------------------------------


def test_invalid_moves_reason() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c5")
    link.mapping.mark_active(eid)

    q = _query(moves=[["B", "A1"], ["X", "A2"]])
    xform.on_query(eid, q)

    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {
        "state": "skipped",
        "reason": "invalid_moves",
    }
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 5. computed — the success path
# ---------------------------------------------------------------------------


def test_computed_state_and_extra_populated() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c6")
    link.mapping.mark_active(eid)

    q = _query()
    xform.on_query(eid, q)

    r = _analyze_response(turn_number=1)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {"state": "computed"}
    assert "extra" in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 6. failed — setup succeeded but this packet's push_packet raised
# ---------------------------------------------------------------------------


def test_failed_state_on_per_packet_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raising_push_packet(self: Any, move_idx: int, packet: Any) -> Any:
        raise RuntimeError("synthetic per-packet enrichment failure")

    monkeypatch.setattr(DeltaAnalysisState, "push_packet", _raising_push_packet)

    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c7")
    link.mapping.mark_active(eid)

    q = _query()
    xform.on_query(eid, q)  # setup succeeds; DeltaAnalysisState.__init__ unpatched

    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {
        "state": "failed",
        "reason": "enrichment_exception",
    }
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 7. not_applicable — enrichment in play, but this response isn't enrichable
# ---------------------------------------------------------------------------


def test_not_applicable_for_analyze_response_without_move_infos() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c8")
    link.mapping.mark_active(eid)

    q = _query()
    xform.on_query(eid, q)

    r = _analyze_response(turn_number=0, with_move_infos=False)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {"state": "not_applicable"}
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


def test_not_applicable_for_metadata_response() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c9")
    link.mapping.mark_active(eid)

    q = _query()
    xform.on_query(eid, q)

    r = MetadataResponse(opaque={})
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {"state": "not_applicable"}
    assert "extra" not in out_r.opaque
    _assert_coupling_invariant(out_r)


# ---------------------------------------------------------------------------
# 8. Cleanup: skip_reasons/request_cache pop on the forward(eid)-is-None
#    condition, same as the pre-existing request_cache cleanup.
# ---------------------------------------------------------------------------


def test_state_is_cleaned_up_once_mapping_forward_returns_none() -> None:
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c10")
    link.mapping.mark_active(eid)

    q = _query(config=_bad_syntax_config())
    xform.on_query(eid, q)

    # Query completes: the real ProxyLink mapping would now report
    # forward(eid) is None (mapping torn down after the last expected
    # response). The response that observes this triggers cleanup.
    link.mapping.mark_done(eid)
    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {
        "state": "skipped",
        "reason": "config_error",
    }

    # A stray further call for the same eid (e.g. a late duplicate)
    # after cleanup sees no cached skip reason and gets no extra_status
    # key at all — proof the internal dict was actually popped, not
    # just left stale-but-unused.
    r2 = _analyze_response(turn_number=1)
    out_r2 = xform.on_response(eid, r2)
    assert out_r2 is not None
    assert "extra_status" not in out_r2.opaque
    _assert_coupling_invariant(out_r2)


def test_computed_state_is_cleaned_up_too() -> None:
    """Same cleanup contract on the success (request_cache) side."""
    link = _make_link()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("c11")
    link.mapping.mark_active(eid)

    q = _query()
    xform.on_query(eid, q)

    link.mapping.mark_done(eid)
    r = _analyze_response(turn_number=0)
    out_r = xform.on_response(eid, r)
    assert out_r is not None
    assert out_r.opaque["extra_status"] == {"state": "computed"}

    r2 = _analyze_response(turn_number=1)
    out_r2 = xform.on_response(eid, r2)
    assert out_r2 is not None
    assert "extra_status" not in out_r2.opaque
    _assert_coupling_invariant(out_r2)


# ---------------------------------------------------------------------------
# 9. _classify_setup_error unit coverage (the closed reason vocabulary,
#    including arms not naturally reachable through the full on_query
#    plumbing, e.g. a bare TypeError from the curated stdlib).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc,expected",
    [
        (RuntimeError("[RegistryInterpreter] compile error in 'vd':\nboom"), "config_error"),
        (ValueError("n_moves must be >= 2 to form at least one delta"), "too_few_moves"),
        (
            ValueError("invalid move color token 'X' at move index 1; expected one of 'B', 'b', 'W', 'w'"),
            "invalid_moves",
        ),
        (ValueError("array size 500 exceeds element-count cap 256"), "config_error"),
        (TypeError("window size must be an integer, got str"), "config_error"),
    ],
    ids=[
        "runtime_error_compile",
        "value_error_too_few_moves",
        "value_error_invalid_color",
        "value_error_other_is_config_error",
        "type_error_is_config_error",
    ],
)
def test_classify_setup_error(exc: Exception, expected: str) -> None:
    assert _classify_setup_error(exc) == expected
