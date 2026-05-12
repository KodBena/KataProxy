"""
tests/test_protocol_parser.py — Unit tests for the KataGo wire-protocol
parser in katago/katago_proxy.py.

Covers:

  - The closed-set action vocabulary (ADR-0002 fail-loud), the
    vanilla-KataGo-compat default for a missing `action` key, the
    query-side wire round-trip, and the dispatcher-side prism gating
    that keeps the receive loop alive on unknown actions (audit H-3).

  - The response-side discriminated union (`AnalyzeResponse |
    MetadataResponse`), structural variant discrimination, the
    half-present-fields protocol violation, the wire transparency
    pin for metadata responses (regression test for the v1.0.12 bug),
    `response_completion_signal`'s bridge to CompletionTracker, and
    `_response_with_terminate_id`'s variant preservation.

Run from the proxy directory: `pytest tests/test_protocol_parser.py`.
"""

from __future__ import annotations

import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, Dict

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import (  # noqa: E402
    AnalyzeResponse,
    KATAGO_QUERY_PRISMS,
    KataGoAction,
    KataGoQuery,
    MetadataResponse,
    parse_query_from_wire,
    parse_response_from_wire,
    response_completion_signal,
    translate_query_to_wire,
    translate_response_to_wire,
)
# RESPONSE_TERMINATE_ID_FIELD is a referential-field binding, used by tests
# to exercise the variant-preserving _response_with_terminate_id contract.
# Not part of katago/__init__.py's public re-exports — pulled directly from
# the submodule.
from katago.katago_proxy import RESPONSE_TERMINATE_ID_FIELD  # noqa: E402
from AbstractProxy.proxy_core import Dispatcher  # noqa: E402


# ---------------------------------------------------------------------------
# Action vocabulary — every wire string the closed set accepts must round
# through to its KataGoAction member, and every member must serialise back
# to the same wire string. The parametrised list is the witness; if a new
# action is added to KataGoAction without updating this test, the round-trip
# member-count check at the bottom of the module will fail.
# ---------------------------------------------------------------------------

_KNOWN_ACTIONS: list[tuple[str, KataGoAction]] = [
    ("analyze", KataGoAction.ANALYZE),
    ("terminate", KataGoAction.TERMINATE),
    ("terminate_all", KataGoAction.TERMINATE_ALL),
    ("query_version", KataGoAction.QUERY_VERSION),
    ("query_models", KataGoAction.QUERY_MODELS),
    ("clear_cache", KataGoAction.CLEAR_CACHE),
]


@pytest.mark.parametrize("wire_str,expected_action", _KNOWN_ACTIONS)
def test_parse_known_actions(wire_str: str, expected_action: KataGoAction) -> None:
    envelope_id, query = parse_query_from_wire({"id": "x", "action": wire_str})
    assert envelope_id == "x"
    assert query.action is expected_action


@pytest.mark.parametrize("wire_str,action", _KNOWN_ACTIONS)
def test_translate_non_analyze_actions_keep_action_field(
    wire_str: str, action: KataGoAction
) -> None:
    """Regression test: translate must not strip `action` for non-ANALYZE actions.

    The original `query_models` bug was a silent fallback to ANALYZE in the
    parser combined with translate's `if action != ANALYZE: wire["action"] = ...`,
    which dropped the action on the wire and sent a malformed analyze query
    to KataGo. This test pins the contract from the translate side.
    """
    wire = translate_query_to_wire(KataGoQuery(action=action), envelope_id="env-1")
    if action is KataGoAction.ANALYZE:
        assert "action" not in wire  # vanilla-KataGo: analyze is implicit
    else:
        assert wire["action"] == wire_str


def test_missing_action_defaults_to_analyze() -> None:
    """Vanilla-KataGo compat: a wire dict with no `action` key is analyze."""
    _, query = parse_query_from_wire({"id": "x"})
    assert query.action is KataGoAction.ANALYZE


def test_unknown_action_raises_value_error() -> None:
    """ADR-0002: an unknown action string is a protocol violation."""
    with pytest.raises(ValueError, match="unknown KataGo wire action"):
        parse_query_from_wire({"id": "x", "action": "query_models_v2"})


def test_unknown_action_error_lists_known_set() -> None:
    """The error message names the closed set so an operator can spot a
    typo or a vocabulary drift without grepping the source."""
    with pytest.raises(ValueError) as exc_info:
        parse_query_from_wire({"id": "x", "action": "unknown_thing"})
    msg = str(exc_info.value)
    for known, _ in _KNOWN_ACTIONS:
        assert known in msg


# ---------------------------------------------------------------------------
# Opaque pass-through and round-trip
# ---------------------------------------------------------------------------

def test_opaque_fields_round_trip_through_query_models() -> None:
    """Unknown wire keys flow through the parser into `opaque`, and the
    translator places them back on the wire. The proxy never inspects them.
    """
    incoming = {"id": "x", "action": "query_models", "extra": 42, "tag": "abc"}
    envelope_id, query = parse_query_from_wire(incoming)
    assert envelope_id == "x"
    assert query.action is KataGoAction.QUERY_MODELS
    assert query.opaque == {"extra": 42, "tag": "abc"}

    outgoing = translate_query_to_wire(query, envelope_id="wire-7")
    assert outgoing["id"] == "wire-7"
    assert outgoing["action"] == "query_models"
    assert outgoing["extra"] == 42
    assert outgoing["tag"] == "abc"


# ---------------------------------------------------------------------------
# Dispatcher / prism gating — receive-loop side
#
# These tests pin the audit-H-3 property: an unknown action must NOT raise
# into the receive loop. The dispatcher emits a no-match (None) which
# proxy_server's _handle_incoming turns into a structured ERROR log without
# tearing down the connection.
# ---------------------------------------------------------------------------

def test_dispatcher_matches_query_models() -> None:
    dispatcher = Dispatcher(KATAGO_QUERY_PRISMS)
    result = dispatcher.match({"id": "x", "action": "query_models"})
    assert result is not None
    prism, env_id, query = result
    assert prism.name == "action"
    assert env_id == "x"
    assert query.action is KataGoAction.QUERY_MODELS


def test_dispatcher_no_match_for_unknown_action() -> None:
    """Unknown action → no prism matches → dispatcher returns None.

    proxy_server's _handle_incoming is responsible for emitting the
    structured ERROR log on this path; the parser does not raise into
    the receive loop.
    """
    dispatcher = Dispatcher(KATAGO_QUERY_PRISMS)
    result = dispatcher.match({"id": "x", "action": "query_models_v2"})
    assert result is None


def test_dispatcher_matches_analyze_with_no_action_key() -> None:
    dispatcher = Dispatcher(KATAGO_QUERY_PRISMS)
    result = dispatcher.match({"id": "x"})
    assert result is not None
    prism, env_id, query = result
    assert prism.name == "analyze"
    assert env_id == "x"
    assert query.action is KataGoAction.ANALYZE


def test_dispatcher_matches_terminate() -> None:
    dispatcher = Dispatcher(KATAGO_QUERY_PRISMS)
    result = dispatcher.match({"id": "x", "action": "terminate", "terminateId": "y"})
    assert result is not None
    prism, env_id, query = result
    assert prism.name == "terminate"
    assert env_id == "x"
    assert query.action is KataGoAction.TERMINATE
    assert query.terminate_id == "y"


# ---------------------------------------------------------------------------
# Vocabulary completeness — guard against an enum member without a wire
# string (or vice versa).
# ---------------------------------------------------------------------------

def test_every_enum_member_has_a_wire_string() -> None:
    """If a new KataGoAction is added but the parametrised _KNOWN_ACTIONS
    list above is not updated, this test fails — keeping the action map,
    the enum, and the test vocabulary in sync."""
    enum_members = {action for _, action in _KNOWN_ACTIONS}
    assert enum_members == set(KataGoAction), (
        f"KataGoAction members not covered by parser tests: "
        f"{set(KataGoAction) - enum_members}"
    )


# ===========================================================================
# Response-side tests — discriminated-union variants
# ===========================================================================
#
# v1.0.13 split KataGoResponse into AnalyzeResponse | MetadataResponse to
# eliminate the v1.0.12 transparency bug where metadata responses
# (query_version, query_models, terminate ack) were polluted with
# fabricated isDuringSearch=False / turnNumber=0 fields. These tests pin
# the variant discrimination, the wire round-trip transparency for both
# variants, the half-present-fields protocol violation, the
# response-completion-signal bridge, and `_response_with_terminate_id`'s
# variant preservation.
# ===========================================================================


# ---------------------------------------------------------------------------
# Parse: variant discrimination
# ---------------------------------------------------------------------------

def test_parse_analyze_response_partial() -> None:
    """A wire dict with isDuringSearch=true and turnNumber parses as
    AnalyzeResponse with both fields populated."""
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "isDuringSearch": True,
        "turnNumber": 5,
        "moveInfos": [{"move": "C3", "visits": 100}],
    })
    assert eid == "kg_1"
    assert isinstance(r, AnalyzeResponse)
    assert r.is_during_search is True
    assert r.turn_number == 5
    assert r.opaque == {"moveInfos": [{"move": "C3", "visits": 100}]}


def test_parse_analyze_response_final() -> None:
    """isDuringSearch=false on an analyze response still parses as
    AnalyzeResponse (the variant is structural, not value-driven)."""
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "isDuringSearch": False,
        "turnNumber": 7,
        "rootInfo": {"scoreLead": 1.5},
    })
    assert isinstance(r, AnalyzeResponse)
    assert r.is_during_search is False
    assert r.turn_number == 7


def test_parse_query_version_response_is_metadata() -> None:
    """A query_version response wire (no isDuringSearch / no turnNumber)
    parses as MetadataResponse with the version payload in opaque."""
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "version": "1.13.0",
        "git_hash": "abcdef",
    })
    assert eid == "kg_1"
    assert isinstance(r, MetadataResponse)
    assert r.opaque == {"version": "1.13.0", "git_hash": "abcdef"}


def test_parse_query_models_response_is_metadata() -> None:
    """A query_models response wire parses as MetadataResponse — the
    direct regression test for the v1.0.12 transparency bug."""
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "models": [{"internalName": "model_a", "fileName": "a.bin.gz"}],
    })
    assert isinstance(r, MetadataResponse)
    assert r.opaque == {
        "models": [{"internalName": "model_a", "fileName": "a.bin.gz"}],
    }


def test_parse_terminate_ack_is_metadata() -> None:
    """A terminate ack (KataGo's verbatim echo) parses as MetadataResponse."""
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "action": "terminate",
        "terminateId": "kg_target",
    })
    assert isinstance(r, MetadataResponse)
    assert r.opaque == {"action": "terminate", "terminateId": "kg_target"}


def test_parse_clear_cache_ack_is_metadata() -> None:
    eid, r = parse_response_from_wire({
        "id": "kg_1",
        "action": "clear_cache",
    })
    assert isinstance(r, MetadataResponse)
    assert r.opaque == {"action": "clear_cache"}


@pytest.mark.parametrize("missing_key,present_key,present_value", [
    ("isDuringSearch", "turnNumber", 0),
    ("turnNumber", "isDuringSearch", False),
])
def test_parse_half_present_fields_raises(
    missing_key: str, present_key: str, present_value: object
) -> None:
    """Half-present fields are a structural protocol violation per ADR-0002."""
    with pytest.raises(ValueError, match="exactly one of"):
        parse_response_from_wire({"id": "kg_1", present_key: present_value})


def test_parse_half_present_error_names_keys() -> None:
    """The error message lists the offending keys so an operator can
    diagnose without rerunning."""
    with pytest.raises(ValueError) as exc_info:
        parse_response_from_wire({"id": "kg_1", "isDuringSearch": True})
    assert "isDuringSearch" in str(exc_info.value)
    assert "id" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Translate: wire transparency for both variants
# ---------------------------------------------------------------------------

def test_translate_analyze_response_carries_both_fields() -> None:
    wire = translate_response_to_wire(
        AnalyzeResponse(
            is_during_search=True,
            turn_number=3,
            opaque={"moveInfos": []},
        ),
        envelope_id="cli_1",
    )
    assert wire == {
        "id": "cli_1",
        "isDuringSearch": True,
        "turnNumber": 3,
        "moveInfos": [],
    }


def test_translate_metadata_response_carries_neither_field() -> None:
    """Regression pin for the v1.0.12 bug: a MetadataResponse must NOT
    emit isDuringSearch or turnNumber on the wire."""
    wire = translate_response_to_wire(
        MetadataResponse(opaque={"version": "1.13.0"}),
        envelope_id="cli_1",
    )
    assert wire == {"id": "cli_1", "version": "1.13.0"}
    assert "isDuringSearch" not in wire
    assert "turnNumber" not in wire


def test_translate_query_models_response_is_transparent() -> None:
    """End-to-end transparency: a query_models wire dict from KataGo
    round-trips out to the client byte-for-byte (modulo id rewriting),
    with no fabricated fields. Direct regression test for the user-
    visible bug."""
    katago_wire = {
        "id": "wire_1",
        "models": [{"internalName": "kata1-b18c384nbt-s9131461376-d4087399203"}],
    }
    eid, r = parse_response_from_wire(katago_wire)
    out_wire = translate_response_to_wire(r, "cli_1")
    assert out_wire == {
        "id": "cli_1",
        "models": [{"internalName": "kata1-b18c384nbt-s9131461376-d4087399203"}],
    }


# ---------------------------------------------------------------------------
# Round-trip: parse → translate → parse is identity (modulo envelope id)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wire", [
    # Analyze partial
    {"id": "x", "isDuringSearch": True, "turnNumber": 0, "moveInfos": []},
    # Analyze final
    {"id": "x", "isDuringSearch": False, "turnNumber": 12, "rootInfo": {}},
    # query_version
    {"id": "x", "version": "1.13.0"},
    # query_models
    {"id": "x", "models": [{"internalName": "n"}]},
    # terminate ack
    {"id": "x", "action": "terminate", "terminateId": "y"},
    # clear_cache ack
    {"id": "x", "action": "clear_cache"},
    # error response (metadata-shaped)
    {"id": "x", "error": "out of memory"},
])
def test_response_round_trip_is_identity(wire: Dict[str, Any]) -> None:
    eid, r = parse_response_from_wire(wire)
    out = translate_response_to_wire(r, eid)
    assert out == wire


# ---------------------------------------------------------------------------
# response_completion_signal — the variant-to-tracker bridge
# ---------------------------------------------------------------------------

def test_completion_signal_analyze_partial() -> None:
    sig = response_completion_signal(
        AnalyzeResponse(is_during_search=True, turn_number=4)
    )
    assert sig == (4, True)


def test_completion_signal_analyze_final() -> None:
    sig = response_completion_signal(
        AnalyzeResponse(is_during_search=False, turn_number=11)
    )
    assert sig == (11, False)


def test_completion_signal_metadata_synthesises_zero_false() -> None:
    """Metadata responses are single-shot; the synthetic (0, False)
    pairs with the `[0]` discriminator set that
    register_query_completion installs for non-analyze queries."""
    sig = response_completion_signal(MetadataResponse(opaque={"version": "1"}))
    assert sig == (0, False)


# ---------------------------------------------------------------------------
# _response_with_terminate_id — variant preservation under functional update
# ---------------------------------------------------------------------------

def test_terminate_id_update_preserves_analyze_variant() -> None:
    original = AnalyzeResponse(
        is_during_search=False,
        turn_number=2,
        opaque={"terminateId": "old", "moveInfos": []},
    )
    updated = RESPONSE_TERMINATE_ID_FIELD.set(original, "new")
    assert isinstance(updated, AnalyzeResponse)
    assert updated.is_during_search is False
    assert updated.turn_number == 2
    assert updated.opaque["terminateId"] == "new"
    assert updated.opaque["moveInfos"] == []  # other opaque fields preserved


def test_terminate_id_update_preserves_metadata_variant() -> None:
    """The common case in practice — terminate acks are metadata-shaped."""
    original = MetadataResponse(
        opaque={"action": "terminate", "terminateId": "old"},
    )
    updated = RESPONSE_TERMINATE_ID_FIELD.set(original, "new")
    assert isinstance(updated, MetadataResponse)
    assert updated.opaque["terminateId"] == "new"
    assert updated.opaque["action"] == "terminate"


def test_terminate_id_update_returns_new_instance() -> None:
    """The original is not mutated — frozen dataclass posture is honest."""
    original = MetadataResponse(opaque={"terminateId": "old"})
    updated = RESPONSE_TERMINATE_ID_FIELD.set(original, "new")
    assert original.opaque["terminateId"] == "old"
    assert updated is not original


# ---------------------------------------------------------------------------
# Frozen-dataclass posture
# ---------------------------------------------------------------------------

def test_analyze_response_is_frozen() -> None:
    """Frozen dataclasses prevent accidental mid-pipeline mutation of
    the variant's discriminating fields."""
    r = AnalyzeResponse(is_during_search=False, turn_number=0)
    with pytest.raises(FrozenInstanceError):
        r.is_during_search = True  # type: ignore[misc]


def test_metadata_response_is_frozen() -> None:
    r = MetadataResponse()
    with pytest.raises(FrozenInstanceError):
        r.opaque = {}  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Per-action response-shape pinning — every non-analyze action's response
# wire parses as MetadataResponse, every analyze response wire parses as
# AnalyzeResponse. Mirrors the action-side _KNOWN_ACTIONS coverage
# parametrisation but for the response side.
# ---------------------------------------------------------------------------

_METADATA_RESPONSE_SAMPLES: list[tuple[str, Dict[str, Any]]] = [
    ("query_version", {"id": "x", "version": "1.13.0"}),
    ("query_models", {"id": "x", "models": []}),
    ("clear_cache", {"id": "x", "action": "clear_cache"}),
    ("terminate", {"id": "x", "action": "terminate", "terminateId": "y"}),
]


@pytest.mark.parametrize("action_name,wire", _METADATA_RESPONSE_SAMPLES)
def test_non_analyze_responses_parse_as_metadata(
    action_name: str, wire: Dict[str, Any]
) -> None:
    _, r = parse_response_from_wire(wire)
    assert isinstance(r, MetadataResponse), (
        f"Action {action_name!r}'s response wire {wire!r} parsed as "
        f"{type(r).__name__}, expected MetadataResponse"
    )
