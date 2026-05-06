"""
test_protocol_parser.py — Unit tests for the KataGo wire-protocol parser
in AbstractProxy/katago_proxy.py.

Covers the closed-set action vocabulary (ADR-0002 fail-loud), the
vanilla-KataGo-compat default for a missing `action` key, the wire
roundtrip, and the dispatcher-side prism gating that keeps the
receive loop alive on unknown actions (audit H-3).

Run from the proxy directory: `pytest tests/test_protocol_parser.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.katago_proxy import (  # noqa: E402
    KATAGO_QUERY_PRISMS,
    KataGoAction,
    KataGoQuery,
    parse_query_from_wire,
    translate_query_to_wire,
)
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
