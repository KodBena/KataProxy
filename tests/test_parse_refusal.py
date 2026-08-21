"""
tests/test_parse_refusal.py — Parse-layer request-reply totality.

Contract under test (the `cache_attach` incident's fix): a well-formed
JSON dict that *looks like a query* (carries `action` or `id`) but
matches no prism is refused TO THE PARTY THAT ASKED with the proxy's
existing structured-error shape ``{"id"?, "error", "field"?}`` — not
only into the operator log. Everything the proxy already accepts keeps
flowing exactly as before, and everything alien (non-JSON, non-dict,
dict with neither `action` nor `id`) stays silent so the bot-noise
floor never reaches the wire.

The tests drive `ClientSession._handle_incoming` directly with a fake
WebSocket and a recording hub — the same seam the diagnose_* scripts
use, minus the transport.

Run from the proxy directory: `pytest tests/test_parse_refusal.py`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import SUPPORTED_WIRE_ACTIONS  # noqa: E402
from AbstractProxy.proxy_core import CanonicalId  # noqa: E402
from proxy_server import ClientSession  # noqa: E402


class _FakeWs:
    """Collects frames the session sends; never receives."""

    def __init__(self) -> None:
        self.sent: List[str] = []
        self.remote_address: Tuple[str, int] = ("127.0.0.1", 9)

    async def send(self, frame: str) -> None:
        self.sent.append(frame)


class _RecordingHub:
    """Records subscribe calls; reports every query as already-known so
    no router dispatch is attempted (the refusal path must never get
    that far anyway)."""

    def __init__(self) -> None:
        self.subscriptions: List[Any] = []

    def subscribe(self, **kwargs: Any) -> Tuple[bool, CanonicalId]:
        self.subscriptions.append(kwargs)
        return (False, CanonicalId("canon-1"))

    def unsubscribe(self, *a: Any, **k: Any) -> bool:
        return False


class _NeverRouter:
    """The refusal path must not touch the router at all."""

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        raise AssertionError(f"router unexpectedly touched: {name}")


def _make_session() -> Tuple[ClientSession, _FakeWs, _RecordingHub]:
    ws = _FakeWs()
    hub = _RecordingHub()
    session = ClientSession(
        ws=ws,
        peer="test-peer",
        hub=hub,  # type: ignore[arg-type]
        router=_NeverRouter(),  # type: ignore[arg-type]
    )
    return session, ws, hub


def _only_reply(ws: _FakeWs) -> Dict[str, Any]:
    assert len(ws.sent) == 1, f"expected exactly one reply, got {ws.sent!r}"
    return json.loads(ws.sent[0])


# ---------------------------------------------------------------------------
# The witnessed defect: unknown action, extractable id.
# ---------------------------------------------------------------------------

async def test_unknown_action_gets_structured_refusal() -> None:
    session, ws, hub = _make_session()
    await session._handle_incoming(json.dumps(
        {"id": "att1", "action": "cache_attach",
         "model": "main", "context": "lvtest1"}
    ))
    reply = _only_reply(ws)
    assert reply["id"] == "att1"
    assert reply["field"] == "action"
    assert "cache_attach" in reply["error"]
    for accepted in SUPPORTED_WIRE_ACTIONS:
        assert accepted in reply["error"]
    assert hub.subscriptions == []


async def test_vocabulary_action_without_id_refused_on_id_field() -> None:
    session, ws, hub = _make_session()
    await session._handle_incoming(json.dumps({"action": "query_version"}))
    reply = _only_reply(ws)
    assert "id" not in reply
    assert reply["field"] == "id"
    assert hub.subscriptions == []


# ---------------------------------------------------------------------------
# Hostile id shapes: refusal must not launder them back.
# ---------------------------------------------------------------------------

async def test_non_string_id_not_echoed() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps(
        {"id": {"nested": True}, "action": "cache_attach"}
    ))
    reply = _only_reply(ws)
    assert "id" not in reply
    assert reply["field"] == "action"


async def test_oversized_id_not_echoed() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps(
        {"id": "x" * 5000, "action": "cache_attach"}
    ))
    reply = _only_reply(ws)
    assert "id" not in reply


async def test_oversized_action_value_truncated_in_error_text() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps(
        {"id": "a1", "action": "y" * 5000}
    ))
    reply = _only_reply(ws)
    assert len(reply["error"]) < 1000
    assert "truncated" in reply["error"]


async def test_unhashable_action_value_refused_not_raised() -> None:
    # `"action": {}` must compare against the tuple vocabulary by
    # equality; a frozenset membership probe would raise TypeError and
    # tear into the receive loop.
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps(
        {"id": "a2", "action": {"deep": []}}
    ))
    reply = _only_reply(ws)
    assert reply["field"] == "action"
    assert reply["id"] == "a2"


# ---------------------------------------------------------------------------
# Silence is preserved where silence is the contract.
# ---------------------------------------------------------------------------

async def test_alien_dict_stays_silent() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps({"foo": "bar"}))
    assert ws.sent == []


async def test_non_dict_json_stays_silent() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming(json.dumps([1, 2, 3]))
    assert ws.sent == []


async def test_non_json_stays_silent() -> None:
    session, ws, _ = _make_session()
    await session._handle_incoming("this is not json {")
    assert ws.sent == []


# ---------------------------------------------------------------------------
# Accepted traffic is untouched: no refusal frame, normal subscribe.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wire", [
    {"id": "q1", "moves": [], "rules": "tromp-taylor"},   # implicit analyze
    {"id": "q2", "action": "query_version"},
    {"id": "q3", "action": "query_models"},
    {"id": "q4", "action": "clear_cache"},
])
async def test_accepted_queries_flow_without_refusal(
    wire: Dict[str, Any],
) -> None:
    session, ws, hub = _make_session()
    await session._handle_incoming(json.dumps(wire))
    assert ws.sent == [], f"accepted query drew a reply: {ws.sent!r}"
    assert len(hub.subscriptions) == 1
