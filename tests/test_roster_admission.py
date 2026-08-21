"""
tests/test_roster_admission.py — RELAY roster admission check (v1.0.33).

Contract under test:

  - `_probe_roster` extracts the searchable-model roster from one
    `query_models` reply, handling BOTH engine generations with one
    shape (vanilla fork-point ccdec959: same per-element keys, array of
    at most primary+human; model-and-cache branch: N searchable models
    + optional human last). Human-profile entries are excluded (not
    `model`-resolvable targets). Timeouts / refusals / malformed
    replies refuse admission loudly.
  - `_verify_roster` enforces set-equality against the connected
    ring's agreed roster; an empty ring re-seeds.
  - `_connect` refuses a divergent member BEFORE it enters
    `_connections` (socket closed), and RAISES on every failure — the
    contract `_reconnect_with_backoff` depends on to actually retry
    (pre-v1.0.33, a swallowed failure ended reconnection after one
    attempt, which would also have silently skipped roster
    re-verification).
  - Disconnect removes the member's roster, so reconnection re-probes
    (roster changes require an engine restart, which drops the
    socket — reconnect-time re-verification is complete coverage).

Run from the proxy directory: `pytest tests/test_roster_admission.py`.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

import sproxy_config as cfg  # noqa: E402
from router import (  # noqa: E402
    InFlightQueryLoad,
    RelayRouter,
    RosterAdmissionError,
)


def _relay(urls: List[str]) -> RelayRouter:
    return RelayRouter(upstream_urls=urls, load_metric=InFlightQueryLoad())


def _models_reply(probe_id: str, entries: List[Dict[str, Any]]) -> str:
    return json.dumps({
        "id": probe_id, "action": "query_models", "models": entries,
    })


class _ScriptedWs:
    """Replays scripted recv frames; captures sends; records close."""

    def __init__(self, script) -> None:
        # script: callable(sent_frames) -> list of frames to serve, or
        # a plain list. Evaluated lazily so replies can echo probe ids.
        self._script = script
        self.sent: List[str] = []
        self.closed = False
        self._served = 0

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def recv(self) -> str:
        frames = (
            self._script(self.sent) if callable(self._script)
            else self._script
        )
        if self._served >= len(frames):
            # Nothing more scripted: park forever. An Event (never a
            # sleep) so tests that no-op asyncio.sleep for backoff
            # loops don't turn this park into a busy return.
            await asyncio.Event().wait()
        frame = frames[self._served]
        self._served += 1
        return frame

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> "_ScriptedWs":
        return self

    async def __anext__(self) -> str:
        # Models a healthy, quiet admitted connection: the read loop
        # parks here until the test's stop() cancels it. An Event, not
        # a sleep — see recv().
        await asyncio.Event().wait()
        raise StopAsyncIteration


def _probe_id_of(ws: _ScriptedWs) -> str:
    return json.loads(ws.sent[0])["id"]


# ---------------------------------------------------------------------------
# _probe_roster
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestProbeRoster:
    async def test_branch_shape_excludes_human_model(self) -> None:
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs(lambda sent: [_models_reply(
            json.loads(sent[0])["id"],
            [
                {"internalName": "m1", "name": "/n/1.gz",
                 "usesHumanSLProfile": False},
                {"internalName": "m2", "name": "/n/2.gz",
                 "usesHumanSLProfile": False},
                {"internalName": "humanv0", "name": "/n/h.gz",
                 "usesHumanSLProfile": True},
            ],
        )])
        roster = await router._probe_roster(ws, "ws://a:1")
        assert roster == frozenset({"m1", "m2"})
        sent = json.loads(ws.sent[0])
        # Bare probe: the branch engine hard-refuses model/cacheContext
        # on query_models.
        assert set(sent.keys()) == {"id", "action"}

    async def test_vanilla_shape_single_model(self) -> None:
        # Vanilla (ccdec959) carries the same keys; usesHumanSLProfile
        # False on the one searchable model.
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs(lambda sent: [_models_reply(
            json.loads(sent[0])["id"],
            [{"internalName": "g170", "name": "/n/g.gz",
              "usesHumanSLProfile": False, "maxBatchSize": 8,
              "version": 8, "usingFP16": "auto"}],
        )])
        roster = await router._probe_roster(ws, "ws://a:1")
        assert roster == frozenset({"g170"})

    async def test_stray_frames_are_skipped(self) -> None:
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs(lambda sent: [
            json.dumps({"id": "stale-analyze", "isDuringSearch": False,
                        "turnNumber": 0}),
            _models_reply(json.loads(sent[0])["id"],
                          [{"internalName": "m1"}]),
        ])
        roster = await router._probe_roster(ws, "ws://a:1")
        assert roster == frozenset({"m1"})

    async def test_error_reply_refuses(self) -> None:
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs(lambda sent: [json.dumps({
            "id": json.loads(sent[0])["id"],
            "error": "boom", "field": "action",
        })])
        with pytest.raises(RosterAdmissionError, match="refused the roster probe"):
            await router._probe_roster(ws, "ws://a:1")

    async def test_missing_models_array_refuses(self) -> None:
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs(lambda sent: [json.dumps({
            "id": json.loads(sent[0])["id"], "action": "query_models",
        })])
        with pytest.raises(RosterAdmissionError, match="without a 'models' array"):
            await router._probe_roster(ws, "ws://a:1")

    async def test_timeout_refuses(self, monkeypatch: Any) -> None:
        monkeypatch.setattr(cfg, "ROSTER_PROBE_TIMEOUT_S", 0.05)
        router = _relay(["ws://a:1"])
        ws = _ScriptedWs([])  # never answers
        with pytest.raises(RosterAdmissionError, match="no query_models reply"):
            await router._probe_roster(ws, "ws://a:1")


# ---------------------------------------------------------------------------
# _verify_roster
# ---------------------------------------------------------------------------

class TestVerifyRoster:
    def test_empty_ring_seeds(self) -> None:
        router = _relay(["ws://a:1"])
        router._verify_roster("ws://a:1", frozenset({"m1"}))  # no raise

    def test_equal_roster_admits(self) -> None:
        router = _relay(["ws://a:1", "ws://b:2"])
        router._connections["ws://a:1"] = object()
        router._roster_for_url["ws://a:1"] = frozenset({"m1", "m2"})
        router._verify_roster("ws://b:2", frozenset({"m2", "m1"}))

    def test_divergent_roster_refused_naming_both(self) -> None:
        router = _relay(["ws://a:1", "ws://b:2"])
        router._connections["ws://a:1"] = object()
        router._roster_for_url["ws://a:1"] = frozenset({"m1"})
        with pytest.raises(RosterAdmissionError) as exc:
            router._verify_roster("ws://b:2", frozenset({"m1", "m2"}))
        text = str(exc.value)
        assert "ws://a:1" in text and "ws://b:2" in text
        assert "m1" in text and "m2" in text

    def test_disconnected_members_roster_is_not_the_reference(self) -> None:
        # A roster left behind by a dropped connection must not veto a
        # new seed (read-loop pops it, but be robust to the window).
        router = _relay(["ws://a:1", "ws://b:2"])
        router._roster_for_url["ws://a:1"] = frozenset({"old"})
        # a not in _connections → ignored; b seeds.
        router._verify_roster("ws://b:2", frozenset({"new"}))


# ---------------------------------------------------------------------------
# _connect admission and the raise contract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestConnectAdmission:
    async def _patched_connect(
        self, monkeypatch: Any, router: RelayRouter, ws_for_url: Dict[str, Any],
    ) -> None:
        import websockets

        async def fake_connect(url: str, **_kw: Any) -> Any:
            ws = ws_for_url.get(url)
            if ws is None:
                raise ConnectionError(f"refused: {url}")
            return ws

        monkeypatch.setattr(websockets, "connect", fake_connect)

    async def test_matching_member_admitted(self, monkeypatch: Any) -> None:
        router = _relay(["ws://a:1", "ws://b:2"])
        reply = lambda sent: [_models_reply(  # noqa: E731
            json.loads(sent[-1])["id"], [{"internalName": "m1"}],
        )]
        wses = {"ws://a:1": _ScriptedWs(reply), "ws://b:2": _ScriptedWs(reply)}
        await self._patched_connect(monkeypatch, router, wses)
        await router._connect("ws://a:1")
        await router._connect("ws://b:2")
        assert set(router._connections) == {"ws://a:1", "ws://b:2"}
        assert router._roster_for_url["ws://b:2"] == frozenset({"m1"})
        await router.stop()

    async def test_divergent_member_refused_and_closed(
        self, monkeypatch: Any,
    ) -> None:
        router = _relay(["ws://a:1", "ws://b:2"])
        ws_a = _ScriptedWs(lambda sent: [_models_reply(
            json.loads(sent[-1])["id"], [{"internalName": "m1"}],
        )])
        ws_b = _ScriptedWs(lambda sent: [_models_reply(
            json.loads(sent[-1])["id"],
            [{"internalName": "m1"}, {"internalName": "m2"}],
        )])
        await self._patched_connect(
            monkeypatch, router, {"ws://a:1": ws_a, "ws://b:2": ws_b},
        )
        await router._connect("ws://a:1")
        with pytest.raises(RosterAdmissionError):
            await router._connect("ws://b:2")
        assert "ws://b:2" not in router._connections
        assert "ws://b:2" not in router._roster_for_url
        assert ws_b.closed is True
        await router.stop()

    async def test_connect_failure_raises_for_backoff_loop(
        self, monkeypatch: Any,
    ) -> None:
        # The raise contract _reconnect_with_backoff depends on: a
        # swallowed failure (pre-v1.0.33) ended reconnection after one
        # attempt.
        router = _relay(["ws://a:1"])
        await self._patched_connect(monkeypatch, router, {})  # all refuse
        with pytest.raises(ConnectionError):
            await router._connect("ws://a:1")

    async def test_initial_admission_failure_schedules_retry(
        self, monkeypatch: Any,
    ) -> None:
        """Cold-start self-heal (audit finding): a member that fails
        INITIAL admission — unreachable, or roster-divergent while
        mid-rolling-restart when the RELAY starts — must enter the
        same reconnect-with-backoff as a disconnected member, not stay
        permanently absent until a proxy restart."""
        router = _relay(["ws://a:1", "ws://b:2"])
        good = lambda: _ScriptedWs(lambda sent: [_models_reply(  # noqa: E731
            json.loads(sent[-1])["id"], [{"internalName": "m1"}],
        )])
        b_attempts: List[int] = []
        ws_a = good()

        import websockets

        async def fake_connect(url: str, **_kw: Any) -> Any:
            if url == "ws://a:1":
                return ws_a
            b_attempts.append(1)
            if len(b_attempts) < 2:
                raise ConnectionError("b down at relay start")
            return good()

        monkeypatch.setattr(websockets, "connect", fake_connect)

        async def no_sleep(_s: float) -> None:
            pass

        monkeypatch.setattr(asyncio, "sleep", no_sleep)
        await router.start()
        # a admitted; b failed initially but a retry task is armed.
        assert "ws://a:1" in router._connections
        assert router._reconnect_tasks, "initial failure must arm a retry"
        for task in list(router._reconnect_tasks):
            await task
        assert "ws://b:2" in router._connections
        assert router._roster_for_url["ws://b:2"] == frozenset({"m1"})
        await router.stop()

    async def test_reconnect_retries_until_success(
        self, monkeypatch: Any,
    ) -> None:
        router = _relay(["ws://a:1"])
        attempts: List[int] = []
        good_ws = _ScriptedWs(lambda sent: [_models_reply(
            json.loads(sent[-1])["id"], [{"internalName": "m1"}],
        )])

        import websockets

        async def flaky_connect(url: str, **_kw: Any) -> Any:
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("not yet")
            return good_ws

        monkeypatch.setattr(websockets, "connect", flaky_connect)

        async def no_sleep(_s: float) -> None:
            pass

        monkeypatch.setattr(asyncio, "sleep", no_sleep)
        await router._reconnect_with_backoff("ws://a:1")
        assert len(attempts) == 3
        assert "ws://a:1" in router._connections
        await router.stop()
