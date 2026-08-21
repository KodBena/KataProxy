"""
tests/test_cache_verbs.py — proxy-owned broadcast cache verbs (v1.0.31).

Contract under test:

  - The four persistent-NN-cache verbs (cache_attach / cache_detach /
    cache_dump / cache_stats) join the proxy vocabulary and parse via
    the standard prisms.
  - SELECTOR routes them by label exactly like ANALYZE (missing label
    refused; label consumed at _forward; configured engine model
    minted).
  - RELAY fans them to EVERY configured ring member and aggregates the
    per-member replies into ONE metadata response keyed by member url,
    with partial failure explicit (disconnected / send-failed /
    timed-out members appear as structured errors, never silently
    absorbed) and internal sub-wire-ids stripped from member values.
  - The replay cache never serves them: lookup and store are both
    ANALYZE-gated in the hub.

Run from the proxy directory: `pytest tests/test_cache_verbs.py`.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

import sproxy_config as cfg  # noqa: E402
from AbstractProxy.proxy_core import CanonicalId, Dispatcher, InternalId  # noqa: E402
from katago import (  # noqa: E402
    CACHE_VERB_ACTIONS,
    KATAGO_QUERY_PRISMS,
    KataGoAction,
    KataGoQuery,
    SUPPORTED_WIRE_ACTIONS,
    translate_query_to_wire,
)
from pubsub_hub import LRUCacheStore, PubSubHub  # noqa: E402
from router import (  # noqa: E402
    InFlightQueryLoad,
    RelayRouter,
    SelectorRouter,
    SyntheticCallbackOrigin,
)


class _MockWebSocket:
    def __init__(self, url: str = "") -> None:
        self.url = url
        self.sent: List[str] = []
        self.closed = False

    async def send(self, msg: str) -> None:
        if self.closed:
            raise ConnectionError(f"ws[{self.url}] closed")
        self.sent.append(msg)


def _cache_dump_query(**extra: Any) -> KataGoQuery:
    opaque: Dict[str, Any] = {"context": "ctx1", "what": "both"}
    opaque.update(extra)
    return KataGoQuery(action=KataGoAction.CACHE_DUMP, opaque=opaque)


# ---------------------------------------------------------------------------
# Vocabulary and parsing
# ---------------------------------------------------------------------------

def test_cache_verbs_in_supported_vocabulary() -> None:
    for verb in ("cache_attach", "cache_detach", "cache_dump", "cache_stats"):
        assert verb in SUPPORTED_WIRE_ACTIONS


def test_cache_verb_parses_via_prisms() -> None:
    d = Dispatcher(KATAGO_QUERY_PRISMS)
    result = d.match({
        "id": "a1", "action": "cache_attach",
        "context": "lv1", "model": "main",
    })
    assert result is not None
    _prism, raw_id, query = result
    assert raw_id == "a1"
    assert query.action == KataGoAction.CACHE_ATTACH
    assert query.opaque["context"] == "lv1"


def test_cache_verb_wire_round_trip_keeps_payload() -> None:
    q = _cache_dump_query(admission={"minObservations": 3}, model="m")
    wire = translate_query_to_wire(q, "cid-1")
    assert wire["action"] == "cache_dump"
    assert wire["context"] == "ctx1"
    assert wire["what"] == "both"
    assert wire["admission"] == {"minObservations": 3}
    assert wire["model"] == "m"


# ---------------------------------------------------------------------------
# SELECTOR: label-routed like ANALYZE, engine model minted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSelectorCacheVerbs:
    def _router(self) -> Tuple[SelectorRouter, Dict[str, _MockWebSocket]]:
        router = SelectorRouter(models=(
            ("main", "ws://h1:1", "b6c96-s1-d1"),
            ("plain", "ws://h2:2"),
        ))
        sockets: Dict[str, _MockWebSocket] = {}
        for spec in router._models:
            router._url_for_label[spec.label] = spec.url
            if spec.engine_model is not None:
                router._engine_model_for_label[spec.label] = spec.engine_model
            router._failure_budget[spec.label] = 3
            ws = _MockWebSocket(spec.url)
            router._connections[spec.label] = ws
            sockets[spec.label] = ws
        return router, sockets

    async def test_label_routed_and_engine_model_minted(self) -> None:
        router, sockets = self._router()

        async def on_response(_c: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_c: CanonicalId) -> None: pass

        q = _cache_dump_query(model="main")
        wire = translate_query_to_wire(q, "cid-cv1")
        await router.dispatch(CanonicalId("cid-cv1"), wire, q, on_response, on_complete)
        sent = json.loads(sockets["main"].sent[0])
        assert sent["action"] == "cache_dump"
        assert sent["model"] == "b6c96-s1-d1"  # label consumed, config minted
        assert sockets["plain"].sent == []

    async def test_unconfigured_label_forwards_no_model(self) -> None:
        router, sockets = self._router()

        async def on_response(_c: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_c: CanonicalId) -> None: pass

        q = _cache_dump_query(model="plain")
        wire = translate_query_to_wire(q, "cid-cv2")
        await router.dispatch(CanonicalId("cid-cv2"), wire, q, on_response, on_complete)
        sent = json.loads(sockets["plain"].sent[0])
        assert "model" not in sent  # engine primary-model default

    async def test_missing_label_refused(self) -> None:
        router, _sockets = self._router()
        replies: List[Dict[str, Any]] = []

        async def on_response(_c: CanonicalId, w: Dict[str, Any]) -> None:
            replies.append(w)
        async def on_complete(_c: CanonicalId) -> None: pass

        q = _cache_dump_query()  # no model/label
        wire = translate_query_to_wire(q, "cid-cv3")
        await router.dispatch(CanonicalId("cid-cv3"), wire, q, on_response, on_complete)
        assert replies and "error" in replies[0]
        assert replies[0]["field"] == "model"


# ---------------------------------------------------------------------------
# RELAY: broadcast-aggregate with explicit partial failure
# ---------------------------------------------------------------------------

def _relay(urls: List[str]) -> RelayRouter:
    return RelayRouter(upstream_urls=urls, load_metric=InFlightQueryLoad())


async def _collect(router: RelayRouter, q: KataGoQuery, cid: str) -> Tuple[
    List[Dict[str, Any]], List[str],
]:
    replies: List[Dict[str, Any]] = []
    completes: List[str] = []

    async def on_response(_c: CanonicalId, w: Dict[str, Any]) -> None:
        replies.append(w)

    async def on_complete(c: CanonicalId) -> None:
        completes.append(c)

    wire = translate_query_to_wire(q, cid)
    await router.dispatch(CanonicalId(cid), wire, q, on_response, on_complete)
    return replies, completes


@pytest.mark.asyncio
class TestRelayCacheVerbAggregate:
    async def test_aggregate_over_mixed_members(self) -> None:
        urls = ["ws://a:1", "ws://b:2", "ws://c:3"]
        router = _relay(urls)
        # a and b connected; c disconnected.
        for url in urls[:2]:
            router._connections[url] = _MockWebSocket(url)

        replies: List[Dict[str, Any]] = []
        completes: List[str] = []

        async def on_response(_c: CanonicalId, w: Dict[str, Any]) -> None:
            replies.append(w)

        async def on_complete(c: CanonicalId) -> None:
            completes.append(c)

        q = _cache_dump_query()
        wire = translate_query_to_wire(q, "cid-agg")
        await router.dispatch(CanonicalId("cid-agg"), wire, q, on_response, on_complete)

        # No aggregate yet — two live members still pending.
        assert replies == []

        # Feed each live member's engine reply through the real
        # callback machinery (as _read_loop would).
        for i, url in enumerate(urls[:2]):
            sent = json.loads(router._connections[url].sent[0])
            sub_id = sent["id"]
            assert sub_id.startswith("cid-agg:cv")
            cb = router._callbacks[sub_id]
            m_resp, m_done, sentinel = cb
            assert sentinel is SyntheticCallbackOrigin.CACHE_VERB_AGGREGATE
            engine_reply = {
                "id": sub_id, "action": "cache_dump",
                "context": "ctx1", "entriesWritten": 10 + i,
            }
            await m_resp(sub_id, engine_reply)
            await m_done(sub_id)

        assert len(replies) == 1, "exactly one aggregate reply"
        agg = replies[0]
        assert agg["id"] == "cid-agg"          # client-facing id preserved
        assert agg["action"] == "cache_dump"
        assert set(agg["members"]) == set(urls)  # every configured member
        # Live members: verbatim reply, internal sub-id stripped.
        assert agg["members"]["ws://a:1"]["entriesWritten"] == 10
        assert "id" not in agg["members"]["ws://a:1"]
        # Disconnected member: explicit structured error.
        assert "error" in agg["members"]["ws://c:3"]
        assert completes == ["cid-agg"]

    async def test_straggler_reported_as_timeout(self, monkeypatch: Any) -> None:
        monkeypatch.setattr(cfg, "CACHE_VERB_TIMEOUT_S", 0.05)
        urls = ["ws://a:1", "ws://b:2"]
        router = _relay(urls)
        for url in urls:
            router._connections[url] = _MockWebSocket(url)

        replies, completes = await _collect(router, _cache_dump_query(), "cid-to")
        assert replies == []

        # Only member a replies; b straggles past the timeout.
        sent = json.loads(router._connections["ws://a:1"].sent[0])
        sub_id = sent["id"]
        m_resp, m_done, _ = router._callbacks[sub_id]
        await m_resp(sub_id, {"id": sub_id, "ok": True})
        await m_done(sub_id)

        await asyncio.sleep(0.2)
        # The aggregate arrived via the timeout path.
        # (replies list captured by _collect's closures.)
        # _collect returned before replies arrived; re-fetch via the
        # closure lists it returned.
        assert len(replies) == 1
        agg = replies[0]
        assert "ok" in agg["members"]["ws://a:1"]
        b = agg["members"]["ws://b:2"]
        assert "error" in b and "no reply within" in b["error"]
        assert completes == ["cid-to"]

    async def test_send_failure_is_explicit(self) -> None:
        urls = ["ws://a:1", "ws://b:2"]
        router = _relay(urls)
        good = _MockWebSocket(urls[0])
        bad = _MockWebSocket(urls[1])
        bad.closed = True
        router._connections[urls[0]] = good
        router._connections[urls[1]] = bad

        replies, completes = await _collect(router, _cache_dump_query(), "cid-sf")
        sent = json.loads(good.sent[0])
        sub_id = sent["id"]
        m_resp, m_done, _ = router._callbacks[sub_id]
        await m_resp(sub_id, {"id": sub_id, "ok": True})
        await m_done(sub_id)

        agg = replies[0]
        assert "error" in agg["members"]["ws://b:2"]
        assert "send to upstream failed" in agg["members"]["ws://b:2"]["error"]
        assert completes == ["cid-sf"]

    async def test_early_reply_during_backpressured_send_does_not_lose_aggregate(
        self,
    ) -> None:
        """Audit-found race (2026-08-21): member A's reply is processed
        while member B's ws.send has the dispatch coroutine suspended
        on backpressure. Pre-fix, `pending` was populated only after
        each send returned and `timeout_task` was bound only after the
        send loop — A's completion saw an empty `pending`, finalized,
        and hit the unbound `timeout_task` (NameError inside the read
        loop; aggregate never delivered). The mock's send suspends to
        model the backpressure schedule the plain mock cannot."""
        urls = ["ws://a:1", "ws://b:2"]
        router = _relay(urls)
        a = _MockWebSocket(urls[0])
        gate = asyncio.Event()

        class _SuspendingWs:
            def __init__(self) -> None:
                self.sent: List[str] = []

            async def send(self, msg: str) -> None:
                await gate.wait()  # backpressure until released
                self.sent.append(msg)

        b = _SuspendingWs()
        router._connections[urls[0]] = a
        router._connections[urls[1]] = b

        replies: List[Dict[str, Any]] = []
        completes: List[str] = []

        async def on_response(_c: CanonicalId, w: Dict[str, Any]) -> None:
            replies.append(w)

        async def on_complete(c: CanonicalId) -> None:
            completes.append(c)

        q = _cache_dump_query()
        wire = translate_query_to_wire(q, "cid-race")
        dispatch_task = asyncio.create_task(router.dispatch(
            CanonicalId("cid-race"), wire, q, on_response, on_complete,
        ))
        await asyncio.sleep(0)  # A sent; dispatch suspended in B's send

        # A replies while B's send is still suspended.
        sent_a = json.loads(a.sent[0])
        sub_a = sent_a["id"]
        m_resp, m_done, _ = router._callbacks[sub_a]
        await m_resp(sub_a, {"id": sub_a, "ok": "a"})
        await m_done(sub_a)
        assert replies == [], "must not finalize while B's send is pending"

        gate.set()
        await dispatch_task
        # Now B replies; the aggregate completes exactly once.
        sent_b = json.loads(b.sent[0])
        sub_b = sent_b["id"]
        m_resp, m_done, _ = router._callbacks[sub_b]
        await m_resp(sub_b, {"id": sub_b, "ok": "b"})
        await m_done(sub_b)

        assert len(replies) == 1
        agg = replies[0]
        assert agg["members"]["ws://a:1"]["ok"] == "a"
        assert agg["members"]["ws://b:2"]["ok"] == "b"
        assert completes == ["cid-race"]

    async def test_all_members_down_still_one_aggregate(self) -> None:
        urls = ["ws://a:1", "ws://b:2"]
        router = _relay(urls)  # nothing connected
        replies, completes = await _collect(router, _cache_dump_query(), "cid-down")
        assert len(replies) == 1
        agg = replies[0]
        assert set(agg["members"]) == set(urls)
        for url in urls:
            assert "error" in agg["members"][url]
        assert completes == ["cid-down"]


# ---------------------------------------------------------------------------
# Hub: cache verbs are never replay-served
# ---------------------------------------------------------------------------

def test_replay_lookup_and_store_are_analyze_gated() -> None:
    hub = PubSubHub(cache_store=LRUCacheStore(maxsize=10))
    q = KataGoQuery(
        action=KataGoAction.CACHE_STATS,
        opaque={"lookup_cache": True, "cache": True},
    )
    queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
    is_new, canonical = hub.subscribe(q, InternalId("sub-1"), queue)
    # Not replay-served: the subscribe went to the normal (dispatch)
    # path even though lookup_cache was set.
    assert is_new is True
    entry = hub._by_canonical[canonical]
    # Not recorded either: store is ANALYZE-gated.
    assert entry.record_cache is False


def test_cache_verb_actions_is_exactly_the_four() -> None:
    assert CACHE_VERB_ACTIONS == {
        KataGoAction.CACHE_ATTACH,
        KataGoAction.CACHE_DETACH,
        KataGoAction.CACHE_DUMP,
        KataGoAction.CACHE_STATS,
    }
