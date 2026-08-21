"""
tests/test_selector_router.py — Phase 2+3 SELECTOR router tests.

Covers all of Phase 2+3's pure and effectful units:

  - SELECTOR_MODELS parser (well-formed entries, whitespace handling,
    malformed entries raise ValueError naming the entry).
  - CoalescingPolicy includes "model" in capturing_fields, so two
    queries differing only in `model` produce different content_hashes.
  - translate_query_to_wire passes "model" through (engine-facing
    since the v1.0.30 reclassification); SelectorRouter._forward is
    the single boundary that consumes the label and mints the
    configured engine internalName (TestEngineModelInjection).
  - SelectorRouter.start validates configuration: empty SELECTOR_MODELS
    and duplicate labels both raise SelectorStartupError.
  - SelectorRouter.dispatch matrix:
      ANALYZE without `model` → structured error
      ANALYZE with unknown `model` → structured error naming the
        available labels
      ANALYZE with healthy `model` → forwarded to the right upstream
      ANALYZE with unhealthy `model` → structured error
      QUERY_MODELS → synthesised from configured labels (no upstream
        traffic); each entry carries `healthy: bool` so the SPA's
        model-selector dropdown can grey out advertised-but-disconnected
        labels
      QUERY_VERSION / CLEAR_CACHE / TERMINATE_ALL → broadcast to every
        healthy upstream; first response wins; per-upstream send
        failures log and continue
      Broadcast with no healthy upstream → structured error
      Broadcast with all sends raising → structured error (degenerate
        case: every healthy upstream's WS refused the send)
  - SelectorRouter.terminate routes by remembered label; synthesises
    ack on dead upstream.
  - Capabilities advertiser includes `selector` when cfg.ROLE ==
    "SELECTOR"; absent in other roles.

Run from the proxy directory: `pytest tests/test_selector_router.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

import sproxy_config as cfg  # noqa: E402
from AbstractProxy.proxy_core import CanonicalId  # noqa: E402
from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    translate_query_to_wire,
)
from pubsub_hub import CoalescingPolicy  # noqa: E402
from router import (  # noqa: E402
    SelectorRouter,
    SelectorStartupError,
)
from sproxy_config import _parse_selector_models  # noqa: E402


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Minimal websocket mock: records sent messages, supports close.

    The async-iteration protocol is implemented by reading from
    `_inbox`; tests that exercise the read loop push messages via
    `push()`.
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed: bool = False
        self._inbox: asyncio.Queue[str] = asyncio.Queue()

    async def send(self, msg: str) -> None:
        if self.closed:
            raise ConnectionError("websocket closed")
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> "_MockWebSocket":
        return self

    async def __anext__(self) -> str:
        if self.closed:
            raise StopAsyncIteration
        return await self._inbox.get()

    def push(self, msg: str) -> None:
        self._inbox.put_nowait(msg)


def _make_router(
    models: tuple[tuple[str, str], ...] = (("strong", "ws://h1:1"), ("weak", "ws://h2:2")),
    max_connect_failures: int = 3,
) -> SelectorRouter:
    """Construct a SelectorRouter without starting it."""
    return SelectorRouter(models=models, max_connect_failures=max_connect_failures)


def _populate_post_start_state(
    router: SelectorRouter,
    healthy_labels: list[str],
    disconnected_labels: Optional[list[str]] = None,
    unhealthy_labels: Optional[list[str]] = None,
) -> Dict[str, _MockWebSocket]:
    """Bypass start(): populate _url_for_label, _failure_budget, _connections.

    Returns the mock websockets keyed by label so tests can inspect
    `sent` and inject responses.
    """
    disconnected_labels = disconnected_labels or []
    unhealthy_labels = unhealthy_labels or []
    sockets: Dict[str, _MockWebSocket] = {}
    for spec in router._models:
        router._url_for_label[spec.label] = spec.url
        if spec.engine_model is not None:
            router._engine_model_for_label[spec.label] = spec.engine_model
        router._failure_budget[spec.label] = router._max_connect_failures
    for label in healthy_labels:
        ws = _MockWebSocket()
        router._connections[label] = ws
        sockets[label] = ws
    for label in unhealthy_labels:
        router._unhealthy_models.add(label)
    return sockets


def _make_analyze_query(
    model: Optional[str] = None,
    extra_opaque: Optional[Dict[str, Any]] = None,
) -> KataGoQuery:
    opaque: Dict[str, Any] = {
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": [["B", "Q4"]],
    }
    if extra_opaque:
        opaque.update(extra_opaque)
    if model is not None:
        opaque["model"] = model
    return KataGoQuery(action=KataGoAction.ANALYZE, opaque=opaque)


# ===========================================================================
# SELECTOR_MODELS parser
# ===========================================================================


class TestSelectorModelsParser:
    def test_well_formed_pair(self) -> None:
        assert _parse_selector_models("strong=ws://h1:1") == \
               (("strong", "ws://h1:1", None),)

    def test_multiple_entries(self) -> None:
        assert _parse_selector_models("strong=ws://h1:1,weak=ws://h2:2") == \
               (("strong", "ws://h1:1", None), ("weak", "ws://h2:2", None))

    def test_engine_model_component_parsed(self) -> None:
        assert _parse_selector_models(
            "main=ws://h:1|b6c96-s1-d1,alt=ws://h:1|b6c96-s2-d2"
        ) == (
            ("main", "ws://h:1", "b6c96-s1-d1"),
            ("alt", "ws://h:1", "b6c96-s2-d2"),
        )

    def test_engine_model_component_whitespace_trimmed(self) -> None:
        assert _parse_selector_models(" main = ws://h:1 | b6c96-s1-d1 ") == \
               (("main", "ws://h:1", "b6c96-s1-d1"),)

    def test_mixed_entries_with_and_without_component(self) -> None:
        assert _parse_selector_models(
            "plain=ws://h1:1,tuned=ws://h2:2|b6c96-s1-d1"
        ) == (
            ("plain", "ws://h1:1", None),
            ("tuned", "ws://h2:2", "b6c96-s1-d1"),
        )

    def test_trailing_pipe_with_empty_component_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty engine-model"):
            _parse_selector_models("main=ws://h:1|")

    def test_empty_string(self) -> None:
        assert _parse_selector_models("") == ()

    def test_whitespace_trimmed(self) -> None:
        assert _parse_selector_models("  strong  =  ws://h1:1  ,  weak=ws://h2:2  ") == \
               (("strong", "ws://h1:1", None), ("weak", "ws://h2:2", None))

    def test_consecutive_commas_skipped(self) -> None:
        assert _parse_selector_models("strong=ws://h1:1,,weak=ws://h2:2") == \
               (("strong", "ws://h1:1", None), ("weak", "ws://h2:2", None))

    def test_missing_separator_raises(self) -> None:
        with pytest.raises(ValueError, match="missing a `label=url` separator"):
            _parse_selector_models("no_separator")

    def test_missing_separator_names_offending_entry(self) -> None:
        with pytest.raises(ValueError, match="'no_separator'"):
            _parse_selector_models("no_separator")

    def test_empty_label_raises(self) -> None:
        with pytest.raises(ValueError, match="empty label"):
            _parse_selector_models("=ws://h1:1")

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ValueError, match="empty url"):
            _parse_selector_models("strong=")

    def test_order_preserved(self) -> None:
        # Configuration order matters: _first_healthy_label walks
        # this order, so the parser must preserve insertion order.
        result = _parse_selector_models("c=ws://3,a=ws://1,b=ws://2")
        assert [label for label, _url, _em in result] == ["c", "a", "b"]


# ===========================================================================
# CoalescingPolicy includes model
# ===========================================================================


class TestCoalescingWithModel:
    def test_model_field_is_in_capturing_fields(self) -> None:
        policy = CoalescingPolicy()
        assert "model" in policy.capturing_fields

    def test_different_model_produces_different_hashes(self) -> None:
        policy = CoalescingPolicy()
        h_strong = policy.query_hash(_make_analyze_query(model="strong"))
        h_weak = policy.query_hash(_make_analyze_query(model="weak"))
        assert h_strong != h_weak

    def test_absent_model_hashes_uniformly(self) -> None:
        # Two queries that don't carry `model` should hash the same
        # (legacy clients still coalesce among themselves).
        policy = CoalescingPolicy()
        h1 = policy.query_hash(_make_analyze_query())
        h2 = policy.query_hash(_make_analyze_query())
        assert h1 == h2

    def test_absent_vs_present_model_differs(self) -> None:
        policy = CoalescingPolicy()
        h_absent = policy.query_hash(_make_analyze_query())
        h_present = policy.query_hash(_make_analyze_query(model="strong"))
        assert h_absent != h_present


# ===========================================================================
# Wire classification (v1.0.30): `model` is engine-facing and passes
# through the central wire-builder at every role; the SELECTOR's
# _forward is the single boundary that consumes it (tested in
# TestEngineModelInjection below). The pre-v1.0.30 pin asserting
# "model not in wire" enshrined the stale vanilla-KataGo rationale.
# ===========================================================================


class TestWireModelClassification:
    def test_model_passes_through_wire_builder(self) -> None:
        q = _make_analyze_query(model="b6c96-s1-d1")
        wire = translate_query_to_wire(q, "eid-1")
        assert wire["model"] == "b6c96-s1-d1"

    def test_proxy_only_fields_still_excluded(self) -> None:
        for f in ("cache", "lookup_cache", "replay_final_only",
                  "analysis_config", "capabilities"):
            q = _make_analyze_query(extra_opaque={f: {"any": "value"}})
            wire = translate_query_to_wire(q, "eid-1")
            assert f not in wire, f"{f} leaked through wire-strip"


# ===========================================================================
# Engine-model injection (v1.0.30): _forward is THE label→engine-model
# boundary. The client's label is consumed unconditionally; the
# configured engine internalName (if any) is minted in its place —
# sole writer on the forwarded side.
# ===========================================================================


@pytest.mark.asyncio
class TestEngineModelInjection:
    async def test_configured_label_mints_engine_model(self) -> None:
        router = _make_router(models=(
            ("main", "ws://h1:1", "b6c96-s1-d1"),
            ("alt", "ws://h1:1", "b6c96-s2-d2"),
        ))
        sockets = _populate_post_start_state(
            router, healthy_labels=["main", "alt"]
        )

        async def on_response(_c: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_c: CanonicalId) -> None: pass

        q = _make_analyze_query(model="alt")
        wire = translate_query_to_wire(q, "cid-inj")
        assert wire["model"] == "alt"  # label present pre-boundary
        await router.dispatch(CanonicalId("cid-inj"), wire, q, on_response, on_complete)

        sent = json.loads(sockets["alt"].sent[0])
        assert sent["model"] == "b6c96-s2-d2"
        assert sockets["main"].sent == []
        # The caller's wire_dict is not mutated (copy-on-write).
        assert wire["model"] == "alt"

    async def test_unconfigured_label_forwards_no_model(self) -> None:
        # A label without an engine_model component must never leak the
        # label upstream (a multi-model engine would refuse it as an
        # unknown internalName) — byte-identical to pre-v1.0.30 wire.
        router = _make_router()  # default fixture: no engine models
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )

        async def on_response(_c: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_c: CanonicalId) -> None: pass

        q = _make_analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-noinj")
        await router.dispatch(CanonicalId("cid-noinj"), wire, q, on_response, on_complete)
        assert "model" not in json.loads(sockets["strong"].sent[0])

    async def test_broadcast_never_carries_model(self) -> None:
        # Broadcast actions reach EVERY upstream; a label (or any
        # client-supplied model value) has no meaning there and a
        # multi-model engine would hard-refuse it.
        router = _make_router(models=(
            ("main", "ws://h1:1", "b6c96-s1-d1"),
            ("alt", "ws://h2:2", "b6c96-s2-d2"),
        ))
        sockets = _populate_post_start_state(
            router, healthy_labels=["main", "alt"]
        )

        async def on_response(_c: CanonicalId, _w: Dict[str, Any]) -> None: pass
        async def on_complete(_c: CanonicalId) -> None: pass

        q = KataGoQuery(
            action=KataGoAction.QUERY_VERSION,
            opaque={"model": "main"},
        )
        wire = translate_query_to_wire(q, "cid-bc")
        await router.dispatch(CanonicalId("cid-bc"), wire, q, on_response, on_complete)
        for label, ws in sockets.items():
            for frame in ws.sent:
                assert "model" not in json.loads(frame), (
                    f"model leaked to {label} on broadcast"
                )


# ===========================================================================
# SelectorRouter startup validation
# ===========================================================================


@pytest.mark.asyncio
class TestSelectorStartupValidation:
    async def test_empty_models_raises(self) -> None:
        router = _make_router(models=())
        with pytest.raises(SelectorStartupError, match="at least one entry"):
            await router.start()

    async def test_duplicate_labels_raises(self) -> None:
        router = _make_router(
            models=(("strong", "ws://h1:1"), ("strong", "ws://h2:2")),
        )
        with pytest.raises(SelectorStartupError, match="duplicate label"):
            await router.start()

    async def test_duplicate_label_message_names_label(self) -> None:
        router = _make_router(
            models=(("modelA", "ws://h1:1"), ("modelA", "ws://h2:2")),
        )
        with pytest.raises(SelectorStartupError, match="'modelA'"):
            await router.start()


# ===========================================================================
# SelectorRouter.dispatch matrix
# ===========================================================================


@pytest.mark.asyncio
class TestSelectorDispatch:
    async def test_analyze_without_model_field_returns_error(self) -> None:
        router = _make_router()
        _populate_post_start_state(router, healthy_labels=["strong", "weak"])
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = _make_analyze_query()  # no model
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        assert len(responses) == 1
        assert responses[0][1]["error"].startswith("missing 'model' field")
        assert responses[0][1].get("field") == "model"
        assert completes == ["cid-1"]

    async def test_analyze_unknown_model_returns_error(self) -> None:
        router = _make_router()
        _populate_post_start_state(router, healthy_labels=["strong", "weak"])
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = _make_analyze_query(model="nonexistent")
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        assert len(responses) == 1
        assert "nonexistent" in responses[0][1]["error"]
        assert "strong" in responses[0][1]["error"]
        assert "weak" in responses[0][1]["error"]
        assert responses[0][1].get("field") == "model"
        assert completes == ["cid-1"]

    async def test_analyze_unhealthy_model_returns_error(self) -> None:
        router = _make_router()
        _populate_post_start_state(
            router,
            healthy_labels=["strong"],
            unhealthy_labels=["weak"],
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = _make_analyze_query(model="weak")
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        assert len(responses) == 1
        assert "weak" in responses[0][1]["error"]
        assert "unavailable" in responses[0][1]["error"]
        assert responses[0][1].get("field") == "model"

    async def test_analyze_healthy_model_forwards_to_right_upstream(self) -> None:
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = _make_analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        # Forwarded to "strong", not "weak".
        assert len(sockets["strong"].sent) == 1
        assert sockets["weak"].sent == []
        sent_wire = json.loads(sockets["strong"].sent[0])
        # v1.0.30: the label is consumed at the _forward boundary; with
        # no engine_model configured, nothing replaces it.
        assert "model" not in sent_wire
        # No synthetic response yet — it comes from the upstream.
        assert responses == []

    async def test_analyze_disconnected_model_within_budget_returns_error(self) -> None:
        # Healthy label, but currently disconnected (within retry
        # budget). Dispatch should fail loudly rather than queue.
        router = _make_router()
        _populate_post_start_state(
            router,
            healthy_labels=[],  # nothing connected
            disconnected_labels=["strong"],
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = _make_analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)

        assert "strong" in responses[0][1]["error"]
        assert "disconnected" in responses[0][1]["error"]

    async def test_query_models_synthesised_no_upstream_traffic(self) -> None:
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        q = KataGoQuery(action=KataGoAction.QUERY_MODELS, opaque={})
        wire = translate_query_to_wire(q, "cid-qm")
        await router.dispatch(CanonicalId("cid-qm"), wire, q, on_response, on_complete)

        # Synthesised — no upstream traffic.
        assert sockets["strong"].sent == []
        assert sockets["weak"].sent == []
        # Response shape: list of {label, healthy} entries in
        # configuration order. Both labels are healthy in this setup,
        # so both `healthy` fields are True.
        assert len(responses) == 1
        models = responses[0][1]["models"]
        assert models == [
            {"label": "strong", "healthy": True},
            {"label": "weak", "healthy": True},
        ]
        assert completes == ["cid-qm"]

    async def test_query_models_works_with_no_healthy_upstreams(self) -> None:
        # Synthesising query_models never needs a live upstream — even
        # an all-unhealthy SELECTOR can enumerate its configured labels.
        router = _make_router()
        _populate_post_start_state(
            router, healthy_labels=[], unhealthy_labels=["strong", "weak"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_MODELS, opaque={})
        wire = translate_query_to_wire(q, "cid-qm")
        await router.dispatch(CanonicalId("cid-qm"), wire, q, on_response, on_complete)
        # Both labels are unhealthy — the SPA's dropdown grey-outs both.
        assert responses[0][1]["models"] == [
            {"label": "strong", "healthy": False},
            {"label": "weak", "healthy": False},
        ]

    async def test_query_version_broadcasts_to_all_healthy(self) -> None:
        # Heartbeat fanout: every healthy LEAF must receive query_version
        # so its KeepAliveMiddleware._last_heartbeat resets. The
        # pre-fix "forward to first healthy upstream" routing let the
        # other LEAF's watchdog fire on long ANALYZE queries routed to
        # it by `model`. See the SELECTOR watchdog postmortem in the
        # umbrella's docs/notes/.
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_VERSION, opaque={})
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        # Every healthy upstream received the wire exactly once.
        assert len(sockets["strong"].sent) == 1
        assert len(sockets["weak"].sent) == 1
        # _callbacks holds a single entry for the canonical, with the
        # broadcast sentinel as the label slot.
        assert "cid-qv" in router._callbacks
        _on_resp, _on_comp, label_slot = router._callbacks["cid-qv"]
        assert label_slot == "__broadcast__"
        # The SPA hasn't seen a synthetic response — it waits for the
        # first upstream's reply through the read loop.
        assert responses == []

    async def test_terminate_all_broadcasts_to_all_healthy(self) -> None:
        # Same broadcast semantic as query_version. The SPA's
        # expectation for TERMINATE_ALL is "cancel every in-flight
        # query the session has, regardless of which LEAF carries it";
        # routing to a single LEAF would silently leave queries on the
        # other LEAFs running.
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.TERMINATE_ALL, opaque={})
        wire = translate_query_to_wire(q, "cid-ta")
        await router.dispatch(CanonicalId("cid-ta"), wire, q, on_response, on_complete)

        assert len(sockets["strong"].sent) == 1
        assert len(sockets["weak"].sent) == 1

    async def test_clear_cache_broadcasts_to_all_healthy(self) -> None:
        # KataGo's analysis cache is per-LEAF (per-subprocess); a
        # SPA-issued CLEAR_CACHE wants every LEAF cleared. Routing to a
        # single LEAF would silently leave the others' caches stale.
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.CLEAR_CACHE, opaque={})
        wire = translate_query_to_wire(q, "cid-cc")
        await router.dispatch(CanonicalId("cid-cc"), wire, q, on_response, on_complete)

        assert len(sockets["strong"].sent) == 1
        assert len(sockets["weak"].sent) == 1

    async def test_broadcast_no_healthy_returns_error(self) -> None:
        router = _make_router()
        _populate_post_start_state(
            router, healthy_labels=[], unhealthy_labels=["strong", "weak"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_VERSION, opaque={})
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)
        assert "no healthy upstream" in responses[0][1]["error"]

    async def test_broadcast_skips_unhealthy_targets_remainder(self) -> None:
        # When the first configured label is unhealthy, the broadcast
        # falls through to the next healthy label(s) — only labels in
        # _connections AND not in _unhealthy_models receive the wire.
        router = _make_router()
        sockets = _populate_post_start_state(
            router,
            healthy_labels=["weak"],
            unhealthy_labels=["strong"],
        )
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_VERSION, opaque={})
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)
        # "strong" is unhealthy → not in _connections in this fixture
        # AND in _unhealthy_models. "weak" is healthy. Broadcast hits
        # only "weak".
        assert len(sockets["weak"].sent) == 1
        # "strong" has no socket entry at all (unhealthy state) — the
        # _populate_post_start_state helper doesn't create one for
        # unhealthy labels. The dispatch must not raise.

    async def test_broadcast_continues_after_per_upstream_send_failure(self) -> None:
        # If one upstream's WebSocket send raises, the broadcast logs
        # and continues to the next. The healthy upstream still
        # receives the wire; the canonical's _callbacks entry is
        # installed (the broadcast didn't abort).
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        # "strong" refuses every send.
        sockets["strong"].closed = True
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_VERSION, opaque={})
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        # "weak" got it; "strong" didn't (the send raised).
        assert sockets["strong"].sent == []
        assert len(sockets["weak"].sent) == 1
        # _callbacks still has the entry — broadcast didn't abort.
        assert "cid-qv" in router._callbacks

    async def test_broadcast_all_sends_failing_returns_error(self) -> None:
        # Degenerate case: every healthy upstream's send raised. The
        # broadcast pops the _callbacks entry and surfaces a structured
        # error (a hung canonical would be the worse outcome).
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        sockets["strong"].closed = True
        sockets["weak"].closed = True
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        q = KataGoQuery(action=KataGoAction.QUERY_VERSION, opaque={})
        wire = translate_query_to_wire(q, "cid-qv")
        await router.dispatch(CanonicalId("cid-qv"), wire, q, on_response, on_complete)

        # _callbacks was popped — the canonical isn't hung.
        assert "cid-qv" not in router._callbacks
        # SPA sees a structured error.
        assert len(responses) == 1
        assert "no healthy upstream" in responses[0][1]["error"]


# ===========================================================================
# SelectorRouter.terminate
# ===========================================================================


@pytest.mark.asyncio
class TestSelectorTerminate:
    async def test_terminate_routes_to_remembered_label(self) -> None:
        router = _make_router()
        sockets = _populate_post_start_state(
            router, healthy_labels=["strong", "weak"]
        )
        # Pretend an in-flight query exists routed to "weak".
        async def cb_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def cb_complete(cid: CanonicalId) -> None: pass
        router._callbacks[CanonicalId("cid-1")] = (cb_response, cb_complete, "weak")

        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def on_complete(cid: CanonicalId) -> None: pass

        await router.terminate(CanonicalId("cid-1"), on_response, on_complete)

        # Terminate sent to "weak", not "strong".
        assert len(sockets["weak"].sent) == 1
        assert sockets["strong"].sent == []
        sent = json.loads(sockets["weak"].sent[0])
        assert sent["action"] == "terminate"
        assert sent["terminateId"] == "cid-1"

    async def test_terminate_unknown_canonical_synthesises_ack(self) -> None:
        router = _make_router()
        _populate_post_start_state(router, healthy_labels=["strong"])
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        completes: List[CanonicalId] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: completes.append(cid)

        await router.terminate(CanonicalId("never-existed"), on_response, on_complete)

        # Synthetic ack: action=terminate, terminateId=never-existed.
        assert len(responses) == 1
        assert responses[0][1]["action"] == "terminate"
        assert responses[0][1]["terminateId"] == "never-existed"
        assert len(completes) == 1

    async def test_terminate_dead_upstream_synthesises_ack(self) -> None:
        # Callback recorded for label that's now disconnected.
        router = _make_router()
        _populate_post_start_state(
            router, healthy_labels=[], disconnected_labels=["weak"]
        )
        async def cb_response(cid: CanonicalId, w: Dict[str, Any]) -> None: pass
        async def cb_complete(cid: CanonicalId) -> None: pass
        router._callbacks[CanonicalId("cid-1")] = (cb_response, cb_complete, "weak")

        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        await router.terminate(CanonicalId("cid-1"), on_response, on_complete)

        assert len(responses) == 1
        assert responses[0][1]["action"] == "terminate"


# ===========================================================================
# Failure budget mechanics
# ===========================================================================


@pytest.mark.asyncio
class TestFailureBudget:
    async def test_unhealthy_set_blocks_dispatch(self) -> None:
        # Direct-state test of the unhealthy gate; the transition
        # mechanics (how a label gets into _unhealthy_models via
        # reconnect failures) are tested below.
        router = _make_router()
        _populate_post_start_state(
            router, healthy_labels=[], unhealthy_labels=["strong"]
        )
        responses: List[tuple[CanonicalId, Dict[str, Any]]] = []
        async def on_response(cid: CanonicalId, w: Dict[str, Any]) -> None: responses.append((cid, w))
        async def on_complete(cid: CanonicalId) -> None: pass

        q = _make_analyze_query(model="strong")
        wire = translate_query_to_wire(q, "cid-1")
        await router.dispatch(CanonicalId("cid-1"), wire, q, on_response, on_complete)
        assert "unavailable" in responses[0][1]["error"]

    async def test_reconnect_loop_decrements_budget_and_marks_unhealthy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Patch websockets.connect to always fail; patch asyncio.sleep
        # to be a no-op so the loop iterates immediately. After the
        # budget is consumed the label transitions to unhealthy and
        # the loop exits.
        import websockets

        async def always_fail(*args: Any, **kwargs: Any) -> _MockWebSocket:
            raise ConnectionError("simulated upstream down")

        async def no_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr(websockets, "connect", always_fail)
        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        router = _make_router(max_connect_failures=2)
        # Set up minimal pre-start state for one label.
        router._url_for_label["strong"] = "ws://h1:1"
        router._failure_budget["strong"] = router._max_connect_failures

        await router._reconnect_with_backoff("strong")

        assert "strong" in router._unhealthy_models
        assert router._failure_budget["strong"] <= 0
        assert "strong" not in router._connections

    async def test_reconnect_loop_succeeds_within_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # First attempt fails, second succeeds. Budget should not be
        # exhausted; label should be in _connections.
        import websockets

        attempts = {"n": 0}

        async def fail_then_succeed(*args: Any, **kwargs: Any) -> _MockWebSocket:
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionError("simulated transient blip")
            return _MockWebSocket()

        async def no_sleep(_delay: float) -> None:
            return None

        monkeypatch.setattr(websockets, "connect", fail_then_succeed)
        monkeypatch.setattr(asyncio, "sleep", no_sleep)

        router = _make_router(max_connect_failures=3)
        router._url_for_label["strong"] = "ws://h1:1"
        router._failure_budget["strong"] = router._max_connect_failures

        await router._reconnect_with_backoff("strong")

        assert "strong" in router._connections
        assert "strong" not in router._unhealthy_models
        # One failure consumed → budget reduced by 1.
        assert router._failure_budget["strong"] == 2
        # Cleanup: cancel the spawned reader task so the test
        # doesn't leak it.
        for t in router._reader_tasks.values():
            t.cancel()


# ===========================================================================
# Capabilities advertiser includes selector when role is SELECTOR
# ===========================================================================


class TestSelectorAdvertisement:
    def test_selector_advertised_when_role_is_selector(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # _build_advertised_capabilities reads cfg.ROLE; patch it.
        monkeypatch.setattr(cfg, "ROLE", "SELECTOR")
        from proxy_server import _build_advertised_capabilities
        advertised = _build_advertised_capabilities()
        assert "selector" in advertised
        assert advertised["selector"] == {}

    def test_selector_not_advertised_when_role_is_leaf(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cfg, "ROLE", "LEAF")
        from proxy_server import _build_advertised_capabilities
        advertised = _build_advertised_capabilities()
        assert "selector" not in advertised

    def test_selector_not_advertised_when_role_is_relay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cfg, "ROLE", "RELAY")
        from proxy_server import _build_advertised_capabilities
        advertised = _build_advertised_capabilities()
        assert "selector" not in advertised

    def test_role_check_is_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # cfg.ROLE.upper() in _build_advertised_capabilities.
        monkeypatch.setattr(cfg, "ROLE", "selector")
        from proxy_server import _build_advertised_capabilities
        advertised = _build_advertised_capabilities()
        assert "selector" in advertised
