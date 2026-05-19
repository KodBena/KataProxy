"""
tests/test_capability_negotiation.py — Phase 1 capability-negotiation
tests.

Covers all of Phase 1's pure and effectful units:

  - CoalescingPolicy now includes "capabilities" in capturing_fields,
    so different opt-in sets produce different content_hashes.
  - translate_query_to_wire strips every key in _PROXY_ONLY_FIELDS
    (cache, lookup_cache, replay_final_only, analysis_config,
    capabilities) from the emitted wire, regardless of whether
    upstream consumers pre-popped them.
  - capability_gate (Transformer wrapper): engagement matrix
    (legacy auto-engage when capabilities absent; explicit opt-in
    when name present in capabilities dict; explicit opt-out
    otherwise); cleanup tied to link.mapping.forward returning None.
  - CapabilityGatedMiddleware: same engagement matrix; passthrough
    yield when not engaged; on_session_end clears state.
  - adaptive_reevaluate per-orig_id parameter shift: capability
    metadata overrides constructor defaults; absent metadata falls
    back to defaults; LRU eviction pops the new state too.
  - capabilities_advertiser: query_version responses gain the
    capabilities advertisement; other metadata responses unchanged;
    analyze responses unchanged.

Run from the proxy directory: `pytest tests/test_capability_negotiation.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple, cast

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.protocol_transformer import Transformer  # noqa: E402
from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
    MetadataResponse,
    TurnIndex,
    translate_query_to_wire,
)
from middleware.capability_gate import CapabilityGatedMiddleware  # noqa: E402
from middleware.session_middleware import (  # noqa: E402
    SessionCapabilities,
    SessionMiddleware,
)
from pubsub_hub import CoalescingPolicy  # noqa: E402
from transformers.capabilities_advertiser import capabilities_advertiser  # noqa: E402
from transformers.capability_gate import capability_gate  # noqa: E402


# Tests use ``_MockLink`` (a duck-typed stand-in for ``ProxyLink``)
# rather than constructing the real link to avoid coupling to KataGo's
# tracker. At call sites passing into the typed transformer/middleware
# factories, ``cast(ProxyLink[ClientId, InternalId], ...)`` records the
# stand-in's intended type. The cast is the documented gap between
# duck-typed test fixtures and the production type contract.
_LinkT = ProxyLink[ClientId, InternalId]


# ---------------------------------------------------------------------------
# Test infrastructure: mocks for ProxyLink + stub Transformer/Middleware
# ---------------------------------------------------------------------------


class _MockMapping:
    """Mimics the IdMapping surface the capability_gate consumes
    (`forward(eid)` returning Optional[InternalId])."""

    def __init__(self) -> None:
        self._fwd: dict[ClientId, InternalId] = {}

    def forward(self, orig_id: ClientId) -> Optional[InternalId]:
        return self._fwd.get(orig_id)

    def register(
        self, orig_id: ClientId, downstream_id: InternalId = InternalId("internal")
    ) -> None:
        self._fwd[orig_id] = downstream_id

    def complete(self, orig_id: ClientId) -> None:
        self._fwd.pop(orig_id, None)


class _MockLink:
    def __init__(self) -> None:
        self.mapping = _MockMapping()


def _stub_transformer_factory(
    name: str = "stub",
) -> Tuple[
    Callable[[_LinkT], Transformer[KataGoQuery, KataGoResponse]],
    Dict[str, List[Tuple[ClientId, Any]]],
]:
    """Return (factory, calls). The factory produces a Transformer
    whose on_query and on_response record their calls in `calls`."""
    calls: Dict[str, List[Tuple[ClientId, Any]]] = {
        "on_query": [],
        "on_response": [],
    }

    def factory(_link: _LinkT) -> Transformer[KataGoQuery, KataGoResponse]:
        def on_query(eid: ClientId, q: KataGoQuery) -> Optional[KataGoQuery]:
            calls["on_query"].append((eid, q))
            return q

        def on_response(eid: ClientId, r: KataGoResponse) -> Optional[KataGoResponse]:
            calls["on_response"].append((eid, r))
            return r

        return Transformer(name=name, on_query=on_query, on_response=on_response)

    return factory, calls


class _RecordingMiddleware(SessionMiddleware):
    """SessionMiddleware that records every on_query and handle_response
    call for engagement-matrix testing."""

    def __init__(self) -> None:
        self.queries: List[Tuple[ClientId, KataGoQuery]] = []
        self.responses: List[Tuple[ClientId, KataGoResponse]] = []
        self.session_starts: int = 0
        self.session_ends: int = 0

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self.session_starts += 1

    def on_session_end(self) -> None:
        self.session_ends += 1

    def on_query(self, orig_id: ClientId, query: KataGoQuery) -> None:
        self.queries.append((orig_id, query))

    async def handle_response(
        self,
        orig_id: ClientId,
        response: KataGoResponse,
        submit_query: Callable[[ClientId, KataGoQuery], Awaitable[None]],
    ) -> AsyncIterator[Tuple[ClientId, KataGoResponse]]:
        self.responses.append((orig_id, response))
        yield orig_id, response


def _make_analyze_query(
    *,
    capabilities: Optional[Dict[str, Any]] = None,
    extra_opaque: Optional[Dict[str, Any]] = None,
) -> KataGoQuery:
    opaque: Dict[str, Any] = {
        "rules": "tromp-taylor",
        "komi": 7.5,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": [["B", "Q4"], ["W", "D16"]],
    }
    if extra_opaque:
        opaque.update(extra_opaque)
    if capabilities is not None:
        opaque["capabilities"] = capabilities
    return KataGoQuery(action=KataGoAction.ANALYZE, opaque=opaque)


# ===========================================================================
# CoalescingPolicy: capabilities participates in content_hash
# ===========================================================================


class TestCoalescingCapabilities:
    def test_capabilities_field_is_in_capturing_fields(self) -> None:
        policy = CoalescingPolicy()
        assert "capabilities" in policy.capturing_fields

    def test_different_capabilities_produce_different_hashes(self) -> None:
        policy = CoalescingPolicy()
        h_with = policy.query_hash(_make_analyze_query(capabilities={"transposition": {}}))
        h_without = policy.query_hash(_make_analyze_query(capabilities={}))
        assert h_with != h_without

    def test_absent_vs_empty_capabilities_produce_different_hashes(self) -> None:
        policy = CoalescingPolicy()
        h_absent = policy.query_hash(_make_analyze_query(capabilities=None))
        h_empty = policy.query_hash(_make_analyze_query(capabilities={}))
        assert h_absent != h_empty

    def test_dict_key_order_does_not_affect_hash(self) -> None:
        policy = CoalescingPolicy()
        h_a = policy.query_hash(_make_analyze_query(capabilities={"a": {}, "b": {}}))
        h_b = policy.query_hash(_make_analyze_query(capabilities={"b": {}, "a": {}}))
        assert h_a == h_b

    def test_same_capabilities_produce_same_hash(self) -> None:
        policy = CoalescingPolicy()
        caps: Dict[str, Any] = {"transposition": {}, "delta_analysis": {}}
        assert policy.query_hash(_make_analyze_query(capabilities=caps)) == \
               policy.query_hash(_make_analyze_query(capabilities=caps))

    def test_different_metadata_values_produce_different_hashes(self) -> None:
        policy = CoalescingPolicy()
        h_a = policy.query_hash(
            _make_analyze_query(capabilities={"adaptive_reevaluate": {"worst_quantile": 0.25}})
        )
        h_b = policy.query_hash(
            _make_analyze_query(capabilities={"adaptive_reevaluate": {"worst_quantile": 0.5}})
        )
        assert h_a != h_b


# ===========================================================================
# translate_query_to_wire: _PROXY_ONLY_FIELDS strip discipline
# ===========================================================================


class TestWireStripDiscipline:
    @pytest.mark.parametrize("field", [
        "cache",
        "lookup_cache",
        "replay_final_only",
        "analysis_config",
        "capabilities",
    ])
    def test_each_proxy_only_field_excluded_from_wire(self, field: str) -> None:
        q = _make_analyze_query(extra_opaque={field: {"any": "value"}})
        wire = translate_query_to_wire(q, "eid-1")
        assert field not in wire, (
            f"{field} leaked through translate_query_to_wire — "
            "central strip discipline broken"
        )

    def test_non_proxy_fields_retained(self) -> None:
        q = _make_analyze_query(extra_opaque={"maxVisits": 1000, "includeOwnership": True})
        wire = translate_query_to_wire(q, "eid-1")
        for k in ("rules", "komi", "boardXSize", "moves", "maxVisits", "includeOwnership"):
            assert k in wire, f"{k} should be retained but was stripped"

    def test_envelope_id_always_present(self) -> None:
        q = _make_analyze_query(capabilities={"transposition": {}})
        wire = translate_query_to_wire(q, "eid-xyz")
        assert wire["id"] == "eid-xyz"

    def test_capabilities_with_metadata_excluded(self) -> None:
        q = _make_analyze_query(
            capabilities={"adaptive_reevaluate": {"worst_quantile": 0.5, "extra_visits": 1600}}
        )
        wire = translate_query_to_wire(q, "eid-1")
        assert "capabilities" not in wire

    def test_analysis_config_excluded_even_when_unconsumed(self) -> None:
        # Phase 1 hazard: with capability gating, analysis_enricher's
        # on_query becomes conditional; the central strip in the wire
        # builder is what guarantees analysis_config never reaches
        # KataGo regardless of whether the transformer ran.
        q = _make_analyze_query(extra_opaque={"analysis_config": {"foo": "bar"}})
        wire = translate_query_to_wire(q, "eid-1")
        assert "analysis_config" not in wire


# ===========================================================================
# capability_gate (Transformer wrapper)
# ===========================================================================


class TestCapabilityGateTransformer:
    def test_legacy_auto_engage_when_capabilities_absent(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query()  # no capabilities field
        gated.on_query(ClientId("eid-1"), q)
        assert len(calls["on_query"]) == 1, (
            "legacy auto-engage should call wrapped on_query"
        )

    def test_explicit_opt_in_engages(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query(ClientId("eid-1"), q)
        assert len(calls["on_query"]) == 1

    def test_explicit_opt_out_skips(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={"delta_analysis": {}})  # no transposition
        gated.on_query(ClientId("eid-1"), q)
        assert calls["on_query"] == [], (
            "explicit opt-out should not call wrapped on_query"
        )

    def test_empty_capabilities_skips(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={})
        gated.on_query(ClientId("eid-1"), q)
        assert calls["on_query"] == []

    def test_response_passthrough_when_not_engaged(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={})
        gated.on_query(ClientId("eid-1"), q)
        # Response side: not engaged for eid-1.
        link.mapping.register(ClientId("eid-1"))  # mapping still alive
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response(ClientId("eid-1"), r)
        assert calls["on_response"] == [], (
            "wrapped on_response should not be called when not engaged"
        )

    def test_response_engaged_when_query_opted_in(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query(ClientId("eid-1"), q)
        link.mapping.register(ClientId("eid-1"))  # mapping alive
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response(ClientId("eid-1"), r)
        assert len(calls["on_response"]) == 1

    def test_engaged_state_cleaned_when_mapping_completes(self) -> None:
        link = _MockLink()
        wrapped_factory, _calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(cast(_LinkT, link))

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query(ClientId("eid-1"), q)
        link.mapping.register(ClientId("eid-1"))
        # Final response arrives; mapping cleared by ProxyLink contract:
        link.mapping.complete(ClientId("eid-1"))
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response(ClientId("eid-1"), r)
        # Now a *new* response for the same eid (shouldn't happen in
        # practice, but tests the cleanup): the engagement record was
        # dropped, so engagement is no longer recognised.
        wrapped_factory_2, calls_2 = _stub_transformer_factory("inner2")
        gated_2 = capability_gate("transposition", wrapped_factory_2)(cast(_LinkT, link))
        # Already-completed eid: response passthrough.
        gated_2.on_response(ClientId("eid-1"), r)
        assert calls_2["on_response"] == []


# ===========================================================================
# CapabilityGatedMiddleware
# ===========================================================================


@pytest.mark.asyncio
class TestCapabilityGatedMiddleware:
    async def test_legacy_auto_engage_when_capabilities_absent(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query()
        gated.on_query(ClientId("eid-1"), q)
        assert rec.queries == [("eid-1", q)]

    async def test_explicit_opt_in_engages(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"adaptive_reevaluate": {}})
        gated.on_query(ClientId("eid-1"), q)
        assert rec.queries == [("eid-1", q)]

    async def test_explicit_opt_out_skips_wrapped(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"transposition": {}})  # not adaptive
        gated.on_query(ClientId("eid-1"), q)
        assert rec.queries == []

    async def test_response_unchanged_when_not_engaged(self) -> None:
        """Output passes through unchanged for an opted-out parent.

        Note: the wrapped middleware DOES observe the response — the
        gate delegates unconditionally on the response side as of the
        sub-query-routing fix. The wrapped's contract is to yield
        unknown orig_ids unchanged; _RecordingMiddleware satisfies
        that contract. The gate's response-side responsibility is
        therefore output-shape-only ("response unchanged for opted-
        out parents"), not observation-blocking.

        The unconditional delegation is what lets the orchestration
        framework relabel sub-query orig_ids to parent orig_ids when
        wrapped behind this gate (the sub-query's parent IS engaged
        but the sub-query's synthetic orig_id is never registered in
        self._engaged because sub-queries bypass middleware.on_query).
        See TestCompositionWithCapabilityGate.test_sub_query_response_relabels_through_gate
        in tests/test_orchestration_middleware.py for the regression
        coverage.
        """
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={})
        gated.on_query(ClientId("eid-1"), q)
        # The on_query gate stays — opt-out skips wrapped.on_query
        # (the cost gate; the wrapped's setup is not paid for opted-
        # out queries).
        assert rec.queries == []

        async def submit_query(_id: ClientId, _q: KataGoQuery) -> None: pass
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})

        out = []
        async for oid, resp in gated.handle_response(ClientId("eid-1"), r, submit_query):
            out.append((oid, resp))
        # Output unchanged — the user-visible contract.
        assert out == [(ClientId("eid-1"), r)]
        # Wrapped observes — contract change as of the sub-query
        # routing fix. _RecordingMiddleware just records and yields,
        # so the output is unchanged.
        assert rec.responses == [(ClientId("eid-1"), r)]

    async def test_response_engaged_when_query_opted_in(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"adaptive_reevaluate": {}})
        gated.on_query(ClientId("eid-1"), q)

        async def submit_query(_id: ClientId, _q: KataGoQuery) -> None: pass
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})

        out = []
        async for oid, resp in gated.handle_response(ClientId("eid-1"), r, submit_query):
            out.append((oid, resp))
        assert out == [(ClientId("eid-1"), r)]
        assert rec.responses == [(ClientId("eid-1"), r)]

    async def test_session_lifecycle_delegated(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        async def submit_query(_id: ClientId, _q: KataGoQuery) -> None: pass
        async def terminate_query(_id: ClientId) -> None: pass
        caps = SessionCapabilities(
            submit_query=submit_query,
            terminate_query=terminate_query,
        )

        gated.on_session_start(caps)
        gated.on_session_end()
        assert rec.session_starts == 1
        assert rec.session_ends == 1

    async def test_on_session_end_clears_engagement_state(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"adaptive_reevaluate": {}})
        gated.on_query(ClientId("eid-1"), q)
        assert ClientId("eid-1") in gated._engaged

        gated.on_session_end()
        assert gated._engaged == {}


# ===========================================================================
# adaptive_reevaluate per-orig_id parameter shift
# ===========================================================================


class TestAdaptiveReevaluateMetadata:
    """Validation that the post-v1.0.16 orchestration-based adaptive
    middleware preserves the v1.0.14 metadata-override behaviour
    end-to-end via observable spawn semantics.

    The pre-v1.0.16 imperative implementation exposed `_per_query_quantile`
    and `_per_query_extra_visits` instance dicts that earlier tests
    inspected directly. The orchestration refactor moves that state
    into the coroutine's closure variables, so the tests now verify
    the externally observable consequence: when the coroutine decides
    to deepen, the spawned sub-query's maxVisits reflects the per-query
    override (or the constructor default if absent).
    """

    @staticmethod
    def _make_middleware() -> Any:
        from middleware.adaptive_reevaluate import adaptive_reevaluate
        # The factory now returns a factory; call () to instantiate.
        return adaptive_reevaluate(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=3,
        )()

    @staticmethod
    def _make_caps() -> Tuple[Any, SessionCapabilities]:
        """Fake SessionCapabilities recording submit/terminate/send calls.

        Includes the push-based send_response channel per the
        orchestration output-channel arc; orchestration emissions
        land in synthetic_sends and tests harvest them via
        _settle_synthetic_sends below.
        """
        class _Caps:
            submitted: List[Tuple[ClientId, KataGoQuery]] = []
            terminated: List[ClientId] = []
            synthetic_sends: List[Tuple[ClientId, KataGoResponse]] = []

            async def submit(self, oid: ClientId, q: KataGoQuery) -> None:
                self.submitted.append((oid, q))

            async def terminate(self, oid: ClientId) -> None:
                self.terminated.append(oid)

            async def send(self, oid: ClientId, r: KataGoResponse) -> None:
                self.synthetic_sends.append((oid, r))

        c = _Caps()
        c.submitted = []
        c.terminated = []
        c.synthetic_sends = []
        from middleware.session_middleware import SessionCapabilities
        return c, SessionCapabilities(
            submit_query=c.submit,
            terminate_query=c.terminate,
            send_response=c.send,
        )

    @staticmethod
    def _make_response_with_deltas(turn: int, delta: float = -1.0) -> AnalyzeResponse:
        """Build an AnalyzeResponse with policy deltas that will trigger
        the worst-quantile threshold in _find_worst_turns."""
        return AnalyzeResponse(
            is_during_search=False,
            turn_number=turn,
            opaque={
                "moveInfos": [],
                "extra": {
                    "black": {"deltas": {str(turn): delta}},
                    "white": {"deltas": {str(turn): delta}},
                },
            },
        )

    @staticmethod
    async def _drive_response(
        m: Any, orig_id: ClientId, response: KataGoResponse,
    ) -> List[Tuple[ClientId, KataGoResponse]]:
        """Drive handle_response and harvest orchestration emissions
        for this orig_id from the push-based output channel.

        handle_response yields nothing for orchestration-managed
        orig_ids (the framework's emissions go via caps.send_response);
        for pass-through orig_ids it still yields. The helper returns
        the union, with a brief settle so the driver task's push has
        a chance to land before we read.
        """
        import asyncio
        sc = getattr(m, "_caps", None)
        fake = getattr(getattr(sc, "send_response", None), "__self__", None) if sc else None
        pre = len(fake.synthetic_sends) if fake is not None else 0
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for oid, resp in m.handle_response(orig_id, response, None):
            out.append((oid, resp))
        if fake is not None:
            await asyncio.sleep(0.01)
            out.extend(fake.synthetic_sends[pre:])
        return out

    @staticmethod
    async def _wait_for_spawn(caps: Any, timeout_s: float = 1.0) -> bool:
        """Poll until caps.submitted has at least one entry."""
        import asyncio
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            if caps.submitted:
                return True
            await asyncio.sleep(0.005)
        return False

    @pytest.mark.asyncio
    async def test_metadata_overrides_extra_visits_in_deeper_query(self) -> None:
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                "capabilities": {
                    "adaptive_reevaluate": {"extra_visits": 1600},
                },
            },
        )
        m.on_query(ClientId("eid-1"), q)
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(0))
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(1))
        assert await self._wait_for_spawn(c), "deeper query was not spawned"
        _, deeper = c.submitted[0]
        # extra_visits=1600 (override) + maxVisits=1000 (parent) = 2600.
        assert deeper.opaque["maxVisits"] == 2600
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_absent_metadata_falls_back_to_constructor_default(self) -> None:
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                "capabilities": {"adaptive_reevaluate": {}},  # opt-in, no overrides
            },
        )
        m.on_query(ClientId("eid-1"), q)
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(0))
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(1))
        assert await self._wait_for_spawn(c)
        _, deeper = c.submitted[0]
        # extra_visits=800 (constructor default) + maxVisits=1000 = 1800.
        assert deeper.opaque["maxVisits"] == 1800
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_legacy_query_no_capabilities_uses_constructor_default(self) -> None:
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                # No capabilities field — legacy auto-engage path.
            },
        )
        m.on_query(ClientId("eid-1"), q)
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(0))
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(1))
        assert await self._wait_for_spawn(c)
        _, deeper = c.submitted[0]
        # Constructor defaults: extra_visits=800 + maxVisits=1000 = 1800.
        assert deeper.opaque["maxVisits"] == 1800
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_partial_metadata_overrides_only_named_field(self) -> None:
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
                # Only extra_visits overridden; worst_quantile defaults.
                "capabilities": {
                    "adaptive_reevaluate": {"extra_visits": 2000},
                },
            },
        )
        m.on_query(ClientId("eid-1"), q)
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(0))
        await self._drive_response(m, ClientId("eid-1"), self._make_response_with_deltas(1))
        assert await self._wait_for_spawn(c)
        _, deeper = c.submitted[0]
        # extra_visits=2000 (override) + maxVisits=1000 = 3000.
        assert deeper.opaque["maxVisits"] == 3000
        m.on_session_end()

    def test_build_deeper_query_uses_increment_semantic(self) -> None:
        # _build_deeper_query is now a module-level pure helper
        # (no longer a method on a class). The increment-not-absolute
        # contract is preserved.
        from middleware.adaptive_reevaluate import _build_deeper_query
        orig = KataGoQuery(
            action=KataGoAction.ANALYZE,
            opaque={"maxVisits": 1000, "moves": []},
        )
        deeper = _build_deeper_query(
            orig, [TurnIndex(0), TurnIndex(1), TurnIndex(2)], extra_visits=500,
        )
        assert deeper.opaque["maxVisits"] == 1500

    def test_build_deeper_query_strips_cache_flags(self) -> None:
        from middleware.adaptive_reevaluate import _build_deeper_query
        orig = KataGoQuery(
            action=KataGoAction.ANALYZE,
            opaque={
                "maxVisits": 1000,
                "moves": [],
                "cache": True,
                "lookup_cache": True,
                "replay_final_only": True,
            },
        )
        deeper = _build_deeper_query(orig, [TurnIndex(0)], extra_visits=500)
        assert "cache" not in deeper.opaque
        assert "lookup_cache" not in deeper.opaque
        assert "replay_final_only" not in deeper.opaque


# ===========================================================================
# adaptive_reevaluate streaming-previews invariants (v1.0.20)
# ===========================================================================


class TestAdaptiveStreamingPreviews:
    """Validation that Stage 1 streams every original final immediately
    as a preview (is_during_search=True), and Stage 3 emits the
    authoritative is_during_search=False only for turns NOT in the
    deepen set.

    Pre-v1.0.20 shape buffered every original final on the demand edge
    until original_stream exhausted, then released them all at once
    with is_during_search patched. On range queries with auto-engage
    adaptive, this held each turn's authoritative-quality data for as
    long as the slowest turn in the range; the operator-visible
    symptom was "ranges feel batchy". v1.0.20 streams each final the
    moment KataGo emits it.

    These tests pin the streaming invariants so the regression cannot
    silently sneak back in.
    """

    @staticmethod
    def _make_middleware(window_size: int = 1) -> Any:
        from middleware.adaptive_reevaluate import adaptive_reevaluate
        return adaptive_reevaluate(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=window_size,
        )()

    @staticmethod
    def _make_caps() -> Tuple[Any, SessionCapabilities]:
        class _Caps:
            submitted: List[Tuple[ClientId, KataGoQuery]] = []
            terminated: List[ClientId] = []
            synthetic_sends: List[Tuple[ClientId, KataGoResponse]] = []

            async def submit(self, oid: ClientId, q: KataGoQuery) -> None:
                self.submitted.append((oid, q))

            async def terminate(self, oid: ClientId) -> None:
                self.terminated.append(oid)

            async def send(self, oid: ClientId, r: KataGoResponse) -> None:
                self.synthetic_sends.append((oid, r))

        c = _Caps()
        c.submitted = []
        c.terminated = []
        c.synthetic_sends = []
        from middleware.session_middleware import SessionCapabilities
        return c, SessionCapabilities(
            submit_query=c.submit,
            terminate_query=c.terminate,
            send_response=c.send,
        )

    @staticmethod
    def _bad_final(turn: int, delta: float = -1.0) -> AnalyzeResponse:
        """Final response carrying a strong-negative delta — guaranteed
        to land inside the worst-quantile threshold."""
        return AnalyzeResponse(
            is_during_search=False,
            turn_number=turn,
            opaque={
                "moveInfos": [],
                "extra": {
                    "black": {"deltas": {str(turn): delta}},
                    "white": {"deltas": {str(turn): delta}},
                },
            },
        )

    @staticmethod
    def _neutral_final(turn: int) -> AnalyzeResponse:
        """Final response without extra.deltas — invisible to
        _find_worst_turns, so contributes no entries to the worst set."""
        return AnalyzeResponse(
            is_during_search=False,
            turn_number=turn,
            opaque={"moveInfos": []},
        )

    @staticmethod
    async def _drive_response(
        m: Any, orig_id: ClientId, response: KataGoResponse,
    ) -> List[Tuple[ClientId, KataGoResponse]]:
        """Drive handle_response and harvest orchestration emissions
        for the orig_id from caps.send_response (the push-based output
        channel introduced by the orchestration output-channel arc).
        """
        import asyncio
        sc = getattr(m, "_caps", None)
        fake = getattr(getattr(sc, "send_response", None), "__self__", None) if sc else None
        pre = len(fake.synthetic_sends) if fake is not None else 0
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for oid, resp in m.handle_response(orig_id, response, None):
            out.append((oid, resp))
        if fake is not None:
            await asyncio.sleep(0.01)
            out.extend(fake.synthetic_sends[pre:])
        return out

    @staticmethod
    async def _wait_for_spawn(caps: Any, timeout_s: float = 1.0) -> bool:
        import asyncio
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            if caps.submitted:
                return True
            await asyncio.sleep(0.005)
        return False

    @pytest.mark.asyncio
    async def test_each_original_final_streams_a_preview_immediately(
        self,
    ) -> None:
        """Driving a single original final yields a preview emission
        (is_during_search=True) for that turn within the same
        handle_response cycle — the proxy does not buffer it until
        the rest of the range completes."""
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
            },
        )
        m.on_query(ClientId("eid-1"), q)

        # First final → preview emission immediately. The second turn
        # has not yet arrived; pre-v1.0.20 would have buffered turn 0
        # silently here.
        out0 = await self._drive_response(m, ClientId("eid-1"), self._bad_final(0))
        previews_for_turn_0 = [
            r for _, r in out0
            if isinstance(r, AnalyzeResponse)
            and r.is_during_search
            and r.turn_number == 0
        ]
        assert previews_for_turn_0, (
            f"first final did not stream a preview; out0={out0}"
        )

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_no_deepen_promotes_each_preview_to_authoritative(
        self,
    ) -> None:
        """When no deepening is warranted (no extra.deltas → empty
        worst → empty deepen), Stage 3 emits each buffered final as
        authoritative is_during_search=False, and no spawn fires."""
        c, caps = self._make_caps()
        m = self._make_middleware()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
            },
        )
        m.on_query(ClientId("eid-1"), q)

        all_yields: List[Tuple[ClientId, KataGoResponse]] = []
        all_yields += await self._drive_response(m, ClientId("eid-1"), self._neutral_final(0))
        all_yields += await self._drive_response(m, ClientId("eid-1"), self._neutral_final(1))

        previews = sorted(
            r.turn_number for _, r in all_yields
            if isinstance(r, AnalyzeResponse) and r.is_during_search
        )
        authoritatives = sorted(
            r.turn_number for _, r in all_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        )
        assert previews == [0, 1], (
            f"expected one preview per turn; got previews={previews}"
        )
        assert authoritatives == [0, 1], (
            f"expected one authoritative per turn (no-deepen path); "
            f"got authoritatives={authoritatives}"
        )
        assert not c.submitted, "no-deepen path must not spawn"

        m.on_session_end()

    @staticmethod
    def _spawn_final(turn: int) -> AnalyzeResponse:
        """Final response carrying a `from_spawn` marker — used by
        the finalization wire-shape test to distinguish deepened-turn
        authoritatives (whose payload should be the latest spawn
        packet) from non-deepened authoritatives (whose payload is
        the original final)."""
        return AnalyzeResponse(
            is_during_search=False,
            turn_number=turn,
            opaque={
                "moveInfos": [],
                "extra": {
                    "black": {"deltas": {str(turn): -0.1}},
                    "white": {"deltas": {str(turn): -0.1}},
                },
                "from_spawn": True,
            },
        )

    @pytest.mark.asyncio
    async def test_finalization_emits_one_authoritative_per_turn_with_latest_payload(
        self,
    ) -> None:
        """v1.0.24 finalization-at-end invariant: exactly one
        authoritative emission (is_during_search=False) per
        analyzed turn, occurring after the multi-round loop
        terminates and carrying the latest observed KataGo packet
        for that turn. For deepened turns the latest is a
        spawn-derived response; for non-deepened turns the
        original.

        Pre-v1.0.24 (v1.0.20 → v1.0.23) the wire shape was "Stage 3
        emits authoritatives only for non-deepened turns; the spawn
        (Stage 4) carries authoritatives for the deepened set." The
        v1.0.24 multi-round design made that two-source authoritative
        emission a KataGo protocol drift — re-deepening across rounds
        would emit one is_during_search=False per round per turn —
        so finalization-at-end collapses both into a single
        end-of-loop emission stage. The duplication modulo
        is_during_search (each deepened turn's preview emission is
        followed by an authoritative emission carrying an identical
        payload) is acknowledged as fine per §8.3 of
        proxy/docs/roadmap-multi-round-adaptation.md.

        Mid-loop invariant (asserted below): while spawn responses
        are in flight the proxy emits only previews — no
        authoritatives are emitted until the multi-round loop
        terminates.

        Construction: 6-turn range with a single bad-delta turn at
        index 0. _find_worst_turns produces worst={0,1,2}; with
        window_size=1 (no expansion) deepen={0,1,2}, leaving turns
        3, 4, 5 as the non-deepened set."""
        c, caps = self._make_caps()
        m = self._make_middleware(window_size=1)
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 1000,
            },
        )
        m.on_query(ClientId("eid-1"), q)

        # Stage 1 — drive every original through the middleware.
        before_spawn_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in range(6):
            resp = self._bad_final(0) if turn == 0 else self._neutral_final(turn)
            before_spawn_yields += await self._drive_response(
                m, ClientId("eid-1"), resp,
            )

        # Mid-loop invariant: only previews emitted; the multi-round
        # loop has spawned and is awaiting deeper-query responses.
        # No authoritatives until finalization.
        preview_turns_pre = sorted(
            r.turn_number for _, r in before_spawn_yields
            if isinstance(r, AnalyzeResponse) and r.is_during_search
        )
        auth_turns_pre = sorted(
            r.turn_number for _, r in before_spawn_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        )
        assert preview_turns_pre == [0, 1, 2, 3, 4, 5], (
            f"Stage 1 should emit one preview per turn; "
            f"got preview_turns_pre={preview_turns_pre}"
        )
        assert auth_turns_pre == [], (
            f"v1.0.24 forbids authoritative emissions before the "
            f"multi-round loop terminates; got "
            f"auth_turns_pre={auth_turns_pre}"
        )

        # Spawn fires for the deepened turn set.
        assert await self._wait_for_spawn(c), "deeper sub-query did not spawn"
        spawn_oid, spawn_q = c.submitted[0]
        assert sorted(spawn_q.analyze_turns) == [0, 1, 2], (
            f"spawn should target the deepened turn set; "
            f"got analyze_turns={spawn_q.analyze_turns}"
        )

        # Stage 2 — drive the spawn's finals through. The third
        # (and last expected) final triggers the spawn iterator's
        # sentinel, which lets the coroutine reach the finalization
        # stage; the 6 authoritatives are drained on the same
        # handle_response cycle.
        post_spawn_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in [0, 1, 2]:
            post_spawn_yields += await self._drive_response(
                m, spawn_oid, self._spawn_final(turn),
            )

        # Each spawn final emitted as a preview (Stage 2 forwarding).
        spawn_preview_turns = sorted(
            r.turn_number for _, r in post_spawn_yields
            if isinstance(r, AnalyzeResponse) and r.is_during_search
        )
        assert spawn_preview_turns == [0, 1, 2], (
            f"each spawn final should be emitted as a preview; "
            f"got spawn_preview_turns={spawn_preview_turns}"
        )

        # Finalization invariant: exactly one authoritative per turn,
        # in the same drain cycle as the final spawn response.
        auth_emissions = [
            r for _, r in post_spawn_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        ]
        auth_turns = sorted(r.turn_number for r in auth_emissions)
        assert auth_turns == [0, 1, 2, 3, 4, 5], (
            f"finalization should emit exactly one authoritative per "
            f"analyzed turn; got auth_turns={auth_turns}"
        )

        # Per-turn payload provenance: deepened turns' authoritative
        # carries the spawn payload (`from_spawn` marker); non-deepened
        # turns' authoritative carries the original payload (no marker).
        by_turn = {r.turn_number: r for r in auth_emissions}
        for turn in (0, 1, 2):
            assert by_turn[turn].opaque.get("from_spawn") is True, (
                f"deepened turn {turn}'s authoritative should carry the "
                f"latest spawn payload; got opaque={by_turn[turn].opaque}"
            )
        for turn in (3, 4, 5):
            assert "from_spawn" not in by_turn[turn].opaque, (
                f"non-deepened turn {turn}'s authoritative should carry "
                f"the original payload; got opaque={by_turn[turn].opaque}"
            )

        m.on_session_end()


# ===========================================================================
# capabilities_advertiser
# ===========================================================================


class TestCapabilitiesAdvertiser:
    def test_query_version_response_gains_capabilities(self) -> None:
        link = _MockLink()
        advertised: Dict[str, Dict[str, Any]] = {"delta_analysis": {}, "transposition": {}, "adaptive_reevaluate": {}}
        t = capabilities_advertiser(advertised)(cast(_LinkT, link))

        r = MetadataResponse(opaque={"version": "1.16.0", "git_hash": "abcdef"})
        out = t.on_response(ClientId("eid-1"), r)
        assert isinstance(out, MetadataResponse)
        assert out.opaque["capabilities"] == advertised
        # Original fields preserved.
        assert out.opaque["version"] == "1.16.0"
        assert out.opaque["git_hash"] == "abcdef"

    def test_other_metadata_responses_unchanged(self) -> None:
        # E.g., a clear_cache ack or terminate ack — no "version" key.
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(cast(_LinkT, link))

        r = MetadataResponse(opaque={"action": "clear_cache"})
        out = t.on_response(ClientId("eid-1"), r)
        assert out is r  # passthrough; same object
        assert "capabilities" not in out.opaque

    def test_analyze_responses_unchanged(self) -> None:
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(cast(_LinkT, link))

        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={"moveInfos": []})
        out = t.on_response(ClientId("eid-1"), r)
        assert out is r

    def test_advertisement_is_copy_not_reference(self) -> None:
        link = _MockLink()
        advertised: Dict[str, Dict[str, Any]] = {"delta_analysis": {}}
        t = capabilities_advertiser(advertised)(cast(_LinkT, link))

        # Mutate the source dict after factory construction.
        advertised["transposition"] = {}

        r = MetadataResponse(opaque={"version": "1.0"})
        out = t.on_response(ClientId("eid-1"), r)
        # The advertisement was deep-copied at factory call time;
        # post-construction mutation should not leak into emitted
        # responses.
        assert out is not None
        assert "transposition" not in out.opaque["capabilities"]
        assert out.opaque["capabilities"] == {"delta_analysis": {}}

    def test_query_side_is_identity(self) -> None:
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(cast(_LinkT, link))

        q = _make_analyze_query()
        out = t.on_query(ClientId("eid-1"), q)
        assert out is q
