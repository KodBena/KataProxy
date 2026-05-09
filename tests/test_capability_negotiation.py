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
from typing import Any, Callable, Optional

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.protocol_transformer import Transformer  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    MetadataResponse,
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


# ---------------------------------------------------------------------------
# Test infrastructure: mocks for ProxyLink + stub Transformer/Middleware
# ---------------------------------------------------------------------------


class _MockMapping:
    """Mimics the IdMapping surface the capability_gate consumes
    (`forward(eid)` returning Optional[str])."""

    def __init__(self) -> None:
        self._fwd: dict[str, str] = {}

    def forward(self, orig_id: str) -> Optional[str]:
        return self._fwd.get(orig_id)

    def register(self, orig_id: str, downstream_id: str = "internal") -> None:
        self._fwd[orig_id] = downstream_id

    def complete(self, orig_id: str) -> None:
        self._fwd.pop(orig_id, None)


class _MockLink:
    def __init__(self) -> None:
        self.mapping = _MockMapping()


def _stub_transformer_factory(
    name: str = "stub",
) -> tuple[Callable[[_MockLink], Transformer], dict]:
    """Return (factory, calls). The factory produces a Transformer
    whose on_query and on_response record their calls in `calls`."""
    calls: dict[str, list] = {"on_query": [], "on_response": []}

    def factory(_link: _MockLink) -> Transformer:
        def on_query(eid: str, q: KataGoQuery) -> Optional[KataGoQuery]:
            calls["on_query"].append((eid, q))
            return q

        def on_response(eid: str, r: Any) -> Optional[Any]:
            calls["on_response"].append((eid, r))
            return r

        return Transformer(name=name, on_query=on_query, on_response=on_response)

    return factory, calls


class _RecordingMiddleware(SessionMiddleware):
    """SessionMiddleware that records every on_query and handle_response
    call for engagement-matrix testing."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, KataGoQuery]] = []
        self.responses: list[tuple[str, Any]] = []
        self.session_starts: int = 0
        self.session_ends: int = 0

    def on_session_start(self, caps: SessionCapabilities) -> None:
        self.session_starts += 1

    def on_session_end(self) -> None:
        self.session_ends += 1

    def on_query(self, orig_id: str, query: KataGoQuery) -> None:
        self.queries.append((orig_id, query))

    async def handle_response(self, orig_id, response, submit_query):
        self.responses.append((orig_id, response))
        yield orig_id, response


def _make_analyze_query(
    *,
    capabilities: Optional[dict] = None,
    extra_opaque: Optional[dict] = None,
) -> KataGoQuery:
    opaque: dict = {
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
        caps = {"transposition": {}, "delta_analysis": {}}
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
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query()  # no capabilities field
        gated.on_query("eid-1", q)
        assert len(calls["on_query"]) == 1, (
            "legacy auto-engage should call wrapped on_query"
        )

    def test_explicit_opt_in_engages(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query("eid-1", q)
        assert len(calls["on_query"]) == 1

    def test_explicit_opt_out_skips(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={"delta_analysis": {}})  # no transposition
        gated.on_query("eid-1", q)
        assert calls["on_query"] == [], (
            "explicit opt-out should not call wrapped on_query"
        )

    def test_empty_capabilities_skips(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={})
        gated.on_query("eid-1", q)
        assert calls["on_query"] == []

    def test_response_passthrough_when_not_engaged(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={})
        gated.on_query("eid-1", q)
        # Response side: not engaged for eid-1.
        link.mapping.register("eid-1")  # mapping still alive
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response("eid-1", r)
        assert calls["on_response"] == [], (
            "wrapped on_response should not be called when not engaged"
        )

    def test_response_engaged_when_query_opted_in(self) -> None:
        link = _MockLink()
        wrapped_factory, calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query("eid-1", q)
        link.mapping.register("eid-1")  # mapping alive
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response("eid-1", r)
        assert len(calls["on_response"]) == 1

    def test_engaged_state_cleaned_when_mapping_completes(self) -> None:
        link = _MockLink()
        wrapped_factory, _calls = _stub_transformer_factory("inner")
        gated = capability_gate("transposition", wrapped_factory)(link)

        q = _make_analyze_query(capabilities={"transposition": {}})
        gated.on_query("eid-1", q)
        link.mapping.register("eid-1")
        # Final response arrives; mapping cleared by ProxyLink contract:
        link.mapping.complete("eid-1")
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})
        gated.on_response("eid-1", r)
        # Now a *new* response for the same eid (shouldn't happen in
        # practice, but tests the cleanup): the engagement record was
        # dropped, so engagement is no longer recognised.
        wrapped_factory_2, calls_2 = _stub_transformer_factory("inner2")
        gated_2 = capability_gate("transposition", wrapped_factory_2)(link)
        # Already-completed eid: response passthrough.
        gated_2.on_response("eid-1", r)
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
        gated.on_query("eid-1", q)
        assert rec.queries == [("eid-1", q)]

    async def test_explicit_opt_in_engages(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"adaptive_reevaluate": {}})
        gated.on_query("eid-1", q)
        assert rec.queries == [("eid-1", q)]

    async def test_explicit_opt_out_skips_wrapped(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"transposition": {}})  # not adaptive
        gated.on_query("eid-1", q)
        assert rec.queries == []

    async def test_response_passthrough_when_not_engaged(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={})
        gated.on_query("eid-1", q)

        async def submit_query(_id, _q): pass
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})

        out = []
        async for oid, resp in gated.handle_response("eid-1", r, submit_query):
            out.append((oid, resp))
        assert out == [("eid-1", r)]
        assert rec.responses == [], (
            "wrapped middleware should not observe response when not engaged"
        )

    async def test_response_engaged_when_query_opted_in(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        q = _make_analyze_query(capabilities={"adaptive_reevaluate": {}})
        gated.on_query("eid-1", q)

        async def submit_query(_id, _q): pass
        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={})

        out = []
        async for oid, resp in gated.handle_response("eid-1", r, submit_query):
            out.append((oid, resp))
        assert out == [("eid-1", r)]
        assert rec.responses == [("eid-1", r)]

    async def test_session_lifecycle_delegated(self) -> None:
        rec = _RecordingMiddleware()
        gated = CapabilityGatedMiddleware("adaptive_reevaluate", rec)

        async def submit_query(_id, _q): pass
        async def terminate_query(_id): pass
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
        gated.on_query("eid-1", q)
        assert "eid-1" in gated._engaged

        gated.on_session_end()
        assert gated._engaged == {}


# ===========================================================================
# adaptive_reevaluate per-orig_id parameter shift
# ===========================================================================


class TestAdaptiveReevaluateMetadata:
    @staticmethod
    def _make_middleware():
        # Imported lazily because adaptive_reevaluate pulls in numpy
        # and scipy via its own module imports; tests above run without
        # those if pytest collects this class when those deps are
        # missing, the whole class fails to collect and the rest still
        # runs. (pyproject.toml lists numpy/scipy as runtime deps; if
        # the dev environment matches pyproject they are present.)
        from middleware.adaptive_reevaluate import AdaptiveReevaluateMiddleware

        return AdaptiveReevaluateMiddleware(
            worst_quantile=0.25,
            extra_visits=800,
            window_size=3,
            max_inflight=10,
        )

    def test_capability_metadata_overrides_defaults(self) -> None:
        m = self._make_middleware()
        q = _make_analyze_query(
            capabilities={"adaptive_reevaluate": {"worst_quantile": 0.5, "extra_visits": 1600}},
            extra_opaque={"maxVisits": 1000},
        )
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque=q.opaque,
        )
        m.on_query("eid-1", q)
        assert m._per_query_quantile["eid-1"] == 0.5
        assert m._per_query_extra_visits["eid-1"] == 1600

    def test_absent_metadata_falls_back_to_defaults(self) -> None:
        m = self._make_middleware()
        # Capabilities present but adaptive_reevaluate has empty metadata.
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [],
                "capabilities": {"adaptive_reevaluate": {}},
            },
        )
        m.on_query("eid-1", q)
        assert m._per_query_quantile["eid-1"] == 0.25  # constructor default
        assert m._per_query_extra_visits["eid-1"] == 800  # constructor default

    def test_legacy_query_no_capabilities_uses_defaults(self) -> None:
        m = self._make_middleware()
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [],
            },
        )
        m.on_query("eid-1", q)
        assert m._per_query_quantile["eid-1"] == 0.25
        assert m._per_query_extra_visits["eid-1"] == 800

    def test_partial_metadata_overrides_only_named_field(self) -> None:
        m = self._make_middleware()
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor",
                "komi": 7.5,
                "boardXSize": 19,
                "moves": [],
                "capabilities": {"adaptive_reevaluate": {"extra_visits": 2000}},
            },
        )
        m.on_query("eid-1", q)
        assert m._per_query_quantile["eid-1"] == 0.25  # default
        assert m._per_query_extra_visits["eid-1"] == 2000  # overridden

    def test_lru_eviction_pops_per_query_state(self) -> None:
        m = self._make_middleware()
        # max_inflight=10; submit 11 queries to trigger eviction of the
        # oldest.
        for i in range(11):
            q = KataGoQuery(
                action=KataGoAction.ANALYZE,
                analyze_turns=[i],
                opaque={
                    "rules": "tromp-taylor",
                    "komi": 7.5,
                    "boardXSize": 19,
                    "moves": [],
                    "capabilities": {
                        "adaptive_reevaluate": {"extra_visits": 100 + i}
                    },
                },
            )
            m.on_query(f"eid-{i}", q)
        # The oldest (eid-0) should have been evicted.
        assert "eid-0" not in m._per_query_quantile
        assert "eid-0" not in m._per_query_extra_visits
        # The newest should be present.
        assert m._per_query_extra_visits["eid-10"] == 110

    def test_build_deeper_query_uses_increment_semantic(self) -> None:
        m = self._make_middleware()
        orig = KataGoQuery(
            action=KataGoAction.ANALYZE,
            opaque={"maxVisits": 1000, "moves": []},
        )
        deeper = m._build_deeper_query(orig, [0, 1, 2], extra_visits=500)
        # extra_visits is an increment, not an absolute.
        assert deeper.opaque["maxVisits"] == 1500

    def test_build_deeper_query_strips_cache_flags(self) -> None:
        m = self._make_middleware()
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
        deeper = m._build_deeper_query(orig, [0], extra_visits=500)
        assert "cache" not in deeper.opaque
        assert "lookup_cache" not in deeper.opaque
        assert "replay_final_only" not in deeper.opaque


# ===========================================================================
# capabilities_advertiser
# ===========================================================================


class TestCapabilitiesAdvertiser:
    def test_query_version_response_gains_capabilities(self) -> None:
        link = _MockLink()
        advertised = {"delta_analysis": {}, "transposition": {}, "adaptive_reevaluate": {}}
        t = capabilities_advertiser(advertised)(link)

        r = MetadataResponse(opaque={"version": "1.16.0", "git_hash": "abcdef"})
        out = t.on_response("eid-1", r)
        assert isinstance(out, MetadataResponse)
        assert out.opaque["capabilities"] == advertised
        # Original fields preserved.
        assert out.opaque["version"] == "1.16.0"
        assert out.opaque["git_hash"] == "abcdef"

    def test_other_metadata_responses_unchanged(self) -> None:
        # E.g., a clear_cache ack or terminate ack — no "version" key.
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(link)

        r = MetadataResponse(opaque={"action": "clear_cache"})
        out = t.on_response("eid-1", r)
        assert out is r  # passthrough; same object
        assert "capabilities" not in out.opaque

    def test_analyze_responses_unchanged(self) -> None:
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(link)

        r = AnalyzeResponse(is_during_search=False, turn_number=1, opaque={"moveInfos": []})
        out = t.on_response("eid-1", r)
        assert out is r

    def test_advertisement_is_copy_not_reference(self) -> None:
        link = _MockLink()
        advertised = {"delta_analysis": {}}
        t = capabilities_advertiser(advertised)(link)

        # Mutate the source dict after factory construction.
        advertised["transposition"] = {}

        r = MetadataResponse(opaque={"version": "1.0"})
        out = t.on_response("eid-1", r)
        # The advertisement was deep-copied at factory call time;
        # post-construction mutation should not leak into emitted
        # responses.
        assert "transposition" not in out.opaque["capabilities"]
        assert out.opaque["capabilities"] == {"delta_analysis": {}}

    def test_query_side_is_identity(self) -> None:
        link = _MockLink()
        t = capabilities_advertiser({"delta_analysis": {}})(link)

        q = _make_analyze_query()
        out = t.on_query("eid-1", q)
        assert out is q
