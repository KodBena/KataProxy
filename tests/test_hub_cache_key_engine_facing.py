"""
tests/test_hub_cache_key_engine_facing.py — the replay cache key covers
exactly the ENGINE-FACING query, not the full opaque payload.

DEFECT this pins the fix for: ``PubSubHub._compute_cache_key`` used to
hash the full opaque minus only "id" (the three control flags were
already popped pre-hash). That made two proxy-evaluated fields —
``analysis_config`` (the user's enrichment palette, consumed entirely by
``transformers/analysis_enricher.py`` and never seen by the engine) and
``capabilities`` (gates which per-session transformers/middleware
engage) — discriminate the cache key, even though the cached record is
the RAW backend stream captured in ``on_response`` *before* any
transformer runs. Result: changing the palette forced a full engine
re-run, defeating FRAMEWORK.md §3's documented purpose ("replay through
transformers with the new parameters ... as if from a live GPU").

The fix: ``katago/katago_proxy.py:CACHE_KEY_EXCLUDED_FIELDS`` (equal to
``_PROXY_ONLY_FIELDS`` since the v1.0.30 reclassification of ``model``
as engine-facing) is the single source of
truth for which proxy-only fields don't affect engine output;
``pubsub_hub.py:_compute_cache_key`` imports and applies it instead of
hashing the full opaque.

Structure (ADR-0021 — observe the property, not a symptom):

  1. ``TestCacheHitAcrossPaletteAndCapabilityChange`` — record under
     palette P1/capabilities C1, then look up with the SAME engine
     query but P2/C2 → HIT, and the replayed stream is byte-identical
     (modulo id-relabelling) to the recorded raw stream. This is the
     property directly: a proxy-only-field change must not force a
     fresh engine run.
  2. ``TestEngineFacingFieldsStillDiscriminate`` — negative controls:
     komi, maxVisits, and model (separately) still MISS. Without this,
     a broken fix that excludes *everything* (or excludes engine
     fields by accident) would pass test 1 vacuously.
  3. ``TestEnrichmentOverReplayMatchesFreshEvaluation`` — the
     transformer-level property FRAMEWORK.md §3 promises: running
     ``analysis_enricher`` configured with the new palette P2 over a
     replayed (deep-copied) raw stream produces the same enrichment as
     running it fresh over the same raw content. Drives the
     Transformer directly, following ``test_analysis_enricher_extra_
     status.py``'s pattern.
  4. ``TestRegressionIdenticalQueryStillHits`` — the existing behaviour
     (same analysis_config on both sides) is unchanged.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import ClientId, InternalId, ProxyLink  # noqa: E402
from katago import (  # noqa: E402
    KataGoAction,
    KataGoQuery,
    parse_response_from_wire,
)
from pubsub_hub import LRUCacheStore, PubSubHub  # noqa: E402
from transformers.analysis_enricher import analysis_enricher  # noqa: E402

_LinkT = ProxyLink[ClientId, InternalId]


# ---------------------------------------------------------------------------
# Shared query / palette builders
# ---------------------------------------------------------------------------

_PALETTE_P1: Dict[str, Any] = {
    "bindings": {"delta_fn": "vd", "summary_fn": "ms"},
    "parameters": {},
    "symbols": {
        "vd": 'x[1]["rootInfo"]["visits"] - x[0]["rootInfo"]["visits"]',
        "ms": "float(min(x))",
    },
}

_PALETTE_P2: Dict[str, Any] = {
    "bindings": {"delta_fn": "vd2", "summary_fn": "ms2"},
    "parameters": {},
    "symbols": {
        # A palette that reads differently from P1 — the property under
        # test only holds if the two palettes are actually distinguishable.
        "vd2": 'x[1]["rootInfo"]["visits"] * 2 - x[0]["rootInfo"]["visits"]',
        "ms2": "float(max(x))",
    },
}

_MOVES = [["B", "A1"], ["W", "A2"], ["B", "A3"]]


def _engine_query(
    *,
    cache: bool = False,
    lookup_cache: bool = False,
    replay_final_only: bool = False,
    analysis_config: Optional[Dict[str, Any]] = _PALETTE_P1,
    capabilities: Optional[List[str]] = None,
    komi: float = 7.5,
    max_visits: int = 100,
    model: Optional[str] = None,
) -> KataGoQuery:
    """A well-formed ANALYZE query whose engine-facing fields (rules,
    komi, board size, moves, maxVisits, model) are independently
    parametrisable, alongside the three proxy-control flags and the two
    proxy-only fields (analysis_config, capabilities) under test."""
    opaque: Dict[str, Any] = {
        "rules": "tromp-taylor",
        "komi": komi,
        "boardXSize": 19,
        "boardYSize": 19,
        "moves": deepcopy(_MOVES),
        "maxVisits": max_visits,
        "cache": cache,
        "lookup_cache": lookup_cache,
        "replay_final_only": replay_final_only,
    }
    if analysis_config is not None:
        opaque["analysis_config"] = analysis_config
    if capabilities is not None:
        opaque["capabilities"] = capabilities
    if model is not None:
        opaque["model"] = model
    return KataGoQuery(action=KataGoAction.ANALYZE, analyze_turns=[0, 1], opaque=opaque)


def _raw_stream(cid: str) -> List[Dict[str, Any]]:
    """A synthetic raw backend stream: exactly what the router would
    hand `hub.on_response` — pre-transformer, un-enriched, un-relabelled."""
    return [
        {
            "id": cid,
            "isDuringSearch": False,
            "turnNumber": 0,
            "moveInfos": [{"move": "A1", "visits": 10}],
            "rootInfo": {"visits": 40, "scoreLead": 0.5},
        },
        {
            "id": cid,
            "isDuringSearch": False,
            "turnNumber": 1,
            "moveInfos": [{"move": "A2", "visits": 12}],
            "rootInfo": {"visits": 55, "scoreLead": 0.7},
        },
    ]


async def _record(
    hub: PubSubHub, query: KataGoQuery, sub_id: InternalId,
) -> str:
    """Drive subscribe → on_response(×N) → on_complete for a `cache: true`
    query, returning the canonical_id used."""
    q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
    is_new, cid = hub.subscribe(query, sub_id, q)
    assert is_new, "recording subscribe must create a new in-flight slot"
    for wire in _raw_stream(str(cid)):
        await hub.on_response(cid, wire)
    await hub.on_complete(cid)
    return str(cid)


async def _drain(q: "asyncio.Queue[Dict[str, Any]]", expected: int) -> List[Dict[str, Any]]:
    """Wait for the replay task (a separately-scheduled asyncio.Task) to
    deliver `expected` messages, then return them in order."""
    for _ in range(expected + 10):
        if q.qsize() >= expected:
            break
        await asyncio.sleep(0)
    assert q.qsize() == expected, (
        f"expected {expected} replayed message(s), got {q.qsize()}"
    )
    return [q.get_nowait() for _ in range(expected)]


# ---------------------------------------------------------------------------
# 1. Palette / capability change still HITs and replays the raw stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCacheHitAcrossPaletteAndCapabilityChange:
    async def test_hit_and_replay_equals_raw_stream(self) -> None:
        hub = PubSubHub(cache_store=LRUCacheStore(maxsize=100))

        # Record under P1 / capabilities C1.
        q1 = _engine_query(
            cache=True, analysis_config=_PALETTE_P1,
            capabilities=["delta_analysis"],
        )
        cid1 = await _record(hub, q1, InternalId("sub-1"))
        raw = _raw_stream(cid1)

        # Look up the SAME engine query but a different palette AND a
        # different capability set.
        q2 = _engine_query(
            lookup_cache=True, analysis_config=_PALETTE_P2,
            capabilities=["transposition"],
        )
        sub2_q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        is_new2, _cid2 = hub.subscribe(q2, InternalId("sub-2"), sub2_q)

        assert is_new2 is False, (
            "a palette/capability-only change must still HIT the "
            "engine-facing cache; got a MISS (is_new_query=True)"
        )

        replayed = await _drain(sub2_q, expected=len(raw))
        for orig, got in zip(raw, replayed):
            assert got["id"] == "sub-2", "replay must relabel id onto the subscriber"
            orig_no_id = {k: v for k, v in orig.items() if k != "id"}
            got_no_id = {k: v for k, v in got.items() if k != "id"}
            assert got_no_id == orig_no_id, (
                "replayed wire content must exactly equal the recorded "
                f"raw stream; orig={orig_no_id!r} got={got_no_id!r}"
            )


# ---------------------------------------------------------------------------
# 2. Negative controls: engine-facing fields still discriminate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestEngineFacingFieldsStillDiscriminate:
    async def _assert_miss(self, baseline: KataGoQuery, varied: KataGoQuery) -> None:
        hub = PubSubHub(cache_store=LRUCacheStore(maxsize=100))
        await _record(hub, baseline, InternalId("sub-1"))

        varied_q = deepcopy(varied)
        varied_q.opaque["lookup_cache"] = True
        sub2_q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        is_new2, _cid2 = hub.subscribe(varied_q, InternalId("sub-2"), sub2_q)
        assert is_new2 is True, (
            "an engine-facing field change must MISS the replay cache "
            f"(query.opaque={varied.opaque!r})"
        )

    async def test_komi_difference_misses(self) -> None:
        baseline = _engine_query(cache=True, komi=7.5)
        varied = _engine_query(komi=6.5)
        await self._assert_miss(baseline, varied)

    async def test_max_visits_difference_misses(self) -> None:
        baseline = _engine_query(cache=True, max_visits=100)
        varied = _engine_query(max_visits=200)
        await self._assert_miss(baseline, varied)

    async def test_model_difference_misses(self) -> None:
        baseline = _engine_query(cache=True, model="b18-fast")
        varied = _engine_query(model="b18-strong")
        await self._assert_miss(baseline, varied)


# ---------------------------------------------------------------------------
# 2b. Deployment salt (v1.0.30): a changed SELECTOR label→engine
# mapping must MISS rather than replay the old mapping's streams;
# an empty salt leaves keys exactly as before.
# ---------------------------------------------------------------------------


class TestCacheKeySalt:
    def test_empty_salt_leaves_key_unchanged(self) -> None:
        q = _engine_query(model="main")
        unsalted = PubSubHub()._compute_cache_key(q)
        empty = PubSubHub(cache_key_salt="")._compute_cache_key(q)
        assert unsalted == empty

    def test_different_salts_produce_different_keys(self) -> None:
        q = _engine_query(model="main")
        k1 = PubSubHub(
            cache_key_salt='[["main","ws://h:1","b6c96-s1-d1"]]'
        )._compute_cache_key(q)
        k2 = PubSubHub(
            cache_key_salt='[["main","ws://h:1","b6c96-s2-d2"]]'
        )._compute_cache_key(q)
        assert k1 != k2

    def test_same_salt_is_stable(self) -> None:
        q = _engine_query(model="main")
        salt = '[["main","ws://h:1","b6c96-s1-d1"]]'
        assert (
            PubSubHub(cache_key_salt=salt)._compute_cache_key(q)
            == PubSubHub(cache_key_salt=salt)._compute_cache_key(q)
        )


# ---------------------------------------------------------------------------
# 3. Regression: identical query (same analysis_config) still hits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRegressionIdenticalQueryStillHits:
    async def test_identical_analysis_config_still_hits(self) -> None:
        hub = PubSubHub(cache_store=LRUCacheStore(maxsize=100))
        q1 = _engine_query(cache=True, analysis_config=_PALETTE_P1)
        await _record(hub, q1, InternalId("sub-1"))

        q2 = _engine_query(lookup_cache=True, analysis_config=_PALETTE_P1)
        sub2_q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        is_new2, _cid2 = hub.subscribe(q2, InternalId("sub-2"), sub2_q)
        assert is_new2 is False, "identical query must still hit the cache"


# ---------------------------------------------------------------------------
# 4. Transformer-level: palette-over-replay equals fresh evaluation
# ---------------------------------------------------------------------------


class _MockMapping:
    """Same shape as test_analysis_enricher_extra_status.py's mock: the
    only surface analysis_enricher consumes is `forward(eid)`."""

    def __init__(self) -> None:
        self._active: set[ClientId] = set()

    def mark_active(self, eid: ClientId) -> None:
        self._active.add(eid)

    def forward(self, eid: ClientId) -> Optional[InternalId]:
        return InternalId("internal") if eid in self._active else None


class _MockLink:
    def __init__(self) -> None:
        self.mapping = _MockMapping()


def _run_enricher_over_wire_stream(
    config: Dict[str, Any], wire_stream: List[Dict[str, Any]],
) -> List[Any]:
    """Drive a FRESH `analysis_enricher` Transformer instance over a
    wire-format stream (parsed into KataGoResponse the way the receive
    loop would), returning the `extra` payload for every response."""
    link = _MockLink()
    xform = analysis_enricher(cast(_LinkT, link))
    eid = ClientId("eid")
    link.mapping.mark_active(eid)

    q = KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0, 1],
        opaque={
            "rules": "tromp-taylor", "komi": 7.5,
            "boardXSize": 19, "boardYSize": 19,
            "moves": deepcopy(_MOVES), "analysis_config": config,
        },
    )
    xform.on_query(eid, q)

    results: List[Any] = []
    for wire in wire_stream:
        _envelope_id, resp = parse_response_from_wire(wire)
        out = xform.on_response(eid, resp)
        assert out is not None
        results.append(deepcopy(out.opaque.get("extra")))
    return results


@pytest.mark.asyncio
class TestEnrichmentOverReplayMatchesFreshEvaluation:
    async def test_palette_change_over_replayed_record_equals_fresh_evaluation(
        self,
    ) -> None:
        # "Fresh": the raw stream as it would arrive live from the engine.
        fresh_wire = _raw_stream("live-cid")

        # "Replay": the same content, round-tripped through the hub's
        # actual record/replay path (record under P1, replay under P2's
        # request — the replay task deep-copies and relabels "id", which
        # is exactly the transformation the real replay path performs).
        hub = PubSubHub(cache_store=LRUCacheStore(maxsize=100))
        q1 = _engine_query(cache=True, analysis_config=_PALETTE_P1)
        await _record(hub, q1, InternalId("sub-1"))

        q2 = _engine_query(lookup_cache=True, analysis_config=_PALETTE_P2)
        sub2_q: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        is_new2, _cid2 = hub.subscribe(q2, InternalId("sub-2"), sub2_q)
        assert is_new2 is False
        replayed_wire = await _drain(sub2_q, expected=len(fresh_wire))

        fresh_results = _run_enricher_over_wire_stream(_PALETTE_P2, fresh_wire)
        replayed_results = _run_enricher_over_wire_stream(_PALETTE_P2, replayed_wire)

        assert fresh_results == replayed_results, (
            "P2 evaluated fresh over the raw stream must equal P2 "
            "evaluated over the same content replayed through the hub's "
            f"cache path; fresh={fresh_results!r} replayed={replayed_results!r}"
        )
        # And sanity: P2 actually differs from what P1 would have
        # produced, so this isn't vacuously true because both palettes
        # give the same answer.
        p1_results = _run_enricher_over_wire_stream(_PALETTE_P1, fresh_wire)
        assert p1_results != fresh_results, (
            "P1 and P2 must be distinguishable palettes for this test to "
            "be meaningful"
        )
