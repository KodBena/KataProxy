"""
tests/test_adaptive_cache_matrix.py — Parametrized in-process integration
tests for the adaptive_reevaluate × replay-cache interaction.

Pins the user-visible contract: regardless of which combination of cache
flags (`cache`, `lookup_cache`, `replay_final_only`) a client uses,
`adaptive_reevaluate` must fire and deeper-analysis sub-query responses
must reach the wire with populated `extra.state` so the SPA's palette
state-fns reading deeper-visit values can update.

The matrix (after collapses):

  A. live, no store         (cache=F, lookup_cache=F, rfo=irrelevant)
  B. live, store            (cache=T, lookup_cache=F, rfo=irrelevant)
  C. cache hit, full replay (after B, cache=F, lookup_cache=T, rfo=F)
  D. cache hit, finals only (after B, cache=F, lookup_cache=T, rfo=T)

Rows where `replay_final_only=T` and there's no replay (cache=F/lookup=F)
collapse mathematically into the rows above because the flag only fires
on the replay path.

Harness: drives a full Layer 1+2+3 chain in-process using the existing
`SyntheticPonderingRouter` as the upstream. Captures wire output via a
`MockWebSocket.sent` list and parses it back into wire dicts. No live
KataGo subprocess, no separate proxy_server process, no WebSocket
listener — tests finish in well under a second each.

Synthetic backend reproduces the bug condition naturally: it emits
intermediate (`is_during_search=True`) packets at visits = emit_count,
then a final (`is_during_search=False`) packet at the same emit_count.
The last intermediate and the final have byte-identical content, which
triggers the `_are_equal` short-circuit in
`DeltaAnalysisState.push_packet` — the precise mechanism that starves
adaptive's `_find_worst_turns` on the live path.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from katago import KataGoAction, KataGoQuery  # noqa: E402
from middleware.adaptive_reevaluate import adaptive_reevaluate  # noqa: E402
from middleware.capability_gate import CapabilityGatedMiddleware  # noqa: E402
from middleware.session_middleware import SessionMiddleware  # noqa: E402
from proxy_server import ClientSession  # noqa: E402
from pubsub_hub import LRUCacheStore, PubSubHub  # noqa: E402
from transformers.analysis_enricher import analysis_enricher  # noqa: E402
from transformers.capability_gate import capability_gate  # noqa: E402

from AbstractProxy.proxy_core import CanonicalId, ClientId  # noqa: E402
from router import BackendRouter, OnComplete, OnResponse  # noqa: E402
from tests.synthetic_backend import SyntheticPonderingRouter  # noqa: E402


# ---------------------------------------------------------------------------
# MaxVisits-aware synthetic backend
# ---------------------------------------------------------------------------


class MaxVisitsSyntheticRouter(BackendRouter):
    """Synthetic upstream that derives intermediate / final visit counts
    from the query's `maxVisits`, so parent queries (maxVisits=100) and
    adaptive's deeper sub-queries (maxVisits=150) produce distinguishable
    wire output.

    Why a new class rather than extending `SyntheticPonderingRouter`:
    the existing router's emit_count semantics is "monotonic per
    canonical_id, increments on every tick across every analyzeTurns
    entry," which is fine for keep-alive watchdog tests but produces
    indistinguishable parent-vs-deeper visit counts. This variant
    threads `maxVisits` through into the emitted `rootInfo.visits`,
    which is the signal adaptive's `_find_worst_turns` and the test
    assertions rely on.

    Per query: emits `n_intermediates` intermediates at visits derived
    by linear interpolation up to `maxVisits`, then one final per turn
    at `maxVisits`. The last intermediate and the final at the same
    turn share the exact same visits value, reproducing the bug
    condition that defeats `DeltaAnalysisState`'s `_are_equal` check.
    """

    def __init__(
        self,
        *,
        n_intermediates: int = 3,
        emit_interval_s: float = 0.005,
    ) -> None:
        self._n_intermediates = n_intermediates
        self._emit_interval_s = emit_interval_s
        self._live: dict[CanonicalId, asyncio.Task[None]] = {}
        self.dispatched: list[CanonicalId] = []
        self.terminated: list[CanonicalId] = []

    async def start(self) -> None:
        pass

    async def dispatch(
        self, canonical_id: CanonicalId, wire_dict: Dict[str, Any], query: KataGoQuery,
        on_response: OnResponse, on_complete: OnComplete,
    ) -> None:
        self.dispatched.append(canonical_id)
        if query.action != KataGoAction.ANALYZE:
            ack = {"id": canonical_id}
            ack.update({k: v for k, v in wire_dict.items() if k != "id"})
            await on_response(canonical_id, ack)
            await on_complete(canonical_id)
            return
        max_visits = int(query.opaque.get("maxVisits", 100))
        turns = query.analyze_turns if query.analyze_turns else [0]
        task = asyncio.create_task(
            self._emit_for(canonical_id, max_visits, list(turns), on_response, on_complete),
            name=f"emit:{canonical_id}",
        )
        self._live[canonical_id] = task

    async def _emit_for(
        self, canonical_id: CanonicalId, max_visits: int, turns: list[int],
        on_response: OnResponse, on_complete: OnComplete,
    ) -> None:
        try:
            # `n_intermediates` ticks; each tick emits one packet per turn
            # at a visit count interpolating from 0 to max_visits.
            for i in range(1, self._n_intermediates + 1):
                await asyncio.sleep(self._emit_interval_s)
                visits = int((i / self._n_intermediates) * max_visits)
                for turn in turns:
                    await on_response(canonical_id, {
                        "id": canonical_id,
                        "isDuringSearch": True,
                        "turnNumber": turn,
                        "moveInfos": [],
                        "rootInfo": {"scoreLead": float(turn), "visits": visits},
                    })
            # Finals at max_visits — identical content to the last
            # intermediate at the same turn, which is the bug-reproducing
            # condition we want the tests to exercise.
            for turn in turns:
                await on_response(canonical_id, {
                    "id": canonical_id,
                    "isDuringSearch": False,
                    "turnNumber": turn,
                    "moveInfos": [],
                    "rootInfo": {"scoreLead": float(turn), "visits": max_visits},
                })
            await on_complete(canonical_id)
        except asyncio.CancelledError:
            raise
        finally:
            self._live.pop(canonical_id, None)

    async def terminate(
        self, canonical_id: CanonicalId,
        on_response: OnResponse, on_complete: OnComplete,
    ) -> None:
        self.terminated.append(canonical_id)
        task = self._live.pop(canonical_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        import secrets
        # Synthetic ack's wire id is brand-CanonicalId at routing-key
        # sites; see SyntheticPonderingRouter.terminate for the same
        # convention.
        wire_id = CanonicalId(f"kg_{secrets.token_hex(6)}")
        await on_response(wire_id, {
            "id": wire_id, "action": "terminate", "terminateId": canonical_id,
        })
        await on_complete(wire_id)

    async def stop(self) -> None:
        for cid, task in list(self._live.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._live.pop(cid, None)


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


class _MockWebSocket:
    """Captures send() calls into a list; satisfies the surface
    `ClientSession` consumes (remote_address, send, close)."""

    def __init__(self) -> None:
        self.remote_address = ("127.0.0.1", 0)
        self.sent: list[str] = []

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        pass


def _palette() -> Dict[str, Any]:
    """Minimal analysis_config compatible with the synthetic backend's
    emitted fields. State_fns read rootInfo.visits / rootInfo.scoreLead;
    delta_fn reads visits across the windowed pair. Sufficient to drive
    `DeltaAnalysisState`'s pipeline through enough updates that the
    `_find_worst_turns` selection has data to work on."""
    return {
        "bindings": {
            "delta_fn": "vd",
            "state_fns": {"V": "v", "L": "sl"},
            "summary_fn": "ms",
        },
        "parameters": {},
        "symbols": {
            "v": 'x["rootInfo"]["visits"]',
            "sl": 'x["rootInfo"]["scoreLead"]',
            "vd": 'x[1]["rootInfo"]["visits"] - x[0]["rootInfo"]["visits"]',
            "ms": "float(min(x))",
        },
    }


def _build_query(
    orig_id_suffix: str,
    *,
    n_turns: int = 4,
    n_moves: int = 4,
    cache: bool = False,
    lookup_cache: bool = False,
    replay_final_only: bool = False,
) -> KataGoQuery:
    """Construct a multi-turn ANALYZE query with palette + adaptive opt-in.

    n_moves >= 2 is required for `analysis_enricher`'s gate (it builds
    `DeltaAnalysisState`, which needs ≥ 2 moves to form at least one
    delta). n_turns drives how many turns adaptive's worst-quantile
    selection has to choose from; 4 is enough to exercise the deepen
    branch without making logs huge.
    """
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=list(range(n_turns)),
        opaque={
            "moves": [["B", f"A{i + 1}"] for i in range(n_moves)],
            "rules": "tromp-taylor",
            "komi": 7.5,
            "boardXSize": 19,
            "boardYSize": 19,
            "maxVisits": 100,
            "cache": cache,
            "lookup_cache": lookup_cache,
            "replay_final_only": replay_final_only,
            "analysis_config": _palette(),
            "capabilities": {
                "delta_analysis": {},
                "adaptive_reevaluate": {"worst_quantile": 0.5, "extra_visits": 50},
            },
        },
    )


def _build_chain() -> SessionMiddleware:
    """Production-shaped middleware: capability-gated adaptive_reevaluate.

    The full proxy_server._make_middleware wraps this with
    KeepAliveMiddleware, but the adaptive contract is independent of
    keep-alive — omitting the outer wrapper keeps these tests focused
    on adaptive's behaviour without spinning watchdog timers.
    """
    return CapabilityGatedMiddleware(
        "adaptive_reevaluate",
        adaptive_reevaluate(
            worst_quantile=0.5, extra_visits=50, window_size=3,
        )(),
    )


def _transformer_factory(
    link: Any,
) -> Any:
    """Capability-gated analysis_enricher — mirrors proxy_server's main()
    composition for the delta_analysis capability."""
    return capability_gate("delta_analysis", analysis_enricher)(link)


async def _run_query(
    *,
    session: ClientSession,
    query: KataGoQuery,
    orig_id: ClientId,
    settle_s: float,
) -> List[Dict[str, Any]]:
    """Drive a single query through the session and capture wire output.

    Resets the MockWebSocket's `sent` buffer before issuing so the
    returned list is the wires produced by THIS query specifically
    (callers running multi-query scenarios get per-query isolation).

    `settle_s` is the post-issue wait. The synthetic backend emits
    deterministically at `emit_interval_s`, so a fixed sleep is the
    pragmatic choice over a more complex completion-signal mechanism;
    pick generously enough to cover N intermediates + finals + any
    adaptive-spawned sub-query.
    """
    ws: _MockWebSocket = session._ws
    ws.sent.clear()
    # The production receive loop calls middleware.on_query before
    # dispatching; for direct _handle_query callers (this test, the
    # diagnose_* scripts) we mirror that explicitly so the orchestration
    # bookkeeping fires.
    session._middleware.on_query(orig_id, query)
    await session._handle_query(orig_id, query)
    await asyncio.sleep(settle_s)
    out: List[Dict[str, Any]] = []
    for raw in ws.sent:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out


async def _setup_session(
    *,
    cache_store: Optional[LRUCacheStore] = None,
    max_intermediates: int = 3,
    emit_interval_s: float = 0.01,
) -> tuple[ClientSession, "MaxVisitsSyntheticRouter", _MockWebSocket, asyncio.Task[None]]:
    """Build a fully-wired ClientSession with a running send_loop.

    Returns (session, router, ws, send_task). Caller is responsible for
    awaiting the send_task to cancel/finalise and for stopping the
    router. The standard cleanup is the `_teardown_session` helper below.

    `max_intermediates=3` keeps tests fast; the synthetic backend will
    emit 3 D packets then 1 F per turn, totalling 4 wires per turn from
    upstream. With 4 analyze_turns that's 16 raw wires going into the
    Hub; adaptive's coroutine and the transformer chain produce the
    actual user-facing wire count, which the test asserts on directly.
    """
    hub = PubSubHub(cache_store=cache_store) if cache_store is not None else PubSubHub()
    router = MaxVisitsSyntheticRouter(
        n_intermediates=max_intermediates,
        emit_interval_s=emit_interval_s,
    )
    await router.start()

    middleware = _build_chain()
    ws = _MockWebSocket()
    session = ClientSession(
        ws, "test-peer", hub, router,
        transformer_factory=_transformer_factory,
        middleware=middleware,
        rate_limit=None,
    )
    send_task = asyncio.create_task(
        session._send_loop(),
        name="test-send-loop",
    )
    return session, router, ws, send_task


async def _teardown_session(
    session: ClientSession,
    router: "MaxVisitsSyntheticRouter",
    send_task: asyncio.Task[None],
) -> None:
    """Standard cleanup: cancel send_loop, stop router, await middleware end."""
    send_task.cancel()
    try:
        await send_task
    except asyncio.CancelledError:
        pass
    await router.stop()
    # on_session_end is sync (the SessionMiddleware ABC declares it as
    # such); call it bare to release the orchestration framework's
    # bookkeeping. Tasks spawned by on_session_start should already be
    # cancelled by the send_loop teardown above.
    session._middleware.on_session_end()


# ---------------------------------------------------------------------------
# Wire-output assertions
# ---------------------------------------------------------------------------


def _summarise(wires: list[dict[str, Any]]) -> dict[str, Any]:
    """Compact summary of the wire output for assertion shaping and
    debugging surface when a test fails."""
    by_turn: dict[int, list[tuple[int, bool, bool]]] = {}
    deeper_present = False
    deeper_state_populated = False
    for w in wires:
        tn = w.get("turnNumber")
        if tn is None:
            continue
        v = w.get("rootInfo", {}).get("visits", 0)
        ids = bool(w.get("isDuringSearch", False))
        state = w.get("extra", {}).get("state") or {}
        has_state = bool(state)
        by_turn.setdefault(tn, []).append((v, ids, has_state))
        # "Deeper" here means visits above the original maxVisits=100;
        # +extra_visits=50 → deeper queries land at 150ish in the
        # synthetic backend's monotonic-visits scheme.
        if v > 100:
            deeper_present = True
            if has_state:
                deeper_state_populated = True
    return {
        "by_turn": by_turn,
        "deeper_present": deeper_present,
        "deeper_state_populated": deeper_state_populated,
        "total_wires": len(wires),
    }


# ---------------------------------------------------------------------------
# Parametrized tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAdaptiveCacheMatrix:
    """The four distinct cache flows. Each test runs in-process from
    query to wire-capture; the synthetic backend reproduces the bug
    condition without needing a live KataGo."""

    async def test_A_live_no_store(self) -> None:
        """Scenario A: cache=F, lookup_cache=F.

        First-time live query, no cache touch. This is the most common
        path and the one where the live-path adaptive bug surfaces:
        `_find_worst_turns` reads F packets whose `extra.<color>.deltas`
        have been emptied by the analysis_enricher pipeline's
        `_are_equal` short-circuit against the prior identical D packet.
        Adaptive fails to deepen; no deeper-visit response ever lands
        on the wire."""
        session, router, ws, send_task = await _setup_session()
        try:
            wires = await _run_query(
                session=session,
                query=_build_query("A", cache=False, lookup_cache=False),
                orig_id=ClientId("orig-A"),
                settle_s=0.5,
            )
        finally:
            await _teardown_session(session, router, send_task)

        s = _summarise(wires)
        # Contract: adaptive must fire and the deeper response's
        # `extra.state[turn]` must reach the wire populated. If either
        # half fails, the SPA's palette state-fn reading
        # rootInfo.visits via extra.state[turn] cannot reflect the
        # deeper analysis.
        assert s["deeper_present"], (
            f"adaptive did not deepen any turn on the live path "
            f"(no wire response with visits > maxVisits). Summary: {s}"
        )
        assert s["deeper_state_populated"], (
            f"deeper-visit responses reached the wire but with empty "
            f"extra.state — the SPA's palette state-fn will keep the "
            f"original (pre-deepening) value. Summary: {s}"
        )

    async def test_B_live_store(self) -> None:
        """Scenario B: cache=T, lookup_cache=F.

        Live path that ALSO stores the response stream into the cache.
        Behaviourally identical to A on the wire side — the store is
        a side-effect that doesn't change what the client sees. Pinned
        separately so a future change that conflates store-side
        bookkeeping with response-emission would be caught."""
        cache_store = LRUCacheStore(maxsize=10)
        session, router, ws, send_task = await _setup_session(cache_store=cache_store)
        try:
            wires = await _run_query(
                session=session,
                query=_build_query("B", cache=True, lookup_cache=False),
                orig_id=ClientId("orig-B"),
                settle_s=0.5,
            )
        finally:
            await _teardown_session(session, router, send_task)

        s = _summarise(wires)
        assert s["deeper_present"], (
            f"adaptive did not deepen on the live-store path. Summary: {s}"
        )
        assert s["deeper_state_populated"], (
            f"deeper response reached wire but extra.state empty. Summary: {s}"
        )
        # The cache should now have an entry — this is the store-side
        # bookkeeping the test pins separately.
        assert len(cache_store) >= 1, (
            "cache=True should have stored at least one response stream"
        )

    async def test_C_hit_full_replay(self) -> None:
        """Scenario C: prime cache with B, then second query with
        cache=F, lookup_cache=T, replay_final_only=F.

        The second query short-circuits to a cache replay; analysis_enricher
        runs a FRESH analyzer (different orig_id, fresh request_cache
        entry) over the replayed raw stream. Tests the path where the
        user's setup naturally fell into when they reported "adaptive
        fires on the second query but not the first" — that observation
        was exactly this scenario: query #1 was the live path (broken),
        query #2 was a cache hit (working)."""
        cache_store = LRUCacheStore(maxsize=10)
        session, router, ws, send_task = await _setup_session(cache_store=cache_store)
        try:
            # Prime: live store.
            await _run_query(
                session=session,
                query=_build_query("prime", cache=True, lookup_cache=False),
                orig_id=ClientId("orig-prime"),
                settle_s=0.5,
            )
            assert len(cache_store) >= 1, "prime should populate cache"

            # The hit query. Same content modulo `cache`/`lookup_cache` flags
            # (which are stripped before the hub computes the cache key),
            # so the hub should hit the entry seeded by the prime.
            wires = await _run_query(
                session=session,
                query=_build_query(
                    "C", cache=False, lookup_cache=True, replay_final_only=False,
                ),
                orig_id=ClientId("orig-C"),
                settle_s=0.5,
            )
        finally:
            await _teardown_session(session, router, send_task)

        s = _summarise(wires)
        assert s["deeper_present"], (
            f"adaptive did not deepen on the cache-hit-full-replay path. "
            f"Summary: {s}"
        )
        assert s["deeper_state_populated"], (
            f"deeper response on full-replay had empty extra.state. "
            f"Summary: {s}"
        )

    async def test_D_hit_finals_only_replay(self) -> None:
        """Scenario D: prime cache with B, then second query with
        cache=F, lookup_cache=T, replay_final_only=T.

        The replay path drops every `isDuringSearch=True` cached wire,
        feeding only the F packets through the transformer chain. The
        analysis_enricher's pipeline at slot N sees only the F (no
        preceding D), so the `_are_equal` short-circuit can't fire on
        F (the slot's in_mem is empty when F arrives). Adaptive should
        see populated deltas and deepen; deeper responses should carry
        populated extra.state.

        Distinct from scenario C because the user-controllable
        `replay_final_only` flag genuinely changes the bytes the
        transformer chain sees on the replay path."""
        cache_store = LRUCacheStore(maxsize=10)
        session, router, ws, send_task = await _setup_session(cache_store=cache_store)
        try:
            await _run_query(
                session=session,
                query=_build_query("prime", cache=True, lookup_cache=False),
                orig_id=ClientId("orig-prime"),
                settle_s=0.5,
            )
            wires = await _run_query(
                session=session,
                query=_build_query(
                    "D", cache=False, lookup_cache=True, replay_final_only=True,
                ),
                orig_id=ClientId("orig-D"),
                settle_s=0.5,
            )
        finally:
            await _teardown_session(session, router, send_task)

        s = _summarise(wires)
        assert s["deeper_present"], (
            f"adaptive did not deepen on the finals-only-replay path. "
            f"Summary: {s}"
        )
        assert s["deeper_state_populated"], (
            f"deeper response on finals-only-replay had empty extra.state. "
            f"Summary: {s}"
        )
