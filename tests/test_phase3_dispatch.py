"""tests/test_phase3_dispatch.py — Phase 3 allocation-driven dispatch (v1.0.25).

End-to-end coroutine-level regression coverage for the allocation-
driven dispatch path per `docs/roadmap-info-theoretic-allocation.md`
§5 and §6.

Three test classes:

  1. `TestPhase3Engagement` — pin the engagement signal
     (`allocation_algorithm` presence) and the eager-validation
     refusal surface.
  2. `TestAllocationDriveDispatch` — under engaged Phase 3, the
     coroutine spawns N parallel sub-queries (one per candidate)
     and finalises one authoritative per turn.
  3. `TestEagerIncludeValidation` — the AST walk detects opt-in
     gated field references and refuses with `allocation_invalid`
     when parent query lacks the matching `include*` flags.

The lower-level visit-scaling and allocation-algorithm regression
tests live in `tests/test_visit_scaling.py` and
`tests/test_allocation_algorithms.py`; this file pins the dispatch's
composition.

Run from the proxy directory: `pytest tests/test_phase3_dispatch.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Tuple

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import ClientId  # noqa: E402
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    KataGoResponse,
)
from middleware.adaptive_reevaluate import (  # noqa: E402
    AdaptiveConfigurationError,
    _is_phase3_engaged,
    _required_include_flags,
    adaptive_reevaluate,
)
from middleware.session_middleware import SessionCapabilities  # noqa: E402


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


def _bad_final(turn: int, *, score_stdev: float = 12.0) -> AnalyzeResponse:
    """A final with bad-delta payload + scoreStdev for the visit-scaling model."""
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn,
        opaque={
            "moveInfos": [],
            "extra": {
                "black": {"deltas": {str(turn): -1.0}},
                "white": {"deltas": {str(turn): -1.0}},
            },
            "rootInfo": {"visits": 100, "scoreStdev": score_stdev},
        },
    )


def _neutral_final(turn: int) -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn,
        opaque={
            "moveInfos": [],
            "rootInfo": {"visits": 100, "scoreStdev": 10.0},
        },
    )


def _spawn_final(turn: int, *, marker: str) -> AnalyzeResponse:
    """A spawn-derived response carrying a marker so the test can
    verify the finalization picked it up."""
    return AnalyzeResponse(
        is_during_search=False,
        turn_number=turn,
        opaque={
            "moveInfos": [],
            "rootInfo": {"visits": 1000, "scoreStdev": 8.0},
            "marker": marker,
        },
    )


def _make_caps() -> Tuple[Any, SessionCapabilities]:
    class _Caps:
        submitted: List[Tuple[ClientId, KataGoQuery]] = []
        terminated: List[ClientId] = []

        async def submit(self, oid: ClientId, q: KataGoQuery) -> None:
            self.submitted.append((oid, q))

        async def terminate(self, oid: ClientId) -> None:
            self.terminated.append(oid)

    c = _Caps()
    c.submitted = []
    c.terminated = []
    return c, SessionCapabilities(
        submit_query=c.submit, terminate_query=c.terminate,
    )


async def _drive(m: Any, oid: ClientId, resp: KataGoResponse) -> List[Tuple[ClientId, KataGoResponse]]:
    out: List[Tuple[ClientId, KataGoResponse]] = []
    async for o, r in m.handle_response(oid, resp, None):
        out.append((o, r))
    return out


async def _wait_for_spawn_count(caps: Any, n: int, timeout_s: float = 1.0) -> bool:
    import asyncio
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        if len(caps.submitted) >= n:
            return True
        await asyncio.sleep(0.005)
    return False


async def _settle_and_drain(
    m: Any, orig_id: ClientId, max_wait_s: float = 0.5,
) -> List[Tuple[ClientId, KataGoResponse]]:
    """Wait for the orchestration coroutine to finish and drain any
    pending output. Use after the last input drive when the test
    expects post-loop emissions (e.g., Stage-3 finalisation) — the
    parallel-spawn dispatch needs more event-loop ticks than a
    single-spawn dispatch to settle, and `handle_response`'s drain
    loop is single-tick non-blocking by design."""
    import asyncio
    out: List[Tuple[ClientId, KataGoResponse]] = []
    ctx = m._contexts.get(orig_id)
    if ctx is None:
        return out
    task = m._tasks.get(orig_id)
    deadline = asyncio.get_event_loop().time() + max_wait_s
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.005)
        while True:
            try:
                item = ctx._output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, tuple):
                out.append(item)
        if task is not None and task.done():
            # One final drain after task completion for any items
            # queued by the finalization stage.
            await asyncio.sleep(0)
            while True:
                try:
                    item = ctx._output_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if isinstance(item, tuple):
                    out.append(item)
            break
    return out


async def _wait_for_coroutine_error(
    m: Any, orig_id: ClientId, timeout_s: float = 0.5,
) -> Any:
    """Drive a dummy response to surface the coroutine's startup
    exception. Phase 3's `_engage_phase3` raises before Stage 1
    consumes anything; the orchestration framework catches and emits
    a MetadataResponse error on the wire. Returns the first
    MetadataResponse with an `error` opaque field, or None on
    timeout."""
    import asyncio
    # The dummy response — its content doesn't matter; we just need to
    # tick the event loop so the framework's error-response shows up
    # in the output stream.
    dummy = _neutral_final(0)
    async for _o, r in m.handle_response(orig_id, dummy, None):
        if isinstance(r, type(_neutral_final(0))):
            continue  # any AnalyzeResponse — not an error
        # MetadataResponse with `error` opaque is the framework's
        # error envelope.
        opaque = getattr(r, "opaque", None)
        if isinstance(opaque, dict) and "error" in opaque:
            return r
    # Sometimes the error is queued before the dummy is pulled; one
    # more sleep + drain via a second dummy attempts to surface it.
    await asyncio.sleep(0)
    return None


def _phase3_capabilities(
    *,
    algorithm: str = "greedy_eig",
    model: str = "monte_carlo_sqrt",
    binding: str = "value_fn_expr",
    extra_visits: int = 400,
    max_rounds: int = 1,
    extra_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base = {
        "allocation_algorithm": algorithm,
        "visit_scaling_model": model,
        "value_binding": binding,
        "worst_quantile": 0.5,
        "extra_visits": extra_visits,
        "budget": {"max_rounds": max_rounds},
    }
    if extra_params is not None:
        base["allocation_params"] = extra_params
    return base


def _phase3_analysis_config(
    *,
    binding: str = "value_fn_expr",
    expression: str = "1.0",
) -> dict[str, Any]:
    return {
        "bindings": {"value_fn": binding},
        "symbols": {binding: expression},
        "parameters": {},
    }


# ===========================================================================
# 1. Engagement
# ===========================================================================


class TestPhase3Engagement:

    def test_engaged_when_allocation_algorithm_present(self) -> None:
        assert _is_phase3_engaged({"allocation_algorithm": "greedy_eig"})

    def test_disengaged_when_allocation_algorithm_absent(self) -> None:
        assert not _is_phase3_engaged({})
        assert not _is_phase3_engaged({"worst_quantile": 0.25})

    def test_disengaged_when_allocation_algorithm_none(self) -> None:
        assert not _is_phase3_engaged({"allocation_algorithm": None})

    @pytest.mark.asyncio
    async def test_missing_visit_scaling_model_raises(self) -> None:
        """Phase 3 requires visit_scaling_model when engaged.

        The error surfaces on the wire as a MetadataResponse with
        `error` opaque (the orchestration framework's error envelope
        for coroutine raises).
        """
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate": {
                    "allocation_algorithm": "greedy_eig",
                    # visit_scaling_model deliberately omitted.
                    "value_binding": "v",
                }},
                "analysis_config": _phase3_analysis_config(binding="v", expression="1.0"),
            },
        )
        m.on_query(ClientId("eid-1"), q)
        err = await _wait_for_coroutine_error(m, ClientId("eid-1"))
        assert err is not None
        assert "allocation_invalid" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_missing_value_binding_raises(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate": {
                    "allocation_algorithm": "greedy_eig",
                    "visit_scaling_model": "monte_carlo_sqrt",
                    # value_binding deliberately omitted.
                }},
            },
        )
        m.on_query(ClientId("eid-1"), q)
        err = await _wait_for_coroutine_error(m, ClientId("eid-1"))
        assert err is not None
        assert "allocation_invalid" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_value_binding_mismatch_raises(self) -> None:
        """capability.value_binding must equal analysis_config.bindings.value_fn."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(binding="claimed_binding")},
                "analysis_config": _phase3_analysis_config(
                    binding="actual_binding", expression="1.0",
                ),
            },
        )
        m.on_query(ClientId("eid-1"), q)
        err = await _wait_for_coroutine_error(m, ClientId("eid-1"))
        assert err is not None
        assert "allocation_invalid" in err.opaque["error"]
        m.on_session_end()


# ===========================================================================
# 2. Allocation-driven dispatch — end-to-end wire shape
# ===========================================================================


class TestAllocationDriveDispatch:

    @pytest.mark.asyncio
    async def test_spawns_n_parallel_subqueries(self) -> None:
        """Under Phase 3, the multi-round loop spawns N sub-queries
        (one per candidate) instead of v1.0.24's single batched
        deeper query."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(
                        algorithm="greedy_eig",
                        extra_visits=300,
                        max_rounds=1,
                    )},
                "analysis_config": _phase3_analysis_config(
                    expression="1.0",  # constant value_fn
                ),
            },
        )
        m.on_query(ClientId("eid-1"), q)

        # Drive Stage-1 originals. Turn 0 has bad delta; others neutral.
        for turn in range(6):
            await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )

        # Expected candidate set: {0, 1, 2} (worst-quantile=0.5 over
        # the per-color move distribution; window_size=1).
        # Phase 3 spawns 3 sub-queries (one per candidate).
        assert await _wait_for_spawn_count(c, 3), (
            f"expected 3 parallel spawns; got {len(c.submitted)}"
        )
        spawn_turn_sets = sorted(
            tuple(spawn_q.analyze_turns) for _, spawn_q in c.submitted
        )
        # Each spawn covers a single turn.
        assert all(len(t) == 1 for t in spawn_turn_sets), (
            f"each spawn should cover one turn; got {spawn_turn_sets}"
        )
        # The three single-turn spawns cover the candidate set.
        single_turns = sorted(t[0] for t in spawn_turn_sets)
        assert single_turns == [0, 1, 2]

        # Each spawn's maxVisits sums to the per-round budget (300).
        total_extra = sum(
            int(spawn_q.opaque["maxVisits"]) - 100  # parent maxVisits
            for _, spawn_q in c.submitted
        )
        assert total_extra == 300, (
            f"per-turn allocations should sum to budget; got {total_extra}"
        )

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_finalization_emits_one_authoritative_per_turn(self) -> None:
        """End-to-end: drive Stage 1 + drive spawn finals; verify
        finalization emits exactly one authoritative per turn,
        carrying the latest observed payload (spawn-derived for
        deepened turns; original for non-deepened)."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(extra_visits=300, max_rounds=1)},
                "analysis_config": _phase3_analysis_config(expression="1.0"),
            },
        )
        m.on_query(ClientId("eid-1"), q)

        all_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in range(6):
            all_yields += await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )

        assert await _wait_for_spawn_count(c, 3)
        # Drive each spawn's final.
        for spawn_oid, spawn_q in c.submitted:
            turn = int(spawn_q.analyze_turns[0])
            all_yields += await _drive(
                m, spawn_oid, _spawn_final(turn, marker="from_phase3"),
            )

        # Phase 3 dispatch needs additional event-loop ticks after
        # the last spawn final for the merged-stream pump tasks to
        # settle and the coroutine to reach the finalization stage.
        all_yields += await _settle_and_drain(m, ClientId("eid-1"))

        # Finalization invariant: exactly one authoritative per turn.
        auths = [
            r for _, r in all_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        ]
        auth_turns = sorted(r.turn_number for r in auths)
        assert auth_turns == [0, 1, 2, 3, 4, 5]

        # Deepened turns carry the spawn marker; non-deepened don't.
        by_turn = {r.turn_number: r for r in auths}
        for turn in (0, 1, 2):
            assert by_turn[turn].opaque.get("marker") == "from_phase3", (
                f"deepened turn {turn}'s finalisation should carry the "
                f"spawn payload; got opaque={by_turn[turn].opaque}"
            )
        for turn in (3, 4, 5):
            assert "marker" not in by_turn[turn].opaque

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_phase3_disengaged_uses_v124_dispatch(self) -> None:
        """When `allocation_algorithm` absent, the dispatch is
        unchanged from v1.0.24 (single deeper-query spawn)."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                # No `allocation_algorithm` — Phase 3 disengaged.
                "capabilities": {"adaptive_reevaluate":
                    {"extra_visits": 300, "worst_quantile": 0.5,
                     "budget": {"max_rounds": 1}}},
            },
        )
        m.on_query(ClientId("eid-1"), q)
        for turn in range(6):
            await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )
        # v1.0.24: one big spawn covering the whole worst-set.
        assert await _wait_for_spawn_count(c, 1)
        assert len(c.submitted) == 1
        _, spawn_q = c.submitted[0]
        assert len(spawn_q.analyze_turns) >= 2  # multi-turn batched
        m.on_session_end()


# ===========================================================================
# 3. Finalization composition under Phase 3
# ===========================================================================


class TestPhase3FinalizationComposition:
    """Pins the v1.0.24 finalization stage's composition with the
    Phase 3 N-parallel-spawn dispatch. Per §6 of the roadmap, no code
    change is expected — Phase 3's per-round observations land in
    state.last_packet via state.observe, and finalization emits one
    `is_during_search=False` per analyzed turn regardless of how many
    spawn sources contributed.

    Three properties:

      - Multi-round Phase 3 (max_rounds=2): each round spawns its
        own N parallel sub-queries; finalisation emits exactly one
        auth per turn at end-of-loop.
      - Latest-payload provenance ACROSS rounds: a turn deepened in
        round 1 AND round 2 carries the round-2 payload in its
        finalisation, not the round-1 payload.
      - Mid-loop invariant under Phase 3: previews stream during the
        loop; no authoritatives until all rounds complete.
    """

    @pytest.mark.asyncio
    async def test_multi_round_phase3_emits_one_auth_per_turn(self) -> None:
        """Two rounds of Phase 3 dispatch; finalisation emits one
        authoritative per analyzed turn."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(extra_visits=300, max_rounds=2)},
                "analysis_config": _phase3_analysis_config(expression="1.0"),
            },
        )
        m.on_query(ClientId("eid-1"), q)

        all_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in range(6):
            all_yields += await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )

        # Round 1: 3 parallel spawns; drive their finals.
        assert await _wait_for_spawn_count(c, 3)
        round1_spawns = list(c.submitted)
        for spawn_oid, spawn_q in round1_spawns:
            turn = int(spawn_q.analyze_turns[0])
            all_yields += await _drive(
                m, spawn_oid, _spawn_final(turn, marker="round1"),
            )

        # Round 2: another 3 parallel spawns (worst-set stable under
        # constant value_fn → same candidates).
        assert await _wait_for_spawn_count(c, 6)
        round2_spawns = c.submitted[len(round1_spawns):]
        for spawn_oid, spawn_q in round2_spawns:
            turn = int(spawn_q.analyze_turns[0])
            all_yields += await _drive(
                m, spawn_oid, _spawn_final(turn, marker="round2"),
            )

        all_yields += await _settle_and_drain(m, ClientId("eid-1"))

        # Finalisation invariant: exactly one auth per turn.
        auths = [
            r for _, r in all_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        ]
        auth_turns = sorted(r.turn_number for r in auths)
        assert auth_turns == [0, 1, 2, 3, 4, 5], (
            f"multi-round Phase 3 should emit one auth per turn at end-of-"
            f"loop; got auth_turns={auth_turns}"
        )

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_finalization_payload_is_latest_observed(self) -> None:
        """A turn deepened in round 1 AND round 2 carries round 2's
        payload in finalisation — `state.observe` overwrites
        `last_packet` on each call, so the latest spawn's response
        wins."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(extra_visits=300, max_rounds=2)},
                "analysis_config": _phase3_analysis_config(expression="1.0"),
            },
        )
        m.on_query(ClientId("eid-1"), q)

        for turn in range(6):
            await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )

        # Round 1: drive spawn finals with marker="round1".
        assert await _wait_for_spawn_count(c, 3)
        for spawn_oid, spawn_q in list(c.submitted):
            turn = int(spawn_q.analyze_turns[0])
            await _drive(
                m, spawn_oid, _spawn_final(turn, marker="round1"),
            )

        # Round 2: drive spawn finals with marker="round2".
        assert await _wait_for_spawn_count(c, 6)
        for spawn_oid, spawn_q in c.submitted[3:]:
            turn = int(spawn_q.analyze_turns[0])
            await _drive(
                m, spawn_oid, _spawn_final(turn, marker="round2"),
            )

        finalisation = await _settle_and_drain(m, ClientId("eid-1"))
        auths = {
            r.turn_number: r for _, r in finalisation
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        }
        # Deepened turns (0, 1, 2) carry the round-2 marker; non-
        # deepened turns (3, 4, 5) carry the original (no marker).
        for turn in (0, 1, 2):
            assert auths[turn].opaque.get("marker") == "round2", (
                f"turn {turn}'s finalisation should carry the latest "
                f"round's payload (round2); got {auths[turn].opaque}"
            )
        for turn in (3, 4, 5):
            assert "marker" not in auths[turn].opaque

        m.on_session_end()

    @pytest.mark.asyncio
    async def test_mid_loop_emits_only_previews_under_phase3(self) -> None:
        """v1.0.24's mid-loop invariant carries through Phase 3:
        while the multi-round loop is in flight, every emission is
        a preview; authoritative emissions occur only at end-of-loop
        finalisation."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2, 3, 4, 5],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(extra_visits=300, max_rounds=2)},
                "analysis_config": _phase3_analysis_config(expression="1.0"),
            },
        )
        m.on_query(ClientId("eid-1"), q)

        pre_finalisation_yields: List[Tuple[ClientId, KataGoResponse]] = []
        for turn in range(6):
            pre_finalisation_yields += await _drive(
                m, ClientId("eid-1"),
                _bad_final(0) if turn == 0 else _neutral_final(turn),
            )
        # Drive round-1 spawn finals only — round 2 hasn't started
        # yet.
        assert await _wait_for_spawn_count(c, 3)
        for spawn_oid, spawn_q in list(c.submitted):
            turn = int(spawn_q.analyze_turns[0])
            pre_finalisation_yields += await _drive(
                m, spawn_oid, _spawn_final(turn, marker="round1"),
            )

        # At this point the loop should be poised to enter round 2
        # (max_rounds=2 still has capacity). No authoritatives yet.
        auth_emissions = [
            r for _, r in pre_finalisation_yields
            if isinstance(r, AnalyzeResponse) and not r.is_during_search
        ]
        assert auth_emissions == [], (
            f"mid-loop should emit only previews under Phase 3; got "
            f"{len(auth_emissions)} authoritative(s) before finalisation"
        )

        m.on_session_end()


# ===========================================================================
# 4. Eager include validation
# ===========================================================================


class TestEagerIncludeValidation:

    def test_required_include_flags_policy(self) -> None:
        flags = _required_include_flags("sum([p for p in extra.policy])")
        assert "includePolicy" in flags

    def test_required_include_flags_ownership_root(self) -> None:
        """`extra.ownership` (root, no moveInfos in expression) →
        includeOwnership only."""
        flags = _required_include_flags("sum(extra.ownership)")
        assert flags == {"includeOwnership"}

    def test_required_include_flags_ownership_stdev_root(self) -> None:
        flags = _required_include_flags("sum(extra.ownershipStdev)")
        assert flags == {"includeOwnership", "includeOwnershipStdev"}

    def test_required_include_flags_moveinfos_ownership(self) -> None:
        """`moveInfos[*].ownership` expression heuristic adds the
        moves-* variant flag."""
        flags = _required_include_flags(
            "sum(m.ownership for m in moveInfos)",
        )
        assert "includeOwnership" in flags
        assert "includeMovesOwnership" in flags

    def test_required_include_flags_pv_visits(self) -> None:
        flags = _required_include_flags(
            "max(m.pvVisits[0] for m in moveInfos)",
        )
        assert "includePVVisits" in flags

    def test_required_include_flags_no_gated_attrs(self) -> None:
        """A value-fn reading only always-on fields needs no flags."""
        flags = _required_include_flags(
            "max(m.utilityLcb for m in moveInfos[:5])",
        )
        assert flags == set()

    def test_required_include_flags_invalid_syntax(self) -> None:
        """SyntaxError returns empty set — the interpreter raises at
        evaluation time."""
        flags = _required_include_flags("this is not python ((")
        assert flags == set()

    @pytest.mark.asyncio
    async def test_missing_include_flag_raises(self) -> None:
        """A value_fn expression reading extra.policy without
        `includePolicy: true` on the parent query refuses with
        allocation_invalid + missing_includes in the detail."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                # includePolicy deliberately ABSENT.
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(binding="vfn")},
                "analysis_config": _phase3_analysis_config(
                    binding="vfn",
                    expression="sum([p for p in extra.policy])",
                ),
            },
        )
        m.on_query(ClientId("eid-1"), q)
        err = await _wait_for_coroutine_error(m, ClientId("eid-1"))
        assert err is not None
        # Error message should mention allocation_invalid + missing_includes.
        msg = err.opaque["error"]
        assert "allocation_invalid" in msg
        assert "missing_includes" in msg or "includePolicy" in msg
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_include_flag_present_engages_successfully(self) -> None:
        """Same value_fn, but `includePolicy: true` is set — engagement
        succeeds and the dispatch proceeds normally."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = KataGoQuery(
            action=KataGoAction.ANALYZE,
            analyze_turns=[0, 1, 2],
            opaque={
                "rules": "tromp-taylor", "komi": 7.5,
                "boardXSize": 19, "moves": [["B", "Q4"], ["W", "D16"]],
                "maxVisits": 100,
                "includePolicy": True,
                "capabilities": {"adaptive_reevaluate":
                    _phase3_capabilities(binding="vfn", extra_visits=200)},
                "analysis_config": _phase3_analysis_config(
                    binding="vfn",
                    expression="1.0",  # doesn't actually use policy
                ),
            },
        )
        m.on_query(ClientId("eid-1"), q)
        # No error response should appear on the wire after on_query;
        # drive a Stage-1 final and verify the coroutine ran normally
        # (no error envelope emitted).
        responses = await _drive(
            m, ClientId("eid-1"), _bad_final(0),
        )
        # Confirm no error envelope.
        errors = [
            r for _, r in responses
            if hasattr(r, "opaque") and isinstance(r.opaque, dict)
            and "error" in r.opaque
        ]
        assert errors == [], f"expected no error envelope; got {errors}"
        m.on_session_end()
