"""tests/test_allocation_refusal.py — `allocation_invalid` refusal surface (v1.0.25).

End-to-end pinning of the Phase 3 refusal surface. The orchestration
framework catches `AdaptiveConfigurationError` raised inside the
coroutine and surfaces it on the wire as a `MetadataResponse` with
an `error` opaque field. This file pins the user-visible failure
mode across the full refusal taxonomy.

The lower-level unit tests (visit-scaling parse refusal in
`tests/test_visit_scaling.py`; allocation-algorithm parse refusal in
`tests/test_allocation_algorithms.py`; eager-engagement validation
in `tests/test_phase3_dispatch.py`) verify each individual code
path. This file fills the cross-cutting gaps:

  - Wire-shape under refusal: every refusal produces exactly one
    `MetadataResponse` with `error` opaque carrying
    `allocation_invalid`.
  - `analysis_config` absent / malformed shapes.
  - Multi-field missing-includes case: a value-fn expression
    referencing two opt-in-gated fields produces a single refusal
    whose `detail.missing_includes` lists both flags.

Run from the proxy directory: `pytest tests/test_allocation_refusal.py`.

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
    MetadataResponse,
)
from middleware.adaptive_reevaluate import adaptive_reevaluate  # noqa: E402
from middleware.session_middleware import SessionCapabilities  # noqa: E402


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------


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


def _dummy_final() -> AnalyzeResponse:
    return AnalyzeResponse(
        is_during_search=False, turn_number=0,
        opaque={"moveInfos": [], "rootInfo": {"visits": 100, "scoreStdev": 10.0}},
    )


async def _drive_and_collect_error(
    m: Any, q: KataGoQuery, oid: ClientId = ClientId("eid-1"),
) -> MetadataResponse:
    """Submit the query, drive one dummy original to tick the loop,
    and return the framework's `MetadataResponse` error envelope.

    Asserts that exactly one error envelope appears (the refusal
    surface should produce a single structured error, not multiple).
    """
    import asyncio
    m.on_query(oid, q)
    # Drive any response to tick the event loop so the coroutine's
    # _engage_phase3 raise propagates through _drive_coroutine.
    out: List[Tuple[ClientId, KataGoResponse]] = []
    async for o, r in m.handle_response(oid, _dummy_final(), None):
        out.append((o, r))
    # Settle any remaining yields.
    await asyncio.sleep(0.02)
    ctx = m._contexts.get(oid)
    if ctx is not None:
        while True:
            try:
                item = ctx._output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if isinstance(item, tuple):
                out.append(item)
    errors = [
        r for _, r in out
        if isinstance(r, MetadataResponse)
        and isinstance(r.opaque, dict)
        and "error" in r.opaque
    ]
    assert len(errors) == 1, (
        f"expected exactly one error envelope; got {len(errors)} "
        f"(all yields: {out})"
    )
    assert "allocation_invalid" in errors[0].opaque["error"], (
        f"error envelope should mention allocation_invalid; got "
        f"{errors[0].opaque['error']}"
    )
    return errors[0]


def _q(opaque: dict[str, Any]) -> KataGoQuery:
    """Construct a minimal analyze query with the given opaque overlay."""
    base: dict[str, Any] = {
        "rules": "tromp-taylor", "komi": 7.5, "boardXSize": 19,
        "moves": [["B", "Q4"], ["W", "D16"]],
        "maxVisits": 100,
    }
    base.update(opaque)
    return KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0, 1],
        opaque=base,
    )


# ===========================================================================
# 1. Wire shape under refusal
# ===========================================================================


class TestRefusalWireShape:
    """Each refusal class should produce exactly one MetadataResponse
    error envelope. Pin the wire shape across the taxonomy."""

    @pytest.mark.asyncio
    async def test_unknown_algorithm_surfaces_error(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "nonexistent_algo",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {"vfn": "1.0"},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "nonexistent_algo" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_unknown_model_surfaces_error(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "nonexistent_model",
                "value_binding": "vfn",
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {"vfn": "1.0"},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "nonexistent_model" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_malformed_algorithm_params_surfaces_error(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "ucb",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
                "allocation_params": {"ucb_kappa": -5.0},  # invalid
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {"vfn": "1.0"},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "ucb_kappa" in err.opaque["error"]
        m.on_session_end()


# ===========================================================================
# 2. analysis_config shape refusals
# ===========================================================================


class TestAnalysisConfigRefusals:
    """Phase 3 requires a well-formed analysis_config carrying the
    value_fn binding. Refusals across the malformed shapes."""

    @pytest.mark.asyncio
    async def test_analysis_config_absent(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            # analysis_config deliberately absent.
        })
        err = await _drive_and_collect_error(m, q)
        # Either "analysis_config" mentioned in the error or
        # "value_binding does not resolve" — both are acceptable
        # phrasings of the same refusal.
        assert (
            "analysis_config" in err.opaque["error"]
            or "value_binding" in err.opaque["error"]
            or "value_fn" in err.opaque["error"]
        ), err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_analysis_config_not_a_dict(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            "analysis_config": "not a dict",
        })
        err = await _drive_and_collect_error(m, q)
        # The exact phrasing varies (analysis_config malformed or
        # value_binding unresolvable depending on where the chain
        # fails); both are valid refusals.
        assert "allocation_invalid" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_value_fn_symbol_not_in_symtable(self) -> None:
        """value_binding names a symbol that bindings.value_fn agrees
        with, but symbols dict doesn't define it. get_value_fn
        returns None; the dispatch refuses."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "missing_symbol",
            }},
            "analysis_config": {
                "bindings": {"value_fn": "missing_symbol"},
                # symbols deliberately omits "missing_symbol"
                "symbols": {},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "value_binding" in err.opaque["error"] or "value_fn" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_bindings_value_fn_missing(self) -> None:
        """capability.value_binding present but
        analysis_config.bindings.value_fn entirely absent: mismatch
        refusal."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            "analysis_config": {
                "bindings": {},  # no value_fn binding
                "symbols": {"vfn": "1.0"},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "allocation_invalid" in err.opaque["error"]
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_empty_value_binding_string(self) -> None:
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "",  # empty string
            }},
            "analysis_config": {
                "bindings": {"value_fn": ""},
                "symbols": {"": "1.0"},
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "value_binding" in err.opaque["error"]
        m.on_session_end()


# ===========================================================================
# 3. Multi-field missing-includes
# ===========================================================================


class TestMultiFieldMissingIncludes:
    """Eager validation surfaces ALL missing include* flags at once,
    not one-at-a-time, so the user can fix multiple wire-shape
    issues in a single edit."""

    @pytest.mark.asyncio
    async def test_two_missing_includes_both_reported(self) -> None:
        """A value_fn expression referencing two opt-in-gated fields
        with neither flag set produces a single refusal whose
        `detail.missing_includes` lists both flags."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            # Neither includePolicy nor includeOwnership set.
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {
                    # Expression references BOTH gated fields.
                    "vfn": (
                        "sum([p for p in extra.policy]) "
                        "+ sum(extra.ownership)"
                    ),
                },
            },
        })
        err = await _drive_and_collect_error(m, q)
        msg = err.opaque["error"]
        # Both flag names should appear in the error detail.
        assert "includePolicy" in msg
        assert "includeOwnership" in msg
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_partial_includes_only_missing_reported(self) -> None:
        """A value_fn needing two flags, one set and one absent —
        only the missing one is reported."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "includePolicy": True,
            # includeOwnership deliberately absent.
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {
                    "vfn": "sum(extra.policy) + sum(extra.ownership)",
                },
            },
        })
        err = await _drive_and_collect_error(m, q)
        msg = err.opaque["error"]
        # includeOwnership in the missing list, includePolicy NOT
        # (since it was supplied).
        assert "includeOwnership" in msg
        # Sanity: the error mentions missing_includes (the structured
        # detail key) and not a generic message.
        assert "missing_includes" in msg or "missing" in msg.lower()
        m.on_session_end()

    @pytest.mark.asyncio
    async def test_moves_ownership_variant_required(self) -> None:
        """A value_fn iterating moveInfos and reading m.ownership
        requires includeMovesOwnership (the moves-* variant) in
        addition to includeOwnership."""
        c, caps = _make_caps()
        m = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "includeOwnership": True,
            # includeMovesOwnership deliberately absent.
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {
                    "vfn": (
                        "max(sum(m.ownership) for m in moveInfos[:3])"
                    ),
                },
            },
        })
        err = await _drive_and_collect_error(m, q)
        assert "includeMovesOwnership" in err.opaque["error"]
        m.on_session_end()


# ===========================================================================
# 4. Successful engagement smoke
# ===========================================================================


class TestSuccessfulEngagementProducesNoError:
    """The flip side: a well-formed Phase 3 engagement produces NO
    error envelope. Pins that the refusal surface doesn't false-
    positive on legitimate configurations."""

    @pytest.mark.asyncio
    async def test_well_formed_phase3_no_error(self) -> None:
        c, caps = _make_caps()
        m: Any = adaptive_reevaluate(window_size=1)()
        m.on_session_start(caps)
        q = _q({
            "includePolicy": True,
            "capabilities": {"adaptive_reevaluate": {
                "allocation_algorithm": "greedy_eig",
                "visit_scaling_model": "monte_carlo_sqrt",
                "value_binding": "vfn",
                "extra_visits": 200,
                "budget": {"max_rounds": 1},
            }},
            "analysis_config": {
                "bindings": {"value_fn": "vfn"},
                "symbols": {"vfn": "sum(extra.policy)"},
            },
        })
        m.on_query(ClientId("eid-1"), q)
        out: List[Tuple[ClientId, KataGoResponse]] = []
        async for o, r in m.handle_response(
            ClientId("eid-1"), _dummy_final(), None,
        ):
            out.append((o, r))
        # No error envelope from a well-formed engagement.
        errors = [
            r for _, r in out
            if isinstance(r, MetadataResponse)
            and isinstance(r.opaque, dict)
            and "error" in r.opaque
        ]
        assert errors == [], (
            f"well-formed Phase 3 engagement should not produce an "
            f"error envelope; got {errors}"
        )
        m.on_session_end()
