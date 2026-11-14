# SPDX-License-Identifier: MIT
from __future__ import annotations

import hashlib
from typing import Any

import pytest

from transformers import stencil_cover_enricher as stencil
from transformers.stencil_cover.stencil_bulk_solver import StencilBulkSolver
from transformers.stencil_cover.stencil_cover import stencil_bulk_solver as bulk_module
from transformers.stencil_cover.stencil_solver_api import (
    BackendStatusReported,
    Incumbent,
    LowerBoundUnavailable,
    NoIncumbent,
    NoIncumbentReason,
    NativeEffortUnavailable,
    NoNativeEffortReason,
    NoSolverFailure,
    OutcomeStatus,
    NoNativeEffortLimit,
    NativeEffortLimit,
    PlacementWitness,
    SolverBudget,
    ProvenLowerBound,
    SolverIdentity,
    SolverOutcome,
)
from transformers.stencil_cover.stencil_solver_runtime import solve_component
from transformers.stencil_cover.stencil_cache_storage import (
    SolverCacheRequest,
    SolverWorkProfile,
    make_solver_work_profile,
)
from transformers.stencil_cover.stencil_solver_factory import (
    create_solver_backend, register_solver_backend,
)


def _identity(name: str = "fake", mode: str = "exact") -> SolverIdentity:
    config = '{"mode":"' + mode + '"}'
    return SolverIdentity(
        backend_name=name,
        backend_version="test-1",
        adapter_version="test-adapter-v1",
        configuration_json=config,
        configuration_fingerprint=hashlib.sha256(config.encode("utf-8")).hexdigest(),
    )


class _FakeOptimalBackend:
    def __init__(self, *, name: str = "fake", mode: str = "exact") -> None:
        self._identity = _identity(name, mode)
        self.calls: list[tuple[Any, Any]] = []

    @property
    def identity(self) -> SolverIdentity:
        return self._identity

    def solve(self, problem: Any, budget: Any) -> SolverOutcome:
        self.calls.append((problem, budget))
        # The two-cell horizontal domino is a canonical family placement.
        witness = PlacementWitness((("1x2", ((0, 0), (0, 1))),))
        return SolverOutcome(
            status=OutcomeStatus.OPTIMAL,
            incumbent=Incumbent(1, witness),
            lower_bound=ProvenLowerBound(1),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
            ),
            backend_status=BackendStatusReported("FAKE_OPTIMAL"),
            backend_metrics_json='{"fake_metric":1}',
            error=NoSolverFailure(),
        )


def _domino_family() -> tuple[tuple[str, tuple[tuple[int, int], ...]], ...]:
    return (
        ("1x1", ((0, 0),)),
        ("1x2", ((0, 0), (0, 1))),
    )


def _domino_key() -> bytes:
    key, _frame = stencil._component_shape_key((0, 1), 2, _domino_family())
    return key


def test_fake_backend_substitutes_without_transformer_solver_changes() -> None:
    family = _domino_family()
    backend = _FakeOptimalBackend()
    request = SolverCacheRequest(
        shape_key=_domino_key(),
        family=family,
        backend=backend,
        budget=SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
    )
    candidate = stencil._solve_component_uncached(request)
    optimal, status, encoded = candidate
    assert candidate.provenance["disposition"] == "present"
    assert candidate.provenance["producer_path"] == "foreground"
    assert optimal is True
    assert status == 0
    assert len(encoded) > 2
    assert len(backend.calls) == 1
    problem, budget = backend.calls[0]
    assert problem.occupied_cells == ((0, 0), (0, 1))
    assert problem.family_fingerprint
    assert budget.wall_time_limit_ms == 250
    assert budget.policy_id == "foreground-250ms"


def test_solver_factory_registers_and_selects_a_second_backend() -> None:
    name = "test-fake-backend"
    register_solver_backend(name, lambda _configuration: _FakeOptimalBackend(name=name))
    backend = create_solver_backend(name, {"unused": True})
    assert backend.identity.backend_name == name


def test_fake_backend_can_be_injected_at_transformer_component_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This test covers dependency injection, not Redis cache reuse. A globally
    # cached optimal cover is reusable across backends and would skip solve().
    def solve_without_persistent_cache(
        request: SolverCacheRequest,
    ) -> tuple[bool, int, bytes]:
        return stencil._solve_component_uncached(request).result

    monkeypatch.setattr(
        stencil, "_solve_component_cached", solve_without_persistent_cache,
    )
    family = _domino_family()
    backend = _FakeOptimalBackend(name="fake-transformer-boundary")
    result = stencil._solve_region(
        [0, 1], width=2, height=1, family=family, solver_backend=backend,
    )
    assert result["optimal"] is True
    assert result["tileCount"] == 1
    assert result["placements"] == [
        {"stencil": "1x2", "points": [[0, 0], [1, 0]]},
    ]
    assert len(backend.calls) == 1


def test_shared_runtime_measures_fake_adapter_and_retains_backend_identity() -> None:
    family = _domino_family()
    backend = _FakeOptimalBackend(name="second-backend")
    execution = solve_component(
        backend,
        _domino_key(),
        family,
        SolverBudget(
            wall_time_limit_ms=10_000,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="bulk-10000ms",
        ),
    )
    assert execution.identity.backend_name == "second-backend"
    assert execution.elapsed_wall_ns >= 0
    assert execution.outcome.status is OutcomeStatus.OPTIMAL
    assert execution.cache_result[0:2] == (True, 0)
    assert execution.fallback_used is False


class _MemoryStore:
    def __init__(self, initial: tuple[bool, int, bytes]) -> None:
        self.result = initial
        self.stored: list[tuple[bool, int, bytes]] = []
        self.provenance: dict[str, Any] | None = None

    def initialize_pending_queue(
        self, _family: Any, _work_profile: SolverWorkProfile,
    ) -> int:
        return 0

    def timeout_metadata(
        self, _shape_key: bytes, _family: Any, _work_profile: SolverWorkProfile,
    ) -> None:
        return None

    def get_result(self, _shape_key: bytes, _family: Any) -> tuple[bool, int, bytes]:
        return self.result

    def put_result(
        self, _shape_key: bytes, _family: Any, result: tuple[bool, int, bytes],
        *, provenance: dict[str, Any] | None = None,
        work_profile: SolverWorkProfile | None = None,
    ) -> bool:
        self.result = result
        self.stored.append(result)
        self.provenance = provenance
        return True

    def mark_timed_out(
        self,
        _shape_key: bytes,
        _family: Any,
        _result: tuple[bool, int, bytes],
        *,
        time_limit_ms: int,
        attempted_at: float,
        work_profile: SolverWorkProfile,
    ) -> bool:
        return True

    def pending_components(
        self, _family: Any, _work_profile: SolverWorkProfile,
    ) -> frozenset[bytes]:
        return frozenset()


def test_bulk_worker_uses_the_same_backend_boundary_and_budget() -> None:
    family = _domino_family()
    fallback = stencil._encode_cached_placements(
        [("1x1", (0,)), ("1x1", (1,))], family,
    )
    store = _MemoryStore((False, 2, fallback))
    backend = _FakeOptimalBackend()
    worker = StencilBulkSolver(store, family=family, solver_backend=backend)

    assert worker.process_component(_domino_key()) == "optimal"
    assert store.result[0:2] == (True, 0)
    assert len(backend.calls) == 1
    _problem, budget = backend.calls[0]
    assert budget.wall_time_limit_ms == 10_000
    assert budget.policy_id == "bulk-10000ms"
    assert store.provenance is not None
    assert store.provenance["disposition"] == "present"
    assert store.provenance["producer_path"] == "bulk"



def test_feasible_outcome_rejects_a_lower_bound_above_its_incumbent() -> None:
    incumbent = Incumbent(
        tile_count=2,
        placement_witness=PlacementWitness((
            ("1x2", ((0, 0), (0, 1))),
            ("1x2", ((1, 0), (1, 1))),
        )),
    )
    with pytest.raises(ValueError, match="lower bound exceeds incumbent"):
        SolverOutcome(
            status=OutcomeStatus.FEASIBLE,
            incumbent=incumbent,
            lower_bound=ProvenLowerBound(3),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
            ),
            backend_status=BackendStatusReported("FAKE_FEASIBLE"),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def test_local_lru_keeps_different_backend_profiles_separate() -> None:
    family = _domino_family()
    calls: list[str] = []

    def invoke(request: SolverCacheRequest) -> tuple[bool, int, bytes]:
        calls.append(request.backend.identity.fingerprint)
        return (True, 0, b"\x01\x00\x00\x02\x00\x00\x00\x01\x00")

    cache = stencil._InvalidatableSolverCache(invoke, maxsize=8)
    first = _FakeOptimalBackend(name="same", mode="first")
    second = _FakeOptimalBackend(name="same", mode="second")

    def request(backend: _FakeOptimalBackend) -> SolverCacheRequest:
        return SolverCacheRequest(
            shape_key=_domino_key(),
            family=family,
            backend=backend,
            budget=SolverBudget(
                wall_time_limit_ms=250,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id="foreground-250ms",
            ),
        )

    cache(request(first))
    cache(request(second))
    cache(request(first))
    assert calls == [first.identity.fingerprint, second.identity.fingerprint]


class _FakeNoIncumbentBackend:
    @property
    def identity(self) -> SolverIdentity:
        return _identity(name="empty", mode="no-incumbent")

    def solve(self, problem: Any, budget: Any) -> SolverOutcome:
        return SolverOutcome(
            status=OutcomeStatus.NO_INCUMBENT,
            incumbent=NoIncumbent(NoIncumbentReason.BUDGET_EXHAUSTED),
            lower_bound=LowerBoundUnavailable(),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DID_NOT_REPORT_EFFORT,
            ),
            backend_status=BackendStatusReported("TIME_LIMIT"),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def test_no_incumbent_uses_a_distinct_singleton_fallback() -> None:
    family = (("domino", ((0, 0), (0, 1))),)
    key, _frame = stencil._component_shape_key((0,), 1, family)
    execution = solve_component(
        _FakeNoIncumbentBackend(),
        key,
        family,
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
    )
    assert execution.outcome.status is OutcomeStatus.NO_INCUMBENT
    assert execution.fallback_used is True
    assert execution.cache_result[0:2] == (False, 2)


class _FakeFeasibleBackend(_FakeOptimalBackend):
    def solve(self, problem: Any, budget: Any) -> SolverOutcome:
        optimal_result = super().solve(problem, budget)
        return SolverOutcome(
            status=OutcomeStatus.FEASIBLE,
            incumbent=optimal_result.incumbent,
            lower_bound=LowerBoundUnavailable(),
            native_effort=optimal_result.native_effort,
            backend_status=BackendStatusReported("TIME_LIMIT_WITH_INCUMBENT"),
            backend_metrics_json=optimal_result.backend_metrics_json,
            error=NoSolverFailure(),
        )


def test_valid_feasible_incumbent_is_preserved_without_claiming_optimality() -> None:
    execution = solve_component(
        _FakeFeasibleBackend(),
        _domino_key(),
        _domino_family(),
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
    )
    assert execution.outcome.status is OutcomeStatus.FEASIBLE
    assert execution.outcome.optimality_proven is False
    assert execution.cache_result[0:2] == (False, 1)
    assert execution.fallback_used is False


class _FakeInvalidWitnessBackend(_FakeOptimalBackend):
    def solve(self, problem: Any, budget: Any) -> SolverOutcome:
        self.calls.append((problem, budget))
        invalid = PlacementWitness((("1x2", ((0, 0), (0, 2))),))
        return SolverOutcome(
            status=OutcomeStatus.OPTIMAL,
            incumbent=Incumbent(1, invalid),
            lower_bound=ProvenLowerBound(1),
            native_effort=NativeEffortUnavailable(
                NoNativeEffortReason.BACKEND_DOES_NOT_EXPOSE_EFFORT,
            ),
            backend_status=BackendStatusReported("FAKE_OPTIMAL"),
            backend_metrics_json="{}",
            error=NoSolverFailure(),
        )


def test_invalid_backend_witness_is_rejected_and_not_cached_as_optimal() -> None:
    execution = solve_component(
        _FakeInvalidWitnessBackend(),
        _domino_key(),
        _domino_family(),
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NoNativeEffortLimit(),
            policy_id="foreground-250ms",
        ),
    )
    assert execution.outcome.status is OutcomeStatus.ERROR
    assert execution.outcome.optimality_proven is False
    assert execution.cache_result[0:2] == (False, 2)
    assert execution.fallback_used is True


def test_bulk_attempt_records_a_bulk_sighting_before_solving(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "false")
    family = _domino_family()
    fallback = stencil._encode_cached_placements(
        [("1x1", (0,)), ("1x1", (1,))], family,
    )
    store = _MemoryStore((False, 2, fallback))
    backend = _FakeOptimalBackend()
    sightings: list[tuple[bytes, str]] = []

    def record_sighting(
        shape_key: bytes, seen_family: Any, producer_path: str,
    ) -> None:
        assert tuple(seen_family) == family
        sightings.append((shape_key, producer_path))

    monkeypatch.setattr(bulk_module, "publish_instance_seen", record_sighting)
    worker = StencilBulkSolver(store, family=family, solver_backend=backend)

    assert worker.process_component(_domino_key()) == "optimal"
    assert sightings == [(_domino_key(), "bulk")]


def test_bulk_sighting_precedes_timeout_and_result_cache_returns(
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "false")
    family = _domino_family()
    optimal = stencil._encode_cached_placements([("1x2", (0, 1))], family)
    store = _MemoryStore((True, 0, optimal))
    order: list[str] = []

    def record_sighting(
        shape_key: bytes, seen_family: Any, producer_path: str,
    ) -> None:
        assert shape_key == _domino_key()
        assert tuple(seen_family) == family
        assert producer_path == "bulk"
        order.append("sighting")

    original_timeout = store.timeout_metadata
    original_result = store.get_result

    def timeout_lookup(
        shape_key: bytes, seen_family: Any, work_profile: SolverWorkProfile,
    ) -> None:
        order.append("timeout")
        return original_timeout(shape_key, seen_family, work_profile)

    def result_lookup(
        shape_key: bytes, seen_family: Any,
    ) -> tuple[bool, int, bytes]:
        order.append("result")
        return original_result(shape_key, seen_family)

    monkeypatch.setattr(bulk_module, "publish_instance_seen", record_sighting)
    monkeypatch.setattr(store, "timeout_metadata", timeout_lookup)
    monkeypatch.setattr(store, "get_result", result_lookup)
    backend = _FakeOptimalBackend()
    worker = StencilBulkSolver(store, family=family, solver_backend=backend)

    assert worker.process_component(_domino_key()) == "optimal"
    assert order == ["sighting", "timeout", "result"]
    assert backend.calls == []



def test_solver_work_profile_fingerprint_is_stable_and_covers_all_budget_dimensions() -> None:
    identity = _identity(name="profiled", mode="first")
    baseline = SolverBudget(
        wall_time_limit_ms=10_000,
        native_effort_limit=NoNativeEffortLimit(),
        policy_id="bulk-v1",
    )
    profile = make_solver_work_profile(identity, baseline)
    assert make_solver_work_profile(identity, baseline) == profile

    variants = (
        make_solver_work_profile(
            _identity(name="profiled", mode="second"), baseline,
        ),
        make_solver_work_profile(
            identity,
            SolverBudget(
                wall_time_limit_ms=5_000,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id="bulk-v1",
            ),
        ),
        make_solver_work_profile(
            identity,
            SolverBudget(
                wall_time_limit_ms=10_000,
                native_effort_limit=NativeEffortLimit(100, "nodes"),
                policy_id="bulk-v1",
            ),
        ),
        make_solver_work_profile(
            identity,
            SolverBudget(
                wall_time_limit_ms=10_000,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id="bulk-v2",
            ),
        ),
    )
    assert all(item.fingerprint != profile.fingerprint for item in variants)
    assert len({profile.fingerprint, *(item.fingerprint for item in variants)}) == 5
    assert profile.document["solver_identity"]["fingerprint"] == identity.fingerprint
    assert profile.document["budget"]["wall_time_limit_ms"] == 10_000


def test_solver_work_profile_identity_includes_native_effort_unit_and_value() -> None:
    identity = _identity()
    first = make_solver_work_profile(
        identity,
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NativeEffortLimit(100, "nodes"),
            policy_id="foreground-v1",
        ),
    )
    changed_unit = make_solver_work_profile(
        identity,
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NativeEffortLimit(100, "iterations"),
            policy_id="foreground-v1",
        ),
    )
    changed_value = make_solver_work_profile(
        identity,
        SolverBudget(
            wall_time_limit_ms=250,
            native_effort_limit=NativeEffortLimit(200, "nodes"),
            policy_id="foreground-v1",
        ),
    )
    assert len({first.fingerprint, changed_unit.fingerprint, changed_value.fingerprint}) == 3
