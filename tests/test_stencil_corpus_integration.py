# SPDX-License-Identifier: MIT
"""Real-Redis integration tests for the stencil corpus capture pipeline.

These tests are intentionally run by the isolated-Redis Make target. They
exercise the production publisher, transformer/proxy path, Redis Lua-backed
store, SQLite spool/store, and consumer-group materializer.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Iterator, Literal, cast

import pytest
import redis
import websockets
from urllib.parse import urlsplit

_TEST_REDIS_URL = os.environ.get("KATAPROXY_TEST_REDIS_URL")
if not _TEST_REDIS_URL:
    pytest.skip(
        "run make test-stencil-corpus-integration to start isolated Redis",
        allow_module_level=True,
    )
_test_redis = urlsplit(_TEST_REDIS_URL)
os.environ["KATAPROXY_REDIS_HOST"] = _test_redis.hostname or "127.0.0.1"
os.environ["KATAPROXY_REDIS_PORT"] = str(_test_redis.port or 6379)
os.environ["KATAPROXY_REDIS_DB"] = (_test_redis.path.lstrip("/") or "0")

import proxy_server
import sproxy_config as cfg
from AbstractProxy.proxy_core import CanonicalId
from katago import KataGoQuery
from router import BackendRouter, OnComplete, OnResponse, WireDict
from transformers import stencil_cover_enricher as stencil
from transformers import stencil_corpus_publisher as corpus_publisher
from transformers import stencil_corpus_supervisor
from transformers.stencil_corpus_supervisor import StencilCorpusMaterializerSupervisor
from transformers import stencil_cache_storage
from transformers.stencil_cache_storage import (
    RedisCacheConflict,
    RedisStencilStore,
    SolverWorkProfile,
)
from transformers.stencil_corpus_events import (
    make_instance_seen_event,
    parse_event,
    serialize_event,
)
from transformers.stencil_corpus_materializer import (
    RedisStreamPort,
    StencilCorpusMaterializer,
)
from transformers.stencil_corpus_publisher import (
    CORPUS_CONSUMER_GROUP,
    CORPUS_STREAM_KEY,
    CorpusEventPublisher,
)
from transformers.stencil_corpus_store import SQLiteCorpusStore
from transformers.stencil_solver_api import NoNativeEffortLimit, SolverBudget
from transformers.stencil_solver_factory import create_solver_backend
from transformers.stencil_solver_problem import canonical_problem_from_key
from transformers.stencil_solver_runtime import solve_component


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    redis_url = os.environ.get("KATAPROXY_TEST_REDIS_URL")
    if not redis_url:
        pytest.fail(
            "KATAPROXY_TEST_REDIS_URL is required; run "
            "make test-stencil-corpus-integration to provision isolated Redis"
        )
    client = redis.Redis.from_url(redis_url, decode_responses=False)
    try:
        client.ping()
    except redis.RedisError as exc:
        client.close()
        pytest.fail(f"isolated integration Redis is not reachable: {exc}")
    try:
        yield client
    finally:
        client.close()


def _stream_port(client: redis.Redis) -> RedisStreamPort:
    # redis-py models several Streams return shapes as optional/unions; the
    # materializer only consumes the narrow operations declared by its port.
    return cast(RedisStreamPort, client)


def _profile(name: str) -> SolverWorkProfile:
    document: dict[str, Any] = {
        "schema": "kataproxy.stencil-cover.solver-work-profile.v1",
        "integration_profile": name,
    }
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return SolverWorkProfile(hashlib.sha256(encoded).hexdigest(), document)


def test_real_redis_executes_result_arbitration_timeout_and_pending_lua_scripts(
    redis_client: redis.Redis,
) -> None:
    family = (("dot", ((0, 0),)),)
    shape_key = b"real-redis-lua-arbitration-" + uuid.uuid4().hex.encode("ascii")
    store = RedisStencilStore(redis_client, "integration-lua-" + uuid.uuid4().hex)
    profile_a = _profile("profile-a")
    profile_b = _profile("profile-b")

    assert store.initialize_pending_queue(family, profile_a) == 0
    assert store.initialize_pending_queue(family, profile_b) == 0

    incumbent = (False, 1, b"\x01\x00incumbent")
    assert store.put_result(
        shape_key, family, incumbent,
        provenance={"disposition": "legacy_unknown"},
        work_profile=profile_a,
    )
    assert store.get_result(shape_key, family) == incumbent
    assert store.hard_components(family) == frozenset({shape_key})
    assert store.pending_components(family, profile_a) == frozenset({shape_key})
    assert store.pending_components(family, profile_b) == frozenset({shape_key})

    # A repeated candidate is an idempotent no-op, not a second mutation.
    assert not store.put_result(
        shape_key, family, incumbent,
        provenance={"disposition": "legacy_unknown"},
        work_profile=profile_a,
    )

    assert store.mark_timed_out(
        shape_key, family, incumbent,
        time_limit_ms=10_000, attempted_at=123.5,
        work_profile=profile_a,
    )
    metadata_a = store.timeout_metadata(shape_key, family, profile_a)
    assert metadata_a is not None
    assert metadata_a["attempted_at"] == 123.5
    assert metadata_a["work_profile_fingerprint"] == profile_a.fingerprint
    assert store.pending_components(family, profile_a) == frozenset()
    assert store.pending_components(family, profile_b) == frozenset({shape_key})
    assert store.timeout_metadata(shape_key, family, profile_b) is None
    assert not store.mark_timed_out(
        shape_key, family, incumbent,
        time_limit_ms=10_000, attempted_at=124.0,
        work_profile=profile_a,
    )

    # A profile registered after a hard result is seeded from the hard index.
    profile_c = _profile("profile-c")
    assert store.initialize_pending_queue(family, profile_c) == 1
    assert store.pending_components(family, profile_c) == frozenset({shape_key})
    assert store.initialize_pending_queue(family, profile_c) == 0

    exact = (True, 0, b"\x01\x00exact")
    assert store.put_result(
        shape_key, family, exact,
        provenance={"disposition": "legacy_unknown"},
        work_profile=profile_c,
    )
    assert store.get_result(shape_key, family) == exact
    assert store.hard_components(family) == frozenset()
    assert store.pending_components(family, profile_a) == frozenset()
    assert store.pending_components(family, profile_b) == frozenset()
    assert store.pending_components(family, profile_c) == frozenset()
    assert store.timeout_metadata(shape_key, family, profile_a) is None
    assert not store.put_result(
        shape_key, family, exact,
        provenance={"disposition": "legacy_unknown"},
        work_profile=profile_c,
    )
    with pytest.raises(RedisCacheConflict):
        store.put_result(
            shape_key, family, (True, 0, b"\x02\x00conflict"),
            provenance={"disposition": "legacy_unknown"},
            work_profile=profile_c,
        )
    assert store.get_result(shape_key, family) == exact


def test_real_redis_executes_legacy_result_migration_lua_script(
    redis_client: redis.Redis,
) -> None:
    family = (("dot", ((0, 0),)),)
    shape_key = b"real-redis-legacy-" + uuid.uuid4().hex.encode("ascii")
    legacy = RedisStencilStore(redis_client, "solver-component-d4-v3")
    current = RedisStencilStore(redis_client, "solver-component-d4-v4")
    profile = _profile("migration-target")

    old_result = (False, 1, b"\x01\x00legacy")
    assert legacy.put_result(shape_key, family, old_result)
    # Register the current profile and copy the old hard index before reading
    # the legacy value. Both operations use the production public API.
    assert current.initialize_pending_queue(family, profile) == 1
    assert current.pending_components(family, profile) == frozenset({shape_key})

    migrated = current.get_result_record(shape_key, family)
    assert migrated is not None
    assert migrated.result == old_result
    assert migrated.provenance == {"disposition": "legacy_unknown"}
    assert current.get_result_record(shape_key, family) == migrated
    assert current.hard_components(family) == frozenset({shape_key})
    assert current.pending_components(family, profile) == frozenset({shape_key})
    # A same-result write after migration is a no-op and preserves its row.
    assert not current.put_result(
        shape_key, family, old_result,
        provenance={"disposition": "legacy_unknown"},
        work_profile=profile,
    )


def _seen_event() -> dict[str, Any]:
    family = stencil.STENCIL_FAMILY
    shape_key, _frame = stencil._component_shape_key((0, 1), 2, family)
    problem = canonical_problem_from_key(shape_key, family)
    return make_instance_seen_event(
        problem, shape_key, producer_path="foreground",
        event_id=str(uuid.uuid4()),
    )


class _FailingPipeline:
    def __enter__(self) -> "_FailingPipeline":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def xadd(
        self, _name: str, _fields: dict[bytes, bytes], **_kwargs: Any,
    ) -> "_FailingPipeline":
        return self

    def execute(self) -> list[Any]:
        raise redis.ConnectionError("controlled integration XADD failure")


class _FailingXAdd:
    """Inject a pipelined XADD transport failure against live Redis."""

    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    def pipeline(self, *, transaction: bool = True) -> _FailingPipeline:
        assert transaction is True
        return _FailingPipeline()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _OwnershipRouter(BackendRouter):
    """Synthetic KataGo edge that returns a real ownership-shaped final frame."""

    async def start(self) -> None:
        return None

    async def dispatch(
        self,
        canonical_id: CanonicalId,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        await on_response(canonical_id, {
            "id": canonical_id,
            "isDuringSearch": False,
            "turnNumber": 0,
            "moveInfos": [],
            "rootInfo": {"scoreLead": 0.0, "visits": 1},
            "ownership": [0.8, 0.7, -0.8, -0.7],
        })
        await on_complete(canonical_id)

    async def terminate(
        self,
        canonical_id: CanonicalId,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        await on_response(canonical_id, {
            "id": canonical_id,
            "action": "terminate",
            "isDuringSearch": False,
            "turnNumber": 0,
        })
        await on_complete(canonical_id)

    async def stop(self) -> None:
        return None


def _install_capture(
    monkeypatch: pytest.MonkeyPatch,
    client: Any,
    database: Path,
) -> CorpusEventPublisher:
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "1")
    publisher = CorpusEventPublisher(redis_client=client, sqlite_path=database)
    monkeypatch.setattr(corpus_publisher, "_DEFAULT_PUBLISHER", publisher)
    monkeypatch.setitem(
        stencil_cache_storage._PERSISTENT_CLIENTS,
        "solver-component-d4-v4",
        client,
    )
    stencil._invalidate_component_cache()
    return publisher


async def _start_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[proxy_server.ProxyServer, asyncio.Task[None], Any]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = int(listener.getsockname()[1])
    listener.close()

    monkeypatch.setattr(cfg, "ROLE", "ECHO")
    monkeypatch.setattr(cfg, "HOST", "127.0.0.1")
    monkeypatch.setattr(cfg, "PORT", port)
    monkeypatch.setattr(
        proxy_server, "make_router",
        lambda **_kwargs: _OwnershipRouter(),
    )
    server = proxy_server.ProxyServer(
        transformer_factory=lambda link: stencil.stencil_cover_enricher(link),
    )
    task = asyncio.create_task(server.start())
    uri = f"ws://127.0.0.1:{port}"
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if task.done():
            await task
            raise RuntimeError("proxy server exited before accepting a connection")
        try:
            client = await websockets.connect(uri, open_timeout=0.2)
            return server, task, client
        except Exception:
            await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await server.stop()
    raise TimeoutError(f"proxy did not accept WebSocket connections at {uri}")


async def _stop_proxy(
    server: proxy_server.ProxyServer,
    task: asyncio.Task[None],
    websocket: Any,
) -> None:
    try:
        await websocket.close()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await server.stop()


async def _capture_one_query(websocket: Any) -> dict[str, Any]:
    await websocket.send(json.dumps({
        "id": "stencil-capture-integration",
        "action": "analyze",
        "boardXSize": 2,
        "boardYSize": 2,
        "moves": [],
        "capabilities": {
            "stencil_cover": {"truncation_threshold": 0.5},
        },
    }))
    raw = await asyncio.wait_for(websocket.recv(), timeout=5.0)
    response = json.loads(raw)
    if not isinstance(response, dict):
        raise AssertionError("proxy returned a non-object response")
    return response




async def _start_ordinary_service(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[asyncio.Task[None], Any]:
    """Start the real _main entry point with a deterministic synthetic router."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = int(listener.getsockname()[1])
    listener.close()

    monkeypatch.setattr(cfg, "ROLE", "ECHO")
    monkeypatch.setattr(cfg, "HOST", "127.0.0.1")
    monkeypatch.setattr(cfg, "PORT", port)
    monkeypatch.setattr(
        proxy_server, "make_router",
        lambda **_kwargs: _OwnershipRouter(),
    )

    serving = asyncio.Event()
    original_serve = websockets.serve

    def observed_serve(*args: Any, **kwargs: Any) -> Any:
        context = original_serve(*args, **kwargs)

        class _ObservedServe:
            async def __aenter__(self) -> Any:
                result = await context.__aenter__()
                serving.set()
                return result

            async def __aexit__(
                self, exc_type: Any, exc_value: Any, traceback: Any,
            ) -> Any:
                return await context.__aexit__(exc_type, exc_value, traceback)

        return _ObservedServe()

    monkeypatch.setattr(websockets, "serve", observed_serve)
    task = asyncio.create_task(proxy_server._main())
    await asyncio.wait_for(serving.wait(), timeout=10.0)
    websocket = await websockets.connect(
        f"ws://127.0.0.1:{port}", open_timeout=5.0,
    )
    return task, websocket


async def _stop_ordinary_service(
    task: asyncio.Task[None], websocket: Any,
) -> None:
    try:
        await websocket.close()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_ordinary_service_startup_enables_and_supervises_continuous_capture(
    redis_client: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    state_home = tmp_path / "xdg-state"
    database = state_home / "kataproxy" / "stencil-cover-corpus.sqlite3"
    monkeypatch.delenv("KATAPROXY_STENCIL_CORPUS_ENABLED", raising=False)
    monkeypatch.delenv("KATAPROXY_STENCIL_CORPUS_DB", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(state_home))
    monkeypatch.setattr(corpus_publisher, "_DEFAULT_PUBLISHER", None)
    monkeypatch.setitem(
        stencil_cache_storage._PERSISTENT_CLIENTS,
        "solver-component-d4-v4",
        redis_client,
    )
    stencil._invalidate_component_cache()
    assert corpus_publisher.corpus_events_enabled() is True
    assert corpus_publisher.default_corpus_database_path() == database
    assert "KATAPROXY_STENCIL_CORPUS_DB" not in os.environ
    assert "KATAPROXY_STENCIL_CORPUS_ENABLED" not in os.environ

    supervisors: list[StencilCorpusMaterializerSupervisor] = []

    class _TrackingSupervisor(StencilCorpusMaterializerSupervisor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            supervisors.append(self)

    monkeypatch.setattr(
        proxy_server, "StencilCorpusMaterializerSupervisor", _TrackingSupervisor,
    )
    supervisor_errors: list[str] = []
    original_error = stencil_corpus_supervisor.logger.error

    def record_supervisor_error(
        message: str, *args: Any, **kwargs: Any,
    ) -> None:
        supervisor_errors.append(message)
        original_error(message, *args, **kwargs)

    monkeypatch.setattr(
        stencil_corpus_supervisor.logger, "error", record_supervisor_error,
    )

    task, websocket = await _start_ordinary_service(monkeypatch)
    try:
        assert len(supervisors) == 1
        supervisor = supervisors[0]
        assert supervisor.is_running
        assert supervisor.worker_pid is not None
        assert database.exists()

        response = await _capture_one_query(websocket)
        assert "stencilCover" in response
        assert await supervisor.wait_for_materialization(timeout_seconds=10.0)

        store = SQLiteCorpusStore(database)
        foreground_counts = store.counts()
        assert foreground_counts["instances"] >= 1
        assert foreground_counts["spool_pending"] == 0

        # Exercise the real shared solver-runtime foreground and bulk paths.
        # Pair each actual attempt with a sighting in one Redis transaction so
        # the materializer observes the required instance-before-attempt order.
        family = stencil.STENCIL_FAMILY
        shape_key, _frame = stencil._component_shape_key((0, 1), 2, family)
        problem = canonical_problem_from_key(shape_key, family)

        class _CollectingSink:
            def __init__(self) -> None:
                self.events: list[dict[str, Any]] = []

            def publish(self, event: dict[str, Any]) -> str:
                self.events.append(event)
                return "captured"

        corpus_database_publisher = CorpusEventPublisher(
            redis_client=redis_client, sqlite_path=database,
        )
        producer_paths: tuple[Literal["foreground", "bulk"], ...] = (
            "foreground", "bulk",
        )
        for producer_path in producer_paths:
            collecting_sink = _CollectingSink()
            budget = SolverBudget(
                wall_time_limit_ms=10_000,
                native_effort_limit=NoNativeEffortLimit(),
                policy_id=f"{producer_path}-10000ms-integration",
            )
            execution = solve_component(
                create_solver_backend("highs"),
                shape_key,
                family,
                budget,
                producer_path=producer_path,
                event_publisher=collecting_sink,
            )
            assert execution.attempt_id
            assert len(collecting_sink.events) == 1
            sighting = make_instance_seen_event(
                problem, shape_key, producer_path=producer_path,
            )
            assert corpus_database_publisher.publish_many(
                (sighting, *collecting_sink.events),
            ) == "redis_stream"

        def has_both_producer_paths() -> bool:
            with sqlite3.connect(database) as connection:
                seen_paths = {
                    str(row[0]) for row in connection.execute(
                        "SELECT DISTINCT producer_path FROM instance_sighting_paths"
                    ).fetchall()
                }
                attempt_paths = {
                    str(row[0]) for row in connection.execute(
                        "SELECT DISTINCT source_path FROM solver_attempts"
                    ).fetchall()
                }
            return (
                {"foreground", "bulk"} <= seen_paths
                and {"foreground", "bulk"} <= attempt_paths
                and store.counts()["attempts"] >= 2
            )

        deadline = asyncio.get_running_loop().time() + 10.0
        while True:
            supervisor.clear_materialization_event()
            if has_both_producer_paths():
                break
            remaining = deadline - asyncio.get_running_loop().time()
            assert remaining > 0, "materializer did not commit both producer paths"
            assert await supervisor.wait_for_materialization(
                timeout_seconds=remaining,
            )

        foreground_counts = store.counts()
        assert foreground_counts["attempts"] >= 2
        assert foreground_counts["spool_pending"] == 0

        with sqlite3.connect(database) as connection:
            sighting_paths = {
                str(row[0]) for row in connection.execute(
                    "SELECT DISTINCT producer_path FROM instance_sighting_paths"
                ).fetchall()
            }
            attempts = connection.execute(
                "SELECT source_path, started_at_utc, elapsed_wall_ns "
                "FROM solver_attempts ORDER BY started_at_utc"
            ).fetchall()
            identities = connection.execute(
                "SELECT COUNT(*) FROM solver_identities"
            ).fetchone()
        assert {"foreground", "bulk"} <= sighting_paths
        assert {str(row[0]) for row in attempts} >= {"foreground", "bulk"}
        assert all(str(row[1]).endswith("Z") and int(row[2]) >= 0 for row in attempts)
        assert identities is not None and int(identities[0]) >= 1
        assert corpus_database_publisher is not None

        # Unexpected child exit is logged and followed by a fresh worker.
        old_pid = supervisor.worker_pid
        assert old_pid is not None
        supervisor.clear_restart_event()
        os.kill(old_pid, signal.SIGTERM)
        assert await supervisor.wait_for_restart(timeout_seconds=10.0)
        assert supervisor.is_running
        assert supervisor.worker_pid is not None
        assert supervisor.restart_count >= 1
        assert any("exited unexpectedly" in message for message in supervisor_errors)
        assert supervisor.last_exit_code == 0

        # Stop the worker independently, publish while stopped, and restart it.
        await supervisor.stop()
        assert not supervisor.is_running
        assert supervisor.worker_pid is None
        before_restart = store.counts()["receipts"]
        restart_event = make_instance_seen_event(
            problem, shape_key, producer_path="bulk",
            event_id=str(uuid.uuid4()),
        )
        assert corpus_database_publisher.publish(restart_event) == "redis_stream"
        assert store.counts()["receipts"] == before_restart

        await supervisor.start()
        assert supervisor.is_running
        restart_deadline = asyncio.get_running_loop().time() + 10.0
        while store.counts()["receipts"] < before_restart + 1:
            supervisor.clear_materialization_event()
            remaining = restart_deadline - asyncio.get_running_loop().time()
            assert remaining > 0, "restarted materializer did not drain new stream event"
            assert await supervisor.wait_for_materialization(
                timeout_seconds=remaining,
            )
        assert store.counts()["receipts"] == before_restart + 1
    finally:
        await _stop_ordinary_service(task, websocket)

    assert not supervisors[0].is_running
    assert supervisors[0].worker_pid is None


@pytest.mark.asyncio
async def test_ordinary_service_explicit_opt_out_keeps_analysis_available(
    redis_client: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    database = tmp_path / "opt-out.sqlite3"
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_ENABLED", "false")
    monkeypatch.setenv("KATAPROXY_STENCIL_CORPUS_DB", str(database))
    monkeypatch.setattr(corpus_publisher, "_DEFAULT_PUBLISHER", None)

    supervisors: list[StencilCorpusMaterializerSupervisor] = []

    class _TrackingSupervisor(StencilCorpusMaterializerSupervisor):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            supervisors.append(self)

    monkeypatch.setattr(
        proxy_server, "StencilCorpusMaterializerSupervisor", _TrackingSupervisor,
    )
    task, websocket = await _start_ordinary_service(monkeypatch)
    try:
        assert supervisors == []
        response = await _capture_one_query(websocket)
        assert "stencilCover" in response
        assert redis_client.xrange(CORPUS_STREAM_KEY) == []
        assert not database.exists()
    finally:
        await _stop_ordinary_service(task, websocket)


@pytest.mark.asyncio
async def test_real_proxy_capture_backlogs_then_materializes_with_materializer_stopped(
    redis_client: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    database = tmp_path / "capture.sqlite3"
    _install_capture(monkeypatch, redis_client, database)

    server, task, websocket = await _start_proxy(monkeypatch)
    try:
        response = await _capture_one_query(websocket)
        assert "stencilCover" in response
    finally:
        await _stop_proxy(server, task, websocket)

    # No materializer has been constructed or started during proxy capture.
    stream_response = redis_client.xrange(CORPUS_STREAM_KEY)
    assert stream_response is not None
    stream_rows = [
        (message_id, fields)
        for message_id, fields in stream_response
        if message_id is not None and fields is not None
    ]
    assert len(stream_rows) >= 2
    events: list[dict[str, Any]] = []
    for _message_id, fields in stream_rows:
        payload = fields.get(b"event")
        assert isinstance(payload, (bytes, str))
        events.append(parse_event(payload))
    assert any(event["event_type"] == "instance_seen" for event in events)
    assert all(event["producer_path"] == "foreground" for event in events)
    assert all("instance" in event for event in events)

    store = SQLiteCorpusStore(database)
    assert store.counts()["receipts"] == 0
    materializer = StencilCorpusMaterializer(
        store, _stream_port(redis_client), consumer_name="proxy-capture-drain",
    )
    counts = materializer.run_once(block_ms=0)
    assert counts["stream_ingested"] == len(stream_rows)
    assert counts["acked"] == len(stream_rows)
    assert store.counts()["receipts"] == len(stream_rows)
    assert store.counts()["instances"] >= 1
    pending = redis_client.xpending(
        CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP,
    )
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_real_proxy_capture_spools_exact_bytes_on_redis_failure_and_recovers(
    redis_client: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    database = tmp_path / "fallback.sqlite3"
    failing_client = _FailingXAdd(redis_client)
    _install_capture(monkeypatch, failing_client, database)

    server, task, websocket = await _start_proxy(monkeypatch)
    try:
        response = await _capture_one_query(websocket)
        assert "stencilCover" in response
    finally:
        await _stop_proxy(server, task, websocket)

    assert redis_client.xrange(CORPUS_STREAM_KEY) == []
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT event_json, payload_sha256 FROM stencil_corpus_spool_events "
            "ORDER BY queued_at_utc, event_id"
        ).fetchall()
    assert rows
    payloads = [bytes(row[0]) for row in rows]
    for payload, digest in zip(payloads, (str(row[1]) for row in rows)):
        assert hashlib.sha256(payload).hexdigest() == digest
        assert serialize_event(parse_event(payload)) == payload

    store = SQLiteCorpusStore(database)
    materializer = StencilCorpusMaterializer(
        store, _stream_port(redis_client), consumer_name="spool-recovery",
    )
    first = materializer.run_once(block_ms=0)
    assert first["spool_ingested"] == len(payloads)
    assert store.counts()["spool_pending"] == 0
    assert store.counts()["receipts"] == len(payloads)

    # A repeated recovery pass sees no raw rows and cannot duplicate receipts.
    second = materializer.run_once(block_ms=0)
    assert second["spool_ingested"] == 0
    assert store.counts()["receipts"] == len(payloads)
    assert store.counts()["instances"] >= 1


def test_real_redis_stream_backlog_is_consumed_after_materializer_resumes(
    redis_client: redis.Redis,
    tmp_path: Path,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    database = tmp_path / "backlog.sqlite3"
    publisher = CorpusEventPublisher(
        redis_client=redis_client, sqlite_path=database,
    )
    events = (_seen_event(), _seen_event())
    for event in events:
        assert publisher.publish(event) == "redis_stream"

    # The materializer is deliberately absent while the backlog accumulates.
    assert redis_client.xlen(CORPUS_STREAM_KEY) == len(events)
    store = SQLiteCorpusStore(database)
    assert store.counts()["receipts"] == 0

    materializer = StencilCorpusMaterializer(
        store, _stream_port(redis_client), consumer_name="backlog-resumed",
    )
    counts = materializer.run_once(block_ms=0)
    assert counts["stream_ingested"] == len(events)
    assert counts["acked"] == len(events)
    assert store.counts()["receipts"] == len(events)
    assert store.counts()["instances"] == 1
    assert redis_client.xpending(
        CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP,
    )["pending"] == 0


def test_real_process_interruption_after_sqlite_commit_replays_pending_entry(
    redis_client: redis.Redis,
    tmp_path: Path,
) -> None:
    redis_client.delete(CORPUS_STREAM_KEY)
    database = tmp_path / "crash-boundary.sqlite3"
    publisher = CorpusEventPublisher(
        redis_client=redis_client, sqlite_path=database,
    )
    event = _seen_event()
    assert publisher.publish(event) == "redis_stream"
    redis_url = os.environ["KATAPROXY_TEST_REDIS_URL"]

    # The child exits abruptly from the injected boundary after store.ingest
    # has returned (SQLite commit complete) and before the production XACK.
    child_code = (
        "import os, sys, redis; "
        "from transformers.stencil_corpus_store import SQLiteCorpusStore; "
        "from transformers.stencil_corpus_materializer import StencilCorpusMaterializer; "
        "client = redis.Redis.from_url(sys.argv[2], decode_responses=False); "
        "store = SQLiteCorpusStore(sys.argv[1]); "
        "worker = StencilCorpusMaterializer("
        "store, client, consumer_name='crashed-worker', claim_idle_ms=1, "
        "after_sqlite_commit=lambda _message_id: os._exit(73)); "
        "worker.run_once(block_ms=0); "
        "raise SystemExit(74)"
    )
    repository_root = Path(__file__).resolve().parents[1]
    process = subprocess.Popen(
        [sys.executable, "-c", child_code, str(database), redis_url],
        cwd=repository_root,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=10.0)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        pytest.fail(
            "crash-boundary consumer did not reach the injected exit hook; "
            f"stdout={stdout!r}, stderr={stderr!r}"
        )
    assert process.returncode == 73, (
        "consumer did not exit at the post-commit/pre-XACK hook: "
        f"returncode={process.returncode}, stdout={stdout!r}, stderr={stderr!r}"
    )

    # The receipt proves the SQLite transaction committed; the Redis pending
    # entry proves XACK was not reached by the interrupted process.
    store = SQLiteCorpusStore(database)
    assert store.counts()["receipts"] == 1
    assert store.counts()["instances"] == 1
    pending = redis_client.xpending_range(
        CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP, "-", "+", 10,
    )
    assert len(pending) == 1
    deadline = time.monotonic() + 5.0
    while int(pending[0]["time_since_delivered"]) < 1:
        if time.monotonic() >= deadline:
            pytest.fail("pending entry did not become claimable within five seconds")
        time.sleep(0.001)
        pending = redis_client.xpending_range(
            CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP, "-", "+", 10,
        )
        assert pending

    # Restart through the production CLI in a new process. Its normal
    # startup constructs a fresh consumer and recovers through XAUTOCLAIM.
    repository_root = Path(__file__).resolve().parents[1]
    restarted = subprocess.run(
        [
            sys.executable,
            "-m", "transformers.stencil_corpus_materializer",
            "--database", str(database),
            "--once",
            "--block-ms", "0",
            "--claim-idle-ms", "1",
        ],
        cwd=repository_root,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=10.0,
        check=False,
    )
    assert restarted.returncode == 0, (
        "restarted materializer CLI failed: "
        f"stdout={restarted.stdout!r}, stderr={restarted.stderr!r}"
    )
    assert "duplicates': 1" in restarted.stderr
    assert "acked': 1" in restarted.stderr
    assert store.counts()["receipts"] == 1
    assert store.counts()["instances"] == 1
    assert redis_client.xpending(
        CORPUS_STREAM_KEY, CORPUS_CONSUMER_GROUP,
    )["pending"] == 0
