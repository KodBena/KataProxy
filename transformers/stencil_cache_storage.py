# SPDX-License-Identifier: MIT
"""Redis-backed result storage for the stencil component solver.

The persistent key is the canonical problem and stencil family, deliberately
not the solver time budget: a later, longer solve must be able to upgrade the
same cached entry. Feasible incumbents are retained, while their canonical
problem keys are also indexed for offline improvement. No solver internals or
checkpoint state are serialized.
"""
from __future__ import annotations

import base64
import functools
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, FrozenSet, Iterator, TypeAlias

import redis

from transformers.stencil_solver_api import (
    NativeEffortLimit,
    NoNativeEffortLimit,
    SolverBackend,
    SolverBudget,
    SolverIdentity,
)
from transformers.stencil_solver_geometry import Stencil
from transformers.stencil_solver_identity import (
    canonical_json_value as _json_key_value,
    stable_digest as _stable_digest,
)

logger = logging.getLogger("kataproxy.stencil_cache_storage")

_INVALIDATORS: Dict[str, list[Callable[[bytes], None]]] = {}
_INVALIDATOR_THREADS: Dict[str, threading.Thread] = {}
_PERSISTENT_CLIENTS: Dict[str, Any] = {}
_INVALIDATOR_LOCK = threading.RLock()
_PROCESS_NONCE = uuid.uuid4().hex.encode("ascii")


def _current_process_token() -> bytes:
    # Include the PID at call time: a process created with fork inherits module
    # globals, but it must not share the parent's Pub/Sub origin identity.
    return str(os.getpid()).encode("ascii") + b"-" + _PROCESS_NONCE


def _invalidation_payload(shape_key: bytes) -> bytes:
    return _current_process_token() + b":" + base64.b64encode(shape_key)


def _decode_invalidation_payload(message: bytes) -> bytes | None:
    """Return the changed key, None for our own publication, or b'' to flush."""
    origin, separator, encoded_shape = message.partition(b":")
    if separator and b"-" in origin:
        process_id, nonce = origin.split(b"-", 1)
        try:
            int(process_id)
            valid_token = len(nonce) == 32 and int(nonce, 16) >= 0
        except ValueError:
            valid_token = False
        if valid_token:
            if origin == _current_process_token():
                return None
            try:
                return base64.b64decode(encoded_shape, validate=True)
            except (ValueError, TypeError):
                return b""
    # Accept legacy raw-key messages during a rolling deployment.
    return message


def _invalidation_channel(namespace: str) -> str:
    return f"kataproxy:stencil-cover:{namespace}:invalidate"


def _dispatch_cache_invalidation(namespace: str, shape_key: bytes = b"") -> None:
    with _INVALIDATOR_LOCK:
        callbacks = tuple(_INVALIDATORS.get(namespace, ()))
    for callback in callbacks:
        try:
            callback(shape_key)
        except Exception:
            logger.exception("Local stencil-cache invalidation callback failed")


def register_cache_invalidator(
    namespace: str, callback: Callable[[bytes], None],
) -> bool:
    """Clear a process-local cache when another process upgrades a Redis row.

    Redis Pub/Sub is consumed by a daemon thread, never by the solver hot path.
    Re-subscribing also clears the local cache, covering upgrades published
    while this process was disconnected from Redis.
    """
    with _INVALIDATOR_LOCK:
        callbacks = _INVALIDATORS.setdefault(namespace, [])
        if callback not in callbacks:
            callbacks.append(callback)
        thread = _INVALIDATOR_THREADS.get(namespace)
        if thread is not None and thread.is_alive():
            return True

        def listen() -> None:
            warned = False
            while True:
                pubsub = None
                try:
                    with _INVALIDATOR_LOCK:
                        client = _PERSISTENT_CLIENTS.get(namespace)
                    if client is None:
                        client = create_redis_client()
                    if not callable(getattr(client, "pubsub", None)):
                        return
                    with _INVALIDATOR_LOCK:
                        _PERSISTENT_CLIENTS[namespace] = client
                    pubsub = client.pubsub(ignore_subscribe_messages=True)
                    pubsub.subscribe(_invalidation_channel(namespace))
                    _dispatch_cache_invalidation(namespace)
                    warned = False
                    while True:
                        message = pubsub.get_message(timeout=1.0)
                        if message is None:
                            continue
                        if message.get("type") == "message":
                            data = message.get("data", b"")
                            if isinstance(data, str):
                                data = data.encode("latin-1")
                            if not isinstance(data, bytes):
                                data = b""
                            changed_shape = _decode_invalidation_payload(data)
                            if changed_shape is not None:
                                _dispatch_cache_invalidation(namespace, changed_shape)
                except Exception as exc:
                    if not warned:
                        logger.warning(
                            "Stencil-cache invalidation subscription disconnected "
                            "for namespace %s; retrying (%s)",
                            namespace, exc,
                        )
                        warned = True
                    time.sleep(2.0)
                finally:
                    if pubsub is not None:
                        try:
                            pubsub.close()
                        except Exception:
                            pass

        thread = threading.Thread(
            target=listen,
            name=f"stencil-cache-invalidator-{namespace}",
            daemon=True,
        )
        _INVALIDATOR_THREADS[namespace] = thread
        thread.start()
        return True


def create_redis_client() -> Any:
    """Connect to the proxy's dedicated Redis database, failing explicitly."""
    client = redis.Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        db=_REDIS_DB,
        socket_connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=_SOCKET_TIMEOUT_SECONDS,
        decode_responses=False,
    )
    client.ping()
    return client

# Production defaults remain loopback DB 7. Explicit environment overrides let
# isolated integration processes use their own Redis endpoint/database without
# ever probing or mutating a developer's configured cache.
_REDIS_HOST = os.environ.get("KATAPROXY_REDIS_HOST", "127.0.0.1")
_REDIS_PORT = int(os.environ.get("KATAPROXY_REDIS_PORT", "6379"))
_REDIS_DB = int(os.environ.get("KATAPROXY_REDIS_DB", "7"))
_CONNECT_TIMEOUT_SECONDS = 0.25
_SOCKET_TIMEOUT_SECONDS = 0.25
_VALUE_SCHEMA_VERSION = 3
_LEGACY_CACHE_NAMESPACE = "solver-component-d4-v3"
_CURRENT_CACHE_NAMESPACE = "solver-component-d4-v4"
_LEGACY_VALUE_SCHEMA_VERSION = 2


class RedisCacheConflict(RuntimeError):
    """Two independently optimal results disagree on the same exact problem."""


def _result_cost(encoded_placements: bytes) -> int:
    if len(encoded_placements) < 2:
        raise ValueError("encoded stencil placements are truncated")
    return int.from_bytes(encoded_placements[:2], "little")


def _result_fields(result: Any) -> tuple[bool, int, bytes, int]:
    if not isinstance(result, tuple) or len(result) != 3:
        raise TypeError("stencil solver cache result must be a 3-tuple")
    optimal, status, encoded_placements = result
    if not isinstance(optimal, bool):
        raise TypeError("stencil solver optimal flag must be bool")
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or status not in (0, 1, 2)
    ):
        raise TypeError("stencil solver status must be 0, 1, or 2")
    if optimal != (status == 0):
        raise TypeError("only status 0 may be marked optimal")
    if not isinstance(encoded_placements, bytes):
        raise TypeError("stencil solver placements must be bytes")
    cost = _result_cost(encoded_placements)
    return optimal, status, encoded_placements, cost


@dataclass(frozen=True)
class SolverCacheCandidate:
    """New solver result plus the invocation provenance that produced it."""

    result: tuple[bool, int, bytes]
    provenance: Dict[str, Any]

    def __iter__(self) -> Iterator[Any]:
        return iter(self.result)


@dataclass(frozen=True)
class StoredSolverResult:
    """Decoded result row, including explicit legacy provenance disposition."""

    result: tuple[bool, int, bytes]
    provenance: Dict[str, Any]
    quality_lower_bound: int | None



@dataclass(frozen=True)
class SolverWorkProfile:
    """Stable identity for one backend/configuration/budget work protocol.

    The best-result cache is intentionally not partitioned by this value.
    Pending and timeout state is.  The document is canonical JSON data and its
    SHA-256 fingerprint is the Redis-safe profile key.
    """

    fingerprint: str
    document: Dict[str, Any]

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", self.fingerprint):
            raise ValueError("work-profile fingerprint must be lowercase SHA-256")
        if not isinstance(self.document, dict):
            raise TypeError("work-profile document must be an object")
        try:
            encoded = json.dumps(
                self.document, sort_keys=True, separators=(",", ":"),
                ensure_ascii=True, allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("work-profile document must be finite JSON") from exc
        if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != self.fingerprint:
            raise ValueError("work-profile fingerprint does not match its document")
        if self.document.get("schema") != "kataproxy.stencil-cover.solver-work-profile.v1":
            raise ValueError("unsupported solver work-profile schema")


def make_solver_work_profile(
    identity: SolverIdentity,
    budget: SolverBudget,
) -> SolverWorkProfile:
    """Build a canonical identity including every independent budget dimension."""
    native = budget.native_effort_limit
    if isinstance(native, NativeEffortLimit):
        native_document: Dict[str, Any] = {
            "disposition": "limited",
            "value": native.value,
            "unit": native.unit,
        }
    elif isinstance(native, NoNativeEffortLimit):
        native_document = {
            "disposition": "not_requested",
            "reason": native.reason.value,
        }
    else:  # Defensive guard for callers bypassing SolverBudget validation.
        raise TypeError("unsupported native effort budget")
    document: Dict[str, Any] = {
        "schema": "kataproxy.stencil-cover.solver-work-profile.v1",
        "solver_identity": {
            "fingerprint": identity.fingerprint,
            "backend_name": identity.backend_name,
            "backend_version": identity.backend_version,
            "adapter_version": identity.adapter_version,
            "configuration_json": identity.configuration_json,
            "configuration_fingerprint": identity.configuration_fingerprint,
        },
        "budget": {
            "wall_time_limit_ms": budget.wall_time_limit_ms,
            "native_effort_limit": native_document,
            "policy_id": budget.policy_id,
        },
    }
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return SolverWorkProfile(
        fingerprint=hashlib.sha256(encoded).hexdigest(),
        document=document,
    )


CacheResult: TypeAlias = tuple[bool, int, bytes]


@dataclass(frozen=True)
class SolverCacheRequest:
    """Complete typed input for one foreground solver/cache invocation.

    The global result identity remains shape plus family. This value also
    carries the selected backend and budget so local LRU and work-profile
    identity derive from one immutable request contract.
    """

    shape_key: bytes
    family: tuple[Stencil, ...]
    backend: SolverBackend
    budget: SolverBudget
    work_profile: SolverWorkProfile = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.shape_key, bytes) or not self.shape_key:
            raise ValueError("solver cache request shape_key must be non-empty bytes")
        if not isinstance(self.family, tuple) or not self.family:
            raise TypeError("solver cache request family must be a non-empty immutable tuple")
        if not isinstance(self.budget, SolverBudget):
            raise TypeError("solver cache request budget must be SolverBudget")
        if not callable(getattr(self.backend, "solve", None)):
            raise TypeError("solver cache request backend must implement solve()")
        profile = make_solver_work_profile(self.backend.identity, self.budget)
        object.__setattr__(self, "work_profile", profile)


SolverCacheFunction: TypeAlias = Callable[[SolverCacheRequest], SolverCacheCandidate]
SolverCacheResultFunction: TypeAlias = Callable[[SolverCacheRequest], CacheResult]

_LEGACY_SOLVER_WORK_PROFILE_DOCUMENT: Dict[str, Any] = {
    "schema": "kataproxy.stencil-cover.solver-work-profile.v1",
    "solver_identity": {"disposition": "legacy_unknown"},
    "budget": {"disposition": "legacy_unscoped"},
}
_LEGACY_SOLVER_WORK_PROFILE_JSON = json.dumps(
    _LEGACY_SOLVER_WORK_PROFILE_DOCUMENT,
    sort_keys=True, separators=(",", ":"), ensure_ascii=True,
)
LEGACY_SOLVER_WORK_PROFILE = SolverWorkProfile(
    fingerprint=hashlib.sha256(
        _LEGACY_SOLVER_WORK_PROFILE_JSON.encode("utf-8")
    ).hexdigest(),
    document=_LEGACY_SOLVER_WORK_PROFILE_DOCUMENT,
)


def _validate_result_provenance(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Redis result provenance must be an object")
    disposition = value.get("disposition")
    if disposition == "legacy_unknown":
        if set(value) != {"disposition"}:
            raise ValueError("legacy_unknown provenance must not invent attempt fields")
    elif disposition == "present":
        required = {
            "disposition", "attempt_id", "solver_identity", "producer_path",
            "budget", "started_at_utc", "elapsed_wall_ns", "outcome",
        }
        if set(value) != required:
            raise ValueError("result provenance has an unsupported field set")
        try:
            parsed_id = str(uuid.UUID(value["attempt_id"]))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("result provenance attempt_id must be a canonical UUID") from exc
        if parsed_id != value["attempt_id"]:
            raise ValueError("result provenance attempt_id must be lowercase canonical UUID")
        identity = value["solver_identity"]
        if not isinstance(identity, dict):
            raise ValueError("result provenance solver_identity must be an object")
        expected_identity = {
            "fingerprint", "backend_name", "backend_version", "adapter_version",
            "configuration_json", "configuration_fingerprint",
        }
        if set(identity) != expected_identity:
            raise ValueError("result provenance solver identity is malformed")
        for key in expected_identity:
            if not isinstance(identity[key], str) or not identity[key]:
                raise ValueError("result provenance solver identity fields must be non-empty")
        if not re.fullmatch(r"[0-9a-f]{64}", identity["fingerprint"]):
            raise ValueError("result provenance solver fingerprint must be lowercase SHA-256")
        try:
            configuration = json.loads(identity["configuration_json"])
            canonical_configuration = json.dumps(
                configuration, sort_keys=True, separators=(",", ":"),
                ensure_ascii=True, allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("result provenance configuration must be finite JSON") from exc
        if not isinstance(configuration, dict) or (
            canonical_configuration != identity["configuration_json"]
        ):
            raise ValueError("result provenance configuration must be canonical JSON")
        expected_configuration_fingerprint = hashlib.sha256(
            canonical_configuration.encode("utf-8")
        ).hexdigest()
        if identity["configuration_fingerprint"] != expected_configuration_fingerprint:
            raise ValueError("result provenance configuration fingerprint is inconsistent")
        identity_fields = {
            "adapter_version": identity["adapter_version"],
            "backend_name": identity["backend_name"],
            "backend_version": identity["backend_version"],
            "configuration_fingerprint": identity["configuration_fingerprint"],
        }
        expected_solver_fingerprint = hashlib.sha256(
            json.dumps(
                identity_fields, sort_keys=True, separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        if identity["fingerprint"] != expected_solver_fingerprint:
            raise ValueError("result provenance solver fingerprint is inconsistent")
        if value["producer_path"] not in ("foreground", "bulk"):
            raise ValueError("result provenance producer_path is unsupported")
        budget = value["budget"]
        if not isinstance(budget, dict) or set(budget) != {
            "wall_time_limit_ms", "policy_id", "native_effort_limit",
        }:
            raise ValueError("result provenance budget is malformed")
        if (
            isinstance(budget["wall_time_limit_ms"], bool)
            or not isinstance(budget["wall_time_limit_ms"], int)
            or budget["wall_time_limit_ms"] <= 0
            or not isinstance(budget["policy_id"], str)
            or not budget["policy_id"]
            or not isinstance(budget["native_effort_limit"], dict)
        ):
            raise ValueError("result provenance budget fields are malformed")
        started_at = value["started_at_utc"]
        if not isinstance(started_at, str) or not started_at.endswith("Z"):
            raise ValueError("result provenance started_at_utc must be UTC")
        try:
            datetime.fromisoformat(started_at[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError("result provenance started_at_utc is malformed") from exc
        elapsed = value["elapsed_wall_ns"]
        if isinstance(elapsed, bool) or not isinstance(elapsed, int) or elapsed < 0:
            raise ValueError("result provenance elapsed_wall_ns is malformed")
        if not isinstance(value["outcome"], dict):
            raise ValueError("result provenance outcome must be an object")
        try:
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("result provenance must be finite JSON data") from exc
    else:
        raise ValueError("Redis result provenance disposition is unsupported")
    return value


def _quality_lower_bound(provenance: Dict[str, Any]) -> int | None:
    if provenance.get("disposition") != "present":
        return None
    lower = provenance["outcome"].get("lower_bound")
    if isinstance(lower, dict) and lower.get("disposition") == "reported":
        value = lower.get("value")
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
    return None


def _encode_solver_result(
    result: Any,
    provenance: Dict[str, Any] | None = None,
) -> bytes:
    optimal, status, encoded_placements, cost = _result_fields(result)
    provenance_value = _validate_result_provenance(
        {"disposition": "legacy_unknown"} if provenance is None else provenance
    )
    quality_bound = _quality_lower_bound(provenance_value)
    if quality_bound is not None and quality_bound > cost:
        raise ValueError("result provenance lower bound exceeds incumbent tile count")
    payload = {
        "version": _VALUE_SCHEMA_VERSION,
        "optimal": optimal,
        "status": status,
        "tile_count": cost,
        "placements": base64.b64encode(encoded_placements).decode("ascii"),
        "quality_lower_bound": quality_bound,
        "provenance": provenance_value,
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _decode_solver_result_record(raw: Any) -> StoredSolverResult:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8")
    elif isinstance(raw, str):
        text = raw
    else:
        raise ValueError("Redis cache value is not bytes or text")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("Redis cache value must be an object")
    version = payload.get("version")
    if version not in (_LEGACY_VALUE_SCHEMA_VERSION, _VALUE_SCHEMA_VERSION):
        raise ValueError("Redis cache value has an unsupported schema")

    optimal = payload.get("optimal")
    status = payload.get("status")
    tile_count = payload.get("tile_count")
    placements = payload.get("placements")
    if not isinstance(optimal, bool):
        raise ValueError("Redis cache optimal flag is malformed")
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or status not in (0, 1, 2)
    ):
        raise ValueError("Redis cache solver status is malformed")
    if optimal != (status == 0):
        raise ValueError("Redis cache optimal flag conflicts with its status")
    if (
        isinstance(tile_count, bool)
        or not isinstance(tile_count, int)
        or tile_count < 0
    ):
        raise ValueError("Redis cache tile count is malformed")
    if not isinstance(placements, str):
        raise ValueError("Redis cache placements field is malformed")
    try:
        encoded_placements = base64.b64decode(placements, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("Redis cache placements are not valid base64") from exc
    if _result_cost(encoded_placements) != tile_count:
        raise ValueError("Redis cache tile count does not match its placement list")

    if version == _LEGACY_VALUE_SCHEMA_VERSION:
        provenance = {"disposition": "legacy_unknown"}
        quality_bound = None
    else:
        provenance = _validate_result_provenance(payload.get("provenance"))
        quality_bound = payload.get("quality_lower_bound")
        if quality_bound is not None and (
            isinstance(quality_bound, bool)
            or not isinstance(quality_bound, int)
            or quality_bound < 0
            or quality_bound > tile_count
        ):
            raise ValueError("Redis cache quality lower bound is malformed")
        if (
            provenance["disposition"] == "present"
            and status == 0
            and quality_bound != tile_count
        ):
            raise ValueError("proven optimal result provenance requires a matching lower bound")

    return StoredSolverResult(
        result=(optimal, status, encoded_placements),
        provenance=provenance,
        quality_lower_bound=quality_bound,
    )


def _decode_solver_result(raw: Any) -> tuple[bool, int, bytes]:
    return _decode_solver_result_record(raw).result


def _candidate_fields(
    value: SolverCacheCandidate,
) -> tuple[CacheResult, Dict[str, Any]]:
    provenance = _validate_result_provenance(value.provenance)
    _result_fields(value.result)
    return value.result, provenance


# A single Lua operation stores improvements and atomically maintains the
# hard index, pending queue, and timeout metadata for the best stored row.
# Status order is 0 (optimal), 1 (feasible incumbent), 2 (fallback). Within the
# same non-optimal status, fewer tiles is better.
_STORE_RESULT_SCRIPT = """
-- STENCIL_PROFILE_STORE: global best result, profile-scoped work state.
local old_raw = redis.call('GET', KEYS[1])
local new_status = tonumber(ARGV[2])
local new_cost = tonumber(ARGV[3])
local new_bound = tonumber(ARGV[6])
local should_store = true
local had_valid_old = false

if old_raw then
    local ok, old = pcall(cjson.decode, old_raw)
    local old_status = nil
    local old_cost = nil
    local old_bound = nil
    if ok and type(old) == 'table' and
       old.version == tonumber(ARGV[4]) and
       type(old.optimal) == 'boolean' and
       type(old.placements) == 'string' and
       string.len(old.placements) > 0 then
        old_status = tonumber(old.status)
        old_cost = tonumber(old.tile_count)
        old_bound = tonumber(old.quality_lower_bound)
        if old.optimal ~= (old_status == 0) then
            old_status = nil
            old_cost = nil
            old_bound = nil
        end
    end
    if old_status ~= nil and old_cost ~= nil and
       old_status >= 0 and old_status <= 2 and old_cost >= 0 then
        had_valid_old = true
        if old_status == 0 and new_status == 0 and old_cost ~= new_cost then
            return -1
        end
        if new_status > old_status or
           (new_status == old_status and new_cost > old_cost) then
            should_store = false
        elseif new_status == old_status and new_cost == old_cost then
            if new_bound == nil and old_bound == nil then
                should_store = false
            elseif new_bound == nil and old_bound ~= nil then
                should_store = false
            elseif new_bound ~= nil and old_bound ~= nil and new_bound <= old_bound then
                should_store = false
            end
        end
    end
end

if should_store then
    redis.call('SET', KEYS[1], ARGV[1])
end

local final_raw = redis.call('GET', KEYS[1])
local final = cjson.decode(final_raw)
local profile_ids = redis.call('HKEYS', KEYS[3])
local family_prefix = ARGV[7]

if tonumber(final.status) == 0 then
    redis.call('SREM', KEYS[2], ARGV[5])
    for _, profile_id in ipairs(profile_ids) do
        redis.call('SREM', family_prefix .. ':pending:' .. profile_id, ARGV[5])
        redis.call('HDEL', family_prefix .. ':timeouts:' .. profile_id, ARGV[5])
    end
else
    redis.call('SADD', KEYS[2], ARGV[5])
    if should_store then
        if not had_valid_old then
            -- A newly discovered hard instance is eligible in every profile
            -- that has already registered with this family.
            for _, profile_id in ipairs(profile_ids) do
                redis.call('SADD', family_prefix .. ':pending:' .. profile_id, ARGV[5])
                redis.call('HDEL', family_prefix .. ':timeouts:' .. profile_id, ARGV[5])
            end
        else
            -- A later improvement reopens only the profile that produced it;
            -- unrelated profiles retain their retirement state.
            local profile_id = ARGV[8]
            redis.call('SADD', family_prefix .. ':pending:' .. profile_id, ARGV[5])
            redis.call('HDEL', family_prefix .. ':timeouts:' .. profile_id, ARGV[5])
        end
    end
end

if should_store then
    return 1
end
return 0
"""

_MIGRATE_LEGACY_RESULT_SCRIPT = """
-- STENCIL_PROFILE_MIGRATE
if redis.call('SETNX', KEYS[1], ARGV[1]) == 0 then
    return 0
end
local value = cjson.decode(ARGV[1])
local family_prefix = ARGV[3]
if tonumber(value.status) ~= 0 then
    redis.call('SADD', KEYS[2], ARGV[2])
    for _, profile_id in ipairs(redis.call('HKEYS', KEYS[3])) do
        redis.call('SADD', family_prefix .. ':pending:' .. profile_id, ARGV[2])
        redis.call('HDEL', family_prefix .. ':timeouts:' .. profile_id, ARGV[2])
    end
else
    redis.call('SREM', KEYS[2], ARGV[2])
    for _, profile_id in ipairs(redis.call('HKEYS', KEYS[3])) do
        redis.call('SREM', family_prefix .. ':pending:' .. profile_id, ARGV[2])
        redis.call('HDEL', family_prefix .. ':timeouts:' .. profile_id, ARGV[2])
    end
end
return 1
"""

_MARK_TIMED_OUT_SCRIPT = """
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
    return 0
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then
    return 0
end
local raw = redis.call('GET', KEYS[3])
if not raw then
    return 0
end
local ok, result = pcall(cjson.decode, raw)
local result_status = nil
if ok and type(result) == 'table' then
    result_status = tonumber(result.status)
end
if result_status == nil or result_status < 1 or result_status > 2 then
    redis.call('SREM', KEYS[1], ARGV[1])
    redis.call('SREM', KEYS[2], ARGV[1])
    redis.call('HDEL', KEYS[4], ARGV[1])
    return 0
end
local metadata_ok, metadata = pcall(cjson.decode, ARGV[2])
if not metadata_ok or type(metadata) ~= 'table' then
    return redis.error_reply('invalid stencil timeout metadata')
end
metadata.status = tonumber(result.status)
metadata.tile_count = tonumber(result.tile_count)
redis.call('HSET', KEYS[4], ARGV[1], cjson.encode(metadata))
redis.call('SREM', KEYS[2], ARGV[1])
return 1
"""

_INITIALIZE_PENDING_SCRIPT = """
-- STENCIL_PROFILE_INITIALIZE
-- Copy the old v3 hard index once. Its old timeout metadata is intentionally
-- ignored because it did not identify a solver profile.
if redis.call('SETNX', KEYS[5], '1') == 1 then
    for _, shape_key in ipairs(redis.call('SMEMBERS', KEYS[4])) do
        redis.call('SADD', KEYS[1], shape_key)
    end
end
if redis.call('SETNX', KEYS[3], '1') == 0 then
    return 0
end
local queued = 0
for _, shape_key in ipairs(redis.call('SMEMBERS', KEYS[1])) do
    queued = queued + redis.call('SADD', KEYS[2], shape_key)
end
return queued
"""

class RedisStencilStore:
    """Persist global best results and profile-scoped work scheduling state.

    Hard components are global non-optimal problems. Each registered solver
    work profile has an independent pending set and timeout hash; optimality
    clears all profiles, while non-optimal improvements reopen only the
    profile that produced the improvement.
    """

    def __init__(self, client: Any, namespace: str) -> None:
        if not namespace or not all(
            char.isalnum() or char in "-_." for char in namespace
        ):
            raise ValueError("persistent cache namespace must be a non-empty safe token")
        self._client = client
        self._namespace = namespace
        self._legacy_namespace = (
            _LEGACY_CACHE_NAMESPACE
            if namespace == _CURRENT_CACHE_NAMESPACE
            else None
        )

    @staticmethod
    def _problem_key_for_namespace(
        namespace: str, shape_key: bytes, family: Any,
    ) -> str:
        if not isinstance(shape_key, bytes) or not shape_key:
            raise TypeError("canonical stencil component key must be non-empty bytes")
        digest = _stable_digest({
            "namespace": namespace,
            "shape": shape_key,
            "family": family,
        })
        return f"kataproxy:stencil-cover:{namespace}:result:{digest}"

    def _problem_key(self, shape_key: bytes, family: Any) -> str:
        return self._problem_key_for_namespace(self._namespace, shape_key, family)

    def _family_key(self, kind: str, family: Any) -> str:
        fingerprint = _stable_digest(family)
        return (
            f"kataproxy:stencil-cover:{self._namespace}:"
            f"{kind}:{fingerprint}"
        )

    def _hard_set_key(self, family: Any) -> str:
        return self._family_key("hard", family)

    def _work_family_prefix(self, family: Any) -> str:
        return self._family_key("work", family)

    def _profile_registry_key(self, family: Any) -> str:
        return self._work_family_prefix(family) + ":profiles"

    @staticmethod
    def _coerce_work_profile(
        work_profile: SolverWorkProfile | None,
    ) -> SolverWorkProfile:
        return LEGACY_SOLVER_WORK_PROFILE if work_profile is None else work_profile

    def _profile_pending_set_key(
        self, family: Any, work_profile: SolverWorkProfile | None = None,
    ) -> str:
        profile = self._coerce_work_profile(work_profile)
        return self._work_family_prefix(family) + ":pending:" + profile.fingerprint

    def _profile_timeout_hash_key(
        self, family: Any, work_profile: SolverWorkProfile | None = None,
    ) -> str:
        profile = self._coerce_work_profile(work_profile)
        return self._work_family_prefix(family) + ":timeouts:" + profile.fingerprint

    def _profile_initialized_key(
        self, family: Any, work_profile: SolverWorkProfile | None = None,
    ) -> str:
        profile = self._coerce_work_profile(work_profile)
        return self._work_family_prefix(family) + ":initialized:" + profile.fingerprint

    # Backward-compatible private helpers now resolve to the explicit legacy
    # profile. New production paths pass a SolverWorkProfile.
    def _pending_set_key(self, family: Any) -> str:
        return self._profile_pending_set_key(family)

    def _timeout_hash_key(self, family: Any) -> str:
        return self._profile_timeout_hash_key(family)

    def _pending_initialized_key(self, family: Any) -> str:
        return self._profile_initialized_key(family)

    def _register_work_profile(
        self, family: Any, work_profile: SolverWorkProfile,
    ) -> None:
        registry = self._profile_registry_key(family)
        encoded = json.dumps(
            work_profile.document, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        ).encode("utf-8")
        existing = self._client.hget(registry, work_profile.fingerprint)
        if existing is not None:
            if isinstance(existing, str):
                existing = existing.encode("utf-8")
            if existing != encoded:
                raise ValueError("solver work-profile fingerprint collision in Redis")
            return
        self._client.hset(registry, work_profile.fingerprint, encoded)

    def initialize_pending_queue(
        self, family: Any, work_profile: SolverWorkProfile | None = None,
    ) -> int:
        """Register and seed one profile from the global non-optimal index.

        Every new profile is seeded independently of timeout metadata belonging
        to any other profile. The previous v3 global timeout hash is never used.
        """
        profile = self._coerce_work_profile(work_profile)
        self._register_work_profile(family, profile)
        legacy_hard = (
            f"kataproxy:stencil-cover:{self._legacy_namespace}:"
            f"hard:{_stable_digest(family)}"
            if self._legacy_namespace is not None
            else self._hard_set_key(family)
        )
        return int(self._client.eval(
            _INITIALIZE_PENDING_SCRIPT,
            5,
            self._hard_set_key(family),
            self._profile_pending_set_key(family, profile),
            self._profile_initialized_key(family, profile),
            legacy_hard,
            self._work_family_prefix(family) + ":v3-hard-migrated",
        ))

    def _members(self, redis_key: str) -> FrozenSet[bytes]:
        members = self._client.smembers(redis_key)
        result: set[bytes] = set()
        for member in members:
            if isinstance(member, bytes):
                result.add(member)
            elif isinstance(member, str):
                result.add(member.encode("latin-1"))
            else:
                raise ValueError("Redis stencil index contains a non-byte member")
        return frozenset(result)

    def get_result_record(
        self, shape_key: bytes, family: Any,
    ) -> StoredSolverResult | None:
        current_key = self._problem_key(shape_key, family)
        raw = self._client.get(current_key)
        if raw is None and self._legacy_namespace is not None:
            legacy_key = self._problem_key_for_namespace(
                self._legacy_namespace, shape_key, family,
            )
            legacy_raw = self._client.get(legacy_key)
            if legacy_raw is not None:
                legacy_record = _decode_solver_result_record(legacy_raw)
                # Copy the best result without claiming that the old attempt's
                # solver identity, budget, timing or event provenance is known.
                migrated = _encode_solver_result(
                    legacy_record.result,
                    {"disposition": "legacy_unknown"},
                )
                self._client.eval(
                    _MIGRATE_LEGACY_RESULT_SCRIPT,
                    3,
                    current_key,
                    self._hard_set_key(family),
                    self._profile_registry_key(family),
                    migrated,
                    shape_key,
                    self._work_family_prefix(family),
                )
                raw = self._client.get(current_key)
                if raw is None:
                    raw = migrated
        if raw is None:
            return None
        return _decode_solver_result_record(raw)

    def get_result(
        self, shape_key: bytes, family: Any,
    ) -> tuple[bool, int, bytes] | None:
        record = self.get_result_record(shape_key, family)
        return None if record is None else record.result

    def put_result(
        self,
        shape_key: bytes,
        family: Any,
        result: tuple[bool, int, bytes],
        *,
        provenance: Dict[str, Any] | None = None,
        work_profile: SolverWorkProfile | None = None,
    ) -> bool:
        """Persist the global best result and update only profile work state."""
        _optimal, status, _placements, cost = _result_fields(result)
        provenance_value = _validate_result_provenance(
            {"disposition": "legacy_unknown"} if provenance is None else provenance
        )
        raw = _encode_solver_result(result, provenance_value)
        quality_bound = _quality_lower_bound(provenance_value)
        profile = self._coerce_work_profile(work_profile)
        self._register_work_profile(family, profile)
        outcome = self._client.eval(
            _STORE_RESULT_SCRIPT,
            3,
            self._problem_key(shape_key, family),
            self._hard_set_key(family),
            self._profile_registry_key(family),
            raw,
            str(status),
            str(cost),
            str(_VALUE_SCHEMA_VERSION),
            shape_key,
            "" if quality_bound is None else str(quality_bound),
            self._work_family_prefix(family),
            profile.fingerprint,
        )
        if int(outcome) == -1:
            raise RedisCacheConflict(
                "two optimal stencil covers disagree on the same canonical component"
            )
        updated = bool(outcome)
        if updated:
            try:
                self._client.publish(
                    _invalidation_channel(self._namespace),
                    _invalidation_payload(shape_key),
                )
            except Exception as exc:
                # Persistence is already committed. A missed notification is
                # repaired when a disconnected subscriber reconnects.
                logger.warning("Could not publish stencil-cache invalidation (%s)", exc)
        return updated

    def hard_components(self, family: Any) -> FrozenSet[bytes]:
        """Return every canonical component whose result is not yet optimal."""
        return self._members(self._hard_set_key(family))

    def pending_components(
        self, family: Any, work_profile: SolverWorkProfile | None = None,
    ) -> FrozenSet[bytes]:
        """Return hard components not yet retired for this exact work profile."""
        profile = self._coerce_work_profile(work_profile)
        return self._members(self._profile_pending_set_key(family, profile))

    def timeout_metadata(
        self,
        shape_key: bytes,
        family: Any,
        work_profile: SolverWorkProfile | None = None,
    ) -> Dict[str, Any] | None:
        profile = self._coerce_work_profile(work_profile)
        raw = self._client.hget(
            self._profile_timeout_hash_key(family, profile), shape_key,
        )
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            raise ValueError("Redis timeout metadata is not bytes or text")
        value = json.loads(raw)
        if (
            not isinstance(value, dict)
            or value.get("version") != 2
            or value.get("work_profile_fingerprint") != profile.fingerprint
            or value.get("work_profile") != profile.document
        ):
            raise ValueError("Redis timeout metadata has an unsupported or mismatched profile")
        return value

    def mark_timed_out(
        self,
        shape_key: bytes,
        family: Any,
        result: tuple[bool, int, bytes],
        *,
        time_limit_ms: int,
        attempted_at: float,
        work_profile: SolverWorkProfile | None = None,
    ) -> bool:
        """Retire only the pending queue entry for the profile that ran."""
        optimal, status, _placements, _cost = _result_fields(result)
        if optimal or status == 0:
            raise ValueError("an optimal result cannot be marked as timed out")
        profile = self._coerce_work_profile(work_profile)
        profile_budget = profile.document.get("budget")
        if (
            isinstance(profile_budget, dict)
            and isinstance(profile_budget.get("wall_time_limit_ms"), int)
            and profile_budget["wall_time_limit_ms"] != int(time_limit_ms)
        ):
            raise ValueError("timeout wall-time limit disagrees with the work profile")
        metadata = json.dumps(
            {
                "version": 2,
                "reason": "time_limit_reached_without_proving_optimality",
                "time_limit_ms": int(time_limit_ms),
                "attempted_at": float(attempted_at),
                "work_profile_fingerprint": profile.fingerprint,
                "work_profile": profile.document,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        outcome = self._client.eval(
            _MARK_TIMED_OUT_SCRIPT,
            4,
            self._hard_set_key(family),
            self._profile_pending_set_key(family, profile),
            self._problem_key(shape_key, family),
            self._profile_timeout_hash_key(family, profile),
            shape_key,
            metadata,
        )
        return bool(outcome)



def redis_persistent_cache(
    namespace: str,
) -> Callable[[SolverCacheFunction], SolverCacheResultFunction]:
    """Decorate the typed solver request port with the global Redis best result.

    Redis result identity excludes solver profile and budget; work scheduling
    remains profile-specific in the Redis Lua scripts.
    """
    if not namespace or not all(
        char.isalnum() or char in "-_." for char in namespace
    ):
        raise ValueError("persistent cache namespace must be a non-empty safe token")

    def decorate(function: SolverCacheFunction) -> SolverCacheResultFunction:
        client = None
        try:
            client = create_redis_client()
            with _INVALIDATOR_LOCK:
                _PERSISTENT_CLIENTS[namespace] = client
        except Exception as exc:
            logger.warning(
                "Redis stencil cache unavailable at %s:%d db=%d; "
                "persistent caching disabled temporarily (%s)",
                _REDIS_HOST, _REDIS_PORT, _REDIS_DB, exc,
            )

        store = RedisStencilStore(client, namespace) if client is not None else None
        enabled = True
        state_lock = threading.Lock()

        def disable_after_error(exc: Exception) -> None:
            nonlocal enabled
            with state_lock:
                if enabled:
                    enabled = False
                    logger.warning(
                        "Redis stencil cache became unavailable; "
                        "persistent caching disabled for this process (%s)",
                        exc,
                    )

        @functools.wraps(function)
        def wrapped(request: SolverCacheRequest) -> CacheResult:
            nonlocal store
            if not enabled:
                return _candidate_fields(function(request))[0]

            # The invalidation listener may reconnect after a startup outage.
            # Adopting its already-validated client is the composition handoff.
            if store is None:
                with state_lock:
                    if store is None:
                        with _INVALIDATOR_LOCK:
                            recovered_client = _PERSISTENT_CLIENTS.get(namespace)
                        if recovered_client is not None:
                            store = RedisStencilStore(recovered_client, namespace)
            if store is None:
                return _candidate_fields(function(request))[0]

            try:
                cached_record = store.get_result_record(
                    request.shape_key, request.family,
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                logger.warning(
                    "Ignoring malformed Redis stencil cache entry (%s)", exc,
                )
                cached_record = None
            except Exception as exc:
                disable_after_error(exc)
                return _candidate_fields(function(request))[0]

            if cached_record is not None:
                return cached_record.result

            result, provenance = _candidate_fields(function(request))
            try:
                updated = store.put_result(
                    request.shape_key,
                    request.family,
                    result,
                    provenance=provenance,
                    work_profile=request.work_profile,
                )
                if not updated:
                    latest = store.get_result(request.shape_key, request.family)
                    if latest is not None:
                        return latest
            except RedisCacheConflict:
                raise
            except Exception as exc:
                disable_after_error(exc)
            return result

        return wrapped

    return decorate
