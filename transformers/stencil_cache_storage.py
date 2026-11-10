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
import importlib
import json
import logging
import math
import threading
import time
import uuid
from inspect import signature
from typing import Any, Callable, Dict, FrozenSet, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger("kataproxy.stencil_cache_storage")

_INVALIDATORS: Dict[str, list[Callable[[bytes], None]]] = {}
_INVALIDATOR_THREADS: Dict[str, threading.Thread] = {}
_PERSISTENT_CLIENTS: Dict[str, Any] = {}
_INVALIDATOR_LOCK = threading.RLock()
_PROCESS_ID = uuid.uuid4().hex.encode("ascii")


def _invalidation_payload(shape_key: bytes) -> bytes:
    return _PROCESS_ID + b":" + base64.b64encode(shape_key)


def _decode_invalidation_payload(message: bytes) -> bytes | None:
    """Return the changed key, None for our own publication, or b'' to flush."""
    origin, separator, encoded_shape = message.partition(b":")
    if separator and len(origin) == 32:
        try:
            int(origin, 16)
        except ValueError:
            pass
        else:
            if origin == _PROCESS_ID:
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
    redis_module = importlib.import_module("redis")
    client = redis_module.Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        db=_REDIS_DB,
        socket_connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=_SOCKET_TIMEOUT_SECONDS,
        decode_responses=False,
    )
    client.ping()
    return client

_REDIS_HOST = "127.0.0.1"
_REDIS_PORT = 6379
_REDIS_DB = 7
_CONNECT_TIMEOUT_SECONDS = 0.25
_SOCKET_TIMEOUT_SECONDS = 0.25
_VALUE_SCHEMA_VERSION = 2


class RedisCacheConflict(RuntimeError):
    """Two independently optimal results disagree on the same exact problem."""


def _json_key_value(value: Any) -> Any:
    """Convert supported solver arguments to stable JSON-compatible values."""
    if isinstance(value, bytes):
        return {"bytes_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return [_json_key_value(item) for item in value]
    if isinstance(value, list):
        return [_json_key_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("persistent cache key dictionaries require string keys")
        return {key: _json_key_value(value[key]) for key in sorted(value)}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("persistent cache keys cannot contain non-finite floats")
        return value
    raise TypeError(f"unsupported persistent cache key value: {type(value).__name__}")


def _stable_digest(value: Any) -> str:
    encoded = json.dumps(
        _json_key_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def _encode_solver_result(result: Any) -> bytes:
    optimal, status, encoded_placements, cost = _result_fields(result)
    payload = {
        "version": _VALUE_SCHEMA_VERSION,
        "optimal": optimal,
        "status": status,
        "tile_count": cost,
        "placements": base64.b64encode(encoded_placements).decode("ascii"),
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _decode_solver_result(raw: Any) -> tuple[bool, int, bytes]:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8")
    elif isinstance(raw, str):
        text = raw
    else:
        raise ValueError("Redis cache value is not bytes or text")
    payload = json.loads(text)
    if (
        not isinstance(payload, dict)
        or payload.get("version") != _VALUE_SCHEMA_VERSION
    ):
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
    return optimal, status, encoded_placements


# A single Lua operation stores improvements and atomically maintains the
# hard index, pending queue, and timeout metadata for the best stored row.
# Status order is 0 (optimal), 1 (feasible incumbent), 2 (fallback). Within the
# same non-optimal status, fewer tiles is better.
_STORE_RESULT_SCRIPT = """
local old_raw = redis.call('GET', KEYS[1])
local new_status = tonumber(ARGV[2])
local new_cost = tonumber(ARGV[3])
local should_store = true

if old_raw then
    local ok, old = pcall(cjson.decode, old_raw)
    local old_status = nil
    local old_cost = nil
    if ok and type(old) == 'table' and
       old.version == tonumber(ARGV[4]) and
       type(old.optimal) == 'boolean' and
       type(old.placements) == 'string' and
       string.len(old.placements) > 0 then
        old_status = tonumber(old.status)
        old_cost = tonumber(old.tile_count)
        if old.optimal ~= (old_status == 0) then
            old_status = nil
            old_cost = nil
        end
    end
    if old_status ~= nil and old_cost ~= nil and
       old_status >= 0 and old_status <= 2 and old_cost >= 0 then
        if old_status == 0 and new_status == 0 and old_cost ~= new_cost then
            return -1
        end
        if new_status > old_status or
           (new_status == old_status and new_cost >= old_cost) then
            should_store = false
        end
    end
end

if should_store then
    redis.call('SET', KEYS[1], ARGV[1])
end

local final_raw = redis.call('GET', KEYS[1])
local final = cjson.decode(final_raw)
if tonumber(final.status) == 0 then
    redis.call('SREM', KEYS[2], ARGV[5])
    redis.call('SREM', KEYS[3], ARGV[5])
    redis.call('HDEL', KEYS[4], ARGV[5])
else
    redis.call('SADD', KEYS[2], ARGV[5])
    if should_store then
        redis.call('SADD', KEYS[3], ARGV[5])
        redis.call('HDEL', KEYS[4], ARGV[5])
    end
end

if should_store then
    return 1
end
return 0
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
if redis.call('SETNX', KEYS[4], '1') == 0 then
    return 0
end
local queued = 0
for _, shape_key in ipairs(redis.call('SMEMBERS', KEYS[1])) do
    if redis.call('HEXISTS', KEYS[3], shape_key) == 0 then
        queued = queued + redis.call('SADD', KEYS[2], shape_key)
    end
end
return queued
"""

class RedisStencilStore:
    """Persist best results and maintain pending and timed-out component indexes.

    Hard components are all non-optimal problems. Pending components are the
    subset not yet given the bulk solver's full time budget. Timeout metadata
    remains durable after a worker attempt so hard components are not retried
    continuously.
    """

    def __init__(self, client: Any, namespace: str) -> None:
        if not namespace or not all(
            char.isalnum() or char in "-_." for char in namespace
        ):
            raise ValueError("persistent cache namespace must be a non-empty safe token")
        self._client = client
        self._namespace = namespace

    def _problem_key(self, shape_key: bytes, family: Any) -> str:
        if not isinstance(shape_key, bytes) or not shape_key:
            raise TypeError("canonical stencil component key must be non-empty bytes")
        digest = _stable_digest({
            "namespace": self._namespace,
            "shape": shape_key,
            "family": family,
        })
        return f"kataproxy:stencil-cover:{self._namespace}:result:{digest}"

    def _family_key(self, kind: str, family: Any) -> str:
        fingerprint = _stable_digest(family)
        return (
            f"kataproxy:stencil-cover:{self._namespace}:"
            f"{kind}:{fingerprint}"
        )

    def _hard_set_key(self, family: Any) -> str:
        return self._family_key("hard", family)

    def _pending_set_key(self, family: Any) -> str:
        return self._family_key("pending", family)

    def _timeout_hash_key(self, family: Any) -> str:
        return self._family_key("timeouts", family)

    def _pending_initialized_key(self, family: Any) -> str:
        return self._family_key("pending-initialized", family)

    def initialize_pending_queue(self, family: Any) -> int:
        """Seed the new queue from pre-existing non-optimal v3 result indexes.

        This one-time atomic migration preserves components cached before the
        pending queue existed. A marker prevents rescanning the hard index on
        every worker poll or after process restarts.
        """
        return int(self._client.eval(
            _INITIALIZE_PENDING_SCRIPT,
            4,
            self._hard_set_key(family),
            self._pending_set_key(family),
            self._timeout_hash_key(family),
            self._pending_initialized_key(family),
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

    def get_result(
        self, shape_key: bytes, family: Any,
    ) -> tuple[bool, int, bytes] | None:
        raw = self._client.get(self._problem_key(shape_key, family))
        if raw is None:
            return None
        return _decode_solver_result(raw)

    def put_result(
        self,
        shape_key: bytes,
        family: Any,
        result: tuple[bool, int, bytes],
    ) -> bool:
        """Persist a result, queueing each newly improved non-optimal result."""
        _optimal, status, _placements, cost = _result_fields(result)
        raw = _encode_solver_result(result)
        outcome = self._client.eval(
            _STORE_RESULT_SCRIPT,
            4,
            self._problem_key(shape_key, family),
            self._hard_set_key(family),
            self._pending_set_key(family),
            self._timeout_hash_key(family),
            raw,
            str(status),
            str(cost),
            str(_VALUE_SCHEMA_VERSION),
            shape_key,
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

    def pending_components(self, family: Any) -> FrozenSet[bytes]:
        """Return non-optimal components not yet processed at the bulk budget."""
        return self._members(self._pending_set_key(family))

    def timeout_metadata(self, shape_key: bytes, family: Any) -> Dict[str, Any] | None:
        raw = self._client.hget(self._timeout_hash_key(family), shape_key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            raise ValueError("Redis timeout metadata is not bytes or text")
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("Redis timeout metadata has an unsupported schema")
        return value

    def mark_timed_out(
        self,
        shape_key: bytes,
        family: Any,
        result: tuple[bool, int, bytes],
        *,
        time_limit_ms: int,
        attempted_at: float,
    ) -> bool:
        """Persist why a hard component was retired from the pending queue."""
        optimal, status, _placements, _cost = _result_fields(result)
        if optimal or status == 0:
            raise ValueError("an optimal result cannot be marked as timed out")
        metadata = json.dumps(
            {
                "version": 1,
                "reason": "time_limit_reached_without_proving_optimality",
                "time_limit_ms": int(time_limit_ms),
                "attempted_at": float(attempted_at),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        outcome = self._client.eval(
            _MARK_TIMED_OUT_SCRIPT,
            4,
            self._hard_set_key(family),
            self._pending_set_key(family),
            self._problem_key(shape_key, family),
            self._timeout_hash_key(family),
            shape_key,
            metadata,
        )
        return bool(outcome)



def redis_persistent_cache(namespace: str) -> Callable[[F], F]:
    """Persist solver results; Redis is optional and failures degrade to a no-op.

    The Redis key deliberately excludes time_limit_ms. A better solution found
    by an offline, longer solve can therefore upgrade the exact same problem
    record. Feasible results are cached and their canonical problem keys are
    added to the hard and pending indexes. No solver checkpoint/state is written.
    """
    if not namespace or not all(
        char.isalnum() or char in "-_." for char in namespace
    ):
        raise ValueError("persistent cache namespace must be a non-empty safe token")

    def decorate(function: F) -> F:
        client = None
        try:
            client = create_redis_client()
            with _INVALIDATOR_LOCK:
                _PERSISTENT_CLIENTS[namespace] = client
        except Exception as exc:
            logger.warning(
                "Redis stencil cache unavailable at %s:%d db=%d; "
                "persistent caching temporarily disabled (%s)",
                _REDIS_HOST, _REDIS_PORT, _REDIS_DB, exc,
            )

        store = RedisStencilStore(client, namespace) if client is not None else None
        function_signature = signature(function)
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
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal store
            if not enabled:
                return function(*args, **kwargs)

            # If Redis was unavailable during module import, the background
            # subscriber may reconnect later. Adopt its client lazily here;
            # this work occurs only on an LRU miss, not on the hot cache hit.
            if store is None:
                with state_lock:
                    if store is None:
                        with _INVALIDATOR_LOCK:
                            recovered_client = _PERSISTENT_CLIENTS.get(namespace)
                        if recovered_client is not None:
                            store = RedisStencilStore(recovered_client, namespace)
            if store is None:
                return function(*args, **kwargs)

            bound = function_signature.bind(*args, **kwargs)
            bound.apply_defaults()
            try:
                shape_key = bound.arguments["shape_key"]
                family = bound.arguments["family"]
            except KeyError as exc:
                raise TypeError(
                    "persistent stencil solver requires shape_key and family arguments"
                ) from exc

            try:
                cached = store.get_result(shape_key, family)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                logger.warning(
                    "Ignoring malformed Redis stencil cache entry (%s)", exc,
                )
                cached = None
            except Exception as exc:
                disable_after_error(exc)
                return function(*args, **kwargs)

            if cached is not None:
                return cached

            result = function(*args, **kwargs)
            try:
                updated = store.put_result(shape_key, family, result)
                if not updated:
                    # A concurrent worker or request may already have stored a
                    # better result. Return that row so the outer LRU cannot
                    # re-cache a stale incumbent after an invalidation race.
                    latest = store.get_result(shape_key, family)
                    if latest is not None:
                        return latest
            except RedisCacheConflict:
                raise
            except Exception as exc:
                disable_after_error(exc)
            return result
    return decorate
