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
from inspect import signature
from typing import Any, Callable, Dict, FrozenSet, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger("kataproxy.stencil_cache_storage")

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


# A single Lua operation stores the new result only if it improves the current
# one, and keeps the hard-instance index consistent with the best stored row.
# Status order is 0 (optimal), 1 (feasible incumbent), 2 (fallback). Within the
# same non-optimal status, fewer tiles is better.
_STORE_RESULT_SCRIPT = """
local old_raw = redis.call('GET', KEYS[1])
local new_status = tonumber(ARGV[2])
local new_cost = tonumber(ARGV[3])
local should_store = true

if old_raw then
    local ok, old = pcall(cjson.decode, old_raw)
    if ok and old.version == tonumber(ARGV[4]) then
        local old_status = tonumber(old.status)
        local old_cost = tonumber(old.tile_count)
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
else
    redis.call('SADD', KEYS[2], ARGV[5])
end

if should_store then
    return 1
end
return 0
"""


class RedisStencilStore:
    """Store best results and enumerate components whose solve was not optimal.

    The hard-instance set is partitioned by namespace and complete stencil
    family. Its members are the canonical component keys themselves, so a
    bulk solver can reconstruct each problem without a second payload or any
    saved HiGHS state.
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

    def _hard_set_key(self, family: Any) -> str:
        fingerprint = _stable_digest(family)
        return (
            f"kataproxy:stencil-cover:{self._namespace}:"
            f"hard:{fingerprint}"
        )

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
        """Persist an incumbent or optimum; never replace a better result."""
        _optimal, status, _placements, cost = _result_fields(result)
        raw = _encode_solver_result(result)
        result_key = self._problem_key(shape_key, family)
        hard_key = self._hard_set_key(family)
        outcome = self._client.eval(
            _STORE_RESULT_SCRIPT,
            2,
            result_key,
            hard_key,
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
        return bool(outcome)

    def hard_components(self, family: Any) -> FrozenSet[bytes]:
        """Return the canonical component keys awaiting a more thorough solve."""
        members = self._client.smembers(self._hard_set_key(family))
        result = set()
        for member in members:
            if isinstance(member, bytes):
                result.add(member)
            elif isinstance(member, str):
                result.add(member.encode("latin-1"))
            else:
                raise ValueError("Redis hard-instance index contains a non-byte member")
        return frozenset(result)


def redis_persistent_cache(namespace: str) -> Callable[[F], F]:
    """Persist solver results; Redis is optional and failures degrade to a no-op.

    The Redis key deliberately excludes time_limit_ms. A better solution found
    by an offline, longer solve can therefore upgrade the exact same problem
    record. Feasible results are cached and their canonical problem keys are
    added to the hard-instance set. No solver checkpoint/state is written.
    """
    if not namespace or not all(
        char.isalnum() or char in "-_." for char in namespace
    ):
        raise ValueError("persistent cache namespace must be a non-empty safe token")

    def decorate(function: F) -> F:
        try:
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
        except Exception as exc:
            logger.warning(
                "Redis stencil cache unavailable at %s:%d db=%d; "
                "persistent caching disabled (%s)",
                _REDIS_HOST, _REDIS_PORT, _REDIS_DB, exc,
            )
            return function

        store = RedisStencilStore(client, namespace)
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
            if not enabled:
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
                store.put_result(shape_key, family, result)
            except RedisCacheConflict:
                raise
            except Exception as exc:
                disable_after_error(exc)
            return result

        return cast(F, wrapped)

    return decorate
