# SPDX-License-Identifier: MIT
"""Optional Redis persistence for the stencil component solver cache.

The process-local LRU remains the first cache layer. This decorator is placed
inside it, so Redis is consulted only after a local cache miss. If Redis is
unavailable during decoration, the function is returned unchanged and the
ordinary LRU continues to work.

Only the solver's small, explicitly encoded result tuple is persisted; no
pickle or executable object deserialisation is used.
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
from typing import Any, Callable, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger("kataproxy.stencil_cache_storage")

_REDIS_HOST = "127.0.0.1"
_REDIS_PORT = 6379
_REDIS_DB = 7
_CONNECT_TIMEOUT_SECONDS = 0.25
_SOCKET_TIMEOUT_SECONDS = 0.25
_VALUE_SCHEMA_VERSION = 1


def _json_key_value(value: Any) -> Any:
    """Convert supported solver arguments to a stable JSON-compatible value."""
    if isinstance(value, bytes):
        return {"bytes_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return [_json_key_value(item) for item in value]
    if isinstance(value, list):
        return [_json_key_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("persistent cache key dictionaries require string keys")
        return {
            key: _json_key_value(value[key])
            for key in sorted(value)
        }
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("persistent cache keys cannot contain non-finite floats")
        return value
    raise TypeError(
        f"unsupported persistent cache key value: {type(value).__name__}"
    )


def _make_cache_key(
    namespace: str,
    function_signature: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> str:
    bound = function_signature.bind(*args, **kwargs)
    bound.apply_defaults()
    material = {
        "namespace": namespace,
        "arguments": {
            name: _json_key_value(value)
            for name, value in bound.arguments.items()
        },
    }
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"kataproxy:stencil-cover:{namespace}:{digest}"


def _encode_solver_result(result: Any) -> bytes:
    if not isinstance(result, tuple) or len(result) != 3:
        raise TypeError("stencil solver cache result must be a 3-tuple")
    optimal, status, encoded_placements = result
    if not isinstance(optimal, bool):
        raise TypeError("stencil solver optimal flag must be bool")
    if isinstance(status, bool) or not isinstance(status, int) or status not in (0, 1, 2):
        raise TypeError("stencil solver status must be 0, 1, or 2")
    if not isinstance(encoded_placements, bytes):
        raise TypeError("stencil solver placements must be bytes")
    payload = {
        "version": _VALUE_SCHEMA_VERSION,
        "optimal": optimal,
        "status": status,
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
    if not isinstance(payload, dict) or payload.get("version") != _VALUE_SCHEMA_VERSION:
        raise ValueError("Redis cache value has an unsupported schema")
    optimal = payload.get("optimal")
    status = payload.get("status")
    placements = payload.get("placements")
    if not isinstance(optimal, bool):
        raise ValueError("Redis cache optimal flag is malformed")
    if isinstance(status, bool) or not isinstance(status, int) or status not in (0, 1, 2):
        raise ValueError("Redis cache solver status is malformed")
    if not isinstance(placements, str):
        raise ValueError("Redis cache placements field is malformed")
    try:
        encoded_placements = base64.b64decode(placements, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("Redis cache placements are not valid base64") from exc
    return optimal, status, encoded_placements


def redis_persistent_cache(namespace: str) -> Callable[[F], F]:
    """Persist solver results in Redis, degrading to a no-op when unavailable.

    The Redis connection is checked once when the decorator is applied (which
    happens at module creation/import time). A failed initial ping emits a
    warning and returns the original function. If the server becomes
    unreachable later, persistence is disabled after one warning while the
    wrapped solver continues to compute normally.
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

        enabled = True
        state_lock = threading.Lock()
        function_signature = signature(function)

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

            key = _make_cache_key(
                namespace, function_signature, args, kwargs,
            )
            try:
                cached = client.get(key)
            except Exception as exc:
                disable_after_error(exc)
                return function(*args, **kwargs)

            if cached is not None:
                try:
                    return _decode_solver_result(cached)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
                    logger.warning(
                        "Ignoring malformed Redis stencil cache entry %s (%s)",
                        key, exc,
                    )

            result = function(*args, **kwargs)
            try:
                client.set(key, _encode_solver_result(result))
            except Exception as exc:
                disable_after_error(exc)
            return result

        return cast(F, wrapped)

    return decorate
