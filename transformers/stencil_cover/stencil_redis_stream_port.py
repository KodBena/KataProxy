# SPDX-License-Identifier: MIT
"""Synchronous Redis Streams command boundary.

Redis-py's command methods expose sync/async unions in their shared stubs.
The materializer is deliberately synchronous, so this adapter is the only
boundary that accepts those foreign command results and refuses awaitables.
"""
from __future__ import annotations

from inspect import isawaitable
from types import CoroutineType
from typing import Protocol


class RedisStreamCommands(Protocol):
    """The Redis command surface consumed by the synchronous adapter."""

    def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False,
    ) -> object: ...

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> object: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[
            bytes | str | memoryview[int],
            int | bytes | str | memoryview[int],
        ],
        count: int | None = None,
        block: int | None = None,
    ) -> object | None: ...

    def xack(self, name: str, groupname: str, *ids: str) -> object: ...


class RedisStreamPort(Protocol):
    """Synchronous port used by the materializer after boundary validation."""

    def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False,
    ) -> bool: ...

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> object: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> object | None: ...

    def xack(self, name: str, groupname: str, *ids: str) -> int: ...


def _require_synchronous_result(value: object, command: str) -> object:
    if isawaitable(value):
        # Close a coroutine returned by an async client so refusal does not
        # leave an un-awaited coroutine warning behind.
        if isinstance(value, CoroutineType):
            value.close()
        raise TypeError(
            f"synchronous Redis Streams port received an awaitable for {command}"
        )
    return value


class SynchronousRedisStreamAdapter:
    """Translate redis-py's sync/async command union into the sync-only port."""

    def __init__(self, client: RedisStreamCommands) -> None:
        self._client = client

    def xgroup_create(
        self, name: str, groupname: str, id: str = "$", mkstream: bool = False,
    ) -> bool:
        response = _require_synchronous_result(
            self._client.xgroup_create(name, groupname, id=id, mkstream=mkstream),
            "XGROUP CREATE",
        )
        if not isinstance(response, bool):
            raise TypeError("Redis XGROUP CREATE response must be a boolean")
        return response

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> object:
        response = self._client.xautoclaim(
            name,
            groupname,
            consumername,
            min_idle_time,
            start_id=start_id,
            count=count,
        )
        return _require_synchronous_result(response, "XAUTOCLAIM")

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> object | None:
        redis_streams: dict[
            bytes | str | memoryview[int],
            int | bytes | str | memoryview[int],
        ] = {
            stream_name: stream_id
            for stream_name, stream_id in streams.items()
        }
        response = self._client.xreadgroup(
            groupname,
            consumername,
            redis_streams,
            count=count,
            block=block,
        )
        if response is None:
            return None
        return _require_synchronous_result(response, "XREADGROUP")

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        response = _require_synchronous_result(
            self._client.xack(name, groupname, *ids),
            "XACK",
        )
        if isinstance(response, bool) or not isinstance(response, int) or response < 0:
            raise TypeError("Redis XACK response must be a non-negative integer")
        return response
