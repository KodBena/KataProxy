"""
proxy_json.py — JSON decoder with a structural depth bound.

Why this exists
---------------
Python's stdlib ``json.loads`` parses recursively without an exposed
depth limit. A crafted payload of N levels of nested objects /
arrays triggers Python's interpreter recursion limit (default 1000)
and raises ``RecursionError`` — which the proxy's existing handlers
catch only as the broad ``except Exception`` at the bottom of the
receive loops, tearing the connection down with a logged stack trace.

A 1 KB JSON message can hold 1000 nested ``[`` characters; that
single line is enough to kill any connection. Audit M-3.

The fix is ``loads_bounded``: a pre-scan over the raw string that
counts structural ``{``/``[``/``}``/``]`` characters (skipping over
string literals so embedded brackets in user data aren't counted),
and refuses payloads exceeding ``max_depth`` before ``json.loads``
runs. Depth budget is configurable via ``PROXY_JSON_MAX_DEPTH``
(default 64); legitimate KataGo JSON is depth ≤ 5 in practice, so
the default has 12× headroom.

Pre-scan cost is O(n) over the input string. For a 4 MB message
that's a few tens of milliseconds in CPython — acceptable for a
defensive measure that runs once per inbound message.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""
from __future__ import annotations

import json
from typing import Any


__all__ = ["loads_bounded", "JsonDepthExceededError"]


class JsonDepthExceededError(ValueError):
    """Raised by :func:`loads_bounded` when the input's structural depth
    exceeds the caller-supplied limit. Distinct from
    ``json.JSONDecodeError`` so callers can refuse depth-bombed payloads
    without conflating them with malformed JSON."""

    def __init__(self, depth: int, limit: int) -> None:
        self.depth = depth
        self.limit = limit
        super().__init__(
            f"JSON nesting depth {depth} exceeds limit {limit}"
        )


def _check_depth(s: str, max_depth: int) -> None:
    """Raise :class:`JsonDepthExceededError` if *s* nests deeper than
    *max_depth* structural levels.

    The scan tracks string literals and escapes so that embedded
    brackets / braces in user content are not counted as structural.
    Numeric literals, ``true``/``false``/``null``, and whitespace are
    ignored (they don't change depth).
    """
    depth = 0
    in_string = False
    escape = False
    for ch in s:
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[" or ch == "{":
            depth += 1
            if depth > max_depth:
                raise JsonDepthExceededError(depth, max_depth)
        elif ch == "]" or ch == "}":
            depth -= 1


def loads_bounded(s: str, *, max_depth: int) -> Any:
    """Decode JSON from *s* with a hard structural depth bound.

    Equivalent to ``json.loads(s)`` for accepted payloads. For payloads
    nesting deeper than *max_depth* structural levels, raises
    :class:`JsonDepthExceededError` before any recursive decode is
    attempted, so Python's interpreter recursion limit is never reached.

    A *max_depth* of 0 or negative disables the bound (the pre-scan is
    skipped) — useful for callers that want the canonical
    ``json.loads`` semantics on an explicit opt-out, e.g. for trusted
    ingress paths.
    """
    if max_depth > 0:
        _check_depth(s, max_depth)
    return json.loads(s)
