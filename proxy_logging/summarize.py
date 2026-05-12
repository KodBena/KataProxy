"""
proxy_logging/summarize.py — Helpers for rendering wire-derived values.

Three families of helper:

  - log_safe(s)       : repr-truncated, newline-escaped string. The
                        audit-H-4 defence against log injection +
                        unbounded growth. Use for ANY value that
                        originated from the wire (client query
                        bodies, peer addresses, KataGo stdout
                        lines, upstream messages) before f-stringing
                        it into a log record.

  - filter_dict(d)    : drops three high-volume KataGo response keys
                        (moveInfos, ownership, policy). Use when
                        emitting a whole-response dict at DEBUG.

  - summarize_query(q): the INFO-level compact summary of a
                        KataGoQuery. Returns a short string like
                        "ANALYZE turns=[0..185] visits=200 model=foo"
                        (or `query_version` / `query_models` /
                        `terminate` for non-analyze queries). Pre-fix
                        the proxy emitted the dataclass __repr__,
                        which dumped the entire opaque dict; this
                        helper is the structural skeleton without
                        the payload.

  - format_query_filtered(q): the DEBUG-level full payload — the
                        opaque dict passed through filter_dict
                        (moves preserved; moveInfos/ownership/
                        policy stripped). The DEBUG complement of
                        summarize_query.

Both `log_safe` and `filter_dict` are carried over from the pre-arc
logging_config.py so existing call sites continue to work via the
shim. summarize_query and format_query_filtered are new.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import os
from typing import Any


_DEFAULT_LOG_TRUNCATE = int(os.environ.get("PROXY_LOG_TRUNCATE", "256"))


def log_safe(s: object, *, max_len: int = _DEFAULT_LOG_TRUNCATE) -> str:
    """Render *s* for inclusion in a log record, defended against log
    injection and unbounded log-line growth.

    The result is the Python ``repr()`` of the (possibly-truncated)
    input. ``repr()`` escapes newlines, carriage returns, and tab
    characters consistently for both ``str`` and ``bytes``, so a
    client that sends ``{"id":"x\\n[FAKE LOG ENTRY]","moves":[]}``
    cannot use that newline to forge log lines once the value flows
    through this helper. The truncation cap (default 256 chars;
    configurable via ``PROXY_LOG_TRUNCATE``) bounds the per-record
    size so a single multi-megabyte message can't blow up the log
    file.

    Use for ANY value that originated from the wire — client query
    bodies, peer addresses, KataGo stdout lines, upstream messages —
    before f-stringing it into a log record.
    """
    truncated: object
    if isinstance(s, (bytes, bytearray)):
        truncated = bytes(s[:max_len]) + (b"..." if len(s) > max_len else b"")
    else:
        text = str(s)
        truncated = text[:max_len] + ("..." if len(text) > max_len else "")
    return repr(truncated)


# Three KataGo response keys are bulky enough that logging them inline
# defeats the readability of every other field. moveInfos can be hundreds
# of move-info dicts; ownership is a board-area-shaped float array; policy
# is a per-cell distribution. Strip them when the goal is "show the shape
# of this response in a log record" rather than "preserve every byte."
_BULKY_KATAGO_RESPONSE_KEYS = frozenset({"moveInfos", "ownership", "policy"})


def filter_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Drop bulky KataGo response keys for log readability.

    The three stripped keys (``moveInfos``, ``ownership``, ``policy``)
    each carry per-move or per-cell payload that drowns out the rest
    of the response shape in a log record. Used by the proxy_server
    and router log paths when emitting whole-response DEBUG records.
    """
    return {k: v for k, v in d.items() if k not in _BULKY_KATAGO_RESPONSE_KEYS}


def _format_turn_range(turns: list[int] | None) -> str:
    """Compact turn-range render: [0..N] when contiguous, else [n,n,…]."""
    if not turns:
        return "[]"
    sorted_turns = sorted(turns)
    if sorted_turns == list(range(sorted_turns[0], sorted_turns[-1] + 1)):
        return f"[{sorted_turns[0]}..{sorted_turns[-1]}]"
    if len(sorted_turns) <= 6:
        return "[" + ",".join(str(t) for t in sorted_turns) + "]"
    return (
        f"[{sorted_turns[0]},{sorted_turns[1]},…,"
        f"{sorted_turns[-2]},{sorted_turns[-1]} ({len(sorted_turns)})]"
    )


def summarize_query(q: Any) -> str:
    """Compact INFO-level summary of a KataGoQuery.

    Returns a short string suitable for the structural skeleton in a
    log record. Examples:

      "ANALYZE turns=[0..185] visits=200 model=really_weak"
      "ANALYZE turn=12 visits=1000"
      "QUERY_VERSION"
      "QUERY_MODELS"
      "TERMINATE → range-abc-…"

    Does not include the ``moves``, ``analysis_config``,
    ``overrideSettings``, or any other payload. The DEBUG-level
    complement is ``format_query_filtered``.

    The argument is typed as ``Any`` so this module doesn't have a
    runtime import dependency on ``katago.KataGoQuery`` (which would
    create a circular import once the rest of the proxy starts using
    proxy_logging). The duck-typed interface is: ``q.action`` (an
    enum-like with ``.name``), ``q.analyze_turns`` (Optional list of
    int), ``q.terminate_id`` (Optional str), ``q.opaque`` (dict-like
    with optional ``maxVisits`` and ``model`` keys).
    """
    action_name = getattr(getattr(q, "action", None), "name", str(q))

    if action_name == "ANALYZE":
        turns = getattr(q, "analyze_turns", None)
        opaque = getattr(q, "opaque", {}) or {}
        max_visits = opaque.get("maxVisits")
        model = opaque.get("model")
        parts = [action_name]
        if turns:
            if len(turns) == 1:
                parts.append(f"turn={turns[0]}")
            else:
                parts.append(f"turns={_format_turn_range(turns)}")
        if max_visits is not None:
            parts.append(f"visits={max_visits}")
        if model is not None:
            parts.append(f"model={model}")
        return " ".join(parts)

    if action_name == "TERMINATE":
        target = getattr(q, "terminate_id", None) or "?"
        # log_safe defends against terminate_id being wire-derived.
        return f"TERMINATE → {log_safe(target, max_len=64)}"

    # All other actions (QUERY_VERSION, QUERY_MODELS, CLEAR_CACHE,
    # TERMINATE_ALL): action name suffices.
    return action_name


def format_query_filtered(q: Any) -> dict[str, Any]:
    """DEBUG-level full payload of a KataGoQuery, with bulky response
    keys stripped from the opaque dict.

    Returns a plain dict that JSON-encodes / repr's compactly. The
    INFO-level complement is ``summarize_query``.

    The opaque dict is shallow-copied through ``filter_dict`` to drop
    moveInfos / ownership / policy. The ``moves`` array is preserved
    (it's part of the structural query identity; an operator
    debugging at DEBUG wants to see which position was queried).
    Override via PROXY_LOG_FULL_PAYLOAD=true is enforced by the
    formatter, not here — this helper always returns the
    filter_dict-ed shape.
    """
    action = getattr(getattr(q, "action", None), "name", str(q))
    opaque = getattr(q, "opaque", {}) or {}
    out: dict[str, Any] = {
        "action": action,
        "opaque": filter_dict(dict(opaque)),
    }
    turns = getattr(q, "analyze_turns", None)
    if turns is not None:
        out["analyze_turns"] = list(turns)
    terminate_id = getattr(q, "terminate_id", None)
    if terminate_id is not None:
        out["terminate_id"] = terminate_id
    return out
