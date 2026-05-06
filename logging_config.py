# logging_config.py
"""
logging_config.py — Logging factory and log-safety helpers for KataProxy.

Attempts to use ColoredLogger (private, optional) for enhanced terminal
output. Falls back transparently to the stdlib logger. All other modules
use logging.getLogger() directly and are unaffected by this choice.

Default level (post v1.0.4): INFO. The pre-v1.0.4 default was DEBUG,
which (a) emitted operator-visible records for every wire frame received
and sent — including full client query JSON with potential PII like SGF
player names — and (b) embedded peer-controlled strings into log records
without sanitisation, opening a log-injection surface (a client could
forge log lines by including newlines in the message, audit H-4).
PYTHONLOGLEVEL=DEBUG re-enables verbose logging when needed.

Two log-rendering helpers live here:

  - ``log_safe(s)`` — quoted, length-bounded, newline-escaped repr of an
    untrusted string (defends against log injection + unbounded growth).
    Use for any value that originated from the wire.

  - ``filter_dict(d)`` — strips three high-volume KataGo response keys
    (``moveInfos``, ``ownership``, ``policy``) so log records remain
    readable. Use when emitting an entire response dict at DEBUG.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""
import logging
import os
from typing import Any


_DEFAULT_LOG_TRUNCATE = int(os.environ.get("PROXY_LOG_TRUNCATE", "256"))


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return an appropriately configured logger for the given name.

    If ColoredLogger is available in the environment, it is used for
    enhanced terminal output. Otherwise a standard stdlib logger is
    returned. The interface is identical in both cases.
    """
    try:
        from colored_logger import ColoredLogger  # type: ignore[import]
        return ColoredLogger(
            n_colors=5,
            time_format="%H:%M:%S",
            logger_name=name,
            level=level,
        )
    except ImportError:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        return logger


def log_safe(s: object, *, max_len: int = _DEFAULT_LOG_TRUNCATE) -> str:
    """Render *s* for inclusion in a log record, defended against log
    injection and unbounded log-line growth.

    The result is the Python ``repr()`` of the (possibly-truncated)
    input. ``repr()`` escapes newlines, carriage returns, and tab
    characters consistently for both ``str`` and ``bytes``, so a client
    that sends ``{"id":"x\\n[FAKE LOG ENTRY]","moves":[]}`` cannot use
    that newline to forge log lines once the value flows through this
    helper. The truncation cap (default 256 chars; configurable via
    ``PROXY_LOG_TRUNCATE``) bounds the per-record size so a single
    multi-megabyte message can't blow up the log file.

    Use for ANY value that originated from the wire — client query
    bodies, peer addresses, KataGo stdout lines, upstream RELAY
    messages — before f-stringing them into a log record.
    """
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
    each carry per-move or per-cell payload that drowns out the rest of
    the response shape in a log record. Used by the proxy_server and
    router log paths when emitting whole-response DEBUG records.
    """
    return {k: v for k, v in d.items() if k not in _BULKY_KATAGO_RESPONSE_KEYS}
