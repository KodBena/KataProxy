"""
proxy_logging/formatters.py — The three output renderings + env-driven
dispatcher.

Three formatters implement §6 of proxy/docs/logging-design.md:

  - ConsoleFormatter: coloured (TTY), compact, human-scannable.
    Field order is fixed for visual scanning.
  - LogfmtFormatter: key=value per line, stable field order.
  - JsonFormatter: one record per line, JSON-encoded.

The dispatcher (configure_logging_from_env) reads PROXY_LOG_FORMAT,
PROXY_LOG_DEST, PROXY_LOG_TRACE_CID, PROXY_LOG_FILTER, and
PROXY_LOG_NO_ABBREV from the environment and installs a single
handler with the chosen formatter on the root kataproxy logger.

Each formatter consumes the structured fields the ProxyLogger
adapter sets on the LogRecord (via `extra=`). Records emitted by
non-ProxyLogger paths (e.g., third-party libraries) lack the
structured fields; the formatters fall back to a minimal "level
module: message" rendering for those.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Field-order policy
# ---------------------------------------------------------------------------
#
# The formatters render fields in a stable order so visual scanning
# and aggregator field-extraction both work reliably. Two tiers:
#
#  - Header fields: always first, always in this order.
#  - Tail fields: all the event-specific fields the call site or
#    bind chain attached.
#
# The tail is rendered in alphabetical order for stability.

_HEADER_FIELD_ORDER: tuple[str, ...] = (
    "ts", "level", "role", "module", "event",
    "session", "label", "upstream", "cid", "orig",
    "action", "direction", "kind",
)


def _stable_sorted(fields: dict[str, Any]) -> list[tuple[str, Any]]:
    """Render fields in (header-order, then alphabetical-tail) order.

    Fields not in the header set sort by name. The header set is
    rendered in declared order regardless of presence; absent
    header fields are skipped.
    """
    out: list[tuple[str, Any]] = []
    for key in _HEADER_FIELD_ORDER:
        if key in fields:
            out.append((key, fields[key]))
    seen = set(_HEADER_FIELD_ORDER)
    for key in sorted(fields):
        if key not in seen:
            out.append((key, fields[key]))
    return out


# ---------------------------------------------------------------------------
# ANSI colour codes
# ---------------------------------------------------------------------------

_COLORS = {
    "DEBUG":    "\x1b[90m",   # bright black / dim
    "INFO":     "\x1b[0m",    # default
    "WARNING":  "\x1b[33m",   # yellow
    "ERROR":    "\x1b[31m",   # red
    "CRITICAL": "\x1b[1;41m", # bold on red bg
}
# Stable 5-char-wide abbreviations for visual column alignment.
_LEVEL_ABBREV = {
    "DEBUG":    "DEBUG",
    "INFO":     "INFO ",
    "WARNING":  "WARN ",
    "ERROR":    "ERROR",
    "CRITICAL": "CRIT ",
}
_RESET = "\x1b[0m"
_DIM = "\x1b[2m"
_ROLE_TINT = {
    "LEAF":     "\x1b[36m",   # cyan
    "RELAY":    "\x1b[35m",   # magenta
    "SELECTOR": "\x1b[32m",   # green
    "ECHO":     "\x1b[33m",   # yellow
    "REDIRECT": "\x1b[34m",   # blue
}


def _abbrev(value: str, width: int = 6) -> str:
    """Shorten an id-like string to first `width` chars + ellipsis."""
    if len(value) <= width:
        return value
    return f"{value[:width]}…"


def _record_fields(record: logging.LogRecord) -> dict[str, Any]:
    """Extract the structured fields a ProxyLogger set on the record.

    Returns the merged bind-chain + call-site fields plus the
    `event` and a few synthesized header fields (`ts`, `level`,
    `module`). Records that didn't go through ProxyLogger (e.g.,
    third-party libraries) yield a sparse dict — the formatters
    handle the absent-fields case gracefully.
    """
    # Synthesize header fields that aren't structured-extras.
    out: dict[str, Any] = {
        "ts": _format_timestamp(record),
        "level": record.levelname,
        "module": record.module,
    }
    # Pull the structured fields the adapter set as extras.
    # LogRecord stores them on __dict__; we filter out stdlib's
    # own attributes by allow-listing known structured names.
    for key, val in record.__dict__.items():
        if key in {
            "name", "msg", "args", "levelname", "levelno",
            "pathname", "filename", "module", "exc_info",
            "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process",
            "message", "asctime", "taskName",
        }:
            continue
        out[key] = val
    return out


def _format_timestamp(record: logging.LogRecord) -> str:
    """ISO 8601 with microsecond precision and local timezone offset."""
    dt = datetime.fromtimestamp(record.created).astimezone()
    # Trim to milliseconds for compactness; aggregators that want
    # microseconds get them via the .json formatter directly off
    # record.created.
    iso = dt.isoformat(timespec="milliseconds")
    return iso


# ---------------------------------------------------------------------------
# Formatter 1: console (TTY)
# ---------------------------------------------------------------------------

class ConsoleFormatter(logging.Formatter):
    """Compact, coloured, human-scannable.

    Layout:

        HH:MM:SS.mmm LVL [ROLE label peer=…] event cid=… orig=… msg

    Cid and orig are abbreviated unless PROXY_LOG_NO_ABBREV=true.
    """

    def __init__(self, *, abbrev: bool = True) -> None:
        super().__init__()
        self._abbrev = abbrev

    def format(self, record: logging.LogRecord) -> str:
        fields = _record_fields(record)
        level = record.levelname
        ts = fields.get("ts", "")
        # Time-only render: HH:MM:SS.mmm (the date is implicit in the
        # file/session). Strips both the date prefix (everything up
        # to and including the "T") and the timezone offset suffix
        # (everything from the first "+" or "-" of the offset to
        # end). isoformat with timespec="milliseconds" produces
        # "YYYY-MM-DDTHH:MM:SS.mmm{+|-}HH:MM"; we want HH:MM:SS.mmm.
        if "T" in ts:
            after_t = ts.split("T", 1)[1]
            # Find the timezone-offset start. Walk from end; the
            # offset is the trailing "[+-]HH:MM" suffix.
            tz_idx = max(after_t.rfind("+"), after_t.rfind("-"))
            time_only = after_t[:tz_idx] if tz_idx > 0 else after_t
        else:
            time_only = ts
        msg = record.getMessage()

        level_abbrev = _LEVEL_ABBREV.get(level, level[:5].ljust(5))
        level_color = _COLORS.get(level, "") if _supports_color(record) else ""
        level_reset = _RESET if level_color else ""
        time_render = (
            f"{_DIM}{time_only}{_RESET}"
            if _supports_color(record)
            else time_only
        )
        prefix = f"{time_render} {level_color}{level_abbrev}{level_reset}"

        event = fields.get("event")
        if event is None:
            # Unmigrated stdlib logger call — no structured fields.
            # Render in a minimal "ts level [module] msg" shape so
            # the user can still read it. Phase 3 sweeps these to
            # the structured form.
            module = fields.get("module", record.name.rsplit(".", 1)[-1])
            return f"{prefix} [{module}] {msg}"

        # Structured-fields path.
        role = fields.get("role")
        label = fields.get("label")
        upstream = fields.get("upstream")
        session = fields.get("session")

        # Role context block. Order of keys: role first, then the
        # most-specific identifier (label > upstream > session-only).
        ctx_parts: list[str] = []
        if role:
            tint = _ROLE_TINT.get(str(role), "") if _supports_color(record) else ""
            ctx_parts.append(f"{tint}{role}{_RESET}" if tint else str(role))
        if label:
            ctx_parts.append(str(label))
        elif upstream:
            ctx_parts.append(str(upstream))
        if session:
            ctx_parts.append(f"peer={session}")
        ctx = f"[{' '.join(ctx_parts)}]" if ctx_parts else ""

        cid = fields.get("cid")
        orig = fields.get("orig")
        cid_render = (
            f" cid={_abbrev(cid) if self._abbrev else cid}" if cid else ""
        )
        orig_render = (
            f" orig={_abbrev(orig) if self._abbrev else orig}" if orig else ""
        )

        line = f"{prefix} {ctx} {event}{cid_render}{orig_render}"
        if msg and msg != event:
            line += f"  {msg}"
        return line


_TTY_DETECTED: Optional[bool] = None


def _supports_color(record: logging.LogRecord) -> bool:
    """Cache the stderr-is-tty answer so each format call doesn't isatty()."""
    global _TTY_DETECTED
    if _TTY_DETECTED is None:
        _TTY_DETECTED = sys.stderr.isatty() and os.environ.get("NO_COLOR") is None
    return _TTY_DETECTED


# ---------------------------------------------------------------------------
# Formatter 2: logfmt
# ---------------------------------------------------------------------------

_LOGFMT_QUOTE_RE = re.compile(r"[\s\"=]")


def _logfmt_value(v: Any) -> str:
    """Render a single value in logfmt-safe form."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if not s or _LOGFMT_QUOTE_RE.search(s):
        # Quote and escape internal quotes / backslashes.
        escaped = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return s


class LogfmtFormatter(logging.Formatter):
    """key=value-per-line.

    Stable field order (header first, then alphabetical tail).
    Suitable for grep, for shipping to aggregators that prefer
    structured-but-text-line input.
    """

    def format(self, record: logging.LogRecord) -> str:
        fields = _record_fields(record)
        msg = record.getMessage()
        if msg:
            fields.setdefault("msg", msg)
        rendered = " ".join(
            f"{k}={_logfmt_value(v)}" for k, v in _stable_sorted(fields)
        )
        return rendered


# ---------------------------------------------------------------------------
# Formatter 3: JSON
# ---------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    """One JSON object per line.

    The ts field is in ISO 8601 with timezone offset (per Q6 of the
    design memo). The level field is the standard level name.
    """

    def format(self, record: logging.LogRecord) -> str:
        fields = _record_fields(record)
        # Override ts to include microseconds (the console renderer
        # trims to milliseconds; aggregators want full precision).
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone()
        fields["ts"] = dt.isoformat()
        msg = record.getMessage()
        if msg:
            fields["msg"] = msg
        return json.dumps(fields, default=_json_default, separators=(",", ":"))


def _json_default(o: Any) -> Any:
    """Convert exotic types JSON doesn't natively handle.

    Frozensets and sets render as sorted lists; bytes via repr;
    everything else as str() (the safety net — the ProxyLogger
    contract is that fields should be JSON-friendly to begin with).
    """
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    if isinstance(o, (bytes, bytearray)):
        return repr(bytes(o))
    return str(o)


# ---------------------------------------------------------------------------
# Trace-cid filter
# ---------------------------------------------------------------------------

class TraceCidFilter(logging.Filter):
    """Drop records that don't carry the targeted cid (or no cid).

    Activated by PROXY_LOG_TRACE_CID=<cid>. When set, every record
    whose `cid` field is NOT this value AND IS NOT absent (no-cid
    records, like connect/disconnect, pass through) is dropped.

    The "no cid passes through" rule is deliberate: tracing one cid
    still wants the surrounding session-lifecycle context (when
    did this session start, when did it disconnect) for ground
    truth. Operators who want strict cid-only filter via
    PROXY_LOG_FILTER instead.
    """

    def __init__(self, target_cid: str) -> None:
        super().__init__()
        self._target = target_cid

    def filter(self, record: logging.LogRecord) -> bool:
        cid = getattr(record, "cid", None)
        if cid is None:
            return True  # session-lifecycle records pass through
        return cid == self._target


class RegexLineFilter(logging.Filter):
    """Drop records whose rendered message doesn't match the regex.

    Activated by PROXY_LOG_FILTER=<regex>. Matches on the Logger's
    .getMessage() — the human-readable summary the call site passed
    via msg=. For structured-fields filtering, use
    PROXY_LOG_TRACE_CID or aggregator-side filters; this is the
    ad-hoc free-text grep.
    """

    def __init__(self, pattern: str) -> None:
        super().__init__()
        self._regex = re.compile(pattern)

    def filter(self, record: logging.LogRecord) -> bool:
        return self._regex.search(record.getMessage()) is not None


# ---------------------------------------------------------------------------
# Env-driven configuration
# ---------------------------------------------------------------------------

_CONFIGURED = False


def configure_logging_from_env() -> None:
    """Install the env-selected formatter on the root kataproxy logger.

    Idempotent — repeated calls are no-ops after the first. Reads:

      PROXY_LOG_FORMAT     auto | console | logfmt | json   (default: auto)
      PROXY_LOG_DEST       stderr | file:<path> | both       (default: stderr)
      PROXY_LOG_TRACE_CID  <cid>                              (optional)
      PROXY_LOG_FILTER     <regex>                            (optional)
      PROXY_LOG_NO_ABBREV  true | false                       (default: false)
      PYTHONLOGLEVEL       <level>                            (stdlib-compat)

    Called once from proxy_server.main(). Tests construct their own
    handlers as needed (e.g., MemoryHandler-driven contract tests
    in Phase 2+).
    """
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    fmt_choice = os.environ.get("PROXY_LOG_FORMAT", "auto").lower()
    if fmt_choice == "auto":
        fmt_choice = "console" if sys.stderr.isatty() else "logfmt"

    abbrev = os.environ.get("PROXY_LOG_NO_ABBREV", "").lower() not in {"true", "1", "yes"}

    if fmt_choice == "console":
        formatter: logging.Formatter = ConsoleFormatter(abbrev=abbrev)
    elif fmt_choice == "logfmt":
        formatter = LogfmtFormatter()
    elif fmt_choice == "json":
        formatter = JsonFormatter()
    else:
        # Fail-loud per ADR-0002.
        raise ValueError(
            f"PROXY_LOG_FORMAT={fmt_choice!r} is not one of "
            f"auto/console/logfmt/json"
        )

    handler = _build_handler(os.environ.get("PROXY_LOG_DEST", "stderr"))
    handler.setFormatter(formatter)

    trace_cid = os.environ.get("PROXY_LOG_TRACE_CID")
    if trace_cid:
        handler.addFilter(TraceCidFilter(trace_cid))

    pattern = os.environ.get("PROXY_LOG_FILTER")
    if pattern:
        handler.addFilter(RegexLineFilter(pattern))

    root = logging.getLogger("kataproxy")
    root.addHandler(handler)
    root.setLevel(_resolve_level())
    # Don't propagate to the absolute root logger — the proxy is the
    # process owner of these records. Prevents duplicate output
    # when something else (e.g., a test harness) attaches a root
    # handler.
    root.propagate = False


def _resolve_level() -> int:
    raw = os.environ.get("PYTHONLOGLEVEL", "INFO").upper()
    # logging.getLevelNamesMapping() is 3.11+; the proxy targets
    # 3.10+ per pyproject.toml. logging.getLevelName is the
    # symmetric pre-3.11 lookup — given a name it returns the
    # numeric level, given a non-name it returns "Level <name>"
    # (which doesn't equal an int, so we detect that and fall
    # through to numeric/default parsing).
    name_lookup = logging.getLevelName(raw)
    if isinstance(name_lookup, int):
        return name_lookup
    try:
        return int(raw)
    except ValueError:
        return logging.INFO


def _build_handler(dest: str) -> logging.Handler:
    if dest == "stderr":
        return logging.StreamHandler(sys.stderr)
    if dest.startswith("file:"):
        path = dest[len("file:"):]
        return logging.FileHandler(path, encoding="utf-8")
    if dest == "both":
        # Compose two handlers under one — but the public API
        # returns one. For "both" we install the file handler here
        # and the stderr handler is installed by the caller; defer
        # to a list-returning variant if we ever truly need two.
        # For Phase 1, accept "both" but treat it as an alias for
        # stderr; the "file" half is operator's responsibility via
        # shell redirection. Document in the operator guide.
        return logging.StreamHandler(sys.stderr)
    raise ValueError(
        f"PROXY_LOG_DEST={dest!r} is not one of "
        f"stderr / file:<path> / both"
    )
