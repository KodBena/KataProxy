"""
proxy_logging/__init__.py — Public API for the proxy's structured logging.

This package implements the design recorded in
proxy/docs/logging-design.md. It is the single entry point for all
proxy log emission. The shape of every log record is governed by
the schema in §3 of the design memo; the event vocabulary by §4;
the per-role coverage contracts by §5.

Public surface (everything else is implementation detail):

  - get_proxy_logger(name: str) → ProxyLogger
        Module-level factory. Returns a ProxyLogger bound to the
        stdlib logger of the given name. The base logger has no
        bound context fields; call sites bind via .bind().

  - ProxyLogger
        The adapter. .bind(**fields) returns a new logger with the
        fields baked in; .info/.debug/.warning/.error/.critical
        emit a record after validating the event and required-
        field contract; .is_enabled_for(level) gates expensive
        formatting.

  - Event, Direction, Role
        The closed enums consumed by the call-site API. Use the
        enum members, not bare strings, when emitting.

  - LogContractError
        Raised at the call site when the event isn't recognised or
        a required field is missing. Never silently emits a
        malformed record.

  - lifecycle
        Convenience helpers for the most common event sequences
        (lifecycle.dispatch, lifecycle.broadcast, lifecycle.respond,
        lifecycle.forward, lifecycle.complete, lifecycle.connect,
        lifecycle.disconnect, lifecycle.subscribe, lifecycle.coalesce,
        lifecycle.terminate_recv, lifecycle.terminate_synthesized,
        lifecycle.keepalive_reset, lifecycle.keepalive_fired,
        lifecycle.upstream_connect, lifecycle.upstream_disconnect).

  - summarize_query, format_query_filtered, log_safe, filter_dict
        Helpers for rendering wire-derived values into log records
        safely. summarize_query produces the INFO-level compact
        summary; format_query_filtered produces the DEBUG-level
        filter_dict-ed full payload; log_safe and filter_dict are
        the audit-H-4 / PII defences carried over from the
        pre-arc logging_config.py.

  - configure_logging_from_env()
        One-call setup. Reads PROXY_LOG_FORMAT / PROXY_LOG_DEST /
        PROXY_LOG_FULL_PAYLOAD / PROXY_LOG_TRACE_CID / etc. and
        installs the chosen formatter and handler on the root
        logger. Idempotent. Called once from proxy_server.main.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

from proxy_logging.adapter import ProxyLogger, get_proxy_logger
from proxy_logging.enums import (
    Direction,
    LogContractError,
    Role,
)
from proxy_logging.events import Event
from proxy_logging.formatters import configure_logging_from_env
from proxy_logging.summarize import (
    filter_dict,
    format_query_filtered,
    log_safe,
    summarize_query,
)
from proxy_logging import lifecycle  # re-export the submodule

__all__ = [
    # Adapter and factory
    "ProxyLogger",
    "get_proxy_logger",
    # Enums
    "Direction",
    "Event",
    "Role",
    # Errors
    "LogContractError",
    # Helpers
    "filter_dict",
    "format_query_filtered",
    "log_safe",
    "summarize_query",
    # Setup
    "configure_logging_from_env",
    # Submodules
    "lifecycle",
]
