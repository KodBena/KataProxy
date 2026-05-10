# logging_config.py
"""
logging_config.py — Backward-compatibility shim.

The structured logging surface lives at ``proxy_logging`` (added in
the v1.0.20 logging arc; see ``proxy/docs/logging-design.md``). This
module is kept as a thin wrapper that re-exports the three public
helpers existing call sites expect — ``get_logger``, ``log_safe``,
``filter_dict`` — so the migration to ``proxy_logging`` can proceed
file-by-file without a flag-day rewrite.

New code: import from ``proxy_logging`` directly:

    from proxy_logging import get_proxy_logger, log_safe, filter_dict, lifecycle, Event

Migration in progress: ``proxy_server.py`` and the router /
middleware / transformer modules continue to import from
``logging_config`` until their per-module sweep in Phase 2 / 3 of
the logging arc.

For the operator-facing schema, event vocabulary, format options,
and env-var matrix, read ``proxy/docs/logging-design.md`` (or
``proxy/docs/logging.md`` once Phase 4 lands).

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import logging

from proxy_logging.summarize import filter_dict, log_safe


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a stdlib Logger configured for the kataproxy hierarchy.

    Backward-compat with the pre-arc API. New code should call
    ``proxy_logging.get_proxy_logger(name)`` instead — that returns
    the structured-fields adapter the new logging contract is built
    around. This wrapper still exists because Phase 2 / 3 of the
    arc are mid-sweep; not every call site has migrated.

    The returned Logger is the underlying stdlib instance the
    ProxyLogger adapter wraps, so a call site mid-migration can
    continue using ``logger.info("…")`` while the same hierarchy's
    new call sites use ``proxy_log.info(Event.…)``.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


__all__ = ["filter_dict", "get_logger", "log_safe"]
