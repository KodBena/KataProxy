"""
proxy_logging/adapter.py — The ProxyLogger adapter.

Wraps a stdlib ``logging.Logger`` and exposes a structured-fields
API. Two responsibilities:

  - Maintain the bind-chain context. Each call to ``.bind(**fields)``
    returns a NEW ProxyLogger (immutable in spirit) whose context is
    the merge of the parent's context and the supplied fields.
    Session-scoped and upstream-scoped loggers are constructed via
    bind chains in ClientSession / router constructors; the call
    sites never re-supply the bound fields.

  - Validate at the call site. ``log(event, **fields)`` checks that
    the event is a member of the closed Event enum, and that every
    field in ``EVENT_REQUIRED_FIELDS[event]`` is present in the
    merged context (bound + call-site). Violations raise
    LogContractError at the call site, never silently emit a
    malformed record. Per ADR-0002.

The wrapped stdlib Logger is the level-filtering / handler-routing
substrate; the formatter (one of the three in formatters.py) reads
the structured fields off the LogRecord's __dict__ and renders them
in the chosen output shape.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

from proxy_logging.enums import LogContractError
from proxy_logging.events import EVENT_REQUIRED_FIELDS, Event


# Reserved attribute names on stdlib LogRecord. We must not let our
# structured fields collide with these — stdlib's Logger._log()
# sets `extra` keys directly onto record.__dict__, and clashing
# would either silently overwrite the stdlib field (breaking
# formatter behaviour) or raise KeyError on extra-merge.
#
# Source: cpython/Lib/logging/__init__.py LogRecord.__init__ +
# Logger._log. The set is closed across Python 3.10–3.13.
_LOGRECORD_RESERVED: frozenset[str] = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname",
    "filename", "module", "exc_info", "exc_text", "stack_info",
    "lineno", "funcName", "created", "msecs", "relativeCreated",
    "thread", "threadName", "processName", "process", "message",
    "asctime", "taskName",  # 3.12+
})


class ProxyLogger:
    """Structured-logging adapter; the public surface for log emission.

    Construct via ``get_proxy_logger(name)``; refine via ``.bind()``.
    Emit via ``.info(event, **fields)`` / ``.debug(...)`` / etc.

    Adapters are immutable: ``.bind()`` returns a NEW adapter rather
    than mutating self. The bound-field dict is shared by reference
    across descendants where possible (no per-bind allocation cost
    for the parent's fields).
    """

    __slots__ = ("_logger", "_bound")

    def __init__(
        self,
        logger: logging.Logger,
        bound: Mapping[str, Any] | None = None,
    ) -> None:
        self._logger = logger
        # Stored as a regular dict (not frozen) for cheap merging.
        # Treat as immutable by convention; bind() always creates a
        # new instance, never mutates self._bound.
        self._bound: dict[str, Any] = dict(bound) if bound else {}

    # ------------------------------------------------------------------
    # Context construction
    # ------------------------------------------------------------------

    def bind(self, **fields: Any) -> "ProxyLogger":
        """Return a new adapter with the given fields baked into the
        bind chain. The original adapter is unchanged.

        Forbidden field names (would collide with LogRecord reserved
        attributes) raise LogContractError immediately rather than
        silently breaking the formatter at emission time.
        """
        for k in fields:
            if k in _LOGRECORD_RESERVED:
                raise LogContractError(
                    f"bind() refused field {k!r}: collides with stdlib "
                    f"LogRecord reserved attribute"
                )
        merged = dict(self._bound)
        merged.update(fields)
        return ProxyLogger(self._logger, merged)

    # ------------------------------------------------------------------
    # Level gating
    # ------------------------------------------------------------------

    def is_enabled_for(self, level: int) -> bool:
        """Hot-path level check. Use to gate expensive field
        computation:

            if logger.is_enabled_for(logging.DEBUG):
                logger.debug(Event.RESPOND, …, extra=expensive_dict())
        """
        return self._logger.isEnabledFor(level)

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

    def log(
        self,
        level: int,
        event: Event | str,
        *,
        msg: str | Callable[[], str] | None = None,
        exc_info: bool = False,
        **fields: Any,
    ) -> None:
        """Emit a structured log record at the given level.

        ``event`` is an Event enum member; a bare string equal to
        an Event value is also accepted (the string is normalised to
        the enum). Anything else raises LogContractError.

        ``msg`` is the human-readable summary for the console
        renderer. Accepts either a string or a zero-argument
        callable; callables are invoked only if the level is
        enabled (lazy formatting).

        ``exc_info`` mirrors the stdlib Logger flag: when True, the
        current exception's traceback is captured and rendered.
        Use via ``.exception(event, msg=...)`` (a convenience that
        passes exc_info=True at ERROR level).

        ``**fields`` are the structured fields for this event.
        Merged with the bind chain; the merge result must include
        every key in EVENT_REQUIRED_FIELDS[event] or LogContractError
        raises.
        """
        # Cheap level check first — skip everything else when filtered.
        if not self._logger.isEnabledFor(level):
            return

        normalised_event = self._normalise_event(event)
        merged = self._merge_fields(fields)
        self._validate(normalised_event, merged)

        # Resolve the human-readable message.
        if callable(msg):
            rendered_msg = msg()
        elif msg is None:
            rendered_msg = normalised_event.value
        else:
            rendered_msg = msg

        # Stdlib Logger.log(level, msg, extra=...) sets the extras as
        # attributes on the LogRecord. Our formatters read them by
        # name. The `event` field is set explicitly so formatters
        # can branch on it.
        extra = {"event": normalised_event.value, **merged}

        # Final reserved-name check (covers fields supplied at call
        # time but not present at bind time; the bind-time check
        # only sees the bind kwargs).
        for k in extra:
            if k in _LOGRECORD_RESERVED:
                raise LogContractError(
                    f"log() refused field {k!r}: collides with stdlib "
                    f"LogRecord reserved attribute"
                )

        self._logger.log(level, rendered_msg, extra=extra, exc_info=exc_info)

    def debug(self, event: Event | str, *, msg: Any = None, **fields: Any) -> None:
        """Emit at DEBUG level."""
        self.log(logging.DEBUG, event, msg=msg, **fields)

    def info(self, event: Event | str, *, msg: Any = None, **fields: Any) -> None:
        """Emit at INFO level."""
        self.log(logging.INFO, event, msg=msg, **fields)

    def warning(self, event: Event | str, *, msg: Any = None, **fields: Any) -> None:
        """Emit at WARNING level."""
        self.log(logging.WARNING, event, msg=msg, **fields)

    def error(
        self,
        event: Event | str,
        *,
        msg: Any = None,
        exc_info: bool = False,
        **fields: Any,
    ) -> None:
        """Emit at ERROR level. Pass exc_info=True from inside an
        except: clause to capture the traceback (or use .exception()
        for the standard convenience)."""
        self.log(logging.ERROR, event, msg=msg, exc_info=exc_info, **fields)

    def critical(self, event: Event | str, *, msg: Any = None, **fields: Any) -> None:
        """Emit at CRITICAL level."""
        self.log(logging.CRITICAL, event, msg=msg, **fields)

    def exception(
        self,
        event: Event | str,
        *,
        msg: Any = None,
        **fields: Any,
    ) -> None:
        """Emit at ERROR level with the current exception's traceback
        captured. Mirrors stdlib Logger.exception. Use only inside
        an except: clause."""
        self.log(logging.ERROR, event, msg=msg, exc_info=True, **fields)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_event(event: Event | str) -> Event:
        """Coerce string-valued events to the enum; reject unknowns."""
        if isinstance(event, Event):
            return event
        if isinstance(event, str):
            try:
                return Event(event)
            except ValueError as exc:
                raise LogContractError(
                    f"unknown event {event!r}; not a member of "
                    f"proxy_logging.Event"
                ) from exc
        raise LogContractError(
            f"event must be Event or str (got {type(event).__name__})"
        )

    def _merge_fields(self, call_fields: dict[str, Any]) -> dict[str, Any]:
        """Merge bind-chain context with call-site fields.

        Call-site fields win on conflict; this is the natural
        "innermost wins" semantics callers expect (e.g., a session-
        scoped logger that bound a default action="ANALYZE" can be
        overridden by a call site passing action="QUERY_VERSION").
        """
        if not call_fields:
            return dict(self._bound)
        merged = dict(self._bound)
        merged.update(call_fields)
        return merged

    @staticmethod
    def _validate(event: Event, merged: dict[str, Any]) -> None:
        """Check that every required field is present. ADR-0002 fail-loud."""
        required = EVENT_REQUIRED_FIELDS.get(event, frozenset())
        missing = required - merged.keys()
        if missing:
            raise LogContractError(
                f"event {event.value!r} requires fields {sorted(required)!r}; "
                f"missing {sorted(missing)!r}. "
                f"Provided: {sorted(merged.keys())!r}"
            )


# Process-wide role state. Set once at startup via set_process_role()
# (called from proxy_server._main); read by get_proxy_logger() so
# every module-level logger emits records carrying role= without
# the per-module .bind(role=…) boilerplate. Per-session contexts
# refine further (.bind(session=…)) on top of this.
_PROCESS_ROLE: Any = None


def set_process_role(role: Any) -> None:
    """Set the process-wide role bound onto every get_proxy_logger.

    Called once at startup (proxy_server._main) with the role
    derived from cfg.ROLE. Idempotent at the value level: passing
    the same role twice is harmless; passing a different role
    overrides (so test harnesses can re-bind cleanly between cases).
    Pre-startup callers (modules imported before _main runs) get a
    role-less logger; the bind chain merges role at call-time
    on top of whatever process-role the adapter sees at that
    moment, so late-binding is fine.
    """
    global _PROCESS_ROLE
    _PROCESS_ROLE = role


def get_proxy_logger(name: str) -> ProxyLogger:
    """Module-level factory.

    The returned logger has the process-wide role bound (when
    set_process_role has been called). Call sites refine further
    via .bind() to add session / cid / upstream / label as
    appropriate.

    The underlying stdlib logger uses the standard kataproxy
    namespace ('kataproxy.<module>') so existing log-level
    configuration keeps working unchanged. The structured-fields
    handler is installed by configure_logging_from_env() at startup.
    """
    if not name.startswith("kataproxy"):
        # Match the existing convention: every module logs under
        # 'kataproxy.<module-name>'. The pre-arc logging_config.get_logger
        # used 'kataproxy' verbatim; here we accept any name and
        # prefix it for hierarchy if it isn't already prefixed.
        name = f"kataproxy.{name}"
    base = ProxyLogger(logging.getLogger(name))
    if _PROCESS_ROLE is not None:
        return base.bind(role=_PROCESS_ROLE)
    return base
