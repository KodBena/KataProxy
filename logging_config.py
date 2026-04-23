# logging_config.py
"""
logging_config.py — Logging factory for KataProxy.

Attempts to use ColoredLogger (private, optional) for enhanced terminal
output. Falls back transparently to the stdlib logger. All other modules
use logging.getLogger() directly and are unaffected by this choice.
"""
import logging


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
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
