"""Dependency-neutral contract for the orchestration middleware seam."""
from __future__ import annotations


class MiddlewareChainConfigurationError(RuntimeError):
    """More than one orchestration middleware was composed into one chain."""


class OrchestrationMiddlewareMarker:
    """Nominal marker consumed by the session middleware chain guard."""
