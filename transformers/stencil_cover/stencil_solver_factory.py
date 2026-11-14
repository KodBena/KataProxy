"""Explicit solver composition root.

The selected default is HiGHS. Optional backends are registered by importing
their adapter at an application's composition boundary; this module does not
load unselected optional solver packages.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping

from .stencil_solver_api import SolverBackend
from .stencil_solver_highs import HiGHSBackend


class SolverBackendUnavailable(RuntimeError):
    """The requested backend is not registered or cannot be constructed."""


BackendFactory = Callable[[Mapping[str, Any]], SolverBackend]
_BACKEND_FACTORIES: dict[str, BackendFactory] = {
    "highs": lambda configuration: HiGHSBackend(configuration),
}


def register_solver_backend(
    name: str, factory: BackendFactory, *, replace: bool = False,
) -> None:
    """Register a backend factory at the composition boundary."""
    if not name or not name.strip():
        raise ValueError("solver backend name must not be empty")
    if name in _BACKEND_FACTORIES and not replace:
        raise ValueError(f"solver backend {name!r} is already registered")
    _BACKEND_FACTORIES[name] = factory


def available_solver_backends() -> tuple[str, ...]:
    return tuple(sorted(_BACKEND_FACTORIES))


def create_solver_backend(
    name: str = "highs",
    configuration: Mapping[str, Any] | None = None,
) -> SolverBackend:
    """Build and validate the selected backend through its registered factory."""
    factory = _BACKEND_FACTORIES.get(name)
    if factory is None:
        raise SolverBackendUnavailable(
            f"solver backend {name!r} is unavailable; registered backends: "
            f"{', '.join(available_solver_backends())}"
        )
    backend = factory(dict(configuration or {}))
    identity = backend.identity
    if not identity.backend_name:
        raise SolverBackendUnavailable("selected solver backend has no stable identity")
    if not callable(getattr(backend, "solve", None)):
        raise SolverBackendUnavailable("selected solver backend does not implement solve()")
    return backend

# The default foreground adapter is constructed once at the composition boundary.
DEFAULT_SOLVER_BACKEND: SolverBackend = create_solver_backend("highs")
