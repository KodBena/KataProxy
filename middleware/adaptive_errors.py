"""Typed error contract shared by adaptive middleware collaborators."""
from __future__ import annotations

from typing import Any


class AdaptiveConfigurationError(RuntimeError):
    """A query's adaptive configuration is internally inconsistent."""

    def __init__(self, *, code: str, detail: dict[str, Any]) -> None:
        super().__init__(f"adaptive_reevaluate: {code} {detail}")
        self.code = code
        self.detail = detail
