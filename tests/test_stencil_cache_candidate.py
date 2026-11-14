# SPDX-License-Identifier: MIT
from __future__ import annotations

from dataclasses import FrozenInstanceError, is_dataclass

import pytest

from transformers.stencil_cover.stencil_cache_storage import SolverCacheCandidate


def test_solver_cache_candidate_is_a_frozen_dataclass() -> None:
    assert is_dataclass(SolverCacheCandidate)
    candidate = SolverCacheCandidate(
        result=(False, 1, b"\\x00\\x00"),
        provenance={"disposition": "present"},
    )

    with pytest.raises(FrozenInstanceError):
        setattr(candidate, "result", (True, 0, b"\\x00\\x00"))
