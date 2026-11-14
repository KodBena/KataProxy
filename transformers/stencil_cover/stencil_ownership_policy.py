
# SPDX-License-Identifier: MIT
"""Environment-backed composition of the stencil ownership policy."""

from __future__ import annotations

import sproxy_config as cfg

from .stencil_ownership import OwnershipRegionPolicy
from .stencil_ownership_threshold import ThresholdOwnershipPolicy


def create_ownership_region_policy() -> OwnershipRegionPolicy:
    """Build the configured policy once at transformer composition time."""
    selected = cfg.STENCIL_OWNERSHIP_POLICY
    if selected == "threshold":
        return ThresholdOwnershipPolicy()
    raise ValueError(
        "KATAPROXY_STENCIL_OWNERSHIP_POLICY must be 'threshold'"
    )


DEFAULT_OWNERSHIP_POLICY = create_ownership_region_policy()
