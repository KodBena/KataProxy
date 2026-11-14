# SPDX-License-Identifier: MIT
"""Eager environment-selected composition of stencil ownership policies."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Callable, cast

import sproxy_config as cfg

from .stencil_ownership import OwnershipRegionPolicy
from .stencil_ownership_coherent import CoherentOwnershipPolicy
from .stencil_ownership_cut import CoherentCutOwnershipPolicy
from .stencil_ownership_threshold import ThresholdOwnershipPolicy


_CustomPolicyFactory = Callable[[], OwnershipRegionPolicy]
_CUSTOM_POLICY_FACTORY: _CustomPolicyFactory | None = None

# A third-party policy is imported at this module's import boundary, not on
# first request. Its complete dependency footprint is therefore paid at
# startup and visible to importers, in accordance with ADR-0012 P12.
if cfg.STENCIL_OWNERSHIP_POLICY not in {"coherent", "coherent-cut", "threshold"}:
    _module_name, separator, _factory_name = cfg.STENCIL_OWNERSHIP_POLICY.partition(":")
    if not separator or not _module_name or not _factory_name:
        raise ValueError(
            "custom KATAPROXY_STENCIL_OWNERSHIP_POLICY must be "
            "'python.module:factory_name'"
        )
    _plugin_module = import_module(_module_name)
    _plugin_factory: Any = getattr(_plugin_module, _factory_name, None)
    if not callable(_plugin_factory):
        raise TypeError(
            f"ownership policy plugin {_module_name!r} has no callable "
            f"factory {_factory_name!r}"
        )
    _CUSTOM_POLICY_FACTORY = cast(_CustomPolicyFactory, _plugin_factory)


def _validate_policy(policy: OwnershipRegionPolicy) -> OwnershipRegionPolicy:
    """Refuse an incomplete or dishonest policy implementation at composition."""
    if not isinstance(policy.policy_id, str) or not policy.policy_id.strip():
        raise TypeError("ownership policy policy_id must be a non-empty string")
    if not isinstance(policy.model_version, str) or not policy.model_version.strip():
        raise TypeError("ownership policy model_version must be a non-empty string")
    if not isinstance(policy.requires_ownership_stdev, bool):
        raise TypeError("ownership policy requires_ownership_stdev must be bool")
    if not callable(policy.validate_request_metadata):
        raise TypeError("ownership policy must validate request metadata")
    if not callable(policy.classify):
        raise TypeError("ownership policy must classify ownership evidence")
    return policy


def create_ownership_region_policy() -> OwnershipRegionPolicy:
    """Construct the environment-selected policy at the composition boundary."""
    selected = cfg.STENCIL_OWNERSHIP_POLICY
    if selected == "threshold":
        return ThresholdOwnershipPolicy()
    if selected == "coherent":
        return CoherentOwnershipPolicy(
            kappa=cfg.STENCIL_OWNERSHIP_CONFIDENCE_KAPPA,
            beta=cfg.STENCIL_OWNERSHIP_SPATIAL_BETA,
            score_tolerance=cfg.STENCIL_OWNERSHIP_SCORE_TOLERANCE,
            time_limit_ms=cfg.STENCIL_OWNERSHIP_SOLVER_TIME_LIMIT_MS,
        )
    if selected == "coherent-cut":
        return CoherentCutOwnershipPolicy(
            kappa=cfg.STENCIL_OWNERSHIP_CONFIDENCE_KAPPA,
            beta=cfg.STENCIL_OWNERSHIP_SPATIAL_BETA,
            score_tolerance=cfg.STENCIL_OWNERSHIP_SCORE_TOLERANCE,
            time_limit_ms=cfg.STENCIL_OWNERSHIP_SOLVER_TIME_LIMIT_MS,
        )
    if _CUSTOM_POLICY_FACTORY is None:
        raise ValueError(
            "custom ownership policy was not loaded at startup; restart the proxy "
            "after setting KATAPROXY_STENCIL_OWNERSHIP_POLICY"
        )
    return _validate_policy(_CUSTOM_POLICY_FACTORY())


DEFAULT_OWNERSHIP_POLICY = create_ownership_region_policy()
