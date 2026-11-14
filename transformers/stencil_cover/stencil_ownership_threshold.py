# SPDX-License-Identifier: MIT
"""Legacy per-point threshold ownership selection."""

from __future__ import annotations

import math
from typing import Any, Mapping

from .stencil_ownership import OwnershipEvidence, OwnershipRegions


def _threshold_from_metadata(metadata: Any) -> float:
    if not isinstance(metadata, dict):
        raise ValueError("stencil_cover capability metadata must be an object")
    value = metadata.get("truncation_threshold")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("truncation_threshold must be a finite number in [0, 1]")
    threshold = float(value)
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
        raise ValueError("truncation_threshold must be a finite number in [0, 1]")
    return threshold


class ThresholdOwnershipPolicy:
    """The original hard cutoff, retained as an explicitly selectable policy."""

    policy_id = "threshold"
    requires_ownership_stdev = False

    def validate_request_metadata(self, metadata: Mapping[str, Any]) -> None:
        _threshold_from_metadata(metadata)

    def classify(self, evidence: OwnershipEvidence) -> OwnershipRegions:
        threshold = _threshold_from_metadata(evidence.policy_metadata)
        positive = set(evidence.dead_black)
        negative = set(evidence.dead_white)

        for point in evidence.empty_points:
            value = evidence.ownership[point]
            if threshold == 0.0:
                if value > 0.0:
                    positive.add(point)
                elif value < 0.0:
                    negative.add(point)
            elif value >= threshold:
                positive.add(point)
            elif value <= -threshold:
                negative.add(point)

        return OwnershipRegions(
            positive=tuple(sorted(positive)),
            negative=tuple(sorted(negative)),
            policy_id=self.policy_id,
            threshold=threshold,
            diagnostics={
                "emptyPointCount": len(evidence.empty_points),
                "selectedEmptyPointCount": sum(
                    point in positive or point in negative
                    for point in evidence.empty_points
                ),
            },
        )
