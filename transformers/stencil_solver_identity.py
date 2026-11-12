"""Canonical JSON identity encoding shared by stencil solver and cache layers.

This module owns the stable value-to-bytes mapping used for family and profile
fingerprints. Consumers derive their digest from this implementation; none
reimplements the codec.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any


def canonical_json_value(value: Any) -> Any:
    """Translate supported values into deterministic, finite JSON data."""
    if isinstance(value, bytes):
        import base64
        return {"bytes_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return [canonical_json_value(item) for item in value]
    if isinstance(value, list):
        return [canonical_json_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("persistent cache key dictionaries require string keys")
        return {key: canonical_json_value(value[key]) for key in sorted(value)}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("persistent cache keys cannot contain non-finite floats")
        return value
    raise TypeError(f"unsupported persistent cache key value: {type(value).__name__}")


def stable_digest(value: Any) -> str:
    encoded = json.dumps(
        canonical_json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
