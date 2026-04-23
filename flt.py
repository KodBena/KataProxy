from __future__ import annotations
from typing import Any

def filter_dict(d: dict[str, Any]) -> dict[str, Any]:
    return { k:d[k] for k in d if k not in [ 'moveInfos','ownership','policy'] }
