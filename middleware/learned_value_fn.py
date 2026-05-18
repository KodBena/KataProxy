"""middleware/learned_value_fn.py — Phase 3.5 learned value-function
substrate (v1.0.26).

Provides proxy-hosted LightGBM predictors that plug into the
existing Phase 3 substrate's `value_fn` binding via the `learned_*`
namespace convention (per `docs/dispatch/proxy-to-frontend-
learned-vf.md`).

A `LearnedValueFn` exposes:
  - `__call__(turn_view) -> float` returning the r_full prediction
    (V=pre → V=oracle entropy reduction). Implements the standard
    `Callable[[TurnView], float]` protocol so any existing algorithm
    can consume it as a regular value function.
  - `predict_int(turn_view) -> float` returning the r_int prediction
    (V=pre → V=intermediate). Used by the new `LearnedPiecewiseAllocator`
    for segment-based water-fill on the empirically-anchored curve.
  - `prepare(candidates)` called once by the allocator before any
    per-turn predictions; pre-computes range-level features that
    summarize the full candidate set (mean / std / min / max of
    per-turn features). Idempotent; re-prepare is allowed if the
    candidate set changes.

The `LearnedValueFnRegistry` is a singleton loaded at proxy startup.
It scans `proxy/models/learned_value_fn/v{N}/` directories for
bundled models; each version that loads successfully is advertised
in `query_version.capabilities.adaptive_reevaluate.available_value_bindings`.

Graceful degradation: if LightGBM is not installed in the proxy's
Python environment, the registry is empty and `available_value_bindings`
is omitted from the advertisement. Substrate dispatch refuses
`learned_*` bindings with `allocation_invalid` and the structured
detail names the empty registry.

The feature extractor mirrors `docs/archive/phase3.5-learned-vf/extract_features.py`
function-for-function — same numeric values are computed from a
KataGo analyze response at V=pre. The bundled model's `metadata.json`
carries the expected feature-name list; mismatch at construction
time fails fast (Event.STARTUP_WARNING, version is dropped from
advertisement; proxy keeps starting).

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from katago import AnalyzeResponse, TurnIndex
from middleware.adaptive_reevaluate import TurnView
from proxy_logging import Event, get_proxy_logger

_log = get_proxy_logger(__name__)

# Graceful degradation: lightgbm is an optional dependency. If
# unavailable, the learned-VF capability is silently absent — the
# proxy still serves all non-learned use cases. Operators who want
# the learned VF install lightgbm; the bundled model files alone
# aren't sufficient.
try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False
    # `assignment` if lightgbm is installed (lgb has the module type;
    # assigning None is a real type error worth silencing). `unused-
    # ignore` for CI where lightgbm isn't installed under the
    # `[dev]` extras — `ignore_missing_imports` makes lgb `Any` and
    # the assignment is fine without the ignore. The combined pragma
    # silences both cases regardless of which environment mypy runs in.
    lgb = None  # type: ignore[assignment, unused-ignore]


# ---------------------------------------------------------------------------
# Feature extraction (ported from docs/archive/phase3.5-learned-vf/extract_features.py)
# ---------------------------------------------------------------------------

def _safe_float(v: Any, default: float = 0.0) -> float:
    """Coerce a wire-field value to float, returning `default` on
    non-numeric inputs (including bools, None, missing keys, etc.).

    KataGo wire responses occasionally carry `null` for fields that
    are normally numeric (e.g. `rootInfo.scoreStdev` on positions
    near terminal). The defensive coercion keeps the feature
    extractor robust to those without per-feature special-casing.
    """
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    return default


def _shannon_entropy(probs: List[float]) -> float:
    """Shannon entropy in bits of a non-normalised probability list.
    Treats zero / negative entries as having zero contribution; does
    NOT renormalise (caller's responsibility to pass values that sum
    to ~1, or accept that the absolute scale reflects the raw mass).
    """
    return -sum(p * math.log2(p) for p in probs if p > 0)


def _gini(values: List[float]) -> float:
    """Gini coefficient on non-negative values.
    Returns 0 for empty / all-zero inputs (no concentration to
    measure). Used to summarise the top-K visit-distribution
    concentration as one scalar.
    """
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    total = sum(s)
    if total <= 0:
        return 0.0
    cum = 0.0
    for i, v in enumerate(s):
        cum += (i + 1) * v
    return (2 * cum) / (n * total) - (n + 1) / n


def _per_turn_features(packet: Dict[str, Any], to_play: str) -> Dict[str, float]:
    """Extract per-turn feature vector from one V=pre analyze response.

    Field names mirror docs/archive/phase3.5-learned-vf/extract_features.py;
    the bundled LightGBM model's expected feature-name list (in
    metadata.json) corresponds to these keys prefixed with `f_`.

    Defensive against malformed responses: missing fields default to
    0.0; missing moveInfos / policy / pv arrays produce zeros rather
    than raises. Mirrors the offline extractor's behaviour exactly
    so training-vs-runtime feature distributions match.
    """
    root = packet.get("rootInfo", {}) if isinstance(packet.get("rootInfo"), dict) else {}
    mi = packet.get("moveInfos", []) if isinstance(packet.get("moveInfos"), list) else []
    policy = packet.get("policy", []) if isinstance(packet.get("policy"), list) else []

    score_stdev = _safe_float(root.get("scoreStdev"))
    score_lead = _safe_float(root.get("scoreLead"))
    winrate = _safe_float(root.get("winrate"))
    raw_lead = _safe_float(root.get("rawLead"))
    raw_winrate = _safe_float(root.get("rawWinrate"))
    raw_score_selfplay = _safe_float(root.get("rawScoreSelfplay"))
    raw_var_time_left = _safe_float(root.get("rawVarTimeLeft"))
    raw_noresult = _safe_float(root.get("rawNoResultProb"))
    visits = _safe_float(root.get("visits"))
    weight = _safe_float(root.get("weight"))

    top5 = mi[:5] if mi else []
    top5_visits = [_safe_float(m.get("visits")) for m in top5 if isinstance(m, dict)]
    top5_prior = [_safe_float(m.get("prior")) for m in top5 if isinstance(m, dict)]
    top5_utilityLcb = [_safe_float(m.get("utilityLcb")) for m in top5 if isinstance(m, dict)]
    top5_winrate = [_safe_float(m.get("winrate")) for m in top5 if isinstance(m, dict)]
    top5_scoreMean = [_safe_float(m.get("scoreMean")) for m in top5 if isinstance(m, dict)]

    total_visits = sum(top5_visits) or 1.0
    visits_dist = [v / total_visits for v in top5_visits]
    top1_mass = visits_dist[0] if visits_dist else 0.0
    visits_entropy = _shannon_entropy(visits_dist)
    visits_gini = _gini(top5_visits)
    prior_entropy = _shannon_entropy(top5_prior)
    lcb_spread = (max(top5_utilityLcb) - min(top5_utilityLcb)) if len(top5_utilityLcb) >= 2 else 0.0
    winrate_gap = (top5_winrate[0] - top5_winrate[1]) if len(top5_winrate) >= 2 else 0.0
    score_gap = (top5_scoreMean[0] - top5_scoreMean[1]) if len(top5_scoreMean) >= 2 else 0.0

    pv = (top5[0].get("pv", []) or []) if top5 else []
    pv_visits = (top5[0].get("pvVisits", []) or []) if top5 else []
    pv_len = len(pv)
    pv_decay = 0.0
    if isinstance(pv_visits, list) and len(pv_visits) >= 2:
        first = _safe_float(pv_visits[0])
        last = _safe_float(pv_visits[-1])
        if first > 0:
            pv_decay = last / first

    policy_entropy_val = _shannon_entropy(
        [_safe_float(p) for p in policy if isinstance(p, (int, float))]
    )

    return {
        "score_stdev": score_stdev,
        "score_lead": score_lead,
        "winrate": winrate,
        "raw_lead": raw_lead,
        "raw_winrate": raw_winrate,
        "raw_score_selfplay": raw_score_selfplay,
        "raw_var_time_left": raw_var_time_left,
        "raw_noresult": raw_noresult,
        "visits_at_v200": visits,
        "weight_at_v200": weight,
        "winrate_minus_raw": winrate - raw_winrate,
        "score_lead_minus_raw": score_lead - raw_score_selfplay,
        "top1_visits_mass": top1_mass,
        "visits_entropy": visits_entropy,
        "visits_gini": visits_gini,
        "prior_entropy": prior_entropy,
        "lcb_spread": lcb_spread,
        "winrate_gap_top1_top2": winrate_gap,
        "score_gap_top1_top2": score_gap,
        "pv_len": float(pv_len),
        "pv_visit_decay_ratio": pv_decay,
        "policy_entropy": policy_entropy_val,
        "to_play_is_black": 1.0 if to_play == "black" else 0.0,
    }


def _range_summary(
    per_turn: List[Dict[str, float]],
    feature_keys: List[str],
) -> Dict[str, float]:
    """Compute mean / std / min / max of each per-turn feature across
    the candidate range. Matches the offline extractor's range_
    feature naming convention (e.g. `range_score_stdev_mean`).
    """
    out: Dict[str, float] = {}
    for k in feature_keys:
        vals = [t[k] for t in per_turn if k in t]
        if not vals:
            continue
        out[f"range_{k}_mean"] = statistics.mean(vals)
        out[f"range_{k}_std"] = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        out[f"range_{k}_min"] = min(vals)
        out[f"range_{k}_max"] = max(vals)
    return out


# ---------------------------------------------------------------------------
# LearnedValueFn — the predictor
# ---------------------------------------------------------------------------

class LearnedValueFn:
    """Wraps a pair of LightGBM boosters (r_full + r_int) and a
    feature-name schema. Implements `Callable[[TurnView], float]`
    (returns r_full); exposes `predict_int(turn_view) -> float` for
    the r_int companion prediction.

    Range-level features (`range_X_mean`, etc.) are computed once via
    `prepare(candidates)` and cached for the duration of the
    allocation call. The substrate's resolver constructs a fresh
    `LearnedValueFn` per allocation and calls `prepare()` before
    handing it to the algorithm.
    """

    def __init__(
        self,
        model_full: Any,
        model_int: Any,
        feature_names: List[str],
        version: str,
        v_pre: int = 200,
        v_intermediate: int = 1000,
        v_oracle: int = 5000,
    ) -> None:
        # Lazy lightgbm import (already imported at module level if
        # available; the registry won't construct a LearnedValueFn
        # without it).
        assert LIGHTGBM_AVAILABLE, "LearnedValueFn requires lightgbm"
        self._model_full = model_full
        self._model_int = model_int
        self._feature_names = list(feature_names)
        self._feature_name_set = set(self._feature_names)
        self._version = version
        self._v_pre = v_pre
        self._v_intermediate = v_intermediate
        self._v_oracle = v_oracle
        # Per-turn feature cache, populated on the first call after
        # prepare(). Keyed by turn index.
        self._per_turn_features: Dict[TurnIndex, Dict[str, float]] = {}
        # Range-level features, populated by prepare(). Shared across
        # all per-turn predictions in this allocation.
        self._range_features: Dict[str, float] = {}
        # Context features (cell-level metadata; populated by prepare).
        self._context_features: Dict[str, float] = {}
        # Prediction caches (per turn), populated on first __call__ /
        # predict_int for that turn; subsequent calls return cached.
        self._cache_full: Dict[TurnIndex, float] = {}
        self._cache_int: Dict[TurnIndex, float] = {}

    @property
    def version(self) -> str:
        return self._version

    @property
    def v_int_extra(self) -> int:
        """Visits between V_pre and V_intermediate. Consumed by
        LearnedPiecewiseAllocator for segment-1 capacity."""
        return self._v_intermediate - self._v_pre

    @property
    def v_full_extra(self) -> int:
        """Visits between V_pre and V_oracle. Consumed by
        LearnedPiecewiseAllocator for segment-2 capacity / total
        per-turn cap."""
        return self._v_oracle - self._v_pre

    def prepare(self, candidates: List[TurnView]) -> None:
        """Pre-compute per-turn features for every candidate, then
        derive range-level summaries from them. Idempotent.

        After this call, `self._per_turn_features` carries one
        entry per candidate, and `self._range_features` carries the
        mean/std/min/max aggregates plus context features.

        Notes:
          - Context features (`context_turn_start`, `context_phase_fraction`,
            `context_komi`, `context_board_size`, `context_turn_count`,
            `context_n_moves`) ARE in the model's feature list. We
            approximate them from the candidate set: `turn_start`
            from the first candidate's `turn_index`, `turn_count`
            from `len(candidates)`. `phase_fraction`, `komi`,
            `board_size`, `n_moves` are not available from the
            candidate alone — we default to neutral values
            (`phase_fraction=0.5`, `komi=6.5`, `board_size=19`,
            `n_moves=turn_count + turn_start * 2` rough estimate).
            The model's feature importance for context fields is
            generally low, but if production calibration shows
            issues we may need to thread these through the substrate.
        """
        self._per_turn_features.clear()
        self._cache_full.clear()
        self._cache_int.clear()

        per_turn_list: List[Dict[str, float]] = []
        for c in candidates:
            t = int(c.turn_index)
            opaque = c.packet.opaque if isinstance(c.packet, AnalyzeResponse) else {}
            features = _per_turn_features(opaque, c.to_play)
            self._per_turn_features[c.turn_index] = features
            per_turn_list.append(features)

        if not per_turn_list:
            self._range_features = {}
            self._context_features = {}
            return

        feature_keys = sorted(per_turn_list[0].keys())
        self._range_features = _range_summary(per_turn_list, feature_keys)

        # Context features. Approximated from candidate metadata.
        # See class docstring for the caveat about precise values.
        turn_indices = sorted(int(c.turn_index) for c in candidates)
        turn_start = float(turn_indices[0])
        turn_count = float(len(candidates))
        self._context_features = {
            "context_turn_start": turn_start,
            "context_turn_count": turn_count,
            # Conservative defaults — see docstring.
            "context_n_moves": max(turn_start + turn_count * 2, turn_start + 50),
            "context_phase_fraction": 0.5,
            "context_komi": 6.5,
            "context_board_size": 19.0,
        }

    def _build_feature_vector(self, turn_index: TurnIndex) -> List[float]:
        """Assemble the feature vector for one turn in the order
        `self._feature_names` expects. Missing features default to 0."""
        per_turn = self._per_turn_features.get(turn_index, {})
        vec: List[float] = []
        for name in self._feature_names:
            if name.startswith("f_"):
                key = name[2:]
                if key.startswith("range_") or key.startswith("context_"):
                    # Range / context feature: from cached summary.
                    val = self._range_features.get(key, self._context_features.get(key, 0.0))
                else:
                    # Per-turn feature.
                    val = per_turn.get(key, 0.0)
                vec.append(val)
            else:
                vec.append(0.0)
        return vec

    def __call__(self, turn_view: TurnView) -> float:
        """Return the predicted r_full (V=pre → V=oracle entropy
        reduction) for one turn. Caches the prediction so repeated
        calls within the same allocation don't repeat the LightGBM
        inference."""
        t = turn_view.turn_index
        if t in self._cache_full:
            return self._cache_full[t]
        if t not in self._per_turn_features:
            # Defensive: the algorithm called us for a turn the
            # predictor hasn't prepared. Compute per-turn features
            # on the fly; range features remain whatever prepare()
            # set them to. Log so this is visible.
            opaque = turn_view.packet.opaque if isinstance(turn_view.packet, AnalyzeResponse) else {}
            self._per_turn_features[t] = _per_turn_features(opaque, turn_view.to_play)
        vec = self._build_feature_vector(t)
        import numpy as np
        prediction = float(self._model_full.predict(np.array([vec], dtype=np.float64))[0])
        self._cache_full[t] = prediction
        return prediction

    def predict_int(self, turn_view: TurnView) -> float:
        """Return the predicted r_int (V=pre → V=intermediate entropy
        reduction). Used by LearnedPiecewiseAllocator for segment-
        based water-fill."""
        t = turn_view.turn_index
        if t in self._cache_int:
            return self._cache_int[t]
        if t not in self._per_turn_features:
            opaque = turn_view.packet.opaque if isinstance(turn_view.packet, AnalyzeResponse) else {}
            self._per_turn_features[t] = _per_turn_features(opaque, turn_view.to_play)
        vec = self._build_feature_vector(t)
        import numpy as np
        prediction = float(self._model_int.predict(np.array([vec], dtype=np.float64))[0])
        self._cache_int[t] = prediction
        return prediction


# ---------------------------------------------------------------------------
# Registry — loaded at proxy startup
# ---------------------------------------------------------------------------

class LearnedValueFnRegistry:
    """Singleton-style registry that scans `proxy/models/learned_value_fn/v{N}/`
    for bundled model directories and loads each successfully.

    Per the dispatch (docs/dispatch/proxy-to-frontend-learned-vf.md):
      - Each version directory contains `r_full.txt`, `r_int.txt`,
        and `metadata.json` (with `feature_names` list).
      - Failure to load any version logs at WARNING via
        Event.DIAGNOSTIC and the version is silently absent from the
        registry (and from the advertisement). Proxy startup is
        not blocked.

    `available_versions()` returns the list of loaded version strings
    in stable order (e.g. ["learned_v1"]) for the capability
    advertisement. `get(name)` returns a fresh LearnedValueFn
    instance per call — the predictor is stateful (per-allocation
    feature caches) so the registry hands out new instances rather
    than sharing.
    """

    def __init__(self, models_root: Optional[Path] = None) -> None:
        # Default location: proxy/models/learned_value_fn/v{N}/
        if models_root is None:
            # proxy/ root = parent of this file's parent (middleware/).
            here = Path(__file__).resolve().parent.parent
            models_root = here / "models" / "learned_value_fn"
        self._models_root = models_root
        # Loaded models: { "learned_v1": (booster_full, booster_int, feature_names, metadata_dict) }
        self._loaded: Dict[str, tuple[Any, Any, List[str], Dict[str, Any]]] = {}
        self._load_all()

    def _load_all(self) -> None:
        if not LIGHTGBM_AVAILABLE:
            _log.info(
                Event.DIAGNOSTIC,
                msg=(
                    "lightgbm not installed; learned-VF capability is "
                    "absent. Install lightgbm in the proxy's Python "
                    "environment to enable proxy-hosted learned "
                    "predictors."
                ),
            )
            return
        if not self._models_root.exists():
            _log.info(
                Event.DIAGNOSTIC,
                msg=(
                    f"learned-VF model registry directory does not "
                    f"exist: {self._models_root}; no learned predictors "
                    f"will be advertised"
                ),
            )
            return
        # Scan v1, v2, ... directories.
        for version_dir in sorted(self._models_root.iterdir()):
            if not version_dir.is_dir():
                continue
            if not version_dir.name.startswith("v"):
                continue
            version_name = f"learned_{version_dir.name}"
            try:
                self._load_version(version_name, version_dir)
            except Exception as e:
                _log.warning(
                    Event.DIAGNOSTIC,
                    msg=(
                        f"failed to load learned-VF version "
                        f"{version_name} from {version_dir}: {e!r}; "
                        f"this version will not be advertised"
                    ),
                )

    def _load_version(self, name: str, version_dir: Path) -> None:
        full_path = version_dir / "r_full.txt"
        int_path = version_dir / "r_int.txt"
        meta_path = version_dir / "metadata.json"
        for p in (full_path, int_path, meta_path):
            if not p.exists():
                raise FileNotFoundError(f"required file missing: {p}")
        metadata = json.loads(meta_path.read_text())
        feature_names = metadata.get("feature_names")
        if not isinstance(feature_names, list) or not feature_names:
            raise ValueError(
                f"metadata.json missing or invalid 'feature_names' list"
            )
        booster_full = lgb.Booster(model_file=str(full_path))
        booster_int = lgb.Booster(model_file=str(int_path))
        # Sanity check: the booster's reported feature names should
        # match the metadata's. Mismatch = bundle corruption.
        booster_full_names = booster_full.feature_name()
        if list(booster_full_names) != list(feature_names):
            raise ValueError(
                f"r_full booster feature names disagree with "
                f"metadata.json"
            )
        self._loaded[name] = (
            booster_full, booster_int, list(feature_names), metadata,
        )
        _log.info(
            Event.DIAGNOSTIC,
            msg=f"loaded learned-VF version {name} from {version_dir}",
        )

    def available_versions(self) -> List[str]:
        return sorted(self._loaded.keys())

    def get(self, name: str) -> Optional[LearnedValueFn]:
        entry = self._loaded.get(name)
        if entry is None:
            return None
        booster_full, booster_int, feature_names, metadata = entry
        return LearnedValueFn(
            model_full=booster_full,
            model_int=booster_int,
            feature_names=feature_names,
            version=name,
            v_pre=int(metadata.get("v_pre", 200)),
            v_intermediate=int(metadata.get("v_intermediate", 1000)),
            v_oracle=int(metadata.get("v_oracle", 5000)),
        )


# Module-level singleton, initialised lazily on first access.
_REGISTRY: Optional[LearnedValueFnRegistry] = None


def get_registry() -> LearnedValueFnRegistry:
    """Return the module-level registry singleton, initialising on
    first call. Safe to call repeatedly; the initialiser is run once."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = LearnedValueFnRegistry()
    return _REGISTRY
