"""
reginterp.py — User-supplied analysis-expression compiler with a curated stdlib.

Architecture (post v1.0.3)
──────────────────────────
The asteval Interpreter receives a `parameters` / `symbols` / `bindings`
configuration from the caller (typically `analysis_enricher` in `baduk.py`,
which reads `analysis_config` off an incoming KataGo query). Symbol bodies
are compiled as `def <name>(x): return <expr>` blocks; bindings later
resolve abstract roles (`delta_fn`, `summary_fn`, `state_fns`) to compiled
symbol-table entries.

The wire surface for `analysis_config` originates from any client connected
to the proxy. The corresponding security posture, established by the
2026-05-02 audit and the proxy ↔ frontend curation dispatch, is:

  • The symtable carries ONLY curated Python wrappers — never the `numpy`
    module, never `scipy.stats.entropy` directly.
  • Each wrapper validates positional args (size cap, refused dtypes) and
    accepts only its declared kwargs (Python's natural keyword-argument
    rejection does the gating; we never see `out=`, `dtype=`, `keepdims=`,
    `where=`, etc.).
  • A `apply_window(f, x, w)` higher-order combinator preserves
    user-extensibility for sliding aggregations; `f` MUST be an
    asteval-compiled `Procedure` — passing in an arbitrary Python callable
    is rejected, because that would re-open the very surface this module is
    here to close.
  • Names not in the curated table are unbound at compile time and surface
    via `aeval.error` → `RuntimeError` from `_exec`. The caller in
    `baduk.analysis_enricher` catches that and logs a warning so a single
    bad config does not kill the WebSocket connection.

This design sits on the analysis-flexibility ↔ security Pareto frontier
deliberately: the curated stdlib covers every analysis pattern observed in
the LengYue frontend's default registry, plus the higher-order combinator
gives unbounded expressivity over sliding windows. Defenses against
residual asteval risk (per-evaluation wall-clock, rlimit memory ceiling,
process isolation) layer on top of this module without changing it; they
are scoped into v1.0.4.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import builtins as _builtins
import logging
import os

import numpy as _np
from scipy.stats import entropy as _scipy_entropy
from asteval import Interpreter

# `max` is shadowed by our curated wrapper in the symtable, but `arange` (and
# any other helper that wants Python's two-arg max for plain scalars) still
# needs the builtin. Bind it once under a private alias so the shadow
# downstream does not matter.
_builtins_max = _builtins.max

# Procedure is the runtime type asteval uses for `def`-compiled functions in
# the symtable. We use it for the strict callable check inside apply_window.
# The import path has moved between asteval versions; fall back to a
# duck-typed name+module check if both known import paths fail.
try:
    from asteval.astutils import Procedure as _AstevalProcedure  # asteval ≥ 0.9.30 layout
except ImportError:  # pragma: no cover — version-compat fallback
    try:
        from asteval import Procedure as _AstevalProcedure  # older layout
    except ImportError:  # pragma: no cover
        _AstevalProcedure = None  # see _is_asteval_callable() for the fallback


logger = logging.getLogger("kataproxy." + __name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Element-count cap on any array a wrapper produces or accepts. Six orders of
# magnitude over realistic LengYue usage (~9 000 deltas at the worst single-
# game extreme), so legitimate analyses never trip it; crafted inputs do.
# Override via env for operators with unusual workloads.

_DEFAULT_ARRAY_CAP = int(os.environ.get("KATAPROXY_ANALYSIS_ARRAY_CAP", "1000000"))


# ---------------------------------------------------------------------------
# Coercion + validation helpers (private — never enter the symtable)
# ---------------------------------------------------------------------------

def _to_array(x, *, max_size: int | None = None, allow_complex: bool = False):
    """Coerce x to ndarray with a strict size cap and refused-dtype set.

    Refused dtypes:
      • object — re-opens attribute walks via per-element Python dispatch.
      • complex — doubles memory and is not used by any current analysis.
    """
    cap = _DEFAULT_ARRAY_CAP if max_size is None else max_size
    a = x if isinstance(x, _np.ndarray) else _np.asarray(x)
    if a.dtype == object:
        raise TypeError("object-dtype arrays are not permitted in analysis_config")
    if a.dtype.kind == "c" and not allow_complex:
        raise TypeError("complex arrays are not permitted in analysis_config")
    if a.size > cap:
        raise ValueError(f"array size {a.size} exceeds element-count cap {cap}")
    return a


def _check_window(w, n: int) -> None:
    """Validate a sliding-window size against the array length it will scan."""
    # Reject bool explicitly — `bool` is a subclass of `int` in Python and
    # `True`/`False` would otherwise sneak through the integer check below.
    if isinstance(w, bool) or not isinstance(w, (int, _np.integer)):
        raise TypeError(f"window size must be an integer, got {type(w).__name__}")
    if w < 1:
        raise ValueError(f"window size must be >= 1, got {w}")
    if w > n:
        raise ValueError(f"window size {w} exceeds array length {n}")


def _check_count(n, name: str) -> None:
    """Validate an array-construction element count against the global cap."""
    if isinstance(n, bool) or not isinstance(n, (int, _np.integer)):
        raise TypeError(f"{name}: count must be an integer, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"{name}: count must be >= 0, got {n}")
    if n > _DEFAULT_ARRAY_CAP:
        raise ValueError(
            f"{name}: count {n} exceeds element-count cap {_DEFAULT_ARRAY_CAP}"
        )


def _is_asteval_callable(f) -> bool:
    """True iff f is an asteval-compiled Procedure.

    `apply_window` uses this gate to refuse arbitrary Python callables; only
    user `symbols`-defined functions (which are themselves sandboxed) may be
    passed. The fallback name+module check exists for cross-version asteval
    robustness; it is strictly looser than the isinstance form.
    """
    if _AstevalProcedure is not None:
        return isinstance(f, _AstevalProcedure)
    t = type(f)
    return t.__name__ == "Procedure" and "asteval" in (t.__module__ or "")


def _check_q(q, name: str, *, upper: float):
    """Validate a percentile/quantile q value (scalar or short array)."""
    q_arr = _np.asarray(q, dtype=float)
    if q_arr.size > 100:
        raise ValueError(f"{name}: q-array size exceeds 100")
    if not _np.all((0 <= q_arr) & (q_arr <= upper)):
        raise ValueError(f"{name}: q must be in [0, {upper}]")
    return q_arr


# ===========================================================================
# Curated stdlib — wrappers exposed in the asteval symtable
# ===========================================================================
# Each wrapper:
#   • coerces positional ndarray-shaped args via _to_array (size cap +
#     dtype gate);
#   • accepts ONLY the kwargs declared in its signature (Python's natural
#     keyword-argument rejection does the gating);
#   • returns a numpy result that asteval-compiled user code can consume.
#
# Out=, dtype=, keepdims=, where= are intentionally absent everywhere.
# Functions that take arbitrary callables (frompyfunc, apply_along_axis,
# einsum, fftconvolve, ndimage filters) are intentionally absent.
# ===========================================================================

# ---- Reductions ----

def mean(x, axis=None):
    return _np.mean(_to_array(x), axis=axis)

def median(x, axis=None):
    return _np.median(_to_array(x), axis=axis)

def std(x, axis=None):
    return _np.std(_to_array(x), axis=axis)

def var(x, axis=None):
    return _np.var(_to_array(x), axis=axis)

def sum(x, axis=None):  # noqa: A001 — deliberate shadow per dispatch
    return _np.sum(_to_array(x), axis=axis)

def prod(x, axis=None):
    return _np.prod(_to_array(x), axis=axis)

def min(x, axis=None):  # noqa: A001 — deliberate shadow per dispatch
    return _np.min(_to_array(x), axis=axis)

def max(x, axis=None):  # noqa: A001 — deliberate shadow per dispatch
    return _np.max(_to_array(x), axis=axis)

def percentile(x, q, axis=None):
    a = _to_array(x)
    q_arr = _check_q(q, "percentile", upper=100.0)
    return _np.percentile(a, q_arr, axis=axis)

def quantile(x, q, axis=None):
    a = _to_array(x)
    q_arr = _check_q(q, "quantile", upper=1.0)
    return _np.quantile(a, q_arr, axis=axis)

def argmin(x, axis=None):
    return _np.argmin(_to_array(x), axis=axis)

def argmax(x, axis=None):
    return _np.argmax(_to_array(x), axis=axis)

def argsort(x, axis=-1):
    return _np.argsort(_to_array(x), axis=axis)


# ---- Element-wise ----

def log(x):
    return _np.log(_to_array(x))

def exp(x):
    return _np.exp(_to_array(x))

def sqrt(x):
    return _np.sqrt(_to_array(x))

def abs(x):  # noqa: A001 — deliberate shadow per dispatch
    return _np.abs(_to_array(x))

def sign(x):
    return _np.sign(_to_array(x))

def clip(x, lo, hi):
    # Refuse array-shaped lo/hi to keep the operation predictable and the
    # output shape determined entirely by x.
    if not _np.isscalar(lo) or not _np.isscalar(hi):
        raise TypeError("clip bounds must be scalars")
    return _np.clip(_to_array(x), lo, hi)

def isnan(x):
    return _np.isnan(_to_array(x))

def isfinite(x):
    return _np.isfinite(_to_array(x))

def where(cond, a, b):
    """Ternary form only — the 1-arg form (returns indices) is intentionally
    not exposed; it leaks shape via the index list and has no analysis use
    case the dispatch enumerated."""
    return _np.where(_to_array(cond), _to_array(a), _to_array(b))


# ---- Convolution / correlation ----

_CONVOLVE_MODES = ("full", "same", "valid")

def convolve(a, v, mode="full"):
    if mode not in _CONVOLVE_MODES:
        raise ValueError(
            f"convolve mode must be one of {_CONVOLVE_MODES}, got {mode!r}"
        )
    return _np.convolve(_to_array(a), _to_array(v), mode=mode)

def correlate(a, v, mode="full"):
    if mode not in _CONVOLVE_MODES:
        raise ValueError(
            f"correlate mode must be one of {_CONVOLVE_MODES}, got {mode!r}"
        )
    return _np.correlate(_to_array(a), _to_array(v), mode=mode)


# ---- Sliding window ----

def sliding_window(x, w):
    a = _to_array(x)
    _check_window(w, a.size)
    return _np.lib.stride_tricks.sliding_window_view(a, w)

def sliding_mean(x, w):
    return _np.mean(sliding_window(x, w), axis=-1)

def sliding_median(x, w):
    return _np.median(sliding_window(x, w), axis=-1)

def sliding_std(x, w):
    return _np.std(sliding_window(x, w), axis=-1)

def sliding_percentile(x, w, q):
    view = sliding_window(x, w)
    q_arr = _check_q(q, "sliding_percentile", upper=100.0)
    return _np.percentile(view, q_arr, axis=-1)


def apply_window(f, x, w):
    """Apply a symbol-defined function `f` over each w-sized window of `x`.

    `f` MUST be an asteval-compiled Procedure (i.e. a function declared in
    the same `analysis_config['symbols']` block). Smuggling in an arbitrary
    Python callable is refused: that would re-open the very surface the
    curated stdlib closes.
    """
    if not _is_asteval_callable(f):
        raise TypeError(
            "apply_window: f must be a function defined in the analysis_config "
            f"symbols block; got {type(f).__name__}"
        )
    a = _to_array(x)
    _check_window(w, a.size)
    view = _np.lib.stride_tricks.sliding_window_view(a, w)
    return _np.array([f(view[i]) for i in range(view.shape[0])])


# ---- Stats ----

def entropy(p, q=None):
    pk = _to_array(p)
    qk = _to_array(q) if q is not None else None
    return _scipy_entropy(pk, qk)

def normalized_entropy(p):
    pk = _to_array(p)
    if pk.size <= 1:
        return 0.0
    return float(_scipy_entropy(pk) / _np.log(pk.size))


# ---- Array construction (size-capped) ----

def array(x):
    # `.copy()` detaches the result from any source list/array, so downstream
    # mutation (which user code can do — asteval permits item-assignment on
    # ndarrays) cannot reach back into the caller's data structures.
    return _to_array(x).copy()

def zeros(n):
    _check_count(n, "zeros")
    return _np.zeros(n)

def ones(n):
    _check_count(n, "ones")
    return _np.ones(n)

def full(n, v):
    _check_count(n, "full")
    return _np.full(n, v)

def arange(*args):
    """Mirror numpy's arange: arange(stop) | arange(start, stop) | arange(start, stop, step).

    The size of the resulting array is computed up-front and checked against
    the element-count cap before any allocation, so a crafted (start, stop,
    step) cannot produce a giant array via numpy itself.
    """
    if not args or len(args) > 3:
        raise TypeError("arange requires 1, 2, or 3 positional args")
    if len(args) == 1:
        start, stop, step = 0, args[0], 1
    elif len(args) == 2:
        start, stop, step = args[0], args[1], 1
    else:
        start, stop, step = args
    if step == 0:
        raise ValueError("arange step must be nonzero")
    size = _builtins_max(0, int(_np.ceil((stop - start) / step)))
    if size > _DEFAULT_ARRAY_CAP:
        raise ValueError(
            f"arange size {size} exceeds element-count cap {_DEFAULT_ARRAY_CAP}"
        )
    return _np.arange(*args)

def linspace(start, stop, n):
    _check_count(n, "linspace")
    return _np.linspace(start, stop, n)


# ---- Indexing ----

def take(x, indices):
    return _np.take(_to_array(x), _to_array(indices))

def nonzero(x):
    return _np.nonzero(_to_array(x))


# ===========================================================================
# Bit-equivalence contract — load-bearing for cross-team migrations
# ===========================================================================
# For inputs that pass _to_array's gate (no object/complex dtype, within the
# element-count cap), most wrappers below return values bit-identical to
# their numpy / scipy.stats originals when called with the wrapper's exact
# signature. This is the property the umbrella's frontend migrations.ts
# (step 11 → 12) relies on when it mechanically rewrites `np.<fn>(...)` →
# `<fn>(...)` in symbol bodies. See the dispatch chain at
# <umbrella>/docs/dispatch/{proxy,frontend}-to-{frontend,proxy}-analysis-
# config-curation*.md for the coordination record.
#
# Strictly bit-equivalent (subject to dtype/size gating):
#   reductions     mean median std var sum prod min max
#                  percentile quantile argmin argmax argsort
#   elementwise    log exp sqrt abs sign isnan isfinite
#   convolution    convolve correlate (mode kwarg only)
#   stats          entropy (positional p, q only — no axis/base/nan_policy)
#   construction   zeros ones full arange linspace
#   indexing       take nonzero
#
# Not strictly bit-equivalent — the wrapper narrows the domain:
#   clip(x, lo, hi)    refuses array-shaped lo/hi (np.clip accepts); scalar
#                      bounds are bit-equivalent.
#   where(cond, a, b)  ternary form only; np.where(cond) returns indices and
#                      is not exposed.
#   array(x)           returns .copy() unconditionally; np.array uses
#                      copy=True by default with some path-dependent sharing
#                      — semantically identical for read-only use.
#
# Not numpy-derived (no equivalence claim — these are new wrappers):
#   normalized_entropy, sliding_window, sliding_mean, sliding_median,
#   sliding_std, sliding_percentile, apply_window
#
# A wrapper that no longer satisfies bit-equivalence must be moved to the
# "narrowed domain" list here so downstream rewriters can refuse to migrate
# call-sites that would diverge.


# ===========================================================================
# Symtable — the complete set of names exposed to user code
# ===========================================================================
# Anything not in this dict is unbound. asteval defers name resolution inside
# def-bodies to call time, so a user symbol referencing e.g. `np` compiles
# cleanly but raises NameError on first invocation; that exception propagates
# through the BSA pipeline to baduk.analysis_enricher's on_response
# except-Exception clause, where it surfaces as
# `Enrichment failed: name '<X>' is not defined`. No other path to numpy or
# scipy exists from a user expression.

_CURATED_SYMTABLE = {
    # reductions
    "mean": mean, "median": median, "std": std, "var": var,
    "sum": sum, "prod": prod, "min": min, "max": max,
    "percentile": percentile, "quantile": quantile,
    "argmin": argmin, "argmax": argmax, "argsort": argsort,
    # element-wise
    "log": log, "exp": exp, "sqrt": sqrt, "abs": abs, "sign": sign,
    "clip": clip, "isnan": isnan, "isfinite": isfinite, "where": where,
    # convolution / correlation
    "convolve": convolve, "correlate": correlate,
    # sliding window
    "sliding_window": sliding_window,
    "sliding_mean": sliding_mean, "sliding_median": sliding_median,
    "sliding_std": sliding_std, "sliding_percentile": sliding_percentile,
    "apply_window": apply_window,
    # stats
    "entropy": entropy, "normalized_entropy": normalized_entropy,
    # array construction
    "array": array, "zeros": zeros, "ones": ones, "full": full,
    "arange": arange, "linspace": linspace,
    # indexing
    "take": take, "nonzero": nonzero,
}


# ---------------------------------------------------------------------------
# Asteval-defined helpers — retained from the pre-v1.0.3 _STDLIB.
# ---------------------------------------------------------------------------
# Direct references to `np.X` and the bare `entropy` import are rewritten in
# terms of curated names. The exposed identifiers (safe, _uniform_entropy,
# _visit_entropy, _spread, _visit_ratio, _uservisits, _maxvisits) keep their
# names so frontend payloads referring to them continue to work unchanged.

_STDLIB = """
def safe(x):
    return 0 if isnan(x) else x

def _uniform_entropy(n):
    return 1 if n <= 1 else log(n)

def _visit_entropy(x):
    return safe(entropy([mi['visits'] for mi in x['moveInfos']]))

def _spread(x):
    return x[0]['moveInfos'][0]['visits'] / x[0]['rootInfo']['visits']

def _visit_ratio(x):
    return _uservisits(x[0]) / x[0]['moveInfos'][0]['visits']

def _uservisits(x):
    return x['userMoveInfo'] and x['userMoveInfo']['visits'] or 0

def _spread(x):
    return x['moveInfos'][0]['visits'] / x['rootInfo']['visits']

def _maxvisits(x):
    return max([z['visits'] for z in x['moveInfos']])
"""


class RegistryInterpreter:
    """An AST-based execution engine for `analysis_config` symbol bodies.

    Compared to the pre-v1.0.3 design, the asteval symtable carries ONLY
    curated wrapper functions — never the `np` module, never
    `scipy.stats.entropy` directly. User code that references `np.X` fails
    fast at compile time with an unbound-name diagnostic.

    Initialization order
    --------------------
    1. Inject the curated wrappers into the symtable.
    2. Compile the asteval-defined ``_STDLIB`` (rewritten to use curated names).
    3. Inject user ``parameters`` as plain values, refusing names that would
       shadow a curated wrapper.
    4. Compile user ``symbols`` as single-arg ``def`` blocks. Asteval resolves
       names lazily, so a user symbol may reference any other user symbol OR
       any curated wrapper OR any ``_STDLIB`` helper. Names that shadow a
       curated wrapper are refused.
    5. Store ``bindings`` for later resolution via ``get_*_fn()``.

    Compile-time errors raise ``RuntimeError`` from ``_exec``; the caller in
    ``baduk.analysis_enricher`` catches them and logs a warning so a single
    bad ``analysis_config`` does not kill the WebSocket connection.
    """

    def __init__(self, config):
        logger.debug(f"Interp: config = {config}")
        self.aeval = Interpreter()

        # 1. Curated wrappers — the only path user code has to numpy/scipy.
        for name, wrapper in _CURATED_SYMTABLE.items():
            self.aeval.symtable[name] = wrapper

        # 2. _STDLIB — asteval-side helpers built on the curated wrappers.
        self._exec(_STDLIB, label="<stdlib>")

        # 3. User parameters — plain values (numbers, strings, lists thereof).
        for name, val in (config or {}).get("parameters", {}).items():
            if name in _CURATED_SYMTABLE:
                raise RuntimeError(
                    f"parameter name {name!r} shadows a curated stdlib symbol"
                )
            self.aeval.symtable[name] = val

        # 4. User symbols — single-arg functions over a packet input.
        for name, expr in (config or {}).get("symbols", {}).items():
            if name in _CURATED_SYMTABLE:
                raise RuntimeError(
                    f"symbol name {name!r} shadows a curated stdlib function"
                )
            code = f"def {name}(x):\n    return {expr}"
            logger.debug(f"compiled code\n{code}")
            self._exec(code, label=name)

        # 5. Bindings — resolved lazily via get_*_fn.
        self.bindings = (config or {}).get("bindings", {})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _exec(self, code, label="<unknown>"):
        """Execute *code* in the interpreter, raising on any compile failure."""
        self.aeval(code)
        if self.aeval.error:
            msgs = [str(e.get_error()) for e in self.aeval.error]
            self.aeval.error.clear()
            raise RuntimeError(
                f"[RegistryInterpreter] compile error in '{label}':\n"
                + "\n".join(msgs)
            )

    # ------------------------------------------------------------------
    # Public API — unchanged shape from pre-v1.0.3
    # ------------------------------------------------------------------

    def resolve_binding(self, key):
        """Look up *key* in bindings; return the bound function or a zero stub.

        Falls back to ``lambda x: 0`` if the binding is missing or names a
        symbol that did not compile. The fallback's purpose is to keep the
        analysis pipeline running when one binding is broken — symmetric with
        the warn-and-skip stance in ``baduk.analysis_enricher``.
        """
        symbol_name = self.bindings.get(key)
        if symbol_name and symbol_name in self.aeval.symtable:
            return self.aeval.symtable[symbol_name]
        logger.warning(
            f"FALLBACK: no binding for key={key!r} (symbol={symbol_name!r})"
        )
        return lambda x: 0

    def get_delta_fn(self):
        return self.resolve_binding("delta_fn")

    def get_summary_fn(self):
        return self.resolve_binding("summary_fn")

    def get_state_fns(self):
        """Return the {label: fn} mapping requested by bindings['state_fns'].

        Missing symbols are silently skipped rather than raising — same
        rationale as ``resolve_binding``.
        """
        mapping = self.bindings.get("state_fns", {})
        return {
            label: self.aeval.symtable[symbol_name]
            for label, symbol_name in mapping.items()
            if symbol_name in self.aeval.symtable
        }
