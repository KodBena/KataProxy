import numpy as np
from scipy.stats import entropy
from asteval import Interpreter

# ---------------------------------------------------------------------------
# Standard library injected into every interpreter instance.
#
# Defined as asteval `def` blocks rather than Python lambdas so that all
# functions share the same symtable and can call each other freely.
# Name resolution in asteval is lazy (at call time), so the order of defs
# here doesn't matter, and user-defined symbols (e.g. `_uservisits`) will
# be visible to stdlib functions like `_visit_ratio` as long as they are
# compiled into the same interpreter before being called.
# ---------------------------------------------------------------------------
_STDLIB = """
def safe(x):
    return 0 if np.isnan(x) else x

def _uniform_entropy(n):
    return 1 if n <= 1 else np.log(n)

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
import logging
logger = logging.getLogger("kataproxy." + __name__)


class RegistryInterpreter:
    """
    An AST-based execution engine for Registry lambdas.
    Uses asteval to provide a safe, NumPy-aware environment.

    Initialization order
    --------------------
    1. Inject primitive Python objects (np, entropy).
    2. Compile the standard library into the symtable.
    3. Inject user parameters as plain values.
    4. Compile user symbols as single-argument functions ``def name(x): return <expr>``.
       User symbols may call stdlib helpers or each other; asteval resolves
       all names lazily at call time.
    5. Store bindings for later resolution via get_*_fn().
    """

    def __init__(self, config):
        logger.debug(f"Interp: config = {config}")
        self.aeval = Interpreter()

        # 1. Primitives
        self.aeval.symtable['np'] = np
        self.aeval.symtable['entropy'] = entropy

        # 2. Standard library
        self._exec(_STDLIB, label="<stdlib>")

        # 3. User parameters
        for name, val in config.get('parameters', {}).items():
            self.aeval.symtable[name] = val

        # 4. User symbols
        for name, expr in config.get('symbols', {}).items():
            code = f"def {name}(x):\n    return {expr}"
            logger.debug(f" compiled code\n{code}")
            self._exec(code, label=name)

        # 5. Bindings (resolved lazily via get_*_fn)
        self.bindings = config.get('bindings', {})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _exec(self, code, label="<unknown>"):
        """Execute *code* in the interpreter, raising a clear error on failure."""
        self.aeval(code)
        if self.aeval.error:
            msgs = [str(e.get_error()) for e in self.aeval.error]
            self.aeval.error.clear()
            raise RuntimeError(
                f"[RegistryInterpreter] Compile error in '{label}':\n" +
                "\n".join(msgs)
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve_binding(self, key):
        """
        Look up *key* in bindings and return the compiled function from the
        symtable.  Falls back to a zero-constant function if the binding or
        the symbol it references is missing.
        """
        symbol_name = self.bindings.get(key)
        if symbol_name and symbol_name in self.aeval.symtable:
            return self.aeval.symtable[symbol_name]

        logger.warning(f"FALLBACK: no binding for key={key!r} (symbol={symbol_name!r})")
        return lambda x: 0

    def get_delta_fn(self):
        return self.resolve_binding('delta_fn')

    def get_summary_fn(self):
        return self.resolve_binding('summary_fn')

    def get_state_fns(self):
        """
        Maps user-facing labels to resolved asteval functions.
        Example: {'Score Advantage': <asteval_func>}
        Missing symbols are skipped rather than raising.
        """
        mapping = self.bindings.get('state_fns', {})
        return {
            label: self.aeval.symtable[symbol_name]
            for label, symbol_name in mapping.items()
            if symbol_name in self.aeval.symtable
        }
