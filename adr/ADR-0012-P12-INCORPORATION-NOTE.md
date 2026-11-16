# ADR-0012 P12 — Incorporation and transfer note

**Disposition:** Ratified in spirit by the maintainer on 2026-11-04. The complete
ADR-0012 transfer copy has the amendment appended after P11 and before
`## License`; see `ADR-0012-compositional-and-structural-hygiene-P12-updated.md`.

This note remains in the source tree as a provenance aid. It does not claim that
the wider ADR corpus has acquired a canonical home; that separate single-source-
of-truth issue remains acknowledged by the maintainer.

### Amendment — 2026-11-04: P12 — Eager imports are the module's dependency contract (lazy imports are banned)

*(Provenance: the maintainer's original 2026-07-02 edict, supplied again on
2026-11-04 and ratified in spirit on that date. The rule originated after
deferred imports interfered with benchmarking and performance-sensitive code
in an unrelated project. The omission from ADR-0012 is identified by the
maintainer as an executive lapse.)*

**Rule (checkable).** Every runtime Python `import` or `from ... import`
statement executes at module import time. Import statements inside a function
or method body — including nested functions and methods — are **forbidden,
without an allowlist**. A deferred import transfers work to whichever caller
first reaches that branch, hides the module's actual dependency footprint, and
makes that footprint a module-scale lying contract.

If some importers of a module should not pay for one of its dependencies, the
module is mis-factored. **Split the module at the dependency boundary** so each
module has one honest, complete top-of-file dependency footprint; have an
explicit composition root import and inject the collaborator where concerns
must be combined. Do not defer an import to preserve a monolithic module or to
make an optional backend appear absent from the importing module's contract.
An optional backend belongs in its own module, which consumers import
explicitly when they choose that backend.

**Terminological boundary.** “Lazy import” in this amendment means deferred
Python module loading only. It is not a rejection of lazy evaluation, non-strict
semantics, lazy data structures, or functional laziness as a programming model;
the maintainer's inspiration from Haskell and Hughes's *Why Functional
Programming Matters* is compatible with this rule. The prohibited property is
that a Python module dependency is hidden until a later execution path pays its
import-time cost.

**The narrow exemptions are execution semantics, not allowlists.** An import
inside an `if TYPE_CHECKING:` block is exempt because that block never executes
at runtime. Module-level imports nested under `if`, `try`, or another module
scope construct are permitted when they execute during module import. Imports
in a class body are also permitted because the class body executes when the
class is defined. No function-body or method-body import is permitted merely
because it is guarded, optional, rarely reached, or convenient.

**Mechanization.** `gates/no_lazy_imports.py` parses live Python source and
reports every `Import` / `ImportFrom` statement nested in a function or method
body. In a Git checkout it reads staged blobs by default; `--tree` selects the
working tree. It excludes only historical snapshots and generated, vendored,
or environment trees that are not live project source. The required result is
**zero violations**. There is no per-module baseline, suppressions list, or
allowlist. A parse/read failure is itself a gate failure because the gate cannot
certify the file.

**Cancer prevented.** Hidden dependency edges, delayed first-use cost, and
module import footprints that lie about what the module depends on. This
composes with P2 (explicit ports and dependency inversion), P3 (one-owner
collaborators), and P8 (the signature is the contract's single source of truth):
a module's imports are its dependency contract, and the module boundary must
honor it honestly.

**Self-application / adoption status.** The first scan after introducing the
gate reports existing violations in the live tree. They are not grandfathered
or allowlisted; the gate remains red until the owning modules are decomposed
and the count reaches zero. The current inventory is recorded in `BACKLOG.md`.
Adding the gate alone does not discharge the rule; the gate must reach zero.
