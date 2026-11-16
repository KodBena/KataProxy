# Work order: ADR-0022 audit and remediation

**Status:** filed, not started. **Starts after** the refactor-proper (ML layering and the config work in
`BACKLOG.md`), because phases 1 and 4 touch `hard_region_hunt.cli`, `rl_campaign` and the hunt store.
**Authority:** maintainer instruction of 2026-11-08. ADR-0022 (Proposed) excludes retroactive sweeps, so this
is the explicit, authorized amendment pass. Ordering rule from the maintainer, in the spirit of ADR-0000:
remediating the evidence path comes before fixing the bug it hid, because it makes this bug findable and
keeps the class from recurring. This order does not fix any solver bug.

## Why: two specimens already in the tree

1. **`stencil_study/family_study.py:122`** raises `INVARIANT VIOLATION: <n> instance(s) where adding shapes moved
   the optimum the wrong way` (the wording varies with `kind`) and names no instance, values or delta. It matches the ADR's
   founding specimen. `tests/test_family_study.py` protects it with `match="INVARIANT VIOLATION"` only, which
   is the ADR's "negative-path test checking only raises" anti-pattern. For contrast, `stencil_study/violation.py`
   builds a message naming the label and component index, and `certify.py` collects such messages as strings;
   neither is yet a stable, structured evidence path.
2. **`solver_research/broken.txt`**: `lpbb.py:130` raised `a fractional vertex without a fractional cell pair`
   with no case data, while the vertex (`frac`, `xr`), pair weights (`w`), `art` and the node count were live. It
   fired while handling `hand_exact.BudgetExceeded` (a resource limit, which is an unevaluable outcome, not a
   violated claim), inside a pool worker: the trace is a `concurrent.futures` remote traceback through
   `hard_region_hunt/solver.py` (`solver_worker`, `solve_prepared_candidate`, `_solve_one_orientation`), with the
   executor created in `hard_region_hunt/cli.py`. Reproducing it
   costs hours of `rl_loop.py`.

## Inventory (AST scan, 2026-11-08; excludes tests and legacy)

| Directory | assert (bare) | `raise AssertionError` | `raise ValueError` | broad `except` |
|---|---|---|---|---|
| `stencil_cover` | 2 (2) | 18 | 53 | 3 |
| `simplex_lab` | 116 (111) | 40 | 51 | 12 |
| `hard_corpus_dash` | 0 | 0 | 44 | 16 |
| `hard_region_hunt` | 0 | 1 | 12 | 11 |
| `stencil_study` | 0 | 3 | 8 | 0 |
| `solver_dev` | 0 | 9 | 13 | 1 |
| `rl_campaign` | 0 | 0 | 1 | 6 |
| `hunt_ml`, `dual_ranking`, `hunt_tools` | 0 | 0 | 14 | 5 |

Process boundaries that carry outcomes: `ProcessPoolExecutor` (`hard_region_hunt/cli.py`), a `multiprocessing.Process`
child (`stencil_cover/cover.py`, the killable z3 call), `multiprocessing.Pool` (`stencil_study/extract.py`).
Persistence boundaries: the hunt SQLite store (`runs`, records) and the JSONL logs.

## Method (applies to every phase)

For each check in scope, fill one ledger row (Rule 1): claim and stable id; check shape (pointwise, relational,
aggregate, missing prerequisite, resource limit, unevaluable); evaluation identity (instance, seed, config,
attempt); what is observed at detection; who consumes the failure; where the evidence goes. Then remediate
with the smallest mechanism that closes the gap (ADR-0011 Rule 2); a shared helper is adopted only if
phases 1 to 3 show the same omission class repeating (ADR-0022, Alternatives C). Each remediated durable
check gets a negative-path test that fails when evidence is lost or distorted while the check still raises
(Rule 5). Keep outcomes distinct: violated, unevaluable, resource-limited (Rules 1 and 4).

## Phases (each sized for one turn unless noted)

**Phase 0. Charter and baseline.** (a) Triage the 28 failing baseline tests into environment-caused and
real, so the safety net is trustworthy. (b) Create `adr/ADR-0022-audit-ledger.md` from the seed ledger below
for `stencil_cover`, `stencil_study`, `solver_dev` and `hard_region_hunt`. (c) Decide the open questions at
the end of this document. *Done when:* the ledger exists, the baseline failures are classified, decisions are recorded.

**Phase 1. The process-pool boundary.** The evidence was lost where the worker's outcome crossed
`ProcessPoolExecutor`. Capture, in `solver_worker` (and per orientation in `_solve_one_orientation`), the instance (grid hash or record locator, size,
orientation), seed, settings and attempt, and carry them with the exception across the boundary; persist a
failure record the operator can open (location per decision (c)). Preserve the original outcome unmasked. *Test:*
a fault-injecting solver fails inside a worker; the retained record names the instance and seed.
*Why first:* it makes every later failure attributable, including `lpbb.py`'s.

**Phase 2a. The founding specimen.** Remediate `family_study.py:122` and the `violation.py`/`certify.py`
path: name the instance, show before/after optimum and signed delta, state `shown n of m`. *Test:* assert the
semantic fields, not prose. **Phase 2b. The lpbb specimen.** Capture `frac`, `xr`, `w`, `art`, node count and
LP status at detection in `lpbb.py:130`; separate the unevaluable path (budget exhausted) from the violated
claim and determine which outcome was primary in the chained exception. Then use the new evidence, and the
preserved `simplex_original.py` / `lagrange_original.py` baselines, to localize the fault. Only after remediation.

**Phase 3. The remaining `stencil_cover` checks.** The rest of the ledger (`hand_exact`: 7, `solver_api`: 3,
`cover`, `zdd_exact`, `lagrange`, `lns`, `simplex`) and triage of the 53 `ValueError` and 6 `RuntimeError`
raises: most are argument validation, so confirm each names the offending value and scope. *Size:* may take
two turns.

**Phase 4. Hunt and campaign layer.** The 11 broad excepts in `hard_region_hunt`, 6 in `rl_campaign`
(runner restart and abort paths), and store/schema checks in `hunt_store`: each must preserve the primary
outcome (Rule 4: no formatter or handler may replace it). Check the retry and wind-down paths for dropped
evidence.

**Phase 5. Study pipeline, gates and tests.** `stencil_study` (`certify`, `violation`, `canary`, `extract`'s
pool), the gates (`no_lazy_imports` is already a good model: file, line, import, enclosing function), and the
existing tests that check only `raises`.

**Phase 6. The dashboard boundary.** `hard_corpus_dash` (16 broad excepts, 44 `ValueError` refusals) is a
user-facing surface: apply Rule 4 (audience, sanitization of untrusted input, operator versus public detail).

**Phase 7. `simplex_lab`.** 116 asserts, 111 bare. It is opt-in research code, so first triage: which asserts
guard results that `hand_exact` or the studies rely on (full contract), and which are one-off probes (the ADR
requires the durable contract only once relied upon). *Size:* one triage turn, then as needed.

**Phase 8. Mechanize and close.** Apply ADR-0011 Rule 2 to the classes that recurred: for example a ratchet
gate against bare `assert` and constant-message `AssertionError` in listed packages, and CI retention of
failure artifacts. Update the ADR's "Enforcement surface" statement to describe only what exists.

## Open decisions for the maintainer (needed by Phase 0)

(a) Shared evidence helper or per-check structure (ADR-0022 allows either; default: per-check until a class repeats).
(b) Failure-record location for Phase 1: a table in the hunt SQLite store, a failures JSONL beside the run, or both.
(c) Disclosure policy: which surfaces may show instance data (default: operator surfaces only; the dashboard stays generic with a reference).
(d) `simplex_lab` scope in Phase 7.

## Appendix: seed ledger (generated from the code, 2026-11-08)

Message kinds: `values` = the message formats observed values, `constant` = fixed text, `none` = bare.

| Location | Form | Message carries | Message |
|---|---|---|---|
| `hard_region_hunt/engine.py:589` | raise AssertionError | values | `f"hand_exact disagrees with HiGHS on a {n}-cell region ({origin}): "  ` |
| `solver_dev/bench_simplex.py:155` | raise AssertionError | values | `f"semantic mismatch during warm-up for {name}"` |
| `solver_dev/bench_simplex.py:171` | raise AssertionError | values | `f"semantic mismatch in {'optimized' if improved else 'reference'} run ` |
| `solver_dev/bench_synth.py:115` | raise AssertionError | values | `f"{family}(target={target}, seed={seed}, {int(g.sum())} cells): {arm} ` |
| `solver_dev/diagnose_hand.py:72` | raise AssertionError | constant | `"the root LP dual was not certified"` |
| `solver_dev/diagnose_hand.py:74` | raise AssertionError | values | `f"inconsistent root bounds: hand {bound}, subgradient {lag}, LP {lp}, ` |
| `solver_dev/diagnose_hand.py:76` | raise AssertionError | values | `f"inconsistent root bounds: hand {bound}, LP {lp}, optimum {opt}"` |
| `solver_dev/diagnose_hand.py:80` | raise AssertionError | values | `f"hand solver disagrees with the certified optimum: opt {opt}, hand op` |
| `solver_dev/diagnose_hand.py:88` | raise AssertionError | values | `f"a partition cheaper than the certified optimum {opt}"` |
| `solver_dev/stress_blobs.py:69` | raise AssertionError | values | `f"blob(size={size}, seed={s}, width={width_for(size)}): {arm} proves {` |
| `stencil_cover/cover.py:330` | raise AssertionError | values | `f"backend returned an invalid solution for {p.mode}"` |
| `stencil_cover/cover.py:388` | assert | none | `(no message)` |
| `stencil_cover/cover.py:512` | raise AssertionError | values | `f"certificate lower bound {cert_lb} exceeds a verified feasible partit` |
| `stencil_cover/hand_exact.py:324` | raise AssertionError | values | `f"internal: {len(self.gmasks) - len(ids)} placements share a grid mask` |
| `stencil_cover/hand_exact.py:540` | raise AssertionError | constant | `"a component without any placement"` |
| `stencil_cover/hand_exact.py:574` | raise AssertionError | constant | `"not a partition into valid placements"` |
| `stencil_cover/hand_exact.py:577` | raise AssertionError | constant | `"tiles do not cover the component"` |
| `stencil_cover/hand_exact.py:826` | raise AssertionError | constant | `"internal: an improvement was found but not realised"` |
| `stencil_cover/hand_exact.py:913` | raise AssertionError | values | `f"internal: an attempt proved {ub} tiles optimal but an earlier attemp` |
| `stencil_cover/hand_exact.py:918` | raise AssertionError | constant | `"internal: tiles and upper bound disagree"` |
| `stencil_cover/lagrange.py:51` | raise AssertionError | constant | `"internal: weights are not dual feasible"` |
| `stencil_cover/lns.py:43` | raise AssertionError | constant | `"internal: a window improvement was found but not realised"` |
| `stencil_cover/lpbb.py:130` | raise AssertionError | constant | `"a fractional vertex without a fractional cell pair (Ryan-Foster branc` |
| `stencil_cover/simplex.py:206` | raise AssertionError | values | `f"steepest-edge weight of placement {bad} is {gam[bad]:.9g} after pivo` |
| `stencil_cover/solver_api.py:99` | raise AssertionError | constant | `"tiles are not a partition into valid placements"` |
| `stencil_cover/solver_api.py:102` | raise AssertionError | constant | `"tiles do not cover the component exactly"` |
| `stencil_cover/solver_api.py:104` | raise AssertionError | values | `f"{len(out.tiles)} tiles but opt = {out.opt}"` |
| `stencil_cover/zdd_exact.py:131` | assert | none | `(no message)` |
| `stencil_cover/zdd_exact.py:206` | raise AssertionError | constant | `"no partition exists (the lone point should always be available)"` |
| `stencil_cover/zdd_exact.py:210` | raise AssertionError | constant | `"internal: min_cover disagrees with mn"` |
| `stencil_study/extract.py:185` | raise AssertionError | constant | `"layout/ownership length disagree"` |
| `stencil_study/family_study.py:122` | raise AssertionError | values | `f"INVARIANT VIOLATION: {int((d < 0).sum())} instance(s) where {'adding` |
| `stencil_study/tiers.py:73` | raise AssertionError | constant | `"allocation cannot place the remainder"` |

`simplex_lab` is summarized in the inventory table and deliberately not enumerated here (Phase 7).
