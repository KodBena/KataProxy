# ADR-0022 audit ledger

Working record for `adr/WORK-ORDER-adr-0022-audit-and-remediation.md`. Scope of this ledger: the assertion
and `AssertionError` sites in `stencil_cover`, `stencil_study`, `solver_dev` and `hard_region_hunt`
(33 sites, regenerated from the code by AST scan on 2026-11-10; the count matches the work order's seed
ledger). `ValueError`/`RuntimeError` refusals, broad `except` handlers and `simplex_lab` are not listed
here; the work order's phases 3 to 7 cover them.

**Columns.** *Message carries*: `values` = the message formats observed values, `constant` = fixed text,
`none` = bare. *Status*: `unaudited` (not yet read against Rule 1), `specimen` (known gap, see the work
order), later `remediated` or `sound` (audited, evidence path adequate for the check's shape). An audited
row gets its Rule 1 facts recorded beside it when its phase runs: claim id, check shape, evaluation
identity, what is observed at detection, consumer, where the evidence goes.

**Decisions (Phase 0).** (a) per-check structure until a failure class repeats, then the smallest mechanism;
(b) failure records go to a failures JSONL beside the run's log (`<log>.failures.jsonl`; Phase 1, done); (c) instance
data on operator surfaces only, the dashboard stays generic with a reference; (d) `simplex_lab` scope is
settled at Phase 7. Rigor is proportioned to the NP-hard horizon: evidence is captured where the check
fails, without over-engineering.

| Location | Form | Message carries | Message | Status |
|---|---|---|---|---|
| `hard_region_hunt/engine.py:590` | raise AssertionError | values | `f"hand_exact disagrees with HiGHS on a {n}-cell region ({origin}): hand opt {opt} lb {lb}…` | unaudited |
| `solver_dev/bench_simplex.py:155` | raise AssertionError | values | `f'semantic mismatch during warm-up for {name}'` | unaudited |
| `solver_dev/bench_simplex.py:171` | raise AssertionError | values | `f'semantic mismatch in {('optimized' if improved else 'reference')} run for {name}'` | unaudited |
| `solver_dev/bench_synth.py:115` | raise AssertionError | values | `f'{family}(target={target}, seed={seed}, {int(g.sum())} cells): {arm} proves {r.opt} tile…` | unaudited |
| `solver_dev/diagnose_hand.py:72` | raise AssertionError | constant | `'the root LP dual was not certified'` | unaudited |
| `solver_dev/diagnose_hand.py:74` | raise AssertionError | values | `f'inconsistent root bounds: hand {bound}, subgradient {lag}, LP {lp}, optimum {opt}'` | unaudited |
| `solver_dev/diagnose_hand.py:76` | raise AssertionError | values | `f'inconsistent root bounds: hand {bound}, LP {lp}, optimum {opt}'` | unaudited |
| `solver_dev/diagnose_hand.py:80` | raise AssertionError | values | `f'hand solver disagrees with the certified optimum: opt {opt}, hand opt {r.opt} lb {r.lb}…` | unaudited |
| `solver_dev/diagnose_hand.py:88` | raise AssertionError | values | `f'a partition cheaper than the certified optimum {opt}'` | unaudited |
| `solver_dev/stress_blobs.py:69` | raise AssertionError | values | `f'blob(size={size}, seed={s}, width={width_for(size)}): {arm} proves {r.opt} tiles but Hi…` | unaudited |
| `stencil_cover/cover.py:331` | raise PartitionFault (via coverage_fault) | values | claim, uncovered and over-covered cell indices, chosen placements, mode, opt; bounded with totals | remediated (phase 3) |
| `stencil_cover/cover.py:391` | raise invariant (an AssertionError) | values | cells, group, least size, candidate count | remediated (phase 3) |
| `stencil_cover/cover.py:512` | raise AssertionError | values | `f'certificate lower bound {cert_lb} exceeds a verified feasible partition of {s.opt} tile…` | unaudited |
| `stencil_cover/hand_exact.py:329` | raise invariant (an AssertionError) | values | which masks are duplicated, counts | remediated (phase 3) |
| `stencil_cover/hand_exact.py:558` | raise invariant (an AssertionError) | values | component mask, anchor cell, anchored placements, target | remediated (phase 3) |
| `stencil_cover/hand_exact.py:590` | raise PartitionFault (replaces rows 574 and 577) | values | claim, tiles not allowed, doubly covered, uncovered and foreign cells as (y, x); bounded with totals | remediated (phase 3) |
| `stencil_cover/hand_exact.py:846` | raise invariant (an AssertionError) | values | search value, bound, tiles found, tile masks | remediated (phase 3) |
| `stencil_cover/hand_exact.py:935` | raise invariant (an AssertionError) | values | proved tiles, earlier best, lb, attempts, nodes | remediated (phase 3) |
| `stencil_cover/hand_exact.py:941` | raise invariant (an AssertionError) | values | tiles, ub, lb, improvements, attempts, tile masks | remediated (phase 3) |
| `stencil_cover/lagrange.py:53` | raise invariant (an AssertionError) | values | heaviest before and after scaling, scale, sizes | remediated (phase 3) |
| `stencil_cover/lns.py:45` | raise invariant (an AssertionError) | values | bound, tiles found, window tiles and mask | remediated (phase 3) |
| `stencil_cover/lpbb.py:161` | raise NoBranchingPair (an AssertionError) | values | node, LP status, fractional columns, pair-weight summary, in `detail` | evidence kept (phase 2b); fault not yet localized |
| `stencil_cover/simplex.py:206` | raise AssertionError | values | `f'steepest-edge weight of placement {bad} is {gam[bad]:.9g} after pivot {pivots} (enterin…` | unaudited |
| `stencil_cover/solver_api.py:104` | raise PartitionFault (replaces rows 99, 102, 104) | values | as hand_exact.verify_grid, plus tile count against opt | remediated (phase 3) |
| `stencil_cover/zdd_exact.py:133` | raise invariant (an AssertionError) | values | residual mask, order, cells | remediated (phase 3) |
| `stencil_cover/zdd_exact.py:208` | raise invariant (an AssertionError) | values | cells, budget, order, placements | remediated (phase 3) |
| `stencil_cover/zdd_exact.py:213` | raise invariant (an AssertionError) | values | min_cover tiles, mn, cells, order, diagram nodes | remediated (phase 3) |
| `stencil_study/extract.py:185` | raise AssertionError | constant | `'layout/ownership length disagree'` | unaudited |
| `stencil_study/family_study.py:122` | raise InstanceTotalsViolation (an AssertionError) | values | instances, base, new, signed delta, `shown n of m` | remediated (phase 2a); was specimen 1 |
| `stencil_study/tiers.py:73` | raise AssertionError | constant | `'allocation cannot place the remainder'` | unaudited |

Note: `values` is a mechanical class (the message contains a formatted value). It does not mean the message names the instance: `family_study.py:122` formats only a count.

Phase 2a (2026-11-10): row `family_study.py:122` no longer matches the `raise AssertionError` scan, so a regeneration would drop it; it is kept here as the record. `certify.py`'s violations are still strings (they name the instance key and both values); its report now states how many it lists of how many it found.
Phase 2b (2026-11-10): `lpbb.py:130` (now line 161) raises `NoBranchingPair` with its state in `detail`, and `solve_hand` runs the Ryan-Foster round outside its `except BudgetExceeded` handler, so the claim failure is primary (in `broken.txt` it was chained as raised while handling `BudgetExceeded`, which was the routine end of an attempt's allowance, not the cause). Like row 122, this row no longer matches the `raise AssertionError` scan.
Phase 3 (2026-11-10), partition verification: the three verification sites (`solver_api.verify_outcome`, `hand_exact.verify_grid`, `cover.verify`) shared one omission class (a constant message naming no tile or cell) and now raise `PartitionFault` (`stencil_cover/partition_fault.py`), the first shared mechanism; per the work order it was adopted because the class repeated across three sites. Rows 574, 577, 99, 102 and 104 are merged into the rows above. `hand_exact.py:540` is a different class (an internal reconstruction invariant) and stays for the next unit.
Phase 3 (2026-11-10), internal invariants: eleven sites with a constant or absent message over values the check had in hand now raise `invariant(...)` (`stencil_cover/invariant.py`; message plus JSON-able `detail`, bounded sequences, pickle-safe). Adopted because the class repeated across `hand_exact`, `lns`, `lagrange`, `zdd_exact` and `cover`. Not changed, deliberately: `cover.py:512` (certificate bound above a verified partition) and the `simplex` weight check already name their values, and the hunt's pool record supplies the instance, so they are `sound` pending the Phase 4 look at the hunt layer.

## Phase 3 triage: refusals and handlers in `stencil_cover` (proposal, 2026-11-10; no code changed)

Scope: every `raise` other than `AssertionError`/`invariant`/`PartitionFault` (72) and every `except` handler (12) in `stencil_cover`, found by AST
scan. Calibrated by the substitution test of ADR-0008: not by how benign the instance was that surfaced the shape, but by what the shape costs on the
worst surface it can reach. The worst surface here is a pool worker in a long hunt on a hard instance. `W` marks the 21 `stencil_cover` modules a
worker imports (`hard_region_hunt.solver` closure): every site below except `shapes`, `solver_api`, `stencil_cover_ref` and `zdd_exact` is on that path.

| Class | Sites | Severity (worst surface) | Proposed action |
|---|---|---|---|
| A. A violated claim about a real component, reported without the component | `eval_hand_exact.py:51` (oracle did not prove, no limit or component named), `:55` (oracle disagreement: only the size), `:80` (hand solver mismatch), `:82` (hand bounds wrong); `hard_region_hunt/engine.py:590` is the same shape (Phase 4) | High: this is where an instance costs hours | Dump the component points and every value to a replayable JSON, name the path in the message (the `violation.dump` pattern, for a stencil_cover-level caller); consider one shared dump once Phase 4 shows the third site |
| B. An internal claim raised as `RuntimeError` | `simplex.py:120`, `:160` ("LP is unbounded: impossible for a cover with singleton placements", constant), `nodedual.py:116` (names status, not the residual), `stencil_cover_ref.py:61`, `:72`, `:81` (backend status strings, no instance) | High for `simplex` (an LP defect at hunt scale is the `broken.txt` class), medium for the rest | `invariant(...)` with the state: iteration, entering column, ratio test, sizes (`simplex`); residual mask, options, status (`nodedual`); instance size and mode (reference backends) |
| C. A failure swallowed | `cover.py:282` (`ChildRunner.call`: a z3 child that crashed, a broken pipe and a timeout all return `None`, so a crash is read as "no answer in time" and the child's traceback goes to a daemon's stderr); `lagrange_nb.py:23`, `simplex_nb.py:15` (`except Exception` around `import numba`: any failure silently demotes `engine='auto'` to the numpy path) | High: a crash reported as a timeout, or a silently different engine, on a hunt | `call`: record how the child ended (timeout after N s / exit code / pipe error) on the runner and in the result note, keeping the `None` contract; numba: catch `ImportError`, keep the reason in `UNAVAILABLE_REASON` and in `solver_fingerprint` |
| D. Data validation with a constant message, on real data | `region.py:45`, `:48` (a policy with non-finite or out-of-range values: says neither how many nor where), `config.py:88` (duplicate shape names: names none), `shapes.py:26` | Medium: real ownership maps are the input | Count and first offenders (positions, values, names) |
| E. Option validation with a constant message | `cover.py:53`, `:55`, `:181`, `:562`; `hand_exact.py:397`, `:401`, `:720`, `:726`, `:738`; `simplex.py:64` | Low: deterministic from the options, cheap to reproduce, the offending value is the caller's argument | Name the offending value; the `hand_exact` option checks will move with the option-space refactor (ADR-0012 P1/P3), so editing them now is cheap but not final |
| F. Sound as they stand | 48 raises and 9 handlers: validation that names the offending value (`config`, `cover`, `geometry`, `hand_exact`, `nodedual`, `prng`, `simplex`, `zdd_exact`, ...), `BudgetExceeded` control flow, parse errors re-raised with the text, `solver_api` resolution, stale-pool messages with the remedy, `except BaseException: ...; raise` cleanup | n/a | None |

### Phase 3, class A done: the oracle checks in `eval_hand_exact` (2026-11-12)

`eval_hand_exact.oracle` and `run_budgets` (sites `:51`, `:55`, `:80`, `:82`) no longer end the interpreter from library code with a constant string. They raise
typed outcomes from `stencil_cover/oracle_fault.py`, and the two CLIs that call them (`eval_hand_exact.main`, `solver_dev/bench_solvers.main`) write the
component and every value compared to `OUT.fault.json` and exit.
- **Two outcomes, no longer one.** A solver that did not prove its optimum within the limit is `OracleUnevaluable` (exit 3), carrying the blocker, partial
  progress and what stays unknown. Before, CP-SAT timing out was reported as "ORACLE DISAGREEMENT ... CP-SAT -1: stop and report": an unevaluable check
  reported as a violated claim (ADR-0022 Rule 1). A proven disagreement is `OracleFault` (exit 2), with both solvers' values, certificate status, the time
  limit, and the component as points.
- **Hand-solver faults** carry the node budget being run, the hand result (value, bound, exact, nodes, tiles), the oracle value, whether the memo was seeded,
  the symmetry group size, and for the bounds claim which side is violated.
- **Hazards found on the way, fixed here:** the module documented "exit 2" while `SystemExit(str)` exits 1; the evidence writer first failed on a missing
  output directory and its `FileNotFoundError` replaced the fault (Rule 4: the evidence path must not mask the outcome), so the writer now creates the
  directory and, if the write still fails, says so and exits with the original outcome.
- **Not done here:** `hard_region_hunt/engine.py:590` has the same shape (hand versus HiGHS disagreement) and is Phase 4. After that a second use will say
  whether the component dump wants to be one shared mechanism with `stencil_study.violation.dump` (not decided).

### Phase 3, class C done: swallowed failures in `stencil_cover` (2026-11-12)

- **`cover.py` solver results (was `:282`, `ChildRunner.call`).** The row understated the problem: the z3 child runner returned `None` for a timeout, a broken
  pipe and a crash alike, but so did every backend's result: `Solved(-1, False, ...)` was one sentinel for a timeout (no incumbent within the limit), an
  infeasible model and a solver status outside its contract (ADR-0002 rule 3: a sentinel indistinguishable from a legitimate result is not loud enough).
  `Solved` now carries a required `status` (optimal, feasible, timeout, infeasible) and a `detail`; a timeout stays a status because it is a designed outcome of
  a budgeted solve and a witness of difficulty, with its own evidence (limit, wall time, the solver's reason); a crash or contract breach raises `SolverFailure`
  (`stencil_cover/solver_failure.py`) with backend, size, mode, limits and the solver's message. `ChildRunner.call` returns a `ChildReply` (reply, timeout, or
  died with the exit code); an exception inside the z3 child is sent back with its traceback; `CompResult` carries `status` and `status_detail` (old cached
  results read `unrecorded`). **Behaviour change, deliberate:** a z3 child crash, HiGHS "unbounded/other" without a point, CP-SAT `MODEL_INVALID` and z3
  `unknown` for a non-timeout reason used to become a quiet `-1`; they now raise. A HiGHS odd status *with* a point keeps the verified incumbent and records the
  oddity. Not yet carried further: `InstResult` and the hunt's records still aggregate `opt`/`proven` without the status (Phase 4).
- **`lagrange_nb.py:23`, `simplex_nb.py:15`.** `except Exception` around `import numba` now catches `ImportError` only: absence falls back silently, an installed
  but unimportable numba falls back with a `RuntimeWarning` naming the reason, anything else propagates; `UNAVAILABLE_REASON` is kept.
- **Found while doing this:** the two `test_solve_tier` stubs predated `solve_instance`'s `certify` parameter (a baseline failure, diagnosed and fixed); the
  `solve_tier` tests edited for exact-only on 2026-11-10 now run and pass.

### Phase 3, class B done: internal claims and backend failures raised as `RuntimeError` (2026-11-12)

- **`simplex.py:120`, `:160`** (the "LP is unbounded: impossible for a cover" claim, numpy loop and compiled loop): now an invariant violation with the engine, the
  entering placement and reduced cost, the largest entry of its column against the tolerance, the pivot counts, the basis and the range of basic values. The compiled
  loop does not return its entering column, so it is recomputed from the state it stopped in. A real cover LP has no free ray, so the test reaches the check through an
  abnormal tolerance (stated in the test).
- **`nodedual.py:116`**: `SolverFailure` carrying the residual mask and size, placements, LP status and message, target, node-dual spec.
- **`stencil_cover_ref.py:61`, `:72`, `:81`** (reference backends): a timeout or an infeasible model is a `NoIncumbent` with `status` and the instance; anything else a
  `SolverFailure`. Before, one bare `RuntimeError` covered all three.
- **Found, not changed (flagged):** `simplex.edge_weights` prints `slow path` on every call of its numpy fallback. It is the only operator-visible sign of the engine
  downgrade (now also a `RuntimeWarning` at import for an unimportable numba). It is stdout noise in long runs and not a diagnostic with evidence; its fate is a
  decision (a once-only warning carrying `UNAVAILABLE_REASON`, or removal) and is in the backlog.


### Phase 3, class D done: data validation on real data (2026-11-12)

- **`region.py:45`, `:48` (`legal_mask`)**: a policy with non-finite or out-of-contract entries now reports how many, by kind, a bounded sample with index and value, and the
  payload it came from (`where`, which `stencil_study.extract` fills with file and row: the sibling checks in that loop already did). The caller passing provenance in is
  the Rule 3 shape: the callee cannot know which of thousands of cache entries it was given.
- **`config.py:88` (`load_family`)**: duplicates are named with their counts; while there, the sibling raises of the same function got the same treatment: a block without a
  `name:` line names file and block, and "empty, or characters other than '#' and '.'" is split into two claims, the second listing the offending characters.
- **`shapes.py:26` (`_largest`)**: the rejected grid's shape is named; `shapes.make` adds family, size and seed as context without replacing the exception (the caller knows
  what the callee does not; `candidates.py` already records the same facts when it catches the error).
- **Mechanism:** `stencil_cover/input_error.py` (`InputError`, a `ValueError`, with `detail`, bounded counted samples and `add_context`); adopted because three sites in three
  modules had the same omission.
- **Found, not in this class:** `stencil_study/extract.py` still has `raise AssertionError("layout/ownership length disagree")` (constant, both lengths in scope): it belongs
  to the `stencil_study` assertion rows and is left for that unit.
