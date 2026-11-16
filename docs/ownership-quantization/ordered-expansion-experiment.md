# Ordered-label expansion experiment

This is the first additional approximation strategy in the ownership
quantization investigation. It is an offline research method; it does not
change the production policy factory.

## Method

The solver starts with the budget-exact marginal labeling, then performs
deterministic alpha-expansion sweeps in label order `-1, 0, +1`. For each
alpha, every empty point chooses either its current label or alpha. Since
`beta * abs(q_i - q_j)` is a metric pairwise potential, each binary
expansion subproblem is submodular and is minimized by an s-t minimum cut.

Capacities are rounded to integer units at the requested scale (default
`1e9`). The scale is dynamically reduced for each move so that no individual
sum of all integer arc capacities stays at or below `1e9`; this keeps total
flow safely below signed-32-bit limits in SciPy's max-flow implementation. The result
records the requested scale; the effective scale is deterministic for each move. Final
proposals are always evaluated using the original floating-point energy.

An expansion move can leave the aggregate integer interval. Such a proposal
is projected to the nearest interval endpoint by the existing deterministic
connected-growth repair. The repaired proposal is accepted only if it is
feasible and strictly improves the original full unary-plus-TV energy. Thus
the returned energy never increases from the feasible initializer. Repair
does not preserve the classical unconstrained alpha-expansion approximation
guarantee; no global approximation ratio is claimed for the cardinality-
constrained problem.

## Latency–optimality-gap observations

The forest/MM comparison runner also executes ordered expansion. Each record
contains a `latency_gap_trace` with the marginal initializer (pass 0) and the
best feasible labeling after each complete expansion sweep. Latency is
cumulative wall time within the ordered-expansion call, including its
marginal initializer, repair, and cut construction. A trace point includes
the full energy, cut and repair counts, and a serialized labeling.

For each evidence/objective instance, `latency_gap_comparison_points`
collects comparable observations for the marginal initializer, forest DP,
forest MM, and every ordered-expansion checkpoint.
`latency_gap_pareto_frontier` retains the non-dominated points across those
methods for that instance. It must not be pooled across different boards or
objective parameter sets.

Gaps are interpreted conservatively:

- If the archived reference is proven optimal, `gap` is the exact additive
  optimality gap `candidate_energy - proven_optimum_energy`.
- Otherwise, if an archived valid lower bound exists, `gap` is the additive
  gap upper bound `candidate_energy - lower_bound`.
- Otherwise, no gap coordinate is reported. Difference from an unproven
  incumbent is not an optimality gap.

For corpus summaries, compare per-instance gap distributions and latency
percentiles, stratified by reference quality. Wall times are hardware/load
dependent, so run competing methods on the same machine and keep the source
archive and split seed fixed. The reported wall time for forest DP excludes
its separately reported initializer; forest MM and ordered expansion include
their marginal initializer. Keep those definitions in mind when comparing
end-to-end policy latency versus individual solver stages.

## Running it

The runner accepts the same read-only SQLite archive and held-out board split
as the forest/MM experiment:

```bash
python scripts/run_ownership_forest_mm.py \
  --database /path/to/ownership-quantization.sqlite3 \
  --split test \
  --output /path/to/ownership-ordered-expansion.results.json
```

Use `--limit 5` for a smoke run before running the complete split. The runner
refuses to overwrite an existing result file unless `--overwrite` is given.
It never modifies the archive.
