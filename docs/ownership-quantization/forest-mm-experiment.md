# Cardinality-aware forest and MM experiment

**Status:** offline research implementation; not a production ownership policy.

This is the first implementation tranche from the ownership-quantization
consultation. It implements the separable budget-exact initializer, an exact
cardinality-aware spanning-forest surrogate, and budget-exact
majorization-minimization (MM) passes over that forest. It deliberately does
not wire a new policy into the foreground request path.

## Reference problem and exact reduction

The input remains the coherent ownership objective:

- Empty points carry labels (q_i\in\{-1,0,+1\}).
- Dead Black and dead White points retain their fixed (+1) and (-1) labels.
- Living stones are absent from the graph.
- Spatial interactions are the original four-neighbour total variation.
- The hard integer interval is computed from the **original** empty-point ownership means and score tolerance.

For each empty point, let (d_i^+) and (d_i^-) count its fixed positive and
negative dead-stone neighbours. With

\[
a_i=1+2\kappa\sigma_i,\qquad
b_i=-2\mu_i+\beta(d_i^- -d_i^+),
\]

the reduced objective is

\[
F(q)=\sum_{i\in E}(a_i|q_i|+b_iq_i)
+\beta\sum_{(i,j)\in A_E}|q_i-q_j|.
\]

Here (A_E) contains only empty-empty edges. The omitted interactions with
fixed vertices contribute a label-independent offset:

\[
c=\beta\sum_i(d_i^++d_i^-)
+\beta\sum_{(i,j)\in A_{DD}}|s_i-s_j|,
\]

where (A_{DD}) is the set of dead-dead edges and (s_i\in\{-1,+1\}) is
the fixed dead-stone label. The original full energy equals (F(q)+c).

The aggregate interval is **not** adjusted by (b_i). It remains

\[
L\leq\sum_{i\in E}q_i\leq U,
\]

with (L,U) derived from the original (mu_i) values and score tolerance.
The implementation validates the reduced energy against the existing
full unary-plus-TV evaluator before writing a result.

## Implemented methods

### Budget-exact marginal initializer (B2)

For each point, the two ordered unary increments are

\[
f_i(0)-f_i(-1)=-a_i+b_i,\qquad
f_i(+1)-f_i(0)=a_i+b_i.
\]

Since (a_i\geq0), sorting all (2|E|) increments respects the within-point
precedence and gives the exact separable optimum for every admissible prefix.
The best prefix whose resulting net sum lies in ([L,U]) is selected.
This is a feasible initializer, not a spatial optimum.

### Cardinality-aware forest DP (E1)

A deterministic breadth-first spanning forest (T\subseteq A_E) is built.
The DP retains the current root label and integer subtree sum. Its local
objective is

\[
F_T(q)=\sum_i(a_i|q_i|+b_iq_i)
+\beta\sum_{(i,j)\in T}|q_i-q_j|.
\]

Each disconnected component produces a cost profile by total sum. A single
global min-plus convolution combines those profiles under the original
aggregate interval. No per-component target is imposed, so cross-component
zero-sum exchanges remain possible.

An optimum of (F_T) is exact **for the forest surrogate only**. For its
labeling, the report includes the omitted-edge contribution

\[
\Delta_R(q)=\beta\sum_{(i,j)\in A_E\setminus T}|q_i-q_j|.
\]

For the exact forest-DP optimum, this is also a valid additive upper bound
on its suboptimality under the original empty-empty graph: the forest objective
is a lower bound on the original optimum, and the omitted-edge energy is the
difference at the forest solution. It is not a measured gap or a claim that the
bound is tight. The report keeps the forest surrogate energy separate from the
restored fixed-edge constant.

### Budget-exact forest MM (E2)

For a feasible reference labeling (r), every omitted edge receives

\[
z_{ij}=\frac{r_i+r_j}{2}.
\]

The inequality

\[
|q_i-q_j|\leq|q_i-z_{ij}|+|q_j-z_{ij}|
\]

turns omitted edges into unary additions, yielding a forest objective that is
exactly solvable by the same global-cardinality DP. The majorizer touches the
original reduced energy at (r). Every accepted pass is checked for

\[
F(q^{t+1})\leq F(q^t).
\]

The run stops when a pass fails to improve the original energy within numeric
tolerance or the configured pass limit is reached. It retains the best feasible
labeling under the original full energy. Monotonicity is not a global
optimality guarantee.

## Offline experiment runner

The command reuses the existing read-only archive analyzer and full-energy
evaluator:

```sh
# Run the board-grouped training split.
python -m scripts.run_ownership_forest_mm --split train \
  --output /tmp/ownership-forest-mm.train.json

# Inspect validation after selecting candidate parameters.
python -m scripts.run_ownership_forest_mm --split validation \
  --output /tmp/ownership-forest-mm.validation.json

# Run the held-out evaluation only after decisions are fixed.
python -m scripts.run_ownership_forest_mm --split test \
  --output /tmp/ownership-forest-mm.test.json

# A small deterministic smoke run.
python -m scripts.run_ownership_forest_mm --split test --limit 5 \
  --output /tmp/ownership-forest-mm.smoke.json
```

The default database remains
`~/.local/state/kataproxy/ownership-quantization.sqlite3`. It is opened
read-only through `load_archive`. Results are written to a separate JSON file;
the output path is rejected if it equals the source archive, and an existing
output is not replaced unless `--overwrite` is specified.

Problems are deduplicated by the existing evidence identity plus exact
objective identity. Board groups are assigned deterministically using the
SHA-256 digest of the split seed and a board signature containing dimensions,
coordinate convention, and exact replayed board-state bytes:

- 70 buckets for training,
- 15 buckets for validation,
- 15 buckets for held-out testing.

Every evidence/objective problem sharing that board signature stays in the same
split. The archive has no game/session ID; this split is board-group leakage
prevention, not a claim about game chronology. The default split is `test`.
Do not tune forest construction or MM pass count against held-out results.

The proven-reference population is kept distinct from unresolved examples.
A result whose best archived reference is not proven optimal is labelled
`best_feasible_unproven_incumbent`; its energy difference is not called an
optimality gap. Where an archived lower bound is available, the report gives
the candidate-minus-bound additive upper bound. It never promotes the nine
unresolved incumbents to ground truth.

For each evaluated problem, the report stores:

- the B2 initializer, E1 forest-DP and E2 forest-MM full-energy results;
- aggregate interval, objective parameters, reference provenance, and split;
- per-method empty-point sum, expected sum, residual, and explicit feasibility;
- full-graph TV energy, unary energy, boundary-edge count, Hamming disagreement,
  and boundary symmetric difference;
- E1 omitted-edge TV contribution and its additive upper bound, plus the MM
  monotonicity trace;
- per-method wall time, forest DP state-entry count, component count, and
  compact labeling witnesses using the archive's
  `point-u32le-label-i8-v1` codec.

The DP state-entry count is a structural memory/workload diagnostic, not a
byte-accurate resident-memory measurement. If byte-level memory profiling is
needed, measure it separately from the wall-time campaign so profiler overhead
does not contaminate latency comparisons.

## Interpretation and next tranche

A forest optimum is not an optimum of the original grid graph. An MM result is
a feasible labeling with monotonic original-energy improvement, not a
certificate of global optimality. Keep those statuses distinct in any summary.

This tranche does not implement bounded-width strip DP, full-graph Lagrangian
dual certificates, or the continuous piecewise-linear TV relaxation. The
existing coherent MIP and coherent-cut implementations remain the production
baselines. No request-path policy, factory, layer ID, collection behavior,
Redis behavior, or evidence archive schema is changed.
