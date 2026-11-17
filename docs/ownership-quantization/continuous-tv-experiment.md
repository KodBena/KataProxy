# Continuous ordered-label TV experiment

## Relationship to the ordered-label LP experiment

The continuous model in this experiment is **not a distinct feasible-set relaxation** from the ordered-label LP introduced in [ordered-label-lp-experiment.md](ordered-label-lp-experiment.md).

Both use the same two ordered threshold variables per empty point: (a_i = 1[q_i \geq 0]), (b_i = 1[q_i = +1]), (0 \leq b_i \leq a_i \leq 1), and (q_i=a_i+b_i-1). Both minimize the same affine unary extension and two-threshold graph-TV term under the same hard aggregate interval. The LP expresses absolute edge differences with epigraph variables and is solved by HiGHS. This experiment solves the equivalent nonsmooth convex TV form directly with a primal-dual first-order method. The mathematical relaxation and lower bound are therefore comparable; solver and rounding variants are not new independent approximation families.

This distinction matters for the breadth-first survey: retain this implementation as a solver/rounding variation, but do not count it as a new relaxation family or infer that the LP and continuous-TV approaches provide independent bounds.

## Continuous formulation

The primal objective is the ordered-threshold affine unary term plus

\[\beta \sum_{(i,j)\in E} (|a_i-a_j|+|b_i-b_j|)\]

with \(L+n \leq \sum_i(a_i+b_i) \leq U+n\). The hard aggregate interval is not softened or dropped. Fixed dead-chain interactions remain in the existing reduced unary coefficients and fixed-edge offset.

The implementation uses deterministic Chambolle--Pock primal-dual iterations. Dual edge variables are projected onto \([-\beta,\beta]\); the aggregate dual variable uses the exact interval-indicator proximal operator; primal variables are projected onto the ordered triangle. A conservative operator-norm upper bound is used for the common step size.

The reported lower bound is the Fenchel dual objective at the returned dual iterate. The fractional primal iterate is interpolated toward the existing budget-exact feasible labeling when necessary to obtain a feasible continuous primal point for the primal/dual gap diagnostic. The code reports both the raw fractional aggregate residual and the feasible-primal energy. A finite iteration budget does not imply convergence: the convergence flag and iteration count are authoritative.

## Rounding variations

1. **Common-threshold co-area rounding.** Enumerate all distinct common thresholds induced by fractional \(a_i,b_i\) values (breakpoints and interval midpoints). This preserves the ordered-label relation pointwise. Every candidate is projected into the hard aggregate interval by deterministic one-unit moves using exact reduced unary and TV deltas. Select the feasible candidate with lowest original reduced energy.
2. **Simplex argmax rounding.** Interpret \((1-a_i,a_i-b_i,b_i)\) as the three label masses and choose the maximum-mass label with a fixed tie order. Apply the same deterministic aggregate repair if needed.

The aggregate repair is a feasibility projection, not an optimality certificate. All returned integral energies are cross-checked against the shared full-energy evaluator. The dual lower bound is checked against each feasible rounded candidate. No approximation-factor guarantee is claimed.

## Measurements

The offline comparison runner records solver latency, dual lower bound, feasible-primal energy, primal/dual gap, iteration count, convergence state, raw aggregate residual, number of co-area candidates, number requiring repair, and both rounded candidate energies and additive gap upper bounds. The two rounding outputs appear as separate methods on the per-instance latency/quality frontier; their common relaxation solve is charged once to both outputs.

This method remains offline research code. It does not alter the production ownership policy or the evidence, region-preparation, or exact-cover stages.
