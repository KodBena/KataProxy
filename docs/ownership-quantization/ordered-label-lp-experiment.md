# Ordered-label LP relaxation experiment

This is the second ranked approximation family in
[the approximation agenda](approximation-agenda.md). It is an offline
research solver and does not implement or enter the production policy path.

## Relaxation

For each ternary label q_i in {-1, 0, +1}, introduce ordered threshold
variables

- y_i1 = 1[q_i >= 0]
- y_i2 = 1[q_i >= +1]

with y_i2 <= y_i1 and q_i = y_i1 + y_i2 - 1. The unary term becomes affine
in the two thresholds. The chain metric decomposes exactly:

|q_i - q_j| = |y_i1 - y_j1| + |y_i2 - y_j2|.

Relax each threshold to [0, 1] and linearize both absolute differences with
non-negative edge auxiliaries. The original aggregate interval remains a hard
linear constraint. The resulting LP is a relaxation of the integer problem;
its optimum is a lower bound on the integer optimum, including the fixed-edge
constant.

The implementation uses SciPy HiGHS. The reported bound is the solver's
floating-point optimum plus the exact model constants. It is a numerical
relaxation bound, not an independently verified rational dual certificate.
Solver success, finite primal values, threshold ordering, and the relation
between the bound and the returned feasible labeling are checked.

## Deterministic rounding

A single common threshold is applied to both ordered variables at every
point. The implementation evaluates every distinct labeling induced by the
LP threshold breakpoints and the open intervals between them. This preserves
the ternary ordering. Only labelings satisfying the original aggregate interval
are eligible. If no threshold rounding is feasible, the budget-exact marginal
initializer is returned as a guaranteed feasible fallback.

The chosen labeling minimizes the original reduced objective among feasible
threshold roundings. The LP bound and feasible energy therefore expose a
per-instance additive gap upper bound. No integrality-gap claim is made for
the hard aggregate-constrained problem.

## Interpretation

This method adds a lower-bounding relaxation and a deterministic primal
candidate. It does not alter the coherent objective, aggregate interval,
archive, production policy, or evidence model. Compare it per evidence/objective
instance against the marginal initializer, forest DP/MM, and ordered expansion.
Do not pool latency/gap points across different instances.
