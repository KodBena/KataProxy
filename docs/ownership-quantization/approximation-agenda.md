# Ownership quantization — approximation agenda

This note records the problem, candidate methods, measurement plan, and
disposal criteria for a latency-oriented sibling of the coherent ownership
quantizer. It is written so that, once the ownership-evidence corpus exists,
the agenda can be turned into concrete tests without re-deriving the modelling
choices.

It deliberately does not prescribe an implementation. It only fixes the
reference problem, the quantities that must be compared, and the conditions
under which an approximation may be declared fit for production use.

Related material:

- Mathematical problem and existing solvers:
  [../stencil-ownership-policy.md](../stencil-ownership-policy.md)
- Policy contract:
  `transformers/stencil_cover/stencil_ownership.py`

## 1. Reference problem (unchanged)

The declared energy and constraint are exactly those of the coherent policy:

- Ternary labels \(q_i \in \{-1,0,+1\}\) on empty points; dead chains fixed;
  living stones absent from the graph.
- Unary costs
  \[
  C_i(0)=0,\quad
  C_i(+1)=1-2\mu_i+2\kappa\sigma_i,\quad
  C_i(-1)=1+2\mu_i+2\kappa\sigma_i.
  \]
- Four-neighbour total variation \(\beta\sum |q_i-q_j|\).
- Aggregate integer interval on empty-point net sum derived from
  \(\sum\mu_i\) and \(\epsilon\) (default \(\epsilon=0.5\)).

Defaults remain \(\kappa=1\), \(\beta=0.25\), \(\epsilon=0.5\) unless a deliberate
model-identity change is recorded.

Any approximation is judged against this energy (full unary-plus-TV, not the
MIP’s constant-offset objective). A shared energy evaluator is required for
all cross-policy comparisons.

## 2. Why an approximation is wanted

The direct MIP (and, on hard instances, the Lagrange-path cut) is not always
suitable for real-time analysis under a tight foreground budget. The
approximation agenda therefore seeks a method that:

- returns a feasible labeling (aggregate interval satisfied) within a
  deterministic or statistically tight latency bound,
- produces energy close to the reference optimum on the corpus,
- exposes an honest status vocabulary compatible with the existing policy
  contract,
- can be swapped in via the existing policy factory without changes to region
  preparation or exact-cover.

## 3. Candidate families (ordered by fidelity to the reference)

1. **Ordered-label expansion / fusion moves + cardinality repair**
   - Metric on the chain \(-1\!-\!0\!-+1\) remains submodular under expansion.
   - Classical factor-2 guarantee on the pairwise part; repair is the same
     local-move procedure already used for MIP seeding and cut repair.
   - Natural status: optimal when unconstrained optimum is feasible;
     feasible-supported / feasible-repaired otherwise.

2. **LP / metric-labeling relaxation + deterministic rounding**
   - Linear metric on three ordered labels has integrality gap 1 for the
     pairwise terms; cardinality handled by Lagrangian or soft penalty.
   - Rounding must be fixed and deterministic so that results are reproducible.

3. **Continuous convex Potts / TV relaxation + co-area or argmax rounding**
   - Duality gap supplies an a-posteriori bound.
   - Useful when a continuous certificate is desired; latency depends on the
     chosen first-order solver.

4. **Pure local search / ICM from the unary MAP, with aggregate repair**
   - Lowest implementation cost; weakest a-priori guarantee.
   - Acceptable only if corpus measurements show energy and mask quality
     indistinguishable from stronger methods for the intended use.

5. **Restricted MIP / cut on uncertain points only**
   - Freeze points whose unary MAP is unambiguous; optimise the residual
     graph. Hybrid that can still claim optimality on the free set.

History-dependent temporal stickiness is *out of scope* for this agenda; see
the sensitivity agenda for history-free stability levers.

## 4. Measurement plan (once the corpus exists)

For every ownership-evidence record in the corpus, compute:

| Quantity | Definition |
| --- | --- |
| Reference energy | Full unary-plus-TV energy of the best available coherent solution (MIP optimal if proved; otherwise best feasible incumbent or cut path). |
| Candidate energy | Same evaluator on the approximation’s labeling. |
| Energy ratio | candidate / reference (or additive gap). |
| Hamming / TV distance to reference | Pointwise and boundary disagreement. |
| Aggregate residual | Distance of empty-point net sum from the target interval (must be zero for feasibility). |
| Wall-clock / CPU time | Under the same budget policy used in production. |
| Status | optimal / feasible_supported / feasible_repaired / seed_only (or equivalent). |

Aggregate over the corpus:

- Distribution of energy ratios and additive gaps.
- Fraction of instances that remain feasible without repair.
- Latency percentiles (p50, p95, p99) versus the MIP and cut baselines.
- Qualitative spot-checks on boards where the MIP previously timed out or
  produced visible speckle.

## 5. Disposal criteria

An approximation may be promoted to a production policy when, on the held-out
portion of the corpus:

1. **Feasibility** — 100 % of returned labelings satisfy the aggregate interval
   (or the policy explicitly reports failure rather than a silent violation).
2. **Energy** — median energy ratio ≤ 1.05 and p95 ratio ≤ 1.15 relative to the
   reference (or an additive gap small enough that visual and downstream
   exact-cover behaviour is unaffected).
3. **Latency** — p95 wall time under the configured foreground budget, with
   no unbounded native calls that can overrun the reserve.
4. **Honesty** — status vocabulary distinguishes proved optima from repaired
   or seed-only answers; diagnostics are sufficient for later audit.
5. **Interface** — implements `OwnershipRegionPolicy` without requiring changes
   to evidence construction, region preparation, or exact-cover.

Failure on any of the above keeps the method experimental. The corpus and the
shared energy evaluator are the sole arbiters; qualitative preference alone is
not sufficient for promotion.

## 6. Implementation notes for later

- Re-use the ordered-bit encoding and the existing repair routine wherever
  possible so that energy numbers stay comparable.
- Record the approximation’s own model identity (algorithm + parameter set) in
  the same way the coherent policies record \(\kappa,\beta,\epsilon\).
- Prefer deterministic behaviour under a fixed random seed or a fully
  deterministic algorithm; non-determinism must be documented and measured.
- When the MIP later proves a tighter optimum on a corpus item, the
  approximation’s energy ratio can only improve or stay the same; never
  re-tune the approximation against a weaker reference.
