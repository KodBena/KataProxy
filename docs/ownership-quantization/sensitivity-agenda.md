# Ownership quantization — sensitivity agenda

This note records the problem of output sensitivity to small changes in
KataGo ownership evidence, the history-free levers that can improve intrinsic
stability, a measurement protocol, and disposal criteria. It is written so
that, once the ownership-evidence corpus exists (including sequences or near-
duplicate snapshots), the agenda can be turned into concrete tests without
re-deriving the modelling choices.

Related material:

- Mathematical problem and existing solvers:
  [../stencil-ownership-policy.md](../stencil-ownership-policy.md)
- Approximation agenda (orthogonal concern):
  [approximation-agenda.md](approximation-agenda.md)

## 1. Phenomenon

The coherent quantizer (MIP or cut) optimises a pure MAP energy on each
ownership snapshot independently. Small drifts in the continuous ownership
map \(\mu\) (and \(\sigma\)) can move the discrete optimum discontinuously:

- unary decision boundaries flip when \(\mu_i\) crosses a cost threshold,
- the global net-sum constraint can force a compensating flip in a distant
  region,
- the energy contains no term that prefers continuity with any previous
  discrete labeling.

This is expected behaviour for an unconstrained (or only cardinality-
constrained) Potts / metric-labeling energy on instances that are not
perturbation-stable in the sense of the MAP-inference literature. It is not
an artefact of HiGHS or of the Lagrange path.

## 2. Design constraint: history-free stability

Stability must be a property of the energy (and of any fixed rounding rule),
not of a dependence on a previous discrete solution \(q^{\mathrm{prev}}\).

A term that anchors the current solve to a concrete earlier labeling is an
*optional operational* regulariser. It may be useful for interactive
sessions, but it is outside the core model: the quantizer must already be
acceptably stable when run on a single ownership map with no history.

Consequently this agenda considers only static modelling changes and
deterministic continuous-to-discrete maps.

## 3. History-free levers

All of the following leave the policy interface and the ordered-label encoding
intact. They change the energy or the decoding rule, not the solver history.

1. **Stronger spatial weight**  
   Increase \(\beta\) (or add a mild higher-order / volume term). Larger
   contiguous regions become cheaper relative to isolated flips; the chance
   that a typical instance is perturbation-stable rises. Trade-off: lag in
   following genuine ownership changes.

2. **More conservative uncertainty / margin**  
   Strengthen the existing \(\kappa\sigma_i\) penalty, or introduce an explicit
   soft margin so that a non-neutral label is chosen only when \(|\mu_i|\)
   exceeds a threshold that itself depends on \(\sigma_i\). Fragile decision
   boundaries shrink.

3. **Soft aggregate instead of a hard interval**  
   Replace the hard net-sum band by a quadratic or Huber penalty on the
   residual \((\sum q_i - \sum \mu_i)^2\). Removes the global “must compensate
   somewhere” coupling that propagates local flips across the board.

4. **Continuous convex relaxation + fixed deterministic rounding**  
   Solve a convex relaxation of the ordered-label TV (or Potts) energy; round
   by a fixed rule (independent argmax, fixed co-area threshold, …). The
   continuous map is Lipschitz in the unaries; discrete flips become rarer and
   more predictable. Duality gap remains available as a certificate.

5. **Static “potential instability” penalty**  
   Soft penalty on points whose unary costs for two labels are close. Purely a
   function of the current \(\mu,\sigma\); no history.

Any combination of the above defines a new model identity and must be recorded
as such (parameter vector + algorithm) for caching and corpus comparison.

## 4. Measurement plan (once the corpus exists)

The corpus should ideally contain:

- near-duplicate ownership maps (same position, successive KataGo visits or
  small temperature / noise variation),
- short temporal sequences from the same game,
- controlled synthetic perturbations of real maps (additive noise on \(\mu\)
  scaled by \(\sigma\), or multiplicative edge-weight perturbations in the
  stability sense).

For each pair (or sequence) of related evidence records, compute on the
*same* quantizer configuration:

| Quantity | Definition |
| --- | --- |
| Hamming distance | Fraction of empty points whose discrete label differs. |
| Boundary TV distance | Absolute difference of total-variation values, or symmetric difference of ownership boundaries. |
| Net-sum jump | Change in empty-point net ownership. |
| Energy continuity | Change in full unary-plus-TV energy relative to the change in the continuous input (e.g. \(\ell_2\) or \(\ell_\infty\) drift of \(\mu\)). |
| Flip localisation | Whether flips are contiguous to existing boundaries or appear as distant compensations. |

Also measure, on single snapshots:

- fraction of points whose unary costs for the two best labels lie inside a
  small margin (proxy for fragile boundaries),
- size of the largest stable block under a simple adversarial unary or pairwise
  perturbation (diagnostic only).

## 5. Disposal criteria

A history-free configuration may be accepted when, on the held-out related
pairs / sequences:

1. **Small-input small-output** — for input drifts below a chosen continuous
   threshold (e.g. median \(|\Delta\mu_i|\) or \(\ell_2\) norm), the median Hamming
   distance stays below a tight bound (target to be set from the corpus
   baseline of the current coherent policy) and distant compensating flips are
   rare.
2. **No regression on single-shot quality** — energy relative to the current
   coherent reference, and downstream exact-cover behaviour, remain inside the
   tolerances already required by the approximation agenda (or by the existing
   coherent policy if no approximation is involved).
3. **Model identity is explicit** — every parameter that affects stability
   (\(\beta\), \(\kappa\), margin schedule, soft-vs-hard aggregate, rounding rule)
   is part of the recorded model identity.
4. **History-free** — the configuration does not read a previous discrete
   labeling. Any optional temporal stickiness is a separate, documented
   regulariser and is not required for the criteria above.

Failure on (1) with success on (2) indicates that stronger levers (or a
different continuous-to-discrete map) are still needed. Failure on (2)
indicates that the stability change has damaged the primary objective and must
be rolled back or re-tuned.

## 6. Relationship to the approximation agenda

Sensitivity and approximation are orthogonal:

- An approximation can be stable or unstable.
- A stable energy can be solved by MIP, cut, expansion, or convex relaxation.

The preferred path is to first obtain a history-free energy (and rounding) that
meets the sensitivity criteria, then to seek a fast solver for *that* energy
under the approximation agenda. Measuring both on the same corpus keeps the
trade-offs visible.

## 7. Explicit non-goals

- Guaranteeing formal \((\beta,\gamma)\)-stability for every board; the lattice-with-
  holes graphs that arise in practice need not satisfy the strongest
  literature conditions.
- Eliminating all discrete flips; ownership can change for genuine reasons.
  The goal is to eliminate flips that are disproportionate to the continuous
  evidence drift.
- Encoding temporal inertia into the core model via \(q^{\mathrm{prev}}\).
