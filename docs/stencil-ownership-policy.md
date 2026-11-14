# Stencil ownership quantization and solver policies

This document describes the ownership-region optimization used by stencil-cover
enrichment. It records the mathematical problem, its provenance, the distinction
from exact-cover solving, and the implementation decisions that matter when
building or auditing another solver for the same input.

## Scope and place in the pipeline

An analyze request opts in to stencil-cover enrichment with the
\`capabilities.stencil_cover\` capability. The transformer requests KataGo's
ownership map; policies that need uncertainty also request
\`ownershipStdev\`. The transformer reconstructs the board at the response's
\`turnNumber\` from the submitted initial stones and move sequence.

The pipeline then has distinct stages:

1. **Evidence construction** validates the dimensions and numeric ranges of the
   ownership and standard-deviation arrays, replays the board, identifies empty
   intersections, and classifies dead chains using the shared chain-liveness
   heuristic.
2. **Ownership quantization** maps the evidence to three dispositions:
   positive (White-owned), neutral, or negative (Black-owned). This is the
   optimization discussed in this document.
3. **Region preparation** splits the selected positive and negative masks into
   four-connected components and canonicalizes each component.
4. **Exact cover** tiles each already-selected component with the configured
   stencil family, minimizing tile count. This has its own solver, cache and
   result statuses.

Ownership classification is therefore not part of the exact-cover problem.
The exact-cover solver cannot add an omitted White point to the White mask or
exchange it with a Black point. It only tiles the masks it receives.
Conversely, ownership optimization does not choose stencil placements.

The ownership policy port is deliberately upstream of region preparation and
exact cover. The policy contract is in
[stencil_ownership.py](../transformers/stencil_cover/stencil_ownership.py);
the orchestration is in
[stencil_region_solver.py](../transformers/stencil_cover/stencil_region_solver.py).

## Provenance and purpose

The original ownership-region behavior was an independent per-intersection
threshold. Each empty point was selected for one side or left neutral based on
its ownership value and the client-supplied truncation threshold. That behavior
remains available as the \`threshold\` policy.

The coherent policy was introduced as a different ownership-region rule. Its
purpose is to turn a noisy, pointwise ownership estimate into two spatially
coherent masks while retaining the aggregate signed ownership implied by the
ownership map. Independent thresholds make every point decision in isolation;
they do not coordinate neighbouring decisions, and they do not impose one
global signed-sum constraint. The coherent model makes both effects explicit.

The input is KataGo's ownership evidence, not a separate Go rules solver. The
policy does not prove life and death, calculate territory from first principles,
or optimize KataGo's value head. The existing dead-chain classifier is a
separate heuristic that fixes dead Black chains as White-owned and dead White
chains as Black-owned. It mirrors KataGo's chain-level anticipated-status
heuristic: a White chain is considered alive only when its average ownership
is greater than 0.2 and its minimum point ownership is greater than -0.6; a
Black chain is considered alive only when its average is less than -0.2 and
its maximum point ownership is less than 0.6. A chain failing the corresponding
test is treated as dead. This uses the ownership mean, not ownershipStdev, and
is not a new life-and-death proof. Living stones are excluded from the ownership
cover. The coherent objective operates on the remaining empty intersections
and those fixed dead-chain points.

The mathematical model below is a modelling choice over those inputs. Its
weights are not calibrated probabilities or a claim that the objective is a
complete model of Go territory. In particular, the aggregate target is the sum
of the pointwise ownership means; it is not \`scoreLead\` and is not an explicit
constraint on the value-head estimate.

## The ownership quantization problem

Let:

- \(E\) be the set of empty intersections on the replayed board.
- \(D_B\) be the intersections in chains classified as dead Black.
- \(D_W\) be the intersections in chains classified as dead White.
- \(V = E \cup D_B \cup D_W\) be the eligible spatial graph vertices.
- \(A\) be the undirected four-neighbour edges whose two endpoints are both
  in \(V\).
- \(\mu_i\) and \(\sigma_i\) be KataGo's ownership mean and standard deviation
  at empty point \(i\).

For each empty point, choose a ternary label

\[
q_i \in \{-1,0,+1\},
\]

where \(+1\) means White-owned, \(-1\) means Black-owned, and \(0\) means
unassigned. Dead-chain labels are fixed:

\[
q_i=+1\quad(i\in D_B),\qquad
q_i=-1\quad(i\in D_W).
\]

Living stones are not vertices in \(V\). They therefore do not receive an
ownership label, do not contribute to the aggregate constraint, and break
four-neighbour paths between otherwise adjacent empty areas. Dead chains are
vertices with fixed labels: they affect the spatial boundary cost and the
reported net ownership, but not the empty-point aggregate constraint.

The declared objective for empty points is

\[
C_i(0)=0,
\]

\[
C_i(+1)=1-2\mu_i+2\kappa\sigma_i,
\qquad
C_i(-1)=1+2\mu_i+2\kappa\sigma_i.
\]

The full energy is

\[
\mathcal{E}(q)=
\sum_{i\in E} C_i(q_i)
+
\beta\sum_{(i,j)\in A}|q_i-q_j|.
\]

The aggregate constraint is

\[
\left|
\sum_{i\in E}q_i-\sum_{i\in E}\mu_i
\right|\le\epsilon.
\]

Equivalently, the integer empty-point sum must lie in

\[
L \le \sum_{i\in E}q_i \le U,
\]

where

\[
L=\max\left(-|E|,
\left\lceil\sum_{i\in E}\mu_i-\epsilon\right\rceil\right),
\qquad
U=\min\left(|E|,
\left\lfloor\sum_{i\in E}\mu_i+\epsilon\right\rfloor\right).
\]

The implementation applies a small \(10^{-9}\) numerical margin around the
ceil/floor operations. If the interval contains no attainable integer, the
policy raises an error rather than silently changing the constraint.

### Meaning of the unary costs

The unary expression can be written as

\[
C_i(q_i)=q_i^2-2q_i\mu_i+
2\kappa\sigma_i\,\mathbf{1}[q_i\ne0].
\]

For \(q_i\in\{-1,0,+1\}\), this is the difference in squared error
\((q_i-\mu_i)^2-\mu_i^2\), plus an uncertainty surcharge for making a
non-neutral assignment. This gives the stated three costs.

This identity explains the shape of the formula; it does not make it a
probabilistic likelihood. The \(\kappa\sigma_i\) term is a chosen confidence
penalty: as the ownership standard deviation increases, a non-neutral
classification becomes more expensive. \(\kappa\) controls the strength of
that penalty.

### Meaning of the spatial term

The graph is the orthogonal board lattice induced by eligible points.
Diagonal contacts do not create edges. The total-variation term charges
\(\beta\) for a one-unit label change and \(2\beta\) for a direct
White/Black boundary. It discourages isolated changes and fragmented
boundaries; it does not impose a hard connectedness constraint on either
ownership colour.

The dead-chain vertices are included with fixed labels so a neighbouring empty
point pays the corresponding boundary cost. Living stones are absent, so the
spatial graph may be disconnected.

### Meaning and limits of the aggregate constraint

The sum constraint couples all empty points, including points in spatially
disconnected components. It permits a zero-sum exchange: an extra White point
in one area can be balanced by an extra Black point elsewhere. There is no
separate per-region conservation law.

The target is the sum of the empty-point ownership means. The signed
contribution of dead chains is added to the diagnostic target and actual net
values, but the hard constraint itself is applied only to empty points.
Neither dead nor living stones alter the expected empty-point sum.

With the default \(\epsilon=0.5\), the interval normally selects the nearest
attainable integer net sum; at an exact half-integer it may contain two
integers. A wider tolerance can admit more integer sums. A narrower tolerance
can make the integer interval empty and is rejected.

## Why the constrained problem is treated as NP-hard

The difficulty is not the ternary labels or the four-neighbour total variation
in isolation. The unconstrained ordered-label energy has a graph-cut
representation. The global integer-sum constraint is the additional coupling
that makes the general problem a cardinality-constrained graph-labeling
problem.

A special case using the unary formula itself makes the complexity
boundary explicit. Let the graph have an even number of vertices, set every
ownership mean to \(\mu_i=0.5\), every standard deviation to zero, and set
\(\epsilon=0\). Then the expected sum is \(|V|/2\), and the unary costs are
\(C_i(0)=0\), \(C_i(+1)=0\), and \(C_i(-1)=2\). Choose a positive
\(\beta\) small enough that \(\beta |A|<2\). A feasible assignment using
only labels 0 and +1 has exactly \(|V|/2\) positive labels and has energy at
most \(\beta|A|<2\). Any assignment containing a -1 label pays at least 2
in unary cost and has non-negative spatial cost, so it cannot be optimal.
The remaining optimization therefore chooses exactly half the vertices as
+1 and the rest as 0, minimizing

\[
\beta\sum_{(i,j)\in A}|q_i-q_j|,
\]

which is minimum graph bisection on the underlying graph. Minimum bisection is
NP-hard on general graphs. Thus the general graph-labeling family defined by
this objective and aggregate constraint contains an NP-hard special case.

This establishes the NP-hardness of the general graph formulation, not a
hardness result for every restricted adjacency class. The production
adjacency graph is more specific: it is a four-neighbour board-lattice
subgraph with holes induced by excluded living stones. The reduction above
does not prove a matching worst-case lower bound for every such restricted
board graph, nor does it imply that every ordinary Go position is difficult.
The operational board-size cap and structure may make many real instances
tractable.

This is the reason to treat the global ownership quantization problem as an
NP-hard optimization problem and to design for feasible answers, bounded
foreground effort, and subsequent evidence-based improvement. The reduction
is a statement about the general graph problem family. The production
adjacency graph is more specific: it is a four-neighbour board-lattice
subgraph with holes induced by excluded living stones. This reduction alone
does not prove a matching worst-case lower bound for every such restricted
board graph, nor does it imply that every ordinary Go position is difficult.
The operational board-size cap and structure may make many real instances
tractable.

A useful distinction follows:

- Without the aggregate constraint, the coherent unary-plus-TV problem is
  solvable by one minimum cut in this ordered-label encoding.
- With the aggregate constraint, a single unconstrained cut may have the
  wrong net sum. A Lagrange-multiplier path gives exact solutions to a
  sequence of unconstrained subproblems, but the discrete path can skip
  allowed integer sums. The direct MIP represents the aggregate constraint
  explicitly; the graph-cut sibling has to handle skipped sums separately.

## Ordered binary encoding shared by the two coherent solvers

Both coherent implementations use two ordered binary indicators:

\[
q_i=a_i+b_i-1,\qquad b_i\le a_i,
\]

with

\[
-1=(0,0),\qquad 0=(1,0),\qquad +1=(1,1).
\]

For these ordered labels,

\[
|q_i-q_j|=|a_i-a_j|+|b_i-b_j|.
\]

This identity is load-bearing. It makes the four-neighbour total-variation
term a sum of two binary Potts terms, which is suitable for both the linear
MIP formulation and an s-t minimum cut.

The empty-point sum also becomes linear:

\[
\sum_{i\in E}q_i
=
\sum_{i\in E}(a_i+b_i-1).
\]

The two binary variables are ordered by an explicit constraint. Fixed dead
chains are fixed to the corresponding pair of bits. No binary variable is
needed for living stones because they are not in the eligible graph.

## The \`coherent\` policy: direct constrained MIP

The default \`coherent\` policy is implemented in
[stencil_ownership_coherent.py](../transformers/stencil_cover/stencil_ownership_coherent.py).
It represents the declared constrained problem directly as a mixed-integer
linear program.

### Variables and constraints

For \(n=|V|\) eligible points and \(m=|A|\) spatial edges, the formulation
uses:

- \(2n\) ordered binary label variables, two per eligible point;
- \(2m\) continuous auxiliary variables, one per edge and ordered bit.

For each point it enforces \(b_i\le a_i\). For each edge and each bit, two
linear inequalities make the auxiliary variable at least the absolute
difference of the endpoint bits. Its positive objective coefficient makes
the optimum set it to that absolute difference. The aggregate interval is one
explicit linear constraint over empty-point label bits. Fixed dead-chain
labels are encoded as fixed variable bounds.

Only the \(2n\) label columns are integer. The total-variation auxiliaries
are continuous; their lower-bound inequalities and positive objective costs
are sufficient for them to take the required absolute-difference values at
an optimum.

The unary coefficients in the linear objective are

\[
-a_i C_i(-1)+b_i C_i(+1),
\]

where the displayed expression means the coefficient of \(a_i\) is
\(-C_i(-1)\) and the coefficient of \(b_i\) is \(C_i(+1)\). This omits the
label-independent constant \(\sum_{i\in E}C_i(-1)\) from the objective.
Omitting that constant does not change the minimizing labeling, but it does
shift the reported MIP objective value. **The MIP policy's current
\`ownershipDiagnostics.objective\` is therefore offset by
\(-\sum_{i\in E}C_i(-1)\) relative to the full unary-plus-TV energy.**
The graph-cut policy's \`objective\` diagnostic is computed from the full
unary-plus-TV energy. Raw objective diagnostics from the two policies must
not be compared without restoring this constant or normalizing both through
one shared energy evaluator.

### Feasible start and foreground budget

A solver time limit alone is not a useful live-response contract if model
construction or the lack of an incumbent can consume the whole request.
The MIP implementation therefore constructs a feasible labeling before
branch-and-bound:

1. Start at the independent unary minimum for each empty point; dead-chain
   labels are already fixed.
2. Compute the nearest target integer in the allowed interval using
   \(\lfloor \text{expected net}+0.5\rfloor\), clipped to the interval.
3. If the initial empty-point sum differs from that target, make one-unit
   label moves in the required direction. A priority queue orders moves by
   their current unary-plus-neighbour-TV marginal cost. After a move, the
   moved point and its empty neighbours have their marginal costs refreshed.
4. Encode the resulting labels and exact auxiliary total-variation values as
   a complete MIP start.

This is a deterministic feasible seed, not a global optimum and not a claim
that greedy one-cell moves solve the constrained problem. Its purpose is to
ensure a valid aggregate-feasible answer exists before the optimizer spends
its remaining budget.

The implementation uses the direct \`highspy.Highs\` API rather than the
higher-level SciPy \`milp\` wrapper. During development, the SciPy path showed
approximately 5.8–7 seconds of wall-clock time on synthetic 25x25 smooth or
uniform inputs despite a requested 250 ms solver limit. That was an observed
integration/runtime result for those fixtures, not a universal benchmark of
SciPy. The direct API was adopted to reduce adapter overhead and make the
time budget more explicit.

The current implementation also:

- converts the assembled rows directly to the CSR arrays expected by
  \`Highs.addRows\`, avoiding another sparse-matrix conversion and per-row
  calls;
- runs HiGHS with one thread to make foreground resource use more predictable;
- counts model construction and seed creation against the same end-to-end
  deadline;
- gives the solver 80% of the remaining time and retains a reserve for
  returning and validating the result;
- returns the feasible seed if optimization cannot start or does not yield
  another valid incumbent.

The current status vocabulary is \`optimal\` or \`feasible_time_limit\`.
Only the former means HiGHS reported an optimal model status. The latter means
the returned labeling satisfies the model constraints but optimality was not
proved. There is an audit limitation: the current implementation uses
\`feasible_time_limit\` both when HiGHS returns a feasible incumbent and when
the precomputed seed is returned without a usable solver incumbent (including
when setup has consumed the budget). It does not currently expose an explicit
\`seed_only\` disposition, the MIP best bound/gap, or a full elapsed-time
breakdown. Those distinctions should be made explicit in any future common
attempt/outcome contract.

## The \`coherent-cut\` policy: exact cuts plus an approximate aggregate path

The optional \`coherent-cut\` policy is implemented in
[stencil_ownership_cut.py](../transformers/stencil_cover/stencil_ownership_cut.py)
and
[stencil_ownership_graphcut.py](../transformers/stencil_cover/stencil_ownership_graphcut.py).
It keeps the same unary energy, spatial graph, and aggregate interval as the
MIP, but uses a different strategy.

### Fixed-multiplier subproblem

For a fixed Lagrange multiplier \(\lambda\), it minimizes

\[
\mathcal{E}(q)+\lambda\sum_{i\in E}q_i.
\]

Under the ordered-bit encoding, adding the Lagrange term adds \(\lambda\) to
both bit coefficients for each empty point, up to a label-independent
constant. The resulting binary energy is submodular:

- unary coefficients become terminal source/sink capacities;
- the implication \(b_i\le a_i\) is represented as a hard directed arc;
- fixed dead-chain bits are represented by hard source/sink arcs;
- each board edge contributes symmetric pairwise arcs for the two bits.

The minimum cut is found with SciPy's sparse maximum-flow implementation
using Dinic's method. This is an exact minimum cut for the integer-capacity
graph supplied to the flow routine. Finite capacities are rounded to integer
units at a scale of \(10^9\); thus the graph-cut subproblem is exact for that
quantized-capacity representation, not necessarily for distinctions smaller
than the capacity resolution in the original floating-point energy. The
implementation computes a finite upper bound on any ordinary cut and uses a
larger capacity for hard constraints, so an optimal cut has no reason to
violate ordered labels or fixed dead-chain labels.

### Searching the discrete Lagrange path

At \(\lambda=0\), the cut gives the unconstrained minimum of the declared
unary-plus-TV objective (to the graph capacity resolution). If that labeling
already satisfies the aggregate interval, it is also a global optimum of the
constrained problem: it minimizes the objective over the larger unconstrained
set and is feasible for the constrained set.

Otherwise, the policy searches a bounded monotone multiplier interval by
bisection. Increasing \(\lambda\) penalizes positive net ownership and moves
the cut solution toward lower sums. The bounds include the maximum unary
coefficient magnitudes and a bound on local total-variation influence. The
implementation makes at most 24 cut calls including the initial
\(\lambda=0\) call, subject also to a wall-time reserve.

The set of attainable sums is discrete. A monotone Lagrange path need not
visit every integer sum: a jump can move from above the permitted interval to
below it. This is why parametric cuts alone are not an exact solution method
for the hard aggregate constraint.

### Feasibility repair and honest status

If no path cut satisfies the aggregate interval, the policy takes the
best path labeling by aggregate residual (breaking equal-residual ties by
the declared energy) and uses
[stencil_ownership_repair.py](../transformers/stencil_cover/stencil_ownership_repair.py)
to move one empty-point label at a time toward the nearest target integer.
A priority queue chooses the lowest current unary-plus-TV marginal move and
refreshes costs for the moved point and its neighbours.

The repair has an additional shape rule. If a same-sign seed already exists
in an eligible connected component, a newly created cell of that sign must
touch a same-sign neighbour. A first seed is allowed in a component that has
none. This prevents the repair procedure from creating a row-major sequence
of isolated new seeds. It does **not** merge or remove disconnected same-sign
components that were already present in the path labeling; it is not a
general connected-region constraint. Eligible components are separated by
living stones.

The policy distinguishes three outcomes:

- **\`optimal\`**: the \(\lambda=0\) unconstrained optimum is feasible, or the
  allowed interval contains exactly one integer and a path cut reaches that
  integer. In the singleton case, all feasible labelings have the same sum,
  so the Lagrange term is constant over the constrained feasible set; a
  global solution of that fixed-multiplier subproblem is also a constrained
  optimum. This status is subject to the integer-capacity precision described
  above.
- **\`feasible_supported\`**: a path cut is inside a multi-integer interval,
  but neither of the preceding optimality arguments applies. The labeling is
  feasible; the policy does not claim it is the global constrained optimum.
- **\`feasible_repaired\`**: no path cut reaches the interval, so the greedy
  connected-growth repair produces a feasible labeling. It is not claimed to
  be the global constrained optimum.

A small 2x2 test fixture happens to reach the enumerated full-energy optimum
after repair, but the test deliberately asserts \`feasible_repaired\` and
\`optimal=False\`. That fixture is evidence that the result is good on that
case, not a proof for the general repair path.

The policy records the path sum and residual, selected multiplier, cut-call
count, repair moves and objective delta, wall time, time-budget overrun flag,
and capacity scale. A single native maximum-flow call can itself overrun the
nominal budget; the reserve only bounds when another call is started, not the
duration of an in-progress native call. The graph-cut policy is therefore a
latency-oriented sibling, not a replacement proof that the constrained
problem has become one minimum-cut problem.

## Legacy \`threshold\` policy

The original per-point policy remains available with
\`KATAPROXY_STENCIL_OWNERSHIP_POLICY=threshold\`. Each empty point is selected
independently when its signed ownership crosses the client-supplied
\`truncation_threshold\`; a zero threshold selects strictly positive or
negative values and leaves exact zero neutral. Dead chains are assigned to
their opponent's ownership, and living stones are excluded.

The threshold policy does not request or require \`ownershipStdev\`.

## Configuration and response diagnostics

The policy is selected at process startup with
\`KATAPROXY_STENCIL_OWNERSHIP_POLICY\`:

- \`coherent\` is the default and selects the bounded HiGHS MIP.
- \`coherent-cut\` selects the graph-cut path and optional connected-growth
  repair.
- \`threshold\` selects the legacy independent cutoff.
- \`python.module:factory_name\` eagerly imports a custom policy factory.

The factory must return an implementation of the
\`OwnershipRegionPolicy\` protocol. It declares \`policy_id\`,
\`requires_ownership_stdev\`, \`validate_request_metadata(metadata)\`, and
\`classify(evidence)\`. A new policy is a sibling implementation; it does not
require the board replay, component preparation, or exact-cover solver to
know its algorithm.

Current defaults are:

| Variable | Default | Meaning |
| --- | ---: | --- |
| \`KATAPROXY_STENCIL_OWNERSHIP_CONFIDENCE_KAPPA\` | \`1.0\` | Penalty for making non-neutral assignments at uncertain points |
| \`KATAPROXY_STENCIL_OWNERSHIP_SPATIAL_BETA\` | \`0.25\` | Cost per unit of label difference across an orthogonal edge |
| \`KATAPROXY_STENCIL_OWNERSHIP_SCORE_TOLERANCE\` | \`0.5\` | Allowed difference between quantized and expected empty-point ownership sums |
| \`KATAPROXY_STENCIL_OWNERSHIP_SOLVER_TIME_LIMIT_MS\` | \`250\` | Per-response policy budget |

These are initial modelling values, not calibrated constants. Changing them
changes the optimization problem and therefore belongs in the model identity
for any future cache or corpus.

The diagnostics are included in
\`opaque.stencilCover.ownershipDiagnostics\`. Both coherent policies report
expected and quantized net ownership, aggregate residual, point and edge
counts, solver status, and optimality indication. The graph-cut policy also
reports Lagrange-path and repair diagnostics. The MIP currently reports an
objective with the constant offset described above.

## Audit boundary and evidence-collection gap

The ownership classifier currently runs directly inside
\`OwnershipRegionPolicy.classify(evidence)\`. It is not yet routed through the
shared stencil solver runtime, solver-attempt corpus, persistent result cache,
or long-budget background worker. The current response diagnostics are not a
durable record of the full optimization input and all solver evidence.

The existing stencil exact-cover infrastructure is useful precedent, but its
problem and result types are specific to tiling:

- the exact-cover canonical input is a normalized occupied-cell shape plus a
  stencil family;
- its incumbent is a tile count and a placement witness;
- its quality ordering and cache codec validate stencil placements.

Those are not honest substitutes for an ownership evidence snapshot or a
ternary labeling. Reuse of the solver lifecycle, event transport, durable
spool, corpus materializer, work-profile queue, provenance conventions and
longer-budget re-solve pattern is a separate question from reusing
placement-specific contracts.

If an ownership corpus / background solver is added, the reproducible problem
identity needs to include at least:

- board width and height and a stable board-coordinate convention;
- the lossless ownership mean and standard-deviation arrays;
- the replayed board disposition, including empty points and the fixed
  dead-chain sets (or enough source board and turn data to reconstruct them);
- the ownership-model/evidence schema version;
- the effective objective parameters and aggregate tolerance.

The problem identity should not include a solver time limit. Solver algorithm,
implementation version, configuration, and requested work budget belong to
attempt/work-profile provenance, separate from the identity of the evidence
being solved. Do not round ownership or standard-deviation inputs when
creating a reusable problem key: rounding can merge distinct optimization
instances. Preserve the original evidence losslessly in the corpus even when
an exact-input result-cache hit is unlikely.

A useful experiment record would retain the initial heuristic seed separately
from each optimizer incumbent, along with the objective in a common
normalization, any proven lower bound and gap, elapsed time, solver status,
and algorithm/configuration provenance. This allows later comparison of
time-shortcircuited heuristics against longer-budget and proven-optimal
solutions. It also permits comparison of masks, aggregate residuals and
region shape. The value-head estimate and final game outcome may be retained
as contextual evaluation data, but neither should silently become a hard
constraint in this ownership objective.

An exact optimum is a gold reference for this mathematical model, not proof
that the model perfectly represents Go territory. Keep model quality
(agreement with outcomes or expert judgments) distinct from solver quality
(gap to the optimum of the declared objective).

## Tests and known evidence

The current focused tests include:

- a small MIP instance exhaustively compared against all feasible ternary
  labelings for the explicit unary-plus-spatial objective;
- a synthetic 25x25 MIP case checking that a feasible aggregate-respecting
  answer survives the bounded foreground call;
- a small graph-cut fixture compared with exhaustive energy enumeration;
  its repaired result is explicitly not marked optimal even though that
  fixture reaches the enumerated optimum;
- a 25x25 diffuse-ownership case checking aggregate and connectivity
  properties on that fixture;
- a 5x5 skipped-integer case checking that repair reaches the required sum
  without adding disconnected stipple in that fixture.

These tests validate particular encodings, objective calculations, feasibility
and selected shape/performance properties. They are not a held-out corpus
evaluation, a calibration of \(\kappa\) or \(\beta\), or evidence that the
current objective is a complete territory model. No ownership corpus or
longer-budget ownership worker is currently implemented.

## Relevant implementation and history

- [stencil_ownership.py](../transformers/stencil_cover/stencil_ownership.py):
  evidence validation, shared dead-chain interpretation, policy contract.
- [stencil_ownership_coherent.py](../transformers/stencil_cover/stencil_ownership_coherent.py):
  direct constrained MIP and feasible start.
- [stencil_ownership_cut.py](../transformers/stencil_cover/stencil_ownership_cut.py):
  Lagrange path and result-status policy.
- [stencil_ownership_graphcut.py](../transformers/stencil_cover/stencil_ownership_graphcut.py):
  ordered-bit graph construction and fixed-multiplier minimum cut.
- [stencil_ownership_repair.py](../transformers/stencil_cover/stencil_ownership_repair.py):
  energy calculation and deterministic aggregate repair.
- [stencil_region_solver.py](../transformers/stencil_cover/stencil_region_solver.py):
  ownership selection followed by region preparation and exact-cover calls.
- [stencil_region_components.py](../transformers/stencil_cover/stencil_region_components.py):
  four-connected component splitting and canonical geometry.
- [stencil_cache_storage.py](../transformers/stencil_cover/stencil_cache_storage.py),
  [stencil_solver_runtime.py](../transformers/stencil_cover/stencil_solver_runtime.py),
  [stencil_bulk_solver.py](../transformers/stencil_cover/stencil_bulk_solver.py),
  and the stencil corpus modules:
  existing lifecycle precedent for the separate exact-cover problem.

Relevant implementation milestones:

- [add85f08](https://github.com/KodBena/KataProxy/commit/add85f08090e0b01a0dcdf667c594550c90411e8):
  separated ownership-region policy from board and exact-cover concerns,
  preserving the threshold policy as a selectable implementation.
- [33ff5b02](https://github.com/KodBena/KataProxy/commit/33ff5b02162839e3122e0e8bccac7f35d912650f):
  introduced the coherent ownership model alongside the policy extension
  point.
- [f8113936](https://github.com/KodBena/KataProxy/commit/f8113936b9fb77b53142416d8db5e3bb977c2b67):
  replaced the original SciPy MIP invocation with the bounded direct HiGHS
  path and feasible start after the observed wall-time issue.
- [8cf784e](https://github.com/KodBena/KataProxy/commit/8cf784e29274fcca72cd774ac0ffdee04d07d504):
  added the graph-cut ownership sibling without removing the MIP.
- [a39566bd](https://github.com/KodBena/KataProxy/commit/a39566bd8eb2005655696ddda97f3870fd18b9a0):
  current branch checkpoint at the time this document was extended.

For the general minimum-bisection complexity fact, see Feige and
Krauthgamer, “A Polylogarithmic Approximation of the Minimum Bisection,”
SIAM Review 48(1), 2006, [doi:10.1137/S0097539701387660](https://doi.org/10.1137/S0097539701387660).
This reference supports the general-graph complexity context, not a separate
hardness claim for every board-lattice subgraph.
