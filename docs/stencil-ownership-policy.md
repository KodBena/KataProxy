# Stencil ownership-region policies

## Selection and extension point

The policy is selected at process startup with
`KATAPROXY_STENCIL_OWNERSHIP_POLICY`:

- `coherent` (default) selects the spatially coherent ternary quantizer.
- `threshold` selects the former per-intersection hard cutoff.
- `python.module:factory_name` eagerly imports a custom module and calls its
  named zero-argument factory at startup.

The factory must return an implementation of the `OwnershipRegionPolicy`
protocol in `transformers.stencil_cover.stencil_ownership`. It declares
`policy_id`, `requires_ownership_stdev`,
`validate_request_metadata(metadata)`, and
`classify(evidence)`. Classification receives an immutable
`OwnershipEvidence` with validated ownership values, optional stdev values,
the replayed board, empty points, and dead-chain dispositions. It returns an
`OwnershipRegions` value containing positive/negative cells and diagnostics.
The policy is a sibling collaborator: a new policy does not change board
replay, region construction, the cache, or exact-cover solving.

A custom module must be importable in the proxy's Python environment. It is
loaded during module import, not deferred until the first request. This makes
its dependency footprint and import cost part of proxy startup and avoids
function-local lazy imports. Environment changes require a proxy restart.

## Coherent model

For each empty point \(i\), the policy chooses \(q_i \in \{-1,0,+1\}\), with
positive meaning White ownership. Given KataGo ownership mean \(\mu_i\) and
ownership standard deviation \(\sigma_i\), its unary costs are

\[
C_i(0)=0,\qquad
C_i(+1)=1-2\mu_i+2\kappa\sigma_i,\qquad
C_i(-1)=1+2\mu_i+2\kappa\sigma_i.
\]

The model adds a four-neighbour total-variation penalty

\[
\beta\sum_{(i,j)} |q_i-q_j|
\]

and an aggregate conservation constraint

\[
\left|\sum_{i\in E}q_i-\sum_{i\in E}\mu_i\right|\le\epsilon,
\]

where \(E\) is the set of empty intersections. Dead chains are fixed to their
opponent's ownership and contribute that fixed signed amount to the reported
net; living stones are excluded from the cover and from the aggregate
constraint. Thus the target is derived from the ownership evidence on eligible
points; it is not KataGo's `scoreLead`, which is a different output quantity
and perspective.

The model is a mixed-integer linear program. Two ordered binary indicators
represent each ternary label: \(q_i=a_i+b_i-1\), with \(b_i\le a_i\).
For ordered ternary labels,

\[
|q_i-q_j|=|a_i-a_j|+|b_i-b_j|,
\]

so the spatial term is represented exactly by auxiliary variables for the
absolute differences. The aggregate interval is an explicit integer
constraint. This avoids relying on a Lagrange-multiplier search to happen to
land on a score sum that may be skipped by discrete label changes.

The implementation uses the HiGHS MIP engine through its Python API. Before
branch-and-bound it constructs a feasible labeling by starting at the
independent unary minima and applying the cheapest one-point changes needed to
meet the integer score interval. The seed includes the exact auxiliary values
for the spatial total-variation terms, so it is a complete feasible MIP start,
not an unconstrained fallback. HiGHS runs single-threaded; model assembly and
optimization both consume the configured per-response budget. An optimal
solution is marked as such. If the budget expires first, the best valid
incumbent (at least the feasible seed) is returned with
`solverStatus=feasible_time_limit`; the transformer does not need to discard
an otherwise usable annotation merely because optimality was not proven.

These weights are modelling choices, not calibrated constants. They are
exposed for investigation:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `KATAPROXY_STENCIL_OWNERSHIP_CONFIDENCE_KAPPA` | `1.0` | Penalty for acting on points with higher ownership-search uncertainty |
| `KATAPROXY_STENCIL_OWNERSHIP_SPATIAL_BETA` | `0.25` | Cost per unit of label difference across an orthogonal boundary |
| `KATAPROXY_STENCIL_OWNERSHIP_SCORE_TOLERANCE` | `0.5` | Allowed absolute difference between quantized and expected empty-point ownership sums |
| `KATAPROXY_STENCIL_OWNERSHIP_SOLVER_TIME_LIMIT_MS` | `250` | Per-response optimization time limit |

The diagnostics are included in `opaque.stencilCover.ownershipDiagnostics`,
including expected and quantized net ownership, residual, edge/point counts,
solver status, and whether optimality was proven. Defaults are initial
investigation values, not a claim of empirical calibration; they should be
evaluated against held-out positions and final outcomes.

## Legacy threshold model

When `KATAPROXY_STENCIL_OWNERSHIP_POLICY=threshold`, the former behavior is
retained. Each empty point is selected independently when its signed ownership
crosses the client-supplied `truncation_threshold`; a zero threshold selects
strictly positive or negative values and leaves exact zero neutral. Dead chains
are still assigned to the opponent, and living stones remain excluded.

Example:

```json
{
  "capabilities": {
    "stencil_cover": {
      "truncation_threshold": 0.6
    }
  }
}
```

The threshold policy does not request or require `ownershipStdev`.

## Operational notes

- KataGo's ownership map is requested for every opted-in stencil cover. The
  coherent policy also requests `ownershipStdev`; threshold does not.
- A missing or malformed `ownershipStdev` map is an error for coherent
  policy, not an implicit zero-uncertainty estimate.
- Policy parameters are process-level environment settings, not per-query
  knobs. This keeps the objective stable for all requests served by a process.
