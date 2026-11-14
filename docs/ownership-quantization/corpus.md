# Ownership quantization evidence corpus

## Purpose and operating boundary

This is a research archive for the ownership-quantization approximation and
sensitivity agendas. It is not a solver-result cache and not a general
KataGo request/response archive.

Collection is opt-in:

```sh
export KATAPROXY_STENCIL_OWNERSHIP_CORPUS_ENABLED=1
# Optional location override:
export KATAPROXY_STENCIL_OWNERSHIP_CORPUS_DB=/path/to/ownership-quantization.sqlite3
```

The default path is
`$XDG_STATE_HOME/kataproxy/ownership-quantization.sqlite3`, or
`~/.local/state/kataproxy/ownership-quantization.sqlite3` when
`XDG_STATE_HOME` is unset. The path is expanded for a leading tilde and its
parent directory is created on first write.

Every opted-in analyze response is captured, including in-search updates and
final packets. When collection is enabled, the request asks KataGo for
`ownershipStdev` even when the selected classifier (for example, the legacy
`threshold` policy) does not use it. The parser preserves a supplied stdev map
without making that map a requirement of policies that do not need it.

Writes happen synchronously in the foreground, after ownership classification
and before exact-cover region preparation. A failed archive write logs an
error and does not suppress the interactive stencil-cover response. The
database uses WAL and `synchronous=FULL`; each observation is one transaction.
There is no Redis dependency, result cache, event stream, spool, materializer,
solver queue, retry worker, or background ownership solver on this path.

## SQLite schema boundary

The archive is a separate SQLite database file from the stencil exact-cover
corpus. Writers attach it to an in-memory connection under the SQLite schema
name `ownership`. This gives the archive its own `PRAGMA user_version`,
migration table, indexes, constraints, and backup lifecycle without mixing
ownership rows into the exact-cover corpus. SQLite schema aliases are
connection-local; when opening the file directly with the SQLite CLI, its
tables appear in the main schema.

Current schema version: `PRAGMA user_version = 1`.

The tables are:

| Table | Role |
|---|---|
| `evidence` | One row per distinct lossless ownership snapshot and replayed board |
| `models` | One row per effective ownership objective/policy identity |
| `solver_identities` | Backend, library version, adapter version, and algorithm configuration |
| `attempts` | One row per actual coherent MIP or coherent-cut invocation |
| `observations` | Append-only policy output witness, turn/search state, diagnostics, and context |
| `schema_migrations` | Applied schema versions and timestamps |

The writer refuses a database whose `user_version` is newer. A non-empty,
unversioned database is backed up before the writer refuses to reinterpret it.
Future migrations must be append-only and versioned; they must preserve the
raw evidence and historical witnesses.

## Evidence identity and raw codecs

An evidence identity covers:

- board width and height and the row-major, top-origin coordinate convention;
- the complete replayed board disposition (empty, Black stone, White stone);
- the ownership mean and, when available, the ownership standard-deviation map;
- the derived empty-point, dead-Black, and dead-White index sets; and
- the version of the chain-liveness interpretation used to derive those sets.

Both numeric maps are stored as little-endian IEEE-754 binary64 arrays. The
stored BLOB is the exact bit representation of the float64 values consumed by
the current policy implementation; no decimal formatting, rounding, or
quantization is applied. The evidence header names the codec explicitly.
Board states are one byte per point: 0 = empty, 1 = Black, 2 = White. Derived
point-index arrays use little-endian uint32 values.

The evidence ID is a SHA-256 digest over a canonical versioned header and the
length-delimited raw blobs. Distinct binary64 maps therefore remain distinct
evidence, even when their values differ below a human-readable decimal
precision. Repeated sightings of exactly the same maps and replayed board
reuse the evidence row; each classifier invocation still creates a new
observation row.

The evidence row is independent of the ownership model and solver. It can
therefore be evaluated under multiple objective configurations without
duplicating the raw map.

## Model and attempt identities

A model identity covers the objective schema, policy class and ID, stable
`model_version`, effective policy parameters, request metadata that changes
classification (such as the threshold cutoff), dead-chain rule version, and
coordinate convention. Custom policies must declare `model_version` and bump
it whenever their output semantics change.

The per-call wall-time limit is intentionally excluded from model identity.
A different `kappa`, `beta`, aggregate tolerance, threshold, policy plugin, or
objective version is a different model. The policy configuration and validated
request metadata are retained in canonical JSON for audit.

A completed optimizer invocation is recorded only when the policy actually
invokes an optimizer. Each attempt includes solver/backend and library
identity, canonical solver configuration, requested wall-time budget and
policy ID, UTC start time, monotonic elapsed nanoseconds, outcome status, and
available objective evidence. The MIP path records a returned seed even when
HiGHS fails after seed construction; a graph-cut exception that prevents a
policy result is logged by the transformer and does not create a fabricated
labeling observation.

For the coherent MIP policy, the attempt stores the feasible initial heuristic
labeling and its full unary-plus-spatial objective separately from the
validated HiGHS optimizer incumbent and its objective. It also stores the
normalized lower bound, absolute gap, backend-reported MIP gap, backend status,
and any error. The HiGHS bound is shifted by the same label-independent
constant as its objective formulation before comparison with the full-energy
evaluator. Its disposition is stored independently of incumbent presence, so
a certified dual bound survives even when the backend returns no primal
witness; an absolute gap is recorded only when both objective and bound exist.

For the coherent-cut policy, the linked observation records the selected
supported or repaired witness, and its diagnostics retain the Lagrange path,
cut count, and repair information. The attempt row records solver identity,
budget, status, and timing. An optimality status is used only where the current
policy's proof conditions apply. A repaired witness is never stored as a solver
incumbent or described as globally optimal.

The observation row stores the labeling actually returned by the selected
policy. Its compact witness codec is a uint32 point count followed by sorted
(point:uint32, label:int8) entries, where labels are -1, 0, or +1. The MIP's
seed and optimizer witnesses remain separately available on the attempt row.
Threshold observations are stored without fabricating solver attempts.

## Context and interpretation

Each observation records the response turn number and whether the response was
during search. The optional context JSON preserves the supplied `rootInfo`,
plus selected top-level value-head/game-result context fields when present.
This context is for later stratification and visual inspection; it does not
enter the ownership objective or model identity.

The replayed board is stored directly, rather than relying on later replay code
to reconstruct historical eligibility. The raw ownership and stdev maps are
the inputs; the dead-chain sets and rule version make the derived eligible
graph explicit. No client ID, query ID, SGF, full request, full response, or
solver-internal resumable state is stored.


## Post-collection iterative-deepening solve campaign

After representative evidence has accumulated, the bulk solver can improve
reference witnesses without changing the source archive:

```sh
# Inspect how many unique coherent evidence/objective problems need work.
python scripts/analyze_ownership_corpus.py summary |
  jq '.unique_problem_tally'

# Preview the deduplicated campaign and the default cumulative limits.
python -m scripts.solve_ownership_corpus --plan-only

# Run the campaign (default cumulative per-instance ceilings: 10, 20, 40,
# 80, and 160 seconds; each native solve slice is at most 10 seconds).
python -m scripts.solve_ownership_corpus
```

The source SQLite database is opened read-only. The campaign deduplicates by
exact evidence identity plus coherent objective identity, so repeated
in-search/final observations and the coherent MIP/cut variants do not create
duplicate solve tasks. Instances already proved optimal and instances with no
empty points are not scheduled.

Each unresolved instance receives the same cumulative budget ceiling at a
given level. The scheduler visits every outstanding instance at that level
before advancing the campaign to the next ceiling. Individual MIP calls are
bounded to at most 10 seconds. The best feasible witness is supplied as the
next MIP start, and the strongest valid lower bound is retained across
attempts. Only a solver-reported proof is marked optimal; a small observed gap
is still reported as approximate.

The campaign writes two files beside the archive by default:
`<database>.bulk-solve.checkpoint.json` and
`<database>.bulk-solve.results.json`. They are separate from the evidence
database. Checkpoints are atomically replaced after each solve slice and can
be resumed by running the same command with the same budget schedule. The
checkpoint is tied to the deduplicated archive snapshot and schedule; if the
archive or schedule changes, use a new checkpoint path.

Progress is printed at roughly 10-second intervals. Ctrl+C stops scheduling
new slices, saves the completed work, writes an interrupted results report,
and exits with status 130. Resume with the same command to continue. The
runner does not preserve the HiGHS branch-and-bound tree between processes;
it reuses feasible incumbents and valid lower bounds only. The bulk solver
never writes attempts or witnesses back into the collection database.

## Inspection and backup

Open the database directly (the `ownership` alias is not persistent):

```sh
DB="${KATAPROXY_STENCIL_OWNERSHIP_CORPUS_DB:-${XDG_STATE_HOME:-$HOME/.local/state}/kataproxy/ownership-quantization.sqlite3}"

sqlite3 "$DB" "PRAGMA user_version;"
sqlite3 "$DB" "PRAGMA integrity_check;"
sqlite3 "$DB" "SELECT COUNT(*) FROM evidence;"
sqlite3 "$DB" "SELECT COUNT(*) FROM models;"
sqlite3 "$DB" "SELECT COUNT(*) FROM observations;"
sqlite3 "$DB" "SELECT COUNT(*) FROM attempts;"
```

Recent model/attempt summaries:

```sql
SELECT o.observed_at_utc, o.turn_number, o.is_during_search,
       m.policy_id, a.status, a.elapsed_wall_ns,
       a.initial_objective, a.optimizer_objective,
       a.lower_bound_disposition, a.lower_bound, a.absolute_gap
FROM observations AS o
JOIN models AS m USING (model_id)
LEFT JOIN attempts AS a USING (attempt_id)
ORDER BY o.observed_at_utc DESC
LIMIT 100;
```

Back up the database using SQLite's online backup API, for example:

```sh
sqlite3 "$DB" ".backup 'ownership-quantization.backup.sqlite3'"
```

There is no automatic pruning or retention policy. Evidence rows are
deduplicated, but observations and actual attempts are append-only. Monitor
disk use and take backups before schema upgrades or filesystem-level moves.
