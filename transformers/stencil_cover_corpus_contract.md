# Stencil-cover solver and corpus contracts (Phase 0–1)

**Status:** Phase 0 contracts remain frozen. Phase 1 extracts the default
HiGHS adapter and routes foreground/background solving through the shared
solver port. Corpus publishing and SQLite materialization remain future phases.

This document is the design contract for the next stencil-cover iteration.
The typed solver port lives in `transformers/stencil_solver_api.py`; this
document owns the cross-process event and persistence contracts. Phase 1
implements the default adapter in `transformers/stencil_solver_highs.py`,
with composition in `transformers/stencil_solver_factory.py` and shared
invocation in `transformers/stencil_solver_runtime.py`.

## 1. Solver port

The solver boundary is `SolverBackend.solve(CanonicalProblem, SolverBudget)
-> SolverOutcome`. The canonical problem is solver-facing data only:
canonical width/height, sorted occupied `(y, x)` cells, the complete active
stencil family, its fingerprint, and the canonicalization protocol version.
It contains no Go position, ownership polarity, KataGo coordinate, Redis or
SQLite handle, WebSocket type, or caller-frame transform.

The boundary types are immutable. A solver must not mutate the problem or
retain caller-owned mutable request state. Returned placements, when present,
are expressed in canonical component coordinates and identify the selected
family member.

### Budget and units

Every invocation has a positive `wall_time_limit_ms` and a named
`policy_id`. Native effort is an independent tagged value:
`NativeEffortLimit(value, unit)` or
`NoNativeEffortLimit(reason='not_requested')`. The foreground and bulk
policies initially remain 250 ms and 10,000 ms respectively. Those are
wall-clock milliseconds, not portable native-effort limits.

Native effort reported by an adapter is separately tagged with its unit.
Nodes, iterations, simplex iterations, expansions, and seconds are distinct
quantities. They must never be converted or compared as if they were the
same metric. The per-call budget is not part of backend identity.

A backend must apply its wall-time limit, or be invoked through a killable /
otherwise enforceable boundary that does. Passing a duration to a backend
that ignores it is not enforcement. Foreground invocation must not be able
to run unbounded. The orchestration layer measures total adapter invocation
time with `time.perf_counter_ns()`; a wall-clock UTC timestamp is recorded
separately for chronology. Optional preparation, backend-call, and validation
durations are also monotonic deltas.

### Outcomes

The status vocabulary is closed:

| Status | Meaning |
| --- | --- |
| `optimal` | An incumbent exists and a proof closes the lower bound at the incumbent objective. |
| `feasible` | A valid incumbent exists, but optimality is not proved. |
| `no_incumbent` | The backend returned no incumbent. |
| `error` | Invocation failed; an error record is required and no result is trusted. |

The type makes the incumbent/no-incumbent and error/no-error cases explicit.
A lower bound and native effort likewise carry either a value or a typed
unavailability reason. An incumbent may include a placement witness or an
explicit indication that the backend did not return one. An incumbent
objective without a witness is useful attempt data, but it is not a cover
that KataProxy may serve or cache as a placement result.

Only `optimal` is optimal. A `feasible` incumbent must be retained when its
placement witness validates. A backend that returns no usable cover does not
turn the transformer's guaranteed singleton cover into a solver result:
KataProxy uses its existing fallback, marked `fallback`, while the attempt
remains `no_incumbent` or `error`. Every backend-supplied cover is validated
in canonical coordinates before it can enter Redis or a response: each
placement is a member of the active family, placements are pairwise disjoint,
and their union is exactly the component.

Backend status and backend-specific metrics are retained independently of
normalized status. Exceptions are normalized into `error` outcomes; they
do not masquerade as infeasible or as a valid fallback.

### Backend identity and composition

A solver identity consists of backend name, backend/package version,
adapter implementation version, canonical configuration JSON, and its
SHA-256 configuration fingerprint. The identity fingerprint is derived from
those fields. A different configuration is a different identity. Time limit,
native effort budget, and foreground/background policy are recorded on each
attempt, not folded into backend implementation identity.

HiGHS remains the default and will be the first adapter extracted in Phase 1.
No gomecsolver or OR-Tools dependency is introduced in this phase. A fake
backend will exercise substitution in Phase 1.

The seam follows ADR-0012 P2: the transformer depends on the port, and the
selected backend is composed at the boundary. It also follows ADR-0012 P12:
backend-specific modules own their eager top-level imports. Do not put
solver imports inside a function to simulate optional dependencies. If an
optional backend is selected, the composition root imports its module
explicitly; importing the transformer alone must not pull every backend in.
This is also consistent with ADR-0000's type-first rule: the outcome states
and their required companion values are represented in types rather than
explained only by comments.

## 2. Canonical instance identity

### Identity tuple

The durable identity of an instance is exactly:

`(family_fingerprint, canonicalization_version, canonical_key)`

It excludes solver identity, adapter configuration, budget, source path,
elapsed time, and any hard/easy label. Dimensions and cell count are
derivable from the canonical key and are stored as indexed/queryable columns
for convenience; they do not replace the key.

The current family fingerprint is KataProxy's existing
`_stable_digest(family)`: SHA-256 of the compact, ASCII, sorted-key JSON
encoding produced by `_json_key_value`. Tuples are serialized as arrays;
array order is preserved. The full ordered family representation, including
stencil names and cell coordinates, is included. Family order is significant:
the current compact placement cache encoding uses family indices, so silently
sorting the family would change that contract. Two different fingerprints
must never share an instance, even when a particular component's geometry
happens to look the same.

### Canonicalization protocol v1

The version identifier is:

`kata-proxy-stencil-d4-lsb-v1`

It names the current KataProxy codec and algorithm; it is a separate identity
field and is deliberately **not** prepended to the byte string. This preserves
the current canonical key bytes.

1. Interpret component points as integer `(y, x)` coordinates.
2. Apply translation normalization to each candidate image by subtracting
   its minimum row and minimum column.
3. Compute the family stabilizer: exactly those of the eight D4 transforms
   for which the set of translation-normalized shapes of the complete
   active family is unchanged.
4. For each permitted transform, form the tight bounding rectangle and its
   row-major occupancy bitmap.
5. Encode each candidate and select the lexicographically smallest byte
   string. D4 transform IDs are considered in ascending order, so the first
   transform wins an exact tie.
6. Retain the selected transform and translation offsets in
   `ComponentFrame`; use that frame and the inverse D4 map to recover
   placements in the caller's board coordinates.

The codec is byte-for-byte:

| Offset | Encoding |
| --- | --- |
| byte 0 | canonical height, unsigned 8-bit integer |
| byte 1 | canonical width, unsigned 8-bit integer |
| byte 2 onward | row-major occupancy bitmap, LSB-first within each byte |

For cell `(y, x)`, let `i = y * width + x`. Set bit
`i % 8` in byte `i // 8`, i.e. `bitmap[i >> 3] |= 1 << (i & 7)`.
The high unused bits of the final byte are zero. The bitmap byte count is
`ceil(width * height / 8)`. The codec accepts dimensions representable by
the current one-byte header (the active KataProxy board is at most 25).

A golden fixture is the 2x2 L-triomino occupying
`(0,0), (1,0), (1,1)`: its key is
`02 02 0d` (hexadecimal). Decoding yields width 2, height 2, flat
row-major cell indices `(0, 2, 3)`.

### D4 closure and compatibility

The current `transformers/stencil_family.txt` is intended to be D4-closed.
Phase 0 tests assert that its computed stabilizer is exactly all eight
transforms and that all eight transformed family shape sets equal the
original. A future family that is not D4-closed is canonicalized only under
its actual stabilizer; tests include a single L-tromino family whose
stabilizer is `(0, 7)`, and assert that a 90-degree rotation outside that
stabilizer does not merge.

The reference `gomecsolver` implementation currently encodes the bitmap
with NumPy's default `packbits` order (MSB-first). KataProxy's current
codec is LSB-first. Equivalent geometry therefore does not imply identical
key bytes. The protocol IDs and codecs remain repository-local; no
cross-repository byte compatibility is claimed. A shared codec requires a
deliberate shared-codec change, a version bump, and cross-repository golden
tests. Any incompatible KataProxy key encoding must bump this protocol
version and the Redis namespace/schema that embeds the old key.

## 3. Versioned corpus event contract

Foreground and bulk publish the same immutable event envelope. The initial
wire schema is `kataproxy.stencil-cover.corpus-event.v1`. Events are UTF-8
JSON with canonical compact serialization (sorted object keys, no NaN), and
BLOB-valued canonical keys are standard padded Base64 in JSON.

Common envelope fields:

| Field | Type / rule |
| --- | --- |
| `schema` | exact schema identifier above |
| `event_id` | canonical lowercase UUID generated once; unchanged on every retry |
| `event_type` | `instance_seen` or `solver_attempt` |
| `emitted_at_utc` | RFC 3339 UTC timestamp with `Z` |
| `producer_path` | `foreground` or `bulk` |
| `instance` | identity plus canonical dimensions and cell count |

The `instance` object contains `family_fingerprint`,
`canonicalization_version`, `canonical_key_b64`, `width`, `height`,
and `cell_count`. It contains no raw ownership map, Go position, SGF,
request payload, or caller orientation.

An `instance_seen` event is emitted once per component sighting, including
before local-LRU or Redis cache return. Sighting is not a solver invocation.
A cache hit emits no `solver_attempt` and must never fabricate a zero-second
attempt or overwrite cached provenance.

A `solver_attempt` event is emitted once per actual backend invocation,
whether the result is fast, easy, optimal, feasible, no-incumbent, or error.
It carries:

- `attempt_id` (equal to the event UUID), `solver_identity_fingerprint`,
  backend name/version, adapter version, canonical configuration JSON and
  configuration fingerprint;
- `budget`: wall limit in milliseconds, policy ID, and a tagged native
  effort limit with explicit unit or `not_requested`;
- `started_at_utc`, plus `elapsed_wall_ns` measured as a monotonic
  `perf_counter_ns()` delta around the complete adapter invocation;
- optional preparation, backend-call, and validation nanoseconds, each
  measured monotonically and never inferred from UTC timestamps;
- normalized status, optimality-proven state, incumbent disposition/objective,
  lower-bound disposition/value, native-effort disposition/value/unit,
  backend-status disposition/value, backend metrics, and error disposition;
- `fallback_disposition` (`not_needed` or `singleton_fallback_used`),
  separate from solver outcome, and optional validated placement payload in
  canonical coordinates with its payload codec version.

The event format uses tagged dispositions where absence has a meaningful
reason. The v1 closed vocabularies are: incumbent
`present|not_returned`; lower bound `reported|not_reported`; native effort
`reported|not_supported|not_reported`; backend status
`reported|not_reported`; placements
`validated|not_returned|rejected_invalid`; error `present|none`.
A bare JSON null or SQL NULL must not be used to smuggle such a reason
(ADR-0012 P11). Required/forbidden companions are checked by the event
validator and SQLite constraints. A solver attempt is an append-only fact:
retries of transport reuse its event ID; a genuine second solver invocation
creates a new attempt/event ID.

### Stream and materialization

The proposed stream is
`kataproxy:stencil-cover:corpus:events:v1` in Redis DB 7, with consumer
group `stencil-corpus-materializer-v1`. Redis Streams carry immutable
events; Redis result keys remain the separate best-result state store.

The producer `XADD`s an event without a trimming policy that can evict
unmaterialized entries. The materializer reads through a consumer group,
inserts the event receipt, instance sighting update, and (for attempts) the
attempt row in one SQLite transaction, commits, then `XACK`s the stream
entry. A crash after commit and before acknowledgement replays safely
because `event_id` is unique in SQLite. Pending entries are reclaimed after
a consumer failure and ingested through the same idempotent path. An
ingest-only mode drains this stream without starting solver work. If the
worker is stopped, the foreground can continue and the stream backlog waits
for later materialization.

A Redis Stream is not disk durability by itself. Redis persistence is
server-wide, not DB-specific. Production operation must configure Redis AOF
and a persistent volume (recommended baseline: `appendonly yes`,
`appendfsync everysec`; use `always` only when the latency/durability
trade-off is explicitly accepted). The durability guarantee is only as strong
as that server configuration and storage.

If stream publication fails, the producer writes the same event through an
idempotent direct-SQLite/spool fallback. It must not invent another event
format. If every persistence path fails, emit an operator-visible CRITICAL
diagnostic including `event_id` and the persistence errors, while allowing
the original KataGo analysis response to proceed. A failed corpus write must
not corrupt or block best-result cache arbitration.

## 4. SQLite corpus schema proposal

The SQLite database is a durable research archive, not a runtime answer
cache. Use `PRAGMA user_version` as the schema version. Migrations are
append-only, additive, ordered, and applied one version at a time, each in
its own transaction. Before migrating an existing database, create a
consistent backup using SQLite's backup API. Reject a database whose
`user_version` is newer than the writer. Record migration version and note
in a migration metadata table. Enable foreign keys and use WAL. The selected
`synchronous` setting must be documented by the writer; use `FULL` for the
local durable fallback/archive unless a measured operational decision
documents another durability trade-off. This follows the migration
discipline in `gomecsolver/hard_region_hunt/hunt_store.py`.

The following is the proposed logical schema; it is not installed in Phase 0.

### `instances`

- `instance_id TEXT PRIMARY KEY`: SHA-256 of canonical compact JSON with
  fields `family_fingerprint`, `canonicalization_version`, and
  `canonical_key_b64`; object keys sorted, UTF-8/ASCII JSON, no whitespace;
- `family_fingerprint TEXT NOT NULL`;
- `canonicalization_version TEXT NOT NULL`;
- `canonical_key BLOB NOT NULL`;
- `canonical_width INTEGER NOT NULL CHECK(canonical_width > 0)`;
- `canonical_height INTEGER NOT NULL CHECK(canonical_height > 0)`;
- `cell_count INTEGER NOT NULL CHECK(cell_count > 0)`;
- `first_seen_at_utc TEXT NOT NULL`, `last_seen_at_utc TEXT NOT NULL`;
- `sighting_count INTEGER NOT NULL CHECK(sighting_count > 0)`;
- unique constraint on
  `(family_fingerprint, canonicalization_version, canonical_key)`.

No permanent hard/easy boolean belongs here.

### `solver_identities`

- `solver_identity_id TEXT PRIMARY KEY`: identity fingerprint;
- `backend_name TEXT NOT NULL`, `backend_version TEXT NOT NULL`,
  `adapter_version TEXT NOT NULL`;
- `configuration_json TEXT NOT NULL`,
  `configuration_fingerprint TEXT NOT NULL`;
- unique constraint over backend name/version, adapter version, and
  configuration fingerprint.

Budget is not stored here: it belongs to each attempt.

### `solver_attempts`

Append-only one row per actual invocation. `attempt_id` is the event UUID
and is a foreign key to `corpus_event_receipts.event_id`.

- `attempt_id TEXT PRIMARY KEY`;
- `instance_id TEXT NOT NULL REFERENCES instances(instance_id)`;
- `solver_identity_id TEXT NOT NULL REFERENCES solver_identities(solver_identity_id)`;
- `source_path TEXT NOT NULL CHECK(source_path IN ('foreground','bulk'))`;
- `budget_json TEXT NOT NULL` (tagged wall/native values and policy ID);
- `started_at_utc TEXT NOT NULL`, `elapsed_wall_ns INTEGER NOT NULL
  CHECK(elapsed_wall_ns >= 0)`;
- `timing_breakdown_json TEXT NOT NULL` (all durations in monotonic ns);
- `outcome_status TEXT NOT NULL CHECK(outcome_status IN
  ('optimal','feasible','no_incumbent','error'))`;
- `optimality_proven INTEGER NOT NULL CHECK(optimality_proven IN (0,1))`;
- `incumbent_disposition TEXT NOT NULL CHECK(incumbent_disposition IN
  ('present','not_returned'))`, `incumbent_tile_count INTEGER`;
- `lower_bound_disposition TEXT NOT NULL CHECK(lower_bound_disposition IN
  ('reported','not_reported'))`, `lower_bound INTEGER`;
- `native_effort_disposition TEXT NOT NULL CHECK(native_effort_disposition IN
  ('reported','not_supported','not_reported'))`,
  `native_effort_value REAL`, `native_effort_unit TEXT`;
- `backend_status_disposition TEXT NOT NULL CHECK(backend_status_disposition IN
  ('reported','not_reported'))`, `backend_status TEXT`;
- `backend_metrics_json TEXT NOT NULL`;
- `placements_disposition TEXT NOT NULL CHECK(placements_disposition IN
  ('validated','not_returned','rejected_invalid'))`,
  `placements_payload BLOB`, `placements_codec_version TEXT`;
- `fallback_disposition TEXT NOT NULL CHECK(fallback_disposition IN
  ('not_needed','singleton_fallback_used'))`;
- `error_disposition TEXT NOT NULL CHECK(error_disposition IN ('present','none'))`,
  `error_json TEXT`.

CHECK constraints couple each disposition to its payload:
`incumbent_disposition='present'` iff `incumbent_tile_count IS NOT NULL`;
`lower_bound_disposition='reported'` iff `lower_bound IS NOT NULL`;
native effort reported requires value and unit together, while other effort
dispositions require both absent; backend status not reported requires no
status value; placements validated requires both payload and codec version,
while not-returned/rejected-invalid require both absent; error present iff
`outcome_status='error'`, and error none requires no error payload.
`optimality_proven=1` iff outcome is `optimal`, and an optimal attempt
requires incumbent and lower bound equal to the incumbent tile count.
A lower-bound value is a certified bound, not a heuristic estimate.

Indexes:
- `(instance_id, started_at_utc)`;
- `(solver_identity_id, started_at_utc)`;
- `(outcome_status, started_at_utc)`;
- optional `(source_path, started_at_utc)`.

### `corpus_event_receipts`

- `event_id TEXT PRIMARY KEY`;
- `event_type TEXT NOT NULL`, `schema_version TEXT NOT NULL`;
- `emitted_at_utc TEXT NOT NULL`, `ingested_at_utc TEXT NOT NULL`.

Receipt, sighting aggregate and attempt insertions happen in one transaction.
This makes replay idempotent for both sightings and attempts, including a
worker crash after commit but before stream acknowledgement.

### Optional `instance_sighting_paths`

Aggregate first/last-seen timestamp and count by
`(instance_id, producer_path)`. This is bounded aggregate metadata, not a
copy of a request or ownership map for each sighting.

No `hard` label is persisted on `instances`. Future derived labels must be
scoped to a versioned protocol that names solver identity/configuration,
budget/effort unit, metric, and threshold. Raw attempts remain the source of
truth.

## 5. Redis result/cache and queue migration decisions

Phase 0 does not modify Redis data, Lua scripts, result payloads, queue keys,
or cache behavior.

The current raw key bytes remain unchanged, and the active namespace is
`solver-component-d4-v3` with result value schema 2. Therefore Phase 0 does
not bump either. The canonicalization protocol identifier is frozen for the
corpus/instance contract now; the existing namespace remains the implicit
version fence for the unchanged Redis key bytes. A future code change that
alters the raw key encoding must bump both the protocol version and Redis
namespace/schema.

When Phase 4 adds result provenance, make the planned compatibility break
explicit: use a new namespace (`solver-component-d4-v4`) and value schema
3. Preserve old v3 data during cutover. Existing rows migrate/read through as
a best result with provenance disposition `legacy_unknown`; do not invent a
solver identity, elapsed time, native effort, or historical attempt event.
Old non-profile-aware timeout retirement metadata must not suppress work for
a newly selected profile.

The best-result row remains global to the instance/family, independent of
backend and budget. Provenance belongs to the attempt that produced that
winning result. Result arbitration must keep the existing monotonic ranking:
proven optimum first; otherwise lower incumbent tile count, with a certified
lower bound as a quality discriminator; conflicting proven optima remain an
explicit consistency error.

Work scheduling is profile-specific. The work key must include solver
identity/configuration fingerprint and a named budget profile (wall limit,
native effort value/unit, and policy). A profile that timed out cannot retire
a different backend/configuration profile. A new profile may attempt an
instance already attempted by another profile. The local LRU must include
active solver profile when backend/configuration changes can change its
answer; cross-process invalidation still evicts every profile entry for the
same instance.

## 6. Phase boundaries

Phase 0 froze these contracts and canonicalization tests. Phase 1 extracted
the HiGHS adapter, routed foreground and bulk execution through the shared
solver interface, and added fake-backend substitution tests. No SQLite writer,
corpus event publisher, Redis schema migration, gomecsolver integration, or
OR-Tools integration is part of Phase 1.
