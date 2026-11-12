# Stencil-cover solver: concurrency and background-work semantics

This document describes the intended concurrency model for
`stencil_cover_enricher.py`, its Redis result store, and the optional bulk
solver process. It is intended as a review aid: it explains which state is
authoritative, where work may run concurrently, how races are resolved, and
which operating assumptions the current implementation makes.

## Summary

There is one required solver path and one optional improvement path:

1. **Foreground request path (required for the feature).** A request solves each canonical connected
   component with a 250 ms wall-clock budget through the configured solver backend (HiGHS by default). It first checks a process-local
   LRU; on an LRU miss it consults Redis, and only solves when Redis has no
   result.
2. **Background improvement path (optional).** A separately managed process
   reads the Redis pending set and attempts each component with a 10,000 ms
   wall-clock budget through the same configured solver backend. It validates the cover and atomically upgrades the stored
   result when it has found a better one. Not running this process does not
   disable stencil-cover annotations; it only means that foreground incumbents
   are not given this later, longer improvement attempt.

Redis is the shared source of truth for results and work indexes. The solver
model, backend state, and intermediate search state are deliberately not shared
or serialized. A background attempt starts from the canonical problem
instance and solves it again.

The foreground path is allowed to return a feasible incumbent. It must never
label an incumbent or singleton fallback as proven optimal. Only a backend outcome that explicitly proves optimality is represented as an exact result.

## Solver port and composition (Phase 1)

The transformer depends on `SolverBackend` from
`transformers/stencil_solver_api.py`. Its canonical input contains only the
canonical component, dimensions, complete active family, family fingerprint,
and canonicalization protocol version. The backend does not see KataGo,
WebSocket, ownership polarity, Redis, or the caller's board frame.

The default adapter is `HiGHSBackend` in
`transformers/stencil_solver_highs.py`. Its only solver-specific imports are
owned by that module. `stencil_solver_factory.py` is the explicit composition
root and currently registers HiGHS as the default; optional adapters must be
registered by importing their own module at composition time. The transformer
does not import OR-Tools or gomecsolver.

Both paths use `stencil_solver_runtime.solve_component`: the foreground's
existing Redis/LRU wrapper adapts its typed outcome to the unchanged compact
result row, and the bulk worker calls the same runtime boundary directly. The
runtime measures total adapter invocation time with
`time.perf_counter_ns()`, validates a returned placement witness in the
canonical frame, and uses the singleton fallback only when no validated
solver cover is available. A backend error remains an error (the worker leaves
the item pending); it is not relabeled as a feasible or optimal result.

The local LRU key includes the selected backend identity fingerprint as well
as the existing shape, family, and foreground time budget. Invalid backend
placement witnesses normalize to error outcomes; the singleton fallback is
marked distinctly and cannot be cached as a proven or feasible solver cover. Redis continues to
store one global best result per canonical problem/family, independent of
backend and budget. The work queue is still the legacy global pending/timeout
index in this phase; solver-profile-aware queue migration remains Phase 4.

Programmatic composition can inject a backend through
`stencil_cover_enricher(link, solver_backend=...)` or
`StencilBulkSolver(..., solver_backend=...)`. No gomecsolver or OR-Tools
dependency is added by this phase.

## Corpus event capture (Phase 2)

Corpus capture is opt-in. Set `KATAPROXY_STENCIL_CORPUS_ENABLED=1` to
publish events; set `KATAPROXY_STENCIL_CORPUS_DB` to choose the SQLite
fallback path. Without the enable flag, the normal solver path performs no
corpus transport or filesystem I/O.

For each foreground component, the producer emits `instance_seen` before
consulting the process-local LRU or shared Redis result cache. A cache hit
therefore records a sighting but does not fabricate a solver attempt. Bulk
processing records a bulk sighting before its actual solve. The shared solver
runtime emits exactly one `solver_attempt` envelope per actual backend
invocation, preserving the backend-reported outcome separately from the
normalized result used by KataProxy.

The publisher writes the canonical v1 event bytes to the untrimmed Redis
Stream `kataproxy:stencil-cover:corpus:events:v1`. If Redis publication fails,
it appends those exact bytes to a local SQLite spool with event-ID
idempotency. The spool uses WAL and `synchronous=FULL`; a repeated event ID
with different bytes is an immutable-event conflict. If both persistence
paths fail, the publisher logs a CRITICAL diagnostic and continues the
analysis/worker path. Redis Stream durability still depends on the Redis
server's persistence and volume configuration.

This is a raw event spool only. A consumer group and materializer that turn
these events into the normalized SQLite corpus are not part of this phase.

## Why the queue is in Redis, not a multiprocessing.Queue

A `multiprocessing.Queue` is a reasonable choice when a single parent process
owns a fixed set of child processes and their lifetime. That is not the shape
of this work:

- Foreground requests may be handled by several independently started proxy
  processes. Each process can discover a hard component. A normal
  `multiprocessing.Queue` belongs to one multiprocessing family; it is not a
  shared queue for unrelated proxy processes.
- The background worker is a separately managed command. It may be started,
  stopped, or restarted independently of the proxy. Work discovered while it
  is stopped must remain available after it starts again.
- Queue membership, the current best result, and timeout retirement must
  agree. Redis Lua scripts can update the result row and its indexes in one
  atomic operation. An in-memory multiprocessing queue would still need a
  second durable coordination mechanism to prevent the queue and persisted
  result from drifting apart.
- A multiprocessing design would require another lifecycle owner (for example
  a long-lived manager process), IPC setup across proxy workers, shutdown and
  crash recovery rules, and a separate durable hand-off to Redis. Redis is
  already a service dependency for the result cache, so a second queue
  mechanism would add moving parts without removing the need for Redis.

For those reasons the queue is represented by Redis sets rather than Python
IPC. The worker deliberately processes the pending snapshot sequentially; it
does not create a multiprocessing pool. This keeps CPU use low-priority and
makes one worker's resource consumption predictable.

**Current operating constraint:** run one bulk-worker instance for this
namespace. Queue entries are not claimed with a lease. If two bulk workers are
run concurrently, Redis will still protect the stored result and timeout
indexes from corruption, but both workers may spend up to 10 seconds solving
the same component. Multi-worker claiming is not currently implemented.

## Problem identity and stored values

A connected ownership component is canonicalized before it reaches either
solver path. The canonical shape key contains its normalized bitmap and uses
only the dihedral transforms that preserve the complete active stencil
family. The family identity is included in the persistent result key. Thus
two translated or permitted rotated/reflected instances can share a result,
but a result from a different stencil family cannot be mixed in.

The Redis result identity intentionally excludes the solver time budget.
There is one best-known result for a canonical shape and family, regardless of
whether it was found by a 250 ms foreground solve or a 10-second background
solve. The process-local LRU does include the time budget in its key, because
it caches the request call's result for that call signature.

A stored result is only:

- an `optimal` flag;
- a status code;
- the tile count; and
- the compact encoded stencil placements.

The problem instance is the canonical shape key. No solver object, model,
basis, branch-and-bound tree, solver checkpoint, or other search state is
persisted.

Status codes are ordered as follows:

| Code | Meaning | May be reported as optimal? |
| --- | --- | --- |
| `0` | The selected backend proved an optimum | Yes |
| `1` | A valid feasible incumbent, but optimality was not proved | No |
| `2` | Guaranteed singleton fallback because no valid solver cover was available | No |

For non-optimal rows, status 1 ranks ahead of status 2. Within the same
non-optimal status, fewer tiles ranks ahead of more tiles. A proven-optimal
row always ranks ahead of either non-optimal status. If two proven-optimal
writes disagree on tile count for the same canonical problem, the store raises
a `RedisCacheConflict` rather than silently accepting the contradiction.

## Redis indexes and their lifecycle

The indexes are partitioned by cache namespace and full stencil-family
fingerprint:

- **Hard set:** every component whose best stored row is not proven optimal.
- **Pending set:** hard components not yet retired after a bulk attempt.
- **Timeout hash:** metadata for a hard component that has already had its
  bulk attempt and was not proven optimal.
- **Pending-initialized marker:** a one-time migration marker for the pending
  set.

When a foreground solve first stores a non-optimal result, the same Redis Lua
operation writes the result, adds the canonical shape key to the hard set, and
adds it to the pending set. A proven-optimal result removes the key from the
hard and pending sets and removes any timeout metadata.

At worker startup, a one-time atomic migration seeds the pending set from the
pre-existing hard set, excluding entries that already have timeout metadata.
This preserves non-optimal entries written by the earlier result-cache
version. The marker prevents a full hard-set scan on every poll.

A strictly better non-optimal result re-adds the key to the pending set and
clears its previous timeout metadata. An equal or worse result does not
restart work. A proven-optimal result removes the key from all three work
indexes.

## Foreground request path

For each connected component:

1. Convert the component to its canonical shape key and retain the caller's
   coordinate frame for solution recovery.
2. Check the process-local, entry-bounded LRU (currently 90,000 entries).
   An LRU hit performs no Redis operation.
3. On an LRU miss, try the persistent Redis result row. If one exists, decode
   and return it; no new solve is started.
4. If Redis has no row, run the selected backend through the shared solver
   interface with the foreground wall-clock budget (currently 250 ms), encode
   the selected cover, and write the result to Redis.
5. Recover the canonical placements into the caller's frame and verify that
   they cover exactly that caller's component.

The foreground Redis wrapper degrades to solving without persistence when
Redis is unavailable. An initial connection outage is recoverable: the
invalidation listener retries its connection, and a later LRU miss can adopt
the recovered client. A runtime persistence error disables that wrapper's
Redis access for the remainder of that process; requests continue through the
solver path rather than failing because the cache is unavailable.

The process-local LRU is not authoritative. It is a performance layer over
the Redis result row and can be discarded at any time.

## Background worker

The source module is `transformers/stencil_bulk_solver.py`; there is no
standalone `kataproxy-stencil-bulk-solver.py` source file. The console command
is generated when the project is installed (for example, by `pip install -e .`).
Run it from the active environment with:

```sh
kataproxy-stencil-bulk-solver
```

Or run it directly from a repository checkout with:

```sh
python -m transformers.stencil_bulk_solver
```

Add `--once` to either command to process the current pending snapshot and
exit. This worker is optional; the foreground feature does not depend on it.
It uses the same Redis DB
(`127.0.0.1:6379`, DB 7) and cache namespace as the foreground transformer.
By default it polls every five seconds when idle. `--once` processes the
current pending snapshot and exits. It attempts to set its process priority to
nice 19 where the operating system supports it.

For each pending key in the snapshot, the worker:

1. Checks for timeout metadata and skips an already retired item.
2. Loads the current best result. A pending key without a result is treated as
   an inconsistent queue entry and is logged; it is not guessed or solved
   without the stored problem/result contract.
3. Calls the shared `stencil_solver_runtime.solve_component` boundary with a
   10,000 ms wall-clock budget and the worker's selected backend. It does not
   use the foreground LRU and does not resume foreground solver state.
4. Validates the returned result in the canonical frame: placement encoding
   must decode, every stencil name and geometry must match the active family,
   placements must be disjoint, and their union must equal the component.
5. Writes the candidate through the same atomic result-upgrade operation as
   the foreground path, then reads the best row back. The worker does not
   assume its own candidate won a concurrent write.
6. If the best row is proven optimal, it is complete and is removed from the
   hard and pending indexes by the result-write script.
7. Otherwise, it retires the pending attempt. The timeout Lua script checks
   that the key is still both hard and pending and reads the current result
   row inside the same atomic operation. It writes timeout metadata from that
   current row and removes the key from pending, while retaining it in hard.

The timeout record stores its schema version, configured backend wall-clock limit,
attempt start time, and the status/tile count from the row that was current at
retirement. It describes an attempt that did not prove optimality under the
configured limit; it is not a saved solver state.

The selected backend must enforce the supplied wall-clock budget. The default
HiGHS adapter maps it to HiGHS' `time_limit` option. Candidate generation,
model construction, result validation, and Redis I/O are outside that native
solver limit; the shared runtime separately measures the complete adapter
invocation with a monotonic clock.

## Concurrency and race semantics

### Several foreground proxy processes

Each proxy process has its own LRU and a Redis Pub/Sub subscriber. Redis
serializes result/index updates through Lua. A better result written by one
process is published on a namespace-specific channel with the canonical
shape key in the message.

The publishing process tags the message with its process token and ignores
its own notification; otherwise a process would evict the result it just
computed. Other processes evict all local LRU entries for that canonical
shape (including entries made under different foreground time budgets).
Notifications are best effort, not a durable event log. After a subscriber
reconnects, it clears its whole local LRU once to cover notifications missed
while disconnected.

The LRU tracks in-flight computations by shape key. If an invalidation arrives
while a computation for that shape is running, the generation changes. The
in-flight result is discarded from the LRU and the call retries through the
persistent layer. Therefore an obsolete computation cannot repopulate the
local cache after a newer Redis row has been announced.

### Foreground solve races with the bulk worker

The solver work itself is intentionally not locked across processes. Two
processes can independently solve the same canonical problem. This is safe
for stored state because result writes are ranked and atomic:

- If a foreground or background solve stores an optimum first, a later
  non-optimal write cannot replace it.
- If two writes race with different non-optimal results, the better-ranked
  row wins.
- If the worker finishes after a foreground improvement, it reads back the
  stored best row and bases retirement on that row, not its local candidate.
- If an optimum is stored before timeout retirement, the timeout script sees
  the optimum, removes stale hard/pending membership, and writes no timeout.
- If a non-optimal improvement is stored before retirement, timeout metadata
  records that improved row's status and tile count.
- If an improvement is stored after retirement, the result-write script
  re-queues the component and clears the old timeout marker.

These rules make duplicate solver work wasteful rather than destructive. They
do not make duplicate bulk workers efficient; see the single-worker constraint
above.

### Cache invalidation versus result correctness

Redis result rows and their Lua-protected indexes are authoritative. Pub/Sub
only keeps per-process LRU copies fresh. Losing a Pub/Sub message cannot
corrupt a Redis result or queue entry. A disconnected subscriber flushes its
LRU when it resubscribes. A process-local cache may therefore be stale for
the brief interval between a Redis update and receipt of its notification,
but it is never the source used to arbitrate persistent writes.

## Failure handling and recovery

- **Redis unavailable at proxy startup:** requests can still solve. The
  subscriber retries; after it reconnects, the next LRU miss can resume
  persistent reads/writes.
- **Redis persistence error at runtime:** that proxy's persistent wrapper
  disables Redis access for the process and continues serving from the solver.
  Restarting the proxy is currently required to re-enable its persistence
  wrapper after such a runtime failure.
- **Subscriber disconnect:** it retries with backoff. Re-subscription clears
  the local LRU to cover missed notifications.
- **Worker process exits during a solve:** no timeout is written because the
  retirement step was not reached. The key remains pending and can be picked
  up by the next worker process.
- **Worker gets a non-optimal result:** its current best row is retained and
  timeout metadata removes it from pending. It will not be retried automatically
  until a strictly better result is stored.
- **Malformed or failed item:** the worker logs it and leaves it pending for
  operator investigation/retry. It backs off when items are skipped rather
  than spinning continuously.
- **Worker and foreground both store results:** Redis arbitrates the best row;
  cache invalidation informs the other proxy processes.

## Review invariants

A reviewer can use these invariants when changing this subsystem:

1. **Only proof means optimal.** Status 0 is the only optimal status. A
   feasible incumbent or fallback must never be upgraded merely because it
   came from the background worker.
2. **The canonical key identifies the problem.** Include the full family
   identity in every persistent result/index operation. Do not include the
   time budget in the persistent result identity.
3. **A result and its indexes move together.** Changes to the result row,
   hard set, pending set, and timeout metadata must remain atomic in Redis.
4. **Timeout retirement uses the current row.** Do not base timeout metadata
   solely on the worker's local result; a foreground process may have improved
   it while the solver was running.
5. **Validate before publishing.** A worker result must decode to a valid,
   disjoint exact cover of the canonical component.
6. **The LRU is disposable.** Redis is authoritative. Cache invalidation must
   not require Redis work on an ordinary local-LRU hit.
7. **Do not retry hard instances in a tight loop.** A non-optimal bulk attempt
   is retired durably and only requeued after a better result.
8. **One bulk worker at a time.** Adding concurrent workers requires an
   explicit claim/lease protocol, expiry and crash recovery, and tests for
   lease races; the Redis result arbitration alone is not a work-claim system.
