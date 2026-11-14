# Stencil corpus integration testing and benchmarking

## Single developer command

From the repository root, install the project development dependencies and run:

    python -m pip install -e ".[dev]"
    make test-stencil-corpus-integration

The command provisions a dedicated Redis server process on an ephemeral loopback
port, uses Redis DB 0, gives every test isolated temporary SQLite paths, runs
the real-Redis integration tests, and executes the capture benchmark. It does
not connect to a developer's Redis endpoint, inspect or flush another Redis
database, or require a KataGo process.

Prerequisites are Python 3.10+, Make, a local redis-server executable (Redis
6.2 or newer for XAUTOCLAIM), and the project development dependencies. The
command starts and stops Redis itself. Redis persistence is disabled for this
disposable server, whose working directory is temporary.

The production Redis defaults are unchanged: 127.0.0.1:6379, DB 7. The cache
client factory also accepts explicit KATAPROXY_REDIS_HOST,
KATAPROXY_REDIS_PORT, and KATAPROXY_REDIS_DB overrides. The runner sets those
before production modules are imported, including import-time cache
invalidation listeners, so every connection targets the disposable process.
SQLite corpus/spool locations are placed in the temporary test directory.

## Scenarios exercised

The integration module tests public production APIs against real Redis Lua
execution and real Redis Streams:

- Result arbitration and conflict handling through RedisStencilStore.put_result,
  legacy result migration through get_result_record, profile-specific timeout
  retirement through mark_timed_out, and hard-index seeding through
  initialize_pending_queue. Duplicate writes and repeated initialization are
  asserted for idempotency.
- A real ProxyServer WebSocket session uses the production
  stencil_cover_enricher transformer while the materializer is not running.
  A deterministic synthetic backend supplies a known ownership response; the
  proxy still executes the actual capture and solver-runtime code. The test
  confirms events are retained in the production stream and are subsequently
  consumed and acknowledged.
- The same proxy capture path runs with a controlled XADD failure. The
  production publisher persists canonical event bytes in SQLite. The production
  materializer drains the spool; a second recovery pass is checked for
  duplicate corpus records.
- Stream messages accumulate while the materializer is absent and are consumed
  after it starts.
- A child materializer process exits with os._exit at the injected
  after_sqlite_commit boundary. The receipt is visible in SQLite while the
  Redis entry remains pending. A different consumer reclaims it with the
  production XAUTOCLAIM path, safely replays it, and acknowledges it without
  adding a second logical instance or receipt.

The interruption callback is optional, defaults to None, and runs strictly
after SQLite ingestion returns (transaction committed) and before XACK. It
does not change the production commit-before-ack ordering.

The existing focused unit suites remain useful and are not replaced:
tests/test_stencil_cover_enricher.py contains the in-memory Redis behavioral
model; tests/test_stencil_corpus_publisher.py covers publisher/spool contracts;
tests/test_stencil_corpus_store.py covers schema, receipts and idempotency; and
tests/test_stencil_corpus_materializer.py covers materializer logic using fake
stream ports. The integration module adds the missing real Redis and network
boundaries.

## Benchmark

The benchmark measures the one-event publisher path and a request-sized
batch over the same deterministic canonical-component workload. The production
foreground transformer collects sightings from both ownership regions and
publishes them through one transactional Redis pipeline before performing
solver result-cache lookups. The SQLite fallback path is also measured; a
fallback batch is committed in one SQLite transaction. Its disabled case runs
with the opt-in variable unset and does not construct or invoke the default
DB 7 publisher. Enabled cases inject an explicit publisher that writes only to
a unique temporary stream key.

The report records the iteration and warm-up counts, Python/platform and Redis
metadata, disabled/enabled median and nearest-rank p95 for single-event and
batch hooks, normalized batch overhead per component, and the fallback
measurements. These are capture-hook timings, not total KataGo request latency.
Overhead is the difference between aggregate quantiles, not paired
per-request deltas.

No performance acceptance limit is defined in the repository. The report
therefore marks threshold acceptance as pending an explicit engineering
decision; it does not fail the suite based on an invented limit. The benchmark
can be run independently with:

    python benchmark/benchmark_stencil_corpus_capture.py \
      --redis-url redis://127.0.0.1:6379/15 \
      --iterations 1000 --warmup 100 --output-json benchmark-report.json

Use a disposable Redis database for this standalone command. The Make target
does not use that manual setup: it passes the URL of its own isolated server.

## Reports and diagnostics

The Make target writes these files under .artifacts/stencil-corpus/:

| File | Contents |
|---|---|
| integration-report.json | Overall status, exit codes, test summary, benchmark comparison, and artifact references |
| integration-junit.xml | Machine-readable pytest test cases and failures |
| benchmark-report.json | Actual benchmark measurements and environment metadata |
| integration.log | Pytest output, including failure tracebacks |
| benchmark.log | Benchmark command output |
| redis.log | Disposable Redis process diagnostics |

The command returns nonzero if the required integration tests or benchmark
fail, if Redis cannot be provisioned, or if a bounded command timeout expires.
It stops the Redis process on normal exit, test failure, benchmark failure, and
interruption. The reports remain in the output directory. Set
STENCIL_BENCHMARK_ITERATIONS and STENCIL_BENCHMARK_WARMUP to tune local run
cost; CI uses 500 iterations and 50 warm-ups and uploads all reports/logs as a
workflow artifact.

## Production startup and supervision

Stencil corpus capture is enabled by default. The ordinary service commands
start it without a second manual process:

    python proxy_server.py
    # or, for the standard leaf launch:
    ./run_leaf.sh

The installed console command `kataproxy` uses the same `proxy_server:main`
entry point. Set `KATAPROXY_STENCIL_CORPUS_ENABLED=false` (also accepts
`0`, `no`, or `off`) to disable both capture and automatic materializer
startup. An unset variable means enabled.

During startup the proxy launches the existing
`transformers.stencil_cover.stencil_corpus_materializer` CLI as a supervised child process.
The supervisor waits for SQLite initialization before declaring startup ready.
It monitors the child, reports an unexpected exit, and restarts it with
exponential backoff from 1 second up to 30 seconds. The child uses a bounded
Redis blocking read (100 ms, below the configured Redis socket timeout) and handles SIGTERM by completing its current
materialization pass and then exiting. On service shutdown the proxy gives the
worker up to 5 seconds to stop gracefully, then force-terminates it and logs
a warning. Shutdown is bounded; it does not wait indefinitely for
the corpus to become empty.

The materializer CLI remains independently executable for controlled recovery
and testing:

    kataproxy-stencil-corpus-materializer
    python -m transformers.stencil_cover.stencil_corpus_materializer --once
    python -m transformers.stencil_cover.stencil_corpus_materializer --spool-only --once

Do not start another long-running materializer unless intentionally operating
an additional consumer in the same Redis consumer group. The normal service
already owns one supervised worker.

## Database location and operations

The raw SQLite fallback spool and normalized corpus share one database file.
Unless overridden, its resolved path is:

- `$XDG_STATE_HOME/kataproxy/stencil-cover-corpus.sqlite3` when
  `XDG_STATE_HOME` is set.
- `~/.local/state/kataproxy/stencil-cover-corpus.sqlite3` otherwise.

Override the path with `KATAPROXY_STENCIL_CORPUS_DB=/path/to/corpus.sqlite3`.
A leading `~` is expanded. Relative overrides are relative to the service
working directory. The same path is used by the publisher's durable spool and
the materializer's normalized store.

Stop the proxy before making a filesystem-level backup so SQLite has closed
its WAL. Alternatively, use SQLite's online backup command while the service
is running:

    sqlite3 ~/.local/state/kataproxy/stencil-cover-corpus.sqlite3 \
      ".backup 'stencil-cover-corpus.backup.sqlite3'"

Inspect basic integrity and row counts with:

    sqlite3 ~/.local/state/kataproxy/stencil-cover-corpus.sqlite3 \
      "PRAGMA integrity_check;"
    sqlite3 ~/.local/state/kataproxy/stencil-cover-corpus.sqlite3 \
      "SELECT COUNT(*) FROM instances;"
    sqlite3 ~/.local/state/kataproxy/stencil-cover-corpus.sqlite3 \
      "SELECT COUNT(*) FROM solver_attempts;"

Export solver attempts for analysis using the SQLite CLI:

    sqlite3 -header -csv ~/.local/state/kataproxy/stencil-cover-corpus.sqlite3 \
      "SELECT a.source_path, i.backend_name, i.backend_version, \
              a.started_at_utc, a.elapsed_wall_ns, a.outcome_status, \
              a.budget_json, a.backend_metrics_json \
         FROM solver_attempts AS a \
         JOIN solver_identities AS i USING (solver_identity_id);" \
      > solver-attempts.csv

The database contains canonical component sightings and actual solver attempts
that pass through the shared solver runtime. Attempt records include solver
backend/adapter identity and configuration provenance, budget, start time,
elapsed wall-clock nanoseconds, outcome, and reported backend metrics. Sightings
retain whether they came from the foreground or bulk producer path. These are
component-level records for solver analysis and tuning.

This is deliberately **not** a general KataGo traffic archive. It does not
collect full KataGo requests or responses, games, SGFs, or ownership maps.

## Outage and recovery behavior

Redis Streams remain the primary event transport. If Redis publication fails,
the publisher writes the exact same immutable event bytes to the SQLite spool.
When Redis recovers, the supervised materializer reconnects and drains the
spool, then recovers abandoned Redis pending entries with XAUTOCLAIM. If the
materializer exits unexpectedly, the proxy logs the exit and restarts it. The
existing commit-before-XACK and idempotent receipt behavior makes replay after
a crash safe.

If both Redis publication and SQLite spool persistence fail, the publisher
emits a CRITICAL log containing the affected event IDs and both underlying
errors. That event was not durably captured; the solver result or analysis
response continues independently, and operators must treat the log as a data
capture gap.

To disable collection, set `KATAPROXY_STENCIL_CORPUS_ENABLED=false` before
starting the proxy. This does not delete an existing corpus. To recover after a
Redis outage, restore Redis availability and let the service's supervised
worker reconnect; no schema reset or stream flush is required. To recover a
local spool manually, use `kataproxy-stencil-corpus-materializer --spool-only
--once`. For a full stream and pending-entry recovery pass, use
`kataproxy-stencil-corpus-materializer --once` with Redis available.

Redis and SQLite remain complementary required system components. Redis is
the shared event stream and solver-cache store; SQLite is the normalized corpus
and durable local failure spool. The spool is an outage-recovery path, not a
supported SQLite-only operating mode.
