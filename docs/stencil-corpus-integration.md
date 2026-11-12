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

The existing benchmark measures the synchronous production capture hook over
the same deterministic canonical-component workload with capture disabled
and capture enabled against Redis. It also reports the SQLite fallback path
separately. Its disabled case runs with the opt-in variable unset and does not
construct or invoke the default DB 7 publisher. Enabled cases inject an
explicit publisher that writes only to a unique temporary stream key.

The report records the iteration and warm-up counts, Python/platform and Redis
metadata, disabled/enabled median and nearest-rank p95, median/p95 overhead,
and the fallback measurements. These are capture-hook timings, not total
KataGo request latency. Overhead is the difference between aggregate
quantiles, not paired per-request deltas.

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

## Runtime lifecycle finding

The repository exposes the proxy command and the separate
kataproxy-stencil-corpus-materializer command, but has no production supervisor
or deployment unit that owns both process lifecycles. This test harness starts
and stops the materializer independently only to exercise backlog/recovery; it
does not add a new production supervisor. Normal deployments must continue to
supervise the proxy and materializer as separate processes if they want corpus
events continuously materialized.

Redis and SQLite remain complementary required system components. Redis is
the shared event stream and solver-cache store; SQLite is the normalized corpus
and durable local failure spool. The spool is an outage-recovery path, not a
supported SQLite-only operating mode.
