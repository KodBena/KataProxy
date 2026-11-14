# KataProxy

A WebSocket proxy and middleware framework for the KataGo analysis engine.

KataProxy sits between Go/Baduk/Weiqi clients and one or more KataGo analysis
engines. It speaks the KataGo analysis protocol on both sides, so any client
that already talks to KataGo will talk to KataProxy without changes.

What it does:

- **LEAF**: spawn a local KataGo subprocess and serve it over WebSocket
- **RELAY**: load-balance queries across a fleet of upstream LEAF nodes,
  with consistent-hash routing and query coalescing
- **Transform** queries and responses through composable middleware
  (enrichment, filtering, caching, adaptive re-evaluation)

It is designed for go schools, online go services, and individuals who want
to share a powerful analysis machine across multiple users or front-ends
without giving each client direct access to the engine.

KataProxy is small (a few thousand lines of Python) but disciplined:
~280 pytest cases plus a `mypy --strict` CI gate, with the three-layer
architecture, the ID-namespace translation, and the load-aware fallback
path covered by in-process unit tests and multi-process end-to-end
diagnostics exercised against a live KataGo cluster. A reproducible
benchmark ([benchmark.md](benchmark.md)) characterises throughput,
latency, coalescing efficiency, dispatch distribution, and the
operator-facing `RELAY_MAX_LOAD` tuning knob under realistic mixed
workloads — sustained ~15,000 KataGo visits/sec through a 3-LEAF
cluster on a single 4-core host, with 100/100 hot positions fully
coalesced, dispatch distribution within ±0.5% of ideal-uniform, and
the admission knob shown to have no measurable effect on throughput
across two orders of magnitude. The analysis methodology (Hartigans'
dip test, Gaussian-Mixture-Model regime decomposition) and the raw
data behind every chart are committed alongside the doc so an
operator can re-run or extend any measurement against their own
cluster.

To date the proxy has only seen local-cluster use. The benchmark's
§"Honest assessment" names what was deliberately not tested — upstream
failure, multi-region deployment, multi-day endurance, chained-proxy
topologies — and the path from those gaps to characterised behaviour
runs through actual institutional use. If you're considering
KataProxy for a go school or shared-analysis service, the
architectural documents ([`ARCHITECTURE.md`](ARCHITECTURE.md),
[`FRAMEWORK.md`](FRAMEWORK.md)) explain the design exhaustively, and
the project would benefit from hearing how it survives contact with
your workload.

---

## Requirements

- Python 3.10 or newer
- For the LEAF role, three artefacts from upstream KataGo:
  - a built [KataGo](https://github.com/lightvector/KataGo) **binary**
  - a neural network **model** (`.bin.gz` or `.txt.gz`)
  - an analysis-engine **config** (the upstream source ships
    `cpp/configs/analysis_example.cfg`; copy it to `analysis.cfg` or
    point `KATAGO_CFG` at it)

  The RELAY, ECHO, and REDIRECT roles do not need any of these.

---

## Quickstart — run a LEAF in three commands

```bash
git clone https://github.com/<your-org>/kataproxy.git
cd kataproxy
pip install -e .
```

Set the paths to your KataGo model and analysis config, then start the
server:

```bash
export KATAGO_MODEL=/path/to/model.bin.gz
export KATAGO_CFG=/path/to/analysis.cfg
./run_leaf.sh
```

The LEAF now listens on `ws://127.0.0.1:41948`. Point any KataGo analysis
client at that URL.

If `katago` is not on your `$PATH`, also set `KATAGO_PATH` to the
absolute path of the executable. All three — binary, model, and config
— must be present and acceptable to KataGo; if any is missing or
rejected, the proxy raises `LeafStartupError` at startup with KataGo's
own error in the message (see
[LEAF startup behaviour](#leaf-startup-behaviour)).

---

## Roles

KataProxy runs in one of four modes, selected by the `PROXY_ROLE`
environment variable.

| Role | What it does | Typical use |
|---|---|---|
| **LEAF** | Spawns a local KataGo subprocess and serves it over WebSocket. | Single-machine setups; the building block for everything else. |
| **RELAY** | Forwards queries to one or more upstream LEAF nodes via WebSocket. Hashes queries onto a consistent ring; falls back to the least-loaded peer when the preferred one is saturated. | Fleet of GPU machines behind a single client-facing endpoint. |
| **ECHO** | Returns synthetic responses immediately. No KataGo subprocess, no upstream. | Integration tests and protocol fuzzing. |
| **REDIRECT** | Tells connecting clients to reconnect to one of the configured upstreams (round-robin). Performs no analysis. | Service-discovery point that hands clients off to a real LEAF/RELAY. |

The default port is **41949**. The included `run_leaf.sh` overrides this to
`41948` and `run_relay.sh` runs on `41949` forwarding to `41948` — together
they demonstrate a two-process LEAF + RELAY setup on a single host.

---

## Configuration

All configuration is read from environment variables. See `.env.example`
for the full reference, with defaults and inline explanations.

For most operators the relevant variables are:

- `KATAGO_PATH`, `KATAGO_CFG`, `KATAGO_MODEL` — LEAF only
- `KATAGO_STARTUP_TIMEOUT_S` — LEAF startup-gate timeout (default 60s)
- `PROXY_HOST`, `PROXY_PORT` — network binding
- `PROXY_ROLE` — `LEAF` (default), `RELAY`, `ECHO`, or `REDIRECT`
- `UPSTREAM_URLS` — comma-separated list, required for RELAY and REDIRECT

Copy `.env.example` to `.env` and edit it; the file is loaded automatically
on startup. Variables already set in the shell take precedence over the
file.

### LEAF startup behaviour

When the LEAF role starts, it spawns KataGo, sends a tiny probe query,
and waits for the engine to respond. If KataGo exits before responding —
the typical cause is a missing `analysis.cfg`, a missing model file, or
a GPU that won't initialise — the proxy raises a `LeafStartupError`
that includes KataGo's own stderr output, and refuses to begin
serving. KataGo's stderr is also forwarded continuously to the proxy's
log under `kataproxy.router` while the engine is running.

If KataGo crashes after a successful start, the LEAF respawns it up to
3 times (each retry logged at WARNING) before giving up. After the
budget is exhausted the router enters an unhealthy state: subsequent
queries return an immediate error response rather than hanging.

---

## Network exposure

**KataProxy has no built-in authentication or transport encryption.** This
is a deliberate choice: getting application-layer security right is hard,
and operators who need it have better tools available at the network layer.

If you need to expose a KataProxy instance beyond the loopback interface,
put it behind one of:

- An SSH tunnel (`ssh -L 41949:localhost:41949 …`)
- A WireGuard or Tailscale overlay network
- An authenticating reverse proxy (e.g. nginx with mTLS or HTTP basic auth)

The default bind address is `127.0.0.1`, which prevents accidental network
exposure. Change `PROXY_HOST` to `0.0.0.0` only when one of the above is in
place.

---

## The `goboard_transposition` extension (optional)

KataProxy can enrich analysis responses with transposition information
(positions reachable by multiple move orders) when the `goboard_transposition`
native module is built and importable. If the module is missing, the proxy
runs normally without enrichment and logs one warning at startup:

```
go_transposition native module not found; transposition enrichment disabled.
```

Linux users can build the module from the `goboard_transposition/` directory
following the included build instructions. Windows users can download
pre-built wheels from the project's GitHub Releases page (built in CI for
each tagged release).

The module is optional. The core proxy (queries, responses, coalescing,
load balancing) works identically with or without it.

---

## Stencil-cover enrichment (opt-in)

An analyze query opts in to the signed stencil-cover annotation with:

```json
{
  "capabilities": {
    "stencil_cover": {}
  }
}
```

The default `coherent` ownership policy requests KataGo's ownership and
ownership-standard-deviation maps automatically. It uses the HiGHS MIP model
with a spatially-aware feasible start and the aggregate ownership constraint.
The response reports whether the MIP proved optimality or returned a valid
feasible incumbent before its time budget expired.

An alternative `coherent-cut` policy is available with
`KATAPROXY_STENCIL_OWNERSHIP_POLICY=coherent-cut`. It solves each fixed
Lagrange-multiplier subproblem by an s-t graph cut. If the discrete cut path
skips the permitted integer ownership sum, it uses a deterministic connected
sum-repair. The result reports `optimal`, `feasible_supported`, or
`feasible_repaired`; repaired results are feasible but are not claimed to be
the global constrained optimum. This policy is intended for comparing
large-board latency and region shape against the MIP model.

The legacy `threshold` policy remains available by setting
`KATAPROXY_STENCIL_OWNERSHIP_POLICY=threshold`. With that policy, each opting-in
client must include `truncation_threshold` in its capability metadata, for
example `{"stencil_cover":{"truncation_threshold":0.6}}`.

A custom implementation can be selected with
`KATAPROXY_STENCIL_OWNERSHIP_POLICY=python.module:factory_name`. Its module is
eagerly imported at proxy startup; its factory returns an implementation of
`OwnershipRegionPolicy`. Custom policies must also declare a stable,
non-empty `model_version` so the evidence archive can distinguish
implementation revisions. Parameters and policy semantics are documented in
`docs/stencil-ownership-policy.md`.

The foreground solver is the normal operating mode: it attempts each
component with a 250 ms HiGHS time limit and returns its best valid result,
which may be feasible rather than proven optimal. **No background process is
required.** Redis at `127.0.0.1:6379`, DB 7, adds shared result persistence;
the foreground solver continues to work when Redis is unavailable.

An optional background **exact-cover placement** process can spend up
to a 10-second HiGHS optimization limit trying to improve cached non-optimal
stencil tilings. This worker is for the placement problem only; it does not
re-solve ownership quantization. After installing the project in the active
environment (`pip install -e .`), run:

```sh
kataproxy-stencil-bulk-solver
```

The same worker can be launched from a checkout with
`python -m transformers.stencil_cover.stencil_bulk_solver`. Add `--once` to process the
current pending snapshot and exit. At present, run at most one bulk worker
for this cache namespace.



Optional corpus capture records canonical component sightings and actual
solver attempts. It is disabled by default; enable it with:

```sh
export KATAPROXY_STENCIL_CORPUS_ENABLED=1
# Optional: choose where Redis-unavailable events are spooled.
export KATAPROXY_STENCIL_CORPUS_DB=/var/lib/kataproxy/stencil-corpus-spool.sqlite3
```

When enabled, events are written to the Redis Stream
`kataproxy:stencil-cover:corpus:events:v1` in DB 7. If Redis is unavailable,
the exact event bytes are appended to a local SQLite spool (default:
`$XDG_STATE_HOME/kataproxy/stencil-cover-corpus.sqlite3`, or
`~/.local/state/kataproxy/stencil-cover-corpus.sqlite3`). Configure Redis
AOF and persistent storage for stream durability.

Run the separate materializer to populate the normalized SQLite corpus:

```sh
kataproxy-stencil-corpus-materializer
```

Use `--once` to process one batch and exit, or `--spool-only --once` to
ingest a local outage spool without connecting to Redis. The materializer
creates the consumer group `stencil-corpus-materializer-v1`, reclaims
abandoned pending entries, commits each event to SQLite before acknowledging
the Redis entry, and safely deduplicates a replay after a crash. The Redis
server needs XAUTOCLAIM support (Redis 6.2 or newer). The spool and normalized
tables share the same SQLite file; a v1 raw spool is backed up and migrated to
schema version 2 automatically. Query/analysis tooling is not included yet.

---

## Ownership quantization evidence capture

The ownership classifier has a separate research archive for the approximation
and sensitivity agendas. It records each opted-in analyze response snapshot,
including in-search updates, with the replayed board, exact ownership mean and
standard-deviation arrays, fixed dead-chain sets, the selected ternary
labeling, policy/model identity, optimizer evidence, diagnostics, and optional
KataGo `rootInfo` context.

Enable it explicitly:

```sh
export KATAPROXY_STENCIL_OWNERSHIP_CORPUS_ENABLED=1
# Optional: choose a separate archive file.
export KATAPROXY_STENCIL_OWNERSHIP_CORPUS_DB=/var/lib/kataproxy/ownership-quantization.sqlite3
```

The default database is
`$XDG_STATE_HOME/kataproxy/ownership-quantization.sqlite3`, or
`~/.local/state/kataproxy/ownership-quantization.sqlite3` when
`XDG_STATE_HOME` is unset. Its independent SQLite schema and tables are
documented in [the ownership corpus guide](docs/ownership-quantization/corpus.md).

This path writes directly to SQLite in the foreground. It does not use Redis,
the stencil exact-cover result cache, a Redis Stream, a spool/materializer, or a
background ownership solver. Evidence maps are stored as lossless binary64
blobs; identical evidence is deduplicated, while each response observation
and each actual optimizer attempt remains append-only. Collection is disabled
by default because it adds a durable write for every response packet. A
storage failure is logged and does not suppress the interactive stencil-cover
response.

---

## Logs

KataProxy uses the standard `logging` module. The default level is `INFO`;
set `PYTHONLOGLEVEL=DEBUG` for verbose per-message logging when
debugging, or `PYTHONLOGLEVEL=WARNING` to see only anomalies.

Untrusted strings (peer-controlled wire content, KataGo stdout, RELAY
upstream messages) are passed through `log_safe()` before being formatted
into log records: control characters (newlines, tabs, etc.) are escaped
and the text is truncated to `PROXY_LOG_TRUNCATE` characters (default
256). This blocks log-injection attempts and bounds per-record size.

Log records are namespaced under `kataproxy.*` (e.g. `kataproxy.router`,
`kataproxy.pubsub_hub`) so individual subsystems can be filtered
independently.

---

## Extending

KataProxy is built as three composable layers — sessions, a coalescing hub,
and a backend router — with **transformers** and **middleware** as the
extension points. A transformer is a synchronous content transformation
applied to queries and responses; a middleware is an async, stateful
policy that can buffer, suppress, or inject messages.

If you want to add custom enrichment, filtering, caching strategies, or
adaptive policies, see [`ARCHITECTURE.md`](ARCHITECTURE.md) for the layer
model and the extension contracts.

---

## Licensing

KataProxy is released into the public domain under the [Unlicense](UNLICENSE),
with one exception: the `goboard_transposition/` subdirectory is derived
from KataGo and carries the upstream MIT License. See [`NOTICE`](NOTICE) for
the full boundary documentation and downstream redistribution requirements.

---

## Contributing

Bug reports and pull requests are welcome via GitHub. There are no formal
contribution guidelines yet; pragmatic patches with clear commit messages
are appreciated.

---

## Client impact — 2026-08 cache & model-selection arc

What changed for CLIENTS (OmegaGo and any other consumer of the proxy's
WebSocket) in commits `2812423..0eca42b`. Written for a reader who saw
none of the underlying work. Implementation details live in the commit
messages; this section is only what you observe on the wire.

### 1. Requests that used to hang now get structured errors

Previously some malformed-but-well-meaning requests were logged
server-side and never answered; the client waited out its own timeout.
Now every well-formed JSON request gets a reply. One error shape is used
everywhere, by the proxy and by the engine alike:

```json
{"id": "<your id, when safely echoable>", "error": "<teaching text>", "field": "<offending field>"}
```

(`id` and `field` may be absent; `error` is always present.)

- Unknown/unmatched `action` values are refused with the accepted action
  vocabulary named in the text (`2812423`).
- A query whose `terminateId` names no in-flight query is refused,
  naming the field and the unresolvable id (`16f8639`).
- Terminating an already-completed query is still silently tolerated
  (fire-and-forget terminates keep working exactly as before).

If your code has timeout-based workarounds for "the proxy sometimes
says nothing", you can replace them with an error handler on this shape.

### 2. Queries with unknown fields no longer hang through the proxy

The engine warns about unexpected fields (`warnUnusedFields`) with a
`{"id", "field", "warning"}` message BEFORE the real responses. The
proxy used to treat that warning as the query's completion and drop the
real answers — the client saw the warning, then silence. Fixed in
`3c5d05d`: the warning is delivered as non-terminal metadata and the
analysis results follow normally. Clients that stripped fields
defensively (or special-cased the hang) can stop.

### 3. Model selection

- Nothing changes in the client vocabulary: on a SELECTOR you still
  send `"model": "<label>"` on analyze queries, and `query_models`
  still returns the label list (`{"models": [{"label", "healthy"}]}`).
- NEW, config-side only (`0eca42b`): a SELECTOR label may carry an
  engine-model component — `SELECTOR_MODELS=label=url|internalName` —
  letting one multi-model engine serve several labels. To clients this
  is invisible except as more labels in `query_models`. Client-supplied
  `model` values are consumed at the SELECTOR exactly as before; engine
  internalNames are never a client concern through the proxy.
- Direct-to-engine clients (no proxy): the KataGo build (`2d82734a`,
  KataGo repo) hosts extra models via `-extra-model` /
  `extraModelFile0..N` config keys, selects per-query by
  `"model": "<internalName>"`, lists them via `query_models`, and
  refuses unknown names with the selectable set named.

### 4. Engine cache actions

~~The proxy does not forward cache verbs (`cache_attach` etc.) — they now
draw the structured refusal of item 1 instead of hanging. If you drive
the engine's cache actions DIRECTLY, note (KataGo repo, current branch):~~

*(Struck 2026-08-21, maintainer-authorized correction, NOT validated
against the documentation corpus — the full consult that would validate
it was deliberately not run; trust `c3f4743`'s commit message over this
paragraph where they disagree.)* Since `c3f4743` the four cache verbs
(`cache_attach` / `cache_detach` / `cache_dump` / `cache_stats`) ARE in
the proxy vocabulary: SELECTOR label-routes them like analyze (the
label's engine-model component is minted onto the verb), LEAF forwards
them verbatim, RELAY broadcast-aggregates one reply per configured ring
member (`PROXY_CACHE_VERB_TIMEOUT_S` bounds stragglers). Whether you
drive the engine's cache actions through the proxy or DIRECTLY, note
(KataGo repo, current branch):

- The admission bound is `"minObservations"`; the old `"minLookups"`
  key is refused with teaching text. Counts are per-context observation
  counts (demands for a key), not retrieval counts.
- Omitting the admission bound defaults to `"minObservations": 2`
  (store what has been demanded at least twice).
- Old-format `.nncounts` files (format version 1) are refused with
  guidance: delete the `.nncounts`, keep the `.nnevals`
  (FORMAT_VERSION is now 2).
- `cacheContext` on analyze queries passes through the proxy untouched
  (it is an ordinary engine-facing field).

### 5. Rollout notes for operators

- All proxy changes above are on `fable-branch`, `2812423..0eca42b`
  plus this addendum. Running proxies pick them up ONLY on restart.
- Existing label-only `SELECTOR_MODELS` configs behave byte-identically
  (witnessed by transcript diff against the pre-change baseline).
- Upgrade RELAYs before (or together with) any SELECTOR that uses the
  new `|internalName` component — an old RELAY between them silently
  strips the injected model.


### Automated stencil corpus validation

The real-Redis integration suite and capture benchmark run together with one
command:

    make test-stencil-corpus-integration

It starts an isolated temporary Redis server, creates temporary SQLite corpus
and spool paths, exercises the production proxy/publisher/materializer
recovery paths, runs the enabled-versus-disabled capture benchmark, and writes
JSON, JUnit, and diagnostic logs under .artifacts/stencil-corpus/. See
the [integration workflow documentation](docs/stencil-corpus-integration.md)
for prerequisites, scenario coverage, report formats, and the current runtime
lifecycle finding.
