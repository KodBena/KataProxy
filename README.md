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
