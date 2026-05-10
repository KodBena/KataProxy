# Design — Structured logging for institutional deployments

A planning artifact for the post-v1.0.19 logging overhaul. Written
against the proxy as it stands today — colored_logger optional,
`logging_config.py` providing `log_safe` / `filter_dict` /
`get_logger`, default level INFO, ad-hoc f-string log call sites
across `pubsub_hub.py`, `router.py`, `proxy_server.py`, and the
middleware/transformer modules.

The work is motivated by the SELECTOR watchdog incident
(2026-05-10; postmortem in the umbrella's
docs/notes/postmortem-selector-watchdog-2026-05.md). The band-aid
that shipped before the structural fix was reached for partly
because the proxy's log stream was not dense enough in operator-
useful information for the cause to be visible without manual
re-instrumentation. The user's framing for this design was
"institutional software; spare no pains in making logging as
useful as possible." The bar that follows is sized for
multi-tenant deployments — go schools, online go services,
research groups sharing analysis machines — where the operator
is not the developer, the log is the diagnostic surface, and an
aggregator (ELK / Loki / Datadog / structured-grep) sits on the
output.

This memo proposes:

  1. A structured record schema (typed fields, machine-readable
     by default).
  2. A closed event vocabulary with per-event field contracts.
  3. Per-role coverage contracts (each role MUST emit a declared
     set of lifecycle events).
  4. Three operator-selectable output renderings (console, logfmt,
     JSON).
  5. A `ProxyLogger` adapter API that enforces both levels of
     contract at the call site.
  6. Operability env vars for tracing, filtering, and PII gating.
  7. A four-phase migration plan with per-phase tag bumps.

The memo is the reviewable artifact before any code lands. Schema
mistakes are cheap to revise here; renaming an event field after
operators are parsing it is expensive.

## 1. Background — what's wrong now

Three coupled problems in the current logging:

**Untiered payload dumping.** Several call sites log objects via
their dataclass `__repr__` (`f"… {query!r}"`). Python's default
dataclass repr dumps the entire opaque dict, including 200-element
move lists. The `filter_dict` helper exists in `logging_config.py`
but is applied at only a handful of sites. The result: an INFO
line about "subscribe a query" is hundreds of characters,
dominated by payload, with the structural fields (action, turn
range, model) buried.

**Single namespace, no role discrimination.** Every module logs
under `kataproxy.<module>`. A LEAF, RELAY, SELECTOR, ECHO process
all log in the same shape; the only role discriminator is the
KataGo subprocess `pid=` substring (and only on subprocess stdout,
not on proxy-internal log lines). In the SELECTOR-stack topology
where three proxy processes interleave on stdout, this is
operationally hostile.

**No causality markers.** Lifecycle events (incoming → coalesced
→ dispatched ↑ engine ↓ response → delivered) aren't visually
distinguishable. Tracing one canonical_id end-to-end requires
reading the log line by line. There is no "trace a single query"
filter switch.

None of these are inherent to the architecture; they're an
accumulated thinness in the call-site discipline. The
infrastructure to fix them exists half-built (`log_safe`,
`filter_dict`, optional `colored_logger`); what's missing is a
coherent contract that the call sites can satisfy uniformly.

## 2. Requirements (institutional bar)

Each of these is load-bearing. The design has to satisfy each one
or it isn't done.

  **R1. Structured records, machine-readable by default.** Every
  log record carries a fixed set of typed fields. The free-form
  `msg` is for humans; the fields are for machines. Operators
  filter on `level=ERROR AND role=LEAF AND label=really_weak`
  without scraping the rendered message string.

  **R2. Three output renderings, operator-selectable.** Console
  (TTY, coloured, compact), logfmt (`key=value` per line), JSON
  (one record per line). Selectable via `PROXY_LOG_FORMAT`
  defaulting to `auto` (console if stderr is a tty, logfmt
  otherwise).

  **R3. Closed event vocabulary.** A code-enumerated set of event
  names. New events are added via the same code path as new
  commits, so the set is reviewable. Aggregators pivot on
  `event=<name>` reliably.

  **R4. Mandatory context propagation.** Every record carries the
  context it pertains to: `role` always; `session` for any
  client-connection record; `upstream`/`label` for any
  upstream-connection record; `cid` and `orig` for any record
  about a specific query. Logger adapters bake these into `extra`
  once at construction; call sites never re-supply them.

  **R5. Privacy / PII tier discipline.** INFO carries structural
  metadata only — no `moves`, `analysis_config`, `overrideSettings`,
  or `extra.<color>.deltas`. DEBUG carries payloads through
  `filter_dict` (already strips `moveInfos`/`ownership`/`policy`).
  An explicit `PROXY_LOG_FULL_PAYLOAD=true` switch is the only way
  to get untruncated payloads. The audit-H-4 / pre-v1.0.4 PII
  posture is preserved and made explicit.

  **R6. Per-event field contract.** Each event in the closed
  vocabulary declares its required and optional fields. The logger
  API enforces field presence at the call site — calling
  `log(Event.DISPATCH, …)` without the event's required fields
  raises at the call site, not silently emits a malformed record
  that breaks aggregator queries six months later.

  **R7. Per-role coverage contract.** Each role declares the
  events it MUST emit during its lifecycle. A role test drives the
  role through a representative scenario and asserts that every
  obligation appears.

  **R8. Performance discipline.** Hot-path call sites use
  `logger.isEnabledFor(level)` to skip expensive formatting when
  the level is filtered out. The structured-fields API takes
  pre-built dicts or lazy callables, not f-strings that pay
  formatting cost regardless of level.

  **R9. Operability switches.** Beyond level + format:
  `PROXY_LOG_TRACE_CID=<cid>` to trace one query; `PROXY_LOG_FILTER`
  for ad-hoc regex; `PROXY_LOG_FULL_PAYLOAD` for untruncated DEBUG;
  `PROXY_LOG_DEST` for stderr|file|both.

  **R10. Documented contract.** A `proxy/docs/logging.md` page
  documenting the schema, the event vocabulary, the per-event /
  per-role contracts, the format options, the env-var matrix, and
  worked examples for each role. Operators don't read source.

## 3. Record schema

Every log record produced by the proxy carries the same root
schema. Some fields are always present; others are present when
applicable to the event. Aggregator queries can rely on the
"always present" fields unconditionally.

```
{
  "ts":          "2026-05-10T19:35:32.150123+02:00",  // ISO 8601 with TZ
  "level":       "INFO",                              // TRACE|DEBUG|INFO|WARNING|ERROR|CRITICAL
  "role":        "SELECTOR",                          // see Section 5
  "module":      "router",                            // emitting Python module
  "event":       "broadcast",                         // see Section 4
  "msg":         "broadcast query_version to 2 upstream(s)",  // human-readable summary

  // Mandatory-per-context fields (present when applicable):
  "session":     "192.168.122.1:54321",               // per-ClientSession (peer addr)
  "cid":         "hub_a6940f0fc3458649380b",          // canonical_id (query-scoped)
  "orig":        "range-b3804abc-…-1778434532501",    // orig_id from client
  "upstream":    "ws://upstream-a:1",                 // upstream URL (RELAY)
  "label":       "really_weak",                       // SELECTOR upstream label

  // Event-specific fields (declared per-event; see Section 4):
  "action":      "ANALYZE",
  "direction":   "proxy→upstream",                    // recv|forward|proxy→upstream|upstream→proxy
  "duration_ms": 1234,
  "extra":       { … }                                // event-specific payload
}
```

### Always-present fields

| field    | type     | semantics                                   |
|----------|----------|---------------------------------------------|
| `ts`     | string   | ISO 8601 with timezone offset.              |
| `level`  | string   | Log level name.                             |
| `role`   | string   | Process role; closed set (Section 5).       |
| `module` | string   | Python module emitting the record.          |
| `event`  | string   | Event name; closed set (Section 4).         |
| `msg`    | string   | Human-readable summary; for console render. |

### Mandatory-per-context fields

These are required *when applicable*. The applicability rules are
declared per-event (Section 4). For example, `dispatch` requires
`cid` and `orig`; `connect` requires `session` but not `cid`.

| field      | type     | semantics                                            |
|------------|----------|------------------------------------------------------|
| `session`  | string   | Peer address `host:port` of the client WebSocket.    |
| `cid`      | string   | Canonical ID assigned by the hub.                    |
| `orig`     | string   | Original ID from the client wire payload.            |
| `upstream` | string   | Upstream URL (RELAY's hash-ringed peers).            |
| `label`    | string   | SELECTOR's labelled-upstream identifier.             |

### Event-specific fields

Each event declares optional structured fields beyond the always-
present and mandatory-per-context ones. See Section 4 for the
full per-event schema.

## 4. Event vocabulary

The event set is closed: enumerated as a Python `StrEnum` in
`proxy/structured_logging.py::Event`. New events land via code
review. The names are stable across versions (renames go through a
deprecation cycle).

### 4.1 Connection-lifecycle events (session-scoped, no cid)

| event             | level   | required fields beyond root        | semantics                            |
|-------------------|---------|------------------------------------|--------------------------------------|
| `connect`         | INFO    | `session`, `peer_ip`               | Client WebSocket accepted.           |
| `disconnect`      | INFO    | `session`, `code`, `reason`        | Client WebSocket closed.             |
| `connect_refused` | WARNING | `session`, `peer_ip`, `cause`      | Connection rejected (caps, ratelimit). |
| `rate_limited`    | WARNING | `session`, `peer_ip`               | Per-IP rate limit kicked in.         |

### 4.2 Wire-side ingress events (per client message)

| event          | level   | required fields beyond root              | semantics                            |
|----------------|---------|------------------------------------------|--------------------------------------|
| `recv`         | DEBUG   | `session`, `raw_size_bytes`              | Wire frame read from client.         |
| `parse`        | DEBUG   | `session`, `cid`, `orig`, `action`       | Parsed query.                        |
| `parse_error`  | ERROR   | `session`, `error_kind`, `raw_excerpt`   | Parse failure (depth-bomb / JSON / dispatcher). |

`raw_excerpt` uses `log_safe` (capped, repr'd) and is suppressed
unless DEBUG is on.

### 4.3 Hub-coalescing events

| event          | level   | required fields beyond root                              | semantics                            |
|----------------|---------|----------------------------------------------------------|--------------------------------------|
| `subscribe`    | INFO    | `session`, `cid`, `orig`, `action`                       | New subscription registered.         |
| `coalesce`     | INFO    | `session`, `cid`, `orig`, `action`, `subscriber_count`   | Joined an existing canonical.        |
| `cache_hit`    | INFO    | `session`, `cid`, `orig`, `action`, `cache_key`          | Replay-cache short-circuit.          |
| `cache_miss`   | DEBUG   | `session`, `cid`, `orig`, `action`, `cache_key`          | Cache lookup attempted, no hit.      |
| `unsubscribe`  | DEBUG   | `session`, `cid`, `orig`, `was_last`                     | Subscriber departure.                |

### 4.4 Dispatch events (router-scoped)

| event              | level   | required fields beyond root                                          | semantics                            |
|--------------------|---------|----------------------------------------------------------------------|--------------------------------------|
| `dispatch`         | INFO    | `cid`, `orig`, `action`, `direction=proxy→upstream`; `upstream` or `label` | Query forwarded to one upstream.     |
| `broadcast`        | INFO    | `cid`, `orig`, `action`, `target_count`, `targets[]`                 | Broadcast to all healthy upstreams.  |
| `dispatch_error`   | ERROR   | `cid`, `orig`, `error_kind`; `upstream` or `label`                   | Send failure on a specific upstream. |
| `no_upstream`      | ERROR   | `cid`, `orig`, `action`                                              | No healthy upstream available.       |

### 4.5 Response events

| event              | level   | required fields beyond root                                                          | semantics                            |
|--------------------|---------|--------------------------------------------------------------------------------------|--------------------------------------|
| `respond`          | DEBUG   | `cid`, `orig`, `kind=partial|final|metadata|error`, `direction=upstream→proxy`        | Response received from upstream.     |
| `forward`          | DEBUG   | `cid`, `orig`, `kind`, `direction=proxy→client`                                       | Forwarded to client.                 |
| `respond_dropped`  | DEBUG   | `cid`, `orig`, `cause=stale|broadcast_followup`                                      | Subsequent broadcast response dropped. |
| `complete`         | INFO    | `cid`, `orig`, `total_responses`, `duration_ms`                                      | Query lifecycle ended.               |

### 4.6 Terminate events

| event                   | level   | required fields beyond root                                | semantics                            |
|-------------------------|---------|------------------------------------------------------------|--------------------------------------|
| `terminate_recv`        | INFO    | `session`, `cid`, `orig`                                   | Client TERMINATE received.           |
| `terminate_dispatch`    | INFO    | `cid`, `orig`, `direction=proxy→upstream`                  | Sent to upstream LEAF.               |
| `terminate_synthesized` | INFO    | `cid`, `orig`, `cause=coalesced|already_completed|no_upstream` | Synthetic ack returned to client.    |
| `terminate_complete`    | INFO    | `cid`, `orig`                                              | Terminate ack delivered.             |

### 4.7 Keep-alive events

| event              | level   | required fields beyond root                            | semantics                            |
|--------------------|---------|--------------------------------------------------------|--------------------------------------|
| `keepalive_reset`  | DEBUG   | `session`                                              | Heartbeat observed; timer reset.     |
| `keepalive_check`  | DEBUG   | `session`, `idle_seconds`, `in_flight_count`           | Watchdog tick (no fire).             |
| `keepalive_fired`  | WARNING | `session`, `idle_seconds`, `terminated_cids[]`         | Watchdog terminated stranded queries.|

### 4.8 KataGo subprocess events (LEAF role only)

| event             | level   | required fields beyond root                | semantics                            |
|-------------------|---------|--------------------------------------------|--------------------------------------|
| `kg_spawn`        | INFO    | `kg_pid`, `kg_cmd`                         | Subprocess started.                  |
| `kg_ready`        | INFO    | `kg_pid`, `startup_seconds`                | Probe query completed.               |
| `kg_unready`      | ERROR   | `kg_pid`, `cause`, `stderr_tail`           | Probe failed; refusing to bind.      |
| `kg_crash`        | WARNING | `kg_pid`, `exit_code`, `stderr_tail`       | Subprocess exited.                   |
| `kg_respawn`      | INFO    | `kg_pid_new`, `attempt`, `budget_remaining` | Restart attempted.                   |
| `kg_unhealthy`    | ERROR   | `cause`                                    | Restart budget exhausted.            |

These are proxy-side observations of the subprocess (the LeafRouter
spawns, probes, watches, and respawns KataGo). KataGo's own stderr
stream is **not** wrapped by the proxy logger — KataGo manages its
own logging through its `.cfg` file (operators can redirect to file
if they prefer), and re-wrapping verbatim KataGo lines would
obscure the operator-chosen format. KataGo's stderr passes through
unchanged. The `stderr_tail` field on `kg_unready` / `kg_crash` is
captured at those specific failure transitions for diagnostic
context only — it's a snapshot, not a stream.

### 4.9 Upstream-connection events (RELAY / SELECTOR roles)

| event             | level   | required fields beyond root                            | semantics                            |
|-------------------|---------|--------------------------------------------------------|--------------------------------------|
| `upstream_connect`     | INFO    | `upstream` or `label`                          | Connected to upstream.               |
| `upstream_disconnect`  | WARNING | `upstream` or `label`, `cause`                 | Lost upstream connection.            |
| `upstream_reconnect`   | INFO    | `upstream` or `label`, `attempt`, `delay_seconds` | Reconnect attempted.              |
| `upstream_unhealthy`   | ERROR   | `label`, `budget_remaining=0`                  | Reconnect budget exhausted (SELECTOR). |

### 4.10 Middleware and transformer events

| event                  | level   | required fields beyond root                                                | semantics                            |
|------------------------|---------|----------------------------------------------------------------------------|--------------------------------------|
| `middleware_engage`    | DEBUG   | `cid`, `orig`, `middleware_name`, `capability`                             | Capability gate engaged middleware.  |
| `middleware_skip`      | DEBUG   | `cid`, `orig`, `middleware_name`, `cause=opt_out|absent_capability`        | Capability gate bypassed.            |
| `transformer_apply`    | DEBUG   | `cid`, `orig`, `transformer_name`, `direction=on_query|on_response`        | Transformer modified the message.    |
| `transformer_drop`     | DEBUG   | `cid`, `orig`, `transformer_name`                                          | Transformer suppressed (returned None). |
| `orchestration_spawn`  | INFO    | `cid` (parent), `sub_orig`, `name`                                         | Orchestration coroutine spawned sub-query. |
| `orchestration_done`   | INFO    | `cid`, `name`, `outcome=normal|error|cancelled`                            | Orchestration coroutine completed.   |

The vocabulary is intentionally larger than minimal — institutional
operators need to filter on specific event classes (e.g. "show me
every `kg_crash` for the past hour") and the closed set lets them
without grep-engineering. The cost is one additional enum entry +
one TypedDict per new event.

## 5. Roles and per-role coverage contracts

The `role` field is one of:

| role       | meaning                                                                  |
|------------|--------------------------------------------------------------------------|
| `LEAF`     | Single proxy with a managed KataGo subprocess.                           |
| `RELAY`    | Proxy with N hash-ringed upstream WebSocket peers.                       |
| `SELECTOR` | Proxy with N labelled upstream WebSocket peers.                          |
| `ECHO`     | Proxy returning synthetic responses (test/replay).                       |
| `REDIRECT` | Proxy that issues a redirect message and closes (Layer 1; ClientSession-less). |

The PROXY_ROLE env var selects one. Each role has a coverage
contract — events that MUST appear during its normal lifecycle.

### LEAF coverage contract

| event             | when                                                       |
|-------------------|------------------------------------------------------------|
| `kg_spawn`        | start() begins KataGo spawn.                               |
| `kg_ready`        | startup probe completes successfully.                      |
| `kg_crash`        | subprocess exits during normal operation.                  |
| `kg_respawn`      | each restart attempt.                                      |
| `kg_unhealthy`    | restart budget exhausted.                                  |
| `connect` / `disconnect` | every accepted client WebSocket.                    |
| `subscribe` / `complete` | every query lifecycle.                              |
| `dispatch`        | every analyze query forwarded to subprocess.               |
| `respond`         | every response read from subprocess (DEBUG).               |

### RELAY coverage contract

LEAF's events except the `kg_*` family, **plus**:

| event                   | when                                                |
|-------------------------|-----------------------------------------------------|
| `upstream_connect`      | each upstream reachable.                            |
| `upstream_disconnect`   | each upstream lost.                                 |
| `upstream_reconnect`    | each reconnect attempt.                             |
| `dispatch`              | single-target hash-ring dispatch.                   |
| `broadcast`             | metadata fanout (QUERY_VERSION / TERMINATE_ALL / CLEAR_CACHE). |

### SELECTOR coverage contract

RELAY's events, **plus**:

| event                   | when                                                |
|-------------------------|-----------------------------------------------------|
| `upstream_unhealthy`    | reconnect budget exhausted for a label.             |
| `no_upstream`           | dispatch to a label with no healthy upstream.       |

### Verification

Each role's coverage contract is verified by a pytest test that
drives the role through a representative scenario and asserts the
declared events appear in the captured log stream. The test fixture
captures records via a `MemoryHandler` and inspects the `event=`
field. When a new code path is added that should emit a
contract-event, the test fails until the call site is added.

## 6. Output renderings

Three formatters live in `proxy/structured_logging/formatters.py`
(notional path; final layout TBD per the file-size budget). The
operator selects via `PROXY_LOG_FORMAT=auto|console|logfmt|json`.

### 6.1 Console (TTY default)

Coloured, compact, human-scannable. Field order is fixed for
visual scanning: `ts level [role context] event {cid orig} msg`.

```
19:35:32.150 INFO  [SELECTOR really_weak peer=192.168.122.1:54321] dispatch
                   cid=hub_a694… orig=range-b380… → ANALYZE to really_weak
19:35:32.151 DEBUG [SELECTOR really_weak peer=192.168.122.1:54321] respond
                   cid=hub_a694… kind=partial
19:35:34.001 INFO  [SELECTOR really_weak peer=192.168.122.1:54321] keepalive_reset
                   session=192.168.122.1:54321
19:36:08.034 WARN  [SELECTOR 2026_02 peer=192.168.122.1:54321] keepalive_fired
                   idle=25.0s terminated_cids=[hub_2c3f…]
```

`cid` and `orig` are abbreviated to first 4 / first 8 hex digits in
console mode for scannability; the full values are in the underlying
record (visible in logfmt/JSON; expandable via
`PROXY_LOG_NO_ABBREV=true`). Colors map level → standard tty
palette (DEBUG dim, INFO default, WARN yellow, ERROR red, CRITICAL
bg-red).

### 6.2 Logfmt (file/aggregator default)

`key=value` per line. Stable field order: root fields first, then
context fields, then event-specific fields. Values with spaces are
quoted; values that contain quotes are escaped per logfmt
conventions.

```
ts=2026-05-10T19:35:32.150+02:00 level=INFO role=SELECTOR module=router event=dispatch session=192.168.122.1:54321 cid=hub_a6940f0fc3458649380b orig=range-b3804abc-c51e-402c-bfd1-122df0243557-1778434532501 label=really_weak action=ANALYZE direction=proxy→upstream msg="→ ANALYZE to really_weak"
```

### 6.3 JSON

One record per line; the schema in Section 3 verbatim. For
aggregators that want strict typing.

```
{"ts":"2026-05-10T19:35:32.150+02:00","level":"INFO","role":"SELECTOR","module":"router","event":"dispatch","session":"192.168.122.1:54321","cid":"hub_a6940f0fc3458649380b","orig":"range-b3804abc-…-1778434532501","label":"really_weak","action":"ANALYZE","direction":"proxy→upstream","msg":"→ ANALYZE to really_weak"}
```

### 6.4 Format dispatch

`PROXY_LOG_FORMAT=auto` chooses console if stderr is a tty,
logfmt otherwise. Operators with explicit needs (an aggregator
shipper that wants JSON regardless of tty) override.

## 7. API design

### 7.1 The `ProxyLogger` adapter

```python
from proxy_logging import get_proxy_logger, Event, Direction, Role

# Module-scoped base.
logger = get_proxy_logger(__name__)

# Per-session adapter, constructed in ClientSession.__init__.
session_log = logger.bind(role=Role.SELECTOR, session=peer)

# Per-upstream sub-adapter, constructed in SelectorRouter._connect.
upstream_log = session_log.bind(label=label)

# Call site:
upstream_log.info(
    Event.DISPATCH,
    cid=canonical_id, orig=orig_id,
    action=action.name,
    direction=Direction.PROXY_TO_UPSTREAM,
    msg=f"→ {action.name} to {label}",
)
```

`bind()` returns a new adapter with the additional fields
permanently attached. The original adapter is unchanged. Bind
chains compose freely; `session_log.bind(label=…).bind(cid=…)` is
allowed even though `cid` is normally a per-call field.

The adapter validates at log time:

  - Event is a member of the `Event` enum (not a free string).
  - All required fields for the event are present (either via
    `bind()` or via the call-site kwargs).
  - Field types are honoured (cid/orig/label are str, etc.).

Validation failures are `LogContractError` (a new exception type),
raised at the call site. They never silently emit a malformed
record.

### 7.2 The `Event` and `Direction` enums

```python
class Event(str, enum.Enum):
    CONNECT       = "connect"
    DISCONNECT    = "disconnect"
    SUBSCRIBE     = "subscribe"
    DISPATCH      = "dispatch"
    BROADCAST     = "broadcast"
    RESPOND       = "respond"
    FORWARD       = "forward"
    COMPLETE      = "complete"
    # … (full set per Section 4)

class Direction(str, enum.Enum):
    RECV               = "recv"               # client→proxy
    FORWARD            = "forward"             # proxy→client
    PROXY_TO_UPSTREAM  = "proxy→upstream"
    UPSTREAM_TO_PROXY  = "upstream→proxy"
    INTERNAL           = "internal"
```

### 7.3 The per-event field contract

Each event has a TypedDict describing its required and optional
fields. Centralised in `proxy_logging/event_schemas.py`:

```python
class DispatchFields(TypedDict, total=False):
    cid: Required[str]
    orig: Required[str]
    action: Required[str]
    direction: Required[str]
    upstream: NotRequired[str]
    label: NotRequired[str]
    duration_ms: NotRequired[int]

EVENT_SCHEMAS: dict[Event, type] = {
    Event.DISPATCH: DispatchFields,
    Event.BROADCAST: BroadcastFields,
    # … (full set)
}
```

The validator inspects the `Required` markers and checks each
required field against the merged set of bound + call-site fields.
TypedDict's runtime introspection isn't ideal pre-3.11; the
project targets 3.10+ (per pyproject.toml's `requires-python =
">=3.10"`), so we use `typing.get_type_hints` + `__required_keys__`
fallback or a small manual decorator to declare requireds.

### 7.4 Lazy formatting on the hot path

```python
if upstream_log.is_enabled_for(logging.DEBUG):
    upstream_log.debug(
        Event.RESPOND,
        cid=cid, orig=orig, kind="partial",
        direction=Direction.UPSTREAM_TO_PROXY,
        msg=lambda: f"partial response, {format_query_filtered(response)}",
    )
```

`msg=` accepts either a string or a callable; callables are
invoked only if the level is enabled. The same for any expensive
field computation.

### 7.5 Convenience lifecycle helpers

For the most common event sequences, a small set of helpers reduces
boilerplate:

```python
from proxy_logging import lifecycle

lifecycle.dispatch(
    upstream_log,
    cid=canonical_id, orig=orig_id, action=action.name,
    upstream_or_label=label,
)
# … inside SelectorRouter._broadcast:
lifecycle.broadcast(
    session_log, cid=canonical_id, orig=orig_id, action=action.name,
    targets=sent_to,
)
```

Helpers exist for: `connect`, `disconnect`, `subscribe`, `dispatch`,
`broadcast`, `respond`, `forward`, `complete`, `terminate_recv`,
`terminate_synthesized`, `keepalive_reset`, `keepalive_fired`,
`upstream_connect`, `upstream_disconnect`. They all delegate to
the adapter's `info()` / `warning()` / etc. with the right event
and field shape.

## 8. Operability — env vars

| variable                  | values                  | semantics                                             |
|---------------------------|-------------------------|-------------------------------------------------------|
| `PYTHONLOGLEVEL`          | TRACE/DEBUG/INFO/WARN…  | Stdlib-compatible level (kept for compat).            |
| `PROXY_LOG_FORMAT`        | auto/console/logfmt/json| Output rendering (auto picks based on tty).           |
| `PROXY_LOG_DEST`          | stderr/file:`<path>`/both | Where records are written.                          |
| `PROXY_LOG_TRACE_CID`     | `<cid>`                 | When set, drop records that don't carry this cid (or no cid). |
| `PROXY_LOG_FILTER`        | `<regex>`               | Drop records whose rendered line doesn't match.       |
| `PROXY_LOG_FULL_PAYLOAD`  | true/false              | At DEBUG, opt out of `filter_dict`'s field-stripping. |
| `PROXY_LOG_NO_ABBREV`     | true/false              | Console mode: don't abbreviate cid/orig.              |
| `PROXY_LOG_TRUNCATE`      | int                     | (existing) `log_safe` cap on string fields.           |

`PROXY_LOG_TRACE_CID` is the institutional debugging primitive —
when an operator gets a complaint about a specific query, they can
restart the proxy with `PROXY_LOG_TRACE_CID=hub_xxx` (or hot-set it
if the proxy supports SIGHUP-driven config reload; out of scope
for v1.0.20) and see only that query's lifecycle without the
surrounding noise.

## 9. Privacy / PII tier

Three tiers, monotonically increasing visibility:

  **INFO and above (default).** Structural metadata only. No
  payload content. `subscribe` records `action=ANALYZE
  turn_count=186 max_visits=200 model=really_weak`, not the moves.
  `respond` records `kind=partial` not the move infos.

  **DEBUG.** Payloads through `filter_dict`. `moveInfos`,
  `ownership`, `policy` stripped from response dicts. `moves` array
  in queries replaced with `moves=<elided len=186>` summary. Full
  `analysis_config` dict visible (the operator may need to see the
  palette for diagnosis); SGF player names are not in the proxy's
  payload graph (they're a frontend concern), so this is safe.

  **DEBUG with `PROXY_LOG_FULL_PAYLOAD=true`.** Untruncated. The
  user explicitly chose verbose mode; the responsibility for PII
  shifts to them.

The `recv` event's `raw_excerpt` field is suppressed at INFO,
truncated via `log_safe` at DEBUG, untruncated under
`PROXY_LOG_FULL_PAYLOAD`.

The audit-H-4 log-injection defence (everything wire-derived goes
through `log_safe` before being formatted into a record) is
preserved. The structured-fields path inherits it — string fields
in the record go through `log_safe` automatically before rendering.

## 10. Performance

The structured-logging path is in front of every log call site,
and the proxy's hot paths (per-query dispatch, per-response
delivery) issue many. Performance discipline:

  **Level-gated formatting.** Every adapter exposes
  `is_enabled_for(level)`; expensive field computation is gated
  behind it. Callable-valued `msg=` and field arguments are
  invoked only on emission.

  **Static field paths.** The `bind`-chain accumulates fields into
  a single dict; emission is one merge with the call-site kwargs.
  No per-call class-allocations.

  **Validator skip at high-volume levels.** The per-event field
  validator runs at DEBUG and below by default; at INFO+ the
  validator only fires when an `EnvVar=PROXY_LOG_VALIDATE=true` is
  set (for testing / staging environments). Production INFO+ logs
  trust the call site (which is itself test-covered).

  **JSON encoding cost.** JSON output is the worst case; the
  encoder is `orjson` if installed (already a transitive dep
  through some Python ecosystems), `json` otherwise. The decision
  is at module-import time.

Benchmarks: at INFO level, a typical call site costs ≤ 5 µs; at
DEBUG with full payload formatting, ≤ 50 µs per call. Validated
against current baseline in Phase 1 acceptance.

## 11. Migration

Phasing is conservative — no big-bang rewrites. The entire arc
lands on a single feature branch (`feat/structured-logging` in
the proxy submodule) as a sequence of reviewable commits. **No
per-phase tags. No merging to main.** The branch is pushed for
remote review after each phase. The whole arc ships as one
release once user review of the integrated experience is
complete; at that point a single tag (suggested: v1.0.20) is cut
and main absorbs the branch.

  **Phase 1 — `proxy/proxy_logging/` infrastructure.** New
  package: the `ProxyLogger` adapter, `Event` and `Direction`
  enums, three formatters, the env-var dispatcher, the per-event
  required-field declarations, the validator, lifecycle helpers,
  `summarize_query` and `format_query_filtered`. `logging_config.py`
  becomes a thin shim re-exporting the existing `log_safe` /
  `filter_dict` / `get_logger` from the new package; existing
  call sites continue to work unchanged. Unit tests for each
  formatter, the bind chain, the validator, the trace-cid filter.
  **One commit on the feature branch.**

  **Phase 2 — adopt at the boundaries that matter most.**
  ClientSession (per-session adapter), LeafRouter / RelayRouter /
  SelectorRouter (per-upstream adapter), `_handle_incoming` and
  `_handle_query` (the lifecycle markers most operators trace
  through), pubsub_hub (subscribe / coalesce / cache_hit /
  cache_miss). The role-coverage contract tests for LEAF / RELAY /
  SELECTOR / ECHO are written and pass against the migrated call
  sites. **One commit on the feature branch.**

  **Phase 3 — sweep the remaining call sites.** Transformers
  (`analysis_enricher`, `transposition_enricher`, `capability_gate`,
  `capabilities_advertiser`), middleware (`adaptive_reevaluate`,
  `keep_alive`, `capability_gate`, `orchestration`). At end of
  phase, no f-string `logger.{debug,info,warning,error}` calls
  remain in the proxy; everything goes through the structured
  adapter. **One commit on the feature branch.**

  **Phase 4 — documentation + operability hardening.**
  `proxy/docs/logging.md` (the operator-facing reference).
  `proxy/CLAUDE.md` gains a logging-conventions section (analog of
  the heartbeat-fanout-contract section). The role-coverage
  contract tests are extended to include negative-path events
  (parse_error, dispatch_error, kg_crash). The
  `PROXY_LOG_TRACE_CID` and `PROXY_LOG_FILTER` mechanisms are
  exercised via `tests/diagnose_log_format.py`. **One commit on
  the feature branch.**

Each phase keeps the rest of the proxy buildable and testable; the
shim at Phase 1's `logging_config.py` is what makes Phase 2 / 3 a
gradual sweep rather than a flag-day rewrite. The user reviews
each phase's commit (and the branch as a whole) on GitHub before
the integrated arc is merged.

## 12. Decisions

The six open questions in the original draft have been answered.
Recorded here as the decision ledger for this design.

  **Q1. Where does the new package live?** **Decided: (b)
  `proxy/proxy_logging/`.** Matches the existing `proxy_json.py` /
  `proxy_server.py` naming convention; avoids shadowing the
  stdlib `logging` package.

  **Q2. Validator strictness in production.** **Decided: always
  validate.** The per-call cost (~1 µs) is acceptable; the
  no-malformed-records guarantee is unconditional. Operators
  don't have to remember to enable validation for staging vs.
  production.

  **Q3. Coverage-contract tests.** **Decided: pytest.** The
  role-coverage contracts (Section 5) live in
  `tests/test_role_coverage_*.py` next to the existing
  `test_selector_router.py` / `test_relay_router.py` files.
  Diagnose scripts continue to handle the full-stack scenarios
  they already cover; no new diagnose scripts are added for
  contract enforcement.

  **Q4. Multi-process correlation.** **Decided: defer.** The cid
  is the institutional tracer for queries that have reached the
  hub. The pre-canonical events (recv, parse_error) lack cross-
  process correlation; institutional operators chain by session
  + timestamp + role for those. A shared trace ID is a future
  arc if operational demand surfaces. Note from user: the
  shared-terminal log artifact in `~/error` was a consequence of
  `frontend/scripts/run-selector-stack.py` running all proxies in
  one terminal, not of any per-proxy multi-process awareness.

  **Q5. KataGo subprocess stdout/stderr.** **Decided: don't
  wrap.** KataGo emits its diagnostics on stderr by default; its
  `.cfg` file lets the operator redirect to a file. The proxy
  doesn't try to re-frame KataGo's stream into structured events
  — KataGo's stderr passes through unchanged, and the operator
  manages KataGo's logging through KataGo's own controls. The
  proxy-side `kg_*` lifecycle events (Section 4.8) cover the
  transitions the proxy itself observes (spawn, ready, crash,
  respawn, unhealthy); they include `stderr_tail` snapshots at
  failure transitions for diagnostic context but do not stream
  KataGo's output line-by-line.

  **Q6. JSON-mode timestamp format.** **Decided: ISO 8601 with
  TZ.** Documented as the output format. Operators who need
  epoch-millis transform it in their shipping pipeline (one
  jq filter).

### Note on enforcement mechanism

The original draft sketched per-event field schemas using PEP 655
`Required` / `NotRequired` markers. Those require Python 3.11+
(or `typing_extensions`). The proxy's `pyproject.toml` declares
`requires-python = ">=3.10"`, so the implementation uses the
slightly less-typed approach: a `TypedDict` for the field shape
(IDE / documentation aid) plus a sibling `frozenset[str]` of
required field names per event (the runtime validator's data).
Two declarations side-by-side in `proxy_logging/events.py`; the
"two sources of truth" risk is mitigated by colocation and by
the validator's tests pinning both at once.

## 13. Out of scope

The following are deliberately not in this design:

  - **Frontend logging.** The umbrella's SPA writes to the
    browser console. Different consumer, different contract; not
    addressed here.
  - **Backend logging.** The umbrella's FastAPI service uses
    stdlib logging via uvicorn. Different deployment mode;
    different contract. If the institutional bar pulls them in
    later, the schema in Section 3 generalises but the API would
    be backend-shaped.
  - **Metric emission.** Latency histograms, counter rollups, and
    similar belong in a metrics export (Prometheus, OpenTelemetry).
    Logs are the diagnostic surface; metrics are a separate one.
    A future arc may add metric exporters; this design stays out
    of their way (the closed event set is what such a metric
    exporter would tap into).
  - **Distributed tracing.** OpenTelemetry-style trace spans across
    the SELECTOR → LEAFs path would be a substantial separate
    project. Q4's "shared trace ID" is the smallest seed of a
    future trace surface; full tracing waits for institutional
    demand.
  - **Log archival / rotation.** stdlib `RotatingFileHandler` and
    operator-side log shipping handle this at the deployment
    layer. The proxy emits a stream; the operator's shipper
    archives.

---

## Review checklist

This memo is the reviewable artifact. Things to react to before
any code lands:

  - [ ] Schema fields (Section 3) — anything missing or surplus?
  - [ ] Event vocabulary (Section 4) — closed set OK? Names stable
    enough to commit to?
  - [ ] Per-event field contracts — required vs. optional split
    sensible?
  - [ ] Per-role coverage contracts (Section 5) — coverage events
    sufficient to diagnose typical operator complaints?
  - [ ] Output renderings (Section 6) — three formats enough?
    Anything in the rendering shape that conflicts with your
    aggregator stack?
  - [ ] API design (Section 7) — `bind` chain ergonomics
    acceptable? Lifecycle helpers right granularity?
  - [ ] Env vars (Section 8) — namespace OK? Anything missing?
  - [ ] PII tier (Section 9) — INFO/DEBUG/full split honest about
    what's exposed?
  - [ ] Phasing (Section 11) — four phases at v1.0.20–23 sized
    right?
  - [ ] Open questions (Section 12) — recommendations align with
    your view?

When the schema and event-vocabulary are agreed, Phase 1 lands as
the next proxy-side branch. The role-coverage tests in Phase 2 are
where the contract actually starts enforcing itself; that's the
inflection point.

---

*Author: Claude Opus 4.7 (1M context). Proxy-side artifact;
authored under the umbrella's
docs/notes/postmortem-selector-watchdog-2026-05.md scope and the
institutional-software bar set by the user during the v1.0.20
planning conversation.*
