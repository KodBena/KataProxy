# Logging — Operator Reference

The proxy emits a structured log over a closed event vocabulary. This
document is the operator-facing reference: how to configure output,
how to read each formatter, how to filter for the question you have
in front of you, and what each role's logs look like in practice.

For the design rationale, the schema invariants, and the contract
that defines what a "valid" log record is, see
`proxy/docs/logging-design.md`. This file is the **runtime** view;
the design memo is the **architectural** view.

Cross-references:

  - `proxy/docs/logging-design.md` — design memo (event vocabulary,
    field schema, formatter shapes, phasing).
  - `proxy/proxy_logging/__init__.py` — public API for the package.
  - `proxy/proxy_logging/events.py` — the closed `Event` enum and
    `EVENT_REQUIRED_FIELDS` schema.
  - `proxy/CLAUDE.md` — the logging-conventions section names the
    rules that apply when authoring or migrating call sites.

---

## 1. Quick start

The proxy configures its logger from the environment at startup.
The defaults are sensible for an interactive console session; one
or two env vars get the right behaviour for files, aggregators, and
debugging.

| Variable                         | Values                              | Default          | Effect                                                                                                                    |
|----------------------------------|-------------------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------|
| `PROXY_LOG_FORMAT`               | `auto` / `console` / `logfmt` / `json` | `auto` (TTY → console; pipe → logfmt) | Selects the renderer.                                                                                                     |
| `PROXY_LOG_DEST`                 | `stderr` / `file:<path>`            | `stderr`         | Where the log goes. `both` is currently aliased to `stderr`; for two destinations, redirect via shell.                    |
| `PYTHONLOGLEVEL`                 | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` / numeric | `INFO` | Standard stdlib level filter. Applies to the `kataproxy` logger hierarchy.                                                |
| `PROXY_LOG_TRACE_CID`            | a canonical-id prefix               | unset            | When set, drop every record whose `cid` ≠ this value. No-cid records (lifecycle: connect, disconnect) pass through.       |
| `PROXY_LOG_FILTER`               | a regex                             | unset            | Drop records whose rendered `msg` doesn't match. Free-text grep on the human-readable summary line.                       |
| `PROXY_LOG_NO_ABBREV`            | `true` / `false`                    | `false`          | Console formatter: render `cid` and `orig` in full instead of `prefix…`.                                                  |
| `PROXY_LOG_TRUNCATE`             | int (chars)                         | `256`            | `log_safe`'s upper bound on rendered string length.                                                                       |
| `PROXY_ROLE`                     | `LEAF` / `RELAY` / `SELECTOR` / `ECHO` / `REDIRECT` | `LEAF` | Process-wide role bound onto every record. Drives the role-tinted role token in the console renderer and the role coverage check. |

A few minimal recipes:

```bash
# Default interactive: coloured console on stderr, INFO+.
katago_proxy

# Aggregator-shipping: JSON, one record per line, full precision.
PROXY_LOG_FORMAT=json PYTHONLOGLEVEL=INFO katago_proxy 2> proxy.jsonl

# Debug a specific query end-to-end.
PROXY_LOG_TRACE_CID=hub_acd4d5d8 PYTHONLOGLEVEL=DEBUG katago_proxy

# DEBUG everything to a file in logfmt for grep.
PROXY_LOG_FORMAT=logfmt PYTHONLOGLEVEL=DEBUG \
  PROXY_LOG_DEST=file:/tmp/proxy.log katago_proxy
```

---

## 2. The three formatters

Every record carries the same structured-fields envelope; the
formatter only changes the rendering. Switching formatters never
loses information — the JSON renderer is the most precise (full
microsecond timestamp, no field truncation), and the console
renderer is the least (compact for human scanning).

### 2.1 Console (interactive)

```
23:03:35.225 INFO  [LEAF peer=('192.168.122.1', 47748)] complete cid=hub_6c… orig=range-…  complete (3929ms)
23:03:35.225 INFO   diagnostic cid=range-…  adaptive: orig_id='range-b3804abc-c51e-402c-bfd1-122df0243557-1778447011289' deepening turns=[3, 4, 5, 6, 7] quantile=0.25 extra_visits=800
23:03:35.226 INFO  [LEAF peer=('192.168.122.1', 47748)] orchestration_spawn cid=range-…  orchestration[adaptive_reevaluate] spawn sub=__orch__275f1542e06f parent=range-b3804abc-…
```

Layout:

```
HH:MM:SS.mmm LVL [ROLE label upstream peer=…] event cid=… orig=… msg
```

  - **Time-only** prefix: the date is implicit in the file or
    session. The full ISO 8601 timestamp with timezone is preserved
    in the underlying record (see logfmt / JSON).
  - **Level** is rendered as a stable 5-char abbreviation
    (`DEBUG/INFO /WARN /ERROR/CRIT`) so the columns stay aligned
    when grepping.
  - **Role context block** wraps the most-specific identifier the
    record's bind chain carries: `[ROLE label]` for SELECTOR with a
    label bound; `[ROLE upstream]` for RELAY with an upstream URL
    bound; `[ROLE peer=…]` for LEAF / ECHO with a session bound.
    Bare `[ROLE]` for process-level records before per-session
    binding.
  - **Cid / orig** are abbreviated to their first 6 chars + `…` for
    visual scanning. Full values are in the underlying record;
    set `PROXY_LOG_NO_ABBREV=true` to render them in full.
  - **Msg** is the human-readable summary the call site passed via
    `msg=`. It's elided when it equals `event` (the typical
    "subscribe ANALYZE" case where the summary adds no information
    over the event token + structured fields).

The role token is colour-tinted on TTY (cyan LEAF, magenta RELAY,
green SELECTOR, yellow ECHO, blue REDIRECT). Set `NO_COLOR=1` to
disable.

### 2.2 logfmt (pipe / aggregator-friendly)

```
ts=2026-05-10T23:03:35.225+02:00 level=INFO role=LEAF module=proxy_server event=complete session="('192.168.122.1', 47748)" cid=hub_6ce2b53d1f9216ec8a9a orig=range-b3804abc-c51e-402c-bfd1-122df0243557-1778447011289 duration_ms=3929 msg="complete (3929ms)"
```

Header fields render in fixed order
(`ts level role module event session label upstream cid orig action direction kind`),
the event-specific tail in alphabetical order. Stable ordering means
`grep "event=complete"` and `awk '/event=complete/ {…}'` work
reliably across releases.

### 2.3 JSON (machine-consumable)

```json
{"ts":"2026-05-10T21:03:35.225417+00:00","level":"INFO","role":"LEAF","module":"proxy_server","event":"complete","session":"('192.168.122.1', 47748)","cid":"hub_6ce2b53d1f9216ec8a9a","orig":"range-b3804abc-c51e-402c-bfd1-122df0243557-1778447011289","duration_ms":3929,"msg":"complete (3929ms)"}
```

One record per line. The `ts` field is full-precision UTC ISO 8601
with timezone offset. Sets / frozensets render as sorted lists;
bytes via `repr()`. The renderer is JSON-strict — every field is
JSON-friendly because the schema requires it.

---

## 3. The event vocabulary at a glance

The closed `Event` enum partitions log records into eleven
categories. The table below is a navigation aid; the full per-event
required-field schema is in `proxy/proxy_logging/events.py` and the
authoritative tables are §4 of `logging-design.md`.

| Group               | Examples                                                       | Typical level  |
|---------------------|----------------------------------------------------------------|----------------|
| Connection          | `connect`, `disconnect`, `connect_refused`, `rate_limited`     | INFO / WARNING |
| Wire ingress        | `recv`, `parse`, `parse_error`                                 | DEBUG / ERROR  |
| Hub coalescing      | `subscribe`, `coalesce`, `cache_hit`, `cache_miss`             | INFO           |
| Dispatch            | `dispatch`, `broadcast`, `dispatch_error`, `no_upstream`       | INFO / ERROR   |
| Response            | `respond`, `forward`, `respond_dropped`, `complete`            | DEBUG\* / INFO |
| Terminate           | `terminate_recv`, `terminate_dispatch`, `terminate_synthesized`, `terminate_complete` | INFO  |
| Keep-alive          | `keepalive_reset`, `keepalive_check`, `keepalive_fired`        | DEBUG / WARNING |
| KataGo subprocess   | `kg_spawn`, `kg_ready`, `kg_crash`, `kg_respawn`, `kg_unhealthy` | INFO / ERROR  |
| Upstream            | `upstream_connect`, `upstream_disconnect`, `upstream_unhealthy` | INFO / ERROR   |
| Middleware          | `middleware_engage`, `middleware_skip`, `transformer_apply`, `orchestration_spawn`, `orchestration_done` | INFO  |
| Diagnostic          | `diagnostic` (catch-all)                                       | varies         |

\*`forward` and `respond` are kind-aware: `partial` → DEBUG,
`final` / `metadata` / `error` → INFO. The demand-edge timing for
authoritative responses stays visible at the default level without
flooding INFO with per-partial mid-search updates. See
`lifecycle.forward` and `lifecycle.respond` in
`proxy_logging/lifecycle.py`.

Every record additionally carries the `role`, `module`, `ts`, and
`level` envelope. `role` comes from `PROXY_ROLE` (bound process-wide
at startup); `module` is the originating Python module's last
segment; `ts` and `level` are stdlib-derived.

---

## 4. Tracing, filtering, redaction

### 4.1 Tracing one query end-to-end

`PROXY_LOG_TRACE_CID=<cid>` drops every record whose `cid` field is
present and not equal to the target. Records with no `cid`
(connect, disconnect, kg_spawn, kg_crash, keepalive_*, etc.) pass
through unconditionally — the trace mode is "this query plus the
session-lifecycle context around it," not "literally only records
with this cid."

Useful with the canonical id from a subscribe / dispatch event:

```bash
# Discover the canonical from a SUBSCRIBE event…
katago_proxy 2>&1 | rg subscribe | head -3
# 23:03:31.296 INFO  [LEAF peer=…] subscribe cid=hub_6c… orig=range-…  subscribe ANALYZE
# Then trace it:
PROXY_LOG_TRACE_CID=hub_6ce2b53d1f9216ec8a9a katago_proxy
```

The `cid` on the wire is the **canonical** id (post-coalescing).
The `orig` is the per-subscriber wire id (the SPA's request id, or
an internal `__orch__…` for orchestration sub-queries). When more
than one subscriber rides one canonical (the coalescing case),
their per-subscriber records will all share a `cid` and differ in
`orig`.

### 4.2 Free-text filter

`PROXY_LOG_FILTER=<regex>` keeps only records whose `msg` matches.
This is the ad-hoc grep, useful for "I want every record mentioning
this peer" or "everything that says 'unhealthy'":

```bash
PROXY_LOG_FILTER='192\.168\.122\.1' katago_proxy   # one peer
PROXY_LOG_FILTER='unhealthy' katago_proxy          # any unhealthy event
```

The regex matches against `getMessage()` — the human-readable
summary, not the structured fields. For structured-field filtering
(e.g., "every record with `kind=final`"), prefer aggregator-side
filters on the JSON output, or use the per-cid trace.

### 4.3 Redaction & truncation

The proxy ships two PII / payload-safety primitives carried over
from the pre-arc shim:

  - `log_safe(s)` truncates at `PROXY_LOG_TRUNCATE` chars (default
    256), repr-escapes newlines, and renders the head + a
    `…[N more chars]` suffix when truncated. Used wherever a wire
    payload's substring or stderr tail might land in a record.
  - `filter_dict(d)` strips three high-volume KataGo-response keys
    (`policy`, `ownership`, `moveInfos`'s heavy fields) before
    rendering. Used by the DEBUG-level wire-trace renderer to keep
    a single record from spanning hundreds of lines.

Both are called inside the structured emitters; operators rarely
need to invoke them directly. To raise the truncation cap for an
incident: `PROXY_LOG_TRUNCATE=4096 katago_proxy`.

---

## 5. Reading a record in each role

The bind chain attaches different context to records depending on
the role. The interesting per-role bindings:

### 5.1 LEAF

Every record carries `role=LEAF` and (per-session) `session=…`. The
KataGo subprocess events carry `kg_pid` and (on spawn) `kg_cmd`; on
crash, `exit_code` and `stderr_tail`.

```
23:03:30.318 INFO  [LEAF peer=('192.168.122.1', 47748)] subscribe cid=hub_ac… orig=wd-177…  subscribe QUERY_VERSION
23:03:30.319 WARN  [router] katago[pid=2721644]: 2026-05-10 23:03:30+0200: Request: {"id": "hub_acd4d5d85296de7577d5", "action": "query_version"}
```

The second line is KataGo's own stderr forwarded as a WARNING
through the `kg_stderr` shim — these don't carry structured fields,
so the formatter falls back to `[module] msg`. The kataproxy
`router` module is the stderr drain.

### 5.2 RELAY

Records inside a per-upstream `ClientSession` (the relay's own
client side, talking to a downstream LEAF) carry `upstream=<URL>` in
the bind chain. The upstream-connection events
(`upstream_connect`, `upstream_disconnect`, `upstream_unhealthy`)
fire from the RelayRouter's connection management.

The hash-ring routing means each cid lands deterministically on one
upstream for `ANALYZE`. Metadata actions (`QUERY_VERSION`,
`TERMINATE_ALL`, `CLEAR_CACHE`) broadcast — expect one `dispatch`
per ANALYZE and one `broadcast` per metadata action. See the
heartbeat-fanout-contract section in `proxy/CLAUDE.md`.

### 5.3 SELECTOR

Same shape as RELAY for the per-upstream side, plus a `label=<name>`
bound onto the per-upstream session. The label is the SELECTOR
configuration key (from `SELECTOR_MODELS`); it's the wire-routing
key the SPA sets via the `model` field on the analysis query.

```
... [SELECTOR strong upstream=ws://gpu1:41949] respond cid=… orig=… kind=final  ↓ final
```

The `label` and `upstream` bindings render as the role-context block
in the console formatter; in logfmt / JSON they appear as
top-level fields.

### 5.4 ECHO / REDIRECT

ECHO synthesises canned responses without an upstream and binds
neither `upstream` nor `label`. REDIRECT sends a `redirect` wire
frame and closes; it logs at `connect` and `disconnect` only.

---

## 6. Operator recipes

### 6.1 "When did query X actually reach the SPA?"

Use the `forward` event (kind-aware level: finals/metadata/errors at
INFO, partials at DEBUG):

```bash
PROXY_LOG_TRACE_CID=<cid> PYTHONLOGLEVEL=INFO katago_proxy \
  | rg 'event=(respond|forward|complete)'
```

  - `respond` (INFO for finals / metadata / errors) is the
    upstream-to-proxy edge — when KataGo emitted the response.
  - `forward` (INFO for finals / metadata / errors) is the
    proxy-to-client edge — when the proxy actually sent it on the
    wire to the SPA.
  - `complete` (INFO) is the per-orig_id lifecycle close.

The gap between `respond` and `forward` for a turn is the proxy's
in-band middleware processing time. Adaptive_reevaluate's range-
query buffering — when the v1.0.20 streaming-previews refactor
shipped — is the worked example: see the rationale in
`proxy/middleware/adaptive_reevaluate.py`.

### 6.2 "Why does this query hang?"

The lifecycle should be `subscribe → dispatch → respond × N → complete`.
A hang typically means one of the middle steps didn't fire. With
DEBUG enabled, every step is visible:

```bash
PYTHONLOGLEVEL=DEBUG katago_proxy 2>&1 | rg "cid=hub_6ce2b53d"
```

Common patterns:

  - `dispatch` fires but no `respond` follows → check
    `kg_unhealthy` / `upstream_unhealthy` / `kg_crash` events; the
    backend has stopped responding.
  - `respond` records arrive but `complete` never does → the
    CompletionTracker still expects more turns. Check the orig
    query's `analyze_turns` count vs. the `respond` count for the
    cid.
  - `subscribe` but no `dispatch` (RELAY or SELECTOR) → no healthy
    upstream available; expect a `no_upstream` ERROR record.

### 6.3 "What's the keep-alive watchdog doing?"

```bash
PYTHONLOGLEVEL=DEBUG katago_proxy 2>&1 \
  | rg 'event=(keepalive_reset|keepalive_check|keepalive_fired)'
```

  - `keepalive_reset` — the per-session watchdog rearmed (a
    QUERY_VERSION arrived, or the deadline was extended for an
    in-flight query).
  - `keepalive_check` (DEBUG) — the periodic check tick. Carries
    `idle_seconds` and `in_flight_count`.
  - `keepalive_fired` (WARNING) — the watchdog terminated active
    queries on the session because the idle deadline elapsed.
    Carries the list of `terminated_cids`.

If you're seeing `keepalive_fired` more than expected on a
multi-upstream router, see the heartbeat-fanout-contract section
in `proxy/CLAUDE.md` — the SELECTOR watchdog regression
(v1.0.18 / v1.0.19 fixes) is the canonical case where this
matters.

### 6.4 "Did this query coalesce?"

When two clients submit semantically-identical analyze queries, the
hub coalesces them onto a single canonical. Each client gets its
own `subscribe` record (with their own `orig`); the second one
gets a `coalesce` event instead of a `dispatch`:

```bash
rg 'event=(subscribe|coalesce|dispatch|cache_hit) cid=<cid>'
```

`cache_hit` fires when a previously-cached canonical's full
response sequence is replayed — the subscriber gets every
historical response without re-running KataGo.

### 6.5 "Show me everything for one peer"

```bash
PROXY_LOG_FILTER="$(printf '%q' "192.168.122.1")" katago_proxy
```

Or in logfmt:

```bash
PROXY_LOG_FORMAT=logfmt katago_proxy 2>&1 \
  | rg 'session="\(.192.168.122.1.,'
```

The `session` field carries the peer tuple `('<ip>', <port>)` — the
quoting in the regex matches the logfmt-quoted form.

---

## 7. Troubleshooting

**The console renderer doesn't show colours.**
Stderr isn't a TTY (you redirected, or you're inside a pipeline).
Set `PROXY_LOG_FORMAT=console` to force the renderer regardless of
TTY detection; or `NO_COLOR=` (empty) won't help — the proxy uses
NO_COLOR=set as the disable signal, not value.

**The `forward` event isn't appearing for partials at INFO.**
That's the kind-aware level split. Partials are DEBUG to avoid
flooding INFO; set `PYTHONLOGLEVEL=DEBUG` to see them.

**A field I expect is missing from the rendered output.**
The console formatter renders only the header tier explicitly
(`ts level role module event session label upstream cid orig action direction kind`)
plus the `msg`. Tail fields land in the structured record but
aren't shown by the console renderer. Use logfmt or JSON to see
the full set; this is intentional — the console renderer
optimises for visual scanning, not completeness.

**A record raised `LogContractError` at startup or in a test.**
The call site bound or supplied a field name that collides with a
stdlib `LogRecord` reserved attribute (`name`, `msg`, `args`,
`levelname`, etc.) — the structured logger refuses these to
prevent silent corruption of the record. Rename the field at the
call site (the `name` → `orch_name` rename in v1.0.20 is the
worked example). The full reserved-name set is in
`proxy_logging/adapter.py:_LOGRECORD_RESERVED`.

**A `LogContractError` says "unknown event".**
The call site passed a string that doesn't match any `Event` enum
member. The vocabulary is closed by design — adding a new event
requires a code change in `proxy_logging/events.py` (Event member
+ `EVENT_REQUIRED_FIELDS` entry). For one-off diagnostic records
that don't fit a typed category, use `Event.DIAGNOSTIC`.

**A `LogContractError` says "missing fields".**
The merged bind-chain + call-site fields didn't include every key
the event's `EVENT_REQUIRED_FIELDS` entry names. The error message
lists the missing names; either the call site needs to supply
them, or the bind chain (in `ClientSession.__init__` /
`router.py`'s router constructors) needs to bind them earlier.

---

## License

Public Domain (The Unlicense). See `UNLICENSE` at the project root.
