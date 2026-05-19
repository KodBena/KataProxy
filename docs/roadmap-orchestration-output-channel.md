# Roadmap — Orchestration output channel (post-v1.0.26)

- **Status:** `design-note: planned`
- **Date:** 2026-05-19
- **Scope:** `proxy/middleware/orchestration.py` and the
  `_deliver_upstream` path in `proxy/proxy_server.py`. The
  Layer 1 mechanism by which an orchestration coroutine's yields
  reach the client. No wire-shape changes; no new role; no env-var
  additions. Internal framework lift only.
- **Origin:** Diagnosis of the omitted-finals symptom surfaced
  2026-05-19 under adaptive_reevaluate's Phase 3 dispatch path
  with multi-round (jerry-rig: `max_rounds=8`). The SPA observes
  Stage 1 partials reaching, some adaptive partials reaching, but
  the finalization stage's `is_during_search=False` emissions
  never arriving. Proxy log confirms `orchestration_done
  outcome=normal` with zero `forward kind=final` events for the
  parent orig_id.
- **Companion to:** the v1.0.16 design-time record at
  `roadmap-orchestration-middleware.md`. That roadmap is the
  authoritative planning artefact for the orchestration substrate;
  this one is the follow-on that addresses an
  implementation-level gap the original didn't cover.

---

## TL;DR

The orchestration framework's `handle_response` is contractually
the output channel: when an orchestration coroutine `yield`s a
response, that yield is expected to reach the WebSocket via
`_deliver_upstream`'s `async for ... in middleware.handle_response`.

The current implementation harvests yields by:

  1. routing the incoming response into the coroutine's input
     queue (parent's `_original_queue` or a sub-query's
     `record.queue`),
  2. `await asyncio.sleep(0)` to give the coroutine a chance to
     run, then
  3. non-blocking drain of `ctx._output_queue`.

`asyncio.sleep(0)` yields exactly one event-loop iteration. For
parent-original responses the scheduling order works — the driver
task is waiting on `_original_queue.get()` and is the only task in
`_ready` besides handle_response, so it runs first and its yields
land in `_output_queue` before the drain. **For sub-query
responses a pump task is interposed** (the
`_stream_parallel_spawns` machinery reads from each sub-query's
`record.queue` and forwards into a merge queue), pushing the
driver's wakeup to the **next** event-loop iteration. The drain
runs in iteration N; the driver's yields land in `_output_queue`
in iteration N+1 — too late for handle_response_(N), and there is
no future `handle_response` invocation for the parent orig_id to
catch them on iteration N+1+k.

Concretely:

- **Stage 1 originals** reach the wire (driver runs ahead of
  drain).
- **Stage 2 previews** of sub-query responses are stranded for
  one response and drained by handle_response_(N+1) — i.e., they
  lag by one response.
- **Stage 3 finalizations** are stranded entirely: they're emitted
  after the LAST sub-query response is processed, and no future
  `handle_response` for the parent orig_id ever fires. The driver's
  finally pushes `_SENTINEL` and pops the context; the items sit
  in the now-orphaned queue until GC.

The fix shape: make the orchestration framework's output channel
**push-based** rather than rely on a drain heuristic. The driver
task delivers yields directly into the session's send path,
decoupling output timing from input-response arrival.

---

## What the SPA sees vs. what the proxy emits

Surface observation (user, 2026-05-19):

> SPA sees no final turn. … I get all the partials up to … the
> first? or all? of the adaptive re-evaluations. Only the finals
> are omitted.

Live log signature for one run (8-round jerry-rig active,
adaptive_reevaluate with Phase 3 / learned_v1):

| Event | Count |
|---|---|
| `subscribe` (parent ANALYZE, `orig=range-…`) | 1 |
| `complete` (parent QUERY_COMPLETE at Hub) | 1 |
| `diagnostic` (adaptive round emit, one per round) | 8 |
| `orchestration_spawn` (one per sub-query) | 128 |
| `complete` (sub-query QUERY_COMPLETE) | 128 |
| `orchestration_done` (coroutine returned `normal`) | 1 |
| `forward kind=final` for parent orig_id | **0** |
| `forward kind=metadata` for parent orig_id | **0** |
| `forward kind=partial` for parent orig_id | unknown (DEBUG-filtered out of INFO log; SPA confirms "some" arrived) |

`outcome=normal` is the diagnostic that proves Stage 3 ran: the
coroutine's natural return path is via the end of the function
body, which is the final loop. An exception in Stage 3 would
yield `outcome=error`; cancellation would yield
`outcome=cancelled`. Neither fired.

So Stage 3 ran. Its `is_during_search=False` yields were
produced. Those yields never became `forward` events. Therefore
they never reached `lifecycle.forward` in `_deliver_upstream`'s
loop, therefore they never reached `ws.send`.

---

## The drain/driver race — mechanism

### The orchestration substrate's output path

A yield from the orchestration coroutine flows through this
sequence:

```
coro yield
  → _drive_coroutine's `async for resp in coro`
  → await ctx._output_queue.put((parent_id, resp))    # synchronous on unbounded queue
  → … coroutine continues to next yield or return …
  → driver's `async for` exits (coro returned)
  → driver's finally:
      log ORCHESTRATION_DONE
      terminate any leftover sub-queries
      await ctx._output_queue.put(_SENTINEL)
      pop context from self._contexts
```

Meanwhile, the drain side:

```
_deliver_upstream(wire) for some incoming response
  → middleware.handle_response(orig_id, response, …)
      (orchestration middleware's handle_response)
  → await ctx._push_{original,sub_response}(response)
      → puts to _original_queue or record.queue
      → schedules the driver task / a pump task as ready
  → await asyncio.sleep(0)
      → adds handle_response_task to _ready
  → drain ctx._output_queue with get_nowait
      → yields each non-sentinel item back through the
        async generator to _deliver_upstream's `async for`
```

The drain only runs as part of `handle_response`'s invocation,
which itself only runs when an incoming response arrives for this
session.

### CPython's `_run_once` snapshot semantics

CPython's asyncio event loop processes a snapshot of `_ready`:

```python
def _run_once(self):
    ...
    ntodo = len(self._ready)
    for i in range(ntodo):
        handle = self._ready.popleft()
        ...
        handle._run()
```

`ntodo` is sampled at the start. Callbacks scheduled by
`call_soon` (which is what `Queue.put_nowait`'s
`_wakeup_next(getters)` ultimately does) during the iteration go
into `_ready` but are **not** processed in this iteration. They
fire on the next `_run_once`.

`await asyncio.sleep(0)` schedules the current task's resumption
via `call_soon`. So after `sleep(0)` yields, the task is at the
end of `_ready` for the next iteration.

### The Stage 1 (originals) path — works

For a parent original response:

```
iteration N _ready snapshot: [driver_task, handle_response_task]
  - driver_task: was suspended on `await self._original_queue.get()`;
    wakes from the push, consumes the response, yields preview through
    coro, driver puts (parent_id, resp) to _output_queue, asks
    next __anext__, coro suspends on original_queue.get again
    (queue now empty)
  - handle_response_task: sleep(0) returns, drains output_queue:
    sees the preview, yields it to _deliver_upstream
  → lifecycle.forward(kind=partial) fires
  → ws.send fires
```

Why driver_task is in the iteration-N snapshot: when
`_push_original(response)` calls `_original_queue.put(response)`,
the put's `_wakeup_next` is called **before**
`handle_response`'s `await asyncio.sleep(0)`. The driver's getter
future is set_result'd, scheduling its callback — but it's
scheduled into `_ready` *before* sleep(0) adds the
handle_response task. So both are in the snapshot.

### The Stage 2 (sub-query response) path — lags by one

For a sub-query response (the common case during deepening):

```
iteration N _ready snapshot: [pump_task, handle_response_task]
  - pump_task: was suspended on `await record.queue.get()` inside
    ctx.spawn; wakes from the push, consumes the response, calls
    `await merge.queue.put(resp)` which schedules driver_task
    (call_soon → _ready, deferred to N+1), consumes the SENTINEL
    on next iter, exits ctx.spawn, puts merge_sentinel, task done
  - handle_response_task: sleep(0) returns, drains output_queue:
    EMPTY (driver hasn't run yet)
  → handle_response yields nothing this invocation

iteration N+1 _ready: [driver_task]
  - driver_task: consumes from merge.queue, coro runs Stage 2 body,
    state.observe(resp), yields preview, driver puts to
    output_queue, coro suspends on next merge.queue.get
  → output_queue now has one item, but no consumer this iteration

(later, when next sub-query response triggers handle_response_(N+1)):
  handle_response_(N+1) push, sleep(0), drain
  → drain picks up the preview pushed in iteration N+1
  → forward kind=partial (DEBUG-filtered out of INFO log)
```

So Stage 2 previews **do** reach the wire, with a one-response
delay. The user reports "the first? or all? of the adaptive
re-evaluations" reach — the last round's previews are the ones
stranded together with Stage 3 (below).

### The Stage 3 (finalization) path — stranded

The LAST sub-query response of the LAST round triggers
handle_response_(LAST). In iteration N+1 after that invocation
returns, the driver runs, processes the final sub-query response,
exits the Stage 2 loop, runs Stage 3's `for f in finals:
yield replace(latest, is_during_search=False)` — synchronous loop,
each yield → output_queue.put — coroutine returns, driver's
finally pushes `_SENTINEL`, pops context.

By the time iteration N+1 ends, output_queue contains:
`[preview_last, stage3_1, stage3_2, …, stage3_N, _SENTINEL]`.

**No future `handle_response` invocation will fire for the parent
orig_id** — all sub-queries are done, the parent has reached its
QUERY_COMPLETE. The session's send loop is blocked on
`_send_queue.get()` waiting for the next Hub fanout (heartbeats
arriving every 5s on `wd-…` orig_ids only).

Heartbeats go through their own `OrchestrationContext` (each query
gets one via `on_query`). Their `handle_response` invocation
drains *their own* context's `_output_queue`, not the range's.
So the range's Stage 3 emissions are stranded indefinitely. The
context is popped from `self._contexts` (driver's finally), so
even if some later mechanism tried to look up the parent context,
it wouldn't find it.

The user's observation — "GPU is idle and the SPA is saying that
the query is still in flight" — matches exactly: all proxy work
is done; the SPA's pending-final accounting will never settle
because the `is_during_search=False` packets never arrive.

---

## Why the original v1.0.16 design didn't surface this

`roadmap-orchestration-middleware.md`'s §"Risks and design
decisions" covers cancellation, error masking, response-routing
complexity in `_deliver_upstream`, depth-overflow, and the
single-orchestration-per-chain limit. **None of these address
output-side timing.** The roadmap treats `handle_response` as the
output channel implicitly, via the existing `SessionMiddleware`
contract; the `await asyncio.sleep(0) + drain` pattern is an
implementation detail of the v1.0.16 commit, not a contract
discussed in the design.

The roadmap also predates Phase 2's multi-round dispatch
(v1.0.24) and Phase 3's per-turn parallel dispatch (v1.0.25). In
the v1.0.16 single-round shape, adaptive_reevaluate spawned **one**
deeper query covering the whole deepening set. That one sub-query
produced N finals (one per deepening turn) on a single
`record.queue`. The Stage 3 stage didn't exist yet — the
v1.0.16-shape adaptive simply yielded sub-query responses through.

The trailing-window stranding fails to surface there because the
last yield always corresponded to a sub-query response that was
itself triggered by an incoming wire — handle_response drained it
the same way it'd drain a parent original. It's the v1.0.24
finalization-at-end stage (single authoritative emission per turn
emitted *after* the loop body, decoupled from any incoming
response) that turns the latent race into a guaranteed
stranding.

The "Risk: cancellation semantics" section mentions a
`try/finally` in the framework that cleans up in-flight
sub-queries. There is no symmetric mechanism for flushing pending
output. That asymmetry is the gap this arc closes.

---

## Severity calibration (ADR-0008 §"substitution test")

The substitution test: name the failure shape in its most general
form; list surfaces it could apply to; calibrate to the worst case.

**Failure shape:** orchestration coroutine yields produced after
the last input-response-triggered `handle_response` invocation are
stranded in `_output_queue` and never reach the wire.

**Surfaces this could apply to:**

- adaptive_reevaluate's Stage 3 finalization
  (`is_during_search=False` per-turn finals). Current observed
  instance; the parent's protocol-level QUERY_COMPLETE never
  reaches the SPA.
- Any future orchestration coroutine that emits a summary or
  closure response after its last sub-query (e.g., the
  `jsd_compare` use case the v1.0.16 roadmap anticipates: emit
  per-position JSD scores after the last comparison sub-query
  completes).
- Any orchestration coroutine that calls
  `ctx.discard_originals()` and emits its own response set —
  the closure response is by construction not tied to an input
  arrival.
- Error-path responses that the framework's
  `_drive_coroutine` synthesises from an exception
  (`MetadataResponse(opaque={"error": ...})`) — these are pushed
  to `_output_queue` in the `except` branch. If the exception
  fires *after* the last input response, they're stranded too —
  the client sees nothing.

The worst case on this list is the error-path stranding: a
structurally-impossible-to-recover-from error in the orchestration
coroutine produces a structured error response that's then
silently dropped. The SPA hangs with no signal at all. That
violates the ADR-0002 fail-loudly tenet at the level the tenet
exists to police — the failure becomes invisible at exactly the
point an operator most needs visibility.

**Calibration:** the fix shape must guarantee output delivery
across the **entire** lifetime of the orchestration coroutine,
including the trailing window after the last input response.
A heuristic that catches most cases but not the trailing window
is not adequate — the trailing window is the surface the
fail-loudly tenet most needs covered.

---

## Three fix shapes

### Option 1 — Push-based output channel

The driver task delivers yields directly into the session's send
path, decoupled from `handle_response`'s drain.

Shape:

- Extend `SessionCapabilities` with a method like
  `send_response(orig_id, response)` that pushes a synthetic
  response into the session's inbound delivery pipeline (the
  same point at which Hub fanout deposits responses).
- `_drive_coroutine` calls `caps.send_response(parent_id, resp)`
  for each yield from the coroutine, instead of pushing to
  `ctx._output_queue`.
- `handle_response` becomes input-only: route the incoming
  response to the parent's `original_queue` or a sub-query's
  `record.queue`, and yield nothing (the orchestration
  middleware emits no responses through the `async for`
  channel).
- The `_output_queue` and the drain disappear entirely from the
  orchestration substrate.

What this requires architecturally:

- `caps.send_response(orig_id, response)` needs to feed the
  response back through the same wire-translation +
  middleware-chain path that the Hub's fanout uses. The
  natural insertion point is the session's `_send_queue`
  (after wire-encoding) or a sibling channel that joins the
  pipeline just before `_deliver_upstream`. The latter is
  cleaner: synthetic responses can carry pre-built
  `KataGoResponse` dataclasses rather than re-wire-encoded
  JSON.
- The middleware chain's outer wrappers (KeepAliveMiddleware,
  any future outer middleware) still need to see the synthetic
  responses. So the insertion point has to be *before* the
  outer chain runs — which means `send_response` feeds back
  through `handle_response` of any outer middleware.

The trickiness: feeding back through `handle_response` of an
outer middleware means the orchestration's output looks to outer
middlewares like an "incoming response," which is the right
semantic (KeepAliveMiddleware should observe the
heartbeat-resetting effect of an orchestration's emission).
But the outer middleware's `handle_response` returns an async
generator; the orchestration framework's pushback needs to
collect that generator's yields and forward them to ws.send. This
is doable but requires the outer chain to be re-entered, which
isn't how `_deliver_upstream` is currently structured.

A cleaner shape: `send_response` joins the pipeline **at**
`_deliver_upstream`'s entry, going through the full chain just
like a regular response. The session's `_deliver_upstream` is the
single chokepoint. To avoid recursion (the orchestration's own
inner `handle_response` would also see the synthetic), the
orchestration middleware tags synthetic responses so its own
`handle_response` passes them through unchanged.

This is the principled fix. It addresses the asymmetry head-on:
input-side and output-side are now both push-based and traverse
the chain in the same direction.

**Implementation cost:** medium-substantial. New method on
`SessionCapabilities`. Threading of "synthetic" tag (or a
side-table of orig_ids the orchestration is currently driving)
through `_deliver_upstream`. Removal of `_output_queue` and the
drain. The orchestration coroutine's behaviour is unchanged from
the author's perspective; only the framework's internals change.

**Test impact:** the existing orchestration test suite's
"handle_response yields back" assertions need rewriting against
the new contract. The wire-shape assertions (the
TestAdaptiveReevaluateMetadata class and downstream regressions)
should pass unchanged.

### Option 2 — Settled-wait on a driver-signalled condition

`handle_response` waits on an `asyncio.Event` that the driver
sets after producing a batch of yields. The event is set whenever
the driver runs through its body until the next true suspension
(merge.queue.get / record.queue.get / coroutine return).

Shape:

- Per-`OrchestrationContext`: an `asyncio.Event` named
  `_progress` and a flag `_done`.
- `_drive_coroutine`'s `async for resp in coro` body: after each
  `output_queue.put`, set `_progress`. On exit/finally, set
  `_done`.
- `handle_response` after pushing input: `await
  ctx._progress.wait()` with a small timeout (e.g., 5ms) OR
  `ctx._done`. Clear `_progress` after waking. Then drain.
- Trailing window: when the driver's finally runs after the last
  sub-query response, it sets `_done`. The last
  `handle_response` would normally have returned by then, but a
  separate "trailing flush" mechanism is still needed —
  `handle_response` after the LAST response has already yielded
  to its caller; there's no further "wake up and drain."

So Option 2 alone doesn't address the trailing-window stranding.
It addresses the Stage 2 lag (each handle_response would catch
its own driver's emissions) but Stage 3 still needs Option 1 or
a separate flush mechanism.

The fundamental problem: `handle_response` runs per incoming
response. The trailing window has zero incoming responses by
definition. No `handle_response` runs in the trailing window.

**Implementation cost:** small (the `_progress` event +
`_done` flag + a small await). **But doesn't fix the
trailing window**, which is the user-observed symptom and the
worst-case surface (per the substitution test).

### Option 3 — Multi-tick heuristic

Replace `await asyncio.sleep(0)` with a loop that yields multiple
times until output_queue appears settled.

Shape:

```python
for _ in range(N):
    await asyncio.sleep(0)
while not output_queue.empty():
    item = output_queue.get_nowait()
    if isinstance(item, _Sentinel):
        break
    yield item
    await asyncio.sleep(0)
```

This buys more event-loop iterations for the driver to produce
yields before handle_response gives up. With enough iterations,
the driver finishes Stage 3 and pushes the SENTINEL; the drain
sees them all.

**This is still a heuristic.** The right N depends on the
coroutine's internal await structure. The Stage 3 case in adaptive
runs synchronously between yields (no internal awaits), so
plausibly 1-2 extra iterations would catch it. But future
orchestration coroutines might have internal `await`s that change
the picture. The fix is fragile.

The trailing-window stranding is also not fully addressed: the
LAST handle_response runs the multi-tick drain, which catches
Stage 3 yields **only if** the driver finishes Stage 3 within the
N extra iterations. If a coroutine takes longer (e.g., does some
async I/O in its closure), the yields are still stranded.

**Implementation cost:** trivial (a small loop). **Heuristic, not
guaranteed.** Doesn't honor the substitution test's worst-case
calibration.

---

## Recommendation: Option 1

Per the substitution test in §"Severity calibration", the fix
shape needs to guarantee output delivery across the entire
coroutine lifetime including the trailing window. Only Option 1
does this. Options 2 and 3 are partial fixes that leave the worst
case (error-path responses synthesised after the last input
response) silently stranded — exactly the failure shape ADR-0002
forbids.

Option 1 also matches the original orchestration roadmap's
design philosophy: "the framework owns parent-child tracking,
lifecycle, cleanup, cancellation; the middleware owns *what* gets
spawned and *how results are joined*." Output delivery is part of
"lifecycle" — the framework should own it. The current drain
pattern is the framework half-owning it (via handle_response's
heuristic), which is the asymmetry this arc closes.

The principle the near-miss letters surface applies here too:
when the abstraction promises something it can't structurally
deliver, the honest fix is to revise the abstraction rather than
add operational glue to make it look like it works. handle_response
as the output channel was an under-specified contract in v1.0.16;
this arc rewrites it.

---

## Migration arc sketch

A focused multi-commit arc on a feat branch. Six commits, each
keeps `mypy --strict` green and the existing test suite passing
(modulo the orchestration tests, which need the contract
rewrite).

### Commit 1 — `SessionCapabilities.send_response` API

Additive. Extend `SessionCapabilities` with:

```python
async def send_response(
    self, orig_id: ClientId, response: KataGoResponse,
) -> None:
    """Inject a synthetic response into the session's delivery path.

    The response traverses the full middleware chain just like a
    Hub-fanned-out response, so outer middlewares observe it.
    The framework's orchestration middleware tags self-injected
    responses so they pass through its own handle_response
    unchanged (no infinite recursion).
    """
```

The implementation routes through a session-internal channel
(call it `_synthetic_queue`) that the send loop drains alongside
`_send_queue`. The send loop now awaits `asyncio.wait` over both
queues, processes whichever has data first.

No caller yet. Pure surface addition; existing tests unchanged.

### Commit 2 — `_deliver_upstream` accepts synthetic responses

Synthetic responses bypass the wire-parse step (they're already
`KataGoResponse` dataclasses). Otherwise they traverse the same
middleware chain.

`_deliver_upstream` gains a code path: if the incoming work item
is a synthetic response (carrying an `is_synthetic` marker or
arriving via the synthetic channel), skip `parse_response_from_wire`
and go straight to `chain.translate_upstream` → middleware chain
→ ws.send.

The middleware chain's `handle_response` runs the same as for
regular responses. The orchestration middleware's
`handle_response` detects the synthetic origin and passes the
response through unchanged (so its own coroutine doesn't get
re-fed its own output).

Tests: a unit test that synthetic responses traverse the chain
and reach ws.send.

### Commit 3 — Orchestration framework: push-based output

`OrchestrationMiddleware`'s `_drive_coroutine` rewrites to use
`caps.send_response`:

```python
async def _drive_coroutine(self, parent_id, ctx, coro):
    outcome = "normal"
    try:
        async for resp in coro:
            await self._caps.send_response(parent_id, resp)
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except Exception as e:
        outcome = "error"
        self._log.exception(...)
        err_response = MetadataResponse(opaque={
            "error": f"orchestration error in {self.name}: {e}",
        })
        await self._caps.send_response(parent_id, err_response)
    finally:
        self._log.info(Event.ORCHESTRATION_DONE, ...)
        for sub_orig_id in list(ctx._sub_queries.keys()):
            try:
                if self._caps is not None:
                    await self._caps.terminate_query(sub_orig_id)
            except Exception:
                self._log.debug(...)
        self._contexts.pop(parent_id, None)
        self._tasks.pop(parent_id, None)
        for sub_orig_id in list(ctx._sub_queries.keys()):
            self._sub_to_parent.pop(sub_orig_id, None)
```

`_output_queue` and `_Sentinel`-related machinery removed from
`OrchestrationContext`. The orchestration's `handle_response`
becomes input-only:

```python
async def handle_response(self, orig_id, response, submit_query):
    if self._is_synthetic(response):
        # Pass synthetic responses unchanged (we injected them).
        yield orig_id, response
        return
    ctx = self._contexts.get(orig_id)
    if ctx is not None:
        await ctx._push_original(response)
        return
    parent_id = self._sub_to_parent.get(orig_id)
    if parent_id is None:
        yield orig_id, response
        return
    ctx = self._contexts.get(parent_id)
    if ctx is None:
        self._log.debug(...)
        return
    await ctx._push_sub_response(orig_id, response)
```

No drain. No sleep(0). The coroutine's yields take their own
path to the wire.

### Commit 4 — Orchestration tests rewrite

The existing orchestration tests assert handle_response yields
back. They need to assert "synthetic responses reach the
session's send pipeline" instead. The wire-shape tests
(TestAdaptiveReevaluateMetadata + any downstream regressions) should
pass unchanged.

New tests:

- Stage 3 finalization yields all reach the wire (the regression
  test that wouldn't have caught the v1.0.24 bug pre-fix).
- Error-path synthesised error responses reach the wire even
  when the exception fires after the last input response.
- Multi-round deepening: every round's previews reach the wire
  in real time (not lagged by one response).

### Commit 5 — Adaptive_reevaluate regression

Run the existing capability-negotiation and multi-round tests
against the rewritten orchestration substrate. Bit-for-bit
preservation of observable behaviour is the validation gate.

The specific case the user observed (8-round jerry-rig + Phase 3
+ learned_v1 + ~60 turns of deepening) should produce N
`forward kind=final` events at INFO, where N is the number of
analyzed turns. Wire frame inspection should match what KataGo's
contract says: exactly one `is_during_search=False` per
analyzed turn.

### Commit 6 — Documentation

- Update `FRAMEWORK.md`'s orchestration section: name
  `send_response` as the framework-managed output path; clarify
  that `handle_response` is input-routing only.
- Update `ARCHITECTURE.md`'s extension-points discussion to
  reflect the push-based shape.
- Sibling-revision `roadmap-orchestration-middleware.md` per
  ADR-0005 Rule 8: leave the v1.0.16 record in place; reference
  this roadmap as the post-v1.0.26 follow-on.

### Release

`v1.0.27` (next minor bump). Annotated tag with the per-release
changelog naming the bug and the structural fix.

---

## Risks and open questions

### Risk: outer middleware re-entrance

If an outer middleware (`KeepAliveMiddleware` is the only current
example) does work on each response it observes, synthetic
responses from orchestration will go through it. The
`KeepAliveMiddleware`'s heartbeat-resetting effect on observing
an orchestration's emission is structurally correct — an emission
IS activity on the session. But any future outer middleware that
mutates responses needs to be authored with this in mind.

Mitigation: synthetic responses are tagged so middleware can
choose to skip them if their semantics demand it. The orchestration
middleware itself uses the tag to avoid re-feeding its own output.

### Risk: ordering between synthetic and regular responses

The session's send loop draining two queues (regular and
synthetic) has no inherent ordering between them. For
adaptive_reevaluate, this matters: Stage 1 originals (regular,
since they come from Hub fanout to the parent's canonical) and
Stage 1 previews (synthetic, emitted by the coroutine) need to
arrive at the SPA in the right order — preview after original,
not before.

Mitigation: in the current implementation, Stage 1 yields a
preview *for each original it consumes*. So the natural order
is "original arrives → coroutine yields preview → send loop
emits preview." With the push-based shape, the preview's
`send_response` call happens AFTER the original was consumed
from `_original_queue`. As long as the synthetic queue's
delivery doesn't race ahead of the parent's QUERY_COMPLETE, the
order is preserved.

The cleaner shape: collapse the synthetic queue into the regular
`_send_queue`, so there's exactly one ordering. Synthetic
responses get a wire-encoded form (or a sibling channel that
joins the regular queue at the same point). Worth confirming
during implementation.

### Open question: do we still need `_output_queue` anywhere?

The push-based shape removes the per-context output queue. But
`_drive_coroutine`'s error-path synthesis still produces a
response; pushing via `caps.send_response` is the right shape.
The orchestration framework no longer owns any per-context
buffering on the output side — the per-session send pipeline owns
all of it.

The trade: a malicious or buggy coroutine that emits an unbounded
stream of yields would push them all into `_send_queue`. If
`_send_queue` has no bound, that's a memory pressure risk.
`_send_queue`'s bounds (or lack thereof) are an existing concern;
this arc doesn't change them. Worth flagging as a follow-on
hardening item.

### Open question: depth-overflow error reporting

The current depth-overflow path raises
`OrchestrationDepthError` from `ctx.spawn`. The error propagates
up to `_drive_coroutine`'s `except Exception` handler, which
synthesises a `MetadataResponse(opaque={"error": ...})` and pushes
to `_output_queue`. Under the push-based shape, the error pushes
via `caps.send_response` instead. The depth-overflow path needs
to be verified to deliver to the wire under the new shape.

### Open question: is `cfg.ORCHESTRATION_BUFFER_MAX` still needed?

The current bound applies to the per-context `_original_queue`
(bounds the buffer of parent originals before the coroutine
consumes them). This stays. The output-side buffer disappears.

### Open question: backward compatibility for the SessionMiddleware contract

The `SessionMiddleware` ABC declares `handle_response` returns a
`ResponseStream` (async iterator of `(orig_id, response)` tuples).
The orchestration middleware's implementation under the new shape
yields zero items for parent-managed responses (input-only).
Non-orchestration middlewares (KeepAliveMiddleware, the
not-orchestrated pass-through case inside the orchestration's own
`handle_response`) continue to yield through.

This is a semantic change for the framework but not for any
specific middleware author. The ABC's docstring should be updated
to reflect that an orchestration-style middleware delivers output
via `caps.send_response` rather than via yields.

---

## What this arc does NOT do

- **Does not change the orchestration coroutine's author-facing
  API.** Authors write the same `async def coro(parent, ctx)` and
  use `ctx.spawn`, `ctx.parallel`, `ctx.original_stream` exactly
  as before. The framework's internals change; the contract the
  middleware author writes against does not.
- **Does not change wire shape.** No new fields, no new control
  flags, no version bump on the wire. Pure internal lift.
- **Does not address the question of chained orchestration.** The
  single-orchestration-per-chain limit from v1.0.16 is structural
  per the algebraic-laws note in
  `roadmap-orchestration-middleware.md` §"On chained
  orchestration". This arc doesn't lift it.
- **Does not relate to the v1.0.21 identity-type branding
  migration.** Branded `ClientId` continues to flow through
  unchanged.
- **Does not relate to the v1.0.24 multi-round substrate or the
  v1.0.25 Phase 3 allocation substrate.** Those substrates'
  correctness depends on the orchestration framework working —
  which is precisely what this arc fixes — but their own code
  (adaptive_reevaluate.py, allocation.py, visit_scaling.py)
  doesn't change.

---

## Related documents

- `roadmap-orchestration-middleware.md` — the v1.0.16 design-time
  record this arc follows on from. That document remains the
  authoritative planning artefact for the orchestration substrate;
  this one is the post-v1.0.26 implementation-level follow-on.
- `design-fork-join-orchestration.md` — the broader design
  exploration that landed Option C (generator-style coroutines).
  The choice between coroutines and a true effects system (Option
  E) is the deeper question the substitution-test analysis here
  re-surfaces; the current arc stays within Option C but tightens
  its output-side contract.
- `roadmap-multi-round-adaptation.md` — the v1.0.24 multi-round
  substrate that introduced the explicit Stage 3 "finalization at
  end-of-loop" pattern. The Stage 3 design is *correct* against
  the KataGo wire contract; what fails is the orchestration
  framework's delivery of Stage 3's yields to the wire.
- `proxy/CLAUDE.md` §"Heartbeat-fanout contract" — the analogous
  "framework owns lifecycle" pattern on the input side; this arc
  brings the output side to symmetric coverage.
- The umbrella's two proxy-to-proxy near-miss letters
  (`id-translation-near-miss`, `selector-canonical-key-near-miss`)
  — the lesson their composition prescribes ("read the abstractions
  first, *and* read them in full before claiming what falls out
  of them") applies to this arc too. The output-channel race
  surfaced from a *code read* of the orchestration substrate; the
  v1.0.16 roadmap describes the design without naming this gap.
  The two letters' posture supports the move this arc takes:
  revise the abstraction when it under-delivers on its advertised
  contract.

---

## Sunsetting

This memo is `design-note: planned`. When the six commits land
and v1.0.27 is tagged, the memo transitions to `design-note:
implemented` with implementation notes inline (per the v1.0.21 /
v1.0.22 / v1.0.23 / v1.0.24 / v1.0.25 / v1.0.26 post-implementation
annotation pattern). The original v1.0.16 roadmap stays
unchanged as the planning-time record for the orchestration
substrate's initial shape.

---

License: Public Domain (The Unlicense)
