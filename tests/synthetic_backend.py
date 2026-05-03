"""
synthetic_backend.py — A BackendRouter mock for KataProxy tests.

Drop-in replacement for LeafRouter that simulates a long-running ponder
without spawning KataGo. Used by tests/diagnose_*.py scripts to observe
proxy behaviour deterministically and without the 30–60s KataGo restart
penalty when something goes wrong.

What it does:

- For ANALYZE queries: spawns an asyncio task that emits an
  isDuringSearch=True wire response every `emit_interval_s` seconds for
  `max_intermediates` ticks, then a final isDuringSearch=False response
  per registered turn (mirroring how a real LEAF concludes an analyze).
  Setting `max_intermediates` very high effectively simulates an
  unbounded ponder.
- For TERMINATE: cancels the emit task for the targeted canonical and
  delivers a synthetic terminate ack via the supplied callbacks (matching
  the LeafRouter convention of registering the ack on a freshly-minted
  wire id with on_complete fired right after on_response).
- For other actions (TERMINATE_ALL, QUERY_VERSION, CLEAR_CACHE): single
  synthetic ack and complete.

What it records:

- `dispatched`: list of canonical_ids in dispatch order.
- `terminated`: list of canonical_ids in terminate order.
- `live`: dict of currently-pondering canonical_ids → emit counts.

The recorder lists are public attributes; tests inspect them directly.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from dataclasses import dataclass, field
from typing import Optional

from AbstractProxy.katago_proxy import KataGoAction, KataGoQuery
from router import BackendRouter, OnComplete, OnResponse, WireDict

logger = logging.getLogger("kataproxy.tests.synthetic_backend")


@dataclass
class _LiveCanonical:
    canonical_id: str
    on_response: OnResponse
    on_complete: OnComplete
    turns: list[int]
    emit_task: Optional[asyncio.Task] = None
    emit_count: int = 0


class SyntheticPonderingRouter(BackendRouter):
    """A BackendRouter that simulates long-running analyze without KataGo.

    Construct with `emit_interval_s` (how often to emit an intermediate
    response, default 50ms) and `max_intermediates` (how many intermediates
    before the natural-completion final fires, default 1_000_000 — enough
    to look unbounded for any reasonable test duration).
    """

    def __init__(
        self,
        *,
        emit_interval_s: float = 0.05,
        max_intermediates: int = 1_000_000,
    ) -> None:
        self._emit_interval_s = emit_interval_s
        self._max_intermediates = max_intermediates
        self._live: dict[str, _LiveCanonical] = {}
        self.dispatched: list[str] = []
        self.terminated: list[str] = []

    @property
    def live(self) -> dict[str, int]:
        """Currently-pondering canonical_ids → intermediate emit counts."""
        return {cid: lc.emit_count for cid, lc in self._live.items()}

    async def start(self) -> None:
        logger.info("started")

    async def dispatch(
        self,
        canonical_id: str,
        wire_dict: WireDict,
        query: KataGoQuery,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"dispatch canonical_id={canonical_id} action={query.action.name}")
        self.dispatched.append(canonical_id)

        if query.action != KataGoAction.ANALYZE:
            ack: WireDict = {"id": canonical_id}
            ack.update({k: v for k, v in wire_dict.items() if k != "id"})
            await on_response(canonical_id, ack)
            await on_complete(canonical_id)
            return

        turns = query.analyze_turns if query.analyze_turns else [0]
        live = _LiveCanonical(
            canonical_id=canonical_id,
            on_response=on_response,
            on_complete=on_complete,
            turns=list(turns),
        )
        self._live[canonical_id] = live
        live.emit_task = asyncio.create_task(
            self._emit_loop(canonical_id),
            name=f"synthetic-emit:{canonical_id}",
        )

    async def _emit_loop(self, canonical_id: str) -> None:
        try:
            for _ in range(self._max_intermediates):
                await asyncio.sleep(self._emit_interval_s)
                live = self._live.get(canonical_id)
                if live is None:
                    return
                live.emit_count += 1
                for turn in live.turns:
                    intermediate: WireDict = {
                        "id": canonical_id,
                        "isDuringSearch": True,
                        "turnNumber": turn,
                        "moveInfos": [],
                        "rootInfo": {"scoreLead": 0.0, "visits": live.emit_count},
                    }
                    await live.on_response(canonical_id, intermediate)
            # Natural completion: emit final per turn, then on_complete.
            live = self._live.pop(canonical_id, None)
            if live is None:
                return
            for turn in live.turns:
                final: WireDict = {
                    "id": canonical_id,
                    "isDuringSearch": False,
                    "turnNumber": turn,
                    "moveInfos": [],
                    "rootInfo": {"scoreLead": 0.0, "visits": live.emit_count},
                }
                await live.on_response(canonical_id, final)
            await live.on_complete(canonical_id)
        except asyncio.CancelledError:
            raise

    async def terminate(
        self,
        canonical_id: str,
        on_response: OnResponse,
        on_complete: OnComplete,
    ) -> None:
        logger.debug(f"terminate canonical_id={canonical_id}")
        self.terminated.append(canonical_id)

        live = self._live.pop(canonical_id, None)
        if live is not None and live.emit_task is not None:
            live.emit_task.cancel()
            try:
                await live.emit_task
            except asyncio.CancelledError:
                pass

        # Synthetic terminate ack on a freshly-minted wire id, matching the
        # LeafRouter convention. The ack carries terminateId=canonical_id;
        # the proxy's _handle_terminate relabel callback (or Phase 2's
        # synthesized ack) will rewrite it to the client's namespace via
        # the response policy's referential field.
        term_wire_id = f"kg_{secrets.token_hex(6)}"
        ack: WireDict = {
            "id": term_wire_id,
            "action": "terminate",
            "terminateId": canonical_id,
        }
        await on_response(term_wire_id, ack)
        await on_complete(term_wire_id)

    async def stop(self) -> None:
        logger.info("stopping")
        for canonical_id in list(self._live.keys()):
            live = self._live.pop(canonical_id)
            if live.emit_task is not None:
                live.emit_task.cancel()
                try:
                    await live.emit_task
                except asyncio.CancelledError:
                    pass
