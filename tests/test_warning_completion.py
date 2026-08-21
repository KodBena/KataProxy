"""
tests/test_warning_completion.py — engine warning envelopes are
non-terminal.

Contract under test: a metadata response carrying `warning` (and no
`error`) — the engine's `{"id", "field", "warning"}` envelope, e.g.
warnUnusedFields on an analyze with a stray field — must NOT complete
the query's outstanding turns. The engine emits warnings *before* the
responses the query is still owed, on the same id; treating them as
the single-shot metadata completion retired the query at the router,
dropped the real responses at the "no callback" branch, and hung the
client (witnessed live against the model-and-cache engine build,
2026-08-21: warning relayed through a SELECTOR, analyze result never
delivered; the direct-to-engine control received warning + result).

Error envelopes stay terminal — the engine refuses INSTEAD of
answering — and ordinary metadata (acks, query_version) stays
single-shot.

Run from the proxy directory: `pytest tests/test_warning_completion.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from AbstractProxy.proxy_core import (  # noqa: E402
    CompletionSignal,
    CompletionTracker,
    InternalId,
)
from katago import (  # noqa: E402
    AnalyzeResponse,
    KataGoAction,
    KataGoQuery,
    MetadataResponse,
    register_query_completion,
    response_completion_signal,
)

WARNING_WIRE_OPAQUE = {
    "field": "frobnicate",
    "warning": (
        "Unexpected or unused field, do you have a typo? "
        "(set warnUnusedFields=false in the config to disable this warning)"
    ),
}


def test_warning_metadata_is_partial() -> None:
    disc, is_partial = response_completion_signal(
        MetadataResponse(opaque=dict(WARNING_WIRE_OPAQUE))
    )
    assert is_partial is True, "warning envelope must not complete anything"


def test_error_metadata_stays_terminal() -> None:
    _, is_partial = response_completion_signal(
        MetadataResponse(opaque={"error": "boom", "field": "model"})
    )
    assert is_partial is False


def test_error_beats_warning_when_both_present() -> None:
    # Defensive: an envelope carrying both is a refusal, not chatter.
    _, is_partial = response_completion_signal(
        MetadataResponse(opaque={"error": "boom", "warning": "w"})
    )
    assert is_partial is False


def test_plain_metadata_stays_single_shot() -> None:
    _, is_partial = response_completion_signal(
        MetadataResponse(opaque={"action": "query_version", "version": "1.17.2"})
    )
    assert is_partial is False


def test_warning_then_final_completes_exactly_once() -> None:
    """The witnessed sequence, at the tracker level: analyze registered
    for one turn; warning arrives first (must not retire the turn);
    the real final retires it."""
    tracker: CompletionTracker[InternalId, int] = CompletionTracker()
    qid = InternalId("kg_test1")
    query = KataGoQuery(
        action=KataGoAction.ANALYZE,
        analyze_turns=[0],
        opaque={"moves": []},
    )
    register_query_completion(tracker, qid, query)

    disc, is_partial = response_completion_signal(
        MetadataResponse(opaque=dict(WARNING_WIRE_OPAQUE))
    )
    assert tracker.signal(qid, disc, is_partial) == CompletionSignal.PARTIAL

    disc, is_partial = response_completion_signal(
        AnalyzeResponse(is_during_search=False, turn_number=0, opaque={})
    )
    assert tracker.signal(qid, disc, is_partial) == CompletionSignal.QUERY_COMPLETE
