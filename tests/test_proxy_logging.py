"""
tests/test_proxy_logging.py — Phase 1 unit tests for the
proxy_logging/ package.

Coverage:

  - ProxyLogger: bind chain, validation (unknown event, missing
    required fields, reserved-name collision), level gating, lazy
    msg= callable, kwarg precedence over bound fields.
  - Event / Role / Direction enums: closed-set membership, str
    coercion.
  - LogContractError: raising shape and surface text.
  - summarize_query: ANALYZE summary, turn-range formatting,
    non-analyze actions.
  - format_query_filtered: opaque dict passes through filter_dict;
    analyze_turns and terminate_id surface when present.
  - log_safe: truncation, repr-based escape (audit-H-4 defence).
  - filter_dict: bulky-key stripping.
  - Formatters: console / logfmt / JSON output shapes against a
    captured LogRecord.
  - configure_logging_from_env: idempotent, raises on bad
    PROXY_LOG_FORMAT (ADR-0002 fail-loud).
  - lifecycle helpers: produce records with the expected event
    and a sensible msg.

The tests don't exercise the env-driven full handler configuration
(side-effects on the root logger leak between test cases). The
formatter tests construct LogRecords manually via Logger.makeRecord
or via a dedicated MemoryHandler.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from proxy_logging import (  # noqa: E402
    Direction,
    Event,
    LogContractError,
    ProxyLogger,
    Role,
    filter_dict,
    format_query_filtered,
    get_proxy_logger,
    lifecycle,
    log_safe,
    summarize_query,
)
from proxy_logging.formatters import (  # noqa: E402
    ConsoleFormatter,
    JsonFormatter,
    LogfmtFormatter,
    RegexLineFilter,
    TraceCidFilter,
    configure_logging_from_env,
)


# ===========================================================================
# Test fixtures
# ===========================================================================


class _MemoryHandler(logging.Handler):
    """Captures records for inspection. Tests assert on .records."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def memory_handler() -> _MemoryHandler:
    handler = _MemoryHandler()
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("kataproxy.test")
    # Ensure the test logger isn't affected by handler accumulation
    # across tests.
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return handler


# Minimal mock query — duck-typed. Avoids importing katago module
# (which would be a circular dep once the rest of the proxy starts
# using proxy_logging).
class _MockAction:
    def __init__(self, name: str) -> None:
        self.name = name


class _MockQuery:
    def __init__(
        self,
        action_name: str,
        opaque: dict | None = None,
        analyze_turns: list[int] | None = None,
        terminate_id: str | None = None,
    ) -> None:
        self.action = _MockAction(action_name)
        self.opaque = opaque or {}
        self.analyze_turns = analyze_turns
        self.terminate_id = terminate_id


# ===========================================================================
# Enums
# ===========================================================================


class TestEnums:
    def test_role_values(self) -> None:
        assert Role.LEAF.value == "LEAF"
        assert Role.SELECTOR.value == "SELECTOR"
        # str() returns the value, not the enum repr.
        assert str(Role.LEAF) == "LEAF"

    def test_direction_values(self) -> None:
        assert Direction.PROXY_TO_UPSTREAM.value == "proxy→upstream"
        assert str(Direction.RECV) == "recv"

    def test_event_values_stable(self) -> None:
        # Pin a few — these are the wire-shape strings aggregators
        # depend on. Renames here are breaking.
        assert Event.DISPATCH.value == "dispatch"
        assert Event.BROADCAST.value == "broadcast"
        assert Event.KEEPALIVE_FIRED.value == "keepalive_fired"
        assert Event.UPSTREAM_DISCONNECT.value == "upstream_disconnect"
        assert Event.ORCHESTRATION_SPAWN.value == "orchestration_spawn"

    def test_event_lookup_from_string(self) -> None:
        # Round-trip a string through the enum constructor.
        assert Event("dispatch") is Event.DISPATCH

    def test_event_unknown_raises_value_error(self) -> None:
        with pytest.raises(ValueError):
            Event("not_a_real_event")


# ===========================================================================
# ProxyLogger — bind chain
# ===========================================================================


class TestBindChain:
    def test_bind_returns_new_instance(self) -> None:
        base = get_proxy_logger("kataproxy.test")
        bound = base.bind(role=Role.LEAF)
        assert bound is not base
        # The original is unchanged.
        assert base._bound == {}
        assert bound._bound["role"] is Role.LEAF

    def test_bind_chains_compose(self) -> None:
        base = get_proxy_logger("kataproxy.test")
        a = base.bind(role=Role.SELECTOR)
        b = a.bind(session="peer:1234")
        c = b.bind(label="strong")
        assert c._bound == {
            "role": Role.SELECTOR,
            "session": "peer:1234",
            "label": "strong",
        }
        # The intermediate adapters are not contaminated.
        assert "label" not in a._bound

    def test_bind_reserved_name_raises(self) -> None:
        # Reserved names (LogRecord built-in attributes) must not be
        # bindable — they'd silently overwrite stdlib fields.
        base = get_proxy_logger("kataproxy.test")
        with pytest.raises(LogContractError, match="reserved attribute"):
            base.bind(name="oops")
        with pytest.raises(LogContractError, match="reserved attribute"):
            base.bind(message="oops")


# ===========================================================================
# ProxyLogger — validation
# ===========================================================================


class TestValidation:
    def test_unknown_event_raises(self, memory_handler: _MemoryHandler) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        with pytest.raises(LogContractError, match="unknown event"):
            plog.info("not_a_real_event", session="peer")

    def test_event_must_be_event_or_str(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        with pytest.raises(LogContractError, match="must be Event or str"):
            plog.info(42, session="peer")  # type: ignore[arg-type]

    def test_missing_required_field_raises(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        # DISPATCH requires cid, orig, action, direction. We supply none.
        with pytest.raises(LogContractError, match="requires fields"):
            plog.info(Event.DISPATCH)

    def test_required_via_bind_chain_satisfies(
        self, memory_handler: _MemoryHandler
    ) -> None:
        # All required fields can come from the bind chain — call site
        # need only pass the ones that vary.
        plog = (
            get_proxy_logger("kataproxy.test")
            .bind(role=Role.LEAF, cid="hub_x", orig="r1", action="ANALYZE")
        )
        plog.info(Event.DISPATCH, direction=Direction.PROXY_TO_UPSTREAM)
        assert len(memory_handler.records) == 1
        rec = memory_handler.records[0]
        assert rec.event == "dispatch"
        assert rec.cid == "hub_x"

    def test_call_site_kwargs_win_over_bound(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = (
            get_proxy_logger("kataproxy.test")
            .bind(role=Role.LEAF, action="ANALYZE")
        )
        plog.info(
            Event.DISPATCH,
            cid="hub_x", orig="r1", action="QUERY_VERSION",
            direction=Direction.PROXY_TO_UPSTREAM,
        )
        assert memory_handler.records[0].action == "QUERY_VERSION"

    def test_call_site_reserved_field_raises(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = (
            get_proxy_logger("kataproxy.test")
            .bind(role=Role.LEAF)
        )
        with pytest.raises(LogContractError, match="reserved attribute"):
            plog.info(
                Event.CONNECT,
                session="peer", peer_ip="192.0.2.1",
                name="oops",  # reserved
            )

    def test_reserved_field_raises_even_at_filtered_level(
        self, memory_handler: _MemoryHandler
    ) -> None:
        # Per the validator's contract, a reserved-name collision is a
        # call-site coding bug — it would corrupt the LogRecord at any
        # level. The check must run regardless of isEnabledFor so the
        # bug surfaces uniformly across DEBUG / INFO / WARNING /
        # ERROR. Otherwise a record at a filtered-out level (e.g.,
        # a DEBUG emission when the level is INFO) would silently
        # accept an invalid call site and only fire when the level
        # is later turned up — the orchestration_spawn = name= bug
        # pattern from Phase 3.
        logger = logging.getLogger("kataproxy.test")
        logger.setLevel(logging.WARNING)  # filter out INFO + DEBUG
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        # Both the bound-side and the call-site kwargs are checked.
        with pytest.raises(LogContractError, match="reserved attribute"):
            plog.info(  # filtered-out level
                Event.CONNECT,
                session="peer", peer_ip="192.0.2.1",
                name="oops",  # reserved; would corrupt record
            )
        with pytest.raises(LogContractError, match="reserved attribute"):
            plog.debug(  # also filtered out
                Event.CONNECT,
                session="peer", peer_ip="192.0.2.1",
                module="bad",  # reserved
            )


# ===========================================================================
# ProxyLogger — level gating + lazy formatting
# ===========================================================================


class TestLevelGating:
    def test_below_threshold_skips_emission(
        self, memory_handler: _MemoryHandler
    ) -> None:
        logger = logging.getLogger("kataproxy.test")
        logger.setLevel(logging.WARNING)
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        plog.info(  # below threshold
            Event.CONNECT, session="peer", peer_ip="192.0.2.1",
        )
        assert memory_handler.records == []

    def test_lazy_msg_callable_only_invoked_when_enabled(
        self, memory_handler: _MemoryHandler
    ) -> None:
        logger = logging.getLogger("kataproxy.test")
        logger.setLevel(logging.WARNING)  # filter out INFO
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        invoked = {"value": False}

        def expensive_msg() -> str:
            invoked["value"] = True
            return "expensive"

        # INFO — filtered out, callable must NOT fire.
        plog.info(
            Event.CONNECT,
            session="peer", peer_ip="192.0.2.1",
            msg=expensive_msg,
        )
        assert invoked["value"] is False

        # WARNING — passes filter, callable fires.
        logger.setLevel(logging.WARNING)
        plog.warning(
            Event.RATE_LIMITED,
            session="peer", peer_ip="192.0.2.1",
            msg=expensive_msg,
        )
        assert invoked["value"] is True

    def test_is_enabled_for(self, memory_handler: _MemoryHandler) -> None:
        logger = logging.getLogger("kataproxy.test")
        logger.setLevel(logging.WARNING)
        plog = get_proxy_logger("kataproxy.test")
        assert plog.is_enabled_for(logging.WARNING) is True
        assert plog.is_enabled_for(logging.INFO) is False


# ===========================================================================
# log_safe + filter_dict (carried over from logging_config.py)
# ===========================================================================


class TestSafetyHelpers:
    def test_log_safe_truncates(self) -> None:
        s = "x" * 1000
        out = log_safe(s, max_len=10)
        assert "xxxxxxxxxx" in out
        assert "..." in out

    def test_log_safe_escapes_newlines(self) -> None:
        # The audit-H-4 defence: a wire-derived string with embedded
        # newlines cannot forge log lines.
        out = log_safe("foo\n[FAKE LOG]")
        assert "\n" not in out
        assert "\\n" in out

    def test_log_safe_handles_bytes(self) -> None:
        out = log_safe(b"hello\x00world", max_len=20)
        assert "hello" in out
        assert "\\x00" in out

    def test_filter_dict_strips_bulky_keys(self) -> None:
        d = {
            "id": "x", "isDuringSearch": True,
            "moveInfos": [{"move": "Q4"}] * 100,
            "ownership": [0.5] * 361,
            "policy": [0.0] * 361,
            "rootInfo": {"visits": 200},
        }
        filtered = filter_dict(d)
        assert "moveInfos" not in filtered
        assert "ownership" not in filtered
        assert "policy" not in filtered
        assert filtered["id"] == "x"
        assert filtered["rootInfo"]["visits"] == 200


# ===========================================================================
# summarize_query / format_query_filtered
# ===========================================================================


class TestSummarizeQuery:
    def test_analyze_with_turn_range(self) -> None:
        q = _MockQuery(
            "ANALYZE",
            analyze_turns=list(range(0, 186)),
            opaque={"maxVisits": 200, "model": "really_weak"},
        )
        s = summarize_query(q)
        assert s.startswith("ANALYZE")
        assert "turns=[0..185]" in s
        assert "visits=200" in s
        assert "model=really_weak" in s

    def test_analyze_single_turn(self) -> None:
        q = _MockQuery("ANALYZE", analyze_turns=[12], opaque={"maxVisits": 1000})
        s = summarize_query(q)
        assert "turn=12" in s
        assert "visits=1000" in s

    def test_analyze_non_contiguous_turns(self) -> None:
        q = _MockQuery("ANALYZE", analyze_turns=[3, 7, 11], opaque={"maxVisits": 100})
        s = summarize_query(q)
        # Compact list rendering for non-contiguous + small.
        assert "[3,7,11]" in s

    def test_analyze_no_turns(self) -> None:
        q = _MockQuery("ANALYZE", opaque={"maxVisits": 100})
        s = summarize_query(q)
        assert s == "ANALYZE visits=100"

    def test_query_version(self) -> None:
        q = _MockQuery("QUERY_VERSION")
        assert summarize_query(q) == "QUERY_VERSION"

    def test_terminate(self) -> None:
        q = _MockQuery("TERMINATE", terminate_id="range-abc-123")
        s = summarize_query(q)
        assert s.startswith("TERMINATE → ")
        assert "range-abc-123" in s


class TestFormatQueryFiltered:
    def test_strips_bulky_response_keys_from_opaque(self) -> None:
        # If the opaque has stray response keys (uncommon but
        # possible during round-trip diagnostics), filter_dict
        # strips them.
        q = _MockQuery(
            "ANALYZE",
            opaque={
                "moves": [["B", "Q4"]],
                "moveInfos": [{"move": "x"}],  # gets stripped
            },
        )
        out = format_query_filtered(q)
        assert "moveInfos" not in out["opaque"]
        assert out["opaque"]["moves"] == [["B", "Q4"]]

    def test_includes_analyze_turns_and_terminate_id(self) -> None:
        q = _MockQuery(
            "ANALYZE",
            analyze_turns=[0, 1, 2],
            opaque={"moves": []},
        )
        out = format_query_filtered(q)
        assert out["analyze_turns"] == [0, 1, 2]
        assert "terminate_id" not in out  # absent on analyze


# ===========================================================================
# Formatters
# ===========================================================================


def _make_record(
    *,
    name: str = "kataproxy.test",
    level: int = logging.INFO,
    msg: str = "hello",
    extra: dict | None = None,
) -> logging.LogRecord:
    """Construct a LogRecord with structured-fields extras attached."""
    record = logging.LogRecord(
        name=name, level=level, pathname=__file__, lineno=0,
        msg=msg, args=(), exc_info=None, func="test_func",
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


class TestConsoleFormatter:
    def test_renders_role_and_event(self) -> None:
        formatter = ConsoleFormatter(abbrev=True)
        rec = _make_record(
            extra={
                "event": "dispatch",
                "role": "SELECTOR",
                "label": "strong",
                "session": "192.0.2.1:54321",
                "cid": "hub_a6940f0fc3458649380b",
                "orig": "range-b3804abc-c51e-402c-bfd1",
                "action": "ANALYZE",
            },
        )
        # Disable color for the test — tty detection is cached at
        # module level otherwise.
        formatter._tty_detected = False  # type: ignore[attr-defined]
        out = formatter.format(rec)
        assert "SELECTOR" in out
        assert "strong" in out
        assert "peer=192.0.2.1:54321" in out
        assert "dispatch" in out
        # Abbreviated cid + orig.
        assert "cid=hub_a6…" in out or "cid=hub_a6" in out

    def test_no_abbrev_full_ids(self) -> None:
        formatter = ConsoleFormatter(abbrev=False)
        rec = _make_record(
            extra={
                "event": "dispatch",
                "role": "LEAF",
                "cid": "hub_full_canonical_id",
                "orig": "range-full-orig-id",
            },
        )
        out = formatter.format(rec)
        assert "hub_full_canonical_id" in out
        assert "range-full-orig-id" in out

    def test_unmigrated_record_falls_back_to_module(self) -> None:
        # A record without the `event` structured field — the case
        # for un-migrated stdlib `logger.info(f"...")` calls during
        # the gradual sweep. Should render in a minimal "ts level
        # [module] msg" shape rather than a confused [?] block.
        formatter = ConsoleFormatter(abbrev=True)
        rec = _make_record(
            name="kataproxy.router",
            level=logging.INFO,
            msg="listening on ws://localhost:1235",
            extra=None,
        )
        out = formatter.format(rec)
        assert "?" not in out, (
            f"unmigrated record should not render as event=?, got: {out!r}"
        )
        assert "listening on ws://localhost:1235" in out
        # The module fallback uses record.module which is whatever
        # makeRecord populated; for our test fixture's pathname it's
        # the file's basename without extension.

    def test_level_is_abbreviated_to_five_chars(self) -> None:
        formatter = ConsoleFormatter(abbrev=True)
        # WARNING is 7 chars; should render as "WARN " (5).
        rec = _make_record(
            level=logging.WARNING,
            extra={"event": "rate_limited", "role": "LEAF", "session": "p"},
        )
        out = formatter.format(rec)
        assert "WARN " in out
        assert "WARNING" not in out  # not the full stdlib name


class TestLogfmtFormatter:
    def test_emits_key_value_pairs(self) -> None:
        formatter = LogfmtFormatter()
        rec = _make_record(
            extra={
                "event": "dispatch",
                "role": "SELECTOR",
                "label": "strong",
                "cid": "hub_x",
            },
        )
        out = formatter.format(rec)
        assert "event=dispatch" in out
        assert "role=SELECTOR" in out
        assert "label=strong" in out
        assert "cid=hub_x" in out

    def test_quotes_values_with_spaces(self) -> None:
        formatter = LogfmtFormatter()
        rec = _make_record(
            msg="hello world with spaces",
            extra={"event": "subscribe", "role": "LEAF"},
        )
        out = formatter.format(rec)
        assert 'msg="hello world with spaces"' in out

    def test_escapes_internal_quotes(self) -> None:
        formatter = LogfmtFormatter()
        rec = _make_record(
            msg='he said "hi"',
            extra={"event": "subscribe", "role": "LEAF"},
        )
        out = formatter.format(rec)
        assert r'\"' in out


class TestJsonFormatter:
    def test_one_json_per_line(self) -> None:
        formatter = JsonFormatter()
        rec = _make_record(
            extra={
                "event": "dispatch",
                "role": "SELECTOR",
                "cid": "hub_x",
            },
        )
        out = formatter.format(rec)
        parsed = json.loads(out)
        assert parsed["event"] == "dispatch"
        assert parsed["role"] == "SELECTOR"
        assert parsed["cid"] == "hub_x"

    def test_iso_timestamp_includes_tz(self) -> None:
        formatter = JsonFormatter()
        rec = _make_record(extra={"event": "subscribe", "role": "LEAF"})
        parsed = json.loads(formatter.format(rec))
        # Q6: ISO 8601 with TZ. "T" between date and time + tz offset
        # at the end (or "Z" for UTC).
        assert "T" in parsed["ts"]
        assert "+" in parsed["ts"] or parsed["ts"].endswith("Z") or "-" in parsed["ts"][10:]


# ===========================================================================
# Filters
# ===========================================================================


class TestTraceCidFilter:
    def test_passes_target_cid(self) -> None:
        f = TraceCidFilter("hub_x")
        rec = _make_record(extra={"cid": "hub_x"})
        assert f.filter(rec) is True

    def test_drops_other_cid(self) -> None:
        f = TraceCidFilter("hub_x")
        rec = _make_record(extra={"cid": "hub_y"})
        assert f.filter(rec) is False

    def test_passes_no_cid_records(self) -> None:
        # connect / disconnect / parse_error / etc. — session
        # context but no cid yet — pass through.
        f = TraceCidFilter("hub_x")
        rec = _make_record(extra={"session": "peer"})
        assert f.filter(rec) is True


class TestRegexLineFilter:
    def test_matches(self) -> None:
        f = RegexLineFilter(r"FAILED")
        rec = _make_record(msg="connection FAILED upstream")
        assert f.filter(rec) is True

    def test_drops_no_match(self) -> None:
        f = RegexLineFilter(r"FAILED")
        rec = _make_record(msg="connection ok")
        assert f.filter(rec) is False


# ===========================================================================
# configure_logging_from_env
# ===========================================================================


class TestEnvConfig:
    def test_unknown_format_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Reset the module-level idempotency flag so configure runs.
        import proxy_logging.formatters as fmtmod
        monkeypatch.setattr(fmtmod, "_CONFIGURED", False)
        monkeypatch.setenv("PROXY_LOG_FORMAT", "yaml")
        with pytest.raises(ValueError, match="not one of"):
            configure_logging_from_env()


# ===========================================================================
# Lifecycle helpers
# ===========================================================================


class TestLifecycleHelpers:
    def test_dispatch_helper_emits_correct_event(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = (
            get_proxy_logger("kataproxy.test")
            .bind(role=Role.SELECTOR, session="peer:1234")
        )
        lifecycle.dispatch(
            plog, cid="hub_x", orig="r1", action="ANALYZE",
            label="strong",
        )
        assert len(memory_handler.records) == 1
        rec = memory_handler.records[0]
        assert rec.event == "dispatch"
        assert rec.cid == "hub_x"
        assert rec.action == "ANALYZE"
        assert rec.label == "strong"
        assert rec.direction == "proxy→upstream"

    def test_broadcast_helper_includes_targets(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.SELECTOR)
        lifecycle.broadcast(
            plog, cid="hub_x", orig="r1", action="QUERY_VERSION",
            targets=["strong", "weak"],
        )
        rec = memory_handler.records[0]
        assert rec.event == "broadcast"
        assert rec.target_count == 2
        assert rec.targets == ["strong", "weak"]

    def test_complete_helper_with_duration(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.complete(plog, cid="hub_x", orig="r1", duration_ms=1234)
        rec = memory_handler.records[0]
        assert rec.event == "complete"
        assert rec.duration_ms == 1234

    def test_keepalive_fired_warning(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.keepalive_fired(
            plog, session="peer", idle_seconds=25.0,
            terminated_cids=["hub_x"],
        )
        rec = memory_handler.records[0]
        assert rec.event == "keepalive_fired"
        assert rec.levelname == "WARNING"
        assert rec.terminated_cids == ["hub_x"]

    def test_upstream_connect_with_label(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.SELECTOR)
        lifecycle.upstream_connect(plog, label="strong")
        rec = memory_handler.records[0]
        assert rec.event == "upstream_connect"
        assert rec.label == "strong"

    def test_no_upstream_is_error_level(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.SELECTOR)
        lifecycle.no_upstream(plog, cid="hub_x", orig="r1", action="ANALYZE")
        rec = memory_handler.records[0]
        assert rec.event == "no_upstream"
        assert rec.levelname == "ERROR"


class TestLifecycleForwardKindLevel:
    """Validate the kind-aware level split in lifecycle.forward.

    The contract: `partial` → DEBUG (volume scales with
    reportDuringSearchEvery × turns and would flood INFO);
    `final`/`metadata`/`error` → INFO (one per turn or per non-analyze
    query; visible at the default level so the demand-edge timestamp
    is operator-readable). The split lives inside the helper so call
    sites pass `kind=…` and don't pick the level themselves.

    Pinning the helper's contract here means refactors that "simplify"
    the helper by collapsing the split (or accidentally swap DEBUG/INFO)
    fail loudly rather than silently degrading the operator's visibility.
    """

    def test_partial_kind_emits_at_debug(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.forward(plog, cid="hub_x", orig="r1", kind="partial")
        rec = memory_handler.records[0]
        assert rec.event == "forward"
        assert rec.levelname == "DEBUG"
        assert rec.kind == "partial"
        assert rec.direction == Direction.FORWARD

    def test_final_kind_emits_at_info(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.forward(plog, cid="hub_x", orig="r1", kind="final")
        rec = memory_handler.records[0]
        assert rec.event == "forward"
        assert rec.levelname == "INFO"
        assert rec.kind == "final"

    def test_metadata_kind_emits_at_info(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.forward(plog, cid="hub_x", orig="r1", kind="metadata")
        rec = memory_handler.records[0]
        assert rec.event == "forward"
        assert rec.levelname == "INFO"
        assert rec.kind == "metadata"

    def test_error_kind_emits_at_info(
        self, memory_handler: _MemoryHandler
    ) -> None:
        plog = get_proxy_logger("kataproxy.test").bind(role=Role.LEAF)
        lifecycle.forward(plog, cid="hub_x", orig="r1", kind="error")
        rec = memory_handler.records[0]
        assert rec.event == "forward"
        assert rec.levelname == "INFO"
        assert rec.kind == "error"


# ===========================================================================
# logging_config.py shim — backward compat
# ===========================================================================


class TestLoggingConfigShim:
    def test_filter_dict_re_exported(self) -> None:
        # The shim must re-export the existing helpers so unmigrated
        # call sites continue to work unchanged.
        import logging_config
        assert logging_config.filter_dict is filter_dict
        assert logging_config.log_safe is log_safe

    def test_get_logger_returns_stdlib_logger(self) -> None:
        import logging_config
        logger = logging_config.get_logger("kataproxy.shim_test")
        assert isinstance(logger, logging.Logger)
        # Behaves like the pre-arc API: same name → same instance.
        assert logger is logging.getLogger("kataproxy.shim_test")
