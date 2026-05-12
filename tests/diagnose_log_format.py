#!/usr/bin/env python3
"""
tests/diagnose_log_format.py — Format / level / role matrix smoke test.

Exercises each supported PROXY_LOG_FORMAT (console, logfmt, json) by
emitting a representative scenario through each formatter and printing
the rendered output. The point isn't to pass/fail — it's to give an
operator a side-by-side visual confirmation that:

  - each formatter renders the same record in its expected shape,
  - role-tinted bind chains produce the right context block per role,
  - kind-aware level splitting on `forward` produces DEBUG for
    `partial` and INFO for `final` / `metadata` / `error`,
  - the trace-cid filter and regex line filter both drop the right
    records.

Run from the proxy directory:

    python tests/diagnose_log_format.py

The output goes to stdout (each formatter's section is clearly
delimited). Operators expecting a particular formatter to render a
particular field shape can grep this script's output rather than
chasing the wire-shape across a real proxy session.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

from proxy_logging import (  # noqa: E402
    Direction,
    Event,
    Role,
    get_proxy_logger,
    lifecycle,
    set_process_role,
)
from proxy_logging.formatters import (  # noqa: E402
    ConsoleFormatter,
    JsonFormatter,
    LogfmtFormatter,
    RegexLineFilter,
    TraceCidFilter,
)


def _install_handler(
    formatter: logging.Formatter,
    *,
    level: int = logging.DEBUG,
    extra_filter: logging.Filter | None = None,
) -> tuple[logging.Logger, logging.Handler]:
    """Install a single fresh handler with the given formatter on the
    kataproxy logger and return both for teardown."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    if extra_filter is not None:
        handler.addFilter(extra_filter)
    root = logging.getLogger("kataproxy")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    return root, handler


def _section(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def _scenario_leaf(role_log: Any) -> None:
    """Per-session LEAF scenario: connect → subscribe → dispatch → respond
    × 3 → complete → disconnect."""
    session = "('192.168.122.1', 47748)"
    cid = "hub_acd4d5d85296de7577d5"
    orig = "wd-1778447010318"

    s_log = role_log.bind(session=session)

    lifecycle.connect(s_log, peer_ip="192.168.122.1")
    lifecycle.subscribe(
        s_log, cid=cid, orig=orig, action="QUERY_VERSION",
    )
    lifecycle.dispatch(
        role_log, cid=cid, orig=orig, action="QUERY_VERSION",
    )
    # Two partials (DEBUG), then one final (INFO via kind-split).
    lifecycle.respond(role_log, cid=cid, orig=orig, kind="partial")
    lifecycle.respond(role_log, cid=cid, orig=orig, kind="partial")
    lifecycle.respond(role_log, cid=cid, orig=orig, kind="final")
    lifecycle.forward(role_log, cid=cid, orig=orig, kind="partial")
    lifecycle.forward(role_log, cid=cid, orig=orig, kind="final")
    lifecycle.complete(s_log, cid=cid, orig=orig, duration_ms=1234)
    lifecycle.disconnect(s_log, code=1000, reason="bye")


def _scenario_relay(role_log: Any) -> None:
    """RELAY scenario: per-upstream session bound; broadcast for
    metadata, dispatch for analyze, dispatch_error on a closed
    upstream."""
    cid = "hub_6ce2b53d1f9216ec8a9a"
    orig = "range-b3804abc-c51e-402c-bfd1-122df0243557-1778447011289"

    upstream_a = role_log.bind(upstream="ws://gpu1:41949")
    upstream_b = role_log.bind(upstream="ws://gpu2:41949")

    lifecycle.upstream_connect(upstream_a)
    lifecycle.upstream_connect(upstream_b)
    lifecycle.dispatch(
        role_log, cid=cid, orig=orig, action="ANALYZE",
        upstream="ws://gpu1:41949",
    )
    lifecycle.broadcast(
        role_log, cid=cid, orig=orig, action="QUERY_VERSION",
        targets=["ws://gpu1:41949", "ws://gpu2:41949"],
    )
    role_log.error(
        Event.DISPATCH_ERROR,
        cid=cid, orig=orig,
        upstream="ws://gpu2:41949",
        error_kind="send_failed: ConnectionError(closed)",
        msg="broadcast send failed for ws://gpu2:41949",
    )


def _scenario_selector(role_log: Any) -> None:
    """SELECTOR scenario: per-label upstream bind; dispatch picks the
    label; unhealthy label surfaces upstream_unhealthy."""
    cid = "hub_3f0594e2c68e361a89a8"
    orig = "deepsearch-001"

    strong = role_log.bind(label="strong", upstream="ws://gpu1:41949")
    weak = role_log.bind(label="weak", upstream="ws://gpu2:41949")

    lifecycle.upstream_connect(strong)
    lifecycle.upstream_connect(weak)
    lifecycle.dispatch(
        role_log, cid=cid, orig=orig, action="ANALYZE",
        label="strong",
    )
    lifecycle.respond(strong, cid=cid, orig=orig, kind="final")
    role_log.warning(
        Event.UPSTREAM_UNHEALTHY,
        label="weak", budget_remaining=0,
        msg="weak: connect-failure budget exhausted; marking unhealthy",
    )


def _scenario_echo(role_log: Any) -> None:
    """ECHO scenario: synthetic responses; minimal lifecycle."""
    cid = "echo-001"
    orig = "client-orig-1"
    lifecycle.dispatch(
        role_log, cid=cid, orig=orig, action="ANALYZE",
    )


def _scenario_diagnostic(role_log: Any) -> None:
    """The catch-all: a record that doesn't fit a typed lifecycle
    event but still goes through the structured envelope."""
    role_log.info(
        Event.DIAGNOSTIC,
        cid="some-cid",
        msg=(
            "adaptive: orig_id='range-001' deepening turns=[3, 4, 5] "
            "quantile=0.25 extra_visits=800"
        ),
    )


def _run_role(role: Role, formatter: logging.Formatter) -> None:
    set_process_role(role)
    role_log = get_proxy_logger("kataproxy.diagnose").bind(role=role)
    if role == Role.LEAF:
        _scenario_leaf(role_log)
    elif role == Role.RELAY:
        _scenario_relay(role_log)
    elif role == Role.SELECTOR:
        _scenario_selector(role_log)
    elif role == Role.ECHO:
        _scenario_echo(role_log)
    _scenario_diagnostic(role_log)


def _run_filter_demo(role_log: Any) -> None:
    """Demonstrate trace-cid + regex filters on a small mixed
    sequence."""
    cids = ["hub_aaaa", "hub_bbbb", "hub_aaaa"]
    for i, cid in enumerate(cids):
        lifecycle.subscribe(
            role_log, cid=cid, orig=f"orig-{i}", action="ANALYZE",
        )
    # A connect record with no cid — should pass the trace filter
    # (no-cid records are framing context).
    lifecycle.connect(role_log, peer_ip="192.0.2.1")


def main() -> None:
    formatters: list[tuple[str, logging.Formatter]] = [
        ("console (TTY)", ConsoleFormatter(abbrev=True)),
        ("logfmt", LogfmtFormatter()),
        ("json", JsonFormatter()),
    ]
    roles: list[Role] = [Role.LEAF, Role.RELAY, Role.SELECTOR, Role.ECHO]

    for fmt_name, formatter in formatters:
        for role in roles:
            _section(f"{fmt_name}  ·  ROLE={role.value}  ·  level=DEBUG")
            _install_handler(formatter, level=logging.DEBUG)
            _run_role(role, formatter)

        # INFO-level sample with LEAF — shows what gets filtered out
        # at the default level (DEBUG-level partials disappear, INFO+
        # records survive).
        _section(f"{fmt_name}  ·  ROLE=LEAF  ·  level=INFO (filter view)")
        _install_handler(formatter, level=logging.INFO)
        set_process_role(Role.LEAF)
        role_log = get_proxy_logger("kataproxy.diagnose").bind(role=Role.LEAF)
        _scenario_leaf(role_log)

    # Filter demonstration: PROXY_LOG_TRACE_CID + PROXY_LOG_FILTER.
    _section("trace-cid filter  ·  PROXY_LOG_TRACE_CID=hub_aaaa")
    set_process_role(Role.LEAF)
    role_log = get_proxy_logger("kataproxy.diagnose").bind(
        role=Role.LEAF, session="peer:test",
    )
    _install_handler(
        ConsoleFormatter(abbrev=True),
        level=logging.DEBUG,
        extra_filter=TraceCidFilter("hub_aaaa"),
    )
    _run_filter_demo(role_log)

    _section("regex filter  ·  PROXY_LOG_FILTER='ANALYZE'")
    _install_handler(
        ConsoleFormatter(abbrev=True),
        level=logging.DEBUG,
        extra_filter=RegexLineFilter(r"ANALYZE"),
    )
    _run_filter_demo(role_log)

    print()
    print("=" * 78)
    print("Done. Review above for:")
    print("  - each formatter renders ts/level/role/event/cid/orig stably")
    print("  - kind=partial → DEBUG, kind=final/metadata/error → INFO")
    print("  - role-tinted role token in the console renderer")
    print("  - trace-cid keeps no-cid records and the matching cid")
    print("  - regex filter matches on the rendered msg")


if __name__ == "__main__":
    main()
