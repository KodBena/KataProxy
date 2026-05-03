"""
sproxy_config.py — Centralised runtime configuration for KataProxy.

Implements 12-Factor App principle III: all configuration is sourced
exclusively from environment variables. No other module should call
os.environ directly.

Configuration precedence (highest to lowest):
  1. Variables set in the shell environment (e.g. via export)
  2. Variables defined in a .env file at the project root
  3. Hard defaults declared in this file

The .env file is loaded by _load_dotenv() at module import. If no .env
file exists, only shell environment + hard defaults apply.

Logging is intentionally NOT configured here. See logging_config.py.
"""

import os
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# .env file loader (stdlib only — no python-dotenv dependency)
#
# Loaded BEFORE any os.environ reads in this module so file-defined values
# are visible to subsequent get() calls. Shell-set variables win: we never
# overwrite an existing environment variable. This honours the Twelve-Factor
# precedence: shell > file > hard default.
# ---------------------------------------------------------------------------

def _load_dotenv(path: Path = Path(".env")) -> None:
    """Load KEY=value pairs from a .env file into os.environ.

    Existing environment variables are preserved (shell wins). Missing file
    is not an error — the .env file is optional.

    Format: one KEY=value per line. Blank lines and lines starting with #
    are ignored. Values may be wrapped in single or double quotes; quotes
    are stripped on read. No interpolation, no multi-line values.
    """
    if not path.exists():
        logging.getLogger("kataproxy.config").debug(
            f"no .env file at {path} — using shell environment only"
        )
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes if present
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        # Shell wins: do not overwrite existing env vars
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------
# Network / Role
# ---------------------------------------------------------------------------

HOST: str = os.environ.get("PROXY_HOST", "127.0.0.1")
PORT: int = int(os.environ.get("PROXY_PORT", "41949"))

# LEAF   — spawn a local KataGo subprocess via stdin/stdout
# RELAY  — forward queries to upstream SovereignProxy nodes via WebSocket
# ECHO   — return synthetic responses immediately (testing / fuzzing)
# REDIRECT / DELEGATE — redirect clients to an upstream (no analysis performed)
ROLE: str = os.environ.get("PROXY_ROLE", "LEAF").upper()

UPSTREAM_URLS: List[str] = [
    u.strip()
    for u in os.environ.get("UPSTREAM_URLS", "").split(",")
    if u.strip()
]

# ---------------------------------------------------------------------------
# KataGo subprocess (LEAF role only)
# ---------------------------------------------------------------------------

# KATAGO_PATH defaults to the bare executable name so that users who have
# katago on their $PATH / %PATH% need not set this variable at all.
KATAGO_PATH: str = os.environ.get("KATAGO_PATH", "katago")

# KATAGO_MODEL is intentionally optional at config-load time.
# RELAY / ECHO / REDIRECT roles do not require it.
# The LEAF router will produce a clear subprocess error if it is absent.
KATAGO_MODEL: Optional[str] = os.environ.get("KATAGO_MODEL") or None

KATAGO_CFG: Path = Path(os.environ.get("KATAGO_CFG", "analysis.cfg"))

KATAGO_CMD: List[str] = [
    KATAGO_PATH,
    "analysis",
    "-config", str(KATAGO_CFG),
    *(["-model", KATAGO_MODEL] if KATAGO_MODEL else []),
    "-quit-without-waiting",
]

# Seconds to wait for KataGo to acknowledge the LEAF startup probe
# before raising LeafStartupError. Cold model loads on slow disks /
# CPUs / GPUs can legitimately take a while; the default is generous.
# A startup that exceeds this is far more likely to be a config or
# hardware problem than a slow but-eventually-successful boot.
KATAGO_STARTUP_TIMEOUT_S: float = float(
    os.environ.get("KATAGO_STARTUP_TIMEOUT_S", "60")
)

# ---------------------------------------------------------------------------
# Relay tuning
# ---------------------------------------------------------------------------

RELAY_MAX_LOAD: int = int(os.environ.get("RELAY_MAX_LOAD", "10"))
HASH_RING_REPLICAS: int = int(os.environ.get("HASH_RING_REPLICAS", "150"))

# ---------------------------------------------------------------------------
# Hub replay-cache bound (v1.0.4)
# ---------------------------------------------------------------------------
#
# Maximum number of analysis-level replay-cache entries the hub keeps. When
# the cache is full, the least-recently-used entry is evicted.
#
# Set to 0 (or any non-positive integer) to disable the bound entirely — the
# cache then behaves as a plain dict and grows without limit. Operators with
# very large active query corpora may want this; the default is conservative
# because any client can populate the cache via {"cache": true} queries and
# unbounded growth is then a one-connection memory-amplification surface
# (audit H-2).

HUB_CACHE_MAX: int = int(os.environ.get("PROXY_HUB_CACHE_MAX", "1024"))


# ---------------------------------------------------------------------------
# JSON structural depth bound (v1.0.4)
# ---------------------------------------------------------------------------
#
# Maximum nesting depth accepted on inbound JSON (client wire, KataGo
# stdout, RELAY upstream). Payloads exceeding this depth are refused
# before json.loads runs, so a depth-bombed message can't trip Python's
# interpreter recursion limit and tear down the receive loop (audit M-3).
#
# Default 64; legitimate KataGo JSON is depth ≤ 5 in practice. Set to 0
# (or any non-positive integer) to disable the check.

JSON_MAX_DEPTH: int = int(os.environ.get("PROXY_JSON_MAX_DEPTH", "64"))


# ---------------------------------------------------------------------------
# Connection-level resource caps (v1.0.4)
# ---------------------------------------------------------------------------
#
# WebSocket-level bounds that apply per connection or across the whole
# listening server. All three close audit M-1 / per-connection-DoS surfaces
# that were unbounded pre-v1.0.4.

# Maximum WebSocket message size accepted from a client. This applies to
# inbound client queries only; KataGo response sizes (which run ~48 KB per
# turn with the policy head and ownership map enabled) flow outbound and
# are unaffected by this cap. Realistic inbound queries — even with rich
# analysis_config payloads — are well under 100 KB. The pre-v1.0.4 default
# of 64 MB was the most exploitable single value in the codebase; 4 MB
# gives wide headroom for any realistic inbound shape.
MAX_MESSAGE_SIZE: int = int(os.environ.get("PROXY_MAX_MESSAGE_SIZE",
                                            str(4 * 1024 * 1024)))

# Maximum number of concurrent client WebSocket sessions. New connections
# above the cap are closed with WS code 1013 ("try again later"). Set to
# 0 (or any non-positive integer) to disable the cap entirely. 256 covers
# small-to-medium go-school deployments at 1–3 connections per active user.
MAX_SESSIONS: int = int(os.environ.get("PROXY_MAX_SESSIONS", "256"))

# Per-IP query rate limit, in queries per minute. When set, exceeding the
# rate causes the offending message to be dropped (with a WARNING log);
# the connection itself stays open. Default 0 (off) — appropriate for
# deployments behind a reverse proxy where the proxy sees the reverse-
# proxy IP for every user. Operators with direct internet exposure may
# want to set this to e.g. 600 (10 q/sec sustained).
RATELIMIT_PER_IP: int = int(os.environ.get("PROXY_RATELIMIT_PER_IP", "0"))


# ---------------------------------------------------------------------------
# Adaptive middleware (v1.0.5)
# ---------------------------------------------------------------------------
#
# Per-session bound on the number of in-flight queries
# AdaptiveReevaluateMiddleware will track. On overflow, the oldest in-flight
# entry is dropped (its buffered final responses are lost; subsequent
# responses for that orig_id flow through unchanged). This replaces the
# pre-v1.0.5 panic-flush at 5000 entries that wiped every in-flight entry
# for the session at once (audit H-2).
#
# 128 is generous — a typical session has well under 10 in-flight queries
# at any moment. Operators with very long-lived sessions or unusual
# workloads can raise it; a non-positive value disables the bound entirely.

ADAPTIVE_MAX_INFLIGHT: int = int(
    os.environ.get("PROXY_ADAPTIVE_MAX_INFLIGHT", "128")
)


# ---------------------------------------------------------------------------
# Keep-alive middleware (v1.0.10)
# ---------------------------------------------------------------------------
#
# Per-session inactivity watchdog timeout, in seconds. The KeepAliveMiddleware
# tracks the most recent timestamp at which a configurable "heartbeat" query
# (default: action == query_version) was observed on the session. If the gap
# between now and the last heartbeat exceeds this value, every in-flight
# ANALYZE query on the session is terminated.
#
# This catches the WS-stays-open-but-client-silent case that v1.0.7's
# orphan-cleanup cannot: when the WebSocket never closes (HMR-orphaned
# singleton on a frontend, network freeze without TCP RST, frontend bug
# stops sending heartbeats without disconnecting), the proxy never observes
# a disconnect and `_cleanup` never runs. The watchdog is the safety net.
#
# Default 25 seconds: 5x the frontend's existing 5000ms `query_version`
# watchdog cadence. Generous enough to absorb network jitter and one missed
# beat without false positives, tight enough to bound cost on a stranded
# ponder.
#
# Set to 0 (or any non-positive value) to disable keep-alive entirely. The
# middleware factory will then omit KeepAliveMiddleware from the chain.

KEEP_ALIVE_IDLE_TIMEOUT_SECONDS: float = float(
    os.environ.get("KEEP_ALIVE_IDLE_TIMEOUT_SECONDS", "25.0")
)
