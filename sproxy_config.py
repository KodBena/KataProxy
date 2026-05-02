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
