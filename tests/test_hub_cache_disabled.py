"""
tests/test_hub_cache_disabled.py — PROXY_HUB_CACHE_DISABLED (opt-out of the
Hub replay cache entirely).

Covers:

  - sproxy_config boolean parsing of PROXY_HUB_CACHE_DISABLED, matching the
    existing PROXY_ADVERTISE_CAPABILITIES truthy-string style
    (strip().lower() in ("1", "true", "yes", "on")).
  - ProxyServer wiring: when disabled, PubSubHub is constructed with
    cache_store=None instead of an LRUCacheStore; when enabled, the
    existing LRUCacheStore(maxsize=cfg.HUB_CACHE_MAX) wiring is unchanged.
  - Startup log line: when the cache is enabled and HUB_CACHE_MAX <= 0,
    one INFO line announces the replay cache is unbounded. No such line
    when the cache is disabled, and none when it's enabled-but-bounded.
  - query_version capability advertisement: a "cache" entry appears iff
    the cache is enabled, shaped {"bounded": bool, "max_entries": int,
    "key_scope": "engine-facing"} (max_entries present only when
    bounded; key_scope always present, feature-detecting the
    engine-facing replay-cache-key semantics).

Run from the proxy directory: `pytest tests/test_hub_cache_disabled.py`.

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

import pytest

_PROXY_ROOT = Path(__file__).resolve().parent.parent
if str(_PROXY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROXY_ROOT))

import sproxy_config as cfg  # noqa: E402
from pubsub_hub import LRUCacheStore  # noqa: E402


# ===========================================================================
# sproxy_config parsing
# ===========================================================================


class TestHubCacheDisabledParsing:
    def test_default_is_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PROXY_HUB_CACHE_DISABLED", raising=False)
        reloaded = importlib.reload(cfg)
        try:
            assert reloaded.HUB_CACHE_DISABLED is False
        finally:
            importlib.reload(cfg)

    @pytest.mark.parametrize(
        "raw", ["1", "true", "True", "TRUE", "yes", "Yes", "on", "ON"]
    )
    def test_truthy_variants_parse_true(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv("PROXY_HUB_CACHE_DISABLED", raw)
        reloaded = importlib.reload(cfg)
        try:
            assert reloaded.HUB_CACHE_DISABLED is True
        finally:
            monkeypatch.delenv("PROXY_HUB_CACHE_DISABLED", raising=False)
            importlib.reload(cfg)

    @pytest.mark.parametrize("raw", ["0", "false", "False", "no", "off", ""])
    def test_falsy_variants_parse_false(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv("PROXY_HUB_CACHE_DISABLED", raw)
        reloaded = importlib.reload(cfg)
        try:
            assert reloaded.HUB_CACHE_DISABLED is False
        finally:
            monkeypatch.delenv("PROXY_HUB_CACHE_DISABLED", raising=False)
            importlib.reload(cfg)


# ===========================================================================
# ProxyServer wiring
# ===========================================================================


class TestProxyServerHubCacheWiring:
    def test_cache_store_is_none_when_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", True)
        from proxy_server import ProxyServer

        server = ProxyServer()
        assert server._hub_cache is None
        assert server._hub._cache_store is None

    def test_lru_cache_store_when_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", False)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 10)
        from proxy_server import ProxyServer

        server = ProxyServer()
        assert isinstance(server._hub_cache, LRUCacheStore)
        assert server._hub._cache_store is server._hub_cache


# ===========================================================================
# Startup log line: unbounded-cache announcement
# ===========================================================================


class TestUnboundedCacheStartupLog:
    def test_logs_unbounded_when_enabled_and_max_nonpositive(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", False)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 0)
        from proxy_server import ProxyServer

        with caplog.at_level(logging.INFO, logger="kataproxy.proxy_server"):
            ProxyServer()

        assert any(
            "unbounded" in record.getMessage().lower()
            for record in caplog.records
        )

    def test_no_unbounded_log_when_bounded(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", False)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 10)
        from proxy_server import ProxyServer

        with caplog.at_level(logging.INFO, logger="kataproxy.proxy_server"):
            ProxyServer()

        assert not any(
            "unbounded" in record.getMessage().lower()
            for record in caplog.records
        )

    def test_no_unbounded_log_when_disabled(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", True)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 0)
        from proxy_server import ProxyServer

        with caplog.at_level(logging.INFO, logger="kataproxy.proxy_server"):
            ProxyServer()

        assert not any(
            "unbounded" in record.getMessage().lower()
            for record in caplog.records
        )


# ===========================================================================
# query_version capability advertisement
# ===========================================================================


class TestCacheCapabilityAdvertisement:
    def test_cache_capability_absent_when_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", True)
        from proxy_server import _build_advertised_capabilities

        advertised = _build_advertised_capabilities()
        assert "cache" not in advertised

    def test_cache_capability_bounded_when_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", False)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 1024)
        from proxy_server import _build_advertised_capabilities

        advertised = _build_advertised_capabilities()
        assert advertised["cache"] == {
            "bounded": True,
            "max_entries": 1024,
            "key_scope": "engine-facing",
        }

    def test_cache_capability_unbounded_when_enabled_and_max_nonpositive(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cfg, "HUB_CACHE_DISABLED", False)
        monkeypatch.setattr(cfg, "HUB_CACHE_MAX", 0)
        from proxy_server import _build_advertised_capabilities

        advertised = _build_advertised_capabilities()
        assert advertised["cache"] == {
            "bounded": False,
            "key_scope": "engine-facing",
        }
        assert "max_entries" not in advertised["cache"]
