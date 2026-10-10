"""Phase 2 configuration rules and static inspection of the network package."""

from __future__ import annotations

import re
import threading

import pytest
from conftest import ROOT, write_config, write_forward_config

from paperbot.config import ConfigError, LiveModeRejected, load_config
from paperbot_net import rest

NET = ROOT / "src" / "paperbot_net"


def test_forward_hosts_are_allowlisted_public_market_data_only(tmp_path):
    cfg = load_config(write_forward_config(tmp_path))
    assert cfg.forward.rest_host == "api.binance.com" and cfg.forward.ws_host == "data-stream.binance.vision"
    with pytest.raises(ConfigError, match="rest_host"):
        load_config(write_forward_config(tmp_path, forward={"rest_host": "testnet.binance.vision"}, name="a.toml"))
    with pytest.raises(ConfigError, match="ws_host"):
        load_config(write_forward_config(tmp_path, forward={"ws_host": "evil.example"}, name="b.toml"))
    with pytest.raises(LiveModeRejected):
        load_config(write_forward_config(tmp_path, forward={"api_key": "x"}, name="c.toml"))
    with pytest.raises(LiveModeRejected):
        load_config(write_forward_config(tmp_path, overrides={"run.mode": "live"}, name="d.toml"))


def test_paths_and_monitoring_do_not_change_the_configuration_identity(tmp_path):
    a = load_config(write_forward_config(tmp_path, name="a.toml"))
    b = load_config(write_forward_config(tmp_path / "x" if (tmp_path / "x").mkdir() is None else tmp_path,
                                         telegram={"enabled": True, "chat_ids": ["5"]}, name="b.toml"))
    assert a.sha256 == b.sha256  # different recordings/lock dirs and Telegram settings: same economics
    c = load_config(write_forward_config(tmp_path, forward={"quote_sample_ms": 500}, name="c.toml"))
    assert c.sha256 != a.sha256  # a normalization change is a new configuration version
    assert load_config(write_config(tmp_path)).sha256 != a.sha256
    # the accepted Phase 1 configuration hash is unchanged by Phase 2 code
    assert load_config(ROOT / "config" / "paper.toml").sha256 == \
        "3f9aba57ef97c6dab422298e5dbb26bdb9f98469e0b79baf8e4ea7c3c4e46d0b"


@pytest.mark.parametrize("bad", [{"heartbeat_ms": 5000}, {"warmup_bars": 100}, {"metadata_max_age_s": 10},
                                 {"daily_summary_local": "25:00"}, {"unknown": 1}])
def test_forward_settings_are_validated(tmp_path, bad):
    with pytest.raises(ConfigError):
        load_config(write_forward_config(tmp_path, forward=bad))


def test_network_package_has_no_signer_key_order_or_account_path():
    forbidden = re.compile(r"hmac|hashlib\.sha256\(.*secret|X-MBX-APIKEY|signature|/api/v3/order|/api/v3/account|"
                           r"userDataStream|listenKey|/sapi/|testnet", re.I)
    for f in NET.glob("*.py"):
        text = f.read_text()
        assert not forbidden.search(text), f"{f.name}: {forbidden.search(text).group(0)}"
    assert all(path.startswith("/api/v3/") for path, _ in rest.ENDPOINTS.values())
    assert set(rest.ENDPOINTS) == {"time", "exchangeInfo", "executionRules", "klines", "avgPrice", "referencePrice"}
    rest_src = (NET / "rest.py").read_text()
    assert 'method="GET"' in rest_src and "POST" not in rest_src and "PUT" not in rest_src
    assert "DELETE" not in rest_src


def test_core_package_still_has_no_network_imports():
    core = ROOT / "src" / "paperbot"
    pat = re.compile(r"^\s*(import|from)\s+(socket|ssl|http|urllib|requests|websockets?|asyncio)\b", re.M)
    for f in core.glob("*.py"):
        assert not pat.search(f.read_text()), f


def test_daily_summary_is_written_once_per_local_day(tmp_path):
    from fake_binance import FakeMarket, Harness, boot, standard_bars

    from paperbot_net.telegram import notifier_factory

    bars = standard_bars()
    cfg_path = write_forward_config(tmp_path, telegram={"enabled": True, "chat_ids": ["1"]},
                                    forward={"daily_summary_local": "00:05"})
    h = Harness(tmp_path, cfg_path, FakeMarket(bars))
    try:
        t = boot(h, bars[249].end_us + 10_000_000)
        cfg = load_config(cfg_path)

        class Out:
            clock = type("C", (), {"now_us": staticmethod(lambda: t)})()

            def put(self, *a):
                pass

        n = notifier_factory(cfg, str(h.state), Out(), threading.Event(), api=object())
        # bars[249] ends 16:10Z = 23:10 Asia/Jakarta: before 00:05 local -> nothing yet
        n.after_tick(h.session, t)
        from conftest import rows

        assert not rows(h.state, "SELECT * FROM outbox WHERE kind = 'daily'")
        later = t + 60 * 60 * 1_000_000  # 00:10 local the next day
        n.after_tick(h.session, later)
        n.after_tick(h.session, later + 1_000_000)
        daily = rows(h.state, "SELECT * FROM outbox WHERE kind = 'daily'")
        assert len(daily) == 1 and daily[0]["msg_id"] == "daily:2026-10-01"  # summarizes the day that ended
        assert daily[0]["text"].startswith("PAPER | PUBLIC DATA | FORWARD | MOCKED | DAILY")
    finally:
        h.close()


def test_websockets_floor_matches_the_real_connector_arguments(tmp_path, monkeypatch):
    """The declared minimum websockets release accepts every argument default_connect passes (review
    regression: 13.x/14.x forward ping_interval to socket creation and raise TypeError)."""
    import inspect
    import tomllib

    import websockets
    import websockets.sync.client as client

    from paperbot.config import ConfigError
    from paperbot_net import ws

    deps = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    (spec,) = [d for d in deps if d.startswith("websockets")]
    assert spec.startswith("websockets>=15.0,") and ws.MIN_WEBSOCKETS == (15, 0)
    named = inspect.getfullargspec(client.connect)
    assert set(ws.connect_kwargs(1.0)) <= set(named.args) | set(named.kwonlyargs)
    ws.check_websockets()  # the installed release is supported

    def old_connect(uri, *, sock=None, ssl=None, server_hostname=None, origin=None, extensions=None,
                    subprotocols=None, additional_headers=None, user_agent_header=None, compression="deflate",
                    open_timeout=10, close_timeout=10, max_size=2 ** 20, logger=None, create_connection=None,
                    **kwargs):  # websockets 13.1 shape: extras go to socket creation
        raise AssertionError("must not be called")

    monkeypatch.setattr(client, "connect", old_connect)
    monkeypatch.setattr(websockets, "__version__", "13.1")
    with pytest.raises(ConfigError, match="ping_interval"):
        ws.check_websockets()
    from paperbot_net.runner import run_forward

    state = tmp_path / "s.sqlite"
    with pytest.raises(ConfigError):  # refused before any lock is taken or any state is created
        run_forward(str(write_forward_config(tmp_path)), str(state), install_signals=False, log=lambda *_: None)
    assert not state.exists()
