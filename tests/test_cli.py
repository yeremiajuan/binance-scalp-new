"""CLI behavior, PAPER/SYNTHETIC labeling, read-only reports, live rejection and offline operation."""

from __future__ import annotations

import hashlib
import json
import re
import socket
from pathlib import Path

import pytest
from conftest import ROOT, write_config
from scenarios import entry_scenario, rich_scenario

from paperbot.cli import main
from paperbot.config import LiveModeRejected, load_config


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_validate_config(capsys):
    assert main(["validate-config", str(ROOT / "config" / "paper.toml")]) == 0
    out = capsys.readouterr().out
    assert out.startswith("PAPER | SYNTHETIC | configuration OK") and "config_sha256" in out


@pytest.mark.parametrize("argv", [
    ["--live", "status", "--state", "x"],
    ["replay", "--live", "--config", "c", "--input", "i", "--state", "s"],
    ["replay", "--mode=live", "--config", "c", "--input", "i", "--state", "s"],
    ["status", "--state", "x", "--testnet"],
])
def test_live_flags_are_rejected(argv, capsys):
    assert main(argv) == 2
    assert "REJECTED" in capsys.readouterr().err


@pytest.mark.parametrize("override", [
    {"run.mode": "live"}, {"run.mode": "testnet"}, {"run.mode": "LIVE"},
])
def test_live_mode_in_config_is_rejected(tmp_path, override, capsys):
    cfg = write_config(tmp_path, override)
    with pytest.raises(LiveModeRejected):
        load_config(cfg)
    assert main(["validate-config", str(cfg)]) == 2


@pytest.mark.parametrize("extra", ['api_key = "abc"', 'live = true', 'binance_secret = "x"', 'endpoint = "x"'])
def test_credential_or_live_keys_are_rejected(tmp_path, extra):
    cfg = write_config(tmp_path)
    text = cfg.read_text().replace("[run]\n", f"[run]\n{extra}\n")
    cfg.write_text(text)
    with pytest.raises(LiveModeRejected):
        load_config(cfg)


@pytest.mark.parametrize("override, msg", [
    ({"fees.buy_fee_asset": "BNB"}, "BNB"),
    ({"account.starting_btc": "0.1"}, "starting_btc"),
    ({"run.symbol": "ETHUSDT"}, "symbol"),
    ({"run.strategy": "B20-T5-v2"}, "strategy"),
])
def test_unsupported_configuration_rejected(tmp_path, override, msg, capsys):
    cfg = write_config(tmp_path, override)
    assert main(["validate-config", str(cfg)]) == 2
    assert msg in capsys.readouterr().err


def test_toml_float_rejected(tmp_path, capsys):
    cfg = write_config(tmp_path)
    cfg.write_text(cfg.read_text().replace('buy_fee_bps = "10"', "buy_fee_bps = 10.0"))
    assert main(["validate-config", str(cfg)]) == 2
    assert "not TOML floats" in capsys.readouterr().err


def test_replay_status_report_are_labeled_and_read_only(tmp_path, capsys):
    cfg = write_config(tmp_path)
    inp = rich_scenario().write(tmp_path / "in.jsonl")
    db = tmp_path / "s.sqlite"
    assert main(["replay", "--config", str(cfg), "--input", str(inp), "--state", str(db)]) == 0
    out = capsys.readouterr().out
    assert "PAPER | SYNTHETIC | replay" in out and sha(inp) in out
    h = sha(db)
    assert main(["status", "--state", str(db)]) == 0
    assert main(["report", "--state", str(db)]) == 0
    assert main(["report", "--state", str(db), "--json"]) == 0
    assert sha(db) == h  # read commands never mutate
    out = capsys.readouterr().out
    for needle in ("PAPER | SYNTHETIC", "config_sha256", "input_sha256", "metadata_label: SYNTHETIC",
                   "skip reasons", "ioc_remainder_canceled", "fees by native asset", "dust",
                   "UNRESOLVED", "exchange_time not supplied", "signal ", "submitted ", "ready ",
                   "identity starting+realized+unrealized = cash+liquidation", "-> True", "exit_reason=target",
                   "exit_reason=health:quotes_stale", "not evidence of a trading edge"):
        assert needle in out, needle
    js = json.loads(out[out.index("{\n"):])
    assert js["labels"] == ["PAPER", "SYNTHETIC"] and js["reconciliation"]["ok"]
    # nonempty account is never silently overwritten
    assert main(["replay", "--config", str(cfg), "--input", str(inp), "--state", str(db)]) == 4
    assert sha(db) == h


def test_kill_and_reset_cli_are_audited(tmp_path, capsys):
    cfg = write_config(tmp_path)
    sc, bb = entry_scenario()
    inp = sc.write(tmp_path / "in.jsonl")
    db = tmp_path / "s.sqlite"
    assert main(["replay", "--config", str(cfg), "--input", str(inp), "--state", str(db), "--quiet"]) == 0
    clock = lambda: "2026-10-09T05:00:00.000+00:00"  # noqa: E731 - injected wall clock for the audit record
    assert main(["kill", "--state", str(db), "--reason", "test"], wall_clock=clock) == 0
    assert main(["kill", "--state", str(db), "--reason", "again"], wall_clock=clock) == 4  # already latched
    assert main(["reset", "--state", str(db), "--latch", "manual_kill", "--reason", "ok"]) != 0  # needs --confirm
    assert main(["reset", "--state", str(db), "--latch", "manual_kill", "--reason", "ok", "--confirm"],
                wall_clock=clock) == 0
    capsys.readouterr()
    assert main(["report", "--state", str(db)]) == 0
    out = capsys.readouterr().out
    assert "CONTROL kill manual_kill" in out and "CONTROL reset manual_kill" in out
    assert "wall 2026-10-09T05:00:00.000+00:00" in out


def test_replay_runs_with_network_disabled(tmp_path, monkeypatch, capsys):
    def no_network(*a, **k):
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket, "socket", no_network)
    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    cfg = write_config(tmp_path)
    inp = rich_scenario().write(tmp_path / "in.jsonl")
    db = tmp_path / "s.sqlite"
    assert main(["replay", "--config", str(cfg), "--input", str(inp), "--state", str(db), "--quiet"]) == 0
    assert main(["report", "--state", str(db)]) == 0


def test_source_has_no_network_credential_signer_or_order_endpoint_path():
    src = ROOT / "src" / "paperbot"
    forbidden_imports = re.compile(
        r"^\s*(import|from)\s+(socket|ssl|http|urllib|requests|httpx|aiohttp|websocket|websockets|asyncio|hmac|"
        r"binance|ccxt|telegram)\b", re.M)
    forbidden_text = re.compile(r"api\.binance|/api/v3/order|X-MBX-APIKEY|signature=|testnet\.binance", re.I)
    for f in src.rglob("*.py"):
        text = f.read_text()
        assert not forbidden_imports.search(text), f
        assert not forbidden_text.search(text), f
