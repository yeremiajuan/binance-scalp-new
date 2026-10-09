from __future__ import annotations

import json
import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

FULL_METADATA = ROOT / "fixtures" / "exchange_info_btcusdt_synthetic.json"

BASE_CONFIG = {
    "run": {"mode": "paper", "account_id": "test-account", "symbol": "BTCUSDT", "strategy": "B20-T5-v1"},
    "account": {"starting_usdt": "1000", "starting_btc": "0"},
    "fees": {"buy_fee_bps": "10", "sell_fee_bps": "10", "buy_fee_asset": "BTC"},
    "execution": {
        "slippage_bps": "1", "latency_ms": 250, "quote_max_age_ms": 2000, "candle_max_lateness_ms": 5000,
        "signal_expiry_ms": 5000, "max_spread_bps": "5", "max_entry_drift_atr": "0.25",
        "buy_limit_cushion_bps": "5", "sell_limit_cushion_bps": "50", "participation_fraction": "0.10",
        "reference_max_age_ms": 120000,
    },
    "risk": {
        "max_exposure_fraction": "0.20", "risk_per_entry_fraction": "0.001", "daily_loss_fraction": "0.01",
        "drawdown_fraction": "0.03", "day_timezone": "Asia/Jakarta",
    },
}


def minimal_metadata(**changes) -> dict:
    """SYNTHETIC metadata without percent-price filters (so plain scenarios need no reference prices)."""
    filters = [
        {"filterType": "PRICE_FILTER", "minPrice": "0.01", "maxPrice": "1000000.00", "tickSize": "0.01"},
        {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "9000", "stepSize": "0.00001"},
        {"filterType": "NOTIONAL", "minNotional": "5", "applyMinToMarket": True, "maxNotional": "9000000",
         "applyMaxToMarket": False, "avgPriceMins": 5},
        {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "100", "stepSize": "0"},
    ]
    meta = {
        "label": "SYNTHETIC", "retrieved_at": "2026-10-09T04:46:08.396Z", "description": "test fixture",
        "symbol": {"symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT",
                   "isSpotTradingAllowed": True, "orderTypes": ["LIMIT", "MARKET"], "filters": filters},
    }
    meta["symbol"].update(changes.pop("symbol", {}))
    meta.update(changes)
    return meta


def _toml_value(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    return json.dumps(str(v))


def write_config(directory: Path, overrides: dict | None = None, metadata: dict | Path | None = None,
                 name: str = "config.toml") -> Path:
    cfg = json.loads(json.dumps(BASE_CONFIG))
    for dotted, v in (overrides or {}).items():
        section, key = dotted.split(".")
        cfg[section][key] = v
    if metadata is None:
        metadata = minimal_metadata()
    if isinstance(metadata, dict):
        mpath = directory / f"{name}.metadata.json"
        mpath.write_text(json.dumps(metadata), encoding="utf-8")
    else:
        mpath = metadata
    lines = []
    for section, table in cfg.items():
        lines.append(f"[{section}]")
        lines += [f"{k} = {_toml_value(v)}" for k, v in table.items()]
    lines += ["[metadata]", f"path = {json.dumps(str(mpath))}"]
    p = directory / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def rows(db: Path, sql: str, args: tuple = ()) -> list[dict]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args)]
    finally:
        conn.close()


def dump_db(db: Path) -> dict:
    """Every table's full contents, for exact equivalence comparisons."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in tables}
    finally:
        conn.close()


def report(db: Path) -> dict:
    from paperbot.report import build_report
    from paperbot.storage import Storage

    s = Storage.open_readonly(db)
    try:
        return build_report(s)
    finally:
        s.close()


def D(x) -> Decimal:
    return Decimal(str(x))


@pytest.fixture
def cfg_path(tmp_path: Path) -> Path:
    return write_config(tmp_path)


def write_forward_config(directory: Path, overrides: dict | None = None, forward: dict | None = None,
                         telegram: dict | None = None, name: str = "forward.toml") -> Path:
    """A PAPER forward-runner configuration (public market data) for mocked/offline tests."""
    base = write_config(directory, overrides, name=name)
    fwd = {"recordings_dir": str(directory / "recordings"), "profile_lock_dir": str(directory / "locks"),
           **(forward or {})}
    text = base.read_text()
    text += "[forward]\n" + "".join(f"{k} = {_toml_value(v)}\n" for k, v in fwd.items())
    if telegram:
        text += "[telegram]\n" + "".join(
            f"{k} = {json.dumps(v) if isinstance(v, list) else _toml_value(v)}\n" for k, v in telegram.items())
    base.write_text(text)
    return base


@pytest.fixture
def fwd_cfg(tmp_path: Path) -> Path:
    return write_forward_config(tmp_path)
