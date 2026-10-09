"""Strict TOML configuration for the PAPER core.

Every cost, execution and risk value is a configurable, unvalidated hypothesis.
B20-T5-v1 strategy parameters are frozen in ``strategy.py`` and are not
configurable: changing them would be a new strategy version.

Validation is strict: unknown keys, binary floats, live/testnet modes and any
credential-like key are rejected before anything else happens.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .money import BPS, ZERO, dtext, exact, to_dec
from .strategy import STRATEGY_VERSION
from .timeutil import US_PER_MS

SUPPORTED_SYMBOL = "BTCUSDT"
SUPPORTED_BUY_FEE_ASSETS = ("BTC", "USDT")
FORBIDDEN_KEY_FRAGMENTS = ("key", "secret", "token", "password", "credential", "signer", "endpoint", "url")


class ConfigError(ValueError):
    pass


class LiveModeRejected(ConfigError):
    pass


# section -> {key: (kind, required)}; kind is "str", "dec", "int"
_SCHEMA: dict[str, dict[str, tuple[str, bool]]] = {
    "run": {
        "mode": ("str", True),
        "account_id": ("str", True),
        "symbol": ("str", True),
        "strategy": ("str", True),
    },
    "account": {
        "starting_usdt": ("dec", True),
        "starting_btc": ("dec", True),
    },
    "fees": {
        "buy_fee_bps": ("dec", True),
        "sell_fee_bps": ("dec", True),
        "buy_fee_asset": ("str", True),
    },
    "execution": {
        "slippage_bps": ("dec", True),
        "latency_ms": ("int", True),
        "quote_max_age_ms": ("int", True),
        "candle_max_lateness_ms": ("int", True),
        "signal_expiry_ms": ("int", True),
        "max_spread_bps": ("dec", True),
        "max_entry_drift_atr": ("dec", True),
        "buy_limit_cushion_bps": ("dec", True),
        "sell_limit_cushion_bps": ("dec", True),
        "participation_fraction": ("dec", True),
        "reference_max_age_ms": ("int", True),
    },
    "risk": {
        "max_exposure_fraction": ("dec", True),
        "risk_per_entry_fraction": ("dec", True),
        "daily_loss_fraction": ("dec", True),
        "drawdown_fraction": ("dec", True),
        "day_timezone": ("str", True),
    },
    "metadata": {
        "path": ("str", True),
    },
}


@dataclass(frozen=True)
class Config:
    account_id: str
    symbol: str
    strategy: str
    starting_usdt: Decimal
    starting_btc: Decimal
    buy_fee: Decimal
    sell_fee: Decimal
    buy_fee_asset: str
    slippage: Decimal
    latency_us: int
    quote_max_age_us: int
    candle_max_lateness_us: int
    signal_expiry_us: int
    max_spread: Decimal
    max_entry_drift_atr: Decimal
    buy_limit_cushion: Decimal
    sell_limit_cushion: Decimal
    participation: Decimal
    reference_max_age_us: int
    max_exposure: Decimal
    risk_per_entry: Decimal
    daily_loss: Decimal
    drawdown: Decimal
    day_timezone: str
    metadata_path: str
    canonical: str  # canonical JSON of the economic configuration (excludes file paths)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical.encode()).hexdigest()


def _check_forbidden(obj: object, path: str = "") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in ("live", "testnet") or any(f in lk for f in FORBIDDEN_KEY_FRAGMENTS):
                raise LiveModeRejected(
                    f"configuration key {path}{k!r} is not allowed: Phase 1 is PAPER-only and has no live, "
                    "testnet, credential or endpoint settings"
                )
            _check_forbidden(v, f"{path}{k}.")


def _parse_value(kind: str, raw: object, field: str) -> object:
    if kind == "str":
        if not isinstance(raw, str) or not raw.strip():
            raise ConfigError(f"{field}: expected non-empty string")
        return raw
    if kind == "int":
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ConfigError(f"{field}: expected integer")
        return raw
    if kind == "dec":
        if isinstance(raw, float):
            raise ConfigError(f"{field}: write decimals as strings (e.g. \"10\"), not TOML floats")
        try:
            return to_dec(raw, field)
        except (TypeError, ValueError) as exc:
            raise ConfigError(str(exc)) from exc
    raise AssertionError(kind)


@exact
def parse_config(data: dict, base_dir: Path | None = None) -> Config:
    if not isinstance(data, dict):
        raise ConfigError("configuration must be a table")
    _check_forbidden(data)
    unknown = set(data) - set(_SCHEMA)
    if unknown:
        raise ConfigError(f"unknown configuration sections: {sorted(unknown)}")
    values: dict[str, dict[str, object]] = {}
    for section, keys in _SCHEMA.items():
        table = data.get(section)
        if not isinstance(table, dict):
            raise ConfigError(f"missing [{section}] table")
        extra = set(table) - set(keys)
        if extra:
            raise ConfigError(f"[{section}] unknown keys: {sorted(extra)}")
        values[section] = {}
        for key, (kind, required) in keys.items():
            if key not in table:
                if required:
                    raise ConfigError(f"[{section}] missing required key {key!r}")
                continue
            values[section][key] = _parse_value(kind, table[key], f"{section}.{key}")

    run = values["run"]
    mode = str(run["mode"]).lower()
    if mode != "paper":
        raise LiveModeRejected(
            f"run.mode={run['mode']!r} rejected: only 'paper' exists. Live and testnet execution are not "
            "implemented and cannot be enabled by configuration."
        )
    if run["symbol"] != SUPPORTED_SYMBOL:
        raise ConfigError(f"run.symbol must be {SUPPORTED_SYMBOL!r} in Phase 1")
    if run["strategy"] != STRATEGY_VERSION:
        raise ConfigError(f"run.strategy must be {STRATEGY_VERSION!r}")

    acct = values["account"]
    if acct["starting_usdt"] <= 0:
        raise ConfigError("account.starting_usdt must be > 0")
    if acct["starting_btc"] != 0:
        raise ConfigError(
            "account.starting_btc must be 0 in Phase 1 (pre-existing inventory would need a declared cost basis "
            "and exit semantics that are not part of this phase)"
        )

    fees = values["fees"]
    asset = str(fees["buy_fee_asset"]).upper()
    if asset not in SUPPORTED_BUY_FEE_ASSETS:
        raise ConfigError(
            f"fees.buy_fee_asset={fees['buy_fee_asset']!r} unsupported: use 'BTC' (base) or 'USDT' (quote). "
            "BNB fee conversion is not implemented."
        )
    for k in ("buy_fee_bps", "sell_fee_bps"):
        if not (ZERO <= fees[k] < 10000):
            raise ConfigError(f"fees.{k} must be in [0, 10000)")

    ex = values["execution"]
    for k in ("latency_ms", "quote_max_age_ms", "candle_max_lateness_ms", "signal_expiry_ms", "reference_max_age_ms"):
        if ex[k] < 0:
            raise ConfigError(f"execution.{k} must be >= 0")
    for k in ("slippage_bps", "max_spread_bps", "buy_limit_cushion_bps", "sell_limit_cushion_bps"):
        if not (ZERO <= ex[k] < 10000):
            raise ConfigError(f"execution.{k} must be in [0, 10000)")
    if ex["max_entry_drift_atr"] < 0:
        raise ConfigError("execution.max_entry_drift_atr must be >= 0")
    if not (ZERO < ex["participation_fraction"] <= 1):
        raise ConfigError("execution.participation_fraction must be in (0, 1]")

    risk = values["risk"]
    for k in ("max_exposure_fraction", "risk_per_entry_fraction", "daily_loss_fraction", "drawdown_fraction"):
        if not (ZERO < risk[k] <= 1):
            raise ConfigError(f"risk.{k} must be in (0, 1]")
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(str(risk["day_timezone"]))
    except Exception as exc:  # noqa: BLE001 - any lookup failure is a config error
        raise ConfigError(f"risk.day_timezone {risk['day_timezone']!r} is not a valid IANA zone") from exc

    meta_path = Path(str(values["metadata"]["path"]))
    if base_dir is not None and not meta_path.is_absolute():
        meta_path = base_dir / meta_path

    canonical_obj = {
        section: {k: (dtext(v) if isinstance(v, Decimal) else v) for k, v in sorted(vals.items())}
        for section, vals in sorted(values.items())
        if section != "metadata"
    }
    canonical_obj["run"]["mode"] = "paper"
    canonical_obj["fees"]["buy_fee_asset"] = asset
    canonical = json.dumps(canonical_obj, sort_keys=True, separators=(",", ":"))

    return Config(
        account_id=str(run["account_id"]),
        symbol=str(run["symbol"]),
        strategy=str(run["strategy"]),
        starting_usdt=acct["starting_usdt"],
        starting_btc=acct["starting_btc"],
        buy_fee=fees["buy_fee_bps"] * BPS,
        sell_fee=fees["sell_fee_bps"] * BPS,
        buy_fee_asset=asset,
        slippage=ex["slippage_bps"] * BPS,
        latency_us=ex["latency_ms"] * US_PER_MS,
        quote_max_age_us=ex["quote_max_age_ms"] * US_PER_MS,
        candle_max_lateness_us=ex["candle_max_lateness_ms"] * US_PER_MS,
        signal_expiry_us=ex["signal_expiry_ms"] * US_PER_MS,
        max_spread=ex["max_spread_bps"] * BPS,
        max_entry_drift_atr=ex["max_entry_drift_atr"],
        buy_limit_cushion=ex["buy_limit_cushion_bps"] * BPS,
        sell_limit_cushion=ex["sell_limit_cushion_bps"] * BPS,
        participation=ex["participation_fraction"],
        reference_max_age_us=ex["reference_max_age_ms"] * US_PER_MS,
        max_exposure=risk["max_exposure_fraction"],
        risk_per_entry=risk["risk_per_entry_fraction"],
        daily_loss=risk["daily_loss_fraction"],
        drawdown=risk["drawdown_fraction"],
        day_timezone=str(risk["day_timezone"]),
        metadata_path=str(meta_path),
        canonical=canonical,
    )


def load_config(path: str | Path) -> Config:
    p = Path(path)
    try:
        data = tomllib.loads(p.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{p}: invalid TOML: {exc}") from exc
    return parse_config(data, base_dir=p.resolve().parent)


def config_from_canonical(canonical: str, metadata_path: str = "<stored>") -> Config:
    """Rebuild the Config stored in a state database (identity is the canonical JSON)."""
    data = json.loads(canonical)
    data["metadata"] = {"path": metadata_path}
    cfg = parse_config(data)
    if cfg.canonical != canonical:
        raise ConfigError("stored configuration does not round-trip to the same canonical form")
    return cfg
