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


# Optional sections: key -> (kind, default). Path-like keys and monitoring settings are excluded from the
# canonical (economic) configuration, so they never change the configuration identity.
_FORWARD = {
    "rest_host": ("str", "default"),  # resolved against paperbot_net.hosts (public market-data hosts only)
    "ws_host": ("str", "default"),
    "quote_sample_ms": ("int", 1000),
    "heartbeat_ms": ("int", 1000),
    "metadata_refresh_s": ("int", 3600),
    "metadata_max_age_s": ("int", 5400),
    "reference_refresh_s": ("int", 30),
    "warmup_bars": ("int", 300),
    "backfill_timeout_s": ("int", 30),
    "reconnect_initial_ms": ("int", 1000),
    "reconnect_max_ms": ("int", 60000),
    "ws_silence_s": ("int", 30),
    "clock_check_s": ("int", 300),
    "clock_max_offset_ms": ("int", 1000),
    "rest_timeout_s": ("int", 10),
    "rest_weight_fraction": ("dec", "0.5"),
    "daily_summary_local": ("str", "00:05"),
    "recordings_dir": ("path", "recordings"),
    "profile_lock_dir": ("path", "~/.local/state/paperbot/locks"),
}
_TELEGRAM = {
    "enabled": ("bool", False),
    "env_var": ("str", "PAPERBOT_TELEGRAM_BOT"),
    "chat_ids": ("list_str", []),
    "max_attempts": ("int", 5),
    "retention_days": ("int", 7),
    "delayed_after_s": ("int", 60),
    "poll_commands": ("bool", True),
}
_OPTIONAL = {"forward": _FORWARD, "telegram": _TELEGRAM}


@dataclass(frozen=True)
class ForwardSettings:
    rest_host: str
    ws_host: str
    quote_sample_ms: int
    heartbeat_ms: int
    metadata_refresh_s: int
    metadata_max_age_s: int
    reference_refresh_s: int
    warmup_bars: int
    backfill_timeout_s: int
    reconnect_initial_ms: int
    reconnect_max_ms: int
    ws_silence_s: int
    clock_check_s: int
    clock_max_offset_ms: int
    rest_timeout_s: int
    rest_weight_fraction: Decimal
    daily_summary_local: str
    recordings_dir: str
    profile_lock_dir: str



@dataclass(frozen=True)
class TelegramSettings:
    enabled: bool
    env_var: str
    chat_ids: tuple[str, ...]
    max_attempts: int
    retention_days: int
    delayed_after_s: int
    poll_commands: bool


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
    forward: ForwardSettings | None = None
    telegram: TelegramSettings | None = None

    @property
    def metadata_max_age_us(self) -> int | None:
        return None if self.forward is None else self.forward.metadata_max_age_s * 1_000_000

    @property
    def clock_max_offset_us(self) -> int | None:
        return None if self.forward is None else self.forward.clock_max_offset_ms * 1000

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
    unknown = set(data) - set(_SCHEMA) - set(_OPTIONAL)
    if unknown:
        raise ConfigError(f"unknown configuration sections: {sorted(unknown)}")
    values: dict[str, dict[str, object]] = {}
    optional = _parse_optional(data, base_dir)
    for section, keys in _SCHEMA.items():
        table = data.get(section)
        if section == "metadata" and table is None and "forward" in optional:
            table = {"path": "<forward: metadata is fetched from the public REST API>"}
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
    if "forward" in optional:
        canonical_obj["forward"] = {k: (dtext(v) if isinstance(v, Decimal) else v)
                                    for k, v in sorted(optional["forward"].items())
                                    if _FORWARD[k][0] != "path"}
    canonical = json.dumps(canonical_obj, sort_keys=True, separators=(",", ":"))
    forward = ForwardSettings(**optional["forward"]) if "forward" in optional else None
    telegram = None
    if "telegram" in optional:
        t = dict(optional["telegram"])
        t["chat_ids"] = tuple(t["chat_ids"])
        telegram = TelegramSettings(**t)

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
        forward=forward,
        telegram=telegram,
    )


def _parse_optional(data: dict, base_dir: Path | None) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for section, schema in _OPTIONAL.items():
        if section not in data:
            continue
        table = data[section]
        if not isinstance(table, dict):
            raise ConfigError(f"[{section}] must be a table")
        extra = set(table) - set(schema)
        if extra:
            raise ConfigError(f"[{section}] unknown keys: {sorted(extra)}")
        vals: dict[str, object] = {}
        for key, (kind, default) in schema.items():
            raw = table.get(key, default)
            field_name = f"{section}.{key}"
            if kind == "bool":
                if not isinstance(raw, bool):
                    raise ConfigError(f"{field_name}: expected boolean")
                vals[key] = raw
            elif kind == "list_str":
                if not isinstance(raw, list) or not all(isinstance(x, str | int) and not isinstance(x, bool)
                                                        for x in raw):
                    raise ConfigError(f"{field_name}: expected a list of chat ids")
                vals[key] = [str(x) for x in raw]
            elif kind == "path":
                if not isinstance(raw, str) or not raw:
                    raise ConfigError(f"{field_name}: expected a path string")
                pth = Path(raw).expanduser()
                if base_dir is not None and not pth.is_absolute():
                    pth = base_dir / pth
                vals[key] = str(pth)
            else:
                vals[key] = _parse_value(kind, raw, field_name)
        out[section] = vals
    fwd = out.get("forward")
    if fwd is not None:
        from paperbot_net.hosts import REST_HOSTS, WS_HOSTS  # the network package owns the host allowlist

        if fwd["rest_host"] == "default":
            fwd["rest_host"] = REST_HOSTS[0]
        if fwd["ws_host"] == "default":
            fwd["ws_host"] = next(iter(WS_HOSTS))
        if fwd["rest_host"] not in REST_HOSTS:
            raise ConfigError(f"forward.rest_host must be one of {REST_HOSTS} (public market data only)")
        if fwd["ws_host"] not in WS_HOSTS:
            raise ConfigError(f"forward.ws_host must be one of {sorted(WS_HOSTS)} (public market data only)")
        for k in ("quote_sample_ms", "heartbeat_ms", "metadata_refresh_s", "metadata_max_age_s",
                  "reference_refresh_s", "backfill_timeout_s", "reconnect_initial_ms", "reconnect_max_ms",
                  "ws_silence_s", "clock_check_s", "clock_max_offset_ms", "rest_timeout_s"):
            if int(fwd[k]) <= 0:
                raise ConfigError(f"forward.{k} must be > 0")
        if not 260 <= int(fwd["warmup_bars"]) <= 1000:
            raise ConfigError("forward.warmup_bars must be in [260, 1000] (250-bar warm-up + lookback; REST max 1000)")
        if fwd["metadata_max_age_s"] <= fwd["metadata_refresh_s"]:
            raise ConfigError("forward.metadata_max_age_s must exceed metadata_refresh_s")
        if not (0 < fwd["rest_weight_fraction"] <= 1):
            raise ConfigError("forward.rest_weight_fraction must be in (0, 1]")
        if fwd["heartbeat_ms"] > 1000 or fwd["quote_sample_ms"] > 1000:
            raise ConfigError("forward.heartbeat_ms and quote_sample_ms must be <= 1000 (2 s freshness limit)")
        hh, _, mm = str(fwd["daily_summary_local"]).partition(":")
        if not (hh.isdigit() and mm.isdigit() and int(hh) < 24 and int(mm) < 60):
            raise ConfigError("forward.daily_summary_local must be HH:MM")
    tg = out.get("telegram")
    if tg is not None and tg["enabled"] and not tg["chat_ids"]:
        raise ConfigError("telegram.enabled requires an allowlist in telegram.chat_ids")
    return out


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
    if "forward" not in data:
        data["metadata"] = {"path": metadata_path}
    cfg = parse_config(data)
    if cfg.canonical != canonical:
        raise ConfigError("stored configuration does not round-trip to the same canonical form")
    return cfg
