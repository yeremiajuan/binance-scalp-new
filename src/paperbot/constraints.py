"""Dated exchange-metadata fixtures and LIMIT IOC order validation.

Phase 1 loads metadata from a dated fixture file. Values in a fixture are
observations or synthetic stand-ins; they are never permanent exchange
defaults. Public metadata never establishes account permissions, private
filters or effective commissions.

Validation rules (Binance spot filter semantics, LIMIT orders only):

* Increments come from filters (tickSize, stepSize), never from asset
  precision digit counts. A zero increment/limit disables that rule; no
  division or modulo by zero is performed.
* Orders are validated after rounding. Quantities are never rounded up to meet
  a minimum.
* MARKET_LOT_SIZE applies to MARKET orders and is inapplicable to LIMIT IOC.
  ``applyMinToMarket``/``applyMaxToMarket``/``applyToMarket`` only change MARKET
  order handling; LIMIT orders are always subject to the notional bounds.
* PERCENT_PRICE / PERCENT_PRICE_BY_SIDE need a fresh weighted-average reference
  price over the filter's ``avgPriceMins``. A missing, stale or mismatched
  reference blocks the order; no midpoint or last price is substituted.
* An unknown filter type is an unknown applicable constraint and blocks.
* Minimums apply to submitted orders, not to individual partial-fill fragments.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .money import ZERO, dtext, exact, is_multiple, to_dec

LABELS = ("SYNTHETIC", "PUBLIC_SNAPSHOT")

INAPPLICABLE = {
    "MARKET_LOT_SIZE": "applies to MARKET orders; Phase 1 submits LIMIT IOC only",
    "ICEBERG_PARTS": "no iceberg orders are submitted",
    "MAX_NUM_ICEBERG_ORDERS": "no iceberg orders are submitted",
    "MAX_NUM_ALGO_ORDERS": "no STOP_LOSS/TAKE_PROFIT algo orders are submitted (stops are local triggers)",
    "TRAILING_DELTA": "no trailing-stop orders are submitted",
    "MAX_NUM_ORDER_LISTS": "no OCO/order lists are submitted",
    "MAX_NUM_ORDER_AMENDS": "orders are never amended",
}
APPLICABLE = (
    "PRICE_FILTER",
    "LOT_SIZE",
    "MIN_NOTIONAL",
    "NOTIONAL",
    "PERCENT_PRICE",
    "PERCENT_PRICE_BY_SIDE",
    "MAX_NUM_ORDERS",
    "MAX_POSITION",
)

_DEC_FIELDS = {
    "PRICE_FILTER": ("minPrice", "maxPrice", "tickSize"),
    "LOT_SIZE": ("minQty", "maxQty", "stepSize"),
    "MIN_NOTIONAL": ("minNotional",),
    "NOTIONAL": ("minNotional", "maxNotional"),
    "PERCENT_PRICE": ("multiplierUp", "multiplierDown"),
    "PERCENT_PRICE_BY_SIDE": ("bidMultiplierUp", "bidMultiplierDown", "askMultiplierUp", "askMultiplierDown"),
    "MAX_POSITION": ("maxPosition",),
}
_INT_FIELDS = {
    "PERCENT_PRICE": ("avgPriceMins",),
    "PERCENT_PRICE_BY_SIDE": ("avgPriceMins",),
    "MAX_NUM_ORDERS": ("maxNumOrders",),
}


class MetadataError(ValueError):
    pass


@dataclass(frozen=True)
class SymbolRules:
    label: str
    retrieved_at: str
    description: str
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    spot_allowed: bool
    order_types: tuple[str, ...]
    filters: tuple[tuple[str, dict], ...]  # (filterType, parsed fields)
    sha256: str
    raw_json: str

    def filter(self, ftype: str) -> dict | None:
        for t, f in self.filters:
            if t == ftype:
                return f
        return None

    @property
    def tick(self) -> Decimal:
        f = self.filter("PRICE_FILTER")
        return f["tickSize"] if f else ZERO

    @property
    def step(self) -> Decimal:
        f = self.filter("LOT_SIZE")
        return f["stepSize"] if f else ZERO

    @property
    def min_qty(self) -> Decimal:
        f = self.filter("LOT_SIZE")
        return f["minQty"] if f else ZERO

    @property
    def max_qty(self) -> Decimal:
        f = self.filter("LOT_SIZE")
        return f["maxQty"] if f else ZERO

    @property
    def max_position(self) -> Decimal:
        f = self.filter("MAX_POSITION")
        return f["maxPosition"] if f else ZERO

    def min_notional(self) -> Decimal:
        best = ZERO
        for t in ("MIN_NOTIONAL", "NOTIONAL"):
            f = self.filter(t)
            if f is not None:
                best = max(best, f["minNotional"])
        return best


def _parse_filter(raw: dict) -> tuple[str, dict]:
    ftype = raw.get("filterType")
    if not isinstance(ftype, str):
        raise MetadataError(f"filter without filterType: {raw!r}")
    parsed: dict[str, object] = {}
    for k in _DEC_FIELDS.get(ftype, ()):
        if k not in raw:
            raise MetadataError(f"{ftype}: missing field {k}")
        v = to_dec(raw[k], f"{ftype}.{k}")
        if v < 0:
            raise MetadataError(f"{ftype}.{k} must be >= 0")
        parsed[k] = v
    for k in _INT_FIELDS.get(ftype, ()):
        if k not in raw or isinstance(raw[k], bool) or not isinstance(raw[k], int) or raw[k] < 0:
            raise MetadataError(f"{ftype}: missing or invalid integer field {k}")
        parsed[k] = raw[k]
    # Keep every other field verbatim (flags, limits of inapplicable filters).
    for k, v in raw.items():
        if k not in parsed and k != "filterType":
            parsed[k] = v
    return ftype, parsed


def parse_metadata(text: str) -> SymbolRules:
    try:
        data = json.loads(text, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        raise MetadataError(f"metadata is not valid JSON: {exc}") from exc
    label = data.get("label")
    if label not in LABELS:
        raise MetadataError(f"metadata label must be one of {LABELS}, got {label!r}")
    retrieved = data.get("retrieved_at")
    if not isinstance(retrieved, str) or not retrieved:
        raise MetadataError("metadata must be dated: missing retrieved_at")
    sym = data.get("symbol")
    if not isinstance(sym, dict):
        raise MetadataError("metadata.symbol must be an object")
    filters_raw = sym.get("filters")
    if not isinstance(filters_raw, list):
        raise MetadataError("symbol.filters must be a list")
    filters = tuple(_parse_filter(f) for f in filters_raw)
    types_seen = [t for t, _ in filters]
    if len(types_seen) != len(set(types_seen)):
        raise MetadataError("duplicate filter types")
    return SymbolRules(
        label=label,
        retrieved_at=retrieved,
        description=str(data.get("description", "")),
        symbol=str(sym.get("symbol")),
        status=str(sym.get("status")),
        base_asset=str(sym.get("baseAsset")),
        quote_asset=str(sym.get("quoteAsset")),
        spot_allowed=sym.get("isSpotTradingAllowed") is True,
        order_types=tuple(sym.get("orderTypes") or ()),
        filters=filters,
        sha256=hashlib.sha256(text.encode()).hexdigest(),
        raw_json=text,
    )


def load_metadata(path: str | Path) -> SymbolRules:
    return parse_metadata(Path(path).read_text(encoding="utf-8"))


@dataclass(frozen=True)
class Reference:
    """A weighted-average reference price observation (synthetic in Phase 1)."""

    avg_price: Decimal
    mins: int
    recv_us: int
    event_id: str


@dataclass(frozen=True)
class Check:
    filter: str
    rule: str
    status: str  # pass | fail | disabled | inapplicable
    detail: str = ""


@dataclass(frozen=True)
class Validation:
    checks: tuple[Check, ...]

    @property
    def ok(self) -> bool:
        return all(c.status != "fail" for c in self.checks)

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if c.status == "fail"]

    def first_failure(self) -> str | None:
        f = self.failures
        return f"{f[0].filter}:{f[0].rule}" if f else None

    def size_failure_only(self) -> bool:
        """True when every failure is a quantity/notional size rule (the remainder is unsellable dust)."""
        size_rules = {("LOT_SIZE", "minQty"), ("LOT_SIZE", "stepSize"), ("ORDER", "qty_positive"),
                      ("MIN_NOTIONAL", "minNotional"), ("NOTIONAL", "minNotional")}
        f = self.failures
        return bool(f) and all((c.filter, c.rule) in size_rules for c in f)

    def summary(self) -> list[dict]:
        return [{"filter": c.filter, "rule": c.rule, "status": c.status, "detail": c.detail} for c in self.checks]


@exact
def validate_limit_order(
    rules: SymbolRules,
    side: str,
    price: Decimal,
    qty: Decimal,
    *,
    open_orders: int,
    base_position: Decimal,
    reference: Reference | None,
    now_us: int,
    reference_max_age_us: int,
) -> Validation:
    """Validate a LIMIT IOC order exactly as it would be submitted (after rounding)."""
    assert side in ("BUY", "SELL")
    checks: list[Check] = []

    def add(f: str, rule: str, ok: bool, detail: str = "") -> None:
        checks.append(Check(f, rule, "pass" if ok else "fail", detail))

    add("SYMBOL", "status", rules.status == "TRADING", f"status={rules.status}")
    add("SYMBOL", "spot_allowed", rules.spot_allowed, "")
    add("SYMBOL", "order_type", "LIMIT" in rules.order_types, "LIMIT required")
    add("ORDER", "price_positive", price > 0, dtext(price))
    add("ORDER", "qty_positive", qty > 0, dtext(qty))
    notional = price * qty

    for ftype, f in rules.filters:
        if ftype in INAPPLICABLE:
            checks.append(Check(ftype, "-", "inapplicable", INAPPLICABLE[ftype]))
            continue
        if ftype not in APPLICABLE:
            add(ftype, "unknown_filter", False, "unknown active constraint blocks the order")
            continue
        if ftype == "PRICE_FILTER":
            mn, mx, tick = f["minPrice"], f["maxPrice"], f["tickSize"]
            _bound(checks, ftype, "minPrice", mn, price >= mn, f"{dtext(price)} >= {dtext(mn)}")
            _bound(checks, ftype, "maxPrice", mx, price <= mx, f"{dtext(price)} <= {dtext(mx)}")
            _bound(checks, ftype, "tickSize", tick, is_multiple(price - mn, tick) if tick else True,
                   f"({dtext(price)}-{dtext(mn)}) % {dtext(tick)} == 0")
        elif ftype == "LOT_SIZE":
            mn, mx, step = f["minQty"], f["maxQty"], f["stepSize"]
            _bound(checks, ftype, "minQty", mn, qty >= mn, f"{dtext(qty)} >= {dtext(mn)}")
            _bound(checks, ftype, "maxQty", mx, qty <= mx, f"{dtext(qty)} <= {dtext(mx)}")
            _bound(checks, ftype, "stepSize", step, is_multiple(qty - mn, step) if step else True,
                   f"({dtext(qty)}-{dtext(mn)}) % {dtext(step)} == 0")
        elif ftype == "MIN_NOTIONAL":
            mn = f["minNotional"]
            _bound(checks, ftype, "minNotional", mn, notional >= mn,
                   f"{dtext(notional)} >= {dtext(mn)} (LIMIT order; applyToMarket only affects MARKET)")
        elif ftype == "NOTIONAL":
            mn, mx = f["minNotional"], f["maxNotional"]
            _bound(checks, ftype, "minNotional", mn, notional >= mn,
                   f"{dtext(notional)} >= {dtext(mn)} (LIMIT order; applyMinToMarket only affects MARKET)")
            _bound(checks, ftype, "maxNotional", mx, notional <= mx,
                   f"{dtext(notional)} <= {dtext(mx)} (LIMIT order; applyMaxToMarket only affects MARKET)")
        elif ftype in ("PERCENT_PRICE", "PERCENT_PRICE_BY_SIDE"):
            if ftype == "PERCENT_PRICE":
                up, down = f["multiplierUp"], f["multiplierDown"]
            elif side == "BUY":
                up, down = f["bidMultiplierUp"], f["bidMultiplierDown"]
            else:
                up, down = f["askMultiplierUp"], f["askMultiplierDown"]
            if up == 0 and down == 0:
                checks.append(Check(ftype, "reference", "disabled", "both multipliers are zero"))
                continue
            mins = f["avgPriceMins"]
            if reference is None:
                add(ftype, "reference", False, f"required {mins}-minute weighted average price unavailable")
                continue
            if reference.mins != mins:
                add(ftype, "reference", False, f"reference window {reference.mins}m != required {mins}m")
                continue
            age = now_us - reference.recv_us
            if age < 0 or age > reference_max_age_us:
                add(ftype, "reference", False, f"reference age {age}us outside [0, {reference_max_age_us}]")
                continue
            add(ftype, "reference", True, f"avgPrice={dtext(reference.avg_price)} ({mins}m)")
            ref = reference.avg_price
            _bound(checks, ftype, "multiplierUp", up, price <= ref * up, f"{dtext(price)} <= {dtext(ref * up)}")
            _bound(checks, ftype, "multiplierDown", down, price >= ref * down,
                   f"{dtext(price)} >= {dtext(ref * down)}")
        elif ftype == "MAX_NUM_ORDERS":
            mx = f["maxNumOrders"]
            if mx == 0:
                checks.append(Check(ftype, "maxNumOrders", "disabled", "0"))
            else:
                add(ftype, "maxNumOrders", open_orders + 1 <= mx, f"{open_orders}+1 <= {mx}")
        elif ftype == "MAX_POSITION":
            mx = f["maxPosition"]
            if side == "SELL":
                checks.append(Check(ftype, "maxPosition", "inapplicable", "sell orders reduce the position"))
            else:
                _bound(checks, ftype, "maxPosition", mx, base_position + qty <= mx,
                       f"{dtext(base_position)}+{dtext(qty)} <= {dtext(mx)}")
    return Validation(tuple(checks))


def _bound(checks: list[Check], ftype: str, rule: str, limit: Decimal, ok: bool, detail: str) -> None:
    if limit == 0:
        checks.append(Check(ftype, rule, "disabled", f"{rule}=0 disables this rule"))
    else:
        checks.append(Check(ftype, rule, "pass" if ok else "fail", detail))
