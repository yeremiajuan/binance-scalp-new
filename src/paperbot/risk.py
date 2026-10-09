"""Conservative liquidation equity, exposure and persistent risk latches.

Loss controls are protective settings, not guaranteed loss caps: gaps,
latency and thin liquidity can overshoot them. Each latch records the
observed loss beside its threshold so overshoot is visible.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .constraints import SymbolRules
from .execution import sell_fill_price, sell_limit_price
from .ledger import Balances, Pool
from .money import ONE, ZERO, exact, floor_to

LATCHES = ("manual_kill", "daily_loss", "drawdown")


@dataclass
class RiskState:
    day: str | None = None
    day_baseline: Decimal | None = None
    baseline_pending: bool = True
    hwm: Decimal | None = None
    latches: list[str] = field(default_factory=list)
    last_equity: Decimal | None = None
    last_mark_us: int | None = None


@dataclass(frozen=True)
class Valuation:
    bid: Decimal
    total_btc: Decimal
    sellable_qty: Decimal
    dust_qty: Decimal
    liquidation_price: Decimal
    liquidation_value: Decimal  # sellable BTC at bid after sell slippage and fee; dust counts zero
    equity: Decimal  # conservative liquidation equity
    btc_mark_value: Decimal  # all BTC (incl. dust) at bid, for exposure and disclosure
    exposure: Decimal  # all BTC at bid plus pending buy reservations


@exact
def sellable_quantity(total_btc: Decimal, rules: SymbolRules, bid: Decimal, sell_cushion: Decimal) -> Decimal:
    """Quantity that could be submitted as a valid SELL LIMIT now; the rest is unsellable dust."""
    q = floor_to(total_btc, rules.step)
    if q <= 0:
        return ZERO
    if rules.min_qty and q < rules.min_qty:
        return ZERO
    limit = sell_limit_price(bid, sell_cushion, rules.tick)
    if limit <= 0 or q * limit < rules.min_notional():
        return ZERO
    return q


@exact
def value(balances: Balances, pool: Pool, rules: SymbolRules, bid: Decimal, *, slippage: Decimal,
          sell_fee: Decimal, sell_cushion: Decimal) -> Valuation:
    total = balances.btc_total
    sellable = sellable_quantity(total, rules, bid, sell_cushion)
    px = sell_fill_price(bid, slippage, rules.tick)
    lv = sellable * px * (ONE - sell_fee)
    mark = total * bid
    return Valuation(
        bid=bid,
        total_btc=total,
        sellable_qty=sellable,
        dust_qty=total - sellable,
        liquidation_price=px,
        liquidation_value=lv,
        equity=balances.usdt_total + lv,
        btc_mark_value=mark,
        exposure=mark + balances.usdt_locked,
    )
