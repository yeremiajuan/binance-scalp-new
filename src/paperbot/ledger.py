"""Balances, native fee arithmetic and average-cost inventory.

Supported fee models (no others; BNB conversion is rejected by config):

* Buy, base-asset fee (default): debit ``q*p`` USDT, charge ``q*f`` BTC,
  credit ``q*(1-f)`` BTC.
* Buy, quote-asset fee: debit ``q*p*(1+f)`` USDT, credit ``q`` BTC.
* Sell (quote-asset fee): debit ``q`` BTC, credit ``q*p*(1-f)`` USDT.

Inventory uses one average-cost pool for all BTC held, including dust. The
pool tracks the gross execution cost (fill price times BTC credited) and the
entry-fee cost separately, so the entry fee is attributed exactly once and the
execution-gross figure never subtracts a fee.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .money import ZERO, exact, quantize_alloc


class InvariantViolation(RuntimeError):
    """An accounting invariant would be broken; the transaction must abort."""


@dataclass
class Balances:
    usdt_free: Decimal
    usdt_locked: Decimal
    btc_free: Decimal
    btc_locked: Decimal

    def get(self, asset: str, kind: str) -> Decimal:
        return getattr(self, f"{asset.lower()}_{kind}")

    def apply(self, asset: str, free_delta: Decimal, locked_delta: Decimal) -> None:
        if asset not in ("USDT", "BTC"):
            raise InvariantViolation(f"unknown asset {asset}")
        free = self.get(asset, "free") + free_delta
        locked = self.get(asset, "locked") + locked_delta
        if free < 0 or locked < 0:
            raise InvariantViolation(
                f"{asset} balance would go negative (free={free}, locked={locked})"
            )
        setattr(self, f"{asset.lower()}_free", free)
        setattr(self, f"{asset.lower()}_locked", locked)

    @property
    def btc_total(self) -> Decimal:
        return self.btc_free + self.btc_locked

    @property
    def usdt_total(self) -> Decimal:
        return self.usdt_free + self.usdt_locked


@dataclass
class Pool:
    qty: Decimal
    gross_cost: Decimal
    entry_fee_cost: Decimal

    @property
    def basis(self) -> Decimal:
        return self.gross_cost + self.entry_fee_cost

    @property
    def avg_gross_price(self) -> Decimal | None:
        return self.gross_cost / self.qty if self.qty > 0 else None


@dataclass(frozen=True)
class BuyAmounts:
    usdt_debit: Decimal
    btc_credit: Decimal
    fee_asset: str
    fee_amount: Decimal
    fee_usdt: Decimal
    gross_cost: Decimal
    entry_fee_cost: Decimal


@dataclass(frozen=True)
class SellAmounts:
    btc_debit: Decimal
    proceeds: Decimal
    fee_amount: Decimal  # USDT
    usdt_credit: Decimal


@exact
def buy_amounts(qty: Decimal, price: Decimal, fee: Decimal, fee_asset: str) -> BuyAmounts:
    notional = qty * price
    if fee_asset == "BTC":
        fee_btc = qty * fee
        credit = qty - fee_btc
        return BuyAmounts(
            usdt_debit=notional,
            btc_credit=credit,
            fee_asset="BTC",
            fee_amount=fee_btc,
            fee_usdt=fee_btc * price,
            gross_cost=credit * price,
            entry_fee_cost=fee_btc * price,
        )
    if fee_asset == "USDT":
        fee_usdt = notional * fee
        return BuyAmounts(
            usdt_debit=notional + fee_usdt,
            btc_credit=qty,
            fee_asset="USDT",
            fee_amount=fee_usdt,
            fee_usdt=fee_usdt,
            gross_cost=notional,
            entry_fee_cost=fee_usdt,
        )
    raise InvariantViolation(f"unsupported buy fee asset {fee_asset!r}")


@exact
def sell_amounts(qty: Decimal, price: Decimal, fee: Decimal) -> SellAmounts:
    proceeds = qty * price
    fee_usdt = proceeds * fee
    return SellAmounts(btc_debit=qty, proceeds=proceeds, fee_amount=fee_usdt, usdt_credit=proceeds - fee_usdt)


@exact
def allocate(pool: Pool, qty: Decimal) -> tuple[Decimal, Decimal]:
    """Return (gross_cost, entry_fee_cost) allocated to ``qty`` sold from the pool."""
    if qty <= 0 or qty > pool.qty:
        raise InvariantViolation(f"cannot allocate {qty} from pool of {pool.qty}")
    if qty == pool.qty:
        return pool.gross_cost, pool.entry_fee_cost
    gross = quantize_alloc(pool.gross_cost * qty / pool.qty)
    fee = quantize_alloc(pool.entry_fee_cost * qty / pool.qty)
    return gross, fee


@exact
def add_to_pool(pool: Pool, amounts: BuyAmounts) -> None:
    pool.qty += amounts.btc_credit
    pool.gross_cost += amounts.gross_cost
    pool.entry_fee_cost += amounts.entry_fee_cost


@exact
def remove_from_pool(pool: Pool, qty: Decimal, gross: Decimal, fee: Decimal) -> None:
    pool.qty -= qty
    pool.gross_cost -= gross
    pool.entry_fee_cost -= fee
    if pool.qty < 0 or pool.gross_cost < 0 or pool.entry_fee_cost < 0:
        raise InvariantViolation("inventory pool went negative")
    if pool.qty == 0 and (pool.gross_cost != 0 or pool.entry_fee_cost != 0):
        raise InvariantViolation("empty pool retains cost basis")


ZERO_POOL = Pool(ZERO, ZERO, ZERO)
