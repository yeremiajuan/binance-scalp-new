"""Price-protected marketable LIMIT IOC simulation helpers.

There is no matching engine, queue position, hidden liquidity, market impact
model or historical order book here, and nothing proves a production order
would fill. A fill needs a quote observation *received* at or after
``submitted_at + latency`` (and, when the payload carries an exchange event
time, an exchange time at or after readiness). The quote cached at submission,
the signal candle close and candle highs/lows never supply fills.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .money import ONE, ZERO, ceil_to, exact, floor_to


@dataclass(frozen=True)
class QuoteView:
    event_id: str
    recv_us: int
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    exchange_us: int | None


@exact
def spread_fraction(bid: Decimal, ask: Decimal) -> Decimal:
    mid = (bid + ask) / 2
    return (ask - bid) / mid


@exact
def buy_limit_price(ask: Decimal, cushion: Decimal, tick: Decimal) -> Decimal:
    return floor_to(ask * (ONE + cushion), tick)


@exact
def sell_limit_price(bid: Decimal, cushion: Decimal, tick: Decimal) -> Decimal:
    return ceil_to(bid * (ONE - cushion), tick)


@exact
def buy_fill_price(ask: Decimal, slippage: Decimal, tick: Decimal) -> Decimal:
    """Cross the ask, add adverse slippage, round adversely (up)."""
    return ceil_to(ask * (ONE + slippage), tick)


@exact
def sell_fill_price(bid: Decimal, slippage: Decimal, tick: Decimal) -> Decimal:
    """Cross the bid, subtract adverse slippage, round adversely (down)."""
    return floor_to(bid * (ONE - slippage), tick)


@exact
def fill_quantity(remaining: Decimal, visible: Decimal, participation: Decimal, step: Decimal) -> Decimal:
    cap = floor_to(visible * participation, step)
    q = min(remaining, cap)
    return q if q > 0 else ZERO


def eligibility(quote: QuoteView, ready_us: int, max_age_us: int) -> str | None:
    """None when ``quote`` may fill an order that became ready at ``ready_us``; else the reason it may not."""
    if quote.recv_us < ready_us:
        return "received_before_order_ready"
    if quote.exchange_us is not None and quote.exchange_us < ready_us:
        return "exchange_event_before_order_ready"
    if quote.recv_us > ready_us + max_age_us:
        return "observation_too_long_after_ready"
    return None


@dataclass(frozen=True)
class CostGate:
    cost_fraction: Decimal
    net_target: Decimal
    net_risk: Decimal
    ratio: Decimal | None
    ok: bool
    reason: str | None


@exact
def cost_gate(
    p: Decimal, d: Decimal, *, buy_fee: Decimal, sell_fee: Decimal, spread: Decimal, slippage: Decimal,
    min_net_target_fraction: Decimal, min_reward_risk: Decimal, target_mult: Decimal,
) -> CostGate:
    c = buy_fee + sell_fee + spread + 2 * slippage
    net_target = target_mult * d - p * c
    net_risk = d + p * c
    if net_risk <= 0:
        return CostGate(c, net_target, net_risk, None, False, "cost_gate_nonpositive_denominator")
    ratio = net_target / net_risk
    if net_target < min_net_target_fraction * p:
        return CostGate(c, net_target, net_risk, ratio, False, "cost_gate_net_target_cushion")
    if ratio < min_reward_risk:
        return CostGate(c, net_target, net_risk, ratio, False, "cost_gate_reward_risk")
    return CostGate(c, net_target, net_risk, ratio, True, None)
