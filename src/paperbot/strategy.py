"""B20-T5-v1: finalized 1m breakout filtered by a rising finalized 5m EMA20.

Frozen conventions (PROJECT_PLAN.md section C):

* 1m bars are UTC-aligned ``[start, start+60s)``; the canonical bar end is the
  exclusive interval end.
* ``H_i = max(high[i-20:i])`` excludes bar i. A signal needs ``close_i > H_i``
  and ``close_(i-1) <= H_(i-1)`` (a fresh crossing).
* 5m candles are built only from five unique, contiguous, finalized 1m bars of
  one UTC-aligned 5m interval. Partial groups are discarded.
* EMA20 on 5m closes: seed = mean of the first 20 closes, then alpha = 2/21.
* ATR14 on 1m bars (Wilder): TR = max(high-low, |high-prev_close|,
  |low-prev_close|). A TR needs a previous close inside the current contiguous
  run, so the first bar of a run yields no TR. Seed = mean of the first 14 TRs,
  then ATR = (13*ATR + TR) / 14.
* Warm-up: 50 completed 5m candles (250 contiguous 1m bars) in the current run.
* A gap (missing minute) resets every indicator; warm-up restarts. Missing
  minutes are never forward-filled.

This module is pure: no I/O, no clock, no randomness. Nothing here is a
validated edge; every parameter is a hypothesis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .money import exact
from .timeutil import FIVE_MINUTES_US, MINUTE_US

STRATEGY_VERSION = "B20-T5-v1"
BREAKOUT_LOOKBACK = 20
ATR_PERIOD = 14
EMA_PERIOD = 20
WARMUP_5M = 50
STOP_ATR_MULT = Decimal(2)
TARGET_STOP_MULT = Decimal(3)
TIMEOUT_US = 10 * MINUTE_US
COOLDOWN_BARS = 3
MIN_NET_TARGET_FRACTION = Decimal("0.001")
MIN_NET_REWARD_RISK = Decimal(1)


@dataclass
class Bar:
    start_us: int
    end_us: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    recv_us: int


@dataclass
class FiveMin:
    start_us: int
    end_us: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    available_us: int  # receipt time of the final constituent 1m bar
    ema: Decimal | None
    prev_ema: Decimal | None
    index: int  # 1-based count of completed 5m candles in the current run


@dataclass
class StrategyState:
    last_start_us: int | None = None
    run_bars: int = 0
    window: list[Bar] = field(default_factory=list)  # last 21 bars of the run
    prev_close: Decimal | None = None
    tr_seed: list[Decimal] = field(default_factory=list)
    atr: Decimal | None = None
    group: list[Bar] = field(default_factory=list)
    ema_seed: list[Decimal] = field(default_factory=list)
    ema: Decimal | None = None
    five_count: int = 0
    last_five: FiveMin | None = None


@dataclass(frozen=True)
class BarResult:
    bar: Bar
    gap_bars: int  # missing minutes detected before this bar (0 = contiguous)
    reset: bool
    h: Decimal | None
    prev_h: Decimal | None
    prev_close: Decimal | None
    crossing: bool
    atr: Decimal | None
    new_five: FiveMin | None
    last_five: FiveMin | None
    trend_ok: bool
    warm: bool


def ema_alpha() -> Decimal:
    return Decimal(2) / Decimal(EMA_PERIOD + 1)


def true_range(high: Decimal, low: Decimal, prev_close: Decimal) -> Decimal:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def trend_passes(five: FiveMin | None) -> bool:
    return (
        five is not None
        and five.ema is not None
        and five.prev_ema is not None
        and five.close > five.ema
        and five.ema > five.prev_ema
    )


def trend_invalidated(five: FiveMin | None) -> bool:
    return five is not None and five.ema is not None and five.close <= five.ema


@exact
def update(state: StrategyState, bar: Bar) -> BarResult:
    """Fold one finalized, validated, strictly-later 1m bar into ``state``."""
    gap_bars = 0
    reset = False
    if state.last_start_us is not None:
        expected = state.last_start_us + MINUTE_US
        if bar.start_us < expected:
            raise ValueError("bars must be strictly increasing; the engine rejects duplicates/out-of-order bars")
        if bar.start_us != expected:
            gap_bars = (bar.start_us - expected) // MINUTE_US
            reset = True
            fresh = StrategyState()
            state.__dict__.update(fresh.__dict__)

    window = state.window
    h = max(b.high for b in window[-BREAKOUT_LOOKBACK:]) if len(window) >= BREAKOUT_LOOKBACK else None
    prev_h = (
        max(b.high for b in window[-BREAKOUT_LOOKBACK - 1 : -1]) if len(window) >= BREAKOUT_LOOKBACK + 1 else None
    )
    prev_close = window[-1].close if window else None
    crossing = (
        h is not None and prev_h is not None and prev_close is not None and bar.close > h and prev_close <= prev_h
    )

    # ATR through bar i (inclusive).
    if state.prev_close is not None:
        tr = true_range(bar.high, bar.low, state.prev_close)
        if state.atr is None:
            state.tr_seed.append(tr)
            if len(state.tr_seed) == ATR_PERIOD:
                state.atr = sum(state.tr_seed, Decimal(0)) / Decimal(ATR_PERIOD)
                state.tr_seed = []
        else:
            state.atr = (Decimal(ATR_PERIOD - 1) * state.atr + tr) / Decimal(ATR_PERIOD)
    state.prev_close = bar.close

    window.append(bar)
    if len(window) > BREAKOUT_LOOKBACK + 1:
        del window[: len(window) - (BREAKOUT_LOOKBACK + 1)]
    state.run_bars += 1
    state.last_start_us = bar.start_us

    # 5m aggregation from complete, contiguous, aligned groups only.
    new_five = None
    if bar.start_us % FIVE_MINUTES_US == 0:
        state.group = [bar]
    elif state.group and state.group[-1].start_us + MINUTE_US == bar.start_us:
        state.group.append(bar)
    else:
        state.group = []
    if len(state.group) == 5:
        g = state.group
        close = g[-1].close
        prev_ema = state.ema
        if state.ema is None:
            state.ema_seed.append(close)
            if len(state.ema_seed) == EMA_PERIOD:
                state.ema = sum(state.ema_seed, Decimal(0)) / Decimal(EMA_PERIOD)
                state.ema_seed = []
        else:
            state.ema = state.ema + ema_alpha() * (close - state.ema)
        state.five_count += 1
        new_five = FiveMin(
            start_us=g[0].start_us,
            end_us=g[-1].end_us,
            open=g[0].open,
            high=max(b.high for b in g),
            low=min(b.low for b in g),
            close=close,
            available_us=g[-1].recv_us,
            ema=state.ema,
            prev_ema=prev_ema,
            index=state.five_count,
        )
        state.last_five = new_five
        state.group = []

    warm = (
        state.five_count >= WARMUP_5M and state.atr is not None and h is not None and prev_h is not None
    )
    return BarResult(
        bar=bar,
        gap_bars=gap_bars,
        reset=reset,
        h=h,
        prev_h=prev_h,
        prev_close=prev_close,
        crossing=crossing,
        atr=state.atr,
        new_five=new_five,
        last_five=state.last_five,
        trend_ok=trend_passes(state.last_five),
        warm=warm,
    )
