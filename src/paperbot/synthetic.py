"""Deterministic SYNTHETIC scenario builders for tests and the demo fixture.

These produce engineering fixtures only. Price paths are hand-designed so the
strategy's mechanics (warm-up, fresh crossings, guards, fills, exits) are
exercised. They are not market data, not tuned to profit and not evidence of
any edge.

Staircase pattern (deterministic): segments of 25 one-minute bars, each a
5-minute-aligned block. Bar 0 of a segment is a breakout bar that closes at
the new level ``L_k = level0 + step*k``; bars 1..24 range around ``L_k`` with
fixed offsets so they never make a fresh crossing. After 10 segments (250 bars
= 50 five-minute candles) the next breakout bar is the first warm signal.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .events import RawEvent, parse_lines
from .money import dtext
from .timeutil import MINUTE_US, US_PER_MS, iso, parse_ts

RANGE_OFFSETS = [-20, 0, 15, -10, 25, 5, -15, 10, 20, -5]
SEGMENT_BARS = 25
T0 = "2026-10-01T12:00:00Z"


def D(x) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


@dataclass
class BarSpec:
    start_us: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    @property
    def end_us(self) -> int:
        return self.start_us + MINUTE_US


class Scenario:
    def __init__(self, start: str = T0, description: str = "synthetic scenario"):
        self.t0 = parse_ts(start)
        self.description = description
        self.objs: list[dict] = []
        self._n = 0

    def _id(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}-{self._n:06d}"

    def add(self, obj: dict) -> dict:
        self.objs.append(obj)
        return obj

    def candle(self, bar: BarSpec, *, recv_delay_ms: int = 500, final: bool = True, eid: str | None = None,
               start_override: str | None = None, end: str | None = None) -> dict:
        obj = {
            "type": "candle", "id": eid or self._id("c"), "interval": "1m",
            "start": start_override or iso(bar.start_us), "recv": iso(bar.end_us + recv_delay_ms * US_PER_MS),
            "open": dtext(bar.open), "high": dtext(bar.high), "low": dtext(bar.low), "close": dtext(bar.close),
            "volume": "1", "final": final,
        }
        if end is not None:
            obj["end"] = end
        return self.add(obj)

    def quote(self, recv_us: int, bid, ask, *, bid_qty="0.5", ask_qty="0.5", exchange_us: int | None = None,
              eid: str | None = None) -> dict:
        obj = {"type": "quote", "id": eid or self._id("q"), "recv": iso(recv_us), "bid": dtext(D(bid)),
               "ask": dtext(D(ask)), "bid_qty": dtext(D(bid_qty)), "ask_qty": dtext(D(ask_qty))}
        if exchange_us is not None:
            obj["exchange_time"] = iso(exchange_us)
        return self.add(obj)

    def mid_quote(self, recv_us: int, mid, *, half_spread="1", **kw) -> dict:
        m, h = D(mid), D(half_spread)
        return self.quote(recv_us, m - h, m + h, **kw)

    def reference(self, recv_us: int, price, mins: int = 5) -> dict:
        return self.add({"type": "reference_price", "id": self._id("r"), "recv": iso(recv_us),
                         "avg_price": dtext(D(price)), "mins": mins, "synthetic": True})

    def heartbeat(self, recv_us: int) -> dict:
        return self.add({"type": "heartbeat", "id": self._id("h"), "recv": iso(recv_us)})

    def lines(self) -> list[str]:
        header = {"type": "header", "evidence": "SYNTHETIC", "symbol": "BTCUSDT", "description": self.description,
                  "generator": "paperbot.synthetic"}
        return [json.dumps(header, sort_keys=True)] + [json.dumps(o, sort_keys=True) for o in self.objs]

    def text(self) -> str:
        return "\n".join(self.lines()) + "\n"

    def write(self, path: str | Path) -> Path:
        p = Path(path)
        p.write_text(self.text(), encoding="utf-8", newline="\n")
        return p

    def raw_events(self) -> list[RawEvent]:
        return parse_lines(self.lines())[1]


def staircase(start_us: int, segments: int, *, level0="60000", step="150") -> list[BarSpec]:
    """Bars for ``segments`` staircase segments starting at a 5-minute-aligned ``start_us``."""
    assert start_us % (5 * MINUTE_US) == 0
    bars: list[BarSpec] = []
    close = D(level0) - 20
    t = start_us
    for k in range(segments):
        level = D(level0) + D(step) * k
        o, c = close, level
        bars.append(BarSpec(t, o, c + 30, o - 40, c))
        close, t = c, t + MINUTE_US
        for j in range(1, SEGMENT_BARS):
            o, c = close, level + RANGE_OFFSETS[(j - 1) % len(RANGE_OFFSETS)]
            bars.append(BarSpec(t, o, max(o, c) + 30, min(o, c) - 40, c))
            close, t = c, t + MINUTE_US
    return bars


def range_bars(start_us: int, n: int, level, prev_close) -> list[BarSpec]:
    """``n`` range bars around ``level`` (no fresh crossing), continuing from ``prev_close``."""
    bars, close, t = [], D(prev_close), start_us
    for j in range(n):
        o, c = close, D(level) + RANGE_OFFSETS[j % len(RANGE_OFFSETS)]
        bars.append(BarSpec(t, o, max(o, c) + 30, min(o, c) - 40, c))
        close, t = c, t + MINUTE_US
    return bars


def keepalive(sc: Scenario, t_from_us: int, t_to_us: int, mid, *, every_ms: int = 1500, **kw) -> None:
    """Fresh quotes at ``mid`` every ``every_ms`` in ``(t_from_us, t_to_us)``: continuous, unremarkable data."""
    t = t_from_us + every_ms * US_PER_MS
    while t < t_to_us:
        sc.mid_quote(t, mid, **kw)
        t += every_ms * US_PER_MS


def emit_edge(sc: Scenario, bars: list[BarSpec], *, offsets_ms: tuple[int, ...] = (200,), qty="0.5",
              reference: bool = False, every_ms: int | None = None) -> None:
    """Each bar plus quotes at the bar's close, received ``offsets_ms`` after the bar end.

    The first quote precedes the candle's receipt (500 ms), so it is the fresh planning quote at decision time.
    Quotes received after the candle belong to the next minute's flow.
    """
    for b in bars:
        if every_ms is not None:  # keep quotes fresh through the bar (at its close, which lies in [low, high])
            keepalive(sc, b.start_us + max([o for o in offsets_ms if o >= 500], default=0) * US_PER_MS,
                      b.end_us, b.close, every_ms=every_ms, bid_qty=qty, ask_qty=qty)
        early = [o for o in offsets_ms if o < 500]
        late = [o for o in offsets_ms if o >= 500]
        for o in early:
            sc.mid_quote(b.end_us + o * US_PER_MS, b.close, bid_qty=qty, ask_qty=qty)
        if reference:
            sc.reference(b.end_us + (max(early, default=0) + 50) * US_PER_MS, b.close)
        sc.candle(b)
        for o in late:
            sc.mid_quote(b.end_us + o * US_PER_MS, b.close, bid_qty=qty, ask_qty=qty)


def warm_scenario(segments: int = 10, *, start: str = T0, reference: bool = False,
                  description: str = "staircase warm-up") -> tuple[Scenario, list[BarSpec]]:
    sc = Scenario(start, description)
    bars = staircase(sc.t0, segments)
    emit_edge(sc, bars, reference=reference)
    return sc, bars


def breakout_bar(prev: BarSpec, level) -> BarSpec:
    o, c = prev.close, D(level)
    return BarSpec(prev.end_us, o, c + 30, o - 40, c)
