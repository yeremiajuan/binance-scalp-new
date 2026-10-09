"""Generate fixtures/synthetic_demo.jsonl: a deterministic SYNTHETIC replay fixture.

The price path is hand-designed to exercise the Phase 1 mechanics end to end:
warm-up, fresh crossings, a skipped wide-spread signal, a full entry with a
target exit, a partial entry with thin-bid exit retries and base-fee dust, an
adverse quote jump that cancels an entry at its fill observation, a timeout
exit, a quote outage while the market gaps through a stop, a crossing of
00:00 Asia/Jakarta, unfinished/duplicate inputs, and an open position at the
end of the file. It is engineering evidence only, not market data, and its
P&L means nothing about any edge.

Usage: python scripts/make_demo_fixture.py [output_path]
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

from paperbot.money import dtext
from paperbot.synthetic import RANGE_OFFSETS, Scenario
from paperbot.timeutil import MINUTE_US, US_PER_MS, iso

D = Decimal
QUOTE_OFFSETS_MS = [100 + 2000 * k for k in range(30)]


class Path_:
    """Per-minute path points (seconds, mid) plus quote overrides."""

    def __init__(self, points, *, spread=None, ask_qty=None, bid_qty=None, drop=None, spike=None):
        self.points = [(s, D(p)) for s, p in points]
        self.spread = spread or (lambda ms: D(1))
        self.ask_qty = ask_qty or (lambda ms: D("0.5"))
        self.bid_qty = bid_qty or (lambda ms: D("0.5"))
        self.drop = drop or (lambda ms: False)
        self.spike = spike or (lambda ms: D(0))

    def mid(self, ms: int) -> Decimal:
        s = D(ms) / 1000
        pts = self.points
        for (s0, p0), (s1, p1) in zip(pts, pts[1:], strict=False):
            if s0 <= s <= s1:
                return (p0 + (p1 - p0) * (s - s0) / (s1 - s0)).quantize(D("0.01"))
        return pts[-1][1]

    @property
    def ohlc(self):
        ps = [p for _, p in self.points]
        return ps[0], max(ps), min(ps), ps[-1]


def range_path(prev_close, level, j, **kw):
    o, c = D(prev_close), D(level) + RANGE_OFFSETS[j % len(RANGE_OFFSETS)]
    return Path_([(0, o), (20, max(o, c) + 30), (40, min(o, c) - 40), (60, c)], **kw)


def breakout_path(prev_close, level, **kw):
    o, c = D(prev_close), D(level)
    return Path_([(0, o), (10, o - 40), (50, c + 30), (60, c)], **kw)


def ramp_path(prev_close, to, **kw):
    o, c = D(prev_close), D(to)
    return Path_([(0, o), (30, (o + c) / 2), (60, c)], **kw)


class Builder:
    def __init__(self, start: str):
        self.sc = Scenario(start, "SYNTHETIC Phase 1 demo fixture (scripts/make_demo_fixture.py)")
        self.t = self.sc.t0
        self.close = D(60000) - 20
        self.level = D(60000)
        self.events: list[tuple[int, int, dict]] = []
        self.n = 0
        self.minute_index = 0

    def _ev(self, recv_us: int, obj: dict) -> None:
        self.n += 1
        self.events.append((recv_us, self.n, obj))

    def minute(self, path: Path_) -> None:
        start = self.t
        o, h, low, c = path.ohlc
        for ms in QUOTE_OFFSETS_MS:
            if path.drop(ms):
                continue
            mid = path.mid(ms) + path.spike(ms)
            half = path.spread(ms)
            self.n += 1
            self._ev(start + ms * US_PER_MS, {
                "type": "quote", "id": f"q-{self.n:06d}", "recv": iso(start + ms * US_PER_MS),
                "bid": dtext(mid - half), "ask": dtext(mid + half),
                "bid_qty": dtext(path.bid_qty(ms)), "ask_qty": dtext(path.ask_qty(ms)),
            })
        self._ev(start + 150 * US_PER_MS, {
            "type": "reference_price", "id": f"r-{self.n:06d}", "recv": iso(start + 150 * US_PER_MS),
            "avg_price": dtext(o), "mins": 5, "synthetic": True,
        })
        candle = {
            "type": "candle", "id": f"c-{start // MINUTE_US}", "interval": "1m", "start": iso(start),
            "recv": iso(start + MINUTE_US + 500 * US_PER_MS), "open": dtext(o), "high": dtext(h),
            "low": dtext(low), "close": dtext(c), "volume": "1", "final": True,
        }
        if self.minute_index % 30 == 7:  # an unfinished kline update, as streams deliver: must be rejected
            m = path.mid(30000)
            self._ev(start + 30_000 * US_PER_MS, dict(
                candle, id=f"c-{start // MINUTE_US}-partial", final=False, recv=iso(start + 30_000 * US_PER_MS),
                close=dtext(m), high=dtext(max(o, m)), low=dtext(min(o, m))))
        self._ev(start + MINUTE_US + 500 * US_PER_MS, candle)
        self.close = c
        self.t += MINUTE_US
        self.minute_index += 1

    def range(self, n: int, start_j: int = 0) -> None:
        for j in range(start_j, start_j + n):
            self.minute(range_path(self.close, self.level, j))

    def breakout(self, **kw) -> None:
        self.level += 150
        self.minute(breakout_path(self.close, self.level, **kw))

    def segment(self) -> None:
        self.breakout()
        self.range(24)

    def finish(self, duplicate_after: int = 2000) -> Scenario:
        # a closing quote after the last candle, so the open inventory is valued at a fresh mark
        self._ev(self.t + 600 * US_PER_MS, {
            "type": "quote", "id": "q-final", "recv": iso(self.t + 600 * US_PER_MS), "bid": dtext(self.close - 1),
            "ask": dtext(self.close + 1), "bid_qty": "0.5", "ask_qty": "0.5"})
        self.events.sort(key=lambda e: (e[0], e[1]))
        objs = [o for _, _, o in self.events]
        dup = dict(objs[duplicate_after])  # an exact redelivery of an already-processed event
        objs.insert(duplicate_after + 3, dup)
        self.sc.objs = objs
        return self.sc


def build() -> Scenario:
    b = Builder("2026-10-01T12:00:00Z")
    b.close = D(60000) - 20
    b.level = D(60000) - 150  # first breakout() lifts it to 60000
    for _ in range(10):  # 250 bars of warm-up = 50 completed 5m candles
        b.segment()

    # A: full entry, ramp to target, full exit.
    b.breakout()
    for k in range(1, 5):
        b.minute(ramp_path(b.close, b.level + 175 * k))
    b.level += 700
    b.range(20)

    # B: wide spread around the decision -> skipped "spread".
    def wide(ms):
        return D(20) if ms >= 50_000 else D(1)

    b.breakout(spread=wide)
    b.minute(range_path(b.close, b.level, 0, spread=lambda ms: D(20) if ms < 3000 else D(1)))
    b.range(23, start_j=1)

    # C: partial entry (thin ask), then target with thin bids -> several exit IOCs and base-fee dust.
    b.breakout()  # 17:00Z breakout bar -> 00:00 Asia/Jakarta day rollover happens inside this minute
    # the planning quote (+0.1 s) shows normal size; the first eligible observation (+2.1 s) is thin
    b.minute(ramp_path(b.close, b.level + 300, ask_qty=lambda ms: D("0.02") if 1000 <= ms < 5000 else D("0.5")))
    b.minute(ramp_path(b.close, b.level + 700, bid_qty=lambda ms: D("0.01")))
    b.level += 700
    b.range(22)

    # D: adverse quote jump at the first eligible observation -> entry canceled at fill time.
    b.breakout()
    b.minute(range_path(b.close, b.level, 0, spike=lambda ms: D(45) if 1000 <= ms < 3000 else D(0)))
    b.range(23, start_j=1)

    # F: entry, then flat -> ten-minute timeout exit.
    b.breakout()
    b.range(24)

    # E: entry, quote outage while the path gaps through the stop; exit only on later quotes.
    b.breakout()
    b.minute(Path_([(0, b.close), (10, b.close - 10), (40, b.close - 420), (60, b.close - 430)],
                   drop=lambda ms: ms >= 12_000))
    b.minute(Path_([(0, b.close), (60, b.close - 10)], drop=lambda ms: ms < 20_000))
    b.level -= 440
    b.range(4)
    for k in range(1, 6):  # rebound
        b.minute(ramp_path(b.close, b.level + 120 * k))
    b.level += 600
    b.range(13)

    # Final: a breakout whose position is still open when the file ends.
    b.range(10, start_j=10)
    b.breakout()
    b.range(3)
    return b.finish()


def main() -> None:
    default = Path(__file__).resolve().parents[1] / "fixtures" / "synthetic_demo.jsonl"
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else default
    sc = build()
    sc.write(out)
    print(f"wrote {out} ({len(sc.objs)} events)")


if __name__ == "__main__":
    main()
