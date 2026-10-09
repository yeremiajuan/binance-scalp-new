"""Shared SYNTHETIC scenarios for engine tests (engineering fixtures, not market data)."""

from __future__ import annotations

from pathlib import Path

from paperbot.replay import new_run, resume_run
from paperbot.synthetic import BarSpec, Scenario, breakout_bar, emit_edge, keepalive, range_bars, warm_scenario
from paperbot.timeutil import US_PER_MS

MS = US_PER_MS
BREAKOUT_LEVEL = 61500  # level after the 10-segment staircase warm-up (last level 61350 + 150)


def entry_scenario(*, reference: bool = False, planning_qty="0.5", spread_half="1") -> tuple[Scenario, BarSpec]:
    """Warm staircase + one breakout bar. The candle is received at end+500ms (= decision and submission);
    the order becomes ready at end+750ms. The planning quote at end+200ms is cached at submission."""
    sc, bars = warm_scenario(10, reference=reference)
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.mid_quote(bb.end_us + 200 * MS, bb.close, half_spread=spread_half, bid_qty=planning_qty,
                 ask_qty=planning_qty)
    if reference:
        sc.reference(bb.end_us + 250 * MS, bb.close)
    sc.candle(bb)
    return sc, bb


def run(tmp_path: Path, sc: Scenario, cfg: Path, name: str = "state", stop_after: int | None = None,
        fault_hook=None) -> Path:
    inp = sc.write(tmp_path / f"{name}.jsonl")
    db = tmp_path / f"{name}.sqlite"
    new_run(str(cfg), str(inp), str(db), stop_after=stop_after, fault_hook=fault_hook)
    return db


def resume(tmp_path: Path, cfg: Path, name: str = "state", stop_after: int | None = None, fault_hook=None):
    return resume_run(str(cfg), str(tmp_path / f"{name}.jsonl"), str(tmp_path / f"{name}.sqlite"),
                      stop_after=stop_after, fault_hook=fault_hook)


def follow_range(sc: Scenario, prev: BarSpec, n: int, level, **kw) -> list[BarSpec]:
    bars = range_bars(prev.end_us, n, level, prev.close)
    emit_edge(sc, bars, **kw)
    return bars


def rich_scenario() -> Scenario:
    """Two trades: (1) partial entry, target with thin bids -> several exit IOCs, base-fee dust;
    (2) full entry, quote outage while the market gaps through the stop: the outage queues a health exit that
    executes only on fresh quotes after latency."""
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502, ask_qty="0.02", bid_qty="0.02")
    t = bb.end_us + 20_000 * MS
    keepalive(sc, bb.end_us + 900 * MS, t, 61502)
    sc.mid_quote(t, 62200, bid_qty="0.01")
    for k in range(1, 5):
        sc.mid_quote(t + k * 400 * MS, 62200 - 3 * k, bid_qty="0.01")
    after = follow_range(sc, bb, 6, BREAKOUT_LEVEL, offsets_ms=(200, 700))
    nb = breakout_bar(after[-1], 62400)
    sc.mid_quote(nb.end_us + 200 * MS, nb.close)
    sc.candle(nb)
    sc.mid_quote(nb.end_us + 900 * MS, nb.close + 2)
    gap = range_bars(nb.end_us, 1, 61900, nb.close)[0]
    sc.candle(gap)  # outage: no quotes for the whole minute
    sc.mid_quote(gap.end_us + 1000 * MS, 61900)
    sc.mid_quote(gap.end_us + 1500 * MS, 61880)
    follow_range(sc, gap, 3, 61900, offsets_ms=(200, 700))
    return sc
