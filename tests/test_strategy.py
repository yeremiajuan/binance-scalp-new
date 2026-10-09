"""B20-T5-v1 indicator conventions, 5m derivation, crossing rule and causality (pure strategy layer)."""

from __future__ import annotations

import decimal
from decimal import Decimal

from conftest import D

from paperbot.money import CTX
from paperbot.strategy import Bar, StrategyState, trend_passes, update
from paperbot.synthetic import breakout_bar, staircase
from paperbot.timeutil import MINUTE_US, parse_ts

T0 = parse_ts("2026-10-01T00:00:00Z")


def bar(i: int, o, h, low, c, t0: int = T0) -> Bar:
    s = t0 + i * MINUTE_US
    return Bar(s, s + MINUTE_US, D(o), D(h), D(low), D(c), s + MINUTE_US + 500_000)


def feed(bars):
    st = StrategyState()
    return st, [update(st, b) for b in bars]


def test_ema20_seed_and_alpha_hand_calculation():
    # 21 five-minute groups; every 1m bar in group k is flat at 100+k, so 5m close_k = 100+k.
    bars = [bar(i, 100 + i // 5, 100 + i // 5, 100 + i // 5, 100 + i // 5) for i in range(21 * 5)]
    st, res = feed(bars)
    fives = [r.new_five for r in res if r.new_five is not None]
    assert len(fives) == 21
    assert all(f.ema is None for f in fives[:19])
    assert fives[19].ema == D("109.5")  # mean(100..119)
    assert fives[19].prev_ema is None
    # 109.5 + 2/21 * (120 - 109.5) = 110.5 exactly
    assert fives[20].ema == D("110.5")
    assert fives[20].prev_ema == D("109.5")
    assert trend_passes(fives[20])  # 120 > 110.5 and 110.5 > 109.5
    assert not trend_passes(fives[19])  # needs a previous EMA


def test_ema_alpha_is_two_over_twentyone_in_fixed_context():
    bars = [bar(i, 100, 100, 100, 100) for i in range(100)] + [bar(100 + i, 121, 121, 121, 121) for i in range(5)]
    _, res = feed(bars)
    last = [r.new_five for r in res if r.new_five][-1]
    with decimal.localcontext(CTX):
        assert last.ema == D(100) + (D(2) / D(21)) * (D(121) - D(100))
    assert last.ema == D(102)


def test_wilder_atr14_seed_and_recursion_hand_calculation():
    bars = [bar(i, 100, 101, 99, 100) for i in range(15)]  # first bar has no TR; 14 TRs of 2
    bars.append(bar(15, 100, 110, 100, 105))  # TR = max(10, |110-100|, |100-100|) = 10
    bars.append(bar(16, 95, 95, 94, 94))  # gap down: TR = max(1, |95-105|, |94-105|) = 11
    _, res = feed(bars)
    assert res[13].atr is None  # only 13 TRs so far
    assert res[14].atr == D(2)
    with decimal.localcontext(CTX):
        a15 = (D(13) * D(2) + D(10)) / D(14)
        a16 = (D(13) * a15 + D(11)) / D(14)
    assert res[15].atr == a15
    assert res[16].atr == a16


def test_fresh_crossing_excludes_signal_bar_and_requires_previous_close_at_or_below_h():
    base = [bar(i, 100, 101 + (i % 3), 99, 100) for i in range(21)]  # max high in any 20-window = 103
    sig = bar(21, 100, 500, 99, D("103.01"))  # own huge high is excluded from H_i
    again = bar(22, 104, 106, 103, 105)  # close > H but previous close 103.01 > its H (103): not fresh
    _, res = feed(base + [sig, again])
    r = res[21]
    assert r.h == D(103) and r.prev_h == D(103) and r.prev_close == D(100)
    assert r.crossing
    assert res[22].h == D(500) or not res[22].crossing
    assert not res[22].crossing
    # close equal to H is not a breakout
    _, res2 = feed(base + [bar(21, 100, 103, 99, 103)])
    assert not res2[21].crossing


def test_crossing_needs_21_prior_bars():
    bars = [bar(i, 100, 101, 99, 100) for i in range(20)] + [bar(20, 100, 120, 99, 119)]
    _, res = feed(bars)
    assert res[20].h == D(101) and res[20].prev_h is None and not res[20].crossing


def test_five_minute_groups_need_five_aligned_contiguous_members():
    start = T0 + 2 * MINUTE_US  # run starts mid-group at :02
    bars = [bar(i, 100, 101, 99, 100, t0=start) for i in range(8)]  # :02..:09
    _, res = feed(bars)
    fives = [r.new_five for r in res if r.new_five]
    assert len(fives) == 1  # :02-:04 partial group discarded; :05-:09 complete
    assert fives[0].start_us == T0 + 5 * MINUTE_US and fives[0].end_us == T0 + 10 * MINUTE_US
    assert fives[0].available_us == bars[-1].recv_us


def test_gap_resets_indicators_and_never_forward_fills():
    bars = [bar(i, 100, 101, 99, 100) for i in range(30)]
    gapped = bars + [bar(32, 100, 101, 99, 100)]  # minutes 30 and 31 missing
    st, res = feed(gapped)
    r = res[-1]
    assert r.reset and r.gap_bars == 2
    assert r.atr is None and r.h is None and st.five_count == 0 and st.run_bars == 1


def test_warmup_requires_fifty_completed_five_minute_candles():
    bars = staircase(T0, 10)
    st, res = feed([Bar(b.start_us, b.end_us, b.open, b.high, b.low, b.close, b.end_us + 1) for b in bars])
    crossings = [r for r in res if r.crossing]
    assert len(crossings) == 9 and not any(r.warm for r in crossings)
    assert st.five_count == 50
    bb = breakout_bar(bars[-1], 61500)
    r = update(st, Bar(bb.start_us, bb.end_us, bb.open, bb.high, bb.low, bb.close, bb.end_us + 1))
    assert r.crossing and r.warm and r.trend_ok


def test_prefix_invariance_future_bars_cannot_change_earlier_results():
    bars = [Bar(b.start_us, b.end_us, b.open, b.high, b.low, b.close, b.end_us + 1) for b in staircase(T0, 12)]
    _, full = feed(bars)
    for k in (30, 137, 251, 299):
        altered = bars[:k] + [Bar(b.start_us, b.end_us, b.open * 2, b.high * 3, b.low, b.close * 2, b.recv_us)
                              for b in bars[k:]]
        _, res = feed(altered)
        assert res[:k] == full[:k]


def test_decimal_values_are_exact_not_float():
    _, res = feed([bar(i, "100.01", "100.02", "100.00", "100.01") for i in range(16)])
    assert isinstance(res[-1].atr, Decimal)
