"""Causal IOC fills: latency, eligibility, protection limits, liquidity caps, entry/exit lifecycle."""

from __future__ import annotations

import json
import sqlite3

import pytest
from conftest import D, report, rows, write_config
from scenarios import BREAKOUT_LEVEL, MS, entry_scenario, follow_range, run

from paperbot.money import ceil_to, floor_to
from paperbot.synthetic import breakout_bar, range_bars, staircase, warm_scenario

TICK = D("0.01")


def entry_order(db):
    (o,) = rows(db, "SELECT * FROM orders WHERE purpose = 'entry'")
    return o


def test_full_fill_needs_a_later_quote_received_after_latency(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    early = sc.mid_quote(bb.end_us + 600 * MS, 61502)  # after submission (500) but before readiness (750)
    fillq = sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg_path)

    (cand,) = [c for c in rows(db, "SELECT * FROM candidates") if c["status"] == "submitted"]
    o = entry_order(db)
    (f,) = rows(db, "SELECT * FROM fills")
    decision = bb.end_us + 500 * MS
    assert cand["decision_us"] == decision == o["submitted_us"] == o["signal_us"]
    assert o["ready_us"] == decision + 250 * MS
    assert o["reference_quote_id"] != f["quote_event_id"]  # quote cached at submission never fills
    assert f["quote_event_id"] == fillq["id"] != early["id"]
    assert f["quote_recv_us"] >= o["ready_us"] and f["fill_us"] == bb.end_us + 900 * MS
    assert o["ineligible_quotes_seen"] == 1
    # cross the ask + 1bp adverse slippage, rounded up; within the limit (ask*1.0005 rounded down)
    assert D(f["price"]) == ceil_to(D(61503) * D("1.0001"), TICK) == D("61509.16")
    assert D(o["limit_price"]) == floor_to(D(61501) * D("1.0005"), TICK) == D("61531.75")
    assert D(f["price"]) != D(bb.close)  # the signal close never supplies the fill
    assert o["status"] == "filled" and f["qty"] == o["qty"]
    # exchange event time was not supplied and is not invented
    assert f["quote_exchange_us"] is None
    r = report(db)
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]


def test_position_size_is_the_binding_cap_floored_to_step(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg_path)
    (c,) = [c for c in report(db)["candidates"]["rows"] if c["status"] == "submitted"]

    d = json.loads(c["detail"])
    caps = {k: D(v) for k, v in d["size_caps"].items()}
    assert D(c["qty"]) == floor_to(min(caps.values()), D("0.00001"))
    # modeled risk budget: qty * (d + pC) <= 0.1% of equity
    assert D(c["qty"]) * D(d["net_risk"]) <= D("1.0")
    assert d["binding_cap"] == "risk"


def test_exchange_event_time_before_readiness_is_ineligible(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    ready = bb.end_us + 750 * MS
    late_delivery = sc.mid_quote(bb.end_us + 800 * MS, 61502, exchange_us=ready - 100 * MS)
    good = sc.mid_quote(bb.end_us + 1000 * MS, 61502, exchange_us=ready + 10 * MS)
    db = run(tmp_path, sc, cfg_path)
    (f,) = rows(db, "SELECT * FROM fills")
    assert f["quote_event_id"] == good["id"] != late_delivery["id"]
    assert f["quote_exchange_us"] == ready + 10 * MS


def test_no_quote_within_window_cancels_entry_and_releases_reservation(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    ready = bb.end_us + 750 * MS
    # keeps quotes fresh but is ineligible (exchange event before readiness, delivered late)
    sc.mid_quote(bb.end_us + 1500 * MS, 61502, exchange_us=ready - 1)
    sc.mid_quote(bb.end_us + 3000 * MS, 61502)  # > ready + 2 s: too long after readiness to represent the IOC
    db = run(tmp_path, sc, cfg_path)
    o = entry_order(db)
    assert o["status"] == "canceled" and o["outcome_reason"] == "no_eligible_quote_after_ready"
    assert rows(db, "SELECT * FROM fills") == []
    (u,) = rows(db, "SELECT * FROM balances WHERE asset = 'USDT'")
    assert (u["free"], u["locked"]) == ("1000", "0")


def test_signal_expiry_cancels_before_any_fill(tmp_path):
    cfg = write_config(tmp_path, {"execution.latency_ms": 6000})
    sc, bb = entry_scenario()
    for k in range(1, 5):  # fresh but before readiness (submitted +500 ms, ready +6500 ms)
        sc.mid_quote(bb.end_us + (500 + 1500 * k) * MS, 61502)
    sc.mid_quote(bb.end_us + 6600 * MS, 61502)  # eligible by latency, but the signal is > 5 s old
    db = run(tmp_path, sc, cfg)
    o = entry_order(db)
    assert o["status"] == "canceled" and o["outcome_reason"] == "signal_expired"
    assert rows(db, "SELECT * FROM fills") == []


def test_adverse_gap_beyond_protection_limit_is_a_zero_fill(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    # ask jumps from 61501 to 61541; ask*1.0001 rounded up = 61547.16 > limit 61531.75
    sc.quote(bb.end_us + 900 * MS, 61539, 61541)
    sc.mid_quote(bb.end_us + 1200 * MS, 61502)  # later good quote must not resurrect the IOC
    cfg = write_config(tmp_path, {"execution.max_entry_drift_atr": "1"})
    db = run(tmp_path, sc, cfg)
    o = entry_order(db)
    assert o["status"] == "zero" and o["outcome_reason"] == "price_protection"
    assert rows(db, "SELECT * FROM fills") == []
    assert report(db)["balances"]["USDT"]["free"] == D(1000)


def test_guard_rechecked_at_fill_observation(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.quote(bb.end_us + 900 * MS, 61480, 61520)  # spread 40/61500 = 6.5 bps > 5 bps at fill time
    db = run(tmp_path, sc, cfg_path)
    o = entry_order(db)
    assert o["status"] == "canceled" and o["outcome_reason"] == "guard_failed_at_fill:spread"


def test_partial_entry_cancels_remainder_and_never_retries(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502, ask_qty="0.02", bid_qty="0.02")  # cap 0.002 BTC
    follow_range(sc, bb, 3, BREAKOUT_LEVEL, offsets_ms=(200, 700, 1200))
    db = run(tmp_path, sc, cfg_path)
    o = entry_order(db)
    (f,) = rows(db, "SELECT * FROM fills WHERE side = 'BUY'")
    assert D(o["qty"]) > D("0.002") and D(f["qty"]) == D("0.002")
    assert o["status"] == "partial" and o["outcome_reason"] == "ioc_remainder_canceled"
    assert len(rows(db, "SELECT * FROM orders WHERE side = 'BUY'")) == 1  # no retry of the remainder
    (pos,) = rows(db, "SELECT * FROM positions")
    assert D(pos["entry_qty"]) == D("0.002") * D("0.999")  # base-asset fee reduces acquired BTC
    r = report(db)
    assert r["balances"]["USDT"]["locked"] == 0  # reservation remainder released
    assert r["reconciliation"]["ok"]


def test_zero_visible_liquidity_is_a_zero_fill(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502, ask_qty="0.00009")  # 10% = 0.000009 floors to 0
    db = run(tmp_path, sc, cfg_path)
    o = entry_order(db)
    assert o["status"] == "zero" and o["outcome_reason"] == "insufficient_visible_liquidity"


def test_candle_touch_without_executable_quote_never_fills(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    # Next candle's low pierces the stop (~61303) but every executable bid stays above it.
    deep = range_bars(bb.end_us, 1, BREAKOUT_LEVEL, bb.close)[0]
    deep.low = D(61000)
    sc.mid_quote(bb.end_us + 30_000 * MS, 61450)
    sc.mid_quote(deep.end_us + 200 * MS, 61480)
    sc.candle(deep)
    db = run(tmp_path, sc, cfg_path)
    assert rows(db, "SELECT * FROM orders WHERE side = 'SELL'") == []
    (pos,) = rows(db, "SELECT * FROM positions")
    assert pos["status"] == "open" and D(deep.low) < D(pos["stop_price"])


def test_stale_quotes_preserve_inventory_and_gap_exit_fills_only_on_later_quotes(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    # Outage: no quotes for ~40 s while the market gaps down through the stop.
    gap = range_bars(bb.end_us, 1, 61000, bb.close)[0]
    sc.candle(gap)  # candle shows the drop; it cannot trigger or fill a stop
    sc.mid_quote(gap.end_us + 1000 * MS, 61000)  # first fresh quote after the outage: bid 60999 <= stop
    sc.mid_quote(gap.end_us + 1500 * MS, 60950)  # eligible exit observation (ready at +1250)
    db = run(tmp_path, sc, cfg_path)
    (pos,) = rows(db, "SELECT * FROM positions")
    (sell,) = rows(db, "SELECT * FROM orders WHERE side = 'SELL'")
    (sf,) = rows(db, "SELECT * FROM fills WHERE side = 'SELL'")
    assert pos["exit_reason"] == "stop"
    assert sell["submitted_us"] == gap.end_us + 1000 * MS and sf["fill_us"] == gap.end_us + 1500 * MS
    assert D(sf["price"]) == floor_to(D(60949) * D("0.9999"), TICK)
    assert D(sf["price"]) < D(pos["stop_price"])  # no fill at the stop price: overshoot is visible
    r = report(db)
    stale = [h for h in r["health"]["events"] if h["kind"] == "quotes_stale"]
    assert any('"inventory_exposed": true' in h["detail"] for h in stale)
    assert r["health"]["unprotected_total_us"] > 30_000_000
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]


def test_partial_exits_retry_with_new_orders_later_quotes_and_updated_limits(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    t = bb.end_us + 30_000 * MS
    sc.mid_quote(t, 62200, bid_qty="0.01")  # target hit -> exit order 1 submitted here
    quotes = [sc.mid_quote(t + k * 400 * MS, 62200 - 3 * k, bid_qty="0.01") for k in range(1, 6)]
    db = run(tmp_path, sc, cfg_path)
    sells = rows(db, "SELECT * FROM orders WHERE side = 'SELL' ORDER BY attempt")
    fills = rows(db, "SELECT * FROM fills WHERE side = 'SELL' ORDER BY fill_us")
    assert len(sells) >= 3 and all(o["exit_reason"] == "target" for o in sells)
    assert len({o["order_id"] for o in sells}) == len(sells)
    for o in sells[:-1]:
        assert o["status"] == "partial" and D(o["filled_qty"]) == D("0.001")  # 10% of 0.01 visible
    # each retry is submitted on the observation that closed the previous IOC, referencing it,
    # and fills only on a later observation after latency, at a newly computed limit
    for prev, nxt in zip(sells, sells[1:], strict=False):
        assert nxt["submitted_us"] == prev["closed_us"] and nxt["reference_quote_id"] == prev["outcome_quote_id"]
        assert nxt["ready_us"] == nxt["submitted_us"] + 250 * MS
    assert len({o["limit_price"] for o in sells}) == len(sells)
    assert len({f["quote_event_id"] for f in fills}) == len(fills)  # each quote consumed at most once
    assert {f["quote_event_id"] for f in fills} <= {q["id"] for q in quotes}
    (pos,) = rows(db, "SELECT * FROM positions")
    assert pos["status"] == "closed"
    r = report(db)
    assert r["inventory"]["dust_btc"] > 0 and r["inventory"]["dust_btc"] < D("0.00001")
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]


def test_quote_consumption_is_unique_in_storage(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg_path)
    (f,) = rows(db, "SELECT * FROM fills")
    conn = sqlite3.connect(db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO fills SELECT 'x', order_id, quote_event_id, seq, side, qty, price, gross_notional, "
                     "fee_asset, fee_amount, fee_usdt, usdt_delta, btc_delta, basis_gross, basis_fee, gross_pnl, "
                     "net_pnl, bid, ask, bid_qty, ask_qty, quote_recv_us, quote_exchange_us, signal_us, "
                     "submitted_us, ready_us, fill_us, committed_us FROM fills")
    conn.close()


def test_timeout_exit_after_ten_minutes_from_first_fill(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    follow_range(sc, bb, 12, BREAKOUT_LEVEL, offsets_ms=(200, 700))
    db = run(tmp_path, sc, cfg_path)
    (pos,) = rows(db, "SELECT * FROM positions")
    (intent,) = rows(db, "SELECT * FROM exit_intents")
    assert intent["reason"] == "timeout"
    assert intent["created_us"] - pos["opened_us"] >= 10 * 60 * 1_000_000
    assert intent["created_us"] - pos["opened_us"] < 11 * 60 * 1_000_000
    assert pos["status"] == "closed"


def test_trend_invalidation_exit_on_finalized_five_minute_close(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    # Contrived on purpose: candles close below the 5m EMA20 while executable bids stay above the stop,
    # isolating the finalized-5m invalidation rule from the quote-driven stop.
    drop = range_bars(bb.end_us, 4, 61100, bb.close)
    for b in drop:
        sc.mid_quote(b.end_us + 200 * MS, 61450)
        sc.candle(b)
        sc.mid_quote(b.end_us + 700 * MS, 61450)
    db = run(tmp_path, sc, cfg_path)
    (intent,) = rows(db, "SELECT * FROM exit_intents")
    assert intent["reason"] == "trend_invalidation"
    five_end = drop[-1].end_us
    assert intent["created_us"] == five_end + 500 * MS  # at receipt of the final 5m constituent


@pytest.mark.parametrize("bars_after, blocked", [(3, True), (4, False)])
def test_cooldown_requires_three_completed_bars_after_exit(tmp_path, cfg_path, bars_after, blocked):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    t = bb.end_us + 20_000 * MS
    sc.mid_quote(t, 62200)  # target -> exit submitted
    sc.mid_quote(t + 400 * MS, 62200)  # exit fills inside the first following bar (which does not count)
    after = range_bars(bb.end_us, bars_after, BREAKOUT_LEVEL, bb.close)
    for b in after:
        sc.mid_quote(b.end_us + 200 * MS, b.close)
        sc.candle(b)
    nxt = breakout_bar(after[-1], 62400)
    sc.mid_quote(nxt.end_us + 200 * MS, nxt.close)
    sc.candle(nxt)
    db = run(tmp_path, sc, cfg_path)
    (exit_fill,) = rows(db, "SELECT * FROM fills WHERE side = 'SELL'")
    assert exit_fill["fill_us"] == t + 400 * MS
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (nxt.start_us,))
    assert ("cooldown" in c["detail"]) is blocked
    if not blocked:
        assert c["status"] == "submitted"


def test_entry_guards_skip_with_explicit_reasons(tmp_path, cfg_path):
    # wide spread at decision
    sc, bb = entry_scenario(spread_half="20")
    db = run(tmp_path, sc, cfg_path, name="spread")
    (c,) = rows(db, "SELECT * FROM candidates WHERE status='skipped' AND skip_reason NOT LIKE 'warmup%'")
    assert c["skip_reason"] == "spread"

    # late finalized candle (received 6 s after its interval end)
    sc, bars = warm_scenario(10)
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.mid_quote(bb.end_us + 5800 * MS, bb.close)
    sc.candle(bb, recv_delay_ms=6000)
    db = run(tmp_path, sc, cfg_path, name="late")
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "late_candle"
    assert any(h["kind"] == "candle_late" for h in report(db)["health"]["events"])

    # no fresh quote at decision (last quote 60 s old)
    sc, bars = warm_scenario(10)
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.candle(bb)
    db = run(tmp_path, sc, cfg_path, name="noquote")
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "no_fresh_quote"

    # adverse drift: ask far above the signal close (drift > 0.25 ATR)
    sc, bars = warm_scenario(10)
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.mid_quote(bb.end_us + 200 * MS, bb.close + 40)
    sc.candle(bb)
    db = run(tmp_path, sc, cfg_path, name="drift")
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "entry_drift"


def test_cost_gate_rejects_low_volatility_relative_to_price(tmp_path):
    # Same absolute ranges at 10x the price: ATR/price ~1.7 bps, far below the ~12 bps the gate needs.
    from paperbot.synthetic import Scenario, emit_edge

    sc = Scenario()
    bars = staircase(sc.t0, 10, level0="600000")
    emit_edge(sc, bars)
    bb = breakout_bar(bars[-1], 600000 + 1500)
    sc.mid_quote(bb.end_us + 200 * MS, bb.close)
    sc.candle(bb)
    cfg = write_config(tmp_path, {"execution.max_entry_drift_atr": "10"})  # isolate the cost gate
    db = run(tmp_path, sc, cfg)
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] in ("cost_gate_reward_risk", "cost_gate_net_target_cushion")
    assert rows(db, "SELECT * FROM orders") == []


def test_quote_asset_buy_fee_mode_end_to_end(tmp_path):
    cfg = write_config(tmp_path, {"fees.buy_fee_asset": "USDT"})
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    sc.mid_quote(bb.end_us + 20_000 * MS, 62200)
    sc.mid_quote(bb.end_us + 20_400 * MS, 62200)
    db = run(tmp_path, sc, cfg)
    buy, sell = rows(db, "SELECT * FROM fills ORDER BY fill_us")
    assert buy["fee_asset"] == "USDT" and D(buy["btc_delta"]) == D(buy["qty"])
    assert D(buy["usdt_delta"]) == -(D(buy["qty"]) * D(buy["price"]) * D("1.001"))
    assert D(sell["qty"]) == D(buy["qty"])  # no base fee -> full quantity sellable, zero dust
    r = report(db)
    assert r["inventory"]["btc_total"] == 0
    assert r["balances"]["USDT"]["free"] == D(1000) + D(buy["usdt_delta"]) + D(sell["usdt_delta"])
    assert r["pnl"]["realized_net"] == r["balances"]["USDT"]["free"] - D(1000)


def test_partial_fragment_below_min_notional_is_valid_and_leaves_unsellable_residual(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    # 10% of 0.0007 visible = 0.00007 BTC (~4.3 USDT < 5 USDT minNotional): the *submitted* order passed the
    # minimum; the fragment is not re-validated as if it were a new order.
    sc.mid_quote(bb.end_us + 900 * MS, 61502, ask_qty="0.0007")
    sc.mid_quote(bb.end_us + 30_000 * MS, 61000)  # stop trigger
    sc.mid_quote(bb.end_us + 30_400 * MS, 61000)
    db = run(tmp_path, sc, cfg_path)
    o = entry_order(db)
    (f,) = rows(db, "SELECT * FROM fills")
    assert D(o["qty"]) * D(o["limit_price"]) >= 5
    assert D(f["qty"]) == D("0.00007") and D(f["qty"]) * D(f["price"]) < 5
    assert o["status"] == "partial"
    (pos,) = rows(db, "SELECT * FROM positions")
    assert pos["status"] == "closed" and pos["exit_reason"] == "stop"
    assert json.loads(pos["close_detail"])["how"] == "residual_unsellable"
    assert rows(db, "SELECT * FROM orders WHERE side = 'SELL'") == []  # no invalid sell was submitted
    r = report(db)
    assert r["inventory"]["btc_total"] == D("0.00007") * D("0.999") == r["inventory"]["dust_btc"]
    assert r["inventory"]["basis_usdt"] == -D(f["usdt_delta"])  # retained with its basis, never deleted
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]
