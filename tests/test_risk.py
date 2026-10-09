"""Exposure, conservative equity, loss latches, manual kill, midnight/restart persistence and audited reset."""

from __future__ import annotations

import json
import sqlite3

import pytest
from conftest import D, dump_db, report, rows, write_config
from scenarios import BREAKOUT_LEVEL, MS, entry_scenario, follow_range, resume, run

from paperbot.constraints import parse_metadata
from paperbot.ledger import Balances, Pool
from paperbot.reconcile import ReconciliationError
from paperbot.replay import control
from paperbot.risk import value
from paperbot.synthetic import Scenario, breakout_bar, emit_edge, staircase
from paperbot.timeutil import parse_ts


def test_exposure_includes_pending_buy_reservations_and_all_btc_including_dust():
    from conftest import minimal_metadata

    rules = parse_metadata(json.dumps(minimal_metadata()))
    kw = dict(slippage=D("0.0001"), sell_fee=D("0.001"), sell_cushion=D("0.005"))
    v = value(Balances(D(800), D(200), D(0), D(0)), Pool(D(0), D(0), D(0)), rules, D(60000), **kw)
    assert v.exposure == D(200) and v.equity == D(1000)
    # dust (below step) counts fully toward exposure at the bid but has zero liquidation value
    v = value(Balances(D(990), D(0), D("0.000007"), D(0)), Pool(D("0.000007"), D("0.42"), D(0)), rules, D(60000),
              **kw)
    assert v.dust_qty == D("0.000007") and v.liquidation_value == 0
    assert v.exposure == D("0.42") and v.equity == D(990)
    # sellable inventory is valued at bid minus slippage, rounded down, minus the sell fee
    v = value(Balances(D(800), D(0), D("0.003"), D(0)), Pool(D("0.003"), D(180), D(0)), rules, D(60000), **kw)
    assert v.liquidation_value == D("0.003") * D("59994") * D("0.999")
    assert v.equity == D(800) + v.liquidation_value


def test_exposure_cap_binds_sizing(tmp_path):
    cfg = write_config(tmp_path, {"risk.max_exposure_fraction": "0.05"})
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg)
    (c,) = rows(db, "SELECT * FROM candidates WHERE status = 'submitted'")
    d = json.loads(c["detail"])
    assert d["binding_cap"] == "exposure"
    assert D(c["qty"]) * D(c["limit_price"]) <= D("0.05") * D(d["equity"])


def test_dust_counts_toward_next_entry_exposure(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    sc.mid_quote(bb.end_us + 20_000 * MS, 62200)
    sc.mid_quote(bb.end_us + 20_400 * MS, 62200)  # target exit leaves base-fee dust
    after = follow_range(sc, bb, 6, BREAKOUT_LEVEL)
    nxt = breakout_bar(after[-1], 62400)
    sc.mid_quote(nxt.end_us + 200 * MS, nxt.close)
    sc.candle(nxt)
    db = run(tmp_path, sc, cfg_path)
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (nxt.start_us,))
    d = json.loads(c["detail"])
    dust = D(report(db)["inventory"]["dust_btc"])
    assert dust > 0
    assert D(d["exposure_before"]) == dust * D(nxt.close - 1)  # dust at the planning bid


def _latch_after_entry(tmp_path, overrides):
    cfg = write_config(tmp_path, overrides)
    sc, bb = entry_scenario()
    fq = sc.mid_quote(bb.end_us + 900 * MS, 61502)
    sc.mid_quote(bb.end_us + 1400 * MS, 61502)
    db = run(tmp_path, sc, cfg)
    return db, fq


@pytest.mark.parametrize("latch, overrides", [
    ("daily_loss", {"risk.daily_loss_fraction": "0.0003"}),
    ("drawdown", {"risk.drawdown_fraction": "0.0003", "risk.daily_loss_fraction": "0.5"}),
])
def test_unrealized_loss_trips_latch_and_forces_halt_exit(tmp_path, latch, overrides):
    db, fq = _latch_after_entry(tmp_path, overrides)
    r = report(db)
    (ev,) = [e for e in r["risk"]["events"] if e["kind"] == "latch_set"]
    det = json.loads(ev["detail"])
    assert det["latch"] == latch
    # the loss is unrealized: conservative liquidation equity right after the buy, before any sell
    assert det["mark_quote"] == fq["id"] and D(det["loss_fraction"]) >= D(det["threshold"])
    assert D(det["overshoot_fraction"]) == D(det["loss_fraction"]) - D(det["threshold"])
    (intent,) = rows(db, "SELECT * FROM exit_intents")
    assert intent["reason"] == "halt"
    assert r["risk"]["latches"] == [latch]
    (sell,) = rows(db, "SELECT * FROM fills WHERE side = 'SELL'")
    assert sell["fill_us"] > ev["ts_us"]


def test_manual_kill_cancels_pending_entry_before_it_can_fill(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    n_until_order = len(sc.objs) - 1  # stop right after the breakout candle (order pending)
    db = run(tmp_path, sc, cfg_path, stop_after=n_until_order)
    assert rows(db, "SELECT status FROM orders") == [{"status": "pending"}]
    control(str(db), "kill", "manual_kill", "operator test kill", "2026-10-09T00:00:00Z")
    resume(tmp_path, cfg_path)
    (o,) = rows(db, "SELECT * FROM orders")
    assert o["status"] == "canceled" and o["outcome_reason"] == "risk_latch:manual_kill"
    assert rows(db, "SELECT * FROM fills") == []
    r = report(db)
    assert r["balances"]["USDT"]["free"] == D(1000) and r["balances"]["USDT"]["locked"] == 0
    (ctl,) = r["risk"]["controls"]
    assert ctl["kind"] == "kill" and ctl["reason"] == "operator test kill"


def test_manual_kill_with_open_position_exits_at_next_trustworthy_observation(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    n = len(sc.objs)
    q1 = sc.mid_quote(bb.end_us + 30_000 * MS, 61490)
    q2 = sc.mid_quote(bb.end_us + 30_400 * MS, 61490)
    db = run(tmp_path, sc, cfg_path, stop_after=n)
    control(str(db), "kill", "manual_kill", "flatten", "2026-10-09T00:00:00Z")
    resume(tmp_path, cfg_path)
    (intent,) = rows(db, "SELECT * FROM exit_intents")
    (sell,) = rows(db, "SELECT * FROM orders WHERE side = 'SELL'")
    (sf,) = rows(db, "SELECT * FROM fills WHERE side = 'SELL'")
    assert intent["reason"] == "halt" and sell["reference_quote_id"] == q1["id"]
    assert sf["quote_event_id"] == q2["id"]


def _midnight_scenario() -> tuple[Scenario, list, object]:
    """Warm-up ending at 17:00Z (= 00:00 Asia/Jakarta) with no fresh quote across midnight."""
    sc = Scenario("2026-10-01T12:50:00Z", "midnight scenario")
    bars = staircase(sc.t0, 10)
    emit_edge(sc, bars[:-1])
    sc.candle(bars[-1])  # 16:59 bar received 17:00:00.5Z, no quote: first event of the new local day
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.candle(bb)  # breakout at 17:01:00.5Z with no fresh mark
    sc.mid_quote(bb.end_us + 10_000 * MS, bb.close)  # first fresh mark of the new day
    follow_range(sc, bb, 3, BREAKOUT_LEVEL)
    return sc, bars, bb


def test_day_rollover_without_fresh_mark_postpones_baseline_and_blocks_entries(tmp_path, cfg_path):
    sc, bars, bb = _midnight_scenario()
    db = run(tmp_path, sc, cfg_path)
    r = report(db)
    roll = [e for e in r["risk"]["events"] if e["kind"] == "day_rollover"]
    assert [json.loads(e["detail"])["day"] for e in roll] == ["2026-10-01", "2026-10-02"]
    assert roll[1]["ts_us"] == parse_ts("2026-10-01T17:00:00.500Z")
    base = [e for e in r["risk"]["events"] if e["kind"] == "day_baseline_set"]
    assert base[-1]["ts_us"] == bb.end_us + 10_000 * MS  # postponed until a fresh mark
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    reasons = json.loads(c["detail"])["reasons"]
    assert "day_baseline_pending" in reasons and "no_fresh_quote" in reasons


def test_latches_survive_midnight_and_restart_and_reset_is_audited(tmp_path, cfg_path):
    sc, bars, bb = _midnight_scenario()
    db = run(tmp_path, sc, cfg_path, stop_after=200)
    control(str(db), "kill", "manual_kill", "pre-midnight kill", "2026-10-09T00:00:00Z")
    resume(tmp_path, cfg_path, stop_after=150)  # restart #1
    resume(tmp_path, cfg_path)  # restart #2, crosses 00:00 Asia/Jakarta
    r = report(db)
    assert r["risk"]["latches"] == ["manual_kill"] and r["risk"]["day"] == "2026-10-02"
    roll = [json.loads(e["detail"]) for e in r["risk"]["events"] if e["kind"] == "day_rollover"][-1]
    assert roll["latches_preserved"] == ["manual_kill"]
    (c,) = rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert "risk_latch:manual_kill" in json.loads(c["detail"])["reasons"]

    before = dump_db(db)
    control(str(db), "reset", "manual_kill", "reviewed; resume paper", "2026-10-09T01:00:00Z")
    after = dump_db(db)
    for table in ("input_log", "candidates", "orders", "fills", "ledger", "risk_events", "health_events"):
        assert after[table] == before[table]  # reset erases no history
    ctl = rows(db, "SELECT * FROM control_events ORDER BY id")
    assert [(c["kind"], c["latch"]) for c in ctl] == [("kill", "manual_kill"), ("reset", "manual_kill")]
    assert report(db)["risk"]["latches"] == []
    with pytest.raises(ValueError):
        control(str(db), "reset", "manual_kill", "again", "2026-10-09T01:00:00Z")  # nothing latched


def test_reset_cannot_forgive_inconsistent_state(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    db = run(tmp_path, sc, cfg_path)
    control(str(db), "kill", "manual_kill", "kill", "2026-10-09T00:00:00Z")
    conn = sqlite3.connect(db)
    conn.execute("UPDATE balances SET free = '1000000' WHERE asset = 'USDT'")
    conn.commit()
    conn.close()
    before = dump_db(db)
    with pytest.raises(ReconciliationError):
        control(str(db), "reset", "manual_kill", "try to forgive", "2026-10-09T00:00:00Z")
    assert dump_db(db) == before


def test_loss_latch_persists_after_recovery_and_midnight(tmp_path):
    cfg = write_config(tmp_path, {"risk.daily_loss_fraction": "0.0003"})
    sc = Scenario("2026-10-01T12:30:00Z", "latch then midnight")
    bars = staircase(sc.t0, 10)
    emit_edge(sc, bars)
    bb = breakout_bar(bars[-1], BREAKOUT_LEVEL)
    sc.mid_quote(bb.end_us + 200 * MS, bb.close)
    sc.candle(bb)
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    follow_range(sc, bb, 40, BREAKOUT_LEVEL, offsets_ms=(200, 700))  # crosses 17:00Z, equity recovers
    db = run(tmp_path, sc, cfg)
    r = report(db)
    assert r["risk"]["latches"] == ["daily_loss"] and r["risk"]["day"] == "2026-10-02"
    assert not r["risk"]["baseline_pending"]  # a new baseline was recorded, the latch was not cleared
