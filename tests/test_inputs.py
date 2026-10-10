"""Finalized-candle contract and input validation at the engine level."""

from __future__ import annotations

import json

from conftest import rows
from scenarios import BREAKOUT_LEVEL, MS, entry_scenario, run

from paperbot.synthetic import BarSpec, breakout_bar, emit_edge, range_bars, warm_scenario
from paperbot.timeutil import MINUTE_US, iso


def dispositions(db):
    out = {}
    for r in rows(db, "SELECT * FROM input_log ORDER BY seq"):  # first occurrence of each event id
        out.setdefault(r["event_id"], (r["disposition"], json.loads(r["detail"]).get("reason")))
    return out


def test_invalid_candles_are_rejected_without_touching_indicators(tmp_path, cfg_path):
    sc, bars = warm_scenario(2)
    last = bars[-1]
    nxt = range_bars(last.end_us, 1, 60150, last.close)[0]
    sc.candle(nxt, eid="early", recv_delay_ms=-100)
    sc.candle(nxt, final=False, eid="unfinished")
    sc.candle(nxt, eid="misaligned", start_override=iso(nxt.start_us + 30_000_000))
    sc.candle(nxt, eid="bad-end", end=iso(nxt.end_us - 1000))
    bad = BarSpec(nxt.start_us, nxt.open, nxt.low - 1, nxt.low, nxt.close)
    sc.candle(bad, eid="bad-ohlc")
    sc.candle(nxt, eid="good")
    sc.candle(nxt, eid="dup-bar")
    sc.candle(last, eid="old-bar", recv_delay_ms=61_000)
    sc.add(dict(sc.objs[-1], id="old-bar"))  # exact redelivery of an event id
    sc.add({"type": "candle", "id": "no-final", "recv": iso(nxt.end_us + 61_000_000)})
    db = run(tmp_path, sc, cfg_path)
    d = dispositions(db)
    assert d["unfinished"] == ("rejected", "unfinished_candle")
    assert d["early"] == ("rejected", "received_before_interval_end")
    assert d["misaligned"] == ("rejected", "misaligned_start")
    assert d["bad-end"] == ("rejected", "misaligned_end")
    assert d["bad-ohlc"] == ("rejected", "invalid_ohlc")
    assert d["good"] == ("accepted", None)
    assert d["dup-bar"] == ("rejected", "duplicate_bar")
    assert d["old-bar"] == ("rejected", "out_of_order_bar")
    assert d["no-final"][0] == "rejected" and "malformed_event" in d["no-final"][1]
    assert len(rows(db, "SELECT * FROM input_log WHERE event_id = 'old-bar'")) == 2
    assert rows(db, "SELECT disposition FROM input_log WHERE event_id='old-bar' ORDER BY seq")[1] == {
        "disposition": "duplicate"}


def test_gap_resets_warmup_so_breakout_after_gap_is_skipped(tmp_path, cfg_path):
    sc, bars = warm_scenario(10)
    skipped_minute = bars[-1].end_us
    bb = breakout_bar(BarSpec(skipped_minute, bars[-1].close, 0, 0, bars[-1].close), BREAKOUT_LEVEL)
    sc.mid_quote(bb.end_us + 200 * MS, bb.close)
    sc.candle(bb)  # one full minute missing before this bar
    db = run(tmp_path, sc, cfg_path)
    health = [h for h in rows(db, "SELECT * FROM health_events") if h["kind"] in ("candle_gap", "candle_missing")]
    assert [h["kind"] for h in health] == ["candle_missing", "candle_gap"]
    assert json.loads(health[1]["detail"])["missing_bars"] == 1
    assert rows(db, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,)) == []  # no H after reset
    assert rows(db, "SELECT * FROM orders") == []


def test_missing_candle_detected_by_clock_cancels_pending_entry(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    # quotes keep arriving (fresh, but the order needs one in its window), the next 1m candle never does
    sc.heartbeat(bb.end_us + 66_000 * MS)
    db = run(tmp_path, sc, cfg_path)
    (o,) = rows(db, "SELECT * FROM orders")
    assert o["status"] == "canceled"
    kinds = [h["kind"] for h in rows(db, "SELECT kind FROM health_events")]
    assert "candle_missing" in kinds


def test_invalid_quotes_are_rejected_and_never_fill(tmp_path, cfg_path):
    sc, bb = entry_scenario()
    sc.quote(bb.end_us + 800 * MS, 61510, 61500, eid="inverted")
    sc.quote(bb.end_us + 850 * MS, 0, 61500, eid="zero")
    sc.quote(bb.end_us + 870 * MS, 61500, 61502, ask_qty="0", eid="nosize")
    sc.quote(bb.end_us + 880 * MS, 61500, 61502, exchange_us=bb.end_us + 2000 * MS, eid="future-exchange-time")
    sc.add({"type": "quote", "id": "float-ish", "recv": iso(bb.end_us + 890 * MS), "bid": "abc"})
    good = sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg_path)
    d = dispositions(db)
    assert d["inverted"] == ("rejected", "inverted_quote")
    assert d["zero"] == ("rejected", "nonpositive_price")
    assert d["nosize"] == ("rejected", "nonpositive_size")
    assert d["future-exchange-time"] == ("rejected", "exchange_time_after_receipt")
    assert d["float-ish"][0] == "rejected"
    (f,) = rows(db, "SELECT * FROM fills")
    assert f["quote_event_id"] == good["id"]


def test_malformed_line_is_committed_as_rejected_and_cursor_advances(tmp_path, cfg_path):
    sc, bars = warm_scenario(1)
    inp = sc.write(tmp_path / "state.jsonl")
    text = inp.read_text(encoding="utf-8").splitlines()
    text.insert(3, "{not json")
    inp.write_text("\n".join(text) + "\n", encoding="utf-8", newline="\n")
    from paperbot.replay import new_run

    new_run(str(cfg_path), str(inp), str(tmp_path / "state.sqlite"))
    db = tmp_path / "state.sqlite"
    (bad,) = rows(db, "SELECT * FROM input_log WHERE seq = 3")
    assert bad["disposition"] == "rejected" and "malformed_json" in bad["detail"]
    assert rows(db, "SELECT seq FROM cursor")[0]["seq"] == len(text) - 1


def test_five_minute_group_with_internal_gap_is_unavailable(tmp_path, cfg_path):
    sc, bars = warm_scenario(1)
    last = bars[-1]
    nb = range_bars(last.end_us, 5, 60000, last.close)
    emit_edge(sc, nb[:2] + nb[3:])  # minute 2 of the group missing
    db = run(tmp_path, sc, cfg_path)
    from paperbot.storage import Storage

    s = Storage.open_readonly(db)
    st = s.load_state()
    s.close()
    assert st.strategy.five_count == 0 and st.strategy.last_five is None  # reset by the gap; partial group unused
    assert st.strategy.last_start_us == nb[-1].start_us
    assert nb[0].start_us % (5 * MINUTE_US) == 0
