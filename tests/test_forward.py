"""Forward-session behavior with mocked public data (deterministic step mode; no threads, no network)."""

from __future__ import annotations

import json

import pytest
from conftest import report, rows, write_forward_config
from fake_binance import FakeMarket, Harness, boot, exchange_info, standard_bars, ws_avg, ws_kline, ws_ref

from paperbot.forward import Item
from paperbot.money import dtext
from paperbot.normalize import kline_candle
from paperbot.storage import ProfileLock, StateLocked
from paperbot.synthetic import breakout_bar, range_bars
from paperbot.timeutil import MINUTE_US, US_PER_MS, US_PER_S

MS = US_PER_MS
BARS = standard_bars()


def health(db, kind=None):
    hs = rows(db, "SELECT * FROM health_events ORDER BY id")
    return [h for h in hs if kind is None or h["kind"] == kind]


@pytest.fixture
def h(tmp_path):
    cfg = write_forward_config(tmp_path)
    harness = Harness(tmp_path, cfg, FakeMarket(BARS))
    yield harness
    harness.close()


def booted(h, at_bar=249):
    now = BARS[at_bar].end_us + 10_000 * MS
    t = boot(h, now)
    assert h.st.health.blocks == [], h.st.health.blocks
    return t


def enter(h, t, bar=250):
    """Live breakout bar with fresh quotes before and after; returns time after the entry fill."""
    bb = BARS[bar]
    t = h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    t = h.run_quotes(bb.end_us + 800 * MS, bb.end_us + 2000 * MS, bb.close + 2)
    assert h.st.position is not None, report(h.state)["candidates"]["rows"][-1]
    return t


def test_startup_blocks_entries_until_warmup_stream_quote_and_metadata_then_rearms(h):
    now = BARS[249].end_us + 10_000 * MS
    h.start(now)
    assert set(h.st.health.blocks) == {"feed_disconnected", "restart_recovery", "clock_unsynced"}
    names = [n for n, _ in h.requests]
    assert names == ["time", "metadata", "avgPrice", "referencePrice", "klines"]
    h.answer_all(now + 100 * MS)
    assert h.st.strategy.five_count == 50 and h.st.metadata_hash is not None
    assert "clock_unsynced" not in h.st.health.blocks  # the server-time check succeeded
    h.tick(now + 150 * MS)
    assert "restart_recovery" in h.st.health.blocks  # no stream yet: not recovered
    h.connect(now + 200 * MS)
    h.tick(now + 250 * MS)
    assert "restart_recovery" in h.st.health.blocks  # no fresh quote yet
    h.quote(now + 300 * MS, BARS[249].close)
    h.tick(now + 310 * MS)
    assert h.st.health.blocks == []
    assert [x["kind"] for x in health(h.state) if x["kind"] in ("feed_recovered", "rearmed")] == [
        "feed_recovered", "rearmed"]


def test_warmup_backfill_never_creates_retroactive_entries(h):
    booted(h)
    cands = rows(h.state, "SELECT * FROM candidates")
    assert len(cands) == 9 and {c["skip_reason"] for c in cands} == {"backfill_no_retroactive_entry"}
    assert rows(h.state, "SELECT * FROM orders") == []


def test_duplicate_and_out_of_order_observations_are_dropped(h):
    t = booted(h) + 2 * US_PER_S  # past the quote sampling interval
    cursor = h.st.cursor
    h.u = 5000
    h.quote(t, 61400)
    h.msg(json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 5001, "s": "BTCUSDT", "b": "1", "B": "1",
                                                                "a": "2", "A": "1"}}), t + 1 * MS)  # same u
    h.msg(json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 4000, "s": "BTCUSDT", "b": "1", "B": "1",
                                                                "a": "2", "A": "1"}}), t + 2 * MS)  # older u
    assert h.st.cursor == cursor + 1 and h.session.stats["quote_duplicate_or_out_of_order"] == 2
    h.msg(ws_kline(BARS[200]), t + 3 * MS)  # an old bar redelivered
    assert h.session.stats["bar_old_or_duplicate"] == 1 and h.st.cursor == cursor + 1
    h.msg("not json", t + 4 * MS)
    assert h.session.stats["ws_unparseable"] == 1


def test_quotes_are_sampled_not_repeated_and_never_refresh_freshness(h):
    t = booted(h)
    n0 = h.session.stats["submitted_quote"]
    for i in range(10):  # 10 real quotes in 900 ms: only those >= quote_sample_ms apart are forwarded
        h.quote(t + i * 100 * MS, 61400)
    assert h.session.stats["submitted_quote"] - n0 == 1
    # silence: ticks send heartbeats but never a quote, so the last quote ages and goes stale
    last = h.st.last_quote.recv_us
    for k in range(1, 6):
        h.tick(t + k * US_PER_S)
    assert h.st.last_quote.recv_us == last and not h.st.health.quotes_fresh
    assert h.session.stats["submitted_heartbeat"] >= 4


def test_silent_feed_with_position_queues_exit_and_discloses_unprotected_time(h):
    t = enter(h, booted(h))
    for k in range(1, 70):  # 69 s of silence: no message at all, only local ticks
        h.tick(t + k * US_PER_S)
    pos = h.st.position
    assert pos is not None and pos.exit_intent is not None and pos.exit_intent.reason == "health:quotes_stale"
    assert h.st.order is None  # no fresh data -> no exit order, no fill invented
    assert any(x["kind"] == "candle_missing" for x in health(h.state))
    now = t + 70 * US_PER_S
    h.msg(ws_ref(None, now // MS), now - 2 * MS)  # the stream returns: reference price observation...
    h.msg(ws_avg("61480", now // MS), now - 1 * MS)
    h.quote(now, 61480)  # ...and a fresh quote: exit order submitted referencing it
    assert h.st.order is not None and h.st.order.purpose == "exit"
    h.quote(now + 400 * MS, 61470)  # fills only on a later quote after latency
    (sell,) = rows(h.state, "SELECT * FROM fills WHERE side='SELL'")
    assert sell["fill_us"] == now + 400 * MS and sell["quote_recv_us"] >= sell["ready_us"]
    r = report(h.state)
    assert r["forward"]["unprotected_total_s"] >= 30 and r["reconciliation"]["ok"]


def test_reconnect_gap_is_backfilled_indicators_repaired_and_no_retroactive_entry(h):
    t = booted(h, at_bar=245)
    h.disconnect(t)
    assert "feed_disconnected" in h.st.health.blocks
    # bars 246..251 (including the bar-250 breakout) are missed while disconnected; bar 252 arrives live
    live = BARS[252]
    h.connect(live.end_us, answer_continuity=False)
    assert h.requests and h.requests[-1][0] == "klines"  # continuity is revalidated on every reconnect
    params = h.requests[-1][1]
    assert params["startTime"] == BARS[246].start_us // MS and params["purpose"] == "backfill"
    h.quote(live.end_us + 100 * MS, live.close)
    h.bar(live)
    assert h.st.strategy.last_start_us == BARS[245].start_us  # the live bar is held until the gap is repaired
    h.tick(live.end_us + 600 * MS)
    assert "feed_recovery" in h.st.health.blocks  # continuity not yet revalidated: not rearmed
    h.answer_all(live.end_us + 900 * MS)
    st = h.st.strategy
    assert st.last_start_us == live.start_us and st.run_bars == 253  # contiguous: no indicator reset
    assert not health(h.state, "candle_gap")
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (BARS[250].start_us,))
    assert c["skip_reason"] == "backfill_no_retroactive_entry"  # the missed breakout is never traded
    assert rows(h.state, "SELECT * FROM orders") == []


def test_failed_backfill_releases_held_bars_and_resets_rather_than_forward_fills(h):
    t = booted(h, at_bar=245)
    h.disconnect(t)
    h.connect(BARS[250].end_us, answer_continuity=False)
    h.bar(BARS[250])
    h.requests.clear()  # the backfill never answers
    h.tick(BARS[250].end_us + 31 * US_PER_S)
    assert any(x["kind"] == "feed_backfill_failed" for x in health(h.state))
    assert h.st.strategy.last_start_us == BARS[250].start_us and h.st.strategy.run_bars == 1  # reset, re-warm
    assert health(h.state, "candle_gap")


def test_rate_limited_rest_blocks_entries_until_rest_recovers(h):
    t = booted(h)
    h.requests.append(("time", {}))
    h.answer_all(t, fail={"time"})
    assert "rest_unavailable" in h.st.health.blocks
    h.requests.append(("time", {}))
    h.answer_all(t + 1000 * MS)
    assert "rest_unavailable" not in h.st.health.blocks


def test_clock_offset_beyond_limit_blocks_entries(h):
    t = booted(h)
    h.market.server_offset_ms = 5000
    h.requests.append(("time", {}))
    h.answer_all(t)
    assert "clock_unsynced" in h.st.health.blocks
    h.market.server_offset_ms = 10
    h.requests.append(("time", {}))
    h.answer_all(t + 1000 * MS)
    assert "clock_unsynced" not in h.st.health.blocks


def test_metadata_changes_are_versioned_and_applied(h):
    t = booted(h)
    v1 = h.st.metadata_hash
    h.market.info = exchange_info(status="BREAK")
    h.requests.append(("metadata", {}))
    h.answer_all(t)
    v2 = h.st.metadata_hash
    assert v2 != v1 and len(rows(h.state, "SELECT * FROM metadata_versions")) == 2  # both versions preserved
    assert [x["kind"] for x in health(h.state) if x["kind"].startswith("metadata")] == [
        "metadata_version", "metadata_version"]
    bb = BARS[250]
    h.run_quotes(t + 10 * MS, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "filter:SYMBOL:status"  # the new version (status BREAK) is the one applied
    assert rows(h.state, "SELECT * FROM orders") == []


def test_expired_metadata_blocks_entries(tmp_path):
    cfg = write_forward_config(tmp_path, forward={"metadata_refresh_s": 10, "metadata_max_age_s": 20})
    h = Harness(tmp_path, cfg, FakeMarket(BARS))
    try:
        booted(h, at_bar=249)
        bb = BARS[250]
        h.run_quotes(BARS[249].end_us + 12 * US_PER_S, bb.end_us + 300 * MS, bb.close)
        h.bar(bb)  # metadata is ~50 s old > 20 s max age
        (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
        assert c["skip_reason"] == "metadata_expired" and rows(h.state, "SELECT * FROM orders") == []
    finally:
        h.close()


def test_missing_reference_blocks_entries(h):
    now = BARS[249].end_us + 10_000 * MS
    h.start(now)
    h.answer_all(now + 100 * MS, errors={"referencePrice"})  # reference price never observed
    h.connect(now + 200 * MS)
    t = h.run_quotes(now + 300 * MS, now + 1500 * MS, BARS[249].close)
    assert h.st.ref_price is None and h.st.health.blocks == []
    bb = BARS[250]
    h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    # unknown reference: neither the PRICE_RANGE execution rule nor PERCENT_PRICE_BY_SIDE can be evaluated
    assert c["skip_reason"] == "execution_reference_unavailable"
    assert rows(h.state, "SELECT * FROM orders") == []


def test_reference_price_non_null_is_used_and_execution_price_range_expires_fills(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    try:
        t = booted(h)
        bb = BARS[250]
        # a reference price far below the market: PRICE_RANGE (+-5%) makes the buy unexecutable
        h.msg(ws_ref("50000", bb.end_us // MS), t)
        t = h.run_quotes(t + 1 * MS, bb.end_us + 300 * MS, bb.close)
        h.bar(bb)
        (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
        assert c["skip_reason"] == "execution_rule_price_range"
        assert rows(h.state, "SELECT * FROM orders") == []
    finally:
        h.close()


def test_profile_lock_prevents_two_states_running_the_same_account(tmp_path):
    cfg = write_forward_config(tmp_path)
    from paperbot.config import load_config

    c = load_config(cfg)
    with ProfileLock(c.forward.profile_lock_dir, c.account_id):
        with pytest.raises(StateLocked):
            ProfileLock(c.forward.profile_lock_dir, c.account_id).acquire()
    ProfileLock(c.forward.profile_lock_dir, c.account_id).acquire().release()


def test_forward_restart_retires_attempts_flattens_and_rearms_without_inventing_fills(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = enter(h, booted(h))
    # target hit -> exit order submitted, then the process dies before any later quote
    h.quote(t, 62400)
    assert h.st.order is not None and h.st.order.purpose == "exit"
    exit_order = h.st.order.order_id
    btc = h.st.balances.btc_total
    h.close()

    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    assert h2.restart
    t2 = t + 500 * MS  # quick restart: the exit IOC is still unresolved when the new session starts
    h2.start(t2)
    o = rows(h2.state, "SELECT * FROM orders WHERE order_id = ?", (exit_order,))[0]
    assert o["status"] == "zero" and o["outcome_reason"] == "retired_on_restart"
    assert rows(h2.state, "SELECT * FROM fills WHERE order_id = ?", (exit_order,)) == []  # no invented fill
    assert h2.st.balances.btc_total == btc and h2.st.balances.btc_locked == 0
    assert set(h2.st.health.blocks) == {"feed_disconnected", "restart_recovery", "clock_unsynced"}
    h2.answer_all(t2 + 100 * MS)
    h2.connect(t2 + 200 * MS)
    h2.quote(t2 + 300 * MS, 62000)  # fresh: flatten exit submitted
    assert h2.st.order is not None and h2.st.order.purpose == "exit"
    h2.tick(t2 + 310 * MS)
    assert "restart_recovery" in h2.st.health.blocks  # still exposed: not rearmed
    h2.quote(t2 + 700 * MS, 61990)  # fill
    h2.tick(t2 + 800 * MS)
    assert h2.st.position is None and h2.st.health.blocks == []
    pos = rows(h2.state, "SELECT * FROM positions")[0]
    assert pos["status"] == "closed"
    r = report(h2.state)
    assert r["inventory"]["dust_btc"] > 0 and r["inventory"]["dust_basis_usdt"] > 0  # dust kept with basis
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]
    h2.close()


def test_restart_cancels_pending_entry(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = booted(h)
    bb = BARS[250]
    h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    assert h.st.order is not None and h.st.order.purpose == "entry"
    oid = h.st.order.order_id
    h.close()
    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    h2.start(bb.end_us + 600 * MS)
    o = rows(h2.state, "SELECT * FROM orders WHERE order_id = ?", (oid,))[0]
    assert o["status"] == "canceled" and o["outcome_reason"] == "forward_restart"
    assert h2.st.balances.usdt_locked == 0
    h2.close()


def test_no_historical_signal_executes_after_recovery_from_a_long_outage(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    booted(h, at_bar=240)
    h.close()
    # down for ~20 minutes, covering the bar-250 breakout; restart backfills it
    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    now = BARS[260].end_us + 5 * US_PER_S
    boot(h2, now)
    (c,) = rows(h2.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (BARS[250].start_us,))
    assert c["skip_reason"] == "backfill_no_retroactive_entry" and rows(h2.state, "SELECT * FROM orders") == []
    assert h2.st.strategy.run_bars > 255  # indicators repaired continuously by backfill
    h2.close()


def test_entry_fill_reports_paper_and_outbox_rows_commit_with_transitions(h):
    enter(h, booted(h))
    out = rows(h.state, "SELECT * FROM outbox ORDER BY created_us")
    kinds = [o["kind"] for o in out]
    assert "fill" in kinds and "session" in kinds
    assert all(o["text"].startswith("PAPER | PUBLIC DATA | MOCKED") for o in out)
    fill_msg = [o for o in out if o["kind"] == "fill"][0]
    (f,) = rows(h.state, "SELECT * FROM fills")
    assert fill_msg["seq"] == f["seq"] and fill_msg["msg_id"] == f"fill:{f['fill_id']}"  # same transaction


def test_session_items_are_recorded_raw(h):
    booted(h)
    sources = {r["source"] for r in h.raw}
    assert {"ws", "ws_status", "rest"} <= sources


def test_dtext_helper_used():
    assert dtext(range_bars(0, 1, 60000, 60000)[0].close) == "59980"
    assert breakout_bar(BARS[0], 1).close == 1
    assert MINUTE_US == 60 * US_PER_S


def test_unknown_item_kind_rejected(h):
    with pytest.raises(ValueError):
        h.session.handle(Item("bogus", 1, {}))


def test_slow_restart_closes_stale_attempts_through_staleness_rules_without_fills(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = enter(h, booted(h))
    h.quote(t, 62400)
    exit_order = h.st.order.order_id
    h.close()
    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    h2.start(t + 120 * US_PER_S)
    o = rows(h2.state, "SELECT * FROM orders WHERE order_id = ?", (exit_order,))[0]
    assert o["status"] == "zero" and o["outcome_reason"] in ("no_eligible_quote_after_ready", "retired_on_restart")
    assert rows(h2.state, "SELECT * FROM fills WHERE order_id = ?", (exit_order,)) == []
    assert h2.st.position is not None and h2.st.position.exit_intent is not None  # still to be flattened
    h2.close()


# ------------------------------------------------- review regressions (87b130b)


@pytest.mark.parametrize("mode", ["continuity_answered_first", "bar_held_until_continuity", "engine_direct"])
def test_reconnect_never_enters_on_pre_disconnect_quotes(h, mode):
    """Disconnect at candle end +300 ms, reconnect at +400 ms, breakout candle at +500 ms: no buy."""
    t = booted(h)
    bb = BARS[250]
    end = bb.end_us
    h.run_quotes(t, end + 300 * MS, bb.close)  # fresh quotes right up to the disconnect
    assert h.st.health.quotes_fresh
    h.disconnect(end + 300 * MS)
    assert not h.st.health.quotes_fresh  # a quote from before the disconnect never counts as fresh again
    assert {"feed_disconnected", "feed_recovery"} <= set(h.st.health.blocks)
    if mode == "engine_direct":  # the engine alone (no continuity hold) must refuse too
        h.session.feed("ws_connected", end + 400 * MS, conn=2)
        h.session.submit(kline_candle(json.loads(ws_kline(bb))["data"], end + 500 * MS))
        expected_reason = "entry_block:feed_recovery"
    else:
        h.connect(end + 400 * MS, answer_continuity=mode == "continuity_answered_first")
        assert "feed_disconnected" not in h.st.health.blocks and "feed_recovery" in h.st.health.blocks
        h.bar(bb, delay_ms=500)
        if mode == "bar_held_until_continuity":
            assert h.st.strategy.last_start_us == BARS[249].start_us  # held while continuity is revalidated
            h.answer_all(end + 600 * MS, only={"klines"})
            expected_reason = "entry_block:feed_recovery"
        else:
            expected_reason = "backfill_no_retroactive_entry"  # the REST check already delivered the bar
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["status"] == "skipped" and c["skip_reason"] == expected_reason, c
    assert rows(h.state, "SELECT * FROM orders") == []
    if mode != "engine_direct":
        # rearming needs a quote received after the reconnection
        h.tick(end + 700 * MS)
        assert "feed_recovery" in h.st.health.blocks
        h.quote(end + 800 * MS, bb.close)
        h.tick(end + 810 * MS)
        assert h.st.health.blocks == []
        assert health(h.state, "rearmed")[-1]["detail"].count("feed_recovery") == 1


def test_failed_initial_clock_check_blocks_entries_until_a_check_succeeds(h):
    now = BARS[249].end_us + 10_000 * MS
    h.start(now)
    h.answer_all(now + 100 * MS, errors={"time"})  # HTTP 503 (not a rate limit)
    assert "clock_unsynced" in h.st.health.blocks
    h.connect(now + 200 * MS)
    h.msg(ws_avg(h.market.last_close(now), now // MS), now + 250 * MS)
    h.msg(ws_ref(None, now // MS), now + 260 * MS)
    bb = BARS[250]
    t = h.run_quotes(now + 300 * MS, bb.end_us + 300 * MS, bb.close)
    assert h.st.health.blocks == ["clock_unsynced"]  # everything else recovered; the clock is still unverified
    assert [n for n, _ in h.requests] == ["time"]  # retried once after 15 s, not hammered
    h.bar(bb)
    t = h.run_quotes(t, bb.end_us + 2000 * MS, bb.close + 2)
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "entry_block:clock_unsynced" and rows(h.state, "SELECT * FROM orders") == []
    h.answer_all(t, errors={"time"})  # the retry fails as well: backoff doubles
    failed = [json.loads(x["detail"]) for x in health(h.state, "feed_clock_check_failed")]
    assert [f["retry_in_s"] for f in failed] == [15, 30]
    t = h.run_quotes(t + 10 * MS, t + 29 * US_PER_S, bb.close + 2)
    assert h.requests == []
    t = h.run_quotes(t, t + 2 * US_PER_S, bb.close + 2)
    assert [n for n, _ in h.requests] == ["time"]
    h.answer_all(t)  # success within the limit
    assert h.st.health.blocks == [] and h.st.clock_checked_us is not None


def test_clock_check_expires_without_a_recent_success(h):
    t = booted(h)
    h.tick(t + 3 * 300 * US_PER_S + US_PER_S)  # clock_check_s 300: no success for more than 3 intervals
    assert "clock_unsynced" in h.st.health.blocks
    sets = [json.loads(x["detail"]) for x in health(h.state, "entry_block_set")]
    assert sets[-1] == {"block": "clock_unsynced", "reason": "clock_check_expired"}
    assert h.st.clock_checked_us is None


def test_metadata_tick_change_keeps_an_open_position_reconciled_and_restartable(tmp_path):
    from decimal import Decimal

    from fake_binance import FILTERS

    from paperbot.money import ceil_to, floor_to
    from paperbot.reconcile import reconcile
    from paperbot.strategy import TARGET_STOP_MULT

    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = enter(h, booted(h))
    pos = h.st.position
    v1 = h.st.metadata_hash
    coarse = Decimal("0.1")
    assert (floor_to(pos.entry_price - pos.stop_distance, coarse) != pos.stop_price
            or ceil_to(pos.entry_price + TARGET_STOP_MULT * pos.stop_distance, coarse) != pos.target_price)
    m.info = exchange_info(filters=[dict(f, tickSize="0.10000000") if f["filterType"] == "PRICE_FILTER" else f
                                    for f in FILTERS])
    h.requests.append(("metadata", {}))
    h.answer_all(t)
    assert h.st.metadata_hash != v1 and h.st.position is not None
    assert report(h.state)["reconciliation"]["ok"]  # frozen prices checked against the entry version
    assert pos.metadata_sha256 == v1
    stop, target = pos.stop_price, pos.target_price
    h.close()

    h2 = Harness(tmp_path, cfg, m, session_id="s2")  # restart reconciles (it halted here before the fix)
    try:
        assert (h2.st.position.stop_price, h2.st.position.target_price) == (stop, target)
        assert reconcile(h2.engine.store, h2.st) == []
        h2.st.position.metadata_sha256 = h2.st.metadata_hash  # a wrong entry version is detected
        assert any("version in force at the entry fill" in p for p in reconcile(h2.engine.store, h2.st))
        h2.st.position.metadata_sha256 = None  # positions opened before the field existed: derived from inputs
        assert reconcile(h2.engine.store, h2.st) == []
        h2.st.position.metadata_sha256 = v1
        boot(h2, t + 60 * US_PER_S)  # flatten on fresh quotes: later orders use the current (0.10) rules
        (ex,) = rows(h2.state, "SELECT * FROM orders WHERE purpose = 'exit'")
        assert Decimal(ex["limit_price"]) % coarse == 0
        assert report(h2.state)["reconciliation"]["ok"]
    finally:
        h2.close()


# ------------------------------------------------ slow persistence: chunked backfill and owner lag (step mode)


def test_chunked_warmup_interleaves_health_checks_and_ends_in_the_same_state(tmp_path):
    from paperbot import codec

    now = BARS[249].end_us + 10_000 * MS
    def indicators(st):
        """Strategy state without receipt stamps (chunked bars are stamped when they are applied)."""
        def strip(x):
            if isinstance(x, dict):
                return {k: strip(v) for k, v in x.items() if k not in ("recv_us", "available_us")}
            return [strip(v) for v in x] if isinstance(x, list) else x
        return strip(codec.dump(st.strategy))

    ref = Harness(tmp_path, write_forward_config(tmp_path, name="ref.toml"), FakeMarket(BARS), state_name="ref.sqlite")
    boot(ref, now)
    expected_strategy = indicators(ref.st)
    ref.close()

    h = Harness(tmp_path, write_forward_config(tmp_path), FakeMarket(BARS))
    try:
        h.session.backfill_chunk = 25
        h.start(now)
        h.answer_all(now + 100 * MS)
        assert h.session.pending_work() and h.st.strategy.five_count < 50  # only the first chunk is applied
        h.connect(now + 200 * MS)
        h.quote(now + 300 * MS, BARS[249].close)
        assert h.st.health.quotes_fresh  # a queued observation is processed between chunks
        h.session.work()
        h.tick(now + 3_000 * MS)  # no message for 2.7 s: stale while the warm-up is still being applied
        assert not h.st.health.quotes_fresh and health(h.state, "quotes_stale")
        assert "restart_recovery" in h.st.health.blocks  # no recovery signal while bars are still pending
        while h.session.pending_work():
            h.session.work()
        assert indicators(h.st) == expected_strategy  # same bars and indicators as the one-call warm-up
        assert rows(h.state, "SELECT count(*) AS n FROM input_log WHERE disposition = 'rejected'")[0]["n"] == 0
        cands = rows(h.state, "SELECT skip_reason FROM candidates")
        assert {c["skip_reason"] for c in cands} == {"backfill_no_retroactive_entry"}
    finally:
        h.close()


def test_owner_lag_blocks_entries_until_the_queue_catches_up(h):
    t = booted(h)
    h.session.observe_lag(1_500 * MS, t)
    assert "owner_lag" not in h.st.health.blocks  # below the 2 s quote freshness limit
    h.session.observe_lag(2_500 * MS, t + 1)
    assert "owner_lag" in h.st.health.blocks
    bb = BARS[250]
    h.run_quotes(t + 10 * MS, bb.end_us + 300 * MS, bb.close, tick=False)
    h.bar(bb)
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["skip_reason"] == "entry_block:owner_lag" and rows(h.state, "SELECT * FROM orders") == []
    h.session.observe_lag(1_200 * MS, bb.end_us + 600 * MS)
    assert "owner_lag" in h.st.health.blocks  # hysteresis: clears only below half the limit
    h.session.observe_lag(0, bb.end_us + 700 * MS)
    assert "owner_lag" not in h.st.health.blocks
    lag = [json.loads(x["detail"]) for x in health(h.state, "feed_owner_lag")]
    assert [x["lagging"] for x in lag] == [True, False] and lag[0]["lag_ms"] == 2500


def test_a_new_session_clears_an_owner_lag_block_left_by_the_previous_process(tmp_path):
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = boot(h, BARS[249].end_us + 10_000 * MS)
    h.session.observe_lag(5_000 * MS, t)
    assert "owner_lag" in h.st.health.blocks
    h.close()  # the process ends while lagging
    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    try:
        boot(h2, t + 60 * US_PER_S)
        assert "owner_lag" not in h2.st.health.blocks and h2.st.health.blocks == []
    finally:
        h2.close()
