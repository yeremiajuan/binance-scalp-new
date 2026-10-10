"""Stale-feed detection in wall time under deliberately slow persistence and queued input (real threaded runner).

Every SQLite commit is slowed by ``SLOW_S`` on top of its real fsync (the reviewed 1-vCPU VPS committed ~16 ms per
input), while the market stream sends quotes during the warm-up and then goes silent. The runner must keep
processing queued observations and heartbeats between warm-up chunks, so a silent feed is detected about
``quote_max_age`` after the last quote, not only after the whole warm-up has been committed.
"""

from __future__ import annotations

import json
import time

from conftest import report, rows, write_forward_config
from fake_binance import FakeMarket, FakeRestTransport, Harness, SilentStream, boot
from runner_helpers import RunnerThread, SlowCommits, wait_for

from paperbot.synthetic import staircase
from paperbot.timeutil import FIVE_MINUTES_US, US_PER_MS

SLOW_S = 0.02  # added to every commit: slower than the reviewed VPS (~16 ms per commit)
QUOTE_MAX_AGE_S = 2.0  # write_config: quote_max_age_ms 2000
# Wall-time allowance after a quote turns stale: one warm-up chunk (20 commits) plus queued inputs, at SLOW_S plus a
# slow fsync each. Before chunking, the delay was the whole warm-up (~300 commits, > 6 s at this speed).
DETECTION_SLACK_S = 2.5
MS = US_PER_MS


def _health(state) -> list[dict]:
    return rows(state, "SELECT id, seq, kind, detail FROM health_events ORDER BY id") if state.exists() else []


def _assert_detected_in_time(timer: SlowCommits, inventory_open: bool) -> list[float]:
    flips = [f for f in timer.stale_flips if f[1] is not None and f[2] == inventory_open]
    assert flips, f"no stale transition recorded (inventory_open={inventory_open}): {timer.stale_flips}"
    delays = [wall - (stamp / 1e6 + QUOTE_MAX_AGE_S) for wall, stamp, _ in flips]
    for d in delays:
        assert -0.2 <= d <= DETECTION_SLACK_S, f"stale detected {d:+.2f} s after the quote became stale ({delays})"
    return delays


def test_silent_feed_is_detected_promptly_while_a_slow_warmup_is_still_being_committed(tmp_path):
    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    market = FakeMarket(staircase(start, 13))
    stream = SilentStream(market.last_close(now), quotes=3)
    cfg = write_forward_config(tmp_path, forward={"ws_silence_s": 2, "reconnect_initial_ms": 100,
                                                   "reconnect_max_ms": 200, "heartbeat_ms": 500})
    state = tmp_path / "slow.sqlite"
    with SlowCommits(SLOW_S) as timer:
        runner = RunnerThread(cfg, state, transport=FakeRestTransport(market),
                              ws_url=f"ws://127.0.0.1:{stream.port}/stream")
        try:
            wait_for(lambda: timer.stale_flips, "a stale-quote transition")
            wait_for(lambda: rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'candle'")[0]["n"]
                     >= 250, "the slow warm-up to be committed")
            wait_for(lambda: stream.connections >= 2, "the silent connection to be replaced")
        finally:
            code = runner.stop()
            stream.close()
    assert code == 0
    candles = [w for w, t in timer.commits if t == "candle"]
    quotes = [w for w, t in timer.commits if t == "quote"]
    assert len(candles) >= 250 and quotes
    # queued input is not starved by the warm-up: quotes received during it were committed before it finished
    assert quotes[0] < candles[-1], (quotes[0] - candles[0], candles[-1] - candles[0])
    assert candles[-1] - candles[0] > 5.0  # the warm-up really was slow (>= 250 commits at >= 20 ms)
    delays = _assert_detected_in_time(timer, inventory_open=False)
    # ordering and durability are unchanged: every input committed in order, nothing rejected as out of order
    assert rows(state, "SELECT count(*) AS n FROM input_log WHERE detail LIKE '%non_monotonic%'")[0]["n"] == 0
    r = report(state)
    assert r["reconciliation"]["ok"], r["reconciliation"]
    stop = [json.loads(h["detail"]) for h in _health(state) if h["kind"] == "feed_session_stop"][-1]
    assert "discarded_queued_inputs" in stop and "max_owner_lag_ms" in stop
    print(f"stale detection delays after the quote went stale: {[round(d, 2) for d in delays]} s; "
          f"warm-up {candles[-1] - candles[0]:.1f} s; first quote committed "
          f"{quotes[0] - candles[0]:.2f} s into it; max owner lag {stop['max_owner_lag_ms']} ms")


def test_open_inventory_on_a_silent_feed_under_slow_persistence_is_held_disclosed_and_never_exited_on_stale_data(
        tmp_path):
    """Restart with an open position into a long slow backfill; the stream sends one quote, then stays silent
    (also after reconnecting).

    The quote is fresh, so the queued restart-flatten exit is submitted on it; with no later quote the attempt
    expires without a fill. (A quote after a reconnect within quote_max_age of readiness could fill it under the
    unchanged fill model; this scenario deliberately has none.)"""
    cfg = write_forward_config(tmp_path, forward={"ws_silence_s": 2, "reconnect_initial_ms": 100,
                                                   "reconnect_max_ms": 200, "heartbeat_ms": 500})
    now = time.time_ns() // 1000
    bars = staircase((now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 750 * 60_000_000, 30)  # 750 bars up to now
    market = FakeMarket(bars)
    h = Harness(tmp_path, cfg, market)  # step mode at normal speed: open a position ~500 minutes ago
    t = boot(h, bars[249].end_us + 10_000 * MS)
    bb = bars[250]
    t = h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    h.run_quotes(bb.end_us + 800 * MS, bb.end_us + 2000 * MS, bb.close + 2)
    assert h.st.position is not None
    btc = h.st.balances.btc_total
    h.close()

    stream = SilentStream(market.last_close(time.time_ns() // 1000), quotes=1, later_quotes=0)
    state = h.state
    before = rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'candle'")[0]["n"]
    with SlowCommits(SLOW_S) as timer:  # the restart must backfill ~500 bars at slow-disk speed
        runner = RunnerThread(cfg, state, transport=FakeRestTransport(market),
                              ws_url=f"ws://127.0.0.1:{stream.port}/stream")
        try:
            wait_for(lambda: [f for f in timer.stale_flips if f[2]], "a stale transition with inventory open")
            wait_for(lambda: [x for x in _health(state) if x["kind"] == "exit_waiting"
                              and "awaiting_fresh_quote" in x["detail"]], "the exit to wait for fresh data")
            wait_for(lambda: rows(state, "SELECT count(*) AS n FROM orders WHERE purpose = 'exit' AND status != "
                                         "'pending'")[0]["n"] >= 1, "an exit attempt to end without a fill")
            wait_for(lambda: rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'candle'")[0]["n"]
                     - before >= 450, "the restart backfill to be committed")
        finally:
            code = runner.stop()
            stream.close()
    assert code == 0
    candles = [w for w, t in timer.commits if t == "candle"]
    stale_open = [w for w, _, inv in timer.stale_flips if inv]
    assert len(candles) >= 400 and candles[-1] - candles[0] > 8.0  # a long, slow restart backfill ...
    assert stale_open[0] < candles[-1]  # ... and the exposed inventory went stale while it was being committed
    assert rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'candle'")[0]["n"] - before == \
        len(candles)
    _assert_detected_in_time(timer, inventory_open=True)
    # inventory is retained, no fill is invented from stale data, the exit stays queued and exposure is disclosed
    assert rows(state, "SELECT * FROM fills WHERE side = 'SELL'") == []
    assert rows(state, "SELECT status FROM positions")[0]["status"] == "open"
    assert rows(state, "SELECT status FROM exit_intents")[0]["status"] == "active"
    exits = rows(state, "SELECT status, outcome_reason FROM orders WHERE purpose = 'exit'")
    assert exits and all(o["status"] in ("zero", "pending") for o in exits)
    stale = [json.loads(x["detail"]) for x in _health(state) if x["kind"] == "quotes_stale"]
    assert any(s.get("inventory_exposed") and "last_quote" in s for s in stale)  # detected by the clock
    r = report(state)
    assert r["reconciliation"]["ok"] and r["inventory"]["btc_total"] == btc
    assert r["forward"]["unprotected_total_s"] > 0 and r["forward"]["queued_exit"]
