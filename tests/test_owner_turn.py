"""The owner loop's turn (paperbot_net.runner.owner_turn) with a real StampedQueue and a controlled clock.

Covers two review findings on fca062e:

1. Owner lag must be enforced before the delayed input is processed: a breakout candle or the first fill quote that
   waited too long in the queue must meet the ``owner_lag`` entry block, not create or fill an entry first. Exits
   are not blocked.
2. Held live candles released after the final backfill chunk must be stamped at or after every input already
   handled (including inputs that produced no engine event: the held candle itself, sampled-out quotes), even when
   the chunk is forced while other inputs are still queued.
"""

from __future__ import annotations

import json

import pytest
from conftest import rows, write_forward_config
from fake_binance import FakeMarket, Harness, boot, standard_bars, ws_book, ws_kline

from paperbot.timeutil import US_PER_MS
from paperbot_net.runner import INPUTS_PER_CHUNK, StampedQueue, owner_turn

MS = US_PER_MS
BARS = standard_bars()
LAG = 2_500 * MS  # above the 2 s quote freshness limit


class FakeClock:
    def __init__(self, t: int):
        self.t = t

    def now_us(self) -> int:
        return self.t


def turn(h: Harness, inbox: StampedQueue, clock: FakeClock, since: int = 0) -> int:
    return owner_turn(h.session, inbox, clock, store=h.engine.store, notifier=None, tick_s=0.0, since_chunk=since)


def quote_text(h: Harness, mid) -> str:
    h.u += 1
    return ws_book(h.u, mid)


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path, write_forward_config(tmp_path), FakeMarket(BARS))
    yield harness
    harness.close()


def booted(h: Harness) -> int:
    t = boot(h, BARS[249].end_us + 10_000 * MS)
    assert h.st.health.blocks == []
    return t


def lag_events(h: Harness) -> list[dict]:
    return [{**json.loads(r["detail"]), "seq": r["seq"]}
            for r in rows(h.state, "SELECT seq, detail FROM health_events WHERE kind = 'feed_owner_lag' ORDER BY id")]


def test_a_delayed_breakout_candle_meets_the_lag_block_before_any_entry_decision(h):
    t = booted(h)
    bb = BARS[250]
    h.run_quotes(t, bb.end_us + 300 * MS, bb.close)  # fresh quotes up to the close (no lag)
    clock = FakeClock(bb.end_us + 400 * MS)
    inbox = StampedQueue(clock)
    inbox.put("ws_message", {"conn": 1, "text": ws_kline(bb)})  # received 400 ms after the close ...
    clock.t += LAG  # ... and reached by the owner 2.5 s later
    turn(h, inbox, clock)
    (c,) = rows(h.state, "SELECT * FROM candidates WHERE bar_start_us = ?", (bb.start_us,))
    assert c["status"] == "skipped" and c["skip_reason"] == "entry_block:owner_lag", c
    assert rows(h.state, "SELECT * FROM orders") == []  # no entry was ever submitted
    lag, cleared = lag_events(h)  # set before the candle; cleared by the tick once the queue is empty
    assert lag["lagging"] and lag["lag_ms"] == 2500 and lag["seq"] < c["seq"]  # the block came first
    assert not cleared["lagging"] and cleared["seq"] > c["seq"]


def test_the_first_delayed_fill_quote_cancels_the_entry_instead_of_filling_it(h):
    t = booted(h)
    bb = BARS[250]
    h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)  # processed without lag: entry submitted, ready after the modeled latency
    o = h.st.order
    assert o is not None and o.purpose == "entry"
    clock = FakeClock(o.ready_us + 50 * MS)
    inbox = StampedQueue(clock)
    inbox.put("ws_message", {"conn": 1, "text": quote_text(h, bb.close + 2)})  # would fill this entry
    clock.t += LAG
    turn(h, inbox, clock)
    assert rows(h.state, "SELECT * FROM fills") == []  # the reviewed code bought 0.00292 BTC here
    (row,) = rows(h.state, "SELECT status, outcome_reason FROM orders")
    assert row["status"] == "canceled" and row["outcome_reason"] == "entry_block:owner_lag"
    assert h.st.position is None and h.st.balances.btc_total == 0
    # the quote itself was still processed, after the block, and nothing was rejected as out of order
    assert rows(h.state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'quote' AND seq > ?",
                (lag_events(h)[0]["seq"],))[0]["n"] == 1
    assert rows(h.state, "SELECT count(*) AS n FROM input_log WHERE disposition = 'rejected'")[0]["n"] == 0


def test_exits_still_run_on_delayed_quotes_while_entries_are_blocked(h):
    t = booted(h)
    bb = BARS[250]
    t = h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    h.run_quotes(bb.end_us + 800 * MS, bb.end_us + 2000 * MS, bb.close + 2)
    pos = h.st.position
    assert pos is not None
    clock = FakeClock(bb.end_us + 2_100 * MS)
    inbox = StampedQueue(clock)
    inbox.put("ws_message", {"conn": 1, "text": quote_text(h, pos.stop_price - 5)})  # stop touched
    clock.t += 400 * MS
    inbox.put("ws_message", {"conn": 1, "text": quote_text(h, pos.stop_price - 6)})  # after the exit latency
    clock.t += LAG
    turn(h, inbox, clock)
    assert "owner_lag" in h.st.health.blocks
    assert h.st.order is not None and h.st.order.purpose == "exit"  # the exit is submitted while lagging
    turn(h, inbox, clock)
    (sell,) = rows(h.state, "SELECT * FROM fills WHERE side = 'SELL'")
    assert sell["quote_recv_us"] >= sell["ready_us"] and h.st.position is None
    assert rows(h.state, "SELECT reason FROM exit_intents")[0]["reason"] == "stop"


def test_held_candle_released_by_a_forced_final_chunk_keeps_a_valid_timestamp(h):
    """Reviewer's reproduction: final warm-up chunk outstanding, a closed live candle (held), then sampled-out quotes
    that advance no engine clock, with another input still queued when the chunk is forced."""
    now = BARS[249].end_us + 10_000 * MS
    h.session.backfill_chunk = 20
    h.start(now)
    h.answer_all(now + 100 * MS)
    for _ in range(12):  # 250 warm-up bars: leave the final chunk (10 bars) outstanding
        h.session.work()
    a = h.session.applying
    assert a is not None and len(a["candles"]) - a["i"] == 10
    bb = BARS[250]
    end = bb.end_us
    clock = FakeClock(end - 200 * MS)
    inbox = StampedQueue(clock)
    inbox.put("ws_message", {"conn": 1, "text": quote_text(h, bb.close)})  # forwarded: last engine clock
    clock.t = end + 500 * MS
    inbox.put("ws_message", {"conn": 1, "text": ws_kline(bb)})  # closed live candle: held
    for k in range(INPUTS_PER_CHUNK):  # sampled out (within 1 s of the forwarded quote): no engine event
        clock.t = end + 600 * MS + k * 10 * MS
        inbox.put("ws_message", {"conn": 1, "text": quote_text(h, bb.close + 1)})
    clock.t = end + 1_300 * MS
    inbox.put("ws_message", {"conn": 1, "text": quote_text(h, bb.close + 2)})  # forwarded, still queued
    since, forced_with_queue = 0, None
    for _ in range(100):
        was_pending = h.session.pending_work()
        since = turn(h, inbox, clock, since)
        if was_pending and not h.session.pending_work():
            forced_with_queue = inbox.q.qsize()
        if inbox.q.empty() and not h.session.pending_work():
            break
    assert forced_with_queue and forced_with_queue >= 1  # the chunk was forced while inputs were still queued
    assert h.session.stats["quote_sampled_out"] >= INPUTS_PER_CHUNK - 2
    rejected = rows(h.state, "SELECT event_type, detail FROM input_log WHERE disposition = 'rejected'")
    assert rejected == [], rejected  # the reviewed code rejected the candle: received_before_interval_end
    (bar,) = rows(h.state, "SELECT * FROM input_log WHERE event_type = 'candle' AND event_id = ?",
                  (f"k1m:{bb.start_us // MS}",))
    assert bar["recv_us"] >= end + 500 * MS  # stamped at processing time, after its own receipt ...
    assert bar["recv_us"] <= end + 1_300 * MS  # ... and before the input still queued at the time
    assert h.st.strategy.last_start_us == bb.start_us
    assert json.loads(bar["detail"]) == {}  # lateness measured at processing time: under 5 s, not late
