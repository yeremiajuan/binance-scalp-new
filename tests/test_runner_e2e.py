"""Threaded forward runner against a local WebSocket server and a fake REST transport (no internet).

Exercises the real network clients (websockets sync client, GET-only REST client), reconnect after a server
drop, the control socket used by local CLI commands while the runner is active, both owner locks, raw
recording, the manifest and a graceful stop.
"""

from __future__ import annotations

import json
import time

import pytest
from conftest import rows, write_forward_config
from fake_binance import FakeMarket, FakeRestTransport, FakeStream, SilentStream
from runner_helpers import RunnerThread, poll_count, poll_rows, wait_for

from paperbot.cli import main
from paperbot.storage import StateLocked
from paperbot.synthetic import staircase
from paperbot.timeutil import FIVE_MINUTES_US
from paperbot_net.control import send_control
from paperbot_net.runner import run_forward


@pytest.fixture
def live(tmp_path):
    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    bars = staircase(start, 13)
    market = FakeMarket(bars)
    stream = FakeStream(market.last_close(now))
    cfg = write_forward_config(tmp_path, forward={"reconnect_initial_ms": 100, "reconnect_max_ms": 500,
                                                   "heartbeat_ms": 500, "quote_sample_ms": 500})
    yield tmp_path, cfg, market, stream
    stream.close()


def test_threaded_runner_reconnects_routes_controls_holds_locks_and_stops_gracefully(live):
    tmp, cfg, market, stream = live
    state = tmp / "live.sqlite"
    transport = FakeRestTransport(market)
    runner = RunnerThread(cfg, state, transport=transport, ws_url=f"ws://127.0.0.1:{stream.port}/stream")
    try:
        wait_for(lambda: stream.connections >= 2, "the client to reconnect after the server closed the stream")
        wait_for(lambda: _ping(state), "the owner to answer on its control channel")
        wait_for(lambda: poll_count(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'candle'")
                 >= 290, "the warm-up to be committed")
        # both owner locks are held: a second runner (same path) and a second state (same account) both fail
        with pytest.raises(StateLocked):
            run_forward(str(cfg), str(state), run_seconds=1, transport=transport, install_signals=False,
                        log=lambda *_: None)
        with pytest.raises(StateLocked, match="paper account"):
            run_forward(str(cfg), str(tmp / "other.sqlite"), run_seconds=1, transport=transport,
                        install_signals=False, log=lambda *_: None)
        assert not (tmp / "other.sqlite").exists() or (tmp / "other.sqlite").stat().st_size == 0
        # local controls while active are routed through the owner (single writer)
        assert main(["kill", "--state", str(state), "--reason", "e2e kill while active"]) == 0
        assert main(["status", "--state", str(state)]) == 0  # read-only status works during the run
        assert main(["stop", "--state", str(state), "--reason", "e2e graceful stop"]) == 0
        assert runner.stop() == 0  # already stopping: waits for the owner to finish
    finally:
        stream.stop.set()
        if runner.running():
            runner.stop()

    feed = [h["kind"] for h in rows(state, "SELECT kind FROM health_events ORDER BY id")]
    assert feed.count("feed_ws_connected") >= 2 and "feed_ws_disconnected" in feed
    assert "feed_session_stop" in feed
    assert rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type='quote' AND disposition='accepted'"
                )[0]["n"] >= 3
    # regression: ticks are never stamped ahead of queued observations, so nothing is lost as out of order
    assert rows(state, "SELECT count(*) AS n FROM input_log WHERE detail LIKE '%non_monotonic%'")[0]["n"] == 0
    (ctl,) = rows(state, "SELECT * FROM control_events")
    assert ctl["kind"] == "kill" and ctl["reason"] == "e2e kill while active"
    (sess,) = rows(state, "SELECT * FROM sessions")
    assert sess["stop_reason"] == "e2e graceful stop" and sess["end_cursor"] > 260
    man = {r["key"]: r["value"] for r in rows(state, "SELECT * FROM manifest")}
    assert {"code_revision", "schema_version", "configuration", "data_sources", "reporting_timezone",
            "evaluation_rules"} <= set(man)
    raw = list((tmp / "recordings" / "test-account").glob("*/raw-0001.jsonl.gz"))
    assert raw and raw[0].stat().st_size > 500
    import gzip

    lines = gzip.open(raw[0], "rt", encoding="utf-8").read().splitlines()
    sources = {json.loads(x)["source"] for x in lines}
    assert {"ws", "ws_status", "rest"} <= sources and len(lines) > 20  # every raw frame/response is kept
    assert all(u.startswith("https://api.binance.com/api/v3/") for u in transport.urls)
    st = rows(state, "SELECT json FROM engine_state")[0]["json"]
    assert "manual_kill" in st


def _ping(state) -> bool:
    try:
        return bool(send_control(str(state), {"cmd": "ping"}, timeout=5).get("ok"))
    except (OSError, EOFError, ValueError):
        return False


def _health(state) -> list[dict]:
    return poll_rows(state, "SELECT id, seq, kind, detail FROM health_events ORDER BY id")


def silent_setup(tmp_path, quotes: int = 3):
    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    market = FakeMarket(staircase(start, 13))
    stream = SilentStream(market.last_close(now), quotes=quotes)
    cfg = write_forward_config(tmp_path, forward={"ws_silence_s": 2, "reconnect_initial_ms": 100,
                                                   "reconnect_max_ms": 200, "heartbeat_ms": 500})
    return market, stream, cfg


def stale_after_fresh(state) -> dict | None:
    """The first quotes_stale detected by the clock (after quotes were fresh), not the session-start one."""
    seen_fresh = False
    for h in _health(state):
        if h["kind"] == "quotes_fresh":
            seen_fresh = True
        elif h["kind"] == "quotes_stale" and seen_fresh:
            return h
    return None


def test_silent_open_stream_is_detected_without_any_message(tmp_path):
    """Readiness-based: each condition is awaited (bounded) instead of assumed to happen within a fixed window,
    which failed on a 1-vCPU VPS where committing the warm-up alone took most of the old 6 s window."""
    market, stream, cfg = silent_setup(tmp_path)
    state = tmp_path / "silent.sqlite"
    runner = RunnerThread(cfg, state, transport=FakeRestTransport(market),
                          ws_url=f"ws://127.0.0.1:{stream.port}/stream")
    try:
        wait_for(lambda: any(h["kind"] == "quotes_fresh" for h in _health(state)), "the first quotes")
        stale = wait_for(lambda: stale_after_fresh(state), "quotes_stale detected by local heartbeats")
        wait_for(lambda: [h for h in _health(state) if h["kind"] == "feed_ws_disconnected" and "silent" in
                          h["detail"]], "the client to close the silent connection itself")
        wait_for(lambda: stream.connections >= 2, "a reconnect")
        wait_for(lambda: poll_count(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'heartbeat' AND "
                                     "seq > ?", (stale["seq"],)) >= 4,
                 "4 heartbeats after the stale detection (the clock advances with no message)")
    finally:
        code = runner.stop()
        stream.close()
    assert code == 0
    hs = _health(state)
    kinds = [h["kind"] for h in hs]
    assert "quotes_stale" in kinds  # detected by local heartbeats, no message needed
    disc = [h for h in hs if h["kind"] == "feed_ws_disconnected"]
    assert any("silent" in h["detail"] for h in disc)  # the client closed the silent connection itself
    assert stream.connections >= 2  # and reconnected
    beats = rows(state, "SELECT count(*) AS n FROM input_log WHERE event_type = 'heartbeat'")[0]["n"]
    assert beats >= 4


class DeadTransport:
    def __init__(self):
        self.calls = 0

    def get(self, url):
        self.calls += 1
        raise OSError("[Errno -2] Name or service not known (simulated: no connectivity)")


def test_missing_connectivity_is_reported_and_blocks_entries_not_concealed(tmp_path, capsys):
    cfg = write_forward_config(tmp_path, forward={"reconnect_initial_ms": 100, "reconnect_max_ms": 300,
                                                   "backfill_timeout_s": 2})
    state = tmp_path / "offline.sqlite"
    transport = DeadTransport()

    def no_route(url, timeout):
        raise ConnectionRefusedError("simulated: no route to market stream")

    runner = RunnerThread(cfg, state, transport=transport, ws_connect=no_route)
    try:
        wait_for(lambda: any(h["kind"] == "feed_backfill_failed" for h in _health(state)),
                 "the warm-up to time out (no REST connectivity)")
        wait_for(lambda: transport.calls >= 5, "repeated REST attempts")
    finally:
        code = runner.stop()
    assert code == 0 and transport.calls >= 5
    hs = rows(state, "SELECT kind, detail FROM health_events ORDER BY id")
    assert any(h["kind"] == "feed_ws_disconnected" and "no route" in h["detail"] for h in hs)
    assert any(h["kind"] == "feed_backfill_failed" for h in hs)
    st = json.loads(rows(state, "SELECT json FROM engine_state")[0]["json"])
    assert {"feed_disconnected", "restart_recovery"} <= set(st["health"]["blocks"])
    assert rows(state, "SELECT * FROM orders") == [] and st["metadata_hash"] is None
    assert main(["status", "--state", str(state)]) == 0
    out = capsys.readouterr().out
    assert "stream DISCONNECTED" in out and "metadata NONE" in out and "restart_recovery" in out
