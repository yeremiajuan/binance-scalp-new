"""Threaded forward runner against a local WebSocket server and a fake REST transport (no internet).

Exercises the real network clients (websockets sync client, GET-only REST client), reconnect after a server
drop, the control socket used by local CLI commands while the runner is active, both owner locks, raw
recording, the manifest and a graceful stop.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
from decimal import Decimal

import pytest
from conftest import rows, write_forward_config
from fake_binance import FakeMarket, ws_avg, ws_book, ws_ref
from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from paperbot.cli import main
from paperbot.storage import StateLocked
from paperbot.synthetic import staircase
from paperbot.timeutil import FIVE_MINUTES_US
from paperbot_net.rest import HttpResponse
from paperbot_net.runner import run_forward


class FakeRestTransport:
    def __init__(self, market: FakeMarket):
        self.market = market
        self.urls: list[str] = []

    def get(self, url: str) -> HttpResponse:
        self.urls.append(url)
        parsed = urllib.parse.urlparse(url)
        assert parsed.scheme == "https" and parsed.hostname == "api.binance.com"
        q = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        for k in ("startTime", "endTime", "limit"):
            if k in q:
                q[k] = int(q[k])
        name = parsed.path.rsplit("/", 1)[-1]
        now = time.time_ns() // 1000
        if name == "exchangeInfo":
            body = self.market.info
        elif name == "executionRules":
            body = self.market.rules
        else:
            body = self.market.answer(name, q, now)
        return HttpResponse(200, {"x-mbx-used-weight-1m": "30"}, json.dumps(body).encode())


class FakeStream:
    """Local market-stream server: quotes every 100 ms; drops the first connection after ~1 s."""

    def __init__(self, mid: Decimal):
        self.mid = mid
        self.connections = 0
        self.u = 10_000
        self.stop = threading.Event()
        self.server = serve(self.handler, "127.0.0.1", 0)
        self.port = self.server.socket.getsockname()[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def handler(self, ws):
        try:
            self._serve(ws)
        except ConnectionClosed:
            pass  # the client went away (graceful stop)

    def _serve(self, ws):
        self.connections += 1
        first = self.connections == 1
        now_ms = time.time_ns() // 1_000_000
        ws.send(ws_avg(self.mid, now_ms))
        ws.send(ws_ref(None, now_ms))
        sent = 0
        while not self.stop.is_set():
            self.u += 1
            ws.send(ws_book(self.u, self.mid))
            sent += 1
            if first and sent >= 10:
                return  # server-side close: the client must reconnect
            time.sleep(0.1)

    def close(self):
        self.stop.set()
        self.server.shutdown()


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
    result: dict = {}

    def runner():
        result["code"] = run_forward(str(cfg), str(state), run_seconds=30, transport=transport,
                                     ws_url=f"ws://127.0.0.1:{stream.port}/stream", install_signals=False,
                                     log=lambda *_: None)

    t = threading.Thread(target=runner)
    t.start()
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and stream.connections < 2:
            time.sleep(0.1)
        assert stream.connections >= 2, "client did not reconnect after the server closed the stream"
        time.sleep(1.0)
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
        t.join(timeout=20)
        assert not t.is_alive() and result["code"] == 0
    finally:
        stream.stop.set()
        t.join(timeout=20)

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

    lines = gzip.open(raw[0], "rt").read().splitlines()
    sources = {json.loads(x)["source"] for x in lines}
    assert {"ws", "ws_status", "rest"} <= sources and len(lines) > 20  # every raw frame/response is kept
    assert all(u.startswith("https://api.binance.com/api/v3/") for u in transport.urls)
    st = rows(state, "SELECT json FROM engine_state")[0]["json"]
    assert "manual_kill" in st


class SilentStream(FakeStream):
    """Sends a few quotes, then keeps the connection open but silent."""

    def _serve(self, ws):
        self.connections += 1
        for _ in range(3):
            self.u += 1
            ws.send(ws_book(self.u, self.mid))
            time.sleep(0.1)
        while not self.stop.is_set():
            time.sleep(0.1)


def test_silent_open_stream_is_detected_without_any_message(tmp_path):
    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    market = FakeMarket(staircase(start, 13))
    stream = SilentStream(market.last_close(now))
    cfg = write_forward_config(tmp_path, forward={"ws_silence_s": 2, "reconnect_initial_ms": 100,
                                                   "reconnect_max_ms": 200, "heartbeat_ms": 500})
    state = tmp_path / "silent.sqlite"
    try:
        code = run_forward(str(cfg), str(state), run_seconds=6, transport=FakeRestTransport(market),
                           ws_url=f"ws://127.0.0.1:{stream.port}/stream", install_signals=False, log=lambda *_: None)
    finally:
        stream.close()
    assert code == 0
    hs = rows(state, "SELECT kind, detail FROM health_events ORDER BY id")
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

    code = run_forward(str(cfg), str(state), run_seconds=4, transport=transport, ws_connect=no_route,
                       install_signals=False, log=lambda *_: None)
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
