"""Mocked protocol tests: payload parsing and the GET-only REST client (no network)."""

from __future__ import annotations

import json

import pytest
from fake_binance import FakeMarket, kline_row, standard_bars, ws_avg, ws_book, ws_kline, ws_ref

from paperbot.events import InputError, parse_event
from paperbot.normalize import (
    PayloadError,
    avg_price_ws,
    book_ticker_quote,
    kline_candle,
    metadata_event,
    reference_price_rest,
    reference_price_ws,
    rest_kline_candles,
    unwrap,
)
from paperbot.timeutil import MINUTE_US, US_PER_MS
from paperbot_net.rest import HttpResponse, PublicRest, RateLimited, RestError, Throttled, Unsupported

MS = US_PER_MS
BARS = standard_bars()


def data(text: str) -> dict:
    return unwrap(json.loads(text))[1]


def test_closed_kline_becomes_candle_with_exchange_time_and_unfinished_is_dropped():
    b = BARS[10]
    recv = b.end_us + 700 * MS
    obj = kline_candle(data(ws_kline(b)), recv)
    ev = parse_event(json.loads(json.dumps(obj)))
    assert ev.start_us == b.start_us and ev.end_us == b.end_us and ev.recv_us == recv
    assert ev.exchange_us == b.end_us and ev.final and not ev.backfill and ev.source == "binance_public"
    assert ev.close == b.close and obj["id"] == f"k1m:{b.start_us // MS}"
    assert kline_candle(data(ws_kline(b, closed=False)), recv) is None
    bad = json.loads(ws_kline(b))
    bad["data"]["k"]["T"] += 1000  # does not end the 1m interval
    with pytest.raises(PayloadError):
        kline_candle(unwrap(bad)[1], recv)


def test_book_ticker_has_no_exchange_time_and_keeps_update_id():
    obj = book_ticker_quote(data(ws_book(77, 60000, qty="0.3")), 123456789)
    ev = parse_event(json.loads(json.dumps(obj)))
    assert ev.exchange_us is None  # bookTicker carries no event time: left explicitly absent
    assert ev.update_id == 77 and str(ev.bid) == "59999" and str(ev.ask) == "60001" and str(ev.ask_qty) == "0.3"
    assert ev.recv_us == 123456789  # receipt time is microsecond-exact


def test_references_avg_price_and_reference_price_including_null_and_never_set():
    avg = parse_event(json.loads(json.dumps(avg_price_ws(data(ws_avg("60001.5", 1790000000000)), 5))))
    assert avg.mins == 5 and str(avg.avg_price) == "60001.5" and not avg.synthetic and avg.exchange_us
    ref = parse_event(json.loads(json.dumps(reference_price_ws(data(ws_ref(None, 1790000000000)), 5))))
    assert ref.value is None and ref.exchange_us == 1790000000000 * MS
    ref2 = parse_event(json.loads(json.dumps(reference_price_ws(data(ws_ref("60000.1", 17900)), 5))))
    assert str(ref2.value) == "60000.1"
    never = parse_event(json.loads(json.dumps(reference_price_rest({"never_set": True}, 9))))
    assert never.value is None and never.exchange_us is None


def test_rest_klines_exclude_the_open_bar_and_are_backfill():
    m = FakeMarket(BARS)
    now = BARS[100].start_us + 30_000 * MS  # bar 100 is still open
    rows = m.klines({"limit": 5}, now)
    assert rows[-1][0] == BARS[100].start_us // MS  # Binance includes the open bar...
    candles = rest_kline_candles(rows, now, now)
    assert [c["id"] for c in candles] == [f"k1m:{b.start_us // MS}" for b in BARS[96:100]]  # ...we drop it
    assert all(c["backfill"] for c in candles)
    with pytest.raises(PayloadError):
        rest_kline_candles([[0, "1", "1", "1", "1", "1", MINUTE_US // MS]], now, now)  # 1 ms too long


def test_metadata_bundle_is_versioned_by_content_and_requires_execution_rules():
    m = FakeMarket(BARS)
    a = metadata_event(m.info, m.rules, "api.binance.com", 1_000_000, 2_000_000)
    b = metadata_event(m.info, m.rules, "api.binance.com", 5_000_000, 6_000_000)
    ev = parse_event(json.loads(json.dumps(a)))
    assert ev.sha256 == a["sha256"] != metadata_event(m.info, {"symbolRules": []}, "api.binance.com", 1, 2)["sha256"]
    assert a["bundle"] != b["bundle"]  # retrieved_at differs -> distinct versions are preserved
    with pytest.raises(PayloadError):
        metadata_event(m.info, None, "api.binance.com", 1, 2)
    tampered = json.loads(json.dumps(a))
    tampered["bundle"]["symbol"]["status"] = "BREAK"
    with pytest.raises(InputError, match="sha256"):
        parse_event(tampered)  # sha256 no longer matches its bundle


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.urls: list[str] = []

    def get(self, url):
        self.urls.append(url)
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def ok(body, weight="10"):
    return HttpResponse(200, {"x-mbx-used-weight-1m": weight}, json.dumps(body).encode())


def test_rest_client_is_get_only_allowlisted_and_tracks_weight():
    clock = Clock()
    t = FakeTransport([ok({"serverTime": 1}, "21")])
    rest = PublicRest("api.binance.com", t, monotonic=clock, wall=clock)
    assert rest.get("time") == {"serverTime": 1}
    assert t.urls == ["https://api.binance.com/api/v3/time"] and rest.used_weight == 21
    with pytest.raises(ValueError):
        PublicRest("evil.example.com", t)
    with pytest.raises(Unsupported):
        rest.get("order", symbol="BTCUSDT")  # no order (or any non-allowlisted) endpoint exists
    md_only = PublicRest("data-api.binance.vision", t, monotonic=clock, wall=clock)
    with pytest.raises(Unsupported):
        md_only.get("executionRules", symbol="BTCUSDT")  # not served by the market-data-only host


def test_rest_429_and_418_pause_all_requests_for_retry_after():
    clock = Clock()
    t = FakeTransport([HttpResponse(429, {"retry-after": "7"}, b"{}"),
                       HttpResponse(418, {"retry-after": "120"}, b"{}"), ok({"serverTime": 2})])
    rest = PublicRest("api.binance.com", t, monotonic=clock, wall=clock)
    with pytest.raises(RateLimited) as e:
        rest.get("time")
    assert e.value.retry_after_s == 7 and not e.value.banned
    with pytest.raises(RateLimited):
        rest.get("time")  # paused: nothing is sent
    assert len(t.urls) == 1
    clock.t += 7.1
    with pytest.raises(RateLimited) as e:
        rest.get("time")
    assert e.value.banned and e.value.retry_after_s == 120
    clock.t += 121
    assert rest.get("time") == {"serverTime": 2} and not rest.banned


def test_rest_local_weight_budget_throttles_before_sending():
    clock = Clock()
    clock.t = 60 * 1000 + 1  # start of a minute
    t = FakeTransport([ok({}, "0") for _ in range(10)])
    rest = PublicRest("api.binance.com", t, monotonic=clock, wall=clock, budget_fraction=0.01)  # 60 weight/min
    rest.get("exchangeInfo", symbol="BTCUSDT")  # 20
    rest.get("exchangeInfo", symbol="BTCUSDT")  # 40
    rest.get("exchangeInfo", symbol="BTCUSDT")  # 60
    with pytest.raises(Throttled) as e:
        rest.get("klines", symbol="BTCUSDT", interval="1m")
    assert 0 < e.value.wait_s <= 60 and len(t.urls) == 3
    clock.t += 60
    rest.get("klines", symbol="BTCUSDT", interval="1m")  # new minute


def test_rest_errors_are_never_success_and_reference_never_set_is_explicit():
    clock = Clock()
    t = FakeTransport([HttpResponse(503, {}, b"busy"), HttpResponse(403, {}, b"waf"), OSError("reset"),
                       HttpResponse(400, {}, json.dumps({"code": -2043, "msg": "no reference"}).encode()),
                       HttpResponse(400, {}, json.dumps({"code": -1121, "msg": "Invalid symbol."}).encode())])
    rest = PublicRest("api.binance.com", t, monotonic=clock, wall=clock)
    for _ in range(3):
        with pytest.raises(RestError):
            rest.get("time")
    assert rest.get("referencePrice", symbol="BTCUSDT") == {"never_set": True}
    with pytest.raises(RestError):
        rest.get("referencePrice", symbol="BTCUSDT")
    assert [c.status for c in rest.calls] == [503, 403, None, 400, 400]  # every call recorded for the raw log


def test_kline_row_shape_matches_docs():
    assert len(kline_row(BARS[0])) == 12
