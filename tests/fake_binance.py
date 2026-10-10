"""Deterministic fake of Binance public spot market data, in the documented payload shapes.

Used by Phase 2 tests only (mocked protocol tests; no network). Built from the SYNTHETIC staircase so the
strategy reaches warm-up and signals. Payload shapes follow binance-spot-api-docs (2026-09).
"""

from __future__ import annotations

import json
import threading
import time
import urllib.parse
from decimal import Decimal
from pathlib import Path

from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from paperbot.forward import ForwardSession, Item
from paperbot.money import dtext
from paperbot.recorded import open_forward
from paperbot.storage import StateLock
from paperbot.synthetic import BarSpec, breakout_bar, range_bars, staircase
from paperbot.timeutil import MINUTE_US, US_PER_MS, parse_ts
from paperbot_net.rest import HttpResponse

T0 = parse_ts("2026-10-01T12:00:00Z")
MS = US_PER_MS

FILTERS = [
    {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
    {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
    {"filterType": "ICEBERG_PARTS", "limit": 10},
    {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00000000", "maxQty": "100.00000000", "stepSize": "0.00000000"},
    {"filterType": "TRAILING_DELTA", "minTrailingAboveDelta": 10, "maxTrailingAboveDelta": 2000,
     "minTrailingBelowDelta": 10, "maxTrailingBelowDelta": 2000},
    {"filterType": "PERCENT_PRICE_BY_SIDE", "bidMultiplierUp": "5", "bidMultiplierDown": "0.2",
     "askMultiplierUp": "5", "askMultiplierDown": "0.2", "avgPriceMins": 5},
    {"filterType": "NOTIONAL", "minNotional": "5.00000000", "applyMinToMarket": True,
     "maxNotional": "9000000.00000000", "applyMaxToMarket": False, "avgPriceMins": 5},
    {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
    {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
]


def exchange_info(status: str = "TRADING", filters=None) -> dict:
    return {
        "timezone": "UTC", "serverTime": 1,
        "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 6000},
                       {"rateLimitType": "ORDERS", "interval": "SECOND", "intervalNum": 10, "limit": 100},
                       {"rateLimitType": "RAW_REQUESTS", "interval": "MINUTE", "intervalNum": 5, "limit": 61000}],
        "exchangeFilters": [],
        "symbols": [{
            "symbol": "BTCUSDT", "status": status, "baseAsset": "BTC", "baseAssetPrecision": 8, "quoteAsset": "USDT",
            "quotePrecision": 8, "quoteAssetPrecision": 8, "orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET",
                                                                           "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"],
            "icebergAllowed": True, "ocoAllowed": True, "isSpotTradingAllowed": True, "isMarginTradingAllowed": True,
            "filters": filters if filters is not None else FILTERS, "permissions": [],
            "permissionSets": [["SPOT", "MARGIN"]],
        }],
    }


def execution_rules(up="1.05", down="0.95") -> dict:
    return {"symbolRules": [{"symbol": "BTCUSDT", "rules": [{
        "ruleType": "PRICE_RANGE", "bidLimitMultUp": up, "bidLimitMultDown": down, "askLimitMultUp": up,
        "askLimitMultDown": down}]}]}


def kline_row(b: BarSpec) -> list:
    return [b.start_us // MS, dtext(b.open), dtext(b.high), dtext(b.low), dtext(b.close), "1.5",
            b.end_us // MS - 1, "90000", 10, "0.7", "42000", "0"]


def ws(stream: str, data: dict) -> str:
    return json.dumps({"stream": stream, "data": data})


def ws_kline(b: BarSpec, closed: bool = True, event_ms: int | None = None) -> str:
    return ws("btcusdt@kline_1m", {"e": "kline", "E": event_ms or b.end_us // MS, "s": "BTCUSDT", "k": {
        "t": b.start_us // MS, "T": b.end_us // MS - 1, "s": "BTCUSDT", "i": "1m", "f": 1, "L": 2,
        "o": dtext(b.open), "c": dtext(b.close), "h": dtext(b.high), "l": dtext(b.low), "v": "1.5", "n": 10,
        "x": closed, "q": "90000", "V": "0.7", "Q": "42000", "B": "0"}})


def ws_book(u: int, mid, half="1", qty="0.5", bid_qty=None, ask_qty=None) -> str:
    m, h = Decimal(str(mid)), Decimal(str(half))
    return ws("btcusdt@bookTicker", {"u": u, "s": "BTCUSDT", "b": dtext(m - h), "B": bid_qty or qty,
                                     "a": dtext(m + h), "A": ask_qty or qty})


def ws_avg(price, event_ms: int) -> str:
    return ws("btcusdt@avgPrice", {"e": "avgPrice", "E": event_ms, "s": "BTCUSDT", "i": "5m",
                                   "w": dtext(Decimal(str(price))), "T": event_ms - 5})


def ws_ref(price, t_ms: int) -> str:
    return ws("btcusdt@referencePrice", {"e": "referencePrice", "s": "BTCUSDT",
                                         "r": None if price is None else dtext(Decimal(str(price))), "t": t_ms})


class FakeMarket:
    """Bars (by start) plus answers for REST requests."""

    def __init__(self, bars: list[BarSpec]):
        self.bars = {b.start_us: b for b in bars}
        self.info = exchange_info()
        self.rules = execution_rules()
        self.ref_price: str | None = None  # None -> referencePrice null
        self.server_offset_ms = 0

    def last_close(self, now_us: int) -> Decimal:
        done = [b for b in self.bars.values() if b.end_us <= now_us]
        return max(done, key=lambda b: b.start_us).close if done else Decimal(60000)

    def klines(self, params: dict, now_us: int) -> list:
        rows = sorted(self.bars.values(), key=lambda b: b.start_us)
        if "startTime" in params:
            rows = [b for b in rows if b.start_us >= params["startTime"] * MS]
        if "endTime" in params:
            rows = [b for b in rows if b.start_us <= params["endTime"] * MS]
        rows = [b for b in rows if b.start_us <= now_us]  # includes the currently open bar, like Binance
        limit = params.get("limit", 500)
        rows = rows[:limit] if "startTime" in params else rows[-limit:]
        return [kline_row(b) for b in rows]

    def answer(self, name: str, params: dict, now_us: int) -> dict:
        if name == "time":
            return {"serverTime": now_us // MS + self.server_offset_ms}
        if name == "metadata":
            return {"exchangeInfo": self.info, "executionRules": self.rules}
        if name == "avgPrice":
            return {"mins": 5, "price": dtext(self.last_close(now_us)), "closeTime": now_us // MS - 100}
        if name == "referencePrice":
            return {"symbol": "BTCUSDT", "referencePrice": self.ref_price, "timestamp": now_us // MS - 50}
        if name == "klines":
            return self.klines(params, now_us)
        raise KeyError(name)


def standard_bars(extra_segments: int = 2) -> list[BarSpec]:
    """Warm staircase (10 segments) plus breakouts: bar 250 is the first warm signal."""
    return staircase(T0, 10 + extra_segments)


class Harness:
    """Step-mode forward session (deterministic: explicit times, no threads, no network)."""

    def __init__(self, tmp: Path, cfg_path: Path, market: FakeMarket, state_name: str = "fwd.sqlite",
                 session_id: str = "s1"):
        from paperbot.config import load_config

        self.cfg = load_config(cfg_path)
        self.market = market
        self.state = tmp / state_name
        self.lock = StateLock(self.state).acquire()
        self.engine, self.restart = open_forward(self.lock, self.cfg, provenance="MOCKED")
        self.requests: list[tuple[str, dict]] = []
        self.raw: list[dict] = []
        self.session = ForwardSession(self.engine, session_id=session_id, restart=self.restart,
                                      raw_sink=self.raw.append, rest_request=lambda n, p: self.requests.append(
                                          (n, dict(p))))
        self.u = 1000 + self.engine.state.cursor * 1000  # bookTicker update ids keep increasing across restarts

    @property
    def st(self):
        return self.engine.state

    def close(self):
        self.engine.store.close()
        self.lock.release()

    def start(self, now: int) -> None:
        self.session.start(now)

    def answer_all(self, now: int, fail: set[str] | None = None, errors: set[str] | None = None,
                   only: set[str] | None = None, **overrides) -> None:
        """Answer queued REST requests; ``fail`` -> HTTP 429 (rate limited), ``errors`` -> HTTP 503;
        ``only`` limits the answers to those endpoint names (the rest stay queued)."""
        reqs = [r for r in self.requests if only is None or r[0] in only]
        self.requests = [r for r in self.requests if only is not None and r[0] not in only]
        for i, (name, params) in enumerate(reqs):
            t = now + i * 10 * MS
            if fail and name in fail:
                self.session.handle(Item("rest", t, {"name": name, "params": params, "ok": False,
                                                      "error": "HTTP 429", "rate_limited": True, "sent_us": t}))
                continue
            if errors and name in errors:
                self.session.handle(Item("rest", t, {"name": name, "params": params, "ok": False,
                                                      "error": "HTTP 503", "sent_us": t}))
                continue
            body = overrides.get(name) or self.market.answer(name, params, t)
            data = {"name": name, "params": params, "ok": True, "parsed": body, "sent_us": t - 5 * MS,
                    "host": "api.binance.com", "fetched_us": t}
            self.session.handle(Item("rest", t, data))

    def connect(self, now: int, answer_continuity: bool = True) -> None:
        """Stream connected. After warm-up the runner revalidates candle continuity from REST; by default the
        fake answers that check 50 ms later."""
        self.session.handle(Item("ws_status", now, {"kind": "ws_connected", "conn": 1}))
        if answer_continuity and any(n == "klines" and p.get("purpose") == "backfill" for n, p in self.requests):
            self.answer_all(now + 50 * MS, only={"klines"})

    def disconnect(self, now: int, reason="test") -> None:
        self.session.handle(Item("ws_status", now, {"kind": "ws_disconnected", "conn": 1, "reason": reason}))

    def msg(self, text: str, now: int) -> None:
        self.session.handle(Item("ws_message", now, {"conn": 1, "text": text}))

    def quote(self, now: int, mid, **kw) -> None:
        self.u += 1
        self.msg(ws_book(self.u, mid, **kw), now)

    def bar(self, b: BarSpec, delay_ms: int = 500) -> None:
        self.msg(ws_kline(b), b.end_us + delay_ms * MS)

    def run_quotes(self, t_from: int, t_to: int, mid, every_ms: int = 500, tick: bool = True, **kw) -> int:
        t = t_from
        while t < t_to:
            self.quote(t, mid, **kw)
            if tick:
                self.session.tick(t + 1)
            t += every_ms * MS
        return t

    def tick(self, now: int) -> None:
        self.session.tick(now)


def boot(h: Harness, now: int, ref_value=None) -> int:
    """Start, answer REST, connect, provide fresh quotes and references until the runner signals recovery."""
    h.market.ref_price = ref_value
    h.start(now)
    h.answer_all(now + 100 * MS)
    h.connect(now + 200 * MS)
    h.msg(ws_avg(h.market.last_close(now), now // MS), now + 250 * MS)
    h.msg(ws_ref(ref_value, now // MS), now + 260 * MS)
    mid = h.market.last_close(now)
    t = h.run_quotes(now + 300 * MS, now + 1500 * MS, mid)
    return t


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
    """Local market-stream server: quotes every 100 ms; drops the first connection after ~1 s (unless told not to)."""

    def __init__(self, mid: Decimal, drop_first: bool = True):
        self.mid = mid
        self.drop_first = drop_first
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
        first = self.connections == 1 and self.drop_first
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


class SilentStream(FakeStream):
    """Sends ``quotes`` bookTicker quotes on the first connection (``later_quotes`` on later ones, default the same),
    then keeps the connection open but silent. ``sent_wall`` holds the wall time (seconds) of every quote sent."""

    def __init__(self, mid: Decimal, quotes: int = 3, later_quotes: int | None = None):
        self.quotes = quotes
        self.later_quotes = quotes if later_quotes is None else later_quotes
        self.sent_wall: list[float] = []
        super().__init__(mid, drop_first=False)

    def _serve(self, ws):
        self.connections += 1
        for _ in range(self.quotes if self.connections == 1 else self.later_quotes):
            self.u += 1
            ws.send(ws_book(self.u, self.mid))
            self.sent_wall.append(time.time())
            time.sleep(0.1)
        while not self.stop.is_set():
            time.sleep(0.1)


__all__ = ["FakeMarket", "FakeRestTransport", "FakeStream", "SilentStream", "Harness", "boot", "standard_bars",
           "breakout_bar", "range_bars", "T0", "MINUTE_US", "MS"]
