"""Pure normalization of Binance public spot market-data payloads into engine input events.

Field semantics follow binance-spot-api-docs (web-socket-streams.md, rest-api.md; checked 2026-10-09,
CHANGELOG "Last Updated: 2026-09-18"):

* ``<symbol>@kline_1m``: ``k.t`` open time and ``k.T`` inclusive close time (ms), ``k.x`` closed flag,
  ``E`` event time. Only closed klines become candles; the canonical end is ``k.T + 1 ms``.
* ``<symbol>@bookTicker``: ``u`` order-book update id, ``b``/``B`` best bid/qty, ``a``/``A`` best ask/qty. The
  payload has **no** event time, so ``exchange_time`` stays absent.
* ``<symbol>@avgPrice``: ``i`` interval (e.g. ``5m``), ``w`` average price, ``E`` event time.
* ``<symbol>@referencePrice``: ``r`` reference price (null when none), ``t`` engine timestamp.
* ``GET /api/v3/klines`` rows: [open time, o, h, l, c, v, close time, ...]; the newest row may still be open.

Every observation keeps its local receipt time. Nothing here repeats, ages or fabricates an observation.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from .events import PUBLIC_SOURCE, canonical_json
from .timeutil import MINUTE_US, US_PER_MS, iso

SYMBOL = "BTCUSDT"


class PayloadError(ValueError):
    pass


def _ms(v: object, field: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise PayloadError(f"{field}: expected millisecond integer, got {v!r}")
    return v * US_PER_MS


def _num(v: object, field: str) -> str:
    if not isinstance(v, str):
        raise PayloadError(f"{field}: expected decimal string, got {v!r}")
    try:
        Decimal(v)
    except ArithmeticError as exc:
        raise PayloadError(f"{field}: invalid decimal {v!r}") from exc
    return v


def unwrap(message: object) -> tuple[str | None, dict]:
    """Combined-stream wrapper ``{"stream": ..., "data": ...}`` -> (stream, data)."""
    if not isinstance(message, dict):
        raise PayloadError("stream message must be an object")
    if "stream" in message and "data" in message:
        if not isinstance(message["data"], dict):
            raise PayloadError("combined stream data must be an object")
        return str(message["stream"]), message["data"]
    return None, message


def kline_candle(data: dict, recv_us: int) -> dict | None:
    """Closed 1m kline -> candle event; ``None`` for an unfinished update (recorded raw, never traded on)."""
    if data.get("e") != "kline" or data.get("s") != SYMBOL:
        raise PayloadError("not a BTCUSDT kline event")
    k = data.get("k")
    if not isinstance(k, dict) or k.get("i") != "1m" or k.get("s") != SYMBOL:
        raise PayloadError("kline payload is not BTCUSDT 1m")
    if not isinstance(k.get("x"), bool):
        raise PayloadError("kline.x must be a boolean")
    if not k["x"]:
        return None
    start = _ms(k.get("t"), "k.t")
    end = _ms(k.get("T"), "k.T") + US_PER_MS
    if end != start + MINUTE_US:
        raise PayloadError(f"kline close time {k.get('T')} does not end the 1m interval starting {k.get('t')}")
    return {
        "type": "candle", "id": f"k1m:{k['t']}", "interval": "1m", "start": iso_us(start), "end": iso_us(end),
        "recv": iso_us(recv_us), "open": _num(k.get("o"), "k.o"), "high": _num(k.get("h"), "k.h"),
        "low": _num(k.get("l"), "k.l"), "close": _num(k.get("c"), "k.c"), "volume": _num(k.get("v"), "k.v"),
        "final": True, "exchange_time": iso_us(_ms(data.get("E"), "E")), "source": PUBLIC_SOURCE,
    }


def rest_kline_candles(rows: object, recv_us: int, now_us: int) -> list[dict]:
    """REST klines -> backfill candles. Rows whose interval has not ended by ``now_us`` are dropped."""
    if not isinstance(rows, list):
        raise PayloadError("klines response must be a list")
    out = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 7:
            raise PayloadError("kline row must be a list of at least 7 fields")
        start = _ms(r[0], "openTime")
        end = _ms(r[6], "closeTime") + US_PER_MS
        if end != start + MINUTE_US:
            raise PayloadError("REST kline does not span exactly one minute")
        if end > now_us:
            continue  # still open: never treat an unfinished bar as final
        out.append({
            "type": "candle", "id": f"k1m:{r[0]}", "interval": "1m", "start": iso_us(start), "end": iso_us(end),
            "recv": iso_us(recv_us), "open": _num(r[1], "o"), "high": _num(r[2], "h"), "low": _num(r[3], "l"),
            "close": _num(r[4], "c"), "volume": _num(r[5], "v"), "final": True, "backfill": True,
            "source": PUBLIC_SOURCE,
        })
    out.sort(key=lambda c: c["start"])
    return out


def book_ticker_quote(data: dict, recv_us: int) -> dict:
    if data.get("s") != SYMBOL:
        raise PayloadError("not a BTCUSDT bookTicker")
    u = data.get("u")
    if isinstance(u, bool) or not isinstance(u, int):
        raise PayloadError("bookTicker.u must be an integer")
    return {
        "type": "quote", "id": f"bt:{u}", "recv": iso_us(recv_us), "bid": _num(data.get("b"), "b"),
        "bid_qty": _num(data.get("B"), "B"), "ask": _num(data.get("a"), "a"), "ask_qty": _num(data.get("A"), "A"),
        "update_id": u, "source": PUBLIC_SOURCE,
    }


def _mins(interval: object) -> int:
    if not isinstance(interval, str) or not interval.endswith("m") or not interval[:-1].isdigit():
        raise PayloadError(f"unsupported average-price interval {interval!r}")
    return int(interval[:-1])


def avg_price_ws(data: dict, recv_us: int) -> dict:
    if data.get("e") != "avgPrice" or data.get("s") != SYMBOL:
        raise PayloadError("not a BTCUSDT avgPrice event")
    e = _ms(data.get("E"), "E")
    return {"type": "reference_price", "id": f"avg:ws:{data['E']}", "recv": iso_us(recv_us),
            "avg_price": _num(data.get("w"), "w"), "mins": _mins(data.get("i")), "synthetic": False,
            "exchange_time": iso_us(e), "source": PUBLIC_SOURCE}


def avg_price_rest(body: dict, recv_us: int) -> dict:
    mins = body.get("mins")
    if isinstance(mins, bool) or not isinstance(mins, int) or mins <= 0:
        raise PayloadError("avgPrice.mins must be a positive integer")
    close = _ms(body.get("closeTime"), "closeTime")
    return {"type": "reference_price", "id": f"avg:rest:{body['closeTime']}:{recv_us}", "recv": iso_us(recv_us),
            "avg_price": _num(body.get("price"), "price"), "mins": mins, "synthetic": False,
            "exchange_time": iso_us(close), "source": PUBLIC_SOURCE}


def reference_price_ws(data: dict, recv_us: int) -> dict:
    if data.get("e") != "referencePrice" or data.get("s") != SYMBOL:
        raise PayloadError("not a BTCUSDT referencePrice event")
    r = data.get("r")
    t = _ms(data.get("t"), "t")
    return {"type": "ref_price", "id": f"ref:ws:{data['t']}", "recv": iso_us(recv_us),
            "value": None if r is None else _num(r, "r"), "exchange_time": iso_us(t), "source": PUBLIC_SOURCE}


def reference_price_rest(body: dict, recv_us: int) -> dict:
    """``{"referencePrice": str|null, "timestamp": ms}``; error -2043 (never set) is passed as never_set."""
    if body.get("never_set"):
        return {"type": "ref_price", "id": f"ref:rest:never:{recv_us}", "recv": iso_us(recv_us), "value": None,
                "exchange_time": None, "source": PUBLIC_SOURCE}
    if body.get("symbol") != SYMBOL:
        raise PayloadError("referencePrice response is not for BTCUSDT")
    r = body.get("referencePrice")
    t = _ms(body.get("timestamp"), "timestamp")
    return {"type": "ref_price", "id": f"ref:rest:{body['timestamp']}:{recv_us}", "recv": iso_us(recv_us),
            "value": None if r is None else _num(r, "referencePrice"), "exchange_time": iso_us(t),
            "source": PUBLIC_SOURCE}


def metadata_event(exchange_info: dict, execution_rules: dict | None, rest_host: str, fetched_us: int,
                   recv_us: int) -> dict:
    """exchangeInfo + executionRules -> a PUBLIC metadata bundle event (versioned by content hash)."""
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        raise PayloadError("exchangeInfo.symbols missing")
    sym = [s for s in symbols if isinstance(s, dict) and s.get("symbol") == SYMBOL]
    if len(sym) != 1:
        raise PayloadError("exchangeInfo does not contain exactly one BTCUSDT symbol")
    if execution_rules is None:
        raise PayloadError("executionRules unavailable: the bundle would be incomplete")
    rules = []
    for entry in execution_rules.get("symbolRules", []):
        if isinstance(entry, dict) and entry.get("symbol") == SYMBOL:
            rules.extend(entry.get("rules") or [])
    bundle = {
        "label": "PUBLIC",
        "retrieved_at": iso_us(fetched_us),
        "description": "Public Binance spot exchangeInfo and executionRules for BTCUSDT as fetched by the PAPER "
                       "forward runner. Public metadata does not establish account permissions, private filters "
                       "or commissions.",
        "source": {"rest_host": rest_host, "endpoints": ["/api/v3/exchangeInfo?symbol=BTCUSDT",
                                                         "/api/v3/executionRules?symbol=BTCUSDT"]},
        "reference_mode": "reference_or_avg",
        "rate_limits": exchange_info.get("rateLimits", []),
        "server_time": exchange_info.get("serverTime"),
        "symbol": sym[0],
        "execution_rules": rules,
    }
    # hash the bundle exactly as the engine will see it after a JSON round trip (floats become Decimals)
    sha = hashlib.sha256(canonical_json(json.loads(canonical_json(bundle), parse_float=Decimal)).encode()).hexdigest()
    return {"type": "metadata", "id": f"md:{sha[:16]}:{fetched_us}", "recv": iso_us(recv_us),
            "fetched": iso_us(fetched_us), "sha256": sha, "bundle": bundle}


def iso_us(us: int) -> str:
    """ISO timestamp with microseconds (lossless for receipt times)."""
    s = iso(us)  # milliseconds
    frac = us % 1000
    return s if frac == 0 else s[:-1] + f"{frac:03d}Z"
