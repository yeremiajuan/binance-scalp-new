"""Forward-runner owner logic: deterministic, single-threaded, no network.

``ForwardSession`` turns timestamped observations (WebSocket frames, REST results, feed-status changes and clock
ticks) into engine input events. The threaded I/O around it (``paperbot_net.runner``) only delivers items in
receipt order; every decision is made here or in the engine, and every engine input is committed with its
normalized line, so a recorded session replays to the same decisions and accounting.

Normalization rules (documented in docs/DECISIONS.md, Phase 2):

* Quotes are deduplicated by bookTicker update id (``u``); an id not greater than the last one is dropped.
* Quotes are sampled, never synthesized: a real observation is forwarded when ``quote_sample_ms`` has passed
  since the last forwarded one, or whenever it can matter (an order is pending, quotes are stale, or the bid is at
  or beyond the position's stop/target). A cached quote is never re-sent to refresh freshness.
* Closed 1m klines are sequenced: a bar after a missing minute is held while the gap is backfilled from REST;
  backfilled bars only repair indicators (they can never create an entry), then held bars are processed with
  their processing time as receipt time (so lateness is measured honestly). A failed backfill releases the held
  bars and the engine resets its indicators.
* Heartbeats (local clock only) are sent when no other input was processed for ``heartbeat_ms`` so staleness,
  missing candles and timeouts are detected even when no message arrives.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal

from .engine import BLOCK_FEED, BLOCK_RECOVERY, Engine
from .events import InputError, RawEvent, canonical_json, parse_event
from .normalize import (
    PayloadError,
    avg_price_rest,
    avg_price_ws,
    book_ticker_quote,
    iso_us,
    kline_candle,
    metadata_event,
    reference_price_rest,
    reference_price_ws,
    rest_kline_candles,
    unwrap,
)
from .timeutil import MINUTE_US, US_PER_MS, US_PER_S, parse_ts

REF_FORWARD_US = 10 * US_PER_S  # forward an unchanged reference observation at most every 10 s
MAX_HELD_BARS = 1000


@dataclass
class Item:
    kind: str  # ws_message | ws_status | rest | control | outbox_result
    recv_us: int
    data: dict


class ForwardSession:
    def __init__(self, engine: Engine, *, session_id: str, restart: bool, raw_sink=None, rest_request=None):
        if engine.cfg.forward is None:
            raise ValueError("forward sessions need a [forward] configuration section")
        self.engine = engine
        self.fwd = engine.cfg.forward
        self.session_id = session_id
        self.restart = restart
        self.raw_sink = raw_sink or (lambda record: None)
        self.rest_request = rest_request or (lambda name, params: None)
        self.n = 0
        self.last_submit_us: int | None = None
        self.last_update_id: int | None = None
        self.last_quote_fwd_us: int | None = None
        self.last_ref: dict[str, tuple[object, int]] = {}
        self.warmup_done = False
        self.held: dict[int, dict] = {}
        self.backfill: tuple[int, int] | None = None  # (from_start_us, requested_at_us)
        self.ws_connected = False
        self.rest_down = False
        self.stats: Counter = Counter()
        self.stopped = False
        self.stop_reason: str | None = None

    # ----------------------------------------------------------------- inputs

    def _id(self, prefix: str) -> str:
        self.n += 1
        return f"{prefix}:{self.session_id}:{self.n}"

    def submit(self, obj: dict) -> str:
        """Commit one normalized input through the engine (one atomic transaction)."""
        line = canonical_json(obj)
        seq = self.engine.state.cursor + 1
        try:
            ev = parse_event(json.loads(line, parse_float=Decimal))
            raw = RawEvent(seq, ev, None, ev.event_id, obj["type"], ev.recv_us, line)
        except (InputError, ValueError) as exc:
            raw = RawEvent(seq, None, f"malformed_event: {exc}", str(obj.get("id", f"seq:{seq}")),
                           str(obj.get("type")), None, line)
        disposition = self.engine.process(raw)
        if raw.recv_us is not None:
            self.last_submit_us = raw.recv_us
        self.stats[f"submitted_{obj['type']}"] += 1
        return disposition

    def feed(self, kind: str, now: int, **detail) -> str:
        return self.submit({"type": "feed", "id": self._id("feed"), "recv": iso_us(now), "kind": kind,
                            "detail": detail})

    # ------------------------------------------------------------------ start

    def start(self, now: int) -> None:
        self.submit({"type": "session_start", "id": f"session:{self.session_id}", "recv": iso_us(now),
                     "session": self.session_id, "restart": self.restart})
        self.rest_request("time", {})
        self.rest_request("metadata", {})
        self.rest_request("avgPrice", {"symbol": "BTCUSDT"})
        self.rest_request("referencePrice", {"symbol": "BTCUSDT"})
        expected = self._expected()
        if expected is not None and now - expected < 990 * MINUTE_US:
            params = {"symbol": "BTCUSDT", "interval": "1m", "startTime": expected // US_PER_MS, "limit": 1000}
        else:
            params = {"symbol": "BTCUSDT", "interval": "1m", "limit": self.fwd.warmup_bars}
        self.backfill = (expected if expected is not None else -1, now)
        self.rest_request("klines", {**params, "purpose": "warmup"})

    # ---------------------------------------------------------------- handle

    def handle(self, item: Item) -> dict | None:
        if item.kind == "ws_message":
            self.raw_sink({"recv_us": item.recv_us, "source": "ws", **item.data})
            self._ws_message(item.data.get("text", ""), item.recv_us)
        elif item.kind == "ws_status":
            self.raw_sink({"recv_us": item.recv_us, "source": "ws_status", **item.data})
            self._ws_status(item.data, item.recv_us)
        elif item.kind == "rest":
            self.raw_sink({"recv_us": item.recv_us, "source": "rest", **{k: v for k, v in item.data.items()
                                                                          if k != "parsed"}})
            self._rest(item.data, item.recv_us)
        elif item.kind == "control":
            return self.control(item.data, item.recv_us)
        else:
            raise ValueError(f"unknown item kind {item.kind!r}")
        return None

    def _ws_message(self, text: str, now: int) -> None:
        try:
            stream, data = unwrap(json.loads(text))
        except (ValueError, PayloadError):
            self.stats["ws_unparseable"] += 1
            return
        etype = data.get("e")
        try:
            if etype == "kline":
                obj = kline_candle(data, now)
                if obj is None:
                    self.stats["kline_unfinished"] += 1
                else:
                    self._live_bar(obj, now)
            elif etype == "avgPrice":
                self._reference("avg", avg_price_ws(data, now), (data.get("w"), data.get("i")), now)
            elif etype == "referencePrice":
                self._reference("ref", reference_price_ws(data, now), data.get("r"), now)
            elif etype == "serverShutdown":
                self.stats["server_shutdown"] += 1
            elif etype is None and "u" in data and "b" in data and "a" in data:
                self._quote(book_ticker_quote(data, now), now)
            elif "result" in data or "id" in data:
                self.stats["ws_ack"] += 1
            else:
                self.stats["ws_ignored"] += 1
        except PayloadError:
            self.stats["payload_error"] += 1

    def _ws_status(self, data: dict, now: int) -> None:
        if data.get("kind") == "ws_connected":
            self.ws_connected = True
            self.feed("ws_connected", now, conn=data.get("conn"))
        elif data.get("kind") == "ws_disconnected":
            self.ws_connected = False
            self.feed("ws_disconnected", now, conn=data.get("conn"), reason=data.get("reason"))

    # ----------------------------------------------------------------- quotes

    def _quote(self, obj: dict, now: int) -> None:
        u = obj["update_id"]
        if self.last_update_id is not None and u <= self.last_update_id:
            self.stats["quote_duplicate_or_out_of_order"] += 1
            return
        self.last_update_id = u
        st = self.engine.state
        pos = st.position
        bid = Decimal(obj["bid"])
        must = (not st.health.quotes_fresh or st.order is not None or self.last_quote_fwd_us is None
                or now - self.last_quote_fwd_us >= self.fwd.quote_sample_ms * US_PER_MS
                or (pos is not None and (bid <= pos.stop_price or bid >= pos.target_price)))
        if not must:
            self.stats["quote_sampled_out"] += 1
            return
        self.last_quote_fwd_us = now
        self.submit(obj)

    def _reference(self, kind: str, obj: dict, value: object, now: int) -> None:
        last = self.last_ref.get(kind)
        if last is not None and last[0] == value and now - last[1] < REF_FORWARD_US:
            self.stats[f"{kind}_unchanged"] += 1
            return
        self.last_ref[kind] = (value, now)
        self.submit(obj)

    # ---------------------------------------------------------------- candles

    def _expected(self) -> int | None:
        last = self.engine.state.strategy.last_start_us
        return None if last is None else last + MINUTE_US

    def _live_bar(self, obj: dict, now: int) -> None:
        start = parse_ts(obj["start"])
        expected = self._expected()
        if expected is not None and start < expected:
            self.stats["bar_old_or_duplicate"] += 1
            return
        if self.warmup_done and self.backfill is None and not self.held and (expected is None or start == expected):
            self.submit(obj)
            return
        if len(self.held) >= MAX_HELD_BARS:
            self.held.pop(min(self.held))
        self.held[start] = {**obj, "ws_recv": obj["recv"]}
        if self.warmup_done and self.backfill is None and expected is not None and start > expected:
            self._request_backfill(expected, start, now)
        self._flush(now)

    def _request_backfill(self, expected: int, first_held: int, now: int) -> None:
        self.backfill = (expected, now)
        self.stats["backfill_requests"] += 1
        self.rest_request("klines", {"symbol": "BTCUSDT", "interval": "1m", "startTime": expected // US_PER_MS,
                                     "endTime": (first_held - 1) // US_PER_MS, "limit": 1000,
                                     "purpose": "backfill"})

    def _flush(self, now: int, force: bool = False) -> None:
        if not self.warmup_done or (self.backfill is not None and not force):
            return
        while self.held:
            expected = self._expected()
            start = min(self.held)
            if expected is not None and start < expected:
                self.held.pop(start)
                continue
            if expected is not None and start > expected and not force:
                self._request_backfill(expected, start, now)
                return
            obj = self.held.pop(start)
            self.submit({**obj, "recv": iso_us(now)})  # processed now: lateness is measured at processing time

    def _backfill_result(self, candles: list[dict], now: int, purpose: str) -> None:
        upper = min(self.held) if self.held else None
        submitted = 0
        for c in candles:
            start = parse_ts(c["start"])
            expected = self._expected()
            if expected is not None and start < expected:
                continue
            if upper is not None and start >= upper:
                break
            if expected is not None and start > expected:
                break  # REST itself has a hole: stop; the engine will see the gap
            self.submit({**c, "recv": iso_us(now)})
            submitted += 1
        self.stats[f"{purpose}_bars"] += submitted
        self.backfill = None
        if purpose == "warmup":
            self.warmup_done = True
        # after warm-up a remaining hole is backfilled; after a gap backfill, held bars are released even if REST
        # itself had a hole (the engine then resets its indicators rather than forward-filling)
        self._flush(now, force=purpose == "backfill")

    # ------------------------------------------------------------------- REST

    def _rest(self, data: dict, now: int) -> None:
        name = data.get("name")
        ok = data.get("ok", False)
        if not ok:
            self.stats[f"rest_error_{name}"] += 1
            if data.get("rate_limited") or data.get("banned"):
                if not self.rest_down:
                    self.rest_down = True
                    self.feed("rest_unavailable", now, reason=data.get("error"), endpoint=name,
                              banned=bool(data.get("banned")))
            if name == "klines" and data.get("params", {}).get("purpose") in ("warmup", "backfill"):
                pass  # the backfill timeout in tick() decides when to give up
            return
        if self.rest_down:
            self.rest_down = False
            self.feed("rest_ok", now, endpoint=name)
        body = data.get("parsed")
        try:
            if name == "time":
                offset = body["serverTime"] * US_PER_MS - (data["sent_us"] + now) // 2
                self.feed("clock_offset", now, offset_us=offset, round_trip_us=now - data["sent_us"])
            elif name == "metadata":
                self.submit(metadata_event(body["exchangeInfo"], body.get("executionRules"), data["host"],
                                           data.get("fetched_us", now), now))
            elif name == "avgPrice":
                self.submit(avg_price_rest(body, now))
            elif name == "referencePrice":
                self.submit(reference_price_rest(body, now))
            elif name == "klines":
                purpose = data.get("params", {}).get("purpose", "backfill")
                self._backfill_result(rest_kline_candles(body, now, now), now, purpose)
        except (PayloadError, KeyError, TypeError) as exc:
            self.stats[f"rest_payload_error_{name}"] += 1
            if name == "metadata":
                self.feed("rest_unavailable", now, reason=f"metadata payload error: {exc}", endpoint=name,
                          banned=False)
                self.rest_down = True

    # ------------------------------------------------------------------- tick

    def tick(self, now: int) -> None:
        if self.backfill is not None and now - self.backfill[1] >= self.fwd.backfill_timeout_s * US_PER_S:
            purpose = "warmup" if not self.warmup_done else "backfill"
            self.feed("backfill_failed", now, purpose=purpose, held_bars=len(self.held))
            self.backfill = None
            self.warmup_done = True
            self._flush(now, force=True)
        if self.last_submit_us is None or now - self.last_submit_us >= self.fwd.heartbeat_ms * US_PER_MS:
            self.submit({"type": "heartbeat", "id": self._id("hb"), "recv": iso_us(now)})
        st = self.engine.state
        if (BLOCK_RECOVERY in st.health.blocks and not st.health.recovery_signaled and self.warmup_done
                and self.backfill is None and self.ws_connected and BLOCK_FEED not in st.health.blocks
                and st.metadata_hash is not None and st.health.quotes_fresh and st.last_quote is not None
                and now - st.last_quote.recv_us <= self.engine.cfg.quote_max_age_us):
            self.feed("recovered", now, note="warm-up/backfill done, stream connected, fresh quote, metadata")

    # ---------------------------------------------------------------- control

    def control(self, cmd: dict, now: int) -> dict:
        """Local controls routed through the single owner while the runner is active."""
        try:
            kind = cmd.get("cmd")
            wall = str(cmd.get("wall_utc") or "")
            if kind == "kill":
                self.engine.apply_control("kill", "manual_kill", str(cmd.get("reason", "")), wall)
                return {"ok": True, "result": "manual_kill latched; effects apply at the next input (<= heartbeat)"}
            if kind == "reset":
                self.engine.apply_control("reset", str(cmd.get("latch")), str(cmd.get("reason", "")), wall)
                return {"ok": True, "result": f"{cmd.get('latch')} reset (audited)"}
            if kind == "stop":
                self.stopped = True
                self.stop_reason = str(cmd.get("reason") or "local stop command")
                return {"ok": True, "result": "graceful stop requested"}
            if kind == "ping":
                return {"ok": True, "result": "owner alive", "cursor": self.engine.state.cursor,
                        "session": self.session_id}
            return {"ok": False, "error": f"unknown command {kind!r}"}
        except (ValueError, RuntimeError) as exc:
            return {"ok": False, "error": str(exc)}

    def stop(self, now: int, reason: str) -> None:
        self.feed("session_stop", now, reason=reason)
