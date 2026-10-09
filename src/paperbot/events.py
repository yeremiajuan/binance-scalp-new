"""Normalized input events and the synthetic JSONL input file.

The input file's first line is a header that must declare
``"evidence": "SYNTHETIC"``: Phase 1 replays synthetic fixtures only. Each
following line is one event. Its position (1-based, after the header) is the
replay sequence number and the cursor unit.

A malformed line does not abort replay: it is committed as a rejected input
with its reason, so the cursor and evidence stay consistent.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .money import to_dec
from .timeutil import MINUTE_US, parse_ts


class InputError(ValueError):
    pass


@dataclass(frozen=True)
class CandleEvent:
    event_id: str
    recv_us: int
    start_us: int
    end_us: int | None  # supplied exclusive end, if any (validated against start+60s)
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    final: bool
    interval: str


@dataclass(frozen=True)
class QuoteEvent:
    event_id: str
    recv_us: int
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    exchange_us: int | None
    update_id: int | None


@dataclass(frozen=True)
class ReferenceEvent:
    event_id: str
    recv_us: int
    avg_price: Decimal
    mins: int
    synthetic: bool


@dataclass(frozen=True)
class HeartbeatEvent:
    event_id: str
    recv_us: int


Event = CandleEvent | QuoteEvent | ReferenceEvent | HeartbeatEvent


@dataclass(frozen=True)
class RawEvent:
    seq: int
    event: Event | None
    error: str | None
    event_id: str
    event_type: str
    recv_us: int | None


@dataclass(frozen=True)
class InputFile:
    path: str
    sha256: str
    header: dict
    events: list[RawEvent]


def _dec(obj: dict, key: str) -> Decimal:
    if key not in obj:
        raise InputError(f"missing {key}")
    try:
        return to_dec(obj[key], key)
    except (TypeError, ValueError) as exc:
        raise InputError(str(exc)) from exc


def _opt_ts(obj: dict, key: str) -> int | None:
    v = obj.get(key)
    return None if v is None else parse_ts(v, key)


def parse_event(obj: object) -> Event:
    if not isinstance(obj, dict):
        raise InputError("event must be an object")
    etype = obj.get("type")
    eid = obj.get("id")
    if not isinstance(eid, str) or not eid:
        raise InputError("missing event id")
    try:
        recv = parse_ts(obj.get("recv"), "recv")
    except ValueError as exc:
        raise InputError(str(exc)) from exc
    try:
        if etype == "candle":
            final = obj.get("final")
            if not isinstance(final, bool):
                raise InputError("candle.final must be a boolean")
            interval = obj.get("interval", "1m")
            return CandleEvent(
                event_id=eid,
                recv_us=recv,
                start_us=parse_ts(obj.get("start"), "start"),
                end_us=_opt_ts(obj, "end"),
                open=_dec(obj, "open"),
                high=_dec(obj, "high"),
                low=_dec(obj, "low"),
                close=_dec(obj, "close"),
                volume=_dec(obj, "volume") if "volume" in obj else Decimal(0),
                final=final,
                interval=str(interval),
            )
        if etype == "quote":
            uid = obj.get("update_id")
            if uid is not None and (isinstance(uid, bool) or not isinstance(uid, int)):
                raise InputError("update_id must be an integer")
            return QuoteEvent(
                event_id=eid,
                recv_us=recv,
                bid=_dec(obj, "bid"),
                bid_qty=_dec(obj, "bid_qty"),
                ask=_dec(obj, "ask"),
                ask_qty=_dec(obj, "ask_qty"),
                exchange_us=_opt_ts(obj, "exchange_time"),
                update_id=uid,
            )
        if etype == "reference_price":
            mins = obj.get("mins")
            if isinstance(mins, bool) or not isinstance(mins, int) or mins <= 0:
                raise InputError("reference_price.mins must be a positive integer")
            if obj.get("synthetic") is not True:
                raise InputError("Phase 1 reference prices must be labeled synthetic=true")
            return ReferenceEvent(eid, recv, _dec(obj, "avg_price"), mins, True)
        if etype == "heartbeat":
            return HeartbeatEvent(eid, recv)
    except ValueError as exc:
        raise InputError(str(exc)) from exc
    raise InputError(f"unknown event type {etype!r}")


def parse_lines(lines: list[str]) -> tuple[dict, list[RawEvent]]:
    if not lines:
        raise InputError("input is empty")
    try:
        header = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise InputError(f"invalid header: {exc}") from exc
    if not isinstance(header, dict) or header.get("type") != "header":
        raise InputError("first line must be a header object")
    if header.get("evidence") != "SYNTHETIC":
        raise InputError("Phase 1 replays SYNTHETIC inputs only: header.evidence must be 'SYNTHETIC'")
    events: list[RawEvent] = []
    for i, line in enumerate(lines[1:], start=1):
        if not line.strip():
            raise InputError(f"line {i + 1}: blank lines are not allowed (they would shift the cursor)")
        try:
            obj = json.loads(line, parse_float=Decimal)
        except json.JSONDecodeError as exc:
            events.append(RawEvent(i, None, f"malformed_json: {exc.msg}", f"seq:{i}", "unknown", None))
            continue
        try:
            ev = parse_event(obj)
            events.append(RawEvent(i, ev, None, ev.event_id, str(obj.get("type")), ev.recv_us))
        except InputError as exc:
            eid = obj.get("id") if isinstance(obj, dict) and isinstance(obj.get("id"), str) else f"seq:{i}"
            et = str(obj.get("type")) if isinstance(obj, dict) else "unknown"
            events.append(RawEvent(i, None, f"malformed_event: {exc}", eid, et, None))
    return header, events


def load_input(path: str | Path) -> InputFile:
    p = Path(path)
    data = p.read_bytes()
    text = data.decode("utf-8")
    header, events = parse_lines(text.splitlines())
    return InputFile(str(p), hashlib.sha256(data).hexdigest(), header, events)


def candle_end(start_us: int) -> int:
    return start_us + MINUTE_US
