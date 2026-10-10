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
    exchange_us: int | None = None  # exchange event time when the payload carries one (WS kline "E")
    backfill: bool = False  # REST backfill/warm-up bar: repairs indicators, never creates an entry
    source: str = "synthetic"


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
    source: str = "synthetic"


@dataclass(frozen=True)
class ReferenceEvent:
    """Weighted average price over ``mins`` minutes (Binance avgPrice)."""

    event_id: str
    recv_us: int
    avg_price: Decimal
    mins: int
    synthetic: bool
    exchange_us: int | None = None
    source: str = "synthetic"


@dataclass(frozen=True)
class RefPriceEvent:
    """Binance referencePrice observation; ``value`` None means "no reference price is set"."""

    event_id: str
    recv_us: int
    value: Decimal | None
    exchange_us: int | None
    source: str


@dataclass(frozen=True)
class MetadataEvent:
    """A versioned exchange-metadata bundle (exchangeInfo symbol + execution rules), as fetched."""

    event_id: str
    recv_us: int
    bundle: str  # canonical JSON text of the bundle
    sha256: str
    fetched_us: int


FEED_KINDS = ("ws_connected", "ws_disconnected", "rest_unavailable", "rest_ok", "clock_offset", "recovered",
              "backfill_failed", "session_stop", "clock_check_failed", "continuity_check", "owner_lag")


@dataclass(frozen=True)
class FeedEvent:
    """A health transition observed by the forward runner (not market data)."""

    event_id: str
    recv_us: int
    kind: str
    detail: str  # canonical JSON


@dataclass(frozen=True)
class SessionStartEvent:
    event_id: str
    recv_us: int
    session_id: str
    restart: bool


@dataclass(frozen=True)
class HeartbeatEvent:
    event_id: str
    recv_us: int


Event = (CandleEvent | QuoteEvent | ReferenceEvent | HeartbeatEvent | RefPriceEvent | MetadataEvent | FeedEvent
         | SessionStartEvent)
EVIDENCE_KINDS = ("SYNTHETIC", "PUBLIC")


@dataclass(frozen=True)
class RawEvent:
    seq: int
    event: Event | None
    error: str | None
    event_id: str
    event_type: str
    recv_us: int | None
    line: str | None = None  # the normalized input line, persisted for recorded public-data sessions


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


PUBLIC_SOURCE = "binance_public"


def canonical_json(obj: object) -> str:
    def enc(x):
        if isinstance(x, Decimal):
            return str(x)
        raise TypeError(type(x).__name__)

    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=enc)


def _opt_bool(obj: dict, key: str) -> bool:
    v = obj.get(key, False)
    if not isinstance(v, bool):
        raise InputError(f"{key} must be a boolean")
    return v


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
                exchange_us=_opt_ts(obj, "exchange_time"),
                backfill=_opt_bool(obj, "backfill"),
                source=str(obj.get("source", "synthetic")),
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
                source=str(obj.get("source", "synthetic")),
            )
        if etype == "reference_price":
            mins = obj.get("mins")
            if isinstance(mins, bool) or not isinstance(mins, int) or mins <= 0:
                raise InputError("reference_price.mins must be a positive integer")
            source = str(obj.get("source", "synthetic"))
            synthetic = obj.get("synthetic")
            if not (synthetic is True or (synthetic is False and source == PUBLIC_SOURCE)):
                raise InputError("reference prices must be labeled synthetic=true, or synthetic=false with "
                                 f"source={PUBLIC_SOURCE!r}")
            return ReferenceEvent(eid, recv, _dec(obj, "avg_price"), mins, synthetic, _opt_ts(obj, "exchange_time"),
                                  source)
        if etype == "heartbeat":
            return HeartbeatEvent(eid, recv)
        if etype == "ref_price":
            if "value" not in obj:
                raise InputError("ref_price.value is required (null means no reference price is set)")
            value = None if obj["value"] is None else _dec(obj, "value")
            return RefPriceEvent(eid, recv, value, _opt_ts(obj, "exchange_time"), str(obj.get("source", "")))
        if etype == "metadata":
            bundle = obj.get("bundle")
            if not isinstance(bundle, dict):
                raise InputError("metadata.bundle must be an object")
            text = canonical_json(bundle)
            sha = hashlib.sha256(text.encode()).hexdigest()
            if obj.get("sha256") not in (None, sha):
                raise InputError("metadata.sha256 does not match its bundle")
            return MetadataEvent(eid, recv, text, sha, parse_ts(obj.get("fetched"), "fetched"))
        if etype == "feed":
            kind = obj.get("kind")
            if kind not in FEED_KINDS:
                raise InputError(f"unknown feed kind {kind!r}")
            detail = obj.get("detail", {})
            if not isinstance(detail, dict):
                raise InputError("feed.detail must be an object")
            return FeedEvent(eid, recv, kind, canonical_json(detail))
        if etype == "session_start":
            sid = obj.get("session")
            if not isinstance(sid, str) or not sid:
                raise InputError("session_start.session is required")
            return SessionStartEvent(eid, recv, sid, _opt_bool(obj, "restart"))
    except ValueError as exc:
        raise InputError(str(exc)) from exc
    raise InputError(f"unknown event type {etype!r}")


def parse_lines(lines: list[str], allowed_evidence: tuple[str, ...] = ("SYNTHETIC",)) -> tuple[dict, list[RawEvent]]:
    if not lines:
        raise InputError("input is empty")
    try:
        header = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise InputError(f"invalid header: {exc}") from exc
    if not isinstance(header, dict) or header.get("type") != "header":
        raise InputError("first line must be a header object")
    if header.get("evidence") not in allowed_evidence:
        raise InputError(f"this command replays {'/'.join(allowed_evidence)} inputs only: header.evidence is "
                         f"{header.get('evidence')!r}")
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
            if header["evidence"] == "SYNTHETIC" and getattr(ev, "source", "synthetic") == PUBLIC_SOURCE:
                raise InputError("public-data events cannot appear in a SYNTHETIC input")
            events.append(RawEvent(i, ev, None, ev.event_id, str(obj.get("type")), ev.recv_us, line))
        except InputError as exc:
            eid = obj.get("id") if isinstance(obj, dict) and isinstance(obj.get("id"), str) else f"seq:{i}"
            et = str(obj.get("type")) if isinstance(obj, dict) else "unknown"
            events.append(RawEvent(i, None, f"malformed_event: {exc}", eid, et, None))
    return header, events


def load_input(path: str | Path, allowed_evidence: tuple[str, ...] = ("SYNTHETIC",)) -> InputFile:
    p = Path(path)
    data = p.read_bytes()
    text = data.decode("utf-8")
    header, events = parse_lines(text.splitlines(), allowed_evidence)
    return InputFile(str(p), hashlib.sha256(data).hexdigest(), header, events)


def candle_end(start_us: int) -> int:
    return start_us + MINUTE_US
