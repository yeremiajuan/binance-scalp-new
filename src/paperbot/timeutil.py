"""Timestamps are integer microseconds since the Unix epoch, always UTC."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

US_PER_MS = 1_000
US_PER_S = 1_000_000
MINUTE_US = 60 * US_PER_S
FIVE_MINUTES_US = 5 * MINUTE_US
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_ts(value: object, field: str = "timestamp") -> int:
    """Parse an ISO-8601 timestamp with an explicit offset into UTC microseconds."""
    if not isinstance(value, str):
        raise ValueError(f"{field}: expected ISO-8601 string, got {value!r}")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field}: invalid ISO-8601 timestamp {value!r}") from exc
    if dt.tzinfo is None:
        raise ValueError(f"{field}: timestamp must carry an explicit UTC offset: {value!r}")
    delta = dt - EPOCH
    return (delta.days * 86400 + delta.seconds) * US_PER_S + delta.microseconds


def to_dt(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=us)


def iso(us: int | None) -> str | None:
    if us is None:
        return None
    return to_dt(us).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def local_iso(us: int | None, tz: str) -> str | None:
    if us is None:
        return None
    return to_dt(us).astimezone(ZoneInfo(tz)).isoformat(timespec="milliseconds")


def local_date(us: int, tz: str) -> str:
    return to_dt(us).astimezone(ZoneInfo(tz)).date().isoformat()
