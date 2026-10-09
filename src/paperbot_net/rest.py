"""GET-only client for Binance public spot REST market data.

Request weights are from rest-api.md (checked 2026-10-09): time 1, exchangeInfo?symbol 20, executionRules?symbol 2,
klines 2, avgPrice 2, referencePrice 2. The client:

* sends no API key and no signed or timestamped parameters, and supports no method but GET;
* tracks ``X-MBX-USED-WEIGHT-1M`` and keeps its own usage below ``budget_fraction`` of the REQUEST_WEIGHT limit;
* on 429 pauses every request for ``Retry-After`` seconds; on 418 (IP ban) does the same and reports a ban;
* treats 403 (WAF), 5xx and transport failures as errors to retry later, never as success;
* maps referencePrice error -2043 ("never set") to an explicit "no reference price" answer.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field

from .hosts import MARKET_DATA_ONLY_REST, REST_HOSTS

ENDPOINTS = {  # name -> (path, weight)
    "time": ("/api/v3/time", 1),
    "exchangeInfo": ("/api/v3/exchangeInfo", 20),
    "executionRules": ("/api/v3/executionRules", 2),
    "klines": ("/api/v3/klines", 2),
    "avgPrice": ("/api/v3/avgPrice", 2),
    "referencePrice": ("/api/v3/referencePrice", 2),
}
NOT_ON_MARKET_DATA_ONLY = ("executionRules", "referencePrice")
DEFAULT_WEIGHT_LIMIT = 6000  # REQUEST_WEIGHT per minute until exchangeInfo.rateLimits says otherwise


class RestError(RuntimeError):
    pass


class RateLimited(RestError):
    def __init__(self, retry_after_s: float, banned: bool, status: int):
        ban = " (IP ban)" if banned else ""
        super().__init__(f"HTTP {status}: rate limited, retry after {retry_after_s:.0f}s{ban}")
        self.retry_after_s = retry_after_s
        self.banned = banned


class Throttled(RestError):
    """Our own weight budget is exhausted for this minute; nothing was sent."""

    def __init__(self, wait_s: float):
        super().__init__(f"local weight budget exhausted; wait {wait_s:.1f}s")
        self.wait_s = wait_s


class Unsupported(RestError):
    pass


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes


class UrllibTransport:
    """Real HTTPS transport (stdlib). GET only."""

    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s

    def get(self, url: str) -> HttpResponse:
        req = urllib.request.Request(url, method="GET", headers={"User-Agent": "paperbot-paper/0.2 (public data)"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as r:
                return HttpResponse(r.status, {k.lower(): v for k, v in r.headers.items()}, r.read())
        except urllib.error.HTTPError as e:
            return HttpResponse(e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read() or b"")


@dataclass
class RestCall:
    """One request/response, for the raw-observation recording."""

    name: str
    url: str
    sent_wall_s: float
    recv_wall_s: float
    status: int | None
    used_weight: str | None
    retry_after: str | None
    body: str
    error: str | None = None


@dataclass
class PublicRest:
    host: str
    transport: object
    monotonic: object = time.monotonic
    wall: object = time.time
    budget_fraction: float = 0.5
    weight_limit: int = DEFAULT_WEIGHT_LIMIT
    paused_until: float = 0.0
    banned: bool = False
    used_weight: int = 0
    _minute: int = -1
    _local_weight: int = 0
    calls: list[RestCall] = field(default_factory=list)

    def __post_init__(self):
        if self.host not in REST_HOSTS:
            raise ValueError(f"REST host {self.host!r} is not an allowlisted public market-data host")

    def url(self, name: str, params: dict) -> str:
        path, _ = ENDPOINTS[name]
        q = urllib.parse.urlencode(params)
        return f"https://{self.host}{path}" + (f"?{q}" if q else "")

    def get(self, name: str, **params) -> object:
        if name not in ENDPOINTS:
            raise Unsupported(f"endpoint {name!r} is not an allowlisted public GET endpoint")
        if self.host == MARKET_DATA_ONLY_REST and name in NOT_ON_MARKET_DATA_ONLY:
            raise Unsupported(f"{name} is not served by {MARKET_DATA_ONLY_REST}")
        now = self.monotonic()
        if now < self.paused_until:
            raise RateLimited(self.paused_until - now, self.banned, 0)
        weight = ENDPOINTS[name][1]
        minute = int(self.wall() // 60)
        if minute != self._minute:
            self._minute, self._local_weight = minute, 0
        budget = int(self.weight_limit * self.budget_fraction)
        if max(self._local_weight, self.used_weight if minute == self._minute else 0) + weight > budget:
            raise Throttled(60 - self.wall() % 60)
        url = self.url(name, params)
        sent = self.wall()
        try:
            resp = self.transport.get(url)
        except Exception as exc:  # noqa: BLE001 - transport failure: unknown outcome, retry later
            err = f"{type(exc).__name__}: {exc}"
            self.calls.append(RestCall(name, url, sent, self.wall(), None, None, None, "", err))
            raise RestError(f"{name}: transport error {type(exc).__name__}: {exc}") from exc
        recv = self.wall()
        self._local_weight += weight
        uw = resp.headers.get("x-mbx-used-weight-1m")
        if uw is not None and uw.isdigit():
            self.used_weight = int(uw)
        text = resp.body.decode("utf-8", "replace")
        ra = resp.headers.get("retry-after")
        self.calls.append(RestCall(name, url, sent, recv, resp.status, uw, ra, text))
        if resp.status in (418, 429):
            wait = float(ra) if ra and ra.replace(".", "", 1).isdigit() else 60.0
            self.paused_until = self.monotonic() + wait
            self.banned = resp.status == 418
            raise RateLimited(wait, self.banned, resp.status)
        if resp.status == 200:
            self.banned = False
            return json.loads(text)
        if name == "referencePrice" and resp.status == 400:
            try:
                err = json.loads(text)
            except ValueError:
                err = {}
            if err.get("code") == -2043:  # "This symbol doesn't have a reference price."
                return {"never_set": True}
        raise RestError(f"{name}: HTTP {resp.status}: {text[:200]}")

    def set_weight_limit(self, exchange_info: dict) -> None:
        for rl in exchange_info.get("rateLimits", []):
            if rl.get("rateLimitType") == "REQUEST_WEIGHT" and rl.get("interval") == "MINUTE" \
                    and rl.get("intervalNum") == 1 and isinstance(rl.get("limit"), int):
                self.weight_limit = rl["limit"]
