"""Public Binance market-stream client (combined stream) with bounded reconnect and silence detection.

Lifecycle facts from web-socket-streams.md (checked 2026-10-09): a connection lasts at most 24 h; the server
pings every 20 s and disconnects without a pong within a minute (the websockets library answers pings); a
``serverShutdown`` event precedes a server-side shutdown; at most 300 connection attempts per 5 minutes per IP;
5 incoming messages per second (we send none after connecting). ``data-stream.binance.vision`` serves only
market data.
"""

from __future__ import annotations

import json
import random
import threading
import time
from collections import deque

from .hosts import WS_HOSTS

STREAMS = ("btcusdt@kline_1m", "btcusdt@bookTicker", "btcusdt@avgPrice", "btcusdt@referencePrice")
MAX_ATTEMPTS_PER_5MIN = 250  # below the documented 300 per 5 minutes per IP
STABLE_AFTER_S = 60.0


def stream_url(host: str, port: int | None = None, streams=STREAMS) -> str:
    if host not in WS_HOSTS:
        raise ValueError(f"WebSocket host {host!r} is not an allowlisted public market-stream host")
    return f"wss://{host}:{port or WS_HOSTS[host]}/stream?streams={'/'.join(streams)}"


def default_connect(url: str, open_timeout: float):
    from websockets.sync.client import connect

    # ping_interval=None: the server sends pings; the library answers them automatically.
    return connect(url, open_timeout=open_timeout, ping_interval=None, close_timeout=2, max_size=2 ** 20)


class StreamFeed(threading.Thread):
    """Reads frames and hands them to ``sink(kind, data)``; never interprets market data itself."""

    def __init__(self, url: str, sink, *, connect=default_connect, initial_backoff_s: float = 1.0,
                 max_backoff_s: float = 60.0, silence_s: float = 30.0, open_timeout_s: float = 10.0,
                 stop_event: threading.Event | None = None, rng: random.Random | None = None,
                 monotonic=time.monotonic):
        super().__init__(name="paperbot-ws", daemon=True)
        self.url = url
        self.sink = sink
        self.connect = connect
        self.initial_backoff_s = initial_backoff_s
        self.max_backoff_s = max_backoff_s
        self.silence_s = silence_s
        self.open_timeout_s = open_timeout_s
        self.stop_event = stop_event or threading.Event()
        self.rng = rng or random.Random()
        self.monotonic = monotonic
        self.attempts: deque[float] = deque()
        self.conn_seq = 0
        self.conn = None

    def stop(self) -> None:
        self.stop_event.set()
        conn = self.conn
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _wait(self, seconds: float) -> None:
        self.stop_event.wait(max(0.0, seconds))

    def _attempt_allowed(self) -> float:
        now = self.monotonic()
        while self.attempts and now - self.attempts[0] > 300:
            self.attempts.popleft()
        if len(self.attempts) >= MAX_ATTEMPTS_PER_5MIN:
            return 300 - (now - self.attempts[0])
        return 0.0

    def run(self) -> None:
        backoff = self.initial_backoff_s
        while not self.stop_event.is_set():
            wait = self._attempt_allowed()
            if wait > 0:
                self._wait(wait)
                continue
            self.attempts.append(self.monotonic())
            self.conn_seq += 1
            conn_id = self.conn_seq
            started = self.monotonic()
            reason = "unknown"
            try:
                self.conn = self.connect(self.url, self.open_timeout_s)
                self.sink("ws_status", {"kind": "ws_connected", "conn": conn_id})
                last_msg = self.monotonic()
                while not self.stop_event.is_set():
                    try:
                        msg = self.conn.recv(timeout=1.0)
                    except TimeoutError:
                        if self.monotonic() - last_msg > self.silence_s:
                            reason = f"silent_for_{self.silence_s:.0f}s"
                            break
                        continue
                    last_msg = self.monotonic()
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", "replace")
                    self.sink("ws_message", {"conn": conn_id, "text": msg})
                    if '"serverShutdown"' in msg:
                        try:
                            if json.loads(msg).get("data", {}).get("e") == "serverShutdown":
                                reason = "server_shutdown"
                                break
                        except ValueError:
                            pass
                else:
                    reason = "stopped"
            except Exception as exc:  # noqa: BLE001 - connection closed/failed: report and reconnect
                reason = f"{type(exc).__name__}: {str(exc)[:200]}"
            finally:
                conn, self.conn = self.conn, None
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
            self.sink("ws_status", {"kind": "ws_disconnected", "conn": conn_id, "reason": reason})
            if self.stop_event.is_set():
                break
            if self.monotonic() - started > STABLE_AFTER_S:
                backoff = self.initial_backoff_s
            self._wait(self.rng.uniform(0.5, 1.0) * backoff)  # bounded exponential backoff with jitter
            backoff = min(self.max_backoff_s, backoff * 2)
