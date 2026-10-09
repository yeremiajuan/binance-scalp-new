"""Optional Telegram monitoring (disabled by default). Read-only: no Telegram message can change state.

* The bot token is read from the environment variable named in ``[telegram] env_var``; it is never stored in
  configuration, the database, recordings or logs (errors are redacted).
* Notifications come from the outbox, which the engine writes in the same transaction as the transition they
  describe. This thread only reads the outbox; delivery results go back to the owner thread, which records them.
  A delivery failure never touches accounting and never retries a trade.
* Bounded retries (``max_attempts``); Telegram's ``retry_after`` is honored; messages older than
  ``delayed_after_s`` are labeled DELAYED. A timeout after the request was sent is AMBIGUOUS: the message may
  have been delivered, so a retry can duplicate it. Every message carries its stable id so duplicates are
  identifiable; exactly-once delivery is not claimed.
* Allowlisted chats may send ``/status`` and ``/positions`` (read-only). Anything else gets a fixed reply that
  state-changing controls are local CLI commands only.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from paperbot.report import build_report, render_positions, render_status
from paperbot.storage import Storage
from paperbot.timeutil import local_date, local_iso

from .hosts import TELEGRAM_HOST


class SendResult:
    def __init__(self, status: str, error: str | None = None, retry_after: float | None = None):
        self.status = status  # sent | ambiguous | retry | failed
        self.error = error
        self.retry_after = retry_after


class TelegramApi:
    def __init__(self, token: str, timeout_s: float = 10.0, opener=urllib.request.urlopen):
        self._token = token
        self.timeout_s = timeout_s
        self.opener = opener

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    def _call(self, method: str, params: dict, timeout: float | None = None) -> dict:
        url = f"https://{TELEGRAM_HOST}/bot{self._token}/{method}"
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        with self.opener(req, timeout=timeout or self.timeout_s) as r:
            return json.loads(r.read().decode())

    def send(self, chat_id: str, text: str) -> SendResult:
        try:
            body = self._call("sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"})
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read().decode())
            except ValueError:
                body = {}
            ra = (body.get("parameters") or {}).get("retry_after")
            if e.code == 429:
                return SendResult("retry", f"HTTP 429 {body.get('description', '')}", float(ra or 30))
            return SendResult("failed" if 400 <= e.code < 500 else "retry", self._redact(f"HTTP {e.code}"))
        except (TimeoutError, socket.timeout) as e:
            return SendResult("ambiguous", self._redact(f"timeout after send: {e}"))
        except (urllib.error.URLError, OSError) as e:
            return SendResult("retry", self._redact(f"{type(e).__name__}: {e}"))
        if body.get("ok"):
            return SendResult("sent")
        return SendResult("failed", self._redact(str(body.get("description"))))

    def updates(self, offset: int | None, timeout_s: int) -> list[dict]:
        params = {"timeout": timeout_s, "allowed_updates": json.dumps(["message"])}
        if offset is not None:
            params["offset"] = offset
        try:
            body = self._call("getUpdates", params, timeout=timeout_s + 5)
        except (urllib.error.URLError, OSError, ValueError):
            return []
        return body.get("result", []) if body.get("ok") else []


def compose(row: dict, now_us: int, delayed_after_s: int, tz: str) -> str:
    text = row["text"]
    age_s = (now_us - row["created_us"]) / 1e6
    prefix = ""
    if age_s > delayed_after_s:
        prefix = f"DELAYED {age_s:.0f}s (created {local_iso(row['created_us'], tz)})\n"
    retry = f"\n(retry {row['attempts'] + 1}; may duplicate an earlier delivery)" if row["attempts"] else ""
    return f"{prefix}{text}{retry}\nid {row['msg_id']}"


class Notifier(threading.Thread):
    def __init__(self, cfg, state_path: str, out, stop_event: threading.Event, api: TelegramApi, clock):
        super().__init__(name="paperbot-telegram", daemon=True)
        self.cfg = cfg
        self.tg = cfg.telegram
        self.state_path = state_path
        self.out = out
        self.stop_event = stop_event
        self.api = api
        self.clock = clock
        self.offset: int | None = None
        self.not_before: dict[str, float] = {}
        # attempts reported to the owner but possibly not yet written: never resend before the row catches up
        self.reported: dict[str, int] = {}
        self.last_daily: str | None = None
        self.last_prune = 0.0

    # owner thread: daily summary and retention (both are owner writes)
    def after_tick(self, session, now_us: int) -> None:
        store = session.engine.store
        tz = self.cfg.day_timezone
        hh, mm = (int(x) for x in self.cfg.forward.daily_summary_local.split(":"))
        local = datetime.fromtimestamp(now_us / 1e6, tz=ZoneInfo(tz))
        day = local_date(now_us, tz)
        due = (local.hour, local.minute) >= (hh, mm)
        if self.last_daily is None:
            # baseline at start: today's boundary already passed -> today is done; otherwise yesterday is
            self.last_daily = day if due else (local.date() - timedelta(days=1)).isoformat()
            return
        if due and self.last_daily != day:
            self.last_daily = day
            summarized = (local.date() - timedelta(days=1)).isoformat()
            try:
                r = build_report(store)
                text = (f"{' | '.join(r['labels'])} | DAILY (day ending {summarized} 24:00 {tz})\n{render_status(r)}\n"
                        f"realized net {r['pnl']['realized_net']} · fees {r['pnl']['fees_usdt_value']} USDT")
            except Exception as exc:  # noqa: BLE001 - a summary failure must not stop the owner
                text = f"PAPER | PUBLIC DATA | DAILY ({summarized}) unavailable: {type(exc).__name__}"
            store.outbox_insert(f"daily:{summarized}", now_us, "daily", text)
        if time.monotonic() - self.last_prune > 3600:
            self.last_prune = time.monotonic()
            store.outbox_prune(now_us - self.tg.retention_days * 86400 * 1_000_000)

    # notifier thread: deliver and answer read-only commands
    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.deliver_once()
                if self.tg.poll_commands:
                    self.commands_once(timeout_s=2)
                else:
                    self.stop_event.wait(1.0)
            except Exception:  # noqa: BLE001 - monitoring must never take the runner down
                self.stop_event.wait(5.0)

    def _pending(self) -> list[dict]:
        store = Storage.open_readonly(self.state_path)
        try:
            return [dict(r) for r in store.conn.execute(
                "SELECT * FROM outbox WHERE status IN ('pending','ambiguous') AND attempts < ? "
                "ORDER BY created_us LIMIT 20", (self.tg.max_attempts,))]
        finally:
            store.close()

    def deliver_once(self) -> int:
        sent = 0
        for row in self._pending():
            mid = row["msg_id"]
            if row["attempts"] < self.reported.get(mid, 0) or time.monotonic() < self.not_before.get(mid, 0):
                continue
            text = compose(row, self.clock.now_us(), self.tg.delayed_after_s, self.cfg.day_timezone)
            results = [self.api.send(chat, text) for chat in self.tg.chat_ids]
            attempts = row["attempts"] + 1
            worst = next((r for r in results if r.status != "sent"), None)
            if worst is None:
                status, err = "sent", None
                sent += 1
            elif worst.status == "failed" or attempts >= self.tg.max_attempts:
                status = "failed"
                err = worst.error + (" (last attempt ambiguous: may have been delivered)"
                                     if worst.status == "ambiguous" else "")
            else:  # retry later; an ambiguous send may already have been delivered
                status, err = ("ambiguous" if worst.status == "ambiguous" else "pending"), worst.error
                self.not_before[mid] = time.monotonic() + (worst.retry_after or min(60, 2 ** attempts))
            self.reported[mid] = attempts
            self.out.put("outbox_result", {"msg_id": mid, "status": status, "attempts": attempts, "error": err})
        return sent

    def commands_once(self, timeout_s: int) -> None:
        for upd in self.api.updates(self.offset, timeout_s):
            self.offset = int(upd.get("update_id", 0)) + 1  # advanced before handling: at-most-once replies
            msg = upd.get("message") or {}
            chat = str((msg.get("chat") or {}).get("id"))
            if chat not in self.tg.chat_ids:
                continue  # not allowlisted: ignored
            text = (msg.get("text") or "").strip().split("@")[0]
            if text in ("/status", "/positions"):
                store = Storage.open_readonly(self.state_path)
                try:
                    r = build_report(store)
                finally:
                    store.close()
                body = render_status(r) if text == "/status" else render_positions(r)
            else:
                body = ("PAPER | read-only bot: /status and /positions only. Kill, reset and stop are local CLI "
                        "commands.")
            self.api.send(chat, body[:3900])


def notifier_factory(cfg, state_path: str, out, stop_event: threading.Event, clock=None, api=None):
    """Build the notifier when Telegram is enabled and the token is present; otherwise None."""
    tg = cfg.telegram
    if tg is None or not tg.enabled:
        return None
    token = os.environ.get(tg.env_var, "")
    if api is None:
        if not token:
            raise RuntimeError(f"telegram.enabled but environment variable {tg.env_var} is not set")
        api = TelegramApi(token)
    return Notifier(cfg, state_path, out, stop_event, api, clock or out.clock)
