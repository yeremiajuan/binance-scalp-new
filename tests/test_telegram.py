"""Optional Telegram monitoring: delivery failure, ambiguous sends, retry limits, read-only allowlisted commands."""

from __future__ import annotations

import socket
import threading
import urllib.error

import pytest
from conftest import dump_db, rows, write_forward_config
from fake_binance import FakeMarket, Harness, boot, standard_bars

from paperbot.config import load_config
from paperbot.timeutil import US_PER_MS
from paperbot_net.telegram import SendResult, TelegramApi, compose, notifier_factory

MS = US_PER_MS
BARS = standard_bars()
ECONOMIC = ("input_log", "candidates", "orders", "fills", "ledger", "positions", "balances", "engine_state",
            "risk_events", "control_events")


class Collector:
    def __init__(self, clock_us):
        self.items: list[tuple[str, dict]] = []
        self.clock = type("C", (), {"now_us": staticmethod(lambda: clock_us)})()

    def put(self, kind, data):
        self.items.append((kind, data))


class FakeApi:
    def __init__(self, script):
        self.script = list(script)
        self.sent: list[tuple[str, str]] = []
        self.updates_script: list[list[dict]] = []

    def send(self, chat, text):
        self.sent.append((chat, text))
        return self.script.pop(0) if self.script else SendResult("sent")

    def updates(self, offset, timeout_s):
        return self.updates_script.pop(0) if self.updates_script else []


@pytest.fixture
def setup(tmp_path):
    cfg_path = write_forward_config(tmp_path, telegram={"enabled": True, "chat_ids": ["111"], "max_attempts": 3,
                                                        "delayed_after_s": 60})
    h = Harness(tmp_path, cfg_path, FakeMarket(BARS))
    t = boot(h, BARS[249].end_us + 10_000 * MS)
    yield h, load_config(cfg_path), t
    h.close()


def apply(h, collector):
    """The owner records delivery results (the notifier thread never writes)."""
    for kind, d in collector.items:
        assert kind == "outbox_result"
        h.engine.store.outbox_update(d["msg_id"], d["status"], d["attempts"], "2026-10-09T00:00:00Z", d.get("error"))
    collector.items.clear()


def make(h, cfg, api, now):
    out = Collector(now)
    n = notifier_factory(cfg, str(h.state), out, threading.Event(), clock=out.clock, api=api)
    return n, out


def test_disabled_by_default_and_token_required(tmp_path):
    cfg = load_config(write_forward_config(tmp_path))
    assert cfg.telegram is None
    assert notifier_factory(cfg, "x", Collector(0), threading.Event()) is None
    cfg2 = load_config(write_forward_config(tmp_path, telegram={"enabled": True, "chat_ids": ["1"],
                                                                "env_var": "PAPERBOT_TEST_NO_SUCH_VAR"},
                                            name="t2.toml"))
    with pytest.raises(RuntimeError, match="PAPERBOT_TEST_NO_SUCH_VAR"):
        notifier_factory(cfg2, "x", Collector(0), threading.Event())


def test_delivery_failure_retries_boundedly_and_never_touches_accounting(setup):
    h, cfg, t = setup
    pending = rows(h.state, "SELECT * FROM outbox WHERE status = 'pending'")
    assert pending  # session start/rearm notices were written with their transitions
    before = {k: v for k, v in dump_db(h.state).items() if k in ECONOMIC}
    api = FakeApi([SendResult("retry", "HTTP 502")] * 100)
    n, out = make(h, cfg, api, t)
    for _ in range(5):
        n.not_before.clear()
        n.deliver_once()
        apply(h, out)
    statuses = {r["status"] for r in rows(h.state, "SELECT status FROM outbox")}
    assert statuses == {"failed"}  # bounded: max_attempts=3, then given up
    assert all(r["attempts"] == 3 for r in rows(h.state, "SELECT attempts FROM outbox"))
    after = {k: v for k, v in dump_db(h.state).items() if k in ECONOMIC}
    assert after == before  # delivery never retries or alters accounting


def test_ambiguous_send_is_retried_with_duplicate_warning_and_stable_id(setup):
    h, cfg, t = setup
    first = rows(h.state, "SELECT * FROM outbox ORDER BY created_us LIMIT 1")[0]
    api = FakeApi([SendResult("ambiguous", "timeout after send")] + [SendResult("sent")] * 50)
    n, out = make(h, cfg, api, t)
    n.deliver_once()
    apply(h, out)
    row = rows(h.state, "SELECT * FROM outbox WHERE msg_id = ?", (first["msg_id"],))[0]
    assert row["status"] == "ambiguous" and row["attempts"] == 1
    n.not_before.clear()
    n.deliver_once()
    apply(h, out)
    texts = [txt for _, txt in api.sent if first["msg_id"] in txt]
    assert len(texts) == 2 and "may duplicate an earlier delivery" in texts[1]
    assert texts[0].endswith(f"id {first['msg_id']}") and texts[1].endswith(f"id {first['msg_id']}")
    assert rows(h.state, "SELECT status FROM outbox WHERE msg_id = ?", (first["msg_id"],))[0]["status"] == "sent"


def test_retry_after_is_honored_and_delayed_messages_are_labeled(setup):
    h, cfg, t = setup
    api = FakeApi([SendResult("retry", "HTTP 429", retry_after=30)] * 50)
    n, out = make(h, cfg, api, t)
    n.deliver_once()
    sent_first = len(api.sent)
    apply(h, out)
    n.deliver_once()  # within retry_after: nothing resent
    assert len(api.sent) == sent_first
    row = dict(rows(h.state, "SELECT * FROM outbox LIMIT 1")[0])
    assert "DELAYED" in compose(row, row["created_us"] + 120 * 1_000_000, 60, "Asia/Jakarta")
    assert "DELAYED" not in compose(row, row["created_us"] + 5 * 1_000_000, 60, "Asia/Jakarta")


def test_commands_are_read_only_and_allowlisted(setup):
    h, cfg, t = setup
    before = dump_db(h.state)
    api = FakeApi([])
    api.updates_script = [[
        {"update_id": 1, "message": {"chat": {"id": 999}, "text": "/status"}},  # not allowlisted: ignored
        {"update_id": 2, "message": {"chat": {"id": 111}, "text": "/status"}},
        {"update_id": 3, "message": {"chat": {"id": 111}, "text": "/positions"}},
        {"update_id": 4, "message": {"chat": {"id": 111}, "text": "/kill"}},
    ]]
    n, out = make(h, cfg, api, t)
    n.commands_once(timeout_s=0)
    chats = [c for c, _ in api.sent]
    assert chats == ["111", "111", "111"]
    assert api.sent[0][1].startswith("PAPER | PUBLIC DATA | FORWARD | MOCKED") and "positions" in api.sent[1][1]
    assert "local CLI commands" in api.sent[2][1]
    assert dump_db(h.state) == before and n.offset == 5  # nothing changed; offset advanced


def test_api_redacts_token_and_classifies_failures():
    token = "123456:SECRET-TOKEN"

    def timeout_opener(req, timeout):
        raise socket.timeout("read timed out")

    r = TelegramApi(token, opener=timeout_opener).send("1", "x")
    assert r.status == "ambiguous" and token not in (r.error or "")

    def refused(req, timeout):
        raise urllib.error.URLError(f"connection refused to https://api.telegram.org/bot{token}/sendMessage")

    r = TelegramApi(token, opener=refused).send("1", "x")
    assert r.status == "retry" and token not in r.error and "***" in r.error
