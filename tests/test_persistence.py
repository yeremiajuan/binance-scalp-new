"""Atomic commits, crash injection, resume equivalence, idempotency, inconsistent state, Decimal text storage."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from conftest import dump_db, report, rows, write_config
from scenarios import MS, entry_scenario, resume, rich_scenario, run

from paperbot.events import parse_lines
from paperbot.reconcile import ReconciliationError
from paperbot.replay import new_run, resume_run
from paperbot.storage import StateError


class Crash(Exception):
    pass


def economic(db: Path) -> dict:
    d = dump_db(db)
    meta = dict(d.pop("meta"))
    meta.pop("input_path")
    d["meta"] = sorted(meta.items())
    return d


@pytest.fixture(scope="module")
def reference(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("reference")
    cfg = write_config(tmp)
    sc = rich_scenario()
    db = run(tmp, sc, cfg, name="ref")
    return {"sc": sc, "db": db, "dump": economic(db), "n": len(sc.objs)}


def test_reference_run_exercises_the_interesting_paths(reference):
    db = reference["db"]
    orders = rows(db, "SELECT * FROM orders")
    statuses = {o["status"] for o in orders}
    assert {"partial", "filled"} <= statuses
    sells = [o for o in orders if o["side"] == "SELL"]
    assert len(sells) >= 3 and any(o["status"] == "partial" for o in sells)
    pos = rows(db, "SELECT * FROM positions ORDER BY opened_us")
    assert [p["exit_reason"] for p in pos] == ["target", "stop"]
    r = report(db)
    assert r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]
    assert r["inventory"]["dust_btc"] > 0


def _interesting_seqs(db: Path) -> list[int]:
    seqs = set()
    for o in rows(db, "SELECT submitted_seq, closed_seq FROM orders"):
        seqs |= {o["submitted_seq"], o["closed_seq"], o["submitted_seq"] + 1}
    return sorted(s for s in seqs if s)


def test_crash_before_commit_then_resume_matches_uninterrupted(tmp_path, reference):
    cfg = write_config(tmp_path)
    for k in _interesting_seqs(reference["db"])[:8]:
        d = tmp_path / f"b{k}"
        d.mkdir()
        cfgk = write_config(d)
        def hook(point, seq, k=k):
            if point == "before_commit" and seq == k:
                raise Crash(k)
        with pytest.raises(Crash):
            run(d, reference["sc"], cfgk, name="ref", fault_hook=hook)
        db = d / "ref.sqlite"
        assert rows(db, "SELECT seq FROM cursor")[0]["seq"] == k - 1  # nothing of event k persisted
        assert not rows(db, "SELECT * FROM input_log WHERE seq = ?", (k,))
        resume(d, cfgk, name="ref")
        assert economic(db) == reference["dump"], f"divergence after crash before commit of seq {k}"
    del cfg


def test_crash_after_commit_before_ack_then_resume_matches_uninterrupted(tmp_path, reference):
    for k in _interesting_seqs(reference["db"])[:8]:
        d = tmp_path / f"a{k}"
        d.mkdir()
        cfgk = write_config(d)
        def hook(point, seq, k=k):
            if point == "after_commit" and seq == k:
                raise Crash(k)
        with pytest.raises(Crash):
            run(d, reference["sc"], cfgk, name="ref", fault_hook=hook)
        db = d / "ref.sqlite"
        assert rows(db, "SELECT seq FROM cursor")[0]["seq"] == k  # committed, not acknowledged
        resume(d, cfgk, name="ref")  # redelivery of k is a no-op: resume starts at k+1
        assert len(rows(db, "SELECT * FROM input_log WHERE seq = ?", (k,))) == 1
        assert economic(db) == reference["dump"], f"divergence after crash after commit of seq {k}"


def test_write_failure_mid_transaction_rolls_back_everything(tmp_path, reference):
    seqs = [o["closed_seq"] for o in rows(reference["db"], "SELECT closed_seq FROM orders WHERE side='SELL'")]
    k = seqs[0]  # a fill event: fill row, ledger rows, order update...
    cfg = write_config(tmp_path)
    calls = {"n": 0}
    def hook(point, seq):
        if point == "mid_write" and seq == k:
            calls["n"] += 1
            if calls["n"] == 2:
                raise sqlite3.OperationalError("disk I/O error (injected)")
    with pytest.raises(sqlite3.OperationalError):
        run(tmp_path, reference["sc"], cfg, name="ref", fault_hook=hook)
    db = tmp_path / "ref.sqlite"
    assert rows(db, "SELECT seq FROM cursor")[0]["seq"] == k - 1
    assert not rows(db, "SELECT * FROM fills WHERE seq = ?", (k,))
    assert not rows(db, "SELECT * FROM ledger WHERE seq = ?", (k,))
    resume(tmp_path, cfg, name="ref")
    assert economic(db) == reference["dump"]


def test_real_sqlite_disk_full_failure_then_resume(tmp_path, reference):
    cfg = write_config(tmp_path)
    state = {"store": None, "armed": False}
    import paperbot.storage as storage_mod

    real_commit = storage_mod.Storage.commit_event

    def spy(self, raw, *a, **kw):
        if raw.seq == 300 and not state["armed"]:
            pages = self.conn.execute("PRAGMA page_count").fetchone()[0]
            self.conn.execute(f"PRAGMA max_page_count = {pages}")
            state["armed"] = True
        return real_commit(self, raw, *a, **kw)

    storage_mod.Storage.commit_event = spy
    try:
        with pytest.raises(sqlite3.OperationalError, match="full"):
            run(tmp_path, reference["sc"], cfg, name="ref")
    finally:
        storage_mod.Storage.commit_event = real_commit
    db = tmp_path / "ref.sqlite"
    failed_at = rows(db, "SELECT seq FROM cursor")[0]["seq"]
    assert 299 <= failed_at < reference["n"]
    resume(tmp_path, cfg, name="ref")  # a new connection has no page cap
    assert economic(db) == reference["dump"]


@pytest.mark.parametrize("cuts", [[1], [251, 260], [300, 1, 1, 5], [511, 512, 513, 514]])
def test_committed_cursor_resume_equivalence(tmp_path, reference, cuts):
    cfg = write_config(tmp_path)
    inp = reference["sc"].write(tmp_path / "ref.jsonl")
    db = tmp_path / "ref.sqlite"
    new_run(str(cfg), str(inp), str(db), stop_after=cuts[0])
    for c in cuts[1:]:
        resume_run(str(cfg), str(inp), str(db), stop_after=c)
    resume_run(str(cfg), str(inp), str(db))
    assert economic(db) == reference["dump"]


def test_resume_mid_order_restores_pending_reservation(tmp_path, reference):
    (o,) = rows(reference["db"], "SELECT * FROM orders WHERE purpose='entry' ORDER BY submitted_us LIMIT 1")
    cfg = write_config(tmp_path)
    run(tmp_path, reference["sc"], cfg, name="ref", stop_after=o["submitted_seq"])
    db = tmp_path / "ref.sqlite"
    r = report(db)
    assert r["orders"]["by_status"] == {"pending": 1}
    assert r["balances"]["USDT"]["locked"] > 0 and r["reconciliation"]["ok"]
    resume(tmp_path, cfg, name="ref")
    assert economic(db) == reference["dump"]


def test_duplicate_and_reordered_inputs_cannot_create_duplicate_fills(tmp_path, cfg_path, reference):
    sc = rich_scenario()
    objs = sc.objs
    fill_quote_ids = {f["quote_event_id"] for f in rows(reference["db"], "SELECT quote_event_id FROM fills")}
    noisy = []
    for o in objs:
        noisy.append(o)
        if o["id"] in fill_quote_ids:
            noisy.append(dict(o))  # exact redelivery of a fill-producing quote
            older = dict(o, id=o["id"] + "-late", recv="2026-10-01T12:00:00.000Z")
            noisy.append(older)  # reordered: older receipt than the injected clock
        if o["type"] == "candle" and o["id"] == "c-000010":
            noisy.append(dict(o, id="c-dup-bar"))  # same bar, different id
    sc.objs = noisy
    db = run(tmp_path, sc, cfg_path, name="noisy")
    disp = {r["disposition"] for r in rows(db, "SELECT disposition FROM input_log")}
    assert disp == {"accepted", "duplicate", "rejected"}
    assert len(rows(db, "SELECT * FROM input_log WHERE disposition='duplicate'")) == len(fill_quote_ids)
    reasons = [json.loads(r["detail"])["reason"] for r in rows(db, "SELECT detail FROM input_log "
                                                                 "WHERE disposition='rejected'")]
    assert reasons.count("non_monotonic_receipt") == len(fill_quote_ids) and "duplicate_bar" in reasons
    cols = "side, qty, price, fee_amount, fee_asset, usdt_delta, btc_delta, net_pnl, quote_event_id, fill_us"
    assert rows(db, f"SELECT {cols} FROM fills ORDER BY fill_us") == rows(
        reference["db"], f"SELECT {cols} FROM fills ORDER BY fill_us")
    assert rows(db, "SELECT * FROM balances") == rows(reference["db"], "SELECT * FROM balances")


def test_engine_ignores_redelivery_of_committed_seq(tmp_path, cfg_path):
    from paperbot.replay import open_engine
    from paperbot.storage import StateLock

    sc, bb = entry_scenario()
    db = run(tmp_path, sc, cfg_path)
    before = dump_db(db)
    raws = parse_lines(sc.lines())[1]
    with StateLock(db) as lock:
        eng = open_engine(lock)
        assert eng.process(raws[5]) == "already_committed"
        assert eng.process(raws[-1]) == "already_committed"
        eng.store.close()
    assert dump_db(db) == before


def test_prefix_invariance_future_inputs_cannot_change_past_decisions(tmp_path, cfg_path, reference):
    n = 512
    sc = rich_scenario()
    for o in sc.objs[n:]:  # rewrite the future: different prices
        for k in ("bid", "ask", "open", "high", "low", "close"):
            if k in o:
                o[k] = str(float(o[k]) * 0.97)[:12]
    db = run(tmp_path, sc, cfg_path, name="future")
    for table, col in (("candidates", "seq"), ("fills", "seq"), ("ledger", "seq"), ("risk_events", "seq")):
        a = rows(db, f"SELECT * FROM {table} WHERE {col} <= ? ORDER BY 1", (n,))
        b = rows(reference["db"], f"SELECT * FROM {table} WHERE {col} <= ? ORDER BY 1", (n,))
        assert a == b, table
    assert rows(db, "SELECT * FROM input_log WHERE seq <= ?", (n,)) == rows(
        reference["db"], "SELECT * FROM input_log WHERE seq <= ?", (n,))


# ---------------------------------------------------------------- inconsistent / incompatible state


def _small(tmp_path, cfg):
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    return run(tmp_path, sc, cfg)


@pytest.mark.parametrize("tamper, needle", [
    ("UPDATE ledger SET free_delta = '1' WHERE entry_id = 5", "ledger sum"),
    ("UPDATE balances SET locked = '1' WHERE asset = 'BTC'", "balance table"),
    ("DELETE FROM fills", "foreign key"),
    ("UPDATE orders SET status = 'pending'", "pending"),
    ("UPDATE cursor SET seq = seq - 1", "cursor"),
    ("INSERT INTO risk_events(seq, ts_us, kind, detail) VALUES (1, 1, 'latch_set', '{\"latch\": \"drawdown\"}')",
     "latch history"),
])
def test_inconsistent_state_halts_resume_without_mutation(tmp_path, cfg_path, tamper, needle):
    db = _small(tmp_path, cfg_path)
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(tamper)
    conn.commit()
    conn.close()
    before = dump_db(db)
    with pytest.raises(ReconciliationError) as exc:
        resume(tmp_path, cfg_path)
    assert any(needle in p for p in exc.value.problems), exc.value.problems
    assert dump_db(db) == before


def test_incompatible_config_input_schema_and_corruption_halt(tmp_path, cfg_path):
    db = _small(tmp_path, cfg_path)
    before = dump_db(db)
    changed = write_config(tmp_path, {"fees.buy_fee_bps": "7.5"}, name="changed.toml")
    with pytest.raises(StateError, match="configuration changed"):
        resume(tmp_path, changed)
    inp = tmp_path / "state.jsonl"
    original = inp.read_text()
    inp.write_text(original + json.dumps({"type": "heartbeat", "id": "extra", "recv": "2026-10-01T20:00:00Z"}) + "\n")
    with pytest.raises(ReconciliationError, match="input_sha256 mismatch"):
        resume(tmp_path, cfg_path)
    inp.write_text(original)
    assert dump_db(db) == before
    conn = sqlite3.connect(db)
    conn.execute("UPDATE meta SET value = '999' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    with pytest.raises(StateError, match="incompatible schema"):
        resume(tmp_path, cfg_path)
    data = bytearray(db.read_bytes())
    data[100:4096] = b"\xff" * (4096 - 100)
    db.write_bytes(bytes(data))
    with pytest.raises(StateError):
        resume(tmp_path, cfg_path)
    assert db.exists()  # never erased or recreated as recovery


def test_new_replay_refuses_to_overwrite_nonempty_account(tmp_path, cfg_path):
    db = _small(tmp_path, cfg_path)
    before = dump_db(db)
    with pytest.raises(StateError, match="refusing to overwrite"):
        new_run(str(cfg_path), str(tmp_path / "state.jsonl"), str(db))
    assert dump_db(db) == before


def test_money_is_decimal_text_never_real(tmp_path, cfg_path):
    db = _small(tmp_path, cfg_path)
    conn = sqlite3.connect(db)
    real = conn.execute("SELECT m.name, i.name FROM sqlite_master m JOIN pragma_table_info(m.name) i "
                        "WHERE m.type='table' AND m.name NOT LIKE 'sqlite_%' "
                        "AND upper(i.type) NOT IN ('TEXT', 'INTEGER')").fetchall()
    assert real == []
    for table, cols in (("fills", ("qty", "price", "fee_amount", "usdt_delta")), ("balances", ("free", "locked")),
                        ("ledger", ("free_delta", "locked_delta")), ("orders", ("qty", "limit_price"))):
        for col in cols:
            types = {r[0] for r in conn.execute(f"SELECT DISTINCT typeof({col}) FROM {table}")}
            assert types == {"text"}, (table, col, types)
    conn.close()
