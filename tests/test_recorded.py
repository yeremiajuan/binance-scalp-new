"""Recorded public-data sessions replay to the same decisions and accounting (and are never SYNTHETIC)."""

from __future__ import annotations

import pytest
from conftest import dump_db, report, write_config, write_forward_config
from fake_binance import FakeMarket, Harness, boot, standard_bars

from paperbot.cli import main
from paperbot.events import InputError
from paperbot.recorded import export_recording, replay_recording
from paperbot.replay import new_run
from paperbot.storage import StateError
from paperbot.timeutil import US_PER_MS, US_PER_S

MS = US_PER_MS
BARS = standard_bars()
NOT_REPLAYED = {"meta", "manifest", "sessions", "outbox"}


def session_with_everything(tmp_path):
    """Entry, target exit, a local kill + reset routed through the owner, a restart, and more activity."""
    cfg = write_forward_config(tmp_path)
    m = FakeMarket(BARS)
    h = Harness(tmp_path, cfg, m)
    t = boot(h, BARS[249].end_us + 10_000 * MS)
    bb = BARS[250]
    t = h.run_quotes(t, bb.end_us + 300 * MS, bb.close)
    h.bar(bb)
    t = h.run_quotes(bb.end_us + 800 * MS, bb.end_us + 3000 * MS, bb.close + 2)
    h.quote(t, 62400)
    h.quote(t + 400 * MS, 62395)  # target exit filled
    assert h.st.position is None
    assert h.session.control({"cmd": "kill", "reason": "operator check", "wall_utc": "2026-10-09T10:00:00Z"},
                             t + 500 * MS)["ok"]
    t = h.run_quotes(t + 1000 * MS, t + 5000 * MS, 61600)
    assert h.session.control({"cmd": "reset", "latch": "manual_kill", "reason": "reviewed",
                              "wall_utc": "2026-10-09T10:05:00Z"}, t)["ok"]
    t = h.run_quotes(t + 100 * MS, t + 3000 * MS, 61600)
    h.session.stop(t, "test stop")
    h.close()
    h2 = Harness(tmp_path, cfg, m, session_id="s2")
    boot(h2, t + 60 * US_PER_S)
    h2.close()
    return cfg, tmp_path / "fwd.sqlite"


def test_recorded_session_replays_to_identical_decisions_and_accounting(tmp_path):
    cfg, db = session_with_everything(tmp_path)
    rec = tmp_path / "session.jsonl"
    res = export_recording(str(db), str(rec))
    assert res["events"] > 300 and res["controls"] == 2
    out = tmp_path / "replayed.sqlite"
    replay_recording(str(cfg), str(rec), str(out))
    a, b = dump_db(db), dump_db(out)
    for table in sorted(set(a) - NOT_REPLAYED):
        assert a[table] == b[table], f"table {table} differs after replay"
    r = report(out)
    assert r["labels"] == ["PAPER", "PUBLIC DATA", "RECORDED REPLAY", "MOCKED"] and r["reconciliation"]["ok"]
    assert r["data_provenance"] == "MOCKED"  # test data is never presented as real public observations
    assert "SYNTHETIC" not in " ".join(r["labels"]) and r["evidence"] == "PUBLIC_RECORDED_REPLAY"
    orig = report(db)
    assert orig["labels"] == ["PAPER", "PUBLIC DATA", "FORWARD", "MOCKED"]
    assert orig["pnl"]["realized_net"] == r["pnl"]["realized_net"] and len(orig["fills"]) == len(r["fills"]) == 2
    assert [c["kind"] for c in r["risk"]["controls"]] == ["kill", "reset"]


def test_recording_cli_and_identity_checks(tmp_path, capsys):
    cfg, db = session_with_everything(tmp_path)
    rec = tmp_path / "session.jsonl"
    assert main(["export-recording", "--state", str(db), "--out", str(rec)]) == 0
    assert main(["replay-recording", "--config", str(cfg), "--input", str(rec), "--state",
                 str(tmp_path / "r.sqlite")]) == 0
    out = capsys.readouterr().out
    assert "PAPER | PUBLIC DATA | RECORDED REPLAY" in out and "SYNTHETIC" not in out
    other = write_forward_config(tmp_path, overrides={"fees.buy_fee_bps": "12"}, name="other.toml")
    with pytest.raises(StateError, match="configuration differs"):
        replay_recording(str(other), str(rec), str(tmp_path / "x.sqlite"))
    # the synthetic replay path refuses public recordings, and recordings refuse synthetic replay
    with pytest.raises(InputError):
        new_run(str(write_config(tmp_path, name="syn.toml")), str(rec), str(tmp_path / "y.sqlite"))
    before = dump_db(db)
    assert main(["status", "--state", str(db)]) == 0 and main(["positions", "--state", str(db)]) == 0
    assert dump_db(db) == before  # read-only
    out = capsys.readouterr().out
    assert "PAPER | PUBLIC DATA | FORWARD | MOCKED" in out and "entry blocks:" in out and "queued exit:" in out
