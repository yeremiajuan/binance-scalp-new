"""The committed SYNTHETIC demo fixture is reproducible and exercises the intended paths."""

from __future__ import annotations

import subprocess
import sys

from conftest import ROOT, report, rows

from paperbot.replay import new_run

DEMO = ROOT / "fixtures" / "synthetic_demo.jsonl"


def test_demo_fixture_regenerates_byte_for_byte(tmp_path):
    out = tmp_path / "demo.jsonl"
    subprocess.run([sys.executable, str(ROOT / "scripts" / "make_demo_fixture.py"), str(out)], check=True,
                   capture_output=True)
    assert out.read_bytes() == DEMO.read_bytes()


def test_demo_replay_outcomes(tmp_path):
    db = tmp_path / "demo.sqlite"
    new_run(str(ROOT / "config" / "paper.toml"), str(DEMO), str(db))
    r = report(db)
    assert r["labels"] == ["PAPER", "SYNTHETIC"] and r["reconciliation"]["ok"] and r["pnl"]["identity"]["holds"]
    assert r["candidates"]["skip_reasons"] == {"warmup_incomplete": 9, "spread": 1}
    assert r["orders"]["by_status"] == {"filled": 10, "partial": 2, "canceled": 1}
    exits = [p["exit_reason"] for p in rows(db, "SELECT exit_reason FROM positions ORDER BY opened_us")]
    assert exits == ["target", "target", "timeout", "health:quotes_stale", "timeout", None]
    assert r["cursor"]["dispositions"] == {"accepted": 13215, "rejected": 14, "duplicate": 1}
    assert r["risk"]["day"] == "2026-10-02" and r["risk"]["latches"] == []
    assert r["mark"]["fresh"] and r["inventory"]["btc_total"] > 0  # open inventory valued, not force-closed
