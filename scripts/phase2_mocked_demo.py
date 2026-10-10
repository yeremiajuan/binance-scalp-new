"""Phase 2 evidence: a MOCKED step-mode forward session, its export, recorded replay and comparison.

Usage: python scripts/phase2_mocked_demo.py WORK_DIR

The session uses the deterministic fake of Binance public payloads in tests/fake_binance.py (no network). It
contains an entry, a target exit, a local kill and audited reset routed through the owner, a graceful stop and a
forward restart. Everything it produces is labeled MOCKED and is not a public-data observation.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))

import compare_states  # noqa: E402
from test_recorded import session_with_everything  # noqa: E402

from paperbot.cli import main as cli  # noqa: E402
from paperbot.recorded import export_recording, replay_recording  # noqa: E402


def main(work: str) -> int:
    w = Path(work)
    w.mkdir(parents=True, exist_ok=True)
    cfg, db = session_with_everything(w)
    print(f"mocked forward session: {db}")
    rec = w / "session.jsonl"
    print("export:", export_recording(str(db), str(rec)))
    out = w / "replayed.sqlite"
    print("replay:", replay_recording(str(cfg), str(rec), str(out)))
    print("\n### compare forward account vs recorded replay")
    rc = compare_states.main(str(db), str(out), recorded=True)
    for args in (["status", "--state", str(db)], ["positions", "--state", str(db)],
                 ["status", "--state", str(out)]):
        print(f"\n### paperbot {' '.join(a if a != str(db) and a != str(out) else Path(a).name for a in args)}")
        cli(args)
    print("\n### sessions (rows are written by the threaded runner `paperbot run`; this step-mode demo has none,"
          " see tests/test_runner_e2e.py)")
    conn = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True)
    for r in conn.execute("SELECT session_id, started_wall, stopped_wall, stop_reason FROM sessions "
                          "ORDER BY started_wall"):
        print(r)
    print("\n### outbox (notification texts written with their transitions; not sent: Telegram disabled)")
    for msg_id, kind, status, text in conn.execute("SELECT msg_id, kind, status, text FROM outbox "
                                                   "ORDER BY created_us"):
        print(f"[{status}] {msg_id} ({kind}): {text}")
    print("\n### feed / health events")
    for r in conn.execute("SELECT kind, COUNT(*) FROM health_events GROUP BY kind ORDER BY kind"):
        print(r)
    conn.close()
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
