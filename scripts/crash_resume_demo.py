"""Interrupted replay of the SYNTHETIC demo: crash injections and stop/resume cycles.

Builds ``OUT_DIR/restarted.sqlite`` from the same config and input as an uninterrupted reference run, with:
  1. a crash raised *before* COMMIT of an entry-fill event (that event must not persist),
  2. a crash raised *after* COMMIT of an exit-fill event, before acknowledgement (it must not be reprocessed),
  3. a crash while an entry order is pending (mid-order: reservation and order must be restored),
  4. several partial (--stop-after style) runs,
each followed by a fresh resume. Crash points are taken from the reference database.
Compare the result with the reference using scripts/compare_states.py.

Usage: python scripts/crash_resume_demo.py CONFIG INPUT REFERENCE_DB OUT_DIR
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from paperbot.replay import new_run, resume_run


class InjectedCrash(Exception):
    pass


def q(db: Path, sql: str) -> list:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def crash_at(point: str, seq: int):
    def hook(p, s):
        if p == point and s == seq:
            raise InjectedCrash(f"{point} of seq {seq}")
    return hook


def main(cfg: str, inp: str, ref: str, out_dir: str) -> None:
    db = Path(out_dir) / "restarted.sqlite"
    for f in (db, Path(str(db) + ".lock")):
        if f.exists():
            f.unlink()
    entry_fill = q(Path(ref), "SELECT seq FROM fills WHERE side='BUY' ORDER BY seq LIMIT 1 OFFSET 1")[0][0]
    exit_fill = q(Path(ref), "SELECT seq FROM fills WHERE side='SELL' ORDER BY seq LIMIT 1 OFFSET 2")[0][0]
    pending_sub = q(Path(ref), "SELECT submitted_seq FROM orders WHERE purpose='entry' "
                               "ORDER BY 1 LIMIT 1 OFFSET 3")[0][0]
    pending_sql = "SELECT order_id, reserved_amount FROM orders WHERE status='pending'"
    locked_sql = "SELECT locked FROM balances WHERE asset='USDT'"

    try:
        new_run(cfg, inp, str(db), fault_hook=crash_at("before_commit", entry_fill))
    except InjectedCrash as exc:
        print(f"[1] crash {exc}: committed cursor {q(db, 'SELECT seq FROM cursor')[0][0]}; "
              f"input_log rows for that seq: {q(db, f'SELECT count(*) FROM input_log WHERE seq={entry_fill}')[0][0]}")
    try:
        resume_run(cfg, inp, str(db), fault_hook=crash_at("after_commit", exit_fill))
    except InjectedCrash as exc:
        print(f"[2] crash {exc}: committed cursor {q(db, 'SELECT seq FROM cursor')[0][0]} "
              "(committed, never acknowledged)")
    r = resume_run(cfg, inp, str(db), stop_after=pending_sub - q(db, "SELECT seq FROM cursor")[0][0])
    print(f"[3] resumed to cursor {r.cursor}; pending orders "
          f"{q(db, pending_sql)}")
    try:
        resume_run(cfg, inp, str(db), fault_hook=crash_at("before_commit", pending_sub + 1))
    except InjectedCrash as exc:
        print(f"[3] crash {exc} while the order was pending: cursor {q(db, 'SELECT seq FROM cursor')[0][0]}, "
              f"USDT locked {q(db, locked_sql)[0][0]}")
    for n in (1500, 2500, 1):
        r = resume_run(cfg, inp, str(db), stop_after=n)
        print(f"[4] partial resume: {r.processed} events -> cursor {r.cursor}/{r.total}")
    r = resume_run(cfg, inp, str(db))
    print(f"[5] final resume: {r.processed} events -> cursor {r.cursor}/{r.total}")
    again = resume_run(cfg, inp, str(db))
    print(f"[6] resume of a finished replay processed {again.processed} events (idempotent)")


if __name__ == "__main__":
    main(*sys.argv[1:5])
