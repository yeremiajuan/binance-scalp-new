"""Compare two PAPER state databases table by table (exact row contents).

Usage: python scripts/compare_states.py [--recorded] A.sqlite B.sqlite
Exit code 0 when every table matches (meta.input_path may differ and is ignored).
--recorded compares a forward account with its recorded replay: the tables that describe the run rather than
its decisions and accounting (meta, manifest, sessions, outbox) are skipped and listed as skipped.
"""

from __future__ import annotations

import hashlib
import sqlite3
import sys
from pathlib import Path

RUN_DESCRIPTION_TABLES = {"meta", "manifest", "sessions", "outbox"}


def digests(path: str) -> dict[str, tuple[int, str]]:
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    out = {}
    for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        if table == "meta":
            rows = [r for r in rows if r[0] != "input_path"]
        h = hashlib.sha256(repr(rows).encode()).hexdigest()
        out[table] = (len(rows), h)
    conn.close()
    return out


def main(a: str, b: str, recorded: bool = False) -> int:
    da, db = digests(a), digests(b)
    ok = True
    print(f"{'table':16} {'rows A':>8} {'rows B':>8}  match  sha256(A)[:16]")
    for t in sorted(set(da) | set(db)):
        ra, rb = da.get(t, (0, "-")), db.get(t, (0, "-"))
        if recorded and t in RUN_DESCRIPTION_TABLES:
            print(f"{t:16} {ra[0]:>8} {rb[0]:>8}  skip   (describes the run, not its decisions)")
            continue
        same = ra == rb
        ok &= same
        print(f"{t:16} {ra[0]:>8} {rb[0]:>8}  {'yes' if same else 'NO ':5}  {ra[1][:16]}")
    print("IDENTICAL" if ok else "DIFFERENT")
    return 0 if ok else 1


if __name__ == "__main__":
    args = sys.argv[1:]
    rec = "--recorded" in args
    paths = [x for x in args if x != "--recorded"]
    sys.exit(main(paths[0], paths[1], recorded=rec))
