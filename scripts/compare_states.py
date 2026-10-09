"""Compare two PAPER state databases table by table (exact row contents).

Usage: python scripts/compare_states.py A.sqlite B.sqlite
Exit code 0 when every table matches (meta.input_path may differ and is ignored).
"""

from __future__ import annotations

import hashlib
import sqlite3
import sys


def digests(path: str) -> dict[str, tuple[int, str]]:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    out = {}
    for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        if table == "meta":
            rows = [r for r in rows if r[0] != "input_path"]
        h = hashlib.sha256(repr(rows).encode()).hexdigest()
        out[table] = (len(rows), h)
    conn.close()
    return out


def main(a: str, b: str) -> int:
    da, db = digests(a), digests(b)
    ok = True
    print(f"{'table':16} {'rows A':>8} {'rows B':>8}  match  sha256(A)[:16]")
    for t in sorted(set(da) | set(db)):
        ra, rb = da.get(t, (0, "-")), db.get(t, (0, "-"))
        same = ra == rb
        ok &= same
        print(f"{t:16} {ra[0]:>8} {rb[0]:>8}  {'yes' if same else 'NO ':5}  {ra[1][:16]}")
    print("IDENTICAL" if ok else "DIFFERENT")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
