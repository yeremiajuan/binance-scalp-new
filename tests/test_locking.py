"""Real concurrent processes against one canonical state path, including path aliases."""

from __future__ import annotations

import os
import signal
import subprocess
import sys

import pytest
from conftest import dump_db, write_config
from scenarios import MS, entry_scenario, run

from paperbot.storage import StateError, StateLock

PY = sys.executable
HOLDER = (
    "import sys, time\n"
    "from paperbot.storage import StateLock\n"
    "lock = StateLock(sys.argv[1]).acquire()\n"
    "print('locked', lock.canonical, flush=True)\n"
    "time.sleep(60)\n"
)


def cli(*args, cwd=None):
    return subprocess.run([PY, "-m", "paperbot", *args], capture_output=True, text=True, cwd=cwd, timeout=120)


@pytest.fixture
def account(tmp_path):
    cfg = write_config(tmp_path)
    sc, bb = entry_scenario()
    sc.mid_quote(bb.end_us + 900 * MS, 61502)
    db = run(tmp_path, sc, cfg, stop_after=100)
    return tmp_path, cfg, db


def hold(path: str, cwd=None) -> subprocess.Popen:
    p = subprocess.Popen([PY, "-c", HOLDER, path], stdout=subprocess.PIPE, text=True, cwd=cwd)
    line = p.stdout.readline()
    assert line.startswith("locked"), line
    return p


def test_second_process_and_path_aliases_fail_before_mutation(account):
    tmp, cfg, db = account
    os.symlink(db, tmp / "alias.sqlite")
    os.symlink(tmp, tmp / "dir-alias")
    (tmp / "sub").mkdir()
    holder = hold(str(db))
    try:
        before = dump_db(db)
        spellings = [str(db), str(tmp / "alias.sqlite"), str(tmp / "dir-alias" / "state.sqlite"),
                     str(tmp / "sub" / ".." / "state.sqlite")]
        for s in spellings:
            r = cli("resume", "--config", str(cfg), "--input", str(tmp / "state.jsonl"), "--state", s)
            assert r.returncode == 3, (s, r.stdout, r.stderr)
            assert "LOCKED" in r.stderr and str(db.resolve()) in r.stderr
            k = cli("kill", "--state", s, "--reason", "x")
            assert k.returncode == 3
        rel = cli("resume", "--config", str(cfg), "--input", str(tmp / "state.jsonl"), "--state", "state.sqlite",
                  cwd=str(tmp))
        assert rel.returncode == 3
        assert dump_db(db) == before
        # read-only status works while the owner holds the lock and does not mutate
        st = cli("status", "--state", str(tmp / "alias.sqlite"))
        assert st.returncode == 0 and st.stdout.startswith("PAPER | SYNTHETIC")
        assert dump_db(db) == before
    finally:
        holder.send_signal(signal.SIGKILL)
        holder.wait()
    # the kernel released the crashed holder's lock; the stale lock file (with its pid text) does not block
    assert (tmp / "state.sqlite.lock").exists()
    r = cli("resume", "--config", str(cfg), "--input", str(tmp / "state.jsonl"), "--state", str(tmp / "alias.sqlite"))
    assert r.returncode == 0, r.stderr


def test_hard_link_alias_is_refused(account):
    tmp, cfg, db = account
    os.link(db, tmp / "hard.sqlite")
    with pytest.raises(StateError, match="hard links"):
        StateLock(tmp / "hard.sqlite").acquire()
    with pytest.raises(StateError, match="hard links"):
        StateLock(db).acquire()


def test_two_simultaneous_replays_create_at_most_one_account(tmp_path):
    from scenarios import rich_scenario

    cfg = write_config(tmp_path)
    inp = rich_scenario().write(tmp_path / "in.jsonl")
    db = tmp_path / "race.sqlite"
    args = [PY, "-m", "paperbot", "replay", "--config", str(cfg), "--input", str(inp), "--state", str(db), "--quiet"]
    procs = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    outs = [p.communicate(timeout=120) for p in procs]
    codes = sorted(p.returncode for p in procs)
    assert codes in ([0, 3], [0, 4]), (codes, outs)
    loser_err = [o[1] for p, o in zip(procs, outs, strict=True) if p.returncode != 0][0]
    assert "LOCKED" in loser_err or "refusing to overwrite" in loser_err
    r = cli("report", "--state", str(db))
    assert r.returncode == 0 and "reconciliation OK" in r.stdout


def test_read_only_status_during_a_live_replay_sees_consistent_snapshots(tmp_path):
    from scenarios import rich_scenario

    from paperbot.report import build_report
    from paperbot.storage import Storage

    cfg = write_config(tmp_path)
    inp = rich_scenario().write(tmp_path / "in.jsonl")
    db = tmp_path / "live.sqlite"
    p = subprocess.Popen([PY, "-m", "paperbot", "replay", "--config", str(cfg), "--input", str(inp), "--state",
                          str(db), "--quiet"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    seen = set()
    while p.poll() is None:
        if not db.exists() or db.stat().st_size == 0:
            continue
        try:
            s = Storage.open_readonly(db)
        except Exception:  # noqa: BLE001 - the file may still be mid-creation
            continue
        try:
            r = build_report(s)
        finally:
            s.close()
        assert r["reconciliation"]["ok"], r["reconciliation"]["problems"]
        seen.add(r["cursor"]["seq"])
    assert p.wait() == 0, p.stderr.read()
    assert len(seen) > 3  # many distinct mid-run snapshots were checked
