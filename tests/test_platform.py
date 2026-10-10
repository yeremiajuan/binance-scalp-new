"""Platform layer: OS lock primitive, path classification, SQLite URIs, per-user dirs and the control channel.

Tests marked ``windows_only`` / ``posix_only`` run on that platform only; the Windows-only behavior they cover is
listed in docs/WINDOWS.md, and tests/test_ownership.py covers the same protected behavior with real processes on
both platforms.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import threading

import pytest
from conftest import write_forward_config

from paperbot.config import load_config
from paperbot.oslock import LockUnavailable, OsLock
from paperbot.storage import (
    StateError,
    StateLock,
    StateLocked,
    Storage,
    _check_local_windows,
    canonical_state_path,
    path_key,
    readonly_uri,
)

windows_only = pytest.mark.skipif(os.name != "nt", reason="Windows-specific behavior")
posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX-specific behavior")


def test_os_lock_is_exclusive_per_handle_and_reusable_after_release(tmp_path):
    a, b = OsLock(str(tmp_path / "x.lock"), "a\n"), OsLock(str(tmp_path / "x.lock"), "b\n")
    assert a.try_acquire() and a.held
    assert not b.try_acquire() and not b.held  # a second handle, even in the same process, is refused
    a.release()
    assert not a.held and b.try_acquire()
    b.release()
    assert (tmp_path / "x.lock").exists()  # the lock file is kept; its existence never means "locked"
    c = OsLock(str(tmp_path / "x.lock"), "c\n")
    assert c.try_acquire()
    c.release()


def test_os_lock_refuses_anything_but_a_native_os_lock(tmp_path, monkeypatch):
    import filelock

    from paperbot import oslock

    monkeypatch.setattr(oslock, "FileLock", filelock.SoftFileLock)  # an existence-only lock is never accepted
    with pytest.raises(LockUnavailable):
        OsLock(str(tmp_path / "soft.lock"), "x").try_acquire()

    class NoFlock(filelock.FileLock):
        def _acquire(self):
            raise OSError(38, "Function not implemented")  # ENOSYS: filesystem without flock

    monkeypatch.setattr(oslock, "FileLock", NoFlock)
    with pytest.raises(LockUnavailable, match="cannot take an OS lock"):
        OsLock(str(tmp_path / "nosys.lock"), "x").try_acquire()
    assert not (tmp_path / "soft.lock").exists()


def test_state_lock_maps_contention_and_reports_the_canonical_path(tmp_path):
    db = tmp_path / "s.sqlite"
    with StateLock(db) as first:
        assert first.held
        with pytest.raises(StateLocked, match=re.escape(canonical_state_path(db))):
            StateLock(tmp_path / "." / "s.sqlite").acquire()
    with StateLock(db):
        pass


@pytest.mark.parametrize(("directory", "drive_type", "refused"), [
    ("C:\\Users\\me\\paper", 3, False),  # DRIVE_FIXED
    ("\\\\?\\C:\\Users\\me\\paper", 3, False),  # extended-length local path
    ("D:\\paper", 2, False),  # DRIVE_REMOVABLE is local
    ("Z:\\paper", 4, True),  # DRIVE_REMOTE (mapped network drive)
    ("\\\\server\\share\\paper", 3, True),  # UNC
    ("\\\\?\\UNC\\server\\share\\paper", 3, True),  # extended-length UNC
])
def test_windows_network_paths_are_refused(directory, drive_type, refused):
    seen = []

    def fake_drive_type(root):
        seen.append(root)
        return drive_type

    if refused:
        with pytest.raises(StateError, match="network"):
            _check_local_windows(directory, drive_type=fake_drive_type)
    else:
        _check_local_windows(directory, drive_type=fake_drive_type)
        assert seen and seen[0].endswith(":\\")


@pytest.mark.parametrize("name", ["plain", "with space", "hash#mark", "percent%20", "question?", "unicode-ü"])
def test_readonly_uri_opens_awkward_paths(tmp_path, name):
    if os.name == "nt" and "?" in name:
        pytest.skip("'?' is not allowed in Windows file names")
    d = tmp_path / name
    d.mkdir()
    db = d / "s.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t(x)")
    conn.execute("INSERT INTO t VALUES (42)")
    conn.commit()
    conn.close()
    ro = sqlite3.connect(readonly_uri(db), uri=True)
    try:
        assert ro.execute("SELECT x FROM t").fetchone() == (42,)
        with pytest.raises(sqlite3.OperationalError):
            ro.execute("INSERT INTO t VALUES (1)")  # really read-only
    finally:
        ro.close()


def test_path_key_folds_case_only_on_windows():
    p = canonical_state_path("Some/State.sqlite")
    assert path_key(p) == (p.lower() if os.name == "nt" else p)


def test_profile_lock_dir_default_is_per_user(tmp_path):
    from paperbot.userdirs import default_profile_lock_dir, user_state_dir

    cfg = load_config(write_forward_config(tmp_path, forward={"profile_lock_dir": "default"}))
    assert cfg.forward.profile_lock_dir == default_profile_lock_dir()
    assert cfg.forward.profile_lock_dir.startswith(user_state_dir())
    if os.name == "nt":
        assert "AppData" in cfg.forward.profile_lock_dir or os.environ.get("LOCALAPPDATA", "") in \
            cfg.forward.profile_lock_dir
    # the path setting is not part of the configuration identity
    assert cfg.sha256 == load_config(write_forward_config(tmp_path, name="other.toml")).sha256


class _Out:
    """Stands in for the owner's queue: answers each control request like the owner thread would."""

    def __init__(self):
        self.seen: list[dict] = []

    def put(self, kind, data):
        assert kind == "control"
        reply = data.pop("_reply")
        self.seen.append(data)
        reply.put({"ok": True, "result": f"did {data.get('cmd')}"})


@pytest.fixture
def server(tmp_path):
    from paperbot_net.control import ControlServer

    out = _Out()
    srv = ControlServer(canonical_state_path(tmp_path / "s.sqlite"), out, threading.Event())
    srv.start()
    yield tmp_path / "s.sqlite", srv, out
    srv.close()


def test_control_round_trip_filters_request_fields_and_withdraws_endpoint(server):
    from paperbot_net.control import info_path, send_control

    state, srv, out = server
    reply = send_control(str(state), {"cmd": "kill", "reason": "r", "latch": "manual_kill", "_reply": "x",
                                      "wall_utc": "forged", "extra": [1, 2]})
    assert reply == {"ok": True, "result": "did kill"}
    assert out.seen == [{"cmd": "kill", "reason": "r", "latch": "manual_kill"}]  # nothing else crosses over
    srv.close()
    assert not srv.is_alive() and not os.path.exists(info_path(str(state)))
    with pytest.raises(OSError):
        send_control(str(state), {"cmd": "ping"})


def test_control_requires_the_per_run_key_in_both_directions(server):
    from multiprocessing import connection as mpc

    from paperbot_net.control import info_path

    state, srv, out = server
    with open(info_path(str(state)), encoding="utf-8") as fh:
        info = json.load(fh)
    assert len(bytes.fromhex(info["authkey"])) == 32
    with pytest.raises(mpc.AuthenticationError):
        mpc.Client(info["address"], family=info["family"], authkey=os.urandom(32))
    assert out.seen == []  # refused before any request was read
    # a client that connects without answering the challenge does not block other clients
    stuck = mpc.Client(info["address"], family=info["family"])
    try:
        from paperbot_net.control import send_control

        assert send_control(str(state), {"cmd": "ping"})["ok"]
    finally:
        stuck.close()


def test_control_endpoint_for_another_state_is_rejected(server, tmp_path):
    from paperbot_net.control import info_path, send_control

    state, srv, out = server
    other = tmp_path / "other.sqlite"
    with open(info_path(str(state)), encoding="utf-8") as fh:
        info = json.load(fh)
    with open(info_path(str(other)), "w", encoding="utf-8") as fh:
        json.dump(info, fh)  # a copied/stale endpoint file naming a different state
    with pytest.raises(ValueError, match="another state"):
        send_control(str(other), {"cmd": "kill", "reason": "x"})
    assert out.seen == []


@posix_only
def test_posix_control_files_are_private(server):
    from paperbot_net.control import control_dir, info_path

    state, srv, out = server
    assert stat.S_IMODE(os.stat(control_dir()).st_mode) == 0o700
    assert stat.S_IMODE(os.stat(info_path(str(state))).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(srv.address).st_mode) == 0o600


@windows_only
def test_windows_control_endpoint_is_a_local_named_pipe_with_unguessable_name(server):
    from paperbot_net.control import LocalPipeListener, endpoint_key

    state, srv, out = server
    assert srv.family == "AF_PIPE" and isinstance(srv.listener, LocalPipeListener)
    prefix = "\\\\.\\pipe\\paperbot-" + endpoint_key(str(state)) + "-"
    assert srv.address.startswith(prefix) and len(srv.address) == len(prefix) + 16


@windows_only
def test_windows_local_drive_check_accepts_the_test_directory(tmp_path):
    from paperbot.storage import _check_local_filesystem

    _check_local_filesystem(canonical_state_path(tmp_path))  # real GetDriveTypeW: a local disk passes


@windows_only
def test_windows_state_open_close_and_reopen_releases_file_handles(tmp_path):
    """Windows refuses to replace or delete files with open handles; the lock and SQLite must close cleanly."""
    from paperbot.config import load_config as lc
    from paperbot.engine import initial_state
    from paperbot.recorded import forward_meta

    cfg = lc(write_forward_config(tmp_path))
    db = tmp_path / "w.sqlite"
    with StateLock(db) as lock:
        Storage.create(lock, forward_meta(cfg), initial_state(cfg)).close()
    os.replace(db, tmp_path / "moved.sqlite")  # no handle left open
    os.replace(tmp_path / "moved.sqlite", db)
    with StateLock(db) as lock:
        Storage.open_existing(lock).close()


def test_control_never_reads_a_request_from_an_unauthenticated_client(server):
    from multiprocessing import connection as mpc

    from paperbot_net.control import info_path

    state, srv, out = server
    with open(info_path(str(state)), encoding="utf-8") as fh:
        info = json.load(fh)
    raw = mpc.Client(info["address"], family=info["family"])  # no key at all
    try:
        assert raw.poll(10), "the server must challenge before reading anything"
        assert raw.recv_bytes().startswith(b"#CHALLENGE#")
        raw.send_bytes(b'{"cmd": "kill", "reason": "intruder"}')  # a request instead of the digest
        assert raw.poll(10) and raw.recv_bytes() == b"#FAILURE#"
    finally:
        raw.close()
    assert out.seen == []  # nothing reached the owner


# ---------------------------------------------- no system timezone database (native Windows, minimal images)

NO_SYSTEM_TZ = {"PYTHONTZPATH": ""}  # zoneinfo then finds no OS database and can only use the tzdata package
CONFIGS = ("config/forward.toml", "config/paper.toml")


def _py(code: str, *args: str, block_tzdata: bool = False) -> subprocess.CompletedProcess:
    from conftest import ROOT

    prefix = "import sys; sys.modules['tzdata'] = None\n" if block_tzdata else ""
    return subprocess.run([sys.executable, "-c", prefix + code, *args], capture_output=True, encoding="utf-8",
                          errors="replace", cwd=ROOT, env={**os.environ, **NO_SYSTEM_TZ}, timeout=120)


def test_without_system_tz_data_zoneinfo_uses_the_installed_tzdata_package():
    r = _py("import zoneinfo, importlib.resources as res\n"
            "assert zoneinfo.TZPATH == (), zoneinfo.TZPATH\n"
            "assert res.files('tzdata.zoneinfo').joinpath('Asia/Jakarta').is_file()\n"
            "print(zoneinfo.ZoneInfo('Asia/Jakarta'))")
    assert r.returncode == 0 and r.stdout.strip() == "Asia/Jakarta", r.stderr
    # control: the same environment without the package reproduces the failure seen on a clean Windows install
    r = _py("import zoneinfo\nzoneinfo.ZoneInfo('Asia/Jakarta')", block_tzdata=True)
    assert r.returncode != 0 and "ZoneInfoNotFoundError" in r.stderr


@pytest.mark.parametrize("config", CONFIGS)
def test_both_configurations_validate_without_system_tz_data(config):
    from conftest import ROOT

    from paperbot.config import load_config

    p = subprocess.run([sys.executable, "-m", "paperbot", "validate-config", config], capture_output=True,
                       encoding="utf-8", errors="replace", cwd=ROOT, env={**os.environ, **NO_SYSTEM_TZ},
                       timeout=120)
    assert p.returncode == 0, p.stdout + p.stderr
    assert "configuration OK" in p.stdout and f"config_sha256 {load_config(ROOT / config).sha256}" in p.stdout
    blocked = _py("from paperbot.cli import main\nsys.exit(main(['validate-config', sys.argv[1]]))", config,
                  block_tzdata=True)
    assert blocked.returncode == 2 and "is not a valid IANA zone" in blocked.stderr  # the reviewed defect


def test_local_day_boundaries_are_identical_with_package_tz_data():
    """Daily risk baselines roll over at 00:00 Asia/Jakarta (17:00 UTC); the package data must agree with the
    system database (the full demo replay is compared in scripts/verify_windows.py)."""
    from paperbot.timeutil import local_date, local_iso, parse_ts

    stamps = [parse_ts(t) for t in ("2026-10-01T16:59:59.999Z", "2026-10-01T17:00:00Z", "2026-12-31T17:00:00Z",
                                    "2027-03-28T17:00:00Z", "2026-10-25T01:30:00Z")]
    code = ("import json\nfrom paperbot.timeutil import local_date, local_iso\n"
            "print(json.dumps([[local_date(s, 'Asia/Jakarta'), local_iso(s, 'Asia/Jakarta')] "
            "for s in json.loads(sys.argv[1])]))")
    r = _py("import sys\n" + code, json.dumps(stamps))
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout) == [[local_date(s, "Asia/Jakarta"), local_iso(s, "Asia/Jakarta")] for s in stamps]
