"""Process ownership on the real OS: separate processes, real locks, the real control channel and real signals.

Each owner is a separate Python process (tests/owner_process.py) running the actual forward runner on mocked
public data (a local WebSocket server in this test process and a fake REST transport in the owner). Competing
processes and controls use the real ``paperbot`` CLI (``python -m paperbot``). These tests run unchanged on Linux and
native Windows; the only platform branches are the alias spellings (symlink on Linux, junction and letter case on
Windows) and the stop signal (SIGINT on Linux, CTRL_BREAK_EVENT on Windows, because Ctrl+C cannot be sent to a
single child process there).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from conftest import rows, write_forward_config
from fake_binance import FakeMarket, FakeStream
from runner_helpers import poll_rows, wait_for

from paperbot.reconcile import reconcile
from paperbot.storage import Storage
from paperbot.timeutil import FIVE_MINUTES_US
from paperbot_net.control import send_control

PY = sys.executable
OWNER = Path(__file__).resolve().parent / "owner_process.py"
WINDOWS = os.name == "nt"
READY_S = 90


def cli(*args, timeout=120):
    return subprocess.run([PY, "-m", "paperbot", *args], capture_output=True, encoding="utf-8", errors="replace",
                          timeout=timeout)


class Owner:
    def __init__(self, tmp: Path, cfg: Path, state: Path, ws_url: str, name: str):
        self.state = state
        self.log = tmp / f"{name}.log"
        self.err = tmp / f"{name}.err"
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0  # lets the test send CTRL_BREAK to it alone
        with open(self.log, "w", encoding="utf-8") as out, open(self.err, "w", encoding="utf-8") as err:
            self.proc = subprocess.Popen([PY, str(OWNER), str(cfg), str(state), ws_url], stdout=out, stderr=err,
                                         creationflags=flags)

    def output(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace") + self.err.read_text(
            encoding="utf-8", errors="replace")

    def wait_ready(self, min_cursor: int = 0) -> dict:
        deadline = time.monotonic() + READY_S
        last = None
        while time.monotonic() < deadline:
            assert self.proc.poll() is None, f"owner exited early ({self.proc.returncode}):\n{self.output()}"
            try:
                last = send_control(str(self.state), {"cmd": "ping"}, timeout=5)
                if last.get("ok") and last.get("cursor", 0) >= min_cursor:
                    return last
            except (OSError, EOFError, ValueError):
                pass
            time.sleep(0.2)
        raise AssertionError(f"owner not ready (last {last}):\n{self.output()}")

    def wait_exit(self, timeout: float = 60) -> int:
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            raise AssertionError(f"owner did not exit:\n{self.output()}") from None

    def kill(self) -> None:
        self.proc.kill()  # SIGKILL on Linux, TerminateProcess on Windows: no cleanup code runs
        self.proc.wait(timeout=30)


@pytest.fixture
def env(tmp_path):
    from paperbot.synthetic import staircase

    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    stream = FakeStream(FakeMarket(staircase(start, 13)).last_close(now), drop_first=False)
    cfg = write_forward_config(tmp_path, forward={"reconnect_initial_ms": 100, "reconnect_max_ms": 500,
                                                   "heartbeat_ms": 500, "quote_sample_ms": 500})
    owners: list[Owner] = []

    def start_owner(state: Path, name: str = "owner") -> Owner:
        o = Owner(tmp_path, cfg, state, f"ws://127.0.0.1:{stream.port}/stream", name)
        owners.append(o)
        return o

    yield tmp_path, cfg, start_owner
    for o in owners:
        if o.proc.poll() is None:
            o.proc.kill()
            o.proc.wait(timeout=30)
    stream.close()


def rearmed_after_last_session_start(state: Path) -> bool:
    kinds = [r["kind"] for r in poll_rows(state, "SELECT kind FROM health_events ORDER BY id")]
    if "session_start" not in kinds:
        return False
    last = len(kinds) - 1 - kinds[::-1].index("session_start")
    return "rearmed" in kinds[last:]


def alias_spellings(tmp: Path, state: Path) -> list[str]:
    """Other spellings of ``state`` that must reach the same lock."""
    (tmp / "sub").mkdir(exist_ok=True)
    out = [str(tmp / "sub" / ".." / state.name)]
    if WINDOWS:
        import _winapi

        _winapi.CreateJunction(str(tmp), str(tmp / "junction"))  # no privilege needed, unlike symlinks
        out += [str(tmp / "junction" / state.name), str(state).upper(), str(state).lower()]
    else:
        os.symlink(tmp, tmp / "dir-link", target_is_directory=True)
        os.symlink(state, tmp / "file-link.sqlite")
        out += [str(tmp / "dir-link" / state.name), str(tmp / "file-link.sqlite")]
    return out


def test_two_competing_owners_one_wins_and_aliases_cannot_bypass(env):
    tmp, cfg, start_owner = env
    state = tmp / "fwd.sqlite"
    a = start_owner(state, "a")
    a.wait_ready(min_cursor=5)
    for spelling in [str(state), *alias_spellings(tmp, state)]:
        r = cli("run", "--config", str(cfg), "--state", spelling, "--run-seconds", "5")
        assert r.returncode == 3, (spelling, r.stdout, r.stderr)
        assert "LOCKED" in r.stderr and "locked by another process" in r.stderr, r.stderr
    assert a.proc.poll() is None  # the owner was not disturbed
    assert a.wait_ready()["ok"]


def test_account_lock_refuses_a_second_state_path_for_the_same_account(env):
    tmp, cfg, start_owner = env
    a = start_owner(tmp / "fwd.sqlite", "a")
    a.wait_ready(min_cursor=5)
    other = tmp / "other.sqlite"
    r = cli("run", "--config", str(cfg), "--state", str(other), "--run-seconds", "5")
    assert r.returncode == 3, (r.stdout, r.stderr)
    assert "paper account 'test-account' is already owned by another process" in r.stderr
    assert not other.exists()  # refused before any database was created
    b = start_owner(other, "b")  # the owner subprocess path is refused the same way
    assert b.wait_exit() == 3 and "already owned" in b.output()


def test_crash_releases_ownership_and_restart_reconciles_and_recovers(env):
    tmp, cfg, start_owner = env
    state = tmp / "fwd.sqlite"
    a = start_owner(state, "a")
    cursor = a.wait_ready(min_cursor=20)["cursor"]
    a.kill()  # crash: no session stop, no lock release by the program
    # the OS released both locks: a direct (unrouted) control works at once, and its latch persists
    k = cli("kill", "--state", str(state), "--reason", "after crash")
    assert k.returncode == 0 and "direct" in k.stdout, (k.stdout, k.stderr)
    b = start_owner(state, "b")
    assert b.wait_ready(min_cursor=cursor + 5)["ok"]
    # recovery is asserted below, so wait for it (bounded) instead of assuming it happened by the stop
    wait_for(lambda: rearmed_after_last_session_start(state), "the restarted owner to rearm")
    st = cli("status", "--state", str(state))
    assert st.returncode == 0 and "manual_kill" in st.stdout  # persistent latch kept across the crash
    r = cli("reset", "--state", str(state), "--latch", "manual_kill", "--reason", "reviewed", "--confirm")
    assert r.returncode == 0 and "routed to the active forward runner" in r.stdout, (r.stdout, r.stderr)
    s = cli("stop", "--state", str(state), "--reason", "test done")
    assert s.returncode == 0, s.stderr
    assert b.wait_exit() == 0
    sessions = rows(state, "SELECT * FROM sessions ORDER BY started_wall")
    assert [x["kind"] for x in sessions] == ["start", "restart"]
    assert sessions[0]["stopped_wall"] is None  # the crash is visible, not papered over
    assert sessions[1]["stop_reason"] == "test done"
    health = [x["kind"] for x in rows(state, "SELECT kind FROM health_events ORDER BY id")]
    assert health.count("session_start") == 2 and "rearmed" in health
    store = Storage.open_readonly(state)
    try:
        assert reconcile(store, store.load_state()) == []
    finally:
        store.close()


def test_controls_are_routed_through_the_running_owner_and_authenticated(env):
    from multiprocessing import connection as mpc

    from paperbot_net.control import info_path

    tmp, cfg, start_owner = env
    state = tmp / "fwd.sqlite"
    a = start_owner(state, "a")
    a.wait_ready(min_cursor=5)
    st = cli("status", "--state", str(state))
    assert st.returncode == 0 and "PAPER | PUBLIC DATA | FORWARD | MOCKED" in st.stdout, st.stderr
    pos = cli("positions", "--state", str(state))
    assert pos.returncode == 0 and "positions" in pos.stdout, pos.stderr
    k = cli("kill", "--state", str(state), "--reason", "drill")
    assert k.returncode == 0 and "routed to the active forward runner" in k.stdout, (k.stdout, k.stderr)
    r = cli("reset", "--state", str(state), "--latch", "manual_kill", "--reason", "drill done", "--confirm")
    assert r.returncode == 0 and "routed to the active forward runner" in r.stdout, (r.stdout, r.stderr)
    # a client without the per-run key is refused before any request is read
    import json

    with open(info_path(str(state)), encoding="utf-8") as fh:
        info = json.load(fh)
    with pytest.raises((mpc.AuthenticationError, EOFError, OSError)):
        conn = mpc.Client(info["address"], family=info["family"], authkey=b"x" * 32)
        conn.send_bytes(b'{"cmd": "kill", "reason": "intruder"}')
        conn.recv_bytes()
    assert a.wait_ready()["ok"]
    s = cli("stop", "--state", str(state), "--reason", "drill stop")
    assert s.returncode == 0 and "graceful stop requested" in s.stdout, s.stderr
    assert a.wait_exit() == 0
    controls = rows(state, "SELECT kind, latch, reason FROM control_events ORDER BY id")
    assert [(c["kind"], c["reason"]) for c in controls] == [("kill", "drill"), ("reset", "drill done")]
    assert not os.path.exists(info_path(str(state)))  # the endpoint is withdrawn on a graceful stop
    after = cli("kill", "--state", str(state), "--reason", "owner gone")
    assert after.returncode == 0 and "direct" in after.stdout  # no owner: the direct path applies again


def test_graceful_signal_shutdown_then_reconciled_restart(env):
    tmp, cfg, start_owner = env
    state = tmp / "fwd.sqlite"
    a = start_owner(state, "a")
    a.wait_ready(min_cursor=10)
    if WINDOWS:
        a.proc.send_signal(signal.CTRL_BREAK_EVENT)  # console control event -> SIGBREAK handler
        expected = "signal SIGBREAK"
    else:
        a.proc.send_signal(signal.SIGINT)  # what Ctrl+C delivers
        expected = "signal SIGINT"
    assert a.wait_exit() == 0, a.output()
    assert expected in a.output()
    (first,) = rows(state, "SELECT * FROM sessions")
    assert first["stop_reason"] == expected and first["stopped_wall"] is not None
    rep = cli("report", "--state", str(state))
    assert rep.returncode == 0 and "reconciliation OK" in rep.stdout, rep.stdout
    b = start_owner(state, "b")
    b.wait_ready(min_cursor=first["end_cursor"] + 5)
    assert cli("stop", "--state", str(state), "--reason", "done").returncode == 0
    assert b.wait_exit() == 0
    kinds = [x["kind"] for x in rows(state, "SELECT kind FROM sessions ORDER BY started_wall")]
    assert kinds == ["start", "restart"]
