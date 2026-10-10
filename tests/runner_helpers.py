"""Helpers for tests that drive the real threaded runner: observable readiness instead of fixed sleeps.

* ``RunnerThread`` runs ``run_forward`` without a time limit and stops it through the real control channel.
* ``wait_for`` polls an observable condition (usually a read-only query of the state database) until a bounded
  deadline and fails with a diagnostic, so slow machines take longer but never pass or fail by luck.
* ``SlowCommits`` slows every SQLite commit by a fixed delay (on top of the real fsync) and records the wall time of
  each commit and of each quote-freshness transition, to measure health detection in wall time.
"""

from __future__ import annotations

import threading
import time

from paperbot.storage import Storage
from paperbot_net.control import send_control
from paperbot_net.runner import run_forward


def poll_rows(state, sql: str, args: tuple = ()) -> list[dict]:
    """Rows of a read-only query while the runner may still be creating the database: a missing file, or a file
    whose schema transaction has not committed yet, means "not ready" (no rows), never an error."""
    import sqlite3

    from conftest import rows

    if not state.exists():
        return []
    try:
        return rows(state, sql, args)
    except sqlite3.OperationalError:
        return []


def poll_count(state, sql: str, args: tuple = ()) -> int:
    r = poll_rows(state, sql, args)
    return r[0]["n"] if r else 0


DEADLINE_S = 120.0  # generous upper bound for a 1-vCPU host with slow disk; conditions usually hold in seconds


def wait_for(predicate, describe, timeout: float = DEADLINE_S, interval: float = 0.1):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout:.0f} s waiting for {describe}; last observation: {last!r}")


class RunnerThread:
    def __init__(self, cfg, state, *, transport, ws_url=None, **kwargs):
        self.state = state
        self.result: dict = {}

        def target():
            try:
                self.result["code"] = run_forward(str(cfg), str(state), transport=transport, ws_url=ws_url,
                                                  install_signals=False, log=lambda *_: None, **kwargs)
            except BaseException as exc:  # noqa: BLE001 - surfaced by stop()
                self.result["error"] = exc

        self.thread = threading.Thread(target=target, name="runner-under-test", daemon=True)
        self.thread.start()

    def running(self) -> bool:
        return self.thread.is_alive()

    def stop(self, timeout: float = DEADLINE_S) -> int:
        if self.thread.is_alive():
            wait_for(lambda: self._try_stop(), "the owner to accept a stop command", timeout)
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "runner did not stop"
        if "error" in self.result:
            raise self.result["error"]
        return self.result["code"]

    def _try_stop(self) -> bool:
        try:
            return bool(send_control(str(self.state), {"cmd": "stop", "reason": "test done"}, timeout=10).get("ok"))
        except (OSError, EOFError, ValueError):
            return not self.thread.is_alive()


class SlowCommits:
    """Context manager: every Storage.commit_event sleeps ``delay_s`` first (a slow disk), and is recorded."""

    def __init__(self, delay_s: float):
        self.delay_s = delay_s
        self.commits: list[tuple[float, str]] = []  # (wall time after commit, event type)
        self.stale_flips: list[tuple[float, int | None, bool]] = []  # (wall, last quote stamp, inventory open)
        self._fresh = None

    def __enter__(self):
        original = self.original = Storage.commit_event
        timer = self

        def slow_commit(store, raw, disposition, detail, state, rec):
            time.sleep(timer.delay_s)
            original(store, raw, disposition, detail, state, rec)
            wall = time.time()
            timer.commits.append((wall, raw.event_type))
            fresh = state.health.quotes_fresh
            if timer._fresh and not fresh:
                q = state.last_quote
                timer.stale_flips.append((wall, q.recv_us if q else None, state.balances.btc_total > 0))
            timer._fresh = fresh

        Storage.commit_event = slow_commit
        return self

    def __exit__(self, *exc):
        Storage.commit_event = self.original
