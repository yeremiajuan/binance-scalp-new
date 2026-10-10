"""Evidence for the slow-persistence / silent-stream fix (reviewed source vs this source).

Usage: python scripts/evidence_slow_persistence.py [--reviewed bd43c0a] [--out evidence/slow-persistence]

VPS emulation (the reported host: 1 vCPU, ~16 ms per SQLite commit): every commit in the test process is slowed by
``--delay`` seconds (default 0.016), the test process is pinned to one CPU (``taskset``, Linux) and a CPU burner runs
on the same CPU. Nothing here touches the network beyond localhost, and no public-data session is started.

Files written:
  environment.txt          host facts
  reproduction.txt         the original silent-stream test on the reviewed source under emulation (expected FAIL),
                           the original test on this source (isolates the fixed-window test from the runtime fix),
                           and the wall-time stale-detection measurement on both sources
  regression_tests.txt     the new/rewritten tests on this source (PASS) and on the reviewed source (FAIL)
  owner_turn_tests.txt     review of fca062e: tests/test_owner_turn.py (lag applied before the input; held-candle
                           timestamps after a forced final chunk) on --reviewed-turn (fca062e's behavior with
                           owner_turn extracted, expected FAIL) and on this source (PASS), under the same emulation
  pytest.txt, ruff.txt, build.txt, recorded_replay.txt   full suite, lint, package build, mocked replay comparison
"""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

PLUGIN = '''
import os, time
from paperbot.storage import Storage
_delay = float(os.environ["EMULATED_COMMIT_DELAY_S"])
_orig = Storage.commit_event
def _slow(self, *a, **k):
    time.sleep(_delay)
    return _orig(self, *a, **k)
Storage.commit_event = _slow
'''

MEASURE = '''
import sys, time, json, tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[1])  # tests dir (current helpers)
import conftest  # noqa: F401
from conftest import write_forward_config
from fake_binance import FakeMarket, FakeRestTransport, SilentStream
from runner_helpers import RunnerThread, SlowCommits, wait_for
from paperbot.synthetic import staircase
from paperbot.timeutil import FIVE_MINUTES_US
tmp = Path(tempfile.mkdtemp())
now = time.time_ns() // 1000
market = FakeMarket(staircase((now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000, 13))
stream = SilentStream(market.last_close(now), quotes=3, later_quotes=0)
cfg = write_forward_config(tmp, forward={"ws_silence_s": 30, "heartbeat_ms": 500})
state = tmp / "m.sqlite"
with SlowCommits(float(sys.argv[2])) as timer:
    r = RunnerThread(cfg, state, transport=FakeRestTransport(market), ws_url=f"ws://127.0.0.1:{stream.port}/stream")
    try:
        wait_for(lambda: timer.stale_flips, "stale detection", timeout=180)
    finally:
        r.stop(); stream.close()
cand = [w for w, t in timer.commits if t == "candle"]
q = [w for w, t in timer.commits if t == "quote"]
flip_wall, stamp, _ = timer.stale_flips[0]
print(json.dumps({
    "warmup_bars_committed_before_stale_detection": sum(1 for w in cand if w < flip_wall),
    "warmup_bars_committed_in_total_before_stop": len(cand),
    "first_quote_commit_after_warmup_start_s": round(q[0] - cand[0], 2),
    "quote_received_after_warmup_start_s": round(stamp / 1e6 - cand[0], 2),
    "stale_due_after_warmup_start_s": round(stamp / 1e6 + 2.0 - cand[0], 2),
    "stale_detected_after_warmup_start_s": round(flip_wall - cand[0], 2),
    "wall_delay_after_quote_went_stale_s": round(flip_wall - (stamp / 1e6 + 2.0), 2),
}, indent=1))
'''


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reviewed", default="bd43c0a")
    ap.add_argument("--out", type=Path, default=ROOT / "evidence" / "slow-persistence")
    ap.add_argument("--delay", type=float, default=0.016)
    ap.add_argument("--reviewed-turn", default="1f2c3f1")
    args = ap.parse_args()
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("*"):
        if f.is_file():
            f.unlink()
    work = Path(tempfile.mkdtemp(prefix="pbslow-"))
    reviewed = work / "reviewed"
    reviewed.mkdir()
    archive = subprocess.run(["git", "archive", args.reviewed, "src", "tests"], cwd=ROOT, capture_output=True,
                             check=True).stdout
    subprocess.run(["tar", "-x", "-C", str(reviewed)], input=archive, check=True)
    reviewed_turn = work / "reviewed-turn"
    reviewed_turn.mkdir()
    archive = subprocess.run(["git", "archive", args.reviewed_turn, "src"], cwd=ROOT, capture_output=True,
                             check=True).stdout
    subprocess.run(["tar", "-x", "-C", str(reviewed_turn)], input=archive, check=True)
    (work / "plugin").mkdir()
    (work / "plugin" / "slow_disk_plugin.py").write_text(PLUGIN, encoding="utf-8")
    pin = ["taskset", "-c", "0"] if shutil.which("taskset") else []
    burner = subprocess.Popen(pin + [PY, "-c", "import time\nwhile True: sum(range(10000))"]) if pin else None

    def run(name: str, cmd: list, env: dict | None = None, note: str = "") -> tuple[int, str]:
        e = {**os.environ, **(env or {})}
        with open(out / name, "a", encoding="utf-8", newline="\n") as fh:
            if note:
                fh.write(f"### {note}\n")
            shown = [str(c) if len(str(c)) < 300 else "<inline measurement script: MEASURE in this file>"
                     for c in cmd]
            fh.write("$ " + " ".join(shown) + "\n")
            for k, v in (env or {}).items():
                fh.write(f"  env {k}={v}\n")
            p = subprocess.run([str(c) for c in cmd], cwd=ROOT, env=e, capture_output=True, encoding="utf-8",
                               errors="replace", timeout=3600)
            text = p.stdout + p.stderr
            fh.write(text + f"[exit {p.returncode}]\n\n")
        return p.returncode, text

    try:
        with open(out / "environment.txt", "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f"platform {platform.platform()}\npython {platform.python_version()}\ncpus {os.cpu_count()}\n"
                     f"emulation: commit delay {args.delay} s; pinned to one CPU with a competing CPU burner: "
                     f"{bool(pin)}\nreviewed source {args.reviewed}\n")
        run("environment.txt", ["git", "rev-parse", "HEAD"])
        run("environment.txt", ["git", "status", "--porcelain"])
        slow = {"EMULATED_COMMIT_DELAY_S": str(args.delay)}
        old_env = {**slow, "PYTHONPATH": f"{reviewed / 'src'}{os.pathsep}{work / 'plugin'}"}
        new_env = {**slow, "PYTHONPATH": f"{ROOT / 'src'}{os.pathsep}{work / 'plugin'}"}
        old_test = reviewed / "tests" / "test_runner_e2e.py"
        sel = "::test_silent_open_stream_is_detected_without_any_message"
        run("reproduction.txt", pin + [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "slow_disk_plugin",
                                       f"{old_test}{sel}"], old_env,
            "1. reviewed source + reviewed test under VPS emulation (expected: FAIL, as reported)")
        run("reproduction.txt", pin + [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "slow_disk_plugin",
                                       f"{old_test}{sel}"], new_env,
            "2. THIS source + the reviewed (fixed-window) test: isolates the fragile 6 s deadline from the runtime")
        run("reproduction.txt", pin + [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "slow_disk_plugin",
                                       f"{old_test}{sel}"],
            {**new_env, "EMULATED_COMMIT_DELAY_S": str(args.delay * 2)},
            "2b. THIS source + the reviewed fixed-window test on a disk twice as slow (result recorded as is)")
        run("reproduction.txt", pin + [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "slow_disk_plugin",
                                       f"tests/test_runner_e2e.py{sel}"],
            {**new_env, "EMULATED_COMMIT_DELAY_S": str(args.delay * 2)},
            "2c. THIS source + the rewritten readiness-based test on the same twice-as-slow disk (expected: PASS)")
        run("reproduction.txt", pin + [PY, "-c", MEASURE, ROOT / "tests", str(args.delay)],
            {"PYTHONPATH": str(reviewed / "src")},
            "3a. wall-time stale detection, reviewed source (runtime delay: the warm-up blocks the owner)")
        run("reproduction.txt", pin + [PY, "-c", MEASURE, ROOT / "tests", str(args.delay)],
            {"PYTHONPATH": str(ROOT / "src")},
            "3b. wall-time stale detection, this source (chunked warm-up interleaved with queued input/heartbeats)")

        tests = ["tests/test_runner_e2e.py", "tests/test_slow_persistence.py",
                 "tests/test_forward.py::test_chunked_warmup_interleaves_health_checks_and_ends_in_the_same_state",
                 "tests/test_forward.py::test_owner_lag_blocks_entries_until_the_queue_catches_up",
                 "tests/test_forward.py::test_a_new_session_clears_an_owner_lag_block_left_by_the_previous_process"]
        emu = {"PAPERBOT_TEST_COMMIT_DELAY_S": str(args.delay)}
        run("regression_tests.txt", pin + [PY, "-m", "pytest", "-v", "-p", "no:cacheprovider", *tests],
            {**emu, "PYTHONPATH": str(ROOT / "src")}, "this source under VPS emulation (expected: PASS)")
        run("regression_tests.txt", pin + [PY, "-m", "pytest", "-q", "-rf", "-p", "no:cacheprovider", *tests],
            {**emu, "PYTHONPATH": str(reviewed / "src")}, "reviewed source, same tests and emulation (expected: FAIL)")
        turn_tests = ["tests/test_owner_turn.py", "tests/test_slow_persistence.py", "tests/test_runner_e2e.py"]
        run("owner_turn_tests.txt", pin + [PY, "-m", "pytest", "-v", "-p", "no:cacheprovider", *turn_tests],
            {**emu, "PYTHONPATH": str(ROOT / "src")}, "this source under VPS emulation (expected: PASS)")
        run("owner_turn_tests.txt", pin + [PY, "-m", "pytest", "-q", "-rf", "-p", "no:cacheprovider",
                                           "tests/test_owner_turn.py"],
            {**emu, "PYTHONPATH": str(reviewed_turn / "src")},
            f"{args.reviewed_turn} (fca062e behavior, owner_turn extracted) (expected: 3 FAIL, exits test passes)")
    finally:
        if burner is not None:
            burner.kill()
            burner.wait()
    run("pytest.txt", [PY, "-m", "pytest", "-v", "-rs", "-p", "no:cacheprovider"], note="full suite (no emulation)")
    run("ruff.txt", [PY, "-m", "ruff", "check", "src", "tests", "scripts"])
    run("build.txt", [PY, "-m", "build", "--outdir", work / "dist"])
    run("recorded_replay.txt", [PY, "scripts/phase2_mocked_demo.py", work / "recorded"])
    sums = [f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}" for p in sorted(out.glob("*.txt"))]
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8", newline="\n")
    shutil.rmtree(work, ignore_errors=True)
    print(f"evidence written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
