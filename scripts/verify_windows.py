r"""Windows verification for binance-scalp-new (PAPER only). Needs Python 3.12+ and nothing else: no Bash, no WSL.

From the repository root in PowerShell:

    py -3.13 scripts\verify_windows.py                 # writes evidence\windows\ (about 10 minutes)
    py -3.13 scripts\verify_windows.py --skip-connectivity

It creates a clean virtual environment in a temporary folder, installs the project there, and saves each step's
exact commands and outputs under the evidence folder, with ``summary.md`` (PASS/FAIL per step) and ``SHA256SUMS``:

  environment.txt       OS, Python, git revision, disk, time service, sleep states, console code page, paths
  connectivity.txt      reachability of api.binance.com and data-stream.binance.vision (one GET /api/v3/time and one
                        TLS handshake; no market data is consumed; informational; --skip-connectivity skips it)
  pytest.txt            full test suite (-v, skip reasons listed)
  ruff.txt, build.txt   lint and package build
  validate_config.txt   both configurations
  synthetic_replay.txt  Phase 1 fixture regenerates byte-for-byte; two replays are identical; reconciliation OK
  recorded_replay.txt   MOCKED forward session -> export -> recorded replay -> table comparison
  ownership_drill.txt   real processes: owner on mocked data, status/positions, competing owners and path aliases,
                        account lock, ACLs of the per-user files, routed kill/reset, unauthenticated client refused,
                        Ctrl+Break graceful stop, crash (TerminateProcess) release, restart and reconciliation

Nothing here starts a public-data session, uses credentials, or places orders. On a non-Windows OS the script
refuses unless --allow-non-windows is given, and then labels the run as NOT a Windows validation.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WINDOWS = os.name == "nt"


class Recorder:
    def __init__(self, out: Path):
        self.out = out
        self.results: list[tuple[str, str, str]] = []  # (step, file, PASS/FAIL/INFO)

    def file(self, name: str) -> Path:
        return self.out / name

    def write(self, name: str, text: str) -> None:
        with open(self.file(name), "a", encoding="utf-8", newline="\n") as fh:
            fh.write(text)

    def run(self, name: str, cmd: list[str], *, cwd: Path = ROOT, env: dict | None = None,
            timeout: float = 1800) -> tuple[int, str]:
        self.write(name, "$ " + subprocess.list2cmdline([str(c) for c in cmd]) + "\n")
        try:
            p = subprocess.run([str(c) for c in cmd], cwd=cwd, env=env, capture_output=True, encoding="utf-8",
                               errors="replace", timeout=timeout)
            out, rc = (p.stdout or "") + (p.stderr or ""), p.returncode
        except (OSError, subprocess.TimeoutExpired) as exc:
            out, rc = f"{type(exc).__name__}: {exc}\n", -1
        self.write(name, out + ("" if out.endswith("\n") or not out else "\n") + f"[exit {rc}]\n\n")
        return rc, out

    def result(self, step: str, name: str, ok: bool | None) -> None:
        self.results.append((step, name, "INFO" if ok is None else ("PASS" if ok else "FAIL")))


# ----------------------------------------------------------------------------------------------- environment


def environment(rec: Recorder, vpy: Path | None) -> None:
    name = "environment.txt"
    facts = {
        "utc_now": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "windows": platform.win32_ver() if WINDOWS else None,
        "machine": platform.machine(),
        "python_driver": sys.version.replace("\n", " "),
        "python_driver_path": sys.executable,
        "repo": str(ROOT),
        "repo_under_onedrive": any(v and str(ROOT).lower().startswith(v.lower())
                                   for v in (os.environ.get("OneDrive"), os.environ.get("OneDriveConsumer"),
                                             os.environ.get("OneDriveCommercial"))),
        "LOCALAPPDATA": os.environ.get("LOCALAPPDATA"),
        "disk_free_gb": round(shutil.disk_usage(ROOT).free / 1e9, 1),
        "stdout_encoding": sys.stdout.encoding,
    }
    if WINDOWS:
        import ctypes

        facts["repo_drive_type"] = ctypes.windll.kernel32.GetDriveTypeW(os.path.splitdrive(str(ROOT))[0] + "\\")
        facts["drive_type_legend"] = "2 removable, 3 fixed, 4 network (refused for state), 5 cd-rom, 6 ram disk"
    rec.write(name, "### facts\n" + json.dumps(facts, indent=2) + "\n\n")
    if shutil.which("git"):
        rec.run(name, ["git", "rev-parse", "HEAD"])
        rec.run(name, ["git", "status", "--porcelain"])
        rec.run(name, ["git", "config", "--get", "core.autocrlf"])
    if WINDOWS:
        rec.run(name, ["cmd", "/c", "ver"])
        rec.run(name, ["cmd", "/c", "chcp"])
        rec.run(name, ["w32tm", "/query", "/status"])  # NTP: entries need |server - local| <= 1 s
        rec.run(name, ["powercfg", "/a"])  # sleep states: the machine must stay awake during a session
        rec.run(name, ["powercfg", "/query", "SCHEME_CURRENT", "SUB_SLEEP", "STANDBYIDLE"])
    else:
        rec.run(name, ["uname", "-srm"])
    if vpy is not None:
        rec.run(name, [vpy, "--version"])
        rec.run(name, [vpy, "-m", "pip", "freeze", "--exclude-editable"])
    rec.result("environment facts", name, None)


def connectivity(rec: Recorder) -> None:
    import socket
    import ssl
    import urllib.request

    name = "connectivity.txt"
    rec.write(name, "Reachability only: one GET /api/v3/time and one TLS handshake. No stream is subscribed.\n\n")
    try:
        sent = time.time()
        with urllib.request.urlopen("https://api.binance.com/api/v3/time", timeout=10) as r:
            body = json.loads(r.read())
        recv = time.time()
        offset_ms = body["serverTime"] - (sent + recv) / 2 * 1000
        rec.write(name, f"GET https://api.binance.com/api/v3/time -> HTTP {r.status}; clock offset estimate "
                        f"{offset_ms:+.0f} ms (round trip {(recv - sent) * 1000:.0f} ms)\n")
    except Exception as exc:  # noqa: BLE001 - a fact to report, not a failure of this machine's setup
        rec.write(name, f"GET https://api.binance.com/api/v3/time -> FAILED: {type(exc).__name__}: {exc}\n")
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection(("data-stream.binance.vision", 443), timeout=10) as s:
            with ctx.wrap_socket(s, server_hostname="data-stream.binance.vision") as t:
                rec.write(name, f"TLS data-stream.binance.vision:443 -> OK ({t.version()})\n")
    except Exception as exc:  # noqa: BLE001
        rec.write(name, f"TLS data-stream.binance.vision:443 -> FAILED: {type(exc).__name__}: {exc}\n")
    rec.result("connectivity probe", name, None)


# ------------------------------------------------------------------------------------------------- checks


def make_venv(rec: Recorder, work: Path) -> Path:
    path = work / "venv"
    rec.write("environment.txt", f"### clean virtual environment: {path}\n")
    venv.EnvBuilder(with_pip=True, clear=True).create(path)
    vpy = path / ("Scripts/python.exe" if WINDOWS else "bin/python")
    rc1, _ = rec.run("environment.txt", [vpy, "-m", "pip", "install", "-q", "--upgrade", "pip"])
    rc2, _ = rec.run("environment.txt", [vpy, "-m", "pip", "install", "-q", "-e", f"{ROOT}[test]"])
    rec.result("clean install (pip install -e .[test])", "environment.txt", rc2 == 0)
    if rc2 != 0:
        raise SystemExit("installation failed; see environment.txt")
    return vpy


def checks(rec: Recorder, vpy: Path, work: Path) -> None:
    env = {**os.environ, "PYTHONUTF8": "1"}
    rc, _ = rec.run("pytest.txt", [vpy, "-m", "pytest", "-v", "-rs", "-p", "no:cacheprovider"], env=env, timeout=3600)
    rec.result("test suite", "pytest.txt", rc == 0)
    rc, _ = rec.run("ruff.txt", [vpy, "-m", "ruff", "check", "src", "tests", "scripts"])
    rec.result("lint", "ruff.txt", rc == 0)
    rc, out = rec.run("build.txt", [vpy, "-m", "build", "--outdir", work / "dist"], timeout=900)
    rec.result("package build", "build.txt", rc == 0 and "Successfully built" in out)
    ok = all(rec.run("validate_config.txt", [vpy, "-m", "paperbot", "validate-config", c])[0] == 0
             for c in ("config/forward.toml", "config/paper.toml"))
    rec.result("validate configurations", "validate_config.txt", ok)

    name = "synthetic_replay.txt"
    regen = work / "regenerated.jsonl"
    rc1, _ = rec.run(name, [vpy, "scripts/make_demo_fixture.py", regen])
    same = rc1 == 0 and regen.read_bytes() == (ROOT / "fixtures" / "synthetic_demo.jsonl").read_bytes()
    rec.write(name, f"fixture regenerates byte-for-byte (LF line endings kept): {same}\n\n")
    states = [work / "replay-a.sqlite", work / "replay-b.sqlite"]
    rcs = [rec.run(name, [vpy, "-m", "paperbot", "replay", "--config", "config/paper.toml", "--input",
                          "fixtures/synthetic_demo.jsonl", "--state", s, "--quiet"])[0] for s in states]
    rc3, cmp_out = rec.run(name, [vpy, "scripts/compare_states.py", *states])
    rc4, rep = rec.run(name, [vpy, "-m", "paperbot", "report", "--state", states[0]])
    rec.result("synthetic replay determinism", name, same and rcs == [0, 0] and rc3 == 0
               and "IDENTICAL" in cmp_out and "reconciliation OK" in rep)

    rc, out = rec.run("recorded_replay.txt", [vpy, "scripts/phase2_mocked_demo.py", work / "recorded"], env=env)
    rec.result("mocked recorded-replay comparison", "recorded_replay.txt", rc == 0 and "IDENTICAL" in out)

    rc, _ = rec.run("ownership_drill.txt", [vpy, Path(__file__).resolve(), "--drill", work / "drill"], env=env,
                    timeout=1200)
    rec.result("ownership, controls and shutdown drill", "ownership_drill.txt", rc == 0)


# -------------------------------------------------------------------------------------------------- drill


def drill(work: Path) -> int:
    """Runs inside the clean venv. Prints a transcript; exit code 0 only if every check passed."""
    import signal
    import uuid

    sys.path.insert(0, str(ROOT / "tests"))
    from conftest import write_forward_config  # test helpers live in tests/
    from fake_binance import FakeMarket, FakeStream

    from paperbot.reconcile import reconcile
    from paperbot.storage import Storage
    from paperbot.synthetic import staircase
    from paperbot.timeutil import FIVE_MINUTES_US
    from paperbot.userdirs import default_control_dir
    from paperbot_net.control import info_path, send_control

    work.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []

    def check(label: str, ok: bool, detail: str = "") -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}" + (f" -- {detail}" if detail and not ok else ""), flush=True)
        if not ok:
            failures.append(label)

    def cli(*args: str) -> subprocess.CompletedProcess:
        p = subprocess.run([sys.executable, "-m", "paperbot", *args], capture_output=True, encoding="utf-8",
                           errors="replace", timeout=180)
        print(f"$ paperbot {' '.join(args)}\n{p.stdout}{p.stderr}[exit {p.returncode}]", flush=True)
        return p

    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    stream = FakeStream(FakeMarket(staircase(start, 13)).last_close(now), drop_first=False)
    account = f"verify-{uuid.uuid4().hex[:8]}"  # never the user's real paper account
    cfg = write_forward_config(work, overrides={"run.account_id": account},
                               forward={"profile_lock_dir": "default", "heartbeat_ms": 500,
                                        "quote_sample_ms": 500, "reconnect_initial_ms": 100,
                                        "reconnect_max_ms": 500})
    state = work / "drill.sqlite"
    owners: list[subprocess.Popen] = []
    print(f"MOCKED public data (local server ws://127.0.0.1:{stream.port}); account {account}; state {state}")
    print(f"per-user control dir: {default_control_dir()}\n", flush=True)

    def start_owner(tag: str) -> subprocess.Popen:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if WINDOWS else 0
        log = open(work / f"{tag}.log", "w", encoding="utf-8")  # noqa: SIM115 - closed with the process
        p = subprocess.Popen([sys.executable, str(ROOT / "tests" / "owner_process.py"), str(cfg), str(state),
                              f"ws://127.0.0.1:{stream.port}/stream"], stdout=log, stderr=subprocess.STDOUT,
                             creationflags=flags)
        owners.append(p)
        return p

    def ready(p: subprocess.Popen, min_cursor: int = 0) -> dict:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and p.poll() is None:
            try:
                r = send_control(str(state), {"cmd": "ping"}, timeout=5)
                if r.get("ok") and r.get("cursor", 0) >= min_cursor:
                    return r
            except (OSError, EOFError, ValueError):
                pass
            time.sleep(0.3)
        return {"ok": False, "exit": p.poll()}

    def sessions() -> list[dict]:
        store = Storage.open_readonly(state)
        try:
            return [dict(r) for r in store.conn.execute("SELECT * FROM sessions ORDER BY started_wall")]
        finally:
            store.close()

    try:
        print("### 1. owner A starts and answers on the authenticated local control channel")
        a = start_owner("owner-a")
        r = ready(a, 10)
        check("owner A ready", r.get("ok", False), str(r))
        with open(info_path(str(state)), encoding="utf-8") as fh:
            info = json.load(fh)
        print(f"control endpoint: {info['family']} {info['address']}")
        check("Windows uses a named pipe" if WINDOWS else "Linux uses a Unix socket",
              info["family"] == ("AF_PIPE" if WINDOWS else "AF_UNIX"))

        print("\n### 2. read-only status and positions while the owner runs")
        st = cli("status", "--state", str(state))
        check("status", st.returncode == 0 and "PAPER | PUBLIC DATA | FORWARD | MOCKED" in st.stdout)
        pos = cli("positions", "--state", str(state))
        check("positions", pos.returncode == 0 and "positions" in pos.stdout)

        print("\n### 3. competing owners and path aliases are refused")
        spellings = [str(state), str(work / "sub" / ".." / state.name)]
        (work / "sub").mkdir(exist_ok=True)
        if WINDOWS:
            import _winapi
            import ctypes

            _winapi.CreateJunction(str(work), str(work / "junction"))
            spellings += [str(work / "junction" / state.name), str(state).upper(), str(state).lower()]
            buf = ctypes.create_unicode_buffer(1024)
            if ctypes.windll.kernel32.GetShortPathNameW(str(state), buf, 1024) and buf.value != str(state):
                spellings.append(buf.value)  # 8.3 short name (only when the volume has short names enabled)
        else:
            os.symlink(work, work / "dir-link", target_is_directory=True)
            spellings.append(str(work / "dir-link" / state.name))
        for s in spellings:
            p = cli("run", "--config", str(cfg), "--state", s, "--run-seconds", "5")
            check(f"second owner refused for {s}", p.returncode == 3 and "LOCKED" in p.stderr)
        other = work / "other.sqlite"
        p = cli("run", "--config", str(cfg), "--state", str(other), "--run-seconds", "5")
        check("same account on another state path refused (profile lock)",
              p.returncode == 3 and "already owned by another process" in p.stderr and not other.exists())

        print("\n### 4. per-user files and their access control")
        from paperbot.config import load_config

        lock_dir = load_config(cfg).forward.profile_lock_dir
        targets = [default_control_dir(), info_path(str(state)), lock_dir]
        if WINDOWS:
            for t in targets:
                out = subprocess.run(["icacls", t], capture_output=True, encoding="utf-8", errors="replace").stdout
                print(f"$ icacls {t}\n{out}")
                broad = [g for g in ("Everyone", "BUILTIN\\Users", "Authenticated Users", "S-1-1-0", "S-1-5-11",
                                     "S-1-5-32-545") if g in out]
                check(f"no broad group can read {os.path.basename(t)}", not broad, f"found {broad}")
        else:
            import stat

            for t in targets:
                mode = stat.S_IMODE(os.stat(t).st_mode)
                print(f"{t}: mode {oct(mode)}")
                check(f"{os.path.basename(t)} is private", mode & 0o077 == 0)

        print("\n### 5. kill and reset are routed through the running owner; a client without the key is refused")
        k = cli("kill", "--state", str(state), "--reason", "verification drill")
        check("kill routed", k.returncode == 0 and "routed to the active forward runner" in k.stdout)
        rs = cli("reset", "--state", str(state), "--latch", "manual_kill", "--reason", "drill reviewed", "--confirm")
        check("reset routed", rs.returncode == 0 and "routed to the active forward runner" in rs.stdout)
        from multiprocessing import connection as mpc

        try:
            c = mpc.Client(info["address"], family=info["family"], authkey=os.urandom(32))
            c.close()
            refused = False
        except (mpc.AuthenticationError, EOFError, OSError) as exc:
            print(f"unauthenticated client: {type(exc).__name__}: {exc}")
            refused = True
        check("client with a wrong key refused", refused)

        print("\n### 6. graceful stop by console signal (Ctrl+Break on Windows, SIGINT on Linux)")
        a.send_signal(signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT)
        code = a.wait(timeout=60)
        why = sessions()[-1]["stop_reason"]
        check("owner A exited 0 after the signal", code == 0, f"exit {code}")
        check("session stop reason recorded", (why or "").startswith("signal "), str(why))
        rep = cli("report", "--state", str(state))
        check("reconciliation OK after graceful stop", "reconciliation OK" in rep.stdout)

        print("\n### 7. crash: owner B is terminated without cleanup; the OS releases both locks")
        b = start_owner("owner-b")
        rb = ready(b, (sessions()[-1]["end_cursor"] or 0) + 5)
        check("owner B (restart) ready", rb.get("ok", False), str(rb))
        b.kill()  # TerminateProcess on Windows, SIGKILL on Linux
        b.wait(timeout=60)
        k2 = cli("kill", "--state", str(state), "--reason", "after crash")
        check("direct control works after the crash (lock released)", k2.returncode == 0 and "direct" in k2.stdout)

        print("\n### 8. restart after the crash: reconcile, keep the latch, recover; stop command")
        c3 = start_owner("owner-c")
        rc3 = ready(c3, (rb.get("cursor") or 0) + 5)
        check("owner C (restart after crash) ready", rc3.get("ok", False), str(rc3))
        st = cli("status", "--state", str(state))
        check("manual_kill latch persisted across the crash", "manual_kill" in st.stdout)
        rs = cli("reset", "--state", str(state), "--latch", "manual_kill", "--reason", "drill done", "--confirm")
        check("reset routed to the restarted owner", rs.returncode == 0 and "routed" in rs.stdout)
        sp = cli("stop", "--state", str(state), "--reason", "drill complete")
        check("stop command", sp.returncode == 0 and c3.wait(timeout=60) == 0)
        ss = sessions()
        print(json.dumps([{k: s[k] for k in ("kind", "stopped_wall", "stop_reason")} for s in ss], indent=1))
        check("sessions: start (signal stop), restart (crash: never stopped), restart (stop command)",
              [s["kind"] for s in ss] == ["start", "restart", "restart"] and ss[1]["stopped_wall"] is None
              and ss[2]["stop_reason"] == "drill complete")
        store = Storage.open_readonly(state)
        try:
            problems = reconcile(store, store.load_state())
        finally:
            store.close()
        check("final reconciliation", problems == [], str(problems))
        check("control endpoint withdrawn after stop", not os.path.exists(info_path(str(state))))
    finally:
        for p in owners:
            if p.poll() is None:
                p.kill()
        stream.close()
        for tag in ("owner-a", "owner-b", "owner-c"):
            log = work / f"{tag}.log"
            if log.exists():
                print(f"\n--- {tag} log ---\n" + log.read_text(encoding="utf-8", errors="replace"))
    print(f"\nDRILL {'PASSED' if not failures else 'FAILED: ' + ', '.join(failures)}")
    return 0 if not failures else 1


# --------------------------------------------------------------------------------------------------- main


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--skip-connectivity", action="store_true")
    ap.add_argument("--allow-non-windows", action="store_true")
    ap.add_argument("--drill", type=Path, help=argparse.SUPPRESS)  # internal: runs inside the clean venv
    args = ap.parse_args()
    if args.drill is not None:
        return drill(args.drill)
    if not WINDOWS and not args.allow_non_windows:
        print("This is the Windows verification script; run it on Windows (or pass --allow-non-windows).")
        return 2
    if sys.version_info < (3, 12):
        print(f"Python 3.12+ is required (this is {platform.python_version()}).")
        return 2
    out = args.out or (ROOT / "evidence" / ("windows" if WINDOWS else f"platform-{platform.system().lower()}"))
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("*"):
        if f.is_file():
            f.unlink()  # including the "NOT RUN" placeholder README
    rec = Recorder(out)
    work = Path(tempfile.mkdtemp(prefix="pbverify-"))
    try:
        vpy = make_venv(rec, work)
        environment(rec, vpy)
        if not args.skip_connectivity:
            connectivity(rec)
        checks(rec, vpy, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    ok = all(r != "FAIL" for _, _, r in rec.results)
    label = ("WINDOWS VALIDATION " + ("PASSED" if ok else "FAILED")) if WINDOWS else \
        f"{platform.system().upper()} RUN ({'all checks passed' if ok else 'FAILED'}) - NOT a Windows validation"
    lines = [f"# Verification summary: {label}", "",
             f"- date (UTC): {datetime.datetime.now(datetime.UTC).isoformat(timespec='seconds')}",
             f"- platform: {platform.platform()}", f"- python: {platform.python_version()}", "",
             "| step | file | result |", "|---|---|---|"]
    lines += [f"| {s} | `{f}` | {r} |" for s, f, r in rec.results]
    rec.write("summary.md", "\n".join(lines) + "\n")
    rec.write("README.md", f"Generated by scripts/verify_windows.py. Result: {label}. See summary.md.\n")
    sums = [f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}" for p in sorted(out.glob("*"))
            if p.is_file() and p.name != "SHA256SUMS"]
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8", newline="\n")
    print("\n".join(lines))
    print(f"\nEvidence written to {out}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
