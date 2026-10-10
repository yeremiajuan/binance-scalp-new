# Running on native Windows (PowerShell)

**PAPER only.** No credentials, signing, account or order endpoints, Testnet or live trading exist in this code.
WSL is not needed and not used. Everything below is PowerShell (Windows PowerShell 5.1 or PowerShell 7).

**Windows validation status: NOT RUN.** The implementation was built and tested on Linux. Native Windows acceptance
requires running `scripts\verify_windows.py` (section 3) on the Windows PC that will host the trial, and reviewing
its `evidence\windows\summary.md`.

## 1. Requirements

| Item | Requirement |
|---|---|
| Windows | Windows 10 (1809 or later) or Windows 11, 64-bit |
| Python | 3.12 or 3.13 from python.org or `winget install Python.Python.3.13` (includes the `py` launcher) |
| Git | Git for Windows (`winget install Git.Git`) |
| Disk | A local NTFS drive with at least 5 GB free. Not a network share or mapped drive (refused), and preferably not a OneDrive/Dropbox-synced folder (sync clients can lock or roll back SQLite files) |
| Clock | Windows Time service synchronized (entries stay blocked until `|server - local| <= 1 s` is verified) |
| Power | The PC must not sleep or hibernate during a session (section 6) |
| Network | Outbound HTTPS 443 to `api.binance.com` and WSS 443 to `data-stream.binance.vision` (only for the authorized smoke session) |

## 2. Install

```powershell
# choose a local folder outside OneDrive, for example C:\paperbot
New-Item -ItemType Directory -Force C:\paperbot | Out-Null
Set-Location C:\paperbot
git clone https://github.com/yeremiajuan/binance-scalp-new.git
Set-Location binance-scalp-new
git checkout claude/trusting-sagan-t9e8iz

# line endings: the repository forces LF through .gitattributes; confirm nothing was converted
git status --porcelain            # must print nothing

py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[test]"

# optional: activate the venv for the session (if script execution is blocked, call .\.venv\Scripts\python.exe
# directly or allow local scripts for your user: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned)
.\.venv\Scripts\Activate.ps1

# UTF-8 for console output and redirected files
$env:PYTHONUTF8 = "1"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
```

Runtime dependencies: `websockets` (public market streams), `filelock` (OS-backed process locks) and `tzdata`
(the IANA time-zone database). Windows has no IANA database of its own, so Python's `zoneinfo` reads
`Asia/Jakarta` (the risk-day time zone) from the `tzdata` package; without it a clean install cannot even
validate a configuration (`risk.day_timezone 'Asia/Jakarta' is not a valid IANA zone`). `pip install -e .`
installs it. No compiler or Windows-specific package (pywin32) is needed.

Check after installing (prints `Asia/Jakarta`):

```powershell
.\.venv\Scripts\python.exe -c "import zoneinfo; print(zoneinfo.ZoneInfo('Asia/Jakarta'))"
```

If it fails, the environment is missing the package: `.\.venv\Scripts\python.exe -m pip install tzdata`. Keep
`tzdata` up to date with the rest of the environment; Asia/Jakarta has been UTC+7 without daylight saving since
1964, so its day boundary (17:00 UTC) does not depend on the release.

## 3. Verify this PC (one script, no Bash/WSL)

```powershell
py -3.13 scripts\verify_windows.py                       # about 10 minutes
# or, without the two reachability probes to Binance:
py -3.13 scripts\verify_windows.py --skip-connectivity
Get-Content evidence\windows\summary.md
```

It creates its own clean virtual environment in a temporary folder and writes `evidence\windows\`:
`environment.txt` (Windows build, Python, git state, `w32tm /query /status`, `powercfg /a`, code page, disk),
`connectivity.txt` (one `GET /api/v3/time` and one TLS handshake; informational), `pytest.txt`, `ruff.txt`,
`build.txt`, `validate_config.txt`, `synthetic_replay.txt`, `timezone.txt` (both configurations validated and the
demo replayed with no system time-zone database, compared with the default replay), `recorded_replay.txt`,
`ownership_drill.txt`, `summary.md` and `SHA256SUMS`. It does not start a public-data session.

The ownership drill runs real processes on mocked public data (a local WebSocket server and a fake REST transport):
a running owner, `status`/`positions`, competing owners refused for the same path and for aliases (`..`,
upper/lower case, a directory junction, the 8.3 short name when the volume has them), the account lock refusing a
second state path, the ACLs (`icacls`) of the per-user control and lock files, routed `kill`/`reset`, a client with
a wrong key refused, a graceful stop by Ctrl+Break, a crash (`TerminateProcess`) releasing both locks, a restart
that reconciles, keeps the kill latch and recovers, `paperbot stop`, and a final reconciliation.

Commit the folder (or send it for review):

```powershell
git add evidence\windows
git commit -m "Windows verification evidence"
git push
```

### Manual check: interactive Ctrl+C

Ctrl+C cannot be sent to a single child process by a script on Windows, so the drill uses Ctrl+Break. To confirm
Ctrl+C by hand (mocked data, no network):

```powershell
$env:PYTHONUTF8 = "1"
New-Item -ItemType Directory -Force $env:TEMP\pbcheck | Out-Null
.\.venv\Scripts\python.exe -c "import sys; sys.path.insert(0, 'tests'); from conftest import write_forward_config; from pathlib import Path; print(write_forward_config(Path(r'$env:TEMP\pbcheck'), overrides={'run.account_id': 'ctrlc-check'}))"
.\.venv\Scripts\python.exe tests\owner_process.py $env:TEMP\pbcheck\forward.toml $env:TEMP\pbcheck\s.sqlite ws://127.0.0.1:9/stream
# wait for "forward runner ... owns ...", press Ctrl+C once; expected last line:
#   PAPER | PUBLIC DATA | forward runner ... stopped (signal SIGINT); cursor N
.\.venv\Scripts\paperbot.exe report --state $env:TEMP\pbcheck\s.sqlite | Select-String "reconciliation"
```

(The stream URL points at a closed port on purpose: the runner reports the missing connectivity and keeps entries
blocked, which is all this check needs.)

## 4. Operate

All commands accept Windows paths. Use one state file per paper account on a local drive.

```powershell
paperbot validate-config config\forward.toml
New-Item -ItemType Directory -Force state | Out-Null

# the owner process (foreground). Stop it with Ctrl+C, Ctrl+Break or `paperbot stop` from another window.
paperbot run --config config\forward.toml --state state\forward.sqlite

# in a second PowerShell window (activate the venv there too):
paperbot status    --state state\forward.sqlite      # read-only
paperbot positions --state state\forward.sqlite      # read-only
paperbot kill      --state state\forward.sqlite --reason "why"                       # routed to the owner
paperbot reset     --state state\forward.sqlite --latch manual_kill --reason "why" --confirm
paperbot stop      --state state\forward.sqlite --reason "why"                       # graceful stop
paperbot report    --state state\forward.sqlite > state\report.txt
paperbot export-recording --state state\forward.sqlite --out state\session.jsonl
paperbot replay-recording --config config\forward.toml --input state\session.jsonl --state state\replay.sqlite
python scripts\compare_states.py --recorded state\forward.sqlite state\replay.sqlite
```

Exit codes: 0 ok, 2 configuration/input error, 3 locked (another owner), 4 state error, 5 halted.

### Stopping and crashes

| Event | Behavior |
|---|---|
| Ctrl+C or Ctrl+Break in the owner's window | graceful stop: session closed with `signal SIGINT`/`SIGBREAK`, locks released, control endpoint withdrawn |
| `paperbot stop` | graceful stop through the control channel |
| Closing the console window, Task Manager "End task", `Stop-Process`, power loss, crash | treated as a crash: Windows releases both locks when the process ends; the session row stays without a stop time; the next `paperbot run` reconciles, cancels unfilled entries, retires unresolved attempts without inventing fills, flattens recovered inventory on fresh quotes and only then rearms |

Avoid closing the window to stop the runner; use Ctrl+C or `paperbot stop`.

## 5. Files and protection on Windows

| What | Where | Protection |
|---|---|---|
| State database | your `--state` path (`.sqlite`) | local drive only; UNC paths and network drives refused; hard-linked databases refused |
| State lock | `<state>.lock` next to it | `LockFileEx` on one byte (via `filelock`); released by Windows when the process ends; its existence means nothing |
| Account (profile) lock | `%LOCALAPPDATA%\paperbot\locks\account-<hash>.lock` (`profile_lock_dir = "default"`) | same primitive; one owner per paper account on this PC, whatever the state path |
| Control endpoint | named pipe `\\.\pipe\paperbot-<hash>-<random>` | local clients only (`PIPE_REJECT_REMOTE_CLIENTS`); default pipe DACL (creator, SYSTEM, Administrators); mutual key challenge on every connection; JSON only |
| Control key | `%LOCALAPPDATA%\paperbot\control\<hash>.json` | per-run 32-byte key; inherits your profile's ACL (you, SYSTEM, Administrators); removed on graceful stop |
| Raw recordings | `recordings_dir` (default `state\recordings`) | gzip JSON lines |

All path spellings of one state reach the same lock: relative paths, `..`, letter case, junctions and symlinks,
and 8.3 short names are resolved before locking. Only `status`, `positions` and `report` read the database
directly (read-only); every change goes through the running owner, or through the lock when no owner runs.

## 6. Before a session (authorized smoke run only)

```powershell
w32tm /query /status                 # "Source" should be a time server; resync if needed:
w32tm /resync                        # (needs an elevated PowerShell)
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
Get-PSDrive -Name C                  # free space
Test-NetConnection api.binance.com -Port 443
Test-NetConnection data-stream.binance.vision -Port 443
```

Exclude the state folder from real-time antivirus scanning if scans make SQLite commits slow (optional; record the
choice). Telegram is optional: set the token for the current window only with
`$env:PAPERBOT_TELEGRAM_BOT = "<token>"`. Never put it in the configuration, the repository or a committed file.
`setx` would store it in your user registry in plain text.

The 24-hour public-data smoke session (`docs/SMOKE_RUN.md`) and the month-long trial still need separate
authorization.
