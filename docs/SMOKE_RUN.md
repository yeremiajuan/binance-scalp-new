# Public-data smoke session (24 hours): instructions

**Status: NOT RUN.** The handoff authorizes Phase 2 implementation and mocked/offline testing only. The build
environment could not reach any Binance host, so no public data was received. Run this only after it is
separately authorized, on the machine that will host the trial.

## Before starting

1. Confirm the facts this repository cannot verify:
   - the OS (Linux was exercised; on Windows, first run `scripts\verify_windows.py` and follow
     `docs/WINDOWS.md`, which has the PowerShell version of every command below; macOS untested);
   - that the machine stays awake and on mains power for 24 h;
   - local disk with at least 5 GB free;
   - an NTP-synchronized clock;
   - outbound HTTPS 443 to `api.binance.com` and WSS 443 to `data-stream.binance.vision`;
   - that public Binance market data is accessible from your network and location.
2. Install, then run the checks and record their outputs:

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -e '.[test]'   # websockets >= 15.0 is required
python -m pytest -q && ruff check src tests scripts && python -m build
paperbot validate-config config/forward.toml
```

3. Review `config/forward.toml`. Every value is a hypothesis; fees are provisional, not verified account rates.
   Optional: enable `[telegram]` with an allowlisted chat id and export the token variable. Never commit it.
4. Use a fresh state path for the smoke session (for example `state/smoke-YYYYMMDD.sqlite`).

## Run

```bash
paperbot run --config config/forward.toml --state state/smoke.sqlite --run-seconds 86400
```

The runner is a foreground process. Use a terminal multiplexer, or a user service that sends SIGTERM to stop.
In another terminal:

```bash
paperbot status    --state state/smoke.sqlite
paperbot positions --state state/smoke.sqlite
```

## Drills during the session (all local, all recorded)

| Drill | How | Expected |
|---|---|---|
| Forced disconnect | block the WS host briefly (firewall rule) or drop the network for ~2 min | `feed_ws_disconnected`, `quotes_stale`, `feed_disconnected` + `feed_recovery` blocks, health exit if exposed, reconnect with backoff, `feed_continuity_check` + gap backfill, no historical entry, `rearmed` only after a post-reconnect quote |
| Clock check failure | block `api.binance.com` before starting (or during a check) | `clock_unsynced` until a check succeeds; `feed_clock_check_failed` retries at 15 s, 30 s, ... |
| Stale quotes | as above, or suspend the process for 5 s (`kill -STOP`/`-CONT`) | `quotes_stale`, unprotected time disclosed, exits only on fresh quotes |
| Restart | `paperbot stop`, then `run` again | session row, retired attempts, flatten before rearm |
| Kill/reset | `paperbot kill ...` then `paperbot reset ... --confirm` while running | control rows, routed via the owner |
| Rate limiting | observe `X-MBX-USED-WEIGHT-1M` in raw recordings; do not provoke a ban | weights well below budget |
| Metadata change | wait for hourly refresh | `metadata_refreshed` (or `metadata_version` on change) |
| Telegram outage (if enabled) | revoke network to api.telegram.org for a few minutes | pending/ambiguous rows, no accounting change |

## After the session

```bash
paperbot report --state state/smoke.sqlite > state/smoke-report.txt
paperbot export-recording --state state/smoke.sqlite --out state/smoke.jsonl
paperbot replay-recording --config config/forward.toml --input state/smoke.jsonl --state state/smoke-replay.sqlite
python scripts/compare_states.py --recorded state/smoke.sqlite state/smoke-replay.sqlite   # economic tables must match
sqlite3 state/smoke.sqlite ".backup state/smoke-backup.sqlite"                   # consistent backup (SQLite API)
```

For the Phase 2 acceptance criteria, review:
- coverage (sessions, gaps, paused time, health events);
- that no signed call or order endpoint appears in raw recordings (every URL is an allowlisted public GET);
- that status reported paused and exposed states correctly;
- quote ages and how often health exits fired;
- disk growth.

Replaying a recording with identical economics does not authorize a trial, Testnet or live trading.
