# binance-scalp-new: PAPER-only BTCUSDT paper bot

**PAPER only. No live trading exists.** This repository implements `plan/PROJECT_PLAN.md`:

* **Phase 1 (accepted at `e3c5a92`)**: an offline, deterministic paper core (strategy B20-T5-v1, conservative
  LIMIT IOC simulation, native-fee accounting, exchange constraints, risk latches, atomic SQLite state) with
  SYNTHETIC replay.
* **Phase 2 (review fixes for `87b130b` applied, awaiting independent re-review)**: a local forward runner on **public** Binance spot market
  data (REST + WebSocket), recording and deterministic replay of recorded sessions, health/recovery handling,
  local monitoring commands and optional Telegram notifications.

There are no trading credentials, signers, account endpoints, order endpoints, Testnet adapter or live switch
anywhere in this code, and none can be configured. Simulated fills (including the documented fill-time resizing)
are not achievable real executions. Nothing here is evidence of a trading edge. The 24-hour public-data smoke
session and the month-long trial have **not** been run (see `docs/SMOKE_RUN.md`).

## Install and check

```bash
python3 -m venv .venv && . .venv/bin/activate      # Python 3.12+
pip install -e '.[test]'                            # runtime dependency: websockets >= 15.0 (public streams)
python -m pytest -q                                 # ~3-4 minutes; offline: no network, no credentials
ruff check src tests scripts
python -m build                                     # sdist + wheel
```

Requirements: Linux (verified) or native Windows 10/11 (implemented; **Windows validation NOT RUN**: run
`scripts\verify_windows.py` on the PC, see `docs/WINDOWS.md` for PowerShell setup and operation; WSL is not
needed). macOS is untested. State must live on a local disk. Runtime dependencies: `websockets` and `filelock`.

## Phase 1: synthetic replay (unchanged)

```bash
mkdir -p state
paperbot replay  --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state state/demo.sqlite
paperbot status  --state state/demo.sqlite          # read-only
paperbot report  --state state/demo.sqlite [--json] # read-only full report
paperbot resume  --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state state/demo.sqlite
paperbot kill    --state state/demo.sqlite --reason "why"
paperbot reset   --state state/demo.sqlite --latch manual_kill --reason "why" --confirm
scripts/evidence.sh                                 # regenerates the Phase 1 evidence/ artifacts
```

## Phase 2: forward runner on public market data

### What it connects to

| Purpose | Endpoint (public, unauthenticated, GET only) | Weight |
|---|---|---|
| server time / clock offset | `https://api.binance.com/api/v3/time` | 1 |
| metadata (hourly) | `/api/v3/exchangeInfo?symbol=BTCUSDT`, `/api/v3/executionRules?symbol=BTCUSDT` | 20 + 2 |
| warm-up / gap backfill | `/api/v3/klines?symbol=BTCUSDT&interval=1m` | 2 |
| filter/execution-rule references | `/api/v3/avgPrice?symbol=BTCUSDT`, `/api/v3/referencePrice?symbol=BTCUSDT` | 2 + 2 |
| live data | `wss://data-stream.binance.vision/stream?streams=btcusdt@kline_1m/btcusdt@bookTicker/btcusdt@avgPrice/btcusdt@referencePrice` | n/a |

Hosts are allowlisted in `src/paperbot_net/hosts.py`. `data-api.binance.vision` can be selected for REST but does
not serve `executionRules`/`referencePrice`, so entries then stay blocked (an unavailable reference blocks orders).

### Local facts you must provide (not verifiable from this repository)

* The machine's OS (Linux was exercised; Windows validation NOT RUN), that it stays awake for the whole session, and a running NTP
  client (entries stay blocked until a server-time check succeeds with `|server - local| <= 1 s`).
* Outbound connectivity: HTTPS 443 to `api.binance.com` and WSS 443 to `data-stream.binance.vision`. The
  environment where this code was written could **not** reach Binance (DNS/egress blocked), so no live
  connectivity has been tested. Whether Binance public data is accessible from your location/network is yours to
  confirm; do not infer it from timezone.
* Free disk: roughly 1-2 GB per week for the SQLite state (about one engine input per second) plus compressed raw
  recordings (all WebSocket frames); measure during the smoke session.

### Commands

```bash
paperbot validate-config config/forward.toml
paperbot run       --config config/forward.toml --state state/forward.sqlite   # foreground owner process
paperbot status    --state state/forward.sqlite      # feed health, entry blocks, queued exits, mark age, dust
paperbot positions --state state/forward.sqlite      # open position, queued exit, inventory/dust, unresolved risk
paperbot report    --state state/forward.sqlite [--json]
paperbot kill      --state state/forward.sqlite --reason "why"           # routed through the active owner
paperbot reset     --state state/forward.sqlite --latch manual_kill --reason "why" --confirm
paperbot stop      --state state/forward.sqlite --reason "why"           # graceful stop (or Ctrl-C / SIGTERM)
paperbot export-recording --state state/forward.sqlite --out state/session.jsonl
paperbot replay-recording --config config/forward.toml --input state/session.jsonl --state state/replay.sqlite
```

`run` holds two OS locks for its lifetime: the canonical state path and the paper account id (profile lock).
While it runs, `kill`/`reset`/`stop` go through its authenticated local control channel (a Unix socket on Linux,
a local-only named pipe on Windows; see `docs/WINDOWS.md`), so there
is still a single writer. On restart it reconciles the database, cancels unfilled entries, retires unresolved IOC
attempts without fills, treats pre-restart quotes as stale, re-warms/backfills indicators (backfilled bars never
create entries), flattens recovered tradable inventory on fresh quotes, and only then re-arms entries. A stream
disconnect is handled the same way: quotes become stale, entries stay blocked after reconnecting until candle
continuity is revalidated from REST and a quote received after the reconnection arrives.

### Optional Telegram (disabled by default)

Uncomment `[telegram]` in `config/forward.toml`, list your chat id(s) in `chat_ids` (allowlist), and export the
bot token in the environment variable named by `env_var` (default `PAPERBOT_TELEGRAM_BOT`). The token is never
written to configuration, the database, recordings or logs. The bot is read-only: `/status` and `/positions`;
every state change stays a local CLI command. Messages are PAPER-labeled, carry stable ids, may be duplicated
after an ambiguous send (no exactly-once claim) and are labeled DELAYED when late. A Telegram failure never
touches accounting.

### Recordings and replay

Every engine input of a forward session (normalized quotes, closed bars, backfill bars, references, metadata
versions, feed-health transitions, heartbeats, session starts) is committed with the decision it produced, and
audited controls are stored with their cursor. `export-recording` writes them as JSON Lines (header evidence
`PUBLIC`, with provenance); `replay-recording` reproduces the same decisions and accounting into a fresh state
labeled `PAPER | PUBLIC DATA | RECORDED REPLAY`. Raw WebSocket frames and REST responses are kept separately,
gzip-compressed, under `forward.recordings_dir`. Sessions built from fake servers or transports are labeled
`MOCKED` and are never presented as public observations.

## Layout

| Path | Contents |
|---|---|
| `src/paperbot/` | Network-free core: strategy, engine (single state owner), ledger, constraints, risk, storage, reconciliation, report, CLI, forward-session logic (`forward.py`), payload normalization (`normalize.py`), recorded sessions (`recorded.py`), OS locks (`oslock.py`), per-user dirs (`userdirs.py`) |
| `src/paperbot_net/` | Public-data adapters only: GET-only REST client, WebSocket client, threaded runner, authenticated local control channel (`control.py`), optional Telegram |
| `config/paper.toml`, `config/forward.toml` | Synthetic and forward configurations (hypotheses) |
| `fixtures/` | Dated SYNTHETIC metadata and the synthetic demo input |
| `tests/` | Phase 1 tests (137) plus Phase 2 mocked protocol/runner/recording/Telegram tests |
| `docs/` | `DECISIONS.md`, `LIMITATIONS.md`, `ACCEPTANCE.md`, `SMOKE_RUN.md`, `WINDOWS.md` |
| `evidence/` | Phase 1 artifacts (preserved); `evidence/phase2/` Phase 2 artifacts; `evidence/windows/` (filled by `scripts\verify_windows.py` on Windows); `evidence/platform-linux/` (the same script run on Linux) |
