# Phase 1 routine implementation decisions

`plan/PROJECT_PLAN.md` is the contract. These are the choices made where it leaves room. None of them changes a
strategy parameter, a cost assumption or a fill rule in a direction that makes results look better.

## Project and tooling

1. **Location.** The handoff prompt file names a `binance-spot-paper` folder, while the request says to work in
   the `binance-scalp-new` project. The package lives at the root of this repository (`src/paperbot`), and
   `plan/` is untouched. The separate stock-alert repository was not accessed or modified.
2. **No runtime dependencies.** Locking uses stdlib `fcntl.flock`, not a third-party file-lock package, so it
   supports Linux and macOS (only Linux was exercised). Windows is refused with a clear error.
   *Superseded by W1 (native Windows support): locking now uses `filelock`.*
3. **Strategy constants are frozen in code** (`strategy.py`), not in configuration. Changing them would create
   a new strategy version (`B20-T5-v1` is checked in the configuration). Costs, execution and risk values are
   configurable hypotheses.
4. **`account.starting_btc` must be 0.** Pre-existing inventory would need a declared cost basis and exit
   semantics that are outside this phase.

## Time, input and identity

5. **Timestamps are integer UTC microseconds.** The injected clock is the receipt time of each input event.
   Decision, submission, readiness, fill and commit times all come from that clock, so commit time equals the
   processing event's receipt time (zero processing latency is modeled). Wall-clock time is recorded only on
   audited kill/reset control events.
6. **The cursor is the event's line position** after the header. Input identity is the SHA-256 of the input
   file, and configuration identity is the SHA-256 of a canonical JSON form that excludes file paths. The
   metadata identity is the SHA-256 of the fixture text, which is also stored in the database.
7. **Malformed lines** are committed as `rejected` inputs with a reason rather than aborting the replay, so
   the cursor and the evidence stay aligned. Duplicate event ids are `duplicate`. Receipt times earlier than
   the clock are `rejected: non_monotonic_receipt` and cannot rewind state.

## Strategy conventions

8. **ATR "first available TR"** needs a previous close inside the current contiguous run, so the first bar of
   a run has no TR. The ATR is first available on the run's 15th bar.
9. **A gap resets every indicator** and warm-up restarts (250 new contiguous bars). Phase 1 has no
   backfill/repair path, so a repair can never become a forward signal. A missing next bar is also detected from
   the clock (`candle_missing`) when any later event arrives.
10. **A late finalized candle** (received more than 5 s after its interval end) still updates the indicators,
    because it is valid finalized data, but it cannot create an entry and it cancels any pending buy.
11. **Signal age** is measured from the decision time, which the contract defines as the signal time. The
    candle-lateness limit separately bounds the delay between bar end and decision.
12. **Cooldown:** after an exit completes, three 1m bars that *start* at or after the exit time must complete
    before a bar can signal. The bar during which the exit happened does not count.
13. **Every fresh crossing** (`close_i > H_i` and `close_(i-1) <= H_(i-1)`) is recorded as a candidate,
    including during warm-up, with all applicable skip reasons.

## Guards, sizing and execution

14. **Planning price `p` is the ask** of the fresh quote at decision. The full spread fraction is
    `(ask - bid) / mid`. Drift is `ceil_tick(ask*(1+slippage)) - signal close`.
15. **Guard order at decision:** the state guards are all evaluated and listed (warm-up, lateness, trend,
    position, pending order, cooldown, latch, day baseline, fresh quote, ATR). Then come spread, drift, cost
    gate, limit-versus-expected-fill, sizing and exchange filters, and the first failure is recorded. At the
    fill observation the engine rechecks signal age, latches, spread, drift and the cost gate (with that
    observation's ask and spread), then price protection, visible liquidity and exposure.
16. **Sizing caps** are risk budget `0.001E/(d+pC)`, free cash at the limit price (including a quote-asset fee),
    exposure headroom `0.20E - exposure` (exposure counts all BTC including dust at the bid plus buy
    reservations), 10% of visible ask size, LOT_SIZE `maxQty`, and MAX_POSITION headroom. The binding cap is
    recorded. The result is floored to the step and never rounded up.
17. **Eligible observation window.** A quote fills an IOC only if it is received at or after readiness, its
    exchange time (when supplied) is at or after readiness, and it is received no later than
    `ready + quote_max_age` (2 s). Without such an observation the IOC's outcome is unknown, so it is
    recorded as a zero fill. A buy is canceled; a sell's exit intent persists and retries on later fresh data.
    This prevents a quote long after an outage from standing in for the book at readiness.
18. **Fill-time caps** (review finding 2). At the fill observation the entry fill quantity is
    `floor_step(min(order qty, 10% of visible ask, risk cap, exposure cap))`. The risk cap is
    `0.001E/(d + pC)` with E, p and C (including the observed spread) recomputed from that observation. The
    exposure cap is `(0.20E - BTC mark) / (bid * BTC credited per unit)`. The submitted quantity is never
    enlarged. A reduced fill is a partial fill whose order outcome names the binding cap
    (`ioc_remainder_canceled;fill_cap=risk`); a zero cap is a zero fill (`fill_cap:risk`). The engine also
    asserts the budget and exposure invariants on every entry fill.
19. **Exit orders** sell `floor_step(free BTC)` capped at `maxQty`. When the remaining inventory cannot form a
    valid SELL (below step, `minQty` or minimum notional at the protected limit), it is kept as dust with basis,
    and the position closes with `residual_unsellable`/`exit_complete`. Dust stays in the average-cost pool. A
    later position's exit may sell part of it once `floor_step(total)` includes it, and that is accounted
    through the same pool.
20. **Exit triggers** are collected per event and the highest priority wins
    (halt > health > stop > trend invalidation > target > timeout). **Health triggers** (review finding 1):
    with an open position, stale quotes, a late finalized candle, a missing minute or a candle gap queue a
    persistent exit intent such as `health:quotes_stale`. It is submitted only on a fresh quote and can fill
    only on a later eligible observation after latency, so an outage never produces a fill. When the expected
    bar itself is the first event past the lateness deadline, it is classified late, not missing. There is one exit intent per position, its reason is
    fixed when it is created, and the intent survives partial and zero fills until no sellable inventory remains.
21. **Every exit retry** is a new order id submitted on the observation that closed the previous IOC. It
    references that observation and has a new limit and new latency, so it can only fill on a later
    observation.
22. **Filters are validated at submission only**, after rounding, as an exchange would. Increments are
    absolute multiples: `price % tickSize == 0` and `qty % stepSize == 0` (review finding 4), not offsets from
    `minPrice`/`minQty`. They are not
    re-applied to fill fragments. Zero increments or limits disable that rule without rounding or division.
    Reference prices for PERCENT_PRICE filters come from `reference_price` input events (labeled synthetic),
    must match `avgPriceMins` and must be at most `reference_max_age_ms` old.

## Accounting and risk

23. **Average-cost pool** for all BTC, which tracks gross execution cost (fill price × BTC credited) and
    entry-fee cost separately. Proportional allocations are quantized to 1e-18, and selling the whole pool
    takes the exact remainder, so basis never leaks.
24. **Report P&L:** realized net = sell proceeds after fee minus allocated basis. Realized execution gross =
    proceeds before fee minus allocated gross cost. Conservative unrealized net = liquidation value
    (sellable BTC at `floor_tick(bid*(1-slippage))` after the sell fee; dust counts zero) minus total remaining
    basis. The report checks `start + realized + unrealized == cash + liquidation value` exactly.
25. **Risk evaluation** runs at the end of every event that has a fresh mark, after any fill. The latch
    condition is `loss >= threshold` against the day baseline (daily) or the high-water mark (drawdown), using
    conservative liquidation equity. The day baseline is set by the first fresh mark of each Asia/Jakarta day.
    Stale marks never update the baseline or the HWM.
26. **Kill/reset** commit immediately and atomically, under the lock, to the latch state and an audit row.
    Their effects (cancel a pending buy, create a halt exit) are applied at the start of the next input event,
    before any fill check, so no order can fill in between. Offline replay has no time between events. Resetting
    `daily_loss` re-bases the day baseline, and resetting `drawdown` re-bases the HWM, at the next fresh mark.
    History is never deleted.
27. **Entries are blocked** if inventory becomes sellable while no position is open (for example dust that
    crosses the minimum notional after a price rise). This is conservative; see the limitations.

## Persistence

28. Rollback journal (`journal_mode=DELETE`) with `synchronous=FULL` and foreign keys, so the database file
    alone is a consistent artifact. Every monetary column is TEXT. One `BEGIN IMMEDIATE ... COMMIT` per input
    event or control action writes the input log, cursor, decision, reservation, order, fill, ledger deltas,
    position/risk/health rows, balances and engine snapshot together.
29. **Startup reconciliation** (review finding 3) compares the snapshot's economic fields against persisted
    records, not just ids:
    - The open position (entry price, credited quantity and time versus its entry fill; stop distance versus
      its candidate; stop/target versus `entry ∓ d`/`entry + 3d` and the positions row; exit-order count; exit
      intent).
    - The pending order (every field versus its orders row; for entries, close/ATR/stop distance/qty/limit and
      signal time versus its candidate).
    - Active exit intents, the last exit time, and the day baseline versus its event.
    - On resume, the indicator state, last quote and reference price, rebuilt from the committed input
      events.
    Any disagreement halts before mutation.
30. The lock file is `<realpath(state)>.lock`, held with `flock` for the process lifetime. A database with more
    than one hard link is refused, because hard links defeat path canonicalization. A network-filesystem check
    reads `/proc/self/mountinfo` on Linux. *(W1/W2: the primitive is now `flock` on Linux and `LockFileEx` on
    Windows; Windows refuses UNC paths and network drives.)*

# Phase 2 decisions (forward runner on public market data)

Sources checked on 2026-10-09 in the official `binance/binance-spot-api-docs` repository (CHANGELOG "Last Updated:
2026-09-18"; `web-socket-streams.md`, `rest-api.md`, `filters.md`, `faqs/market_data_only.md`,
`faqs/price_range_execution_rules.md`). `developers.binance.com` and every Binance API host were unreachable from
the build environment, so the GitHub copy of the docs was used and no live payload was observed.

## Architecture

P1. **Two packages.** `paperbot` (the accepted core) stays network-free; the Phase 1 static test that forbids
    network imports and exchange hostnames in it still runs unchanged. `paperbot_net` holds every socket: the
    GET-only REST client, the WebSocket client, the threaded runner, the local control socket and Telegram.
P2. **One owner thread.** I/O threads only put timestamped items on one queue; the owner (`run_forward`) is the
    only code that touches the engine or writes SQLite. That includes control commands and outbox delivery state.
P3. **Deterministic session logic.** `paperbot.forward.ForwardSession` turns items into engine inputs without
    threads or I/O, so tests drive it step by step with explicit times.
P4. **Clock.** Items are stamped with a UTC clock anchored at start and advanced by the monotonic clock (immune to
    wall-clock steps), floored above the last committed engine time. Heartbeat ticks are stamped only when the
    queue is empty, under the same lock as item stamping: a tick can never be stamped after an observation that
    is still waiting. The threaded test caught this ordering bug, and it is fixed.

## Data and normalization

P5. **Executable quotes are WebSocket bookTicker only.** Its payload has no event time, so `exchange_time` stays
    absent. Quotes are deduplicated by update id `u`. A real quote is forwarded when `quote_sample_ms` (1 s) has
    passed since the last forwarded one, or whenever it can matter: an order is pending, quotes are stale, or the
    bid is at or beyond the stop/target. Quotes are never repeated or synthesized. REST bookTicker is not used.
P6. **Candles.** Only closed WebSocket klines (`k.x`) become candles. Canonical end is `k.T + 1 ms`, validated
    against `k.t + 60 s`, and the kline event time `E` is kept as `exchange_time`. Unfinished updates are kept
    raw only.
P7. **Sequencing and backfill.** A bar after a missing minute is held while REST backfills the gap. Backfilled
    and warm-up bars carry `backfill: true`: they update indicators, never create candidates for entry (recorded
    as `backfill_no_retroactive_entry`), and raise no late or missing alarms. Held live bars are then processed
    with the processing time as their receipt time, so their lateness is measured honestly. If the backfill
    fails (30 s timeout or a REST hole), the held bars are released and the engine resets its indicators instead
    of forward-filling.
P8. **References.** Binance now evaluates PERCENT_PRICE / PERCENT_PRICE_BY_SIDE against the symbol's reference
    price when it is non-null, and only otherwise against the `avgPriceMins` weighted average.
    - PUBLIC metadata uses `reference_mode = reference_or_avg`; Phase 1 SYNTHETIC fixtures keep `avg_price`.
    - An unknown or stale (over 120 s) reference price makes the reference unavailable, which blocks orders.
    - `referencePrice` error -2043 ("never set") is recorded as an explicit "no reference price".
    - Unchanged reference observations are forwarded at most every 10 s.
P9. **Execution rules.** `GET /api/v3/executionRules` `PRICE_RANGE` is applied to the simulated taker
    executions. A fill outside `[ref*multDown, ref*multUp]` for its side is a zero fill
    (`execution_rule_price_range`), as the exchange would expire the IOC. When a PRICE_RANGE rule exists but the
    reference is unknown, orders are blocked. An unknown rule type blocks orders.
P10. **Metadata versions.** exchangeInfo (BTCUSDT) and executionRules are fetched together at start and then
    hourly into one content-hashed PUBLIC bundle. Every version is stored (`metadata_versions`), and the active
    version is part of the committed state. Metadata older than `metadata_max_age_s` (5400 s) blocks entries;
    exits keep using the last known version. An incomplete bundle (executionRules missing) is not applied.

## Health, recovery and restart

P11. **Entry blocks.** `feed_disconnected`, `rest_unavailable` (429/418 or a metadata payload error),
    `clock_unsynced`, `restart_recovery` and `feed_recovery`. `clock_unsynced` is raised at every session start
    (clock unverified) and cleared only by a successful `/api/v3/time` check with `|server - local| <= 1 s`; it is
    raised again when a check exceeds the limit or when no check has succeeded for 3 check intervals (900 s;
    checks run every 300 s). A failed check (5xx/transport) is retried after 15 s, doubling up to 300 s. Any block cancels
    a pending buy and skips candidates as `entry_block:*`. With a position, `feed_disconnected` queues
    `health:feed_disconnected`; stale quotes, late/missing bars and gaps queue the Phase 1 health exits.
P12. **Rearming.** `restart_recovery` is raised at every session start and `feed_recovery` at every stream
    disconnect. A disconnect (like a session start) also invalidates quote freshness: a quote received before it
    never counts as fresh again. Reconnecting clears only `feed_disconnected`; on every connection after warm-up
    the runner revalidates candle continuity with a REST klines request from the next expected bar, holding live
    bars meanwhile (bars it returns are backfill: indicators only). The runner signals recovery only when
    warm-up/backfill is done with no held bars and no missing candle, the stream is connected, a quote received
    after the latest (re)connection is fresh, and metadata is present. The engine then clears the recovery blocks
    only when no position and no order remain, so recovered inventory is flattened first.
P13. **Forward restart policy** (separate from offline replay's exact-cursor resume):
    - lock both the state path and the profile, then reconcile (including rebuilding indicators from the
      persisted inputs);
    - cancel an unfilled entry (`forward_restart`);
    - retire an unresolved exit IOC as a zero fill (`retired_on_restart`), or let the staleness rules close it;
      no fill is invented;
    - mark pre-restart quotes stale;
    - queue `health:restart_flatten` for an open position;
    - re-warm from REST (from the last bar when within 990 minutes, else the latest 300 bars).
P14. **Reconnect.** Bounded exponential backoff with jitter (1 s to 60 s, reset after a minute of stable
    connection). Attempts stay below the documented 300 per 5 minutes. An open but silent stream is closed and
    reconnected after `ws_silence_s` (30 s). `serverShutdown` triggers a reconnect. Pings are answered by the
    `websockets` library.
P15. **REST rate limits.** Local budget is `rest_weight_fraction` (0.5) of REQUEST_WEIGHT per minute, updated
    from exchangeInfo `rateLimits`. `X-MBX-USED-WEIGHT-1M` is tracked. 429 and 418 pause every request for
    `Retry-After` (418 also marks a ban). 403, 5xx and transport errors are failures, never success. A server
    weight reading counts only in the local wall-clock minute it was received; the 1m counter restarts every
    minute, so an old reading never blocks later minutes (and never prevents obtaining a new reading).

## Persistence, recording and controls

P16. **Schema v2** is additive: `input_payloads`, `metadata_versions`, `outbox`, `sessions`, `manifest`. v1
    (Phase 1) databases still open and reconcile, and `codec` fills fields added since then from their defaults.
    Forward accounts commit the normalized input line with every event, so reconciliation rebuilds indicators,
    last quote and reference from them.
P17. **Recordings.** `export-recording` writes the committed inputs, controls (with cursor), sessions and
    manifest. `replay-recording` reproduces every economic table exactly. Its configuration must match the
    recording's hash.
P18. **Provenance labels.** Raw observations (every WS frame, REST response and connection status) go to
    gzip-compressed files under `recordings_dir`, flushed about once per second. Accounts record
    `data_provenance`: `BINANCE_PUBLIC` only when the real network transports are used, otherwise `MOCKED`.
    Reports label MOCKED data, and nothing is ever labeled SYNTHETIC unless it is.
P19. **Two locks.** The canonical state-path lock (Phase 1) and a per-account profile lock
    (`~/.local/state/paperbot/locks/account-<sha256(id)[:24]>.lock`), both `flock`, both held for the runner's
    lifetime. *(W1: OS lock via `filelock`; the default directory is per user and per platform.)*
P20. **Controls while active.** `kill`/`reset`/`stop` first try the Phase 1 direct path. If the state is locked,
    they connect to `<state>.ctl` (Unix socket, mode 0600, removed on stop) and the owner applies the audited
    control between events, so effects apply at the next input (at most one heartbeat later). *(Superseded by
    W3: an authenticated socket/named pipe in the per-user control directory.)*
P21. **Manifest.** Covers code revision (git commit plus a hash of the source files), schema version,
    configuration, data sources, reporting timezone, run sessions (start/stop, cursor range, reason) and the
    predeclared evaluation rules from the plan.

## Monitoring

P22. **Status and positions** show:
    - feed/stream state, quote freshness and mark age, last bar, missing-candle flag;
    - entry blocks, recovery state, metadata version and age;
    - queued exit, pending order, inventory/dust, unprotected exposure and outbox state.
P23. **Telegram** is optional, off by default, and read-only.
    - **Outbox:** rows are written in the same transaction as the transition they describe (fills, exit
      intents, latches, entry blocks, session starts).
    - **Delivery:** happens outside transactions, with bounded attempts (`max_attempts`) and Telegram's
      `retry_after` honored.
    - **Labels:** messages older than `delayed_after_s` are marked DELAYED. A timeout after send is AMBIGUOUS
      and retried with a "may duplicate" note; every message ends with its stable id.
    - **Retention:** sent/failed rows are pruned after `retention_days`.
    - **Daily summary:** sent after 00:05 Asia/Jakarta for the day that ended.
    - **Token:** read from an environment variable, redacted from errors.
P24. **Config identity.** Path-like keys (`recordings_dir`, `profile_lock_dir`) and `[telegram]` are excluded from
    the canonical configuration. Normalization settings in `[forward]` are part of it, so changing them is a
    new version. The Phase 1 configuration hash is unchanged.
P25. **Frozen protective prices keep their metadata version.** A position records the metadata version in force
    at its entry fill (`metadata_sha256`). Reconciliation checks stop/target against that version's tick size
    and cross-checks the version against the committed inputs (for positions recorded before the field existed,
    the version is derived from the inputs). Later orders, including exits, use the current version.
P26. **websockets >= 15.0.** The sync client accepts `ping_interval` from 15.0 (13.x/14.x pass it on to socket
    creation and raise `TypeError`). `paperbot run` checks the installed release's connector parameters before
    taking any lock; the evidence runs the real-connector tests at exactly 15.0.

# Native Windows support

The user runs Windows; the earlier Linux/macOS assumption is superseded. WSL is not required. Strategy, risk,
fees, fill assumptions and accounting are unchanged (the configuration hashes are unchanged).

W1. **OS locks through `filelock` (>= 4.1).** A small, maintained, pure-Python library (tox-dev/py-filelock,
    MIT, no dependencies) supplies the primitive: `fcntl.flock(LOCK_EX|LOCK_NB)` on Linux and
    `LockFileEx(EXCLUSIVE|FAIL_IMMEDIATELY)` on one byte on Windows. Both are kernel locks on an open handle,
    released by the OS when the process ends for any reason. `paperbot.oslock` wraps it non-blocking and fail
    closed: `fallback_to_soft=False` (no silent switch to an existence-only lock on filesystems without flock),
    `preserve_lock_file=True`, and any lock class other than the two native ones is refused. A lock file's
    existence or content never decides ownership. Rejected alternatives: hand-written `msvcrt.locking` (locks
    from the current file position, easy to get wrong), `portalocker` (needs pywin32 on Windows), PID files.
W2. **Paths.** The canonical state path is `realpath(abspath(path))`, which on Windows also resolves junctions,
    symlinks, `..`, 8.3 short names and the on-disk letter case of existing components; lock files are created
    on case-insensitive NTFS, so letter-case variants reach the same lock. Comparison keys (control endpoint
    names) use `normcase`. Hard-linked databases are refused on both platforms (`st_nlink`). Windows refuses UNC
    paths (`\\server\share`, `\\?\UNC\...`) and drives whose `GetDriveTypeW` is `DRIVE_REMOTE`; a mapped
    drive resolves to its UNC target. Read-only SQLite connections use `Path.as_uri()` (`file:///C:/...`, with
    `?`, `#`, `%` and spaces percent-encoded). `profile_lock_dir = "default"` resolves to
    `%LOCALAPPDATA%\paperbot\locks` on Windows and `~/.local/state/paperbot/locks` elsewhere; path settings are
    not part of the configuration identity.
W3. **Control channel.** stdlib `multiprocessing.connection`: a Unix-domain socket in the per-user control
    directory (0700; socket 0600) on Linux, and a named pipe `\\.\pipe\paperbot-<hash>-<random>` on Windows,
    created with `PIPE_REJECT_REMOTE_CLIENTS` (a one-flag override of the stdlib listener) and the default pipe
    DACL (creator, SYSTEM, Administrators). No TCP endpoint exists. Every connection must pass the library's
    mutual keyed-digest challenge with a fresh 32-byte key per run, stored with the endpoint address in
    `<control dir>/<hash>.json` (0600 on Linux; on Windows it inherits the user-only ACL of `%LOCALAPPDATA%`).
    Requests are JSON bytes (never pickles); only `cmd`, `reason` and `latch` are taken, and the owner thread
    executes them, so mutations still go through the single owner. Each connection is handled in its own
    bounded thread (at most 4), so a client stuck in the handshake cannot block others. The endpoint file is
    removed on graceful stop; after a crash it is stale but harmless (nothing listens, and the released lock
    lets controls use the direct path).
W4. **Shutdown.** Handlers for SIGINT (Ctrl+C), SIGTERM (Linux) and SIGBREAK (Ctrl+Break, Windows); `paperbot
    stop` works everywhere through the control channel. Closing a console window or terminating the process is
    treated as a crash (the OS releases the locks; restart reconciles and recovers).
W5. **Text and line endings.** Every file read/write names UTF-8. Files that are hashed or compared byte for
    byte (recordings, fixtures, configurations written by tests) are written with `newline="\n"`, and
    `.gitattributes` keeps LF in Windows checkouts. CLI output redirected to a file or pipe is UTF-8 on every
    platform.
W6. **Verification.** `scripts/verify_windows.py` (Python only) runs the checks and saves outputs under
    `evidence/windows/`. Subprocess tests (`tests/test_ownership.py`) run unchanged on Linux and Windows; platform
    branches are limited to alias spellings and the stop signal.

