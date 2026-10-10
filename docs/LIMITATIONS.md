# Phase 1 assumptions, limitations and known gaps

## What the simulation is not

- **No matching engine.** There is no queue position, full market impact, hidden liquidity or historical order
  book. Nothing here proves that a production order would fill, or fill at these prices.
- **Approximations:** visible best-quote size, the 10% participation cap, 1 bp slippage, 250 ms latency and
  the 5/50 bp price cushions. A fresh quote does not prove that displayed liquidity was still there when an
  order would have arrived.
- **Weaker ordering without exchange time.** When a quote carries no exchange event time, eligibility relies
  on local receipt order, which is a weaker approximation than exchange time.
- **Local triggers, not exchange protection.** Stops and targets are local simulation triggers, not resting
  exchange orders. A stop starts a later executable sell and does not promise the stop price. While data is
  missing or stale, nothing protects the position. The demo's outage episode exits about 241 USDT/BTC below
  its stop, and the report records 68.4 s of unprotected exposure.
- **Loss latches can overshoot.** Daily loss, drawdown and the per-entry stop budget are protective settings,
  not guaranteed caps. Gaps, latency and liquidity can overshoot them. Each latch event records the observed
  loss, the threshold and the overshoot.
- **Synthetic inputs only.** Fixtures and their P&L are engineering evidence about mechanics. They are not
  market data, not a historical backtest and not a forward trial, and they say nothing about profitability.
  Every strategy, cost and risk parameter remains an unvalidated hypothesis.
- **Metadata fixture.** Only tickSize, stepSize/minQty, NOTIONAL minNotional/applyMinToMarket and the
  MARKET_LOT_SIZE zeros mirror the dated public snapshot in PROJECT_PLAN.md. Every other filter value is a
  labeled synthetic stand-in. Public metadata does not establish account permissions, private filters or
  verified commissions, and no fee rate here is a verified account rate.

## Engineering limitations

- **Locking** uses `fcntl.flock` and has only been tested on Linux. macOS should work but is untested, and
  Windows is refused. Network-filesystem detection is Linux-only. Canonicalization handles symlinks and
  relative paths, and refuses hard links. Coordination across machines and the Phase 2 account-profile lock
  are out of scope.
- **No gap repair.** A missing minute resets indicators and re-warms over 250 bars. Phase 1 has no backfill
  path.
- **A missing candle is only detected when a later event arrives.** A completely silent input stream needs
  heartbeat events to advance the clock. (Phase 2: the forward runner sends local heartbeats every second.)
- **Sellable residual without a position blocks entries.** If dust became sellable because the price rose,
  entries are blocked rather than the residual being liquidated automatically. This is conservative and
  disclosed, but it could stall a long run until that is designed.
- **Fill-time resizing is a model choice.** A real IOC cannot be resized after submission. Phase 1 caps the
  simulated fill at the fill-time risk and exposure limits, so the modeled risk never exceeds the budget. This
  makes simulated entries smaller, never larger, than submitted.
- **Health exits flatten on any data interruption.** Stale quotes (over 2 s), a late or missing candle, or a
  gap with an open position queue an exit. With bursty real feeds this may exit often. It is the
  conservative reading of the plan's runtime table, and how often it fires is a Phase 2 measurement.
- **Offline replay restart is not the forward-runner policy.** Restarting resumes the exact pending
  simulation state. The Phase 2 policy (cancel unfilled entries, flatten recovered inventory at the next fresh
  quote, rearm after health checks) is not part of offline replay. (Phase 2 implements it for the forward runner
  only; offline replay keeps exact-cursor resume.)
- **Commit timestamps** are injected-clock times equal to the event's receipt time, so processing delay is
  modeled as zero.
- **Read-only reports and the writer.** `status`/`report` read one consistent snapshot inside a single SQLite
  read transaction, which holds a shared lock for a few milliseconds. The owner's commit waits for it (5 s
  busy timeout). A reader holding the lock longer than that would make the owner's commit fail, and the
  replay would halt safely and resume from its last commit. An early version read without a transaction and
  could mix two commits; the evidence run caught this, it is fixed, and a regression test covers it.
- **Throughput** is about 2-3 ms per event on local disk with `synchronous=FULL`, about 30 s for the
  13,230-event demo. The full test suite takes about 2-3 minutes.
- **Text report rounding.** Long non-terminating decimals (EMA, ATR, proportional basis) are shown rounded
  with `~` in the text report. The database and the JSON report keep exact values.

# Phase 2 limitations

- **No live connectivity was tested.** Every Binance host and developers.binance.com were unreachable from the
  build environment. All protocol behavior is verified against mocked transports and local fake servers built
  from the documented payload shapes, not against real Binance responses. The authorized 24-hour public-data
  smoke session has **not** been run.
- **Simulated fills are not real executions.** Fills come from best bid/ask observations with a 10% visible-size
  cap, slippage, latency and the documented fill-time resizing. A real IOC cannot be resized after submission,
  and nothing proves a real order would have filled at these prices.
- **Quote sampling.** At most about one quote per second is processed while nothing is pending (every quote while
  an order is pending, or when the bid reaches the stop/target). A stop between samples is detected at the next
  forwarded observation, so detection can lag by up to ~1 s.
- **Health exits are aggressive.** With an open position, any stale-quote period over 2 s, a late or missing bar,
  a gap or a stream disconnect queues an exit. Real feeds may trigger this often; how often is a smoke-session
  measurement.
- **Exits also need fresh references.** When a PRICE_RANGE rule or a PERCENT_PRICE filter applies, an unknown or
  stale (over 120 s) reference price blocks exits as well as entries, so inventory can be held through a
  reference outage. This is conservative and disclosed.
- **Receipt order only.** bookTicker has no exchange event time, so fill eligibility relies on local receipt order
  and the monotonic clock. The clock offset to Binance is measured from `/api/v3/time` round trips; it is an
  estimate, not a synchronization.
- **Startup time grows with history.** Restart reconciliation rebuilds indicators from all persisted inputs; over
  a month (millions of inputs) startup can take minutes.
- **Disk.** Expect roughly 1-2 GB per week of SQLite state plus compressed raw recordings; this is an estimate
  that needs measuring. A crash can lose up to ~1 s of raw lines, but not committed inputs or decisions.
- **Locks and Telegram.** Both locks are local (`flock`); there is no cross-machine coordination, and only Linux
  was exercised. Telegram delivery was tested against fakes only (no dry run against the real Bot API).
  Duplicates after ambiguous sends are possible by design.
- **Recovery is conservative.** After every reconnect, entries wait for a REST continuity check and a quote
  received after the reconnection; with an open position they wait until it is flattened. A breakout bar that
  closes during that window is not traded. Entries also wait for a successful server-time check after every
  start, so a REST outage at startup keeps entries blocked.
- **Weight tracking uses the local minute.** The server's 1m weight window and the local wall-clock minute can
  differ by the clock offset (at most 1 s while entries are allowed), so a reading near a boundary may be
  counted in the wrong minute; the 50% budget leaves room for that.
- **Market-data-only host.** With `data-api.binance.vision` as the REST host, executionRules and referencePrice
  are unavailable and entries stay blocked.
- **Not implemented, as instructed.** No historical-data intake or evaluation campaign, no Testnet, no
  credentials, no live trading, and no automatic trial launch.

## Defects

The independent review of `48b60e2` found four defects. All are fixed, with regression tests that fail on
`48b60e2` and pass now:

| # | Finding | Fix | Regression tests |
|---|---|---|---|
| 1 (P1) | Outages, late candles and missing minutes did not queue an exit; recovery inside the stop/target range left the position open | Health triggers create a persistent exit intent, executed only on fresh data after latency | `test_execution.py::test_outage_queues_exit_even_when_recovery_is_inside_the_stop_target_range`, `test_late_or_missing_candle_queues_exit_for_fresh_data[candle_late/candle_missing]` |
| 2 (P1) | Fill-time sizing could exceed the risk budget (wider spread still within the cap) | Fill quantity capped by the risk and exposure limits recomputed at the fill observation | `test_execution.py::test_fill_time_risk_cap_reduces_quantity_when_costs_widen` |
| 3 (P1) | Reconciliation compared only ids; an altered snapshot stop (1 USDT) passed and was used | Economic field cross-checks for position, pending order, intents, baseline, last exit and rebuilt inputs | `test_persistence.py::test_snapshot_economic_fields_must_match_persisted_records[...]` (12 cases), `test_pending_order_fields_must_match...` (8 cases), `test_persisted_protective_price_tampering_is_detected_too` |
| 4 (P2) | Increment checks used `(x - min) % inc` | `price % tickSize`, `qty % stepSize` | `test_constraints.py::test_increments_are_absolute_multiples_not_offsets_from_minimums` |

Beyond these, no Phase 1 defect is known. The independent reviewer accepted Phase 1 at `e3c5a92`.

Found and fixed while implementing Phase 2 (each has a regression test):

- `README.md` had been committed as UTF-16 without a BOM since `fd96f55` (it rendered garbled); it is UTF-8 now.
- A forward restart treated a quote observed by the previous process as fresh and immediately submitted a
  flatten order against it; pre-restart quotes are now stale
  (`test_forward.py::test_forward_restart_retires_attempts_flattens_and_rearms_without_inventing_fills`).
- Runner heartbeat ticks could be stamped after queued observations, which were then rejected as out of order
  (`test_runner_e2e.py`: no `non_monotonic` inputs).
- The daily summary fired immediately when started after 00:05 local time; it now starts from a baseline
  (`test_phase2_static.py::test_daily_summary_is_written_once_per_local_day`).

The independent review of `87b130b` found five Phase 2 defects. All are fixed, with regression tests that fail
against the `87b130b` source and pass now (`evidence/phase2/review_regressions.txt` runs them against both):

| # | Finding | Fix | Regression tests |
|---|---|---|---|
| 1 (P1) | Reconnect cleared the feed block at once; a breakout bar right after reconnecting bought on a pre-disconnect quote | Disconnect invalidates quote freshness and raises `feed_recovery`; rearming needs a quote received after reconnecting and a REST continuity check | `test_forward.py::test_reconnect_never_enters_on_pre_disconnect_quotes[...]` (3 cases) |
| 2 (P1) | A failed (HTTP 503) initial server-time check left entries allowed | Clock verification starts pending at every session start; only a successful check within the limit clears it; failures retried with backoff; a check older than 900 s expires | `test_forward.py::test_failed_initial_clock_check_blocks_entries_until_a_check_succeeds`, `test_clock_check_expires_without_a_recent_success` |
| 3 (P1) | Reconciliation checked frozen stop/target with the latest tick size; a tick change (0.01 to 0.10) halted restart of an unchanged position | Position records its entry metadata version; reconciliation uses it (derived from inputs for older positions) | `test_forward.py::test_metadata_tick_change_keeps_an_open_position_reconciled_and_restartable` |
| 4 (P1) | A server weight reading was kept across minutes; a reading of 3000 throttled every later minute | Readings count only in the minute they were received | `test_public_protocol.py::test_server_weight_reading_expires_at_its_minute_boundary_and_throttling_recovers` |
| 5 (P2) | Declared `websockets>=13` but 13.x/14.x reject `ping_interval` in the sync client | `websockets>=15.0,<16`; the runner checks connector parameters before taking locks; real-connector tests run at exactly 15.0 | `test_phase2_static.py::test_websockets_floor_matches_the_real_connector_arguments`; `evidence/phase2/websockets_minimum.txt` |

Tests changed by these fixes (behavior changed deliberately, not weakened): the startup and restart tests now also
expect `clock_unsynced` until the first server-time answer; the reconnect-gap and failed-backfill tests connect
without auto-answering the new continuity check, so they still exercise the gap backfill and the backfill timeout.

No other Phase 2 defect is known. That is the implementer's assessment; Phase 2 needs independent re-review.
