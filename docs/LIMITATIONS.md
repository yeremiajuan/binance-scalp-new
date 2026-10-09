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
  heartbeat events to advance the clock.
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
  quote, rearm after health checks) is deliberately not implemented.
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

## Defects

The independent review of `48b60e2` found four defects. All are fixed, with regression tests that fail on
`48b60e2` and pass now:

| # | Finding | Fix | Regression tests |
|---|---|---|---|
| 1 (P1) | Outages, late candles and missing minutes did not queue an exit; recovery inside the stop/target range left the position open | Health triggers create a persistent exit intent, executed only on fresh data after latency | `test_execution.py::test_outage_queues_exit_even_when_recovery_is_inside_the_stop_target_range`, `test_late_or_missing_candle_queues_exit_for_fresh_data[candle_late/candle_missing]` |
| 2 (P1) | Fill-time sizing could exceed the risk budget (wider spread still within the cap) | Fill quantity capped by the risk and exposure limits recomputed at the fill observation | `test_execution.py::test_fill_time_risk_cap_reduces_quantity_when_costs_widen` |
| 3 (P1) | Reconciliation compared only ids; an altered snapshot stop (1 USDT) passed and was used | Economic field cross-checks for position, pending order, intents, baseline, last exit and rebuilt inputs | `test_persistence.py::test_snapshot_economic_fields_must_match_persisted_records[...]` (12 cases), `test_pending_order_fields_must_match...` (8 cases), `test_persisted_protective_price_tampering_is_detected_too` |
| 4 (P2) | Increment checks used `(x - min) % inc` | `price % tickSize`, `qty % stepSize` | `test_constraints.py::test_increments_are_absolute_multiples_not_offsets_from_minimums` |

Beyond these, no defect is known. That is the implementer's assessment; it still needs independent
re-review.
