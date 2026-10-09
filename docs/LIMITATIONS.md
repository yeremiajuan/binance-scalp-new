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
- **Exposure breach at fill cancels.** An entry whose exposure would be exceeded at the fill observation is
  canceled, not resized.
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

No defect is known at hand-off beyond the limitations above. This is the implementer's own assessment and has
not been independently reviewed.
