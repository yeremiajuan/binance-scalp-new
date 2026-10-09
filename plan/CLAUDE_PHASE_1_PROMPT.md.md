You are implementing Phase 1 of a new local Binance spot paper-trading project.

Work only in the new `binance-spot-paper` folder. Do not modify the separate stock-alert repository https://github.com/yeremiajuan/trade or any local checkout of it.

Read PROJECT_PLAN.md before editing. It contains the project brief, phased plan, exact strategy/simulation contract, validation rules, independent review checklist and sources. Preserve it. Treat it as the implementation contract.

Implement ONLY the offline, deterministic PAPER core and synthetic replay. Do not begin Phase 2.

Use Python 3.12+, a small package with cohesive modules, Decimal monetary values, dataclasses, TOML configuration, SQLite on local disk and pytest. One symbol: BTCUSDT. Long-only. One state owner. One tradable position. No pyramiding, leverage or shorts. A small cross-platform file-lock dependency is acceptable. Avoid a large framework.

Required behavior:

1. Implement B20-T5-v1 exactly:
   - Finalized 1m fresh crossing above the previous 20 highs, excluding the signal bar.
   - Finalized 5m close above rising EMA20.
   - Derive 5m groups from five contiguous finalized 1m bars.
   - Require 50 completed 5m warm-up bars.
   - Wilder ATR14 on finalized 1m bars.
   - Freeze stop distance at 2 ATR.
   - Target distance is three times that stop distance.
   - Ten-minute timeout.
   - Finalized 5m trend invalidation.
   - Three-completed-minute-bar cooldown after exit.
   - No additional indicators or fixture-driven parameter tuning.
   - All parameters remain unvalidated hypotheses.

2. Implement the contract’s continuity, freshness, expiry, drift, spread and cost gates.
   Initial assumptions:
   - 1,000 USDT, zero BTC.
   - Effective fee 10 bps each side.
   - Adverse slippage 1 bp each side.
   - Order latency 250 ms.
   - Quote freshness limit 2 seconds.
   - Signal/finalized-candle lateness limit 5 seconds.
   - Spread cap 5 bps.
   - Adverse entry drift cap 0.25 ATR.
   - Minimum net target cushion 10 bps.
   - Estimated net reward/risk >=1.
   Use configurable hypotheses and dated metadata fixtures. Never make snapshot limits permanent exchange facts.

3. Simulate price-protected marketable LIMIT IOC orders from subsequent eligible quote observations.
   - Never fill from a candle touch or submission-cached quote.
   - Initial buy/sell price cushions: 5/50 bps.
   - Cross ask to buy and bid to sell.
   - Apply adverse slippage and rounding without violating the order limit.
   - Cap fill quantity at 10% of visible opposing best size.
   - Support full, partial and zero fills.
   - Cancel entry remainder.
   - Persist exit intent for later conservative attempts.
   - Consume each quote observation at most once.
   - Preserve inventory during stale/missing data.
   - Never invent a protective fill during an outage.

4. Implement complete accounting:
   - Free/locked balances and reservations.
   - Orders, fills, position, cost basis and dust.
   - Native fee amount/asset plus USDT value.
   - Default buy fee in BTC, sell fee in USDT.
   - Also support/test quote-asset buy fees.
   - Reject unsupported fee modes, including BNB conversion.
   - Decimal text in SQLite, not REAL.
   - No negative balances, overselling, duplicate fees or deleted dust.
   - Correct partial-exit basis.
   - Realized/unrealized net P&L and execution gross without double-subtracting spread/slippage/fees.
   The manual example in PROJECT_PLAN.md must finish with 999.998501 USDT.

5. Enforce applicable exchange constraints:
   - Status/order-type permission.
   - Price and quantity increments.
   - After-rounding min/max and notional checks.
   - Disabled/zero increment handling.
   - Correct applicability flags.
   - Required weighted/reference prices.
   - Applicable order/position limits.
   - Unknown applicable active constraints or unavailable required references block orders.
   A partial-fill fragment is not incorrectly treated as a new submitted order.

6. Implement controls:
   - Exposure cap 20% equity including pending buys and dust.
   - Modeled stop-loss budget 0.10% equity per entry.
   - Daily loss latch 1%.
   - Trial drawdown latch 3%.
   - Persistent manual kill.
   - Conservative liquidation equity including unrealized losses.
   - UTC timestamps, Asia/Jakarta day boundary.
   - No implicit reset/resume at midnight or restart.
   - Explicit audited local reset.
   Controls do not guarantee losses cannot overshoot during gaps.

7. Make persistence atomic and idempotent:
   - Stable source-event, candidate, order and fill IDs.
   - Commit cursor, decision, reservations, fills, ledger and risk/position transition together.
   - Acquire an OS-backed canonical state-path lock before mutation.
   - Hold it for process lifetime.
   - Reconcile DB, ledger, reserves, inventory, basis, latches, cursor and config/input identity before resume.
   - Incompatible/corrupted state halts; never recreate the account as recovery.
   - Exact synthetic replay resume must match uninterrupted replay.
   - No two processes or path aliases can mutate the same state.

8. Provide CLI commands for:
   - Configuration validation.
   - Synthetic replay/resume.
   - Read-only status/report.
   - Audited persistent local kill/reset.
   Never silently overwrite a nonempty account.
   Outputs must say PAPER and SYNTHETIC and show provenance, configuration/input hashes, timestamps, reasons, fees, nonfills, skipped signals, dust and unresolved risk.
   Use an injected clock.
   Replay and tests require no network or credentials.

EXCLUDED:
Production REST/WebSocket ingestion, historical downloads or profitability backtests, Telegram/network clients, Testnet, Binance trading credentials/signers/account access/order endpoints, broker SDKs, live execution or dormant live code, a --live switch, deployment, UI, derivatives, passive-maker queues, optimizer and multiple symbols.

Attempted live mode must be rejected at configuration/CLI validation.

A month, positive P&L or passing tests cannot authorize live implementation or launch.

Required meaningful tests:
- Prefix invariance/no look-ahead.
- Unfinished, misaligned, duplicated and gapped 1m/5m bars.
- EMA/ATR hand calculations.
- Cached/before-latency/late quotes.
- Adverse gaps, price-protection failure and expiry.
- Full/partial/zero IOC outcomes.
- Filter boundaries after rounding.
- Disabled increments and missing references.
- Both supported fee modes and the manual ledger example.
- Reserves, partial basis, dust and no overselling.
- Unrealized-loss, daily-loss, drawdown and kill controls.
- Latches across midnight/restart.
- Duplicate/reordered inputs.
- Failure before commit and after commit before acknowledgement.
- Disk-write/transaction failures.
- Committed-cursor resume equivalence.
- Inconsistent state.
- Real concurrent processes and canonical-path aliases.
- Offline operation and live rejection.

Tests must assert economic behavior and causal timing, not merely function calls. Synthetic fixtures are engineering evidence, never evidence of a trading edge.

Return:
- Actual changed-file list and diff or accessible commit.
- Installation and reproduction commands.
- Exact test/check commands, exit codes and meaningful output.
- A hand-reconciled ledger/report and SQLite artifact.
- Uninterrupted-versus-restarted replay evidence.
- A two-process exclusion demonstration.
- Acceptance-criterion-to-evidence mapping.
- Assumptions, defects and simulation limitations.

State routine implementation choices and continue. Ask only about a material contradiction that prevents implementation.

STOP AFTER PHASE 1 FOR INDEPENDENT REVIEW.
Do not begin Phase 2 or describe the implementation as ready for live trading.