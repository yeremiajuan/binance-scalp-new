# Binance spot paper bot — project plan

Planning baseline: 9 October 2026, Asia/Jakarta.

This document contains the project brief, phased development plan, strategy and simulation contract, validation plan, and independent review checklist. The companion CLAUDE_PHASE_1_PROMPT.md authorizes implementation of Phase 1 only.

No trading application has been implemented. No historical or forward trial has run. No profitability has been established.

## A. Project brief

### Objective and minimum version

Build a local, understandable Binance spot BTC/USDT, long-only paper bot. The purpose of an approximately 30-day forward trial is to measure operational reliability and a simple strategy’s behavior after execution costs. Profitability is an open question.

The minimum trustworthy trial needs:

- One strategy and one symbol.
- Public finalized candles and executable bid/ask observations.
- Conservative simulated execution.
- Simulated BTC and USDT balances, reservations, orders, fills, positions and fees.
- SQLite persistence, duplicate protection, recovery and reconciliation.
- Exposure limits, loss limits and a persistent kill switch.
- Readable PAPER monitoring.
- Reproducible reports and retained evidence.

The first implementation phase is smaller: an offline core and synthetic quote replay proving these mechanics. Network integration and Telegram follow in Phase 2. The month-long forward trial begins only after that integration passes review.

### Assumptions

All settings below can change before freezing a trial. They are unvalidated hypotheses, not recommendations for investing real money.

- Python 3.12+, one local process, SQLite on a local disk.
- Assume Linux/macOS initially. Isolate locking and verify the user’s actual operating system before the trial.
- Starting simulated balances: 1,000 USDT and zero BTC.
- Effective taker fee assumption: 10 basis points per side.
- No BNB discount or promotional fee exemption assumed.
- Default simulated buy fee deducted in BTC; sell fee deducted in USDT.
- Quote-asset buy fees must also be supported and tested. BNB conversion is deferred.
- One tradable position, no pyramiding, leverage or short selling.
- Maximum exposure: 20% of equity, including pending buys and residual BTC.
- Modeled stop-loss budget per entry: 0.10% of equity.
- Daily loss halt: 1% of conservative liquidation equity.
- Trial drawdown halt: 3% from its conservative-equity high-water mark.
- These are protective settings, not guaranteed maximum losses.
- Crypto operates 24/7. Store UTC timestamps and display Asia/Jakarta times.
- Risk and reporting day begins at 00:00 Asia/Jakarta.
- Persistent risk/manual halts survive midnight and restart until explicitly reset locally.
- Start with a closed-one-minute-candle breakout filtered by a rising five-minute trend.
- Do not implement a setup score or probability of profit.

### Authorization boundary

Do not implement:

- Real orders or signed Binance requests.
- Binance trading credentials or authenticated account access.
- Funds transfers.
- A live broker, dormant live execution code, or a switch enabling live trading.

Public market data later requires no trading credentials. Telegram credentials are optional and introduced only in Phase 2.

A future Testnet adapter is a separate integration task. Testnet results are not evidence of production profitability or fill quality.

Live implementation and live launch each require separate explicit authorization. Elapsed time, passed tests, positive paper P&L or a configuration change cannot authorize either.

### Unresolved items

| Item | Consequence | Resolve by |
|---|---|---|
| Local OS and whether the machine stays awake | Locking, installation and realistic uptime | Phase 2 setup |
| Binance venue/account eligibility and local connectivity | Correct endpoints and account-specific constraints | Before trial data intake |
| Intended simulated equity, effective fees, fee asset and extra charges | Sizing and cost feasibility | Before configuration freeze |
| Telegram bot/chat and summary time | Monitoring configuration | Phase 2 |
| Historical files actually available | Credibility of historical evaluation | Historical-data intake |

None blocks offline Phase 1. Do not request trading credentials to resolve these questions. Do not infer venue eligibility from the user’s timezone.

Assume a daily Telegram summary at 00:05 Asia/Jakarta unless changed before launch.

## B. Phased development plan

### Phase 1 — offline paper core

This is the only implementation phase covered by the current Claude handoff.

Deliver a small Python package, local TOML configuration, SQLite state, synthetic fixtures and an offline replay CLI.

Use a few cohesive modules:

- Strategy.
- Execution and ledger.
- Constraints and risk.
- Storage.
- Replay and reporting.

Split further only when readability warrants it. Normalized input events and an injected clock are sufficient. Do not build a provider plugin system, distributed event bus or general trading framework.

Implement the exact strategy and simulation rules in Section C.

Provide:

- Configuration validation.
- Deterministic synthetic replay.
- Committed-cursor replay resume.
- Read-only status and report commands.
- Audited local persistent kill/reset controls.
- Atomic accounting transitions.
- Stable unique event, signal, order and fill IDs.
- Native fee accounting.
- Full, partial and zero-fill outcomes.
- Dust and residual inventory.
- Exchange-filter validation.
- Exposure and loss controls.
- Canonical state-path process exclusion.
- Startup reconciliation and failure handling.

A fixture replay is an engineering test. It is not a historical backtest or forward trial.

#### Phase 1 acceptance criteria

1. A documented clean-environment command reproduces the fixture outcome with PAPER and SYNTHETIC labels and configuration/input hashes.
2. Hand-calculated fees and fills reconcile with balances, cost basis, realized P&L and unrealized P&L.
3. No negative cash/BTC, overselling, double fees, orphan reservations or silent dust deletion.
4. Decisions are causal and use finalized candles. A cached quote or candle touch never supplies an entry fill.
5. Signal, submission, readiness and simulated-fill times are auditable.
6. Crash injection before/after a transaction and mid-order produces the same economic state as uninterrupted replay.
7. Duplicate or reordered events cannot create duplicate fills.
8. Two processes cannot mutate the same canonical state concurrently.
9. Invalid filters, stale data, missing references, excess exposure and loss thresholds block the intended actions while preserving evidence.
10. Kill and loss latches survive midnight and restart.
11. Attempted live mode is rejected.
12. Code inspection and offline tests find no signer, trading credential, account access, order endpoint or application network execution path.
13. The reviewer receives actual changes, exact checks, outputs, DB/report artifacts and limitations.
14. Stop after Phase 1 for independent review.

#### Phase 1 exclusions

No production REST/WebSocket ingestion, historical downloads, Telegram, Testnet, broker SDK, live code, credentials, UI, deployment, optimizer, multiple symbols, passive maker orders, derivatives, shorts or leverage.

### Phase 2 — public-data runner and basic monitoring

After a separate scoped handoff:

- Add public REST for server time, exchange metadata and warm-up/recovery candles.
- Add public WebSocket one-minute klines and bookTicker observations.
- Derive five-minute candles from complete one-minute groups.
- Archive data needed to reproduce decisions and fills.
- Record local receipt times, source/update IDs and health transitions.
- Record unavailable exchange timestamps honestly.
- Fetch any reference prices required by applicable filters.
- Refresh metadata at startup and periodically; initially assume hourly.
- Missing/expired metadata blocks entries.

Use the same core as Phase 1.

Respect current request weights, rate responses and WebSocket lifecycle rules. Reconnect with bounded backoff. Rebuild warm-up, validate continuity and reconcile before new entries.

Backfilled candles repair indicators. They never create retroactive forward orders.

Public endpoints requiring no credentials is separate from local connectivity being available.

Persist a trial manifest containing:

- Code revision.
- Schema version.
- Strategy/cost/risk configuration.
- Data sources.
- Reporting timezone.
- Trial interval.
- Predeclared evaluation rules.

All state changes go through one owner. Read commands cannot reset state.

#### Telegram scope

Telegram is optional and provides:

- Allowlisted `/status` and `/positions`.
- Simulated trade notifications.
- Daily summaries.
- Error and recovery notices.

State-changing controls remain local CLI commands in this version. Do not add remote trading, restart or configuration commands.

Commit a notification outbox row with its accounting transition. Send outside the transaction.

Use bounded retries, persist delivery state, respect rate responses, limit retention and label delayed notices.

A timeout after sending can cause a duplicate Telegram message. Include stable IDs and do not claim exactly-once delivery.

Notification failure never rolls back a fill or causes the trade itself to be retried.

#### Phase 2 acceptance criteria

- Mocked protocol tests pass.
- An authorized public-data smoke session runs for at least 24 hours.
- Forced disconnect, stale quotes, rate limiting, restart, metadata changes and Telegram outage behave correctly.
- No signed exchange calls occur.
- Recorded inputs reproduce decisions and accounting.
- Status accurately identifies paused and exposed states.
- No historical signal executes after recovery.
- Telegram samples are concise, PAPER-labeled and token-free.
- Locking is verified on the user’s actual OS.
- Missing connectivity is reported rather than concealed or bypassed.

Historical-data intake is a bounded research task in this phase: validate real archive checksums, units and gaps, then freeze chronological splits.

Candle-only estimates must be marked COARSE and kept separate from quote-observed forward paper results.

No historical quote dataset was acquired during planning.

### Phase 3 — approximately 30 consecutive calendar days of forward PAPER

Begin only after Phase 2 acceptance.

Record actual start/end at launch. Extend the period if outages or inadequate observations prevent evaluation. Never disguise uncovered intervals as a completed trial.

A strategy, fee, risk or fill-model change starts a new identified version and evaluation period. Preserve previous results.

#### Before launch

- Freeze code and configuration hashes.
- Validate public access and current exchange metadata.
- Record manually verified fees, or explicitly retain provisional assumptions.
- Reconcile starting balances.
- Test kill/reset and restoration.
- Verify clock, timezone, locking, disk capacity and planned machine uptime.
- Test Telegram dry run if enabled.
- Verify a consistent backup/restore procedure.
- Confirm live mode is rejected.

#### Every day

- Inspect freshness, gaps and process availability.
- Check discrepancies, halts, open risk and dust.
- Reconcile the daily summary against the ledger.
- Review rejected/missed signals and notification backlog.
- Retain input data.
- Make a consistent SQLite backup using its backup API.
- Do not copy only the main file of an active WAL database.
- Perform a recovery drill early and weekly without deleting state.

#### At the end

Produce a read-only report covering:

- Availability, data coverage, gaps and paused time.
- Signals, submitted intents, fills, complete trades and residual inventory.
- Realized and unrealized net P&L.
- Fees by native asset and USDT value.
- Execution gross.
- Market-midpoint comparison when observations support it.
- Exposure and time in market.
- Net expectancy and win rate.
- Profit factor, undefined if there are no losses.
- Conservative-equity drawdown.
- Loss halts and overshoots.
- Results by day and market regime.
- Execution-cost sensitivity and uncertainty.

Show still-open positions with conservative valuation. Do not invent a closing fill at the final candle.

Compare with cash and a clearly labeled buy-and-hold BTC benchmark using the same start/end observations and disclosed costs/exposure.

#### Reliability assessment

Require:

- Zero unreconciled ledger discrepancies.
- Zero duplicate fills.
- No fills on stale/missing observations.
- Successful restart reconciliation.
- Persistent risk halts.
- Explanations for threshold overshoots.

Initially target at least 99% process availability during declared 24/7 coverage, no unexplained candle gaps, and an explanation for every gap over one minute.

Report quote-age distributions, missing observations and paused time even when process availability passes.

Failure means repair and a new measured soak period.

#### Trading assessment

As an initial sufficiency screen, seek at least 100 completed trades across at least 15 active days and more than one market regime.

This is not statistical proof. Fewer trades, narrow regime coverage or uncertainty intervals spanning negative expectancy means inconclusive, even with positive total P&L.

Cost-fragile or loss-making results do not establish an edge.

The end of the month never automatically enables live trading. Outcomes are further evaluation, rejection/revision of the strategy, or a separately authorized later design effort.

### Later improvements

Only after the basic trial:

- Richer depth/trade recording.
- Better liquidity modeling.
- Passive-order queue research.
- More historical coverage.
- A limited robustness study.
- Better monitoring.

Testnet remains a separate later task.

A live design would also need real exchange-state reconciliation, server-side protection, coordination across machines and handling of uncertain order submissions. This paper plan does not certify those capabilities.

### Runtime behavior

| Condition | Required behavior |
|---|---|
| Manual kill, daily loss or trial drawdown halt | Persist latch; cancel pending buys; request paper exit at next trustworthy executable observation; local audited reset only |
| Stale quote, late finalized candle, missing minute or disconnect | Block entries; cancel pending buys; retain inventory; queue exit for fresh valid data; record unprotected time |
| Offline replay restart | Lock, validate DB/schema/config/input identity, reconcile, then resume exact committed cursor and pending simulation state |
| Forward-runner restart | Lock/reconcile; cancel unfilled entries; flatten recovered tradable inventory at next fresh quote before entry rearm |
| Backfill/recovery | Repair indicators without retroactive orders; require fresh quotes, continuous warm-up and successful reconciliation |
| Corrupted/inconsistent state or failed commit | Stop state-changing processing; retain last committed evidence; no automatic reset or fabricated fill |
| Filter or liquidity prevents exit | Retain residual inventory and reason; attempt permitted conservative exits on later observations |
| Telegram down | Keep core protected; persist bounded outbox; expose delivery health locally; never rerun accounting |
| Day rollover without fresh mark | Postpone new baseline; block entries; preserve all existing latches |
| Day rollover with fresh mark | Record new day baseline; preserve manual/daily/drawdown latches; no implicit resume |

Local simulated stops cannot protect while the computer sleeps or data is unavailable.

### Telegram examples

These are hypothetical formatting examples, not measured results.

```text
PAPER | BTC/USDT | RUNNING
11:50 WIB · quote age 0.3s · closed 1m age 1.2s
Cash 900 USDT · position 0.0019 BTC · exposure 9.8%
Today net -0.12 USDT · fees 0.20 USDT · halt none

PAPER BUY #42 | 0.0019 BTC credited
Reason: closed 1m breakout; 5m trend passed
Signal 11:50:00 · submitted 11:50:00.4
Simulated fill 11:50:00.7 WIB
Fee 0.0000019 BTC
Stop/target and assumptions in /positions

PAPER EXIT #43 | STOP | partial
Residual inventory shown in /positions
Net realized -0.08 USDT · fees 0.10 USDT

PAPER DAILY | previous day, Asia/Jakarta
Closed 3 · net realized -0.25 USDT
Unrealized -0.03 USDT · fees 0.67 USDT
Execution gross +0.42 USDT
Coverage 99.7% · paused 4m
Open BTC and dust listed separately

PAPER PAUSED | quotes stale 8.4s
Entries blocked; inventory retained
Exit awaiting fresh data

PAPER RECOVERED | data fresh
State reconciled; pending exit handled
```

Every message identifies PAPER and actual health. Synthetic/coarse outputs add SYNTHETIC or COARSE.

Never describe an intent as a fill, unsellable inventory as sold, or a setup score as a profit probability.

## C. Strategy, simulation and validation contract

Every parameter below is an unvalidated hypothesis. It defines a reproducible experiment, not a profitable default.

### Candidate comparison

| Candidate | Hypothesis | Main issue | Decision |
|---|---|---|---|
| Closed-candle breakout with trend filter | Short continuation after a local high breaks in an existing uptrend | False breakouts, chasing and taker costs | Initial experiment: simple causal rules |
| EMA pullback/recovery | A retracement offers a less extended entry | More state and ambiguity in touch/recovery | Possible later alternative |
| Range mean reversion | Small deviations revert in a stable range | Trend breaks, adverse selection and small net targets | Defer |

The recommendation is based on testability, not established profitability.

Do not implement all three. Do not use stock daily levels, an equity-session VWAP/calendar or a collection of confirmation indicators.

### Initial strategy: B20-T5-v1

#### Hypothesis and suitable conditions

After a completed one-minute close above the preceding 20-minute high, BTC may continue upward briefly when the completed five-minute trend is rising.

Favor sustained upward movement with enough range to pay costs.

Avoid flat/choppy or declining markets, wide spreads, thin quotes, stale data and disorderly gaps.

No verified event/news filter exists. Do not imply one.

#### Candle contract

- One-minute intervals are UTC-aligned `[start, start + 60 seconds)`.
- Canonical bar end is the exclusive interval end, irrespective of inclusive exchange close timestamps.
- Validate final status, interval end and receipt time.
- Derive a five-minute candle only from five unique, consecutive, finalized one-minute members.
- An incomplete group is unavailable.
- Use a five-minute group only once its end and final constituent receipt are already known.
- Never forward-fill missing minutes.

#### Indicator conventions and warm-up

Require 50 consecutive completed five-minute candles—250 one-minute candles—and sufficient one-minute lookbacks.

No signal before warm-up.

Gaps block entries until repaired and continuity revalidated. Repairs cannot become new forward signals.

EMA20:

- Seed with the mean of the first 20 five-minute closes.
- Thereafter use alpha `2/21`.

ATR14 on finalized one-minute bars:

- True range is `max(high-low, abs(high-previous_close), abs(low-previous_close))`.
- Seed with the mean of the first 14 available true ranges.
- Thereafter use `(13*previous_ATR + new_TR)/14`.

Freeze these conventions and test them against hand calculations.

EMA describes trend regime. ATR sizes exits. Do not add RSI, volume rankings or setup probabilities.

#### Trend filter

Require:

- Latest completed five-minute close > its EMA20.
- That EMA20 > the immediately previous completed five-minute EMA20.

#### Entry at finalized one-minute bar i

1. Define `H_i = max(high[i-20:i])`, excluding bar i.
2. Require `close_i > H_i`.
3. Require `close_(i-1) <= H_(i-1)`.

This identifies a fresh crossing rather than repeatedly entering on consecutive new highs.

Also require:

- Completed five-minute trend filter passes.
- Warm-up and continuity pass.
- No tradable open position.
- No pending entry or exit.
- No risk/manual latch.
- Valid exchange metadata.
- Existing dust included in exposure.

Let `a = ATR14` through finalized bar i. Require finite positive a.

Freeze stop distance `d = 2*a` for this candidate. Planned target distance is `3*d`.

Recalculating ATR cannot move an existing stop or target.

#### Entry guards

Require:

- Fresh valid executable quote.
- Full spread <=5 basis points.
- Expected adverse buy-price drift from the signal close <=0.25 ATR.
- Adequate balance and visible liquidity.
- Finalized candle received no more than five seconds after its interval end.
- Signal no more than five seconds old at submission or fill.
- Valid status, filters and reference values.

Reject zero or inverted quotes. Never backdate an opportunity after an outage.

#### Conservative cost gate

Define:

`C = effective_buy_fee + effective_sell_fee + observed_full_spread_fraction + 2*slippage_fraction`

For planning price p, require:

`3*d - p*C >= 0.001*p`

This requires at least a 10-basis-point net target cushion.

Also require:

`(3*d - p*C) / (d + p*C) >= 1.0`

Reject nonpositive denominators.

Recheck the guards before submission and at the eligible fill observation. Never increase quantity after submission.

This is a conservative geometric feasibility estimate, not a win-rate forecast. Exact ledger accounting does not charge the estimated cost a second time.

#### Position sizing

Use conservative liquidation equity E.

Modeled risk budget:

`0.001*E`

Limit quantity by:

- `risk_budget / (d + p*C)`.
- Available cash, including the applicable fee.
- Exposure headroom within `0.20*E`.
- Applicable exchange limits.
- 10% of visible best-ask quantity.

Floor to the applicable quantity step.

If the resulting order is too small, skip it. Do not round up to meet a minimum.

Validate the final quantity and price after rounding.

Record guard outcomes and skip reasons.

Candidate IDs are unique over:

- Paper account/run.
- Symbol.
- Strategy/config version.
- Signal-bar start.

Orders and fills also have unique IDs.

Persist rejected candidates. A redelivered input cannot create a second opportunity.

After an exit, require a three-completed-minute-bar cooldown and another fresh crossing.

No pyramiding or averaging down.

#### Exit rules

Use actual gross buy fill price p_fill and the frozen d.

- Stop: `p_fill-d`, rounded down to the price tick.
- Target: `p_fill+3*d`, rounded up.

Initiate an exit when:

- Fresh executable best bid <= stop.
- Fresh executable best bid >= target.
- Ten minutes have elapsed since the first fill.
- A finalized five-minute close <= its EMA20.
- Health/risk/manual halt requires exit.

Intrabar quotes explicitly provide stop/target triggers. Entries remain finalized-candle decisions.

When reasons coincide, prioritize:

1. Halt/stop.
2. Trend invalidation.
3. Target.
4. Timeout.

Create only one active exit intent.

No trailing stop or target optimization in v1.

Before an entry fill, failed guards, expiry or downward five-minute trend invalidation cancel the pending buy.

Stops and targets are local simulation triggers, not resting exchange orders.

A stop initiates a later executable sell. It does not promise the stop price.

Targets also initiate taker exits. A candle touching a level never supplies a fill.

### Execution feasibility

One basis point is 0.01%.

Illustrative assumptions:

- Buy fee: 10 bps.
- Sell fee: 10 bps.
- Full spread: 2 bps.
- Slippage: 1 bp per side.

Approximate round-trip friction:

`10 + 10 + 2 + 1 + 1 = 24 bps = 0.24%`

A 10-bp gross move is inadequate. A 40-bp move leaves roughly 16 bps before model error.

Free API access does not mean free trading.

Exact break-even depends on fee assets, price path and rounding. Use ledger calculations rather than this approximation.

With these assumptions, the net reward/risk gate requires approximately:

`ATR / price >= 12 bps`

Many calm BTC minutes may fail. A six-ATR target may seldom occur within ten minutes.

Sparse or unprofitable outcomes are valid findings. Do not relax cost assumptions or improve assumed fills to manufacture trades.

A longer timeframe/hold is a new hypothesis requiring a new version.

### Exchange constraints

Fetch metadata dynamically in Phase 2. Phase 1 loads a dated fixture.

Use filter increments, not the number of digits in asset precision fields.

Preserve decimal strings. Use Decimal for quantities, money, fees and rounding.

Handle zero/disabled increments explicitly without modulo by zero.

Validate:

- Symbol status.
- Spot permission.
- Supported order type.
- PRICE_FILTER.
- LOT_SIZE.
- MARKET_LOT_SIZE when modeling market-style orders.
- MIN_NOTIONAL/NOTIONAL with applicability flags.
- PERCENT_PRICE/PERCENT_PRICE_BY_SIDE using the correct current reference.
- Applicable order and position limits.

Phase 1 supports price-protected marketable LIMIT IOC orders only. MARKET_LOT_SIZE therefore does not apply to those orders.

Do not substitute a last trade or midpoint for a required weighted/reference price without marking that constraint unavailable.

Unknown applicable active constraints or missing required references block orders.

Clearly inapplicable constraints—for example trailing-stop constraints on non-trailing IOC orders—are documented as inapplicable.

Submitted orders must pass after rounding.

Minimum notional applies to the submitted order, not separately to every partial-fill fragment.

A valid partial fill can leave too little inventory for a valid sell.

Public exchange metadata does not establish:

- The user’s account permission.
- Account/private asset constraints.
- Effective commissions.
- Historical exchange rules.

Before trial freeze, verify venue, fee components, discounts, fee asset and special/tax charges using manually supplied account facts. Trading keys are not needed for manual input.

Phase 1 accepts effective side-specific fee rates and does not implement authenticated commission retrieval or BNB conversion.

### Paper execution model

#### Evidence categories

Phase 1 uses SYNTHETIC finalized candles and timestamped quote events.

Phase 2 forward PAPER uses recorded public observations.

Historical klines without quotes cannot be relabeled as a realistic quote-based backtest.

#### Timing

Persist:

- Candle interval end.
- Final-candle receipt.
- Signal creation/decision.
- Submission.
- Order readiness.
- Quote local receipt.
- Exchange event time when available.
- Simulated fill time.
- Commit time.

Signal time is when the finalized input is observed and the decision is made. It is not automatically the nominal bar close.

Do not invent an exchange timestamp for payloads that lack one.

Use local sequence/update IDs and monotonic-clock ages in the real runner.

Use an injected clock for deterministic Phase 1 replay.

Initial modeled latency is 250 ms from submission.

A fill needs a new observation received at or after:

`submitted_at + latency`

It cannot use:

- A quote cached at submission.
- The signal candle close.
- An earlier exchange event received late.

If an exchange event time is supplied and predates order readiness, it is ineligible.

When exchange time is unavailable, receipt-order execution is an explicitly weaker approximation.

Signal/submission/fill ordering must be causal.

Quote age limit is two seconds.

Freshness does not prove displayed liquidity is still present when a real order reaches the exchange.

#### Marketable LIMIT IOC simulation

At submission:

- Buy limit = ask increased by 5 bps, rounded down to a tick.
- Sell limit = bid decreased by 50 bps, rounded up.

These cushions are configurable hypotheses.

At the first eligible fresh quote:

- Simulated buy price = ask plus adverse slippage.
- Simulated sell price = bid minus adverse slippage.
- Round buy fills up and sell fills down to valid ticks.
- Enforce the submitted limit.

An adverse move beyond protection causes zero fill, not a fill beyond the limit.

Crossing bid/ask already pays spread. Do not add another half-spread charge.

All fills pay the configured taker fee. No maker discount is assumed.

For each IOC attempt, fill no more than:

- Requested remaining quantity.
- 10% of current visible opposite best-quote quantity.

Floor the fill quantity to the quantity step.

Consume each quote observation at most once for this paper account. Reusing a quote cannot create unlimited liquidity.

Revalidate cash, inventory, exposure and applicable order constraints before committing.

An entry uses one eligible observation:

- Full fill, partial fill or zero fill.
- Cancel the remainder.
- A partial fill creates a smaller position.
- Never retry its unfilled buy remainder.

A partial/zero exit preserves an exit intent.

Retry with a new unique IOC order against a later fresh quote and a newly calculated limit, including modeled latency.

No new entry while tradable residual inventory or an exit attempt remains.

No perfect-liquidity fallback for protective exits.

#### Limitations

The model has no:

- Matching engine.
- Queue position.
- Full market impact.
- Hidden liquidity.
- Proof that a production order would fill.

Displayed size, the participation cap, slippage and latency are approximations.

An outage cannot be retrospectively filled at a protective trigger price.

### Native fee accounting

Default no-BNB model:

For a buy of q BTC at p:

- Debit `q*p` USDT.
- Charge `q*buy_fee` BTC.
- Credit `q*(1-buy_fee)` BTC.

For a quote-asset buy fee:

- Debit `q*p*(1+buy_fee)` USDT.
- Credit q BTC.

For a sell:

- Debit q BTC.
- Credit `q*p*(1-sell_fee)` USDT.

Record fee asset, native amount and USDT value at that fill.

Support these fee modes only in Phase 1. Reject unsupported modes.

#### Hand-check example

Ignore spread/slippage and use hypothetical prices.

Starting cash: 1,000 USDT.

Buy 0.01 BTC at 50,000 with a 0.1% BTC fee:

- Cash: 500 USDT.
- BTC credited: 0.00999.
- Entry fee: 0.00001 BTC, valued at 0.5 USDT.

Sell 0.00999 BTC at 50,100 with a 0.1% USDT fee:

- Proceeds before fee: 500.499 USDT.
- Sell fee: 0.500499 USDT.
- Net proceeds: 499.998501 USDT.
- Final cash: 999.998501 USDT.
- BTC: zero.
- Net P&L: -0.001499 USDT.

This must reconcile despite the price rise.

### Ledger and valuation

Store monetary amounts as Decimal text, never SQLite REAL.

Track:

- Free and locked balances.
- Intents/orders.
- Fills.
- Fee records.
- Position and cost basis.
- Dust.
- Reasons and timestamps.

Reserve cash/BTC transactionally for pending orders.

Release canceled remainder.

Prevent negative balances and overselling.

Charge fees once per fill, never per candle or again after retry.

Allocate cost basis consistently across partial exits. Attribute entry-fee cost exactly once.

Preserve inventory below the step/minimum sell requirement as dust with basis, mark and reason.

No tradable inventory does not necessarily mean zero economic inventory.

Include dust in exposure and reports. Never delete it or call it sold.

Report:

- Cash.
- Tradable inventory.
- Dust.
- Realized net P&L.
- Unrealized net P&L.
- Fees by native asset and USDT value.
- Execution gross.

Execution gross uses actual simulated fill-price movement before fees; spread/slippage are already in those prices.

When midpoint observations exist, separately show a hypothetical midpoint comparison using disclosed equal sizing/timestamps.

Do not subtract spread/slippage twice.

Base-asset entry fees reduce acquired quantity and affect cost basis; they are not just an informational USDT fee column.

Assets, realized/unrealized attribution and initial equity must reconcile without subtracting already charged fees again.

#### Conservative liquidation equity

Risk equity is:

- Available and reserved cash.
- Plus legally sellable BTC at fresh bid after modeled sell slippage/fee.

Unsellable dust has zero liquidation value for risk, but retains disclosed economic bid mark and basis.

Exposure includes all BTC and pending buys/reserves.

On stale/missing prices:

- Show last mark and its age.
- Mark liquidation equity uncertain.
- Block new risk.
- Do not update peaks or present stale valuation as fresh P&L.

At day rollover without a fresh mark, postpone the new baseline and block entries until a fresh mark establishes it.

Daily baseline and trial high-water mark are persistent.

Losses include unrealized inventory and fees, not just completed trades.

### Persistence and reconciliation

Use:

- Local SQLite file.
- One state owner.
- Foreign keys.
- Durable commit settings.
- Schema versioning.

For an input event, commit atomically:

- Cursor.
- Decision.
- Reservations.
- Fill and ledger delta.
- Position/risk transition.

Unique source-event, candidate, order and fill keys enforce idempotency.

A modest normalized ledger plus current state is sufficient. No distributed event-sourcing framework is needed.

Before replay/resume, verify:

- Database integrity.
- Initial balances plus ledger reconciliation.
- Reserves versus pending orders.
- Inventory versus positions/dust.
- Risk latches.
- Sequence cursor.
- Configuration/input identity.

Impossible state or incompatible config/schema/input must halt. Never erase the database as recovery.

Acquire an OS-backed lock for the canonical state path before mutation and retain it for process lifetime.

A second process or symlink spelling of the same path fails fast.

Support local-disk state only initially; reject network-filesystem databases.

Phase 2 adds a canonical paper-profile/account-ID lock to prevent duplicate local profiles targeting the same paper account.

Cross-host real-account coordination is outside scope.

#### Replay versus forward recovery

Offline replay resumes the exact frozen cursor and pending simulation state.

Kill/reset operations are recorded control events.

The later forward runner:

- Cancels unfilled entries after restart.
- Reconciles state.
- Flattens recovered tradable inventory at the next fresh quote.
- Rearms only after health and continuity checks pass.

Never pretend protective orders executed during an outage.

### Honest validation

#### Fixed proposed historical split

If actual files validate, use:

- Raw interval: `[2026-08-10T00:00:00Z, 2026-10-09T00:00:00Z)`.
- Development: `[Aug 10, Sep 21)`, 42 days.
- Untouched evaluation: `[Sep 21, Oct 9)`, 18 days.

These are UTC data boundaries, not local reporting days.

Freeze boundaries before inspecting outcomes or tuning.

Earlier data may provide warm-up, but earlier trades cannot enter the score.

Record:

- Source URLs.
- Retrieval time.
- Archive checksums.
- Units.
- Gaps and duplicates.
- Code/config hashes.
- Every parameter attempt.

These dates are a proposal, not a claim that the data was acquired.

If unavailable, disclose the shortage and select a new chronological interval before inspecting outcomes.

Never replace it silently with a more favorable period.

The official archive documents microsecond spot timestamps from January 2025. REST normally uses milliseconds unless configured otherwise.

Normalize explicitly and test units.

#### Prevent bias

- No random train/test split.
- No future five-minute close or centered indicator.
- No future volatility or fill-price input into an earlier decision.
- No revised historical facts introduced as if already known.
- Current filters on past trades are a labeled assumption, not historical rules.
- Keep a tuning log.
- Evaluation runs once after freezing a version.
- Fresh-start evaluation resets simulated balances/positions.
- Past bars may provide warm-up only.
- Stop development entries at least ten minutes before its boundary.
- Account for unfinished inventory at the boundary.
- Never carry its later outcome into evaluation.

#### Limits of historical candles

Historical candles can establish:

- Causal signal counts.
- Regime coverage.
- Coarse execution scenarios.

Without historical bid/ask, depth and receipt latency, they cannot establish credible microstructure scalping returns.

If a candle-only exploratory backtest is later added:

- Label COARSE.
- Enter strictly after the signal bar.
- Model costs and gaps.
- Resolve ambiguous stop/target bars pessimistically.
- Report the ambiguity count.
- Never treat touched limits as actual fills.
- Never imply precise fill timestamps.
- Keep results separate from quote-observed forward PAPER.

Do not fabricate missing book history.

#### Forward evaluation

Retain quotes, candles, decisions, actual observed delays, outages, rejection counts and skip reasons.

A configuration change starts a new version.

Frozen-data replays can assess execution sensitivity, but remain distinct from the observed baseline paper run.

Include uncovered intervals and unclosed positions.

#### Predeclared execution scenarios

| Scenario | Effective fee/side | Extra full-spread widening | Slippage/side | Latency | Visible-size cap |
|---|---:|---:|---:|---:|---:|
| Baseline | 10 bps | 0 | 1 bp | 250 ms | 10% |
| Worse | 10 bps | +2 bps | 3 bps | 500 ms | 5% |
| Severe hypothesis | 15 bps | +10 bps | 10 bps | 2,000 ms | 5% |

Against an original hypothetical two-bp spread, approximate total costs are 24, 30 and 62 bps.

For quote replay, widen around the recorded midpoint. Do not charge extra spread while leaving ledger prices unchanged.

Run both:

1. Fixed-intent repricing to isolate cost drag.
2. Full causal replay with guards, reservations and possible changes in trade count.

Higher costs may change which trades occur. Therefore total P&L need not decline monotonically in a full strategy replay.

Fixed-fill accounting tests should show the expected direct cost effect.

#### Uncertainty and decision

Report:

- Expectancy by day and regime.
- Average/median holding time.
- Fill ratio and nonfills.
- Dust.
- Fees.
- Slippage versus contemporaneous quotes.
- Drawdown.
- Unprotected time.

Use day-block bootstrap intervals if enough complete days exist.

Individual scalps are correlated; treating each as independent overstates evidence.

Thirty days may be insufficient.

The sample-size screen is not a profitability guarantee.

Positive net results resilient to worse costs support further evaluation only.

Negative/uncertain expectancy means rejection, revision or extension before any new live design discussion.

Testnet proves API mechanics at most.

## E. Independent review checklist

Apply this to Claude’s actual files, commit and artifacts. Do not approve from its summary or test count.

### Reproduce the implementation

- [ ] Identify the actual project root, base/head revisions and all changed/untracked files.
- [ ] Confirm the stock-alert repository was not modified.
- [ ] Inspect source, dependencies, configuration, fixtures and entry points.
- [ ] Trace execution capability; searching for strings alone is insufficient.
- [ ] Check for signers, trading keys, account access, order endpoints, live switches and application network calls.
- [ ] Install in a fresh isolated environment.
- [ ] Run relevant checks independently.
- [ ] Record exact commands, versions, exit codes and meaningful output.
- [ ] Run synthetic replay with application network disabled and no credentials.
- [ ] Confirm PAPER/SYNTHETIC labeling and provenance.
- [ ] Reproduce balances, reports, cursor and configuration from SQLite.

### Causality and fills

- [ ] Hand-check selected EMA, ATR and Donchian values.
- [ ] Verify interval alignment, crossing rule, prior-bar exclusion and seed conventions.
- [ ] Append/change future data and prove prior decisions/fills are unchanged.
- [ ] Reject unfinished, missing, duplicated/misaligned and delayed-finalized bars.
- [ ] Inspect actual decision/submission/readiness/fill times.
- [ ] Prove fills require a later eligible quote and modeled latency.
- [ ] Force a candle touch without an executable quote: no fill.
- [ ] Force price-protection failure, thin liquidity and partial/zero outcomes.
- [ ] Verify canceled entry remainder and persistent exit intent.
- [ ] Verify unique quote-liquidity consumption.
- [ ] Gaps/outages leave later outcomes or unresolved risk, not convenient theoretical fills.
- [ ] Guard failures and skip reasons appear in records.

### Accounting and exchange constraints

- [ ] Reconcile the manual 999.998501-USDT example.
- [ ] Test base-asset and quote-asset buy fees.
- [ ] Reconcile partial cost basis, native fees and free/locked balances.
- [ ] Inspect Decimal text storage.
- [ ] Verify no double fee/spread/slippage subtraction.
- [ ] Ensure open inventory and dust remain in reports and risk.
- [ ] Test min/max/notional boundaries and after-rounding checks.
- [ ] Test zero increments and applicability flags.
- [ ] Test required reference prices and unknown active constraints.
- [ ] Ensure snapshot values are not permanent exchange defaults.
- [ ] Try under-minimum orders, insufficient cash and fee-induced overselling.
- [ ] Verify no rounding up, negative balance or deleted dust.
- [ ] Do not incorrectly apply submitted-order minimums to partial fragments.

### Risk controls

- [ ] Exposure includes pending buys/reserves and residual BTC.
- [ ] Risk uses conservative liquidation equity and unrealized losses.
- [ ] Trigger daily loss, drawdown and manual kill.
- [ ] Cross midnight and restart: latches persist.
- [ ] Reset is local, audited and cannot delete history or forgive inconsistent state.
- [ ] Inject a gap and disclose that loss limits can overshoot.
- [ ] Missing data blocks new risk and cannot create a fictional protective exit.

### Persistence and failures

- [ ] Fail before commit.
- [ ] Fail after commit before acknowledgement.
- [ ] Redeliver events after recovery.
- [ ] Compare with uninterrupted replay economically and by cursor/orders/fills.
- [ ] Reordered/late events cannot rewind state or create old fills.
- [ ] Restart with pending order, partial exit, dust and kill latch.
- [ ] Inspect restored reserves, basis, cursor and hashes.
- [ ] Run two real processes against the same state and path aliases.
- [ ] The loser fails before mutation.
- [ ] A crashed process’s OS lock releases safely.
- [ ] A PID text file alone is insufficient.
- [ ] Test corrupted/incompatible state and config/schema/input changes.
- [ ] Test failed commits/disk-write failures, missing references and stale quotes.
- [ ] No auto-delete/reset recovery.
- [ ] Cursor and economic transition share a transaction.
- [ ] No duplicated fill or orphan reservation.

### Reporting and disposition

- [ ] Outputs explain mode, freshness, reasons, timestamps, net/gross, native fees, dust and unresolved risk.
- [ ] Logs/errors expose no credentials.
- [ ] Synthetic output is not called a historical/forward return.
- [ ] Map each acceptance criterion to independent evidence.
- [ ] Classify passed, failed or unverified.
- [ ] Skipped concurrency/failure cases remain unverified.
- [ ] No Phase 2 or later trading capability appears in Phase 1.
- [ ] Fix material accounting, causality, state and risk defects; rerun affected checks.
- [ ] Save revision, commands, findings, severity, reproductions and limitations.
- [ ] Phase 1 acceptance authorizes no later implementation or live launch.

Later Phase 2 review additionally covers protocol/reconnect/rate-limit behavior, public-data smoke checks, stale-feed and backup drills, Telegram dry run, allowlists/redaction, outbox ambiguity and historical checksum/unit validation.

Month-end review independently recomputes metrics, coverage and stress from retained data; checks tuning/split/version manifests; and evaluates sufficiency and uncertainty.

## Inspection evidence and primary sources

### Reference project inspected

The existing stock-alert project was inspected read-only at commit:

`1a21b433eda7b0e05384fbd73361cc8b3559fb37`

Its working tree remained clean. No stock application or tests were run.

The GitHub page was unavailable through the browser during planning, so architecture observations came from actual local code.

Useful patterns:

| Reference | Observed pattern | New-project adaptation |
|---|---|---|
| `stockalert/storage.py` | SQLite uniqueness, persistent state and transaction helper | Atomic paper ledger/cursor/reserves, Decimal text, strict invariants |
| `stockalert/service.py` | Single DB owner and injected clock | One state owner; separate network delivery |
| `stockalert/engine/alerts.py` | Pending/failed delivery state and stale-notice validation | Later transactional outbox; never retry accounting |
| `stockalert/data/health.py` | Persistent failure/recovery transitions | Separate candle/quote freshness; 24/7 rules |
| `stockalert/telegram/client.py` | Token/URL scrubbing and bounded retries | Optional monitoring and sanitized errors |
| `stockalert/telegram/poller.py` | Offset advanced before handling, with at-most-once tradeoff | Start with read-only remote commands |
| `stockalert/backtest/engine.py` | Shared causal evaluators and pessimistic ambiguous bars | Preserve causality; replace candle-touch fills |
| Timing/reliability/Telegram tests | Boundary and recovery scenarios | Add crypto fees, filters and restart invariants |

Do not transfer whole-share sizing, stock-session exemptions, stock strategies, zero-commission defaults or candle high/low fills.

No LICENSE/COPYING file was found in the normal inspected listing. Reuse architectural ideas and write new code rather than bulk-copying the repository.

### Public Binance snapshot

Public exchange information for BTCUSDT was retrieved during planning.

Server timestamp:

`2026-10-09T04:46:08.396Z`
`2026-10-09T11:46:08.396+07:00`

Observed public values:

| Field | Value |
|---|---|
| Status | TRADING |
| Spot allowed | true |
| PRICE_FILTER tickSize | 0.01 USDT |
| LOT_SIZE stepSize | 0.00001 BTC |
| LOT_SIZE minQty | 0.00001 BTC |
| NOTIONAL minNotional | 5 USDT |
| NOTIONAL applyMinToMarket | true |
| MARKET_LOT_SIZE minQty | 0 |
| MARKET_LOT_SIZE stepSize | 0 |

Other returned filters included PERCENT_PRICE_BY_SIDE and order/algo/list/amend limits.

These are dated observations. Refresh before a trial. They do not establish account permission, commissions, private filters or historical rules.

### Official sources

- Filters:
  https://developers.binance.com/en/docs/products/spot/filters

- Exchange information:
  https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general

- Commission behavior:
  https://developers.binance.com/en/docs/products/spot/faqs/commission_faq

- Public market-data-only endpoints:
  https://developers.binance.com/en/docs/products/spot/faqs/market_data_only

- WebSocket payloads:
  https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~

- REST rate limits:
  https://developers.binance.com/en/docs/products/spot/rest-api

- Market-data endpoints:
  https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market

- Public historical archive:
  https://github.com/binance/binance-public-data

- Spot Testnet:
  https://developers.binance.com/en/docs/products/spot/testnet

Verified documentation points:

- Public market-data-only endpoints require no authentication.
- Kline payload `x` identifies finalized candles.
- BookTicker supplies bid/ask prices and quantities and an update ID; its displayed payload does not provide the trade/kline-style event timestamp.
- Respect request weights, rate responses and Retry-After.
- Historical archive files have checksums and can be corrected later.
- Spot archive timestamps from January 2025 use microseconds.
- No account-specific commission or historical quote dataset was acquired.
- Local Binance REST/WebSocket connectivity was not tested during planning.

### Planning checks completed

- Verified required planning documents.
- Checked their local references.
- Parsed the exchange snapshot.
- Confirmed no application was implemented.
- Verified the proposed 42/18-day chronological split.
- Recomputed the fee example with Decimal: 999.998501 USDT.
- Confirmed the stock checkout remained clean.
- Separated offline replay recovery from future forward recovery.

These are document/arithmetic checks, not strategy-performance or application-test results.