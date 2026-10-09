# Phase 1 routine implementation decisions

`plan/PROJECT_PLAN.md` is the contract. These are the choices made where it leaves room. None of them changes a
strategy parameter, a cost assumption or a fill rule in a direction that makes results look better.

## Project and tooling

1. **Location.** The handoff prompt file names a `binance-spot-paper` folder, while the request says to work in
   the `binance-scalp-new` project. The package lives at the root of this repository (`src/paperbot`), and
   `plan/` is untouched. The separate stock-alert repository was not accessed or modified.
2. **No runtime dependencies.** Locking uses stdlib `fcntl.flock`, not a third-party file-lock package, so it
   supports Linux and macOS (only Linux was exercised). Windows is refused with a clear error.
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
18. **Exposure at the entry fill** is rechecked. If it would be exceeded, the order is canceled rather than
    reduced.
19. **Exit orders** sell `floor_step(free BTC)` capped at `maxQty`. When the remaining inventory cannot form a
    valid SELL (below step, `minQty` or minimum notional at the protected limit), it is kept as dust with basis,
    and the position closes with `residual_unsellable`/`exit_complete`. Dust stays in the average-cost pool. A
    later position's exit may sell part of it once `floor_step(total)` includes it, and that is accounted
    through the same pool.
20. **Exit triggers** are collected per event and the highest priority wins
    (halt > stop > trend invalidation > target > timeout). There is one exit intent per position, its reason is
    fixed when it is created, and the intent survives partial and zero fills until no sellable inventory remains.
21. **Every exit retry** is a new order id submitted on the observation that closed the previous IOC. It
    references that observation and has a new limit and new latency, so it can only fill on a later
    observation.
22. **Filters are validated at submission only**, after rounding, as an exchange would. They are not
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
29. The lock file is `<realpath(state)>.lock`, held with `flock` for the process lifetime. A database with more
    than one hard link is refused, because hard links defeat path canonicalization. A network-filesystem check
    reads `/proc/self/mountinfo` on Linux.
