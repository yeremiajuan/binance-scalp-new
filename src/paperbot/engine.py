"""The single state owner: folds one input event at a time into the paper account.

Every event is processed against a copy of the state; the resulting state,
the cursor and every record produced (decision, reservation, order, fill,
ledger delta, position/risk/health transition) are committed in one SQLite
transaction by ``Storage.commit_event``. If anything fails before the commit,
nothing is persisted and the engine halts; a later resume re-reads the last
committed state.

Time comes only from the input (receipt timestamps): the injected clock.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from decimal import Decimal

from .config import Config
from .constraints import (
    MetadataError,
    Reference,
    SymbolRules,
    execution_rule_violation,
    parse_metadata,
    validate_limit_order,
)
from .events import (
    CandleEvent,
    FeedEvent,
    HeartbeatEvent,
    MetadataEvent,
    QuoteEvent,
    RawEvent,
    ReferenceEvent,
    RefPriceEvent,
    SessionStartEvent,
)
from .execution import (
    QuoteView,
    buy_fill_price,
    buy_limit_price,
    cost_gate,
    eligibility,
    fill_quantity,
    sell_fill_price,
    sell_limit_price,
    spread_fraction,
)
from .ledger import (
    Balances,
    InvariantViolation,
    Pool,
    add_to_pool,
    allocate,
    buy_amounts,
    remove_from_pool,
    sell_amounts,
)
from .money import ONE, ZERO, ceil_to, dtext, exact, floor_to
from .risk import LATCHES, RiskState, Valuation, sellable_quantity, value
from .strategy import (
    COOLDOWN_BARS,
    MIN_NET_REWARD_RISK,
    MIN_NET_TARGET_FRACTION,
    STOP_ATR_MULT,
    STRATEGY_VERSION,
    TARGET_STOP_MULT,
    TIMEOUT_US,
    Bar,
    BarResult,
    StrategyState,
    trend_invalidated,
)
from .strategy import update as strategy_update
from .timeutil import MINUTE_US, US_PER_S, iso, local_date

# Exit-trigger priority (PROJECT_PLAN.md): halt/health/stop, then trend invalidation, target, timeout.
# Health triggers carry their cause, e.g. "health:quotes_stale".
EXIT_PRIORITY = ("halt", "health", "stop", "trend_invalidation", "target", "timeout")


def _priority(trigger: str) -> int:
    return EXIT_PRIORITY.index(trigger.split(":", 1)[0])


class EngineHalted(RuntimeError):
    pass


class CursorError(RuntimeError):
    pass


@dataclass
class QuoteObs:
    event_id: str
    recv_us: int
    bid: Decimal
    bid_qty: Decimal
    ask: Decimal
    ask_qty: Decimal
    exchange_us: int | None

    def view(self) -> QuoteView:
        return QuoteView(self.event_id, self.recv_us, self.bid, self.bid_qty, self.ask, self.ask_qty,
                         self.exchange_us)


@dataclass
class RefObs:
    event_id: str
    recv_us: int
    avg_price: Decimal
    mins: int


@dataclass
class RefPriceObs:
    """Latest exchange reference price observation; value None = the exchange reports no reference price."""

    event_id: str
    recv_us: int
    value: Decimal | None
    exchange_us: int | None


# Entry blocks raised by forward-runner health events; any block stops new entries.
BLOCK_FEED = "feed_disconnected"
BLOCK_REST = "rest_unavailable"
BLOCK_CLOCK = "clock_unsynced"
BLOCK_RECOVERY = "restart_recovery"
BLOCK_FEED_RECOVERY = "feed_recovery"  # after a disconnect: fresh observations + continuity before rearming
RECOVERY_BLOCKS = (BLOCK_RECOVERY, BLOCK_FEED_RECOVERY)
CLOCK_CHECK_STALE_FACTOR = 3  # a successful clock check older than 3 check intervals no longer counts


@dataclass
class PendingOrder:
    order_id: str
    side: str
    purpose: str  # entry | exit
    qty: Decimal
    limit_price: Decimal
    submitted_us: int
    ready_us: int
    reserved_asset: str
    reserved_amount: Decimal
    reference_quote_id: str
    candidate_id: str | None
    position_id: str | None
    attempt: int
    signal_us: int | None
    signal_close: Decimal | None
    atr: Decimal | None
    stop_distance: Decimal | None
    ineligible_seen: int


@dataclass
class ExitIntent:
    intent_id: str
    reason: str
    created_us: int
    attempts: int
    blocked_reason: str | None


@dataclass
class Position:
    position_id: str
    candidate_id: str
    opened_us: int
    entry_price: Decimal
    entry_qty: Decimal
    stop_distance: Decimal
    stop_price: Decimal
    target_price: Decimal
    exit_intent: ExitIntent | None
    exit_orders: int
    metadata_sha256: str | None = None  # metadata version whose tick size froze stop/target (public sessions)


@dataclass
class Health:
    quotes_fresh: bool = False
    stale_since_us: int | None = None
    unprotected_since_us: int | None = None
    unprotected_total_us: int = 0
    candle_missing: bool = False
    blocks: list[str] = field(default_factory=list)
    recovery_signaled: bool = False


@dataclass
class EngineState:
    account_id: str
    cursor: int
    clock_us: int | None
    balances: Balances
    pool: Pool
    strategy: StrategyState
    risk: RiskState
    health: Health
    last_quote: QuoteObs | None
    reference: RefObs | None
    order: PendingOrder | None
    position: Position | None
    last_exit_us: int | None
    bars_since_exit: int
    candidates_seen: int = 0
    notes: dict[str, str] = field(default_factory=dict)
    metadata_hash: str | None = None  # active metadata version (forward/public sessions)
    metadata_fetched_us: int | None = None
    ref_price: RefPriceObs | None = None
    session_id: str | None = None
    clock_checked_us: int | None = None  # last successful clock-offset check within the limit (forward sessions)


class Recorder:
    """Collects the rows one event produces; Storage writes them in the same transaction."""

    def __init__(self, seq: int | None, now: int | None):
        self.seq = seq
        self.now = now
        self.ops: list[tuple] = []
        self.source_event_id: str | None = None

    def insert(self, table: str, row: dict) -> None:
        self.ops.append(("insert", table, row))

    def update(self, table: str, key: str, row: dict) -> None:
        """Update an existing row identified by ``row[key]``; Storage fails the transaction if it is missing."""
        self.ops.append(("update", table, key, row))

    def health(self, kind: str, **detail) -> None:
        self.insert("health_events", {"seq": self.seq, "ts_us": self.now, "kind": kind, "detail": detail})

    def risk(self, kind: str, **detail) -> None:
        self.insert("risk_events", {"seq": self.seq, "ts_us": self.now, "kind": kind, "detail": detail})

    def insert_ignore(self, table: str, row: dict) -> None:
        self.ops.append(("insert_ignore", table, row))


def initial_state(cfg: Config) -> EngineState:
    return EngineState(
        account_id=cfg.account_id,
        cursor=0,
        clock_us=None,
        balances=Balances(cfg.starting_usdt, ZERO, cfg.starting_btc, ZERO),
        pool=Pool(ZERO, ZERO, ZERO),
        strategy=StrategyState(),
        risk=RiskState(),
        health=Health(),
        last_quote=None,
        reference=None,
        order=None,
        position=None,
        last_exit_us=None,
        bars_since_exit=0,
    )


class Engine:
    def __init__(self, cfg: Config, rules: SymbolRules | None, store, state: EngineState):
        """``rules`` is the fixed metadata of a synthetic run; forward/public sessions pass None and receive
        versioned metadata as input events (the active version is part of the committed state)."""
        if rules is not None:
            self._check_rules(rules)
        self.cfg = cfg
        self.base_rules = rules
        self.rules = rules
        self.store = store
        self.state = state
        self.halted = False
        self.notify = False  # forward runner: write PAPER notifications to the outbox with each transition
        self.notify_label = "PAPER | PUBLIC DATA"
        self._rules_cache: dict[str, SymbolRules] = {}

    def _check_rules(self, rules: SymbolRules) -> None:
        if rules.symbol != "BTCUSDT" or rules.base_asset != "BTC" or rules.quote_asset != "USDT":
            raise MetadataError(f"metadata symbol {rules.symbol} ({rules.base_asset}/{rules.quote_asset}) is not "
                                "BTCUSDT")

    def _rules_for(self, st: EngineState) -> SymbolRules | None:
        if st.metadata_hash is None:
            return self.base_rules
        if st.metadata_hash not in self._rules_cache:
            text = self.store.metadata_bundle(st.metadata_hash)
            if text is None:
                raise MetadataError(f"metadata version {st.metadata_hash} is not stored")
            self._rules_cache[st.metadata_hash] = parse_metadata(text)
        return self._rules_cache[st.metadata_hash]

    # ------------------------------------------------------------------ public

    @exact
    def process(self, raw: RawEvent) -> str:
        """Process input ``raw.seq``; returns its disposition. Already-committed inputs are no-ops."""
        if self.halted:
            raise EngineHalted("engine halted after a failure; restart and resume from the committed cursor")
        if raw.seq <= self.state.cursor:
            return "already_committed"
        if raw.seq != self.state.cursor + 1:
            raise CursorError(f"input seq {raw.seq} does not follow committed cursor {self.state.cursor}")
        st = copy.deepcopy(self.state)
        try:
            rec = Recorder(raw.seq, None)
            disposition, detail = self._step(st, rec, raw)
            st.cursor = raw.seq
            self.store.commit_event(raw, disposition, detail, st, rec)
        except BaseException:
            self.halted = True
            raise
        self.state = st
        return disposition

    @exact
    def apply_control(self, kind: str, latch: str, reason: str, wall_utc: str) -> None:
        """Persist a local, audited manual kill or latch reset. Effects apply at the next input event."""
        if self.halted:
            raise EngineHalted("engine halted")
        if not reason or not reason.strip():
            raise ValueError("a reason is required for every control action")
        st = copy.deepcopy(self.state)
        if kind == "kill":
            if latch != "manual_kill":
                raise ValueError("kill sets the manual_kill latch")
            if "manual_kill" in st.risk.latches:
                raise ValueError("manual_kill is already latched")
            st.risk.latches.append("manual_kill")
        elif kind == "reset":
            if latch not in LATCHES:
                raise ValueError(f"unknown latch {latch!r}")
            if latch not in st.risk.latches:
                raise ValueError(f"{latch} is not latched")
            st.risk.latches.remove(latch)
            if latch == "daily_loss":
                st.risk.baseline_pending = True
                st.risk.day_baseline = None
            if latch == "drawdown":
                st.risk.hwm = None
        else:
            raise ValueError(f"unknown control kind {kind!r}")
        try:
            self.store.commit_control(kind, latch, reason, wall_utc, st)
        except BaseException:
            self.halted = True
            raise
        self.state = st

    # ---------------------------------------------------------------- helpers

    def _ledger(self, st: EngineState, rec: Recorder, asset: str, free_d: Decimal, locked_d: Decimal, kind: str,
                order_id: str | None = None, fill_id: str | None = None) -> None:
        st.balances.apply(asset, free_d, locked_d)
        rec.insert("ledger", {
            "seq": rec.seq, "ts_us": rec.now, "asset": asset, "free_delta": free_d, "locked_delta": locked_d,
            "kind": kind, "order_id": order_id, "fill_id": fill_id,
        })

    def _notify(self, rec: Recorder, kind: str, ref: str, text: str) -> None:
        """Queue a PAPER notification in the same transaction as the transition it describes."""
        if self.notify:
            rec.insert_ignore("outbox", {
                "msg_id": f"{kind}:{ref}", "seq": rec.seq, "created_us": rec.now, "kind": kind,
                "text": f"{self.notify_label} | {text}", "status": "pending", "attempts": 0,
            })

    def _set_block(self, st: EngineState, rec: Recorder, block: str, add: bool, **detail) -> None:
        blocks = st.health.blocks
        if add and block not in blocks:
            blocks.append(block)
            rec.health("entry_block_set", block=block, **detail)
            self._notify(rec, "block", f"{block}:{rec.seq}", f"entries blocked: {block}")
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "entry_block:" + block, None)
        elif not add and block in blocks:
            blocks.remove(block)
            rec.health("entry_block_cleared", block=block, **detail)
            self._notify(rec, "unblock", f"{block}:{rec.seq}", f"entry block cleared: {block}")

    def _invalidate_quotes(self, st: EngineState, rec: Recorder, reason: str) -> None:
        """The last quote no longer counts as fresh; a new observation must arrive before any decision uses one."""
        if not st.health.quotes_fresh:
            return
        st.health.quotes_fresh = False
        st.health.stale_since_us = st.clock_us
        exposed = st.balances.btc_total > 0
        if exposed:
            st.health.unprotected_since_us = st.clock_us
        rec.health("quotes_stale", stale_since=iso(st.clock_us), reason=reason, inventory_exposed=exposed)
        if st.order is not None and st.order.purpose == "entry":
            self._close_order(st, rec, "canceled", "quote_stale", None)

    def _fresh_quote(self, st: EngineState) -> QuoteObs | None:
        q = st.last_quote
        if q is None or not st.health.quotes_fresh:
            return None
        if st.clock_us - q.recv_us > self.cfg.quote_max_age_us:
            return None
        return q

    def _value(self, st: EngineState, bid: Decimal) -> Valuation:
        return value(st.balances, st.pool, self.rules, bid, slippage=self.cfg.slippage, sell_fee=self.cfg.sell_fee,
                     sell_cushion=self.cfg.sell_limit_cushion)

    def _avg_reference(self, st: EngineState) -> Reference | None:
        r = st.reference
        return None if r is None else Reference(r.avg_price, r.mins, r.recv_us, r.event_id)

    def _ref_price_known(self, st: EngineState) -> bool:
        rp = st.ref_price
        return rp is not None and 0 <= st.clock_us - rp.recv_us <= self.cfg.reference_max_age_us

    def _reference(self, st: EngineState) -> Reference | None:
        """Reference for PERCENT_PRICE filters: the exchange reference price when non-null, otherwise the
        weighted average; an unknown or stale reference price means the reference is unavailable."""
        if self.rules is None or self.rules.reference_mode == "avg_price":
            return self._avg_reference(st)
        if not self._ref_price_known(st):
            return None
        rp = st.ref_price
        if rp.value is not None:
            return Reference(rp.value, 0, rp.recv_us, rp.event_id, kind="reference_price")
        return self._avg_reference(st)

    def _execution_block(self, st: EngineState, side: str, price: Decimal) -> str | None:
        if self.rules is None or not self.rules.execution_rules:
            return None
        known = self._ref_price_known(st)
        return execution_rule_violation(self.rules, side, price, st.ref_price.value if known else None, known)

    def _candidate_id(self, bar_start_us: int) -> str:
        return f"{self.cfg.account_id}|{self.cfg.symbol}|{STRATEGY_VERSION}|{self.cfg.sha256[:16]}|{iso(bar_start_us)}"

    # ------------------------------------------------------------------- step

    def _step(self, st: EngineState, rec: Recorder, raw: RawEvent) -> tuple[str, dict]:
        if raw.event is None:
            return "rejected", {"reason": raw.error}
        ev = raw.event
        if self.store.source_event_seq(ev.event_id) is not None:
            return "duplicate", {"reason": "event id already committed", "first_seq": self.store.source_event_seq(
                ev.event_id)}
        if st.clock_us is not None and ev.recv_us < st.clock_us:
            return "rejected", {"reason": "non_monotonic_receipt", "clock": iso(st.clock_us),
                                "recv": iso(ev.recv_us)}
        rec.source_event_id = ev.event_id
        st.clock_us = ev.recv_us
        rec.now = ev.recv_us
        if isinstance(ev, MetadataEvent):
            self._on_metadata(st, rec, ev)
        self.rules = self._rules_for(st)
        triggers: list[str] = []
        self._housekeeping(st, rec, triggers, ev)
        disposition, detail = "accepted", {}
        if isinstance(ev, CandleEvent):
            disposition, detail = self._on_candle(st, rec, ev, triggers)
        elif isinstance(ev, QuoteEvent):
            disposition, detail = self._on_quote(st, rec, ev, triggers)
        elif isinstance(ev, ReferenceEvent):
            st.reference = RefObs(ev.event_id, ev.recv_us, ev.avg_price, ev.mins)
        elif isinstance(ev, RefPriceEvent):
            st.ref_price = RefPriceObs(ev.event_id, ev.recv_us, ev.value, ev.exchange_us)
        elif isinstance(ev, FeedEvent):
            self._on_feed(st, rec, ev, triggers)
        elif isinstance(ev, SessionStartEvent):
            self._on_session_start(st, rec, ev, triggers)
        elif isinstance(ev, HeartbeatEvent | MetadataEvent):
            pass
        self._post(st, rec, triggers)
        return disposition, detail

    # ------------------------------------------------------- forward events

    def _on_metadata(self, st: EngineState, rec: Recorder, ev: MetadataEvent) -> None:
        rules = parse_metadata(ev.bundle)
        self._check_rules(rules)
        rec.insert_ignore("metadata_versions", {"sha256": ev.sha256, "fetched_us": ev.fetched_us,
                                                 "first_seq": rec.seq, "json": ev.bundle})
        self._rules_cache[ev.sha256] = rules
        changed = st.metadata_hash != ev.sha256
        st.metadata_hash = ev.sha256
        st.metadata_fetched_us = ev.fetched_us
        rec.health("metadata_version" if changed else "metadata_refreshed", sha256=ev.sha256,
                   fetched=iso(ev.fetched_us), label=rules.label, status=rules.status,
                   execution_rules=len(rules.execution_rules))

    def _on_feed(self, st: EngineState, rec: Recorder, ev: FeedEvent, triggers: list[str]) -> None:
        detail = json.loads(ev.detail)
        rec.health("feed_" + ev.kind, **detail)
        if ev.kind == "ws_disconnected":
            self._set_block(st, rec, BLOCK_FEED, True, reason=detail.get("reason"))
            # Reconnecting is not recovery: quotes from before the disconnect never count as fresh, and entries
            # stay blocked until the runner has fresh observations and has revalidated candle continuity.
            self._set_block(st, rec, BLOCK_FEED_RECOVERY, True, reason="ws_disconnected")
            st.health.recovery_signaled = False
            self._invalidate_quotes(st, rec, "ws_disconnected")
            triggers.append("health:feed_disconnected")
        elif ev.kind == "ws_connected":
            self._set_block(st, rec, BLOCK_FEED, False)
        elif ev.kind == "rest_unavailable":
            self._set_block(st, rec, BLOCK_REST, True, reason=detail.get("reason"))
        elif ev.kind == "rest_ok":
            self._set_block(st, rec, BLOCK_REST, False)
        elif ev.kind == "clock_offset":
            limit = self.cfg.clock_max_offset_us
            offset = int(detail.get("offset_us", 0))
            if limit is not None:
                ok = abs(offset) <= limit
                st.clock_checked_us = st.clock_us if ok else None
                self._set_block(st, rec, BLOCK_CLOCK, not ok, offset_us=offset,
                                reason=None if ok else "offset_exceeds_limit")
        elif ev.kind == "recovered":
            st.health.recovery_signaled = True
        elif ev.kind == "session_stop":
            self._set_block(st, rec, BLOCK_FEED, True, reason="session_stop")

    def _on_session_start(self, st: EngineState, rec: Recorder, ev: SessionStartEvent, triggers: list[str]) -> None:
        """Forward-runner start/restart policy: cancel unfilled entries, retire unresolved exit attempts without
        inventing fills, block entries until recovery, and flatten recovered tradable inventory."""
        st.session_id = ev.session_id
        retired = None
        if st.order is not None:
            retired = st.order.order_id
            if st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "forward_restart", None)
            else:
                self._close_order(st, rec, "zero", "retired_on_restart", None)
        st.health.recovery_signaled = False
        # a quote observed by a previous process is not a fresh observation of this session: flattening and
        # every other decision waits for quotes received after the restart
        self._invalidate_quotes(st, rec, "session_start")
        self._set_block(st, rec, BLOCK_FEED, True, reason="session_start")
        self._set_block(st, rec, BLOCK_RECOVERY, True, reason="session_start")
        if self.cfg.clock_max_offset_us is not None:
            # clock verification starts pending: only a successful server-time check within the limit clears it
            st.clock_checked_us = None
            self._set_block(st, rec, BLOCK_CLOCK, True, reason="clock_unverified")
        rec.health("session_start", session=ev.session_id, restart=ev.restart, retired_order=retired,
                   position=st.position.position_id if st.position else None)
        self._notify(rec, "session", ev.session_id, f"session {'restart' if ev.restart else 'start'} "
                     f"{ev.session_id}; entries blocked until recovery")
        if st.position is not None:
            triggers.append("health:restart_flatten")

    # ------------------------------------------------------------ housekeeping

    def _housekeeping(self, st: EngineState, rec: Recorder, triggers: list[str], ev) -> None:
        cfg, now = self.cfg, st.clock_us
        day = local_date(now, cfg.day_timezone)
        if st.risk.day != day:
            prev = st.risk.day
            st.risk.day = day
            st.risk.baseline_pending = True
            st.risk.day_baseline = None
            rec.risk("day_rollover", previous_day=prev, day=day, timezone=cfg.day_timezone,
                     latches_preserved=list(st.risk.latches))

        if (cfg.forward is not None and st.clock_checked_us is not None
                and now - st.clock_checked_us > CLOCK_CHECK_STALE_FACTOR * cfg.forward.clock_check_s * US_PER_S):
            st.clock_checked_us = None
            self._set_block(st, rec, BLOCK_CLOCK, True, reason="clock_check_expired")

        q = st.last_quote
        if st.health.quotes_fresh and (q is None or now - q.recv_us > cfg.quote_max_age_us):
            stale_at = q.recv_us + cfg.quote_max_age_us
            st.health.quotes_fresh = False
            st.health.stale_since_us = stale_at
            exposed = st.balances.btc_total > 0
            if exposed:
                st.health.unprotected_since_us = stale_at
            rec.health("quotes_stale", stale_since=iso(stale_at), last_quote=q.event_id, inventory_exposed=exposed)
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "quote_stale", None)
            triggers.append("health:quotes_stale")  # queue an exit; it executes only on fresh data + latency

        last = st.strategy.last_start_us
        # The expected bar itself arriving past the deadline is "late" (handled in _on_candle), not "missing".
        is_expected_bar = isinstance(ev, CandleEvent) and last is not None and (
            ev.backfill or ev.start_us == last + MINUTE_US)
        if last is not None and not st.health.candle_missing and not is_expected_bar:
            next_end = last + 2 * MINUTE_US
            if now > next_end + cfg.candle_max_lateness_us:
                st.health.candle_missing = True
                rec.health("candle_missing", expected_bar_start=iso(last + MINUTE_US),
                           detected_at=iso(now))
                if st.order is not None and st.order.purpose == "entry":
                    self._close_order(st, rec, "canceled", "candle_missing", None)
                triggers.append("health:candle_missing")

        o = st.order
        if o is not None:
            if o.purpose == "entry" and now - o.signal_us > cfg.signal_expiry_us:
                self._close_order(st, rec, "canceled", "signal_expired", None)
            elif now > o.ready_us + cfg.quote_max_age_us:
                status = "canceled" if o.purpose == "entry" else "zero"
                self._close_order(st, rec, status, "no_eligible_quote_after_ready", None)

        if st.risk.latches:
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "risk_latch:" + ",".join(st.risk.latches), None)
            triggers.append("halt")

        pos = st.position
        if pos is not None and now - pos.opened_us >= TIMEOUT_US:
            triggers.append("timeout")

    # ----------------------------------------------------------------- candle

    def _on_candle(self, st: EngineState, rec: Recorder, ev: CandleEvent, triggers: list[str]) -> tuple[str, dict]:
        cfg = self.cfg
        end = ev.start_us + MINUTE_US
        reason = None
        if ev.interval != "1m":
            reason = "unsupported_interval"
        elif not ev.final:
            reason = "unfinished_candle"
        elif ev.start_us % MINUTE_US != 0:
            reason = "misaligned_start"
        elif ev.end_us is not None and ev.end_us != end:
            reason = "misaligned_end"
        elif ev.recv_us < end:
            reason = "received_before_interval_end"
        elif not (ZERO < ev.low <= min(ev.open, ev.close) and max(ev.open, ev.close) <= ev.high) or ev.volume < 0:
            reason = "invalid_ohlc"
        else:
            last = st.strategy.last_start_us
            if last is not None and ev.start_us == last:
                reason = "duplicate_bar"
            elif last is not None and ev.start_us < last:
                reason = "out_of_order_bar"
        if reason is not None:
            rec.health("candle_rejected", reason=reason, bar_start=iso(ev.start_us), event_id=ev.event_id)
            return "rejected", {"reason": reason}

        if st.health.candle_missing:
            st.health.candle_missing = False
            rec.health("candle_resumed", bar_start=iso(ev.start_us))
        late = ev.recv_us - end > cfg.candle_max_lateness_us and not ev.backfill
        bar = Bar(ev.start_us, end, ev.open, ev.high, ev.low, ev.close, ev.recv_us)
        res = strategy_update(st.strategy, bar)
        if res.reset:
            rec.health("candle_gap", missing_bars=res.gap_bars, bar_start=iso(ev.start_us),
                       action="indicators reset; warm-up restarts; no forward fill")
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "candle_gap", None)
            triggers.append("health:candle_gap")
        if late:
            rec.health("candle_late", bar_start=iso(ev.start_us), lateness_ms=(ev.recv_us - end) // 1000)
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "candle_late", None)
            triggers.append("health:candle_late")
        five = res.new_five
        if five is not None and trend_invalidated(five):
            if st.order is not None and st.order.purpose == "entry":
                self._close_order(st, rec, "canceled", "trend_invalidated", None)
            if st.position is not None:
                triggers.append("trend_invalidation")
        if res.crossing:
            self._candidate(st, rec, res, late, backfill=ev.backfill)
        if st.last_exit_us is not None and bar.start_us >= st.last_exit_us:
            st.bars_since_exit += 1
        return "accepted", {"late": late} if late else {}

    def _candidate(self, st: EngineState, rec: Recorder, res: BarResult, late: bool, backfill: bool = False) -> None:
        cfg, rules, now, bar = self.cfg, self.rules, st.clock_us, res.bar
        cid = self._candidate_id(bar.start_us)
        st.candidates_seen += 1
        five = res.last_five
        detail: dict = {
            "bar_close": bar.close, "h": res.h, "prev_close": res.prev_close, "prev_h": res.prev_h,
            "atr14": res.atr, "five_close": five.close if five else None, "five_ema20": five.ema if five else None,
            "five_prev_ema20": five.prev_ema if five else None, "five_end": iso(five.end_us) if five else None,
            "five_count": st.strategy.five_count,
        }
        row = {
            "candidate_id": cid, "seq": rec.seq, "bar_start_us": bar.start_us, "bar_end_us": bar.end_us,
            "candle_recv_us": bar.recv_us, "decision_us": now, "close": bar.close, "h": res.h,
            "atr": res.atr, "stop_distance": None, "quote_event_id": None, "qty": None, "limit_price": None,
        }

        def skip(reasons: list[str]) -> None:
            row.update(status="skipped", skip_reason=reasons[0], detail={"reasons": reasons, **detail})
            rec.insert("candidates", row)

        reasons: list[str] = []
        if backfill:
            reasons.append("backfill_no_retroactive_entry")
        if not res.warm:
            reasons.append("warmup_incomplete")
        if late:
            reasons.append("late_candle")
        if not res.trend_ok:
            reasons.append("trend_filter")
        if st.position is not None:
            reasons.append("position_open")
        if st.order is not None:
            reasons.append("order_pending")
        if st.last_exit_us is not None and st.bars_since_exit < COOLDOWN_BARS:
            reasons.append("cooldown")
        if st.risk.latches:
            reasons.append("risk_latch:" + ",".join(st.risk.latches))
        if st.risk.baseline_pending:
            reasons.append("day_baseline_pending")
        if st.health.blocks:
            reasons.append("entry_block:" + ",".join(st.health.blocks))
        if rules is None:
            reasons.append("metadata_missing")
        elif (st.metadata_fetched_us is not None and cfg.metadata_max_age_us is not None
              and now - st.metadata_fetched_us > cfg.metadata_max_age_us):
            reasons.append("metadata_expired")
        if rules is None:
            return skip(reasons)
        q = self._fresh_quote(st)
        if q is None:
            reasons.append("no_fresh_quote")
        elif st.position is None and sellable_quantity(st.balances.btc_total, rules, q.bid,
                                                       cfg.sell_limit_cushion) > 0:
            # residual inventory that became sellable without an open position: block new risk
            reasons.append("tradable_residual_inventory")
        if res.atr is None or res.atr <= 0:
            reasons.append("atr_invalid")
        if reasons:
            return skip(reasons)

        atr = res.atr
        d = STOP_ATR_MULT * atr
        row["stop_distance"] = d
        row["quote_event_id"] = q.event_id
        spread = spread_fraction(q.bid, q.ask)
        exp_buy = buy_fill_price(q.ask, cfg.slippage, rules.tick)
        drift = exp_buy - bar.close
        gate = cost_gate(q.ask, d, buy_fee=cfg.buy_fee, sell_fee=cfg.sell_fee, spread=spread, slippage=cfg.slippage,
                         min_net_target_fraction=MIN_NET_TARGET_FRACTION, min_reward_risk=MIN_NET_REWARD_RISK,
                         target_mult=TARGET_STOP_MULT)
        limit = buy_limit_price(q.ask, cfg.buy_limit_cushion, rules.tick)
        detail.update({
            "quote_bid": q.bid, "quote_ask": q.ask, "quote_ask_qty": q.ask_qty, "quote_recv": iso(q.recv_us),
            "quote_age_us": now - q.recv_us, "spread_fraction": spread, "expected_buy_price": exp_buy,
            "drift": drift, "drift_cap": cfg.max_entry_drift_atr * atr, "stop_distance": d,
            "target_distance": TARGET_STOP_MULT * d, "cost_fraction_C": gate.cost_fraction,
            "net_target": gate.net_target, "net_risk": gate.net_risk, "net_reward_risk": gate.ratio,
            "limit_price": limit,
        })
        if spread > cfg.max_spread:
            return skip(["spread"])
        if drift > cfg.max_entry_drift_atr * atr:
            return skip(["entry_drift"])
        if not gate.ok:
            return skip([gate.reason])
        if exp_buy > limit:
            return skip(["limit_below_expected_fill"])
        blocked = self._execution_block(st, "BUY", exp_buy)
        if blocked is not None:
            return skip([blocked])

        v = self._value(st, q.bid)
        equity = v.equity
        fee_mult = ONE + cfg.buy_fee if cfg.buy_fee_asset == "USDT" else ONE
        unit_cost = limit * fee_mult
        caps = {
            "risk": cfg.risk_per_entry * equity / gate.net_risk,
            "cash": st.balances.usdt_free / unit_cost,
            "exposure": max(ZERO, cfg.max_exposure * equity - v.exposure) / unit_cost,
            "visible_liquidity": q.ask_qty * cfg.participation,
        }
        if rules.max_qty > 0:
            caps["lot_max_qty"] = rules.max_qty
        if rules.max_position > 0:
            caps["max_position"] = max(ZERO, rules.max_position - st.balances.btc_total)
        qty = floor_to(min(caps.values()), rules.step)
        binding = min(caps, key=lambda k: caps[k])
        detail.update({"equity": equity, "exposure_before": v.exposure, "size_caps": caps, "binding_cap": binding,
                       "qty": qty})
        if qty <= 0:
            return skip(["size_zero:" + binding])
        val = validate_limit_order(rules, "BUY", limit, qty, open_orders=0, base_position=st.balances.btc_total,
                                   reference=self._reference(st), now_us=now,
                                   reference_max_age_us=cfg.reference_max_age_us)
        detail["validation"] = val.summary()
        if not val.ok:
            return skip(["filter:" + val.first_failure()])

        reserved = qty * unit_cost
        if reserved > st.balances.usdt_free:
            return skip(["insufficient_cash"])
        row.update(status="submitted", skip_reason=None, qty=qty, limit_price=limit, detail=detail)
        rec.insert("candidates", row)
        order_id = cid + "|entry"
        o = PendingOrder(
            order_id=order_id, side="BUY", purpose="entry", qty=qty, limit_price=limit, submitted_us=now,
            ready_us=now + cfg.latency_us, reserved_asset="USDT", reserved_amount=reserved,
            reference_quote_id=q.event_id, candidate_id=cid, position_id=None, attempt=1, signal_us=now,
            signal_close=bar.close, atr=atr, stop_distance=d, ineligible_seen=0,
        )
        rec.insert("orders", self._order_row(o, rec, status="pending"))
        self._ledger(st, rec, "USDT", -reserved, reserved, "reserve", order_id=order_id)
        st.order = o

    # ------------------------------------------------------------------ quote

    def _on_quote(self, st: EngineState, rec: Recorder, ev: QuoteEvent, triggers: list[str]) -> tuple[str, dict]:
        cfg = self.cfg
        reason = None
        if ev.bid <= 0 or ev.ask <= 0:
            reason = "nonpositive_price"
        elif ev.ask < ev.bid:
            reason = "inverted_quote"
        elif ev.bid_qty <= 0 or ev.ask_qty <= 0:
            reason = "nonpositive_size"
        elif ev.exchange_us is not None and ev.exchange_us > ev.recv_us:
            reason = "exchange_time_after_receipt"
        if reason is not None:
            rec.health("quote_rejected", reason=reason, event_id=ev.event_id)
            return "rejected", {"reason": reason}
        q = QuoteObs(ev.event_id, ev.recv_us, ev.bid, ev.bid_qty, ev.ask, ev.ask_qty, ev.exchange_us)
        st.last_quote = q
        if not st.health.quotes_fresh:
            st.health.quotes_fresh = True
            stale_for = None if st.health.stale_since_us is None else ev.recv_us - st.health.stale_since_us
            unprotected = None
            if st.health.unprotected_since_us is not None:
                unprotected = ev.recv_us - st.health.unprotected_since_us
                st.health.unprotected_total_us += unprotected
                st.health.unprotected_since_us = None
            st.health.stale_since_us = None
            rec.health("quotes_fresh", stale_for_us=stale_for, unprotected_us=unprotected, event_id=ev.event_id)

        o = st.order
        if o is not None:
            why = eligibility(q.view(), o.ready_us, cfg.quote_max_age_us)
            if why is not None:
                o.ineligible_seen += 1
                if why == "observation_too_long_after_ready":
                    status = "canceled" if o.purpose == "entry" else "zero"
                    self._close_order(st, rec, status, "no_eligible_quote_after_ready", None)
            elif o.purpose == "entry":
                self._fill_entry(st, rec, o, q)
            else:
                self._fill_exit(st, rec, o, q)

        pos = st.position
        if pos is not None:
            if q.bid <= pos.stop_price:
                triggers.append("stop")
            if q.bid >= pos.target_price:
                triggers.append("target")
        return "accepted", {}

    def _fill_entry(self, st: EngineState, rec: Recorder, o: PendingOrder, q: QuoteObs) -> None:
        cfg, rules, now = self.cfg, self.rules, st.clock_us
        spread = spread_fraction(q.bid, q.ask)
        price = buy_fill_price(q.ask, cfg.slippage, rules.tick)
        gate = cost_gate(q.ask, o.stop_distance, buy_fee=cfg.buy_fee, sell_fee=cfg.sell_fee, spread=spread,
                         slippage=cfg.slippage, min_net_target_fraction=MIN_NET_TARGET_FRACTION,
                         min_reward_risk=MIN_NET_REWARD_RISK, target_mult=TARGET_STOP_MULT)
        guard = None
        if now - o.signal_us > cfg.signal_expiry_us:
            guard = "signal_expired"
        elif st.risk.latches:
            guard = "risk_latch"
        elif spread > cfg.max_spread:
            guard = "spread"
        elif price - o.signal_close > cfg.max_entry_drift_atr * o.atr:
            guard = "entry_drift"
        elif not gate.ok:
            guard = gate.reason
        if guard is not None:
            return self._close_order(st, rec, "canceled", "guard_failed_at_fill:" + guard, q)
        if price > o.limit_price:
            return self._close_order(st, rec, "zero", "price_protection", q)
        blocked = self._execution_block(st, "BUY", price)
        if blocked is not None:  # the exchange would expire this taker order
            return self._close_order(st, rec, "zero", blocked, q)
        # Fill-time caps, recomputed from this observation: the submitted quantity is never enlarged, and the
        # fill never exceeds the modeled risk budget or exposure headroom at current equity, d + p*C.
        v = self._value(st, q.bid)
        credit_ratio = ONE - cfg.buy_fee if cfg.buy_fee_asset == "BTC" else ONE
        caps = {
            "order": o.qty,
            "visible_liquidity": q.ask_qty * cfg.participation,
            "risk": cfg.risk_per_entry * v.equity / gate.net_risk,
            "exposure": max(ZERO, cfg.max_exposure * v.equity - v.btc_mark_value) / (q.bid * credit_ratio),
        }
        qty = floor_to(min(caps.values()), rules.step)
        binding = min(caps, key=lambda k: caps[k])
        if qty <= 0:
            reason = "insufficient_visible_liquidity" if binding == "visible_liquidity" else f"fill_cap:{binding}"
            return self._close_order(st, rec, "zero", reason, q)
        amounts = buy_amounts(qty, price, cfg.buy_fee, cfg.buy_fee_asset)
        if amounts.usdt_debit > o.reserved_amount:
            raise InvariantViolation("buy fill would exceed its reservation")
        if qty * gate.net_risk > cfg.risk_per_entry * v.equity:
            raise InvariantViolation("entry fill exceeds the modeled risk budget")
        if v.btc_mark_value + amounts.btc_credit * q.bid > cfg.max_exposure * v.equity:
            raise InvariantViolation("entry fill exceeds the exposure cap")

        position_id = o.candidate_id + "|pos"
        d = o.stop_distance
        pos = Position(
            position_id=position_id, candidate_id=o.candidate_id, opened_us=now, entry_price=price,
            entry_qty=amounts.btc_credit, stop_distance=d, stop_price=floor_to(price - d, rules.tick),
            target_price=ceil_to(price + TARGET_STOP_MULT * d, rules.tick), exit_intent=None, exit_orders=0,
            metadata_sha256=st.metadata_hash,
        )
        rec.insert("positions", {
            "position_id": position_id, "candidate_id": o.candidate_id, "opened_us": now, "entry_price": price,
            "entry_qty": amounts.btc_credit, "stop_distance": d, "stop_price": pos.stop_price,
            "target_price": pos.target_price, "status": "open", "exit_reason": None, "closed_us": None,
            "residual_qty": None, "close_detail": None,
        })
        fill_id = o.order_id + "|fill"
        rec.insert("fills", self._fill_row(fill_id, o, q, qty, price, amounts.usdt_debit, rec, side="BUY",
                                           fee_asset=amounts.fee_asset, fee_amount=amounts.fee_amount,
                                           fee_usdt=amounts.fee_usdt, usdt_delta=-amounts.usdt_debit,
                                           btc_delta=amounts.btc_credit, basis_gross=amounts.gross_cost,
                                           basis_fee=amounts.entry_fee_cost, gross_pnl=None, net_pnl=None))
        self._ledger(st, rec, "USDT", ZERO, -amounts.usdt_debit, "buy_fill", o.order_id, fill_id)
        self._ledger(st, rec, "BTC", amounts.btc_credit, ZERO, "buy_fill", o.order_id, fill_id)
        add_to_pool(st.pool, amounts)
        o.position_id = position_id
        st.position = pos
        status = "filled" if qty == o.qty else "partial"
        reason = None if status == "filled" else f"ioc_remainder_canceled;fill_cap={binding}"
        self._close_order(st, rec, status, reason, q, filled_qty=qty, spent=amounts.usdt_debit)
        self._notify(rec, "fill", fill_id, f"SIMULATED BUY {dtext(amounts.btc_credit)} BTC credited @ {dtext(price)} "
                     f"({status}); fee {dtext(amounts.fee_amount)} {amounts.fee_asset}; stop {dtext(pos.stop_price)} "
                     f"target {dtext(pos.target_price)}")

    def _fill_exit(self, st: EngineState, rec: Recorder, o: PendingOrder, q: QuoteObs) -> None:
        cfg, rules = self.cfg, self.rules
        pos = st.position
        intent = pos.exit_intent
        intent.attempts += 1
        price = sell_fill_price(q.bid, cfg.slippage, rules.tick)
        if price < o.limit_price:
            self._close_order(st, rec, "zero", "price_protection", q)
            return
        blocked = self._execution_block(st, "SELL", price)
        if blocked is not None:
            self._close_order(st, rec, "zero", blocked, q)
            return
        qty = fill_quantity(o.qty, q.bid_qty, cfg.participation, rules.step)
        if qty <= 0:
            self._close_order(st, rec, "zero", "insufficient_visible_liquidity", q)
            return
        if qty > st.balances.btc_locked or qty > st.pool.qty:
            raise InvariantViolation("sell fill exceeds reserved inventory")
        amounts = sell_amounts(qty, price, cfg.sell_fee)
        gross_alloc, fee_alloc = allocate(st.pool, qty)
        gross_pnl = amounts.proceeds - gross_alloc
        net_pnl = amounts.usdt_credit - gross_alloc - fee_alloc
        fill_id = o.order_id + "|fill"
        rec.insert("fills", self._fill_row(fill_id, o, q, qty, price, amounts.proceeds, rec, side="SELL",
                                           fee_asset="USDT", fee_amount=amounts.fee_amount,
                                           fee_usdt=amounts.fee_amount, usdt_delta=amounts.usdt_credit,
                                           btc_delta=-qty, basis_gross=gross_alloc, basis_fee=fee_alloc,
                                           gross_pnl=gross_pnl, net_pnl=net_pnl))
        self._ledger(st, rec, "BTC", ZERO, -qty, "sell_fill", o.order_id, fill_id)
        self._ledger(st, rec, "USDT", amounts.usdt_credit, ZERO, "sell_fill", o.order_id, fill_id)
        remove_from_pool(st.pool, qty, gross_alloc, fee_alloc)
        self._notify(rec, "fill", fill_id, f"SIMULATED SELL {dtext(qty)} BTC @ {dtext(price)} ({intent.reason}); "
                     f"net {dtext(net_pnl.quantize(Decimal('0.0001')))} USDT; fee {dtext(amounts.fee_amount)} USDT")
        status = "filled" if qty == o.qty else "partial"
        self._close_order(st, rec, status, None if status == "filled" else "ioc_remainder_canceled", q,
                          filled_qty=qty, spent=qty)
        remaining = sellable_quantity(st.balances.btc_total, rules, q.bid, cfg.sell_limit_cushion)
        if remaining == 0:
            self._close_position(st, rec, "exit_complete" if st.balances.btc_total == 0 else "residual_unsellable")

    def _close_order(self, st: EngineState, rec: Recorder, status: str, reason: str | None, q: QuoteObs | None,
                     filled_qty: Decimal = ZERO, spent: Decimal = ZERO) -> None:
        o = st.order
        release = o.reserved_amount - spent
        if release < 0:
            raise InvariantViolation("order consumed more than its reservation")
        if release > 0:
            self._ledger(st, rec, o.reserved_asset, release, -release, "release", order_id=o.order_id)
        rec.update("orders", "order_id", self._order_row(o, rec, status=status, reason=reason,
                                                         outcome_quote=q.event_id if q else None,
                                                         filled_qty=filled_qty, closed=True))
        st.order = None

    def _close_position(self, st: EngineState, rec: Recorder, how: str) -> None:
        pos = st.position
        now = st.clock_us
        intent = pos.exit_intent
        residual = st.balances.btc_total
        rec.update("positions", "position_id", {
            "position_id": pos.position_id, "status": "closed", "exit_reason": intent.reason if intent else how,
            "closed_us": now, "residual_qty": residual,
            "close_detail": {"how": how, "residual_btc": residual, "exit_orders": pos.exit_orders,
                             "residual_note": "retained as dust with basis; never deleted or called sold"
                             if residual > 0 else None},
        })
        if intent is not None:
            rec.update("exit_intents", "intent_id", {
                "intent_id": intent.intent_id, "status": how, "attempts": intent.attempts, "closed_us": now,
            })
        st.position = None
        st.last_exit_us = now
        st.bars_since_exit = 0

    # ------------------------------------------------------------------- post

    def _post(self, st: EngineState, rec: Recorder, triggers: list[str]) -> None:
        cfg = self.cfg
        q = self._fresh_quote(st) if self.rules is not None else None
        if q is not None:
            v = self._value(st, q.bid)
            r = st.risk
            r.last_equity = v.equity
            r.last_mark_us = q.recv_us
            if r.baseline_pending:
                r.day_baseline = v.equity
                r.baseline_pending = False
                rec.risk("day_baseline_set", day=r.day, equity=v.equity, mark_quote=q.event_id)
            if r.hwm is None:
                r.hwm = v.equity
                rec.risk("hwm_set", equity=v.equity, mark_quote=q.event_id)
            elif v.equity > r.hwm:
                r.hwm = v.equity
            checks = (("daily_loss", r.day_baseline, cfg.daily_loss), ("drawdown", r.hwm, cfg.drawdown))
            for latch, ref, threshold in checks:
                if latch in r.latches or ref is None or ref <= 0:
                    continue
                loss = (ref - v.equity) / ref
                if loss >= threshold:
                    r.latches.append(latch)
                    rec.risk("latch_set", latch=latch, equity=v.equity, reference_equity=ref, loss_fraction=loss,
                             threshold=threshold, overshoot_fraction=loss - threshold, mark_quote=q.event_id,
                             note="protective latch, not a guaranteed loss cap")
                    self._notify(rec, "latch", f"{latch}:{rec.seq}", f"RISK LATCH {latch}: loss {dtext(loss)} "
                                 f">= {dtext(threshold)}; entries halted; exit queued")
                    if st.order is not None and st.order.purpose == "entry":
                        self._close_order(st, rec, "canceled", "risk_latch:" + latch, None)
                    triggers.append("halt")

        pending_recovery = [b for b in RECOVERY_BLOCKS if b in st.health.blocks]
        if pending_recovery and st.health.recovery_signaled and st.position is None and st.order is None:
            for b in pending_recovery:
                self._set_block(st, rec, b, False, note="recovered; no tradable position or order")
            rec.health("rearmed", session=st.session_id, cleared=pending_recovery)
        pos = st.position
        if pos is not None and pos.exit_intent is None and triggers:
            reason = min(triggers, key=_priority)
            intent = ExitIntent(pos.position_id + "|exit-intent", reason, st.clock_us, 0, None)
            pos.exit_intent = intent
            rec.insert("exit_intents", {
                "intent_id": intent.intent_id, "position_id": pos.position_id, "reason": reason,
                "created_us": st.clock_us, "status": "active", "attempts": 0, "closed_us": None,
                "detail": {"triggers": sorted(set(triggers), key=_priority)},
            })
            self._notify(rec, "exit_intent", intent.intent_id, f"EXIT QUEUED ({reason}) for "
                         f"{dtext(st.balances.btc_total)} BTC; executes only on fresh quotes after latency")
        if st.position is not None and st.position.exit_intent is not None and st.order is None:
            if self.rules is None:
                self._exit_blocked(rec, st.position.exit_intent, "metadata_missing")
            else:
                self._submit_exit(st, rec)

    def _submit_exit(self, st: EngineState, rec: Recorder) -> None:
        cfg, rules, now = self.cfg, self.rules, st.clock_us
        pos = st.position
        intent = pos.exit_intent
        q = self._fresh_quote(st)
        if q is None:
            return self._exit_blocked(rec, intent, "awaiting_fresh_quote")
        qty = floor_to(st.balances.btc_free, rules.step)
        if rules.max_qty > 0 and qty > rules.max_qty:
            qty = floor_to(rules.max_qty, rules.step)
        limit = sell_limit_price(q.bid, cfg.sell_limit_cushion, rules.tick)
        val = validate_limit_order(rules, "SELL", limit, qty, open_orders=0, base_position=st.balances.btc_total,
                                   reference=self._reference(st), now_us=now,
                                   reference_max_age_us=cfg.reference_max_age_us)
        if not val.ok:
            if qty <= 0 or val.size_failure_only():
                return self._close_position(st, rec, "residual_unsellable")
            return self._exit_blocked(rec, intent, "filter:" + val.first_failure())
        intent.blocked_reason = None
        pos.exit_orders += 1
        order_id = f"{pos.position_id}|exit|{pos.exit_orders}"
        o = PendingOrder(
            order_id=order_id, side="SELL", purpose="exit", qty=qty, limit_price=limit, submitted_us=now,
            ready_us=now + cfg.latency_us, reserved_asset="BTC", reserved_amount=qty, reference_quote_id=q.event_id,
            candidate_id=None, position_id=pos.position_id, attempt=pos.exit_orders, signal_us=None,
            signal_close=None, atr=None, stop_distance=None, ineligible_seen=0,
        )
        rec.insert("orders", self._order_row(o, rec, status="pending", exit_reason=intent.reason))
        self._ledger(st, rec, "BTC", -qty, qty, "reserve", order_id=order_id)
        st.order = o

    def _exit_blocked(self, rec: Recorder, intent: ExitIntent, reason: str) -> None:
        if intent.blocked_reason != reason:
            intent.blocked_reason = reason
            rec.health("exit_waiting", intent_id=intent.intent_id, reason=reason,
                       note="inventory retained; no protective fill is invented")

    # ------------------------------------------------------------------- rows

    def _order_row(self, o: PendingOrder, rec: Recorder, *, status: str, reason: str | None = None,
                   outcome_quote: str | None = None, filled_qty: Decimal = ZERO, closed: bool = False,
                   exit_reason: str | None = None) -> dict:
        row = {
            "order_id": o.order_id, "status": status, "outcome_reason": reason, "outcome_quote_id": outcome_quote,
            "filled_qty": filled_qty, "closed_us": rec.now if closed else None,
            "closed_seq": rec.seq if closed else None,
            "ineligible_quotes_seen": o.ineligible_seen, "position_id": o.position_id,
        }
        if not closed:
            row.update({
                "candidate_id": o.candidate_id, "purpose": o.purpose, "side": o.side, "attempt": o.attempt,
                "order_type": "LIMIT", "time_in_force": "IOC", "qty": o.qty, "limit_price": o.limit_price,
                "submitted_us": o.submitted_us, "ready_us": o.ready_us, "submitted_seq": rec.seq,
                "reference_quote_id": o.reference_quote_id, "reserved_asset": o.reserved_asset,
                "reserved_amount": o.reserved_amount, "signal_us": o.signal_us, "exit_reason": exit_reason,
            })
        return row

    def _fill_row(self, fill_id: str, o: PendingOrder, q: QuoteObs, qty: Decimal, price: Decimal,
                  gross_notional: Decimal, rec: Recorder, **kw) -> dict:
        return {
            "fill_id": fill_id, "order_id": o.order_id, "quote_event_id": q.event_id, "seq": rec.seq, "qty": qty,
            "price": price, "gross_notional": gross_notional, "quote_recv_us": q.recv_us,
            "quote_exchange_us": q.exchange_us, "bid": q.bid, "ask": q.ask, "bid_qty": q.bid_qty,
            "ask_qty": q.ask_qty, "fill_us": rec.now, "committed_us": rec.now, "signal_us": o.signal_us,
            "submitted_us": o.submitted_us, "ready_us": o.ready_us, **kw,
        }


def detail_json(detail: dict) -> str:
    def enc(x):
        if isinstance(x, Decimal):
            return dtext(x)
        raise TypeError(type(x).__name__)

    return json.dumps(detail, default=enc, sort_keys=True)
