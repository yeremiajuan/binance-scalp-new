"""Startup reconciliation. Any problem halts; nothing here repairs or resets state."""

from __future__ import annotations

import json
from decimal import Decimal

from . import codec
from .constraints import parse_metadata
from .engine import EngineState
from .events import CandleEvent, InputError, QuoteEvent, RawEvent, ReferenceEvent, parse_event
from .money import ZERO, ceil_to, exact, floor_to
from .storage import Storage
from .strategy import TARGET_STOP_MULT, Bar, StrategyState
from .strategy import update as strategy_update
from .timeutil import MINUTE_US


class ReconciliationError(RuntimeError):
    def __init__(self, problems: list[str]):
        super().__init__("state reconciliation failed:\n  - " + "\n  - ".join(problems))
        self.problems = problems


def _d(v: object) -> Decimal:
    return Decimal(v) if v is not None else ZERO


@exact
def reconcile(store: Storage, state: EngineState, *, config_sha: str | None = None,
              metadata_sha: str | None = None, input_sha: str | None = None,
              input_events: list[RawEvent] | None = None) -> list[str]:
    c = store.conn
    p: list[str] = []
    meta = store.meta()

    fk = c.execute("PRAGMA foreign_key_check").fetchall()
    if fk:
        p.append(f"foreign key violations: {len(fk)}")
    real_cols = c.execute(
        "SELECT m.name, i.name FROM sqlite_master m JOIN pragma_table_info(m.name) i "
        "WHERE m.type = 'table' AND upper(i.type) = 'REAL'").fetchall()
    if real_cols:
        p.append(f"REAL columns present: {[tuple(r) for r in real_cols]}")

    for key, want in (("config_sha256", config_sha), ("metadata_sha256", metadata_sha), ("input_sha256", input_sha)):
        if want is not None and meta.get(key) != want:
            p.append(f"{key} mismatch: state has {meta.get(key)}, supplied {want} (incompatible configuration/input)")
    if meta.get("account_id") != state.account_id:
        p.append("account id in snapshot differs from meta")

    seq, clock = store.cursor()
    if seq != state.cursor or clock != state.clock_us:
        p.append(f"cursor table ({seq}, {clock}) != snapshot ({state.cursor}, {state.clock_us})")
    n, mx = c.execute("SELECT count(*), coalesce(max(seq), 0) FROM input_log").fetchone()
    if n != seq or mx != seq:
        p.append(f"input_log has {n} rows / max seq {mx}, cursor is {seq}")

    # Balances: table == snapshot == initial + sum(ledger deltas), all non-negative.
    b = state.balances
    snap = {"USDT": (b.usdt_free, b.usdt_locked), "BTC": (b.btc_free, b.btc_locked)}
    sums = {a: [ZERO, ZERO] for a in snap}
    for r in c.execute("SELECT asset, free_delta, locked_delta FROM ledger"):
        sums[r["asset"]][0] += Decimal(r["free_delta"])
        sums[r["asset"]][1] += Decimal(r["locked_delta"])
    for r in c.execute("SELECT asset, free, locked FROM balances"):
        a = r["asset"]
        table = (Decimal(r["free"]), Decimal(r["locked"]))
        if table != snap[a]:
            p.append(f"{a} balance table {table} != snapshot {snap[a]}")
        if tuple(sums[a]) != table:
            p.append(f"{a} ledger sum {tuple(sums[a])} != balance {table}")
        if table[0] < 0 or table[1] < 0:
            p.append(f"{a} negative balance {table}")

    # Reservations: locked funds belong to exactly the one pending order, if any.
    pending = c.execute(
        "SELECT order_id, reserved_asset, reserved_amount FROM orders WHERE status = 'pending'").fetchall()
    if len(pending) > 1:
        p.append(f"{len(pending)} pending orders; at most one is allowed")
    want_locked = {"USDT": ZERO, "BTC": ZERO}
    if state.order is not None:
        ids = [r["order_id"] for r in pending]
        if ids != [state.order.order_id]:
            p.append(f"pending orders {ids} != snapshot order {state.order.order_id}")
        want_locked[state.order.reserved_asset] = state.order.reserved_amount
    elif pending:
        p.append(f"orphan pending orders in table: {[r['order_id'] for r in pending]}")
    for a in want_locked:
        if snap[a][1] != want_locked[a]:
            p.append(f"{a} locked {snap[a][1]} != reservations of pending orders {want_locked[a]}")
    for r in c.execute(
            "SELECT o.order_id, o.status, o.reserved_asset, o.reserved_amount, "
            "group_concat(l.locked_delta, ' ') AS deltas FROM orders o "
            "LEFT JOIN ledger l ON l.order_id = o.order_id AND l.asset = o.reserved_asset GROUP BY o.order_id"):
        held = sum((Decimal(x) for x in (r["deltas"] or "").split()), ZERO)
        expect = Decimal(r["reserved_amount"]) if r["status"] == "pending" else ZERO
        if held != expect:
            p.append(f"order {r['order_id']} ({r['status']}) still holds {held} locked; expected {expect}")

    # Inventory and cost basis versus fills.
    pool = state.pool
    if b.btc_free + b.btc_locked != pool.qty:
        p.append(f"BTC balance {b.btc_free + b.btc_locked} != inventory pool {pool.qty}")
    btc_delta = usdt_delta = ZERO
    gross = fee = ZERO
    for r in c.execute("SELECT side, usdt_delta, btc_delta, basis_gross, basis_fee, fee_amount FROM fills"):
        btc_delta += Decimal(r["btc_delta"])
        usdt_delta += Decimal(r["usdt_delta"])
        sign = 1 if r["side"] == "BUY" else -1
        gross += sign * Decimal(r["basis_gross"])
        fee += sign * Decimal(r["basis_fee"])
        if Decimal(r["fee_amount"]) < 0:
            p.append("negative fee amount")
    init = {r["asset"]: Decimal(r["free_delta"]) for r in c.execute(
        "SELECT asset, free_delta FROM ledger WHERE kind = 'initial_balance'")}
    if init.get("BTC", ZERO) + btc_delta != pool.qty:
        p.append(f"fills imply BTC {init.get('BTC', ZERO) + btc_delta}, pool has {pool.qty}")
    if init.get("USDT", ZERO) + usdt_delta != b.usdt_free + b.usdt_locked:
        p.append(f"fills imply USDT {init.get('USDT', ZERO) + usdt_delta}, balance {b.usdt_free + b.usdt_locked}")
    if (gross, fee) != (pool.gross_cost, pool.entry_fee_cost):
        p.append(f"fills imply basis ({gross}, {fee}) != pool ({pool.gross_cost}, {pool.entry_fee_cost})")
    if pool.qty == 0 and pool.basis != 0:
        p.append("zero inventory with residual basis")
    dup = c.execute("SELECT count(*) FROM (SELECT order_id FROM fills GROUP BY order_id HAVING count(*) > 1)")
    if dup.fetchone()[0]:
        p.append("an order has more than one fill")
    over = c.execute("SELECT o.order_id, o.qty, f.qty FROM orders o JOIN fills f USING(order_id)").fetchall()
    for r in over:
        if Decimal(r[2]) > Decimal(r[1]):
            p.append(f"fill exceeds order quantity for {r[0]}")

    # Position, its protective prices and exit state versus persisted rows and the entry fill.
    open_pos = [r["position_id"] for r in c.execute("SELECT position_id FROM positions WHERE status = 'open'")]
    want_pos = [state.position.position_id] if state.position is not None else []
    if open_pos != want_pos:
        p.append(f"open positions {open_pos} != snapshot {want_pos}")
    rules = active_rules(store, state, meta)
    tick = rules.tick if rules is not None else None
    if state.position is not None and open_pos == want_pos:
        if tick is None:
            p.append("open position without any metadata version")
        else:
            p += _check_position(c, state, tick)
    active = [r["intent_id"] for r in c.execute("SELECT intent_id FROM exit_intents WHERE status = 'active'")]
    want_active = ([state.position.exit_intent.intent_id]
                   if state.position is not None and state.position.exit_intent is not None else [])
    if active != want_active:
        p.append(f"active exit intents {active} != snapshot {want_active}")
    if state.order is not None and len(pending) == 1 and pending[0]["order_id"] == state.order.order_id:
        p += _check_order(c, state)
    closed = c.execute("SELECT max(closed_us) FROM positions WHERE status = 'closed'").fetchone()[0]
    if closed != state.last_exit_us:
        p.append(f"last exit time {state.last_exit_us} != latest closed position {closed}")
    p += _check_risk_marks(c, state)
    if store.record_payloads:
        p += _check_payloads(store, state)
        if input_events is None:
            input_events = payload_events(store)
    if state.metadata_hash is not None and store.metadata_bundle(state.metadata_hash) is None:
        p.append(f"active metadata version {state.metadata_hash} is not stored")
    if input_events is not None:
        p += _check_inputs(c, state, input_events)

    # Latches: replay risk-latch events and audited control events in order.
    events: list[tuple[int, int, str, str]] = []
    for r in c.execute("SELECT seq, detail FROM risk_events WHERE kind = 'latch_set'"):
        events.append((r["seq"], 0, "set", json.loads(r["detail"])["latch"]))
    for r in c.execute("SELECT at_cursor, kind, latch, id FROM control_events ORDER BY id"):
        events.append((r["at_cursor"], 1, "set" if r["kind"] == "kill" else "reset", r["latch"]))
    latches: list[str] = []
    for _, _, action, latch in sorted(events, key=lambda e: (e[0], e[1])):
        if action == "set" and latch not in latches:
            latches.append(latch)
        elif action == "reset" and latch in latches:
            latches.remove(latch)
    if sorted(latches) != sorted(state.risk.latches):
        p.append(f"latch history implies {sorted(latches)}, snapshot has {sorted(state.risk.latches)}")
    return p


def _same(p: list[str], what: str, snap: object, stored: object) -> None:
    if isinstance(snap, Decimal) and stored is not None:
        stored = Decimal(stored)
    if snap != stored:
        p.append(f"{what}: snapshot {snap!r} != persisted {stored!r}")


def _check_position(c, state: EngineState, tick: Decimal) -> list[str]:
    p: list[str] = []
    pos = state.position
    row = c.execute("SELECT * FROM positions WHERE position_id = ?", (pos.position_id,)).fetchone()
    for f in ("candidate_id", "opened_us", "entry_price", "entry_qty", "stop_distance", "stop_price", "target_price"):
        _same(p, f"position.{f}", getattr(pos, f), row[f])
    fills = c.execute("SELECT f.* FROM fills f JOIN orders o USING(order_id) WHERE o.purpose = 'entry' "
                      "AND o.position_id = ?", (pos.position_id,)).fetchall()
    if len(fills) != 1:
        p.append(f"position {pos.position_id} has {len(fills)} entry fills")
    else:
        f = fills[0]
        _same(p, "entry fill price", pos.entry_price, f["price"])
        _same(p, "entry fill credited BTC", pos.entry_qty, f["btc_delta"])
        _same(p, "entry fill time", pos.opened_us, f["fill_us"])
    cand = c.execute("SELECT stop_distance FROM candidates WHERE candidate_id = ?", (pos.candidate_id,)).fetchone()
    if cand is None:
        p.append(f"position candidate {pos.candidate_id} missing")
    else:
        _same(p, "position stop distance vs candidate", pos.stop_distance, cand["stop_distance"])
    _same(p, "stop price vs entry - d", floor_to(pos.entry_price - pos.stop_distance, tick), pos.stop_price)
    _same(p, "target price vs entry + 3d", ceil_to(pos.entry_price + TARGET_STOP_MULT * pos.stop_distance, tick),
          pos.target_price)
    n_exit = c.execute("SELECT count(*) FROM orders WHERE purpose = 'exit' AND position_id = ?",
                       (pos.position_id,)).fetchone()[0]
    _same(p, "exit order count", pos.exit_orders, n_exit)
    intent = pos.exit_intent
    if intent is not None:
        r = c.execute("SELECT * FROM exit_intents WHERE intent_id = ?", (intent.intent_id,)).fetchone()
        if r is None:
            p.append(f"exit intent {intent.intent_id} not persisted")
        else:
            _same(p, "exit intent position", pos.position_id, r["position_id"])
            _same(p, "exit intent reason", intent.reason, r["reason"])
            _same(p, "exit intent created", intent.created_us, r["created_us"])
            _same(p, "exit intent status", "active", r["status"])
    return p


def _check_order(c, state: EngineState) -> list[str]:
    p: list[str] = []
    o = state.order
    row = c.execute("SELECT * FROM orders WHERE order_id = ?", (o.order_id,)).fetchone()
    for f in ("side", "purpose", "qty", "limit_price", "submitted_us", "ready_us", "reserved_asset",
              "reserved_amount", "reference_quote_id", "candidate_id", "attempt", "signal_us"):
        _same(p, f"order.{f}", getattr(o, f), row[f])
    if o.purpose == "entry":
        cand = c.execute("SELECT * FROM candidates WHERE candidate_id = ?", (o.candidate_id,)).fetchone()
        if cand is None or cand["status"] != "submitted":
            p.append(f"pending entry {o.order_id} has no submitted candidate")
        else:
            _same(p, "order signal close vs candidate", o.signal_close, cand["close"])
            _same(p, "order ATR vs candidate", o.atr, cand["atr"])
            _same(p, "order stop distance vs candidate", o.stop_distance, cand["stop_distance"])
            _same(p, "order qty vs candidate", o.qty, cand["qty"])
            _same(p, "order limit vs candidate", o.limit_price, cand["limit_price"])
            _same(p, "order signal time vs decision", o.signal_us, cand["decision_us"])
    else:
        want = state.position.position_id if state.position is not None else None
        _same(p, "exit order position", want, row["position_id"])
        _same(p, "order.position_id", o.position_id, row["position_id"])
    return p


def _check_risk_marks(c, state: EngineState) -> list[str]:
    p: list[str] = []
    r = state.risk
    if not r.baseline_pending:
        ev = c.execute("SELECT detail FROM risk_events WHERE kind = 'day_baseline_set' ORDER BY id DESC LIMIT 1"
                       ).fetchone()
        if ev is None:
            p.append("day baseline set without a recorded day_baseline_set event")
        else:
            d = json.loads(ev["detail"])
            _same(p, "day baseline", r.day_baseline, d["equity"])
            _same(p, "baseline day", r.day, d["day"])
    return p


def active_rules(store: Storage, state: EngineState, meta: dict | None = None):
    """Exchange rules in force: the active metadata version (public sessions) or the fixture (synthetic)."""
    meta = meta if meta is not None else store.meta()
    if state.metadata_hash is not None:
        text = store.metadata_bundle(state.metadata_hash)
        return None if text is None else parse_metadata(text)
    return parse_metadata(meta["metadata_json"]) if meta.get("metadata_json") else None


def payload_events(store: Storage) -> list[RawEvent]:
    """The committed normalized inputs of a public-data session, parsed exactly as the engine parsed them."""
    out: list[RawEvent] = []
    for seq, line in store.payloads():
        try:
            ev = parse_event(json.loads(line, parse_float=Decimal))
            out.append(RawEvent(seq, ev, None, ev.event_id, type(ev).__name__, ev.recv_us, line))
        except (InputError, ValueError) as exc:
            out.append(RawEvent(seq, None, f"malformed_event: {exc}", f"seq:{seq}", "unknown", None, line))
    return out


def _check_payloads(store: Storage, state: EngineState) -> list[str]:
    n, lo, hi = store.conn.execute("SELECT count(*), coalesce(min(seq), 0), coalesce(max(seq), 0) "
                                   "FROM input_payloads").fetchone()
    if n != state.cursor or hi != state.cursor or (n and lo != 1):
        return [f"input_payloads has {n} rows ({lo}..{hi}); cursor is {state.cursor}"]
    return []


def _check_inputs(c, state: EngineState, events: list[RawEvent]) -> list[str]:
    """Rebuild indicator state and the last observations from the committed inputs and compare."""
    p: list[str] = []
    accepted = {r[0] for r in c.execute("SELECT seq FROM input_log WHERE disposition = 'accepted'")}
    strat = StrategyState()
    last_quote = last_ref = None
    for raw in events[: state.cursor]:
        if raw.seq not in accepted or raw.event is None:
            continue
        ev = raw.event
        if isinstance(ev, CandleEvent):
            end = ev.start_us + MINUTE_US
            strategy_update(strat, Bar(ev.start_us, end, ev.open, ev.high, ev.low, ev.close, ev.recv_us))
        elif isinstance(ev, QuoteEvent):
            last_quote = ev
        elif isinstance(ev, ReferenceEvent):
            last_ref = ev
    if codec.dump(strat) != codec.dump(state.strategy):
        p.append("indicator/strategy state does not match the committed candle inputs")
    q = state.last_quote
    if (q is None) != (last_quote is None) or (q is not None and (
            q.event_id, q.recv_us, q.bid, q.bid_qty, q.ask, q.ask_qty, q.exchange_us) != (
            last_quote.event_id, last_quote.recv_us, last_quote.bid, last_quote.bid_qty, last_quote.ask,
            last_quote.ask_qty, last_quote.exchange_us)):
        p.append("last quote in snapshot does not match the last committed quote input")
    ref = state.reference
    if (ref is None) != (last_ref is None) or (ref is not None and (
            ref.event_id, ref.recv_us, ref.avg_price, ref.mins) != (
            last_ref.event_id, last_ref.recv_us, last_ref.avg_price, last_ref.mins)):
        p.append("reference price in snapshot does not match the last committed reference input")
    return p


def require_reconciled(store: Storage, state: EngineState, **kw) -> None:
    problems = reconcile(store, state, **kw)
    if problems:
        raise ReconciliationError(problems)
