"""Startup reconciliation. Any problem halts; nothing here repairs or resets state."""

from __future__ import annotations

import json
from decimal import Decimal

from .engine import EngineState
from .money import ZERO, exact
from .storage import Storage


class ReconciliationError(RuntimeError):
    def __init__(self, problems: list[str]):
        super().__init__("state reconciliation failed:\n  - " + "\n  - ".join(problems))
        self.problems = problems


def _d(v: object) -> Decimal:
    return Decimal(v) if v is not None else ZERO


@exact
def reconcile(store: Storage, state: EngineState, *, config_sha: str | None = None,
              metadata_sha: str | None = None, input_sha: str | None = None) -> list[str]:
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

    # Position.
    open_pos = [r["position_id"] for r in c.execute("SELECT position_id FROM positions WHERE status = 'open'")]
    want_pos = [state.position.position_id] if state.position is not None else []
    if open_pos != want_pos:
        p.append(f"open positions {open_pos} != snapshot {want_pos}")

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


def require_reconciled(store: Storage, state: EngineState, **kw) -> None:
    problems = reconcile(store, state, **kw)
    if problems:
        raise ReconciliationError(problems)
