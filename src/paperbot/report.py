"""Read-only status and report built from the SQLite state.

Nothing here writes. The report re-derives every figure from stored fills and
the stored snapshot and checks the accounting identity

    starting cash + realized net + conservative unrealized net
        == cash (free + locked) + conservative liquidation value

without subtracting any fee, spread or slippage a second time: fees are in
the ledger once, and spread/slippage are already inside the fill prices.
"""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal

from .config import config_from_canonical
from .money import ZERO, dtext, exact
from .reconcile import active_rules, reconcile
from .risk import value
from .storage import Storage
from .timeutil import iso, local_iso

LABELS = {
    "SYNTHETIC": ["PAPER", "SYNTHETIC"],
    "PUBLIC": ["PAPER", "PUBLIC DATA", "FORWARD"],
    "PUBLIC_RECORDED_REPLAY": ["PAPER", "PUBLIC DATA", "RECORDED REPLAY"],
}
PUBLIC_DISCLAIMER = (
    "PAPER | PUBLIC DATA: simulated paper fills against recorded public Binance best bid/ask observations. No order "
    "was sent to any exchange. Simulated fills (including fill-time resizing) are not achievable real executions: "
    "no matching engine, queue position, hidden liquidity or production fill guarantee is modeled. Live trading is "
    "not implemented. Results are not evidence of a trading edge."
)
DISCLAIMER = (
    "PAPER | SYNTHETIC: simulated fills on synthetic fixtures. Engineering evidence only; not a backtest, not a "
    "forward trial and not evidence of a trading edge. No matching engine, queue position, hidden liquidity or "
    "production fill guarantee is modeled. Live trading is not implemented."
)


def _rows(store: Storage, sql: str, args: tuple = ()) -> list[dict]:
    return [dict(r) for r in store.conn.execute(sql, args)]


def _sum(rows: list[dict], key: str) -> Decimal:
    return sum((Decimal(r[key]) for r in rows if r.get(key) is not None), ZERO)


def build_report(store: Storage) -> dict:
    """Build the report from one consistent read snapshot (a single read transaction).

    Without it, a report taken while the owner process is committing could mix two commits."""
    conn = store.conn
    conn.execute("BEGIN")
    try:
        return _build_report(store)
    finally:
        conn.execute("COMMIT")


@exact
def _build_report(store: Storage) -> dict:
    meta = store.meta()
    state = store.load_state()
    cfg = config_from_canonical(meta["config_canonical"])
    rules = active_rules(store, state, meta)
    tz = cfg.day_timezone
    problems = reconcile(store, state)
    evidence = meta.get("evidence", "SYNTHETIC")
    provenance = meta.get("data_provenance", "BINANCE_PUBLIC" if evidence != "SYNTHETIC" else "SYNTHETIC")
    labels = list(LABELS.get(evidence, ["PAPER", evidence]))
    disclaimer = DISCLAIMER if evidence == "SYNTHETIC" else PUBLIC_DISCLAIMER
    if evidence != "SYNTHETIC" and provenance != "BINANCE_PUBLIC":
        labels.append("MOCKED")
        disclaimer = ("MOCKED: public-format test data from fake servers/transports, not real market "
                      "observations. " + disclaimer)

    b, pool = state.balances, state.pool
    q = state.last_quote
    quote_age = None if q is None or state.clock_us is None else state.clock_us - q.recv_us
    fresh = q is not None and state.health.quotes_fresh and quote_age <= cfg.quote_max_age_us
    val = value(b, pool, rules, q.bid, slippage=cfg.slippage, sell_fee=cfg.sell_fee,
                sell_cushion=cfg.sell_limit_cushion) if q is not None and rules is not None else None

    fills = _rows(store, "SELECT * FROM fills ORDER BY seq")
    sells = [f for f in fills if f["side"] == "SELL"]
    realized_net = _sum(sells, "net_pnl")
    realized_gross = _sum(sells, "gross_pnl")
    fees_native: dict[str, Decimal] = {}
    for f in fills:
        fees_native[f["fee_asset"]] = fees_native.get(f["fee_asset"], ZERO) + Decimal(f["fee_amount"])
    fees_usdt = _sum(fills, "fee_usdt")

    starting = cfg.starting_usdt
    cash = b.usdt_free + b.usdt_locked
    if val is not None:
        unrealized_net = val.liquidation_value - pool.basis
        unrealized_gross_at_bid = pool.qty * q.bid - pool.gross_cost
        identity_lhs = starting + realized_net + unrealized_net
        identity_rhs = cash + val.liquidation_value
    else:
        unrealized_net = unrealized_gross_at_bid = identity_lhs = identity_rhs = None
    dust_qty = val.dust_qty if val is not None else pool.qty
    dust_basis = pool.basis * dust_qty / pool.qty if pool.qty > 0 else ZERO

    candidates = _rows(store, "SELECT * FROM candidates ORDER BY bar_start_us")
    orders = _rows(store, "SELECT * FROM orders ORDER BY submitted_us, order_id")
    positions = _rows(store, "SELECT * FROM positions ORDER BY opened_us")
    intents = _rows(store, "SELECT * FROM exit_intents ORDER BY created_us")
    risk_events = _rows(store, "SELECT * FROM risk_events ORDER BY id")
    controls = _rows(store, "SELECT * FROM control_events ORDER BY id")
    health = _rows(store, "SELECT * FROM health_events ORDER BY id")
    dispositions = dict(Counter(r["disposition"] for r in _rows(store, "SELECT disposition FROM input_log")))
    rejected_inputs = _rows(store, "SELECT seq, event_id, event_type, detail FROM input_log "
                                   "WHERE disposition != 'accepted' ORDER BY seq")

    unresolved = []
    if state.position is not None:
        unresolved.append(f"open position {state.position.position_id} (stop {dtext(state.position.stop_price)}, "
                          f"target {dtext(state.position.target_price)})")
        if state.position.exit_intent is not None:
            unresolved.append(f"active exit intent: {state.position.exit_intent.reason} "
                              f"(attempts {state.position.exit_intent.attempts}, "
                              f"waiting: {state.position.exit_intent.blocked_reason})")
    if state.order is not None:
        unresolved.append(f"pending {state.order.purpose} order {state.order.order_id}")
    if pool.qty > 0:
        unresolved.append(f"BTC inventory {dtext(pool.qty)} (dust {dtext(dust_qty)}) remains; no closing fill invented")
    if state.risk.latches:
        unresolved.append(f"risk latches active: {state.risk.latches}")
    if not fresh:
        unresolved.append("mark is stale/missing: valuation uncertain, new risk blocked")
    if problems:
        unresolved.append("RECONCILIATION FAILED")

    def ts(us):
        return {"utc": iso(us), "local": local_iso(us, tz)} if us is not None else None

    return {
        "labels": labels,
        "evidence": evidence,
        "data_provenance": provenance,
        "disclaimer": disclaimer,
        "forward": _forward_section(store, state, cfg, rules, health, ts) if evidence != "SYNTHETIC" else None,
        "provenance": {
            "account_id": meta["account_id"], "symbol": meta["symbol"], "strategy": meta["strategy"],
            "schema_version": meta["schema_version"], "config_sha256": meta["config_sha256"],
            "metadata_sha256": meta["metadata_sha256"], "metadata_label": meta["metadata_label"],
            "metadata_retrieved_at": meta["metadata_retrieved_at"], "input_path": meta["input_path"],
            "input_sha256": meta["input_sha256"], "input_events": int(meta["input_events"]),
            "day_timezone": tz, "config": json.loads(meta["config_canonical"]),
        },
        "cursor": {"seq": state.cursor, "clock": ts(state.clock_us), "dispositions": dispositions},
        "reconciliation": {"ok": not problems, "problems": problems},
        "balances": {"USDT": {"free": b.usdt_free, "locked": b.usdt_locked},
                     "BTC": {"free": b.btc_free, "locked": b.btc_locked}},
        "inventory": {
            "btc_total": pool.qty, "sellable_btc": val.sellable_qty if val else None, "dust_btc": dust_qty,
            "basis_usdt": pool.basis, "gross_cost_usdt": pool.gross_cost, "entry_fee_cost_usdt": pool.entry_fee_cost,
            "dust_basis_usdt": dust_basis, "dust_mark_value_usdt": dust_qty * q.bid if q else None,
            "dust_liquidation_value_usdt": ZERO,
        },
        "mark": {
            "quote_id": q.event_id if q else None, "bid": q.bid if q else None, "ask": q.ask if q else None,
            "received": ts(q.recv_us) if q else None, "age_us": quote_age, "fresh": fresh,
            "conservative_equity": val.equity if val else None,
            "liquidation_value": val.liquidation_value if val else None,
            "exposure_usdt": val.exposure if val else None,
        },
        "pnl": {
            "starting_usdt": starting, "realized_net": realized_net, "realized_execution_gross": realized_gross,
            "realized_entry_fees_attributed": _sum(sells, "basis_fee"), "realized_exit_fees": _sum(sells, "fee_amount"),
            "unrealized_net_conservative": unrealized_net, "unrealized_execution_gross_at_bid": unrealized_gross_at_bid,
            "fees_native": fees_native, "fees_usdt_value": fees_usdt,
            "identity": {"lhs": identity_lhs, "rhs": identity_rhs,
                         "holds": identity_lhs == identity_rhs if identity_lhs is not None else None},
        },
        "risk": {
            "latches": state.risk.latches, "day": state.risk.day, "day_baseline": state.risk.day_baseline,
            "baseline_pending": state.risk.baseline_pending, "high_water_mark": state.risk.hwm,
            "last_equity": state.risk.last_equity, "last_mark": ts(state.risk.last_mark_us),
            "events": risk_events, "controls": controls,
        },
        "health": {
            "quotes_fresh": state.health.quotes_fresh, "candle_missing": state.health.candle_missing,
            "unprotected_total_us": state.health.unprotected_total_us
            + (state.clock_us - state.health.unprotected_since_us if state.health.unprotected_since_us else 0),
            "counts": dict(Counter(h["kind"] for h in health)), "events": health,
        },
        "candidates": {
            "total": len(candidates), "submitted": sum(1 for c in candidates if c["status"] == "submitted"),
            "skip_reasons": dict(Counter(c["skip_reason"] for c in candidates if c["status"] == "skipped")),
            "rows": candidates,
        },
        "orders": {
            "total": len(orders), "by_status": dict(Counter(o["status"] for o in orders)),
            "nonfills": [o for o in orders if o["status"] in ("zero", "canceled")],
            "partials": [o for o in orders if o["status"] == "partial"], "rows": orders,
        },
        "fills": fills,
        "positions": positions,
        "exit_intents": intents,
        "rejected_inputs": rejected_inputs,
        "unresolved_risk": unresolved,
    }


def _j(x):
    if isinstance(x, Decimal):
        return dtext(x)
    raise TypeError(type(x).__name__)


def to_json(report: dict) -> str:
    return json.dumps(report, default=_j, indent=2, sort_keys=False)


def _t(us, tz):
    return f"{iso(us)} ({local_iso(us, tz)})" if us is not None else "-"


def _f(x) -> str:
    """Text display; long non-terminating Decimals (EMA/ATR, basis allocations) are shown to 10 places with '~'.

    The JSON report and the database keep every value exactly."""
    if x is None:
        return "-"
    if isinstance(x, str):
        try:
            x = Decimal(x)
        except ArithmeticError:
            return x
    if isinstance(x, Decimal):
        if x.as_tuple().exponent < -12:
            return dtext(x.quantize(Decimal("1e-10"))) + "~"
        return dtext(x)
    return str(x)


def render_status(r: dict) -> str:
    pv, m, p, inv = r["provenance"], r["mark"], r["pnl"], r["inventory"]
    lines = [
        f"{' | '.join(r['labels'])} | {pv['symbol']} | {pv['strategy']} | account {pv['account_id']}",
        f"cursor {r['cursor']['seq']}/{pv['input_events']} · clock "
        f"{r['cursor']['clock']['utc'] if r['cursor']['clock'] else '-'}"
        f" · reconciliation {'OK' if r['reconciliation']['ok'] else 'FAILED'}",
        f"config {pv['config_sha256'][:16]} · input {pv['input_sha256'][:16]} · metadata {pv['metadata_label']} "
        f"{pv['metadata_sha256'][:16]} (retrieved {pv['metadata_retrieved_at']})" if not r.get("forward") else
        f"config {pv['config_sha256'][:16]} · input {pv['input_sha256'][:16]} · metadata {pv['metadata_label']}",
        f"cash {_f(r['balances']['USDT']['free'])} free + {_f(r['balances']['USDT']['locked'])} locked USDT · "
        f"BTC {_f(inv['btc_total'])} (sellable {_f(inv['sellable_btc'])}, dust {_f(inv['dust_btc'])})",
        f"mark bid {_f(m['bid'])} age {_f(m['age_us'])}us {'fresh' if m['fresh'] else 'STALE/UNCERTAIN'} · "
        f"conservative equity {_f(m['conservative_equity'])} · exposure {_f(m['exposure_usdt'])}",
        f"realized net {_f(p['realized_net'])} · unrealized net (conservative) {_f(p['unrealized_net_conservative'])}"
        f" · fees {_f(p['fees_usdt_value'])} USDT value",
        f"latches {r['risk']['latches'] or 'none'}",
    ]
    fw = r.get("forward")
    if fw:
        lines += [
            f"feed: stream {fw['stream']} · quotes {'fresh' if fw['quotes_fresh'] else 'STALE'} (mark age "
            f"{_f(m['age_us'])}us) · last 1m bar {fw['last_bar_start'] or '-'} · candle missing {fw['candle_missing']}",
            f"entry blocks: {', '.join(fw['blocks']) or 'none'} · recovery signaled {fw['recovery_signaled']} · "
            f"metadata {fw['metadata_sha256'][:12] if fw['metadata_sha256'] else 'NONE'} age {fw['metadata_age_s']}s",
            f"queued exit: {fw['queued_exit'] or 'none'} · pending order: {fw['pending_order'] or 'none'} · "
            f"unprotected exposure (cumulative) {fw['unprotected_total_s']}s · outbox {fw['outbox'] or '{}'}",
        ]
    for u in r["unresolved_risk"]:
        lines.append(f"UNRESOLVED: {u}")
    return "\n".join(lines)


def render_positions(r: dict) -> str:
    inv, m = r["inventory"], r["mark"]
    clock = r["cursor"]["clock"]["utc"] if r["cursor"]["clock"] else "-"
    out = [f"{' | '.join(r['labels'])} | positions | clock {clock}"]
    open_pos = [p for p in r["positions"] if p["status"] == "open"]
    for p in open_pos or []:
        out.append(f"OPEN {p['position_id'].split('|')[-2]} entry {p['entry_price']} qty {p['entry_qty']} "
                   f"stop {p['stop_price']} target {p['target_price']} opened {p['opened_us']}")
    if not open_pos:
        out.append("no open position")
    fw = r.get("forward") or {}
    out.append(f"queued exit: {fw.get('queued_exit') or 'none'} · pending order: {fw.get('pending_order') or 'none'}")
    out.append(f"inventory {_f(inv['btc_total'])} BTC (sellable {_f(inv['sellable_btc'])}, dust {_f(inv['dust_btc'])}, "
               f"dust basis {_f(inv['dust_basis_usdt'])}) · bid {_f(m['bid'])} age {_f(m['age_us'])}us "
               f"{'fresh' if m['fresh'] else 'STALE/UNCERTAIN'} · liquidation value {_f(m['liquidation_value'])}")
    for u in r["unresolved_risk"]:
        out.append(f"UNRESOLVED: {u}")
    return "\n".join(out)


def _forward_section(store: Storage, state, cfg, rules, health_rows: list[dict], ts) -> dict:
    last_ws = [h for h in health_rows if h["kind"] in ("feed_ws_connected", "feed_ws_disconnected")]
    stream = "unknown"
    if last_ws:
        stream = "connected" if last_ws[-1]["kind"] == "feed_ws_connected" else "DISCONNECTED"
    pos = state.position
    intent = pos.exit_intent if pos is not None else None
    sessions = _rows(store, "SELECT * FROM sessions ORDER BY started_wall") if store.has_table("sessions") else []
    outbox = dict(Counter(r["status"] for r in _rows(store, "SELECT status FROM outbox"))) \
        if store.has_table("outbox") else {}
    now = state.clock_us
    return {
        "stream": stream,
        "quotes_fresh": state.health.quotes_fresh,
        "candle_missing": state.health.candle_missing,
        "last_bar_start": iso(state.strategy.last_start_us),
        "warm_five_minute_bars": state.strategy.five_count,
        "blocks": list(state.health.blocks),
        "recovery_signaled": state.health.recovery_signaled,
        "metadata_sha256": state.metadata_hash,
        "metadata_age_s": None if state.metadata_fetched_us is None or now is None
        else (now - state.metadata_fetched_us) // 1_000_000,
        "metadata_status": rules.status if rules is not None else None,
        "execution_rules": [dict(r) for r in rules.execution_rules] if rules is not None else [],
        "reference_price": None if state.ref_price is None else {
            "value": state.ref_price.value, "received": ts(state.ref_price.recv_us)},
        "queued_exit": None if intent is None else f"{intent.reason} (attempts {intent.attempts}, waiting: "
                                                   f"{intent.blocked_reason or 'no'})",
        "pending_order": None if state.order is None else f"{state.order.purpose} {state.order.side} "
                                                           f"{state.order.qty} @ {state.order.limit_price}",
        "unprotected_total_s": (state.health.unprotected_total_us
                                + (now - state.health.unprotected_since_us if state.health.unprotected_since_us
                                   else 0)) // 1_000_000,
        "sessions": sessions,
        "outbox": outbox,
        "manifest": store.manifest(),
    }


def render_report(r: dict) -> str:
    tz = r["provenance"]["day_timezone"]
    pv, p = r["provenance"], r["pnl"]
    out = [render_status(r), "", r["disclaimer"], "", "== Provenance =="]
    for k in ("account_id", "schema_version", "config_sha256", "metadata_sha256", "metadata_label",
              "metadata_retrieved_at", "input_path", "input_sha256", "input_events"):
        out.append(f"  {k}: {pv[k]}")
    out.append(f"  config: {json.dumps(pv['config'], sort_keys=True)}")
    out.append(f"  input dispositions: {r['cursor']['dispositions']}")
    out += ["", "== P&L (USDT) =="]
    for k in ("starting_usdt", "realized_net", "realized_execution_gross", "realized_entry_fees_attributed",
              "realized_exit_fees", "unrealized_net_conservative", "unrealized_execution_gross_at_bid",
              "fees_usdt_value"):
        out.append(f"  {k}: {_f(p[k])}")
    out.append(f"  fees by native asset: { {k: dtext(v) for k, v in p['fees_native'].items()} }")
    ident = p["identity"]
    out.append(f"  identity starting+realized+unrealized = cash+liquidation: {_f(ident['lhs'])} = {_f(ident['rhs'])}"
               f" -> {ident['holds']}")
    inv = r["inventory"]
    out.append(f"  inventory: total {_f(inv['btc_total'])} BTC, basis {_f(inv['basis_usdt'])}, "
               f"dust {_f(inv['dust_btc'])}"
               f" BTC (basis {_f(inv['dust_basis_usdt'])}, bid mark {_f(inv['dust_mark_value_usdt'])}, "
               "liquidation value 0)")
    out += ["", f"== Candidates ({r['candidates']['total']}, submitted {r['candidates']['submitted']}) =="]
    out.append(f"  skip reasons: {r['candidates']['skip_reasons']}")
    for c in r["candidates"]["rows"]:
        if c["status"] == "submitted" or not c["skip_reason"].startswith("warmup"):
            out.append(f"  {_t(c['bar_start_us'], tz)} {c['status']:9} {c['skip_reason'] or ''} close={c['close']} "
                       f"H={c['h']} atr={_f(c['atr'])} qty={c['qty']} limit={c['limit_price']}")
    out += ["", f"== Orders {r['orders']['by_status']} =="]
    for o in r["orders"]["rows"]:
        out.append(
            f"  {o['order_id'].split('|')[-2:]} {o['side']} {o['purpose']} qty={o['qty']} limit={o['limit_price']} "
            f"status={o['status']} filled={o['filled_qty']} reason={o['outcome_reason'] or '-'} "
            f"exit_reason={o['exit_reason'] or '-'}")
        out.append(f"      signal {_t(o['signal_us'], tz)} | submitted {_t(o['submitted_us'], tz)} | ready "
                   f"{_t(o['ready_us'], tz)} | closed {_t(o['closed_us'], tz)}")
    out += ["", "== Fills =="]
    for f in r["fills"]:
        mid = (Decimal(f["bid"]) + Decimal(f["ask"])) / 2
        out.append(f"  {f['side']} {f['qty']} @ {f['price']} (bid {f['bid']} / ask {f['ask']}, mid {dtext(mid)}) "
                   f"fee {f['fee_amount']} {f['fee_asset']} (={f['fee_usdt']} USDT) usdt {f['usdt_delta']} "
                   f"btc {f['btc_delta']} gross_pnl {_f(f['gross_pnl'])} net_pnl {_f(f['net_pnl'])}")
        out.append(f"      quote recv {_t(f['quote_recv_us'], tz)} exchange_time "
                   f"{iso(f['quote_exchange_us']) or 'not supplied'} | fill {_t(f['fill_us'], tz)}"
                   f" | commit {_t(f['committed_us'], tz)}")
    out += ["", "== Positions =="]
    for pos in r["positions"]:
        out.append(f"  {pos['position_id'].split('|')[-2]} entry {pos['entry_price']} qty {pos['entry_qty']} "
                   f"stop {pos['stop_price']} target {pos['target_price']} status {pos['status']} "
                   f"exit {pos['exit_reason'] or '-'} residual {pos['residual_qty'] or '-'} "
                   f"opened {_t(pos['opened_us'], tz)} closed {_t(pos['closed_us'], tz)}")
    out += ["", "== Risk =="]
    rk = r["risk"]
    out.append(f"  latches {rk['latches'] or 'none'} · day {rk['day']} baseline {_f(rk['day_baseline'])} "
               f"(pending {rk['baseline_pending']}) · HWM {_f(rk['high_water_mark'])}")
    for e in rk["events"]:
        out.append(f"  {_t(e['ts_us'], tz)} {e['kind']} {e['detail']}")
    for c in rk["controls"]:
        out.append(f"  CONTROL {c['kind']} {c['latch']} at cursor {c['at_cursor']} wall {c['wall_utc']}: {c['reason']}")
    out += ["", "== Health =="]
    out.append(f"  counts {r['health']['counts']} · unprotected exposure {r['health']['unprotected_total_us']}us")
    for h in r["health"]["events"]:
        if h["kind"] not in ("quotes_stale", "quotes_fresh"):
            out.append(f"  {_t(h['ts_us'], tz)} {h['kind']} {h['detail']}")
    if r["rejected_inputs"]:
        out += ["", "== Rejected/duplicate inputs =="]
        for x in r["rejected_inputs"]:
            out.append(f"  seq {x['seq']} {x['event_type']} {x['event_id']}: {x['detail']}")
    out += ["", "== Unresolved risk =="] + [f"  {u}" for u in (r["unresolved_risk"] or ["none"])]
    if not r["reconciliation"]["ok"]:
        out += ["", "== RECONCILIATION PROBLEMS =="] + [f"  {x}" for x in r["reconciliation"]["problems"]]
    return "\n".join(out)
