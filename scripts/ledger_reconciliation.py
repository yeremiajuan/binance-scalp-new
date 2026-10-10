"""Independent reconciliation of a PAPER state database (Markdown output).

It recomputes every fill from its recorded quote and the stored configuration,
using only the formulas in PROJECT_PLAN.md, not the engine code paths:

  buy price  = ceil_tick(ask * (1 + slippage)),   must be <= submitted limit
  sell price = floor_tick(bid * (1 - slippage)),  must be >= submitted limit
  base-fee buy:   USDT -q*p, BTC +q*(1-f), fee q*f BTC
  quote-fee buy:  USDT -q*p*(1+f), BTC +q, fee q*p*f USDT
  sell:           BTC -q, USDT +q*p*(1-f), fee q*p*f USDT

Then it rebuilds the balances from the starting values plus those deltas and
checks them against the stored balances, the ledger and the inventory, and
re-derives realized/unrealized P&L. It also replays the PROJECT_PLAN.md
hand-check example (must finish at 999.998501 USDT, zero BTC).

Usage: python scripts/ledger_reconciliation.py STATE.sqlite
"""

from __future__ import annotations

import decimal
import json
import sqlite3
import sys
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

decimal.getcontext().prec = 50
BPS = Decimal("0.0001")


def step_round(x: Decimal, inc: Decimal, mode) -> Decimal:
    return (x / inc).to_integral_value(rounding=mode) * inc


def main(path: str) -> int:
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    meta = {r["key"]: r["value"] for r in conn.execute("SELECT * FROM meta")}
    cfg = json.loads(meta["config_canonical"])
    md = json.loads(meta["metadata_json"])
    filters = {f["filterType"]: f for f in md["symbol"]["filters"]}
    tick = Decimal(filters["PRICE_FILTER"]["tickSize"])
    slip = Decimal(cfg["execution"]["slippage_bps"]) * BPS
    buy_fee = Decimal(cfg["fees"]["buy_fee_bps"]) * BPS
    sell_fee = Decimal(cfg["fees"]["sell_fee_bps"]) * BPS
    fee_asset = cfg["fees"]["buy_fee_asset"]
    usdt = Decimal(cfg["account"]["starting_usdt"])
    btc = Decimal(cfg["account"]["starting_btc"])
    problems: list[str] = []
    out = [
        "# PAPER | SYNTHETIC ledger reconciliation",
        "",
        f"State `{path}` · account `{meta['account_id']}` · config `{meta['config_sha256'][:16]}` · "
        f"input `{meta['input_sha256'][:16]}` · metadata {meta['metadata_label']} `{meta['metadata_sha256'][:16]}`",
        "",
        f"Assumptions from the stored configuration: buy fee {buy_fee} ({fee_asset}), sell fee {sell_fee} (USDT), "
        f"slippage {slip} per side, tick {tick}. Spread is paid by crossing bid/ask and is not charged again.",
        "",
        f"Start: {usdt} USDT, {btc} BTC.",
        "",
        "| # | side | qty | quote bid/ask | recomputed price | limit | fee (native) | USDT delta | BTC delta "
        "| running USDT | running BTC | check |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    fills = conn.execute(
        "SELECT f.*, o.limit_price, o.purpose FROM fills f JOIN orders o USING(order_id) ORDER BY f.seq").fetchall()
    fees: dict[str, Decimal] = {}
    for i, f in enumerate(fills, 1):
        q = Decimal(f["qty"])
        bid, ask, limit = Decimal(f["bid"]), Decimal(f["ask"]), Decimal(f["limit_price"])
        if f["side"] == "BUY":
            p = step_round(ask * (1 + slip), tick, ROUND_CEILING)
            ok_limit = p <= limit
            if fee_asset == "BTC":
                fee = q * buy_fee
                du, db, fa = -(q * p), q - fee, "BTC"
            else:
                fee = q * p * buy_fee
                du, db, fa = -(q * p + fee), q, "USDT"
        else:
            p = step_round(bid * (1 - slip), tick, ROUND_FLOOR)
            ok_limit = p >= limit
            fee = q * p * sell_fee
            du, db, fa = q * p - fee, -q, "USDT"
        usdt += du
        btc += db
        fees[fa] = fees.get(fa, Decimal(0)) + fee
        stored = (Decimal(f["price"]), Decimal(f["fee_amount"]), f["fee_asset"], Decimal(f["usdt_delta"]),
                  Decimal(f["btc_delta"]))
        ok = stored == (p, fee, fa, du, db) and ok_limit and usdt >= 0 and btc >= 0
        if not ok:
            problems.append(f"fill {f['fill_id']}: stored {stored} vs recomputed {(p, fee, fa, du, db)}")
        out.append(f"| {i} | {f['side']} {f['purpose']} | {q} | {bid}/{ask} | {p} | {limit} | {fee} {fa} | {du} | {db} "
                   f"| {usdt} | {btc} | {'OK' if ok else 'MISMATCH'} |")

    bal = {r["asset"]: (Decimal(r["free"]), Decimal(r["locked"])) for r in conn.execute("SELECT * FROM balances")}
    led = {a: [Decimal(0), Decimal(0)] for a in ("USDT", "BTC")}
    for r in conn.execute("SELECT asset, free_delta, locked_delta FROM ledger"):
        led[r["asset"]][0] += Decimal(r["free_delta"])
        led[r["asset"]][1] += Decimal(r["locked_delta"])
    state = json.loads(conn.execute("SELECT json FROM engine_state").fetchone()[0])
    pool_qty = Decimal(state["pool"]["qty"]["$d"])
    gross_cost = Decimal(state["pool"]["gross_cost"]["$d"])
    entry_fee_cost = Decimal(state["pool"]["entry_fee_cost"]["$d"])
    checks = [
        ("recomputed USDT == stored USDT free+locked", usdt == sum(bal["USDT"])),
        ("recomputed BTC == stored BTC free+locked", btc == sum(bal["BTC"])),
        ("ledger sums == stored balances (free, locked)", all(tuple(led[a]) == bal[a] for a in bal)),
        ("BTC balance == inventory pool (tradable + dust)", btc == pool_qty),
        ("no negative balance", all(v >= 0 for pair in bal.values() for v in pair)),
    ]

    # P&L from fills, independent of the engine's stored P&L columns.
    sells = [f for f in fills if f["side"] == "SELL"]
    realized = sum((Decimal(f["usdt_delta"]) - Decimal(f["basis_gross"]) - Decimal(f["basis_fee"]) for f in sells),
                   Decimal(0))
    stored_realized = sum((Decimal(f["net_pnl"]) for f in sells), Decimal(0))
    buys_basis = sum((Decimal(f["basis_gross"]) + Decimal(f["basis_fee"]) for f in fills if f["side"] == "BUY"),
                     Decimal(0))
    buys_cash = sum((-Decimal(f["usdt_delta"]) for f in fills if f["side"] == "BUY"), Decimal(0))
    sold_basis = sum((Decimal(f["basis_gross"]) + Decimal(f["basis_fee"]) for f in sells), Decimal(0))
    checks += [
        ("realized net recomputed == stored", realized == stored_realized),
        ("entry basis == USDT paid for buys (entry fee attributed once)", buys_basis == buys_cash),
        ("remaining basis == buy basis - sold basis", gross_cost + entry_fee_cost == buys_basis - sold_basis),
    ]
    start = Decimal(cfg["account"]["starting_usdt"])
    checks.append(("cash == start - buy cash + sell proceeds",
                   sum(bal["USDT"]) == start - buys_cash + sum((Decimal(f["usdt_delta"]) for f in sells),
                                                               Decimal(0))))
    out += ["", "## Balances and invariants", ""]
    out.append(f"- Final USDT {sum(bal['USDT'])} (free {bal['USDT'][0]}, locked {bal['USDT'][1]}); "
               f"final BTC {sum(bal['BTC'])} (free {bal['BTC'][0]}, locked {bal['BTC'][1]}).")
    out.append(f"- Fees by native asset: { {k: str(v) for k, v in fees.items()} }.")
    out.append(f"- Realized net P&L {realized}; inventory basis {gross_cost + entry_fee_cost} for {pool_qty} BTC.")
    for name, ok in checks:
        out.append(f"- [{'x' if ok else ' '}] {name}")
        if not ok:
            problems.append(name)

    # PROJECT_PLAN.md hand-check example.
    cash = Decimal(1000) - Decimal("0.01") * Decimal(50000)
    credited = Decimal("0.01") * (1 - Decimal("0.001"))
    proceeds = credited * Decimal(50100)
    cash += proceeds - proceeds * Decimal("0.001")
    ok = cash == Decimal("999.998501")  # all credited BTC is sold, so BTC ends at exactly zero
    out += ["", "## PROJECT_PLAN.md hand-check", "",
            f"Buy 0.01 BTC @ 50,000 with a 0.1% BTC fee, sell {credited} BTC @ 50,100 with a 0.1% USDT fee: "
            f"final cash {cash} USDT, BTC 0, net {cash - 1000} -> {'OK' if ok else 'MISMATCH'}"]
    if not ok:
        problems.append("hand-check example")
    out += ["", "RESULT: " + ("RECONCILED" if not problems else "PROBLEMS: " + "; ".join(problems))]
    print("\n".join(out))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
