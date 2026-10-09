"""Native fee arithmetic, the PROJECT_PLAN.md hand-check, cost basis and balance invariants."""

from __future__ import annotations

import pytest
from conftest import D

from paperbot.ledger import (
    Balances,
    InvariantViolation,
    Pool,
    add_to_pool,
    allocate,
    buy_amounts,
    remove_from_pool,
    sell_amounts,
)


def test_manual_hand_check_example_finishes_at_999_998501():
    bal = Balances(D(1000), D(0), D(0), D(0))
    pool = Pool(D(0), D(0), D(0))
    buy = buy_amounts(D("0.01"), D(50000), D("0.001"), "BTC")
    assert buy.usdt_debit == D(500)
    assert buy.btc_credit == D("0.00999")
    assert buy.fee_asset == "BTC" and buy.fee_amount == D("0.00001") and buy.fee_usdt == D("0.5")
    bal.apply("USDT", -buy.usdt_debit, D(0))
    bal.apply("BTC", buy.btc_credit, D(0))
    add_to_pool(pool, buy)
    assert bal.usdt_free == D(500)

    sell = sell_amounts(D("0.00999"), D(50100), D("0.001"))
    assert sell.proceeds == D("500.499")
    assert sell.fee_amount == D("0.500499")
    assert sell.usdt_credit == D("499.998501")
    gross, fee = allocate(pool, D("0.00999"))
    bal.apply("BTC", -sell.btc_debit, D(0))
    bal.apply("USDT", sell.usdt_credit, D(0))
    remove_from_pool(pool, D("0.00999"), gross, fee)

    assert bal.usdt_free == D("999.998501")
    assert bal.btc_free == 0 and pool.qty == 0 and pool.basis == 0
    net = sell.usdt_credit - gross - fee
    assert net == D("-0.001499")
    # execution gross is positive (price rose) yet net is negative: fees are counted once, not hidden
    execution_gross = sell.proceeds - gross
    assert execution_gross == D("0.999")
    assert execution_gross - fee - sell.fee_amount == net


def test_quote_asset_buy_fee():
    buy = buy_amounts(D("0.01"), D(50000), D("0.001"), "USDT")
    assert buy.usdt_debit == D("500.5") and buy.btc_credit == D("0.01")
    assert buy.fee_asset == "USDT" and buy.fee_amount == D("0.5") == buy.fee_usdt
    assert buy.gross_cost == D(500) and buy.entry_fee_cost == D("0.5")
    sell = sell_amounts(D("0.01"), D(50100), D("0.001"))
    assert D(1000) - buy.usdt_debit + sell.usdt_credit == D("999.999")


def test_unsupported_fee_asset_rejected():
    with pytest.raises(InvariantViolation):
        buy_amounts(D("0.01"), D(50000), D("0.001"), "BNB")


def test_partial_exit_allocates_basis_proportionally_and_entry_fee_once():
    pool = Pool(D(0), D(0), D(0))
    add_to_pool(pool, buy_amounts(D("0.003"), D(60000), D("0.001"), "BTC"))  # credit 0.002997
    assert pool.qty == D("0.002997") and pool.basis == D(180)
    g1, f1 = allocate(pool, D("0.001"))
    remove_from_pool(pool, D("0.001"), g1, f1)
    g2, f2 = allocate(pool, D("0.00199"))
    remove_from_pool(pool, D("0.00199"), g2, f2)
    dust_g, dust_f = pool.gross_cost, pool.entry_fee_cost
    assert pool.qty == D("0.000007")  # unsellable below step: retained, not deleted
    # entry fee (0.000003 BTC * 60000 = 0.18 USDT) is attributed exactly once across all pieces
    assert f1 + f2 + dust_f == D("0.18")
    assert g1 + g2 + dust_g == D("179.82")
    g3, f3 = allocate(pool, D("0.000007"))
    assert (g3, f3) == (dust_g, dust_f)  # final allocation takes the exact remainder


def test_no_negative_balances_and_no_overselling():
    bal = Balances(D(10), D(0), D("0.001"), D(0))
    with pytest.raises(InvariantViolation):
        bal.apply("USDT", D(-11), D(0))
    with pytest.raises(InvariantViolation):
        bal.apply("BTC", D(0), D("-0.0001"))
    pool = Pool(D("0.001"), D(60), D("0.06"))
    with pytest.raises(InvariantViolation):
        allocate(pool, D("0.0011"))
    with pytest.raises(InvariantViolation):
        remove_from_pool(pool, D("0.001"), D(59), D(0))  # leftover basis on empty pool
