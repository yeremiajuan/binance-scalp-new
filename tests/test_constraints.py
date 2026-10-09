"""Exchange-filter validation after rounding, disabled rules, references and applicability."""

from __future__ import annotations

import json

import pytest
from conftest import FULL_METADATA, D, minimal_metadata

from paperbot.constraints import MetadataError, Reference, load_metadata, parse_metadata, validate_limit_order
from paperbot.money import ceil_to, floor_to

NOW = 1_000_000_000


def rules(**kw):
    return parse_metadata(json.dumps(minimal_metadata(**kw)))


def v(r, side, price, qty, ref=None, open_orders=0, pos="0"):
    return validate_limit_order(r, side, D(price), D(qty), open_orders=open_orders, base_position=D(pos),
                                reference=ref, now_us=NOW, reference_max_age_us=60_000_000)


def test_min_notional_boundary_after_rounding():
    r = rules()
    assert v(r, "BUY", "50000.00", "0.0001").ok  # notional exactly 5
    res = v(r, "BUY", "49999.99", "0.0001")  # 4.999999
    assert not res.ok and res.first_failure() == "NOTIONAL:minNotional"
    # Raw size 0.000109 floors to 0.00010 (never rounded up to satisfy a minimum)
    q = floor_to(D("0.000109"), r.step)
    assert q == D("0.00010")
    assert not v(r, "BUY", "49000", q).ok


def test_tick_and_step_multiples_checked_after_rounding():
    r = rules()
    assert not v(r, "BUY", "60000.005", "0.001").ok
    assert v(r, "BUY", floor_to(D("60000.005"), r.tick), "0.001").ok
    assert not v(r, "SELL", "60000", "0.0010001").ok
    assert v(r, "SELL", ceil_to(D("59999.991"), r.tick), floor_to(D("0.0010001"), r.step)).ok


def test_min_max_qty_and_price_bounds():
    r = rules()
    assert v(r, "SELL", "60000", "0.00001").checks  # min qty exactly
    assert v(r, "BUY", "1000000", "0.00001").ok
    assert v(r, "BUY", "1000000.01", "0.00001").first_failure() == "PRICE_FILTER:maxPrice"
    assert v(r, "BUY", "60000", "9000.00001").first_failure() == "LOT_SIZE:maxQty"


def test_zero_increments_and_limits_are_disabled_without_division_by_zero():
    meta = minimal_metadata()
    meta["symbol"]["filters"][0] = {"filterType": "PRICE_FILTER", "minPrice": "0", "maxPrice": "0", "tickSize": "0"}
    meta["symbol"]["filters"][1] = {"filterType": "LOT_SIZE", "minQty": "0", "maxQty": "0", "stepSize": "0"}
    r = parse_metadata(json.dumps(meta))
    res = v(r, "BUY", "60000.123456789", "0.000123456789")
    assert res.ok
    statuses = {(c.filter, c.rule): c.status for c in res.checks}
    assert statuses[("PRICE_FILTER", "tickSize")] == "disabled"
    assert statuses[("LOT_SIZE", "stepSize")] == "disabled"
    assert floor_to(D("1.234"), r.step) == D("1.234")  # zero increment => no rounding, no ZeroDivision


def test_market_lot_size_is_inapplicable_to_limit_ioc():
    r = rules()
    res = v(r, "BUY", "60000", "0.001")
    mls = [c for c in res.checks if c.filter == "MARKET_LOT_SIZE"]
    assert mls and mls[0].status == "inapplicable"
    # MARKET_LOT_SIZE stepSize 0 / minQty 0 never blocks or divides


def test_percent_price_by_side_requires_fresh_matching_reference():
    r = load_metadata(FULL_METADATA)
    assert v(r, "BUY", "60000", "0.001").first_failure() == "PERCENT_PRICE_BY_SIDE:reference"
    stale = Reference(D(60000), 5, NOW - 61_000_000, "r1")
    assert v(r, "BUY", "60000", "0.001", ref=stale).first_failure() == "PERCENT_PRICE_BY_SIDE:reference"
    wrong_window = Reference(D(60000), 1, NOW, "r2")
    assert v(r, "BUY", "60000", "0.001", ref=wrong_window).first_failure() == "PERCENT_PRICE_BY_SIDE:reference"
    ok = Reference(D(60000), 5, NOW - 1_000_000, "r3")
    assert v(r, "BUY", "60000", "0.001", ref=ok).ok
    # bid side multipliers (5 / 0.2) bound BUY prices; ask side bounds SELL prices
    assert v(r, "BUY", "300000.01", "0.0001", ref=ok).first_failure() == "PERCENT_PRICE_BY_SIDE:multiplierUp"
    assert v(r, "SELL", "11999.99", "0.001", ref=ok).first_failure() == "PERCENT_PRICE_BY_SIDE:multiplierDown"
    # inapplicable filters are documented, not silently ignored
    statuses = {c.filter: c.status for c in v(r, "BUY", "60000", "0.001", ref=ok).checks}
    assert statuses["TRAILING_DELTA"] == statuses["ICEBERG_PARTS"] == statuses["MAX_NUM_ALGO_ORDERS"] == "inapplicable"


def test_unknown_filter_blocks_and_symbol_status_and_order_type_checked():
    meta = minimal_metadata()
    meta["symbol"]["filters"].append({"filterType": "SOME_NEW_FILTER", "x": "1"})
    res = v(parse_metadata(json.dumps(meta)), "BUY", "60000", "0.001")
    assert res.first_failure() == "SOME_NEW_FILTER:unknown_filter"
    assert v(rules(symbol={"status": "BREAK"}), "BUY", "60000", "0.001").first_failure() == "SYMBOL:status"
    assert v(rules(symbol={"orderTypes": ["MARKET"]}), "BUY", "60000", "0.001").first_failure() == "SYMBOL:order_type"
    assert not v(rules(symbol={"isSpotTradingAllowed": False}), "BUY", "60000", "0.001").ok


def test_order_and_position_limits():
    meta = minimal_metadata()
    meta["symbol"]["filters"] += [{"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 1},
                                  {"filterType": "MAX_POSITION", "maxPosition": "0.01"}]
    r = parse_metadata(json.dumps(meta))
    assert v(r, "BUY", "60000", "0.001", open_orders=0).ok
    assert v(r, "BUY", "60000", "0.001", open_orders=1).first_failure() == "MAX_NUM_ORDERS:maxNumOrders"
    assert v(r, "BUY", "60000", "0.001", pos="0.0095").first_failure() == "MAX_POSITION:maxPosition"
    assert v(r, "SELL", "60000", "0.001", pos="0.0095").ok


def test_metadata_must_be_dated_and_labeled():
    meta = minimal_metadata()
    del meta["retrieved_at"]
    with pytest.raises(MetadataError):
        parse_metadata(json.dumps(meta))
    meta = minimal_metadata(label="PERMANENT_DEFAULT")
    with pytest.raises(MetadataError):
        parse_metadata(json.dumps(meta))
    assert load_metadata(FULL_METADATA).label == "SYNTHETIC"
