"""Decimal helpers.

Every price, quantity, balance, fee and rate is a ``Decimal``. Binary floats are
rejected at every input boundary. Arithmetic that can be inexact (EMA, ATR,
proportional cost-basis allocation) runs under one fixed context so replays are
bit-for-bit reproducible.
"""

from __future__ import annotations

import decimal
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from functools import wraps

CTX = decimal.Context(
    prec=50,
    rounding=ROUND_HALF_EVEN,
    Emin=-999999,
    Emax=999999,
    traps=[decimal.InvalidOperation, decimal.DivisionByZero, decimal.Overflow],
)

ZERO = Decimal(0)
ONE = Decimal(1)
BPS = Decimal("0.0001")
# Proportional cost-basis allocations are quantized to this step. The final
# allocation of a pool always takes the exact remainder, so nothing leaks.
ALLOC_QUANTUM = Decimal("1e-18")


def exact(fn):
    """Run ``fn`` under the fixed project Decimal context."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        with decimal.localcontext(CTX):
            return fn(*args, **kwargs)

    return wrapper


def to_dec(value: object, field: str = "value") -> Decimal:
    """Parse a decimal from str/int/Decimal. Floats and bools are rejected."""
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError(f"{field}: use a decimal string, not {type(value).__name__} {value!r}")
    if isinstance(value, Decimal):
        d = value
    elif isinstance(value, (int, str)):
        try:
            d = Decimal(value)
        except decimal.InvalidOperation as exc:
            raise ValueError(f"{field}: invalid decimal {value!r}") from exc
    else:
        raise TypeError(f"{field}: unsupported type {type(value).__name__}")
    if not d.is_finite():
        raise ValueError(f"{field}: must be finite, got {value!r}")
    return d


def dtext(d: Decimal) -> str:
    """Canonical plain-text form for storage and reports (no exponent)."""
    if d == 0:
        return "0"
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def floor_to(value: Decimal, increment: Decimal) -> Decimal:
    """Round down to a multiple of ``increment``. A zero increment is a disabled rule: no rounding."""
    if increment < 0:
        raise ValueError("increment must be >= 0")
    if increment == 0:
        return value
    with decimal.localcontext(CTX):
        return (value / increment).to_integral_value(rounding=ROUND_FLOOR) * increment


def ceil_to(value: Decimal, increment: Decimal) -> Decimal:
    """Round up to a multiple of ``increment``. A zero increment is a disabled rule: no rounding."""
    if increment < 0:
        raise ValueError("increment must be >= 0")
    if increment == 0:
        return value
    with decimal.localcontext(CTX):
        return (value / increment).to_integral_value(rounding=ROUND_CEILING) * increment


def is_multiple(value: Decimal, increment: Decimal) -> bool:
    """True when ``value`` is a multiple of ``increment``; a zero increment never fails."""
    if increment == 0:
        return True
    with decimal.localcontext(CTX):
        q = value / increment
        return q == q.to_integral_value()


def quantize_alloc(value: Decimal) -> Decimal:
    with decimal.localcontext(CTX):
        return value.quantize(ALLOC_QUANTUM, rounding=ROUND_HALF_EVEN)
