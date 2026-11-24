"""Accounting utils for engine."""

from types import ModuleType


def _round_cents(xp: ModuleType, value):
    """Python's round(value, 2): the nearer of the two cents around the exact value. numpy's round
    scales by 100 first, and that product can land on the other side of a half cent."""
    low = xp.floor(value * 100.0) / 100.0
    high = (xp.floor(value * 100.0) + 1.0) / 100.0
    return xp.where(value - low < high - value, low, high)


def _cost_basis(xp: ModuleType, basis, held, amount, price, traded, fee, multiplier):
    """Position._update_position and adjust_commission_cost_basis."""
    total = held + amount
    same = (xp.sign(held) == xp.sign(amount)) | (held == 0)
    crossed = ~same & (abs(amount) > abs(held))
    averaged = (basis * held + amount * price) / xp.where(total == 0, 1.0, total)
    updated = xp.where(total == 0, 0.0, xp.where(same, averaged, xp.where(crossed, price, basis)))
    with_fee = xp.where(total != 0, (updated * total + fee / multiplier) / xp.where(total == 0, 1.0, total),
                        updated)
    return xp.where(traded, with_fee, basis)
