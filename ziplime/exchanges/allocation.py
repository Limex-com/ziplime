"""Capital allocation and lot sizing for a live strategy on a brokerage account.

A live deployment is given an allocation (its ``total_cash``), but the account
it trades can hold far more. Seeded with the whole account, the ledger makes
``order_target_percent(0.5)`` mean half of the account: a deployment allocated
$50,000 bought 443 NVDA (about $101,000), the entire demo account.

So a live exchange with an allocation reports the account's positions as they
are, and cash as the allocation minus what those positions cost. Portfolio
value is then the allocation plus the unrealized P&L of the positions, and the
strategy sizes its orders against that.

Pure logic, free of broker and engine imports, so both are unit-testable alone.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Holding:
    symbol: str
    quantity: float
    average_price: float


def allocated_cash(*, capital: float, holdings: Iterable[Holding]) -> float:
    """Cash the strategy may use: its allocation minus what the positions cost.

    Deliberately not capped by the broker's cash. On a margin account that can be
    negative, which turned the strategy's equity negative and made it sell short;
    whether the money is there is the broker's check to make. Negative here is
    legitimate: the positions cost more than the allocation, so the strategy
    sells down toward its targets.
    """
    return capital - sum(h.quantity * h.average_price for h in holdings)


def round_to_lots(quantity: float, lot_size: float) -> float:
    """Round toward zero to whole lots, keeping the sign (MOEX trades GAZP in lots of 10)."""
    if lot_size <= 0:
        return quantity
    lots = int(abs(quantity) // lot_size)
    return (lots * lot_size) * (1 if quantity >= 0 else -1)
