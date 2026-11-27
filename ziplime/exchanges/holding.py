"""A holding used by allocation calculations."""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Holding:
    symbol: str
    quantity: float
    average_price: float
