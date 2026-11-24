"""Dividend for market."""

import dataclasses


@dataclasses.dataclass
class Dividend:
    column: int
    ex_bar: int
    pay_bar: int
    amount: float
