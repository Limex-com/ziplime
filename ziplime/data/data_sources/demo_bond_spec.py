"""Specification of one bond in the demo universe."""

import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class DemoBondSpec:
    """The terms of one demo issue, and what it is there to demonstrate."""

    ticker: str
    name: str
    description: str
    issue_date: datetime.date
    maturity_date: datetime.date
    face_value: float = 1000.0
    coupon_rate: float = 0.0
    coupon_frequency: int = 0
    #: ``[(date, principal repaid per bond), ...]``, oldest first.
    amortizations: tuple[tuple[datetime.date, float], ...] = ()
    #: Date of a put window, if the issue has one.
    offer_date: datetime.date | None = None
    #: Clean price the generated series starts at, as a percentage of face value.
    initial_price: float = 100.0
