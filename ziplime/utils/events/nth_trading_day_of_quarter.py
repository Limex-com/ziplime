"""Nth trading day of quarter for events."""

from ziplime.utils.events.trading_day_of_quarter_rule import TradingDayOfQuarterRule


class NthTradingDayOfQuarter(TradingDayOfQuarterRule):
    """A rule that triggers on the nth trading day of the quarter, zero-indexed."""

    def __init__(self, n: int) -> None:
        super(NthTradingDayOfQuarter, self).__init__(n, invert=False)
