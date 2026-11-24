"""Nth trading day of month for events."""

from ziplime.utils.events.trading_day_of_month_rule import TradingDayOfMonthRule


class NthTradingDayOfMonth(TradingDayOfMonthRule):
    """
    A rule that triggers on the nth trading day of the month.
    This is zero-indexed, n=0 is the first trading day of the month.
    """

    def __init__(self, n: int) -> None:
        super(NthTradingDayOfMonth, self).__init__(n, invert=False)
