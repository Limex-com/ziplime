"""Nth trading day of week for events."""

from ziplime.utils.events.trading_day_of_week_rule import TradingDayOfWeekRule


class NthTradingDayOfWeek(TradingDayOfWeekRule):
    """
    A rule that triggers on the nth trading day of the week.
    This is zero-indexed, n=0 is the first trading day of the week.
    """

    def __init__(self, n: int) -> None:
        super(NthTradingDayOfWeek, self).__init__(n, invert=False)
