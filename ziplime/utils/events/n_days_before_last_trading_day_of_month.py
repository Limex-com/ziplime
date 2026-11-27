"""N days before last trading day of month for events."""

from ziplime.utils.events.trading_day_of_month_rule import TradingDayOfMonthRule


class NDaysBeforeLastTradingDayOfMonth(TradingDayOfMonthRule):
    """
    A rule that triggers n days before the last trading day of the month.
    """

    def __init__(self, n: int) -> None:
        super(NDaysBeforeLastTradingDayOfMonth, self).__init__(n, invert=True)
