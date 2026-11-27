"""N days before last trading day of week for events."""

from ziplime.utils.events.trading_day_of_week_rule import TradingDayOfWeekRule


class NDaysBeforeLastTradingDayOfWeek(TradingDayOfWeekRule):
    """
    A rule that triggers n days before the last trading day of the week.
    """

    def __init__(self, n: int) -> None:
        super(NDaysBeforeLastTradingDayOfWeek, self).__init__(n, invert=True)
