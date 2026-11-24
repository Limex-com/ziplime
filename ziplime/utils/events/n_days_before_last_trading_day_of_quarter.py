"""N days before last trading day of quarter for events."""

from ziplime.utils.events.trading_day_of_quarter_rule import TradingDayOfQuarterRule


class NDaysBeforeLastTradingDayOfQuarter(TradingDayOfQuarterRule):
    """A rule that triggers n trading days before the last trading day of the quarter."""

    def __init__(self, n: int) -> None:
        super(NDaysBeforeLastTradingDayOfQuarter, self).__init__(n, invert=True)
