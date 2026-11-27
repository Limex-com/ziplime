"""Trading day of quarter rule for events."""

from abc import ABCMeta
import datetime
import numpy as np
import pandas as pd
from ziplime.utils.events.support import MAX_QUARTER_RANGE
from ziplime.utils.events.stateless_rule import StatelessRule
from ziplime.utils.events.utils import _out_of_range_error
from ziplime.utils.events.utils import _per_calendar
from ziplime.utils.events.utils import lossless_float_to_int


class TradingDayOfQuarterRule(StatelessRule, metaclass=ABCMeta):
    """Trading day *n* of each calendar quarter, counted from either end.

    Not part of upstream zipline, which stops at months. It is here because company fundamentals
    arrive quarterly and a strategy that reads them has no reason to wake up in the two months
    between: expressing "rebalance on the new statements" as a month rule plus a counter is the
    plumbing this API exists to remove.

    Calendar quarters, not fiscal ones. A filer whose year ends in June still reports four times
    a year; which quarter of *its* calendar a statement belongs to is a question about the filing,
    and the answer is in the data rather than in the schedule.
    """

    def __init__(self, n: int, invert: bool) -> None:
        n = lossless_float_to_int("TradingDayOfQuarterRule", "n", n)
        if not 0 <= n < MAX_QUARTER_RANGE:
            raise _out_of_range_error(MAX_QUARTER_RANGE)
        self.td_delta = (-n - 1) if invert else n

    def should_trigger(self, dt: datetime.datetime):
        value = self.cal.minute_to_session(dt, direction="none").value
        return value in self.execution_period_values

    @_per_calendar
    def execution_period_values(self):
        sessions = self.cal.sessions
        return set(
            pd.Series(data=sessions)
            .groupby([sessions.year, sessions.quarter])
            .nth(self.td_delta)
            .astype(np.int64)
        )
