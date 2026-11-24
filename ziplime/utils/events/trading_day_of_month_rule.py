"""Trading day of month rule for events."""

from abc import ABCMeta
import datetime
import numpy as np
import pandas as pd
from ziplime.utils.events.support import MAX_MONTH_RANGE
from ziplime.utils.events.stateless_rule import StatelessRule
from ziplime.utils.events.utils import _out_of_range_error
from ziplime.utils.events.utils import _per_calendar
from ziplime.utils.events.utils import lossless_float_to_int


class TradingDayOfMonthRule(StatelessRule, metaclass=ABCMeta):
    def __init__(self, n: int, invert: bool) -> None:
        n = lossless_float_to_int("TradingDayOfMonthRule", "n", n)
        if not 0 <= n < MAX_MONTH_RANGE:
            raise _out_of_range_error(MAX_MONTH_RANGE)
        if invert:
            self.td_delta = -n - 1
        else:
            self.td_delta = n

    def should_trigger(self, dt: datetime.datetime):
        # is this market minute's period in the list of execution periods?
        value = self.cal.minute_to_session(dt, direction="none").value
        return value in self.execution_period_values

    @_per_calendar
    def execution_period_values(self):
        # calculate the list of periods that match the given criteria
        sessions = self.cal.sessions
        return set(
            pd.Series(data=sessions)
            .groupby([sessions.year, sessions.month])
            .nth(self.td_delta)
            .astype(np.int64)
        )
