"""Trading day of week rule for events."""

from abc import ABCMeta
import datetime
import numpy as np
import pandas as pd
from ziplime.utils.events.support import MAX_WEEK_RANGE
from ziplime.utils.events.stateless_rule import StatelessRule
from ziplime.utils.events.utils import _out_of_range_error
from ziplime.utils.events.utils import _per_calendar
from ziplime.utils.events.utils import lossless_float_to_int


class TradingDayOfWeekRule(StatelessRule, metaclass=ABCMeta):
    def __init__(self, n: int, invert: bool) -> None:
        n = lossless_float_to_int("TradingDayOfWeekRule", "n", n)
        if not 0 <= n < MAX_WEEK_RANGE:
            raise _out_of_range_error(MAX_WEEK_RANGE)

        self.td_delta = (-n - 1) if invert else n

    def should_trigger(self, dt: datetime.datetime):
        # is this market minute's period in the list of execution periods?
        val = self.cal.minute_to_session(dt, direction="none").value
        return val in self.execution_period_values

    @_per_calendar
    def execution_period_values(self):
        # calculate the list of periods that match the given criteria
        sessions = self.cal.sessions
        return set(
            pd.Series(data=sessions)
            # Group by ISO year (0) and week (1)
            .groupby(sessions.map(lambda x: x.isocalendar()[0:2]))
            .nth(self.td_delta)
            .astype(np.int64)
        )
