import datetime
from typing import Any

import numpy as np

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class ReturnsStatistic:
    """A metric that reports an end of simulation scalar or time series
    computed from the algorithm returns.

    Parameters
    ----------
    function : callable
        The function to call on the daily returns.
    field_name : str, optional
        The name of the field. If not provided, it will be
        ``function.__name__``.
    """

    def __init__(self, function, field_name=None):
        if field_name is None:
            field_name = function.__name__

        self._function = function
        self._field_name = field_name

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        res = self._function(ledger.daily_returns_array[: session_ix + 1])
        if not np.isfinite(res):
            res = None
        packet["cumulative_risk_metrics"][self._field_name] = res

    end_of_session = end_of_bar
