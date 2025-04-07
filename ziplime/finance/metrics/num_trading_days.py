import datetime
from typing import Any

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger
from ziplime.sources.benchmark_source import BenchmarkSource


class NumTradingDays:
    """Report the number of trading days."""

    def start_of_simulation(self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
                            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource):
        self._num_trading_days = 0

    def start_of_session(self, ledger, session, bundle_data: BundleData):
        self._num_trading_days += 1

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        packet["cumulative_risk_metrics"]["trading_days"] = self._num_trading_days

    end_of_session = end_of_bar
