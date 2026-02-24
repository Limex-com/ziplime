import datetime
from typing import Any

import pandas as pd
from exchange_calendars import ExchangeCalendar


from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
from ziplime.sources.benchmark_source import BenchmarkSource



class MaxLeverage:
    """Tracks the maximum account leverage."""

    def start_of_simulation(self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
                            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource):
        self._max_leverage = 0.0

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        self._max_leverage = max(self._max_leverage, ledger.account.leverage)
        packet["cumulative_risk_metrics"]["max_leverage"] = self._max_leverage

    end_of_session = end_of_bar
