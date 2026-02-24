import datetime
from typing import Any

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger

from ziplime.sources.benchmark_source import BenchmarkSource


class PNL:
    """Tracks daily and cumulative PNL."""

    def start_of_simulation(
            self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource
    ):
        self._previous_pnl = 0.0

    def start_of_session(self, ledger, session, exchanges: dict[str, Exchange]):
        self._previous_pnl = ledger.portfolio.pnl

    def _end_of_period(self, field, packet, ledger):
        pnl = ledger.portfolio.pnl
        packet[field]["pnl"] = pnl - self._previous_pnl
        packet["cumulative_perf"]["pnl"] = ledger.portfolio.pnl

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        self._end_of_period("minute_perf", packet, ledger)

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        self._end_of_period("daily_perf", packet, ledger)
