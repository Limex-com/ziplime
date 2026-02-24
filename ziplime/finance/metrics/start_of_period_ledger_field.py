import datetime
import operator as op
from typing import Any

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger

from ziplime.sources.benchmark_source import BenchmarkSource


class StartOfPeriodLedgerField:
    """Keep track of the value of a ledger field at the start of the period.

    Parameters
    ----------
    ledger_field : str
        The ledger field to read.
    packet_field : str, optional
        The name of the field to populate in the packet. If not provided,
        ``ledger_field`` will be used.
    """

    def __init__(self, ledger_field, packet_field=None):
        self._get_ledger_field = op.attrgetter(ledger_field)
        if packet_field is None:
            self._packet_field = ledger_field.rsplit(".", 1)[-1]
        else:
            self._packet_field = packet_field
        self._previous_day = 0.0

    def start_of_simulation(
            self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource
    ):
        self._start_of_simulation = self._get_ledger_field(ledger)

    def start_of_session(self, ledger, session, exchanges: dict[str, Exchange]):
        self._previous_day = self._get_ledger_field(ledger)

    def _end_of_period(self, sub_field, packet, ledger):
        # if not hasattr(self, "_previous_day"):
        #     self._previous_day = self._get_ledger_field(ledger)
        packet_field = self._packet_field
        packet["cumulative_perf"][packet_field] = self._start_of_simulation
        packet[sub_field][packet_field] = self._previous_day

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        self._end_of_period("minute_perf", packet, ledger)

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        self._end_of_period("daily_perf", packet, ledger)
