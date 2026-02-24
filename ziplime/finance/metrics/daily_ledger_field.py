import datetime
import operator as op
from typing import Any

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class DailyLedgerField:
    """Like :class:`~ziplime.finance.metrics.metric.SimpleLedgerField` but
    also puts the current value in the ``cumulative_perf`` section.

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

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        field = self._packet_field
        packet["cumulative_perf"][field] = packet["minute_perf"][field] = (
            self._get_ledger_field(ledger)
        )

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        field = self._packet_field
        packet["cumulative_perf"][field] = packet["daily_perf"][field] = (
            self._get_ledger_field(ledger)
        )
