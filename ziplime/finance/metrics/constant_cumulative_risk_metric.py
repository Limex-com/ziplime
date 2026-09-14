import datetime
from typing import Any

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class ConstantCumulativeRiskMetric:
    """A metric which does not change, ever.

    Notes
    -----
    This exists to maintain the existing structure of the perf packets. We
    should kill this as soon as possible.
    """
    #: This metric's ``end_of_bar`` only writes into the packet it is handed -- it carries nothing
    #: from one bar to the next -- so a run that is not emitting intraday packets can skip it.
    #: See :class:`~ziplime.finance.metrics_tracker.MetricsTracker`.
    packet_only = True


    def __init__(self, field, value):
        self._field = field
        self._value = value

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        packet["cumulative_risk_metrics"][self._field] = self._value

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        packet["cumulative_risk_metrics"][self._field] = self._value
