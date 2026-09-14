import datetime
from typing import Any

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class Positions:
    """Tracks daily positions."""
    #: This metric's ``end_of_bar`` only writes into the packet it is handed -- it carries nothing
    #: from one bar to the next -- so a run that is not emitting intraday packets can skip it.
    #: See :class:`~ziplime.finance.metrics_tracker.MetricsTracker`.
    packet_only = True


    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        packet["minute_perf"]["positions"] = ledger.positions(session)

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        packet["daily_perf"]["positions"] = ledger.positions(dt=session)
