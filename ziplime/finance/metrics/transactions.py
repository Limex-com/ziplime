import datetime
from typing import Any

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class Transactions:
    """Tracks daily transactions."""

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        packet["minute_perf"]["transactions"] = ledger.transactions(dt=session)

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        packet["daily_perf"]["transactions"] = ledger.transactions()
