import datetime
from typing import Any

from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger


class PeriodLabel:
    """Backwards compat, please kill me."""

    def start_of_session(self, ledger, session, exchanges: dict[str, Exchange]):
        self._label = session.strftime("%Y-%m")

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        packet["cumulative_risk_metrics"]["period_label"] = self._label

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        packet["cumulative_risk_metrics"]["period_label"] = self._label if hasattr(self, "label") else session.strftime(
            "%Y-%m")
