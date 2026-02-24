import datetime
from typing import Any

import empyrical as ep
import numpy as np
import pandas as pd
from exchange_calendars import ExchangeCalendar


from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
from ziplime.sources.benchmark_source import BenchmarkSource



class AlphaBeta:
    """End of simulation alpha and beta to the benchmark."""

    def start_of_simulation(
            self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource
    ):
        self._daily_returns_array = benchmark_source.daily_returns(
            sessions[0],
            sessions[-1],
        ).select("pct_change")

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        risk = packet["cumulative_risk_metrics"]
        alpha, beta = ep.alpha_beta_aligned(
            ledger.daily_returns_array[: session_ix + 1],
            self._daily_returns_array[: session_ix + 1]["pct_change"].to_pandas(),
        )
        if not np.isfinite(alpha):
            alpha = None
        if np.isnan(beta):
            beta = None

        risk["alpha"] = alpha
        risk["beta"] = beta

    end_of_session = end_of_bar
