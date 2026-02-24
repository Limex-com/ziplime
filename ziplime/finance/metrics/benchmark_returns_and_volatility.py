import datetime
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.data.domain.data_bundle import DataBundle
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.finance_ext import minute_annual_volatility

from ziplime.sources.benchmark_source import BenchmarkSource


class BenchmarkReturnsAndVolatility:
    """Tracks daily and cumulative returns for the benchmark as well as the
    volatility of the benchmark returns.
    """

    def start_of_simulation(
            self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource
    ):
        daily_returns = benchmark_source.daily_returns(
            sessions[0],
            sessions[-1],
        )
        daily_returns = daily_returns.fill_nan(0.0)
        daily_returns_series = daily_returns.select("pct_change")
        if len(daily_returns_series) == 0:
            raise ValueError(f"No daily returns for benchmark. Please check if you properly loaded data for symbol {benchmark_source.benchmark_asset.get_symbol_by_exchange(exchange_name=None)} in memory.")
        self._daily_returns = daily_returns_array = daily_returns_series
        self._daily_cumulative_returns = np.cumprod(1 + daily_returns_array["pct_change"]) - 1
        self._daily_annual_volatility = (daily_returns_series.with_columns(
            expanding_sum=pl.col("pct_change").rolling_std(
                window_size=daily_returns_series.height,
                min_samples=2 if daily_returns_series.height > 1 else 1) * np.sqrt(252)
        ))["expanding_sum"]

        if emission_rate == datetime.timedelta(days=1):
            # none existing in minute mode
            self._minute_cumulative_returns = None
            self._minute_annual_volatility = None
        else:
            open_ = trading_calendar.session_open(sessions[0]).tz_convert(trading_calendar.tz).to_pydatetime()
            close = trading_calendar.session_close(sessions[-1]).tz_convert(trading_calendar.tz).to_pydatetime()
            returns = benchmark_source.get_range(start_dt=open_, end_dt=close)
            returns = returns.with_columns(
                pl.lit(0).alias("sid"),
                pl.lit(0.0).alias("close"),
                pl.lit(0).alias("pct_change"),
                pl.col("date")
            )

            # rrs = (1 + returns["pct_change"].to_pandas()).cumprod() - 1
            self._minute_cumulative_returns = (
                returns.select(pl.col("date"), pl.col("sid"), (1 + pl.col("pct_change")).cum_prod() - 1))
            min_annual_volatility = minute_annual_volatility(
                date_labels=returns["date"].dt.date(),  # returns.index.normalize().view("int64"),
                minute_returns=returns["pct_change"],
                daily_returns=daily_returns_array["pct_change"],
            )

            self._minute_annual_volatility = pl.DataFrame(
                [
                    returns.select("date").to_series(),
                    pl.Series("value", min_annual_volatility)
                ]
            )  # pl.DataFrame([pd.Series(returns.select("date"))])

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   exchanges: dict[str, Exchange]):
        if self._minute_cumulative_returns is None:
            # TODO: fix this so that we don't have minute/daily returns but dynamic frequency returns
            return
        r = self._minute_cumulative_returns["date" == session]["literal"][0]
        if np.isnan(r):
            r = None
        packet["cumulative_risk_metrics"]["benchmark_period_return"] = r

        v = self._minute_annual_volatility["date" == session]["value"][0]
        if np.isnan(v):
            v = None
        packet["cumulative_risk_metrics"]["benchmark_volatility"] = v

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       exchanges: dict[str, Exchange]):
        r = self._daily_cumulative_returns[session_ix]
        if np.isnan(r):
            r = None
        packet["cumulative_risk_metrics"]["benchmark_period_return"] = r

        v = self._daily_annual_volatility[session_ix]
        if v is None or np.isnan(v):
            v = None
        packet["cumulative_risk_metrics"]["benchmark_volatility"] = v
