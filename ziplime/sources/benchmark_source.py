import datetime

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.errors import (
    InvalidBenchmarkAsset,
    BenchmarkAssetNotAvailableTooEarly,
    BenchmarkAssetNotAvailableTooLate,
)
import polars as pl

from ziplime.exchanges.exchange import Exchange


class BenchmarkSource:
    def __init__(
            self,
            asset_service: AssetService,
            trading_calendar: ExchangeCalendar,
            sessions: pl.Series,
            exchange: Exchange,
            emission_rate: datetime.timedelta,
            benchmark_fields: frozenset[str],
            precalculated_series: pl.Series,
            benchmark_asset: ExchangeAsset | None = None,
            benchmark_returns: pl.Series | None = None,
    ):
        self.asset_service = asset_service
        self.benchmark_asset = benchmark_asset
        self.sessions = sessions
        self.emission_rate = emission_rate
        self.exchange = exchange
        self.benchmark_fields = benchmark_fields
        self._precalculated_series = precalculated_series

    def get_value(self, dt: datetime.datetime) -> pd.Series:
        """Look up the returns for a given dt.

        Parameters
        ----------
        dt : datetime
            The label to look up.

        Returns
        -------
        returns : float
            The returns at the given dt or session.

        See Also
        --------
        :class:`ziplime.sources.benchmark_source.BenchmarkSource.daily_returns`

        .. warning::

           This method expects minute inputs if ``emission_rate == 'minute'``
           and session labels when ``emission_rate == 'daily``.
        """
        return self._precalculated_series.iloc[dt]

    def get_range(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> pl.DataFrame:
        """Look up the returns for a given period.

        Parameters
        ----------
        start_dt : datetime
            The inclusive start label.
        end_dt : datetime
            The inclusive end label.

        Returns
        -------
        returns : pd.Series
            The series of returns.

        See Also
        --------
        :class:`ziplime.sources.benchmark_source.BenchmarkSource.daily_returns`

        .. warning::

           This method expects minute inputs if ``emission_rate == 'minute'``
           and session labels when ``emission_rate == 'daily``.
        """
        return self._precalculated_series.filter(pl.col("date").is_between(start_dt, end_dt))

    def daily_returns(self, start: datetime.datetime, end: datetime.datetime | None = None) -> pd.Series:
        """Returns the daily returns for the given period.

        Parameters
        ----------
        start : datetime
            The inclusive starting session label.
        end : datetime, optional
            The inclusive ending session label. If not provided, treat
            ``start`` as a scalar key.

        Returns
        -------
        returns : pd.Series or float
            The returns in the given period. The index will be the trading
            calendar in the range [start, end]. If just ``start`` is provided,
            return the scalar value on that day.
        """

        # todo : returns for first day
        daily_returns = self._precalculated_series.group_by_dynamic(
            index_column="date", every="1d").agg(pl.col("close").tail(1).sum()).with_columns(
            pl.col("date").dt.date().alias("date")
        ).with_columns(pl.col("close").pct_change().alias("pct_change")).fill_null(0)

        if end is None:
            return daily_returns.filter(pl.col("date") >= start)

        return daily_returns.filter(pl.col("date").is_between(start, end))

    def daily_returns_by_session(self, sessions) -> pl.DataFrame:
        """One benchmark return per **session**, in session order.

        :meth:`daily_returns` answers with the days the benchmark has bars for, which is not the
        same list as the sessions the simulation runs: an index can miss a session, and it can
        carry a bar stamped on a day that is not one. Measured on the Moscow daily bundle,
        `IMOEX@RTSX` holds 4,212 bar days against 4,203 XMOS sessions, with 907 sessions
        unrepresented.

        The metrics index this series **by position** -- ``daily_returns[:session_ix + 1]`` -- so
        a single missing day shifts every reading after it onto the wrong session, and the run
        dies on its last bar, where the lengths finally disagree, with
        ``operands could not be broadcast together`` out of ``empyrical``. Aligning here makes
        that impossible: the answer always has one row per session, a session the benchmark did
        not trade counts as a flat day, and position *is* session.
        """
        wanted = pl.DataFrame({
            "date": [session.date() if hasattr(session, "date") else session
                     for session in sessions]
        })
        if not wanted.height:
            return wanted.with_columns(pl.lit(0.0).alias("pct_change"))

        returns = self.daily_returns(wanted["date"][0], wanted["date"][-1])
        if returns.is_empty():
            return wanted.with_columns(pl.lit(0.0).alias("pct_change"))

        return (wanted
                .join(returns.select(["date", "pct_change"]), on="date", how="left")
                .with_columns(pl.col("pct_change").fill_null(0.0).fill_nan(0.0)))

    async def validate_benchmark(self, benchmark_asset: ExchangeAsset):
        # check if this security has a stock dividend.  if so, raise an
        # error suggesting that the user pick a different asset to use
        # as benchmark.
        stock_dividends = await self.asset_service.get_stock_dividends(
            sid=benchmark_asset.sid, trading_days=self.sessions
        )

        if len(stock_dividends) > 0:
            raise InvalidBenchmarkAsset(
                sid=str(benchmark_asset), dt=stock_dividends[0]["ex_date"]
            )

        if benchmark_asset.start_date > self.sessions[0]:
            # the asset started trading after the first simulation day
            raise BenchmarkAssetNotAvailableTooEarly(
                sid=str(benchmark_asset),
                dt=self.sessions[0],
                start_dt=benchmark_asset.start_date,
            )

        if benchmark_asset.end_date < self.sessions[-1]:
            # the asset stopped trading before the last simulation day
            raise BenchmarkAssetNotAvailableTooLate(
                sid=str(benchmark_asset),
                dt=self.sessions[-1],
                end_dt=benchmark_asset.end_date,
            )

    @staticmethod
    def _compute_daily_returns(g):
        return (g[-1] - g[0]) / g[0]

    @classmethod
    def downsample_minute_return_series(cls, trading_calendar: ExchangeCalendar,
                                        minutely_returns: pd.Series) -> pd.Series:
        sessions = trading_calendar.minutes_to_sessions(
            minutes=minutely_returns.index,
        )
        closes = trading_calendar.closes[sessions[0]: sessions[-1]]
        daily_returns = minutely_returns[closes].pct_change()
        daily_returns.index = closes.index
        return daily_returns.iloc[1:]
