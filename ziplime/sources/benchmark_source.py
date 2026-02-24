import datetime

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.asset import Asset
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
            benchmark_asset: Asset | None = None,
            benchmark_returns: pl.Series | None = None,
    ):
        self.asset_service = asset_service
        self.benchmark_asset = benchmark_asset
        self.sessions = sessions
        self.emission_rate = emission_rate
        self.exchange = exchange
        self.benchmark_fields = benchmark_fields
        if len(sessions) == 0:
            self._precalculated_series = pl.Series()

        elif benchmark_asset is not None:
            self._precalculated_series = self._initialize_precalculated_series(
                asset=benchmark_asset, trading_calendar=trading_calendar, trading_days=sessions,
                exchange=exchange
            )
        elif benchmark_returns is not None:
            all_bars = pl.from_pandas(
                trading_calendar.sessions_minutes(start=sessions[0], end=sessions[-1]).tz_convert(trading_calendar.tz)
            )
            self._precalculated_series = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
                index_column="date", every=self.emission_rate
            ).agg(pl.col("close").sum())
        else:
            all_bars = pl.from_pandas(
                trading_calendar.sessions_minutes(start=sessions[0], end=sessions[-1]).tz_convert(trading_calendar.tz)
            )
            self._precalculated_series = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
                index_column="date", every=self.emission_rate
            ).agg(pl.col("close").sum())

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

    async def validate_benchmark(self, benchmark_asset: Asset):
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

    def _initialize_precalculated_series(
            self, asset: Asset, trading_calendar: ExchangeCalendar, trading_days: pl.Series,
            exchange: Exchange
    ):
        """
        Internal method that pre-calculates the benchmark return series for
        use in the simulation.

        Parameters
        ----------
        asset:  Asset to use

        trading_calendar: TradingCalendar

        trading_days: pd.DateTimeIndex

        exchange: Exchange

        Notes
        -----
        If the benchmark asset started trading after the simulation start,
        or finished trading before the simulation end, exceptions are raised.

        If the benchmark asset started trading the same day as the simulation
        start, the first available minute price on that day is used instead
        of the previous close.

        We use history to get an adjusted price history for each day's close,
        as of the look-back date (the last day of the simulation).  Prices are
        fully adjusted for dividends, splits, and mergers.

        Returns
        -------
        returns : pd.Series
            indexed by trading day, whose values represent the %
            change from close to close.
        daily_returns : pd.Series
            the partial daily returns for each minute
        """
        all_bars: pl.Series = pl.from_pandas(
            trading_calendar.sessions_minutes(start=self.sessions[0], end=self.sessions[-1]).tz_convert(
                trading_calendar.tz)
        )  # .to_frame("date").group_by_dynamic(
        #     index_column="date", every=self.emission_rate
        # ).agg()["date"]
        limit = all_bars.to_frame("date").group_by_dynamic(
            index_column="date", every=self.emission_rate
        ).agg()["date"].len()
        # precalculated_series = pl.DataFrame({"date": all_bars, "value": 0.00}).group_by_dynamic(
        #     index_column="date", every=self.timedelta_period
        # ).agg(pl.col("value").sum())

        benchmark_series = self.exchange.get_data_by_limit(
            fields=self.benchmark_fields,
            limit=limit,
            frequency=self.emission_rate,
            end_date=all_bars[-1],
            assets=frozenset({asset}),
            include_end_date=True
        )
        return benchmark_series.with_columns(pl.col(self.benchmark_fields).pct_change().alias("pct_change"))  # [1:]
