import datetime

import polars as pl

from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.sources.benchmark_source import BenchmarkSource


class CurrencyBenchmarkSource(BenchmarkSource):
    """Session-aligned benchmark returns translated into one reporting currency.

    Parameters
    ----------
    source : BenchmarkSource
        Native listing-price, supplied-return, or zero-return benchmark source.
    reporting : CurrencyReporting
        Shared FX service and optional supplied benchmark currency declaration.
    currency : str
        Destination ISO currency code for benchmark returns.
    initial_dt : datetime.datetime
        Aware initial time seeding the first return's FX denominator.

    Attributes
    ----------
    source : BenchmarkSource
        Underlying native benchmark and its trading calendar/session order.
    reporting : CurrencyReporting
        Conversion configuration.
    currency : str
        Destination benchmark-return currency.
    initial_dt : datetime.datetime
        Start of the conversion history used before range filtering.
    base_currency : str
        Native benchmark currency: declared for supplied returns, inferred from a
        listing quote, or the accounting base for a no-benchmark run.
    _zero_benchmark : bool
        Whether an absent benchmark should remain flat instead of acquiring cash FX returns.
    _initial_rate : float
        Initial native-benchmark to reporting multiplier for the first return.

    Raises
    ------
    ValueError
        If supplied returns lack ``benchmark_currency``, a declared benchmark
        currency disagrees with its listing, or initial FX is unavailable.

    Notes
    -----
    Returns combine native price/return movement with the change in FX, not a
    multiplication of the native return by an FX level. No benchmark stays
    a zero-return benchmark in every currency, rather than earning cash FX.
    """

    def __init__(
            self, source: BenchmarkSource, reporting: CurrencyReporting,
            currency: str, initial_dt: datetime.datetime,
    ):
        """Bind the native benchmark and validate its currency/initial FX rate."""
        super().__init__(
            asset_service=source.asset_service, trading_calendar=source.exchange.trading_calendar,
            sessions=source.sessions, exchange=source.exchange, emission_rate=source.emission_rate,
            benchmark_fields=source.benchmark_fields, precalculated_series=source._precalculated_series,
            benchmark_asset=source.benchmark_asset,
            benchmark_returns=source.benchmark_returns,
        )
        self.source = source
        self.reporting = reporting
        self.currency = currency
        self.initial_dt = initial_dt
        self.base_currency = reporting.benchmark_currency
        if source.benchmark_returns is not None and self.base_currency is None:
            raise ValueError("Declare benchmark_currency when supplying benchmark_returns for currency reporting.")
        if (source.benchmark_asset is not None and self.base_currency is not None
                and source.benchmark_asset.quote.asset_name != self.base_currency):
            raise ValueError("benchmark_currency must match the benchmark listing's quote currency.")
        if self.base_currency is None:
            self.base_currency = (source.benchmark_asset.quote.asset_name
                                  if source.benchmark_asset is not None else reporting.base_currency)
        self._zero_benchmark = (
            source.benchmark_asset is None
            and source.benchmark_returns is None
            and source._precalculated_series["close"].eq(other=0).all()
        )
        self._initial_rate = reporting.conversion.rate(base=self.base_currency, quote=currency, dt=initial_dt)

    def daily_returns_by_session(self, sessions) -> pl.DataFrame:
        """Return translated daily returns in the requested session order.

        Parameters
        ----------
        sessions : iterable of datetime.date or datetime.datetime
            Calendar session labels covered by the source. FX is sampled at each close.

        Returns
        -------
        polars.DataFrame
            ``date`` session labels and ``pct_change`` fractional returns.

        Raises
        ------
        ValueError
            If supplied returns do not cover the sessions or a required FX quote is missing/stale.
        """
        native = self.source.daily_returns_by_session(sessions=sessions)
        if self._zero_benchmark or native.is_empty():
            return native
        stamps = [self.source.exchange.trading_calendar.session_close(session=session).to_pydatetime()
                  for session in sessions]
        rates = self.reporting.conversion.rates_at(base=self.base_currency, quote=self.currency, timestamps=stamps)
        return native.with_columns(rates).with_columns(
            ((1 + pl.col("pct_change")) *
             pl.col("fx_rate") / pl.col("fx_rate").shift(1).fill_null(self._initial_rate) - 1)
            .alias("pct_change")
        ).drop("fx_rate")

    def get_range(self, start_dt: datetime.datetime, end_dt: datetime.datetime) -> pl.DataFrame:
        """Translate benchmark observations before filtering to an inclusive time range.

        Parameters
        ----------
        start_dt, end_dt : datetime.datetime
            Aware inclusive bounds. Conversion starts at ``initial_dt`` to preserve
            the prior-rate denominator before filtering the requested start.

        Returns
        -------
        polars.DataFrame
            Timestamped ``pct_change`` returns and source columns. For price-based
            benchmarks ``close`` is also converted; supplied returns are compounded
            with FX changes instead.
        """
        native = self.source.get_range(start_dt=self.initial_dt, end_dt=end_dt)
        if self._zero_benchmark or native.is_empty():
            converted = native.with_columns(pl.lit(value=0.0).alias("pct_change"))
        elif self.source.benchmark_returns is not None:
            rates = self.reporting.conversion.rates_at(
                base=self.base_currency, quote=self.currency, timestamps=native["date"].to_list())
            converted = native.with_columns(rates).with_columns(
                ((1 + pl.col("pct_change")) *
                 pl.col("fx_rate") / pl.col("fx_rate").shift(1).fill_null(self._initial_rate) - 1)
                .alias("pct_change")
            ).drop("fx_rate")
        else:
            rates = self.reporting.conversion.rates_at(
                base=self.base_currency, quote=self.currency, timestamps=native["date"].to_list())
            converted = native.with_columns(rates).with_columns(
                (((1 + pl.col("close").pct_change().fill_null(0.0)) *
                  pl.col("fx_rate") / pl.col("fx_rate").shift(1).fill_null(self._initial_rate)) - 1)
                .alias("pct_change"),
                (pl.col("close") * pl.col("fx_rate")).alias("close"),
            ).drop("fx_rate")
        return converted.filter(pl.col("date") >= pl.lit(value=start_dt).cast(converted.schema["date"]))

    def daily_returns(self, start: datetime.datetime, end: datetime.datetime | None = None) -> pl.DataFrame:
        """Filter translated session returns by inclusive session dates.

        Parameters
        ----------
        start : datetime.datetime
            First session date, taken from the datetime's date component.
        end : datetime.datetime or None, optional
            Last session date; ``None`` includes all remaining source sessions.

        Returns
        -------
        polars.DataFrame
            Session ``date`` and currency-adjusted ``pct_change`` columns.
        """
        daily = self.daily_returns_by_session(sessions=self.source.sessions)
        start_date = start.date() if isinstance(start, datetime.datetime) else start
        if end is None:
            return daily.filter(pl.col("date") >= start_date)
        end_date = end.date() if isinstance(end, datetime.datetime) else end
        return daily.filter(pl.col("date").is_between(start_date, end_date))
