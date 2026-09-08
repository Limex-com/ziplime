import datetime
from typing import Awaitable, Callable

import pandas as pd
import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.continuous_future import ContinuousFuture
from contextlib import contextmanager
import numpy as np

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_source import DataSource


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    """The calendar day of a listing bound, which may be stored either way."""
    return value.date() if isinstance(value, datetime.datetime) else value


@contextmanager
def handle_non_market_minutes(bar_data):
    try:
        bar_data._handle_non_market_minutes = True
        yield
    finally:
        bar_data._handle_non_market_minutes = False


class BarData:
    """Provides methods for accessing minutely and daily price/volume data from
    Algorithm API functions.

    Also provides utility methods to determine if an asset is alive, and if it
    has recent trade data.

    An instance of this object is passed as ``data`` to
    :func:`~ziplime.api.handle_data` and
    :func:`~ziplime.api.before_trading_start`.

    Parameters
    ----------
    data_bundle : DataBundle
        Provider for bar pricing data.
    simulation_dt_func : callable
        Function which returns the current simulation time.
        This is usually bound to a method of TradingSimulation.
    data_frequency : {'minute', 'daily'}
        The frequency of the bar data; i.e. whether the data is
        daily or minute bars
    restrictions : ziplime.finance.asset_restrictions.Restrictions
        Object that combines and returns restricted list information from
        multiple sources
    """

    def __init__(self,
                 data_sources: dict[str, DataSource],
                 simulation_dt_func: Callable,
                 trading_calendar: ExchangeCalendar,
                 restrictions,
                 data_source_resolver: Callable[[str], Awaitable[DataSource]] | None = None):
        # self.data_bundle = data_bundle
        self.simulation_dt_func = simulation_dt_func

        # self._daily_mode = (self.data_bundle == "daily")

        self._adjust_minutes = False

        self._trading_calendar = trading_calendar
        self._is_restricted = restrictions.is_restricted
        self.data_sources = data_sources
        self.default_data_source = data_sources[list(data_sources.keys())[0]]
        # Mounts a source named by a strategy that was never registered up front -- a dataset
        # address such as "hf://owner/name/config". Supplied by TradingAlgorithm, which is what
        # holds the asset database and the simulation window a mount needs.
        self._data_source_resolver = data_source_resolver
        # first_exchange = exchanges[list(exchanges.keys())[0]]
        # self.default_exchange = first_exchange

    async def resolve_data_source(self, data_source) -> DataSource:
        """Return the source a read is addressed to, mounting it on demand.

        Accepts the three things a strategy may pass as ``data_source``:

        * ``None`` -- the default source, which is the market data the simulation runs on;
        * a :class:`~ziplime.data.services.data_source.DataSource` -- used as given, so a source
          built in ``initialize`` can be handed straight back;
        * a name -- an already-registered source, or an address such as
          ``hf://ZipLime/congress-trading/features``, which is mounted the first time it is asked
          for and reused for the rest of the run.

        The mount happens once per address per run. It is deliberately not done eagerly: a
        strategy that never reads a dataset never downloads it.
        """
        if data_source is None:
            return self.default_data_source
        if isinstance(data_source, DataSource):
            # Register it too, so a later read by name finds the same materialised instance
            # rather than mounting a second copy of the same data.
            self.data_sources.setdefault(data_source.name, data_source)
            return data_source
        if data_source in self.data_sources:
            return self.data_sources[data_source]
        if self._data_source_resolver is None:
            raise KeyError(
                f"No data source named {data_source!r}. Registered: "
                f"{', '.join(sorted(self.data_sources))}.")
        resolved = await self._data_source_resolver(data_source)
        self.data_sources[resolved.name] = resolved
        # Also under the name that was asked for, so an address and the source's own name both
        # hit the cache on the next bar.
        self.data_sources[data_source] = resolved
        return resolved

    def _get_current_minute(self):
        """Internal utility method to get the current simulation time.

        Possible answers are:
        - whatever the algorithm's get_datetime() method returns (this is what
            `self.simulation_dt_func()` points to)
        - sometimes we're knowingly not in a market minute, like if we're in
            before_trading_start.  In that case, `self._adjust_minutes` is
            True, and we get the previous market minute.
        - if we're in daily mode, get the session label for this minute.
        """
        dt = self.simulation_dt_func()

        if self._adjust_minutes:
            dt = self._trading_calendar.previous_minute(dt)

        # TODO: check this, is it different for daily?
        #
        # if self._daily_mode:
        #     # if we're in daily mode, take the given dt (which is the last
        #     # minute of the session) and get the session label for it.
        #     dt = self.data_portal.trading_calendar.minute_to_session(dt)

        # return dt
        return dt

    async def current(self, assets: list[Asset], fields: list[str],
                data_source: str | None = None) -> pl.DataFrame:
        """Returns the "current" value of the given fields for the given assets
        at the current simulation time.

        Parameters
        ----------
        assets : ziplime.assets.Asset or iterable of ziplime.assets.Asset
            The asset(s) for which data is requested.
        fields : str or iterable[str].
            Requested data field(s). Valid field names are: "price",
            "last_traded", "open", "high", "low", "close", and "volume".

        Returns
        -------
        current_value : Scalar, pandas Series, or pandas DataFrame.
            See notes below.

        Notes
        -----
        The return type of this function depends on the types of its inputs:

        - If a single asset and a single field are requested, the returned
          value is a scalar (either a float or a ``datetime.datetime`` depending on
          the field).

        - If a single asset and a list of fields are requested, the returned
          value is a :class:`pd.Series` whose indices are the requested fields.

        - If a list of assets and a single field are requested, the returned
          value is a :class:`pd.Series` whose indices are the assets.

        - If a list of assets and a list of fields are requested, the returned
          value is a :class:`pd.DataFrame`.  The columns of the returned frame
          will be the requested fields, and the index of the frame will be the
          requested assets.

        The values produced for ``fields`` are as follows:

        - Requesting "price" produces the last known close price for the asset,
          forward-filled from an earlier minute if there is no trade this
          minute. If there is no last known value (either because the asset
          has never traded, or because it has delisted) NaN is returned. If a
          value is found, and we had to cross an adjustment boundary (split,
          dividend, etc) to get it, the value is adjusted to the current
          simulation time before being returned.

        - Requesting "open", "high", "low", or "close" produces the open, high,
          low, or close for the current minute. If no trades occurred this
          minute, ``NaN`` is returned.

        - Requesting "volume" produces the trade volume for the current
          minute. If no trades occurred this minute, 0 is returned.

        - Requesting "last_traded" produces the datetime of the last minute in
          which the asset traded, even if the asset has stopped trading. If
          there is no last known value, ``pd.NaT`` is returned.

        If the current simulation time is not a valid market time for an asset,
        we use the most recent market close instead.
        """
        assets = frozenset(assets)
        fields = frozenset(fields)
        source = await self.resolve_data_source(data_source)
        # `_adjust_minutes` -- set while `before_trading_start` runs, which is not a market minute
        # -- used to take a second branch here. That branch read `self.data_bundle`, an attribute
        # this class does not have, and built a frame out of un-awaited coroutines, so it raised
        # the moment it was reached. What it was trying to add is a price adjustment relative to
        # the current instant; what actually matters, reading the previous market minute instead of
        # a minute the calendar does not have, is already done by `_get_current_minute`.
        return await source.get_spot_value(
            assets=assets,
            fields=fields,
            dt=self._get_current_minute(),
        )

    async def current_chain(self, continuous_future: ContinuousFuture,
                            data_source: str | None = None):
        """Return the active contracts of a chain, front contract first.

        ``self.data_bundle`` never existed on this object; the bundle is reached through
        ``data_sources``.
        """
        source = self.data_sources[data_source] if data_source else self.default_data_source
        return await source.get_current_future_chain(
            continuous_future=continuous_future,
            dt=self.simulation_dt_func()
        )

    async def current_contract(self, continuous_future: ContinuousFuture,
                               data_source: str | None = None):
        """Return the contract a continuous future holds right now.

        This is what you order: a continuous future itself is a data specifier, not tradeable.
        """
        source = self.data_sources[data_source] if data_source else self.default_data_source
        return await source.current_contract(
            continuous_future=continuous_future,
            dt=self.simulation_dt_func()
        )

    def can_trade(self, assets: list[Asset]):
        """For the given asset or iterable of assets, returns True if all of the
        following are true:

        1. The asset is alive for the session of the current simulation time
           (if current simulation time is not a market minute, we use the next
           session).
        2. The asset's exchange is open at the current simulation time or at
           the simulation calendar's next market minute.
        3. There is a known last price for the asset.

        Parameters
        ----------
        assets: ziplime.assets.Asset or iterable of ziplime.assets.Asset
            Asset(s) for which tradability should be determined.

        Notes
        -----
        The second condition above warrants some further explanation:

        - If the asset's exchange calendar is identical to the simulation
          calendar, then this condition always returns True.
        - If there are market minutes in the simulation calendar outside of
          this asset's exchange's trading hours (for example, if the simulation
          is running on the CMES calendar but the asset is MSFT, which trades
          on the NYSE), during those minutes, this condition will return False
          (for example, 3:15 am Eastern on a weekday, during which the CMES is
          open but the NYSE is closed).

        Returns
        -------
        can_trade : bool or pd.Series[bool]
            Bool or series of bools indicating whether the requested asset(s)
            can be traded in the current minute.
        """
        dt = self.simulation_dt_func()
        assets = set(assets)
        if self._adjust_minutes:
            adjusted_dt = self._get_current_minute()
        else:
            adjusted_dt = dt

        # if isinstance(assets, Asset):
        #     return self._can_trade_for_asset(
        #         assets, dt, adjusted_dt, data_portal
        #     )
        # else:
        tradeable = [
            self._can_trade_for_asset(
                asset=asset, dt=dt, adjusted_dt=adjusted_dt
            )
            for asset in assets
        ]
        return pd.Series(data=tradeable, index=assets, dtype=bool)

    def _can_trade_for_asset(self, asset: ExchangeAsset, dt: datetime.datetime,
                             adjusted_dt: datetime.datetime) -> bool:
        """Whether ``asset`` is listed, unrestricted and on an open venue at ``dt``.

        The method this replaces could not run at all. It called `asset.is_alive_for_session` and
        `asset.is_exchange_open`, which an `ExchangeAsset` does not have, read `self.data_portal`
        and `self.data_frequency`, which this object does not have, and tested a pandas Series for
        truth. Every one of those raises, so `can_trade` raised for every asset it was ever asked
        about.

        **The price condition is not checked here.** `can_trade`'s docstring lists a third
        requirement -- that a last price is known -- and testing it means reading the bundle, which
        is asynchronous, while this method and its caller are not. Rather than make `can_trade`
        async and break every strategy that calls it, the two conditions that can be answered from
        the listing and the calendar are answered, and the price is left to the caller: reading it
        with `data.current` is how a strategy finds out anyway, since it needs the number.
        """
        restricted = self._is_restricted(assets=[asset], dt=adjusted_dt)
        if bool(restricted.iloc[0] if hasattr(restricted, "iloc") else restricted):
            return False

        session_label = self._trading_calendar.minute_to_session(minute=dt)
        session = session_label.date() if hasattr(session_label, "date") else session_label

        # Listed and not yet delisted. A dataset may carry rows for a name years before it lists
        # -- an insider filing predates the ticker's first session -- so this is what stops a
        # strategy ordering something that does not trade yet.
        if asset.start_date and session < _as_date(asset.start_date):
            return False
        if asset.end_date and session > _as_date(asset.end_date):
            return False
        if asset.auto_close_date and session > _as_date(asset.auto_close_date):
            return False

        # The simulation's calendar, not the listing's own: a listing carries its venue but not
        # that venue's schedule, and `can_trade` documents the simulation calendar as the answer
        # when the asset's own is unavailable.
        if self._trading_calendar.is_open_on_minute(minute=dt):
            return True
        return self._trading_calendar.is_session(session_label)

    async def history(self, assets: list[Asset], bar_count: int | None = None,
                frequency: datetime.timedelta | Period = datetime.timedelta(days=1),
                fields: list[str] | None=None,
                data_source: str | None = None,
                since: datetime.timedelta | None = None,
                ) -> pl.DataFrame:
        """Returns a trailing window of length ``bar_count`` with data for
        the given assets, fields, and frequency, adjusted for splits, dividends,
        and mergers as of the current simulation time.

        The semantics for missing data are identical to the ones described in
        the notes for :meth:`current`.

        Parameters
        ----------
        assets: ziplime.assets.Asset or iterable of ziplime.assets.Asset
            The asset(s) for which data is requested.
        fields: string or iterable of string.
            Requested data field(s). Valid field names are: "price",
            "last_traded", "open", "high", "low", "close", and "volume".
        bar_count: int
            Number of data observations requested.
        frequency: str
            String indicating whether to load daily or minutely data
            observations. Pass '1m' for minutely data, '1d' for daily data.

        Returns
        -------
        history : pd.Series or pd.DataFrame or pd.Panel
            See notes below.

        Notes
        -----
        The return type of this function depends on the types of ``assets`` and
        ``fields``:

        - If a single asset and a single field are requested, the returned
          value is a :class:`pd.Series` of length ``bar_count`` whose index is
          :class:`pd.DatetimeIndex`.

        - If a single asset and multiple fields are requested, the returned
          value is a :class:`pd.DataFrame` with shape
          ``(bar_count, len(fields))``. The frame's index will be a
          :class:`pd.DatetimeIndex`, and its columns will be ``fields``.

        - If multiple assets and a single field are requested, the returned
          value is a :class:`pd.DataFrame` with shape
          ``(bar_count, len(assets))``. The frame's index will be a
          :class:`pd.DatetimeIndex`, and its columns will be ``assets``.

        - If multiple assets and multiple fields are requested, the returned
          value is a :class:`pd.DataFrame` with a pd.MultiIndex containing
          pairs of :class:`pd.DatetimeIndex`, and ``assets``, while the columns
          while contain the field(s). It has shape ``(bar_count * len(assets),
          len(fields))``. The names of the pd.MultiIndex are

              - ``date`` if frequency == '1d'`` or ``date_time`` if frequency == '1m``, and
              - ``asset``

        If the current simulation time is not a valid market time, we use the last market close instead.

        Counting rows or counting time
        ------------------------------

        Pass **either** ``bar_count`` or ``since``, not both.

        ``bar_count`` asks for a number of rows. On bar data that is a number of sessions, which is
        almost always what a strategy means.

        ``since`` asks for a span of calendar time -- ``since=datetime.timedelta(days=90)`` is
        "everything filed in the last quarter", however many rows that turns out to be. This is the
        one to use on **event data**, where rows do not arrive on a schedule: thirty rows of
        congressional disclosures for one ticker can span four years, so ``bar_count=30`` there is
        a question about how often that company's insiders file rather than about time.

        Both return the same shape, so they are interchangeable at the call site.
        """
        assets = frozenset(assets)
        fields = frozenset(fields) if fields else None

        if (bar_count is None) == (since is None):
            raise ValueError(
                "history() needs exactly one of bar_count or since: bar_count for a number of "
                "rows, since for a span of calendar time. Event data usually wants since.")

        source = await self.resolve_data_source(data_source)

        if since is not None:
            return await source.get_data_by_window(
                assets=assets, since=since, end_date=self._get_current_minute(),
                frequency=frequency, fields=fields, include_end_date=False)

        df = await source.get_data_by_limit(assets=assets,
                                                         end_date=self._get_current_minute(),
                                                         limit=bar_count,
                                                         frequency=frequency,
                                                         fields=fields,
                                                         include_end_date=False
                                                         )

        # See `current`: the `_adjust_minutes` branch that stood here referred to
        # `self.exchanges[exchange_name]`, with no `exchange_name` in scope, and raised `NameError`
        # whenever it was reached. `_get_current_minute` already answers for the non-market minute.
        return df

    @property
    def current_dt(self):

        return self.simulation_dt_func()

    @property
    def _handle_non_market_minutes(self):
        return self._adjust_minutes

    @_handle_non_market_minutes.setter
    def _handle_non_market_minutes(self, val):
        self._adjust_minutes = val

    @property
    def current_session(self):
        return self._trading_calendar.minute_to_session(
            self.simulation_dt_func(),
            direction="next"
        )
