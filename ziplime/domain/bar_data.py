#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from typing import Callable

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.data.data_portal import DataPortal
from ziplime.domain.data_frequency import DataFrequency

# from zipline._protocol import BarData  # noqa

# Datasource type should completely determine the other fields of a
# message with its type.
# DATASOURCE_TYPE = enum(
#     'AS_TRADED_EQUITY',
#     'MERGER',
#     'SPLIT',
#     'DIVIDEND',
#     'TRADE',
#     'TRANSACTION',
#     'ORDER',
#     'EMPTY',
#     'DONE',
#     'CUSTOM',
#     'BENCHMARK',
#     'COMMISSION',
#     'CLOSE_POSITION'
# )
from contextlib import contextmanager
import numpy as np

from collections.abc import Iterable

from ziplime.assets.utils import Asset
# Expected fields/index values for a dividend Series.
DIVIDEND_FIELDS = [
    'declared_date',
    'ex_date',
    'gross_amount',
    'net_amount',
    'pay_date',
    'payment_sid',
    'ratio',
    'sid',
]
# Expected fields/index values for a dividend payment Series.
DIVIDEND_PAYMENT_FIELDS = [
    'id',
    'payment_sid',
    'cash_amount',
    'share_count',
]


def _is_iterable(obj) -> bool:
    return isinstance(obj, Iterable) and not isinstance(obj, str)


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
    :func:`~zipline.api.handle_data` and
    :func:`~zipline.api.before_trading_start`.

    Parameters
    ----------
    data_portal : DataPortal
        Provider for bar pricing data.
    simulation_dt_func : callable
        Function which returns the current simulation time.
        This is usually bound to a method of TradingSimulation.
    data_frequency : {'minute', 'daily'}
        The frequency of the bar data; i.e. whether the data is
        daily or minute bars
    restrictions : zipline.finance.asset_restrictions.Restrictions
        Object that combines and returns restricted list information from
        multiple sources
    """

    def __init__(self, data_portal: DataPortal,
                 simulation_dt_func: Callable,
                 data_frequency: DataFrequency,
                 trading_calendar: ExchangeCalendar,
                 restrictions):
        self.data_portal = data_portal
        self.simulation_dt_func = simulation_dt_func
        self.data_frequency = data_frequency

        self._daily_mode = (self.data_frequency == "daily")

        self._adjust_minutes = False

        self._trading_calendar = trading_calendar
        self._is_restricted = restrictions.is_restricted

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
            dt = self.data_portal.trading_calendar.previous_minute(dt)

        if self._daily_mode:
            # if we're in daily mode, take the given dt (which is the last
            # minute of the session) and get the session label for it.
            dt = self.data_portal.trading_calendar.minute_to_session(dt)

        # return dt
        return dt

    # @check_parameters(('assets', 'fields'),
    #                   ((Asset, ContinuousFuture, str), (str,)))
    def current(self, assets: list[Asset], fields: list[str]):
        """Returns the "current" value of the given fields for the given assets
        at the current simulation time.

        Parameters
        ----------
        assets : zipline.assets.Asset or iterable of zipline.assets.Asset
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
          value is a scalar (either a float or a ``pd.Timestamp`` depending on
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
        multiple_assets = _is_iterable(assets)
        multiple_fields = _is_iterable(fields)

        # There's some overly verbose code in here, particularly around
        # 'do something if self._adjust_minutes is False, otherwise do
        # something else'. This could be less verbose, but the 99% case is that
        # `self._adjust_minutes` is False, so it's important to keep that code
        # path as fast as possible.

        # There's probably a way to make this method (and `history`) less
        # verbose, but this is OK for now.

        # if not multiple_assets:
        #     asset = assets
        #
        #     if not multiple_fields:
        #         field = fields
        #
        #         # return scalar value
        #         if not self._adjust_minutes:
        #             return self.data_portal.get_spot_value(
        #                 assets=[asset],
        #                 field=field,
        #                 dt=self._get_current_minute(),
        #                 data_frequency=self.data_frequency
        #             )
        #         else:
        #             return self.data_portal.get_adjusted_value(
        #                 asset=[asset],
        #                 field=field,
        #                 dt=self._get_current_minute(),
        #                 perspective_dt=self.simulation_dt_func(),
        #                 data_frequency=self.data_frequency
        #             )
        #     else:
        #         # assume fields is iterable
        #         # return a Series indexed by field
        #         if not self._adjust_minutes:
        #             return pd.Series(data={
        #                 field: self.data_portal.get_spot_value(
        #                     asset,
        #                     field,
        #                     self._get_current_minute(),
        #                     self.data_frequency
        #                 )
        #                 for field in fields
        #             }, index=fields, name=assets.symbol)
        #         else:
        #             return pd.Series(data={
        #                 field: self.data_portal.get_adjusted_value(
        #                     asset,
        #                     field,
        #                     self._get_current_minute(),
        #                     self.simulation_dt_func(),
        #                     self.data_frequency
        #                 )
        #                 for field in fields
        #             }, index=fields, name=assets.symbol)
        # else:
        #     if not multiple_fields:
        #         field = fields
        #
        #         # assume assets is iterable
        #         # return a Series indexed by asset
        #         if not self._adjust_minutes:
        #             return pd.Series(data={
        #                 asset: self.data_portal.get_spot_value(
        #                     asset,
        #                     field,
        #                     self._get_current_minute(),
        #                     self.data_frequency
        #                 )
        #                 for asset in assets
        #             }, index=assets, name=fields)
        #         else:
        #             return pd.Series(data={
        #                 asset: self.data_portal.get_adjusted_value(
        #                     asset,
        #                     field,
        #                     self._get_current_minute(),
        #                     self.simulation_dt_func(),
        #                     self.data_frequency
        #                 )
        #                 for asset in assets
        #             }, index=assets, name=fields)

            # else:
                # both assets and fields are iterable
        data = {}

        if not self._adjust_minutes:
            for field in fields:
                # series = pd.Series(data={
                #     asset: self.data_portal.get_spot_value(
                #         assets=[asset],
                #         field=field,
                #         dt=self._get_current_minute(),
                #         data_frequency=self.data_frequency
                #     )
                #     for asset in assets
                # }, index=assets, name=field)
                # data[field] = series
                return self.data_portal.get_spot_value(
                        assets=assets,
                        fields=[field],
                        dt=self._get_current_minute(),
                        frequency=self.data_frequency
                    )
        else:
            for field in fields:
                series = pd.Series(data={
                    asset: self.data_portal.get_adjusted_value(
                        asset,
                        field,
                        self._get_current_minute(),
                        self.simulation_dt_func(),
                        self.data_frequency
                    )
                    for asset in assets
                }, index=assets, name=field)
                data[field] = series

        return pd.DataFrame(data=data)

    def current_chain(self, continuous_future: ContinuousFuture):
        return self.data_portal.get_current_future_chain(
            continuous_future=continuous_future,
            dt=self.simulation_dt_func()
        )

    # @check_parameters(('assets',), (Asset,))
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
        assets: zipline.assets.Asset or iterable of zipline.assets.Asset
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

    def _can_trade_for_asset(self, asset: Asset, dt: pd.Timestamp, adjusted_dt: pd.Timestamp) -> bool:
        session_label = None
        dt_to_use_for_exchange_check = None

        if self._is_restricted(assets=[asset], dt=adjusted_dt):
            return False

        session_label = self._trading_calendar.minute_to_session(minute=dt)

        if not asset.is_alive_for_session(session_label=session_label):
            # asset isn't alive
            return False

        if asset.auto_close_date and session_label > asset.auto_close_date:
            return False

        if not self._daily_mode:
            # Find the next market minute for this calendar, and check if this
            # asset's exchange is open at that minute.
            if self._trading_calendar.is_open_on_minute(minute=dt):
                dt_to_use_for_exchange_check = dt
            else:
                dt_to_use_for_exchange_check = self._trading_calendar.next_open(minute=dt)

            if not asset.is_exchange_open(dt_minute=dt_to_use_for_exchange_check):
                return False

        # is there a last price?
        return not np.isnan(
            self.data_portal.get_spot_value(
                assets=[asset], field="price", dt=adjusted_dt, data_frequency=self.data_frequency
            )
        )

    # @check_parameters(('assets',), (Asset,))
    def is_stale(self, assets: list[Asset]):
        """For the given asset or iterable of assets, returns True if the asset
        is alive and there is no trade data for the current simulation time.

        If the asset has never traded, returns False.

        If the current simulation time is not a valid market time, we use the
        current time to check if the asset is alive, but we use the last
        market minute/day for the trade data check.

        Parameters
        ----------
        assets: zipline.assets.Asset or iterable of zipline.assets.Asset
            Asset(s) for which staleness should be determined.

        Returns
        -------
        is_stale : bool or pd.Series[bool]
            Bool or series of bools indicating whether the requested asset(s)
            are stale.
        """
        dt = self.simulation_dt_func()
        if self._adjust_minutes:
            adjusted_dt = self._get_current_minute()
        else:
            adjusted_dt = dt

        if isinstance(assets, Asset):
            return self._is_stale_for_asset(
                asset=assets, dt=dt, adjusted_dt=adjusted_dt
            )
        else:
            return pd.Series(data={
                asset: self._is_stale_for_asset(
                    asset=asset, dt=dt, adjusted_dt=adjusted_dt
                )
                for asset in assets
            })

    def _is_stale_for_asset(self, asset: Asset, dt: pd.Timestamp, adjusted_dt: pd.Timestamp) -> bool:
        session_label = dt.normalize()  # FIXME

        if not asset.is_alive_for_session(session_label):
            return False

        current_volume = self.data_portal.get_spot_value(
            assets=[asset], field="volume", dt=adjusted_dt, data_frequency=self.data_frequency
        )

        if current_volume > 0:
            # found a current value, so we know this asset is not stale.
            return False
        else:
            # we need to distinguish between if this asset has ever traded
            # (stale = True) or has never traded (stale = False)
            last_traded_dt = \
                self.data_portal.get_spot_value(assets=[asset], field="last_traded", dt=adjusted_dt,
                                           data_frequency=self.data_frequency)

            return not (last_traded_dt is pd.NaT)

    def history(self, assets: list[Asset], fields: list[str], bar_count: int, frequency: datetime.timedelta):
        """Returns a trailing window of length ``bar_count`` with data for
        the given assets, fields, and frequency, adjusted for splits, dividends,
        and mergers as of the current simulation time.

        The semantics for missing data are identical to the ones described in
        the notes for :meth:`current`.

        Parameters
        ----------
        assets: zipline.assets.Asset or iterable of zipline.assets.Asset
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
        """

        df = self.data_portal.get_history_window(assets=assets,
                                                 end_dt=self._get_current_minute(),
                                                 bar_count=bar_count,
                                                 frequency=frequency,
                                                 fields=fields,
                                                 )

        if self._adjust_minutes:
            adjs = {
                field: self.data_portal.get_adjustments(
                    assets,
                    field,
                    self._get_current_minute(),
                    self.simulation_dt_func()
                )[0] for field in fields
            }

            df = {
                field: df * adjs[field]
                for field, df in df.items()
            }
        return df

    @property
    def current_dt(self):
        return self.simulation_dt_func()

    @property
    def fetcher_assets(self):
        return self.data_portal.get_fetcher_assets(self.simulation_dt_func())

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

    @property
    def current_session_minutes(self):
        return self._trading_calendar.session_minutes(
            self.current_session
        )

