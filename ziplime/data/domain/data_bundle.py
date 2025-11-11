import datetime
from functools import reduce, lru_cache
from operator import mul
from typing import Any

import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.equity import Equity
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.services.data_source import DataSource
from ziplime.utils.date_utils import period_to_timedelta


class DataBundle(DataSource):

    def __init__(self, name: str,
                 version: str,
                 start_date: datetime.date,
                 end_date: datetime.date,
                 trading_calendar: ExchangeCalendar,
                 frequency: datetime.timedelta | Period,
                 original_frequency: datetime.timedelta | Period,
                 data_type: DataType,
                 timestamp: datetime.datetime,
                 data: pl.DataFrame = None,
                 sid_indexes: dict[int, tuple[int, int]] = None):
        super().__init__(name=name,
                         start_date=start_date,
                         end_date=end_date,
                         frequency=frequency,
                         data_type=data_type,
                         original_frequency=original_frequency
                         )
        self.version = version
        self.start_date = start_date
        self.end_date = end_date
        self.trading_calendar = trading_calendar
        self.frequency = frequency
        self.frequency_td = period_to_timedelta(self.frequency)
        self.timestamp = timestamp
        self.data = data
        self.sid_indexes = sid_indexes
        self._logger = structlog.get_logger(__name__)

    def get_dataframe(self) -> pl.DataFrame:
        return self.data

    @lru_cache
    def get_dataframe_with_columns(self, columns: frozenset[str]) -> pl.DataFrame:
        return self.data.select(pl.col(col) for col in columns)

    @lru_cache
    def get_dataframe_by_sid_and_columns(self, sid: str, columns: frozenset[str]) -> pl.DataFrame:
        return self.data.select(pl.col(col) for col in columns).filter(pl.col("sid").is_in(sid))

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:

        cols = set(fields.union({"date", "sid"}))
        if include_bounds:
            asset_sid = [asset.sid for asset in assets][0]

            if len(assets) == 1:
                df = self.get_dataframe_by_sid_and_columns(sid=asset_sid, columns=cols).filter(
                    pl.col("date") <= to_date,
                    pl.col("date") >= from_date,
                )
            else:
                df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") <= to_date,
                    pl.col("date") >= from_date,
                    pl.col("sid").is_in([asset.sid for asset in assets])
                ).group_by(pl.col("sid")).all()
        else:
            asset_sid = [asset.sid for asset in assets][0]
            if len(assets) == 1:
                df = self.get_dataframe_by_sid_and_columns(sid=asset_sid, columns=cols).filter(
                    pl.col("date") < to_date,
                    pl.col("date") > from_date,
                )
            else:
                df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") < to_date,
                    pl.col("date") > from_date,
                    pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).all()
        if self.frequency < frequency:
            df = df.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
        return df.sort(by="date")

    def get_missing_data_by_limit(self, fields: frozenset[str],
                                  limit: int,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta | Period,
                                  assets: frozenset[Asset],
                                  include_end_date: bool,
                                  ) -> pl.DataFrame:

        return self.missing_data_bundle_source.get_data_sync(
            symbols=[asset.get_symbol_by_exchange(None) for asset in assets], frequency=frequency,
            date_from=end_date - frequency * limit,
            date_to=end_date)

    # @lru_cache(maxsize=100)
    def get_data_by_limit(self, fields: frozenset[str] | None,
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta | Period,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        frequency_td = period_to_timedelta(frequency)
        asset_sid = [asset.sid for asset in assets][0]

        total_bar_count = limit
        if end_date > self.end_date:
            raise ValueError(f"Requested end date {end_date} is greater than end date {self.end_date} of the bundle.")
            return self.get_missing_data_by_limit(frequency=frequency, assets=assets, fields=fields,
                                                  limit=limit, include_end_date=include_end_date,
                                                  end_date=end_date
                                                  )  # pl.DataFrame() # we have missing data

        if self.frequency_td < frequency_td:
            multiplier = int(frequency_td / self.frequency_td)
            total_bar_count = limit * multiplier
        df = self.get_dataframe()
        if fields is None:
            fields = frozenset(df.columns)
        cols = list(fields.union({"date", "sid"}))

        if include_end_date:
            if len(assets) == 1:
                sid_index = self.sid_indexes[asset_sid]
                df_raw = self.get_dataframe()[sid_index[0]:sid_index[1]].select(pl.col(col) for col in cols).filter(
                    pl.col("date") <= end_date,
                ).tail(total_bar_count)
            else:
                df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") <= end_date,
                    pl.col("sid").is_in([asset.sid for asset in assets])
                ).group_by(pl.col("sid")).tail(total_bar_count).sort(by="date")
        else:
            if len(assets) == 1:
                sid_index = self.sid_indexes[asset_sid]
                df_raw = self.get_dataframe()[sid_index[0]:sid_index[1]].select(pl.col(col) for col in cols).filter(
                    pl.col("date") < end_date,
                ).tail(
                    total_bar_count).sort(by="date")

            else:
                df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") < end_date,
                    pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).tail(
                    total_bar_count).sort(by="date")

        if self.frequency_td < frequency_td:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields).tail(
                limit)
            return df
        return df_raw

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime,
                       frequency: datetime.timedelta):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset, ContinuousFuture, or iterable of same.
            The asset or assets whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        # print(f"get spot value: {assets}, {fields}, {dt}")
        df_raw = self.get_data_by_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=assets,
            include_end_date=True,
        )
        return df_raw

    async def get_adjusted_value(
            self, asset: Asset, field: str, dt: datetime.datetime, perspective_dt: datetime.datetime,
            data_frequency: datetime.timedelta,
            spot_value: float = None
    ):
        """Returns a scalar value representing the value
        of the desired asset's field at the given dt with adjustments applied.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The value of the given ``field`` for ``asset`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open',
            'high', 'low', 'close', or 'price', the value will be a float. If
            the ``field`` is 'volume' the value will be a int. If the ``field``
            is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            spot_value = self.get_spot_value(assets=frozenset({asset}), fields=frozenset({field}), dt=dt,
                                             data_frequency=data_frequency)

        if isinstance(asset, Equity):  # TODO: fix this, not valid way to check if it is equity
            ratio = self.get_adjustments(assets=frozenset({asset}), field=field, dt=dt, perspective_dt=perspective_dt)[
                0]
            spot_value *= ratio

        return spot_value

    async def _get_adjustment_list(self, asset: Asset, adjustments_dict: dict[str, Any], table_name: str):
        """Internal method that returns a list of adjustments for the given sid.

        Parameters
        ----------
        asset : Asset
            The asset for which to return adjustments.

        adjustments_dict: dict
            A dictionary of sid -> list that is used as a cache.

        table_name: string
            The table that contains this data in the adjustments db.

        Returns
        -------
        adjustments: list
            A list of [multiplier, datetime.datetime], earliest first

        """
        if self.adjustment_repository is None:
            return []

        sid = asset.sid

        try:
            adjustments = adjustments_dict[sid]
        except KeyError:
            adjustments = adjustments_dict[
                sid
            ] = self.adjustment_repository.get_adjustments_for_sid(table_name, sid)

        return adjustments

    async def get_current_future_chain(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        """Retrieves the future chain for the contract at the given `dt` according
        the `continuous_future` specification.

        Returns
        -------

        future_chain : list[Future]
            A list of active futures, where the first index is the current
            contract specified by the continuous future definition, the second
            is the next upcoming contract and so on.
        """
        rf = self._roll_finders[continuous_future.roll_style]
        session = self.trading_calendar.minute_to_session(dt)
        contract_center = rf.get_contract_center(
            continuous_future.root_symbol, session, continuous_future.offset
        )
        oc = self.asset_repository.get_ordered_contracts(continuous_future.root_symbol)
        chain = oc.active_chain(contract_center, session.value)
        return self.asset_repository.retrieve_all(sids=chain)

    async def _get_current_contract(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_sid = rf.get_contract_center(
            continuous_future.root_symbol, dt, continuous_future.offset
        )
        if contract_sid is None:
            return None
        return self.asset_repository.retrieve_asset(sid=contract_sid)

    async def get_adjustments(self, assets: frozenset[Asset], field: str, dt: datetime.datetime,
                              perspective_dt: datetime.datetime):
        """Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of assets

        Parameters
        ----------
        assets : list of type Asset, or Asset
            The asset, or assets whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.

        Returns
        -------
        adjustments : list[Adjustment]
            The adjustments to that field.
        """
        adjustment_ratios_per_asset = []

        def split_adj_factor(x):
            return x if field != "volume" else 1.0 / x

        for asset in assets:
            adjustments_for_asset = []
            split_adjustments = self._get_adjustment_list(
                asset, self._splits_dict, "SPLITS"
            )
            for adj_dt, adj in split_adjustments:
                if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                    adjustments_for_asset.append(split_adj_factor(adj))
                elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                    break

            if field != "volume":
                merger_adjustments = self._get_adjustment_list(
                    asset, self._mergers_dict, "MERGERS"
                )
                for adj_dt, adj in merger_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt > perspective_dt:
                        break

                dividend_adjustments = self._get_adjustment_list(
                    asset,
                    self._dividends_dict,
                    "DIVIDENDS",
                )
                for adj_dt, adj in dividend_adjustments:
                    if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                        break

            ratio = reduce(mul, adjustments_for_asset, 1.0)
            adjustment_ratios_per_asset.append(ratio)

        return adjustment_ratios_per_asset
