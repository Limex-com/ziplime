import datetime
from functools import reduce, lru_cache
from operator import mul
from typing import Any

import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.domain.roll_finder import ROLL_FINDERS, VolumeRollFinder
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
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
                 sid_indexes: dict[int, tuple[int, int]] = None,
                 asset_service=None,
                 roll_finder_settings: dict[str, dict] | None = None):
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
        self.asset_service = asset_service
        # Both roll styles move a session before auto close by default, so the strategy closes the
        # outgoing leg itself. Rolling exactly on auto close means the engine has already
        # liquidated the position, and that liquidation pays neither commission nor slippage.
        self._roll_finder_settings = {
            "calendar": {"roll_offset_days": 1},
            "volume": {"grace_period_days": 1},
            **(roll_finder_settings or {}),
        }
        self._roll_finders = {}
        self._logger = structlog.get_logger(__name__)

    def get_roll_finder(self, roll_style: str):
        """Return (and memoise) the roll finder for ``roll_style``.

        ``_roll_finders`` used to be read but never populated, which made every continuous-future
        lookup fail with ``AttributeError``.
        """
        if self.asset_service is None:
            raise ValueError(
                "This data bundle was loaded without an asset service, so continuous futures "
                "cannot be resolved to a contract. Pass asset_service when loading the bundle."
            )
        if roll_style not in self._roll_finders:
            finder_class = ROLL_FINDERS.get(roll_style)
            if finder_class is None:
                raise ValueError(f"Unknown roll style {roll_style!r}. "
                                 f"Allowed roll styles are {sorted(ROLL_FINDERS)}.")
            settings = self._roll_finder_settings.get(roll_style, {})
            if finder_class is VolumeRollFinder:
                self._roll_finders[roll_style] = finder_class(asset_service=self.asset_service,
                                                              data_source=self, **settings)
            else:
                self._roll_finders[roll_style] = finder_class(asset_service=self.asset_service,
                                                              **settings)
        return self._roll_finders[roll_style]

    def get_dataframe(self) -> pl.DataFrame:
        return self.data

    @lru_cache
    def get_dataframe_with_columns(self, columns: frozenset[str]) -> pl.DataFrame:
        return self.data.select(pl.col(col) for col in columns)

    @lru_cache
    def get_dataframe_by_sid_and_columns(self, sid: str, columns: frozenset[str]) -> pl.DataFrame:

        return self.data.select(pl.col(col) for col in columns).filter(pl.col("sid").is_in(sid))

    def get_data_by_date_and_sids(self, fields: frozenset[str],
                                  start_date: datetime.datetime,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta | Period,
                                  sids: frozenset[int],
                                  include_bounds: bool,
                                  ) -> pl.DataFrame:

        frequency_td = period_to_timedelta(frequency)
        sids_list = list(sids)
        asset_sid = sids_list[0]

        if end_date > self.end_date:
            raise ValueError(f"Requested end date {end_date} is greater than end date {self.end_date} of the bundle.")
        if start_date < self.start_date:
            raise ValueError(
                f"Requested start date {start_date} is lower than start date {self.start_date} of the bundle.")

        df = self.get_dataframe()
        if fields is None:
            fields = frozenset(df.columns)
        cols = list(fields.union({"date", "sid"}))

        if include_bounds:
            if len(sids) == 1:
                sid_index = self.sid_indexes[asset_sid]
                df_raw = self.get_dataframe()[sid_index[0]:sid_index[1]].select(pl.col(col) for col in cols).filter(
                    pl.col("date") >= start_date,
                    pl.col("date") <= end_date,
                )
            else:
                df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") >= start_date,
                    pl.col("date") <= end_date,
                    pl.col("sid").is_in(sids_list)
                ).sort(by=["sid", "date"])
        else:
            if len(sids) == 1:
                sid_index = self.sid_indexes[asset_sid]
                df_raw = self.get_dataframe()[sid_index[0]:sid_index[1]].select(pl.col(col) for col in cols).filter(
                    pl.col("date") > start_date,
                    pl.col("date") < end_date,
                ).sort(by="date")

            else:
                df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                    pl.col("date") > start_date,
                    pl.col("date") < end_date,
                    pl.col("sid").is_in(sids_list)).sort(by=["sid", "date"])

        if self.frequency_td < frequency_td:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
            return df
        return df_raw

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:
        return self.get_data_by_date_and_sids(fields=fields, from_date=from_date,
                                              to_date=to_date, frequency=frequency,
                                              sids=frozenset(asset.sid for asset in assets),
                                              include_bounds=include_bounds)
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
    async def get_data_by_limit(self, fields: frozenset[str] | None,
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta | Period,
                                assets: frozenset[ExchangeAsset | ContinuousFuture],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        """Return up to ``limit`` bars per asset ending at ``end_date``.

        Any :class:`ContinuousFuture` in ``assets`` is resolved to the contracts it held over the
        window and returned as one spliced, back-adjusted series carried under the continuous
        future's own sid.
        """
        continuous_futures = [a for a in assets if isinstance(a, ContinuousFuture)]
        concrete = frozenset(a for a in assets if not isinstance(a, ContinuousFuture))

        if not continuous_futures:
            return await self._get_data_by_limit_for_assets(
                fields=fields, limit=limit, end_date=end_date, frequency=frequency,
                assets=concrete, include_end_date=include_end_date)

        frames = []
        if concrete:
            frames.append(await self._get_data_by_limit_for_assets(
                fields=fields, limit=limit, end_date=end_date, frequency=frequency,
                assets=concrete, include_end_date=include_end_date))
        for continuous_future in continuous_futures:
            frame = await self._get_continuous_future_data(
                continuous_future=continuous_future, fields=fields, limit=limit,
                end_date=end_date, frequency=frequency, include_end_date=include_end_date)
            if not frame.is_empty():
                frames.append(frame)
        frames = [frame for frame in frames if not frame.is_empty()]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal").sort(by="date")

    async def _get_data_by_limit_for_assets(self, fields: frozenset[str] | None,
                                            limit: int,
                                            end_date: datetime.datetime,
                                            frequency: datetime.timedelta | Period,
                                            assets: frozenset[ExchangeAsset],
                                            include_end_date: bool,
                                            ) -> pl.DataFrame:
        frequency_td = period_to_timedelta(frequency)
        assets_list = [asset for asset in assets]
        asset_sid = assets_list[0].sid

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
                # if total_bar_count == 1:
                #     sid_index = self.asset_sid_date_index[(asset_sid, end_date)]
                #     df_raw = self.get_dataframe()[sid_index].select(pl.col(col) for col in cols)
                # else:
                try:
                    sid_index = self.sid_indexes[asset_sid]
                except KeyError:
                    raise ValueError(f"Data for asset sid={asset_sid}, symbol={assets_list[0].symbol}, mic={assets_list[0].mic} requested but it is not found in the loaded bundle.")
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

    #: Price columns spliced across a roll. ``volume`` is left untouched -- adjusting it would
    #: misreport how much actually traded in the contract that was held.
    PRICE_FIELDS = ("open", "high", "low", "close", "price")

    #: Minimum window to look back over when working out which contracts a continuous future held.
    MIN_ROLL_LOOKBACK = datetime.timedelta(days=400)

    async def current_contract(self, continuous_future: ContinuousFuture,
                               dt: datetime.datetime) -> ExchangeAsset | None:
        """Return the contract ``continuous_future`` holds at ``dt``."""
        roll_finder = self.get_roll_finder(continuous_future.roll_style)
        return await roll_finder.get_contract_center(
            root_symbol=continuous_future.root_symbol, dt=dt, offset=continuous_future.offset)

    async def _get_continuous_future_data(self, continuous_future: ContinuousFuture,
                                          fields: frozenset[str] | None,
                                          limit: int,
                                          end_date: datetime.datetime,
                                          frequency: datetime.timedelta | Period,
                                          include_end_date: bool) -> pl.DataFrame:
        """Splice the contracts a continuous future held into one adjusted series.

        Each roll segment is read from the contract that was actually held, then the older segments
        are shifted onto the newest one's price level so that returns across a roll reflect the
        position rather than the gap between two contracts.
        """
        lookback = max(period_to_timedelta(frequency) * limit * 3, self.MIN_ROLL_LOOKBACK)
        window_start = max(end_date - lookback, self._as_datetime(self.start_date))

        roll_finder = self.get_roll_finder(continuous_future.roll_style)
        rolls = await roll_finder.get_rolls(root_symbol=continuous_future.root_symbol,
                                            start=window_start, end=end_date,
                                            offset=continuous_future.offset)
        if not rolls:
            self._logger.warning("No contracts found for continuous future",
                                 continuous_future=str(continuous_future),
                                 start=window_start, end=end_date)
            return pl.DataFrame()

        segments = []
        segment_start = window_start
        for contract, roll_date in rolls:
            # A segment runs from the previous roll up to (but not including) its own roll date;
            # bounding both ends matters, because a contract has bars well outside the window it
            # is actually held for and they would otherwise be spliced in twice.
            segment_end = self._as_datetime(roll_date) if roll_date is not None else None
            fetch_end = min(segment_end, end_date) if segment_end is not None else end_date

            if contract.sid not in (self.sid_indexes or {}) and not self._has_sid(contract.sid):
                self._logger.warning("Contract missing from bundle, skipping roll segment",
                                     symbol=contract.symbol, sid=contract.sid,
                                     continuous_future=str(continuous_future))
                segment_start = segment_end or segment_start
                continue

            frame = await self._get_data_by_limit_for_assets(
                fields=fields, limit=limit, end_date=fetch_end,
                frequency=frequency, assets=frozenset({contract}),
                include_end_date=include_end_date or segment_end is not None)
            if not frame.is_empty():
                frame = frame.filter(pl.col("date") >= segment_start)
                if segment_end is not None:
                    frame = frame.filter(pl.col("date") < segment_end)
                if not frame.is_empty():
                    segments.append((contract, frame))
            segment_start = segment_end or segment_start

        if not segments:
            return pl.DataFrame()

        segments = self._adjust_roll_segments(segments, adjustment=continuous_future.adjustment,
                                              continuous_future=continuous_future)
        combined = pl.concat([frame for _, frame in segments], how="diagonal").sort(by="date")
        combined = combined.with_columns(pl.lit(continuous_future.sid).cast(pl.Int64).alias("sid"))
        return combined.tail(limit)

    def _adjust_roll_segments(self, segments: list[tuple[ExchangeAsset, pl.DataFrame]],
                              adjustment: str | None,
                              continuous_future: ContinuousFuture,
                              ) -> list[tuple[ExchangeAsset, pl.DataFrame]]:
        """Shift each segment onto the price level of the newest one.

        Walking backwards from the most recent segment, the ratio (``mul``) or difference (``add``)
        between the incoming and outgoing contract on the last shared session is accumulated and
        applied to everything older.
        """
        if adjustment is None or len(segments) < 2:
            return segments

        price_columns = [c for c in self.PRICE_FIELDS if c in segments[0][1].columns]
        if not price_columns:
            return segments

        adjusted = [segments[-1]]
        factor = 1.0 if adjustment == "mul" else 0.0
        for index in range(len(segments) - 2, -1, -1):
            older_contract, older_frame = segments[index]
            newer_contract, _ = segments[index + 1]
            roll_dt = older_frame["date"][-1]

            older_price = self._price_at(older_contract.sid, roll_dt)
            newer_price = self._price_at(newer_contract.sid, roll_dt)
            if older_price is None or newer_price is None:
                self._logger.warning(
                    "No overlapping price at roll; leaving segment unadjusted",
                    continuous_future=str(continuous_future), roll_date=roll_dt,
                    outgoing=older_contract.symbol, incoming=newer_contract.symbol)
            elif adjustment == "mul" and older_price == 0:
                # A futures price can legitimately be zero or negative (CL, April 2020). A ratio
                # through zero is undefined, so the ratio method declines rather than emitting
                # inf; use adjustment="add", which stays well defined.
                self._logger.warning(
                    "Multiplicative adjustment is undefined across a zero price; leaving segment "
                    "unadjusted. Use adjustment='add' for a series that crosses zero.",
                    continuous_future=str(continuous_future), roll_date=roll_dt,
                    outgoing=older_contract.symbol, incoming=newer_contract.symbol)
            elif adjustment == "mul":
                factor *= newer_price / older_price
            else:
                factor += newer_price - older_price

            if adjustment == "mul":
                older_frame = older_frame.with_columns(
                    [(pl.col(c) * factor).alias(c) for c in price_columns])
            else:
                older_frame = older_frame.with_columns(
                    [(pl.col(c) + factor).alias(c) for c in price_columns])
            adjusted.insert(0, (older_contract, older_frame))
        return adjusted

    def _price_at(self, sid: int, dt: datetime.datetime) -> float | None:
        """Return the close of ``sid`` at ``dt``, or ``None`` if it did not trade then."""
        df = self.get_dataframe()
        if df is None or df.is_empty():
            return None
        column = "close" if "close" in df.columns else "price"
        if column not in df.columns:
            return None
        match = df.filter(pl.col("sid") == sid, pl.col("date") == dt).select(column)
        if match.is_empty():
            return None
        value = match[column][0]
        return float(value) if value is not None else None

    def _has_sid(self, sid: int) -> bool:
        df = self.get_dataframe()
        if df is None or df.is_empty():
            return False
        return not df.filter(pl.col("sid") == sid).limit(1).is_empty()

    def _as_datetime(self, value) -> datetime.datetime:
        """Normalise a date or datetime to a tz-aware datetime in the calendar's timezone."""
        if isinstance(value, datetime.datetime):
            return value if value.tzinfo else value.replace(tzinfo=self.trading_calendar.tz)
        return datetime.datetime.combine(value, datetime.time.min,
                                         tzinfo=self.trading_calendar.tz)

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
            self, asset: ExchangeAsset, field: str, dt: datetime.datetime, perspective_dt: datetime.datetime,
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

    async def _get_adjustment_list(self, asset: ExchangeAsset, adjustments_dict: dict[str, Any], table_name: str):
        """Internal method that returns a list of adjustments for the given sid.

        Parameters
        ----------
        asset : ExchangeAsset
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

    async def get_current_future_chain(self, continuous_future: ContinuousFuture,
                                       dt: datetime.datetime) -> list[ExchangeAsset]:
        """Return the active contracts of the chain at ``dt``, front contract first.

        Previously this read ``self._roll_finders`` and ``self.asset_repository``, neither of which
        was ever assigned.
        """
        roll_finder = self.get_roll_finder(continuous_future.roll_style)
        contract = await roll_finder.get_contract_center(
            root_symbol=continuous_future.root_symbol, dt=dt, offset=continuous_future.offset)
        if contract is None:
            return []
        ordered_contracts = await roll_finder.get_ordered_contracts(continuous_future.root_symbol)
        session = dt.date() if isinstance(dt, datetime.datetime) else dt
        return ordered_contracts.active_chain(starting_sid=contract.sid, dt=session)

    async def _get_current_contract(self, continuous_future: ContinuousFuture,
                                    dt: datetime.datetime) -> ExchangeAsset | None:
        return await self.current_contract(continuous_future=continuous_future, dt=dt)

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
