import datetime
import sqlite3
from collections import namedtuple
from functools import lru_cache
from itertools import chain
from typing import Self, Any
import polars as pl
import numpy as np
import pandas as pd
import structlog
from numpy import integer as any_integer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.divident_payout_model import DividendPayoutModel
from ziplime.assets.models.merger_model import MergerModel
from ziplime.assets.models.split_model import SplitModel
from ziplime.assets.models.stock_dividend_payout_model import StockDividendPayoutModel
from ziplime.lib.adjustment import Float64Multiply
from ziplime.utils.functional import keysorted
from ziplime.utils.numpy_utils import (
    datetime64ns_dtype,
    float64_dtype,
    int64_dtype,
    uint32_dtype,
    uint64_dtype,
)
from ziplime.utils.pandas_utils import empty_dataframe
from ziplime.utils.sqlite_utils import group_into_chunks, SQLITE_MAX_VARIABLE_NUMBER

from ziplime.data.adjustments import _lookup_dt, ADJ_QUERY_TEMPLATE, SID_QUERIES

from ziplime.assets.repositories.adjustments_repository import AdjustmentRepository

log = structlog.get_logger(__name__)

SQLITE_ADJUSTMENT_TABLENAMES = frozenset(["splits", "dividends", "mergers"])

UNPAID_QUERY_TEMPLATE = """
                        SELECT sid, amount, pay_date
                        from dividend_payouts
                        WHERE ex_date = ?
                          AND sid IN ({0}) \
                        """

# Dividend = namedtuple("Dividend", ["asset", "amount", "pay_date"])

UNPAID_STOCK_DIVIDEND_QUERY_TEMPLATE = """
                                       SELECT sid, payment_sid, ratio, pay_date
                                       from stock_dividend_payouts
                                       WHERE ex_date = ?
                                         AND sid IN ({0}) \
                                       """

StockDividend = namedtuple(
    "StockDividend",
    ["asset", "payment_asset", "ratio", "pay_date"],
)

SQLITE_ADJUSTMENT_COLUMN_DTYPES = {
    "effective_date": any_integer,
    "ratio": float64_dtype,
    "sid": any_integer,
}




class SqlAlchemyAdjustmentRepository(AdjustmentRepository):
    """Loads adjustments based on corporate actions from a SQLite database.

    Expects data written in the format output by `SQLiteAdjustmentWriter`.

    Parameters
    ----------
    conn : str or sqlite3.Connection
        Connection from which to load data.

    See Also
    --------
    :class:`ziplime.data.adjustments.SQLiteAdjustmentWriter`
    """

    def __init__(self, db_url: str):
        self.db_url = db_url

    def __enter__(self):
        return self

    @property
    @lru_cache
    def session_maker(self) -> async_sessionmaker[AsyncSession]:
        engine = create_async_engine(self.db_url, pool_pre_ping=True, pool_size=20)
        session_maker = async_sessionmaker(autocommit=False, autoflush=True, bind=engine, class_=AsyncSession,
                                           expire_on_commit=False)
        return session_maker


    async def _get_split_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:
        q = select(SplitModel.asset_id).where(
            SplitModel.effective_date >= start_date,
            SplitModel.effective_date <= end_date,
        ).distinct()
        return set((await db.execute(q)).scalars())

    async def _get_merger_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:
        q = select(MergerModel.asset_id).where(
            MergerModel.effective_date >= start_date,
            MergerModel.effective_date <= end_date,
        ).distinct()
        return set((await db.execute(q)).scalars())

    async def _get_dividend_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:
        q = select(DividendPayoutModel.asset_id).where(
            DividendPayoutModel.ex_date >= start_date,
            DividendPayoutModel.ex_date <= end_date,
        ).distinct()
        return set((await db.execute(q)).scalars())

    async def _adjustments(self,
                     adjustments_db: AsyncSession,
                     split_sids: set,
                     merger_sids: set,
                     dividends_sids: set,
                     start_date: int,
                     end_date: int,
                     assets: pd.Index):

        async def fetch(model, date_column, selected_ids):
            if not selected_ids:
                return []
            rows = []
            for chunk in group_into_chunks(selected_ids):
                q = select(model.asset_id, model.ratio, date_column).where(
                    model.asset_id.in_(chunk),
                    date_column >= start_date,
                    date_column <= end_date,
                )
                rows.extend((await adjustments_db.execute(q)).all())
            return rows

        return (
            await fetch(SplitModel, SplitModel.effective_date, split_sids & set(assets)),
            await fetch(MergerModel, MergerModel.effective_date, merger_sids & set(assets)),
            await fetch(DividendPayoutModel, DividendPayoutModel.ex_date, dividends_sids & set(assets)),
        )

    async def load_adjustments_from_sqlite(self,
                                           db_session: AsyncSession,
                                           dates: pd.DatetimeIndex,
                                           assets: pd.Index,
                                           should_include_splits: bool,
                                           should_include_mergers: bool,
                                           should_include_dividends: bool,
                                           adjustment_type: str):
        """Load a dictionary of Adjustment objects from adjustments_db.

        Parameters
        ----------
        adjustments_db : sqlite3.Connection
            Connection to a sqlite3 table in the format written by
            SQLiteAdjustmentWriter.
        dates : pd.DatetimeIndex
            Dates for which adjustments are needed.
        assets : pd.Int64Index
            Assets for which adjustments are needed.
        should_include_splits : bool
            Whether split adjustments should be included.
        should_include_mergers : bool
            Whether merger adjustments should be included.
        should_include_dividends : bool
            Whether dividend adjustments should be included.
        adjustment_type : str
            Whether price adjustments, volume adjustments, or both, should be
            included in the output.

        Returns
        -------
        adjustments : dict[str -> dict[int -> Adjustment]]
            A dictionary containing price and/or volume adjustment mappings from
            index to adjustment objects to apply at that index.
        """

        if not (adjustment_type == 'price' or
                adjustment_type == 'volume' or
                adjustment_type == 'all'):
            raise ValueError(
                "%s is not a valid adjustment type.\n"
                "Valid adjustment types are 'price', 'volume', and 'all'.\n" % (
                    adjustment_type,
                )
            )

        should_include_price_adjustments = bool(
            adjustment_type == 'all' or adjustment_type == 'price'
        )
        should_include_volume_adjustments = bool(
            adjustment_type == 'all' or adjustment_type == 'volume'
        )

        if not should_include_price_adjustments:
            should_include_mergers = False
            should_include_dividends = False

        start_date = dates[0].to_pydatetime().date()
        end_date = dates[-1].to_pydatetime().date()
        # TODO: localize dates for adjustments
        # start_date = dates[0].tz_localize(self.trading_calendar.tz).to_pydatetime().date()
        # end_date = dates[-1].tz_localize(self.trading_calendar.tz).to_pydatetime().date()

        if should_include_splits:
            split_sids = await self._get_split_sids(
                db_session,
                start_date,
                end_date,
            )
        else:
            split_sids = set()

        if should_include_mergers:
            merger_sids = await self._get_merger_sids(
                db_session,
                start_date,
                end_date,
            )
        else:
            merger_sids = set()

        if should_include_dividends:
            dividend_sids = await self._get_dividend_sids(
                db_session,
                start_date,
                end_date,
            )
        else:
            dividend_sids = set()

        splits, mergers, dividends = await self._adjustments(
            db_session,
            split_sids,
            merger_sids,
            dividend_sids,
            start_date,
            end_date,
            assets,
        )

        price_adjustments = {}
        volume_adjustments = {}
        result = {}
        asset_ixs = {}  # Cache sid lookups here.
        date_ixs = {}

        _dates_seconds = \
            dates.values.astype('datetime64[s]').view(np.int64)

        def date_to_seconds(value: datetime.date) -> int:
            return int(pd.Timestamp(value, tz="UTC").timestamp())

        # Pre-populate date index cache.
        for i, dt in enumerate(_dates_seconds):
            date_ixs[dt] = i

        # splits affect prices and volumes, volumes is the inverse
        for sid, ratio, eff_date in splits:
            if eff_date < start_date:
                continue

            date_loc = _lookup_dt(date_ixs, date_to_seconds(eff_date), _dates_seconds)

            if sid not in asset_ixs:
                asset_ixs[sid] = assets.get_loc(sid)
            asset_ix = asset_ixs[sid]

            if should_include_price_adjustments:
                price_adj = Float64Multiply(0, date_loc, asset_ix, asset_ix, ratio)
                price_adjustments.setdefault(date_loc, []).append(price_adj)

            if should_include_volume_adjustments:
                volume_adj = Float64Multiply(
                    0, date_loc, asset_ix, asset_ix, 1.0 / ratio
                )
                volume_adjustments.setdefault(date_loc, []).append(volume_adj)

        # mergers and dividends affect prices only
        for sid, ratio, eff_date in chain(mergers, dividends):
            if eff_date < start_date:
                continue

            date_loc = _lookup_dt(date_ixs, date_to_seconds(eff_date), _dates_seconds)

            if sid not in asset_ixs:
                asset_ixs[sid] = assets.get_loc(sid)
            asset_ix = asset_ixs[sid]

            price_adj = Float64Multiply(0, date_loc, asset_ix, asset_ix, ratio)
            price_adjustments.setdefault(date_loc, []).append(price_adj)

        if should_include_price_adjustments:
            result['price'] = price_adjustments
        if should_include_volume_adjustments:
            result['volume'] = volume_adjustments

        return result

    async def load_adjustments(
            self,
            dates,
            assets,
            should_include_splits,
            should_include_mergers,
            should_include_dividends,
            adjustment_type,
    ):
        """Load collection of Adjustment objects from underlying adjustments db.

        Parameters
        ----------
        dates : pd.DatetimeIndex
            Dates for which adjustments are needed.
        assets : pd.Int64Index
            Assets for which adjustments are needed.
        should_include_splits : bool
            Whether split adjustments should be included.
        should_include_mergers : bool
            Whether merger adjustments should be included.
        should_include_dividends : bool
            Whether dividend adjustments should be included.
        adjustment_type : str
            Whether price adjustments, volume adjustments, or both, should be
            included in the output.

        Returns
        -------
        adjustments : dict[str -> dict[int -> Adjustment]]
            A dictionary containing price and/or volume adjustment mappings
            from index to adjustment objects to apply at that index.
        """
        dates = dates.tz_localize("UTC")

        async with self.session_maker() as session:
            return await self.load_adjustments_from_sqlite(
                session,
                dates,
                assets,
                should_include_splits,
                should_include_mergers,
                should_include_dividends,
                adjustment_type,
            )

    async def load_pricing_adjustments(self, columns, dates, assets):
        if "volume" not in set(columns):
            adjustment_type = "price"
        elif len(set(columns)) == 1:
            adjustment_type = "volume"
        else:
            adjustment_type = "all"

        adjustments = await self.load_adjustments(
            dates,
            assets,
            should_include_splits=True,
            should_include_mergers=True,
            should_include_dividends=True,
            adjustment_type=adjustment_type,
        )
        price_adjustments = adjustments.get("price")
        volume_adjustments = adjustments.get("volume")

        return [
            volume_adjustments if column == "volume" else price_adjustments
            for column in columns
        ]

    def get_adjustments_for_sid(self, table_name, sid):
        return []
        t = (sid,)
        c = self.conn.cursor()
        adjustments_for_sid = c.execute(
            "SELECT effective_date, ratio FROM %s WHERE sid = ?" % table_name, t
        ).fetchall()
        c.close()

        return [
            [pd.Timestamp(adjustment[0], unit="s"), adjustment[1]]
            for adjustment in adjustments_for_sid
        ]

    def get_dividends_with_ex_date(self, assets, date):
        # seconds = date.value / int(1e9)
        return []
        c = self.conn.cursor()

        divs = []
        for chunk in group_into_chunks(assets):
            query = UNPAID_QUERY_TEMPLATE.format(",".join(["?" for _ in chunk]))
            t = (date,) + tuple(map(lambda x: int(x), chunk))

            c.execute(query, t)

            rows = c.fetchall()
            for row in rows:
                div = Dividend(
                    asset_finder.retrieve_asset(row[0]),
                    row[1],
                    pd.Timestamp(row[2], unit="s", tz="UTC"),
                )
                divs.append(div)
        c.close()

        return divs

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[StockDividendPayoutModel]:
        return []

    async def get_stock_dividends_with_ex_date(self, assets, date):
        # seconds = date.value / int(1e9)
        return []

        c = self.conn.cursor()

        stock_divs = []
        for chunk in group_into_chunks(assets):
            query = UNPAID_STOCK_DIVIDEND_QUERY_TEMPLATE.format(
                ",".join(["?" for _ in chunk])
            )
            t = (date,) + tuple(map(lambda x: int(x), chunk))

            c.execute(query, t)

            rows = c.fetchall()

            for row in rows:
                stock_div = StockDividend(
                    asset_finder.retrieve_asset(row[0]),  # asset
                    asset_finder.retrieve_asset(row[1]),  # payment_asset
                    row[2],
                    pd.Timestamp(row[3], unit="s", tz="UTC"),
                )
                stock_divs.append(stock_div)
        c.close()

        return stock_divs


    def calc_dividend_ratios(self, dividends):
        """Calculate the ratios to apply to equities when looking back at pricing
        history so that the price is smoothed over the ex_date, when the market
        adjusts to the change in equity value due to upcoming dividend.

        Returns
        -------
        DataFrame
            A frame in the same format as splits and mergers, with keys
            - sid, the id of the equity
            - effective_date, the date in seconds on which to apply the ratio.
            - ratio, the ratio to apply to backwards looking pricing data.
        """
        if dividends is None or dividends.empty:
            return pd.DataFrame(
                np.array(
                    [],
                    dtype=[
                        ("sid", uint64_dtype),
                        ("effective_date", uint32_dtype),
                        ("ratio", float64_dtype),
                    ],
                )
            )

        pricing_reader = self._equity_daily_bar_reader
        input_sids = dividends.sid.values
        unique_sids, sids_ix = np.unique(input_sids, return_inverse=True)
        dates = pricing_reader.sessions.values

        (close,) = pricing_reader.load_raw_arrays(
            ["close"],
            pd.Timestamp(dates[0]),
            pd.Timestamp(dates[-1]),
            unique_sids,
        )
        date_ix = np.searchsorted(dates, dividends.ex_date.values)
        mask = date_ix > 0

        date_ix = date_ix[mask]
        sids_ix = sids_ix[mask]
        input_dates = dividends.ex_date.values[mask]

        # subtract one day to get the close on the day prior to the merger
        previous_close = close[date_ix - 1, sids_ix]
        input_sids = input_sids[mask]

        amount = dividends.amount.values[mask]
        ratio = 1.0 - amount / previous_close

        non_nan_ratio_mask = ~np.isnan(ratio)
        for ix in np.flatnonzero(~non_nan_ratio_mask):
            log.warning(
                "Couldn't compute ratio for dividend"
                " sid=%(sid)s, ex_date=%(ex_date)s, amount=%(amount).3f",
                {
                    "sid": input_sids[ix],
                    "ex_date": pd.Timestamp(input_dates[ix]).strftime("%Y-%m-%d"),
                    "amount": amount[ix],
                },
            )

        positive_ratio_mask = ratio > 0
        for ix in np.flatnonzero(~positive_ratio_mask & non_nan_ratio_mask):
            log.warning(
                "Dividend ratio <= 0 for dividend"
                " sid=%(sid)s, ex_date=%(ex_date)s, amount=%(amount).3f",
                {
                    "sid": input_sids[ix],
                    "ex_date": pd.Timestamp(input_dates[ix]).strftime("%Y-%m-%d"),
                    "amount": amount[ix],
                },
            )

        valid_ratio_mask = non_nan_ratio_mask & positive_ratio_mask
        return pd.DataFrame(
            {
                "sid": input_sids[valid_ratio_mask],
                "effective_date": input_dates[valid_ratio_mask],
                "ratio": ratio[valid_ratio_mask],
            }
        )


    async def get_splits(self, assets: frozenset[Asset], dt: datetime.date):
        """Returns any splits for the given sids and the given dt.

        Parameters
        ----------
        assets : container
            Assets for which we want splits.
        dt : datetime.datetime
            The date for which we are checking for splits. Note: this is
            expected to be midnight UTC.

        Returns
        -------
        splits : list[(asset, float)]
            List of splits, where each split is a (asset, ratio) tuple.
        """
        return []
        if not assets:
            return []

        # convert dt to # of seconds since epoch, because that's what we use
        # in the adjustments db
        # seconds = int(dt.value / 1e9)

        splits = self.adjustment_repository.conn.execute(
            "SELECT sid, ratio FROM SPLITS WHERE effective_date = ?", (dt,)
        ).fetchall()

        splits = [split for split in splits if split[0] in assets]
        splits = [
            (self.asset_repository.retrieve_asset(split[0]), split[1]) for split in splits
        ]

        return splits

    def to_json(self):
        return {
            "base_storage_path": self._base_storage_path,
            "bundle_name": self._bundle_name,
            "bundle_version": self._bundle_version,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(
            base_storage_path=data["base_storage_path"],
            bundle_name=data["bundle_name"],
            bundle_version=data["bundle_version"],
        )
