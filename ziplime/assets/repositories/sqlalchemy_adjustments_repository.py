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
from ziplime.assets.models.dividend import Dividend
from ziplime.assets.models.merger import Merger
from ziplime.assets.models.split import Split
from ziplime.lib.adjustment import Float64Multiply
from ziplime.utils.functional import keysorted
from ziplime.utils.numpy_utils import (
    datetime64ns_dtype,
    float64_dtype,
    int64_dtype,
    uint32_dtype,
    uint64_dtype,
)
from ziplime.utils.pandas_utils import empty_dataframe, timedelta_to_integral_seconds
from ziplime.utils.sqlite_utils import group_into_chunks, SQLITE_MAX_VARIABLE_NUMBER

from ziplime.data.adjustments import _lookup_dt, EPOCH, ADJ_QUERY_TEMPLATE, SID_QUERIES

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

SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES = {
    "sid": any_integer,
    "ex_date": any_integer,
    "declared_date": any_integer,
    "record_date": any_integer,
    "pay_date": any_integer,
    "amount": float,
}

SQLITE_STOCK_DIVIDEND_PAYOUT_COLUMN_DTYPES = {
    "sid": any_integer,
    "ex_date": any_integer,
    "declared_date": any_integer,
    "record_date": any_integer,
    "pay_date": any_integer,
    "payment_sid": any_integer,
    "ratio": float,
}


def specialize_any_integer(d):
    out = {}
    for k, v in d.items():
        if v is any_integer:
            out[k] = int64_dtype
        else:
            out[k] = v
    return out


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

    _datetime_int_cols = {
        "splits": ("effective_date",),
        "mergers": ("effective_date",),
        "dividends": ("effective_date",),
        "dividend_payouts": (
            "declared_date",
            "ex_date",
            "pay_date",
            "record_date",
        ),
        "stock_dividend_payouts": (
            "declared_date",
            "ex_date",
            "pay_date",
            "record_date",
        ),
    }
    _raw_table_dtypes = {
        # We use any_integer above to be lenient in accepting different dtypes
        # from users. For our outputs, however, we always want to return the
        # same types, and any_integer turns into int32 on some numpy windows
        # builds, so specify int64 explicitly here.
        "splits": specialize_any_integer(SQLITE_ADJUSTMENT_COLUMN_DTYPES),
        "mergers": specialize_any_integer(SQLITE_ADJUSTMENT_COLUMN_DTYPES),
        "dividends": specialize_any_integer(SQLITE_ADJUSTMENT_COLUMN_DTYPES),
        "dividend_payouts": specialize_any_integer(
            SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES,
        ),
        "stock_dividend_payouts": specialize_any_integer(
            SQLITE_STOCK_DIVIDEND_PAYOUT_COLUMN_DTYPES,
        ),
    }

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

    async def _get_sids_from_table(db,
                             tablename: str,
                             start_date: int,
                             end_date: int) -> set:
        """Get the unique sids for all adjustments between start_date and end_date
        from table `tablename`.

        Parameters
        ----------
        db : sqlite3.connection
        tablename : str
        start_date : int (seconds since epoch)
        end_date : int (seconds since epoch)

        Returns
        -------
        sids : set
            Set of sets
        """

        cursor = db.execute(
            SID_QUERIES[tablename],
            (start_date, end_date),
        )
        out = set()
        for result in cursor.fetchall():
            out.add(result[0])
        return out

    async def _get_split_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:
        # return await self._get_sids_from_table(db, 'splits', start_date, end_date)
        q = select(Split.sid).filter(Split.effective_date >= start_date, Split.effective_date <= end_date).distinct()
        result = set((await db.execute(q)).scalars())
        return result

    async def _get_merger_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:
        q = select(Merger.sid).filter(Merger.effective_date >= start_date, Merger.effective_date <= end_date).distinct()
        result = set((await db.execute(q)).scalars())
        return result

        # return await self._get_sids_from_table(db, 'mergers', start_date, end_date)

    async def _get_dividend_sids(self, db: AsyncSession, start_date: int, end_date: int) -> set:

        """
        SELECT DISTINCT sid FROM {0}
        WHERE effective_date >= ? AND effective_date <= ?
        """
        q = select(Dividend.sid).filter(Dividend.effective_date >= start_date, Dividend.effective_date <= end_date).distinct()
        result = set((await db.execute(q)).scalars())
        return result
        # return await self._get_sids_from_table(db, 'dividends', start_date, end_date)

    async def _adjustments(self,
                     adjustments_db: AsyncSession,
                     split_sids: set,
                     merger_sids: set,
                     dividends_sids: set,
                     start_date: int,
                     end_date: int,
                     assets: pd.Index):

        splits_to_query = [str(a) for a in assets if a in split_sids]
        splits_results = []
        while splits_to_query:
            query_len = min(len(splits_to_query), SQLITE_MAX_VARIABLE_NUMBER)
            query_assets = splits_to_query[:query_len]
            t = [str(a) for a in query_assets]
            statement = ADJ_QUERY_TEMPLATE.format(
                'splits',
                ",".join(['?' for _ in query_assets]),
                start_date,
                end_date,
            )
            c.execute(statement, t)
            splits_to_query = splits_to_query[query_len:]
            splits_results.extend(c.fetchall())

        mergers_to_query = [str(a) for a in assets if a in merger_sids]
        mergers_results = []
        while mergers_to_query:
            query_len = min(len(mergers_to_query), SQLITE_MAX_VARIABLE_NUMBER)
            query_assets = mergers_to_query[:query_len]
            t = [str(a) for a in query_assets]
            statement = ADJ_QUERY_TEMPLATE.format(
                'mergers',
                ",".join(['?' for _ in query_assets]),
                start_date,
                end_date,
            )
            c.execute(statement, t)
            mergers_to_query = mergers_to_query[query_len:]
            mergers_results.extend(c.fetchall())

        dividends_to_query = [str(a) for a in assets if a in dividends_sids]
        dividends_results = []
        while dividends_to_query:
            query_len = min(len(dividends_to_query), SQLITE_MAX_VARIABLE_NUMBER)
            query_assets = dividends_to_query[:query_len]
            t = [str(a) for a in query_assets]
            statement = ADJ_QUERY_TEMPLATE.format(
                'dividends',
                ",".join(['?' for _ in query_assets]),
                start_date,
                end_date,
            )
            c.execute(statement, t)
            dividends_to_query = dividends_to_query[query_len:]
            dividends_results.extend(c.fetchall())

        return splits_results, mergers_results, dividends_results

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

        # Pre-populate date index cache.
        for i, dt in enumerate(_dates_seconds):
            date_ixs[dt] = i

        # splits affect prices and volumes, volumes is the inverse
        for sid, ratio, eff_date in splits:
            if eff_date < start_date:
                continue

            date_loc = _lookup_dt(date_ixs, eff_date, _dates_seconds)

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

            date_loc = _lookup_dt(date_ixs, eff_date, _dates_seconds)

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

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[Dividend]:
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

    def unpack_db_to_component_dfs(self, convert_dates=False):
        """Returns the set of known tables in the adjustments file in DataFrame
        form.

        Parameters
        ----------
        convert_dates : bool, optional
            By default, dates are returned in seconds since EPOCH. If
            convert_dates is True, all ints in date columns will be converted
            to datetimes.

        Returns
        -------
        dfs : dict{str->DataFrame}
            Dictionary which maps table name to the corresponding DataFrame
            version of the table, where all date columns have been coerced back
            from int to datetime.
        """
        return {
            t_name: self.get_df_from_table(t_name, convert_dates)
            for t_name in self._datetime_int_cols
        }

    def get_df_from_table(self, table_name, convert_dates=False):
        try:
            date_cols = self._datetime_int_cols[table_name]
        except KeyError as exc:
            raise ValueError(
                f"Requested table {table_name} not found.\n"
                f"Available tables: {self._datetime_int_cols.keys()}\n"
            ) from exc

        # Dates are stored in second resolution as ints in adj.db tables.
        kwargs = (
            # {"parse_dates": {col: {"unit": "s", "utc": True} for col in date_cols}}
            {"parse_dates": {col: {"unit": "s"} for col in date_cols}}
            if convert_dates
            else {}
        )

        result = pd.read_sql(
            f"select * from {table_name}",
            self.conn,
            index_col="index",
            **kwargs,
        )
        dtypes = self._df_dtypes(table_name, convert_dates)

        if not len(result):
            return empty_dataframe(*keysorted(dtypes))

        result.rename_axis(None, inplace=True)
        result = result[sorted(dtypes)]  # ensure expected order of columns
        return result

    def _df_dtypes(self, table_name, convert_dates):
        """Get dtypes to use when unpacking sqlite tables as dataframes."""
        out = self._raw_table_dtypes[table_name]
        if convert_dates:
            out = out.copy()
            for date_column in self._datetime_int_cols[table_name]:
                out[date_column] = datetime64ns_dtype

        return out

    """Writer for data to be read by SQLiteAdjustmentReader

    Parameters
    ----------
    conn_or_path : str or sqlite3.Connection
        A handle to the target sqlite database.
    equity_daily_bar_reader : SessionBarReader
        Daily bar reader to use for dividend writes.
    overwrite : bool, optional, default=False
        If True and conn_or_path is a string, remove any existing files at the
        given path before connecting.

    See Also
    --------
    ziplime.data.adjustments.SQLiteAdjustmentReader
    """

    def _write(self, tablename, expected_dtypes, frame):
        if frame is None or frame.empty:
            # keeping the dtypes correct for empty frames is not easy
            # frame = pd.DataFrame(
            #     np.array([], dtype=list(expected_dtypes.items())),
            # )
            frame = pd.DataFrame(expected_dtypes, index=[])
        else:
            if frozenset(frame.columns) != frozenset(expected_dtypes):
                raise ValueError(
                    "Unexpected frame columns:\n"
                    "Expected Columns: %s\n"
                    "Received Columns: %s"
                    % (
                        set(expected_dtypes),
                        frame.columns.tolist(),
                    )
                )

            actual_dtypes = frame.dtypes
            for colname, expected in expected_dtypes.items():
                actual = actual_dtypes[colname]
                if not np.issubdtype(actual, expected):
                    raise TypeError(
                        "Expected data of type {expected} for column"
                        " '{colname}', but got '{actual}'.".format(
                            expected=expected,
                            colname=colname,
                            actual=actual,
                        ),
                    )

        frame.to_sql(
            tablename,
            self.conn,
            if_exists="append",
            chunksize=50000,
        )

    def write_frame(self, tablename, frame):
        if tablename not in SQLITE_ADJUSTMENT_TABLENAMES:
            raise ValueError(
                f"Adjustment table {tablename} not in {SQLITE_ADJUSTMENT_TABLENAMES}"
            )
        if not (frame is None or frame.empty):
            frame = frame.copy()
            frame["effective_date"] = (
                frame["effective_date"]
                .values.astype(
                    "datetime64[s]",
                )
                .astype("int64")
            )
        return self._write(
            tablename,
            SQLITE_ADJUSTMENT_COLUMN_DTYPES,
            frame,
        )

    def write_dividend_payouts(self, frame):
        """Write dividend payout data to SQLite table `dividend_payouts`."""
        return self._write(
            "dividend_payouts",
            SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES,
            frame,
        )

    def write_stock_dividend_payouts(self, frame):
        return self._write(
            "stock_dividend_payouts",
            SQLITE_STOCK_DIVIDEND_PAYOUT_COLUMN_DTYPES,
            frame,
        )

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

    def _write_dividends(self, dividends):
        if dividends is None:
            dividend_payouts = None
        else:
            dividend_payouts = dividends.copy()
            # TODO: Check if that's the right place for this fix for pandas > 1.2.5
            dividend_payouts.fillna(np.datetime64("NaT"), inplace=True)
            dividend_payouts["ex_date"] = (
                dividend_payouts["ex_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            dividend_payouts["record_date"] = (
                dividend_payouts["record_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            dividend_payouts["declared_date"] = (
                dividend_payouts["declared_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            dividend_payouts["pay_date"] = (
                dividend_payouts["pay_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )

        self.write_dividend_payouts(dividend_payouts)

    def _write_stock_dividends(self, stock_dividends):
        if stock_dividends is None:
            stock_dividend_payouts = None
        else:
            stock_dividend_payouts = stock_dividends.copy()
            stock_dividend_payouts["ex_date"] = (
                stock_dividend_payouts["ex_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            stock_dividend_payouts["record_date"] = (
                stock_dividend_payouts["record_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            stock_dividend_payouts["declared_date"] = (
                stock_dividend_payouts["declared_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
            stock_dividend_payouts["pay_date"] = (
                stock_dividend_payouts["pay_date"]
                .values.astype("datetime64[s]")
                .astype(int64_dtype)
            )
        self.write_stock_dividend_payouts(stock_dividend_payouts)

    def write_dividend_data(self, dividends, stock_dividends=None):
        """Write both dividend payouts and the derived price adjustment ratios."""

        # First write the dividend payouts.
        self._write_dividends(dividends)
        self._write_stock_dividends(stock_dividends)

        # Second from the dividend payouts, calculate ratios.
        dividend_ratios = self.calc_dividend_ratios(dividends)
        self.write_frame("dividends", dividend_ratios)

    def write(self, splits=None, mergers=None, dividends=None, stock_dividends=None):
        """Writes data to a SQLite file to be read by SQLiteAdjustmentReader.

        Parameters
        ----------
        splits : pandas.DataFrame, optional
            Dataframe containing split data. The format of this dataframe is:
              effective_date : int
                  The date, represented as seconds since Unix epoch, on which
                  the adjustment should be applied.
              ratio : float
                  A value to apply to all data earlier than the effective date.
                  For open, high, low, and close those values are multiplied by
                  the ratio. Volume is divided by this value.
              sid : int
                  The asset id associated with this adjustment.
        mergers : pandas.DataFrame, optional
            DataFrame containing merger data. The format of this dataframe is:
              effective_date : int
                  The date, represented as seconds since Unix epoch, on which
                  the adjustment should be applied.
              ratio : float
                  A value to apply to all data earlier than the effective date.
                  For open, high, low, and close those values are multiplied by
                  the ratio. Volume is unaffected.
              sid : int
                  The asset id associated with this adjustment.
        dividends : pandas.DataFrame, optional
            DataFrame containing dividend data. The format of the dataframe is:
              sid : int
                  The asset id associated with this adjustment.
              ex_date : datetime64
                  The date on which an equity must be held to be eligible to
                  receive payment.
              declared_date : datetime64
                  The date on which the dividend is announced to the public.
              pay_date : datetime64
                  The date on which the dividend is distributed.
              record_date : datetime64
                  The date on which the stock ownership is checked to determine
                  distribution of dividends.
              amount : float
                  The cash amount paid for each share.

            Dividend ratios are calculated as:
            ``1.0 - (dividend_value / "close on day prior to ex_date")``
        stock_dividends : pandas.DataFrame, optional
            DataFrame containing stock dividend data. The format of the
            dataframe is:
              sid : int
                  The asset id associated with this adjustment.
              ex_date : datetime64
                  The date on which an equity must be held to be eligible to
                  receive payment.
              declared_date : datetime64
                  The date on which the dividend is announced to the public.
              pay_date : datetime64
                  The date on which the dividend is distributed.
              record_date : datetime64
                  The date on which the stock ownership is checked to determine
                  distribution of dividends.
              payment_sid : int
                  The asset id of the shares that should be paid instead of
                  cash.
              ratio : float
                  The ratio of currently held shares in the held sid that
                  should be paid with new shares of the payment_sid.

        See Also
        --------
        ziplime.data.adjustments.SQLiteAdjustmentReader
        """
        self.write_frame("splits", splits)
        self.write_frame("mergers", mergers)
        self.write_dividend_data(dividends, stock_dividends)

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
