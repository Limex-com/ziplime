"""Synthetic data loaders for testing."""

import numpy as np

from numpy.random import RandomState
from pandas import DataFrame, Timestamp
from sqlite3 import connect as sqlite3_connect

from .pipeline_loader import PipelineLoader
from .data_frame_loader import DataFrameLoader


from ziplime.utils.numpy_utils import (
    bool_dtype,
    datetime64ns_dtype,
    float64_dtype,
    int64_dtype,
    object_dtype,
)
from ...assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository

US_EQUITY_PRICING_BCOLZ_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "day",
    "id",
)

UINT_32_MAX = np.iinfo(np.uint32).max


def nanos_to_seconds(nanos):
    return nanos / (1000 * 1000 * 1000)




OHLCV = ("open", "high", "low", "close", "volume")
OHLC = ("open", "high", "low", "close")

PSEUDO_EPOCH_UTC = Timestamp("2000-01-01", tz="UTC")
PSEUDO_EPOCH_NAIVE = Timestamp("2000-01-01")

# TODO FIX TZ MESS


def asset_start(asset_info, asset, tz=None):
    ret = asset_info.loc[asset]["start_date"]
    if tz is not None:
        ret = ret.tz_localize(tz)
    # assert ret.tzname() == "UTC", "Unexpected non-UTC timestamp"
    return ret


def asset_end(asset_info, asset, tz=None):
    ret = asset_info.loc[asset]["end_date"]
    if tz is not None:
        ret = ret.tz_localize(tz)
    # if ret.tz is None:
    #     ret = ret.tz_localize("UTC")
    # assert ret.tzname() == "UTC", "Unexpected non-UTC timestamp"
    return ret


def make_bar_data(asset_info, calendar, holes=None):
    """For a given asset/date/column combination, we generate a corresponding raw
    value using the following formula for OHLCV columns:

    data(asset, date, column) = (100,000 * asset_id)
                              + (10,000 * column_num)
                              + (date - Jan 1 2000).days  # ~6000 for 2015
    where:
        column_num('open') = 0
        column_num('high') = 1
        column_num('low') = 2
        column_num('close') = 3
        column_num('volume') = 4

    We use days since Jan 1, 2000 to guarantee that there are no collisions
    while also the produced values smaller than UINT32_MAX / 1000.

    For 'day' and 'id', we use the standard format expected by the base class.

    Parameters
    ----------
    asset_info : DataFrame
        DataFrame with asset_id as index and 'start_date'/'end_date' columns.
    calendar : pd.DatetimeIndex
        The trading calendar to use.
    holes : dict[int -> tuple[pd.Timestamps]], optional
        A dict mapping asset ids to the tuple of dates that should have
        no data for that asset in the output. Default is no holes.

    Yields
    ------
    p : (int, pd.DataFrame)
        A sid, data pair to be passed to BcolzDailyDailyBarWriter.write
    """
    assert (
        # Using .value here to avoid having to care about UTC-aware dates.
        PSEUDO_EPOCH_UTC.value
        < calendar.normalize().min().value
        <= asset_info["start_date"].min().value
    ), "calendar.min(): %s\nasset_info['start_date'].min(): %s" % (
        calendar.min(),
        asset_info["start_date"].min(),
    )

    assert (asset_info["start_date"] < asset_info["end_date"]).all()

    def _raw_data_for_asset(asset_id):
        """Generate 'raw' data that encodes information about the asset.

        See docstring for a description of the data format.
        """
        # Get the dates for which this asset existed according to our asset
        # info.
        datetimes = calendar[
            calendar.slice_indexer(
                asset_start(asset_info, asset_id, tz=calendar.tz),
                asset_end(asset_info, asset_id, tz=calendar.tz),
            )
        ]

        data = np.full(
            (len(datetimes), len(US_EQUITY_PRICING_BCOLZ_COLUMNS)),
            asset_id * 100 * 1000,
            dtype=np.uint32,
        )

        # Add 10,000 * column-index to OHLCV columns
        data[:, :5] += np.arange(5, dtype=np.uint32) * 1000

        # Add days since Jan 1 2001 for OHLCV columns.
        # TODO FIXME TZ MESS

        if datetimes.tzinfo is None:
            data[:, :5] += np.array(
                (datetimes.tz_localize("UTC") - PSEUDO_EPOCH_UTC).days
            )[:, None].astype(np.uint32)
        else:
            data[:, :5] += np.array((datetimes - PSEUDO_EPOCH_UTC).days)[
                :, None
            ].astype(np.uint32)

        frame = DataFrame(
            data,
            index=datetimes,
            columns=US_EQUITY_PRICING_BCOLZ_COLUMNS,
        )

        if holes is not None and asset_id in holes:
            for dt in holes[asset_id]:
                frame.loc[dt, OHLC] = np.nan
                frame.loc[dt, ["volume"]] = 0

        frame["day"] = nanos_to_seconds(datetimes.asi8)
        frame["id"] = asset_id
        return frame

    for asset in asset_info.index:
        yield asset, _raw_data_for_asset(asset)


def expected_bar_value(asset_id, date, colname):
    """Check that the raw value for an asset/date/column triple is as
    expected.

    Used by tests to verify data written by a writer.
    """
    from_asset = asset_id * 100000
    from_colname = OHLCV.index(colname) * 1000
    from_date = (date - PSEUDO_EPOCH_NAIVE.tz_localize(date.tzinfo)).days
    return from_asset + from_colname + from_date


def expected_bar_value_with_holes(asset_id, date, colname, holes, missing_value):
    # Explicit holes are filled with the missing value.
    if asset_id in holes and date in holes[asset_id]:
        return missing_value

    return expected_bar_value(asset_id, date, colname)


def expected_bar_values_2d(dates, assets, asset_info, colname, holes=None):
    """Return an 2D array containing cls.expected_value(asset_id, date,
    colname) for each date/asset pair in the inputs.

    Missing locs are filled with 0 for volume and NaN for price columns:

        - Values before/after an asset's lifetime.
        - Values for asset_ids not contained in asset_info.
        - Locs defined in `holes`.
    """
    if colname == "volume":
        dtype = np.uint32
        missing = 0
    else:
        dtype = np.float64
        missing = float("nan")

    data = np.full((len(dates), len(assets)), missing, dtype=dtype)
    for j, asset in enumerate(assets):
        # Use missing values when asset_id is not contained in asset_info.
        if asset not in asset_info.index:
            continue

        start = asset_start(asset_info, asset)
        end = asset_end(asset_info, asset)
        for i, date in enumerate(dates):
            # No value expected for dates outside the asset's start/end
            # date.
            # TODO FIXME TZ MESS
            if not (
                start.tz_localize(date.tzinfo) <= date <= end.tz_localize(date.tzinfo)
            ):
                continue

            if holes is not None:
                expected = expected_bar_value_with_holes(
                    asset,
                    date,
                    colname,
                    holes,
                    missing,
                )
            else:
                expected = expected_bar_value(asset, date, colname)

            data[i, j] = expected
    return data


class NullAdjustmentReader(SqlAlchemyAdjustmentRepository):
    """A SQLiteAdjustmentReader that stores no adjustments and uses in-memory
    SQLite.
    """

    def __init__(self):
        conn = sqlite3_connect(":memory:")
        writer = SqlAlchemyAdjustmentRepository(conn, None, None)
        empty = DataFrame(
            {
                "sid": np.array([], dtype=np.uint32),
                "effective_date": np.array([], dtype=np.uint32),
                "ratio": np.array([], dtype=float),
            }
        )
        empty_dividends = DataFrame(
            {
                "sid": np.array([], dtype=np.uint32),
                "amount": np.array([], dtype=np.float64),
                "record_date": np.array([], dtype="datetime64[ns]"),
                "ex_date": np.array([], dtype="datetime64[ns]"),
                "declared_date": np.array([], dtype="datetime64[ns]"),
                "pay_date": np.array([], dtype="datetime64[ns]"),
            }
        )
        writer.write(splits=empty, mergers=empty, dividends=empty_dividends)
        super(NullAdjustmentReader, self).__init__(conn)
