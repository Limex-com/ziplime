import datetime
from collections import namedtuple
import errno
import os
import shutil
import warnings

import click
import logging
import pandas as pd
from attr import dataclass
from exchange_calendars import ExchangeCalendar
from lime_trader.models.market import Period
from ziplime.data.adjustments import SQLiteAdjustmentReader, SQLiteAdjustmentWriter
from zipline.utils.calendar_utils import get_calendar
from toolz import curry, complement, take
from zipline.utils.sqlite_utils import check_and_create_engine

from ziplime.assets.repositories.sqlite_asset_repository import SqliteAssetRepository
from ziplime.assets.domain.db.asset import Asset
from zipline.assets import ASSET_DB_VERSION

from zipline.assets.asset_db_migrations import downgrade
from zipline.utils.cache import (
    dataframe_cache,
    working_dir,
    working_file,
)
from zipline.utils.compat import ExitStack, mappingproxy
from zipline.utils.input_validation import ensure_timestamp, optionally
import ziplime.utils.paths as pth
from zipline.utils.preprocess import preprocess

from ziplime.data.abstract_data_bundle import AbstractDataBundle
from ziplime.data.abstract_fundamendal_data_provider import AbstractFundamentalDataProvider
from ziplime.data.abstract_historical_market_data_provider import AbstractHistoricalMarketDataProvider
from ziplime.data.domain.bundle_core import BundleCore
from ziplime.data.domain.bundle_data import BundleData
from ziplime.data.domain.registered_bundle import RegisteredBundle
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.storages.polars_data_bundle import PolarsDataBundle
from ziplime.domain.column_specification import ColumnSpecification

log = logging.getLogger(__name__)


def asset_db_path(bundle_name, timestr, db_version=None):
    return pth.data_path(
        asset_db_relative(bundle_name, timestr, db_version),
    )


def minute_equity_path(bundle_name, timestr):
    return pth.data_path(
        minute_equity_relative(bundle_name, timestr),
    )


def daily_equity_path(bundle_name, timestr):
    return pth.data_path(
        daily_equity_relative(bundle_name, timestr),
    )


def fundamental_data_path(bundle_name, timestr):
    return pth.data_path(
        fundamental_data_relative(bundle_name, timestr),
    )


def adjustment_db_path(bundle_name, timestr):
    return pth.data_path(
        adjustment_db_relative(bundle_name, timestr),
    )


def cache_path(bundle_name):
    return pth.data_path(
        cache_relative(bundle_name)
    )


def adjustment_db_relative(bundle_name, timestr):
    return bundle_name, timestr, "adjustments.sqlite"


def cache_relative(bundle_name):
    return bundle_name, ".cache"


def daily_equity_relative(bundle_name, timestr):
    return bundle_name, timestr, "daily_equities.bcolz"


def fundamental_data_relative(bundle_name, timestr):
    return bundle_name, timestr, "fundamental_data.bcolz"


def minute_equity_relative(bundle_name, timestr):
    return bundle_name, timestr, "minute_equities.bcolz"


def asset_db_relative(bundle_name, timestr, db_version=None):
    db_version = ASSET_DB_VERSION if db_version is None else db_version

    return bundle_name, timestr, "assets-%d.sqlite" % db_version


def to_bundle_ingest_dirname(ts):
    """Convert a pandas Timestamp into the name of the directory for the
    ingestion.

    Parameters
    ----------
    ts : pandas.Timestamp
        The time of the ingestions

    Returns
    -------
    name : str
        The name of the directory for this ingestion.
    """
    return ts.isoformat().replace(":", ";")


def from_bundle_ingest_dirname(cs):
    """Read a bundle ingestion directory name into a pandas Timestamp.

    Parameters
    ----------
    cs : str
        The name of the directory.

    Returns
    -------
    ts : pandas.Timestamp
        The time when this ingestion happened.
    """
    return pd.Timestamp(cs.replace(";", ":"))


def ingestions_for_bundle(bundle):
    return sorted(
        (
            from_bundle_ingest_dirname(ing)
            for ing in os.listdir(pth.data_path([bundle]))
            if not pth.hidden(ing)
        ),
        reverse=True,
    )


class UnknownBundle(click.ClickException, LookupError):
    """Raised if no bundle with the given name was registered."""

    exit_code = 1

    def __init__(self, name):
        super(UnknownBundle, self).__init__(
            "No bundle registered with the name %r" % name,
        )
        self.name = name

    def __str__(self):
        return self.message


class BadClean(click.ClickException, ValueError):
    """Exception indicating that an invalid argument set was passed to
    ``clean``.

    Parameters
    ----------
    before, after, keep_last : any
        The bad arguments to ``clean``.

    See Also
    --------
    clean
    """

    def __init__(self, before, after, keep_last):
        super(BadClean, self).__init__(
            "Cannot pass a combination of `before` and `after` with "
            "`keep_last`. Must pass one. "
            "Got: before=%r, after=%r, keep_last=%r\n"
            % (
                before,
                after,
                keep_last,
            ),
        )

    def __str__(self):
        return self.message


"""Create a family of data bundle functions that read from the same
bundle mapping.

Returns
-------
bundles : mappingproxy
    The mapping of bundles to bundle payloads.
register : callable
    The function which registers new bundles in the ``bundles`` mapping.
unregister : callable
    The function which deregisters bundles from the ``bundles`` mapping.
ingest : callable
    The function which downloads and write data for a given data bundle.
load : callable
    The function which loads the ingested bundles back into memory.
clean : callable
    The function which cleans up data written with ``ingest``.
"""
_bundles = {}  # the registered bundles
# Expose _bundles through a proxy so that users cannot mutate this
# accidentally. Users may go through `register` to update this which will
# warn when trampling another bundle.
bundles = mappingproxy(_bundles)


# def register(
#         name,
#         f,
#         calendar_name,
#         start_session,
#         end_session,
# ):
#     """Register a data bundle ingest function.
#
#     Parameters
#     ----------
#     name : str
#         The name of the bundle.
#     f : callable
#         The ingest function. This function will be passed:
#           asset_db_writer : AssetDBWriter
#               The asset db writer to write into.
#           data_bundle_writer : BcolzDailyBarWriter
#               The daily bar writer to write into.
#           adjustment_writer : SQLiteAdjustmentWriter
#               The adjustment db writer to write into.
#           calendar : trading_calendars.TradingCalendar
#               The trading calendar to ingest for.
#           start_session : pd.Timestamp
#               The first session of data to ingest.
#           end_session : pd.Timestamp
#               The last session of data to ingest.
#           cache : DataFrameCache
#               A mapping object to temporarily store dataframes.
#               This should be used to cache intermediates in case the load
#               fails. This will be automatically cleaned up after a
#               successful load.
#           show_progress : bool
#               Show the progress for the current load where possible.
#     calendar_name : str, optional
#         The name of a calendar used to align bundle data.
#         Default is 'NYSE'.
#     start_session : pd.Timestamp, optional
#         The first session for which we want data. If not provided,
#         or if the date lies outside the range supported by the
#         calendar, the first_session of the calendar is used.
#     end_session : pd.Timestamp, optional
#         The last session for which we want data. If not provided,
#         or if the date lies outside the range supported by the
#         calendar, the last_session of the calendar is used.
#
#     Notes
#     -----
#     This function my be used as a decorator, for example:
#
#     .. code-block:: python
#
#        @register('quandl')
#        def quandl_ingest_function(...):
#            ...
#
#     See Also
#     --------
#     zipline.data.bundles.bundles
#     """
#     # if name in bundles:
#     #     warnings.warn(
#     #         "Overwriting bundle with name %r" % name,
#     #         stacklevel=3,
#     #     )
#
#     # NOTE: We don't eagerly compute calendar values here because
#     # `register` is called at module scope in zipline, and creating a
#     # calendar currently takes between 0.5 and 1 seconds, which causes a
#     # noticeable delay on the zipline CLI.
#     _bundles[name] = RegisteredBundle(
#         calendar_name=calendar_name,
#         start_session=start_session,
#         end_session=end_session,
#         ingest=f,
#     )
#     return f
#
#
# def ingest(
#         name,
#         fundamental_data_provider: AbstractFundamentalDataProvider,
#         historical_market_data_provider: AbstractHistoricalMarketDataProvider,
#         data_bundle_writer_class: AbstractDataBundle,
#         fundamental_data_writer_class: AbstractDataBundle,
#         market_data_fields: list[ColumnSpecification],
#         fundamental_data_fields: list[ColumnSpecification],
#         start_date: datetime.date,
#         end_date: datetime.date,
#         trading_calendar: ExchangeCalendar,
#         frequency: datetime.timedelta,
#         base_path: str,
#         symbols: list[str],
#         show_progress: bool = False,
# ):
#     """Ingest data for a given bundle.
#
#     Parameters
#     ----------
#     name : str
#         The name of the bundle.
#     timestamp : datetime, optional
#         The timestamp to use for the load.
#         By default this is the current time.
#     assets_versions : Iterable[int], optional
#         Versions of thfe assets db to which to downgrade.
#     show_progress : bool, optional
#         Tell the ingest function to display the progress where possible.
#     kwargs : Any, optional
#         Additional kwargs being passed to the ingest function.
#     """
#     # try:
#     #     bundle = bundles[name]
#     # except KeyError:
#     #     raise UnknownBundle(name)
#
#     start_session = start_date
#     end_session = end_date
#     if start_date  < trading_calendar.first_session:
#         start_session = trading_calendar.first_session
#
#     if end_date > trading_calendar.last_session:
#         end_session = trading_calendar.last_session
#
#     timestamp = pd.Timestamp.utcnow()
#     timestamp = timestamp.tz_convert("utc").tz_localize(None)
#
#     timestr = to_bundle_ingest_dirname(timestamp)
#     pth.ensure_directory(pth.data_path([name, timestr]))
#
#     bars_path = wd.ensure_dir(*minute_equity_relative(name, timestr))
#     data_bundle_writer = data_bundle_writer_class(
#         bars_path
#     )
#     fundamental_data_path = wd.ensure_dir(*fundamental_data_relative(name, timestr))
#     fundamental_data_writer = fundamental_data_writer_class(
#         fundamental_data_path
#     )
#     # Do an empty write to ensure that the daily ctables exist
#     # when we create the SQLiteAdjustmentWriter below. The
#     # SQLiteAdjustmentWriter needs to open the daily ctables so
#     # that it can compute the adjustment ratios for the dividends.
#
#     assets_db_path = wd.getpath(*asset_db_relative(name, timestr))
#     asset_db_writer = AssetDBWriter(assets_db_path)
#
#     adjustment_db_writer = stack.enter_context(
#         SQLiteAdjustmentWriter(
#             wd.getpath(*adjustment_db_relative(name, timestr)),
#             data_bundle_writer,
#             # data_bundle_writer_class(daily_bars_path),
#             overwrite=True,
#         )
#     )
#     log.info("Ingesting %s", name)
#     bundle.ingest(
#         historical_market_data_provider=historical_market_data_provider,
#         fundamental_data_provider=fundamental_data_provider,
#         asset_db_writer=asset_db_writer,
#         data_bundle_writer=data_bundle_writer,
#         fundamental_data_writer=fundamental_data_writer,
#         adjustment_writer=adjustment_db_writer,
#         calendar=trading_calendar,
#         start_session=start_session,
#         end_session=end_session,
#         show_progress=show_progress,
#         output_dir=pth.data_path([name, timestr]),
#         market_data_fields=market_data_fields,
#         fundamental_data_fields=fundamental_data_fields,
#         bundle_service=BundleService(bundle_storage=Polar)
#     )
#

def most_recent_data(bundle_name, timestamp):
    """Get the path to the most recent data after ``date``for the
    given bundle.

    Parameters
    ----------
    bundle_name : str
        The name of the bundle to lookup.
    timestamp : datetime
        The timestamp to begin searching on or before.
    """
    if bundle_name not in bundles:
        raise UnknownBundle(bundle_name)

    try:
        candidates = os.listdir(
            pth.data_path([bundle_name]),
        )
        return pth.data_path(
            [
                bundle_name,
                max(
                    filter(complement(pth.hidden), candidates),
                    key=from_bundle_ingest_dirname,
                ),
            ],
        )
    except (ValueError, OSError) as e:
        if getattr(e, "errno", errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError(
            "no data for bundle {bundle!r} on or before {timestamp}\n"
            "maybe you need to run: $ zipline ingest -b {bundle}".format(
                bundle=bundle_name,
                timestamp=timestamp,
            ),
        )


def load(name, timestamp=None):
    """Loads a previously ingested bundle.

    Parameters
    ----------
    name : str
        The name of the bundle.
    timestamp : datetime, optional
        The timestamp of the data to lookup.
        Defaults to the current time.

    Returns
    -------
    bundle_data : BundleData
        The raw data readers for this bundle.
    """
    if timestamp is None:
        timestamp = pd.Timestamp.utcnow()
    timestr = most_recent_data(name, timestamp)

    return BundleData(
        name=name,
        asset_repository=SqliteAssetRepository(
            check_and_create_engine(asset_db_path(name, timestr), require_exists=True)
        ),
        historical_data_reader=PolarsDataBundle(
            root_directory=minute_equity_path(name, timestr),
        ),
        fundamental_data_reader=PolarsDataBundle(
            root_directory=fundamental_data_path(name, timestr),
        ),
        adjustment_reader=SQLiteAdjustmentReader(
            adjustment_db_path(name, timestr),
        ),
    )


# def clean(name, before=None, after=None, keep_last=None):
#     """Clean up data that was created with ``ingest`` or
#     ``$ python -m zipline ingest``
#
#     Parameters
#     ----------
#     name : str
#         The name of the bundle to remove data for.
#     before : datetime, optional
#         Remove data ingested before this date.
#         This argument is mutually exclusive with: keep_last
#     after : datetime, optional
#         Remove data ingested after this date.
#         This argument is mutually exclusive with: keep_last
#     keep_last : int, optional
#         Remove all but the last ``keep_last`` ingestions.
#         This argument is mutually exclusive with:
#           before
#           after
#
#     Returns
#     -------
#     cleaned : set[str]
#         The names of the runs that were removed.
#
#     Raises
#     ------
#     BadClean
#         Raised when ``before`` and or ``after`` are passed with
#         ``keep_last``. This is a subclass of ``ValueError``.
#     """
#     try:
#         all_runs = sorted(
#             filter(
#                 complement(pth.hidden),
#                 os.listdir(pth.data_path([name])),
#             ),
#             key=from_bundle_ingest_dirname,
#         )
#     except OSError as e:
#         if e.errno != errno.ENOENT:
#             raise
#         raise UnknownBundle(name)
#
#     if before is after is keep_last is None:
#         raise BadClean(before, after, keep_last)
#     if (before is not None or after is not None) and keep_last is not None:
#         raise BadClean(before, after, keep_last)
#
#     if keep_last is None:
#
#         def should_clean(name):
#             dt = from_bundle_ingest_dirname(name)
#             return (before is not None and dt < before) or (
#                     after is not None and dt > after
#             )
#
#     elif keep_last >= 0:
#         last_n_dts = set(take(keep_last, reversed(all_runs)))
#
#         def should_clean(name):
#             return name not in last_n_dts
#
#     else:
#         raise BadClean(before, after, keep_last)
#
#     cleaned = set()
#     for run in all_runs:
#         if should_clean(run):
#             log.info("Cleaning %s.", run)
#             path = pth.data_path([name, run])
#             shutil.rmtree(path)
#             cleaned.add(path)
#
#     return cleaned
