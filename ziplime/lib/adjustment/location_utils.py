"""Location utils for adjustment."""

import pandas as pd
from pandas import isnull


# cimport numpy as np
#
# # Purely for readability. There aren't C-level declarations for these types.
# ctypedef object Int64Index_t
# ctypedef object DatetimeIndex_t
# ctypedef object Timestamp_t

#
# ctypedef fused column_type:
#     np.int64_t
#     np.float64_t
#     np.uint8_t
#     object
def _from_assets_and_dates(cls,
                           dates_index: pd.DatetimeIndex,
                           assets_index: pd.RangeIndex,
                           start_date: pd.Timestamp,
                           end_date: pd.Timestamp,
                           asset_id: int,
                           value):
    """
    Helper for constructing an Adjustment instance from coordinates in
    assets/dates indices.

    Example
    -------

    >>> from pandas import date_range,  Timestamp
    >>> dates = date_range('2014-01-01', '2014-01-07')
    >>> assets = Int64Index(range(10))
    >>> Float64Multiply.from_assets_and_dates(
    ...     dates,
    ...     assets,
    ...     Timestamp('2014-01-03'),
    ...     Timestamp('2014-01-05'),
    ...     3,
    ...     0.5,
    ... )
    Float64Multiply(first_row=2, last_row=4, first_col=3, last_col=3, value=0.500000)
    """
    first_row, last_row, col = get_adjustment_locs(
        dates_index,
        assets_index,
        start_date,
        end_date,
        asset_id,
    )
    return cls(
        first_row=first_row,
        last_row=last_row,
        first_col=col,
        last_col=col,
        value=value,
    )


def get_adjustment_locs(dates_index: pd.DatetimeIndex,
                        assets_index: pd.RangeIndex,
                        start_date: pd.Timestamp,
                        end_date: pd.Timestamp,
                        asset_id: int) -> tuple:
    """
    Compute indices suitable for passing to an Adjustment constructor.

    If the specified dates aren't in dates_index, we return the index of the
    first date **BEFORE** the supplied date.

    Example:

    >>> from pandas import date_range,  Timestamp
    >>> dates = date_range('2014-01-01', '2014-01-07')
    >>> assets = Int64Index(range(10))
    >>> get_adjustment_locs(
    ...     dates,
    ...     assets,
    ...     Timestamp('2014-01-03'),
    ...     Timestamp('2014-01-05'),
    ...     3,
    ... )
    (2, 4, 3)
    """

    # None or NaT signifies "All values before the end_date".
    if isnull(start_date):
        start_date_loc = 0
    else:
        # Location of earliest date on or after start_date.
        start_date_loc = dates_index.get_indexer([start_date], method='bfill')[0]

    return (
        start_date_loc,
        # Location of latest date on or before start_date.
        dates_index.get_indexer([end_date], method='ffill')[0],
        assets_index.get_indexer([asset_id])[0],  # Must be exact match.
    )
