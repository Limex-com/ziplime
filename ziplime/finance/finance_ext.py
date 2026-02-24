from math import sqrt

import numpy as np
import pandas as pd
import structlog

from ziplime.assets.entities.futures_contract import FuturesContract

logger = structlog.get_logger(__name__)


class PositionStats:
    """Computed values from the current positions.

    Attributes
    ----------
    gross_exposure : float64
        The gross position exposure.
    gross_value : float64
        The gross position value.
    long_exposure : float64
        The exposure of just the long positions.
    long_value : float64
        The value of just the long positions.
    net_exposure : float64
        The net position exposure.
    net_value : float64
        The net position value.
    short_exposure : float64
        The exposure of just the short positions.
    short_value : float64
        The value of just the short positions.
    longs_count : int64
        The number of long positions.
    shorts_count : int64
        The number of short positions.
    position_exposure_array : np.ndarray[float64]
        The exposure of each position in the same order as
        ``position_tracker.positions``.
    position_exposure_series : pd.Series[float64]
        The exposure of each position in the same order as
        ``position_tracker.positions``. The index is the numeric sid of each
        asset.

    Notes
    -----
    ``position_exposure_array`` and ``position_exposure_series`` share the same
    underlying memory. The array interface should be preferred if you are doing
    access each minute for better performance.

    ``position_exposure_array`` and ``position_exposure_series`` may be mutated
    when the position tracker next updates the stats. Do not rely on these
    objects being preserved across accesses to ``stats``. If you need to freeze
    the values, you must take a copy.
    """

    # cdef readonly np.float64_t gross_exposure
    # cdef readonly np.float64_t gross_value
    # cdef readonly np.float64_t long_exposure
    # cdef readonly np.float64_t long_value
    # cdef readonly np.float64_t net_exposure
    # cdef readonly np.float64_t net_value
    # cdef readonly np.float64_t short_exposure
    # cdef readonly np.float64_t short_value
    # cdef readonly np.uint64_t longs_count
    # cdef readonly np.uint64_t shorts_count
    # cdef readonly object position_exposure_array
    # cdef readonly object position_exposure_series
    #
    # # These are the same memory exposed through ``position_exposure_array``
    # # and ``position_exposure_series``. These are hidden from Python.
    # cdef object underlying_value_array
    # cdef object underlying_index_array

    def __init__(self):
        self.gross_exposure = 0.0
        self.gross_value = 0.0
        self.long_exposure = 0.0
        self.long_value = 0.0
        self.net_exposure = 0.0
        self.net_value = 0.0
        self.short_exposure = 0.0
        self.short_value = 0.0
        self.longs_count = 0
        self.shorts_count = 0
        self.position_exposure_array = None
        self.position_exposure_series = None

        # These are the same memory exposed through ``position_exposure_array``
        # and ``position_exposure_series``. These are hidden from Python.
        self.underlying_value_array = None
        self.underlying_index_array = None

    @classmethod
    def new(cls):
        self = cls()
        self.position_exposure_series = es = pd.Series(
            np.array([], dtype='float64'),
            index=np.array([], dtype='int64'),
        )
        self.underlying_value_array = self.position_exposure_array = es.values
        self.underlying_index_array = es.index.values
        return self


def calculate_position_tracker_stats(positions, stats):
    """Calculate various stats about the current positions.

    Parameters
    ----------
    positions : OrderedDict
        The ordered dictionary of positions.

    Returns
    -------
    position_stats : PositionStats
        The computed statistics.
    """
    npos = len(positions)

    old_index = stats.underlying_index_array
    old_position_exposure = (
        stats.underlying_value_array
    )

    long_value = 0.0
    short_value = 0.0

    long_exposure = 0.0
    short_exposure = 0.0

    longs_count = 0
    shorts_count = 0

    # attempt to reuse the memory of the old exposure series
    if len(old_index) < npos:
        # we don't have enough space in the cached buffer, allocate a new
        # array
        stats.underlying_index_array = index = np.empty(npos, dtype='int64')
        stats.underlying_value_array = position_exposure = np.empty(
            npos,
            dtype='float64',
        )

        stats.position_exposure_array = position_exposure
        # create a new series to expose the arrays
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )
    elif len(old_index) > npos:
        # we have more space than needed, slice off the extra but leave it
        # available
        index = old_index[:npos]
        position_exposure = old_position_exposure[:npos]

        stats.position_exposure_array = position_exposure
        # create a new series with the sliced arrays
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )
    else:
        # we have exactly the right amount of space, no slicing or allocation
        # needed
        index = old_index
        position_exposure = old_position_exposure

        stats.position_exposure_array = position_exposure
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )

    ix = 0

    for outer_position in positions.values():
        position = outer_position

        # NOTE: this loop does a lot of stuff!
        # we call this function every time the portfolio value is needed,
        # which is at least once per simulation day, so let's not iterate
        # through every single position multiple times.
        # try:
        exposure = position.amount * position.last_sale_price
        # except Exception as e:
        #     print("exception multiplying a")
        #     raise
        if type(position.asset) is FuturesContract:
            # Futures don't have an inherent position value.
            value = 0

            # unchecked cast, this is safe because we do a type check above
            exposure *= position.asset.price_multiplier
        else:
            value = exposure

        if exposure > 0:
            longs_count += 1
            long_value += value
            long_exposure += exposure
        elif exposure < 0:
            shorts_count += 1
            short_value += value
            short_exposure += exposure

        index[ix] = position.asset.sid
        position_exposure[ix] = exposure

        ix += 1

    net_value = long_value + short_value
    gross_value = long_value - short_value

    net_exposure = long_exposure + short_exposure
    gross_exposure = long_exposure - short_exposure

    stats.gross_exposure = gross_exposure
    stats.gross_value = gross_value
    stats.long_exposure = long_exposure
    stats.long_value = long_value
    stats.longs_count = longs_count
    stats.net_exposure = net_exposure
    stats.net_value = net_value
    stats.short_exposure = short_exposure
    stats.short_value = short_value
    stats.shorts_count = shorts_count


def minute_annual_volatility(date_labels,
                             minute_returns,
                             daily_returns):
    """Pre-compute the minute cumulative volatility field.
    """
    out = np.empty_like(minute_returns)
    previous_date = date_labels[0]
    day_ix = 0
    daily_sum = 0
    todays_prod = 1
    annualization_factor = sqrt(252.0)

    for ix in range(len(minute_returns)):
        date = date_labels[ix]
        this_minute_returns = minute_returns[ix]

        if date != previous_date:
            previous_date = date
            daily_sum += daily_returns[day_ix]
            day_ix += 1
            todays_prod = 1

        if day_ix < 1:
            variance = np.nan
        else:
            todays_prod *= 1 + this_minute_returns

            intermediate_sum = daily_sum + todays_prod - 1
            mean = intermediate_sum / (day_ix + 1)

            variance = todays_prod - 1 - mean
            variance *= variance  # squared

            demeaned_old = daily_returns[:day_ix] - mean
            variance += demeaned_old.dot(demeaned_old)

            # variance /= day_ix  # day_count - 1 for ddof=1
            variance = variance/day_ix
        # print(ix, sqrt(variance) * annualization_factor)
        res = sqrt(variance) * annualization_factor
        if np.isnan(res):
            out[ix] = 0
        else:
            out[ix] = res

    return out
