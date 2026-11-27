"""Aggregated statistics for tracked positions."""

import numpy as np
import pandas as pd


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

    def __init__(self) -> None:
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
