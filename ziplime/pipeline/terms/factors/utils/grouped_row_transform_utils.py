import numpy as np
from math import ceil

from numpy import isnan

from ziplime.utils.math_utils import nanmean, nanstd

# Functions to be passed to GroupedRowTransform.  These aren't defined inline
# because the transformation function is part of the instance hash key.
def demean(row):
    return row - nanmean(row)


def zscore(row):
    with np.errstate(divide="ignore", invalid="ignore"):
        return (row - nanmean(row)) / nanstd(row)


def winsorize(row, min_percentile, max_percentile):
    """
    This implementation is based on scipy.stats.mstats.winsorize
    """
    a = row.copy()
    nan_count = isnan(row).sum()
    nonnan_count = a.size - nan_count

    # NOTE: argsort() sorts nans to the end of the array.
    idx = a.argsort()

    # Set values at indices below the min percentile to the value of the entry
    # at the cutoff.
    if min_percentile > 0:
        lower_cutoff = int(min_percentile * nonnan_count)
        a[idx[:lower_cutoff]] = a[idx[lower_cutoff]]

    # Set values at indices above the max percentile to the value of the entry
    # at the cutoff.
    if max_percentile < 1:
        upper_cutoff = int(ceil(nonnan_count * max_percentile))
        # if max_percentile is close to 1, then upper_cutoff might not
        # remove any values.
        if upper_cutoff < nonnan_count:
            start_of_nans = (-nan_count) if nan_count else None
            a[idx[upper_cutoff:start_of_nans]] = a[idx[upper_cutoff - 1]]

    return a
