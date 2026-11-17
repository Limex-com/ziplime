"""
Algorithms for computing quantiles on numpy arrays.
"""
from collections.abc import Sequence

# from numpy.lib import apply_along_axis

from numpy import apply_along_axis
from pandas import qcut
import numpy as np


def quantiles(data: 'np.ndarray', nbins_or_partition_bounds: 'int | Sequence[float]'):
    """
    Compute rowwise array quantiles on an input.
    """
    return apply_along_axis(
        qcut,
        1,
        data,
        q=nbins_or_partition_bounds,
        labels=False,
    )
