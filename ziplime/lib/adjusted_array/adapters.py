"""Adapters for adjusted_array."""

import numpy as np
from ziplime.lib.adjusted_array.adjusted_array import AdjustedArray


def ensure_adjusted_array(ndarray_or_adjusted_array: np.ndarray | AdjustedArray, missing_value):
    if isinstance(ndarray_or_adjusted_array, AdjustedArray):
        return ndarray_or_adjusted_array
    elif isinstance(ndarray_or_adjusted_array, np.ndarray):
        return AdjustedArray(
            ndarray_or_adjusted_array,
            {},
            missing_value,
        )
    else:
        raise TypeError(
            "Can't convert %s to AdjustedArray"
            % type(ndarray_or_adjusted_array).__name__
        )


def ensure_ndarray(ndarray_or_adjusted_array: np.ndarray | AdjustedArray):
    """
    Return the input as a numpy ndarray.

    This is a no-op if the input is already an ndarray.  If the input is an
    adjusted_array, this extracts a read-only view of its internal data buffer.

    Parameters
    ----------
    ndarray_or_adjusted_array : numpy.ndarray | ziplime.data.adjusted_array

    Returns
    -------
    out : The input, converted to an ndarray.
    """
    if isinstance(ndarray_or_adjusted_array, np.ndarray):
        return ndarray_or_adjusted_array
    elif isinstance(ndarray_or_adjusted_array, AdjustedArray):
        return ndarray_or_adjusted_array.data
    else:
        raise TypeError(
            "Can't convert %s to ndarray" % type(ndarray_or_adjusted_array).__name__
        )
