"""Utils for adjusted_array."""

import numpy as np
from ziplime.errors import WindowLengthNotPositive, WindowLengthTooLong
from ziplime.lib.labelarray import LabelArray
from ziplime.utils.numpy_utils import datetime64ns_dtype
from ziplime.lib.adjusted_array.support import BOOL_DTYPES
from ziplime.lib.adjusted_array.support import FLOAT_DTYPES
from ziplime.lib.adjusted_array.support import INT_DTYPES
from ziplime.lib.adjusted_array.support import OBJECT_DTYPES
from ziplime.lib.adjusted_array.support import REPRESENTABLE_DTYPES
from ziplime.lib.adjusted_array.support import STRING_KINDS


def can_represent_dtype(dtype: np.dtype):
    """Can we build an AdjustedArray for a baseline of `dtype``?"""
    return dtype in REPRESENTABLE_DTYPES or dtype.kind in STRING_KINDS


def is_categorical(dtype: np.dtype):
    """Do we represent this dtype with LabelArrays rather than ndarrays?"""
    return dtype in OBJECT_DTYPES or dtype.kind in STRING_KINDS


def _normalize_array(data: np.ndarray, missing_value):
    """
    Coerce buffer data for an AdjustedArray into a standard scalar
    representation, returning the coerced array and a dict of argument to pass
    to np.view to use when providing a user-facing view of the underlying data.

    - float* data is coerced to float64 with viewtype float64.
    - int32, int64, and uint32 are converted to int64 with viewtype int64.
    - datetime[*] data is coerced to int64 with a viewtype of datetime64[ns].
    - bool_ data is coerced to uint8 with a viewtype of bool_.

    Parameters
    ----------
    data : np.ndarray

    Returns
    -------
    coerced, view_kwargs : (np.ndarray, np.dtype)
        The input ``data`` array coerced to the appropriate pipeline type.
        This may return the original array or a view over the same data.
    """
    if isinstance(data, LabelArray):
        return data, {}

    data_dtype = data.dtype
    if data_dtype in BOOL_DTYPES:
        return data.astype(np.uint8, copy=False), {"dtype": np.dtype(np.bool_)}
    elif data_dtype in FLOAT_DTYPES:
        return data.astype(np.float64, copy=False), {"dtype": np.dtype(np.float64)}
    elif data_dtype in INT_DTYPES:
        return data.astype(np.int64, copy=False), {"dtype": np.dtype(np.int64)}
    elif is_categorical(data_dtype):
        if not isinstance(missing_value, LabelArray.SUPPORTED_SCALAR_TYPES):
            raise TypeError(
                "Invalid missing_value for categorical array.\n"
                "Expected None, bytes or unicode. Got %r." % missing_value,
            )
        return LabelArray(data, missing_value), {}
    elif data_dtype.kind == "M":
        try:
            outarray = data.astype("datetime64[ns]", copy=False).view("int64")
            return outarray, {"dtype": datetime64ns_dtype}
        except OverflowError as exc:
            raise ValueError(
                "AdjustedArray received a datetime array "
                "not representable as datetime64[ns].\n"
                "Min Date: %s\n"
                "Max Date: %s\n" % (data.min(), data.max())
            ) from exc
    else:
        raise TypeError(
            "Don't know how to construct AdjustedArray "
            "on data of type %s." % data_dtype
        )


def _check_window_params(data: np.ndarray, window_length: int):
    """
    Check that a window of length `window_length` is well-defined on `data`.

    Parameters
    ----------
    data : np.ndarray[ndim=2]
        The array of data to check.
    window_length : int
        Length of the desired window.

    Returns
    -------
    None

    Raises
    ------
    WindowLengthNotPositive
        If window_length < 1.
    WindowLengthTooLong
        If window_length is greater than the number of rows in `data`.
    """
    if window_length < 1:
        raise WindowLengthNotPositive(window_length=window_length)

    if window_length > data.shape[0]:
        raise WindowLengthTooLong(
            nrows=data.shape[0],
            window_length=window_length,
        )
