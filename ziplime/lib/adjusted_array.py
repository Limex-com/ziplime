import numpy as np

from ziplime.errors import (
    WindowLengthNotPositive,
    WindowLengthTooLong,
)
from ziplime.lib.labelarray import LabelArray
from ziplime.lib.windowtemplate import AdjustedArrayWindow
from ziplime.utils.numpy_utils import (
    datetime64ns_dtype,
    float64_dtype,
    int64_dtype,
    uint8_dtype,
)
# These class names are all the same because of our bootleg templating system.


BOOL_DTYPES = frozenset(
    map(np.dtype, [np.bool_, np.uint8]),
)
FLOAT_DTYPES = frozenset(
    map(np.dtype, [np.float32, np.float64]),
)
INT_DTYPES = frozenset(
    # NOTE: uint64 not supported because it can't be safely cast to int64.
    map(np.dtype, [np.int16, np.uint16, np.int32, np.int64, np.uint32]),
)
DATETIME_DTYPES = frozenset(map(np.dtype, ["datetime64[ns]"]))
# We use object arrays for strings.
OBJECT_DTYPES = frozenset(map(np.dtype, ["O"]))
STRING_KINDS = frozenset(["S", "U"])

REPRESENTABLE_DTYPES = BOOL_DTYPES.union(
    FLOAT_DTYPES,
    INT_DTYPES,
    DATETIME_DTYPES,
    OBJECT_DTYPES,
)


def can_represent_dtype(dtype):
    """Can we build an AdjustedArray for a baseline of `dtype``?"""
    return dtype in REPRESENTABLE_DTYPES or dtype.kind in STRING_KINDS


def is_categorical(dtype):
    """Do we represent this dtype with LabelArrays rather than ndarrays?"""
    return dtype in OBJECT_DTYPES or dtype.kind in STRING_KINDS


CONCRETE_WINDOW_TYPES = {
    float64_dtype: AdjustedArrayWindow,
    int64_dtype: AdjustedArrayWindow,
    uint8_dtype: AdjustedArrayWindow,
}


def _normalize_array(data, missing_value):
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


class AdjustedArray:
    """
    An array that can be iterated with a variable-length window, and which can
    provide different views on data from different perspectives.

    Parameters
    ----------
    data : np.ndarray
        The baseline data values. This array may be mutated by
        ``traverse(..., copy=False)`` calls.
    adjustments : dict[int -> list[Adjustment]]
        A dict mapping row indices to lists of adjustments to apply when we
        reach that row.
    missing_value : object
        A value to use to fill missing data in yielded windows.
        Should be a value coercible to `data.dtype`.
    """

    __slots__ = (
        "_data",
        "_view_kwargs",
        "adjustments",
        "missing_value",
        "_invalidated",
        "__weakref__",
    )

    def __init__(self, data, adjustments, missing_value):
        self._data, self._view_kwargs = _normalize_array(data, missing_value)

        self.adjustments = adjustments
        self.missing_value = missing_value
        self._invalidated = False

    def copy(self):
        """Copy an adjusted array, deep-copying the ``data`` array."""
        if self._invalidated:
            raise ValueError("cannot copy invalidated AdjustedArray")

        return type(self)(
            self.data.copy(order="F"),
            self.adjustments,
            self.missing_value,
        )

    @property
    def data(self):
        """
        The data stored in this array.
        """
        return self._data.view(**self._view_kwargs)

    #@lazyval
    @property
    def dtype(self):
        """
        The dtype of the data stored in this array.
        """
        return self._view_kwargs.get("dtype") or self._data.dtype

    #@lazyval
    @property
    def _iterator_type(self):
        """
        The iterator produced when `traverse` is called on this Array.
        """
        if isinstance(self._data, LabelArray):
            return AdjustedArrayWindow
        return CONCRETE_WINDOW_TYPES[self._data.dtype]

    def traverse(self, window_length, offset=0, perspective_offset=0, copy=True):
        """
        Produce an iterator rolling windows rows over our data.
        Each emitted window will have `window_length` rows.

        Parameters
        ----------
        window_length : int
            The number of rows in each emitted window.
        offset : int, optional
            Number of rows to skip before the first window.  Default is 0.
        perspective_offset : int, optional
            Number of rows past the end of the current window from which to
            "view" the underlying data.
        copy : bool, optional
            Copy the underlying data. If ``copy=False``, the adjusted array
            will be invalidated and cannot be traversed again.
        """
        if self._invalidated:
            raise ValueError("cannot traverse invalidated AdjustedArray")

        data = self._data
        if copy:
            data = data.copy(order="F")
        else:
            self._invalidated = True

        _check_window_params(data, window_length)
        return self._iterator_type(
            data,
            self._view_kwargs,
            self.adjustments,
            offset,
            window_length,
            perspective_offset,
            rounding_places=None,
        )


def ensure_adjusted_array(ndarray_or_adjusted_array, missing_value):
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


def ensure_ndarray(ndarray_or_adjusted_array):
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


def _check_window_params(data, window_length):
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
