"""Adjusted array for adjusted_array."""

import numpy as np
from ziplime.lib.labelarray import LabelArray
from ziplime.lib.windowtemplate import AdjustedArrayWindow
from ziplime.lib.adjustment import Adjustment
from ziplime.lib.adjusted_array.support import CONCRETE_WINDOW_TYPES
from ziplime.lib.adjusted_array.utils import _check_window_params
from ziplime.lib.adjusted_array.utils import _normalize_array


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

    def __init__(self, data: np.ndarray, adjustments: dict[int, list[Adjustment]], missing_value) -> None:
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

    # @lazyval
    @property
    def dtype(self):
        """
        The dtype of the data stored in this array.
        """
        return self._view_kwargs.get("dtype") or self._data.dtype

    # @lazyval
    @property
    def _iterator_type(self):
        """
        The iterator produced when `traverse` is called on this Array.
        """
        if isinstance(self._data, LabelArray):
            return AdjustedArrayWindow
        return CONCRETE_WINDOW_TYPES[self._data.dtype]

    def traverse(self, window_length: int, offset: int = 0, perspective_offset: int = 0, copy: bool = True):
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
