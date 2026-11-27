"""Datetime641 d array overwrite for adjustment."""

import numpy as np
from numpy import asarray
from ziplime.lib.adjustment.array_adjustment import ArrayAdjustment
from ziplime.lib.adjustment.datetime_utils import datetime_to_int


class Datetime641DArrayOverwrite(ArrayAdjustment):
    """
    An adjustment that overwrites subarrays with a value for each subarray.

    Example
    -------

    >>> import numpy as np; import pandas as pd
    >>> dts = pd.date_range('2014', freq='D', periods=9, tz='UTC')
    >>> arr = dts.values.reshape(3, 3)
    >>> arr == np.datetime64(0, 'ns')
    array([[False, False, False],
       [False, False, False],
       [False, False, False]], dtype=bool)
    >>> adj = Datetime641DArrayOverwrite(
    ...           first_row=1,
    ...           last_row=2,
    ...           first_col=1,
    ...           last_col=2,
    ...           values=np.array([
    ...               np.datetime64(0, 'ns'),
    ...               np.datetime64(1, 'ns')
    ...           ])
    ...       )
    >>> adj.mutate(arr.view(np.int64))
    >>> arr == np.datetime64(0, 'ns')
    array([[False, False, False],
       [False,  True,  True],
       [False, False, False]], dtype=bool)
    >>> arr == np.datetime64(1, 'ns')
    array([[False, False, False],
       [False, False, False],
       [False,  True,  True]], dtype=bool)
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 values: np.ndarray) -> None:
        super(Datetime641DArrayOverwrite, self).__init__(
            first_row=first_row,
            last_row=last_row,
            first_col=first_col,
            last_col=last_col,
        )
        if last_row + 1 - first_row != len(values):
            raise ValueError("Mismatch: got %d values for rows starting at"
                             " index %d and ending at index %d." % (
                                 len(values), first_row, last_row)
                             )
        self.values = asarray([datetime_to_int(value) for value in values])

    def mutate(self, data: np.ndarray) -> None:
        values = self.values
        for col in range(self.first_col, self.last_col + 1):
            for i, row in enumerate(range(self.first_row, self.last_row + 1)):
                data[row, col] = values[i]

    def __reduce__(self):
        return type(self), (
            self.first_row,
            self.last_row,
            self.first_col,
            self.last_col,
            self.values.view('datetime64[ns]'),
        )
