"""Float641 d array overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.array_adjustment import ArrayAdjustment


class Float641DArrayOverwrite(ArrayAdjustment):
    """
    An adjustment that overwrites subarrays with a value for each subarray.

    Example
    -------

    >>> import numpy as np
    >>> arr = np.arange(25, dtype=float).reshape(5, 5)
    >>> arr
    array([[  0.,   1.,   2.,   3.,   4.],
           [  5.,   6.,   7.,   8.,   9.],
           [ 10.,  11.,  12.,  13.,  14.],
           [ 15.,  16.,  17.,  18.,  19.],
           [ 20.,  21.,  22.,  23.,  24.]])
    >>> adj = Float641DArrayOverwrite(
    ...     row_start=0,
    ...     row_end=3,
    ...     column_start=0,
    ...     column_end=0,
    ...     values=np.array([1, 2, 3, 4]),
    )
    >>> adj.mutate(arr)
    >>> arr
    array([[  1.,   1.,   2.,   3.,   4.],
           [  2.,   6.,   7.,   8.,   9.],
           [ 3.,  11.,  12.,  13.,  14.],
           [ 4.,  16.,  17.,  18.,  19.],
           [ 20.,  21.,  22.,  23.,  24.]])
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 values: np.ndarray) -> None:
        super(Float641DArrayOverwrite, self).__init__(
            first_row=first_row,
            last_row=last_row,
            first_col=first_col,
            last_col=last_col,
        )
        if last_row + 1 - first_row != len(values):
            raise ValueError(
                "Mismatch: got %d values for rows starting at index %d and "
                "ending at index %d." % (len(values), first_row, last_row)
            )
        self.values = values

    def mutate(self, data: np.ndarray) -> None:
        values = self.values
        for col in range(self.first_col, self.last_col + 1):
            for i, row in enumerate(range(self.first_row, self.last_row + 1)):
                data[row, col] = values[i]
