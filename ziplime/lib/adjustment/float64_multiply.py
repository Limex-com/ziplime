"""Float64 multiply for adjustment."""

import numpy as np
from ziplime.lib.adjustment.float64_adjustment import Float64Adjustment


class Float64Multiply(Float64Adjustment):
    """
    An adjustment that multiplies by a float.

    Example
    -------

    >>> import numpy as np
    >>> arr = np.arange(9, dtype=float).reshape(3, 3)
    >>> arr
    array([[ 0.,  1.,  2.],
           [ 3.,  4.,  5.],
           [ 6.,  7.,  8.]])

    >>> adj = Float64Multiply(
    ...     first_row=1,
    ...     last_row=2,
    ...     first_col=1,
    ...     last_col=2,
    ...     value=4.0,
    ... )
    >>> adj.mutate(arr)
    >>> arr
    array([[  0.,   1.,   2.],
           [  3.,  16.,  20.],
           [  6.,  28.,  32.]])
    """

    def mutate(self, data: np.ndarray) -> None:
        value = self.value

        # last_col + 1 because last_col should also be affected.
        for col in range(self.first_col, self.last_col + 1):
            # last_row + 1 because last_row should also be affected.
            for row in range(self.first_row, self.last_row + 1):
                data[row, col] *= value
