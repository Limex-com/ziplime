"""Datetime64 overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.datetime64_adjustment import Datetime64Adjustment


class Datetime64Overwrite(Datetime64Adjustment):
    """
    An adjustment that overwrites with a datetime.

    This operates on int64 data which should be interpreted as nanoseconds
    since the epoch.

    Example
    -------

    >>> import numpy as np; import pandas as pd
    >>> dts = pd.date_range('2014', freq='D', periods=9, tz='UTC')
    >>> arr = dts.values.reshape(3, 3)
    >>> arr == np.datetime64(0, 'ns')
    array([[False, False, False],
           [False, False, False],
           [False, False, False]], dtype=bool)
    >>> adj = Datetime64Overwrite(
    ...     first_row=1,
    ...     last_row=2,
    ...     first_col=1,
    ...     last_col=2,
    ...     value=np.datetime64(0, 'ns'),
    ... )
    >>> adj.mutate(arr.view(np.int64))
    >>> arr == np.datetime64(0, 'ns')
    array([[False, False, False],
           [False,  True,  True],
           [False,  True,  True]], dtype=bool)
    """

    def mutate(self, data: np.ndarray) -> None:
        value = self.value

        # last_col + 1 because last_col should also be affected.
        for col in range(self.first_col, self.last_col + 1):
            # last_row + 1 because last_row should also be affected.
            for row in range(self.first_row, self.last_row + 1):
                data[row, col] = value
