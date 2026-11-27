"""Object1 d array overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.array_adjustment import ArrayAdjustment


class Object1DArrayOverwrite(ArrayAdjustment):
    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 values: np.ndarray) -> None:
        super(Object1DArrayOverwrite, self).__init__(
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
        self.values = values[:, None]

    def mutate(self, data: np.ndarray) -> None:
        # data is an object here because this is intended to be used with a
        # `ziplime.lib.LabelArray`.

        data[
            self.first_row:self.last_row + 1,
            self.first_col:self.last_col + 1,
        ] = self.values
