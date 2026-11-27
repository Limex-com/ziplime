"""Boolean1 d array overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.array_adjustment import ArrayAdjustment


class Boolean1DArrayOverwrite(ArrayAdjustment):
    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 values: np.ndarray) -> None:
        if values.dtype.kind != 'b':
            raise TypeError('dtype is not bool, got: %r' % values.dtype)

        super(Boolean1DArrayOverwrite, self).__init__(
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
        self.values = values.view('uint8')

    def mutate(self, data: np.ndarray) -> None:
        values = self.values
        for col in range(self.first_col, self.last_col + 1):
            for i, row in enumerate(range(self.first_row, self.last_row + 1)):
                data[row, col] = values[i]

    def __repr__(self):
        return (
                "%s(first_row=%d, last_row=%d,"
                " first_col=%d, last_col=%d, value=%r)" % (
                    type(self).__name__,
                    self.first_row,
                    self.last_row,
                    self.first_col,
                    self.last_col,
                    self.value.view('?'),
                )
        )

    def __reduce__(self):
        return type(self), (
            self.first_row,
            self.last_row,
            self.first_col,
            self.last_col,
            self.value.view('?'),
        )
