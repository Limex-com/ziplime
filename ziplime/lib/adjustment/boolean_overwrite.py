"""Boolean overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.boolean_adjustment import BooleanAdjustment


class BooleanOverwrite(BooleanAdjustment):
    """
    An adjustment that overwrites with a boolean.

    This operates on uint8 data.
    """

    def mutate(self, data: np.ndarray) -> None:
        value = self.value

        # last_col + 1 because last_col should also be affected.
        for col in range(self.first_col, self.last_col + 1):
            # last_row + 1 because last_row should also be affected.
            for row in range(self.first_row, self.last_row + 1):
                data[row, col] = value
