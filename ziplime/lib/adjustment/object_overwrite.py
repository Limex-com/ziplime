"""Object overwrite for adjustment."""

import numpy as np
from ziplime.lib.adjustment.object_adjustment import _ObjectAdjustment


class ObjectOverwrite(_ObjectAdjustment):

    def mutate(self, data: np.ndarray) -> None:
        # data is an object here because this is intended to be used with a
        # `ziplime.lib.LabelArray`.

        # We don't do this in a loop because we only want to look up the label
        # code in the array's categories once.
        data.set_scalar(
            (
                slice(self.first_row, self.last_row + 1),
                slice(self.first_col, self.last_col + 1),
            ),
            self.value,
        )
