"""Array adjustment for adjustment."""

from numpy import asarray
from ziplime.lib.adjustment.adjustment import Adjustment


class ArrayAdjustment(Adjustment):
    """
    Base class for ArrayAdjustments.

    Subclasses should inherit and provide a `values` attribute and a `mutate`
    method.
    """

    def __repr__(self):
        return (
                "%s(first_row=%d, last_row=%d,"
                " first_col=%d, last_col=%d, values=%s)" % (
                    type(self).__name__,
                    self.first_row,
                    self.last_row,
                    self.first_col,
                    self.last_col,
                    asarray(self.values),
                )
        )
