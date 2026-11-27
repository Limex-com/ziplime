"""Boolean adjustment for adjustment."""

from ziplime.lib.adjustment.adjustment import Adjustment


class BooleanAdjustment(Adjustment):
    """
    Base class for adjustments that operate on boolean data.

    Notes
    -----
    Numpy stores boolean values in arrays of type uint8.  There's no
    straightforward way to work with statically-typed boolean data, so
    instead we work with uint8 values everywhere, and we do validation/coercion
    at API boundaries.
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 value: int) -> None:  # bint -> boolean int

        super(BooleanAdjustment, self).__init__(
            first_row=first_row,
            last_row=last_row,
            first_col=first_col,
            last_col=last_col,
        )
        self.value = value

    def __repr__(self):
        return (
                "%s(first_row=%d, last_row=%d,"
                " first_col=%d, last_col=%d, value=%r)" % (
                    type(self).__name__,
                    self.first_row,
                    self.last_row,
                    self.first_col,
                    self.last_col,
                    bool(self.value),
                )
        )

    def __reduce__(self):
        return type(self), (
            self.first_row,
            self.last_row,
            self.first_col,
            self.last_col,
            bool(self.value),
        )
