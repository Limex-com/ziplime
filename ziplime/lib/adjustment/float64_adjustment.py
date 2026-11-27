"""Float64 adjustment for adjustment."""

from ziplime.lib.adjustment.adjustment import Adjustment
from ziplime.lib.adjustment.location_utils import _from_assets_and_dates


class Float64Adjustment(Adjustment):
    """
    Base class for adjustments that operate on Float64 data.
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 value: float) -> None:
        super(Float64Adjustment, self).__init__(
            first_row=first_row,
            last_row=last_row,
            first_col=first_col,
            last_col=last_col,
        )
        self.value = value

    from_assets_and_dates = classmethod(_from_assets_and_dates)

    def __repr__(self):
        return (
                "%s(first_row=%d, last_row=%d,"
                " first_col=%d, last_col=%d, value=%f)" % (
                    type(self).__name__,
                    self.first_row,
                    self.last_row,
                    self.first_col,
                    self.last_col,
                    self.value,
                )
        )
