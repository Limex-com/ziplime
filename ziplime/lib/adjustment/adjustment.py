"""Adjustment for adjustment."""

from ziplime.lib.adjustment.location_utils import _from_assets_and_dates


class Adjustment:
    """
    Base class for Adjustments.

    Subclasses should inherit and provide a `value` attribute and a `mutate`
    method.
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int) -> None:
        if not (0 <= first_row <= last_row):
            raise ValueError(
                'first_row must be in the range [0, last_row], got:'
                ' first_row=%s last_row=%s' % (
                    first_row,
                    last_row,
                ),
            )
        if not (0 <= first_col <= last_col):
            raise ValueError(
                'first_col must be in the range [0, last_col], got:'
                ' first_col=%s last_col=%s' % (
                    first_col,
                    last_col,
                ),
            )

        self.first_col = first_col
        self.last_col = last_col
        self.first_row = first_row
        self.last_row = last_row

    from_assets_and_dates = classmethod(_from_assets_and_dates)

    def __richcmp__(self, other, op: int):
        """
        Rich comparison method.  Only Equality is defined.
        """
        if type(self) is not type(other):
            return NotImplemented

        return self._key() == other._key()

    def _key(self) -> tuple:
        """
        Comparison key
        """
        return (
            self.first_row,
            self.last_row,
            self.first_col,
            self.last_col,
            self.value,
        )

    def __reduce__(self):
        return type(self), self._key()
