"""By name for api."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.api.columns import Columns


class ByName:
    """``signals["fast"]``: ``[name]`` -- that instrument's column, ``[every instrument]`` -- all."""

    def __init__(self, values, columns: Columns) -> None:
        self._values, self._columns = values, columns

    def __getitem__(self, name):
        if name is ALL:
            return self._values
        try:
            column = self._columns.by_name[name]
        except KeyError:
            raise KeyError(f"No instrument {name!r}; there are {self._columns.names}.") from None
        return self._values[:, column:column + 1]
