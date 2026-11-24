"""Signals for api."""

from ziplime.sweep.backends import Backend
from ziplime.sweep.api.by_name import ByName
from ziplime.sweep.api.columns import Columns


class Signals:
    def __init__(self, backend: Backend, columns: Columns) -> None:
        self._backend, self._columns, self.now = backend, columns, {}

    def _raw(self, name: str):
        try:
            return self.now[name]
        except KeyError:
            raise KeyError(f"No signal {name!r}. There are: {sorted(self.now)}.") from None

    def __getitem__(self, name: str) -> ByName:
        return ByName(self._raw(name), self._columns)

    def is_ready(self, name: str):
        """(combinations × 1): the signal has a number for some instrument at this bar."""
        xp, row = self._backend.xp, self._raw(name)
        if row.dtype == self._backend.bool:
            return True
        return ~xp.all(xp.isnan(row), axis=1, keepdims=True)
