"""Window for api."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep import api
from ziplime.sweep.api.window_column import WindowColumn


class Window:
    def __init__(self, data: 'api.Data', asset, length, fields, frequency=None) -> None:
        self._data, self._asset, self._length, self._fields = data, asset, length, fields
        self._frequency = frequency

    def __getitem__(self, field: str) -> WindowColumn:
        if self._fields is not None and field not in self._fields:
            raise KeyError(f"Field {field!r} was not asked for.")
        return WindowColumn(self._data, self._asset, field, self._length, self._frequency)

    def filter(self, *args, **kwargs):
        raise NotConvertible("A history frame of one instrument is not filtered in a sweep; read "
                             "its statistics: window['close'].mean().")
