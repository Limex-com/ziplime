"""Column for api."""

from ziplime.sweep.errors import NotConvertible


class Column:
    """One field of ``data.current(...)`` / ``data.history(...)``: ``[k]`` is the k-th asset asked
    for (``data.current(assets=[a, b])["close"][1]``)."""

    def __init__(self, values: list) -> None:
        self._values = values

    def __getitem__(self, k):
        if isinstance(k, int):
            return self._values[k]
        raise NotConvertible("Index a data.current column by position: frame['close'][0].")

    def __len__(self) -> int:
        return len(self._values)
