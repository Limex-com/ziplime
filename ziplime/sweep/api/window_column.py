"""Window column for api."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep import api


class WindowColumn:
    """One field of ``data.history(assets=[asset], bar_count=n, ...)``: its statistics."""

    def __init__(self, data: 'api.Data', asset, field: str, length, frequency=None) -> None:
        self._data, self._asset, self._field, self._length = data, asset, field, length
        self._frequency = frequency

    def _op(self, op: str):
        return self._data._window(self._asset, self._field, op, self._length, self._frequency)

    def mean(self):
        return self._op("mean")

    def sum(self):
        return self._op("sum")

    def std(self, ddof: int = 1):
        if ddof != 1:
            raise NotConvertible("history(...).std() is supported with ddof=1 only.")
        return self._op("std")

    def var(self, ddof: int = 1):
        if ddof != 1:
            raise NotConvertible("history(...).var() is supported with ddof=1 only.")
        return self._op("var")

    def min(self):
        return self._op("min")

    def max(self):
        return self._op("max")

    def median(self):
        return self._op("median")

    def first(self):
        return self._op("first")

    def last(self):
        return self._op("last")

    def count(self):
        return self._op("count")

    def len(self):
        return self._op("count")

    def __len__(self):
        raise NotConvertible("len(window) is a different number per combination; use "
                             "window['close'].count().")

    def sweep_len(self):
        from ziplime.sweep.lazy import record
        return record(len, (self._lazy(),))

    def __getitem__(self, k):
        if isinstance(k, int) and not isinstance(k, bool) and k == 0:
            return self._op("first")
        if isinstance(k, int) and not isinstance(k, bool) and k == -1:
            return self._op("last")
        return self._lazy()[k]

    def _lazy(self):
        """The rows themselves, for the strategy's own computations on them (lazy.py)."""
        from ziplime.sweep.lazy import leaf

        data = self._data
        column = ALL if self._asset is ALL else data._columns.index(self._asset)
        return leaf(data, column, self._field, self._length, data.before,
                    data._daily(self._frequency))

    def __getattr__(self, name: str):
        # Anything else of the engine's polars Series -- to_numpy(), pct_change(), rolling_mean()
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "mask"):
            raise AttributeError(name)
        return getattr(self._lazy(), name)
