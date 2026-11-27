"""Signal row for signals."""

import pandas as pd


class SignalRow:
    """One bar of one signal: ``row["JNJ"]``, or a bare value for a per-portfolio signal."""

    def __init__(self, name: str, value) -> None:
        self._name = name
        self._value = value

    def __getitem__(self, column: str):
        if not isinstance(self._value, pd.Series):
            raise TypeError(
                f"Signal {self._name!r} is one value per bar, not one per instrument, so "
                f"[{column!r}] means nothing. Use it directly.")
        try:
            return self._value[column]
        except KeyError:
            raise KeyError(
                f"Signal {self._name!r} has no column {column!r}. It has: "
                f"{', '.join(map(str, self._value.index))}.") from None

    def __iter__(self):
        return iter(self._value.items()) if isinstance(self._value, pd.Series) else iter(())

    def __bool__(self) -> bool:
        if isinstance(self._value, pd.Series):
            raise ValueError(
                f"Signal {self._name!r} holds one value per instrument, so its truth is "
                f"ambiguous. Ask for one: context.signals[{self._name!r}]['TICKER'].")
        return bool(self._value)

    def __float__(self) -> float:
        return float(self._value)

    def __repr__(self) -> str:
        return f"SignalRow({self._name!r}, {self._value!r})"

    @property
    def value(self):
        """The underlying scalar or :class:`pandas.Series`."""
        return self._value
