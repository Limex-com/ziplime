"""Values for lazy."""

from ziplime.sweep.api import Data


class Param:
    """A value that differs between combinations, inside a record."""

    def __init__(self, value) -> None:
        self.value = value


class Leaf:
    """``data.history(assets=[asset], bar_count=n, fields=[field])[field]`` at the bar.
    ``column``: the instrument, or ALL inside a loop over the universe -- every one."""

    def __init__(self, data: Data, column, field: str, length, before: bool, daily: bool) -> None:
        self.data, self.column, self.field = data, column, field
        self.length, self.before, self.daily = length, before, daily
