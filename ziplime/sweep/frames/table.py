"""Table for frames."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.frames.history import History
from ziplime.sweep.frames.row import Row
from ziplime.sweep.frames.validation import _refuse


class Table:
    """``group_by("sid").agg(...)``: a row per instrument that has rows in the frame."""

    def __init__(self, frame: History, statistics: list) -> None:
        self.columns = ["sid"] + [each.name for each in statistics]
        if len(set(self.columns)) != len(self.columns):
            raise NotConvertible(f"group_by('sid').agg(...) makes the columns {self.columns}; "
                                 f"give each a different name with .alias(...).")
        self._xp = frame._xp
        self._values = {each.name: each.compute(frame._read) for each in statistics}
        self._present = frame._present(statistics[0].field)

    def iter_rows(self, *, named: bool = False):
        if named:
            return Vectorized([Row(self._values)], mask=self._present)
        return Vectorized([(ALL, *[self._values[name] for name in self.columns[1:]])],
                          mask=self._present)

    def rows(self, *, named: bool = False):
        return self.iter_rows(named=named)

    def is_empty(self):
        return self._xp.logical_not(self._xp.any(self._present, axis=1, keepdims=True))

    def __getitem__(self, name: str):
        raise NotConvertible(f"table[{name!r}] is not supported in a sweep; read the rows: "
                             f"for row in table.iter_rows(named=True): row['sid'], row[...].")

    def __len__(self):
        raise NotConvertible("len() of a grouped frame is not supported in a sweep.")

    def __iter__(self):
        raise NotConvertible("Iterate a grouped frame through iter_rows() in a sweep.")

    def __getattr__(self, name: str):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        _refuse(f"group_by('sid').agg(...).{name}")
