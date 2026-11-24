"""Ranking for cross."""

from types import ModuleType
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.cross.choice import Choice
from ziplime.sweep.cross.selection import Selection
from ziplime.sweep.cross.column_utils import _select


class Ranking:
    """``sorted(...)`` of instruments: each instrument's place within its combination -- and a
    list of instruments that differs between combinations. ``items``: {column: the object the
    strategy put in the list for it (its name or its listing)}."""

    def __init__(self, xp: ModuleType, places, present, items=None) -> None:
        self.xp, self.places, self.present = xp, places, present
        self.items = items or {}

    def _count(self):
        return self.xp.sum(self.present, axis=1, keepdims=True)

    def __getitem__(self, item):
        xp, count = self.xp, self._count()

        def place(value, default):
            if value is None:
                return default
            if not is_array(value) and value < 0:
                return count + value
            return value

        if isinstance(item, slice):
            if item.step not in (None, 1):
                raise NotConvertible("A ranking can be sliced from the top ([:k]) or the bottom "
                                     "([-k:]) only.")
            start, stop = place(item.start, 0), place(item.stop, count)
            chosen = xp.logical_and(xp.logical_and(self.places >= start, self.places < stop),
                                    self.present)
            return Selection(xp, chosen)
        if isinstance(item, int):
            return Choice(xp, xp.logical_and(self.places == place(item, 0), self.present))
        raise NotConvertible("A ranking is indexed by a position or sliced by places.")

    def sweep_vectorized(self):
        return Vectorized([ALL], mask=self.present)

    def __iter__(self):
        return iter(Vectorized([ALL], mask=self.present))

    def sweep_len(self):
        return self._count()

    def sweep_contains(self, key):
        return _select(self.present, key)
