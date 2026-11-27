"""Selection for cross."""

from types import ModuleType
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.cross.column_utils import _select


class Selection:
    """Some instruments of each combination: ``ranked[:k]``."""

    def __init__(self, xp: ModuleType, mask) -> None:
        self.xp, self.mask = xp, mask

    def sweep_contains(self, key):
        return _select(self.mask, key)

    def sweep_len(self):
        return self.xp.sum(self.mask, axis=1, keepdims=True)

    def __iter__(self):
        return iter(Vectorized([ALL], mask=self.mask))

    def sweep_vectorized(self):
        return Vectorized([ALL], mask=self.mask)
