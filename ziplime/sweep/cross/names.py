"""Names for cross."""

from types import ModuleType
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.cross.column_utils import _select


class Names(Vectorized):
    """A comprehension over instruments that yields the instruments themselves
    (``[name for name in scores if scores[name] > 0]``): a selection."""

    def __init__(self, xp: ModuleType, mask) -> None:
        super().__init__([ALL], mask=mask)
        self.xp = xp

    def sweep_len(self):
        return self.xp.sum(self.mask, axis=1, keepdims=True)

    def sweep_contains(self, key):
        return _select(self.mask, key)
