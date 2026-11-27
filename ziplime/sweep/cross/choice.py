"""Choice for cross."""

from types import ModuleType
from ziplime.sweep.cross.column_utils import _select


class Choice:
    """One instrument of each combination: ``ranked[0]``, ``max(scores, key=...)``."""

    def __init__(self, xp: ModuleType, mask) -> None:
        self.xp, self.mask = xp, mask

    def sweep_equals_all(self):
        return self.mask

    def __eq__(self, other):
        return _select(self.mask, other)

    def __ne__(self, other):
        return self.xp.logical_not(self.__eq__(other))

    __hash__ = object.__hash__
