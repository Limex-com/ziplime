"""Containers that iterate over sweep instruments in one vectorized pass."""

from ziplime.sweep.all_assets import ALL


class Vectorized(list):
    """A loop over context.universe: one pass, for every instrument. ``mask``: the instruments the
    pass is for -- a per-instrument dict's keys, say -- True for all of them."""

    mask = True

    def __init__(self, items, mask=True) -> None:
        super().__init__(items)
        self.mask = mask


class VectorUniverse(dict):
    """context.universe in a sweep: len and lookup by name as a dict, iteration in one pass."""

    def items(self) -> Vectorized:
        return Vectorized([(ALL, ALL)])

    def keys(self) -> Vectorized:
        return Vectorized([ALL])

    def values(self) -> Vectorized:
        return Vectorized([ALL])

    def __iter__(self):
        return iter(Vectorized([ALL]))
