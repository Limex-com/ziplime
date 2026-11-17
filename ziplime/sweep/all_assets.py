"""The shared instrument sentinel used by vectorized sweep loops."""

import numpy as np

from ziplime.sweep.errors import NotConvertible


class _AllAssets:
    """The name and the instrument inside a loop over context.universe: every instrument.

    ``name == "JNJ"`` or ``asset == context.universe["JNJ"]`` is a mask over the instruments."""

    #: Set by the simulation: (names, listings, make an array)
    _columns = None

    def __repr__(self) -> str:
        return "<every instrument>"

    def __eq__(self, other):
        if other is self:
            return True
        if hasattr(other, "sweep_equals_all"):
            return other.sweep_equals_all()
        names, listings, array = self.__dict__.get("_columns") or type(self)._columns
        return array(np.array([[name == other or listing == other
                                or getattr(listing, "sid", None) == other
                                for name, listing in zip(names, listings)]]))

    def __ne__(self, other):
        equal = self.__eq__(other)
        return (not equal) if isinstance(equal, bool) else ~equal

    __hash__ = object.__hash__

    @property
    def sid(self):
        """``asset.sid``: every instrument's sid -- a key and a filter like the instrument."""
        return self

    def __getattr__(self, name: str):
        raise NotConvertible(
            f"Inside a loop over context.universe one pass handles every instrument at once, so "
            f"the instrument has no single .{name}.")


ALL = _AllAssets()
