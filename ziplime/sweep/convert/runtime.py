"""Runtime for convert."""

from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import Vectorized


# -- what converted code calls ------------------------------------------------------------------
class Runtime:
    """Masks and elementwise logic. A mask is True/False (the same everywhere) or an array."""

    def __init__(self, xp: ModuleType) -> None:
        self.xp = xp
        #: The current statement's mask: no order and no assignment where it is False
        self.mask = True
        #: NaN and infinity as arrays the simulation hands in. MLX writes a NaN or infinity
        #: constant into a fused Metal kernel as an undeclared identifier and the compile fails,
        #: so inside a compiled step they must be inputs, never literals.
        self.nan, self.inf = np.nan, np.inf

    def truth(self, value):
        from ziplime.sweep.lazy import is_lazy, value_of
        if is_lazy(value):
            value = value_of(value)
        elif hasattr(value, "sweep_truth"):
            value = value.sweep_truth()
        elif not is_array(value) and hasattr(value, "sweep_len"):
            count = value.sweep_len()
            value = count > 0 if is_array(count) else bool(count)
        if is_array(value):
            return value if value.dtype in (np.bool_, getattr(self.xp, "bool_", np.bool_)) \
                else value != 0
        return bool(value)

    def and_(self, left, right: Callable):
        """``left and right``; right is a function, not evaluated when left is False."""
        left = self.truth(left)
        if left is False:
            return False
        right = self.truth(right())
        if left is True:
            return right
        if right is True:
            return left
        if right is False:
            return False
        return self.xp.logical_and(left, right)

    def or_(self, left, right: Callable):
        left = self.truth(left)
        if left is True:
            return True
        right = self.truth(right())
        if left is False:
            return right
        if right is False:
            return left
        if right is True:
            return True
        return self.xp.logical_or(left, right)

    def not_(self, value):
        value = self.truth(value)
        return (not value) if isinstance(value, bool) else self.xp.logical_not(value)

    def alive(self, dead, skip):
        return self.not_(self.or_(dead, lambda: skip))

    def sel(self, mask, new, old: Callable):
        """Assignment under a mask: the new value where it holds, the previous one elsewhere."""
        if mask is True:
            return new
        try:
            previous = old()
        except (NameError, AttributeError, KeyError, IndexError):
            return new  # not defined before -- where the mask is False it is never read
        if mask is False:
            return previous
        return self.choose(mask, new, previous)

    def choose(self, mask, new, previous):
        """``new`` where the mask holds, ``previous`` elsewhere: for numbers, computations on
        history (chosen once they are numbers) and lists of instruments."""
        from ziplime.sweep import cross
        from ziplime.sweep.lazy import Select, is_lazy

        if is_lazy(new) or is_lazy(previous):
            return Select(mask, new, previous)
        merged = cross.merge_instruments(self.xp, mask, new, previous)
        if merged is not None:
            return merged
        if previous is None or new is None:
            raise NotConvertible("A variable is None in some combinations and a number in others; "
                                 "give it a number (0, float('nan')) instead of None.")
        return self.xp.where(mask, new, previous)

    def returned(self, mask, value, previous):
        """A ``return value`` under a mask: lanes that returned earlier keep their value."""
        if mask is True or previous is None:
            return value
        if mask is False:
            return previous
        return self.xp.where(mask, value, previous)

    def where(self, condition, yes, no):
        condition = self.truth(condition)
        if isinstance(condition, bool):
            return yes if condition else no
        return self.choose(condition, yes, no)

    def sort_list(self, items, key: Callable | None = None, reverse: bool = False):
        """``items.sort(key=..., reverse=...)``: a list of instruments ranked per combination;
        any other list sorted in place, as Python does."""
        from ziplime.sweep import cross
        ranked = cross.sort_instruments(self, items, key, reverse)
        if ranked is not None:
            return ranked
        items.sort(key=key, reverse=reverse)
        return items

    def is_none(self, value):
        """``value is None``: a position that is not open is None in the engine."""
        if hasattr(value, "sweep_truth"):
            return self.not_(value.sweep_truth())
        return value is None

    def nested(self, function: Callable):
        """A function defined inside a converted one: it runs under the mask it is called with."""
        def call(*args, **kwargs):
            saved = self.mask
            try:
                return function(*args, **kwargs)
            finally:
                self.mask = saved
        return call

    def plain(self, condition, line: int, what: str):
        """A condition Python has to decide by itself: it must be the same everywhere."""
        condition = self.truth(condition)
        if isinstance(condition, bool):
            return condition
        raise NotConvertible(f"line {line}: {what} on a value that differs between combinations "
                             f"or instruments cannot run on a GPU.")

    def loop_mask(self, iterable):
        return getattr(iterable, "mask", True) if isinstance(iterable, Vectorized) else True

    def contains(self, item, container):
        """``item in container``: elementwise for the sweep's per-instrument containers."""
        if hasattr(container, "sweep_contains"):
            return container.sweep_contains(item)
        if item is ALL:
            raise NotConvertible("`instrument in container` inside a loop over instruments needs a "
                                 "per-instrument dict on the context; a plain list or set holds "
                                 "single instruments.")
        return item in container

    def delete(self, container, key, line: int) -> None:
        if hasattr(container, "sweep_delete"):
            container.sweep_delete(key)
        elif self.plain(self.mask, line, "del"):
            del container[key]

    def comprehension(self, iterable, value: Callable, condition: Callable, arity: int, kind: str):
        if hasattr(iterable, "sweep_vectorized"):
            iterable = iterable.sweep_vectorized()
        from ziplime.sweep.cross import cross_section_of
        vectorized = isinstance(iterable, Vectorized) or type(iterable).__name__ == "VectorUniverse"
        if vectorized:
            items = list(iterable)
            if len(items) == 1:
                item = items[0]
                arguments = item if arity > 1 else (item,)
                mask = self.and_(self.loop_mask(iterable), lambda: condition(*arguments))
                if kind == "dict":
                    return self._dict_of(*value(*arguments), mask)
                return cross_section_of(self.xp, value(*arguments), mask)

        def plain():
            for item in iterable:
                arguments = item if arity > 1 else (item,)
                if self.plain(condition(*arguments), 0, "a comprehension's if"):
                    yield value(*arguments)
        if kind == "dict":
            return self.dict(dict(plain()))
        return list(plain()) if kind == "list" else plain()

    def _dict_of(self, key, value, mask):
        """``{key: value for ... over instruments if ...}``: the entries where the mask holds."""
        from ziplime.sweep.cross import SweepDict, column_mask

        result = SweepDict(self)
        if column_mask(key) is None:
            raise NotConvertible("A dict built over instruments is keyed by the instrument (its "
                                 "name, listing or sid) in a sweep.")
        result._write(key, value, where=mask)
        return result

    def dict(self, value):
        """A dict the strategy builds: one that holds instruments as arrays (see cross.py)."""
        from ziplime.sweep.cross import SweepDict
        return SweepDict(self, value)

    def loop(self, iterable, carried, line: int):
        if hasattr(iterable, "sweep_vectorized"):
            iterable = iterable.sweep_vectorized()
        if isinstance(iterable, Vectorized) and carried:
            raise NotConvertible(
                f"line {line}: the loop over instruments carries {', '.join(carried)} from one "
                f"instrument to the next. Every instrument is processed at once, so one "
                f"instrument's decision cannot depend on the previous one.")
        return iterable
