"""Object method for lazy."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.lazy.objects import Objects
from ziplime.sweep.lazy.object_call import _ObjectCall
from ziplime.sweep.lazy.recording import _is_sweep_value
from ziplime.sweep.lazy.recording import contains_lazy
from ziplime.sweep.lazy.recording import record
from ziplime.sweep.lazy.support import where_in_strategy


class _ObjectMethod:
    def __init__(self, owner: Objects, name: str) -> None:
        self.owner, self.name = owner, name

    def __call__(self, *args, **kwargs):
        owner = self.owner
        if contains_lazy(args, kwargs):
            position = 0 if owner._index is None else owner._index
            return record(_ObjectCall(owner._objects, self.name), (position, *args), kwargs)
        if any(is_array(a) or a is ALL or _is_sweep_value(a) for a in (*args, *kwargs.values())):
            raise NotConvertible(f"{where_in_strategy()}: .{self.name}(...) of an object kept on "
                                 f"the context gets values that differ between combinations. On "
                                 f"features computed from history rows (data.history(...)"
                                 f"['close'].to_numpy()) it is computed ahead of the run.")
        if owner._index is not None:
            raise NotConvertible(f"{where_in_strategy()}: .{self.name}(...) of an object that "
                                 f"differs between combinations, on plain numbers.")
        return getattr(owner._objects[0], self.name)(*args, **kwargs)
