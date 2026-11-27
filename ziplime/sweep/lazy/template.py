"""Template for lazy."""

from ziplime.sweep.all_assets import ALL
from ziplime.sweep.lazy.lazy import Lazy
from ziplime.sweep.lazy.values import Param
from ziplime.sweep.lazy.replay_utils import _function_key
from ziplime.sweep.lazy.recording import _wrap


class _Template:
    """A record's key and what replaying it needs: its parameters in order, and whether it
    reads the loop's instrument (then it is computed for every instrument)."""

    def __init__(self, value: Lazy) -> None:
        self.params = []
        self.per_column = False
        self.key = self._key(value)

    def _key(self, value):
        if isinstance(value, Lazy):
            node = value.node
            if node[0] == "leaf":
                spec = node[1]
                if spec.column is ALL:
                    self.per_column = True
                return ("leaf", "all" if spec.column is ALL else spec.column, spec.field,
                        self._key(_wrap(spec.length)), spec.before, spec.daily)
            if node[0] == "attr":
                return ("attr", self._key(node[1]), node[2])
            return ("call", _function_key(node[1]), tuple(self._key(arg) for arg in node[2]),
                    tuple((name, self._key(arg)) for name, arg in node[3]))
        if isinstance(value, Param):
            self.params.append(value.value)
            return ("param", len(self.params) - 1)
        if isinstance(value, slice):
            return ("slice", self._key(value.start), self._key(value.stop), self._key(value.step))
        if isinstance(value, (list, tuple)):
            return (type(value).__name__, tuple(self._key(item) for item in value))
        if isinstance(value, float) and value != value:
            return ("nan",)
        if isinstance(value, (dict, set, frozenset)):
            return ("object", id(value))
        if callable(value):
            return ("function", _function_key(value))
        return ("value", type(value).__name__, value)
