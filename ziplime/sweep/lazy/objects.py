"""Objects for lazy."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.lazy.support import CHANGING
from ziplime.sweep.lazy.support import where_in_strategy


class Objects:
    """``context.model``: one object for every combination, or one per distinct run of initialize
    (a model trained with the config's parameters). Its methods on history rows are recorded --
    ``context.model.predict(features)`` is then replayed before the run like ``talib.RSI(p)``, and
    which object a combination uses is a parameter of the record, so each model is asked once.
    ``index``: None for one object, or (combinations × 1) positions in ``objects``."""

    def __init__(self, objects: list, index=None) -> None:
        object.__setattr__(self, "_objects", objects)
        object.__setattr__(self, "_index", index)

    def __getattr__(self, name: str):
        from ziplime.sweep.lazy.object_method import _ObjectMethod

        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "ndim"):
            raise AttributeError(name)
        if name in CHANGING:
            raise NotConvertible(f"{where_in_strategy()}: .{name}() changes an object kept on the "
                                 f"context while the run goes on; a sweep replays what is computed "
                                 f"from history before the run. Train it in initialize (it may "
                                 f"read the config) or in compute_signals.")
        attribute = getattr(self._objects[0], name)
        if not callable(attribute):
            if self._index is None:
                return attribute
            raise NotConvertible(f"{where_in_strategy()}: .{name} of an object that differs "
                                 f"between combinations is not supported in a sweep.")
        return _ObjectMethod(self, name)

    def __setattr__(self, name: str, value):
        raise NotConvertible(f"{where_in_strategy()}: an object kept on the context is changed "
                             f"(.{name} = ...) during the run; a sweep cannot follow that.")
