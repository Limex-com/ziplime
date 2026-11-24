"""Select for lazy."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.lazy.support import where_in_strategy


class Select:
    """A record in some combinations and another value in the rest: ``mask`` chooses."""

    def __init__(self, mask, yes, no) -> None:
        self.mask, self.yes, self.no = mask, yes, no

    def __getattr__(self, name: str):
        from ziplime.sweep.lazy.select_utils import _distribute

        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "ndim"):
            raise AttributeError(name)
        return _distribute(lambda value: getattr(value, name), self)

    def __call__(self, *args, **kwargs):
        from ziplime.sweep.lazy.select_utils import _distribute

        return _distribute(lambda value: value(*args, **kwargs), self)

    def __getitem__(self, key):
        from ziplime.sweep.lazy.select_utils import _distribute

        return _distribute(lambda value: value[key], self)

    def __bool__(self):
        raise NotConvertible(f"{where_in_strategy()}: an array of history is used as True/False.")

    __hash__ = object.__hash__
