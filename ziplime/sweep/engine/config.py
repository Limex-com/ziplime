"""Config for engine."""

import numpy as np
from ziplime.sweep.backends import Backend
from ziplime.sweep.engine.varying import _Varying


class _Config:
    """context.algorithm.config for every combination, shaped like the config itself: a numeric
    field is a (combinations × 1) array, a nested model the same object, a list a list of them, a
    string shared by all combinations the string."""

    def __init__(self, fields: dict) -> None:
        self._fields = fields

    def __getattr__(self, name: str):
        try:
            return self.__dict__["_fields"][name]
        except KeyError:
            raise AttributeError(f"The config has no field {name!r}.") from None

    @classmethod
    def stack(cls, values: list, backend: Backend):
        from pydantic import BaseModel

        first = values[0]
        if all(isinstance(value, (bool, int, float)) for value in values):
            if all(value == first for value in values):
                return first
            return backend.array(np.array(values, dtype=float)[:, None])
        if isinstance(first, BaseModel):
            return cls({name: cls.stack([getattr(value, name) for value in values], backend)
                        for name in type(first).model_fields})
        if isinstance(first, (list, tuple)) and len({len(value) for value in values}) == 1:
            if all(value == first for value in values):
                return first
            return [cls.stack([value[k] for value in values], backend) for k in range(len(first))]
        if all(value == first for value in values):
            return first
        return _Varying()
