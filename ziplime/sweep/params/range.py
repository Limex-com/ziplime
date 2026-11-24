"""Range for params."""

import math
from pydantic import Field, model_validator
from ziplime.sweep.params.strict import Strict


class Range(Strict):
    """``{"from": 5, "to": 50, "step": 5}`` -- 5, 10, ..., 50, ``to`` included."""

    start: float = Field(alias="from")
    stop: float = Field(alias="to")
    step: float = 1

    @model_validator(mode="after")
    def check(self):
        if self.step <= 0:
            raise ValueError("step has to be positive")
        if self.stop < self.start:
            raise ValueError("to has to be at least from")
        return self

    def values(self) -> list:
        count = int(math.floor((self.stop - self.start) / self.step + 1e-9)) + 1
        values = [self.start + k * self.step for k in range(count)]
        if all(float(v).is_integer() for v in (self.start, self.stop, self.step)):
            return [int(round(v)) for v in values]
        return [round(v, 10) for v in values]
