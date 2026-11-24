"""Bars for synthetic."""

import dataclasses
import numpy as np


@dataclasses.dataclass
class Bars:
    """The universe's bars side by side, as the paths are built from them."""

    #: {field: (bars, instruments)}, NaN where an instrument has no row
    fields: dict
    #: The bundle's rows of the universe, and each one's bar and instrument
    rows: np.ndarray
    bar: np.ndarray
    column: np.ndarray
    #: The run's first bar: paths are synthetic from here on
    start: int

    @property
    def shape(self) -> tuple[int, int]:
        return self.fields["close"].shape
