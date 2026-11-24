"""Rows for lazy."""

import numpy as np


class _Rows:
    """History rows at a bar, as the engine's polars Series once anything but to_numpy is
    asked of them."""

    def __init__(self, values: np.ndarray, field: str) -> None:
        self.values, self.field = values, field

    def series(self):
        import polars as pl
        return pl.Series(self.field, self.values)
