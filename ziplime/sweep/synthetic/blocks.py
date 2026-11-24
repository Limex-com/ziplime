"""Blocks for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.resampled import Resampled


class Blocks(Resampled):
    """Moving blocks of ``block`` bars; ``circular`` lets a block run past the end into the
    beginning, so every bar is as likely to be drawn."""

    def __init__(self, *args, circular: bool) -> None:
        super().__init__(*args)
        self.circular = circular

    def positions(self, rng: np.random.Generator):
        size = len(self.pool)
        block = min(self.family.block, size)
        count = -(-self.length // block)
        starts = rng.integers(0, size if self.circular else size - block + 1, count)
        return ((starts[:, None] + np.arange(block)).ravel() % size)[:self.length]
