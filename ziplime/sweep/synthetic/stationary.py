"""Stationary for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.resampled import Resampled


class Stationary(Resampled):
    """Blocks of geometric length, ``block`` bars on average, wrapping around the end."""

    def positions(self, rng: np.random.Generator):
        size = len(self.pool)
        new = rng.random(self.length) < 1.0 / self.family.block
        new[0] = True
        firsts = np.flatnonzero(new)
        group = np.cumsum(new) - 1
        starts = rng.integers(0, size, len(firsts))
        return (starts[group] + np.arange(self.length) - firsts[group]) % size
