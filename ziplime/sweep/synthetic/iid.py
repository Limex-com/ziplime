"""Iid for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.resampled import Resampled


class IID(Resampled):
    def positions(self, rng: np.random.Generator):
        return rng.integers(0, len(self.pool), self.length)
