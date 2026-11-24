"""Shuffle for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.resampled import Resampled


class Shuffle(Resampled):
    def positions(self, rng: np.random.Generator):
        rounds = -(-self.length // len(self.pool))
        return np.concatenate([rng.permutation(len(self.pool)) for _ in range(rounds)])[:self.length]
