"""Resampled for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.base_generator import Generator


class Resampled(Generator):
    """Real bars, picked by position in the pool -- the same bar for every instrument."""

    def positions(self, rng: np.random.Generator) -> np.ndarray:
        raise NotImplementedError

    def path(self, rng: np.random.Generator):
        source = self.pool[self.positions(rng)]
        returns = self.feats.returns[source]
        if self.family.drift == "zero":
            returns = returns - self.drift
        return source, returns
