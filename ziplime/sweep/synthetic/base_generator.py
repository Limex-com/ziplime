"""Generator for synthetic."""

import numpy as np
from ziplime.sweep.params import Family
from ziplime.sweep.synthetic.feature_values import Features


class Generator:
    """Draws paths of one family: for each path, the real bar each synthetic bar takes its shape
    and volume from, and the log returns ((bars of the run, instruments))."""

    def __init__(self, family: Family, feats: Features, pool: np.ndarray, start: int) -> None:
        self.family, self.feats, self.pool, self.start = family, feats, pool, start
        self.length = feats.returns.shape[0] - start
        self.pooled = feats.returns[pool]
        #: each instrument's mean log return over the pool -- what zero drift takes out
        self.drift = self.pooled.mean(axis=0)

    def path(self, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
        raise NotImplementedError
