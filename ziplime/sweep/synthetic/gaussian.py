"""Gaussian for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.base_generator import Generator


class Gaussian(Generator):
    """Log returns with the pool's means and covariance; ``df`` makes them a multivariate t."""

    def __init__(self, *args, df: float | None) -> None:
        super().__init__(*args)
        self.df = df
        covariance = np.atleast_2d(np.cov(self.pooled, rowvar=False))
        jitter = 1e-12 * max(1.0, float(np.trace(covariance)))
        self.factor = np.linalg.cholesky(covariance + jitter * np.eye(len(covariance)))
        self.mean = np.zeros_like(self.drift) if self.family.drift == "zero" else self.drift

    def path(self, rng: np.random.Generator):
        shocks = rng.standard_normal((self.length, len(self.mean)))
        if self.df is not None:
            # One mixing draw per bar for all instruments: a multivariate t of unit variance
            shocks *= np.sqrt((self.df - 2) / rng.chisquare(self.df, (self.length, 1)))
        source = self.pool[rng.integers(0, len(self.pool), self.length)]
        return source, self.mean + shocks @ self.factor.T
