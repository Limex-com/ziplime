"""Garch for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.base_generator import Generator
from ziplime.sweep.synthetic.garch_utils import fit_garch
from ziplime.sweep.synthetic.garch_utils import garch_variance


class Garch(Generator):
    """GARCH(1,1) per instrument with variance targeting; the shocks are the real standardised
    residuals of one pooled date for all instruments at once."""

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.mean = np.zeros_like(self.drift) if self.family.drift == "zero" else self.drift
        residuals = self.feats.returns - self.drift
        columns = residuals.shape[1]
        self.params = np.zeros((columns, 3))
        variance = np.zeros_like(residuals)
        for column in range(columns):
            omega, alpha, beta = fit_garch(residuals[self.pool, column])
            self.params[column] = omega, alpha, beta
            variance[:, column] = garch_variance(residuals[:, column], omega, alpha, beta,
                                                 float(np.var(residuals[self.pool, column])))
        sigma = np.sqrt(np.maximum(variance, 1e-300))
        self.standardised = np.where(sigma > 0, residuals / sigma, 0.0)[self.pool]
        # The state at the run's start: the variance the real bars left
        omega, alpha, beta = self.params.T
        self.variance0 = omega + alpha * residuals[self.start - 1] ** 2 + \
            beta * variance[self.start - 1]
        self.omega, self.alpha, self.beta = omega, alpha, beta

    def path(self, rng: np.random.Generator):
        draws = rng.integers(0, len(self.pool), self.length)
        shocks = self.standardised[draws]
        returns = np.empty((self.length, len(self.mean)))
        variance = self.variance0.copy()
        for t in range(self.length):
            residual = np.sqrt(variance) * shocks[t]
            returns[t] = self.mean + residual
            variance = self.omega + self.alpha * residual ** 2 + self.beta * variance
        return self.pool[draws], returns
