"""Garch utils for synthetic."""

import numpy as np
from ziplime.sweep.params import SweepError


def garch_variance(residuals: np.ndarray, omega: float, alpha: float, beta: float,
                   initial: float) -> np.ndarray:
    """σ²[0] = initial; σ²[t] = ω + α e²[t-1] + β σ²[t-1]."""
    from scipy.signal import lfilter

    variance = np.empty(len(residuals))
    variance[0] = initial
    if len(residuals) > 1:
        drive = omega + alpha * residuals[:-1] ** 2
        variance[1:] = lfilter([1.0], [1.0, -beta], drive, zi=[beta * initial])[0]
    return variance


def fit_garch(residuals: np.ndarray) -> tuple[float, float, float]:
    """(ω, α, β) by Gaussian maximum likelihood, ω = var · (1 - α - β) (variance targeting)."""
    try:
        from scipy.optimize import minimize
    except ImportError:
        raise SweepError("The garch family needs scipy (pip install scipy).") from None
    variance = float(np.var(residuals))
    if variance <= 0 or len(residuals) < 30:
        return variance, 0.0, 0.0

    def loss(parameters: np.ndarray):
        alpha, beta = parameters
        if alpha < 0 or beta < 0 or alpha + beta >= 0.999:
            return 1e300
        sigma2 = garch_variance(residuals, variance * (1 - alpha - beta), alpha, beta, variance)
        sigma2 = np.maximum(sigma2, 1e-300)
        return 0.5 * float(np.sum(np.log(sigma2) + residuals ** 2 / sigma2))

    best = minimize(loss, x0=[0.08, 0.88], method="Nelder-Mead",
                    options={"xatol": 1e-5, "fatol": 1e-6, "maxiter": 800})
    alpha, beta = (float(v) for v in best.x)
    if not best.success and loss(best.x) >= loss([0.0, 0.0]):
        alpha, beta = 0.0, 0.0
    return variance * (1 - alpha - beta), alpha, beta
