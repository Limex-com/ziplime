"""Metric utils for engine."""

import numpy as np
from ziplime.sweep.backends import Backend


def _metrics(b: Backend, curves, initial_cash: float, periods_per_year: int) -> dict:
    """Sharpe (returns' mean over their sample deviation, annualised), total return, max drawdown
    and final value per combination, as report.metrics computes them with pandas."""
    xp = b.xp
    returns = curves[:, 1:] / curves[:, :-1] - 1.0
    count = returns.shape[1]
    mean = xp.mean(returns, axis=1)
    deviation = xp.sqrt(xp.sum((returns - mean[:, None]) ** 2, axis=1) / max(count - 1, 1))
    sharpe = xp.where(deviation > 0, mean / deviation * float(np.sqrt(periods_per_year)), np.nan) \
        if b.name != "mlx" else mean / deviation * float(np.sqrt(periods_per_year))
    peak = _running_max(b, curves)
    drawdown = xp.min(curves / peak - 1.0, axis=1)
    result = {"sharpe": b.numpy(sharpe).astype(float),
              "total_return": b.numpy(curves[:, -1] / initial_cash - 1.0).astype(float),
              "max_drawdown": b.numpy(drawdown).astype(float),
              "final_value": b.numpy(curves[:, -1]).astype(float)}
    result["sharpe"][~(b.numpy(deviation) > 0)] = np.nan
    return result


def _running_max(b: Backend, curves):
    """The running maximum along the bars. CuPy has no maximum.accumulate; doubling the shift
    gets there in log2(bars) elementwise passes on any backend."""
    xp = b.xp
    if b.name == "mlx":
        return xp.cummax(curves, axis=1)
    if b.name == "numpy":
        return np.maximum.accumulate(curves, axis=1)
    peak, shift, bars = curves, 1, curves.shape[1]
    while shift < bars:
        shifted = xp.concatenate([peak[:, :shift], peak[:, :-shift]], axis=1)
        peak = xp.maximum(peak, shifted)
        shift *= 2
    return peak
