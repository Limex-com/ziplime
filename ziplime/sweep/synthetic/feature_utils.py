"""Feature utils for synthetic."""

import numpy as np
import pandas as pd
from ziplime.sweep.synthetic.bars import Bars
from ziplime.sweep.synthetic.feature_values import Features


def features(bars: Bars) -> Features:
    close = pd.DataFrame(bars.fields["close"]).ffill().to_numpy()
    previous = np.vstack([np.full((1, close.shape[1]), np.nan), close[:-1]])
    with np.errstate(divide="ignore", invalid="ignore"):
        returns = np.log(close / previous)
        opens = bars.fields.get("open")
        gap = np.log(opens / previous) if opens is not None else returns.copy()
        high = np.log(bars.fields["high"] / bars.fields["close"]) if "high" in bars.fields \
            else np.zeros_like(close)
        low = np.log(bars.fields["low"] / bars.fields["close"]) if "low" in bars.fields \
            else np.zeros_like(close)
    returns = np.where(np.isfinite(returns), returns, 0.0)
    gap = np.where(np.isfinite(gap), gap, returns)
    high = np.where(np.isfinite(high), np.maximum(high, 0.0), 0.0)
    low = np.where(np.isfinite(low), np.minimum(low, 0.0), 0.0)
    return Features(returns=returns, gap=gap, high=high, low=low,
                    volume=bars.fields.get("volume"), close=close)
