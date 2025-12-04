from numpy import average

from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.pipeline.terms.factors.utils.exponential_weights import exponential_weights
from ziplime.utils.numpy_utils import rolling_window


class MovingAverageConvergenceDivergenceSignal(CustomFactor):
    """
    Moving Average Convergence/Divergence (MACD) Signal line
    https://en.wikipedia.org/wiki/MACD

    A technical indicator originally developed by Gerald Appel in the late
    1970's. MACD shows the relationship between two moving averages and
    reveals changes in the strength, direction, momentum, and duration of a
    trend in a stock's price.

    **Default Inputs:** :data:`ziplime.pipeline.data.EquityPricing.close`

    Parameters
    ----------
    fast_period : int > 0, optional
        The window length for the "fast" EWMA. Default is 12.
    slow_period : int > 0, > fast_period, optional
        The window length for the "slow" EWMA. Default is 26.
    signal_period : int > 0, < fast_period, optional
        The window length for the signal line. Default is 9.

    Notes
    -----
    Unlike most pipeline expressions, this factor does not accept a
    ``window_length`` parameter. ``window_length`` is inferred from
    ``slow_period`` and ``signal_period``.
    """

    inputs = (EquityPricing.close,)
    # We don't use the default form of `params` here because we want to
    # dynamically calculate `window_length` from the period lengths in our
    # __new__.
    params = ("fast_period", "slow_period", "signal_period")

    def __new__(cls, fast_period=12, slow_period=26, signal_period=9, *args, **kwargs):
        if fast_period < 1:
            raise ValueError("`fast_period` must be >= 1")
        if slow_period < 1:
            raise ValueError("`slow_period` must be >= 1")
        if signal_period < 1:
            raise ValueError("`signal_period` must be >= 1")

        if slow_period <= fast_period:
            raise ValueError(
                "'slow_period' must be greater than 'fast_period', but got\n"
                "slow_period={slow}, fast_period={fast}".format(
                    slow=slow_period,
                    fast=fast_period,
                )
            )

        return super(MovingAverageConvergenceDivergenceSignal, cls).__new__(
            cls,
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
            window_length=slow_period + signal_period - 1,
            *args,
            **kwargs,
        )

    def _ewma(self, data, length):
        decay_rate = 1.0 - (2.0 / (1.0 + length))
        return average(data, axis=1, weights=exponential_weights(length, decay_rate))

    def compute(
            self, today, assets, out, close, fast_period, slow_period, signal_period
    ):
        slow_EWMA = self._ewma(rolling_window(close, slow_period), slow_period)
        fast_EWMA = self._ewma(
            rolling_window(close, fast_period)[-signal_period:], fast_period
        )
        macd = fast_EWMA - slow_EWMA
        out[:] = self._ewma(macd.T, signal_period)
