from numpy import (
    average,
    sqrt,
    sum as np_sum,
)

from ziplime.pipeline.terms.factors.basic.exponential_weighted_factor import ExponentialWeightedFactor


class ExponentialWeightedMovingStdDev(ExponentialWeightedFactor):
    """
    Exponentially Weighted Moving Standard Deviation

    **Default Inputs:** None

    **Default Window Length:** None

    Parameters
    ----------
    inputs : length-1 list/tuple of BoundColumn
        The expression over which to compute the average.
    window_length : int > 0
        Length of the lookback window over which to compute the average.
    decay_rate : float, 0 < decay_rate <= 1
        Weighting factor by which to discount past observations.

        When calculating historical averages, rows are multiplied by the
        sequence::

            decay_rate, decay_rate ** 2, decay_rate ** 3, ...

    Notes
    -----
    - This class can also be imported under the name ``EWMSTD``.

    See Also
    --------
    :func:`pandas.DataFrame.ewm`
    """

    def compute(self, today, assets, out, data, decay_rate):
        weights = exponential_weights(len(data), decay_rate)

        mean = average(data, axis=0, weights=weights)
        variance = average((data - mean) ** 2, axis=0, weights=weights)

        squared_weight_sum = np_sum(weights) ** 2
        bias_correction = squared_weight_sum / (squared_weight_sum - np_sum(weights**2))
        out[:] = sqrt(variance * bias_correction)
