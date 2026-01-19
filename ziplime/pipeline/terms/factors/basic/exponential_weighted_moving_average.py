from numpy import average

from ziplime.pipeline.terms.factors.basic.exponential_weighted_factor import ExponentialWeightedFactor


class ExponentialWeightedMovingAverage(ExponentialWeightedFactor):
    """
    Exponentially Weighted Moving Average

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
    - This class can also be imported under the name ``EWMA``.

    See Also
    --------
    :meth:`pandas.DataFrame.ewm`
    """

    def compute(self, today, assets, out, data, decay_rate):
        out[:] = average(
            data,
            axis=0,
            weights=exponential_weights(len(data), decay_rate),
        )
