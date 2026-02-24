from numpy import clip

from ziplime.pipeline.terms.factors.custom_factor import CustomFactor


class Clip(CustomFactor):
    """
    Clip (limit) the values in a factor.

    Given an interval, values outside the interval are clipped to the interval
    edges. For example, if an interval of ``[0, 1]`` is specified, values
    smaller than 0 become 0, and values larger than 1 become 1.

    **Default Window Length:** 1

    Parameters
    ----------
    min_bound : float
        The minimum value to use.
    max_bound : float
        The maximum value to use.

    Notes
    -----
    To only clip values on one side, ``-np.inf` and ``np.inf`` may be passed.
    For example, to only clip the maximum value but not clip a minimum value:

    .. code-block:: python

       Clip(inputs=[factor], min_bound=-np.inf, max_bound=user_provided_max)

    See Also
    --------
    numpy.clip
    """

    window_length = 1
    params = ("min_bound", "max_bound")

    def compute(self, today, assets, out, values, min_bound, max_bound):
        clip(values[-1], min_bound, max_bound, out=out)
