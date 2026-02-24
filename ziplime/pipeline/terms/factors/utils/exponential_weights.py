from numpy import arange, full

from ziplime.utils.numpy_utils import float64_dtype


def exponential_weights(length, decay_rate):
    """
    Build a weight vector for an exponentially-weighted statistic.

    The resulting ndarray is of the form::

        [decay_rate ** length, ..., decay_rate ** 2, decay_rate]

    Parameters
    ----------
    length : int
        The length of the desired weight vector.
    decay_rate : float
        The rate at which entries in the weight vector increase or decrease.

    Returns
    -------
    weights : ndarray[float64]
    """
    return full(length, decay_rate, float64_dtype) ** arange(length + 1, 1, -1)
