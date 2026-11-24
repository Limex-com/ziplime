"""Array utils for labelarray."""

import numpy as np


def compare_arrays(left: np.ndarray, right: np.ndarray):
    "Eq check with a short-circuit for identical objects."
    return left is right or ((left.shape == right.shape) and (left == right).all())


def _make_unsupported_method(name: str):
    def method(*args, **kwargs):
        raise NotImplementedError("Method %s is not supported on LabelArrays." % name)

    method.__name__ = name
    method.__doc__ = "Unsupported LabelArray Method: %s" % name
    return method
