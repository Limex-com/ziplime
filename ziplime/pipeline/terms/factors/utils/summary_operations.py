"""Summary operations exposed by factors."""

import numpy as np
from ziplime.utils.math_utils import nanmax, nanmean, nanmedian, nanmin, nanstd, nansum
from ziplime.utils.numpy_utils import is_missing


class summary_funcs:
    """Namespace of functions meant to be used with DailySummary."""

    @staticmethod
    def mean(a: np.ndarray, missing_value):
        return nanmean(a, axis=1)

    @staticmethod
    def stddev(a: np.ndarray, missing_value):
        return nanstd(a, axis=1)

    @staticmethod
    def max(a: np.ndarray, missing_value):
        return nanmax(a, axis=1)

    @staticmethod
    def min(a: np.ndarray, missing_value):
        return nanmin(a, axis=1)

    @staticmethod
    def median(a: np.ndarray, missing_value):
        return nanmedian(a, axis=1)

    @staticmethod
    def sum(a: np.ndarray, missing_value):
        return nansum(a, axis=1)

    @staticmethod
    def notnull_count(a: np.ndarray, missing_value):
        return (~is_missing(a, missing_value)).sum(axis=1)

    names = {k for k in locals() if not k.startswith("_")}
