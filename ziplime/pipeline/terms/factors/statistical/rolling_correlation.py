import numpy as np


from ziplime.errors import IncompatibleTerms
from ziplime.pipeline.terms.factors import CustomFactor


class RollingCorrelation(CustomFactor):

    def __new__(cls, base_factor: np.float64 | np.float32, target: np.float64 | np.float32, correlation_length,
                mask=None):
        if target.ndim == 2 and base_factor.mask is not target.mask:
            raise IncompatibleTerms(term_1=base_factor, term_2=target)
        if correlation_length < 2:
            raise ValueError("correlation_length must be greater than or equal to 2")
        return super(RollingCorrelation, cls).__new__(
            cls,
            inputs=[base_factor, target],
            window_length=correlation_length,
            mask=mask,
        )