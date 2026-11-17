"""Array detection shared by sweep execution and conversion."""

import numpy as np


def is_array(value) -> bool:
    return hasattr(value, "shape") and not isinstance(value, np.generic)
