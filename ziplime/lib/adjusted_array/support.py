"""Support for adjusted_array."""

import numpy as np
from ziplime.lib.windowtemplate import AdjustedArrayWindow
from ziplime.utils.numpy_utils import float64_dtype, int64_dtype, uint8_dtype


# These class names are all the same because of our bootleg templating system.
BOOL_DTYPES = frozenset(
    map(np.dtype, [np.bool_, np.uint8]),
)


FLOAT_DTYPES = frozenset(
    map(np.dtype, [np.float32, np.float64]),
)


INT_DTYPES = frozenset(
    # NOTE: uint64 not supported because it can't be safely cast to int64.
    map(np.dtype, [np.int16, np.uint16, np.int32, np.int64, np.uint32]),
)


DATETIME_DTYPES = frozenset(map(np.dtype, ["datetime64[ns]"]))


OBJECT_DTYPES = frozenset(map(np.dtype, ["O"]))


STRING_KINDS = frozenset(["S", "U"])


REPRESENTABLE_DTYPES = BOOL_DTYPES.union(
    FLOAT_DTYPES,
    INT_DTYPES,
    DATETIME_DTYPES,
    OBJECT_DTYPES,
)


CONCRETE_WINDOW_TYPES = {
    float64_dtype: AdjustedArrayWindow,
    int64_dtype: AdjustedArrayWindow,
    uint8_dtype: AdjustedArrayWindow,
}
