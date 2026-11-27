"""
An ndarray subclass for working with arrays of strings.
"""

from ziplime.lib.labelarray.array_utils import compare_arrays  # noqa: F401
from ziplime.lib.labelarray.errors import MissingValueMismatch  # noqa: F401
from ziplime.lib.labelarray.errors import CategoryMismatch  # noqa: F401
from ziplime.lib.labelarray.label_array import LabelArray  # noqa: F401
from ziplime.lib.labelarray.utils import labelarray_where  # noqa: F401


__all__ = [
    'compare_arrays',
    'MissingValueMismatch',
    'CategoryMismatch',
    'LabelArray',
    'labelarray_where',
]
