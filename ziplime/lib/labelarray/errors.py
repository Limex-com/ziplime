"""Errors for labelarray."""

import numpy as np


class MissingValueMismatch(ValueError):
    """
    Error raised on attempt to perform operations between LabelArrays with
    mismatched missing_values.
    """

    def __init__(self, left: str | bytes | None, right: str | bytes | None) -> None:
        super(MissingValueMismatch, self).__init__(
            "LabelArray missing_values don't match:"
            " left={}, right={}".format(left, right)
        )


class CategoryMismatch(ValueError):
    """
    Error raised on attempt to perform operations between LabelArrays with
    mismatched category arrays.
    """

    def __init__(self, left: np.ndarray, right: np.ndarray) -> None:
        (mismatches,) = np.where(left != right)
        assert len(mismatches), "Not actually a mismatch!"
        super(CategoryMismatch, self).__init__(
            "LabelArray categories don't match:\n"
            "Mismatched Indices: {mismatches}\n"
            "Left: {left}\n"
            "Right: {right}".format(
                mismatches=mismatches,
                left=left[mismatches],
                right=right[mismatches],
            )
        )
