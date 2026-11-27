"""Support for lazy."""

import os
import sys


_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def where_in_strategy() -> str:
    """``file, line N`` of the strategy's code on the stack: converted code keeps its file and
    lines."""
    frame = sys._getframe(1)
    while frame is not None:
        filename = frame.f_code.co_filename
        # A lambda (a sort key, a comprehension) points at its own first line; the statement that
        # runs it is the one to name
        if not filename.startswith("<") and frame.f_code.co_name != "<lambda>" and \
                not os.path.abspath(filename).startswith(_HERE + os.sep):
            return f"{filename}, line {frame.f_lineno}"
        frame = frame.f_back
    return "the strategy"


CHANGING = {"fit", "partial_fit", "fit_transform", "fit_predict", "set_params", "train", "update",
            "learn", "reset", "reset_states", "compile"}


PARALLEL_FROM = 2_000_000
