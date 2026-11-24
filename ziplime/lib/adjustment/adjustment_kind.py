"""Adjustment kind for adjustment."""

import enum


class AdjustmentKind(enum.Enum):
    MULTIPLY = 0
    ADD = 1
    OVERWRITE = 2
