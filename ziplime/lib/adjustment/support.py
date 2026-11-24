"""Support for adjustment."""

from ziplime.lib.adjustment.adjustment_kind import AdjustmentKind


ADJUSTMENT_KIND_NAMES = {
    AdjustmentKind.MULTIPLY: 'MULTIPLY',
    AdjustmentKind.ADD: 'ADD',
    AdjustmentKind.OVERWRITE: 'OVERWRITE',
}
