"""Utils for adjustment."""

from typing import Any
import numpy as np
import pandas as pd
from pandas import Timestamp
from numpy import datetime64, float64, int64, bool_, uint8
from ziplime.utils.compat import unicode
from ziplime.lib.adjustment.support import ADJUSTMENT_KIND_NAMES
from ziplime.lib.adjustment.adjustment import Adjustment
from ziplime.lib.adjustment.adjustment_kind import AdjustmentKind
from ziplime.lib.adjustment.boolean_overwrite import BooleanOverwrite
from ziplime.lib.adjustment.datetime64_overwrite import Datetime64Overwrite
from ziplime.lib.adjustment.float64_add import Float64Add
from ziplime.lib.adjustment.float64_multiply import Float64Multiply
from ziplime.lib.adjustment.float64_overwrite import Float64Overwrite
from ziplime.lib.adjustment.int64_overwrite import Int64Overwrite
from ziplime.lib.adjustment.object_overwrite import ObjectOverwrite


def _is_float(value) -> bool:
    return isinstance(value, (float, float64))


def _is_datetime(value) -> bool:
    return isinstance(value, (datetime64, Timestamp))


def _is_int(value) -> bool:
    return isinstance(value, (int, int64))


def _is_obj(value) -> bool:
    return isinstance(value, (bytes, unicode, type(None)))


def _is_bool(value) -> bool:
    return isinstance(value, (bool, bool_, uint8))


def choose_adjustment_type(adjustment_kind: AdjustmentKind, value):
    """
    Make an adjustment object of the type appropriate for the given kind and
    value.

    Parameters
    ----------
    adjustment_kind : {ADD, MULTIPLY, OVERWRITE}
        The kind of adjustment to construct.
    value : object
        The value parameter to the adjustment.  Only floating-point values and
        datetime-like values are currently supported
    """
    if adjustment_kind in (AdjustmentKind.ADD, AdjustmentKind.MULTIPLY):
        if not _is_float(value):
            raise TypeError(
                "Can't construct %s Adjustment with value of type %r.\n"
                "ADD and MULTIPLY adjustments are only supported for "
                "floating point data." % (
                    ADJUSTMENT_KIND_NAMES[adjustment_kind],
                    type(value),
                )
            )
        return _float_adjustment_types[adjustment_kind]

    elif adjustment_kind == AdjustmentKind.OVERWRITE:
        if _is_float(value):
            return _float_adjustment_types[adjustment_kind]
        elif _is_datetime(value):
            return _datetime_adjustment_types[adjustment_kind]
        elif _is_bool(value):
            return _boolean_adjustment_types[adjustment_kind]
        elif _is_int(value):
            return _int_adjustment_types[adjustment_kind]
        elif _is_obj(value):
            return _object_adjustment_types[adjustment_kind]
        else:
            raise TypeError(
                "Don't know how to make overwrite "
                "adjustments for values of type %r." % type(value),
            )

    else:
        raise ValueError("Unknown adjustment type %d." % adjustment_kind)


def make_adjustment_from_indices(first_row: int,
                                 last_row: int,
                                 first_column: int,
                                 last_column: int,
                                 adjustment_kind: AdjustmentKind,
                                 value) -> Adjustment:
    """
    Make an Adjustment object from row/column indices into a baseline array.
    """
    type_ = choose_adjustment_type(adjustment_kind, value)
    # NOTE_SS: Cython appears to generate incorrect code here if values are
    # passed by name.  This is true even if cython.always_allow_keywords is
    # enabled.  Yay Cython.
    return type_(first_row, last_row, first_column, last_column, value)


def _choose_adjustment_type(adjustment_kind: AdjustmentKind,
                            value: Any) -> type:
    column_type = type(value)
    if adjustment_kind in (AdjustmentKind.ADD, AdjustmentKind.MULTIPLY):
        if column_type is np.float64:
            return _float_adjustment_types[adjustment_kind]
        else:
            raise TypeError(
                "Can't construct %s Adjustment with value of type %r.\n"
                "ADD and MULTIPLY adjustments are only supported for "
                "floating point data." % (
                    ADJUSTMENT_KIND_NAMES[adjustment_kind],
                    column_type,
                )
            )

    elif adjustment_kind == AdjustmentKind.OVERWRITE:
        if column_type is np.float64:
            return _float_adjustment_types[adjustment_kind]
        elif column_type is np.int64:
            return _int_adjustment_types[adjustment_kind]
        elif column_type is np.uint8:
            return _boolean_adjustment_types[adjustment_kind]
        elif column_type is object:
            return _object_adjustment_types[adjustment_kind]
        else:
            raise TypeError(
                "Don't know how to make overwrite "
                "adjustments for values of type %r." % type(value),
            )

    else:
        raise ValueError("Unknown adjustment type %d." % adjustment_kind)


def make_adjustment_from_indices_fused(first_row: int,
                                       last_row: int,
                                       first_column: int,
                                       last_column: int,
                                       adjustment_kind: AdjustmentKind,
                                       value: Any) -> Adjustment:
    """
    Make an Adjustment object from row/column indices into a baseline array.
    """
    type_ = _choose_adjustment_type(adjustment_kind, value)
    # NOTE_SS: Cython appears to generate incorrect code here if values are
    # passed by name.  This is true even if cython.always_allow_keywords is
    # enabled.  Yay Cython.
    return type_(first_row, last_row, first_column, last_column, value)


def make_adjustment_from_labels(dates_index: pd.DatetimeIndex,
                                assets_index: pd.RangeIndex,
                                start_date: pd.Timestamp,
                                end_date: pd.Timestamp,
                                asset_id: int,
                                adjustment_kind: AdjustmentKind,
                                value: Any):
    """
    Make an Adjustment object from date/asset labels into a labelled baseline
    array.
    """
    type_ = choose_adjustment_type(adjustment_kind, value)
    return type_.from_assets_and_dates(
        dates_index,
        assets_index,
        start_date,
        end_date,
        asset_id,
        value,
    )


_float_adjustment_types = {
    AdjustmentKind.ADD: Float64Add,
    AdjustmentKind.MULTIPLY: Float64Multiply,
    AdjustmentKind.OVERWRITE: Float64Overwrite,
}


_datetime_adjustment_types = {
    AdjustmentKind.OVERWRITE: Datetime64Overwrite,
}


_object_adjustment_types = {
    AdjustmentKind.OVERWRITE: ObjectOverwrite,
}


_int_adjustment_types = {
    AdjustmentKind.OVERWRITE: Int64Overwrite,
}


_boolean_adjustment_types = {
    AdjustmentKind.OVERWRITE: BooleanOverwrite,
}
