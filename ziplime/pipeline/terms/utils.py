from ziplime.errors import UnsupportedDType, NotDType, DTypeNotSpecified
from ziplime.lib.adjusted_array import can_represent_dtype
from ziplime.lib.labelarray import LabelArray
from ziplime.utils.numpy_utils import categorical_dtype, default_missing_value_for_dtype

from numpy import (
    array,
    dtype as dtype_class,
)


def validate_dtype(termname, dtype, missing_value):
    """
    Validate a `dtype` and `missing_value` passed to Term.__new__.

    Ensures that we know how to represent ``dtype``, and that missing_value
    is specified for types without default missing values.

    Returns
    -------
    validated_dtype, validated_missing_value : np.dtype, any
        The dtype and missing_value to use for the new term.

    Raises
    ------
    DTypeNotSpecified
        When no dtype was passed to the instance, and the class doesn't
        provide a default.
    NotDType
        When either the class or the instance provides a value not
        coercible to a numpy dtype.
    NoDefaultMissingValue
        When dtype requires an explicit missing_value, but
        ``missing_value`` is None.
    """
    if dtype is None:
        raise DTypeNotSpecified(termname=termname)

    try:
        dtype = dtype_class(dtype)
    except TypeError as exc:
        raise NotDType(dtype=dtype, termname=termname) from exc

    if not can_represent_dtype(dtype):
        raise UnsupportedDType(dtype=dtype, termname=termname)

    if missing_value is None:
        missing_value = default_missing_value_for_dtype(dtype)

    try:
        _coerce_to_dtype(missing_value, dtype)
    except TypeError as exc:
        raise TypeError(
            "Missing value {value!r} is not a valid choice "
            "for term {termname} with dtype {dtype}.\n\n"
            "Coercion attempt failed with: {error}".format(
                termname=termname,
                value=missing_value,
                dtype=dtype,
                error=exc,
            )
        ) from exc

    return dtype, missing_value


def _assert_valid_categorical_missing_value(value):
    """
    Check that value is a valid categorical missing_value.

    Raises a TypeError if the value is cannot be used as the missing_value for
    a categorical_dtype Term.
    """
    label_types = LabelArray.SUPPORTED_SCALAR_TYPES
    if not isinstance(value, label_types):
        raise TypeError(
            "String-dtype classifiers can only produce {types}.".format(
                types=" or ".join([t.__name__ for t in label_types])
            )
        )


def _coerce_to_dtype(value, dtype):
    if dtype == categorical_dtype:
        # This check is necessary because we use object dtype for
        # categoricals, and numpy will allow us to promote numerical
        # values to object even though we don't support them.
        _assert_valid_categorical_missing_value(value)
        return value
    else:
        # For any other type, cast using the same rules as numpy's astype
        # function with casting='same_kind'.
        #
        # 'same_kind' allows casting between things like float32 and float64,
        # but not between str and int. Note that the name is somewhat
        # misleading, since it does allow conversion between different dtype
        # kinds in some cases. In particular, conversion from int to float is
        # allowed.
        return array([value]).astype(dtype=dtype, casting="same_kind")[0]
