import numpy as np
import pytest

from ziplime.utils.numpy_errors import NoDefaultMissingValue
from ziplime.utils.numpy_utils import (
    as_column,
    busday_count_mask_NaT,
    bytes_array_to_native_str_object_array,
    changed_locations,
    coerce_to_dtype,
    default_missing_value_for_dtype,
    float64_dtype,
    int64_dtype,
    int_dtype_with_size_in_bytes,
    is_datetime,
    is_float,
    is_int,
    is_missing,
    is_object,
    isnat,
    rolling_window,
    same,
    unsigned_int_dtype_with_size_in_bytes,
    vectorized_is_element,
)


def test_int_and_uint_dtypes_by_size():
    assert int_dtype_with_size_in_bytes(1) == np.int8
    assert int_dtype_with_size_in_bytes(2) == np.int16
    assert int_dtype_with_size_in_bytes(4) == np.int32
    assert int_dtype_with_size_in_bytes(8) == np.int64

    with pytest.raises(ValueError, match="No integral dtype whose size is 3 bytes"):
        int_dtype_with_size_in_bytes(3)

    assert unsigned_int_dtype_with_size_in_bytes(1) == np.uint8
    assert unsigned_int_dtype_with_size_in_bytes(2) == np.uint16
    assert unsigned_int_dtype_with_size_in_bytes(4) == np.uint32
    assert unsigned_int_dtype_with_size_in_bytes(8) == np.uint64

    with pytest.raises(ValueError, match="No unsigned integral dtype whose size is 5 bytes"):
        unsigned_int_dtype_with_size_in_bytes(5)


def test_kind_checks():
    assert is_float(1.5)
    assert is_float(np.array([1.5], dtype=float))
    assert not is_float(1)

    assert is_int(1)
    assert is_int(np.array([1], dtype=int))
    assert not is_int(1.5)

    dt = np.datetime64("2024-01-01")
    assert is_datetime(dt)
    assert is_datetime(np.array([dt]))
    assert not is_datetime(1)

    assert is_object("hello")
    assert is_object(np.array(["hello"], dtype=object))


def test_coerce_to_dtype():
    assert coerce_to_dtype(int64_dtype, 42) == 42
    assert coerce_to_dtype(float64_dtype, 42) == 42.0

    dt_ns = coerce_to_dtype(np.dtype("datetime64[ns]"), "2024-01-01")
    assert dt_ns == np.datetime64("2024-01-01", "ns")

    with pytest.raises(TypeError, match="Don't know how to coerce"):
        coerce_to_dtype(np.dtype("datetime64[D]"), "2024-01-01")


def test_default_missing_value_for_dtype():
    assert np.isnan(default_missing_value_for_dtype(float64_dtype))
    assert np.isnat(default_missing_value_for_dtype(np.dtype("datetime64[ns]")))
    assert default_missing_value_for_dtype(np.dtype(object)) is None

    with pytest.raises(NoDefaultMissingValue):
        default_missing_value_for_dtype(np.dtype(complex))


def test_rolling_window():
    arr = np.arange(10)
    windows = rolling_window(arr, 3)
    assert windows.shape == (8, 3)
    np.testing.assert_array_equal(windows[0], np.array([0, 1, 2]))
    np.testing.assert_array_equal(windows[-1], np.array([7, 8, 9]))

    with pytest.raises(IndexError, match="Can't restride a scalar"):
        rolling_window(np.array(5), 2)

    with pytest.raises(IndexError, match="Can't restride array"):
        rolling_window(np.arange(3), 3)


def test_isnat_and_is_missing_and_same():
    nat = np.datetime64("NaT", "ns")
    dt = np.datetime64("2024-01-01", "ns")

    assert isnat(np.array(nat))
    assert not isnat(np.array(dt))

    with pytest.raises(ValueError, match="not a numpy datetime or timedelta"):
        isnat(np.array([1, 2]))

    # is_missing
    assert is_missing(np.array([1.0, np.nan]), np.nan).tolist() == [False, True]
    assert is_missing(np.array([dt, nat]), nat).tolist() == [False, True]
    assert is_missing(np.array(["a", None], dtype=object), None).tolist() == [False, True]
    assert is_missing(np.array([1, 2]), 2).tolist() == [False, True]

    # same
    assert same(np.nan, np.nan)
    assert same(nat, nat)
    assert same(1, 1)
    assert not same(1, 2)
    assert not same(np.nan, 1.0)


def test_busday_count_mask_nat():
    begins = np.array(["2024-01-01", "NaT"], dtype="datetime64[D]")
    ends = np.array(["2024-01-05", "2024-01-05"], dtype="datetime64[D]")
    counts = busday_count_mask_NaT(begins, ends)
    assert counts[0] == 4.0
    assert np.isnan(counts[1])


def test_vectorized_is_element():
    arr = np.array([1, 2, 3, 4, 5])
    choices = {2, 4}
    res = vectorized_is_element(arr, choices)
    np.testing.assert_array_equal(res, np.array([False, True, False, True, False]))


def test_as_column():
    arr = np.array([1, 2, 3])
    col = as_column(arr)
    assert col.shape == (3, 1)
    np.testing.assert_array_equal(col, np.array([[1], [2], [3]]))

    with pytest.raises(ValueError, match="1-dimensional"):
        as_column(np.ones((2, 2)))


def test_changed_locations():
    arr = np.array([0, 0, 5, 5, 1, 1])
    np.testing.assert_array_equal(changed_locations(arr, include_first=False), np.array([2, 4]))
    np.testing.assert_array_equal(changed_locations(arr, include_first=True), np.array([0, 2, 4]))

    with pytest.raises(ValueError, match="1D arrays"):
        changed_locations(np.ones((2, 2)), include_first=False)


def test_bytes_array_to_native_str_object_array():
    arr = np.array([b"hello", b"world"], dtype="S5")
    res = bytes_array_to_native_str_object_array(arr)
    assert res.dtype == object
    assert list(res) == ["hello", "world"]
