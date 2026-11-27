import numpy as np

from ziplime.lib.none_first_sort_key import _NoneFirstSortKey
from ziplime.lib.factorize import (
    factorize_strings,
    factorize_strings_known_categories,
    log2,
    smallest_uint_that_can_hold,
)


def test_log2_and_smallest_uint():
    assert log2(8) == 3.0
    assert smallest_uint_that_can_hold(0) == np.uint8
    assert smallest_uint_that_can_hold(255) == np.uint8
    assert smallest_uint_that_can_hold(256) == np.uint8  # 256 fits in 8 bits if 0..255 indexing
    assert smallest_uint_that_can_hold(257) == np.uint16
    assert smallest_uint_that_can_hold(65536) == np.uint16
    assert smallest_uint_that_can_hold(65537) == np.uint32


def test_none_first_sort_key():
    k_none = _NoneFirstSortKey(None)
    k_a = _NoneFirstSortKey("a")
    k_b = _NoneFirstSortKey("b")

    assert k_none == _NoneFirstSortKey(None)
    assert k_none < k_a
    assert not (k_a < k_none)
    assert k_a < k_b
    assert k_b > k_a
    assert not (k_none > k_a)
    assert k_none <= k_a
    assert k_a >= k_none
    assert k_a <= _NoneFirstSortKey("a")


def test_factorize_strings_basic():
    values = np.array(["b", "a", "c", "b", "a", None], dtype=object)
    codes, categories, reverse = factorize_strings(values, missing_value=None, sort=True)

    assert categories[0] is None
    assert list(categories[1:]) == ["a", "b", "c"]
    assert reverse[None] == 0
    assert reverse["a"] == 1
    assert reverse["b"] == 2
    assert reverse["c"] == 3

    assert codes[0] == 2  # "b"
    assert codes[1] == 1  # "a"
    assert codes[2] == 3  # "c"
    assert codes[5] == 0  # None


def test_factorize_strings_unsorted():
    values = np.array(["z", "y", "x"], dtype=object)
    codes, categories, reverse = factorize_strings(values, missing_value="missing", sort=False)
    assert categories[0] == "missing"
    assert "z" in categories
    assert "y" in categories
    assert "x" in categories


def test_factorize_strings_known_categories():
    values = np.array(["a", "b", "unknown", "c"], dtype=object)
    known = ["a", "b", "c"]
    codes, categories, reverse = factorize_strings_known_categories(
        values,
        categories=known.copy(),
        missing_value="missing",
        sort=True,
    )

    assert "missing" in categories
    missing_code = reverse["missing"]
    # "unknown" should receive missing_code
    assert codes[2] == missing_code
    assert codes[0] == reverse["a"]
    assert codes[1] == reverse["b"]
    assert codes[3] == reverse["c"]
