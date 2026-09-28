"""
Factorization algorithms.
"""
import numpy as np

from ziplime.utils.numpy_utils import unsigned_int_dtype_with_size_in_bytes


def log2(d: float) -> float:
    return np.log(d) / np.log(2)


def smallest_uint_that_can_hold(maxval: int):
    """Choose the smallest numpy unsigned int dtype that can hold ``maxval``.
    """
    if maxval < 1:
        # lim x -> 0 log2(x) == -infinity so we floor at uint8
        return np.uint8
    else:
        # The number of bits required to hold the codes up to ``length`` is
        # log2(length). The number of bits per bytes is 8. We cannot have
        # fractional bytes so we need to round up. Finally, we can only have
        # integers with widths 1, 2, 4, or 8 so so we need to round up to the
        # next value by looking up the next largest size in ``_int_sizes``.
        return unsigned_int_dtype_with_size_in_bytes(
            _int_sizes[int(np.ceil(log2(maxval) / 8))]
        )

# ctypedef fused unsigned_integral:
#     np.uint8_t
#     np.uint16_t
#     np.uint32_t
#     np.uint64_t


class _NoneFirstSortKey:
    """Box to sort ``None`` to the front of the categories list.
    """

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        if self.value is None:
            return True
        if other.value is None:
            return False
        return self.value < other.value

    def __gt__(self, other):
        if self.value is None:
            return False
        if other.value is None:
            return True
        return self.value > other.value

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other


def factorize_strings_known_impl(values: np.ndarray,
                                 nvalues: int,
                                 categories: list,
                                 missing_value,
                                 sort: int,
                                 codes: np.ndarray):
    if sort:
        categories = sorted(categories, key=lambda x: (x is None, x))

    reverse_categories = dict(zip(categories, range(len(categories))))
    missing_code = reverse_categories[missing_value]

    for i in range(nvalues):
        codes[i] = reverse_categories.get(values[i], missing_code)

    return codes, np.asarray(categories, dtype=object), reverse_categories


def factorize_strings_known_categories(values: np.ndarray,
                                       categories: list,
                                       missing_value,
                                       sort: int):
    """
    Factorize an array whose categories are already known.

    Any entries not in the specified categories will be given the code for
    `missing_value`.
    """
    if missing_value not in categories:
        categories.insert(0, missing_value)

    ncategories = len(categories)
    nvalues = len(values)
    if ncategories <= 2 ** 8:
        return factorize_strings_known_impl(
            values,
            nvalues,
            categories,
            missing_value,
            sort,
            np.empty(nvalues, dtype=np.uint8)
        )
    elif ncategories <= 2 ** 16:
        return factorize_strings_known_impl(
            values,
            nvalues,
            categories,
            missing_value,
            sort,
            np.empty(nvalues, np.uint16),
        )
    elif ncategories <= 2 ** 32:
        return factorize_strings_known_impl(
            values,
            nvalues,
            categories,
            missing_value,
            sort,
            np.empty(nvalues, np.uint32),
        )
    elif ncategories <= 2 ** 64:
        return factorize_strings_known_impl(
            values,
            nvalues,
            categories,
            missing_value,
            sort,
            np.empty(nvalues, np.uint64),
        )
    else:
        raise ValueError('ncategories larger than uint64')


def factorize_strings_impl(values: np.ndarray,
                           missing_value,
                           sort: int,
                           codes: np.ndarray):
    categories = [missing_value]
    reverse_categories = {missing_value: 0}

    key = None

    for i in range(len(values)):
        key = values[i]
        code = reverse_categories.get(key, -1)
        if code == -1:
            # Assign new code.
            code = len(reverse_categories)
            reverse_categories[key] = code
            categories.append(key)
        codes[i] = code

    # np.ndarray[np.int64_t, ndim=1] sorter
    # cdef np.ndarray[unsigned_integral, ndim=1] reverse_indexer
    # cdef int ncategories
    categories_array = np.asarray(
        categories,
        dtype=object,
    )

    if sort:
        # This is all adapted from pandas.core.algorithms.factorize.
        ncategories = len(categories_array)
        sorter = np.empty(ncategories, dtype=np.int64)

        # Don't include missing_value in the argsort, because None is
        # unorderable with bytes/str in py3. Always just sort it to 0.
        sorter[1:] = categories_array[1:].argsort() + 1
        sorter[0] = 0

        reverse_indexer = np.empty(ncategories, dtype=codes.dtype)
        reverse_indexer.put(sorter, np.arange(ncategories))

        codes = reverse_indexer.take(codes)
        categories_array = categories_array.take(sorter)
        reverse_categories = dict(zip(categories_array, range(ncategories)))

    return codes, categories_array, reverse_categories


_int_sizes = [1, 1, 2, 4, 4, 8, 8, 8, 8]


def factorize_strings(values: np.ndarray,
                      missing_value,
                      sort: int):
    """
    Factorize an array of (possibly duplicated) labels into an array of indices
    into a unique array of labels.

    This is ~30% faster than pandas.factorize, at the cost of not having
    special treatment for NaN, which we don't care about because we only
    support arrays of strings.

    (Though it's faster even if you throw in the nan checks that pandas does,
    because we're using dict and list instead of PyObjectHashTable and
    ObjectVector.  Python's builtin data structures are **really**
    well-optimized.)
    """
    nvalues = len(values)

    # use exclusive less than because we need to account for the possibility
    # that the missing value is not in values
    if nvalues < 2 ** 8:
        # we won't try to shrink because the ``codes`` array cannot get any
        # smaller
        return factorize_strings_impl(
            values,
            missing_value,
            sort,
            np.empty(nvalues, dtype=np.uint8)
        )
    elif nvalues < 2 ** 16:
        (codes,
         categories_array,
         reverse_categories) = factorize_strings_impl(
            values,
            missing_value,
            sort,
            np.empty(nvalues, dtype=np.uint16),
        )
    elif nvalues < 2 ** 32:
        (codes,
         categories_array,
         reverse_categories) = factorize_strings_impl(
            values,
            missing_value,
            sort,
            np.empty(nvalues, dtype=np.uint32),
        )
    elif nvalues < 2 ** 64:
        (codes,
         categories_array,
         reverse_categories) = factorize_strings_impl(
            values,
            missing_value,
            sort,
            np.empty(nvalues, dtype=np.uint64),
        )
    else:
        # unreachable
        raise ValueError('nvalues larger than uint64')

    length = len(categories_array)
    narrowest_dtype = smallest_uint_that_can_hold(length)
    if codes.dtype != narrowest_dtype:
        # condense the codes down to the narrowest dtype possible
        codes = codes.astype(narrowest_dtype)

    return codes, categories_array, reverse_categories
