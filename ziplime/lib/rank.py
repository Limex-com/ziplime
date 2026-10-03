"""
Functions for ranking and sorting.
"""
import numpy as np
from scipy.stats import rankdata
from ziplime.utils.numpy_utils import is_missing


def rankdata_1d_descending(data: np.ndarray, method: str):
    """
    1D descending version of scipy.stats.rankdata.
    """
    return rankdata(-(data.view(np.float64)), method=method)


def masked_rankdata_2d(data: np.ndarray,
                       mask: np.ndarray,
                       missing_value,
                       method: str,
                       ascending: bool):
    """
    Compute masked rankdata on data on float64, int64, or datetime64 data.
    """
    dtype_name = data.dtype.name
    if dtype_name not in ('float64', 'int64', 'datetime64[ns]'):
        raise TypeError(
            "Can't compute rankdata on array of dtype %r." % dtype_name
        )

    missing_locations = (~mask | is_missing(data, missing_value))

    # Interpret the bytes of integral data as floats for sorting.
    data = data.copy().view(np.float64)
    data[missing_locations] = np.nan
    if not ascending:
        data = -data

    # OPTIMIZATION: Fast path the default case with our own specialized
    # Cython implementation.
    if method == 'ordinal':
        result = rankdata_2d_ordinal(data)
    else:
        # FUTURE OPTIMIZATION:
        # Write a less general "apply to rows" method that doesn't do all
        # the extra work that apply_along_axis does.
        result = np.apply_along_axis(rankdata, 1, data, method=method)

        # On SciPy >= 0.17, rankdata returns integers for any method except
        # average.
        if result.dtype.name != 'float64':
            result = result.astype('float64')

    # rankdata will sort missing values into last place, but we want our nans
    # to propagate, so explicitly re-apply.
    result[missing_locations] = np.nan
    return result


def rankdata_2d_ordinal(array: np.ndarray):
    """
    Equivalent to:
    numpy.apply_over_axis(scipy.stats.rankdata, 1, array, method='ordinal')
    """
    nrows, ncols = array.shape
    sort_idxs = None
    out = None

    # scipy.stats.rankdata explicitly uses MERGESORT instead of QUICKSORT for
    # the ordinal branch.  c.f. commit ab21d2fee2d27daca0b2c161bbb7dba7e73e70ba
    # sort_idxs = np.PyArray_ArgSort(array, 1, np.NPY_MERGESORT)
    sort_idxs = np.argsort(array, axis=1, kind='mergesort')
    # Roughly, "out = np.empty_like(array)"
    # out = np.PyArray_EMPTY(2, np.PyArray_DIMS(array), np.NPY_DOUBLE, False)
    out = np.empty(array.shape, dtype=np.float64)
    for i in range(nrows):
        for j in range(ncols):
            out[i, sort_idxs[i, j]] = j + 1.0

    return out


def grouped_masked_is_maximal(data: np.ndarray,  # 2 dim
                              groupby,
                              mask):
    """Build a mask of the top value for each row in ``data``, grouped by
    ``groupby`` and masked by ``mask``.
    Parameters
    ----------
    data : np.array[np.int64_t]
        Data on which we should find maximal values for each row.
    groupby : np.array[np.int64_t]
        Grouping labels for rows of ``data``. We choose one entry in each
        row for each unique grouping key in that row.
    mask : np.array[np.uint8_t]
        Boolean mask of locations to consider as possible maximal values.
        Locations with a 0 in ``mask`` are ignored.
    Returns
    -------
    maximal_locations : np.array[bool]
        Mask containing True for the maximal non-masked value in each row/group.
    """
    # Cython thinks ``.shape`` is an intp_t pointer on ndarrays, so we need to
    # cast to object to get the proper shape attribute.
    if not (data.shape == groupby.shape == data.shape):
        raise AssertionError(
            "Misaligned shapes in grouped_masked_is_maximal:"
            "data={}, groupby={}, mask={}".format(
                data.shape, groupby.shape, mask.shape,
            )
        )

    group = None
    value = None
    out = np.zeros_like(mask)
    best_per_group = {}
    nrows, ncols = data.shape
    for i in range(nrows):
        best_per_group.clear()
        for j in range(ncols):

            # NOTE: Callers are responsible for masking out values that should
            # be treated as null here.
            if not mask[i, j]:
                continue

            value = data[i, j]
            group = groupby[i, j]

            if group not in best_per_group:
                best_per_group[group] = j
                continue

            if value > data[i, best_per_group[group]]:
                best_per_group[group] = j

        for j in best_per_group.values():
            out[i, j] = 1

    return out.view(bool)
