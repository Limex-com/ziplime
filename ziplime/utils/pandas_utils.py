"""
Utilities for working with pandas objects.
"""

from contextlib import contextmanager
from copy import deepcopy
import warnings

import numpy as np
import pandas as pd


def explode(df: pd.DataFrame):
    """Take a DataFrame and return a triple of

    (df.index, df.columns, df.values)
    """
    return df.index, df.columns, df.values


def nearest_unequal_elements(dts, dt):
    """Find values in ``dts`` closest but not equal to ``dt``.

    Returns a pair of (last_before, first_after).

    When ``dt`` is less than any element in ``dts``, ``last_before`` is None.
    When ``dt`` is greater any element in ``dts``, ``first_after`` is None.

    ``dts`` must be unique and sorted in increasing order.

    Parameters
    ----------
    dts : pd.DatetimeIndex
        Dates in which to search.
    dt : pd.Timestamp
        Date for which to find bounds.
    """
    if not dts.is_unique:
        raise ValueError("dts must be unique")

    if not dts.is_monotonic_increasing:
        raise ValueError("dts must be sorted in increasing order")

    if not len(dts):
        return None, None

    sortpos = dts.searchsorted(dt, side="left")
    try:
        sortval = dts[sortpos]
    except IndexError:
        # dt is greater than any value in the array.
        return dts[-1], None

    if dt < sortval:
        lower_ix = sortpos - 1
        upper_ix = sortpos
    elif dt == sortval:
        lower_ix = sortpos - 1
        upper_ix = sortpos + 1
    else:
        lower_ix = sortpos
        upper_ix = sortpos + 1

    lower_value = dts[lower_ix] if lower_ix >= 0 else None
    upper_value = dts[upper_ix] if upper_ix < len(dts) else None

    return lower_value, upper_value


def timedelta_to_integral_seconds(delta):
    """Convert a pd.Timedelta to a number of seconds as an int."""
    return int(delta.total_seconds())



@contextmanager
def ignore_pandas_nan_categorical_warning():
    with warnings.catch_warnings():
        # Pandas >= 0.18 doesn't like null-ish values in categories, but
        # avoiding that requires a broader change to how missing values are
        # handled in pipeline, so for now just silence the warning.
        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
        )
        yield


def categorical_df_concat(df_list: list[pd.DataFrame], inplace: bool=False):
    """Prepare list of pandas DataFrames to be used as input to pd.concat.
    Ensure any columns of type 'category' have the same categories across each
    dataframe.

    Parameters
    ----------
    df_list : list
        List of dataframes with same columns.
    inplace : bool
        True if input list can be modified. Default is False.

    Returns
    -------
    concatenated : df
        Dataframe of concatenated list.
    """

    if not inplace:
        df_list = deepcopy(df_list)

    # Assert each dataframe has the same columns/dtypes
    df = df_list[0]
    if not all([set(df.columns) == set(df_i.columns) for df_i in df_list[1:]]):
        raise ValueError("Input DataFrames must have the same columns.")

    categorical_columns = df.columns[df.dtypes == "category"]

    for col in categorical_columns:
        new_categories = _sort_set_none_first(
            _union_all(frame[col].cat.categories for frame in df_list)
        )

        with ignore_pandas_nan_categorical_warning():
            for df in df_list:
                df[col] = df[col].cat.set_categories(new_categories)

    return pd.concat(df_list)


def _union_all(iterables):
    """Union entries in ``iterables`` into a set."""
    return set().union(*iterables)


def _sort_set_none_first(set_):
    """Sort a set, sorting ``None`` before other elements, if present."""
    if None in set_:
        set_.remove(None)
        out = [None]
        out.extend(sorted(set_))
        set_.add(None)
        return out
    else:
        return sorted(set_)


def empty_dataframe(*columns):
    """Create an empty dataframe with columns of particular types.

    Parameters
    ----------
    *columns
        The (column_name, column_dtype) pairs.

    Returns
    -------
    typed_dataframe : pd.DataFrame
        The empty typed dataframe.

    Examples
    --------
    >>> df = empty_dataframe(
    ...     ('a', 'int64'),
    ...     ('b', 'float64'),
    ...     ('c', 'datetime64[ns]'),
    ... )

    >>> df
    Empty DataFrame
    Columns: [a, b, c]
    Index: []

    df.dtypes
    a             int64
    b           float64
    c    datetime64[ns]
    dtype: object
    """
    return pd.DataFrame(np.array([], dtype=list(columns)))
