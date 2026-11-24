"""Column utils for cross."""

import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible


def _columns():
    names, listings, array = type(ALL)._columns
    return names, listings, array


def column_index(key):
    """The column of an instrument key (name, listing or sid); None for anything else."""
    if key is ALL or isinstance(key, bool) or is_array(key):
        return None
    names, listings, _ = _columns()
    for k, (name, listing) in enumerate(zip(names, listings)):
        try:
            if key == name or key == listing or key == getattr(listing, "sid", None):
                return k
        except Exception:
            return None
    return None


def column_mask(key):
    """(1 × instruments): True at this key -- every instrument for the loop variable."""
    names, listings, array = _columns()
    if key is ALL:
        return True
    hits = [k for k, (name, listing) in enumerate(zip(names, listings))
            if key == name or key == listing or key == getattr(listing, "sid", None)]
    if not hits:
        return None
    mask = np.zeros((1, len(names)), dtype=bool)
    mask[0, hits[0]] = True
    return array(mask)


def _numeric(value) -> bool:
    return isinstance(value, (int, float, np.number)) or is_array(value)


def _windows_of(value):
    from ziplime.sweep.lazy.recording import _any_leaf
    try:
        return _any_leaf(value).data._windows
    except NotConvertible:
        return None


def _select(values, key):
    if key is ALL:
        return values
    names, listings, _ = _columns()
    for k, (name, listing) in enumerate(zip(names, listings)):
        if key == name or key == listing or key == getattr(listing, "sid", None):
            return values[:, k:k + 1]
    raise KeyError(key)
