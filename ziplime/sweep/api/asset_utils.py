"""Asset utils for api."""

import numpy as np
from ziplime.sweep.params import SweepError
from ziplime.sweep.api.columns import Columns


def asset_map(value, columns: Columns) -> bool:
    """A dict a sweep can hold as arrays: keyed by the universe's instruments (names, listings or
    sids), with numbers for values."""
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        try:
            columns.index(key)
        except SweepError:
            return False
        if not isinstance(item, (int, float, bool, np.number)):
            return False
    return True
