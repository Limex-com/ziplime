"""Sortable sentinel for labelarray."""

from functools import total_ordering
from ziplime.utils.functional import instance


@instance  # This makes _sortable_sentinel a singleton instance.
@total_ordering
class _sortable_sentinel:
    """Dummy object that sorts before any other python object."""

    def __eq__(self, other):
        return self is other

    def __lt__(self, other) -> bool:
        return True
