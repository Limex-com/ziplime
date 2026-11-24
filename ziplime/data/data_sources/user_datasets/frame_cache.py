"""Frame cache for user_datasets."""

import collections
import os
import threading
import polars as pl
from ziplime.data.data_sources.user_datasets.support import DEFAULT_CACHE_BYTES


class _FrameCache:
    """Mounted frames by key, least recently used out first, bounded by bytes."""

    def __init__(self) -> None:
        self._entries: collections.OrderedDict[tuple, pl.DataFrame] = collections.OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def budget() -> int:
        value = os.environ.get("ZIPLIME_USER_DATASET_CACHE_MB")
        return int(value) * 1024 * 1024 if value not in (None, "") else DEFAULT_CACHE_BYTES

    def get(self, key: tuple) -> pl.DataFrame | None:
        with self._lock:
            frame = self._entries.get(key)
            if frame is not None:
                self._entries.move_to_end(key)
            return frame

    def put(self, key: tuple, frame: pl.DataFrame) -> None:
        budget = self.budget()
        size = frame.estimated_size()
        with self._lock:
            # A new ETag for the same user and name replaces the old content: drop it now.
            for stale in [k for k in self._entries if k[:2] == key[:2] and k[2] != key[2]]:
                del self._entries[stale]
            if size > budget:
                return
            self._entries[key] = frame
            self._entries.move_to_end(key)
            while sum(f.estimated_size() for f in self._entries.values()) > budget:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def keys(self) -> list[tuple]:
        with self._lock:
            return list(self._entries)


CACHE = _FrameCache()
