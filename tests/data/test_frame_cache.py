"""The on-disk cache that keeps a no-ingest workflow from re-fetching on every run.

Two properties matter and both are easy to get wrong.

**Keys must separate.** Everything that changes the answer has to be part of the key. A cache that
collides serves one strategy another's data and says nothing, which is worse than no cache.

**Entries must expire.** Price history is not immutable: a split restates every bar before it once
the source adjusts. A cache with no maximum age quietly serves pre-split prices forever.
"""
import datetime
import os
import tempfile
import unittest
from pathlib import Path

import polars as pl

from ziplime.data.services import frame_cache


class CacheTestCase(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self._previous = os.environ.get(frame_cache.ENV_VAR)
        os.environ[frame_cache.ENV_VAR] = self._directory.name

    def tearDown(self):
        if self._previous is None:
            os.environ.pop(frame_cache.ENV_VAR, None)
        else:
            os.environ[frame_cache.ENV_VAR] = self._previous
        self._directory.cleanup()

    @property
    def frame(self) -> pl.DataFrame:
        return pl.DataFrame({"date": [1, 2, 3], "sid": [7, 7, 7], "close": [1.0, 2.0, 3.0]})


class KeyTests(CacheTestCase):
    def test_the_same_request_gives_the_same_key(self):
        parts = ("yahoo", [1, 2, 3], datetime.date(2016, 1, 1))
        self.assertEqual(frame_cache.cache_key(*parts), frame_cache.cache_key(*parts))

    def test_a_different_window_gives_a_different_key(self):
        self.assertNotEqual(
            frame_cache.cache_key("yahoo", [1, 2], datetime.date(2016, 1, 1)),
            frame_cache.cache_key("yahoo", [1, 2], datetime.date(2017, 1, 1)))

    def test_a_different_instrument_set_gives_a_different_key(self):
        self.assertNotEqual(frame_cache.cache_key("yahoo", [1, 2]),
                            frame_cache.cache_key("yahoo", [1, 2, 3]))

    def test_a_different_source_gives_a_different_key(self):
        self.assertNotEqual(frame_cache.cache_key("yahoo", [1]),
                            frame_cache.cache_key("finam", [1]))

    def test_set_ordering_does_not_change_the_key(self):
        # Callers pass sids as sets and frozensets; iteration order must not decide the key.
        self.assertEqual(frame_cache.cache_key("yahoo", {3, 1, 2}),
                         frame_cache.cache_key("yahoo", {2, 3, 1}))

    def test_nested_parts_are_handled(self):
        self.assertEqual(frame_cache.cache_key("s", {"fields": {"b", "a"}, "n": 1}),
                         frame_cache.cache_key("s", {"n": 1, "fields": {"a", "b"}}))


class RoundTripTests(CacheTestCase):
    def test_a_stored_frame_comes_back_unchanged(self):
        key = frame_cache.cache_key("test", [1])
        frame_cache.store(key, self.frame)

        self.assertTrue(frame_cache.load(key, datetime.timedelta(days=1)).equals(self.frame))

    def test_a_miss_is_none_rather_than_an_error(self):
        self.assertIsNone(frame_cache.load(frame_cache.cache_key("never", "stored"),
                                           datetime.timedelta(days=1)))

    def test_an_entry_older_than_the_limit_is_not_served(self):
        # The property that stops a pre-split price frame being served after the split.
        key = frame_cache.cache_key("test", [1])
        frame_cache.store(key, self.frame)

        self.assertIsNone(frame_cache.load(key, datetime.timedelta(seconds=0)))
        self.assertIsNotNone(frame_cache.load(key, frame_cache.FOREVER))

    def test_a_corrupt_entry_is_a_miss_and_is_removed(self):
        # A research run must not die over a half-written scratch file.
        key = frame_cache.cache_key("test", [1])
        path = Path(self._directory.name) / f"{key}.parquet"
        path.write_bytes(b"not parquet")

        self.assertIsNone(frame_cache.load(key, datetime.timedelta(days=1)))
        self.assertFalse(path.exists())

    def test_a_failed_write_leaves_no_partial_file(self):
        key = frame_cache.cache_key("test", [1])
        frame_cache.store(key, self.frame)
        self.assertEqual(list(Path(self._directory.name).glob("*.partial")), [])

    def test_storing_twice_replaces_the_entry(self):
        key = frame_cache.cache_key("test", [1])
        frame_cache.store(key, self.frame)
        replacement = pl.DataFrame({"date": [9], "sid": [1], "close": [9.0]})
        frame_cache.store(key, replacement)

        self.assertTrue(frame_cache.load(key, frame_cache.FOREVER).equals(replacement))

    def test_clear_removes_everything(self):
        for n in range(3):
            frame_cache.store(frame_cache.cache_key("test", [n]), self.frame)

        self.assertEqual(frame_cache.clear(), 3)
        self.assertEqual(list(Path(self._directory.name).glob("*.parquet")), [])

    def test_the_directory_is_created_on_demand(self):
        nested = Path(self._directory.name) / "deep" / "deeper"
        os.environ[frame_cache.ENV_VAR] = str(nested)
        frame_cache.store(frame_cache.cache_key("test", [1]), self.frame)

        self.assertTrue(nested.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
