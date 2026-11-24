"""User dataset store for user_datasets."""

from typing import Protocol
import polars as pl


class UserDatasetStore(Protocol):
    """Where the files are. S3 in a hosted run; anything with these two methods in a test."""

    def head(self, bucket: str, key: str) -> tuple[str, int] | None:
        """``(etag, size)`` of the object, or ``None`` when it does not exist (or may not be read)."""

    def scan(self, bucket: str, key: str) -> pl.LazyFrame:
        """A lazy frame over the object, read in place."""
