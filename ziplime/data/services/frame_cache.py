"""A small on-disk cache for data a no-ingest workflow would otherwise re-fetch every run.

Mounting a dataset straight from a vendor is what makes a strategy runnable without an ingest step.
The cost is that the fetch happens again on every process start, and for a research loop -- edit a
rule, run it, read the number, edit again -- that dominates the wall clock. Downloading ten years
of prices for a hundred and twenty five tickers takes eighteen seconds, and a suite of eleven
strategies pays it eleven times unless something remembers.

This is deliberately not a bundle. A bundle is registered, versioned and meant to be shared; this
is a scratch copy of something the vendor will hand over again, safe to delete at any moment. The
distinction matters for what happens on a cache miss: nothing, beyond it being slower.

Staleness
---------

**Price history is not immutable.** A split or a dividend restates every bar before it when the
source adjusts, so a frame cached in June is wrong after a July split. Point-in-time datasets
pinned to a commit are immutable and can be cached forever; prices cannot. So every entry carries
a maximum age and the caller states it -- there is no default that is right for both.

Nothing here is keyed on content, only on the request. Two callers asking for different things must
pass different ``key`` parts, which is why :func:`cache_key` takes them apart rather than accepting
a string: an accidental collision would serve one strategy another's data silently.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import polars as pl
import structlog

_logger = structlog.get_logger(__name__)

#: Where entries live. ``ZIPLIME_CACHE_DIR`` overrides it, which is what a CI run should set.
ENV_VAR = "ZIPLIME_CACHE_DIR"
DEFAULT_DIR = Path.home() / ".cache" / "ziplime" / "frames"

#: Never expire. For data addressed by an immutable identity -- a Hugging Face commit, say.
FOREVER = datetime.timedelta(days=36_500)


def cache_dir() -> Path:
    """The directory entries are written to, created if it does not exist."""
    directory = Path(os.environ.get(ENV_VAR, DEFAULT_DIR))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def cache_key(*parts: Any) -> str:
    """A stable key from the pieces that identify a request.

    Everything that changes the answer must be a part: the source, the instruments, the window, the
    frequency, the columns. A part left out is a way to be served the wrong frame.
    """
    payload = json.dumps([_normalise(part) for part in parts], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _normalise(part: Any) -> Any:
    """Make a key part comparable across runs: sets and tuples become sorted lists."""
    if isinstance(part, (set, frozenset)):
        return sorted(str(item) for item in part)
    if isinstance(part, (list, tuple)):
        return [_normalise(item) for item in part]
    if isinstance(part, dict):
        return {str(k): _normalise(v) for k, v in sorted(part.items())}
    return part


def load(key: str, max_age: datetime.timedelta) -> pl.DataFrame | None:
    """Return the cached frame for ``key``, or ``None`` if it is missing or too old.

    A corrupt or unreadable entry is treated as a miss and removed, not raised: the caller can
    always fetch again, and failing a research run over a bad scratch file would be absurd.
    """
    path = cache_dir() / f"{key}.parquet"
    if not path.exists():
        return None
    age = datetime.timedelta(seconds=max(0.0, datetime.datetime.now().timestamp()
                                         - path.stat().st_mtime))
    if age > max_age:
        _logger.info("Cached frame is too old to use", key=key, age_days=round(age.days, 1),
                     max_age_days=max_age.days)
        return None
    try:
        frame = pl.read_parquet(path)
    except Exception as error:
        _logger.warning("Discarding an unreadable cache entry", key=key, error=str(error))
        path.unlink(missing_ok=True)
        return None
    _logger.info("Read a frame from the cache", key=key, rows=len(frame),
                 age_hours=round(age.total_seconds() / 3600, 1))
    return frame


def store(key: str, frame: pl.DataFrame) -> None:
    """Write ``frame`` under ``key``.

    Written to a temporary file and renamed, so a run interrupted mid-write leaves the previous
    entry intact rather than a half-written one that the next run would have to detect.
    """
    path = cache_dir() / f"{key}.parquet"
    temporary = path.with_suffix(".parquet.partial")
    try:
        frame.write_parquet(temporary)
        temporary.replace(path)
    except Exception as error:
        _logger.warning("Could not cache a frame; the next run will fetch it again",
                        key=key, error=str(error))
        temporary.unlink(missing_ok=True)
        return
    _logger.info("Cached a frame", key=key, rows=len(frame),
                 megabytes=round(path.stat().st_size / 1e6, 1))


def clear() -> int:
    """Delete every entry. Returns how many were removed."""
    removed = 0
    for path in cache_dir().glob("*.parquet"):
        path.unlink(missing_ok=True)
        removed += 1
    return removed
