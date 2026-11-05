"""Private user datasets, ``dataset://<name>``: one Parquet file a user uploaded, read only by them.

    rows = await data.history(assets=context.universe, fields=["score"], bar_count=20,
                              data_source="dataset://my-signals")

The file lives at ``s3://<bucket>/users/<user_id>/<name>.parquet``. Uploading, validating and
managing it is the platform's job; this module only reads it during a backtest, under exactly the
point-in-time rules of an ``hf://`` dataset -- it builds a lazy frame and hands it to
:meth:`HuggingFaceDataSource.from_frame`, so the knowledge-date index, the event-date floor, the
ticker-to-sid mapping and ``data.current`` resolution are the same code.

Isolation
---------

The user is never taken from the strategy. ``user_id`` and the bucket reach the mount through the
run's *address context* -- ``run_simulation(address_context={"user_id": ..., "user_datasets_bucket":
...})`` -- which the hosting runtime fills from the backtest request. The address names only the
dataset: ``dataset://<uuid>/<name>``, ``dataset://../x`` or anything outside
``^[a-z0-9][a-z0-9_-]{0,62}$`` is refused, so a strategy has no way to point at another user's key.
Without a user or a bucket -- a local run -- the scheme refuses rather than returning nothing.

That is isolation of the *API*. Strategy code is ordinary Python running with the runtime's
credentials: if those may read every user's prefix and the strategy may import an S3 client, the
boundary is the credentials, not this module. See the runtime's notes on scrubbing them.

Reading
-------

Nothing is downloaded to disk. The object is scanned in place with column projection (when the
strategy names its fields) and the knowledge-date window filter, collected with the streaming
engine, so a large file costs memory for what the window keeps rather than for the file. One
``HeadObject`` per run gives the ETag and the size: the size is checked against a limit before any
data is read, and the ETag keys the cache.

Cache
-----

A bulk session runs the same strategy over the same dataset thousands of times in one process.
The mounted frame is kept in memory keyed by ``(user_id, name, etag, window, fields, resolution)``
-- the user first, so one user's entry is never another's, and the ETag, so a re-upload (same key,
new content) is read again. Least recently used entries are evicted past a byte budget.

Errors are one plain line each, never a traceback, and never name the bucket or the user.
"""
import collections
import datetime
import os
import re
import threading
import uuid
from typing import Any, Protocol

import polars as pl
import structlog

from ziplime.data.data_sources.huggingface.huggingface_data_source import (
    HuggingFaceDataSource, Resolution,
)
from ziplime.data.data_sources.huggingface.manifest import (
    ManifestError, resolve_entity_column, resolve_event_column, resolve_knowledge_column,
)

_logger = structlog.get_logger(__name__)

SCHEME = "dataset://"
NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")

#: Largest object a run will read. Overridden by the address context (``user_dataset_max_bytes``)
#: or ZIPLIME_USER_DATASET_MAX_BYTES.
DEFAULT_MAX_BYTES = 1024 * 1024 * 1024
#: Memory the mounted-frame cache may hold, per process (ZIPLIME_USER_DATASET_CACHE_MB).
DEFAULT_CACHE_BYTES = 512 * 1024 * 1024


class UserDatasetError(ValueError):
    """An expected failure, worded for the user: one line, nothing internal in it."""


class UserDatasetStore(Protocol):
    """Where the files are. S3 in a hosted run; anything with these two methods in a test."""

    def head(self, bucket: str, key: str) -> tuple[str, int] | None:
        """``(etag, size)`` of the object, or ``None`` when it does not exist (or may not be read)."""

    def scan(self, bucket: str, key: str) -> pl.LazyFrame:
        """A lazy frame over the object, read in place."""


class S3Store:
    """Reads ``s3://bucket/key`` with the credentials the process already has."""

    def __init__(self, region: str | None = None):
        self.region = region or os.environ.get("AWS_REGION") or os.environ.get(
            "AWS_DEFAULT_REGION") or "us-east-1"
        self._client = None

    def _s3(self):
        if self._client is None:
            try:
                import boto3
            except ImportError:  # pragma: no cover - hosted runtimes ship boto3
                raise UserDatasetError(
                    "dataset:// is only available in hosted backtests") from None
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def head(self, bucket: str, key: str) -> tuple[str, int] | None:
        from botocore.exceptions import ClientError
        try:
            answer = self._s3().head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            # Without ListBucket a missing key answers 403, not 404; both mean "not yours to read".
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status in (403, 404):
                return None
            raise
        return answer["ETag"].strip('"'), int(answer["ContentLength"])

    def scan(self, bucket: str, key: str) -> pl.LazyFrame:
        return pl.scan_parquet(f"s3://{bucket}/{key}", storage_options={"aws_region": self.region})


def parse_address(address: str) -> str:
    """The dataset name in ``dataset://<name>``; anything else is an invalid address."""
    if not isinstance(address, str) or not address.startswith(SCHEME):
        raise UserDatasetError(f"invalid dataset address '{address}'")
    name = address[len(SCHEME):]
    if not NAME_PATTERN.fullmatch(name):
        raise UserDatasetError(f"invalid dataset address '{address}'")
    return name


def _canonical_user(user_id: Any) -> str | None:
    if user_id is None or str(user_id).strip() == "":
        return None
    try:
        return str(uuid.UUID(str(user_id)))
    except ValueError:
        return None


def _human_size(size: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size} B"  # pragma: no cover


def _limit(context_limit: Any) -> int:
    for value in (context_limit, os.environ.get("ZIPLIME_USER_DATASET_MAX_BYTES")):
        if value not in (None, ""):
            return int(value)
    return DEFAULT_MAX_BYTES


class _FrameCache:
    """Mounted frames by key, least recently used out first, bounded by bytes."""

    def __init__(self):
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


class UserDatasetSource(HuggingFaceDataSource):
    """An ``hf://``-style source over one user's Parquet file, cached per user and ETag.

    Columns are read on demand. ``data.history(fields=["score"], ...)`` is the first read, and only
    then is it known which of a wide file's columns the run needs; the object is scanned for those
    and the role columns alone. A later read asking for another column scans again for the union.
    A mount given ``fields`` explicitly (``context.user_dataset(..., fields=[...])``) reads exactly
    those.
    """

    _collect_kwargs = {"engine": "streaming"}
    _ALL = "*"

    def _configure(self, scan: pl.LazyFrame, columns: list[str], roles: list[str], cache_base: tuple,
                   dataset_name: str, fixed_fields: frozenset[str] | None) -> None:
        self._scan_all = scan
        self._columns = columns
        self._roles = roles
        self._cache_base = cache_base
        self._dataset_name = dataset_name
        self._projected: frozenset[str] | str | None = None
        if fixed_fields is not None:
            self._project(fixed_fields)
            self._fixed = True
        else:
            self._fixed = False

    def _project(self, fields: frozenset[str] | str) -> None:
        self._projected = fields
        if fields == self._ALL:
            self._source_frame = self._scan_all
            self.requested_fields = None
        else:
            keep = [c for c in self._columns if c in set(self._roles) | set(fields)]
            self._source_frame = self._scan_all.select(keep)
            self.requested_fields = frozenset(fields)
        self.data = None

    def _cache_key(self) -> tuple:
        fields = self._projected
        return self._cache_base + ((fields if fields == self._ALL else tuple(sorted(fields))),)

    def _need(self, fields) -> None:
        """Make sure the materialised frame carries ``fields`` (all columns for ``None``)."""
        if self._fixed:
            return
        wanted = (self._ALL if not fields
                  else frozenset(f for f in fields if f not in ("date", "sid") and f in self._columns))
        current = self._projected
        if current == self._ALL:
            return
        if current is not None and wanted != self._ALL and wanted <= current:
            return
        self._project(self._ALL if wanted == self._ALL else wanted | (current or frozenset()))

    async def materialize(self) -> pl.DataFrame:
        if self.data is not None:
            return self.data
        if self._projected is None:
            self._project(self._ALL)
        key = self._cache_key()
        cached = CACHE.get(key)
        if cached is not None:
            self.data = cached
            self.mount_report = {"dataset": self.name, "cached": True, "rows_mounted": len(cached)}
            self._logger.info("Mounted a user dataset from the cache", rows=len(cached))
            return cached
        try:
            frame = await super().materialize()
        except UserDatasetError:
            raise
        except MemoryError:
            raise UserDatasetError(
                f"dataset '{self._dataset_name}' cannot be read: too large for this runtime") from None
        except Exception as error:
            reason = str(error).splitlines()[0] if str(error) else type(error).__name__
            raise UserDatasetError(
                f"dataset '{self._dataset_name}' cannot be read: {reason[:200]}") from None
        CACHE.put(key, frame)
        return frame

    async def get_data_by_limit(self, fields, limit, end_date, frequency, assets, include_end_date):
        self._need(fields)
        return await super().get_data_by_limit(fields=fields, limit=limit, end_date=end_date,
                                               frequency=frequency, assets=assets,
                                               include_end_date=include_end_date)

    async def get_data_by_window(self, fields, since, end_date, frequency, assets, include_end_date):
        self._need(fields)
        return await super().get_data_by_window(fields=fields, since=since, end_date=end_date,
                                                frequency=frequency, assets=assets,
                                                include_end_date=include_end_date)

    async def get_spot_value(self, assets, fields, dt, frequency=None, **kwargs):
        self._need(fields)
        return await super().get_spot_value(assets=assets, fields=fields, dt=dt,
                                            frequency=frequency, **kwargs)


def mount_user_dataset(address: str, *, asset_service=None, start_date: datetime.date | None = None,
                       end_date: datetime.date | None = None, trading_calendar=None,
                       user_id: Any = None, user_datasets_bucket: str | None = None,
                       user_dataset_store: UserDatasetStore | None = None,
                       user_dataset_max_bytes: int | None = None,
                       fields: list[str] | None = None,
                       resolution: Resolution = Resolution.LATEST_ROW,
                       name: str | None = None, **_ignored) -> UserDatasetSource:
    """Mount ``dataset://<name>`` for the user of the running backtest.

    ``user_id``, ``user_datasets_bucket`` and, in tests, ``user_dataset_store`` come from the run's
    address context, never from the strategy. Raises :class:`UserDatasetError` with a one-line
    message for every expected failure.
    """
    dataset = parse_address(address)
    user = _canonical_user(user_id)
    store = user_dataset_store or (S3Store() if user_datasets_bucket else None)
    if user is None or not user_datasets_bucket or store is None:
        raise UserDatasetError("dataset:// is only available in hosted backtests")

    key = f"users/{user}/{dataset}.parquet"
    try:
        found = store.head(user_datasets_bucket, key)
    except UserDatasetError:
        raise
    except Exception as error:
        _logger.warning("Could not look up a user dataset", error=str(error).splitlines()[0][:200])
        raise UserDatasetError(f"dataset '{dataset}' cannot be read: storage is unavailable") from None
    if found is None:
        raise UserDatasetError(f"403 - dataset does not exist: '{dataset}'")
    etag, size = found
    limit = _limit(user_dataset_max_bytes)
    if size > limit:
        raise UserDatasetError(
            f"dataset '{dataset}' is {_human_size(size)}, over the {_human_size(limit)} limit")

    try:
        scan = store.scan(user_datasets_bucket, key)
        columns = scan.collect_schema().names()
    except Exception as error:
        reason = str(error).splitlines()[0] if str(error) else type(error).__name__
        raise UserDatasetError(f"dataset '{dataset}' cannot be read: {reason[:200]}") from None
    try:
        knowledge = resolve_knowledge_column(columns, f"dataset://{dataset}", "")
        entity = resolve_entity_column(columns, f"dataset://{dataset}", "")
        event = resolve_event_column(columns)
    except ManifestError as error:
        raise UserDatasetError(
            f"dataset '{dataset}' cannot be read: {str(error).splitlines()[0][:200]}") from None
    if "sid" in columns:
        raise UserDatasetError(f"dataset '{dataset}' cannot be read: it has a column named 'sid'")

    wanted = None
    if fields:
        missing = sorted(set(fields) - set(columns))
        if missing:
            raise UserDatasetError(
                f"dataset '{dataset}' cannot be read: no column {', '.join(map(repr, missing))}")
        wanted = frozenset(fields)

    roles = [c for c in (knowledge, entity, event) if c]
    _logger.info("Mounted a user dataset", dataset=dataset, size=size,
                 knowledge_column=knowledge, entity_column=entity, event_column=event)
    source = UserDatasetSource.from_frame(
        scan, name=name or address, knowledge_column=knowledge, entity_column=entity,
        event_column=event, asset_service=asset_service,
        start_date=start_date or datetime.date(1900, 1, 1),
        end_date=end_date or datetime.date(2099, 12, 31),
        trading_calendar=trading_calendar, resolution=resolution)
    source._configure(scan, columns, roles,
                      cache_base=(user, dataset, etag, str(start_date), str(end_date), str(resolution)),
                      dataset_name=dataset, fixed_fields=wanted)
    return source
