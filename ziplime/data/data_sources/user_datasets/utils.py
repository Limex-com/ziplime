"""Utils for user_datasets."""

import datetime
import os
import uuid
from typing import Any
from exchange_calendars import ExchangeCalendar
from ziplime.data.data_sources.huggingface.huggingface_data_source import Resolution
from ziplime.data.data_sources.huggingface.manifest import (
    ManifestError,
    resolve_entity_column,
    resolve_event_column,
    resolve_knowledge_column,
)
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.data_sources.user_datasets.support import DEFAULT_MAX_BYTES
from ziplime.data.data_sources.user_datasets.support import NAME_PATTERN
from ziplime.data.data_sources.user_datasets.s3_store import S3Store
from ziplime.data.data_sources.user_datasets.support import SCHEME
from ziplime.data.data_sources.user_datasets.user_dataset_error import UserDatasetError
from ziplime.data.data_sources.user_datasets.user_dataset_source import UserDatasetSource
from ziplime.data.data_sources.user_datasets.user_dataset_store import UserDatasetStore
from ziplime.data.data_sources.user_datasets.support import _logger


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


def mount_user_dataset(
    address: str,
    *,
    asset_service: AssetService | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    trading_calendar: ExchangeCalendar | None = None,
    user_id: Any = None,
    user_datasets_bucket: str | None = None,
    user_dataset_store: UserDatasetStore | None = None,
    user_dataset_max_bytes: int | None = None,
    fields: list[str] | None = None,
    resolution: Resolution = Resolution.LATEST_ROW,
    name: str | None = None,
    **_ignored,
) -> UserDatasetSource:
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
