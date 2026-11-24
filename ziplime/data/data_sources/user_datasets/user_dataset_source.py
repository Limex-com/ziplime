"""User dataset source for user_datasets."""

import datetime
import polars as pl
from ziplime.data.data_sources.huggingface.huggingface_data_source import HuggingFaceDataSource
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.constants.period import Period
from ziplime.data.data_sources.user_datasets.frame_cache import CACHE
from ziplime.data.data_sources.user_datasets.user_dataset_error import UserDatasetError


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

    def _need(self, fields: frozenset[str] | None) -> None:
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

    async def get_data_by_limit(
        self,
        fields: frozenset[str] | None,
        limit: int,
        end_date: datetime.datetime,
        frequency: datetime.timedelta | Period,
        assets: frozenset[ExchangeAsset],
        include_end_date: bool,
    ):
        self._need(fields)
        return await super().get_data_by_limit(fields=fields, limit=limit, end_date=end_date,
                                               frequency=frequency, assets=assets,
                                               include_end_date=include_end_date)

    async def get_data_by_window(
        self,
        fields: frozenset[str] | None,
        since: datetime.timedelta,
        end_date: datetime.datetime,
        frequency: datetime.timedelta | Period,
        assets: frozenset[ExchangeAsset],
        include_end_date: bool,
    ):
        self._need(fields)
        return await super().get_data_by_window(fields=fields, since=since, end_date=end_date,
                                                frequency=frequency, assets=assets,
                                                include_end_date=include_end_date)

    async def get_spot_value(
        self,
        assets: frozenset[ExchangeAsset],
        fields: frozenset[str] | None,
        dt: datetime.datetime,
        frequency: datetime.timedelta | Period | None = None,
        **kwargs,
    ):
        self._need(fields)
        return await super().get_spot_value(assets=assets, fields=fields, dt=dt,
                                            frequency=frequency, **kwargs)
