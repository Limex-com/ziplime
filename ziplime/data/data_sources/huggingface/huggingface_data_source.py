"""A point-in-time dataset on the Hugging Face Hub, mounted as a ziplime data source.

    df = await data.history(assets=[apple], bar_count=30,
                            data_source="hf://ZipLime/congress-trading/features")

No ingest step, no bundle to build: the first read resolves the dataset to a commit, downloads
only the Parquet parts the simulation window can reach, maps tickers onto the asset database and
keeps the result in memory. Every later read in the run is served from there.

What the mounted frame looks like
---------------------------------

Two columns are renamed so the engine's own filters apply unchanged, and everything else is
carried through as published:

``date``
    The dataset's **knowledge date** -- when the row became observable -- as a UTC timestamp.
    ziplime windows every source with ``date < simulation time``, so putting the knowledge date
    here is what makes the mount point-in-time by construction rather than by convention.
    :mod:`~ziplime.data.data_sources.huggingface.manifest` picks the column and refuses to
    substitute an event date for it.
``sid``
    The listing the row is about, resolved from the dataset's ticker column against the asset
    database.

Rows whose ticker resolves to nothing are dropped, and how many is reported: these datasets carry
bonds identified by CUSIP, delisted names and foreign listings that no equity database will match,
and a mount that silently kept 30% of its rows would be worse than one that says so.

Why the window matters
----------------------

The datasets are large -- insider-trading's ``features`` is 4.1 million rows across 144 Parquet
parts -- and partitioned by ``knowledge_year``. Passing the simulation's own window lets whole
partitions be skipped without being downloaded, so a 2012 backtest never fetches 2015. Without a
window everything is fetched, which is correct and slow.
"""
import datetime
import re
from typing import Any, Self
from zoneinfo import ZoneInfo

import polars as pl
import structlog

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.data_sources.huggingface import hub
from ziplime.data.data_sources.huggingface.manifest import (
    DatasetManifest, ManifestError, parse_manifest, resolve_entity_column,
    resolve_knowledge_column,
)
from ziplime.data.services.data_source import DataSource

_logger = structlog.get_logger(__name__)

#: The scheme that names a dataset inline, as ``hf://owner/name/config``.
ADDRESS_SCHEME = "hf://"

#: Partition keys understood well enough to prune on. ``knowledge_year=2004`` in a path means the
#: part holds only rows from that year, so a window outside it need not be downloaded.
_YEAR_PARTITION = re.compile(r"(?:^|/)(?:knowledge_year|year)=(\d{4})(?:/|$)")

#: The asset class these datasets describe. Resolving against one class avoids the ambiguity of a
#: ticker that exists under two -- see ``AssetService.get_exchange_assets_by_symbols``.
_ENTITY_DOMAIN_ASSET_TYPES = {"us_equities": AssetType.EQUITY}


def is_address(value: Any) -> bool:
    """Whether ``value`` is an ``hf://`` dataset address."""
    return isinstance(value, str) and value.startswith(ADDRESS_SCHEME)


def parse_address(address: str) -> tuple[str, str | None]:
    """Split ``hf://owner/name/config`` into its repository and config.

    The config is optional: ``hf://ZipLime/congress-trading`` mounts the dataset's default table.

    Raises:
        ValueError: The address names no repository.
    """
    if not is_address(address):
        raise ValueError(f"{address!r} is not a Hugging Face address; expected "
                         f"{ADDRESS_SCHEME}owner/name or {ADDRESS_SCHEME}owner/name/config.")
    parts = [part for part in address[len(ADDRESS_SCHEME):].split("/") if part]
    if len(parts) < 2:
        raise ValueError(f"{address!r} names no dataset; expected "
                         f"{ADDRESS_SCHEME}owner/name or {ADDRESS_SCHEME}owner/name/config.")
    repo_id = "/".join(parts[:2])
    config = "/".join(parts[2:]) or None
    return repo_id, config


class HuggingFaceDataSource(DataSource):
    """One config of one Hub dataset, pinned to a commit and mounted point-in-time.

    Build it with :meth:`mount` rather than by hand; the constructor takes the pieces
    :meth:`mount` has already resolved.
    """

    def __init__(self, name: str, revision: hub.RepoRevision, manifest: DatasetManifest,
                 config: str, repo_files: tuple[str, ...], knowledge_column: str,
                 entity_column: str, asset_service, start_date: datetime.date,
                 end_date: datetime.date, fields: frozenset[str] | None = None,
                 frequency: datetime.timedelta | Period = datetime.timedelta(days=1),
                 session_timezone: str = "UTC"):
        # Timestamps are presented in the simulation's own timezone, not in UTC. polars refuses
        # to compare two tz-aware columns in different zones, and every window filter in the
        # engine compares this source's `date` against the simulation clock -- which is stamped
        # in the trading calendar's zone. The instant is unchanged; only its label moves.
        self.session_timezone = session_timezone
        super().__init__(name=name,
                         start_date=_at_zone(start_date, session_timezone),
                         end_date=_at_zone(end_date, session_timezone, end_of_day=True),
                         frequency=frequency, original_frequency=frequency,
                         data_type=DataType.CUSTOM)
        self.window_start = _as_date(start_date)
        self.window_end = _as_date(end_date)
        self.revision = revision
        self.manifest = manifest
        self.config = config
        self.repo_files = repo_files
        self.knowledge_column = knowledge_column
        self.entity_column = entity_column
        self.asset_service = asset_service
        self.requested_fields = fields
        self.data: pl.DataFrame | None = None
        #: Set once the frame is built: what was kept, what was dropped, and why.
        self.mount_report: dict[str, Any] = {}
        self._logger = structlog.get_logger(__name__)

    # ------------------------------------------------------------------ mounting

    @classmethod
    async def mount(cls, address: str, config: str | None = None, revision: str | None = None,
                    asset_service=None, start_date: datetime.date | None = None,
                    end_date: datetime.date | None = None,
                    fields: list[str] | frozenset[str] | None = None,
                    name: str | None = None, session_timezone: str = "UTC",
                    materialize: bool = False) -> Self:
        """Resolve a dataset and prepare it for reading.

        Args:
            address: ``hf://owner/name/config`` or a bare ``owner/name`` repository id.
            config: Table to mount, when the address does not name one. Defaults to the dataset's
                ``features`` table if it has one.
            revision: Branch, tag or commit. Defaults to the current default branch, resolved to
                the commit it points at so the mount is reproducible.
            asset_service: Used to turn the dataset's tickers into sids. Without it the mount
                still works but matches no assets, so it is required in practice.
            start_date, end_date: The simulation window. Used to skip Parquet parts the window
                cannot reach; without it the whole dataset is downloaded.
            fields: Columns to keep, on top of ``date`` and ``sid``. All of them by default.
            name: What the strategy calls this source. Defaults to the address.
            session_timezone: Zone to present timestamps in. Must match the simulation's trading
                calendar, since every window filter compares the two.
            materialize: Fetch the data now rather than on first read. Useful for failing fast in
                an ingest script; a strategy leaves it False so nothing is downloaded until the
                data is actually asked for.

        Returns:
            The mounted source, ready to hand to ``run_simulation(custom_data_sources=[...])`` or
            to ``data.history(data_source=...)``.
        """
        repo_id, address_config = (parse_address(address) if is_address(address)
                                   else (address, None))
        pinned = hub.resolve_revision(repo_id, revision=revision)
        manifest = parse_manifest(
            repo_id=repo_id, revision=pinned.sha,
            manifest_json=hub.read_text(pinned, "manifest.json"),
            readme=hub.read_text(pinned, "README.md"))

        spec = manifest.config(config or address_config)
        repo_files = tuple(path for path in pinned.files
                           if path.endswith(".parquet") and spec.matches(path))
        if not repo_files:
            raise ManifestError(
                f"{repo_id}:{spec.name} declares the paths {list(spec.paths)} but the repository "
                f"holds no Parquet file matching them at {pinned.short_sha}.")

        # The schema comes from one part, which is enough to choose the columns and is far cheaper
        # than reading all of them -- insider-trading's features config has 144.
        probe = pl.scan_parquet(hub.download(pinned, repo_files[0])).collect_schema().names()
        knowledge_column = resolve_knowledge_column(probe, repo_id, spec.name)
        entity_column = resolve_entity_column(probe, repo_id, spec.name)

        source = cls(
            name=name or f"{ADDRESS_SCHEME}{repo_id}/{spec.name}",
            revision=pinned, manifest=manifest, config=spec.name, repo_files=repo_files,
            knowledge_column=knowledge_column, entity_column=entity_column,
            asset_service=asset_service,
            start_date=start_date or manifest.coverage_start or datetime.date(1900, 1, 1),
            end_date=end_date or manifest.coverage_end or datetime.date(2099, 12, 31),
            fields=frozenset(fields) if fields else None,
            session_timezone=session_timezone)

        _logger.info("Mounted a Hugging Face dataset",
                     dataset=pinned.describe(), config=spec.name,
                     knowledge_column=knowledge_column, entity_column=entity_column,
                     parquet_parts=len(repo_files))
        if materialize:
            await source.materialize()
        return source

    async def materialize(self) -> pl.DataFrame:
        """Download, filter and index the data. Idempotent; the first read triggers it."""
        if self.data is not None:
            return self.data

        parts = self._parts_in_window()
        window_start, window_end = self.start_date, self.end_date

        scan = pl.scan_parquet([hub.download(self.revision, part) for part in parts])
        knowledge_dtype = scan.collect_schema()[self.knowledge_column]
        frame = (
            # Normalise the knowledge column first, so the window filter compares like with like:
            # a Date column and a tz-aware bound do not compare in polars.
            scan.with_columns(_to_utc(self.knowledge_column, knowledge_dtype)
                              .dt.convert_time_zone(self.session_timezone).alias("date"))
            .filter(pl.col("date") >= window_start, pl.col("date") <= window_end)
            .collect()
        )

        rows_in_window = len(frame)
        frame = await self._attach_sids(frame)
        frame = self._select_fields(frame).sort("date", "sid")

        self.data = frame
        self.mount_report = {
            "dataset": self.revision.describe(),
            "config": self.config,
            "parquet_parts_available": len(self.repo_files),
            "parquet_parts_read": len(parts),
            "rows_in_window": rows_in_window,
            "rows_mounted": len(frame),
            "instruments": frame["sid"].n_unique() if len(frame) else 0,
        }
        self._logger.info("Materialised a Hugging Face dataset", **self.mount_report)
        return frame

    def _parts_in_window(self) -> list[str]:
        """The Parquet parts whose partition can hold a row inside the window.

        Only year partitions are pruned. A part with no recognisable partition in its path is
        always read, because nothing about its name rules it out.
        """
        first_year, last_year = self.window_start.year, self.window_end.year
        kept = []
        for path in self.repo_files:
            match = _YEAR_PARTITION.search(path)
            if match and not (first_year <= int(match.group(1)) <= last_year):
                continue
            kept.append(path)
        skipped = len(self.repo_files) - len(kept)
        if skipped:
            self._logger.info("Skipped Parquet parts outside the window", skipped=skipped,
                              reading=len(kept),
                              window=f"{self.window_start}..{self.window_end}")
        return kept

    async def _attach_sids(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Resolve the dataset's tickers to sids and drop the rows that match no listing."""
        if frame.is_empty():
            return frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("sid"))
        if self.asset_service is None:
            self._logger.warning(
                "No asset service, so no ticker can be matched to a listing; this source will "
                "return nothing", dataset=self.revision.repo_id)
            return frame.clear().with_columns(pl.lit(None, dtype=pl.Int64).alias("sid"))

        tickers = [t for t in frame[self.entity_column].unique().to_list() if t]
        asset_type = _ENTITY_DOMAIN_ASSET_TYPES.get(self.manifest.entity_domain or "",
                                                    AssetType.EQUITY)
        listings = await self.asset_service.get_exchange_assets_by_symbols(
            symbols=[AssetSymbol(symbol=ticker, mic=None) for ticker in tickers],
            asset_type=asset_type)

        mapping = {ticker: listing.sid
                   for ticker, listing in zip(tickers, listings) if listing is not None}
        resolved = (
            frame.with_columns(
                pl.col(self.entity_column).replace_strict(mapping, default=None,
                                                          return_dtype=pl.Int64).alias("sid"))
            .filter(pl.col("sid").is_not_null())
        )
        dropped = len(frame) - len(resolved)
        if dropped:
            self._logger.info(
                "Dropped rows whose ticker matches no listing", dropped=dropped,
                kept=len(resolved), tickers_resolved=f"{len(mapping)}/{len(tickers)}",
                detail="These datasets carry CUSIP-identified bonds, delisted names and foreign "
                       "listings that an equity database does not hold.")
        return resolved

    def _select_fields(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Keep ``date`` and ``sid`` plus the requested columns, dropping the raw index column."""
        if self.requested_fields is None:
            drop = {self.knowledge_column} - {"date", "sid"}
            return frame.drop(drop, strict=False)
        keep = ["date", "sid"] + [column for column in self.requested_fields
                                  if column in frame.columns and column not in {"date", "sid"}]
        missing = set(self.requested_fields) - set(frame.columns) - {"date", "sid"}
        if missing:
            self._logger.warning("Requested fields the dataset does not publish",
                                 missing=sorted(missing), dataset=self.revision.repo_id)
        return frame.select(keep)

    # ------------------------------------------------------------------ reading

    def get_dataframe(self) -> pl.DataFrame:
        """The mounted frame.

        Raises:
            RuntimeError: Called before the data was materialised. Reads that go through
                :meth:`get_data_by_limit` materialise on demand; this is here for the paths that
                do not, so the failure names the cause instead of surfacing as an empty result.
        """
        if self.data is None:
            raise RuntimeError(
                f"{self.name} has not been materialised yet. Read it through data.history(), or "
                f"call `await source.materialize()` first.")
        return self.data

    async def get_data_by_limit(self, fields: frozenset[str] | None, limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta | Period,
                                assets: frozenset[Asset], include_end_date: bool) -> pl.DataFrame:
        """Serve a trailing window, materialising the dataset on the first call."""
        await self.materialize()
        return await super().get_data_by_limit(
            fields=fields, limit=limit, end_date=end_date, frequency=frequency, assets=assets,
            include_end_date=include_end_date)

    def get_missing_data_by_limit(self, fields: frozenset[str] | None, limit: int,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta | Period,
                                  assets: frozenset[Asset],
                                  include_end_date: bool) -> pl.DataFrame:
        """Return nothing for a window past the dataset's coverage.

        The base class delegates here when the simulation runs past ``end_date``. A dataset that
        stops in 2016 has nothing to say about 2020, and an empty frame is the honest answer --
        the alternative, raising, would make a strategy that merely *consults* the dataset fail
        outright once the backtest walked past its last row.
        """
        empty = self.data if self.data is not None else pl.DataFrame()
        return empty.clear()


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    """The calendar day of a bound, whichever of the two types it arrived as."""
    return value.date() if isinstance(value, datetime.datetime) else value


def _at_zone(day: datetime.date, timezone: str,
             end_of_day: bool = False) -> datetime.datetime:
    """A window bound as an instant in ``timezone``, matching the mounted date column."""
    zone = ZoneInfo(timezone)
    if isinstance(day, datetime.datetime):
        moment = day if day.tzinfo else day.replace(tzinfo=zone)
        return moment.astimezone(zone)
    return datetime.datetime.combine(
        day, datetime.time.max if end_of_day else datetime.time.min, tzinfo=zone)


def _to_utc(column: str, dtype: pl.DataType) -> pl.Expr:
    """Normalise a knowledge column to a UTC timestamp, whatever it was published as.

    The dtype comes from the scan's schema rather than being tested per row: the three shapes seen
    in practice are decided once for the whole column.

    * a plain ``Date`` -- congress ``features``; midnight UTC on that day;
    * a timestamp already carrying a zone -- insider ``feature_available_at``; converted;
    * a naive timestamp -- read **as** UTC rather than as local time. Guessing the publisher's
      timezone would shift every row by hours, and these datasets stamp in UTC.
    """
    expression = pl.col(column)
    if dtype == pl.Date:
        return expression.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            return expression.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        return expression.dt.convert_time_zone("UTC").cast(pl.Datetime("us", time_zone="UTC"))
    raise ManifestError(
        f"The knowledge column {column!r} is a {dtype}, which is not a date or a timestamp, so "
        f"rows cannot be placed in time.")
