"""Hugging face data source for huggingface_data_source."""

import datetime
from typing import Any, Self
from exchange_calendars import ExchangeCalendar
import polars as pl
import structlog
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.data_sources.huggingface import hub
from ziplime.data.data_sources.huggingface.manifest import (
    DatasetManifest,
    ManifestError,
    parse_manifest,
    resolve_entity_column,
    resolve_event_column,
    resolve_knowledge_column,
)
from ziplime.data.services.data_source import DataSource
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import ADDRESS_SCHEME
from ziplime.data.data_sources.huggingface.huggingface_data_source.resolution import Resolution
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import _ENTITY_DOMAIN_ASSET_TYPES
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import _YEAR_PARTITION
from ziplime.data.data_sources.huggingface.huggingface_data_source.time_utils import _as_date
from ziplime.data.data_sources.huggingface.huggingface_data_source.time_utils import _at_zone
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import _logger
from ziplime.data.data_sources.huggingface.huggingface_data_source.time_utils import _to_session_time
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import is_address
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import parse_address


class HuggingFaceDataSource(DataSource):
    """One config of one Hub dataset, pinned to a commit and mounted point-in-time.

    Build it with :meth:`mount` rather than by hand; the constructor takes the pieces
    :meth:`mount` has already resolved.
    """

    #: Passed to ``LazyFrame.collect`` when materialising. A subclass reading a large object in
    #: place (``dataset://``) asks for the streaming engine, so memory follows the rows kept.
    _collect_kwargs: dict = {}

    def __init__(self, name: str, revision: hub.RepoRevision, manifest: DatasetManifest,
                 config: str, repo_files: tuple[str, ...], knowledge_column: str,
                 entity_column: str, event_column: str | None, asset_service: AssetService,
                 start_date: datetime.date,
                 end_date: datetime.date, trading_calendar: ExchangeCalendar,
                 fields: frozenset[str] | None = None,
                 frequency: datetime.timedelta | Period = datetime.timedelta(days=1),
                 row_filter: pl.Expr | None = None,
                 resolution: Resolution = Resolution.LATEST_ROW):
        timezone = str(trading_calendar.tz)
        super().__init__(name=name,
                         start_date=_at_zone(start_date, timezone),
                         end_date=_at_zone(end_date, timezone, end_of_day=True),
                         trading_calendar=trading_calendar,
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
        self.event_column = event_column
        self.row_filter = row_filter
        self.resolution = resolution
        self.asset_service = asset_service
        self.requested_fields = fields
        self.data: pl.DataFrame | None = None
        #: Set by :meth:`from_frame`; materialising reads this instead of downloading Parquet.
        self._source_frame: pl.DataFrame | None = None
        #: Set once the frame is built: what was kept, what was dropped, and why.
        self.mount_report: dict[str, Any] = {}
        self._logger = structlog.get_logger(__name__)

    # ------------------------------------------------------------------ mounting

    @classmethod
    def from_frame(cls, frame: pl.DataFrame, name: str, knowledge_column: str,
                   entity_column: str, asset_service: AssetService, start_date: datetime.date,
                   end_date: datetime.date, trading_calendar: ExchangeCalendar,
                   event_column: str | None = None,
                   fields: list[str] | frozenset[str] | None = None,
                   resolution: Resolution = Resolution.LATEST_ROW,
                   revision: hub.RepoRevision | None = None) -> Self:
        """Mount a frame the caller built, under the same point-in-time rules as a Hub config.

        For the cases the Hub cannot serve directly. The knowledge date a dataset publishes is not
        always the one you want: congressional ``trades`` is keyed on ``notification_date``, which
        on a House report is the day the *filer was told* about a trade in a managed account -- not
        the day the report reached the public. Joining ``filings`` supplies the filing date, which
        is, and the join has to happen before the mount.

        Everything else is unchanged: the knowledge column is floored against the event column,
        tickers are resolved to sids, and the engine's own window filter does the rest.

        Args:
            frame: The rows to mount.
            name: What the strategy calls this source.
            knowledge_column: The column to index on. Named rather than guessed, because a derived
                frame may carry several date columns and only the caller knows which is knowledge.
            entity_column: The column naming the instrument.
            event_column: Floors the knowledge date, when given.
            revision: Recorded for reproducibility, when the frame came from a pinned read.
        """
        source = cls(
            name=name, revision=revision, manifest=None, config="(derived)", repo_files=(),
            knowledge_column=knowledge_column, entity_column=entity_column,
            event_column=event_column, asset_service=asset_service,
            start_date=start_date, end_date=end_date,
            fields=frozenset(fields) if fields else None, trading_calendar=trading_calendar,
            resolution=resolution)
        source._source_frame = frame
        return source

    @classmethod
    async def mount(cls, address: str, trading_calendar: ExchangeCalendar,
                    config: str | None = None, revision: str | None = None,
                    asset_service: AssetService | None = None, start_date: datetime.date | None = None,
                    end_date: datetime.date | None = None,
                    fields: list[str] | frozenset[str] | None = None,
                    name: str | None = None,
                    row_filter: pl.Expr | None = None,
                    resolution: Resolution = Resolution.LATEST_ROW,
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
            trading_calendar: Calendar whose timezone is used for timestamps and query bounds.
            row_filter: A polars expression selecting the rows to keep, applied while the Parquet
                is read rather than afterwards. This is how a table with many rows per instrument
                is narrowed -- to one legislator, or to the rows a dataset documents as clean --
                without materialising the rest.
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
        repo_files = spec.select(path for path in pinned.files if path.endswith(".parquet"))
        if not repo_files:
            raise ManifestError(
                f"{repo_id}:{spec.name} declares the paths {list(spec.paths)} but the repository "
                f"holds no Parquet file matching them at {pinned.short_sha}.")

        # The schema comes from one part, which is enough to choose the columns and is far cheaper
        # than reading all of them -- insider-trading's features config has 144.
        probe = pl.scan_parquet(hub.download(pinned, repo_files[0])).collect_schema().names()
        knowledge_column = resolve_knowledge_column(probe, repo_id, spec.name)
        entity_column = resolve_entity_column(probe, repo_id, spec.name)
        event_column = resolve_event_column(probe, declared=manifest.raw_manifest.get("event_date"))

        source = cls(
            name=name or f"{ADDRESS_SCHEME}{repo_id}/{spec.name}",
            revision=pinned, manifest=manifest, config=spec.name, repo_files=repo_files,
            knowledge_column=knowledge_column, entity_column=entity_column,
            event_column=event_column, asset_service=asset_service, row_filter=row_filter,
            start_date=start_date or manifest.coverage_start or datetime.date(1900, 1, 1),
            end_date=end_date or manifest.coverage_end or datetime.date(2099, 12, 31),
            fields=frozenset(fields) if fields else None,
            trading_calendar=trading_calendar, resolution=resolution)

        _logger.info("Mounted a Hugging Face dataset",
                     dataset=pinned.describe(), config=spec.name,
                     knowledge_column=knowledge_column, entity_column=entity_column,
                     event_column=event_column, parquet_parts=len(repo_files))
        if materialize:
            await source.materialize()
        return source

    async def materialize(self) -> pl.DataFrame:
        """Download, filter and index the data. Idempotent; the first read triggers it."""
        if self.data is not None:
            return self.data

        window_start, window_end = self.start_date, self.end_date
        if self._source_frame is not None:
            parts = []
            scan = self._source_frame.lazy()
        else:
            parts = self._parts_in_window()
            scan = pl.scan_parquet([hub.download(self.revision, part) for part in parts])
        schema = scan.collect_schema()
        if self.row_filter is not None:
            # Applied before anything else, so the rows a caller does not want are never read.
            scan = scan.filter(self.row_filter)

        # Normalise the knowledge column first, so the window filter compares like with like: a
        # Date column and a tz-aware bound do not compare in polars.
        knowledge = _to_session_time(self.knowledge_column, schema[self.knowledge_column],
                                     str(self.trading_calendar.tz))
        floored = 0
        if self.event_column and self.event_column in schema.names():
            # A knowledge date earlier than its own event is impossible, and these datasets carry
            # such rows -- a misread year digit puts a disclosure centuries before the trade it
            # describes, which would make it visible from the first bar of every backtest. The
            # event date is the earliest the row could conceivably have been known, so floor to it.
            event = _to_session_time(self.event_column, schema[self.event_column],
                                     str(self.trading_calendar.tz))
            knowledge_floored = pl.max_horizontal(knowledge, event)
            floored = int(
                scan.select((knowledge < event).fill_null(False).sum())
                .collect(**self._collect_kwargs).item() or 0)
            knowledge = knowledge_floored

        frame = (
            scan.with_columns(knowledge.alias("date"))
            .filter(pl.col("date") >= window_start, pl.col("date") <= window_end)
            .collect(**self._collect_kwargs)
        )
        if floored:
            self._logger.warning(
                "Raised knowledge dates that preceded their own event", rows=floored,
                knowledge_column=self.knowledge_column, event_column=self.event_column,
                detail="Such a row cannot have been known before the thing it describes "
                       "happened; it is held back to the event date rather than trusted.")

        rows_in_window = len(frame)
        frame = await self._attach_sids(frame)
        frame = self._select_fields(frame).sort("date", "sid")

        self.data = frame
        self.mount_report = {
            "dataset": self.revision.describe() if self.revision else self.name,
            "config": self.config,
            "parquet_parts_available": len(self.repo_files),
            "parquet_parts_read": len(parts),
            "rows_in_window": rows_in_window,
            "knowledge_dates_floored": floored,
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
                "return nothing", dataset=self.name)
            return frame.clear().with_columns(pl.lit(None, dtype=pl.Int64).alias("sid"))

        tickers = [t for t in frame[self.entity_column].unique().to_list() if t]
        # A frame mounted through `from_frame` carries no manifest, so the domain is unknown and
        # equities are the assumption -- which is what every dataset of this kind describes.
        domain = self.manifest.entity_domain if self.manifest else None
        asset_type = _ENTITY_DOMAIN_ASSET_TYPES.get(domain or "", AssetType.EQUITY)
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
                                 missing=sorted(missing), dataset=self.name)
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
                                assets: frozenset[ExchangeAsset], include_end_date: bool) -> pl.DataFrame:
        """Serve a trailing window, materialising the dataset on the first call."""
        await self.materialize()
        return await super().get_data_by_limit(
            fields=fields, limit=limit, end_date=end_date, frequency=frequency, assets=assets,
            include_end_date=include_end_date)

    async def get_data_by_window(self, fields: frozenset[str] | None, since: datetime.timedelta,
                                 end_date: datetime.datetime,
                                 frequency: datetime.timedelta | Period,
                                 assets: frozenset[ExchangeAsset], include_end_date: bool) -> pl.DataFrame:
        """Serve a calendar-time window, materialising the dataset on the first call."""
        await self.materialize()
        return await super().get_data_by_window(
            fields=fields, since=since, end_date=end_date, frequency=frequency, assets=assets,
            include_end_date=include_end_date)

    async def get_spot_value(
        self,
        assets: frozenset[ExchangeAsset],
        fields: frozenset[str] | None,
        dt: datetime.datetime,
        frequency: datetime.timedelta | Period | None = None,
        **kwargs,
    ) -> pl.DataFrame:
        """The current state per instrument, resolved the way this source's data requires.

        This is what ``data.current`` calls. The base class takes the newest row, which is right
        for a source that emits one row per instrument per moment and wrong for one that
        republishes revisions -- so the source's own :class:`Resolution` decides, and a strategy
        reads every dataset the same way.
        """
        await self.materialize()
        frame = self.data
        if frame is None or frame.is_empty():
            return pl.DataFrame()

        wanted = list(fields) if fields else [c for c in frame.columns if c not in ("date", "sid")]
        wanted = [c for c in wanted if c in frame.columns]
        sids = [asset.sid for asset in assets]
        # Strictly before the current moment: the same rule every other read here follows, so a
        # filing accepted during the bar being traded is not visible inside it.
        visible = frame.filter(pl.col("date") < dt, pl.col("sid").is_in(sids)).sort("date")
        if visible.is_empty():
            return pl.DataFrame()

        if self.resolution is Resolution.COALESCE:
            return visible.group_by("sid").agg(
                pl.col("date").last(),
                *[pl.col(column).drop_nulls().last() for column in wanted])
        return visible.group_by("sid").agg(
            pl.col("date").last(), *[pl.col(column).last() for column in wanted])

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
