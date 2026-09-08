import datetime

import polars as pl
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.split import Split
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.bundle_storage import BundleStorage


class AdjustmentsService:
    """
    Service class responsible for adjusting bundle OHLCV data based on corporate actions.

    The service exposes two entry points:

    - :meth:`adjust_bundle` loads a bundle (by name/version) through a
      :class:`BundleService`, applies the adjustments, and optionally persists the
      result as a new bundle;
    - :meth:`adjust_data_bundle` takes an already-loaded :class:`DataBundle`, applies
      the adjustments, and returns the adjusted DataFrame (no loading or saving).

    The adjustment is backward-looking (TradingView-style), so the latest bar of each
    asset stays unchanged:

    - splits: bars strictly before the split's effective date have their prices multiplied
      by ``ratio`` and their volumes divided by ``ratio``. The split ratio is the
      existing ``Split.ratio`` convention (old shares divided by new shares), so a
      2-for-1 split has a ratio of ``0.5``;
    - cash dividends: bars strictly before the dividend's ex-date have their prices
      multiplied by a proportional factor ``(Pclose - D) / Pclose``, where ``D`` is the
      dividend amount and ``Pclose`` is the close of the last trading day strictly before
      the ex-date.

    Adjustments for the two event types are combined multiplicatively per bar:
    ``adjusted_price = price * split_factor * dividend_factor`` where ``split_factor``
    is the product of ``1 / ratio`` over all splits after the bar date and
    ``dividend_factor`` is the product of ``(Pclose - D) / Pclose`` over all cash
    dividends with ex-date after the bar date.

    The adjusted values are stored in additional columns (``open_adjusted``,
    ``high_adjusted``, ``low_adjusted``, ``close_adjusted``, ``volume_adjusted``, and
    ``price_adjusted`` when a ``price`` column is present).
    """

    def __init__(self, asset_service: AssetService):
        """
        Args:
            asset_service (AssetService): Service providing assets and their corporate actions
                (splits and dividends).
        """
        self._asset_service = asset_service
        self._logger = structlog.get_logger(__name__)

    async def adjust_bundle(self, bundle_service: BundleService,
                            bundle_name: str,
                            bundle_version: str | None = None,
                            adjusted_bundle_name: str | None = None,
                            save: bool = True,
                            merge: bool = False,
                            target_bundle_storage: BundleStorage | None = None) -> pl.DataFrame:
        """
        Loads a bundle through the bundle service, applies corporate-action adjustments,
        and optionally saves the result as a new bundle.

        This is the high-level entry point. It loads the bundle (by name/version) via the
        provided :class:`BundleService`, forwards the loaded :class:`DataBundle` to
        :meth:`adjust_data_bundle`, and then (optionally) persists the adjusted data as a
        new bundle.

        Args:
            bundle_service (BundleService): Service used to load (and resolve) the bundle.
            bundle_name (str): Name of the data bundle to adjust.
            bundle_version (str | None): Version of the bundle to adjust. If not provided,
                the latest version of the bundle will be used.
            adjusted_bundle_name (str | None): Name to save the adjusted bundle under.
                If not provided, defaults to ``"{bundle_name}_adjusted"``.
            save (bool): If True (default), the adjusted data is saved as a new bundle.
                If False, the adjusted DataFrame is simply returned without persisting.
            merge (bool): If True, the adjusted data is merged into the existing
                ``adjusted_bundle_name`` bundle (matched on ``sid`` and ``date``) instead
                of creating a new version.
            target_bundle_storage: Storage implementation for the adjusted bundle. If not
                provided, the source bundle's storage implementation is reused.

        Returns:
            pl.DataFrame: A new DataFrame containing all original bundle columns plus
            ``open_adjusted``, ``high_adjusted``, ``low_adjusted``, ``close_adjusted``,
            ``volume_adjusted`` (and ``price_adjusted`` if a ``price`` column exists).

        Raises:
            ValueError: If the bundle (or the bundle with the given version) is not found.
        """
        self._logger.info(f"Adjusting bundle: bundle_name={bundle_name}, bundle_version={bundle_version}")

        data_bundle, source_storage = await bundle_service.load_bundle_definition(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )
        data_bundle, _missing_data = await bundle_service.load_bundle(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )
        adjusted_data = await self.adjust_data_bundle(data_bundle=data_bundle)

        if save:
            await self._save_adjusted_bundle(
                source_data_bundle=data_bundle,
                adjusted_data=adjusted_data,
                adjusted_bundle_name=adjusted_bundle_name or f"{bundle_name}_adjusted",
                merge=merge,
                bundle_service=bundle_service,
                bundle_storage=target_bundle_storage or source_storage,
            )

        return adjusted_data

    async def adjust_bundle_streaming(
            self,
            bundle_service: BundleService,
            bundle_name: str,
            bundle_version: str | None = None,
            adjusted_bundle_name: str | None = None,
            batch_days: int | None = 30,
            batch_assets: int | None = 100,
            target_bundle_storage: BundleStorage | None = None,
            assets: list[Asset] | None = None,
    ) -> None:
        """Adjust a bundle in bounded date/asset batches.

        When ``adjusted_bundle_name`` equals ``bundle_name``, the source bundle is
        updated in place. Otherwise, a new adjusted bundle is created.

        Args:
            assets: Optional assets whose rows should receive adjustments. For a new
                adjusted bundle, unselected assets are copied with neutral adjustment
                values. For in-place updates, only selected asset rows are rewritten.
        """
        source_bundle, source_storage = await bundle_service.load_bundle_definition(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )
        bundle_sids = await bundle_service.get_bundle_sids(
            data_bundle=source_bundle,
            bundle_storage=source_storage,
        )
        exchange_assets = await self._asset_service.get_exchange_assets_by_sids(sids=bundle_sids)
        if assets is not None:
            asset_ids = {asset.id for asset in assets}
            exchange_assets = [
                exchange_asset for exchange_asset in exchange_assets
                if exchange_asset.asset is not None and exchange_asset.asset.id in asset_ids
            ]
        selected_sids = {
            exchange_asset.sid for exchange_asset in exchange_assets
            if exchange_asset.sid is not None and exchange_asset.sid in bundle_sids
        }
        if assets is not None and not selected_sids:
            raise ValueError("None of the requested assets are present in the bundle.")
        adjustment_sids = sorted(selected_sids) if assets is not None else bundle_sids
        exchange_assets = [
            exchange_asset for exchange_asset in exchange_assets
            if exchange_asset.sid in adjustment_sids
        ]
        sids_by_asset_id: dict[int, list[int]] = {}
        assets_by_id: dict[int, Asset] = {}
        for exchange_asset in exchange_assets:
            asset = exchange_asset.asset
            if asset is not None and asset.id is not None:
                assets_by_id[asset.id] = asset
                sids_by_asset_id.setdefault(asset.id, []).append(exchange_asset.sid)

        assets = list(assets_by_id.values())
        dividends = await self._asset_service.get_dividends_by_assets_and_ex_date_between(
            assets=assets,
            ex_date_from=source_bundle.start_date.date(),
            ex_date_to=source_bundle.end_date.date(),
        )
        splits = await self._asset_service.get_splits_by_assets_and_effective_date_between(
            assets=assets,
            effective_date_from=source_bundle.start_date.date(),
            effective_date_to=source_bundle.end_date.date(),
        )
        events = self._build_events_frame(
            splits=splits, dividends=dividends, sids_by_asset_id=sids_by_asset_id
        ).with_columns(
            pl.col("event_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone(source_bundle.trading_calendar.tz.key)
            .alias("event_date")
        )
        events = await self._attach_dividend_factors_from_storage(
            bundle_service=bundle_service,
            source_storage=source_storage,
            data_bundle=source_bundle,
            events=events,
        )

        adjusted_name = adjusted_bundle_name or f"{bundle_name}_adjusted"
        in_place = adjusted_name == bundle_name
        target_storage = source_storage if in_place else target_bundle_storage or source_storage
        if in_place:
            target_bundle = source_bundle
            await bundle_service.initialize_adjustment_columns(
                data_bundle=source_bundle,
                bundle_storage=source_storage,
                adjusted_columns={
                    "open_adjusted": "open",
                    "high_adjusted": "high",
                    "low_adjusted": "low",
                    "close_adjusted": "close",
                    "price_adjusted": "price",
                    "volume_adjusted": "volume",
                },
                sids=adjustment_sids if assets is not None else None,
            )
        else:
            target_version = str(int(datetime.datetime.now(
                tz=source_bundle.trading_calendar.tz
            ).timestamp()))
            target_bundle = DataBundle(
                name=adjusted_name,
                version=target_version,
                start_date=source_bundle.start_date,
                end_date=source_bundle.end_date,
                trading_calendar=source_bundle.trading_calendar,
                frequency=source_bundle.frequency,
                original_frequency=source_bundle.original_frequency,
                data_type=source_bundle.data_type,
                timestamp=datetime.datetime.now(tz=source_bundle.trading_calendar.tz),
            )
            await bundle_service.register_bundle(
                data_bundle=target_bundle, bundle_storage=target_storage, merge=False
            )

        first_batch = not in_place
        async for batch in bundle_service.iter_bundle_batches(
                data_bundle=source_bundle,
                bundle_storage=source_storage,
                batch_days=batch_days,
                batch_assets=batch_assets,
                sids=adjustment_sids if in_place and assets is not None else None,
        ):
            price_columns = [
                column for column in ("open", "high", "low", "close", "price")
                if column in batch.columns
            ]
            adjusted = self._apply_adjustments(
                data=batch,
                events=self._align_event_dates(events, batch.schema["date"]),
                price_columns=price_columns,
                volume_column="volume" if "volume" in batch.columns else None,
            )
            target_bundle.data = adjusted
            await bundle_service.store_bundle(
                data_bundle=target_bundle,
                bundle_storage=target_storage,
                merge=not first_batch,
                merge_columns=["sid", "date"],
            )
            first_batch = False

        self._logger.info(
            "Stream-adjusted bundle",
            bundle_name=bundle_name,
            adjusted_bundle_name=adjusted_name,
            batch_days=batch_days,
            batch_assets=batch_assets,
            splits=len(splits),
            dividends=len(dividends),
        )

    async def _attach_dividend_factors_from_storage(
            self,
            bundle_service: BundleService,
            source_storage: BundleStorage,
            data_bundle: DataBundle,
            events: pl.DataFrame,
    ) -> pl.DataFrame:
        """Calculate dividend factors by reading only one reference bar per event."""
        dividend_events = events.filter(pl.col("dividend") > 0)
        if dividend_events.is_empty():
            return events.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("dividend_factor"))

        factors = []

        for event in dividend_events.iter_rows(named=True):
            previous = await bundle_service.load_bundle_before(
                data_bundle=data_bundle,
                bundle_storage=source_storage,
                sid=event["sid"],
                date=event["event_date"],
                columns=["date", "close"],
            )
            if previous.is_empty() or previous["close"][0] is None or previous["close"][0] <= 0:
                self._logger.warning(
                    "Dividend has no positive preceding close; leaving it unadjusted.",
                    sid=event["sid"],
                    event_date=event["event_date"],
                )
                factor = 1.0
            else:
                previous_close = float(previous["close"][0])
                factor = (previous_close - float(event["dividend"])) / previous_close
                if factor <= 0:
                    self._logger.warning(
                        "Dividend factor is non-positive; leaving it unadjusted.",
                        sid=event["sid"],
                        event_date=event["event_date"],
                        previous_close=previous_close,
                        dividend=event["dividend"],
                    )
                    factor = 1.0
            factors.append({
                "sid": event["sid"],
                "event_date": event["event_date"],
                "dividend_factor": factor,
            })

        return (
            events
            .join(pl.DataFrame(factors), on=["sid", "event_date"], how="left")
            .with_columns(pl.col("dividend_factor").fill_null(1.0))
        )

    @staticmethod
    def _align_event_dates(events: pl.DataFrame, date_dtype: pl.DataType) -> pl.DataFrame:
        """Match event-date dtype/timezone to the current market-data batch."""
        event_date = pl.col("event_date")
        if isinstance(date_dtype, pl.Date):
            event_date = event_date.dt.date()
        elif isinstance(date_dtype, pl.Datetime):
            if date_dtype.time_zone is None:
                event_date = event_date.dt.replace_time_zone(None)
            else:
                event_date = event_date.dt.convert_time_zone(date_dtype.time_zone)
            event_date = event_date.cast(date_dtype)
        return events.with_columns(event_date.alias("event_date"))

    async def adjust_data_bundle(self, data_bundle: DataBundle) -> pl.DataFrame:
        """
        Applies corporate-action adjustments to an already-loaded :class:`DataBundle` and
        returns a new DataFrame with the adjusted columns.

        This is the low-level entry point. It does not load or save anything; it only
        transforms the bundle's data. It can be called directly when you already have a
        :class:`DataBundle` in hand.

        Args:
            data_bundle (DataBundle): The data bundle whose ``data`` should be adjusted.

        Returns:
            pl.DataFrame: A new DataFrame containing all original bundle columns plus
            ``open_adjusted``, ``high_adjusted``, ``low_adjusted``, ``close_adjusted``,
            ``volume_adjusted`` (and ``price_adjusted`` if a ``price`` column exists).
        """
        if data_bundle.data_type != DataType.MARKET_DATA:
            raise ValueError(f"Bundle {data_bundle.name} (version={data_bundle.version}) is not MARKET_DATA bundle.")

        data = data_bundle.data
        if data is None or data.is_empty():
            self._logger.warning(f"Bundle {data_bundle.name} (version={data_bundle.version}) contains no data.")
            return data if data is not None else pl.DataFrame()

        exchange_assets = await self._asset_service.get_exchange_assets_by_sids(
            sids=data["sid"].unique().to_list()
        )

        sids_by_asset_id: dict[int, list[int]] = {}
        assets_by_id: dict[int, Asset] = {}
        for exchange_asset in exchange_assets:
            asset = exchange_asset.asset
            if asset is None or asset.id is None:
                continue
            assets_by_id[asset.id] = asset
            sids_by_asset_id.setdefault(asset.id, []).append(exchange_asset.sid)


        price_columns = [column for column in ("open", "high", "low", "close", "price") if column in data.columns]
        volume_column = "volume" if "volume" in data.columns else None

        assets = list(assets_by_id.values())
        ex_date_from = data_bundle.start_date.date()
        ex_date_to = data_bundle.end_date.date() if isinstance(data_bundle.end_date, datetime.datetime) else data_bundle.end_date

        dividends = await self._asset_service.get_dividends_by_assets_and_ex_date_between(
            assets=assets,
            ex_date_from=ex_date_from,
            ex_date_to=ex_date_to
        )
        splits = await self._asset_service.get_splits_by_assets_and_effective_date_between(
            assets=assets,
            effective_date_from=ex_date_from,
            effective_date_to=ex_date_to
        )

        events = self._build_events_frame(splits=splits, dividends=dividends, sids_by_asset_id=sids_by_asset_id)

        events = events.with_columns(
            pl.col("event_date")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone(data_bundle.trading_calendar.tz.key)
            .alias("event_date")
        )
        events = self._attach_dividend_factors(data=data, events=events)
        adjusted_data = self._apply_adjustments(data=data, events=events,
                                                price_columns=price_columns,
                                                volume_column=volume_column)
        self._logger.info(
            f"Adjusted bundle bundle_name={data_bundle.name}, bundle_version={data_bundle.version} "
            f"with {len(splits)} splits and {len(dividends)} dividends."
        )
        return adjusted_data

    async def _save_adjusted_bundle(
            self,
            source_data_bundle: DataBundle,
            adjusted_data: pl.DataFrame,
            adjusted_bundle_name: str,
            merge: bool,
            bundle_service: BundleService,
            bundle_storage: BundleStorage,
    ) -> None:
        """Persists the adjusted data as a new bundle under ``adjusted_bundle_name``.

        Mirrors the save flow used in ``ingest_market_data`` using the source bundle's
        storage implementation.
        """
        calendar = source_data_bundle.trading_calendar

        if merge:
            existing_bundle_metadata = await bundle_service.load_bundle_metadata(
                bundle_name=adjusted_bundle_name,
                bundle_version=None,
            )
            if not existing_bundle_metadata:
                bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))
            else:
                bundle_version = existing_bundle_metadata["version"]
        else:
            bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))

        adjusted_bundle = DataBundle(name=adjusted_bundle_name,
                                     version=bundle_version,
                                     start_date=source_data_bundle.start_date,
                                     end_date=source_data_bundle.end_date,
                                     trading_calendar=source_data_bundle.trading_calendar,
                                     frequency=source_data_bundle.frequency,
                                     original_frequency=source_data_bundle.original_frequency,
                                     data_type=source_data_bundle.data_type,
                                     timestamp=datetime.datetime.now(tz=calendar.tz),
                                     data=adjusted_data)

        await bundle_service.register_bundle(
            data_bundle=adjusted_bundle,
            bundle_storage=bundle_storage,
            merge=merge,
        )
        await bundle_service.store_bundle(
            data_bundle=adjusted_bundle,
            bundle_storage=bundle_storage,
            merge=merge,
            merge_columns=["sid", "date"],
        )

        self._logger.info(f"Saved adjusted bundle: name={adjusted_bundle_name}, bundle_version={bundle_version}")

    @staticmethod
    def _build_events_frame(splits: list[Split], dividends: list[DividendPayout],
                            sids_by_asset_id: dict[int, list[int]]) -> pl.DataFrame:
        """Builds a normalized frame of corporate-action events, exactly one row per (sid, event_date).

        Each row carries a ``split_factor`` (product of ``ratio`` over all splits on that
        date, ``1.0`` if none) and a ``dividend`` amount (sum of cash dividends on that date,
        ``0.0`` if none). A split and a dividend on the same date for the same sid are merged
        into a single row.
        """
        schema = {
            "sid": pl.Int64,
            "event_date": pl.Date,
            "split_factor": pl.Float64,
            "dividend": pl.Float64,
        }
        split_rows = [
            {"sid": sid, "event_date": split.effective_date, "split_factor": split.ratio, "dividend": 0.0}
            for split in splits
            for sid in sids_by_asset_id.get(split.asset.id, [])
        ]
        dividend_rows = [
            {"sid": sid, "event_date": dividend.ex_date, "split_factor": 1.0, "dividend": dividend.amount}
            for dividend in dividends
            for sid in sids_by_asset_id.get(dividend.asset.id, [])
        ]
        if not split_rows and not dividend_rows:
            return pl.DataFrame(schema=schema)

        return (
            pl.DataFrame(split_rows + dividend_rows, schema=schema)
            .group_by(["sid", "event_date"], maintain_order=True)
            .agg(
                pl.col("split_factor").product().alias("split_factor"),
                pl.col("dividend").sum().alias("dividend"),
            )
            .sort(["sid", "event_date"])
        )

    def _attach_dividend_factors(self, data: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
        """
        Attaches a TradingView-style proportional ``dividend_factor`` to each event row.

        For a dividend of amount ``D`` with ex-date ``E``, the factor is
        ``(Pclose - D) / Pclose`` where ``Pclose`` is the close of the last trading bar
        strictly before ``E`` (the ex-date is shifted back by one day and matched with a
        backward asof join, so a non-trading ex-date resolves to the previous trading day).
        Bars strictly before ``E`` are later multiplied by the cumulative product of these
        factors; bars on or after ``E`` are untouched, so the latest price of each asset
        stays unchanged.

        Events without a dividend (splits) get a neutral factor of ``1.0``. If there is no
        positive close strictly before the ex-date, the factor falls back to ``1.0`` and a
        warning is logged.
        """
        if "close" not in data.columns:
            self._logger.warning("Bundle data has no 'close' column; dividends are left unadjusted (factor=1.0).")
            return events.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("dividend_factor"))

        dividend_events = events.filter(pl.col("dividend") > 0)
        if dividend_events.is_empty():
            return events.with_columns(pl.lit(1.0, dtype=pl.Float64).alias("dividend_factor"))

        # Pclose is the close of the last bar strictly before the ex-date: shifting the
        # ex-date back by one day lets the backward asof match that previous trading day.
        sorted_data = data.select(["sid", "date", "close"]).sort(["sid", "date"])
        p_close = (
            dividend_events
            .with_columns((pl.col("event_date") - pl.duration(days=1)).alias("lookup_date"))
            .sort(["sid", "lookup_date"])
            .join_asof(sorted_data,
                       left_on="lookup_date", right_on="date",
                       by="sid", strategy="backward")
        )

        invalid_mask = p_close.get_column("close").is_null() | (p_close.get_column("close") <= 0)
        missing_count = invalid_mask.sum()
        if missing_count > 0:
            self._logger.warning(
                f"{missing_count} dividend event(s) have no positive close strictly before their "
                f"ex-date and are left unadjusted (dividend_factor=1.0)."
            )

        dividend_factors = (
            p_close
            .with_columns(
                pl.when(~invalid_mask)
                .then((pl.col("close") - pl.col("dividend")) / pl.col("close"))
                .otherwise(1.0)
                .alias("dividend_factor")
            )
            .select(["sid", "event_date", "dividend_factor"])
        )

        return (
            events
            .join(dividend_factors, on=["sid", "event_date"], how="left")
            .with_columns(pl.col("dividend_factor").fill_null(1.0))
        )

    @staticmethod
    def _apply_adjustments(data: pl.DataFrame, events: pl.DataFrame,
                           price_columns: list[str], volume_column: str | None) -> pl.DataFrame:
        """
        Applies backward proportional adjustments and appends ``*_adjusted`` columns to the data.

        Every event carries a multiplicative factor (``split_factor * dividend_factor``).
        For each bar, the applied factor is the product of the factors of all events
        strictly after the bar date, so the latest bar of each asset stays unchanged:

        - splits: factor ``ratio``; the bar's volume is scaled by the product of the
          inverse split ratios (``volume / split_factor``);
        - dividends: TradingView-style factor ``(Pclose - D) / Pclose`` (see
          :meth:`_attach_dividend_factors`).

        Result: ``adjusted_price = price * split_factor * dividend_factor``.
        """
        if events.is_empty():
            adjusted_columns = [pl.col(column).alias(f"{column}_adjusted") for column in price_columns]
            if volume_column is not None:
                adjusted_columns.append(pl.col(volume_column).alias(f"{volume_column}_adjusted"))
            adjusted_columns.append(pl.lit(False).alias("adjusted"))
            return data.with_columns(adjusted_columns)

        # For each event, compute the product of the factors of all events that come
        # strictly after it (per sid).
        events_with_cumulative = events.with_columns(
            (pl.col("split_factor") * pl.col("dividend_factor")).alias("factor"),
        ).with_columns(
            pl.col("split_factor").cum_prod().over("sid").alias("split_cumulative"),
            pl.col("factor").cum_prod().over("sid").alias("factor_cumulative"),
        )
        totals = (
            events_with_cumulative
            .group_by("sid", maintain_order=True)
            .agg(
                pl.col("split_cumulative").last().alias("split_total"),
                pl.col("factor_cumulative").last().alias("factor_total"),
            )
        )
        events = (
            events_with_cumulative
            .join(totals, on="sid")
            .with_columns(
                (pl.col("split_total") / pl.col("split_cumulative")).alias("split_factor_after"),
                (pl.col("factor_total") / pl.col("factor_cumulative")).alias("factor_after"),
            )
            .rename({"event_date": "date"})
            .select("sid", "date", "split_factor_after", "factor_after")
        )

        # Match each bar with the last event at or before its date; the factors of the
        # events strictly after the bar date then drive the adjustment. Bars before the
        # first event of their sid are adjusted by all events of that sid, and sids
        # without any event are left unchanged (neutral factors).
        data = (
            data
            .sort(["sid", "date"])
            .join(totals, on="sid", how="left")
            .join_asof(events, on="date", by="sid", strategy="backward")
            .with_columns(
                pl.col("split_factor_after").fill_null(pl.col("split_total")).fill_null(1.0).alias("split_factor"),
                pl.col("factor_after").fill_null(pl.col("factor_total")).fill_null(1.0).alias("factor"),
            )
        )

        adjusted_columns = [
            (pl.col(column) * pl.col("factor")).alias(f"{column}_adjusted")
            for column in price_columns
        ]
        if volume_column is not None:
            adjusted_columns.append(
                (pl.col(volume_column) / pl.col("split_factor")).alias(f"{volume_column}_adjusted")
            )
        return (
            data
            .with_columns(adjusted_columns)
            .with_columns(
                (
                    (pl.col("factor") != 1.0)
                    | (pl.col("split_factor") != 1.0)
                ).alias("adjusted")
            )
            .drop(["split_factor_after", "factor_after", "split_factor", "factor", "split_total", "factor_total"])
        )
