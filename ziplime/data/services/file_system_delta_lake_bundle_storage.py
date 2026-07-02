import datetime

import aiofiles.os
from pathlib import Path
from typing import Any, Sequence, Literal, Self

import structlog
from polars import CredentialProviderFunction, Expr
from polars._typing import ParquetCompression
import polars as pl

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bundle_storage import BundleStorage


class FileSystemDeltaLakeBundleStorage(BundleStorage):

    def __init__(self,
                 base_data_path: str,
                 compression: ParquetCompression = "brotli",
                 compression_level: int | None = None,
                 statistics: bool | str | dict[str, bool] = True,
                 row_group_size: int | None = None,
                 data_page_size: int | None = None,
                 use_pyarrow: bool = False,
                 pyarrow_options: dict[str, Any] | None = None,
                 partition_by: str | Sequence[str] | None = None,
                 partition_chunk_size_bytes: int = 4_294_967_296,
                 storage_options: dict[str, Any] | None = None,
                 credential_provider: (
                         CredentialProviderFunction | Literal["auto"] | None
                 ) = "auto", ):
        super().__init__()
        self.base_data_path = base_data_path
        self.compression = compression
        self.compression_level = compression_level
        self.statistics = statistics
        self.row_group_size = row_group_size
        self.data_page_size = data_page_size
        self.use_pyarrow = use_pyarrow
        self.pyarrow_options = pyarrow_options
        self.partition_by = partition_by
        self.partition_chunk_size_bytes = partition_chunk_size_bytes
        self.storage_options = storage_options
        self._logger = structlog.get_logger(__name__)

    def report_duplicates(self, df: pl.DataFrame, merge_columns: list[str], label: str) -> None:
        total = df.height
        unique = df.unique(subset=merge_columns).height
        dupes = total - unique

        print(f"[{label}] rows={total}, unique_on{merge_columns}={unique}, duplicate_rows={dupes}")

        if dupes > 0:
            # Show the exact key combinations that repeat, with their counts
            offending = (
                df.group_by(merge_columns)
                .agg(pl.len().alias("count"))
                .filter(pl.col("count") > 1)
                .sort("count", descending=True)
            )
            print(f"[{label}] duplicate keys:")
            print(offending)

    async def store_bundle(self, data_bundle: DataBundle, merge: bool, merge_columns: list[str] | None = None):
        # we need here to know where to store bundle, and info is in bundle metadata
        bundle_path = self.get_data_bundle_path(data_bundle=data_bundle)
        await aiofiles.os.makedirs(bundle_path.parent, exist_ok=True)
        if not merge:
            data_bundle.data.write_delta(target=bundle_path)
            return
        if not merge_columns:
            raise ValueError("merge_columns must be provided when merge=True.")
        predicate_string = " AND ".join([f"source.{k} = target.{k}" for k in merge_columns])
        # If there is no existing bundle
        if not await aiofiles.os.path.exists(bundle_path):
            data_bundle.data.write_delta(target=bundle_path)
            return
        data_bundle.data.write_delta(
            target=bundle_path,
            mode="merge",
            delta_merge_options={
                "predicate": predicate_string,
                "source_alias": "source",
                "target_alias": "target",
            }
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def _deduplicate_merge_rows(self, data: pl.DataFrame, merge_columns: list[str]) -> pl.DataFrame:
        deduplicated = data.unique(subset=merge_columns, keep="last")
        if deduplicated.height != data.height:
            self._logger.warning(
                "Dropping duplicate rows before Delta merge",
                merge_columns=merge_columns,
                dropped_rows=data.height - deduplicated.height,
                original_rows=data.height,
                remaining_rows=deduplicated.height,
            )
        return deduplicated

    async def load_data_bundle(self, data_bundle: DataBundle,
                               assets: list[ExchangeAsset] | None = None,
                               start_date: datetime.datetime | None = None,
                               end_date: datetime.datetime | None = None,
                               frequency: datetime.timedelta | Period | None = None,
                               start_auction_delta: datetime.timedelta = None,
                               end_auction_delta: datetime.timedelta = None,
                               aggregations: list[pl.Expr] = None
                               ) -> pl.DataFrame:
        self._logger.info(
            f"Loading data bundle {data_bundle.name} start_date={start_date}, end_date={end_date},"
            f" version={data_bundle.version}, frequency={frequency}")
        bundle_path = self.get_data_bundle_path(data_bundle=data_bundle)
        filters = []
        pl_parquet = pl.scan_delta(bundle_path)
        tz_name = str(data_bundle.trading_calendar.tz)
        pl_parquet = pl_parquet.with_columns(
            pl.col("date").dt.convert_time_zone(tz_name)
        )
        if assets is not None:
            # TODO: update to require AssetSymbol instead of str
            filters.append(pl.col("sid").is_in([asset.sid for asset in assets]))
        if start_date is not None:
            filters.append(pl.col("date") >= start_date.date())
        if end_date is not None:
            filters.append(pl.col("date") <= end_date.date())

        if filters:
            pl_parquet = pl_parquet.filter(*filters)
        pl_parquet = pl_parquet.sort(["sid", "date"])

        if frequency is not None:

            if data_bundle.aggregation_specification:
                pl_parquet = pl_parquet.group_by_dynamic(
                    index_column="date", every=frequency, by="sid").agg(
                    pl.col(field).last() for field in pl_parquet.collect_schema().names() if
                    field not in ('sid', 'date')
                )
            else:
                if start_auction_delta is not None:
                    pl_parquet = pl_parquet.filter(
                        pl.col("date") >= (
                                pl.col("date").min().over(pl.col("date").dt.date()) + pl.duration(
                            seconds=start_auction_delta.total_seconds())
                        )
                    )
                if end_auction_delta is not None:
                    pl_parquet = pl_parquet.filter(pl.col("date") <= (
                            pl.col("date").max().over(pl.col("date").dt.date()) - pl.duration(
                        seconds=end_auction_delta.total_seconds())
                    ))

                if not aggregations and data_bundle.data_type == DataType.MARKET_DATA:
                    aggregations = [
                        pl.col("open").first(),
                        pl.col("high").max(),
                        pl.col("low").min(),
                        pl.col("volume").sum()
                    ]
                if aggregations:
                    aggregation_columns = {aggregation.meta.output_name() for aggregation in aggregations}
                    # default aggregation is last()
                    missing_aggregation_columns = [
                        pl.col(col).last() for col in pl_parquet.collect_schema().names()
                        if col not in aggregation_columns and col not in ('sid', 'date')
                    ]
                    pl_parquet = pl_parquet.group_by_dynamic(
                        index_column="date", every=frequency, by="sid").agg(*aggregations, *missing_aggregation_columns)
                else:
                    pl_parquet = pl_parquet.group_by_dynamic(
                        index_column="date", every=frequency, by="sid").agg(
                        pl.col(field).last() for field in pl_parquet.collect_schema().names() if
                        field not in ('sid', 'date')
                    )
        return pl_parquet.collect()

    @classmethod
    async def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(base_data_path=data["base_data_path"])

    def get_data_bundle_path(self, data_bundle: DataBundle) -> Path:
        return Path(self.base_data_path, "data_bundle", data_bundle.name, data_bundle.version, f"data.delta")

    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]:
        return {
            "base_data_path": self.base_data_path,
        }
