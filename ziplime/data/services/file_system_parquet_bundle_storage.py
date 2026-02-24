import datetime

import aiofiles.os
from pathlib import Path
from typing import Any, Sequence, Literal, Self

import structlog
from polars import CredentialProviderFunction, Expr
from polars._typing import ParquetCompression
import polars as pl

from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bundle_storage import BundleStorage


class FileSystemParquetBundleStorage(BundleStorage):

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

    async def store_bundle(self, data_bundle: DataBundle):
        # we need here to know ehere to store bundle, and info is in bundle metadata
        bundle_path = self.get_data_bundle_path(data_bundle=data_bundle)
        await aiofiles.os.makedirs(bundle_path.parent, exist_ok=True)
        data_bundle.data.write_parquet(bundle_path, compression=self.compression,
                                       compression_level=10,
                                       statistics=self.statistics, row_group_size=self.row_group_size,
                                       data_page_size=self.data_page_size, use_pyarrow=self.use_pyarrow,
                                       pyarrow_options=self.pyarrow_options, partition_by=self.partition_by,
                                       partition_chunk_size_bytes=self.partition_chunk_size_bytes,
                                       storage_options=self.storage_options)

    async def load_data_bundle(self, data_bundle: DataBundle,
                               symbols: list[str] | None = None,
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
        pl_parquet = pl.scan_parquet(bundle_path)
        if symbols is not None:
            filters.append(pl.col("symbol").is_in(symbols))
        if start_date is not None:
            filters.append(pl.col("date") >= start_date)
        if end_date is not None:
            filters.append(pl.col("date") <= end_date)

        if filters:
            pl_parquet = pl_parquet.filter(*filters)

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
        return pl_parquet.sort(["sid", "date"]).collect()

    @classmethod
    async def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(base_data_path=data["base_data_path"])

    def get_data_bundle_path(self, data_bundle: DataBundle) -> Path:
        return Path(self.base_data_path, "data_bundle", data_bundle.name, data_bundle.version, f"data.parquet")

    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]:
        return {
            "base_data_path": self.base_data_path,
        }
