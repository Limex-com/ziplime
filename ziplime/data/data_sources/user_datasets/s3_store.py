"""S3 store for user_datasets."""

import os
import polars as pl
from ziplime.data.data_sources.user_datasets.user_dataset_error import UserDatasetError


class S3Store:
    """Reads ``s3://bucket/key`` with the credentials the process already has."""

    def __init__(self, region: str | None = None) -> None:
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
