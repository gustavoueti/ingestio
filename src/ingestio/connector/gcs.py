"""Connectors for Google Cloud Storage.

`GcsReader` and `GcsWriter` read and write Spark DataFrames as blobs under a prefix, in the
format they are given, with the contract of the disk connectors. google-cloud-storage only
lists the blobs, on the driver; Spark loads exactly the ones selected and writes straight to the
URI. pyspark and google-cloud-storage are imported lazily, so this module imports without them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ingestio.connector._files import (
    RESERVED_BOTH,
    RESERVED_READER,
    RESERVED_WRITER,
    Options,
    Watermark,
    check_format,
    check_options,
    check_readable_format,
    is_hidden,
    units_since_epoch,
)

if TYPE_CHECKING:
    from datetime import datetime

    from google.cloud.storage import Client
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["GcsReader", "GcsWriter"]

_MILLISECOND = 1_000


def _check_path(path: str, *, prefix_required: bool = False) -> tuple[str, str, str]:
    """Return the normalised path, the bucket and the prefix."""
    if not isinstance(path, str) or not path.startswith("gs://"):
        raise ValueError("path must be a gs:// URI")
    bucket, _, prefix = path.removeprefix("gs://").partition("/")
    if not bucket:
        raise ValueError("path must name a bucket")
    prefix = prefix.rstrip("/")
    if prefix and any(segment in ("", ".", "..") for segment in prefix.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    if prefix_required and not prefix:
        raise ValueError("path must name a prefix inside the bucket")
    normalised = f"gs://{bucket}" + (f"/{prefix}" if prefix else "")
    return normalised, bucket, prefix


class GcsReader:
    """Reads the blobs under a GCS prefix into a Spark DataFrame, in the format it is given.

    Args:
        path: Prefix that holds the blobs, as a URI: `gs://<bucket>/<prefix>`. The
            configuration key of the same name.
        format: Name of the Spark data source the blobs are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a blob matches, with
            one group that captures the blob's watermark. The name is the part of the blob's
            full name after the prefix. Without it, a blob's watermark is its creation time,
            `timeCreated`, to the millisecond. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only blobs above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the blobs, as a `StructType` or a DDL string. Spark infers it
            when omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: google-cloud-storage client that lists the blobs, such as
            `storage.Client.from_service_account_info(info)`. When omitted, each read
            creates `storage.Client(project=None)`, with Application Default Credentials,
            which needs the `gcs` extra.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        path: str,
        format: str,  # noqa: A002 - the configuration key
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Options | None = None,
        client: Client | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._path, self._bucket, prefix = _check_path(path)
        self._listing_prefix = f"{prefix}/" if prefix else ""
        check_readable_format(format)
        self._mark = Watermark.parse(watermark_pattern, watermark_value, _MILLISECOND)
        self._options = check_options(options, RESERVED_BOTH + RESERVED_READER)
        if client is None:
            try:
                from google.cloud.storage import Client  # noqa: F401
            except ImportError as exc:
                raise ImportError(
                    "GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'"
                ) from exc
        self._format = format
        self._schema = schema
        self._client = client
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the blobs the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        client = self._client
        if client is None:
            from google.cloud.storage import Client

            client = Client(project=None)
        names = self._select(client)
        if not names:
            from pyspark.sql.types import StructType

            return spark.createDataFrame(
                [], StructType([]) if self._schema is None else self._schema
            )
        reader = spark.read.format(self._format)
        if self._schema is not None:
            reader = reader.schema(self._schema)
        return reader.options(**self._options).load([f"{self._path}/{name}" for name in names])

    def _select(self, client: Client) -> list[str]:
        candidates: list[tuple[str, int]] = []
        blobs = client.list_blobs(self._bucket, prefix=self._listing_prefix, delimiter="/")
        for blob in blobs:
            name = blob.name.removeprefix(self._listing_prefix)
            if not name or "/" in name or is_hidden(name):
                continue
            time = 0
            if self._mark.needs_time:
                time = units_since_epoch(blob.time_created, _MILLISECOND)
            candidates.append((name, time))
        return self._mark.select(candidates)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"


class GcsWriter:
    """Writes a Spark DataFrame as blobs under a GCS prefix, in the format it is given.

    Args:
        path: Prefix to write under, as a URI: `gs://<bucket>/<prefix>`.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        path: str,
        format: str,  # noqa: A002 - the configuration key
        *,
        options: Options | None = None,
    ) -> None:
        self._path, _, _ = _check_path(path, prefix_required=True)
        check_format(format)
        self._format = format
        self._options = check_options(options, RESERVED_BOTH + RESERVED_WRITER)

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` as blobs under the prefix.

        Args:
            data: The rows to write.
            replace: Remove every blob under the prefix first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
        mode = "overwrite" if replace else "append"
        data.write.format(self._format).options(**self._options).mode(mode).save(self._path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"
