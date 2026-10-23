"""Connectors for Amazon S3 and S3-compatible stores.

`S3Reader` and `S3Writer` read and write Spark DataFrames as objects under a prefix, in the
format they are given, with the contract of the disk connectors. boto3 only lists the objects,
on the driver; Spark loads exactly the ones selected and writes straight to the URI. pyspark
and boto3 are imported lazily, so this module imports without them.
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

    from mypy_boto3_s3.client import S3Client
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["S3Reader", "S3Writer"]

_SECOND = 1_000_000


def _check_path(path: str, *, prefix_required: bool = False) -> tuple[str, str, str]:
    """Return the normalised path, the bucket and the prefix."""
    if not isinstance(path, str) or not path.startswith(("s3://", "s3a://")):
        raise ValueError("path must be an s3:// or s3a:// URI")
    scheme, _, rest = path.partition("://")
    bucket, _, prefix = rest.partition("/")
    if not bucket:
        raise ValueError("path must name a bucket")
    prefix = prefix.rstrip("/")
    if prefix and any(segment in ("", ".", "..") for segment in prefix.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    if prefix_required and not prefix:
        raise ValueError("path must name a prefix inside the bucket")
    normalised = f"{scheme}://{bucket}" + (f"/{prefix}" if prefix else "")
    return normalised, bucket, prefix


class S3Reader:
    """Reads the objects under an S3 prefix into a Spark DataFrame, in the format it is given.

    Args:
        path: Prefix that holds the objects, as a URI: `s3://<bucket>/<prefix>` or
            `s3a://<bucket>/<prefix>`. Spark receives the scheme as written. The
            configuration key of the same name.
        format: Name of the Spark data source the objects are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of an object matches, with
            one group that captures the object's watermark. The name is the part of the key
            after the prefix. Without it, an object's watermark is its `LastModified` time, to
            the second. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only objects above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the objects, as a `StructType` or a DDL string. Spark infers it
            when omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: boto3 S3 client that lists the objects, such as
            `boto3.client("s3", region_name=..., endpoint_url=...)`. When omitted, each read
            creates `boto3.client("s3")`, which needs the `s3` extra.
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
        client: S3Client | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._path, self._bucket, prefix = _check_path(path)
        self._listing_prefix = f"{prefix}/" if prefix else ""
        check_readable_format(format)
        self._mark = Watermark.parse(watermark_pattern, watermark_value, _SECOND)
        self._options = check_options(options, RESERVED_BOTH + RESERVED_READER)
        if client is None:
            try:
                import boto3  # noqa: F401
            except ImportError as exc:
                raise ImportError("S3Reader needs boto3: pip install 'ingestio[s3]'") from exc
        self._format = format
        self._schema = schema
        self._client = client
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the objects the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        client = self._client
        if client is None:
            import boto3

            client = boto3.client("s3")
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

    def _select(self, client: S3Client) -> list[str]:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=self._listing_prefix, Delimiter="/"
        )
        candidates: list[tuple[str, int]] = []
        for page in pages:
            for entry in page.get("Contents", []):
                name = entry["Key"].removeprefix(self._listing_prefix)
                if not name or "/" in name or is_hidden(name):
                    continue
                time = 0
                if self._mark.needs_time:
                    time = units_since_epoch(entry["LastModified"], _SECOND)
                candidates.append((name, time))
        return self._mark.select(candidates)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"


class S3Writer:
    """Writes a Spark DataFrame as objects under an S3 prefix, in the format it is given.

    Args:
        path: Prefix to write under, as a URI: `s3://<bucket>/<prefix>` or
            `s3a://<bucket>/<prefix>`. Spark receives the scheme as written.
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
        """Write every row of `data` as objects under the prefix.

        Args:
            data: The rows to write.
            replace: Remove every object under the prefix first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
        mode = "overwrite" if replace else "append"
        data.write.format(self._format).options(**self._options).mode(mode).save(self._path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"
