"""Connectors for files in a Databricks Unity Catalog volume.

`VolumeReader` and `VolumeWriter` read and write Spark DataFrames as files in a directory of a
volume, in the format they are given, with the contract of the disk connectors. The Databricks
SDK only lists the directory, on the driver; Spark loads exactly the files selected and writes
straight to the `/Volumes/...` path. pyspark and databricks-sdk are imported lazily, so this
module imports without them.
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
)

if TYPE_CHECKING:
    from datetime import datetime

    from databricks.sdk import WorkspaceClient
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["VolumeReader", "VolumeWriter"]

_SECOND = 1_000_000


def _check_volume(volume: str) -> str:
    """Return the volume's root, `/Volumes/<catalog>/<schema>/<volume>`."""
    parts = volume.split(".") if isinstance(volume, str) else []
    if len(parts) != 3 or any(not part or "/" in part for part in parts):
        raise ValueError("volume must be 'catalog.schema.volume'")
    return "/Volumes/" + "/".join(parts)


def _check_path(path: str) -> str:
    """Return the normalised path inside the volume."""
    if not isinstance(path, str) or path.startswith("/"):
        raise ValueError("path must be a str relative to the volume")
    path = path.rstrip("/")
    if path and any(segment in ("", ".", "..") for segment in path.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    return path


def _check_location(volume: str, path: str, *, directory_required: bool = False) -> tuple[str, str]:
    """Return the normalised path and the location."""
    root = _check_volume(volume)
    path = _check_path(path)
    if directory_required and not path:
        raise ValueError("path must name a directory inside the volume")
    return path, root + (f"/{path}" if path else "")


class VolumeReader:
    """Reads the files of a directory in a Unity Catalog volume into a Spark DataFrame.

    Args:
        volume: The volume, by its three-part name, `catalog.schema.volume`. The
            configuration key of the same name.
        path: Directory that holds the files, relative to the volume, such as
            `landing/orders`, or `""` for the volume's root. The configuration key of the
            same name.
        format: Name of the Spark data source the files are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a file matches, with one
            group that captures the file's watermark. Without it, a file's watermark is its
            `last_modified` time, to the second. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only files above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the files, as a `StructType` or a DDL string. Spark infers it when
            omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: Databricks SDK client that lists the directory, such as
            `WorkspaceClient(profile="prod")`. When omitted, each read creates
            `WorkspaceClient()`, with the SDK's default authentication, which needs the
            `databricks` extra.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        volume: str,
        path: str,
        format: str,  # noqa: A002 - the configuration key
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Options | None = None,
        client: WorkspaceClient | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._volume = volume
        self._path, self._location = _check_location(volume, path)
        check_readable_format(format)
        self._mark = Watermark.parse(watermark_pattern, watermark_value, _SECOND)
        self._options = check_options(options, RESERVED_BOTH + RESERVED_READER)
        if client is None:
            try:
                import databricks.sdk  # noqa: F401
            except ImportError as exc:
                raise ImportError(
                    "VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'"
                ) from exc
        self._format = format
        self._schema = schema
        self._client = client
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the files the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        client = self._client
        if client is None:
            import databricks.sdk

            client = databricks.sdk.WorkspaceClient()
        names = self._select(client)
        if not names:
            from pyspark.sql.types import StructType

            return spark.createDataFrame(
                [], StructType([]) if self._schema is None else self._schema
            )
        reader = spark.read.format(self._format)
        if self._schema is not None:
            reader = reader.schema(self._schema)
        return reader.options(**self._options).load([f"{self._location}/{name}" for name in names])

    def _select(self, client: WorkspaceClient) -> list[str]:
        candidates: list[tuple[str, int]] = []
        for entry in client.files.list_directory_contents(directory_path=self._location):
            if entry.is_directory is True:
                continue
            name = entry.name
            if not name:
                raise ValueError("the Files API listed an entry without a name")
            if is_hidden(name):
                continue
            time = 0
            if self._mark.needs_time:
                if entry.last_modified is None:
                    raise ValueError(f"the Files API gave no last_modified for '{name}'")
                # Milliseconds since the epoch, truncated to whole seconds.
                time = entry.last_modified // 1000
            candidates.append((name, time))
        return self._mark.select(candidates)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(volume={self._volume!r}, path={self._path!r}, "
            f"format={self._format!r})"
        )


class VolumeWriter:
    """Writes a Spark DataFrame as files in a directory of a Unity Catalog volume.

    Args:
        volume: The volume, by its three-part name, `catalog.schema.volume`.
        path: Directory to write into, relative to the volume, such as `curated/orders`.
            Never the volume's root.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        volume: str,
        path: str,
        format: str,  # noqa: A002 - the configuration key
        *,
        options: Options | None = None,
    ) -> None:
        self._volume = volume
        self._path, self._location = _check_location(volume, path, directory_required=True)
        check_format(format)
        self._format = format
        self._options = check_options(options, RESERVED_BOTH + RESERVED_WRITER)

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` as files in the directory.

        Args:
            data: The rows to write.
            replace: Remove what the directory holds first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
        mode = "overwrite" if replace else "append"
        data.write.format(self._format).options(**self._options).mode(mode).save(self._location)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(volume={self._volume!r}, path={self._path!r}, "
            f"format={self._format!r})"
        )
