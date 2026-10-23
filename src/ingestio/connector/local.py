"""Connectors for local volumes.

A Databricks volume is reachable as an ordinary path, so the same connectors serve a local
disk and a cluster. `JsonLinesWriter` writes records as JSON Lines. `DiskReader` and
`DiskWriter` read and write Spark DataFrames as files in a directory, in the format they are
given. pyspark is imported lazily, so this module imports without it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
    from os import PathLike

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

__all__ = ["DiskReader", "DiskWriter", "JsonLinesWriter"]

_MILLISECOND = 1000


class JsonLinesWriter:
    """Writes records as newline-delimited JSON, one object per line.

    Args:
        path: Destination file. Parent directories are created when missing.
        append: Append to the file instead of replacing it.
        encoding: Text encoding of the output file.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        append: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        self.path = Path(path)
        self.append = append
        self.encoding = encoding

    def write(self, data: Iterable[Mapping[str, Any]]) -> None:
        """Write every record as one JSON line."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self.append else "w"
        with self.path.open(mode, encoding=self.encoding) as handle:
            for record in data:
                # default=str keeps datetimes and similar values writable.
                handle.write(json.dumps(record, ensure_ascii=False, default=str))
                handle.write("\n")

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={str(self.path)!r})"


def _check_path(path: str | PathLike[str]) -> str:
    normalised = Path(path)
    if not normalised.is_absolute():
        raise ValueError("path must be an absolute local path")
    return str(normalised)


class DiskReader:
    """Reads the files of a directory into a Spark DataFrame, in the format it is given.

    Args:
        path: Directory that holds the files: an absolute local path, such as a Databricks
            volume, `/Volumes/<catalog>/<schema>/<volume>/<dir>`. The configuration key of
            the same name.
        format: Name of the Spark data source the files are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a file matches, with one
            group that captures the file's watermark. Without it, a file's watermark is its
            last-modified time. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only files above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the files, as a `StructType` or a DDL string. Spark infers it when
            omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        format: str,  # noqa: A002 - the configuration key
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Options | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._path = _check_path(path)
        check_readable_format(format)
        self._mark = Watermark.parse(watermark_pattern, watermark_value, _MILLISECOND)
        self._options = check_options(options, RESERVED_BOTH + RESERVED_READER)
        self._format = format
        self._schema = schema
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the files the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        names = self._select()
        if not names:
            from pyspark.sql.types import StructType

            return spark.createDataFrame(
                [], StructType([]) if self._schema is None else self._schema
            )
        reader = spark.read.format(self._format)
        if self._schema is not None:
            reader = reader.schema(self._schema)
        directory = Path(self._path)
        return reader.options(**self._options).load([str(directory / name) for name in names])

    def _select(self) -> list[str]:
        with os.scandir(self._path) as entries:
            candidates = [
                (entry.name, entry.stat().st_mtime_ns // 1_000_000 if self._mark.needs_time else 0)
                for entry in entries
                if not is_hidden(entry.name) and entry.is_file()
            ]
        return self._mark.select(candidates)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"


class DiskWriter:
    """Writes a Spark DataFrame as files in a directory, in the format it is given.

    Args:
        path: Directory to write into: an absolute local path, such as a Databricks volume.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        format: str,  # noqa: A002 - the configuration key
        *,
        options: Options | None = None,
    ) -> None:
        self._path = _check_path(path)
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
        data.write.format(self._format).options(**self._options).mode(mode).save(self._path)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(path={self._path!r}, format={self._format!r})"
