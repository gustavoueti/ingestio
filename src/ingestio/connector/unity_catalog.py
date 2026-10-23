"""Unity Catalog connectors for Delta tables named `catalog.schema.table`.

`UnityCatalogReader` returns a Spark DataFrame and filters in the table's scan.
`UnityCatalogWriter` appends a DataFrame to a table, or replaces the table's rows with it, in one
Delta commit, and creates the table when it does not exist. pyspark is imported lazily, so this
module imports without it.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["UnityCatalogReader", "UnityCatalogWriter"]

_READER_RESERVED = ("path", "readchangefeed", "readchangedata")
_WRITER_RESERVED = ("path", "replacewhere", "partitionoverwritemode")

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date


def _split_table(table_name: str) -> tuple[str, ...]:
    parts = tuple(table_name.split("."))
    if len(parts) != 3 or not all(parts):
        raise ValueError("table_name must be 'catalog.schema.table'")
    return parts


def _quote(name: str) -> str:
    """Quote an identifier: wrap it in backticks and double those inside."""
    return "`" + name.replace("`", "``") + "`"


def _quote_table(parts: tuple[str, ...]) -> str:
    return ".".join(_quote(part) for part in parts)


def _string_literal(text: str) -> str:
    """A Spark SQL string literal; assumes `spark.sql.parser.escapedStringLiterals` is false."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _check_options(
    options: _Options | None, reserved: tuple[str, ...]
) -> dict[str, str | int | float | bool]:
    given: dict[str, str | int | float | bool] = dict(options or {})
    for key in given:
        if key.lower() in reserved:
            raise ValueError(f"option '{key}' is set by the connector")
    return given


def _watermark_literal(value: _Watermark) -> str:
    """Validate a watermark value and turn it into a Spark SQL literal."""
    if isinstance(value, bool):
        raise ValueError("unsupported watermark_value type: bool")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("watermark_value must be finite")
        return format(value, "f") + "BD"
    if isinstance(value, str):
        return _string_literal(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watermark_value must be timezone-aware")
        return _string_literal(value.astimezone(UTC).isoformat(sep=" "))
    if isinstance(value, date):
        return _string_literal(value.isoformat())
    raise ValueError(f"unsupported watermark_value type: {type(value).__name__}")


class UnityCatalogReader:
    """Reads a catalog table into a Spark DataFrame, filtering in the table's scan.

    Args:
        table_name: Source table, `catalog.schema.table`. The configuration key of the same
            name.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark read options passed through, such as Delta's `versionAsOf`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._table = _quote_table(_split_table(table_name))
        if watermark_column == "":
            raise ValueError("watermark_column must not be empty")
        if watermark_value is not None and watermark_column is None:
            raise ValueError("watermark_value needs watermark_column")
        self._value = None if watermark_value is None else _watermark_literal(watermark_value)
        self._options = _check_options(options, _READER_RESERVED)
        self._table_name = table_name
        self._column = watermark_column
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        source = spark.read.options(**self._options).table(self._table)
        if self._column is None:
            return source

        quoted = _quote(self._column)
        lower = f"{quoted} > {self._value}" if self._value is not None else None
        bounded = source if lower is None else source.where(lower)
        row = bounded.selectExpr(
            f"CAST(max({quoted}) AS STRING) AS upper_bound",
            f"typeof(max({quoted})) AS upper_bound_type",
        ).first()
        # An aggregate without GROUP BY always returns one row; the guard is for the type checker.
        upper, upper_type = (None, None) if row is None else (row[0], row[1])

        if upper is None:
            predicate = "false"
        else:
            upper_predicate = f"{quoted} <= CAST({_string_literal(str(upper))} AS {upper_type})"
            predicate = upper_predicate if lower is None else f"{lower} AND {upper_predicate}"
        return source.where(predicate)

    def __repr__(self) -> str:
        return f"UnityCatalogReader(table_name={self._table_name!r})"


class UnityCatalogWriter:
    """Writes a Spark DataFrame into a catalog table, in one Delta commit.

    Args:
        table_name: Table to write into, `catalog.schema.table`. Created, as a managed Delta
            table with the DataFrame's columns, when it does not exist.
        options: Delta write options passed through, such as `mergeSchema` or
            `userMetadata`.
    """

    def __init__(
        self,
        table_name: str,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None:
        self._table = _quote_table(_split_table(table_name))
        self._options = _check_options(options, _WRITER_RESERVED)
        self._table_name = table_name

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the table, in one commit.

        Args:
            data: The rows to write.
            replace: Remove every row of the table in the same commit, so that the table ends
                up holding exactly the rows of `data`. Appends when `False`.
        """
        if not data.columns:
            raise ValueError("the DataFrame has no columns")
        options = dict(self._options)
        if replace:
            # Whatever the session says, a replace replaces every partition.
            options["partitionOverwriteMode"] = "static"
        mode = "overwrite" if replace else "append"
        data.write.format("delta").options(**options).mode(mode).saveAsTable(self._table)

    def __repr__(self) -> str:
        return f"UnityCatalogWriter(table_name={self._table_name!r})"
