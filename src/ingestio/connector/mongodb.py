"""MongoDB reader built on the MongoDB Spark Connector.

`MongoDBConnection` holds the connection string of one deployment. `MongoDBReader` returns a Spark
DataFrame over one collection and filters inside MongoDB. pyspark is imported lazily, so this
module imports without it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["MongoDBConnection", "MongoDBReader"]

_SCHEMES = ("mongodb://", "mongodb+srv://")
_RESERVED = ("connection.uri", "database", "collection", "schemahints", "aggregation.pipeline")
_PREFIXES = ("spark.mongodb.read.", "spark.mongodb.")
# The characters that Java's `String.trim()` removes.
_BLANKS = "".join(chr(code) for code in range(0x21))

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date


def _split(uri: str) -> tuple[str, str]:
    """The hosts and the text after the authority of a connection string that starts well."""
    scheme = next(scheme for scheme in _SCHEMES if uri.startswith(scheme))
    rest = uri[len(scheme) :]
    end = len(rest)
    for mark in "/?":
        found = rest.find(mark)
        if found != -1:
            end = min(end, found)
    return rest[:end].rsplit("@", 1)[-1], rest[end:]


@dataclass(frozen=True, repr=False)
class MongoDBConnection:
    """Where and how to reach one MongoDB deployment. Opens nothing by itself.

    Args:
        uri: The connection string, `mongodb://...` or `mongodb+srv://...`, credentials
            included. Never shown in `repr`, `str` or an error message.
    """

    uri: str

    def __post_init__(self) -> None:
        if not isinstance(self.uri, str):
            raise TypeError(f"uri must be a str, not {type(self.uri).__name__}")
        if not self.uri.startswith(_SCHEMES):
            raise ValueError("uri must start with 'mongodb://' or 'mongodb+srv://'")
        hosts, after = _split(self.uri)
        if "@" in after:
            raise ValueError("uri has an unencoded '@' after its hosts")
        if not hosts:
            raise ValueError("uri names no host")

    def __repr__(self) -> str:
        return f"MongoDBConnection(hosts={_split(self.uri)[0]!r})"


def _quote(name: str) -> str:
    """Quote an identifier: wrap it in backticks and double those inside."""
    return "`" + name.replace("`", "``") + "`"


def _string_literal(text: str) -> str:
    """A Spark SQL string literal; assumes `spark.sql.parser.escapedStringLiterals` is false."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _escape_collection(name: str) -> str:
    """Escape the connector's list syntax so that `name` is read as one collection."""
    return "".join("\\" + char if char in "\\,*" else char for char in name)


def _check_options(options: _Options | None) -> dict[str, str | int | float | bool]:
    given: dict[str, str | int | float | bool] = dict(options or {})
    for key in given:
        lowered = key.lower()
        for prefix in _PREFIXES:
            if lowered.startswith(prefix):
                lowered = lowered[len(prefix) :]
                break
        if lowered in _RESERVED:
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


class MongoDBReader:
    """Reads a MongoDB collection into a Spark DataFrame, filtering inside MongoDB.

    Args:
        database: Database of the collection. The configuration key of the same name.
        collection: Source collection, one name. The configuration key of the same name.
        connection: Where the collection lives, with its credentials.
        watermark_column: Top-level field that orders changes. The configuration key of the
            same name.
        watermark_value: Last watermark already ingested. Only documents above it are read.
            Requires `watermark_column`.
        schema_hints: Types of known fields, a DDL string such as
            `"updatedAt TIMESTAMP, total DECIMAL(18,2)"`. Spark infers the other fields. The
            configuration key of the same name.
        options: Read options of the MongoDB Spark Connector passed through, such as
            `sampleSize` or `partitioner`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        database: str,
        collection: str,
        connection: MongoDBConnection,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        schema_hints: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        if database == "":
            raise ValueError("database must not be empty")
        if collection == "":
            raise ValueError("collection must not be empty")
        if watermark_column == "":
            raise ValueError("watermark_column must not be empty")
        if watermark_value is not None and watermark_column is None:
            raise ValueError("watermark_value needs watermark_column")
        self._value = None if watermark_value is None else _watermark_literal(watermark_value)
        if schema_hints is not None:
            trimmed = schema_hints.strip(_BLANKS)
            if not trimmed:
                raise ValueError("schema_hints must not be empty")
            if trimmed.startswith("{"):
                raise ValueError("schema_hints must be a DDL string, not JSON")
        self._options = _check_options(options)
        self._database = database
        self._collection = collection
        self._uri = connection.uri
        self._hosts = _split(connection.uri)[0]
        self._column = watermark_column
        self._hints = schema_hints
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the documents the job needs."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        options: dict[str, str | int | float | bool] = {
            "connection.uri": self._uri,
            "database": self._database,
            "collection": _escape_collection(self._collection),
        }
        if self._hints is not None:
            options["schemaHints"] = self._hints
        options.update(self._options)
        source = spark.read.format("mongodb").options(**options).load()
        if not source.columns:
            return source.where("false")
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
        return (
            f"MongoDBReader(database={self._database!r}, collection={self._collection!r}, "
            f"hosts={self._hosts!r})"
        )
