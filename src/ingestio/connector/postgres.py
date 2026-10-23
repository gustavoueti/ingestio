"""PostgreSQL connectors built on Spark's JDBC data source.

`PostgresReader` returns a Spark DataFrame and filters inside PostgreSQL. `PostgresWriter`
appends a DataFrame to a table, or replaces the table's rows with it, in one transaction.
pyspark and psycopg are imported lazily, so this module imports without either.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeAlias
from urllib.parse import quote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["PostgresConnection", "PostgresReader", "PostgresWriter"]

_SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
_RESERVED = ("url", "dbtable", "query", "user", "password", "driver")
_PARTITIONING = ("partitioncolumn", "lowerbound", "upperbound", "numpartitions")
_FETCHSIZE = 10000

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date


@dataclass(frozen=True, repr=False)
class PostgresConnection:
    """Where and how to reach one PostgreSQL database. Opens nothing by itself.

    Args:
        host: Server host name or address.
        database: Database name.
        user: User to connect as.
        password: The user's password. Never shown in `repr`.
        port: Server port.
        sslmode: libpq / pgJDBC SSL mode: `disable`, `allow`, `prefer`, `require`,
            `verify-ca` or `verify-full`. `None` leaves the drivers' default.
    """

    host: str
    database: str
    user: str
    password: str
    _: KW_ONLY
    port: int = 5432
    sslmode: str | None = None

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must not be empty")
        if not self.database:
            raise ValueError("database must not be empty")
        if not self.user:
            raise ValueError("user must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.sslmode is not None and self.sslmode not in _SSLMODES:
            raise ValueError(f"unsupported sslmode '{self.sslmode}'")

    def __repr__(self) -> str:
        return (
            f"PostgresConnection(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r})"
        )

    def _jdbc_options(self) -> dict[str, str | int | float | bool]:
        host = f"[{self.host}]" if ":" in self.host else self.host
        database = quote(self.database, safe="")
        options: dict[str, str | int | float | bool] = {
            "url": f"jdbc:postgresql://{host}:{self.port}/{database}",
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        if self.sslmode is not None:
            options["sslmode"] = self.sslmode
        return options

    def _connect_arguments(self) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }
        if self.sslmode is not None:
            arguments["sslmode"] = self.sslmode
        return arguments


def _split_table(table_name: str) -> tuple[str, ...]:
    parts = tuple(table_name.split("."))
    if len(parts) > 2 or not all(parts):
        raise ValueError("table_name must be 'table' or 'schema.table'")
    return parts


def _quote(name: str) -> str:
    """Quote an identifier: wrap it in double quotes and double those inside."""
    return '"' + name.replace('"', '""') + '"'


def _quote_table(parts: tuple[str, ...]) -> str:
    return ".".join(_quote(part) for part in parts)


def _literal(text: str) -> str:
    """A SQL string literal; assumes `standard_conforming_strings` is on."""
    return "'" + text.replace("'", "''") + "'"


def _check_options(options: _Options | None) -> dict[str, str | int | float | bool]:
    given = dict(options or {})
    for key in given:
        if key.lower() in _RESERVED:
            raise ValueError(f"option '{key}' is set by the connector")
    return given


def _merge(
    defaults: _Options, user: _Options, *, without: tuple[str, ...] = ()
) -> dict[str, str | int | float | bool]:
    """`defaults` with the user's options replacing the same names, in any case."""
    names = {key.lower() for key in user}
    merged = {key: value for key, value in defaults.items() if key.lower() not in names}
    merged.update({key: value for key, value in user.items() if key.lower() not in without})
    return merged


def _watermark_text(value: _Watermark) -> str:
    """Validate a watermark value and turn it into text."""
    if isinstance(value, bool):
        raise ValueError("unsupported watermark_value type: bool")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("watermark_value must be finite")
        return format(value, "f")
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watermark_value must be timezone-aware")
        return value.astimezone(UTC).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    raise ValueError(f"unsupported watermark_value type: {type(value).__name__}")


class PostgresReader:
    """Reads a PostgreSQL table into a Spark DataFrame, filtering inside PostgreSQL.

    Args:
        table_name: Source table, `table` or `schema.table`. The configuration key of the same
            name.
        connection: Where the table lives.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark JDBC options passed through, such as `partitionColumn`, `lowerBound`,
            `upperBound`, `numPartitions`, `fetchsize`, `sessionInitStatement`,
            `queryTimeout`. They override the connector's defaults.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        connection: PostgresConnection,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._table_parts = _split_table(table_name)
        if watermark_column == "":
            raise ValueError("watermark_column must not be empty")
        if watermark_value is not None and watermark_column is None:
            raise ValueError("watermark_value needs watermark_column")
        self._value = None if watermark_value is None else _watermark_text(watermark_value)
        self._options = _check_options(options)
        self.table_name = table_name
        self.connection = connection
        self.watermark_column = watermark_column
        self.watermark_value = watermark_value
        self.options = options
        self.spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
        spark = self.spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        table = _quote_table(self._table_parts)
        defaults = {**self.connection._jdbc_options(), "fetchsize": _FETCHSIZE}
        column = self.watermark_column
        if column is None:
            return self._load(spark, _merge({**defaults, "dbtable": table}, self._options))

        quoted = _quote(column)
        lower = f"{quoted} > {_literal(self._value)}" if self._value is not None else None
        query = f"SELECT max({quoted})::text AS upper_bound FROM {table}"
        if lower is not None:
            query += f" WHERE {lower}"
        bound_options = _merge({**defaults, "query": query}, self._options, without=_PARTITIONING)
        bound = self._load(spark, bound_options).first()
        upper = None if bound is None else bound[0]

        if upper is None:
            predicate = "false"
        else:
            upper_predicate = f"{quoted} <= {_literal(str(upper))}"
            predicate = upper_predicate if lower is None else f"{lower} AND {upper_predicate}"
        dbtable = f"(SELECT * FROM {table} WHERE {predicate}) AS ingestio_source"
        return self._load(spark, _merge({**defaults, "dbtable": dbtable}, self._options))

    @staticmethod
    def _load(spark: SparkSession, options: _Options) -> DataFrame:
        return spark.read.format("jdbc").options(**options).load()

    def __repr__(self) -> str:
        return (
            f"PostgresReader(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )


class PostgresWriter:
    """Writes a Spark DataFrame into a PostgreSQL table, atomically.

    Args:
        table_name: Table to write into, `table` or `schema.table`. It must exist.
        connection: Where the table lives.
        options: Spark JDBC write options passed through, such as `numPartitions` or
            `batchsize`. They override the connector's defaults.
    """

    def __init__(
        self,
        table_name: str,
        connection: PostgresConnection,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None:
        try:
            import psycopg  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PostgresWriter needs psycopg: pip install 'ingestio[postgres]'"
            ) from exc
        self._table_parts = _split_table(table_name)
        self._options = _check_options(options)
        self.table_name = table_name
        self.connection = connection
        self.options = options

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one transaction.

        Args:
            data: The rows to write.
            replace: Remove every row of the target first, in the same transaction, so that
                the target ends up holding exactly the rows of `data`. Appends when `False`.
        """
        import psycopg
        from psycopg import sql

        columns = list(data.columns)
        if not columns:
            raise ValueError("the DataFrame has no columns")

        arguments = self.connection._connect_arguments()
        schema = self._table_parts[:-1]
        staging_parts = (*schema, f"ingestio_staging_{uuid.uuid4().hex}")
        target = sql.Identifier(*self._table_parts)
        staging = sql.Identifier(*staging_parts)
        cols = sql.SQL(", ").join(sql.Identifier(column) for column in columns)

        # Setup: a failure here leaves nothing behind, so nothing is cleaned up.
        with psycopg.connect(**arguments) as setup:
            setup.execute(
                sql.SQL("CREATE UNLOGGED TABLE {} AS SELECT {} FROM {} WITH NO DATA").format(
                    staging, cols, target
                )
            )

        try:
            load_options = _merge(
                {
                    **self.connection._jdbc_options(),
                    "dbtable": _quote_table(staging_parts),
                    "stringtype": "unspecified",
                    "reWriteBatchedInserts": "true",
                },
                self._options,
            )
            data.write.format("jdbc").options(**load_options).mode("append").save()
            with psycopg.connect(**arguments) as apply:
                if replace:
                    apply.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
                apply.execute(
                    sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                        target, cols, cols, staging
                    )
                )
                apply.execute(sql.SQL("DROP TABLE {}").format(staging))
        except BaseException as error:
            try:
                with psycopg.connect(**arguments) as cleanup:
                    cleanup.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging))
            except Exception as cleanup_error:
                error.add_note(
                    f"staging table {_quote_table(staging_parts)} could not be dropped: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            raise

    def __repr__(self) -> str:
        return (
            f"PostgresWriter(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )
