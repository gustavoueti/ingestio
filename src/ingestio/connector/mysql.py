"""MySQL connectors built on Spark's JDBC data source.

`MySQLReader` returns a Spark DataFrame and filters inside MySQL. `MySQLWriter` appends a
DataFrame to a table, or replaces the table's rows with it, in one transaction. pyspark and
PyMySQL are imported lazily, so this module imports without either.
"""

from __future__ import annotations

import re
import ssl
import uuid
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeAlias
from urllib.parse import quote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

__all__ = ["MySQLConnection", "MySQLReader", "MySQLWriter"]

_SSL_MODES = ("DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY")
_RESERVED = (
    "url",
    "dbtable",
    "query",
    "user",
    "password",
    "driver",
    "sslmode",
    "usessl",
    "requiressl",
    "verifyservercertificate",
)
_PARTITIONING = ("partitioncolumn", "lowerbound", "upperbound", "numpartitions")
_FETCHSIZE = 10000
_NUMBER = re.compile(r"[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date

# Every JDBC session runs in UTC, and Connector/J knows it.
_TIME_ZONE: _Options = {"connectionTimeZone": "UTC", "forceConnectionTimeZoneToSession": True}


@dataclass(frozen=True, repr=False)
class MySQLConnection:
    """Where and how to reach one MySQL database. Opens nothing by itself.

    Args:
        host: Server host name or address.
        database: Database name. An unqualified table name resolves to it.
        user: User to connect as.
        password: The user's password. Never shown in `repr`.
        port: Server port.
        ssl_mode: TLS mode, as MySQL Connector/J names it: `DISABLED`, `PREFERRED`, `REQUIRED`,
            `VERIFY_CA` or `VERIFY_IDENTITY`. `None` leaves both clients' default, which is
            `PREFERRED`.
    """

    host: str
    database: str
    user: str
    password: str
    _: KW_ONLY
    port: int = 3306
    ssl_mode: str | None = None

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must not be empty")
        if not self.database:
            raise ValueError("database must not be empty")
        if not self.user:
            raise ValueError("user must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.ssl_mode is not None and self.ssl_mode not in _SSL_MODES:
            raise ValueError(f"unsupported ssl_mode '{self.ssl_mode}'")

    def __repr__(self) -> str:
        return (
            f"MySQLConnection(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r})"
        )

    def _jdbc_options(self) -> dict[str, str | int | float | bool]:
        host = f"[{self.host}]" if ":" in self.host else self.host
        database = quote(self.database, safe="")
        options: dict[str, str | int | float | bool] = {
            "url": f"jdbc:mysql://{host}:{self.port}/{database}",
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver",
        }
        if self.ssl_mode is not None:
            options["sslMode"] = self.ssl_mode
        return options

    def _connect_arguments(self) -> dict[str, Any]:
        """The keyword arguments of `pymysql.connect`; a new SSL context on each call."""
        arguments: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "charset": "utf8mb4",
        }
        mode = self.ssl_mode
        if mode == "DISABLED":
            arguments["ssl_disabled"] = True
        elif mode == "REQUIRED":
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            arguments["ssl"] = context
        elif mode == "VERIFY_CA":
            context = ssl.create_default_context()
            context.check_hostname = False
            arguments["ssl"] = context
        elif mode == "VERIFY_IDENTITY":
            arguments["ssl"] = ssl.create_default_context()
        return arguments


def _split_table(table_name: str) -> tuple[str, ...]:
    parts = tuple(table_name.split("."))
    if len(parts) > 2 or not all(parts):
        raise ValueError("table_name must be 'table' or 'database.table'")
    return parts


def _quote(name: str) -> str:
    """Quote an identifier: wrap it in backticks and double those inside."""
    return "`" + name.replace("`", "``") + "`"


def _quote_table(parts: tuple[str, ...]) -> str:
    return ".".join(_quote(part) for part in parts)


def _hex(text: str) -> str:
    """A string literal that means the same in every `sql_mode`."""
    return f"_utf8mb4 X'{text.encode('utf-8').hex()}'"


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


def _watermark_literal(value: _Watermark) -> str:
    """Validate a watermark value and turn it into a SQL literal."""
    if isinstance(value, bool):
        raise ValueError("unsupported watermark_value type: bool")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("watermark_value must be finite")
        return format(value, "f")
    if isinstance(value, str):
        return _hex(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watermark_value must be timezone-aware")
        return _hex(value.astimezone(UTC).replace(tzinfo=None).isoformat(sep=" "))
    if isinstance(value, date):
        return _hex(value.isoformat())
    raise ValueError(f"unsupported watermark_value type: {type(value).__name__}")


class MySQLReader:
    """Reads a MySQL table into a Spark DataFrame, filtering inside MySQL.

    Args:
        table_name: Source table, `table` or `database.table`. The configuration key of the same
            name.
        connection: Where the table lives, with its credentials.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark JDBC options and Connector/J properties passed through, such as
            `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `fetchsize` or
            `sessionInitStatement`. They override the connector's defaults.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        connection: MySQLConnection,
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
        self._value = None if watermark_value is None else _watermark_literal(watermark_value)
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
        defaults = {
            **self.connection._jdbc_options(),
            "useCursorFetch": True,
            "fetchsize": _FETCHSIZE,
            **_TIME_ZONE,
        }
        column = self.watermark_column
        if column is None:
            return self._load(spark, _merge({**defaults, "dbtable": table}, self._options))

        from pyspark.sql.types import NumericType

        quoted = _quote(column)
        lower = f"{quoted} > {self._value}" if self._value is not None else None
        query = (
            f"SELECT CAST(max({quoted}) AS CHAR) AS upper_bound, "
            f"max({quoted}) AS upper_bound_value FROM {table}"
        )
        if lower is not None:
            query += f" WHERE {lower}"
        bound_options = _merge({**defaults, "query": query}, self._options, without=_PARTITIONING)
        bound_frame = self._load(spark, bound_options)
        bound_type = bound_frame.schema.fields[1].dataType
        bound = bound_frame.first()
        upper = None if bound is None else bound[0]

        if upper is None:
            predicate = "false"
        else:
            text = str(upper)
            numeric = isinstance(bound_type, NumericType) and _NUMBER.fullmatch(text)
            upper_predicate = f"{quoted} <= {text if numeric else _hex(text)}"
            predicate = upper_predicate if lower is None else f"{lower} AND {upper_predicate}"
        dbtable = f"(SELECT * FROM {table} WHERE {predicate}) AS ingestio_source"
        return self._load(spark, _merge({**defaults, "dbtable": dbtable}, self._options))

    @staticmethod
    def _load(spark: SparkSession, options: _Options) -> DataFrame:
        return spark.read.format("jdbc").options(**options).load()

    def __repr__(self) -> str:
        return (
            f"MySQLReader(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )


class MySQLWriter:
    """Writes a Spark DataFrame into a MySQL table, atomically.

    Args:
        table_name: Table to write into, `table` or `database.table`. It must exist.
        connection: Where the table lives, with its credentials.
        options: Spark JDBC write options and Connector/J properties passed through, such as
            `numPartitions` or `batchsize`. They override the connector's defaults.
    """

    def __init__(
        self,
        table_name: str,
        connection: MySQLConnection,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None:
        try:
            import pymysql  # noqa: F401
        except ImportError as exc:
            raise ImportError("MySQLWriter needs PyMySQL: pip install 'ingestio[mysql]'") from exc
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
        import pymysql

        columns = list(data.columns)
        if not columns:
            raise ValueError("the DataFrame has no columns")

        def execute(*statements: str) -> None:
            """Run statements on a new connection and commit them; always close it."""
            connection = pymysql.connect(**self.connection._connect_arguments())
            try:
                with connection.cursor() as cursor:
                    for statement in statements:
                        cursor.execute(statement)
                connection.commit()
            finally:
                connection.close()

        schema = self._table_parts[:-1]
        staging_parts = (*schema, f"ingestio_staging_{uuid.uuid4().hex}")
        target = _quote_table(self._table_parts)
        staging = _quote_table(staging_parts)
        cols = ", ".join(_quote(column) for column in columns)

        # Setup: a failure here leaves nothing behind, so nothing is cleaned up.
        execute(f"CREATE TABLE {staging} LIKE {target}")

        committed = False
        try:
            load_options = _merge(
                {
                    **self.connection._jdbc_options(),
                    "dbtable": staging,
                    "rewriteBatchedStatements": True,
                    **_TIME_ZONE,
                },
                self._options,
            )
            data.write.format("jdbc").options(**load_options).mode("append").save()

            connection = pymysql.connect(**self.connection._connect_arguments())
            try:
                connection.begin()
                with connection.cursor() as cursor:
                    if replace:
                        cursor.execute(f"DELETE FROM {target}")
                    cursor.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}")
                connection.commit()
                committed = True
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP TABLE {staging}")
                connection.commit()
            finally:
                connection.close()
        except BaseException as error:
            if committed:
                error.add_note(
                    f"the rows were committed to {target}, "
                    f"but staging table {staging} could not be dropped"
                )
                raise
            try:
                execute(f"DROP TABLE IF EXISTS {staging}")
            except Exception as cleanup_error:
                error.add_note(
                    f"staging table {staging} could not be dropped: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            raise

    def __repr__(self) -> str:
        return (
            f"MySQLWriter(table_name={self.table_name!r}, "
            f"host={self.connection.host!r}, database={self.connection.database!r})"
        )
