"""MySQL connectors built on Spark's JDBC data source and on the binary log.

`MySQLReader` returns a Spark DataFrame and filters inside MySQL. `MySQLWriter` appends a
DataFrame to a table, or replaces the table's rows with it, in one transaction. `MySQLCDCReader`
reads the changes of a table from the binary log on the driver, with `python-mysql-replication`.
pyspark, PyMySQL and `python-mysql-replication` are imported lazily, so this module imports
without them.
"""

from __future__ import annotations

import json
import re
import ssl
import uuid
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeAlias
from urllib.parse import quote

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import DataType

__all__ = ["MySQLCDCReader", "MySQLConnection", "MySQLReader", "MySQLWriter"]

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
_BINLOG_FILE = re.compile(r".+\.[0-9]+")
_BITS = re.compile(r"[01]+")
_METADATA = (
    "_ingestio_operation",
    "_ingestio_binlog_file",
    "_ingestio_binlog_offset",
    "_ingestio_commit_time",
    "_ingestio_sequence",
)
# The settings the reader needs, in the order of the statement that reads them.
_REQUIRED_SETTINGS = (
    ("log_bin", "1"),
    ("binlog_format", "ROW"),
    ("binlog_row_image", "FULL"),
    ("binlog_row_metadata", "FULL"),
    ("binlog_row_value_options", ""),
    ("binlog_transaction_compression", "0"),
)

_Options: TypeAlias = Mapping[str, str | int | float | bool]
_Watermark: TypeAlias = int | Decimal | str | date
_Position: TypeAlias = tuple[str, int]

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


def _string_literal(text: str) -> str:
    """A Spark SQL string literal; assumes `spark.sql.parser.escapedStringLiterals` is false."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _order(position: _Position) -> tuple[int, int]:
    """The key that orders binary log positions: the file's number, then the offset."""
    return int(position[0].rsplit(".", 1)[1]), position[1]


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


class MySQLCDCReader:
    """Reads the changes of one MySQL table from the binary log into a Spark DataFrame.

    Args:
        table_name: Source table, `table` or `database.table`. The configuration key of the same
            name.
        server_id: Server id the reader connects to the binary log with, unique among the
            replicas and readers of the server. The configuration key of the same name.
        connection: Where the table lives, with the credentials of a user who holds the
            replication privileges.
        binlog_position: The binary log position the last run reached, `(file, offset)`. Only
            the transactions committed after it are read. Without it, the reader returns a
            snapshot of the table.
        options: Spark JDBC options and Connector/J properties for the snapshot and for the
            table's schema, passed through as on `MySQLReader`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        server_id: int,
        connection: MySQLConnection,
        *,
        binlog_position: tuple[str, int] | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None:
        self._table_parts = _split_table(table_name)
        if not _is_int(server_id) or not 1 <= server_id <= 4294967295:
            raise ValueError("server_id must be an integer between 1 and 4294967295")
        if binlog_position is not None:
            if not isinstance(binlog_position, tuple) or len(binlog_position) != 2:
                raise ValueError("binlog_position must be a (file, offset) tuple")
            file, offset = binlog_position
            if not isinstance(file, str) or not _BINLOG_FILE.fullmatch(file):
                raise ValueError(
                    "binlog_position file must be a binary log name that ends in a number, "
                    "such as 'binlog.000123'"
                )
            if not _is_int(offset) or offset < 4:
                raise ValueError("binlog_position offset must be an integer of at least 4")
        _check_options(options)
        try:
            import pymysql  # noqa: F401
            import pymysqlreplication  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "MySQLCDCReader needs mysql-replication: pip install 'ingestio[mysql-cdc]'"
            ) from exc
        self._table_name = table_name
        self._server_id = server_id
        self._connection = connection
        self._position = binlog_position
        self._given_options = options
        self._spark = spark

    def read(self) -> DataFrame:
        """Return a DataFrame of the table's change events, or its snapshot."""
        spark = self._spark
        if spark is None:
            from pyspark.sql import SparkSession

            spark = SparkSession.active()
        bound = self._check_server()
        table = MySQLReader(
            self._table_name, self._connection, options=self._given_options, spark=spark
        ).read()
        schema = table.schema
        for field in schema.fields:
            if field.name.lower() in _METADATA:
                raise ValueError(
                    f"the table already has a column '{field.name}', "
                    "which MySQLCDCReader adds to every event"
                )
        position = self._position
        if position is None:
            return table.selectExpr(
                "*",
                "'snapshot' AS _ingestio_operation",
                f"{_string_literal(bound[0])} AS _ingestio_binlog_file",
                f"CAST({bound[1]} AS BIGINT) AS _ingestio_binlog_offset",
                "CAST(NULL AS TIMESTAMP) AS _ingestio_commit_time",
                "CAST(0 AS BIGINT) AS _ingestio_sequence",
            )

        from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

        rows = (
            self._binlog_rows(schema, position, bound) if _order(position) < _order(bound) else []
        )
        events = StructType(
            [
                *schema.fields,
                StructField("_ingestio_operation", StringType(), False),
                StructField("_ingestio_binlog_file", StringType(), False),
                StructField("_ingestio_binlog_offset", LongType(), False),
                StructField("_ingestio_commit_time", TimestampType(), True),
                StructField("_ingestio_sequence", LongType(), False),
            ]
        )
        return spark.createDataFrame(rows, events)

    def _check_server(self) -> _Position:
        """Check the server's settings and the position given; return the end of the binary log."""
        import pymysql

        connection = pymysql.connect(**self._connection._connect_arguments())
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, "
                    "@@GLOBAL.binlog_row_metadata, @@GLOBAL.binlog_row_value_options, "
                    "@@GLOBAL.binlog_transaction_compression"
                )
                values = cursor.fetchone()
                for (name, required), value in zip(_REQUIRED_SETTINGS, values, strict=True):
                    if str(value).lower() != required.lower():
                        raise ValueError(
                            f"the server's {name} is '{value}'; MySQLCDCReader needs '{required}'"
                        )
                if self._position is not None:
                    cursor.execute("SHOW BINARY LOGS")
                    self._check_position(self._position, cursor.fetchall())
                cursor.execute("SHOW BINARY LOG STATUS")
                status = cursor.fetchone()
        except BaseException as error:
            try:
                connection.close()
            except Exception as close_error:
                error.add_note(
                    "the control connection could not be closed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            raise
        connection.close()
        return str(status[0]), int(status[1])

    @staticmethod
    def _check_position(position: _Position, logs: Any) -> None:
        sizes = {str(row[0]): int(row[1]) for row in logs}
        file, offset = position
        if file in sizes:
            if offset > sizes[file]:
                raise ValueError(
                    f"binlog_position {position!r} is past the end of '{file}', "
                    f"which is {sizes[file]} bytes long"
                )
            return
        if not sizes:
            return
        oldest, newest = str(logs[0][0]), str(logs[-1][0])
        if (
            oldest.rsplit(".", 1)[0] == file.rsplit(".", 1)[0]
            and _BINLOG_FILE.fullmatch(oldest)
            and _order((oldest, 4)) > _order(position)
        ):
            raise ValueError(
                f"binlog_position {position!r} has been purged from the server; "
                f"its oldest binary log is '{oldest}'"
            )
        raise ValueError(
            f"binlog_position {position!r} names a binary log the server does not have; "
            f"its binary logs are '{oldest}' to '{newest}'"
        )

    def _binlog_rows(
        self, schema: Any, position: _Position, bound: _Position
    ) -> list[tuple[Any, ...]]:
        import pymysqlreplication
        from pymysqlreplication.event import QueryEvent, XidEvent
        from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

        settings = self._connection._connect_arguments()
        settings.pop("database", None)
        parts = self._table_parts
        database = parts[0] if len(parts) == 2 else self._connection.database
        stream = pymysqlreplication.BinLogStreamReader(
            connection_settings=settings,
            server_id=self._server_id,
            resume_stream=True,
            log_file=position[0],
            log_pos=position[1],
            blocking=False,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, XidEvent, QueryEvent],
            only_schemas=[database],
            only_tables=[parts[-1]],
            enable_logging=False,
        )
        rows: list[tuple[Any, ...]] = []
        try:
            buffer: list[tuple[str, tuple[Any, ...]]] = []
            while (event := stream.fetchone()) is not None:
                where = (stream.log_file, stream.log_pos)
                if isinstance(event, WriteRowsEvent):
                    for row in event.rows:
                        buffer.append(("insert", self._values(schema, row["values"], where)))
                elif isinstance(event, DeleteRowsEvent):
                    for row in event.rows:
                        buffer.append(("delete", self._values(schema, row["values"], where)))
                elif isinstance(event, UpdateRowsEvent):
                    keys = event.primary_key
                    names = (keys,) if isinstance(keys, str) and keys else tuple(keys)
                    for row in event.rows:
                        before, after = row["before_values"], row["after_values"]
                        if any(before.get(name) != after.get(name) for name in names):
                            buffer.append(("delete", self._values(schema, before, where)))
                            buffer.append(("insert", self._values(schema, after, where)))
                        else:
                            buffer.append(("update", self._values(schema, after, where)))
                elif isinstance(event, XidEvent) or (
                    isinstance(event, QueryEvent) and event.query.strip().upper() == "COMMIT"
                ):
                    committed = datetime.fromtimestamp(event.timestamp, UTC)
                    for operation, values in buffer:
                        rows.append(
                            (*values, operation, where[0], where[1], committed, len(rows) + 1)
                        )
                    buffer = []
                elif buffer:
                    raise ValueError(
                        f"the binary log has rows of the table at {where!r} that no commit follows"
                    )
                if _order(where) >= _order(bound):
                    break
        except BaseException as error:
            try:
                stream.close()
            except Exception as close_error:
                error.add_note(
                    "the binary log stream could not be closed: "
                    f"{type(close_error).__name__}: {close_error}"
                )
            raise
        stream.close()
        return rows

    @staticmethod
    def _values(schema: Any, values: Mapping[str, Any], where: _Position) -> tuple[Any, ...]:
        """The row's values in the schema's order, converted to the schema's types."""
        by_name = {key.lower(): value for key, value in values.items()}
        converted = []
        for field in schema.fields:
            key = field.name.lower()
            if key not in by_name:
                raise ValueError(
                    f"the event of the table at {where!r} has no column '{field.name}'; "
                    "the table's columns changed since it was written"
                )
            converted.append(_convert(field.name, field.dataType, by_name[key], where))
        return tuple(converted)

    def __repr__(self) -> str:
        return (
            f"MySQLCDCReader(table_name={self._table_name!r}, server_id={self._server_id!r}, "
            f"host={self._connection.host!r}, database={self._connection.database!r})"
        )


def _convert(name: str, kind: DataType, value: Any, where: _Position) -> Any:
    """Turn a value of the binary log into one for the Spark type `kind`."""
    from pyspark.sql import types as t

    if value is None:
        return None
    bits = isinstance(value, str) and _BITS.fullmatch(value) is not None
    integer = _is_int(value)
    epoch = datetime(1970, 1, 1)  # noqa: DTZ001 - the naive epoch of a TIME
    if isinstance(kind, t.BooleanType):
        if isinstance(value, bool):
            return value
        if integer:
            return value != 0
        if bits:
            return int(value, 2) != 0
    elif isinstance(kind, t.ByteType | t.ShortType | t.IntegerType | t.LongType):
        if integer:
            return value
        if bits:
            return int(value, 2)
    elif isinstance(kind, t.DecimalType):
        if isinstance(value, Decimal):
            return value
        if integer:
            return Decimal(value)
    elif isinstance(kind, t.FloatType | t.DoubleType):
        if isinstance(value, float):
            return value
    elif isinstance(kind, t.StringType):
        if isinstance(value, str):
            return value
        if isinstance(value, set):
            return ",".join(sorted(value))
        if isinstance(value, dict | list | bool | int | float):
            return json.dumps(value, ensure_ascii=False)
    elif isinstance(kind, t.BinaryType):
        if isinstance(value, bytes):
            return value
        if bits:
            return int(value, 2).to_bytes((len(value) + 7) // 8, "big")
    elif isinstance(kind, t.DateType):
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if integer:
            return date(value, 1, 1)
    elif isinstance(kind, t.TimestampNTZType):
        if isinstance(value, datetime) and value.tzinfo is None:
            return value
        if isinstance(value, timedelta):
            return epoch + value
    elif isinstance(kind, t.TimestampType):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        if isinstance(value, timedelta):
            return epoch.replace(tzinfo=UTC) + value
    raise ValueError(
        f"column '{name}' of the event at {where!r}: cannot turn a {type(value).__name__} "
        f"into {kind.simpleString()}"
    )
