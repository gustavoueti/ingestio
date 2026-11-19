"""MySQL CDC reader: `MySQLCDCReader`, `ReadSource`'s `binlog_position`, the `mysql_cdc` system.

Covers `specs/mysql_cdc.md`. The tests named after an acceptance criterion always run. They need
no JVM, no network and no MySQL server: they use the doubles below, a fake Spark session, fake
DataFrames, a fake `pymysql.connect` and a fake `pymysqlreplication.BinLogStreamReader`, which all
write to one ordered event log. Reading `schema` on the DataFrame of the table's load is an event
of that log, and so are `selectExpr` and `createDataFrame`. The binary log events that the fake
stream returns are instances of subclasses of the library's own event classes, built without the
library's constructor and holding only the attributes the reader reads; the library declares
`rows` as a lazy property, which the subclasses override. Configurations are written with
`json.dumps` to a file under `tmp_path`, and a missing package is simulated in a fresh
interpreter, or by mapping its name to `None` in `sys.modules`.

`MySQLCDCReader` is reached as an attribute of `ingestio.connector.all`, the module the spec
exports it from.

The integration tests at the end run against a real MySQL server, in two tiers (decision 46 of
the spec). They are marked `integration` and skipped unless `INGESTIO_MYSQL_URL` holds a URL of
the form `mysql://<user>:<password>@<host>:<port>/<database>`, on a server configured with
`binlog_row_metadata=FULL`, whose user holds `REPLICATION SLAVE` and `REPLICATION CLIENT`. Each
test creates its own tables in that database, `ingestio_it_cdc_<stem>_<hex>`, drops them, and
reads the binary log with a random `server_id`. The first tier needs no Spark: the server checks
and the stream run for real, with a fake Spark session whose JDBC load answers the schema that
the test chooses to match the table, and whose `createDataFrame` records the rows. The second
tier also needs `INGESTIO_SPARK_TESTS` set to `1` and `INGESTIO_MYSQL_JDBC_PACKAGE` holding the
Maven coordinate of a MySQL Connector/J release, such as `com.mysql:mysql-connector-j:<version>`,
which a local Spark session loads through `spark.jars.packages`. A JVM loads its packages once,
when it starts: when another test module of the same run has already started one, the second
tier skips, so run this file on its own. A few tests change a global variable of the server or
flush its binary logs, and skip when the user of the URL may not. They were written without being
run: the machine they were written on has no MySQL server and no Java release that pyspark
supports, and the MySQL service in CI is postponed.
"""

from __future__ import annotations

import importlib.metadata
import inspect
import json
import logging
import os
import random
import re
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Any, NoReturn, TypeAlias, cast
from urllib.parse import unquote, urlsplit

import pymysql
import pymysqlreplication
import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from pymysqlreplication.event import QueryEvent, XidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

import ingestio.connector.all as connectors
from ingestio import SCD2, AppendOnly, FullLoad, Process, ReadSource, Step
from ingestio.connector.all import MongoDBConnection, PostgresConnection, Reader

PASSWORD = "s3cret"
# The connect arguments of the criteria, and the settings: the same without `database`.
CONNECT_ARGS: dict[str, object] = {
    "host": "db.internal",
    "port": 3306,
    "user": "ingest",
    "password": PASSWORD,
    "database": "shop",
    "charset": "utf8mb4",
}
SETTINGS: dict[str, object] = {
    key: value for key, value in CONNECT_ARGS.items() if key != "database"
}
# `V`, the statement of step 1 of the server checks, and the two others.
V = (
    "SELECT @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image,"
    " @@GLOBAL.binlog_row_metadata, @@GLOBAL.binlog_row_value_options,"
    " @@GLOBAL.binlog_transaction_compression"
)
LOGS = "SHOW BINARY LOGS"
STATUS = "SHOW BINARY LOG STATUS"
VARIABLES_ROW: tuple[object, ...] = (1, "ROW", "FULL", "FULL", "", 0)
BINARY_LOGS: tuple[tuple[object, ...], ...] = (
    ("binlog.000041", 1000, "No"),
    ("binlog.000042", 5000, "No"),
)
STATUS_ROW: tuple[object, ...] = ("binlog.000042", 5000, "", "", "")
FILE = "binlog.000042"
P = ("binlog.000042", 1200)

# `TABLE`, the default table schema of the fake session, and `META`, the five metadata fields.
TABLE = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
META = [
    StructField("_ingestio_operation", StringType(), False),
    StructField("_ingestio_binlog_file", StringType(), False),
    StructField("_ingestio_binlog_offset", LongType(), False),
    StructField("_ingestio_commit_time", TimestampType(), True),
    StructField("_ingestio_sequence", LongType(), False),
]
EVENTS_SCHEMA = StructType([*TABLE.fields, *META])
METADATA_NAMES = tuple(item.name for item in META)

T = datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC)
T2 = T + timedelta(seconds=1)
T3 = T + timedelta(seconds=2)
A = datetime(2026, 10, 24, 11, 0)  # noqa: DTZ001 - the naive datetime of the criteria
A_UTC = A.replace(tzinfo=UTC)
# The row of AT-021: the default transaction's insert, at its commit.
AT021_ROW: tuple[object, ...] = (1, "a", A_UTC, "insert", FILE, 1600, T, 1)

DEFAULT_SOURCE: dict[str, object] = {
    "system": "mysql_cdc",
    "table_name": "shop.orders",
    "server_id": 4001,
}
SOURCE_NAMES = (
    "bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume"
)
TARGET_NAMES = "bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume"
SCD2_NAMES = "bigquery, mysql, postgres, unity_catalog"
SCD2_SECTION: dict[str, object] = {"key_columns": ["id"], "effective_time_column": "updated_at"}
CDC_TARGET: dict[str, object] = {"system": "mysql_cdc", "table_name": "shop.orders"}
RESERVED_KEYS = (
    "url",
    "DbTable",
    "QUERY",
    "user",
    "password",
    "Driver",
    "sslMode",
    "SSLMODE",
    "useSSL",
    "requireSSL",
    "VerifyServerCertificate",
)
ACCEPTED_OPTIONS: dict[str, str | int | float | bool] = {
    "useCursorFetch": False,
    "fetchsize": 500,
    "connectionTimeZone": "SERVER",
    "trustCertificateKeyStoreUrl": "file:/certs/ca.jks",
    "sessionInitStatement": "SET SESSION sql_mode = 'ANSI'",
}
LIBRARY_EVENTS = {WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, XidEvent, QueryEvent}
IMPORT_MESSAGE = "MySQLCDCReader needs mysql-replication: pip install 'ingestio[mysql-cdc]'"
TABLE_NAME_MESSAGE = "table_name must be 'table' or 'database.table'"
SERVER_ID_MESSAGE = "server_id must be an integer between 1 and 4294967295"
TUPLE_MESSAGE = "binlog_position must be a (file, offset) tuple"
FILE_MESSAGE = (
    "binlog_position file must be a binary log name that ends in a number, such as 'binlog.000123'"
)
OFFSET_MESSAGE = "binlog_position offset must be an integer of at least 4"


# --- Binary log event doubles -------------------------------------------------------------

_Rows: TypeAlias = list[dict[str, Any]]


class _RowsDouble:
    """What the reader reads of a row event: `rows` and `primary_key`.

    Mixed in before the library's row event classes, so that its `__init__` replaces the
    library's constructor and its `rows` replaces the library's lazy property.
    """

    def __init__(self, rows: _Rows, primary_key: str | tuple[str, ...] = "id") -> None:
        self._given_rows = rows
        self.primary_key = primary_key

    @property
    def rows(self) -> _Rows:
        return self._given_rows


class _WriteRows(_RowsDouble, WriteRowsEvent):  # type: ignore[misc]
    """A `WriteRowsEvent` of the library, with only `rows` and `primary_key`."""


class _UpdateRows(_RowsDouble, UpdateRowsEvent):  # type: ignore[misc]
    """An `UpdateRowsEvent` of the library, with only `rows` and `primary_key`."""


class _DeleteRows(_RowsDouble, DeleteRowsEvent):  # type: ignore[misc]
    """A `DeleteRowsEvent` of the library, with only `rows` and `primary_key`."""


class _Xid(XidEvent):  # type: ignore[misc]
    """An `XidEvent` of the library, with only `timestamp`."""

    def __init__(self, timestamp: int) -> None:
        self.timestamp = timestamp


class _Query(QueryEvent):  # type: ignore[misc]
    """A `QueryEvent` of the library, with only `query` and `timestamp`."""

    def __init__(self, query: str, timestamp: int) -> None:
        self.query = query
        self.timestamp = timestamp


def _ts(moment: datetime) -> int:
    """`ts(t)` of the spec: `t`'s seconds since the epoch."""
    return int(moment.timestamp())


def _w(rows: _Rows) -> _WriteRows:
    """`W(rows)` of the spec."""
    return _WriteRows(rows)


def _u(rows: _Rows, primary_key: str | tuple[str, ...] = "id") -> _UpdateRows:
    """`U(rows)` of the spec, whose `primary_key` is `"id"` unless given."""
    return _UpdateRows(rows, primary_key)


def _d(rows: _Rows) -> _DeleteRows:
    """`D(rows)` of the spec."""
    return _DeleteRows(rows)


def _x(timestamp: int) -> _Xid:
    """`X(n)` of the spec."""
    return _Xid(timestamp)


def _q(query: str, timestamp: int | None = None) -> _Query:
    """`Q(text, n)` of the spec, and `Q(text)`, whose timestamp is `ts(T)`."""
    return _Query(query, _ts(T) if timestamp is None else timestamp)


# An item of the fake stream's script: the event, then the stream's `log_file` and `log_pos`.
_Item: TypeAlias = tuple[object, str, int]


def _at(event: object, offset: int, log_file: str = FILE) -> _Item:
    """A script item `(event, n)` of the spec: in `binlog.000042` unless given."""
    return (event, log_file, offset)


def _values(identifier: object, name: object, updated_at: object) -> dict[str, Any]:
    """A row of the default table schema, as the binary log gives it."""
    return {"values": {"id": identifier, "name": name, "updated_at": updated_at}}


def _transaction(row_event: object, commit: object | None = None) -> list[_Item]:
    """The default transaction with `row_event` at 1500, and `commit` at 1600, `X(ts(T))` by
    default."""
    return [
        _at(_q("BEGIN"), 1400),
        _at(row_event, 1500),
        _at(_x(_ts(T)) if commit is None else commit, 1600),
    ]


def _default_transaction() -> list[_Item]:
    """The default transaction of the criteria."""
    return _transaction(_w([_values(1, "a", A)]))


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Connect:
    """`pymysql.connect` was called. `connection` numbers the calls from 1."""

    connection: int
    args: tuple[object, ...]
    kwargs: dict[str, object]


@dataclass(frozen=True)
class _Call:
    """A method of a fake connection ran: `cursor`, `begin`, `commit`, `rollback` or `close`.
    `failed` when it raised."""

    connection: int
    method: str
    failed: bool = False


@dataclass(frozen=True)
class _Execute:
    """A cursor of the connection executed `statement`, with these other arguments."""

    connection: int
    statement: str
    args: tuple[object, ...] = ()
    kwargs: dict[str, object] = field(default_factory=dict)
    failed: bool = False


@dataclass(frozen=True)
class _Fetch:
    """`fetchone()` or `fetchall()` ran on a cursor of the connection, after `statement`."""

    connection: int
    method: str
    statement: str | None
    failed: bool = False


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read`, with this format and these merged options. `frame`
    is the number of the DataFrame it returned."""

    source: str | None
    options: dict[str, str]
    frame: int


@dataclass(frozen=True)
class _Schema:
    """`schema` was read on the DataFrame numbered `frame`."""

    frame: int


@dataclass(frozen=True)
class _SelectExpr:
    """`selectExpr` ran on the DataFrame numbered `frame` and returned the one `result`."""

    frame: int
    exprs: tuple[str, ...]
    result: int


@dataclass(frozen=True)
class _Create:
    """`spark.createDataFrame` ran: `list(data)`, the schema, the type of `data`, any other
    arguments, and the number of the DataFrame it returned."""

    rows: list[object]
    schema: object
    data_type: type
    args: tuple[object, ...]
    kwargs: dict[str, object]
    result: int


@dataclass(frozen=True)
class _Open:
    """`BinLogStreamReader` was called. `stream` numbers the calls from 1."""

    stream: int
    args: tuple[object, ...]
    kwargs: dict[str, object]


@dataclass(frozen=True)
class _StreamCall:
    """`fetchone()` or `close()` ran on the stream numbered `stream`. `failed` when it raised."""

    stream: int
    method: str
    failed: bool = False


_Entry: TypeAlias = (
    _Connect | _Call | _Execute | _Fetch | _Load | _Schema | _SelectExpr | _Create | _Open
) | _StreamCall


class _Log:
    """The ordered event log every double writes to, and the DataFrames the doubles made."""

    def __init__(self) -> None:
        self.events: list[_Entry] = []
        self.frames: list[_FakeDataFrame] = []
        self.spark_attributes: list[str] = []


def _spark_text(value: object) -> str:
    """An option value as Spark sees it: `str(value)`, and booleans as `true` and `false`."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _set_option(options: dict[str, str], key: str, value: object) -> None:
    """Set an option as Spark does: it replaces the key of the same name, in any case."""
    for existing in [name for name in options if name.lower() == key.lower()]:
        del options[existing]
    options[key] = _spark_text(value)


@dataclass
class _SparkScript:
    """What the fake session answers: the schema of a load's DataFrame, and the errors that
    every load and every `createDataFrame` raise once recorded."""

    schema: StructType = field(default_factory=lambda: TABLE)
    load_error: BaseException | None = None
    create_error: BaseException | None = None


class _FakeSpark:
    """Stands in for a `SparkSession`, and records every attribute accessed on it.

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does; each load
    returns a new `_FakeDataFrame` whose `schema` is the script's. `createDataFrame` records its
    arguments and returns a new `_FakeDataFrame`.
    """

    def __init__(self, log: _Log, script: _SparkScript) -> None:
        self._log = log
        self._script = script

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self)

    def createDataFrame(  # noqa: N802 - PySpark's name
        self, data: Any, schema: object = None, *args: object, **kwargs: object
    ) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        rows = list(data)
        self._log.events.append(_Create(rows, schema, type(data), args, kwargs, frame._number))
        if self._script.create_error is not None:
            raise self._script.create_error
        return frame

    def _load(self, source: str | None, options: dict[str, str]) -> _FakeDataFrame:
        """Record a load and return its DataFrame, or raise the script's `load_error`."""
        frame = _FakeDataFrame(self._log, self._script.schema)
        self._log.events.append(_Load(source, dict(options), frame._number))
        if self._script.load_error is not None:
            raise self._script.load_error
        return frame


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_Load` each time `load()` is called."""

    def __init__(self, spark: _FakeSpark) -> None:
        self._spark = spark
        self._source: str | None = None
        self._options: dict[str, str] = {}

    def format(self, source: str) -> _FakeDataFrameReader:
        self._source = source
        return self

    def option(self, key: str, value: object) -> _FakeDataFrameReader:
        _set_option(self._options, key, value)
        return self

    def options(self, **options: object) -> _FakeDataFrameReader:
        for key, value in options.items():
            _set_option(self._options, key, value)
        return self

    def load(self, **options: object) -> _FakeDataFrame:
        self.options(**options)
        return self._spark._load(self._source, self._options)


class _FakeDataFrame:
    """Stands in for a `DataFrame`. The DataFrame of a load has a `schema`, whose reading is an
    event; `selectExpr` records its expressions and returns a new fake DataFrame."""

    def __init__(self, log: _Log, schema: StructType | None = None) -> None:
        self._log = log
        self._schema = schema
        self._number = len(log.frames)
        log.frames.append(self)

    @property
    def schema(self) -> StructType:
        if self._schema is None:
            raise AssertionError("schema was read on a DataFrame that is not the table's load")
        self._log.events.append(_Schema(self._number))
        return self._schema

    def selectExpr(self, *expr: str | list[str]) -> _FakeDataFrame:  # noqa: N802 - PySpark's name
        # As PySpark does, one list stands for its items.
        items = expr[0] if len(expr) == 1 and isinstance(expr[0], list) else expr
        result = _FakeDataFrame(self._log)
        self._log.events.append(
            _SelectExpr(self._number, tuple(cast("Sequence[str]", items)), result._number)
        )
        return result


# The default answers of the fake cursor, by statement: the rows that `fetchall()` returns, and
# whose first one `fetchone()` returns.
DEFAULT_ANSWERS: dict[str, tuple[tuple[object, ...], ...]] = {
    V: (VARIABLES_ROW,),
    LOGS: BINARY_LOGS,
    STATUS: (STATUS_ROW,),
}


class _FakePyMySQL:
    """Stands in for `pymysql.connect`: records each call and returns a `_FakeConnection`.

    `answers` maps a statement to the rows its cursor answers. `connect_error` is raised by every
    call once recorded. `method_errors` maps a method of a connection or a cursor, `cursor`,
    `begin`, `commit`, `rollback`, `close`, `fetchone` or `fetchall`, to the error it raises once
    recorded, and `execute_errors` a statement to the error its execution raises once recorded.
    """

    def __init__(self, log: _Log) -> None:
        self.log = log
        self.answers: dict[str, tuple[tuple[object, ...], ...]] = dict(DEFAULT_ANSWERS)
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.connect_error: BaseException | None = None
        self.method_errors: dict[str, BaseException] = {}
        self.execute_errors: dict[str, BaseException] = {}

    def connect(self, *args: object, **kwargs: object) -> _FakeConnection:
        number = len(self.calls) + 1
        self.calls.append((args, dict(kwargs)))
        self.log.events.append(_Connect(number, args, dict(kwargs)))
        if self.connect_error is not None:
            raise self.connect_error
        return _FakeConnection(self, number)


class _FakeConnection:
    """Stands in for a `pymysql.connections.Connection`. A second `close()` raises
    `Error("Already closed")`, as PyMySQL does."""

    def __init__(self, fake: _FakePyMySQL, number: int) -> None:
        self._fake = fake
        self.number = number
        self.closed = False

    def cursor(self, cursor: object = None) -> _FakeCursor:
        self._call("cursor")
        return _FakeCursor(self)

    def begin(self) -> None:
        self._call("begin")

    def commit(self) -> None:
        self._call("commit")

    def rollback(self) -> None:
        self._call("rollback")

    def close(self) -> None:
        if self.closed:
            self._fake.log.events.append(_Call(self.number, "close", failed=True))
            raise pymysql.err.Error("Already closed")
        self.closed = True
        self._call("close")

    def _call(self, method: str) -> None:
        error = self._fake.method_errors.get(method)
        self._fake.log.events.append(_Call(self.number, method, failed=error is not None))
        if error is not None:
            raise error


class _FakeCursor:
    """Stands in for a `pymysql.cursors.Cursor`: records its executions and its fetches on its
    connection, and answers the statement it executed last from the script."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self._statement: str | None = None

    def execute(self, query: str, *args: object, **kwargs: object) -> int:
        fake = self._connection._fake
        error = fake.execute_errors.get(query)
        fake.log.events.append(
            _Execute(self._connection.number, query, args, dict(kwargs), failed=error is not None)
        )
        if error is not None:
            raise error
        self._statement = query
        return 0

    def fetchone(self) -> tuple[object, ...] | None:
        rows = self._fetch("fetchone")
        return rows[0] if rows else None

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self._fetch("fetchall")

    def _fetch(self, method: str) -> tuple[tuple[object, ...], ...]:
        fake = self._connection._fake
        error = fake.method_errors.get(method)
        fake.log.events.append(
            _Fetch(self._connection.number, method, self._statement, failed=error is not None)
        )
        if error is not None:
            raise error
        if self._statement not in fake.answers:
            raise AssertionError(f"no answer scripted for {self._statement!r}")
        return fake.answers[self._statement]

    def close(self) -> None:
        """Nothing to release."""

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class _FakeReplication:
    """Stands in for `pymysqlreplication.BinLogStreamReader`: records each call and returns a
    `_FakeStream` that plays `script`.

    `error` is raised by every call once recorded. `fetch_errors` maps the number of a
    `fetchone()` call of a stream, from 1, to the error it raises once recorded, and
    `close_error` is raised by every `close()` once recorded.
    """

    def __init__(self, log: _Log) -> None:
        self.log = log
        self.script: list[_Item] = _default_transaction()
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.error: BaseException | None = None
        self.fetch_errors: dict[int, BaseException] = {}
        self.close_error: BaseException | None = None

    def __call__(self, *args: object, **kwargs: object) -> _FakeStream:
        number = len(self.calls) + 1
        self.calls.append((args, dict(kwargs)))
        self.log.events.append(_Open(number, args, dict(kwargs)))
        if self.error is not None:
            raise self.error
        return _FakeStream(self, number, kwargs)


class _FakeStream:
    """A stream of `_FakeReplication`: each `fetchone()` takes the next item of the script, sets
    `log_file` and `log_pos` from it and returns its event, then `None` once the script is
    exhausted."""

    def __init__(self, fake: _FakeReplication, number: int, kwargs: Mapping[str, object]) -> None:
        self._fake = fake
        self._number = number
        self._items = list(fake.script)
        self._fetches = 0
        self.log_file = kwargs.get("log_file")
        self.log_pos = kwargs.get("log_pos")

    def fetchone(self) -> object:
        self._fetches += 1
        error = self._fake.fetch_errors.get(self._fetches)
        self._fake.log.events.append(_StreamCall(self._number, "fetchone", error is not None))
        if error is not None:
            raise error
        if not self._items:
            return None
        event, self.log_file, self.log_pos = self._items.pop(0)
        return event

    def close(self) -> None:
        error = self._fake.close_error
        self._fake.log.events.append(_StreamCall(self._number, "close", error is not None))
        if error is not None:
            raise error


class _Fakes:
    """One set of doubles over one event log: the session, PyMySQL and the stream."""

    def __init__(self) -> None:
        self.log = _Log()
        self.spark_script = _SparkScript()
        self.spark = _FakeSpark(self.log, self.spark_script)
        self.pm = _FakePyMySQL(self.log)
        self.stream = _FakeReplication(self.log)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> _Fakes:
        """Put this set's `pymysql.connect` and `BinLogStreamReader` in place."""
        monkeypatch.setattr(pymysql, "connect", self.pm.connect)
        monkeypatch.setattr(pymysqlreplication, "BinLogStreamReader", self.stream)
        return self

    def twin(self, monkeypatch: pytest.MonkeyPatch) -> _Fakes:
        """A second set in this one's initial state, put in place of this one."""
        other = _Fakes()
        other.spark_script.schema = self.spark_script.schema
        other.pm.answers = dict(self.pm.answers)
        other.stream.script = list(self.stream.script)
        return other.install(monkeypatch)


# --- Fixtures and helpers -----------------------------------------------------------------


def _refuse(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("the active session, pymysql.connect or BinLogStreamReader was used")


@pytest.fixture(autouse=True)
def _isolated(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `SparkSession.active`, `pymysql.connect` and `BinLogStreamReader` fail when called,
    except in the integration tests, which need the real ones."""
    if request.node.get_closest_marker("integration") is not None:
        return
    monkeypatch.setattr(SparkSession, "active", _refuse)
    monkeypatch.setattr(pymysql, "connect", _refuse)
    monkeypatch.setattr(pymysqlreplication, "BinLogStreamReader", _refuse)


@pytest.fixture
def fakes(monkeypatch: pytest.MonkeyPatch) -> _Fakes:
    """The doubles of one test, in their default state and in place."""
    return _Fakes().install(monkeypatch)


@pytest.fixture
def conn() -> connectors.MySQLConnection:
    """The connection of the acceptance criteria."""
    return connectors.MySQLConnection("db.internal", "shop", "ingest", PASSWORD)


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the reader returns."""
    return cast(DataFrame, fake)


def _reader(
    conn: connectors.MySQLConnection,
    fakes: _Fakes,
    table_name: str = "shop.orders",
    **arguments: Any,
) -> connectors.MySQLCDCReader:
    """A reader of `table_name` with `server_id` 4001 over the fake session, `arguments` added."""
    return connectors.MySQLCDCReader(
        table_name, 4001, conn, spark=_session(fakes.spark), **arguments
    )


def _binlog_read(conn: connectors.MySQLConnection, fakes: _Fakes, **arguments: Any) -> DataFrame:
    """A binary log read of the criteria, from `P`."""
    return _reader(conn, fakes, binlog_position=P, **arguments).read()


def _snapshot_read(conn: connectors.MySQLConnection, fakes: _Fakes, **arguments: Any) -> DataFrame:
    """A snapshot read of the criteria."""
    return _reader(conn, fakes, **arguments).read()


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _lower(options: Mapping[str, str]) -> dict[str, str]:
    """`options` with lowercase keys, to compare keys case-insensitively."""
    return {key.lower(): value for key, value in options.items()}


def _notes(error: BaseException) -> list[str]:
    """The notes added to `error`."""
    return list(getattr(error, "__notes__", []))


def _loads(log: _Log) -> list[_Load]:
    return [event for event in log.events if isinstance(event, _Load)]


def _schemas(log: _Log) -> list[_Schema]:
    return [event for event in log.events if isinstance(event, _Schema)]


def _selects(log: _Log) -> list[_SelectExpr]:
    return [event for event in log.events if isinstance(event, _SelectExpr)]


def _creates(log: _Log) -> list[_Create]:
    return [event for event in log.events if isinstance(event, _Create)]


def _opens(log: _Log) -> list[_Open]:
    return [event for event in log.events if isinstance(event, _Open)]


def _executes(log: _Log) -> list[_Execute]:
    return [event for event in log.events if isinstance(event, _Execute)]


def _statements(log: _Log) -> list[str]:
    """The statements executed, failed ones included, in order."""
    return [event.statement for event in _executes(log)]


def _closes(log: _Log) -> list[_Call]:
    """The `close()` calls of the connections, in order."""
    return [event for event in log.events if isinstance(event, _Call) and event.method == "close"]


def _stream_methods(log: _Log) -> list[str]:
    """The `fetchone()` and `close()` calls of the streams, in order."""
    return [event.method for event in log.events if isinstance(event, _StreamCall)]


def _rows(log: _Log) -> list[object]:
    """The rows of the only `createDataFrame` call."""
    (create,) = _creates(log)
    return create.rows


def _typed(value: object) -> object:
    """`value` with its type, and a datetime's offset, so that `1` and `True`, an `int` and a
    `Decimal`, or the same instant in two zones, do not pass for each other."""
    if isinstance(value, tuple):
        return tuple(_typed(item) for item in value)
    if isinstance(value, datetime):
        return (datetime, value, value.utcoffset())
    return (type(value), value)


def _typed_rows(rows: Sequence[object]) -> list[object]:
    return [_typed(row) for row in rows]


def _transcript(log: _Log, *, fetches: bool = True) -> list[str]:
    """The events as lines, without the `cursor()` calls, and without the fetches unless
    `fetches`."""
    lines: list[str] = []
    for event in log.events:
        if isinstance(event, _Connect):
            lines.append(f"connect {event.connection}")
        elif isinstance(event, _Call):
            if event.method != "cursor":
                lines.append(f"{event.connection}: {event.method}")
        elif isinstance(event, _Execute):
            lines.append(f"{event.connection}: execute {event.statement}")
        elif isinstance(event, _Fetch):
            if fetches:
                lines.append(f"{event.connection}: {event.method}")
        elif isinstance(event, _Load):
            lines.append("load")
        elif isinstance(event, _Schema):
            lines.append("schema")
        elif isinstance(event, _SelectExpr):
            lines.append("selectExpr")
        elif isinstance(event, _Create):
            lines.append("createDataFrame")
        elif isinstance(event, _Open):
            lines.append("stream")
        else:
            lines.append(f"stream {event.method}")
    return lines


def _control(log: _Log) -> list[str]:
    """The lines of `_transcript` that the control connections wrote."""
    return [
        line
        for line in _transcript(log)
        if line.startswith("connect ") or line.split(":")[0].isdigit()
    ]


def _status(log_file: str, offset: int) -> tuple[tuple[object, ...], ...]:
    """The answer of `SHOW BINARY LOG STATUS` for the bound `(log_file, offset)`."""
    return ((log_file, offset, "", "", ""),)


def _run_python(code: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run `code` in a fresh interpreter, in `cwd`."""
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _config(directory: Path, document: object, name: str = "job.json") -> Path:
    """Write `document` with `json.dumps` to `directory / name`, and return the path."""
    path = directory / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _no_session(lookups: list[str]) -> Callable[[], SparkSession]:
    """A stand-in for `SparkSession.active` that records each call in `lookups` and raises."""

    def active() -> SparkSession:
        lookups.append("active")
        raise RuntimeError("no active SparkSession")

    return active


def _hide(monkeypatch: pytest.MonkeyPatch, package: str) -> None:
    """Make `import <package>`, and of its submodules, raise `ImportError` for the rest of the
    test."""
    modules = cast("dict[str, object]", sys.modules)
    for name in [name for name in sys.modules if name.split(".")[0] == package]:
        monkeypatch.setitem(modules, name, None)
    monkeypatch.setitem(modules, package, None)


def _assert_connector_error(error: BaseException, direct: BaseException, note: str) -> None:
    """`error` is the error that building the connector directly raised, `direct`, plus `note`."""
    assert type(error) is type(direct)
    assert str(error) == str(direct)
    assert type(error.__cause__) is type(direct.__cause__)
    assert _notes(direct) == []
    assert _notes(error) == [note]


CALLBACKS: dict[str, Callable[..., object]] = {
    "ReadSource": ReadSource,
    "AppendOnly": AppendOnly,
    "FullLoad": FullLoad,
    "SCD2": SCD2,
}


# --- Import surface and the extra ---------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import MySQLCDCReader

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.mysql as mysql

assert "MySQLCDCReader" in connectors.__all__, connectors.__all__
expected = ["MySQLCDCReader", "MySQLConnection", "MySQLReader", "MySQLWriter"]
assert mysql.__all__ == expected, mysql.__all__
assert "MySQLCDCReader" not in ingestio.__all__, ingestio.__all__
"""

AT002_CODE = """
import sys

for name in ("pyspark", "pymysql", "pymysqlreplication"):
    sys.modules[name] = None
for name in (
    "pyspark",
    "pyspark.sql",
    "pymysql",
    "pymysqlreplication",
    "pymysqlreplication.row_event",
    "pymysqlreplication.event",
):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import MySQLCDCReader, MySQLConnection, MySQLReader


class _FakeSpark:
    pass


conn = MySQLConnection("db.internal", "shop", "ingest", "s3cret")
MySQLReader("shop.orders", conn, spark=_FakeSpark())
assert MySQLCDCReader.__name__ == "MySQLCDCReader"
"""


def test_at001_the_reader_is_exported_from_the_connector_surface_only(tmp_path: Path) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connector_surface_imports_without_pyspark_pymysql_and_the_library(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("package", ["pymysqlreplication", "pymysql"])
def test_at003_without_the_library_or_pymysql_building_names_the_extra(
    package: str, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _hide(monkeypatch, package)

    with pytest.raises(ImportError, match=_exactly(IMPORT_MESSAGE)) as excinfo:
        connectors.MySQLCDCReader("shop.orders", 4001, conn)
    assert isinstance(excinfo.value.__cause__, ImportError)

    with pytest.raises(ValueError, match=_exactly(TABLE_NAME_MESSAGE)):
        connectors.MySQLCDCReader("a.b.c", 4001, conn)


ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")


def test_at004_the_distribution_declares_the_mysql_cdc_extra() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "mysql-cdc" in (metadata.get_all("Provides-Extra") or [])
    requirements = [Requirement(text) for text in metadata.get_all("Requires-Dist") or []]

    cdc = [item for item in requirements if str(item.marker) == 'extra == "mysql-cdc"']
    assert len(cdc) == 2, requirements
    by_name: dict[str, Requirement] = {str(canonicalize_name(item.name)): item for item in cdc}
    assert set(by_name) == {"pymysql", "mysql-replication"}, cdc
    assert by_name["pymysql"].extras == {"rsa"}
    assert str(by_name["pymysql"].specifier) == ">=1.2.0"
    assert str(by_name["mysql-replication"].specifier) == ">=1.0.16"

    mysql = [item for item in requirements if str(item.marker) == 'extra == "mysql"']
    assert mysql, requirements
    assert all(canonicalize_name(item.name) != "mysql-replication" for item in mysql), mysql


# --- Building the reader ------------------------------------------------------------------

AT005_NAMES = ("", "orders.", ".orders", "a.b.c")


@pytest.mark.parametrize("name", AT005_NAMES)
def test_at005_a_table_name_other_than_table_or_database_table_is_rejected(
    name: str, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(TABLE_NAME_MESSAGE)):
        connectors.MySQLCDCReader(name, 4001, conn)


@pytest.mark.parametrize("name", ["orders", "shop.orders"])
def test_at005_table_and_database_table_are_accepted(
    name: str, conn: connectors.MySQLConnection
) -> None:
    connectors.MySQLCDCReader(name, 4001, conn)


AT006_IDS: list[object] = [0, -1, 4294967296, True, 1.5, "4001", None]


@pytest.mark.parametrize("server_id", AT006_IDS, ids=[repr(item) for item in AT006_IDS])
def test_at006_a_server_id_that_is_not_an_integer_in_range_is_rejected(
    server_id: object, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(SERVER_ID_MESSAGE)):
        connectors.MySQLCDCReader("shop.orders", cast(Any, server_id), conn)


@pytest.mark.parametrize("server_id", [1, 4294967295])
def test_at006_the_edge_server_ids_are_accepted(
    server_id: int, conn: connectors.MySQLConnection
) -> None:
    connectors.MySQLCDCReader("shop.orders", server_id, conn)


AT007_CASES: list[tuple[object, str]] = [
    (["binlog.000042", 1200], TUPLE_MESSAGE),
    (("binlog.000042",), TUPLE_MESSAGE),
    (("binlog.000042", 1200, 0), TUPLE_MESSAGE),
    ("binlog.000042:1200", TUPLE_MESSAGE),
    (("binlog", 4), FILE_MESSAGE),
    (("binlog.", 4), FILE_MESSAGE),
    (("binlog.00004a", 4), FILE_MESSAGE),
    ((b"binlog.000042", 4), FILE_MESSAGE),
    (("binlog.000042", 3), OFFSET_MESSAGE),
    (("binlog.000042", True), OFFSET_MESSAGE),
    (("binlog.000042", "1200"), OFFSET_MESSAGE),
    (("binlog.000042", 4.0), OFFSET_MESSAGE),
]


@pytest.mark.parametrize(
    ("position", "message"),
    [pytest.param(position, message, id=repr(position)) for position, message in AT007_CASES],
)
def test_at007_a_binlog_position_that_is_not_a_file_and_an_offset_is_rejected(
    position: object, message: str, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MySQLCDCReader("shop.orders", 4001, conn, binlog_position=cast(Any, position))


@pytest.mark.parametrize(
    "position",
    [None, ("binlog.000042", 4), ("mysql-bin.log.1000000", 123456789)],
    ids=["none", "offset-4", "dotted-name"],
)
def test_at007_none_and_file_offset_tuples_are_accepted(
    position: tuple[str, int] | None, conn: connectors.MySQLConnection
) -> None:
    connectors.MySQLCDCReader("shop.orders", 4001, conn, binlog_position=position)


@pytest.mark.parametrize("key", RESERVED_KEYS)
def test_at008_options_the_connector_sets_are_rejected_in_any_case(
    key: str, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        connectors.MySQLCDCReader("shop.orders", 4001, conn, options={key: "x"})


def test_at008_other_options_are_accepted(conn: connectors.MySQLConnection) -> None:
    connectors.MySQLCDCReader("shop.orders", 4001, conn, options=ACCEPTED_OPTIONS)


# The calls of AT-009, each fixing one more argument, and the message each raises.
AT009_STEPS: list[tuple[str, str, int, object, str]] = [
    ("table-name", "a.b.c", 0, ("x", 1), TABLE_NAME_MESSAGE),
    ("server-id", "shop.orders", 0, ("x", 1), SERVER_ID_MESSAGE),
    ("file", "shop.orders", 4001, ("x", 1), FILE_MESSAGE),
    ("offset", "shop.orders", 4001, ("binlog.000042", 1), OFFSET_MESSAGE),
    ("option", "shop.orders", 4001, P, "option 'url' is set by the connector"),
]


@pytest.mark.parametrize(
    ("table_name", "server_id", "position", "message"),
    [pytest.param(*step[1:], id=step[0]) for step in AT009_STEPS],
)
def test_at009_the_arguments_are_checked_in_the_order_of_the_table(
    table_name: str,
    server_id: int,
    position: object,
    message: str,
    conn: connectors.MySQLConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MySQLCDCReader(
            table_name,
            server_id,
            conn,
            binlog_position=cast(Any, position),
            options={"url": "x"},
        )


def test_at010_building_touches_neither_spark_nor_mysql_nor_the_binary_log(
    fakes: _Fakes, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))

    connectors.MySQLCDCReader("shop.orders", 4001, conn)
    connectors.MySQLCDCReader("shop.orders", 4001, conn, binlog_position=P)
    connectors.MySQLCDCReader("orders", 4001, conn, options={"fetchsize": 500})
    connectors.MySQLCDCReader("shop.orders", 4001, conn, spark=_session(fakes.spark))
    connectors.MySQLCDCReader(
        "shop.orders",
        4001,
        conn,
        binlog_position=P,
        options={"fetchsize": 500},
        spark=_session(fakes.spark),
    )

    assert lookups == []
    assert fakes.pm.calls == []
    assert fakes.stream.calls == []
    assert fakes.log.events == []
    assert fakes.log.spark_attributes == []


def test_at011_repr_shows_what_identifies_the_reader_on_the_server(
    conn: connectors.MySQLConnection,
) -> None:
    shown = (
        "MySQLCDCReader(table_name='shop.orders', server_id=4001, host='db.internal',"
        " database='shop')"
    )
    plain = connectors.MySQLCDCReader("shop.orders", 4001, conn)
    full = connectors.MySQLCDCReader(
        "shop.orders", 4001, conn, binlog_position=P, options={"fetchsize": 500}
    )

    for reader in (plain, full):
        assert repr(reader) == shown
        assert str(reader) == shown


# --- Server checks ------------------------------------------------------------------------


def test_at012_a_binary_log_read_checks_the_server_on_one_connection_then_closes_it(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    _binlog_read(conn, fakes)

    assert fakes.pm.calls == [((), CONNECT_ARGS)]
    assert _control(fakes.log) == [
        "connect 1",
        f"1: execute {V}",
        "1: fetchone",
        f"1: execute {LOGS}",
        "1: fetchall",
        f"1: execute {STATUS}",
        "1: fetchone",
        "1: close",
    ]
    for execute in _executes(fakes.log):
        assert (execute.args, execute.kwargs) == ((), {}), execute


def test_at012_a_snapshot_read_runs_the_variables_and_the_status_only(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    _snapshot_read(conn, fakes)

    assert fakes.pm.calls == [((), CONNECT_ARGS)]
    assert _control(fakes.log) == [
        "connect 1",
        f"1: execute {V}",
        "1: fetchone",
        f"1: execute {STATUS}",
        "1: fetchone",
        "1: close",
    ]


@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
def test_at012_each_read_opens_its_own_connection(
    position: tuple[str, int] | None, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    reader = _reader(conn, fakes, binlog_position=position)

    reader.read()
    reader.read()

    assert fakes.pm.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]
    opened_and_closed = [
        line
        for line in _control(fakes.log)
        if line.startswith("connect ") or line.endswith("close")
    ]
    assert opened_and_closed == ["connect 1", "1: close", "connect 2", "2: close"]


AT013_ROWS: list[tuple[str, tuple[object, ...], str]] = [
    (
        "log-bin",
        (0, "ROW", "FULL", "FULL", "", 0),
        "the server's log_bin is '0'; MySQLCDCReader needs '1'",
    ),
    (
        "binlog-format",
        (1, "MIXED", "FULL", "FULL", "", 0),
        "the server's binlog_format is 'MIXED'; MySQLCDCReader needs 'ROW'",
    ),
    (
        "binlog-row-image",
        (1, "ROW", "MINIMAL", "FULL", "", 0),
        "the server's binlog_row_image is 'MINIMAL'; MySQLCDCReader needs 'FULL'",
    ),
    (
        "binlog-row-metadata",
        (1, "ROW", "FULL", "MINIMAL", "", 0),
        "the server's binlog_row_metadata is 'MINIMAL'; MySQLCDCReader needs 'FULL'",
    ),
    (
        "binlog-row-value-options",
        (1, "ROW", "FULL", "FULL", "PARTIAL_JSON", 0),
        "the server's binlog_row_value_options is 'PARTIAL_JSON'; MySQLCDCReader needs ''",
    ),
    (
        "binlog-transaction-compression",
        (1, "ROW", "FULL", "FULL", "", 1),
        "the server's binlog_transaction_compression is '1'; MySQLCDCReader needs '0'",
    ),
    (
        "all-wrong",
        (0, "MIXED", "MINIMAL", "MINIMAL", "PARTIAL_JSON", 1),
        "the server's log_bin is '0'; MySQLCDCReader needs '1'",
    ),
]


@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
@pytest.mark.parametrize(
    ("row", "message"), [pytest.param(row, message, id=name) for name, row, message in AT013_ROWS]
)
def test_at013_a_server_setting_the_reader_cannot_follow_fails_the_read(
    position: tuple[str, int] | None,
    row: tuple[object, ...],
    message: str,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    fakes.pm.answers[V] = (row,)

    with pytest.raises(ValueError, match=_exactly(message)):
        _reader(conn, fakes, binlog_position=position).read()

    assert _statements(fakes.log) == [V]
    assert _loads(fakes.log) == []
    assert fakes.stream.calls == []
    assert _closes(fakes.log) == [_Call(1, "close")]


@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
def test_at013_the_settings_are_compared_as_text_ignoring_case(
    position: tuple[str, int] | None, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.pm.answers[V] = (("1", "row", "full", "Full", "", "0"),)

    _reader(conn, fakes, binlog_position=position).read()

    assert _statements(fakes.log)[-1] == STATUS
    assert len(_loads(fakes.log)) == 1


AT014_CASES: list[tuple[tuple[str, int], str]] = [
    (
        ("binlog.000041", 1001),
        "binlog_position ('binlog.000041', 1001) is past the end of 'binlog.000041', which is"
        " 1000 bytes long",
    ),
    (
        ("binlog.000040", 4),
        "binlog_position ('binlog.000040', 4) has been purged from the server; its oldest binary"
        " log is 'binlog.000041'",
    ),
    (
        ("binlog.000043", 4),
        "binlog_position ('binlog.000043', 4) names a binary log the server does not have; its"
        " binary logs are 'binlog.000041' to 'binlog.000042'",
    ),
    (
        ("mysql-bin.000040", 4),
        "binlog_position ('mysql-bin.000040', 4) names a binary log the server does not have; its"
        " binary logs are 'binlog.000041' to 'binlog.000042'",
    ),
]


@pytest.mark.parametrize(
    ("position", "message"),
    [pytest.param(position, message, id=repr(position)) for position, message in AT014_CASES],
)
def test_at014_a_position_the_server_does_not_have_fails_the_read(
    position: tuple[str, int],
    message: str,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        _reader(conn, fakes, binlog_position=position).read()

    assert _statements(fakes.log) == [V, LOGS]
    assert _loads(fakes.log) == []
    assert fakes.stream.calls == []
    assert _closes(fakes.log) == [_Call(1, "close")]


@pytest.mark.parametrize(
    "position", [("binlog.000041", 4), ("binlog.000041", 1000)], ids=["start", "end-of-file"]
)
def test_at014_a_position_inside_a_listed_file_or_at_its_end_passes_the_check(
    position: tuple[str, int], fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    _reader(conn, fakes, binlog_position=position).read()

    assert _statements(fakes.log) == [V, LOGS, STATUS]
    assert len(_loads(fakes.log)) == 1


@pytest.mark.parametrize("statement", [V, LOGS, STATUS], ids=["variables", "logs", "status"])
def test_at015_a_failed_statement_propagates_itself_and_the_connection_is_closed(
    statement: str, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
    fakes.pm.execute_errors[statement] = err

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err
    assert _notes(err) == []
    assert _closes(fakes.log) == [_Call(1, "close")]


@pytest.mark.parametrize("statement", [V, LOGS, STATUS], ids=["variables", "logs", "status"])
def test_at015_a_failed_close_after_a_failed_statement_becomes_a_note(
    statement: str, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
    fakes.pm.execute_errors[statement] = err
    fakes.pm.method_errors["close"] = ValueError("boom")

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err
    assert _notes(err) == ["the control connection could not be closed: ValueError: boom"]


# --- The table's schema -------------------------------------------------------------------


@pytest.mark.parametrize("options", [None, {"fetchsize": 500}], ids=["defaults", "options"])
@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
def test_at016_the_table_is_loaded_once_as_mysql_reader_loads_it_after_the_checks(
    position: tuple[str, int] | None,
    options: dict[str, str | int | float | bool] | None,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reader(conn, fakes, binlog_position=position, options=options).read()

    (load,) = _loads(fakes.log)
    twin = fakes.twin(monkeypatch)
    connectors.MySQLReader("shop.orders", conn, options=options, spark=_session(twin.spark)).read()
    (twin_load,) = _loads(twin.log)
    assert load.source == twin_load.source
    assert _lower(load.options) == _lower(twin_load.options)
    assert _schemas(fakes.log) == [_Schema(load.frame)]
    lines = _transcript(fakes.log)
    assert lines.index("load") > lines.index("1: close"), lines


METADATA_CLASHES = (
    "_INGESTIO_Operation",
    "_ingestio_binlog_file",
    "_ingestio_binlog_offset",
    "_Ingestio_Commit_Time",
    "_ingestio_sequence",
)


@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
@pytest.mark.parametrize("name", METADATA_CLASHES)
def test_at017_a_table_column_named_as_a_metadata_column_fails_the_read(
    position: tuple[str, int] | None,
    name: str,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    fakes.spark_script.schema = StructType([*TABLE.fields, StructField(name, StringType(), True)])

    message = f"the table already has a column '{name}', which MySQLCDCReader adds to every event"
    with pytest.raises(ValueError, match=_exactly(message)):
        _reader(conn, fakes, binlog_position=position).read()

    assert fakes.stream.calls == []
    assert _selects(fakes.log) == []
    assert _creates(fakes.log) == []


# --- Snapshot -----------------------------------------------------------------------------

SNAPSHOT_EXPRS = (
    "*",
    "'snapshot' AS _ingestio_operation",
    "'binlog.000042' AS _ingestio_binlog_file",
    "CAST(5000 AS BIGINT) AS _ingestio_binlog_offset",
    "CAST(NULL AS TIMESTAMP) AS _ingestio_commit_time",
    "CAST(0 AS BIGINT) AS _ingestio_sequence",
)


def test_at018_a_snapshot_is_the_table_frame_with_the_captured_position(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    frame = _snapshot_read(conn, fakes)

    (load,) = _loads(fakes.log)
    (select,) = _selects(fakes.log)
    assert select.frame == load.frame
    assert select.exprs == SNAPSHOT_EXPRS
    assert frame is _frame(fakes.log.frames[select.result])
    assert fakes.stream.calls == []
    assert _creates(fakes.log) == []
    assert _transcript(fakes.log, fetches=False) == [
        "connect 1",
        f"1: execute {V}",
        f"1: execute {STATUS}",
        "1: close",
        "load",
        "schema",
        "selectExpr",
    ]


def test_at018_the_file_of_the_position_is_a_spark_sql_string_literal(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.pm.answers[STATUS] = _status("bin'log\\x.000042", 5000)

    _snapshot_read(conn, fakes)

    (select,) = _selects(fakes.log)
    assert select.exprs[2] == "'bin\\'log\\\\x.000042' AS _ingestio_binlog_file"


# --- Reading the binary log ---------------------------------------------------------------


def test_at019_the_stream_starts_at_the_position_and_asks_for_the_tables_row_and_commit_events(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    _binlog_read(conn, fakes)

    (stream,) = _opens(fakes.log)
    assert stream.args == ()
    kwargs = dict(stream.kwargs)
    only_events = kwargs.pop("only_events")
    assert kwargs == {
        "connection_settings": SETTINGS,
        "server_id": 4001,
        "resume_stream": True,
        "log_file": "binlog.000042",
        "log_pos": 1200,
        "blocking": False,
        "only_schemas": ["shop"],
        "only_tables": ["orders"],
        "enable_logging": False,
    }
    assert "database" not in cast("dict[str, object]", kwargs["connection_settings"])
    for name in ("resume_stream", "blocking", "enable_logging"):
        assert type(kwargs[name]) is bool, name
    assert type(kwargs["server_id"]) is int
    assert isinstance(only_events, list)
    assert len(only_events) == len(LIBRARY_EVENTS)
    assert set(only_events) == LIBRARY_EVENTS


@pytest.mark.parametrize(
    ("table_name", "schemas", "tables"),
    [("orders", ["shop"], ["orders"]), ("crm.orders", ["crm"], ["orders"])],
)
def test_at019_the_stream_filters_on_the_tables_database_and_name(
    table_name: str,
    schemas: list[str],
    tables: list[str],
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    _reader(conn, fakes, table_name, binlog_position=P).read()

    (stream,) = _opens(fakes.log)
    assert stream.kwargs["only_schemas"] == schemas
    assert stream.kwargs["only_tables"] == tables


def test_at020_a_position_at_the_bound_opens_no_stream_and_gives_no_event(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.pm.answers[STATUS] = _status("binlog.000042", 1200)

    frame = _binlog_read(conn, fakes)

    assert fakes.stream.calls == []
    (create,) = _creates(fakes.log)
    assert create.rows == []
    assert create.schema == EVENTS_SCHEMA
    assert frame is _frame(fakes.log.frames[create.result])


def test_at021_a_transaction_becomes_events_stamped_with_its_commit(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    frame = _binlog_read(conn, fakes)

    (create,) = _creates(fakes.log)
    assert create.data_type is list
    assert all(isinstance(row, tuple) for row in create.rows), create.rows
    assert _typed_rows(create.rows) == _typed_rows([AT021_ROW])
    assert isinstance(create.schema, StructType)
    assert create.schema == EVENTS_SCHEMA
    assert create.schema.fields[-5:] == META
    assert (create.args, create.kwargs) == ((), {})
    assert frame is _frame(fakes.log.frames[create.result])
    assert _stream_methods(fakes.log) == ["fetchone"] * 4 + ["close"]
    lines = _transcript(fakes.log)
    assert lines.index("createDataFrame") > lines.index("stream close"), lines


def test_at022_an_update_carries_the_row_after_it_and_a_delete_the_row_before_it(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = [
        _at(_q("BEGIN"), 1400),
        _at(_w([_values(1, "a", A), _values(4, "d", None)]), 1500),
        _at(
            _u(
                [
                    {
                        "before_values": {"id": 2, "name": "b", "updated_at": A},
                        "after_values": {"id": 2, "name": "c", "updated_at": A},
                    }
                ]
            ),
            1600,
        ),
        _at(_d([_values(3, "x", None)]), 1700),
        _at(_x(_ts(T)), 1800),
    ]

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows(
        [
            (1, "a", A_UTC, "insert", FILE, 1800, T, 1),
            (4, "d", None, "insert", FILE, 1800, T, 2),
            (2, "c", A_UTC, "update", FILE, 1800, T, 3),
            (3, "x", None, "delete", FILE, 1800, T, 4),
        ]
    )


def test_at023_each_transaction_carries_its_own_commit_and_the_sequence_runs_across_them(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = [
        *_default_transaction(),
        _at(_q("BEGIN", _ts(T)), 1800),
        _at(_d([_values(1, "a", A)]), 1900),
        _at(_x(_ts(T2)), 2000),
        _at(_q("BEGIN", _ts(T2)), 2100),
        _at(_w([_values(7, "g", None)]), 2200),
        _at(_q("COMMIT", _ts(T3)), 2300),
    ]

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows(
        [
            (1, "a", A_UTC, "insert", FILE, 1600, T, 1),
            (1, "a", A_UTC, "delete", FILE, 2000, T2, 2),
            (7, "g", None, "insert", FILE, 2300, T3, 3),
        ]
    )


# The rows of AT-024: the primary key, the values before and after, and the rows expected.
AT024_CASES: list[tuple[str, str | tuple[str, ...], dict[str, Any], dict[str, Any], list[Any]]] = [
    (
        "key-changed",
        "id",
        {"id": 2, "name": "b", "updated_at": None},
        {"id": 5, "name": "b", "updated_at": None},
        [(2, "b", None, "delete"), (5, "b", None, "insert")],
    ),
    (
        "key-kept",
        "id",
        {"id": 2, "name": "b", "updated_at": None},
        {"id": 2, "name": "c", "updated_at": None},
        [(2, "c", None, "update")],
    ),
    (
        "composite-key-changed",
        ("id", "name"),
        {"id": 2, "name": "b", "updated_at": None},
        {"id": 2, "name": "c", "updated_at": None},
        [(2, "b", None, "delete"), (2, "c", None, "insert")],
    ),
    (
        "no-key",
        "",
        {"id": 2, "name": "b", "updated_at": None},
        {"id": 5, "name": "c", "updated_at": None},
        [(5, "c", None, "update")],
    ),
]


@pytest.mark.parametrize(
    ("primary_key", "before", "after", "expected"),
    [pytest.param(*case[1:], id=case[0]) for case in AT024_CASES],
)
def test_at024_an_update_that_changes_the_primary_key_is_a_delete_then_an_insert(
    primary_key: str | tuple[str, ...],
    before: dict[str, Any],
    after: dict[str, Any],
    expected: list[tuple[object, ...]],
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    event = _u([{"before_values": before, "after_values": after}], primary_key)
    fakes.stream.script = _transaction(event)

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows(
        [(*row, FILE, 1600, T, number) for number, row in enumerate(expected, start=1)]
    )


@pytest.mark.parametrize("text", ["COMMIT", " commit "])
def test_at025_a_query_event_commit_commits_the_transaction(
    text: str, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = _transaction(_w([_values(1, "a", A)]), _q(text, _ts(T)))

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows([AT021_ROW])


def test_at025_a_ddl_statement_outside_a_transaction_is_skipped(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = [
        _at(_q("ALTER TABLE `shop`.`orders` ADD INDEX (name)"), 1250),
        *_default_transaction(),
    ]

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows([AT021_ROW])


@pytest.mark.parametrize("text", ["ROLLBACK", "XA END X'01'", "BEGIN"])
def test_at026_another_query_event_while_rows_are_buffered_fails_the_read(
    text: str, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = _transaction(_w([_values(1, "a", A)]), _q(text))

    message = (
        "the binary log has rows of the table at ('binlog.000042', 1600) that no commit follows"
    )
    with pytest.raises(ValueError, match=_exactly(message)):
        _binlog_read(conn, fakes)

    assert _creates(fakes.log) == []
    assert _stream_methods(fakes.log).count("close") == 1


def test_at027_the_read_stops_at_the_bound(fakes: _Fakes, conn: connectors.MySQLConnection) -> None:
    fakes.pm.answers[STATUS] = _status("binlog.000042", 1600)
    fakes.stream.script = [
        *_default_transaction(),
        _at(_q("BEGIN"), 1800),
        _at(_w([_values(9, "z", None)]), 1900),
        _at(_x(_ts(T)), 2000),
    ]

    _binlog_read(conn, fakes)

    assert _stream_methods(fakes.log) == ["fetchone"] * 3 + ["close"]
    assert _typed_rows(_rows(fakes.log)) == _typed_rows([AT021_ROW])


def test_at028_binary_log_files_are_ordered_by_their_number(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.pm.answers[LOGS] = (("binlog.999999", 9000, "No"), ("binlog.1000000", 800, "No"))
    fakes.pm.answers[STATUS] = _status("binlog.1000000", 600)
    fakes.stream.script = [
        _at(_w([_values(1, "a", None)]), 1500, "binlog.999999"),
        _at(_x(_ts(T)), 1600, "binlog.999999"),
        _at(_w([_values(2, "b", None)]), 450, "binlog.1000000"),
        _at(_x(_ts(T)), 600, "binlog.1000000"),
        _at(_w([_values(3, "c", None)]), 750, "binlog.1000000"),
        _at(_x(_ts(T)), 800, "binlog.1000000"),
    ]

    _reader(conn, fakes, binlog_position=("binlog.999999", 1200)).read()

    assert _typed_rows(_rows(fakes.log)) == _typed_rows(
        [
            (1, "a", None, "insert", "binlog.999999", 1600, T, 1),
            (2, "b", None, "insert", "binlog.1000000", 600, T, 2),
        ]
    )
    assert _stream_methods(fakes.log) == ["fetchone"] * 4 + ["close"]


def test_at029_rows_that_no_commit_follows_before_the_end_are_dropped(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = [
        *_default_transaction(),
        _at(_q("BEGIN"), 1800),
        _at(_w([_values(9, "z", None)]), 1900),
    ]

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows([AT021_ROW])


@pytest.mark.parametrize(
    "values",
    [
        {"ID": 1, "Name": "a", "updated_at": A},
        {"id": 1, "name": "a", "updated_at": A, "my_row_id": 77},
    ],
    ids=["other-case", "extra-column"],
)
def test_at030_columns_are_matched_ignoring_case_and_the_events_extra_ones_left_out(
    values: dict[str, Any], fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = _transaction(_w([{"values": values}]))

    _binlog_read(conn, fakes)

    assert _typed_rows(_rows(fakes.log)) == _typed_rows([AT021_ROW])


def test_at030_a_column_of_the_schema_missing_from_the_event_fails_the_read(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    fakes.stream.script = _transaction(_w([{"values": {"id": 1, "name": "a"}}]))

    message = (
        "the event of the table at ('binlog.000042', 1500) has no column 'updated_at'; the"
        " table's columns changed since it was written"
    )
    with pytest.raises(ValueError, match=_exactly(message)):
        _binlog_read(conn, fakes)

    assert _stream_methods(fakes.log).count("close") == 1


def _one_column(spark_type: DataType) -> StructType:
    """The table schema of AT-031 and AT-032: one nullable field `c` of `spark_type`."""
    return StructType([StructField("c", spark_type, True)])


AT031_TYPES: list[DataType] = [
    BooleanType(),
    ByteType(),
    IntegerType(),
    LongType(),
    DecimalType(10, 2),
    DecimalType(20, 0),
    DoubleType(),
    FloatType(),
    StringType(),
    BinaryType(),
    DateType(),
    TimestampType(),
    TimestampNTZType(),
]

# The rows of AT-031: an id, the field's Spark type, the value of the event and the value of the
# DataFrame.
AT031_CASES: list[tuple[str, DataType, object, object]] = [
    *[(f"none-{item.simpleString()}", item, None, None) for item in AT031_TYPES],
    ("boolean-true", BooleanType(), True, True),
    ("boolean-int-1", BooleanType(), 1, True),
    ("boolean-int-0", BooleanType(), 0, False),
    ("boolean-int-2", BooleanType(), 2, True),
    ("boolean-bits-1", BooleanType(), "1", True),
    ("boolean-bits-0", BooleanType(), "0", False),
    ("tinyint", ByteType(), 5, 5),
    ("int", IntegerType(), -7, -7),
    ("bigint", LongType(), 2**63 - 1, 2**63 - 1),
    ("bigint-bits", LongType(), "1010", 10),
    ("decimal", DecimalType(10, 2), Decimal("12.50"), Decimal("12.50")),
    (
        "decimal-unsigned-bigint",
        DecimalType(20, 0),
        18446744073709551615,
        Decimal("18446744073709551615"),
    ),
    ("double", DoubleType(), 1.5, 1.5),
    ("float", FloatType(), 0.25, 0.25),
    ("string", StringType(), "é", "é"),
    ("string-set", StringType(), {"b", "a"}, "a,b"),
    ("string-json-object", StringType(), {"k": [1, "é"]}, '{"k": [1, "é"]}'),
    ("string-json-true", StringType(), True, "true"),
    ("string-json-int", StringType(), 3, "3"),
    ("string-json-float", StringType(), 1.5, "1.5"),
    ("binary", BinaryType(), b"\x00\x01", b"\x00\x01"),
    ("binary-bits", BinaryType(), "000100000001", b"\x01\x01"),
    ("date", DateType(), date(2026, 10, 24), date(2026, 10, 24)),
    ("date-year", DateType(), 2026, date(2026, 1, 1)),
    ("timestamp-naive", TimestampType(), A, A_UTC),
    (
        "timestamp-aware",
        TimestampType(),
        datetime(2026, 10, 24, 8, 0, tzinfo=timezone(timedelta(hours=-3))),
        datetime(2026, 10, 24, 11, 0, tzinfo=UTC),
    ),
    (
        "timestamp-time",
        TimestampType(),
        timedelta(hours=8, minutes=30),
        datetime(1970, 1, 1, 8, 30, tzinfo=UTC),
    ),
    ("timestamp-ntz", TimestampNTZType(), A, A),
    (
        "timestamp-ntz-time",
        TimestampNTZType(),
        timedelta(hours=8, minutes=30),
        datetime(1970, 1, 1, 8, 30),  # noqa: DTZ001 - a value of a timestamp_ntz field
    ),
]


@pytest.mark.parametrize(
    ("spark_type", "value", "expected"),
    [pytest.param(*case[1:], id=case[0]) for case in AT031_CASES],
)
def test_at031_values_are_converted_for_the_type_of_their_field(
    spark_type: DataType,
    value: object,
    expected: object,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    fakes.spark_script.schema = _one_column(spark_type)
    fakes.stream.script = _transaction(_w([{"values": {"c": value}}]))

    _binlog_read(conn, fakes)

    (row,) = _rows(fakes.log)
    assert _typed(row) == _typed((expected, "insert", FILE, 1600, T, 1))


# The rows of AT-032: an id, the field's Spark type, the value, and the names in the message.
AT032_CASES: list[tuple[str, DataType, object, str, str]] = [
    ("string-bytes", StringType(), b"x", "bytes", "string"),
    ("bigint-text", LongType(), "12", "str", "bigint"),
    ("bigint-bool", LongType(), True, "bool", "bigint"),
    ("boolean-text", BooleanType(), "yes", "str", "boolean"),
    ("date-text", DateType(), "2026-10-24", "str", "date"),
    (
        "date-datetime",
        DateType(),
        datetime(2026, 10, 24),  # noqa: DTZ001 - the naive datetime under test
        "datetime",
        "date",
    ),
    ("decimal-float", DecimalType(10, 2), 1.5, "float", "decimal(10,2)"),
    ("timestamp-date", TimestampType(), date(2026, 10, 24), "date", "timestamp"),
]


def _conversion_message(python_type: str, spark_type: str) -> str:
    return (
        f"column 'c' of the event at ('binlog.000042', 1500): cannot turn a {python_type}"
        f" into {spark_type}"
    )


@pytest.mark.parametrize(
    ("spark_type", "value", "python_type", "spark_name"),
    [pytest.param(*case[1:], id=case[0]) for case in AT032_CASES],
)
def test_at032_a_value_the_table_cannot_convert_fails_the_read(
    spark_type: DataType,
    value: object,
    python_type: str,
    spark_name: str,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
) -> None:
    fakes.spark_script.schema = _one_column(spark_type)
    fakes.stream.script = _transaction(_w([{"values": {"c": value}}]))

    with pytest.raises(ValueError, match=_exactly(_conversion_message(python_type, spark_name))):
        _binlog_read(conn, fakes)

    assert _stream_methods(fakes.log).count("close") == 1


def test_at033_a_failed_fetch_propagates_itself_and_the_stream_is_closed(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = pymysql.err.OperationalError(1236, "Could not find first log file name")
    fakes.stream.fetch_errors[2] = err

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err
    assert _notes(err) == []
    assert _stream_methods(fakes.log) == ["fetchone", "fetchone", "close"]


def test_at033_a_failed_close_after_a_failed_fetch_becomes_a_note(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = pymysql.err.OperationalError(1236, "Could not find first log file name")
    fakes.stream.fetch_errors[2] = err
    fakes.stream.close_error = ValueError("boom")

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err
    assert _notes(err) == ["the binary log stream could not be closed: ValueError: boom"]


@pytest.mark.parametrize("failing", ["stream", "createDataFrame", "load"])
def test_at033_an_error_of_the_stream_or_of_spark_propagates_itself(
    failing: str, fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = RuntimeError(f"{failing} failed")
    if failing == "stream":
        fakes.stream.error = err
    elif failing == "createDataFrame":
        fakes.spark_script.create_error = err
    else:
        fakes.spark_script.load_error = err

    with pytest.raises(RuntimeError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err


def test_at033_a_failed_close_after_the_loop_propagates_and_builds_no_dataframe(
    fakes: _Fakes, conn: connectors.MySQLConnection
) -> None:
    err = pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query")
    fakes.stream.close_error = err

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        _binlog_read(conn, fakes)

    assert excinfo.value is err
    assert _creates(fakes.log) == []


@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
def test_at034_the_active_session_is_looked_up_when_read_runs(
    position: tuple[str, int] | None,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    reader = connectors.MySQLCDCReader("shop.orders", 4001, conn, binlog_position=position)
    assert lookups == []

    def fake_session() -> SparkSession:
        lookups.append("fake session")
        return _session(fakes.spark)

    monkeypatch.setattr(SparkSession, "active", fake_session)
    reader.read()

    assert "fake session" in lookups
    assert len(_loads(fakes.log)) == 1


def test_at034_without_an_active_session_the_error_of_pyspark_propagates_before_mysql(
    fakes: _Fakes, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    err = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise err

    reader = connectors.MySQLCDCReader("shop.orders", 4001, conn, binlog_position=P)
    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()

    assert excinfo.value is err
    assert fakes.pm.calls == []


# --- In a configuration -------------------------------------------------------------------


@pytest.mark.parametrize("options", [None, {"fetchsize": 500}], ids=["defaults", "options"])
@pytest.mark.parametrize("position", [P, None], ids=["binlog", "snapshot"])
def test_at035_read_source_builds_the_reader_and_reads_as_its_twin(
    position: tuple[str, int] | None,
    options: dict[str, str | int | float | bool] | None,
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    section = DEFAULT_SOURCE if options is None else {**DEFAULT_SOURCE, "options": options}
    path = _config(tmp_path, {"source": section})

    extract = ReadSource(
        path, connection=conn, binlog_position=position, spark=_session(fakes.spark)
    )

    assert lookups == []
    assert fakes.pm.calls == []
    assert fakes.stream.calls == []
    assert fakes.log.events == []

    process = Process([Step("extract", [extract])]).run()

    if position is None:
        (returned,) = [event.result for event in _selects(fakes.log)]
    else:
        (returned,) = [event.result for event in _creates(fakes.log)]
    assert process.state["records"] is fakes.log.frames[returned]
    twin = fakes.twin(monkeypatch)
    connectors.MySQLCDCReader(
        "shop.orders",
        4001,
        conn,
        binlog_position=position,
        options=options,
        spark=_session(twin.spark),
    ).read()
    assert twin.log.events, "the twin recorded nothing"
    assert fakes.log.events == twin.log.events
    assert lookups == []


def _section_text(server_id: str) -> str:
    """The default `source` as JSON text, with `server_id` written as the JSON text given."""
    return json.dumps({"source": {**DEFAULT_SOURCE, "server_id": "<id>"}}).replace(
        '"<id>"', server_id
    )


# The rows of AT-036: an id, the configuration file as JSON text, and the message after
# `<path>: `.
AT036_ROWS: list[tuple[str, str, str]] = [
    (
        "without-table-name",
        json.dumps({"source": {"system": "mysql_cdc", "server_id": 4001}}),
        "source.table_name is required for system 'mysql_cdc'",
    ),
    (
        "without-server-id",
        json.dumps({"source": {"system": "mysql_cdc", "table_name": "shop.orders"}}),
        "source.server_id is required for system 'mysql_cdc'",
    ),
    *[
        (
            f"key-{key}",
            json.dumps({"source": {**DEFAULT_SOURCE, key: "updated_at"}}),
            f"source.{key} is not a key of system 'mysql_cdc'",
        )
        for key in (
            "watermark_column",
            "binlog_position",
            "host",
            "database",
            "user",
            "password",
            "port",
            "ssl_mode",
            "connection",
            "fetchsize",
        )
    ],
    *[
        (f"server-id-{text}", _section_text(text), "source.server_id must be an integer")
        for text in ('"4001"', "4001.0", "4e3", "true", "null", "[4001]")
    ],
    (
        "number-table-name",
        json.dumps({"source": {**DEFAULT_SOURCE, "table_name": 1}}),
        "source.table_name must be a string",
    ),
    (
        "array-option",
        json.dumps({"source": {**DEFAULT_SOURCE, "options": {"fetchsize": [1]}}}),
        "source.options must be an object of strings, numbers and booleans",
    ),
]


@pytest.mark.parametrize(
    ("text", "message"),
    [pytest.param(text, message, id=name) for name, text, message in AT036_ROWS],
)
def test_at036_a_mysql_cdc_section_with_wrong_keys_is_rejected(
    text: str, message: str, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = tmp_path / "job.json"
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        ReadSource(path, connection=conn)


def _at037_arguments(kind: str, conn: connectors.MySQLConnection) -> dict[str, object]:
    """The callback arguments that a row of AT-037 gives."""
    rows: dict[str, dict[str, object]] = {
        "none": {},
        "client": {"connection": conn, "client": object()},
        "watermark-value": {"connection": conn, "watermark_value": 41},
        "postgres-connection": {
            "connection": PostgresConnection("db.internal", "shop", "ingest", "pw")
        },
        "mongodb-connection": {"connection": MongoDBConnection("mongodb://db.internal")},
    }
    return rows[kind]


AT037_ROWS: list[tuple[str, str]] = [
    ("none", "source system 'mysql_cdc' needs the argument connection"),
    ("client", "source system 'mysql_cdc' takes no argument client"),
    ("watermark-value", "source system 'mysql_cdc' takes no argument watermark_value"),
    (
        "postgres-connection",
        "source system 'mysql_cdc' needs a MySQLConnection as the argument connection,"
        " not PostgresConnection",
    ),
    (
        "mongodb-connection",
        "source system 'mysql_cdc' needs a MySQLConnection as the argument connection,"
        " not MongoDBConnection",
    ),
]


@pytest.mark.parametrize(("kind", "message"), [pytest.param(*row, id=row[0]) for row in AT037_ROWS])
def test_at037_the_callback_arguments_of_mysql_cdc_are_checked(
    kind: str, message: str, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        ReadSource(path, **cast(Any, _at037_arguments(kind, conn)))


# The other source systems of AT-037, each with its required keys.
OTHER_SOURCES: dict[str, dict[str, object]] = {
    "mysql": {"system": "mysql", "table_name": "shop.orders"},
    "postgres": {"system": "postgres", "table_name": "public.orders"},
    "disk": {"system": "disk", "path": "/data/landing/orders", "format": "parquet"},
    "unity_catalog": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
    "json_api": {"system": "json_api", "url": "https://api.example.com/v1/orders"},
}


def _other_connection(system: str, conn: connectors.MySQLConnection) -> object | None:
    """The `connection` that the source system of AT-037 needs, if any."""
    if system == "mysql":
        return conn
    if system == "postgres":
        return PostgresConnection("db.internal", "shop", "ingest", "pw")
    return None


@pytest.mark.parametrize("system", list(OTHER_SOURCES))
def test_at037_another_source_system_takes_no_binlog_position(
    system: str, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": OTHER_SOURCES[system]})
    connection = cast(Any, _other_connection(system, conn))

    message = f"{path}: source system '{system}' takes no argument binlog_position"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path, connection=connection, binlog_position=P)

    ReadSource(path, connection=connection, binlog_position=None)


def test_at037_the_client_of_a_mysql_source_is_reported_before_its_binlog_position(
    conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": OTHER_SOURCES["mysql"]})

    message = f"{path}: source system 'mysql' takes no argument client"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path, connection=conn, client=object(), binlog_position=P)


def test_at037_arguments_given_as_none_and_a_subclass_of_the_connection_are_accepted(
    tmp_path: Path, conn: connectors.MySQLConnection
) -> None:
    class _OwnConnection(connectors.MySQLConnection):
        """A job's own subclass of the connection."""

    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    ReadSource(
        path,
        connection=conn,
        client=None,
        watermark_value=None,
        binlog_position=None,
        spark=None,
    )
    ReadSource(path, connection=_OwnConnection("db.internal", "shop", "ingest", PASSWORD))


# The rows of AT-038: an id, the keys added to the default section, the `binlog_position` given
# to `ReadSource`, and the arguments of the reader built directly, after the connection.
AT038_ROWS: list[tuple[str, dict[str, object], object, tuple[object, ...], dict[str, Any]]] = [
    ("server-id-0", {"server_id": 0}, None, ("shop.orders", 0), {}),
    ("three-part-table-name", {"table_name": "a.b.c"}, None, ("a.b.c", 4001), {}),
    (
        "reserved-option",
        {"options": {"dbtable": "x"}},
        None,
        ("shop.orders", 4001),
        {"options": {"dbtable": "x"}},
    ),
    (
        "position-file",
        {},
        ("binlog", 4),
        ("shop.orders", 4001),
        {"binlog_position": ("binlog", 4)},
    ),
]

AT038_MESSAGES = {
    "server-id-0": SERVER_ID_MESSAGE,
    "three-part-table-name": TABLE_NAME_MESSAGE,
    "reserved-option": "option 'dbtable' is set by the connector",
    "position-file": FILE_MESSAGE,
}


def _direct(
    conn: connectors.MySQLConnection, keys: tuple[object, ...], arguments: dict[str, Any]
) -> connectors.MySQLCDCReader:
    """`MySQLCDCReader(<table_name>, <server_id>, conn, **arguments)`."""
    table_name, server_id = keys
    return connectors.MySQLCDCReader(cast(str, table_name), cast(int, server_id), conn, **arguments)


@pytest.mark.parametrize("row", [pytest.param(row, id=row[0]) for row in AT038_ROWS])
def test_at038_an_error_of_the_connector_propagates_with_one_note(
    row: tuple[str, dict[str, object], object, tuple[object, ...], dict[str, Any]],
    conn: connectors.MySQLConnection,
    tmp_path: Path,
) -> None:
    name, added, position, keys, arguments = row
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, **added}})
    with pytest.raises(ValueError) as built_directly:
        _direct(conn, keys, arguments)

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        ReadSource(path, connection=conn, binlog_position=cast(Any, position))

    assert str(excinfo.value) == AT038_MESSAGES[name]
    note = f"while building the source of {path}, system 'mysql_cdc'"
    _assert_connector_error(excinfo.value, built_directly.value, note)


def test_at038_a_missing_library_raises_the_readers_import_error_with_one_note(
    conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    _hide(monkeypatch, "pymysqlreplication")
    with pytest.raises(ImportError) as built_directly:
        connectors.MySQLCDCReader("shop.orders", 4001, conn)

    with pytest.raises(ImportError) as excinfo:
        ReadSource(path, connection=conn)

    assert str(excinfo.value) == IMPORT_MESSAGE
    note = f"while building the source of {path}, system 'mysql_cdc'"
    _assert_connector_error(excinfo.value, built_directly.value, note)
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_at039_a_source_system_close_to_mysql_cdc_is_rejected_with_it_in_the_list(
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": "MySQL_CDC"}})

    message = f"{path}: source.system 'MySQL_CDC' is not one of: {SOURCE_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at039_mysql_cdc_is_not_a_target_system(
    name: str, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": CDC_TARGET})

    message = f"{path}: target.system 'mysql_cdc' is not one of: {TARGET_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        CALLBACKS[name](path, connection=conn)


def test_at039_mysql_cdc_is_not_a_target_of_scd2(
    conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": CDC_TARGET, "scd2": SCD2_SECTION})

    message = f"{path}: target.system 'mysql_cdc' is not one of: {SCD2_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path, connection=conn)


def test_at040_binlog_position_is_a_keyword_of_read_source_after_watermark_value() -> None:
    parameters = inspect.signature(ReadSource).parameters.values()

    keyword_only = [item for item in parameters if item.kind is inspect.Parameter.KEYWORD_ONLY]
    assert [item.name for item in keyword_only] == [
        "into",
        "connection",
        "client",
        "watermark_value",
        "binlog_position",
        "spark",
    ]
    (position,) = [item for item in keyword_only if item.name == "binlog_position"]
    assert position.default is None


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad", "SCD2"])
def test_at040_the_write_callbacks_take_no_binlog_position(name: str, tmp_path: Path) -> None:
    path = _config(
        tmp_path,
        {
            "source": DEFAULT_SOURCE,
            "target": {"system": "mysql", "table_name": "shop.orders"},
            "scd2": SCD2_SECTION,
        },
    )

    with pytest.raises(TypeError, match="unexpected keyword argument 'binlog_position'"):
        cast(Any, CALLBACKS[name])(path, binlog_position=P)


def test_at040_mypy_strict_accepts_a_position_in_read_source_and_the_reader_as_a_reader(
    fakes: _Fakes, tmp_path: Path
) -> None:
    """mypy strict, which checks `tests/`, accepts the annotated bindings and the calls of this
    test."""
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    conn: connectors.MySQLConnection = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD
    )

    callback: ReadSource = ReadSource(
        path, connection=conn, binlog_position=("binlog.000042", 1200)
    )
    reader: Reader[DataFrame] = connectors.MySQLCDCReader(
        "shop.orders", 4001, conn, binlog_position=P, spark=_session(fakes.spark)
    )

    frame = reader.read()
    (create,) = _creates(fakes.log)
    assert frame is _frame(fakes.log.frames[create.result])
    assert isinstance(callback, ReadSource)


# --- Logging and secrets ------------------------------------------------------------------


def test_at041_nothing_writes_a_log_record(
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    with_options = _config(
        tmp_path, {"source": {**DEFAULT_SOURCE, "options": {"fetchsize": 500}}}, "options.json"
    )
    target = _config(tmp_path, {"target": CDC_TARGET, "scd2": SCD2_SECTION}, "target.json")
    wrong = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "server_id": 0}}, "wrong.json")
    close = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": "MySQL_CDC"}}, "close.json")

    connectors.MySQLCDCReader("shop.orders", 4001, conn)
    connectors.MySQLCDCReader("orders", 1, conn, binlog_position=P, options={"fetchsize": 500})
    ReadSource(path, connection=conn, binlog_position=P, spark=_session(fakes.spark))
    ReadSource(with_options, connection=conn, spark=_session(fakes.spark))
    for build in (
        lambda: ReadSource(path),
        lambda: ReadSource(path, connection=conn, watermark_value=41),
        lambda: ReadSource(wrong, connection=conn),
        lambda: ReadSource(close, connection=conn),
        lambda: AppendOnly(target, connection=conn),
        lambda: SCD2(target, connection=conn),
    ):
        with pytest.raises(ValueError):
            build()

    _snapshot_read(conn, fakes)
    _binlog_read(conn, fakes)
    fakes.pm.answers[STATUS] = _status("binlog.000042", 1200)
    _binlog_read(conn, fakes)
    fakes.pm.answers[STATUS] = (STATUS_ROW,)
    fakes.pm.answers[V] = ((0, "ROW", "FULL", "FULL", "", 0),)
    with pytest.raises(ValueError):
        _binlog_read(conn, fakes)
    fakes.pm.answers[V] = (VARIABLES_ROW,)
    fakes.stream.fetch_errors[2] = pymysql.err.OperationalError(1236, "purged")
    with pytest.raises(pymysql.err.OperationalError):
        _binlog_read(conn, fakes)
    fakes.stream.fetch_errors.clear()
    fakes.stream.script = _transaction(_w([_values("x", "a", A)]))
    with pytest.raises(ValueError):
        _binlog_read(conn, fakes)

    # A load for each read but the one that a server check stops.
    assert len(_loads(fakes.log)) == 5
    assert caplog.records == []


def _raised(
    errors: list[BaseException], build: Callable[..., object], *args: Any, **kwargs: Any
) -> None:
    """Call `build` with the arguments, which must raise, and add its error to `errors`."""
    try:
        build(*args, **kwargs)
    except (ValueError, ImportError, TypeError, RuntimeError, pymysql.err.Error) as error:
        errors.append(error)
    else:
        raise AssertionError(f"{build} raised nothing")


@dataclass
class _Setup:
    """How the doubles of one failing read of AT-042 differ from their default state."""

    position: tuple[str, int] | None = P
    answers: dict[str, tuple[tuple[object, ...], ...]] = field(default_factory=dict)
    schema: StructType | None = None
    script: list[_Item] | None = None
    execute_errors: dict[str, BaseException] = field(default_factory=dict)
    method_errors: dict[str, BaseException] = field(default_factory=dict)
    fetch_errors: dict[int, BaseException] = field(default_factory=dict)
    stream_error: BaseException | None = None
    stream_close_error: BaseException | None = None

    def apply(self, fakes: _Fakes) -> None:
        fakes.pm.answers.update(self.answers)
        if self.schema is not None:
            fakes.spark_script.schema = self.schema
        if self.script is not None:
            fakes.stream.script = self.script
        fakes.pm.execute_errors.update(self.execute_errors)
        fakes.pm.method_errors.update(self.method_errors)
        fakes.stream.fetch_errors.update(self.fetch_errors)
        fakes.stream.error = self.stream_error
        fakes.stream.close_error = self.stream_close_error


def _failing_reads() -> list[_Setup]:
    """The failing reads of AT-013 to AT-033."""
    setups: list[_Setup] = []
    for position in (P, None):
        for _, row, _ in AT013_ROWS:
            setups.append(_Setup(position, answers={V: (row,)}))
        for name in METADATA_CLASHES:
            schema = StructType([*TABLE.fields, StructField(name, StringType(), True)])
            setups.append(_Setup(position, schema=schema))
    setups.extend(_Setup(given) for given, _ in AT014_CASES)
    for statement in (V, LOGS, STATUS):
        lost = pymysql.err.OperationalError(2013, "Lost connection")
        setups.append(_Setup(execute_errors={statement: lost}))
        setups.append(
            _Setup(
                execute_errors={statement: pymysql.err.OperationalError(2013, "Lost connection")},
                method_errors={"close": ValueError("boom")},
            )
        )
    for text in ("ROLLBACK", "XA END X'01'", "BEGIN"):
        setups.append(_Setup(script=_transaction(_w([_values(1, "a", A)]), _q(text))))
    setups.append(_Setup(script=_transaction(_w([{"values": {"id": 1, "name": "a"}}]))))
    for _, spark_type, value, _, _ in AT032_CASES:
        script = _transaction(_w([{"values": {"c": value}}]))
        setups.append(_Setup(schema=_one_column(spark_type), script=script))
    setups.append(_Setup(fetch_errors={2: RuntimeError("lost")}))
    setups.append(
        _Setup(fetch_errors={2: RuntimeError("lost")}, stream_close_error=ValueError("boom"))
    )
    setups.append(_Setup(stream_error=RuntimeError("refused")))
    setups.append(_Setup(stream_close_error=RuntimeError("close failed")))
    return setups


def _read_errors(
    conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[BaseException], list[str]]:
    """The errors that the failing reads of AT-013 to AT-033 raise, raised once more on new
    doubles, and the `url` option of every load."""
    errors: list[BaseException] = []
    urls: list[str] = []
    for setup in _failing_reads():
        fakes = _Fakes().install(monkeypatch)
        setup.apply(fakes)
        _raised(errors, _reader(conn, fakes, binlog_position=setup.position).read)
        urls.extend(_lower(load.options)["url"] for load in _loads(fakes.log))
    return errors, urls


def _criteria_errors(
    conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> list[BaseException]:
    """The errors that the builds of AT-003 to AT-040 raise, raised once more."""
    errors: list[BaseException] = []
    reader = connectors.MySQLCDCReader
    for name in AT005_NAMES:
        _raised(errors, reader, name, 4001, conn)
    for server_id in AT006_IDS:
        _raised(errors, reader, "shop.orders", server_id, conn)
    for position, _ in AT007_CASES:
        _raised(errors, reader, "shop.orders", 4001, conn, binlog_position=position)
    for key in RESERVED_KEYS:
        _raised(errors, reader, "shop.orders", 4001, conn, options={key: "x"})
    for _, table_name, server_id, position, _ in AT009_STEPS:
        _raised(
            errors,
            reader,
            table_name,
            server_id,
            conn,
            binlog_position=position,
            options={"url": "x"},
        )

    for number, (_, text, _) in enumerate(AT036_ROWS):
        path = tmp_path / f"at036-{number}.json"
        path.write_text(text, encoding="utf-8")
        _raised(errors, ReadSource, path, connection=conn)
    default = _config(tmp_path, {"source": DEFAULT_SOURCE}, "default.json")
    for kind, _ in AT037_ROWS:
        _raised(errors, ReadSource, default, **_at037_arguments(kind, conn))
    for system, section in OTHER_SOURCES.items():
        path = _config(tmp_path, {"source": section}, f"at037-{system}.json")
        connection = _other_connection(system, conn)
        _raised(errors, ReadSource, path, connection=connection, binlog_position=P)
    for number, (_, added, position, _, _) in enumerate(AT038_ROWS):
        path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, **added}}, f"at038-{number}.json")
        _raised(errors, ReadSource, path, connection=conn, binlog_position=position)
    close = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": "MySQL_CDC"}}, "close.json")
    _raised(errors, ReadSource, close)
    target = _config(tmp_path, {"target": CDC_TARGET, "scd2": SCD2_SECTION}, "target.json")
    for name in ("AppendOnly", "FullLoad", "SCD2"):
        _raised(errors, CALLBACKS[name], target, connection=conn)
        _raised(errors, CALLBACKS[name], target, binlog_position=P)

    with monkeypatch.context() as hidden:
        _hide(hidden, "pymysqlreplication")
        _raised(errors, reader, "shop.orders", 4001, conn)
        _raised(errors, ReadSource, default, connection=conn)
    with monkeypatch.context() as hidden:
        _hide(hidden, "pymysql")
        _raised(errors, reader, "shop.orders", 4001, conn)
    return errors


def test_at042_the_password_appears_in_no_repr_url_or_error(
    fakes: _Fakes,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    tls = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD, ssl_mode="VERIFY_IDENTITY"
    )
    spark = _session(fakes.spark)
    readers = [
        connectors.MySQLCDCReader("shop.orders", 4001, conn, spark=spark),
        connectors.MySQLCDCReader("orders", 1, conn, binlog_position=P, spark=spark),
        connectors.MySQLCDCReader(
            "crm.orders", 4294967295, tls, binlog_position=P, options=ACCEPTED_OPTIONS, spark=spark
        ),
        connectors.MySQLCDCReader(
            "shop.orders", 4001, conn, binlog_position=("mysql-bin.log.1000000", 123456789)
        ),
    ]
    callbacks = [
        ReadSource(path, connection=conn, binlog_position=P, spark=spark),
        ReadSource(path, connection=conn, spark=spark),
        ReadSource(path, connection=tls),
    ]
    texts = [text for value in [*readers, *callbacks] for text in (repr(value), str(value))]

    for reader in readers[:3]:
        reader.read()
    urls = [_lower(load.options)["url"] for load in _loads(fakes.log)]
    assert len(urls) == 3
    read_errors, read_urls = _read_errors(conn, monkeypatch)
    errors = [*read_errors, *_criteria_errors(conn, monkeypatch, tmp_path)]
    assert read_urls
    messages = [text for error in errors for text in (str(error), *_notes(error))]

    for text in [*texts, *urls, *read_urls, *messages]:
        assert PASSWORD not in text, text


# --- Integration --------------------------------------------------------------------------

MYSQL_URL = os.environ.get("INGESTIO_MYSQL_URL", "")
JDBC_PACKAGE = os.environ.get("INGESTIO_MYSQL_JDBC_PACKAGE", "")
# MySQL's error for a missing privilege, such as the one that `SET GLOBAL` needs.
SPECIFIC_ACCESS_DENIED = 1227

needs_mysql = pytest.mark.skipif(
    not MYSQL_URL,
    reason=(
        "INGESTIO_MYSQL_URL is not set: it names the MySQL server to run against, as"
        " mysql://<user>:<password>@<host>:<port>/<database>"
    ),
)
needs_spark = pytest.mark.skipif(
    os.environ.get("INGESTIO_SPARK_TESTS") != "1",
    reason="INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in",
)
needs_connector_j = pytest.mark.skipif(
    not JDBC_PACKAGE,
    reason=(
        "INGESTIO_MYSQL_JDBC_PACKAGE is not set: it names the MySQL Connector/J release that a"
        " local Spark session loads, such as com.mysql:mysql-connector-j:<version>"
    ),
)


def _quote(name: str) -> str:
    """`name` quoted as a MySQL identifier."""
    return "`" + name.replace("`", "``") + "`"


def _server_id() -> int:
    """A `server_id` for one read, unlikely to be any replica's or another reader's."""
    return random.randint(1_000_000, 4_294_967_295)


class _Database:
    """The database of `INGESTIO_MYSQL_URL`, for one test.

    Its statements run through PyMySQL, outside the reader, in sessions of their own whose
    `time_zone` is UTC. It drops the tables that `table` names and restores the global variables
    that `set_global` changes when the test ends.
    """

    def __init__(self, url: str) -> None:
        parts = urlsplit(url)
        assert parts.scheme == "mysql", "INGESTIO_MYSQL_URL does not start with mysql://"
        assert parts.hostname, "INGESTIO_MYSQL_URL has no host"
        assert parts.username, "INGESTIO_MYSQL_URL has no user"
        self.host = parts.hostname
        self.port = parts.port or 3306
        self.user = unquote(parts.username)
        self.password = unquote(parts.password or "")
        self.database = unquote(parts.path.removeprefix("/"))
        assert self.database, "INGESTIO_MYSQL_URL has no database"
        self.connection = connectors.MySQLConnection(
            self.host, self.database, self.user, self.password, port=self.port
        )
        self._tables: list[str] = []
        self._globals: list[tuple[str, object]] = []

    def connect(self) -> pymysql.connections.Connection[pymysql.cursors.Cursor]:
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            autocommit=True,
            init_command="SET time_zone = '+00:00'",
        )

    def execute(self, statement: str, args: Sequence[object] | None = None) -> None:
        self.transaction((statement, args))

    def transaction(self, *statements: tuple[str, Sequence[object] | None]) -> None:
        """Run `statements`, each with its arguments, in one transaction."""
        connection = self.connect()
        try:
            connection.begin()
            with connection.cursor() as cursor:
                for statement, args in statements:
                    cursor.execute(statement, args)
            connection.commit()
        finally:
            connection.close()

    def rows(self, query: str) -> list[tuple[Any, ...]]:
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return [tuple(row) for row in cursor.fetchall()]
        finally:
            connection.close()

    def status(self) -> tuple[str, int]:
        """The end of the binary log now, from `SHOW BINARY LOG STATUS`."""
        ((log_file, offset, *_),) = self.rows("SHOW BINARY LOG STATUS")
        return str(log_file), int(offset)

    def binary_logs(self) -> list[tuple[str, int]]:
        """The binary log files of the server, oldest first, with their sizes."""
        return [(str(row[0]), int(row[1])) for row in self.rows("SHOW BINARY LOGS")]

    def table(self, stem: str) -> str:
        """A new table name, `ingestio_it_cdc_<stem>_<hex>`, dropped when the test ends."""
        name = f"ingestio_it_cdc_{stem}_{uuid.uuid4().hex[:12]}"
        self._tables.append(name)
        return name

    def qualified(self, name: str) -> str:
        """`name` in the URL's database, as `table_name` takes it."""
        return f"{self.database}.{name}"

    def set_global(self, name: str, expression: str) -> None:
        """Set the global variable `name` to the SQL `expression` until the test ends, or skip
        the test when the user may not set it."""
        ((previous,),) = self.rows(f"SELECT @@GLOBAL.{name}")
        try:
            self.execute(f"SET GLOBAL {name} = {expression}")
        except pymysql.err.OperationalError as error:
            if error.args[0] != SPECIFIC_ACCESS_DENIED:
                raise
            pytest.skip(f"the user of INGESTIO_MYSQL_URL may not set the global {name}: {error}")
        self._globals.append((name, previous))

    def flush_binary_logs(self) -> None:
        """Rotate the binary log, or skip the test when the user may not."""
        try:
            self.execute("FLUSH BINARY LOGS")
        except pymysql.err.OperationalError as error:
            if error.args[0] != SPECIFIC_ACCESS_DENIED:
                raise
            pytest.skip(f"the user of INGESTIO_MYSQL_URL may not flush the binary logs: {error}")

    def close(self) -> None:
        for name, previous in reversed(self._globals):
            self.execute(f"SET GLOBAL {name} = %s", [previous])
        for table in reversed(self._tables):
            self.execute(f"DROP TABLE IF EXISTS {_quote(table)}")


@pytest.fixture
def database() -> Iterator[_Database]:
    """The database of `INGESTIO_MYSQL_URL`; the test's tables are dropped afterwards."""
    db = _Database(MYSQL_URL)
    try:
        yield db
    finally:
        db.close()


def _events(
    db: _Database, name: str, schema: StructType, position: tuple[str, int]
) -> list[tuple[Any, ...]]:
    """The rows of a read of `name` from `position`, against the server, over a fake session
    whose JDBC load answers `schema`."""
    log = _Log()
    spark = _FakeSpark(log, _SparkScript(schema=schema))
    connectors.MySQLCDCReader(
        db.qualified(name),
        _server_id(),
        db.connection,
        binlog_position=position,
        spark=_session(spark),
    ).read()
    return [cast("tuple[Any, ...]", row) for row in _rows(log)]


# The schema that Connector/J gives the table `(id INT PRIMARY KEY, name VARCHAR(50))`.
SMALL_TABLE = StructType(
    [StructField("id", IntegerType(), False), StructField("name", StringType(), True)]
)


def _small_table(db: _Database, stem: str) -> tuple[str, str]:
    """Create a table `(id INT PRIMARY KEY, name VARCHAR(50))`; its name and its quoted name."""
    name = db.table(stem)
    db.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, name VARCHAR(50))")
    return name, _quote(name)


# Tier 1: the server checks and the stream against MySQL, with a fake Spark session.


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_changes_read_as_events_with_the_position_and_time_of_their_commits(
    database: _Database,
) -> None:
    name, table = _small_table(database, "events")
    start = database.status()
    earliest = datetime.now(UTC).replace(microsecond=0)
    database.transaction(
        (f"INSERT INTO {table} VALUES (1, 'a')", None),
        (f"UPDATE {table} SET name = 'b' WHERE id = 1", None),
        (f"DELETE FROM {table} WHERE id = 1", None),
    )
    commits = [database.status()]
    for statement in (
        f"INSERT INTO {table} VALUES (2, 'c')",
        f"UPDATE {table} SET name = 'd' WHERE id = 2",
        f"DELETE FROM {table} WHERE id = 2",
    ):
        database.execute(statement)
        commits.append(database.status())
    latest = datetime.now(UTC)

    rows = _events(database, name, SMALL_TABLE, start)

    assert [row[:3] for row in rows] == [
        (1, "a", "insert"),
        (1, "b", "update"),
        (1, "b", "delete"),
        (2, "c", "insert"),
        (2, "d", "update"),
        (2, "d", "delete"),
    ]
    first, *others = commits
    assert [(row[3], row[4]) for row in rows] == [first, first, first, *others]
    assert [row[6] for row in rows] == [1, 2, 3, 4, 5, 6]
    for row in rows:
        commit_time = row[5]
        assert isinstance(commit_time, datetime)
        assert commit_time.utcoffset() == timedelta(0)
        assert commit_time.microsecond == 0
        assert earliest <= commit_time <= latest, (earliest, commit_time, latest)


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_an_update_of_the_primary_key_reads_as_a_delete_and_an_insert(
    database: _Database,
) -> None:
    name, table = _small_table(database, "key")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    start = database.status()
    database.execute(f"UPDATE {table} SET id = 5 WHERE id = 1")
    commit = database.status()

    rows = _events(database, name, SMALL_TABLE, start)

    assert [row[:5] for row in rows] == [(1, "a", "delete", *commit), (5, "a", "insert", *commit)]
    assert [row[6] for row in rows] == [1, 2]


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_a_read_from_the_next_position_returns_exactly_the_later_changes(
    database: _Database,
) -> None:
    name, table = _small_table(database, "resume")
    start = database.status()
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    first = _events(database, name, SMALL_TABLE, start)
    assert [row[:3] for row in first] == [(1, "a", "insert")]
    last = max(first, key=lambda row: cast(int, row[6]))
    following = (cast(str, last[3]), cast(int, last[4]))

    database.execute(f"INSERT INTO {table} VALUES (2, 'b')")
    database.execute(f"UPDATE {table} SET name = 'c' WHERE id = 2")
    second = _events(database, name, SMALL_TABLE, following)

    assert [row[:3] for row in second] == [(2, "b", "insert"), (2, "c", "update")]
    assert [row[6] for row in second] == [1, 2]


def _number(log_file: str) -> int:
    """The number that ends a binary log file name."""
    return int(log_file.rsplit(".", 1)[1])


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_changes_on_both_sides_of_a_rotation_carry_their_own_files(
    database: _Database,
) -> None:
    name, table = _small_table(database, "flush")
    start = database.status()
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    before = database.status()
    database.flush_binary_logs()
    database.execute(f"INSERT INTO {table} VALUES (2, 'b')")
    after = database.status()

    rows = _events(database, name, SMALL_TABLE, start)

    assert [row[:2] for row in rows] == [(1, "a"), (2, "b")]
    assert (rows[0][3], rows[0][4]) == before
    assert (rows[1][3], rows[1][4]) == after
    assert _number(cast(str, rows[1][3])) > _number(cast(str, rows[0][3]))


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_a_position_below_the_oldest_binary_log_fails_as_purged(
    database: _Database,
) -> None:
    name, _ = _small_table(database, "purged")
    oldest = database.binary_logs()[0][0]
    stem, digits = oldest.rsplit(".", 1)
    if int(digits) == 0:
        pytest.skip(f"the oldest binary log, {oldest!r}, has no number below it")
    purged = f"{stem}.{int(digits) - 1:0{len(digits)}d}"

    message = (
        f"binlog_position {(purged, 4)!r} has been purged from the server; its oldest binary log"
        f" is {oldest!r}"
    )
    with pytest.raises(ValueError, match=_exactly(message)):
        _events(database, name, SMALL_TABLE, (purged, 4))


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_a_column_added_since_the_position_fails_the_read(
    database: _Database,
) -> None:
    name, table = _small_table(database, "columns")
    start = database.status()
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    database.execute(f"ALTER TABLE {table} ADD COLUMN extra INT NULL")
    database.execute(f"INSERT INTO {table} VALUES (2, 'b', 3)")
    schema = StructType([*SMALL_TABLE.fields, StructField("extra", IntegerType(), True)])

    pattern = (
        r"^the event of the table at \('[^']+', [0-9]+\) has no column 'extra'; the table's"
        r" columns changed since it was written$"
    )
    with pytest.raises(ValueError, match=pattern):
        _events(database, name, schema, start)


# The table of tier 1's case 7, one column per type, and the row inserted into it.
TYPES_TABLE = (
    "id INT PRIMARY KEY",
    "flag TINYINT(1)",
    "big BIGINT UNSIGNED",
    "amount DECIMAL(10,2)",
    "ratio DOUBLE",
    "happened DATETIME(6)",
    "stamped TIMESTAMP(6) NULL",
    "duration TIME",
    "born YEAR",
    "bit1 BIT(1)",
    "bits_long BIT(12)",
    "bits_binary BIT(12)",
    "size ENUM('s','m','l')",
    "tags SET('c','b','a')",
    "doc JSON",
    "raw VARBINARY(8)",
    "note VARCHAR(50) CHARACTER SET utf8mb4",
)
TYPES_VALUES = (
    "1, 1, 18446744073709551615, 12.50, 1.5, '2026-10-24 11:00:00.123456',"
    " '2026-10-24 11:00:00.123456', '08:30:00', 2026, b'1', b'000100000001', b'000100000001',"
    " 'm', 'a,c', %s, X'0001', %s"
)
TYPES_ARGS: list[object] = ['{"k": [1, "é"]}', "café \U0001d11e"]
# The schema that the test chooses for the table, as Connector/J types it: `BIT(12)` as a
# `bigint` on Spark 3.5 and as `binary` on Spark 4, one column of each.
TYPES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("flag", BooleanType(), True),
        StructField("big", DecimalType(20, 0), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("ratio", DoubleType(), True),
        StructField("happened", TimestampType(), True),
        StructField("stamped", TimestampType(), True),
        StructField("duration", TimestampType(), True),
        StructField("born", DateType(), True),
        StructField("bit1", BooleanType(), True),
        StructField("bits_long", LongType(), True),
        StructField("bits_binary", BinaryType(), True),
        StructField("size", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("doc", StringType(), True),
        StructField("raw", BinaryType(), True),
        StructField("note", StringType(), True),
    ]
)


def _types_table(db: _Database) -> tuple[str, tuple[str, int]]:
    """Create the table of the column types and insert its row; its name and the position from
    before the insert."""
    name = db.table("types")
    db.execute(f"CREATE TABLE {_quote(name)} ({', '.join(TYPES_TABLE)})")
    start = db.status()
    db.execute(f"INSERT INTO {_quote(name)} VALUES ({TYPES_VALUES})", TYPES_ARGS)
    return name, start


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_each_column_type_is_converted_for_its_jdbc_type(
    database: _Database,
) -> None:
    name, start = _types_table(database)
    moment = datetime(2026, 10, 24, 11, 0, 0, 123456, tzinfo=UTC)

    (row,) = _events(database, name, TYPES_SCHEMA, start)

    assert _typed(row[: len(TYPES_SCHEMA.fields)]) == _typed(
        (
            1,
            True,
            Decimal("18446744073709551615"),
            Decimal("12.50"),
            1.5,
            moment,
            moment,
            datetime(1970, 1, 1, 8, 30, tzinfo=UTC),
            date(2026, 1, 1),
            True,
            257,
            b"\x01\x01",
            "m",
            "a,c",
            '{"k": [1, "é"]}',
            b"\x00\x01",
            "café \U0001d11e",
        )
    )


@pytest.mark.integration
@needs_mysql
def test_mysql_cdc_binlog_row_metadata_minimal_fails_the_read(database: _Database) -> None:
    name, _ = _small_table(database, "minimal")
    start = database.status()
    database.set_global("binlog_row_metadata", "'MINIMAL'")

    message = "the server's binlog_row_metadata is 'MINIMAL'; MySQLCDCReader needs 'FULL'"
    with pytest.raises(ValueError, match=_exactly(message)):
        _events(database, name, SMALL_TABLE, start)


# Tier 2: the reader against MySQL, through a local Spark session with Connector/J.


@pytest.fixture(scope="module")
def local_spark() -> Iterator[SparkSession]:
    """A local Spark session in UTC, with MySQL Connector/J."""
    if SparkContext._gateway is not None:
        pytest.skip(
            "a JVM was already started in this run, without MySQL Connector/J: a JVM loads its"
            " packages once, so run this file on its own"
        )
    # The JVM runs in UTC too: how Connector/J converts DATETIME values in another JVM zone is
    # not established (Known limitations of specs/mysql_connectors.md).
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration-mysql-cdc")
        .config("spark.jars.packages", JDBC_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _by_id(frame: DataFrame) -> dict[int, dict[str, Any]]:
    """The rows of `frame` as dictionaries, by their `id`."""
    return {int(row["id"]): row.asDict() for row in frame.collect()}


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_cdc_a_snapshot_holds_the_full_read_and_the_position_before_it(
    database: _Database, local_spark: SparkSession
) -> None:
    name, table = _small_table(database, "snapshot")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, NULL)")
    before = database.status()
    reader = connectors.MySQLCDCReader(
        database.qualified(name), _server_id(), database.connection, spark=local_spark
    )

    frame = reader.read()

    rows = _by_id(frame)
    full = _by_id(
        connectors.MySQLReader(
            database.qualified(name), database.connection, spark=local_spark
        ).read()
    )
    assert frame.columns == ["id", "name", *METADATA_NAMES]
    assert {key: (row["id"], row["name"]) for key, row in rows.items()} == {
        key: (row["id"], row["name"]) for key, row in full.items()
    }
    for row in rows.values():
        assert row["_ingestio_operation"] == "snapshot"
        assert (row["_ingestio_binlog_file"], row["_ingestio_binlog_offset"]) == before
        assert row["_ingestio_commit_time"] is None
        assert row["_ingestio_sequence"] == 0


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_cdc_a_read_from_the_snapshots_position_returns_the_later_changes(
    database: _Database, local_spark: SparkSession
) -> None:
    name, table = _small_table(database, "after")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    snapshot = connectors.MySQLCDCReader(
        database.qualified(name), _server_id(), database.connection, spark=local_spark
    ).read()
    first = snapshot.select("_ingestio_binlog_file", "_ingestio_binlog_offset").first()
    assert first is not None
    position = (str(first[0]), int(first[1]))
    database.execute(f"INSERT INTO {table} VALUES (3, 'c')")
    database.execute(f"UPDATE {table} SET name = 'z' WHERE id = 1")
    database.execute(f"DELETE FROM {table} WHERE id = 2")

    events = connectors.MySQLCDCReader(
        database.qualified(name),
        _server_id(),
        database.connection,
        binlog_position=position,
        spark=local_spark,
    ).read()

    rows = sorted(events.collect(), key=lambda row: int(row["_ingestio_sequence"]))
    assert [(row["id"], row["name"], row["_ingestio_operation"]) for row in rows] == [
        (3, "c", "insert"),
        (1, "z", "update"),
        (2, "b", "delete"),
    ]
    assert [row["_ingestio_sequence"] for row in rows] == [1, 2, 3]


# The columns whose event value differs from the JDBC value of the same row, as Known
# limitations lists them: the event's value, then the JDBC value.
KNOWN_DIFFERENCES: dict[str, tuple[object, object]] = {
    "tags": ("a,c", "c,a"),
    "tags_empty": (None, ""),
    "doc_string": ("x", '"x"'),
    "doc_null": (None, "null"),
}


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_cdc_event_values_equal_the_jdbc_values_but_for_the_known_differences(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("compare")
    columns = (
        *TYPES_TABLE,
        "tags_empty SET('a','b')",
        "doc_string JSON",
        "doc_null JSON",
    )
    database.execute(f"CREATE TABLE {_quote(name)} ({', '.join(columns)})")
    start = database.status()
    database.execute(
        f"INSERT INTO {_quote(name)} VALUES ({TYPES_VALUES}, '', '\"x\"', 'null')", TYPES_ARGS
    )

    events = connectors.MySQLCDCReader(
        database.qualified(name),
        _server_id(),
        database.connection,
        binlog_position=start,
        spark=local_spark,
    ).read()
    full = connectors.MySQLReader(
        database.qualified(name), database.connection, spark=local_spark
    ).read()

    (event,) = events.collect()
    (jdbc,) = full.collect()
    for column in full.columns:
        if column in KNOWN_DIFFERENCES:
            assert (event[column], jdbc[column]) == KNOWN_DIFFERENCES[column], column
        elif column == "doc":
            # Python's text of a JSON value can differ from MySQL's in spacing and numbers.
            assert json.loads(event[column]) == json.loads(jdbc[column]), column
        else:
            assert event[column] == jdbc[column], column
