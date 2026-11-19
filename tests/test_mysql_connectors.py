"""MySQL connectors: `MySQLConnection`, `MySQLReader`, `MySQLWriter` and the `mysql` system.

Covers `specs/mysql_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no MySQL server: they use the doubles below, a fake Spark
session, fake DataFrames and a fake `pymysql.connect`, which all write to one ordered event log.
Reading `schema` on the DataFrame of the bound query and calling `first()` on it are events of
that log. Configurations are written with `json.dumps` to a file under `tmp_path`, and a missing
package is simulated in a fresh interpreter, or by mapping its name to `None` in `sys.modules`.

The three classes are reached as attributes of `ingestio.connector.all`, the module the spec
exports them from.

The integration tests at the end run against a real MySQL server, in two tiers (decision 43 of
the spec). They are marked `integration` and skipped unless `INGESTIO_MYSQL_URL` holds a URL of
the form `mysql://<user>:<password>@<host>:<port>/<database>`. Each test creates its own tables in
that database, `ingestio_it_<stem>_<hex>`, and drops them. The first tier needs no Spark: the
writer runs for real, with a fake DataFrame whose save inserts its rows into the table that its
`dbtable` option names, through a PyMySQL connection of the test. The second tier also needs
`INGESTIO_SPARK_TESTS` set to `1` and `INGESTIO_MYSQL_JDBC_PACKAGE` holding the Maven coordinate
of a MySQL Connector/J release, such as `com.mysql:mysql-connector-j:<version>`, which a local
Spark session loads through `spark.jars.packages`; it needs a Java release that the installed
pyspark supports. A JVM loads its packages once, when it starts: when another test module of the
same run has already started one, the second tier skips, so run this file on its own. A few tests
change the server's global `sql_mode` or `time_zone` and restore it afterwards; they skip when
the user of the URL may not set it. They were written without being run: the machine they were
written on has no MySQL server and no Java release that pyspark supports.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import logging
import os
import re
import ssl
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
import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import ingestio.connector.all as connectors
from ingestio import SCD2, AppendOnly, FullLoad, Process, ReadSource, Step
from ingestio.connector.all import MongoDBConnection, PostgresConnection, Reader, Writer

PASSWORD = "s3cret"
BASE_OPTIONS = {
    "url": "jdbc:mysql://db.internal:3306/shop",
    "user": "ingest",
    "password": PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}
READ_DEFAULTS = {"useCursorFetch": "true", "fetchsize": "10000"}
TIME_ZONE_OPTIONS = {"connectionTimeZone": "UTC", "forceConnectionTimeZoneToSession": "true"}
WRITE_DEFAULTS = {"rewriteBatchedStatements": "true"}
CONNECT_ARGS: dict[str, object] = {
    "host": "db.internal",
    "port": 3306,
    "user": "ingest",
    "password": PASSWORD,
    "database": "shop",
    "charset": "utf8mb4",
}
ORDERS = "`shop`.`orders`"
COLUMNS = ("id", "payload")
QUOTED_COLUMNS = "`id`, `payload`"
BOUND_TEXT = "2026-10-21 09:30:00.500000"
BOUND_ROW: tuple[object, ...] = (BOUND_TEXT, None)
QUALIFIED_STAGING = re.compile(r"`shop`\.`ingestio_staging_[0-9a-f]{32}`")
UNQUALIFIED_STAGING = re.compile(r"`ingestio_staging_[0-9a-f]{32}`")
STAGING_NAME = re.compile(r"ingestio_staging_[0-9a-f]{32}")
HEX_LITERAL = re.compile(r"X'[0-9a-f]*'")
ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")
SSL_MODES = ("DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY")
SOURCE_NAMES = (
    "bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, unity_catalog, volume"
)
TARGET_NAMES = "bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume"
SCD2_NAMES = "bigquery, mysql, postgres, unity_catalog"
DEFAULT_SOURCE: dict[str, object] = {"system": "mysql", "table_name": "shop.orders"}
DEFAULT_TARGET: dict[str, object] = {"system": "mysql", "table_name": "shop.orders"}
POSTGRES_SECTION: dict[str, object] = {"system": "postgres", "table_name": "public.orders"}
MONGODB_SECTION: dict[str, object] = {
    "system": "mongodb",
    "database": "shop",
    "collection": "orders",
}
SCD2_SECTION: dict[str, object] = {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
}
SCD2_FILE: dict[str, object] = {
    "target": {"system": "mysql", "table_name": "shop.dim_customer"},
    "scd2": SCD2_SECTION,
}
BATCH_COLUMNS = ("customer_id", "name", "city", "updated_at")
ACCEPTED_OPTIONS: dict[str, str | int | float | bool] = {
    "useCursorFetch": False,
    "fetchsize": 500,
    "rewriteBatchedStatements": False,
    "connectionTimeZone": "SERVER",
    "trustCertificateKeyStoreUrl": "file:/certs/ca.jks",
    "sessionInitStatement": "SET SESSION sql_mode = 'ANSI'",
}
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


def _h(text: str) -> str:
    """`H(text)` of the spec: a hex string literal of `text` with the `utf8mb4` introducer."""
    return f"_utf8mb4 X'{text.encode('utf-8').hex()}'"


def _bound_schema(value_type: DataType | None = None) -> StructType:
    """The schema of the bound load's DataFrame: the bound as text, then the typed maximum,
    by default a timestamp."""
    return StructType(
        [
            StructField("upper_bound", StringType()),
            StructField("upper_bound_value", TimestampType() if value_type is None else value_type),
        ]
    )


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read`, with this format and these merged options."""

    source: str | None
    options: dict[str, str]
    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Schema:
    """`schema` was read on the fake DataFrame `frame`."""

    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _First:
    """`first()` was called on the fake DataFrame `frame`."""

    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Save:
    """`save()` ran on a fake `DataFrame.write`, with this format, mode and merged options."""

    source: str | None
    mode: str | None
    options: dict[str, str]


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
    """A cursor of the connection executed `statement`, with these other arguments. `failed`
    when the execution raised."""

    connection: int
    statement: str
    args: tuple[object, ...] = ()
    kwargs: dict[str, object] = field(default_factory=dict)
    failed: bool = False


_Event: TypeAlias = _Load | _Schema | _First | _Save | _Connect | _Call | _Execute


class _Log:
    """The ordered event log every double writes to."""

    def __init__(self) -> None:
        self.events: list[_Event] = []
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


class _FakeSpark:
    """Stands in for a `SparkSession`, and records every attribute accessed on it.

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does. The DataFrame of
    a load with a `query` option, the bound load, has `schema` and answers `first()` with the next
    row of `bound_rows`, the last one once they run out. When `load_error` is set, every load
    raises it once recorded; when `first_error` is set, every `first()` raises it once recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        bound_rows: Sequence[tuple[object, ...]] = (BOUND_ROW,),
        schema: StructType | None = None,
        load_error: BaseException | None = None,
        first_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._bound_rows = list(bound_rows)
        self._bound_loads = 0
        self._schema = _bound_schema() if schema is None else schema
        self._load_error = load_error
        self._first_error = first_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self)

    def _load(self, source: str | None, options: dict[str, str]) -> _FakeDataFrame:
        """Record a load and return its DataFrame, or raise `load_error`."""
        bound = any(key.lower() == "query" for key in options)
        row: tuple[object, ...] | None = None
        if bound:
            row = self._bound_rows[min(self._bound_loads, len(self._bound_rows) - 1)]
            self._bound_loads += 1
        frame = _FakeDataFrame(
            self._log,
            row=row,
            schema=self._schema if bound else None,
            first_error=self._first_error if bound else None,
        )
        self._log.events.append(_Load(source, dict(options), frame))
        if self._load_error is not None:
            raise self._load_error
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
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    The DataFrame of the bound load has a `schema` and answers `first()` with its `row`; both are
    events of the log. Each access to `write` returns a new `_FakeDataFrameWriter`, whose `save()`
    calls `on_save` with its options and then raises `save_error`, when they are set.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        row: tuple[object, ...] | None = None,
        schema: StructType | None = None,
        first_error: BaseException | None = None,
        save_error: BaseException | None = None,
        on_save: Callable[[Mapping[str, str]], None] | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
        self._row = row
        self._schema = schema
        self._first_error = first_error
        self._save_error = save_error
        self._on_save = on_save
        self._accessed: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def schema(self) -> StructType:
        if self._schema is None:
            raise AssertionError("schema was read on a DataFrame that is not the bound query's")
        self._log.events.append(_Schema(self))
        return self._schema

    def first(self) -> tuple[object, ...]:
        if self._row is None:
            raise AssertionError("first() ran on a DataFrame that is not the bound query's")
        self._log.events.append(_First(self))
        if self._first_error is not None:
            raise self._first_error
        return self._row

    @property
    def write(self) -> _FakeDataFrameWriter:
        return _FakeDataFrameWriter(self._log, self._save_error, self._on_save)


class _FakeDataFrameWriter:
    """Stands in for `DataFrame.write`: records a `_Save` when `save()` is called."""

    def __init__(
        self,
        log: _Log,
        save_error: BaseException | None,
        on_save: Callable[[Mapping[str, str]], None] | None,
    ) -> None:
        self._log = log
        self._save_error = save_error
        self._on_save = on_save
        self._source: str | None = None
        self._mode: str | None = None
        self._options: dict[str, str] = {}

    def format(self, source: str) -> _FakeDataFrameWriter:
        self._source = source
        return self

    def option(self, key: str, value: object) -> _FakeDataFrameWriter:
        _set_option(self._options, key, value)
        return self

    def options(self, **options: object) -> _FakeDataFrameWriter:
        for key, value in options.items():
            _set_option(self._options, key, value)
        return self

    def mode(self, save_mode: str) -> _FakeDataFrameWriter:
        self._mode = save_mode
        return self

    def save(self, **options: object) -> None:
        self.options(**options)
        self._log.events.append(_Save(self._source, self._mode, dict(self._options)))
        if self._on_save is not None:
            self._on_save(dict(self._options))
        if self._save_error is not None:
            raise self._save_error


class _FakePyMySQL:
    """Stands in for `pymysql.connect`: records each call and returns a `_FakeConnection`.

    `connect_errors` maps a call number, from 1, to the error that call raises once recorded.
    `method_errors` maps a connection number and a method name, `cursor`, `begin`, `commit`,
    `rollback` or `close`, to the error that method raises once recorded. `execute_errors` lists,
    the first match winning, a connection number, or `None` for any, the start of a statement,
    and the error that its execution raises once recorded.
    """

    def __init__(self, log: _Log) -> None:
        self.log = log
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.connections: list[_FakeConnection] = []
        self.connect_errors: dict[int, BaseException] = {}
        self.method_errors: dict[tuple[int, str], BaseException] = {}
        self.execute_errors: list[tuple[int | None, str, BaseException]] = []

    def connect(self, *args: object, **kwargs: object) -> _FakeConnection:
        number = len(self.calls) + 1
        self.calls.append((args, dict(kwargs)))
        self.log.events.append(_Connect(number, args, dict(kwargs)))
        if number in self.connect_errors:
            raise self.connect_errors[number]
        connection = _FakeConnection(self, number)
        self.connections.append(connection)
        return connection


class _FakeConnection:
    """Stands in for a `pymysql.connections.Connection`.

    It follows PyMySQL where the writer can tell: on a closed connection a statement raises
    `InterfaceError`, as do `begin()`, `commit()` and `rollback()`, and a second `close()` raises
    `Error("Already closed")`. It is not a context manager: the writer never relies on one.
    """

    def __init__(self, fake: _FakePyMySQL, number: int) -> None:
        self._fake = fake
        self.number = number
        self.closed = False

    def cursor(self, cursor: object = None) -> _FakeCursor:
        self._call("cursor", refuse_closed=False)
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
        self._call("close", refuse_closed=False)

    def _call(self, method: str, *, refuse_closed: bool = True) -> None:
        error = self._fake.method_errors.get((self.number, method))
        if refuse_closed and self.closed:
            error = pymysql.err.InterfaceError(0, "")
        self._fake.log.events.append(_Call(self.number, method, failed=error is not None))
        if error is not None:
            raise error

    def _execute(self, statement: str, args: tuple[object, ...], kwargs: dict[str, object]) -> int:
        error: BaseException | None = None
        if self.closed:
            error = pymysql.err.InterfaceError(0, "")
        else:
            for number, start, failure in self._fake.execute_errors:
                if number in (None, self.number) and statement.startswith(start):
                    error = failure
                    break
        self._fake.log.events.append(
            _Execute(self.number, statement, args, kwargs, failed=error is not None)
        )
        if error is not None:
            raise error
        return 0


class _FakeCursor:
    """Stands in for a `pymysql.cursors.Cursor`: its executions are recorded on its connection,
    with every argument besides the statement."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def execute(self, query: str, *args: object, **kwargs: object) -> int:
        return self._connection._execute(query, args, kwargs)

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


class _FakeSession:
    """Stands in for the batch's `SparkSession`: compared by identity, and nothing else."""


class _FakeBatch:
    """Stands in for the batch that `SCD2` writes: only its `columns` and its `sparkSession`."""

    def __init__(self, columns: Sequence[str], session: _FakeSession) -> None:
        self._columns = list(columns)
        self._session = session

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def sparkSession(self) -> _FakeSession:  # noqa: N802 - PySpark's name
        return self._session


# --- Fixtures and helpers -----------------------------------------------------------------


def _refuse(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("the active session or pymysql.connect was used")


@pytest.fixture(autouse=True)
def _isolated(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `SparkSession.active` and `pymysql.connect` fail when called, except in the
    integration tests, which need the real ones."""
    if request.node.get_closest_marker("integration") is not None:
        return
    monkeypatch.setattr(SparkSession, "active", _refuse)
    monkeypatch.setattr(pymysql, "connect", _refuse)


@pytest.fixture
def log() -> _Log:
    """The event log the doubles of one test share."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """A fake session whose bound load has the default schema and answers `BOUND_ROW`."""
    return _FakeSpark(log)


@pytest.fixture
def pm(log: _Log, monkeypatch: pytest.MonkeyPatch) -> _FakePyMySQL:
    """Replace `pymysql.connect` with a fake that records on the log."""
    fake = _FakePyMySQL(log)
    monkeypatch.setattr(pymysql, "connect", fake.connect)
    return fake


@pytest.fixture
def conn() -> connectors.MySQLConnection:
    """The connection of the acceptance criteria."""
    return connectors.MySQLConnection("db.internal", "shop", "ingest", PASSWORD)


@pytest.fixture
def df(log: _Log) -> _FakeDataFrame:
    """The DataFrame the writer receives, with the columns `id` and `payload`."""
    return _FakeDataFrame(log, columns=COLUMNS)


def _session(fake: _FakeSpark | _FakeSession) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame | _FakeBatch) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


def _reader(
    conn: connectors.MySQLConnection, spark: _FakeSpark, **arguments: Any
) -> connectors.MySQLReader:
    """The reader of the criteria, `shop.orders` over `spark`, with `arguments` added."""
    return connectors.MySQLReader("shop.orders", conn, spark=_session(spark), **arguments)


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _lower(options: Mapping[str, str]) -> dict[str, str]:
    """`options` with lowercase keys, to compare keys case-insensitively."""
    return {key.lower(): value for key, value in options.items()}


def _loads(log: _Log) -> list[_Load]:
    """The loads recorded, in order."""
    return [event for event in log.events if isinstance(event, _Load)]


def _saves(log: _Log) -> list[_Save]:
    """The saves recorded, in order."""
    return [event for event in log.events if isinstance(event, _Save)]


def _executed(log: _Log, connection: int | None = None) -> list[_Execute]:
    """The executions recorded, failed ones included, in order, on `connection` or on all."""
    return [
        event
        for event in log.events
        if isinstance(event, _Execute) and connection in (None, event.connection)
    ]


def _statements(log: _Log, connection: int | None = None) -> list[str]:
    """The statements of `_executed`."""
    return [event.statement for event in _executed(log, connection)]


def _commits(log: _Log, connection: int) -> list[_Call]:
    """The `commit()` calls of `connection` that did not raise."""
    return [
        event
        for event in log.events
        if isinstance(event, _Call)
        and event.connection == connection
        and event.method == "commit"
        and not event.failed
    ]


def _attributes(frame: _FakeDataFrame) -> list[str]:
    """The attributes accessed on `frame`, in order."""
    return frame._accessed


def _notes(error: BaseException) -> list[str]:
    """The notes added to `error`."""
    return list(getattr(error, "__notes__", []))


def _transcript(events: Sequence[_Event]) -> list[str]:
    """The events as the lines of a session log, without the `cursor()` calls."""
    lines: list[str] = []
    for event in events:
        if isinstance(event, _Load):
            lines.append("load")
        elif isinstance(event, _Schema):
            lines.append("schema")
        elif isinstance(event, _First):
            lines.append("first")
        elif isinstance(event, _Save):
            lines.append("save")
        elif isinstance(event, _Connect):
            lines.append(f"connect {event.connection}")
        elif isinstance(event, _Execute):
            failed = " (failed)" if event.failed else ""
            lines.append(f"{event.connection}: {event.statement}{failed}")
        elif event.method != "cursor":
            failed = " (failed)" if event.failed else ""
            lines.append(f"{event.connection}: {event.method}{failed}")
    return lines


def _committed(log: _Log) -> list[tuple[int, str]]:
    """The statements committed, in the order of their commits, with their connection.

    A statement is committed when its connection calls `commit()` after executing it, with no
    `rollback()` in between. A statement whose execution raised, or a `commit()` that raised,
    commits nothing; closing a connection discards what it has not committed.
    """
    pending: dict[int, list[str]] = {}
    committed: list[tuple[int, str]] = []
    for event in log.events:
        if isinstance(event, _Execute):
            if not event.failed:
                pending.setdefault(event.connection, []).append(event.statement)
        elif isinstance(event, _Call) and not event.failed:
            if event.method == "commit":
                statements = pending.pop(event.connection, [])
                committed.extend((event.connection, statement) for statement in statements)
            elif event.method in ("rollback", "close"):
                pending.pop(event.connection, None)
    return committed


def _staging_tables(log: _Log) -> list[str]:
    """The staging tables that the `CREATE` statements name, in order, as quoted."""
    names: list[str] = []
    for statement in _statements(log):
        match = re.fullmatch(r"CREATE TABLE (.+?) LIKE .+", statement)
        if match:
            names.append(match[1])
    return names


def _staging(log: _Log) -> str:
    """The staging table of the only `CREATE` executed, as quoted."""
    tables = _staging_tables(log)
    assert len(tables) == 1, _transcript(log.events)
    return tables[0]


def _create(stg: str, *, target: str = ORDERS) -> str:
    return f"CREATE TABLE {stg} LIKE {target}"


def _insert(stg: str, *, target: str = ORDERS, columns: str = QUOTED_COLUMNS) -> str:
    return f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {stg}"


def _write_transcript(stg: str, *, target: str = ORDERS, replace: bool = False) -> list[str]:
    """The transcript of a successful write of `df` through the staging table `stg`."""
    delete = [f"2: DELETE FROM {target}"] if replace else []
    return [
        "connect 1",
        f"1: {_create(stg, target=target)}",
        "1: commit",
        "1: close",
        "save",
        "connect 2",
        "2: begin",
        *delete,
        f"2: {_insert(stg, target=target)}",
        "2: commit",
        f"2: DROP TABLE {stg}",
        "2: commit",
        "2: close",
    ]


def _save_options(stg: str) -> dict[str, str]:
    """The options of the save of a write through `stg`, without user options."""
    return {**BASE_OPTIONS, "dbtable": stg, **WRITE_DEFAULTS, **TIME_ZONE_OPTIONS}


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


def _hide_pymysql(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `import pymysql` raise `ImportError` for the rest of the test."""
    modules = cast("dict[str, object]", sys.modules)
    for name in [name for name in sys.modules if name.split(".")[0] == "pymysql"]:
        monkeypatch.setitem(modules, name, None)
    monkeypatch.setitem(modules, "pymysql", None)


def _shape(log: _Log) -> list[tuple[object, ...]]:
    """The reading events of `log`, as values that the log of a twin can equal: each load with
    its format and options, and each `schema` and `first()` with the number of its load."""
    frames = [event.frame for event in _loads(log)]

    def number(frame: _FakeDataFrame) -> int:
        return next(index for index, item in enumerate(frames) if item is frame)

    shape: list[tuple[object, ...]] = []
    for event in log.events:
        if isinstance(event, _Load):
            shape.append(("load", event.source, event.options))
        elif isinstance(event, _Schema):
            shape.append(("schema", number(event.frame)))
        elif isinstance(event, _First):
            shape.append(("first", number(event.frame)))
    return shape


def _assert_twin(built: object, twin: object, *same: object) -> None:
    """`built` has the class and the state of `twin`, and holds each object of `same` that the
    twin holds, itself rather than an equal copy."""
    assert type(built) is type(twin), (built, twin)
    assert vars(built) == vars(twin)
    for name, value in vars(twin).items():
        if any(value is item for item in same):
            assert vars(built)[name] is value, name


def _assert_connector_error(error: BaseException, direct: BaseException, note: str) -> None:
    """`error` is the error that building the connector directly raised, `direct`, plus `note`."""
    assert type(error) is type(direct)
    assert str(error) == str(direct)
    assert type(error.__cause__) is type(direct.__cause__)
    assert _notes(error) == [note]


CALLBACKS: dict[str, Callable[..., object]] = {
    "ReadSource": ReadSource,
    "AppendOnly": AppendOnly,
    "FullLoad": FullLoad,
    "SCD2": SCD2,
}

WriteCall: TypeAlias = "Callable[[connectors.MySQLWriter, DataFrame], object]"

APPEND_CALLS: dict[str, WriteCall] = {
    "write(df)": lambda writer, frame: writer.write(frame),
    "write(df, replace=False)": lambda writer, frame: writer.write(frame, replace=False),
}

EVERY_CALL: dict[str, WriteCall] = {
    **APPEND_CALLS,
    "write(df, replace=True)": lambda writer, frame: writer.write(frame, replace=True),
}


# --- Import surface and the extra ---------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import MySQLConnection, MySQLReader, MySQLWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.mysql as mysql

names = {"MySQLConnection", "MySQLReader", "MySQLWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(mysql.__all__), mysql.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys

sys.modules["pyspark"] = None
sys.modules["pymysql"] = None
for name in ("pyspark", "pyspark.sql", "pyspark.sql.types", "pymysql"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import MySQLConnection, MySQLReader

loads = []


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def load(self, **options):
        loads.append(object())
        return loads[-1]


class _FakeSpark:
    @property
    def read(self):
        return _FakeDataFrameReader()


for mode in (None, "DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"):
    MySQLConnection("db.internal", "shop", "ingest", "s3cret", ssl_mode=mode)
conn = MySQLConnection("db.internal", "shop", "ingest", "s3cret")
frame = MySQLReader("shop.orders", conn, spark=_FakeSpark()).read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
"""


def test_at001_the_three_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connection_and_the_reader_work_without_pyspark_and_pymysql(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at003_without_pymysql_the_writer_names_the_extra_and_the_reader_still_reads(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _hide_pymysql(monkeypatch)

    message = "MySQLWriter needs PyMySQL: pip install 'ingestio[mysql]'"
    with pytest.raises(ImportError, match=_exactly(message)) as excinfo:
        connectors.MySQLWriter("shop.orders", conn)
    assert isinstance(excinfo.value.__cause__, ImportError)

    full = _reader(conn, spark).read()
    sliced = _reader(conn, spark, watermark_column="updated_at", watermark_value=41).read()

    first, _, data = _loads(log)
    assert full is _frame(first.frame)
    assert sliced is _frame(data.frame)


def test_at004_the_distribution_declares_the_mysql_extra_with_pymysql_and_rsa() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "mysql" in (metadata.get_all("Provides-Extra") or [])
    requirements = [Requirement(text) for text in metadata.get_all("Requires-Dist") or []]
    assert requirements
    for requirement in requirements:
        assert requirement.marker is not None, requirement
        assert ANY_EXTRA_MARKER.search(str(requirement.marker)), requirement

    # The `mysql-cdc` extra of specs/mysql_cdc.md names PyMySQL too, under its own marker.
    named = [
        item
        for item in requirements
        if canonicalize_name(item.name) == "pymysql" and str(item.marker) == 'extra == "mysql"'
    ]
    assert len(named) == 1, requirements
    (pymysql_requirement,) = named
    assert pymysql_requirement.extras == {"rsa"}
    assert str(pymysql_requirement.specifier) == ">=1.2.0"


# --- MySQLConnection ----------------------------------------------------------------------

ARGUMENTS = ("db.internal", "shop", "ingest", PASSWORD)

# The calls of AT-005: the positional arguments, the keyword arguments and the message.
AT005_CASES: list[tuple[str, tuple[object, ...], dict[str, object], str]] = [
    ("host", ("", "shop", "ingest", PASSWORD), {}, "host must not be empty"),
    ("database", ("db.internal", "", "ingest", PASSWORD), {}, "database must not be empty"),
    ("user", ("db.internal", "shop", "", PASSWORD), {}, "user must not be empty"),
    ("port-0", ARGUMENTS, {"port": 0}, "port must be between 1 and 65535"),
    ("port-65536", ARGUMENTS, {"port": 65536}, "port must be between 1 and 65535"),
    ("lowercase-ssl-mode", ARGUMENTS, {"ssl_mode": "required"}, "unsupported ssl_mode 'required'"),
    (
        "unknown-ssl-mode",
        ARGUMENTS,
        {"ssl_mode": "VERIFY_FULL"},
        "unsupported ssl_mode 'VERIFY_FULL'",
    ),
]


@pytest.mark.parametrize(
    ("args", "kwargs", "message"),
    [pytest.param(args, kwargs, message, id=name) for name, args, kwargs, message in AT005_CASES],
)
def test_at005_invalid_connection_settings_are_rejected(
    args: tuple[object, ...], kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MySQLConnection(*cast(Any, args), **cast(Any, kwargs))


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"port": 1}, id="port-1"),
        pytest.param({"port": 65535}, id="port-65535"),
        pytest.param({"password": ""}, id="empty-password"),
        *[pytest.param({"ssl_mode": mode}, id=mode) for mode in SSL_MODES],
    ],
)
def test_at005_the_edge_ports_an_empty_password_and_every_ssl_mode_are_accepted(
    kwargs: dict[str, object],
) -> None:
    arguments: dict[str, object] = {
        "host": "db.internal",
        "database": "shop",
        "user": "ingest",
        "password": PASSWORD,
        **kwargs,
    }

    connection = connectors.MySQLConnection(**cast(Any, arguments))

    for name, value in kwargs.items():
        assert getattr(connection, name) == value


def test_at006_repr_shows_what_identifies_the_database_and_the_table(
    conn: connectors.MySQLConnection,
) -> None:
    shown = "MySQLConnection(host='db.internal', port=3306, database='shop', user='ingest')"
    assert repr(conn) == shown
    assert str(conn) == shown
    assert repr(connectors.MySQLReader("shop.orders", conn)) == (
        "MySQLReader(table_name='shop.orders', host='db.internal', database='shop')"
    )
    assert repr(connectors.MySQLWriter("shop.orders", conn)) == (
        "MySQLWriter(table_name='shop.orders', host='db.internal', database='shop')"
    )


def test_at006_repr_is_the_same_with_tls_a_watermark_and_options() -> None:
    connection = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD, ssl_mode="REQUIRED"
    )
    reader = connectors.MySQLReader(
        "shop.orders",
        connection,
        watermark_column="updated_at",
        watermark_value=41,
        options={"fetchsize": 500, "queryTimeout": 30},
    )
    writer = connectors.MySQLWriter("shop.orders", connection, options={"batchsize": 5000})

    assert repr(reader) == (
        "MySQLReader(table_name='shop.orders', host='db.internal', database='shop')"
    )
    assert repr(writer) == (
        "MySQLWriter(table_name='shop.orders', host='db.internal', database='shop')"
    )


def test_at007_a_connection_exposes_its_arguments_and_is_immutable(
    conn: connectors.MySQLConnection,
) -> None:
    assert conn.host == "db.internal"
    assert conn.database == "shop"
    assert conn.user == "ingest"
    assert conn.password == PASSWORD
    assert conn.port == 3306
    assert conn.ssl_mode is None
    changes: dict[str, object] = {
        "host": "other.internal",
        "database": "other",
        "user": "other",
        "password": "other",
        "port": 3307,
        "ssl_mode": "REQUIRED",
    }
    for name, value in changes.items():
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(conn, name, value)


# --- Building the connectors --------------------------------------------------------------


def _build(kind: str, table_name: str, connection: Any, **arguments: Any) -> object:
    """Build the reader or the writer, as `kind` names it."""
    if kind == "reader":
        return connectors.MySQLReader(table_name, connection, **arguments)
    assert kind == "writer", kind
    return connectors.MySQLWriter(table_name, connection, **arguments)


@pytest.mark.parametrize("kind", ["reader", "writer"])
@pytest.mark.parametrize("name", ["", "orders.", ".orders", "a.b.c"])
def test_at008_a_table_name_other_than_table_or_database_table_is_rejected(
    kind: str, name: str, conn: connectors.MySQLConnection
) -> None:
    message = "table_name must be 'table' or 'database.table'"
    with pytest.raises(ValueError, match=_exactly(message)):
        _build(kind, name, conn)


@pytest.mark.parametrize("kind", ["reader", "writer"])
@pytest.mark.parametrize("name", ["orders", "shop.orders"])
def test_at008_table_and_database_table_are_accepted(
    kind: str, name: str, conn: connectors.MySQLConnection
) -> None:
    _build(kind, name, conn)


def test_at009_a_watermark_value_needs_a_watermark_column(
    conn: connectors.MySQLConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_value needs watermark_column")):
        connectors.MySQLReader("shop.orders", conn, watermark_value=41)


def test_at009_an_empty_watermark_column_is_rejected(conn: connectors.MySQLConnection) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_column must not be empty")):
        connectors.MySQLReader("shop.orders", conn, watermark_column="")


# The values of AT-010 that the reader rejects, with the message.
AT010_CASES: list[tuple[str, object, str]] = [
    ("bool", True, "unsupported watermark_value type: bool"),
    ("float", 1.5, "unsupported watermark_value type: float"),
    ("list", [1], "unsupported watermark_value type: list"),
    ("decimal-nan", Decimal("NaN"), "watermark_value must be finite"),
    ("decimal-infinity", Decimal("Infinity"), "watermark_value must be finite"),
    (
        "naive-datetime",
        datetime(2026, 10, 1),  # noqa: DTZ001 - the naive datetime under test
        "watermark_value must be timezone-aware",
    ),
]


@pytest.mark.parametrize(
    ("value", "message"),
    [pytest.param(value, message, id=name) for name, value, message in AT010_CASES],
)
def test_at010_unsupported_watermark_values_are_rejected(
    value: object, message: str, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MySQLReader(
            "shop.orders", conn, watermark_column="updated_at", watermark_value=cast(Any, value)
        )


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="zero"),
        pytest.param("", id="empty-string"),
        pytest.param(Decimal("12.50"), id="decimal"),
        pytest.param(date(2026, 10, 1), id="date"),
        pytest.param(datetime(2026, 10, 1, tzinfo=UTC), id="aware-datetime"),
    ],
)
def test_at010_supported_watermark_values_are_accepted(
    value: int | str | Decimal | date, conn: connectors.MySQLConnection
) -> None:
    connectors.MySQLReader(
        "shop.orders", conn, watermark_column="updated_at", watermark_value=value
    )


@pytest.mark.parametrize("kind", ["reader", "writer"])
@pytest.mark.parametrize("key", RESERVED_KEYS)
def test_at011_options_the_connector_sets_are_rejected_in_any_case(
    kind: str, key: str, conn: connectors.MySQLConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        _build(kind, "shop.orders", conn, options={key: "x"})


@pytest.mark.parametrize("kind", ["reader", "writer"])
def test_at011_the_connectors_defaults_and_other_options_are_accepted(
    kind: str, conn: connectors.MySQLConnection
) -> None:
    _build(kind, "shop.orders", conn, options=ACCEPTED_OPTIONS)


def test_at012_building_the_connectors_touches_neither_spark_nor_mysql(
    log: _Log,
    spark: _FakeSpark,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))

    connectors.MySQLReader("shop.orders", conn)
    connectors.MySQLReader("shop.orders", conn, spark=_session(spark))
    connectors.MySQLReader("shop.orders", conn, watermark_column="updated_at")
    connectors.MySQLReader(
        "shop.orders",
        conn,
        watermark_column="updated_at",
        watermark_value=41,
        spark=_session(spark),
    )
    connectors.MySQLWriter("shop.orders", conn)
    connectors.MySQLWriter("shop.orders", conn, options={"batchsize": 5000})

    assert log.events == []
    assert log.spark_attributes == []
    assert lookups == []
    assert pm.calls == []


# --- Full read ----------------------------------------------------------------------------


def test_at013_a_full_read_loads_the_quoted_table_with_the_base_options_and_the_defaults(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    frame = _reader(conn, spark).read()

    assert _transcript(log.events) == ["load"]
    (load,) = _loads(log)
    assert load.source == "jdbc"
    assert _lower(load.options) == _lower(
        {**BASE_OPTIONS, "dbtable": ORDERS, **READ_DEFAULTS, **TIME_ZONE_OPTIONS}
    )
    for key in [*READ_DEFAULTS, *TIME_ZONE_OPTIONS]:
        assert key in load.options, load.options
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    url = _lower(load.options)["url"]
    assert PASSWORD not in url
    assert "@" not in url


def test_at014_an_ipv6_host_is_bracketed_and_the_database_name_percent_encoded(
    log: _Log, spark: _FakeSpark
) -> None:
    connection = connectors.MySQLConnection("::1", "my db", "ingest", PASSWORD, port=3307)

    _reader(connection, spark).read()

    (load,) = _loads(log)
    assert _lower(load.options)["url"] == "jdbc:mysql://[::1]:3307/my%20db"


def test_at015_user_options_are_passed_through_and_replace_the_defaults(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    options: dict[str, str | int | float | bool] = {
        "partitionColumn": "id",
        "lowerBound": 1,
        "upperBound": 1000,
        "numPartitions": 8,
        "FetchSize": 500,
        "UseCursorFetch": False,
        "connectionTimeZone": "SERVER",
    }

    _reader(conn, spark, options=options).read()

    (load,) = _loads(log)
    assert _lower(load.options) == _lower(
        {
            **BASE_OPTIONS,
            "dbtable": ORDERS,
            "partitionColumn": "id",
            "lowerBound": "1",
            "upperBound": "1000",
            "numPartitions": "8",
            "FetchSize": "500",
            "UseCursorFetch": "false",
            "connectionTimeZone": "SERVER",
            "forceConnectionTimeZoneToSession": "true",
        }
    )
    for key in ("FetchSize", "UseCursorFetch", "connectionTimeZone"):
        assert key in load.options, load.options
    assert "forceConnectionTimeZoneToSession" in load.options, load.options


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at016_the_ssl_mode_reaches_every_load_as_ssl_mode(
    watermark: bool, log: _Log, spark: _FakeSpark
) -> None:
    connection = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD, ssl_mode="VERIFY_IDENTITY"
    )

    _reader(connection, spark, watermark_column="updated_at" if watermark else None).read()

    loads = _loads(log)
    assert len(loads) == (2 if watermark else 1)
    for load in loads:
        assert load.options.get("sslMode") == "VERIFY_IDENTITY", load.options


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at016_without_an_ssl_mode_no_load_carries_one(
    watermark: bool, log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    _reader(conn, spark, watermark_column="updated_at" if watermark else None).read()

    loads = _loads(log)
    assert loads
    for load in loads:
        assert "sslmode" not in _lower(load.options), load.options


def test_at017_the_active_session_is_looked_up_when_read_runs(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    reader = connectors.MySQLReader("shop.orders", conn)
    assert lookups == []

    def fake_session() -> SparkSession:
        lookups.append("fake session")
        return _session(spark)

    monkeypatch.setattr(SparkSession, "active", fake_session)
    frame = reader.read()

    assert "fake session" in lookups
    (load,) = _loads(log)
    assert frame is _frame(load.frame)


def test_at017_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    err = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise err

    reader = connectors.MySQLReader("shop.orders", conn)
    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert _loads(log) == []


# --- Watermark read -----------------------------------------------------------------------


def _bound_query(
    value: str | None = None, *, table: str = ORDERS, column: str = "`updated_at`"
) -> str:
    """The bound query of a watermark read, with `value` as the literal of the watermark."""
    query = (
        f"SELECT CAST(max({column}) AS CHAR) AS upper_bound,"
        f" max({column}) AS upper_bound_value FROM {table}"
    )
    return query if value is None else f"{query} WHERE {column} > {value}"


def _slice(predicate: str, *, table: str = ORDERS) -> str:
    """The `dbtable` of the data load of a watermark read."""
    return f"(SELECT * FROM {table} WHERE {predicate}) AS ingestio_source"


def test_at018_a_watermark_read_captures_a_typed_bound_then_loads_the_slice(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    value = _h("2026-10-01 00:00:00")
    assert value == "_utf8mb4 X'323032362d31302d30312030303a30303a3030'"
    bound = _h(BOUND_TEXT)

    frame = _reader(
        conn,
        spark,
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    assert _transcript(log.events) == ["load", "schema", "first", "load"]
    first, data = _loads(log)
    _, schema, row, _ = log.events
    assert isinstance(schema, _Schema)
    assert isinstance(row, _First)
    assert schema.frame is first.frame
    assert row.frame is first.frame
    assert first.source == "jdbc"
    assert _lower(first.options) == _lower(
        {**BASE_OPTIONS, **READ_DEFAULTS, **TIME_ZONE_OPTIONS, "query": _bound_query(value)}
    )
    assert data.source == "jdbc"
    predicate = f"`updated_at` > {value} AND `updated_at` <= {bound}"
    assert _lower(data.options) == _lower(
        {**BASE_OPTIONS, **READ_DEFAULTS, **TIME_ZONE_OPTIONS, "dbtable": _slice(predicate)}
    )
    assert frame is _frame(data.frame)
    assert _attributes(data.frame) == []


def test_at019_without_a_watermark_value_the_slice_ends_at_the_bound(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    _reader(conn, spark, watermark_column="updated_at").read()

    first, data = _loads(log)
    assert _lower(first.options)["query"] == (
        "SELECT CAST(max(`updated_at`) AS CHAR) AS upper_bound,"
        " max(`updated_at`) AS upper_bound_value FROM `shop`.`orders`"
    )
    assert _lower(data.options)["dbtable"] == _slice(f"`updated_at` <= {_h(BOUND_TEXT)}")


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
def test_at020_without_a_bound_the_slice_is_empty(
    value: int | None, log: _Log, conn: connectors.MySQLConnection
) -> None:
    spark = _FakeSpark(log, bound_rows=[(None, None)])

    frame = _reader(conn, spark, watermark_column="updated_at", watermark_value=value).read()

    _, data = _loads(log)
    assert _lower(data.options)["dbtable"] == (
        "(SELECT * FROM `shop`.`orders` WHERE false) AS ingestio_source"
    )
    assert frame is _frame(data.frame)


AT021_LITERALS = [
    pytest.param(41, "41", id="int"),
    pytest.param(0, "0", id="zero"),
    pytest.param(-7, "-7", id="negative-int"),
    pytest.param(Decimal("12.50"), "12.50", id="decimal"),
    pytest.param(Decimal("1E+3"), "1000", id="decimal-exponent"),
    pytest.param(Decimal("-0.25"), "-0.25", id="negative-decimal"),
    pytest.param("41", "_utf8mb4 X'3431'", id="numeric-string"),
    pytest.param("O'Brien", "_utf8mb4 X'4f27427269656e'", id="quote"),
    pytest.param(r"C:\temp", "_utf8mb4 X'433a5c74656d70'", id="backslash"),
    pytest.param("", "_utf8mb4 X''", id="empty-string"),
    pytest.param("\u00e9", "_utf8mb4 X'c3a9'", id="non-ascii"),
    pytest.param(date(2026, 10, 1), "_utf8mb4 X'323032362d31302d3031'", id="date"),
    pytest.param(
        datetime(2026, 10, 1, 8, 30, tzinfo=UTC), _h("2026-10-01 08:30:00"), id="utc-datetime"
    ),
    pytest.param(
        datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3))),
        _h("2026-10-01 11:30:00.123456"),
        id="offset-datetime",
    ),
]


@pytest.mark.parametrize(("value", "literal"), AT021_LITERALS)
def test_at021_watermark_values_become_numeric_or_hex_string_literals(
    value: int | Decimal | str | date,
    literal: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MySQLConnection,
) -> None:
    _reader(conn, spark, watermark_column="updated_at", watermark_value=value).read()

    first, data = _loads(log)
    query = _lower(first.options)["query"]
    dbtable = _lower(data.options)["dbtable"]
    assert query.endswith(f" WHERE `updated_at` > {literal}"), query
    assert f" WHERE `updated_at` > {literal} AND `updated_at` <= " in dbtable, dbtable
    for load in (first, data):
        for option, text in load.options.items():
            assert "'" not in HEX_LITERAL.sub("", text), (option, text)


AT022_BOUNDS = [
    pytest.param(LongType(), "9007199254740993", "9007199254740993", id="long"),
    pytest.param(IntegerType(), "-7", "-7", id="integer"),
    pytest.param(DecimalType(18, 2), "12.50", "12.50", id="decimal"),
    pytest.param(
        DecimalType(20, 0), "18446744073709551615", "18446744073709551615", id="unsigned-bigint"
    ),
    pytest.param(DoubleType(), "1e20", "1e20", id="double"),
    pytest.param(LongType(), "1 OR 1=1", _h("1 OR 1=1"), id="long-not-a-number"),
    pytest.param(StringType(), "41", "_utf8mb4 X'3431'", id="string"),
    pytest.param(StringType(), "it's", _h("it's"), id="string-with-a-quote"),
    pytest.param(TimestampType(), "2026-10-21 09:30:00", _h("2026-10-21 09:30:00"), id="timestamp"),
    pytest.param(DateType(), "2026-10-21", _h("2026-10-21"), id="date"),
    pytest.param(BooleanType(), "1", _h("1"), id="boolean"),
]


@pytest.mark.parametrize(("value_type", "text", "literal"), AT022_BOUNDS)
def test_at022_the_bound_is_unquoted_only_for_a_numeric_type_and_a_number(
    value_type: DataType, text: str, literal: str, log: _Log, conn: connectors.MySQLConnection
) -> None:
    spark = _FakeSpark(log, bound_rows=[(text, None)], schema=_bound_schema(value_type))

    _reader(conn, spark, watermark_column="w").read()

    _, data = _loads(log)
    assert _lower(data.options)["dbtable"] == _slice(f"`w` <= {literal}")


@pytest.mark.parametrize(
    ("table_name", "column", "quoted_table", "quoted_column"),
    [
        pytest.param("orders", "updated_at", "`orders`", "`updated_at`", id="unqualified"),
        pytest.param("Sales.Orders", "UpdatedAt", "`Sales`.`Orders`", "`UpdatedAt`", id="case"),
        pytest.param("shop.my`table", "my`col", "`shop`.`my``table`", "`my``col`", id="backtick"),
        pytest.param('shop.a"b', 'c"d', '`shop`.`a"b`', '`c"d`', id="double-quote"),
    ],
)
def test_at023_table_and_column_names_are_always_quoted_with_backticks(
    table_name: str,
    column: str,
    quoted_table: str,
    quoted_column: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MySQLConnection,
) -> None:
    connectors.MySQLReader(table_name, conn, spark=_session(spark)).read()
    connectors.MySQLReader(
        table_name, conn, spark=_session(spark), watermark_column=column, watermark_value=1
    ).read()

    full, first, data = _loads(log)
    assert _lower(full.options)["dbtable"] == quoted_table
    assert _lower(first.options)["query"] == _bound_query(
        "1", table=quoted_table, column=quoted_column
    )
    assert _lower(data.options)["dbtable"] == _slice(
        f"{quoted_column} > 1 AND {quoted_column} <= {_h(BOUND_TEXT)}", table=quoted_table
    )


def test_at024_the_bound_load_leaves_out_the_partitioning_options(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection
) -> None:
    options: dict[str, str | int | float | bool] = {
        "partitionColumn": "id",
        "lowerBound": 1,
        "upperBound": 1000,
        "NumPartitions": 8,
        "queryTimeout": 30,
    }

    _reader(conn, spark, watermark_column="updated_at", watermark_value=1, options=options).read()

    first, data = _loads(log)
    partitioning = {"partitioncolumn", "lowerbound", "upperbound", "numpartitions"}
    assert _lower(first.options)["querytimeout"] == "30"
    assert not partitioning & set(_lower(first.options)), first.options
    data_options = _lower(data.options)
    assert {key: data_options.get(key) for key in [*sorted(partitioning), "querytimeout"]} == {
        "lowerbound": "1",
        "numpartitions": "8",
        "partitioncolumn": "id",
        "upperbound": "1000",
        "querytimeout": "30",
    }


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at025_a_failed_load_propagates_and_nothing_is_loaded_after_it(
    watermark: bool, log: _Log, conn: connectors.MySQLConnection
) -> None:
    err = RuntimeError("down")
    spark = _FakeSpark(log, load_error=err)
    reader = _reader(
        conn,
        spark,
        watermark_column="updated_at" if watermark else None,
        watermark_value=41 if watermark else None,
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert len(_loads(log)) == 1


def test_at025_a_failed_bound_row_propagates_and_nothing_is_loaded_after_it(
    log: _Log, conn: connectors.MySQLConnection
) -> None:
    err = RuntimeError("down")
    spark = _FakeSpark(log, first_error=err)
    reader = _reader(conn, spark, watermark_column="updated_at", watermark_value=41)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert len(_loads(log)) == 1


def test_at026_each_read_captures_its_own_bound(
    log: _Log, conn: connectors.MySQLConnection
) -> None:
    later = "2026-10-22 10:00:00"
    spark = _FakeSpark(log, bound_rows=[(BOUND_TEXT, None), (later, None)])
    reader = _reader(conn, spark, watermark_column="updated_at", watermark_value=41)

    first = reader.read()
    second = reader.read()

    loads = _loads(log)
    assert len(loads) == 4
    assert _lower(loads[1].options)["dbtable"] == _slice(
        f"`updated_at` > 41 AND `updated_at` <= {_h(BOUND_TEXT)}"
    )
    assert _lower(loads[3].options)["dbtable"] == _slice(
        f"`updated_at` > 41 AND `updated_at` <= {_h(later)}"
    )
    assert first is _frame(loads[1].frame)
    assert second is _frame(loads[3].frame)
    assert first is not second


# --- Writing ------------------------------------------------------------------------------


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at027_write_appends_through_a_staging_table_in_one_transaction(
    call: WriteCall,
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
) -> None:
    returned = call(connectors.MySQLWriter("shop.orders", conn), _frame(df))

    assert returned is None
    stg = _staging(log)
    assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert _transcript(log.events) == _write_transcript(stg)
    assert pm.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]
    for execution in _executed(log):
        assert (execution.args, execution.kwargs) == ((), {}), execution
    (save,) = _saves(log)
    assert (save.source, save.mode) == ("jdbc", "append")
    assert _lower(save.options) == _lower(_save_options(stg))
    assert all(connection.closed for connection in pm.connections)


def test_at028_write_with_replace_deletes_the_target_rows_in_the_same_transaction(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    connectors.MySQLWriter("shop.orders", conn).write(_frame(df), replace=True)

    stg = _staging(log)
    assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert _transcript(log.events) == _write_transcript(stg, replace=True)
    assert _committed(log) == [
        (1, _create(stg)),
        (2, f"DELETE FROM {ORDERS}"),
        (2, _insert(stg)),
        (2, f"DROP TABLE {stg}"),
    ]
    assert pm.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]
    for execution in _executed(log):
        assert (execution.args, execution.kwargs) == ((), {}), execution
    (save,) = _saves(log)
    assert (save.source, save.mode) == ("jdbc", "append")
    assert _lower(save.options) == _lower(_save_options(stg))
    assert all(connection.closed for connection in pm.connections)


def test_at029_each_write_gets_a_new_staging_table(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    writer = connectors.MySQLWriter("shop.orders", conn)
    writer.write(_frame(df))
    writer.write(_frame(df))

    first, second = _staging_tables(log)
    for stg in (first, second):
        assert STAGING_NAME.search(stg), stg
        assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert first != second


def test_at029_an_unqualified_target_gets_an_unqualified_staging_table(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    connectors.MySQLWriter("orders", conn).write(_frame(df))

    stg = _staging(log)
    assert UNQUALIFIED_STAGING.fullmatch(stg), stg
    assert _transcript(log.events) == _write_transcript(stg, target="`orders`")
    (save,) = _saves(log)
    assert _lower(save.options)["dbtable"] == stg


def test_at030_user_options_replace_the_write_defaults(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    options: dict[str, str | int | float | bool] = {
        "numPartitions": 4,
        "BatchSize": 5000,
        "RewriteBatchedStatements": False,
    }

    connectors.MySQLWriter("shop.orders", conn, options=options).write(_frame(df))

    stg = _staging(log)
    (save,) = _saves(log)
    assert _lower(save.options) == _lower(
        {
            **_save_options(stg),
            "numPartitions": "4",
            "BatchSize": "5000",
            "RewriteBatchedStatements": "false",
        }
    )
    assert save.options.get("RewriteBatchedStatements") == "false", save.options


def test_at030_the_connector_j_properties_keep_their_case(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    connectors.MySQLWriter("shop.orders", conn).write(_frame(df))

    (save,) = _saves(log)
    for key, value in {**WRITE_DEFAULTS, **TIME_ZONE_OPTIONS}.items():
        assert save.options.get(key) == value, save.options


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at030_a_write_reads_only_columns_and_write_and_saves_once(
    call: WriteCall,
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
) -> None:
    call(connectors.MySQLWriter("shop.orders", conn), _frame(df))

    assert set(_attributes(df)) <= {"columns", "write"}, _attributes(df)
    assert len(_saves(log)) == 1


# The rows of AT-031: the `ssl_mode`, the `sslMode` of the save, the keyword arguments that
# `pymysql.connect` receives besides the connect arguments, and, when it receives an
# `ssl.SSLContext` as `ssl`, its `check_hostname` and `verify_mode`.
AT031_CASES: list[
    tuple[str | None, str | None, dict[str, object], tuple[bool, ssl.VerifyMode] | None]
] = [
    (None, None, {}, None),
    ("DISABLED", "DISABLED", {"ssl_disabled": True}, None),
    ("PREFERRED", "PREFERRED", {}, None),
    ("REQUIRED", "REQUIRED", {}, (False, ssl.CERT_NONE)),
    ("VERIFY_CA", "VERIFY_CA", {}, (False, ssl.CERT_REQUIRED)),
    ("VERIFY_IDENTITY", "VERIFY_IDENTITY", {}, (True, ssl.CERT_REQUIRED)),
]


@pytest.mark.parametrize(
    ("ssl_mode", "ssl_option", "added", "context"),
    AT031_CASES,
    ids=[str(case[0]) for case in AT031_CASES],
)
def test_at031_the_ssl_mode_reaches_pymysql_and_connector_j(
    ssl_mode: str | None,
    ssl_option: str | None,
    added: dict[str, object],
    context: tuple[bool, ssl.VerifyMode] | None,
    log: _Log,
    pm: _FakePyMySQL,
    df: _FakeDataFrame,
) -> None:
    connection = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD, ssl_mode=ssl_mode
    )

    connectors.MySQLWriter("shop.orders", connection).write(_frame(df))

    assert len(pm.calls) == 2
    for args, kwargs in pm.calls:
        assert args == ()
        received = dict(kwargs)
        if context is not None:
            ssl_context = received.pop("ssl", None)
            assert isinstance(ssl_context, ssl.SSLContext), kwargs
            assert (ssl_context.check_hostname, ssl_context.verify_mode) == context
        assert received == {**CONNECT_ARGS, **added}
        for name, value in added.items():
            assert received[name] is value, name
    (save,) = _saves(log)
    if ssl_option is None:
        assert "sslmode" not in _lower(save.options), save.options
    else:
        assert save.options.get("sslMode") == ssl_option, save.options


def test_at031_pymysql_connect_is_looked_up_each_time_a_connection_opens(
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = connectors.MySQLWriter("shop.orders", conn)
    replacement = _FakePyMySQL(log)
    monkeypatch.setattr(pymysql, "connect", replacement.connect)

    writer.write(_frame(df))

    assert pm.calls == []
    assert replacement.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at032_a_failed_save_drops_the_staging_table_on_a_new_connection(
    replace: bool, log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection
) -> None:
    err = RuntimeError("executor lost")
    frame = _FakeDataFrame(log, columns=COLUMNS, save_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(frame), replace=replace)

    assert excinfo.value is err
    stg = _staging(log)
    assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert _committed(log) == [(1, _create(stg)), (2, f"DROP TABLE IF EXISTS {stg}")]
    statements = _statements(log)
    assert not [s for s in statements if s.startswith(("INSERT", "DELETE"))], statements
    assert len(pm.calls) == 2
    assert pm.connections
    assert all(connection.closed for connection in pm.connections)


def _assert_cleaned_up_after_the_apply(log: _Log, pm: _FakePyMySQL) -> str:
    """After a failure on the apply's connection: it committed nothing, ran no `DROP TABLE` of
    the staging table and is closed, and a third connection dropped the staging table. Returns
    the staging table."""
    stg = _staging(log)
    assert len(pm.connections) == 3, _transcript(log.events)
    apply, cleanup = pm.connections[1], pm.connections[2]
    assert _commits(log, apply.number) == []
    assert f"DROP TABLE {stg}" not in _statements(log, apply.number)
    assert apply.closed
    assert _statements(log, cleanup.number) == [f"DROP TABLE IF EXISTS {stg}"]
    assert _committed(log) == [(1, _create(stg)), (cleanup.number, f"DROP TABLE IF EXISTS {stg}")]
    assert cleanup.closed
    return stg


def test_at033_a_failed_insert_commits_nothing_and_the_staging_table_is_dropped(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("insert failed")
    pm.execute_errors.append((2, "INSERT ", err))

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df))

    assert excinfo.value is err
    _assert_cleaned_up_after_the_apply(log, pm)


def test_at033_a_failed_delete_commits_nothing_and_the_staging_table_is_dropped(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("delete failed")
    pm.execute_errors.append((2, "DELETE ", err))

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df), replace=True)

    assert excinfo.value is err
    _assert_cleaned_up_after_the_apply(log, pm)
    statements = _statements(log)
    assert not [s for s in statements if s.startswith("INSERT")], statements


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at034_a_failed_commit_of_the_apply_leaves_the_target_and_drops_the_staging_table(
    replace: bool,
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
) -> None:
    err = RuntimeError("commit failed")
    pm.method_errors[(2, "commit")] = err

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df), replace=replace)

    assert excinfo.value is err
    _assert_cleaned_up_after_the_apply(log, pm)


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at034_a_failed_begin_runs_no_statement_on_the_target(
    replace: bool,
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
) -> None:
    err = RuntimeError("begin failed")
    pm.method_errors[(2, "begin")] = err

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df), replace=replace)

    assert excinfo.value is err
    _assert_cleaned_up_after_the_apply(log, pm)
    statements = _statements(log)
    assert not [s for s in statements if s.startswith(("INSERT", "DELETE"))], statements


def test_at035_a_failed_create_closes_its_connection_and_moves_no_data(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("Table 'shop.orders' doesn't exist")
    pm.execute_errors.append((1, "CREATE ", err))

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df))

    assert excinfo.value is err
    assert len(pm.calls) == 1
    assert _committed(log) == []
    assert pm.connections[0].closed
    assert _saves(log) == []


def test_at035_a_failed_first_connection_moves_no_data(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("Can't connect to MySQL server")
    pm.connect_errors[1] = err

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df))

    assert excinfo.value is err
    assert len(pm.calls) == 1
    assert pm.connections == []
    assert _committed(log) == []
    assert _saves(log) == []


@pytest.mark.parametrize("cleanup", ["statement", "connect"])
@pytest.mark.parametrize("failure", ["save", "insert", "commit"])
def test_at036_a_failed_cleanup_becomes_a_note_on_the_original_error(
    failure: str, cleanup: str, log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection
) -> None:
    err = RuntimeError(f"{failure} failed")
    frame = _FakeDataFrame(log, columns=COLUMNS, save_error=err if failure == "save" else None)
    if failure == "insert":
        pm.execute_errors.append((2, "INSERT ", err))
    elif failure == "commit":
        pm.method_errors[(2, "commit")] = err
    # The cleanup's connection is the second after a failed save, the third after the apply.
    number = 2 if failure == "save" else 3
    refused = RuntimeError("refused")
    if cleanup == "statement":
        pm.execute_errors.append((number, "DROP TABLE IF EXISTS ", refused))
    else:
        pm.connect_errors[number] = refused

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(frame))

    assert excinfo.value is err
    stg = _staging(log)
    assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert _notes(err) == [f"staging table {stg} could not be dropped: RuntimeError: refused"]


def test_at037_a_failed_drop_after_the_commit_propagates_with_a_note(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("drop failed")
    pm.execute_errors.append((2, "DROP TABLE `", err))

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter("shop.orders", conn).write(_frame(df))

    assert excinfo.value is err
    stg = _staging(log)
    assert QUALIFIED_STAGING.fullmatch(stg), stg
    assert _notes(err) == [
        f"the rows were committed to {ORDERS}, but staging table {stg} could not be dropped"
    ]
    assert (2, _insert(stg)) in _committed(log)
    assert pm.connections[1].closed
    assert len(pm.calls) == 2


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at038_a_dataframe_without_columns_is_rejected_before_anything_runs(
    replace: bool, log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection
) -> None:
    frame = _FakeDataFrame(log, columns=[])

    with pytest.raises(ValueError, match=_exactly("the DataFrame has no columns")):
        connectors.MySQLWriter("shop.orders", conn).write(_frame(frame), replace=replace)

    assert pm.calls == []
    assert _saves(log) == []


def test_at039_column_names_are_quoted_and_every_statement_runs_without_arguments(
    log: _Log, pm: _FakePyMySQL, conn: connectors.MySQLConnection
) -> None:
    frame = _FakeDataFrame(log, columns=["Id", "my`col", "pct%"])

    connectors.MySQLWriter("shop.orders", conn).write(_frame(frame), replace=True)

    stg = _staging(log)
    columns = "`Id`, `my``col`, `pct%`"
    assert f"INSERT INTO {ORDERS} ({columns}) SELECT {columns} FROM {stg}" in _statements(log)
    executions = _executed(log)
    assert executions
    for execution in executions:
        assert (execution.args, execution.kwargs) == ((), {}), execution


# --- In a configuration -------------------------------------------------------------------


def test_at040_read_source_builds_the_reader_of_the_section_without_touching_spark(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    extract = ReadSource(path, connection=conn, spark=_session(spark))

    assert lookups == []
    assert log.spark_attributes == []
    assert log.events == []

    process = Process([Step("extract", [extract])]).run()

    (load,) = _loads(log)
    assert process.state["records"] is load.frame
    twin_log = _Log()
    _reader(conn, _FakeSpark(twin_log)).read()
    assert _shape(twin_log), "the twin recorded nothing"
    assert _shape(log) == _shape(twin_log)
    assert lookups == []


def test_at040_the_keys_of_the_section_and_the_arguments_reach_the_reader(
    log: _Log, spark: _FakeSpark, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    section = {**DEFAULT_SOURCE, "watermark_column": "updated_at", "options": {"fetchsize": 500}}
    path = _config(tmp_path, {"source": section})
    extract = ReadSource(path, connection=conn, watermark_value=41, spark=_session(spark))

    Process([Step("extract", [extract])]).run()

    twin_log = _Log()
    _reader(
        conn,
        _FakeSpark(twin_log),
        watermark_column="updated_at",
        watermark_value=41,
        options={"fetchsize": 500},
    ).read()
    assert [entry[0] for entry in _shape(twin_log)] == ["load", "schema", "first", "load"]
    assert _shape(log) == _shape(twin_log)


def test_at040_mypy_strict_accepts_a_mysql_connection_in_the_callbacks_and_the_protocols(
    log: _Log, spark: _FakeSpark, pm: _FakePyMySQL, tmp_path: Path
) -> None:
    """mypy strict, which checks `tests/`, accepts the annotated bindings and the calls of this
    test."""
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    conn: connectors.MySQLConnection = connectors.MySQLConnection(
        "db.internal", "shop", "ingest", PASSWORD
    )

    ReadSource(path, connection=conn)
    AppendOnly(path, connection=conn)
    FullLoad(path, connection=conn)
    SCD2(path, connection=conn)
    reader: Reader[DataFrame] = connectors.MySQLReader("shop.orders", conn, spark=_session(spark))
    writer: Writer[DataFrame] = connectors.MySQLWriter("shop.orders", conn)

    frame = reader.read()
    (load,) = _loads(log)
    assert frame is _frame(load.frame)
    assert isinstance(writer, connectors.MySQLWriter)
    assert pm.calls == []


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at041_append_only_and_full_load_build_the_writer_and_choose_the_operation(
    name: str,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"target": {**DEFAULT_TARGET, "options": {"numPartitions": 4}}})
    callback = CALLBACKS[name](path, connection=conn)
    assert pm.calls == []

    calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def write(writer: object, *args: object, **kwargs: object) -> None:
        calls.append((writer, args, kwargs))

    monkeypatch.setattr(connectors.MySQLWriter, "write", write)
    Process([Step("load", [cast(Any, callback)])], state={"records": _frame(df)}).run()

    assert len(calls) == 1, calls
    ((writer, args, kwargs),) = calls
    twin = connectors.MySQLWriter("shop.orders", conn, options={"numPartitions": 4})
    _assert_twin(writer, twin, conn)
    assert any(value is conn for value in vars(writer).values()), vars(writer)
    assert len(args) == 1
    assert args[0] is df
    assert kwargs == ({"replace": True} if name == "FullLoad" else {})
    assert pm.calls == []


# The rows of AT-042: an id, the section, the section's keys and the message after `<path>: `.
AT042_ROWS: list[tuple[str, str, dict[str, object], str]] = [
    (
        "source-without-table-name",
        "source",
        {"system": "mysql"},
        "source.table_name is required for system 'mysql'",
    ),
    (
        "target-without-table-name",
        "target",
        {"system": "mysql"},
        "target.table_name is required for system 'mysql'",
    ),
    *[
        (
            f"source-key-{key}",
            "source",
            {**DEFAULT_SOURCE, key: "db.internal"},
            f"source.{key} is not a key of system 'mysql'",
        )
        for key in (
            "host",
            "database",
            "user",
            "password",
            "port",
            "ssl_mode",
            "connection",
            "watermark_value",
            "fetchsize",
        )
    ],
    (
        "target-watermark-column",
        "target",
        {**DEFAULT_TARGET, "watermark_column": "updated_at"},
        "target.watermark_column is not a key of system 'mysql'",
    ),
    (
        "source-number-table-name",
        "source",
        {**DEFAULT_SOURCE, "table_name": 1},
        "source.table_name must be a string",
    ),
    (
        "source-null-watermark-column",
        "source",
        {**DEFAULT_SOURCE, "watermark_column": None},
        "source.watermark_column must be a string",
    ),
    (
        "target-array-option",
        "target",
        {**DEFAULT_TARGET, "options": {"batchsize": [1]}},
        "target.options must be an object of strings, numbers and booleans",
    ),
]


def _section_callbacks(section: str) -> list[str]:
    """The callbacks that build the connector of `section`."""
    return ["ReadSource"] if section == "source" else ["AppendOnly", "FullLoad"]


@pytest.mark.parametrize(
    ("callback", "section", "keys", "message"),
    [
        pytest.param(callback, section, keys, message, id=f"{callback}-{name}")
        for name, section, keys, message in AT042_ROWS
        for callback in _section_callbacks(section)
    ],
)
def test_at042_a_mysql_section_with_wrong_keys_is_rejected(
    callback: str,
    section: str,
    keys: dict[str, object],
    message: str,
    conn: connectors.MySQLConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {section: keys})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        CALLBACKS[callback](path, connection=conn)


def test_at043_read_source_needs_the_connection(tmp_path: Path) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    message = f"{path}: source system 'mysql' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at043_append_only_and_full_load_need_the_connection(name: str, tmp_path: Path) -> None:
    path = _config(tmp_path, {"target": DEFAULT_TARGET})

    message = f"{path}: target system 'mysql' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        CALLBACKS[name](path)


@pytest.mark.parametrize("name", ["ReadSource", "AppendOnly", "FullLoad"])
def test_at043_mysql_takes_no_client(
    name: str, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    section = "source" if name == "ReadSource" else "target"
    path = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET})

    message = f"{path}: {section} system 'mysql' takes no argument client"
    with pytest.raises(ValueError, match=_exactly(message)):
        CALLBACKS[name](path, connection=conn, client=object())


def test_at043_arguments_given_as_none_are_not_given(
    conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)


# The rows of AT-044: the callback, the file, the `connection` given and the message after
# `<path>: `. `connection` is `postgres` for a `PostgresConnection`, `mongodb` for a
# `MongoDBConnection` and `conn` for the criteria's `MySQLConnection`.
AT044_TO_MYSQL = "system 'mysql' needs a MySQLConnection as the argument connection, not"
AT044_ROWS: list[tuple[str, dict[str, object], str, str]] = [
    (
        "ReadSource",
        {"source": DEFAULT_SOURCE},
        "postgres",
        f"source {AT044_TO_MYSQL} PostgresConnection",
    ),
    (
        "ReadSource",
        {"source": DEFAULT_SOURCE},
        "mongodb",
        f"source {AT044_TO_MYSQL} MongoDBConnection",
    ),
    (
        "AppendOnly",
        {"target": DEFAULT_TARGET},
        "postgres",
        f"target {AT044_TO_MYSQL} PostgresConnection",
    ),
    (
        "FullLoad",
        {"target": DEFAULT_TARGET},
        "postgres",
        f"target {AT044_TO_MYSQL} PostgresConnection",
    ),
    (
        "ReadSource",
        {"source": POSTGRES_SECTION},
        "conn",
        "source system 'postgres' needs a PostgresConnection as the argument connection,"
        " not MySQLConnection",
    ),
    (
        "ReadSource",
        {"source": MONGODB_SECTION},
        "conn",
        "source system 'mongodb' needs a MongoDBConnection as the argument connection,"
        " not MySQLConnection",
    ),
    (
        "AppendOnly",
        {"target": POSTGRES_SECTION},
        "conn",
        "target system 'postgres' needs a PostgresConnection as the argument connection,"
        " not MySQLConnection",
    ),
    (
        "FullLoad",
        {"target": POSTGRES_SECTION},
        "conn",
        "target system 'postgres' needs a PostgresConnection as the argument connection,"
        " not MySQLConnection",
    ),
]


def _postgres() -> PostgresConnection:
    """The `PostgresConnection` of AT-044."""
    return PostgresConnection("db.internal", "shop", "ingest", "pw")


def _given_connection(kind: str, conn: connectors.MySQLConnection) -> object:
    """The `connection` that a row of AT-044 gives."""
    if kind == "postgres":
        return _postgres()
    if kind == "mongodb":
        return MongoDBConnection("mongodb://db.internal")
    assert kind == "conn", kind
    return conn


def _at044_id(callback: str, document: dict[str, object], kind: str) -> str:
    (section,) = document.values()
    return f"{callback}-{cast('dict[str, object]', section)['system']}-{kind}"


@pytest.mark.parametrize(
    ("callback", "document", "kind", "message"),
    [pytest.param(*row, id=_at044_id(row[0], row[1], row[2])) for row in AT044_ROWS],
)
def test_at044_a_connection_of_another_systems_type_is_rejected(
    callback: str,
    document: dict[str, object],
    kind: str,
    message: str,
    conn: connectors.MySQLConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, document)

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        CALLBACKS[callback](path, connection=_given_connection(kind, conn))


def test_at044_a_subclass_of_mysql_connection_is_accepted(tmp_path: Path) -> None:
    class _OwnConnection(connectors.MySQLConnection):
        """A job's own subclass of the connection."""

    path = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET})
    connection = _OwnConnection("db.internal", "shop", "ingest", PASSWORD)

    ReadSource(path, connection=connection)
    AppendOnly(path, connection=connection)


def test_at045_a_source_system_close_to_mysql_is_rejected_with_mysql_in_the_list(
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": "MySQL"}})

    message = f"{path}: source.system 'MySQL' is not one of: {SOURCE_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


def test_at045_mariadb_is_not_a_target_system_and_mysql_is_in_the_list(tmp_path: Path) -> None:
    path = _config(tmp_path, {"target": {**DEFAULT_TARGET, "system": "mariadb"}})

    message = f"{path}: target.system 'mariadb' is not one of: {TARGET_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        AppendOnly(path)


# The rows of AT-046: an id, the section, the keys added to its default, and the reader or the
# writer built directly with the same arguments.
AT046_ROWS: list[tuple[str, str, dict[str, object], str, dict[str, Any]]] = [
    ("three-part-table-name", "source", {"table_name": "a.b.c"}, "a.b.c", {}),
    (
        "reserved-read-option",
        "source",
        {"options": {"dbtable": "x"}},
        "shop.orders",
        {"options": {"dbtable": "x"}},
    ),
    (
        "reserved-write-option",
        "target",
        {"options": {"sslMode": "DISABLED"}},
        "shop.orders",
        {"options": {"sslMode": "DISABLED"}},
    ),
]

AT046_MESSAGES = {
    "three-part-table-name": "table_name must be 'table' or 'database.table'",
    "reserved-read-option": "option 'dbtable' is set by the connector",
    "reserved-write-option": "option 'sslMode' is set by the connector",
}


@pytest.mark.parametrize(
    ("row", "callback"),
    [
        pytest.param(row, callback, id=f"{callback}-{row[0]}")
        for row in AT046_ROWS
        for callback in _section_callbacks(row[1])
    ],
)
def test_at046_an_error_of_the_connector_propagates_with_one_note(
    row: tuple[str, str, dict[str, object], str, dict[str, Any]],
    callback: str,
    conn: connectors.MySQLConnection,
    tmp_path: Path,
) -> None:
    name, section, added, table_name, arguments = row
    default = DEFAULT_SOURCE if section == "source" else DEFAULT_TARGET
    path = _config(tmp_path, {section: {**default, **added}})
    kind = "reader" if section == "source" else "writer"
    with pytest.raises(ValueError) as built_directly:
        _build(kind, table_name, conn, **arguments)

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        CALLBACKS[callback](path, connection=conn)

    assert str(excinfo.value) == AT046_MESSAGES[name]
    note = f"while building the {section} of {path}, system 'mysql'"
    _assert_connector_error(excinfo.value, built_directly.value, note)


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at046_a_missing_pymysql_raises_the_writers_import_error_with_one_note(
    name: str, conn: connectors.MySQLConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET})
    _hide_pymysql(monkeypatch)
    with pytest.raises(ImportError) as built_directly:
        connectors.MySQLWriter("shop.orders", conn)

    with pytest.raises(ImportError) as excinfo:
        CALLBACKS[name](path, connection=conn)

    assert str(excinfo.value) == "MySQLWriter needs PyMySQL: pip install 'ingestio[mysql]'"
    note = f"while building the target of {path}, system 'mysql'"
    _assert_connector_error(excinfo.value, built_directly.value, note)
    assert isinstance(excinfo.value.__cause__, ImportError)
    ReadSource(path, connection=conn)


# --- SCD2 ---------------------------------------------------------------------------------


def test_at047_scd2_builds_with_a_mysql_target_and_checks_its_connection(
    pm: _FakePyMySQL, conn: connectors.MySQLConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, SCD2_FILE)

    SCD2(path, connection=conn)

    assert pm.calls == []
    message = f"{path}: target system 'mysql' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path)
    message = f"{path}: target {AT044_TO_MYSQL} PostgresConnection"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path, connection=_postgres())


def test_at047_scd2_lists_mysql_among_the_targets_it_supports(tmp_path: Path) -> None:
    target = {"system": "disk", "path": "/data/dim", "format": "parquet"}
    path = _config(tmp_path, {**SCD2_FILE, "target": target})

    message = f"{path}: target.system 'disk' is not one of: {SCD2_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path)


def test_at048_scd2_reads_a_mysql_target_with_the_batchs_session(
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    err = RuntimeError("stop")
    readers: list[object] = []
    writes: list[object] = []

    def read(reader: object) -> NoReturn:
        readers.append(reader)
        raise err

    def write(writer: object, *args: object, **kwargs: object) -> None:
        writes.append(writer)

    monkeypatch.setattr(connectors.MySQLReader, "read", read)
    monkeypatch.setattr(connectors.MySQLWriter, "write", write)
    path = _config(tmp_path, SCD2_FILE)
    session = _FakeSession()
    callback = SCD2(path, connection=conn)

    with pytest.raises(RuntimeError) as excinfo:
        Process(
            [Step("load", [callback])],
            state={"records": _FakeBatch(BATCH_COLUMNS, session)},
        ).run()

    assert excinfo.value is err
    assert len(readers) == 1, readers
    twin = connectors.MySQLReader("shop.dim_customer", conn, spark=_session(session))
    _assert_twin(readers[0], twin, session, conn)
    assert writes == []
    assert pm.calls == []


# --- Logging and secrets ------------------------------------------------------------------


def test_at049_nothing_writes_a_log_record(
    log: _Log,
    spark: _FakeSpark,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    source = {**DEFAULT_SOURCE, "watermark_column": "updated_at", "options": {"fetchsize": 500}}
    sliced = _config(tmp_path, {"source": source}, "sliced.json")

    for mode in (None, *SSL_MODES):
        connectors.MySQLConnection("db.internal", "shop", "ingest", PASSWORD, ssl_mode=mode)
    ReadSource(path, connection=conn, spark=_session(spark))
    ReadSource(sliced, connection=conn, watermark_value=41, spark=_session(spark))
    AppendOnly(path, connection=conn)
    FullLoad(path, connection=conn)
    SCD2(path, connection=conn)
    with pytest.raises(ValueError):
        ReadSource(path)
    with pytest.raises(ValueError):
        AppendOnly(path, connection=_postgres())

    _reader(conn, spark).read()
    _reader(conn, spark, watermark_column="updated_at", watermark_value=41).read()
    no_bound = _FakeSpark(log, bound_rows=[(None, None)])
    _reader(conn, no_bound, watermark_column="updated_at").read()

    writer = connectors.MySQLWriter("shop.orders", conn, options={"numPartitions": 4})
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)
    failing = _FakeDataFrame(log, columns=COLUMNS, save_error=RuntimeError("executor lost"))
    with pytest.raises(RuntimeError):
        writer.write(_frame(failing))
    pm.execute_errors.append((None, "DROP TABLE IF EXISTS ", RuntimeError("refused")))
    with pytest.raises(RuntimeError):
        writer.write(_frame(failing))

    assert len(_loads(log)) == 5
    assert len(_saves(log)) == 4
    assert caplog.records == []


def _raised(
    errors: list[BaseException], build: Callable[..., object], *args: Any, **kwargs: Any
) -> None:
    """Call `build` with the arguments, which must raise, and add its error to `errors`."""
    try:
        build(*args, **kwargs)
    except (ValueError, ImportError, RuntimeError) as error:
        errors.append(error)
    else:
        raise AssertionError(f"{build} raised nothing")


def _criteria_errors(
    log: _Log,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> list[BaseException]:
    """The errors that AT-005 to AT-048 raise, raised once more."""
    errors: list[BaseException] = []
    mysql = connectors.MySQLConnection
    for _, args, kwargs, _ in AT005_CASES:
        _raised(errors, mysql, *args, **kwargs)
    for kind in ("reader", "writer"):
        for name in ("", "orders.", ".orders", "a.b.c"):
            _raised(errors, _build, kind, name, conn)
        for key in RESERVED_KEYS:
            _raised(errors, _build, kind, "shop.orders", conn, options={key: "x"})
    reader = connectors.MySQLReader
    _raised(errors, reader, "shop.orders", conn, watermark_value=41)
    _raised(errors, reader, "shop.orders", conn, watermark_column="")
    for _, value, _ in AT010_CASES:
        _raised(errors, reader, "shop.orders", conn, watermark_column="u", watermark_value=value)

    writer = connectors.MySQLWriter("shop.orders", conn)
    lost = _FakeDataFrame(log, columns=COLUMNS, save_error=RuntimeError("executor lost"))
    _raised(errors, writer.write, _frame(lost))
    pm.execute_errors.append((None, "DROP TABLE IF EXISTS ", RuntimeError("refused")))
    _raised(errors, writer.write, _frame(lost))
    pm.execute_errors[:] = [(None, "DROP TABLE `", RuntimeError("drop failed"))]
    _raised(errors, writer.write, _frame(_FakeDataFrame(log, columns=COLUMNS)))
    pm.execute_errors.clear()
    _raised(errors, writer.write, _frame(_FakeDataFrame(log, columns=[])))

    for number, (_, section, keys, _) in enumerate(AT042_ROWS):
        path = _config(tmp_path, {section: keys}, f"at042-{number}.json")
        for callback in _section_callbacks(section):
            _raised(errors, CALLBACKS[callback], path, connection=conn)
    both = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET}, "both.json")
    for callback in ("ReadSource", "AppendOnly", "FullLoad"):
        _raised(errors, CALLBACKS[callback], both)
        _raised(errors, CALLBACKS[callback], both, connection=conn, client=object())
    for number, (callback, document, kind, _) in enumerate(AT044_ROWS):
        path = _config(tmp_path, document, f"at044-{number}.json")
        _raised(errors, CALLBACKS[callback], path, connection=_given_connection(kind, conn))
    for number, (_, section, added, _, _) in enumerate(AT046_ROWS):
        default = DEFAULT_SOURCE if section == "source" else DEFAULT_TARGET
        path = _config(tmp_path, {section: {**default, **added}}, f"at046-{number}.json")
        for callback in _section_callbacks(section):
            _raised(errors, CALLBACKS[callback], path, connection=conn)
    dim = _config(tmp_path, SCD2_FILE, "dim.json")
    _raised(errors, SCD2, dim)
    _raised(errors, SCD2, dim, connection=_given_connection("postgres", conn))

    with monkeypatch.context() as hidden:
        _hide_pymysql(hidden)
        _raised(errors, connectors.MySQLWriter, "shop.orders", conn)
        _raised(errors, AppendOnly, both, connection=conn)
    return errors


def test_at050_the_password_appears_in_no_repr_url_or_error(
    log: _Log,
    spark: _FakeSpark,
    pm: _FakePyMySQL,
    conn: connectors.MySQLConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    connections = [
        conn,
        connectors.MySQLConnection("::1", "my db", "ingest", PASSWORD, port=3307),
        *[
            connectors.MySQLConnection("db.internal", "shop", "ingest", PASSWORD, ssl_mode=mode)
            for mode in SSL_MODES
        ],
    ]
    built: list[object] = [*connections]
    for connection in connections:
        built.append(_reader(connection, spark))
        built.append(
            _reader(
                connection,
                spark,
                watermark_column="updated_at",
                watermark_value=41,
                options={"fetchsize": 500},
            )
        )
        built.append(connectors.MySQLWriter("shop.orders", connection))
        built.append(connectors.MySQLWriter("orders", connection, options={"batchsize": 5000}))
    built.extend(
        [
            ReadSource(path, connection=conn, spark=_session(spark)),
            AppendOnly(path, connection=conn),
            FullLoad(path, connection=conn),
            SCD2(path, connection=conn),
        ]
    )
    texts = [text for value in built for text in (repr(value), str(value))]

    for value in built:
        if isinstance(value, connectors.MySQLReader):
            value.read()
        elif isinstance(value, connectors.MySQLWriter):
            value.write(_frame(df))
    urls = [
        _lower(event.options)["url"] for event in log.events if isinstance(event, _Load | _Save)
    ]
    # Each connection gives a full read, a watermark read of two loads and two writes.
    assert len(urls) == 5 * len(connections)

    errors = _criteria_errors(log, pm, conn, monkeypatch, tmp_path)
    assert errors
    messages = [text for error in errors for text in (str(error), *_notes(error))]

    for text in [*texts, *urls, *messages]:
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


class _Database:
    """The database of `INGESTIO_MYSQL_URL`, for one test.

    Its statements run through PyMySQL, outside the connectors, in sessions of their own whose
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
        self._staging_before = set(self.staging_tables())

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
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, args)
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

    def table(self, stem: str) -> str:
        """A new table name, `ingestio_it_<stem>_<hex>`, dropped when the test ends."""
        name = f"ingestio_it_{stem}_{uuid.uuid4().hex[:12]}"
        self._tables.append(name)
        return name

    def qualified(self, name: str) -> str:
        """`name` in the URL's database, as `table_name` takes it."""
        return f"{self.database}.{name}"

    def staging_tables(self) -> list[str]:
        """The tables of the database whose name starts with `ingestio_staging_`."""
        names = self.rows(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
        )
        return sorted(str(name) for (name,) in names if str(name).startswith("ingestio_staging_"))

    def new_staging_tables(self) -> list[str]:
        """The staging tables that did not exist when the test started."""
        return [name for name in self.staging_tables() if name not in self._staging_before]

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


def _loading(
    db: _Database,
    columns: Sequence[str],
    rows: Sequence[tuple[object, ...]],
    *,
    fail_after: int | None = None,
    counts: list[int] | None = None,
) -> _FakeDataFrame:
    """A fake DataFrame whose save inserts `rows` into the table its `dbtable` option names,
    one row per transaction, through a PyMySQL connection of the test.

    With `fail_after`, the save raises `RuntimeError("executor lost")` after inserting that many
    rows, and adds the number of rows the table then holds to `counts`.
    """

    def load(options: Mapping[str, str]) -> None:
        table = _lower(options)["dbtable"]
        names = ", ".join(_quote(column) for column in columns)
        marks = ", ".join(["%s"] * len(columns))
        connection = db.connect()
        try:
            with connection.cursor() as cursor:
                for number, row in enumerate(rows):
                    if number == fail_after:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        found = cursor.fetchone()
                        assert found is not None
                        if counts is not None:
                            counts.append(int(found[0]))
                        raise RuntimeError("executor lost")
                    cursor.execute(f"INSERT INTO {table} ({names}) VALUES ({marks})", row)
        finally:
            connection.close()

    return _FakeDataFrame(_Log(), columns=columns, on_save=load)


# Tier 1: the writer against MySQL, with fake DataFrames and no Spark.


@pytest.mark.integration
@needs_mysql
def test_mysql_write_appends_and_the_target_fills_the_columns_the_dataframe_lacks(
    database: _Database,
) -> None:
    name = database.table("append")
    database.execute(
        f"CREATE TABLE {_quote(name)} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
        " name VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'new')"
    )
    database.execute(f"INSERT INTO {_quote(name)} (name, status) VALUES ('old', 'loaded')")
    frame = _loading(database, ["name"], [("a",), ("b",)])

    connectors.MySQLWriter(database.qualified(name), database.connection).write(_frame(frame))

    rows = database.rows(f"SELECT id, name, status FROM {_quote(name)} ORDER BY id")
    assert len(rows) == 3, rows
    assert rows[0][1:] == ("old", "loaded")
    assert sorted((row[1], row[2]) for row in rows[1:]) == [("a", "new"), ("b", "new")]
    # The target generated the ids of the new rows, after the id of the row it held.
    ids = [row[0] for row in rows]
    assert len(set(ids)) == 3
    assert all(isinstance(number, int) and number > ids[0] for number in ids[1:]), ids
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_mysql
def test_mysql_write_with_replace_leaves_exactly_the_rows_of_the_dataframe(
    database: _Database,
) -> None:
    name = database.table("replace")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, name VARCHAR(50))")
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    writer = connectors.MySQLWriter(database.qualified(name), database.connection)

    writer.write(_frame(_loading(database, ["id", "name"], [(10, "x"), (11, "y")])), replace=True)
    assert database.rows(f"SELECT id, name FROM {_quote(name)} ORDER BY id") == [
        (10, "x"),
        (11, "y"),
    ]

    writer.write(_frame(_loading(database, ["id", "name"], [])), replace=True)
    assert database.rows(f"SELECT id, name FROM {_quote(name)}") == []
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_mysql
def test_mysql_a_row_that_conflicts_with_the_target_fails_the_apply_and_changes_nothing(
    database: _Database,
) -> None:
    name = database.table("conflict")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, name VARCHAR(50))")
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 'a')")
    frame = _loading(database, ["id", "name"], [(2, "b"), (1, "again")])

    with pytest.raises(pymysql.err.IntegrityError):
        connectors.MySQLWriter(database.qualified(name), database.connection).write(_frame(frame))

    assert database.rows(f"SELECT id, name FROM {_quote(name)}") == [(1, "a")]
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_mysql
@pytest.mark.parametrize("action", ["RESTRICT", "CASCADE"])
def test_mysql_replace_fires_the_foreign_keys_that_reference_the_target(
    action: str, database: _Database
) -> None:
    parent, child = database.table("parent"), database.table("child")
    database.execute(
        f"CREATE TABLE {_quote(parent)} (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB"
    )
    database.execute(
        f"CREATE TABLE {_quote(child)} (id INT PRIMARY KEY, parent_id INT,"
        f" FOREIGN KEY (parent_id) REFERENCES {_quote(parent)} (id) ON DELETE {action})"
        " ENGINE=InnoDB"
    )
    database.execute(f"INSERT INTO {_quote(parent)} VALUES (1, 'a'), (2, 'b')")
    database.execute(f"INSERT INTO {_quote(child)} VALUES (1, 1)")
    frame = _loading(database, ["id", "name"], [(1, "a"), (3, "c")])
    writer = connectors.MySQLWriter(database.qualified(parent), database.connection)

    if action == "RESTRICT":
        with pytest.raises(pymysql.err.IntegrityError):
            writer.write(_frame(frame), replace=True)
        assert database.rows(f"SELECT id, name FROM {_quote(parent)} ORDER BY id") == [
            (1, "a"),
            (2, "b"),
        ]
        assert database.rows(f"SELECT id, parent_id FROM {_quote(child)}") == [(1, 1)]
    else:
        writer.write(_frame(frame), replace=True)
        assert database.rows(f"SELECT id, name FROM {_quote(parent)} ORDER BY id") == [
            (1, "a"),
            (3, "c"),
        ]
        assert database.rows(f"SELECT id, parent_id FROM {_quote(child)}") == []
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_mysql
@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_mysql_a_save_that_fails_midway_leaves_the_target_unchanged(
    replace: bool, database: _Database
) -> None:
    name = database.table("midway")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, name VARCHAR(50))")
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 'a')")
    counts: list[int] = []
    frame = _loading(
        database, ["id", "name"], [(2, "b"), (3, "c"), (4, "d")], fail_after=2, counts=counts
    )

    with pytest.raises(RuntimeError) as excinfo:
        connectors.MySQLWriter(database.qualified(name), database.connection).write(
            _frame(frame), replace=replace
        )

    assert str(excinfo.value) == "executor lost"
    assert counts == [2], "the save did not insert rows into the staging table before failing"
    assert database.rows(f"SELECT id, name FROM {_quote(name)}") == [(1, "a")]
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_mysql
def test_mysql_the_writers_statements_hold_under_ansi_quotes_and_no_backslash_escapes(
    database: _Database,
) -> None:
    name = database.table("modes")
    column = 'a"b`c'
    database.execute(
        f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, {_quote(column)} VARCHAR(50))"
    )
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 'old')")
    database.set_global(
        "sql_mode",
        "CONCAT_WS(',', NULLIF(@@GLOBAL.sql_mode, ''), 'ANSI_QUOTES', 'NO_BACKSLASH_ESCAPES')",
    )
    ((mode,),) = database.rows("SELECT @@SESSION.sql_mode")
    assert "ANSI_QUOTES" in str(mode), mode
    assert "NO_BACKSLASH_ESCAPES" in str(mode), mode
    writer = connectors.MySQLWriter(database.qualified(name), database.connection)
    select = f"SELECT id, {_quote(column)} FROM {_quote(name)} ORDER BY id"

    writer.write(_frame(_loading(database, ["id", column], [(2, "x")])))
    assert database.rows(select) == [(1, "old"), (2, "x")]

    writer.write(_frame(_loading(database, ["id", column], [(3, "y")])), replace=True)
    assert database.rows(select) == [(3, "y")]
    assert database.new_staging_tables() == []


# Tier 2: the reader and the writer against MySQL, through a local Spark session.


@pytest.fixture(scope="module")
def local_spark() -> Iterator[SparkSession]:
    """A local Spark session in UTC, with MySQL Connector/J."""
    if SparkContext._gateway is not None:
        pytest.skip(
            "a JVM was already started in this run, without MySQL Connector/J: a JVM loads its"
            " packages once, so run this file on its own"
        )
    # The JVM runs in UTC too: how Connector/J converts DATETIME values in another JVM zone is
    # not established (Known limitations of the spec).
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration-mysql")
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


def _ids(frame: DataFrame) -> list[int]:
    """The sorted values of the `id` column of `frame`."""
    return sorted(int(row["id"]) for row in frame.collect())


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_a_full_read_returns_every_row_with_mysql_types(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("full")
    database.execute(
        f"CREATE TABLE {_quote(name)} (id BIGINT PRIMARY KEY, amount DECIMAL(10,2),"
        " happened DATETIME(6), stamped TIMESTAMP(6) NULL,"
        " note VARCHAR(50) CHARACTER SET utf8mb4, doc JSON)"
    )
    note = "caf\u00e9 \U0001d11e"
    database.execute(
        f"INSERT INTO {_quote(name)} VALUES"
        " (9007199254740993, 12.50, '2026-10-01 08:30:00.123456',"
        " '2026-10-01 08:30:00.123456', %s, '{\"a\": 1}'),"
        " (2, -3.00, '2026-10-02 23:59:59.500000', '2026-10-02 23:59:59.500000',"
        " 'plain', '[1, 2]'),"
        " (3, NULL, NULL, NULL, NULL, NULL)",
        [note],
    )
    pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS"

    frame = connectors.MySQLReader(
        database.qualified(name), database.connection, spark=local_spark
    ).read()

    rows = frame.selectExpr(
        "id",
        "amount",
        f"date_format(happened, '{pattern}') AS happened",
        f"date_format(stamped, '{pattern}') AS stamped",
        "note",
        "doc",
    ).collect()
    assert sorted(
        (
            int(row["id"]),
            row["amount"],
            row["happened"],
            row["stamped"],
            row["note"],
            None if row["doc"] is None else json.loads(row["doc"]),
        )
        for row in rows
    ) == [
        (
            2,
            Decimal("-3.00"),
            "2026-10-02 23:59:59.500000",
            "2026-10-02 23:59:59.500000",
            "plain",
            [1, 2],
        ),
        (3, None, None, None, None, None),
        (
            9007199254740993,
            Decimal("12.50"),
            "2026-10-01 08:30:00.123456",
            "2026-10-01 08:30:00.123456",
            note,
            {"a": 1},
        ),
    ]


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_a_watermark_read_on_a_bigint_above_two_to_the_53_is_exact_and_frozen(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("bigint")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, w BIGINT NULL)")
    database.execute(
        f"INSERT INTO {_quote(name)} VALUES (1, 9007199254740992), (2, 9007199254740993),"
        " (3, 9007199254740994), (4, NULL)"
    )

    frame = connectors.MySQLReader(
        database.qualified(name),
        database.connection,
        watermark_column="w",
        watermark_value=9007199254740992,
        spark=local_spark,
    ).read()

    assert _ids(frame) == [2, 3]
    database.execute(f"INSERT INTO {_quote(name)} VALUES (5, 9007199254740995), (6, NULL)")
    assert _ids(frame) == [2, 3]
    first_run = connectors.MySQLReader(
        database.qualified(name), database.connection, watermark_column="w", spark=local_spark
    ).read()
    assert _ids(first_run) == [1, 2, 3, 5]


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
@pytest.mark.parametrize("column_type", ["DATETIME(6)", "TIMESTAMP(6)"])
def test_mysql_a_watermark_read_with_an_aware_datetime_ignores_the_servers_time_zone(
    column_type: str, database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("temporal")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, w {column_type} NULL)")
    database.execute(
        f"INSERT INTO {_quote(name)} VALUES (1, '2026-10-01 00:00:00'),"
        " (2, '2026-10-02 00:00:00'), (3, '2026-10-02 02:00:00'), (4, '2026-10-03 00:00:00')"
    )
    database.set_global("time_zone", "'-03:00'")
    # 22:00 at -03:00 is 01:00 UTC on 2026-10-02: only the rows after it are above the value.
    value = datetime(2026, 10, 1, 22, 0, tzinfo=timezone(timedelta(hours=-3)))

    frame = connectors.MySQLReader(
        database.qualified(name),
        database.connection,
        watermark_column="w",
        watermark_value=value,
        spark=local_spark,
    ).read()

    assert _ids(frame) == [3, 4]


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
@pytest.mark.parametrize("no_backslash_escapes", [False, True], ids=["default", "no-escapes"])
def test_mysql_a_watermark_read_on_text_with_quotes_and_backslashes_is_exact(
    no_backslash_escapes: bool, database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("text")
    database.execute(
        f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, w VARCHAR(20) CHARACTER SET utf8mb4)"
    )
    values = ["a'\\a", "b'\\b", "c'\\c", "d'\\d"]
    for number, text in enumerate(values, start=1):
        database.execute(f"INSERT INTO {_quote(name)} VALUES (%s, %s)", [number, text])
    if no_backslash_escapes:
        database.set_global(
            "sql_mode", "CONCAT_WS(',', NULLIF(@@GLOBAL.sql_mode, ''), 'NO_BACKSLASH_ESCAPES')"
        )

    frame = connectors.MySQLReader(
        database.qualified(name),
        database.connection,
        watermark_column="w",
        watermark_value=values[1],
        spark=local_spark,
    ).read()

    assert sorted((int(row["id"]), row["w"]) for row in frame.collect()) == [
        (3, values[2]),
        (4, values[3]),
    ]


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_timestamps_read_and_written_back_keep_their_instants(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("instants")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, stamped TIMESTAMP(6) NULL)")
    database.execute(
        f"INSERT INTO {_quote(name)} VALUES (1, '2026-10-01 08:30:00.123456'),"
        " (2, '2026-03-08 06:30:00.000000')"
    )
    database.set_global("time_zone", "'-03:00'")
    table_name = database.qualified(name)

    frame = connectors.MySQLReader(table_name, database.connection, spark=local_spark).read()
    connectors.MySQLWriter(table_name, database.connection).write(frame, replace=True)

    assert database.rows(f"SELECT id, stamped FROM {_quote(name)} ORDER BY id") == [
        (1, datetime(2026, 10, 1, 8, 30, 0, 123456)),  # noqa: DTZ001 - PyMySQL's naive UTC
        (2, datetime(2026, 3, 8, 6, 30)),  # noqa: DTZ001 - PyMySQL's naive UTC
    ]
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_a_value_the_target_type_rejects_fails_during_the_spark_load(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("range")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, small TINYINT NOT NULL)")
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 1)")
    frame = local_spark.createDataFrame([(2, 1000)], "id INT, small INT")

    with pytest.raises(Exception, match=r"(?i)out of range") as excinfo:
        connectors.MySQLWriter(database.qualified(name), database.connection).write(frame)

    # The staging table rejected the value, in Spark: the apply never ran.
    assert not isinstance(excinfo.value, pymysql.err.Error)
    assert database.rows(f"SELECT id, small FROM {_quote(name)}") == [(1, 1)]
    assert database.new_staging_tables() == []


@pytest.mark.integration
@needs_connector_j
@needs_spark
@needs_mysql
def test_mysql_a_column_the_target_lacks_fails_before_any_row_is_written(
    database: _Database, local_spark: SparkSession
) -> None:
    name = database.table("columns")
    database.execute(f"CREATE TABLE {_quote(name)} (id INT PRIMARY KEY, name VARCHAR(50))")
    database.execute(f"INSERT INTO {_quote(name)} VALUES (1, 'a')")
    frame = local_spark.createDataFrame([(2, "b", "x")], "id INT, name STRING, extra STRING")

    # Spark matches the DataFrame's columns with the staging table's before it writes a row.
    with pytest.raises(Exception, match="extra") as excinfo:
        connectors.MySQLWriter(database.qualified(name), database.connection).write(frame)

    assert not isinstance(excinfo.value, pymysql.err.Error)
    assert database.rows(f"SELECT id, name FROM {_quote(name)}") == [(1, "a")]
    assert database.new_staging_tables() == []
