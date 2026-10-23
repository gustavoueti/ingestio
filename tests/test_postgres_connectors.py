"""PostgreSQL connectors: `PostgresConnection`, `PostgresReader` and `PostgresWriter`.

Covers `specs/postgres_connectors.md`. The tests named after an acceptance criterion always run.
They need neither a JVM nor a PostgreSQL server: they use the doubles below, a fake Spark
session, fake DataFrames and a fake `psycopg.connect`, which all write to one ordered event log.

The integration tests at the end run the connectors against a real PostgreSQL and a local Spark
session. They are marked `integration` and skipped unless `INGESTIO_POSTGRES_URL` holds a URL of
the form `postgresql://<user>:<password>@<host>:<port>/<database>`. They need Java 17 or 21, and
the local session fetches the PostgreSQL JDBC driver from Maven Central through
`spark.jars.packages`. Each test works in a schema of its own, dropped afterwards.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
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
from typing import Any, TypeAlias, cast
from urllib.parse import unquote, urlsplit

import psycopg
import pytest
from psycopg import sql
from pyspark.sql import DataFrame, SparkSession

from ingestio import Process, Reader, ReadSource, Step, Writer, WriteTarget
from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter

BASE_OPTIONS = {
    "url": "jdbc:postgresql://db.internal:5432/shop",
    "user": "ingest",
    "password": "s3cret",
    "driver": "org.postgresql.Driver",
}
CONNECT_ARGS: dict[str, object] = {
    "host": "db.internal",
    "port": 5432,
    "dbname": "shop",
    "user": "ingest",
    "password": "s3cret",
}
BOUND = "2026-10-21 09:30:00.5+00"
ORDERS = '"public"."orders"'
QUALIFIED_STAGING = r'"public"\."ingestio_staging_[0-9a-f]{32}"'
UNQUALIFIED_STAGING = r'"ingestio_staging_[0-9a-f]{32}"'
EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']postgres["']""")


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read`, with this format and these merged options."""

    source: str | None
    options: dict[str, str]
    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Save:
    """`save()` ran on a fake `DataFrame.write`, with this format, mode and merged options."""

    source: str | None
    mode: str | None
    options: dict[str, str]


@dataclass(frozen=True)
class _Connect:
    """`psycopg.connect` was called. `connection` numbers the calls from 1."""

    connection: int
    args: tuple[object, ...]
    kwargs: dict[str, object]


@dataclass(frozen=True)
class _Execute:
    """A statement ran on a connection, as text. `failed` when its execution raised."""

    connection: int
    statement: str
    failed: bool


@dataclass(frozen=True)
class _Commit:
    """A connection committed."""

    connection: int


@dataclass(frozen=True)
class _Rollback:
    """A connection rolled back."""

    connection: int


@dataclass(frozen=True)
class _Close:
    """A connection closed."""

    connection: int


_Event: TypeAlias = _Load | _Save | _Connect | _Execute | _Commit | _Rollback | _Close


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

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does. Every load
    returns a new `_FakeDataFrame` with `columns`. The DataFrame of a load with a `query` option
    answers its actions with `bound_row`. When `load_error` is set, every load raises it once
    recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        bound_row: tuple[object, ...] = (BOUND,),
        columns: Sequence[str] = (),
        load_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._bound_row = bound_row
        self._columns = list(columns)
        self._load_error = load_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self._log, self._bound_row, self._columns, self._load_error)


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_Load` each time `load()` is called."""

    def __init__(
        self,
        log: _Log,
        bound_row: tuple[object, ...],
        columns: list[str],
        load_error: BaseException | None,
    ) -> None:
        self._log = log
        self._bound_row = bound_row
        self._columns = columns
        self._load_error = load_error
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

    def load(self) -> _FakeDataFrame:
        query = any(key.lower() == "query" for key in self._options)
        frame = _FakeDataFrame(
            self._log, columns=self._columns, row=self._bound_row if query else None
        )
        self._log.events.append(_Load(self._source, dict(self._options), frame))
        if self._load_error is not None:
            raise self._load_error
        return frame


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    The DataFrame of a load with a `query` option answers `collect()`, `first()`, `head()` and
    `take(n)` with its single `row`. Each access to `write` returns a new
    `_FakeDataFrameWriter`, whose `save()` raises `save_error` once recorded, when it is set.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        row: tuple[object, ...] | None = None,
        save_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
        self._row = row
        self._save_error = save_error
        self._accessed: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def write(self) -> _FakeDataFrameWriter:
        return _FakeDataFrameWriter(self._log, self._save_error)

    def collect(self) -> list[tuple[object, ...]]:
        return [self._bound_row()]

    def first(self) -> tuple[object, ...]:
        return self._bound_row()

    def head(self, n: int | None = None) -> tuple[object, ...] | list[tuple[object, ...]]:
        return self._bound_row() if n is None else [self._bound_row()][:n]

    def take(self, num: int) -> list[tuple[object, ...]]:
        return [self._bound_row()][:num]

    def _bound_row(self) -> tuple[object, ...]:
        if self._row is None:
            raise AssertionError("an action ran on a DataFrame that is not the bound query's")
        return self._row


class _FakeDataFrameWriter:
    """Stands in for `DataFrame.write`: records a `_Save` when `save()` is called."""

    def __init__(self, log: _Log, save_error: BaseException | None) -> None:
        self._log = log
        self._save_error = save_error
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

    def save(self) -> None:
        self._log.events.append(_Save(self._source, self._mode, dict(self._options)))
        if self._save_error is not None:
            raise self._save_error


def _statement_text(query: object) -> str:
    """A statement as text: a `psycopg.sql` composable is rendered without a connection."""
    if isinstance(query, sql.Composable):
        return query.as_string()
    return str(query)


class _FakePsycopg:
    """Stands in for `psycopg.connect`: records each call and returns a `_FakeConnection`.

    `connect_errors` maps a call number, from 1, to the error that call raises once recorded.
    `execute_errors` maps the start of a statement to the error its execution raises, on any
    connection.
    """

    def __init__(self, log: _Log) -> None:
        self._log = log
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.connections: list[_FakeConnection] = []
        self.connect_errors: dict[int, BaseException] = {}
        self.execute_errors: dict[str, BaseException] = {}

    def connect(self, *args: object, **kwargs: object) -> _FakeConnection:
        number = len(self.calls) + 1
        self.calls.append((args, kwargs))
        self._log.events.append(_Connect(number, args, kwargs))
        if number in self.connect_errors:
            raise self.connect_errors[number]
        connection = _FakeConnection(self._log, number, self.execute_errors)
        self.connections.append(connection)
        return connection


class _FakeConnection:
    """Stands in for a `psycopg.Connection`, with psycopg's transaction semantics.

    As a context manager it commits on a clean exit, rolls back on an exception, then closes. A
    failed statement aborts the transaction: the next statement fails, and only a rollback, or
    a commit that PostgreSQL turns into one, ends it. A closed connection refuses every call
    but `close()`.
    """

    def __init__(self, log: _Log, number: int, execute_errors: dict[str, BaseException]) -> None:
        self._log = log
        self._execute_errors = execute_errors
        self._aborted = False
        self.number = number
        self.closed = False

    def execute(self, query: object, params: object = None) -> _FakeCursor:
        self._check_open()
        statement = _statement_text(query)
        error: BaseException | None = None
        if self._aborted:
            error = psycopg.errors.InFailedSqlTransaction("current transaction is aborted")
        else:
            for start, failure in self._execute_errors.items():
                if statement.startswith(start):
                    error = failure
                    break
        self._log.events.append(_Execute(self.number, statement, failed=error is not None))
        if error is not None:
            self._aborted = True
            raise error
        return _FakeCursor(self)

    def cursor(self) -> _FakeCursor:
        self._check_open()
        return _FakeCursor(self)

    def commit(self) -> None:
        self._check_open()
        self._aborted = False
        self._log.events.append(_Commit(self.number))

    def rollback(self) -> None:
        self._check_open()
        self._aborted = False
        self._log.events.append(_Rollback(self.number))

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._log.events.append(_Close(self.number))

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.closed:
            return
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

    def _check_open(self) -> None:
        if self.closed:
            raise psycopg.OperationalError("the connection is closed")


class _FakeCursor:
    """Stands in for a `psycopg.Cursor`: its statements are recorded on its connection."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def execute(self, query: object, params: object = None) -> _FakeCursor:
        self._connection.execute(query, params)
        return self

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


# --- Fixtures and helpers -----------------------------------------------------------------


@pytest.fixture
def log() -> _Log:
    """The event log the doubles of one test share."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """A fake session whose bound query answers `(BOUND,)`."""
    return _FakeSpark(log)


@pytest.fixture
def pg(log: _Log, monkeypatch: pytest.MonkeyPatch) -> _FakePsycopg:
    """Replace `psycopg.connect` with a fake that records on the log."""
    fake = _FakePsycopg(log)
    monkeypatch.setattr(psycopg, "connect", fake.connect)
    return fake


@pytest.fixture
def conn() -> PostgresConnection:
    """The connection of the acceptance criteria."""
    return PostgresConnection("db.internal", "shop", "ingest", "s3cret")


@pytest.fixture
def df(log: _Log) -> _FakeDataFrame:
    """The DataFrame the writer receives, with the columns `id` and `payload`."""
    return _FakeDataFrame(log, columns=["id", "payload"])


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


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


def _executed(log: _Log) -> list[_Execute]:
    """The statements executed, in order, on every connection."""
    return [event for event in log.events if isinstance(event, _Execute)]


def _attributes(frame: _FakeDataFrame) -> list[str]:
    """The attributes accessed on `frame`, in order."""
    return frame._accessed


def _transcript(events: Sequence[_Event]) -> list[str]:
    """The events as the lines of a session log.

    A commit or a rollback with no statement executed since the last one on its connection
    changes nothing, and is left out.
    """
    pending: dict[int, int] = {}
    lines: list[str] = []
    for event in events:
        if isinstance(event, _Load):
            lines.append("load")
        elif isinstance(event, _Save):
            lines.append("save")
        elif isinstance(event, _Connect):
            lines.append(f"connect {event.connection}")
        elif isinstance(event, _Execute):
            pending[event.connection] = pending.get(event.connection, 0) + 1
            failed = " (failed)" if event.failed else ""
            lines.append(f"{event.connection}: {event.statement}{failed}")
        elif isinstance(event, _Commit):
            if pending.pop(event.connection, 0):
                lines.append(f"{event.connection}: COMMIT")
        elif isinstance(event, _Rollback):
            if pending.pop(event.connection, 0):
                lines.append(f"{event.connection}: ROLLBACK")
        else:
            lines.append(f"{event.connection}: close")
    return lines


def _committed(log: _Log) -> list[tuple[int, str]]:
    """The statements committed, in the order of their commits, with their connection.

    A statement is committed when its connection commits after executing it, with no rollback
    in between. After a failed statement, the commit commits nothing: PostgreSQL rolls back.
    """
    pending: dict[int, list[str]] = {}
    aborted: set[int] = set()
    committed: list[tuple[int, str]] = []
    for event in log.events:
        if isinstance(event, _Execute):
            if event.failed:
                aborted.add(event.connection)
            else:
                pending.setdefault(event.connection, []).append(event.statement)
        elif isinstance(event, _Commit):
            statements = pending.pop(event.connection, [])
            if event.connection not in aborted:
                committed.extend((event.connection, statement) for statement in statements)
            aborted.discard(event.connection)
        elif isinstance(event, _Rollback | _Close):
            pending.pop(event.connection, None)
            aborted.discard(event.connection)
    return committed


def _staging_tables(log: _Log) -> list[str]:
    """The staging tables that the `CREATE` statements name, in order, as quoted."""
    prefix = "CREATE UNLOGGED TABLE "
    return [
        event.statement.removeprefix(prefix).split(" ", 1)[0]
        for event in _executed(log)
        if event.statement.startswith(prefix)
    ]


def _staging(log: _Log) -> str:
    """The staging table of the only `CREATE` executed, as quoted."""
    tables = _staging_tables(log)
    assert len(tables) == 1, _transcript(log.events)
    return tables[0]


def _create(stg: str, *, target: str = ORDERS, columns: str = '"id", "payload"') -> str:
    return f"CREATE UNLOGGED TABLE {stg} AS SELECT {columns} FROM {target} WITH NO DATA"


def _insert(stg: str, *, target: str = ORDERS, columns: str = '"id", "payload"') -> str:
    return f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {stg}"


def _write_transcript(stg: str, *, target: str = ORDERS, replace: bool = False) -> list[str]:
    """The transcript of a successful write of `df` through the staging table `stg`."""
    truncate = [f"2: TRUNCATE TABLE {target}"] if replace else []
    return [
        "connect 1",
        f"1: {_create(stg, target=target)}",
        "1: COMMIT",
        "1: close",
        "save",
        "connect 2",
        *truncate,
        f"2: {_insert(stg, target=target)}",
        f"2: DROP TABLE {stg}",
        "2: COMMIT",
        "2: close",
    ]


def _save_options(stg: str) -> dict[str, str]:
    """The options of the save of a write through `stg`, without user options."""
    return {
        **BASE_OPTIONS,
        "dbtable": stg,
        "stringtype": "unspecified",
        "reWriteBatchedInserts": "true",
    }


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


WriteCall: TypeAlias = Callable[[PostgresWriter, DataFrame], object]

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
from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter

import ingestio
import ingestio.connector.all as connectors

names = {"PostgresConnection", "PostgresReader", "PostgresWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys

sys.modules["pyspark"] = None
sys.modules["psycopg"] = None
for name in ("pyspark", "pyspark.sql", "psycopg"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import PostgresConnection, PostgresReader

loads = []


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def load(self):
        loads.append(object())
        return loads[-1]


class _FakeSpark:
    @property
    def read(self):
        return _FakeDataFrameReader()


conn = PostgresConnection("db.internal", "shop", "ingest", "s3cret")
reader = PostgresReader("public.orders", conn, spark=_FakeSpark())
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
"""


def test_at001_the_three_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_reader_works_without_pyspark_and_psycopg_installed(tmp_path: Path) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at003_building_a_writer_without_psycopg_names_the_extra(
    conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    modules = cast("dict[str, object]", sys.modules)
    for name in [name for name in sys.modules if name.split(".")[0] == "psycopg"]:
        monkeypatch.setitem(modules, name, None)

    message = "PostgresWriter needs psycopg: pip install 'ingestio[postgres]'"
    with pytest.raises(ImportError, match=_exactly(message)) as excinfo:
        PostgresWriter("public.orders", conn)
    assert isinstance(excinfo.value.__cause__, ImportError)


def _project(requirement: str) -> str:
    """The normalised name of the project a `Requires-Dist` value names."""
    name = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    assert name, requirement
    return re.sub(r"[-_.]+", "-", name[1]).lower()


def test_at004_the_distribution_declares_the_postgres_extra_with_psycopg() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "postgres" in (metadata.get_all("Provides-Extra") or [])
    requirements: list[str] = metadata.get_all("Requires-Dist") or []
    assert requirements
    for requirement in requirements:
        assert EXTRA_MARKER.search(requirement.partition(";")[2]), requirement
    assert any(_project(requirement) == "psycopg" for requirement in requirements), requirements


# --- Connection settings ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("build", "message"),
    [
        pytest.param(
            lambda: PostgresConnection("", "shop", "ingest", "s3cret"),
            "host must not be empty",
            id="host",
        ),
        pytest.param(
            lambda: PostgresConnection("db.internal", "", "ingest", "s3cret"),
            "database must not be empty",
            id="database",
        ),
        pytest.param(
            lambda: PostgresConnection("db.internal", "shop", "", "s3cret"),
            "user must not be empty",
            id="user",
        ),
        pytest.param(
            lambda: PostgresConnection("db.internal", "shop", "ingest", "s3cret", port=0),
            "port must be between 1 and 65535",
            id="port-0",
        ),
        pytest.param(
            lambda: PostgresConnection("db.internal", "shop", "ingest", "s3cret", port=65536),
            "port must be between 1 and 65535",
            id="port-65536",
        ),
        pytest.param(
            lambda: PostgresConnection("db.internal", "shop", "ingest", "s3cret", sslmode="maybe"),
            "unsupported sslmode 'maybe'",
            id="sslmode",
        ),
    ],
)
def test_at005_invalid_connection_settings_are_rejected(
    build: Callable[[], object], message: str
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        build()


@pytest.mark.parametrize("port", [1, 65535])
def test_at005_the_lowest_and_the_highest_port_are_accepted(port: int) -> None:
    PostgresConnection("db.internal", "shop", "ingest", "s3cret", port=port)


@pytest.mark.parametrize(
    "sslmode", ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
)
def test_at005_every_libpq_sslmode_is_accepted(sslmode: str) -> None:
    PostgresConnection("db.internal", "shop", "ingest", "s3cret", sslmode=sslmode)


def test_a_connection_exposes_its_arguments_as_attributes() -> None:
    connection = PostgresConnection(
        "db.internal", "shop", "ingest", "s3cret", port=6543, sslmode="require"
    )
    assert connection.host == "db.internal"
    assert connection.database == "shop"
    assert connection.user == "ingest"
    assert connection.password == "s3cret"
    assert connection.port == 6543
    assert connection.sslmode == "require"


def test_at006_repr_shows_what_identifies_the_database_and_the_table(
    conn: PostgresConnection, pg: _FakePsycopg
) -> None:
    assert repr(conn) == (
        "PostgresConnection(host='db.internal', port=5432, database='shop', user='ingest')"
    )
    assert repr(PostgresReader("public.orders", conn)) == (
        "PostgresReader(table_name='public.orders', host='db.internal', database='shop')"
    )
    assert repr(PostgresWriter("public.orders", conn)) == (
        "PostgresWriter(table_name='public.orders', host='db.internal', database='shop')"
    )


def test_at006_neither_repr_nor_str_shows_the_password(pg: _FakePsycopg) -> None:
    connection = PostgresConnection("db.internal", "shop", "ingest", "s3cret", sslmode="require")
    reader = PostgresReader(
        "public.orders",
        connection,
        watermark_column="updated_at",
        watermark_value=41,
        options={"fetchsize": 500, "queryTimeout": 30},
    )
    writer = PostgresWriter("public.orders", connection, options={"batchsize": 5000})
    for connector in (connection, reader, writer):
        assert "s3cret" not in repr(connector)
        assert "s3cret" not in str(connector)


# --- Building the connectors --------------------------------------------------------------


@pytest.mark.parametrize("build", [PostgresReader, PostgresWriter], ids=["reader", "writer"])
@pytest.mark.parametrize("name", ["", "orders.", ".orders", "a.b.c"])
def test_at007_a_table_name_other_than_table_or_schema_table_is_rejected(
    build: type[PostgresReader] | type[PostgresWriter],
    name: str,
    conn: PostgresConnection,
    pg: _FakePsycopg,
) -> None:
    with pytest.raises(ValueError, match=_exactly("table_name must be 'table' or 'schema.table'")):
        build(name, conn)


@pytest.mark.parametrize("build", [PostgresReader, PostgresWriter], ids=["reader", "writer"])
@pytest.mark.parametrize("name", ["orders", "public.orders"])
def test_at007_table_and_schema_table_are_accepted(
    build: type[PostgresReader] | type[PostgresWriter],
    name: str,
    conn: PostgresConnection,
    pg: _FakePsycopg,
) -> None:
    build(name, conn)


def test_at008_a_watermark_value_needs_a_watermark_column(conn: PostgresConnection) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_value needs watermark_column")):
        PostgresReader("public.orders", conn, watermark_value=41)


def test_at008_an_empty_watermark_column_is_rejected(conn: PostgresConnection) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_column must not be empty")):
        PostgresReader("public.orders", conn, watermark_column="")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        pytest.param(True, "unsupported watermark_value type: bool", id="bool"),
        pytest.param(1.5, "unsupported watermark_value type: float", id="float"),
        pytest.param([1], "unsupported watermark_value type: list", id="list"),
        pytest.param(Decimal("NaN"), "watermark_value must be finite", id="decimal-nan"),
        pytest.param(
            datetime(2026, 10, 1),  # noqa: DTZ001 - the naive datetime under test
            "watermark_value must be timezone-aware",
            id="naive-datetime",
        ),
    ],
)
def test_at009_unsupported_watermark_values_are_rejected(
    value: object, message: str, conn: PostgresConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        PostgresReader(
            "public.orders",
            conn,
            watermark_column="updated_at",
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize("value", [0, ""], ids=["zero", "empty-string"])
def test_at009_zero_and_the_empty_string_are_watermark_values(
    value: int | str, conn: PostgresConnection
) -> None:
    PostgresReader("public.orders", conn, watermark_column="updated_at", watermark_value=value)


@pytest.mark.parametrize("build", [PostgresReader, PostgresWriter], ids=["reader", "writer"])
@pytest.mark.parametrize("key", ["url", "DbTable", "QUERY", "user", "password", "Driver"])
def test_at010_options_the_connector_sets_are_rejected_in_any_case(
    build: type[PostgresReader] | type[PostgresWriter],
    key: str,
    conn: PostgresConnection,
    pg: _FakePsycopg,
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        build("public.orders", conn, options={key: "x"})


def test_at011_building_the_connectors_touches_neither_spark_nor_postgresql(
    log: _Log, spark: _FakeSpark, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    PostgresReader("public.orders", conn, spark=_session(spark))
    PostgresReader("public.orders", conn, spark=_session(spark), watermark_column="updated_at")
    PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=41,
    )
    PostgresWriter("public.orders", conn)

    assert log.events == []
    assert log.spark_attributes == []
    assert pg.calls == []


# --- Full read ----------------------------------------------------------------------------


def test_at012_a_full_read_loads_the_quoted_table_with_the_base_options(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    frame = PostgresReader("public.orders", conn, spark=_session(spark)).read()

    assert _transcript(log.events) == ["load"]
    (load,) = _loads(log)
    assert load.source == "jdbc"
    assert _lower(load.options) == _lower({**BASE_OPTIONS, "dbtable": ORDERS, "fetchsize": "10000"})
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    url = _lower(load.options)["url"]
    assert "s3cret" not in url
    assert "@" not in url


def test_at013_an_ipv6_host_is_bracketed_and_the_database_name_percent_encoded(
    log: _Log, spark: _FakeSpark
) -> None:
    connection = PostgresConnection("::1", "my db", "ingest", "s3cret", port=6543)
    PostgresReader("public.orders", connection, spark=_session(spark)).read()

    (load,) = _loads(log)
    assert _lower(load.options)["url"] == "jdbc:postgresql://[::1]:6543/my%20db"


def test_at014_user_options_are_passed_through_and_replace_the_defaults(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    options: dict[str, str | int | float | bool] = {
        "partitionColumn": "id",
        "lowerBound": 1,
        "upperBound": 1000,
        "numPartitions": 8,
        "FetchSize": 500,
    }
    PostgresReader("public.orders", conn, spark=_session(spark), options=options).read()

    (load,) = _loads(log)
    assert _lower(load.options) == _lower(
        {
            **BASE_OPTIONS,
            "dbtable": ORDERS,
            "partitionColumn": "id",
            "lowerBound": "1",
            "upperBound": "1000",
            "numPartitions": "8",
            "fetchsize": "500",
        }
    )


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at015_the_sslmode_is_sent_with_its_pgjdbc_name(
    watermark: bool, log: _Log, spark: _FakeSpark
) -> None:
    connection = PostgresConnection("db.internal", "shop", "ingest", "s3cret", sslmode="require")
    PostgresReader(
        "public.orders",
        connection,
        spark=_session(spark),
        watermark_column="updated_at" if watermark else None,
    ).read()

    loads = _loads(log)
    assert len(loads) == (2 if watermark else 1)
    for load in loads:
        assert load.options.get("sslmode") == "require", load.options


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at015_without_sslmode_no_load_carries_one(
    watermark: bool, log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at" if watermark else None,
    ).read()

    loads = _loads(log)
    assert loads
    for load in loads:
        assert "sslmode" not in _lower(load.options), load.options


def test_at016_the_active_session_is_looked_up_when_read_runs(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    def fake_session() -> SparkSession:
        lookups.append("fake session")
        return _session(spark)

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = PostgresReader("public.orders", conn)
    assert lookups == []

    monkeypatch.setattr(SparkSession, "active", fake_session)
    frame = reader.read()

    assert "fake session" in lookups
    (load,) = _loads(log)
    assert frame is _frame(load.frame)


def test_at016_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    error = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise error

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = PostgresReader("public.orders", conn)

    with pytest.raises(RuntimeError, match=_exactly("no active SparkSession")) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert _loads(log) == []


# --- Watermark read -----------------------------------------------------------------------


def test_at017_a_watermark_read_captures_a_bound_then_loads_the_slice(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    frame = PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    assert _transcript(log.events) == ["load", "load"]
    bound, data = _loads(log)
    assert bound.source == "jdbc"
    assert _lower(bound.options) == _lower(
        {
            **BASE_OPTIONS,
            "fetchsize": "10000",
            "query": (
                'SELECT max("updated_at")::text AS upper_bound FROM "public"."orders"'
                " WHERE \"updated_at\" > '2026-10-01 00:00:00+00:00'"
            ),
        }
    )
    assert data.source == "jdbc"
    assert _lower(data.options) == _lower(
        {
            **BASE_OPTIONS,
            "fetchsize": "10000",
            "dbtable": (
                '(SELECT * FROM "public"."orders"'
                " WHERE \"updated_at\" > '2026-10-01 00:00:00+00:00'"
                " AND \"updated_at\" <= '2026-10-21 09:30:00.5+00') AS ingestio_source"
            ),
        }
    )
    assert frame is _frame(data.frame)
    assert _attributes(data.frame) == []


def test_at018_without_a_watermark_value_the_slice_ends_at_the_bound(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    PostgresReader(
        "public.orders", conn, spark=_session(spark), watermark_column="updated_at"
    ).read()

    bound, data = _loads(log)
    assert _lower(bound.options)["query"] == (
        'SELECT max("updated_at")::text AS upper_bound FROM "public"."orders"'
    )
    assert _lower(data.options)["dbtable"] == (
        '(SELECT * FROM "public"."orders"'
        " WHERE \"updated_at\" <= '2026-10-21 09:30:00.5+00') AS ingestio_source"
    )


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
def test_at019_without_a_bound_the_slice_is_empty(
    value: int | None, log: _Log, conn: PostgresConnection
) -> None:
    spark = _FakeSpark(log, bound_row=(None,))
    frame = PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=value,
    ).read()

    _, data = _loads(log)
    assert _lower(data.options)["dbtable"] == (
        '(SELECT * FROM "public"."orders" WHERE false) AS ingestio_source'
    )
    assert frame is _frame(data.frame)


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        pytest.param(41, "'41'", id="int"),
        pytest.param(0, "'0'", id="zero"),
        pytest.param(Decimal("12.50"), "'12.50'", id="decimal"),
        pytest.param(Decimal("1E+3"), "'1000'", id="decimal-exponent"),
        pytest.param("O'Brien", "'O''Brien'", id="quote"),
        pytest.param("", "''", id="empty-string"),
        pytest.param(date(2026, 10, 1), "'2026-10-01'", id="date"),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, tzinfo=UTC), "'2026-10-01 08:30:00+00:00'", id="utc"
        ),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3))),
            "'2026-10-01 11:30:00.123456+00:00'",
            id="offset",
        ),
    ],
)
def test_at020_watermark_values_become_quoted_literals(
    value: int | Decimal | str | date,
    literal: str,
    log: _Log,
    spark: _FakeSpark,
    conn: PostgresConnection,
) -> None:
    PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=value,
    ).read()

    bound, data = _loads(log)
    assert f'"updated_at" > {literal}' in _lower(bound.options)["query"]
    assert f'"updated_at" > {literal}' in _lower(data.options)["dbtable"]


def test_at020_the_bound_becomes_a_quoted_literal(log: _Log, conn: PostgresConnection) -> None:
    spark = _FakeSpark(log, bound_row=("it's",))
    PostgresReader(
        "public.orders", conn, spark=_session(spark), watermark_column="updated_at"
    ).read()

    _, data = _loads(log)
    assert "\"updated_at\" <= 'it''s'" in _lower(data.options)["dbtable"]


@pytest.mark.parametrize(
    ("table_name", "column", "quoted_table", "quoted_column"),
    [
        pytest.param("orders", "updated_at", '"orders"', '"updated_at"', id="unqualified"),
        pytest.param("Sales.Orders", "UpdatedAt", '"Sales"."Orders"', '"UpdatedAt"', id="case"),
        pytest.param(
            'public.my"table', 'my"col', '"public"."my""table"', '"my""col"', id="double-quote"
        ),
    ],
)
def test_at021_table_and_column_names_are_always_quoted(
    table_name: str,
    column: str,
    quoted_table: str,
    quoted_column: str,
    log: _Log,
    spark: _FakeSpark,
    conn: PostgresConnection,
) -> None:
    PostgresReader(table_name, conn, spark=_session(spark)).read()
    PostgresReader(
        table_name, conn, spark=_session(spark), watermark_column=column, watermark_value=1
    ).read()

    full, bound, data = _loads(log)
    assert _lower(full.options)["dbtable"] == quoted_table
    assert _lower(bound.options)["query"] == (
        f"SELECT max({quoted_column})::text AS upper_bound FROM {quoted_table}"
        f" WHERE {quoted_column} > '1'"
    )
    assert _lower(data.options)["dbtable"] == (
        f"(SELECT * FROM {quoted_table} WHERE {quoted_column} > '1'"
        f" AND {quoted_column} <= '{BOUND}') AS ingestio_source"
    )


def test_at022_the_bound_load_leaves_out_the_partitioning_options(
    log: _Log, spark: _FakeSpark, conn: PostgresConnection
) -> None:
    options: dict[str, str | int | float | bool] = {
        "partitionColumn": "id",
        "lowerBound": 1,
        "upperBound": 1000,
        "NumPartitions": 8,
        "queryTimeout": 30,
    }
    PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=1,
        options=options,
    ).read()

    bound, data = _loads(log)
    partitioning = {"partitioncolumn", "lowerbound", "upperbound", "numpartitions"}
    assert _lower(bound.options)["querytimeout"] == "30"
    assert not partitioning & set(_lower(bound.options)), bound.options
    data_options = _lower(data.options)
    assert {key: data_options.get(key) for key in [*sorted(partitioning), "querytimeout"]} == {
        "lowerbound": "1",
        "numpartitions": "8",
        "partitioncolumn": "id",
        "upperbound": "1000",
        "querytimeout": "30",
    }


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at023_a_failed_load_propagates_and_nothing_is_loaded_after_it(
    watermark: bool, log: _Log, conn: PostgresConnection
) -> None:
    err = RuntimeError("down")
    spark = _FakeSpark(log, load_error=err)
    reader = PostgresReader(
        "public.orders",
        conn,
        spark=_session(spark),
        watermark_column="updated_at" if watermark else None,
        watermark_value=41 if watermark else None,
    )

    with pytest.raises(RuntimeError, match=_exactly("down")) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert len(_loads(log)) == 1


# --- Writing ------------------------------------------------------------------------------


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at024_write_appends_through_a_staging_table_in_one_transaction(
    call: WriteCall, log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    returned = call(PostgresWriter("public.orders", conn), _frame(df))

    assert returned is None
    stg = _staging(log)
    assert re.fullmatch(QUALIFIED_STAGING, stg), stg
    assert _transcript(log.events) == _write_transcript(stg)
    assert pg.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]
    (save,) = _saves(log)
    assert (save.source, save.mode) == ("jdbc", "append")
    assert _lower(save.options) == _lower(_save_options(stg))
    assert all(connection.closed for connection in pg.connections)
    assert set(_attributes(df)) <= {"columns", "write"}


def test_at025_write_with_replace_truncates_the_target_in_the_same_transaction(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    PostgresWriter("public.orders", conn).write(_frame(df), replace=True)

    stg = _staging(log)
    assert re.fullmatch(QUALIFIED_STAGING, stg), stg
    assert _transcript(log.events) == _write_transcript(stg, replace=True)
    assert pg.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]
    (save,) = _saves(log)
    assert (save.source, save.mode) == ("jdbc", "append")
    assert _lower(save.options) == _lower(_save_options(stg))
    assert all(connection.closed for connection in pg.connections)
    assert set(_attributes(df)) <= {"columns", "write"}


def test_at026_each_write_gets_a_new_staging_table(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    writer = PostgresWriter("public.orders", conn)
    writer.write(_frame(df))
    writer.write(_frame(df))

    first, second = _staging_tables(log)
    for stg in (first, second):
        assert re.fullmatch(QUALIFIED_STAGING, stg), stg
    assert first != second


def test_at026_an_unqualified_target_gets_an_unqualified_staging_table(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    PostgresWriter("orders", conn).write(_frame(df))

    stg = _staging(log)
    assert re.fullmatch(UNQUALIFIED_STAGING, stg), stg
    assert _transcript(log.events) == _write_transcript(stg, target='"orders"')
    (save,) = _saves(log)
    assert _lower(save.options)["dbtable"] == stg


def test_at027_user_options_replace_the_write_defaults(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    options: dict[str, str | int | float | bool] = {
        "numPartitions": 4,
        "BatchSize": 5000,
        "stringtype": "varchar",
    }
    PostgresWriter("public.orders", conn, options=options).write(_frame(df))

    stg = _staging(log)
    (save,) = _saves(log)
    assert _lower(save.options) == _lower(
        {**_save_options(stg), "numPartitions": "4", "BatchSize": "5000", "stringtype": "varchar"}
    )


def test_at027_the_pgjdbc_properties_keep_their_case(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    PostgresWriter("public.orders", conn).write(_frame(df))

    (save,) = _saves(log)
    assert save.options.get("stringtype") == "unspecified", save.options
    assert save.options.get("reWriteBatchedInserts") == "true", save.options


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at027_a_write_reads_only_columns_and_write_and_saves_once(
    call: WriteCall, log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    call(PostgresWriter("public.orders", conn), _frame(df))

    assert set(_attributes(df)) <= {"columns", "write"}, _attributes(df)
    assert len(_saves(log)) == 1


def test_at028_the_sslmode_reaches_psycopg_and_pgjdbc(
    log: _Log, pg: _FakePsycopg, df: _FakeDataFrame
) -> None:
    connection = PostgresConnection(
        "db.internal", "shop", "ingest", "s3cret", sslmode="verify-full"
    )
    PostgresWriter("public.orders", connection).write(_frame(df))

    assert pg.calls
    for args, kwargs in pg.calls:
        assert args == ()
        assert kwargs == {**CONNECT_ARGS, "sslmode": "verify-full"}
    (save,) = _saves(log)
    assert save.options.get("sslmode") == "verify-full", save.options


def test_at028_psycopg_connect_is_looked_up_each_time_a_connection_opens(
    log: _Log,
    pg: _FakePsycopg,
    conn: PostgresConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = PostgresWriter("public.orders", conn)
    replacement = _FakePsycopg(log)
    monkeypatch.setattr(psycopg, "connect", replacement.connect)

    writer.write(_frame(df))

    assert pg.calls == []
    assert replacement.calls == [((), CONNECT_ARGS), ((), CONNECT_ARGS)]


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at029_a_failed_save_drops_the_staging_table_on_a_new_connection(
    replace: bool, log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    err = RuntimeError("executor lost")
    frame = _FakeDataFrame(log, columns=["id", "payload"], save_error=err)

    with pytest.raises(RuntimeError, match=_exactly("executor lost")) as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(frame), replace=replace)

    assert excinfo.value is err
    stg = _staging(log)
    assert re.fullmatch(QUALIFIED_STAGING, stg), stg
    assert _committed(log) == [(1, _create(stg)), (2, f"DROP TABLE IF EXISTS {stg}")]
    statements = [event.statement for event in _executed(log)]
    assert not [s for s in statements if s.startswith(("INSERT", "TRUNCATE"))], statements
    assert pg.connections
    assert all(connection.closed for connection in pg.connections)


def test_at030_a_failed_insert_commits_nothing_and_the_staging_table_is_dropped(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("insert failed")
    pg.execute_errors["INSERT "] = err

    with pytest.raises(RuntimeError, match=_exactly("insert failed")) as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(df))

    assert excinfo.value is err
    stg = _staging(log)
    assert len(pg.connections) == 3
    apply, cleanup = pg.connections[1], pg.connections[2]
    assert _Commit(apply.number) not in log.events
    assert apply.closed
    assert [e.statement for e in _executed(log) if e.connection == cleanup.number] == [
        f"DROP TABLE IF EXISTS {stg}"
    ]
    assert _committed(log) == [(1, _create(stg)), (cleanup.number, f"DROP TABLE IF EXISTS {stg}")]
    assert cleanup.closed


def test_at030_a_failed_truncate_commits_nothing_and_the_staging_table_is_dropped(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("truncate failed")
    pg.execute_errors["TRUNCATE "] = err

    with pytest.raises(RuntimeError, match=_exactly("truncate failed")) as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(df), replace=True)

    assert excinfo.value is err
    stg = _staging(log)
    assert len(pg.connections) == 3
    apply, cleanup = pg.connections[1], pg.connections[2]
    assert _Commit(apply.number) not in log.events
    assert apply.closed
    assert [e.statement for e in _executed(log) if e.connection == cleanup.number] == [
        f"DROP TABLE IF EXISTS {stg}"
    ]
    assert _committed(log) == [(1, _create(stg)), (cleanup.number, f"DROP TABLE IF EXISTS {stg}")]
    assert cleanup.closed
    statements = [event.statement for event in _executed(log)]
    assert not [s for s in statements if s.startswith("INSERT")], statements


def test_at031_a_failed_create_closes_its_connection_and_moves_no_data(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("permission denied for schema public")
    pg.execute_errors["CREATE "] = err

    with pytest.raises(RuntimeError, match=_exactly(str(err))) as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(df))

    assert excinfo.value is err
    assert len(pg.calls) == 1
    assert not [event for event in log.events if isinstance(event, _Commit)]
    assert pg.connections[0].closed
    assert _saves(log) == []


def _dropped_note(stg: str) -> list[str]:
    return [f"staging table {stg} could not be dropped: RuntimeError: refused"]


def test_at032_a_failed_cleanup_after_a_failed_save_becomes_a_note(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    err = RuntimeError("executor lost")
    frame = _FakeDataFrame(log, columns=["id", "payload"], save_error=err)
    pg.execute_errors["DROP TABLE IF EXISTS "] = RuntimeError("refused")

    with pytest.raises(RuntimeError, match=r"^executor lost") as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(frame))

    assert excinfo.value is err
    stg = _staging(log)
    assert re.fullmatch(QUALIFIED_STAGING, stg), stg
    assert err.__notes__ == _dropped_note(stg)


def test_at032_a_failed_cleanup_connection_becomes_a_note(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    err = RuntimeError("executor lost")
    frame = _FakeDataFrame(log, columns=["id", "payload"], save_error=err)
    pg.connect_errors[2] = RuntimeError("refused")

    with pytest.raises(RuntimeError, match=r"^executor lost") as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(frame))

    assert excinfo.value is err
    assert err.__notes__ == _dropped_note(_staging(log))


def test_at032_a_failed_cleanup_after_a_failed_insert_becomes_a_note(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection, df: _FakeDataFrame
) -> None:
    err = RuntimeError("insert failed")
    pg.execute_errors["INSERT "] = err
    pg.execute_errors["DROP TABLE IF EXISTS "] = RuntimeError("refused")

    with pytest.raises(RuntimeError, match=r"^insert failed") as excinfo:
        PostgresWriter("public.orders", conn).write(_frame(df))

    assert excinfo.value is err
    assert err.__notes__ == _dropped_note(_staging(log))


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at033_a_dataframe_without_columns_is_rejected_before_anything_runs(
    replace: bool, log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    frame = _FakeDataFrame(log, columns=[])

    with pytest.raises(ValueError, match=_exactly("the DataFrame has no columns")):
        PostgresWriter("public.orders", conn).write(_frame(frame), replace=replace)

    assert pg.calls == []
    assert _saves(log) == []


def test_at034_column_names_are_quoted_as_identifiers(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    frame = _FakeDataFrame(log, columns=["Id", 'say "hi"'])
    PostgresWriter("public.orders", conn).write(_frame(frame))

    stg = _staging(log)
    statements = [event.statement for event in _executed(log)]
    assert (
        f'CREATE UNLOGGED TABLE {stg} AS SELECT "Id", "say ""hi""" FROM "public"."orders"'
        " WITH NO DATA"
    ) in statements
    assert (
        f'INSERT INTO "public"."orders" ("Id", "say ""hi""") SELECT "Id", "say ""hi""" FROM {stg}'
    ) in statements


# --- Composition --------------------------------------------------------------------------


def test_at035_read_source_and_write_target_move_a_dataframe_and_append(
    log: _Log, pg: _FakePsycopg, conn: PostgresConnection
) -> None:
    spark = _FakeSpark(log, columns=["id", "payload"])
    source: Reader[DataFrame] = PostgresReader("public.orders", conn, spark=_session(spark))
    target: Writer[DataFrame] = PostgresWriter("public.orders", conn)

    process = Process(
        [
            Step("extract", [ReadSource(source, into="orders")]),
            Step("load", [WriteTarget(target, key="orders")]),
        ]
    ).run()

    (load,) = _loads(log)
    assert log.events[0] == load
    assert process.state["orders"] is _frame(load.frame)
    stg = _staging(log)
    assert _transcript(log.events[1:]) == _write_transcript(stg)
    (save,) = _saves(log)
    assert _lower(save.options) == _lower(_save_options(stg))
    statements = [event.statement for event in _executed(log)]
    assert not [s for s in statements if s.startswith("TRUNCATE")], statements


# --- Integration --------------------------------------------------------------------------

POSTGRES_URL = os.environ.get("INGESTIO_POSTGRES_URL", "")
PGJDBC = "org.postgresql:postgresql:42.7.4"

needs_postgres = pytest.mark.skipif(
    not POSTGRES_URL, reason="INGESTIO_POSTGRES_URL is not set: no PostgreSQL to run against"
)


class _Database:
    """The PostgreSQL of `INGESTIO_POSTGRES_URL`, with a schema of its own for one test.

    Its statements run through psycopg, outside the connectors.
    """

    def __init__(self, url: str) -> None:
        parts = urlsplit(url)
        host, user = parts.hostname, parts.username
        assert host, "INGESTIO_POSTGRES_URL has no host"
        assert user, "INGESTIO_POSTGRES_URL has no user"
        self.url = url
        self.schema = f"ingestio_it_{uuid.uuid4().hex[:12]}"
        self.connection = PostgresConnection(
            host,
            unquote(parts.path.removeprefix("/")),
            unquote(user),
            unquote(parts.password or ""),
            port=parts.port or 5432,
        )

    def table(self, name: str) -> str:
        """`name` in the test's schema, as `table_name` takes it and as SQL reads it."""
        return f"{self.schema}.{name}"

    def execute(self, statement: str) -> None:
        with psycopg.connect(self.url, autocommit=True) as connection:
            connection.execute(statement)

    def rows(self, query: str) -> list[tuple[Any, ...]]:
        with psycopg.connect(self.url) as connection:
            return connection.execute(query).fetchall()

    def staging_tables(self) -> list[str]:
        """The tables of the test's schema whose name starts with `ingestio_staging_`."""
        with psycopg.connect(self.url) as connection:
            names = connection.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s", [self.schema]
            ).fetchall()
        return [name for (name,) in names if str(name).startswith("ingestio_staging_")]


@pytest.fixture
def database() -> Iterator[_Database]:
    """A schema of its own on the PostgreSQL of `INGESTIO_POSTGRES_URL`, dropped afterwards."""
    db = _Database(POSTGRES_URL)
    db.execute(f'CREATE SCHEMA "{db.schema}"')
    try:
        yield db
    finally:
        db.execute(f'DROP SCHEMA "{db.schema}" CASCADE')


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """A local Spark session with the PostgreSQL JDBC driver."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration")
        .config("spark.jars.packages", PGJDBC)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.mark.integration
@needs_postgres
def test_a_full_read_returns_every_row_of_a_table_with_postgres_types(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("source")
    database.execute(
        f"CREATE TABLE {table} "
        "(ref uuid, doc jsonb, amount numeric(10,2), changed_at timestamptz, note text)"
    )
    database.execute(
        f"INSERT INTO {table} VALUES "
        "('0b6f3c1e-8a4d-4f7e-9c2a-1d5e6f7a8b9c', '{\"a\": 1}', 12.50,"
        " '2026-10-01 08:30:00+00', 'first'), "
        "('5d2e4f60-1a2b-4c3d-8e9f-0a1b2c3d4e5f', '[1, 2]', 0.05,"
        " '2026-10-02 23:59:59.5+00', 'second'), "
        "('9f8e7d6c-5b4a-4392-8170-6f5e4d3c2b1a', '{\"b\": null}', -3.00,"
        " '2026-10-03 00:00:00-03', 'third')"
    )

    frame = PostgresReader(table, database.connection, spark=spark_session).read()

    rows = sorted(frame.collect(), key=lambda row: str(row["note"]))
    # Spark hands timestamps to Python as naive local times: `astimezone` reads them so.
    assert [
        (
            row["ref"],
            json.loads(row["doc"]),
            row["amount"],
            row["changed_at"].astimezone(UTC),
            row["note"],
        )
        for row in rows
    ] == [
        (
            "0b6f3c1e-8a4d-4f7e-9c2a-1d5e6f7a8b9c",
            {"a": 1},
            Decimal("12.50"),
            datetime(2026, 10, 1, 8, 30, tzinfo=UTC),
            "first",
        ),
        (
            "5d2e4f60-1a2b-4c3d-8e9f-0a1b2c3d4e5f",
            [1, 2],
            Decimal("0.05"),
            datetime(2026, 10, 2, 23, 59, 59, 500000, tzinfo=UTC),
            "second",
        ),
        (
            "9f8e7d6c-5b4a-4392-8170-6f5e4d3c2b1a",
            {"b": None},
            Decimal("-3.00"),
            datetime(2026, 10, 3, 3, 0, tzinfo=UTC),
            "third",
        ),
    ]


@pytest.mark.integration
@needs_postgres
def test_a_watermark_read_returns_a_frozen_slice_above_the_value(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("source")
    database.execute(f"CREATE TABLE {table} (id integer, updated_at timestamptz)")
    database.execute(
        f"INSERT INTO {table} VALUES "
        "(1, '2026-10-01 00:00:00+00'), (2, '2026-10-02 00:00:00+00'),"
        " (3, '2026-10-03 00:00:00+00'), (4, NULL)"
    )

    frame = PostgresReader(
        table,
        database.connection,
        spark=spark_session,
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    assert sorted(row["id"] for row in frame.collect()) == [2, 3]
    database.execute(f"INSERT INTO {table} VALUES (5, '2026-10-04 00:00:00+00'), (6, NULL)")
    assert sorted(row["id"] for row in frame.collect()) == [2, 3]

    first_run = PostgresReader(
        table, database.connection, spark=spark_session, watermark_column="updated_at"
    ).read()
    assert sorted(row["id"] for row in first_run.collect()) == [1, 2, 3, 5]


@pytest.mark.integration
@needs_postgres
def test_write_appends_and_the_target_fills_what_the_dataframe_lacks(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("target")
    database.execute(
        f"CREATE TABLE {table} ("
        "id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        " ref uuid NOT NULL,"
        " doc jsonb NOT NULL,"
        " status text NOT NULL DEFAULT 'new')"
    )
    database.execute(
        f"INSERT INTO {table} (ref, doc, status) VALUES "
        "('00000000-0000-4000-8000-000000000001', '{\"old\": true}', 'loaded')"
    )
    frame = spark_session.createDataFrame(
        [
            ("6f1c2d3e-4b5a-4c6d-8e7f-901a2b3c4d5e", '{"a": 1}'),
            ("7a8b9c0d-1e2f-4a3b-9c4d-5e6f7a8b9c0d", '{"b": [1, 2]}'),
        ],
        "ref string, doc string",
    )

    PostgresWriter(table, database.connection).write(frame)

    rows = database.rows(f"SELECT id, ref::text, doc, status FROM {table} ORDER BY id")
    assert [row[0] for row in rows] == [1, 2, 3]
    assert rows[0][1:] == ("00000000-0000-4000-8000-000000000001", {"old": True}, "loaded")
    assert sorted((row[1:] for row in rows[1:]), key=lambda values: str(values[0])) == [
        ("6f1c2d3e-4b5a-4c6d-8e7f-901a2b3c4d5e", {"a": 1}, "new"),
        ("7a8b9c0d-1e2f-4a3b-9c4d-5e6f7a8b9c0d", {"b": [1, 2]}, "new"),
    ]
    assert database.staging_tables() == []


@pytest.mark.integration
@needs_postgres
def test_write_with_replace_leaves_exactly_the_rows_of_the_dataframe(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("target")
    database.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, name text)")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    writer = PostgresWriter(table, database.connection)

    writer.write(
        spark_session.createDataFrame([(10, "x"), (11, "y")], "id int, name string"),
        replace=True,
    )
    assert database.rows(f"SELECT id, name FROM {table} ORDER BY id") == [(10, "x"), (11, "y")]

    writer.write(spark_session.createDataFrame([], "id int, name string"), replace=True)
    assert database.rows(f"SELECT id, name FROM {table}") == []
    assert database.staging_tables() == []


@pytest.mark.integration
@needs_postgres
def test_a_dataframe_that_violates_a_constraint_leaves_the_target_unchanged(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("target")
    database.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, name text NOT NULL)")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    writer = PostgresWriter(table, database.connection)

    with pytest.raises(psycopg.errors.NotNullViolation):
        writer.write(spark_session.createDataFrame([(2, None)], "id int, name string"))
    with pytest.raises(psycopg.errors.UniqueViolation):
        writer.write(spark_session.createDataFrame([(1, "again")], "id int, name string"))

    assert database.rows(f"SELECT id, name FROM {table}") == [(1, "a")]
    assert database.staging_tables() == []


@pytest.mark.integration
@needs_postgres
def test_a_value_the_target_type_rejects_fails_during_the_spark_write(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("target")
    database.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, ref uuid NOT NULL)")
    database.execute(f"INSERT INTO {table} VALUES (1, '00000000-0000-4000-8000-000000000001')")
    frame = spark_session.createDataFrame([(2, "not-a-uuid")], "id int, ref string")

    with pytest.raises(Exception, match="uuid") as excinfo:
        PostgresWriter(table, database.connection).write(frame)

    # The staging table rejected the value, in Spark: the apply never ran.
    assert not isinstance(excinfo.value, psycopg.Error)
    assert database.rows(f"SELECT id, ref::text FROM {table}") == [
        (1, "00000000-0000-4000-8000-000000000001")
    ]
    assert database.staging_tables() == []


@pytest.mark.integration
@needs_postgres
def test_replace_on_a_target_referenced_by_a_foreign_key_leaves_it_unchanged(
    database: _Database, spark_session: SparkSession
) -> None:
    parent, child = database.table("parent"), database.table("child")
    database.execute(f"CREATE TABLE {parent} (id integer PRIMARY KEY, name text)")
    database.execute(
        f"CREATE TABLE {child} (id integer PRIMARY KEY, parent_id integer REFERENCES {parent})"
    )
    database.execute(f"INSERT INTO {parent} VALUES (1, 'a'), (2, 'b')")
    database.execute(f"INSERT INTO {child} VALUES (1, 1)")
    frame = spark_session.createDataFrame([(3, "c")], "id int, name string")

    with pytest.raises(psycopg.errors.FeatureNotSupported):
        PostgresWriter(parent, database.connection).write(frame, replace=True)

    assert database.rows(f"SELECT id, name FROM {parent} ORDER BY id") == [(1, "a"), (2, "b")]
    assert database.staging_tables() == []


@pytest.mark.integration
@needs_postgres
def test_a_column_the_target_lacks_fails_before_any_row_is_written(
    database: _Database, spark_session: SparkSession
) -> None:
    table = database.table("target")
    database.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, name text)")
    database.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    frame = spark_session.createDataFrame([(2, "b", "x")], "id int, name string, extra string")

    # The CREATE of the staging table fails in psycopg, before Spark writes anything.
    with pytest.raises(psycopg.errors.UndefinedColumn):
        PostgresWriter(table, database.connection).write(frame)

    assert database.rows(f"SELECT id, name FROM {table}") == [(1, "a")]
    assert database.staging_tables() == []
