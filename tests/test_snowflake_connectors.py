"""Snowflake connectors: `SnowflakeConnection`, `SnowflakeReader`, `SnowflakeWriter`, `snowflake`.

Covers `specs/snowflake_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no Snowflake account: they use the doubles below, a fake Spark
session, fake DataFrames, a fake `spark.read` and a fake `DataFrame.write`, which all write to one
ordered event log. Reading `schema` on a loaded DataFrame and calling `first()` on it are events of
that log. Configurations are written with `json.dumps` to a file under `tmp_path`, and a missing
pyspark is simulated in a fresh interpreter. No test needs a real key: `KEY` is base64 text, and
`PEM` is that text in lines of 64 characters between the two armour lines.

The three classes are reached as attributes of `ingestio.connector.all`, the module the spec
exports them from.

The integration tests at the end run against a real Snowflake account, through a local Spark
session (decision 32 of the spec). They are marked `integration` and skipped unless
`INGESTIO_SPARK_TESTS` is `1`, `INGESTIO_SNOWFLAKE_PACKAGES` holds the Maven coordinates of the
Snowflake Connector for Spark and of the Snowflake JDBC driver, separated by a comma, which the
session loads through `spark.jars.packages`, and `INGESTIO_SNOWFLAKE_HOST`,
`INGESTIO_SNOWFLAKE_USER`, `INGESTIO_SNOWFLAKE_PRIVATE_KEY` (the PEM text),
`INGESTIO_SNOWFLAKE_DATABASE`, `INGESTIO_SNOWFLAKE_SCHEMA` and `INGESTIO_SNOWFLAKE_WAREHOUSE` name
the account, with `INGESTIO_SNOWFLAKE_ROLE` when it needs one. They need a Java release that the
installed pyspark supports. A JVM loads its packages once, when it starts: when another test
module of the same run has already started one, they skip, so run this file in its own process.
Each test creates its own tables, `INGESTIO_IT_<STEM>_<HEX>`, and drops them, with statements run
through the connector's `Utils.runQuery` over py4j, so no Python client is installed. They were
written without being run: the machine they were written on has no Java release that pyspark
supports and no Snowflake account.
"""

from __future__ import annotations

import base64
import dataclasses
import importlib.metadata
import json
import logging
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
from typing import Any, NoReturn, TypeAlias, cast

import pytest
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

import ingestio.connector.all as connectors
from ingestio import SCD2, AppendOnly, FullLoad, Process, ReadSource, Step
from ingestio.connector.all import (
    MongoDBConnection,
    MySQLConnection,
    PostgresConnection,
    Reader,
    Writer,
)

HOST = "acme-shop.snowflakecomputing.com"
TABLE = "shop.public.orders"
KEY = base64.b64encode(bytes(range(100))).decode()
KEY_LINES = [KEY[start : start + 64] for start in range(0, len(KEY), 64)]
PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    + "".join(f"{line}\n" for line in KEY_LINES)
    + "-----END PRIVATE KEY-----\n"
)
TOKEN = "t0ken-secret"
# What no repr, str, message or note may hold: the key, each of its 64-character lines, the token.
SECRETS = (KEY, *[line for line in KEY_LINES if len(line) == 64], TOKEN)

# The connection options of `conn` and of `oauth`, with their keys in the case the spec gives.
CONN_OPTIONS = {"sfURL": HOST, "sfDatabase": "shop", "sfUser": "ingest", "pem_private_key": KEY}
OAUTH_OPTIONS = {
    "sfURL": HOST,
    "sfDatabase": "shop",
    "sfAuthenticator": "oauth",
    "sfToken": TOKEN,
}
# The options every save carries besides the connection options, and the one a replace adds.
WRITE_OPTIONS = {
    "dbtable": TABLE,
    "usestagingtable": "off",
    "truncate_table": "on",
    "column_mapping": "name",
}
TRUNCATE = {"preactions": "TRUNCATE TABLE %s"}

BOUND_TEXT = "2026-10-21T09:30:00.500000000+00:00"
BOUND_ROW: tuple[object, ...] = (BOUND_TEXT,)
BOUND = f"'{BOUND_TEXT}'"
DEFAULT_SCHEMA = StructType([StructField("UPDATED_AT", TimestampType())])
TIMESTAMP_FORMAT = """'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM'"""
COLUMNS = ("id", "payload")
TABLE_NAME_MESSAGE = (
    "table_name must be 'table', 'schema.table' or 'database.schema.table',"
    " each part a Snowflake identifier"
)
SOURCE_NAMES = (
    "bigquery, disk, gcs, json_api, mongodb, mysql, mysql_cdc, postgres, s3, snowflake, "
    "unity_catalog, volume"
)
TARGET_NAMES = (
    "bigquery, disk, gcs, json_lines, mysql, postgres, s3, snowflake, unity_catalog, volume"
)
SCD2_NAMES = "bigquery, mysql, postgres, snowflake, unity_catalog"
DEFAULT_SOURCE: dict[str, object] = {"system": "snowflake", "table_name": TABLE}
DEFAULT_TARGET: dict[str, object] = {"system": "snowflake", "table_name": TABLE}
MYSQL_SECTION: dict[str, object] = {"system": "mysql", "table_name": "shop.orders"}
MONGODB_SECTION: dict[str, object] = {
    "system": "mongodb",
    "database": "shop",
    "collection": "orders",
}
POSTGRES_SECTION: dict[str, object] = {"system": "postgres", "table_name": "public.orders"}
SCD2_SECTION: dict[str, object] = {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
}
SCD2_FILE: dict[str, object] = {
    "target": {"system": "snowflake", "table_name": "shop.public.dim_customer"},
    "scd2": SCD2_SECTION,
}
BATCH_COLUMNS = ("customer_id", "name", "city", "updated_at")
EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']snowflake["']""")


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


_Event: TypeAlias = _Load | _Schema | _First | _Save


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
    """Set an option as Spark does: it replaces the key of the same name, in any case, and keeps
    the case of the key given last."""
    for existing in [name for name in options if name.lower() == key.lower()]:
        del options[existing]
    options[key] = _spark_text(value)


class _FakeSpark:
    """Stands in for a `SparkSession`, and records every attribute accessed on it.

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does, and each `load()`
    returns a new `_FakeDataFrame`. Every loaded DataFrame has `schema`, the real `StructType`
    given here, and answers `first()` with the next row of `rows`, the last one once they run out;
    a row may be `None`. When `load_error` is set, every load raises it once recorded; when
    `schema_error` or `first_error` is set, every `schema` or `first()` raises it once recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        schema: StructType = DEFAULT_SCHEMA,
        rows: Sequence[tuple[object, ...] | None] = (BOUND_ROW,),
        load_error: BaseException | None = None,
        schema_error: BaseException | None = None,
        first_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._schema = schema
        self._rows = list(rows)
        self._firsts = 0
        self._load_error = load_error
        self._schema_error = schema_error
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
        frame = _FakeDataFrame(self._log, spark=self)
        self._log.events.append(_Load(source, dict(options), frame))
        if self._load_error is not None:
            raise self._load_error
        return frame

    def _next_row(self) -> tuple[object, ...] | None:
        row = self._rows[min(self._firsts, len(self._rows) - 1)]
        self._firsts += 1
        return row


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

    def load(
        self,
        path: object = None,
        format: str | None = None,  # noqa: A002 - PySpark's name
        schema: object = None,
        **options: object,
    ) -> _FakeDataFrame:
        if path is not None or schema is not None:
            raise AssertionError("load() was given a path or a schema")
        if format is not None:
            self._source = format
        self.options(**options)
        return self._spark._load(self._source, self._options)


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    A DataFrame that a load returned has `schema` and answers `first()` from its session; both are
    events of the log. A DataFrame given to the writer has `columns`, and each access to `write`
    returns a new `_FakeDataFrameWriter`, whose `save()` raises `save_error` once recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        spark: _FakeSpark | None = None,
        columns: Sequence[str] = (),
        save_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._spark = spark
        self._columns = list(columns)
        self._save_error = save_error
        self._accessed: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    def _session(self) -> _FakeSpark:
        if self._spark is None:
            raise AssertionError("a DataFrame that no load returned was read from")
        return self._spark

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def schema(self) -> StructType:
        spark = self._session()
        self._log.events.append(_Schema(self))
        if spark._schema_error is not None:
            raise spark._schema_error
        return spark._schema

    def first(self) -> tuple[object, ...] | None:
        spark = self._session()
        self._log.events.append(_First(self))
        if spark._first_error is not None:
            raise spark._first_error
        return spark._next_row()

    @property
    def write(self) -> _FakeDataFrameWriter:
        return _FakeDataFrameWriter(self._log, self._save_error)


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

    def mode(self, saveMode: str) -> _FakeDataFrameWriter:  # noqa: N803 - PySpark's name
        self._mode = saveMode
        return self

    def save(
        self,
        path: object = None,
        format: str | None = None,  # noqa: A002 - PySpark's name
        mode: str | None = None,
        partitionBy: object = None,  # noqa: N803 - PySpark's name
        **options: object,
    ) -> None:
        if path is not None or partitionBy is not None:
            raise AssertionError("save() was given a path or partitionBy")
        if format is not None:
            self._source = format
        if mode is not None:
            self._mode = mode
        self.options(**options)
        self._log.events.append(_Save(self._source, self._mode, dict(self._options)))
        if self._save_error is not None:
            raise self._save_error


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
    raise AssertionError("the active session was looked up")


@pytest.fixture(autouse=True)
def _isolated(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `SparkSession.active` fail when called, except in the integration tests, which need
    the real one."""
    if request.node.get_closest_marker("integration") is not None:
        return
    monkeypatch.setattr(SparkSession, "active", _refuse)


@pytest.fixture
def log() -> _Log:
    """The event log the doubles of one test share."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """A fake session whose DataFrames have the default schema and answer `BOUND_ROW`."""
    return _FakeSpark(log)


@pytest.fixture
def conn() -> connectors.SnowflakeConnection:
    """The key-pair connection of the acceptance criteria."""
    return connectors.SnowflakeConnection(HOST, "shop", user="ingest", private_key=PEM)


@pytest.fixture
def oauth() -> connectors.SnowflakeConnection:
    """The OAuth connection of the acceptance criteria."""
    return connectors.SnowflakeConnection(HOST, "shop", oauth_token=TOKEN)


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
    conn: connectors.SnowflakeConnection, spark: _FakeSpark, **arguments: Any
) -> connectors.SnowflakeReader:
    """The reader of the criteria, `shop.public.orders` over `spark`, with `arguments` added."""
    return connectors.SnowflakeReader(TABLE, conn, spark=_session(spark), **arguments)


def _build(kind: str, table_name: str, connection: Any, **arguments: Any) -> object:
    """Build the reader or the writer, as `kind` names it."""
    if kind == "reader":
        return connectors.SnowflakeReader(table_name, connection, **arguments)
    assert kind == "writer", kind
    return connectors.SnowflakeWriter(table_name, connection, **arguments)


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


def _query(load: _Load) -> str:
    """The `query` option of `load`."""
    return _lower(load.options)["query"]


def _attributes(frame: _FakeDataFrame) -> list[str]:
    """The attributes accessed on `frame`, in order."""
    return frame._accessed


def _notes(error: BaseException) -> list[str]:
    """The notes added to `error`."""
    return list(getattr(error, "__notes__", []))


def _transcript(events: Sequence[_Event]) -> list[str]:
    """The kinds of the events, in order."""
    names = {_Load: "load", _Schema: "schema", _First: "first", _Save: "save"}
    return [names[type(event)] for event in events]


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


def _type_query(*, column: str = "updated_at", table: str = TABLE) -> str:
    """The query of the type load of a watermark read."""
    return f"SELECT {column} FROM {table} WHERE FALSE"


def _bound_query(
    value: str | None = None,
    *,
    column: str = "updated_at",
    table: str = TABLE,
    render: str | None = None,
) -> str:
    """The query of the bound load, with `value` as the literal of the watermark, and by default
    the render of a timestamp."""
    shown = f"TO_VARCHAR(MAX({column}), {TIMESTAMP_FORMAT})" if render is None else render
    query = f"SELECT {shown} AS UPPER_BOUND FROM {table}"
    return query if value is None else f"{query} WHERE {column} > {value}"


def _data_query(predicate: str, *, table: str = TABLE) -> str:
    """The query of the data load of a watermark read."""
    return f"SELECT * FROM {table} WHERE {predicate}"


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


def _assert_twin(built: object, twin: object, *same: object) -> None:
    """`built` has the class and the state of `twin`, and holds each object of `same` that the
    twin holds, itself rather than an equal copy."""
    assert type(built) is type(twin), (built, twin)
    assert vars(built) == vars(twin)
    for name, value in vars(twin).items():
        if any(value is item for item in same):
            assert vars(built)[name] is value, name


def _assert_no_secret(*texts: str) -> None:
    """None of `texts` holds the key, a 64-character line of it, or the token."""
    for text in texts:
        for secret in SECRETS:
            assert secret not in text, text


CALLBACKS: dict[str, Callable[..., object]] = {
    "ReadSource": ReadSource,
    "AppendOnly": AppendOnly,
    "FullLoad": FullLoad,
    "SCD2": SCD2,
}

WriteCall: TypeAlias = "Callable[[connectors.SnowflakeWriter, DataFrame], object]"

APPEND_CALLS: dict[str, WriteCall] = {
    "write(df)": lambda writer, frame: writer.write(frame),
    "write(df, replace=False)": lambda writer, frame: writer.write(frame, replace=False),
}

EVERY_CALL: dict[str, WriteCall] = {
    **APPEND_CALLS,
    "write(df, replace=True)": lambda writer, frame: writer.write(frame, replace=True),
}


def _replaces(name: str) -> bool:
    """Whether the call of `EVERY_CALL` named `name` replaces."""
    return name.endswith("replace=True)")


def _section_callbacks(section: str) -> list[str]:
    """The callbacks that build the connector of `section`."""
    return ["ReadSource"] if section == "source" else ["AppendOnly", "FullLoad"]


# --- Import surface and the extra ---------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import SnowflakeConnection, SnowflakeReader, SnowflakeWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.snowflake as snowflake

names = {"SnowflakeConnection", "SnowflakeReader", "SnowflakeWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(snowflake.__all__), snowflake.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = r"""
import base64
import sys

sys.modules["pyspark"] = None
for name in ("pyspark", "pyspark.sql", "pyspark.sql.types"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import SnowflakeConnection, SnowflakeReader, SnowflakeWriter

KEY = base64.b64encode(bytes(range(100))).decode()
PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    + "".join(KEY[start : start + 64] + "\n" for start in range(0, len(KEY), 64))
    + "-----END PRIVATE KEY-----\n"
)
loads = []
saves = []


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def load(self, *args, **kwargs):
        loads.append(object())
        return loads[-1]


class _FakeSpark:
    @property
    def read(self):
        return _FakeDataFrameReader()


class _FakeDataFrameWriter:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def mode(self, *args, **kwargs):
        return self

    def save(self, *args, **kwargs):
        saves.append(object())


class _FakeDataFrame:
    columns = ["id", "payload"]

    @property
    def write(self):
        return _FakeDataFrameWriter()


host = "acme-shop.snowflakecomputing.com"
conn = SnowflakeConnection(host, "shop", user="ingest", private_key=PEM)
oauth = SnowflakeConnection(host, "shop", oauth_token="t0ken-secret")
frame = SnowflakeReader("shop.public.orders", conn, spark=_FakeSpark()).read()
assert len(loads) == 1, loads
assert frame is loads[0], frame

writer = SnowflakeWriter("shop.public.orders", conn)
assert writer.write(_FakeDataFrame()) is None
assert len(saves) == 1, saves
assert writer.write(_FakeDataFrame(), replace=True) is None
assert len(saves) == 2, saves
"""


def test_at001_the_three_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connection_the_reader_and_the_writer_work_without_pyspark(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at003_the_distribution_declares_no_snowflake_extra() -> None:
    metadata = importlib.metadata.metadata("ingestio")

    assert "snowflake" not in (metadata.get_all("Provides-Extra") or [])
    for requirement in metadata.get_all("Requires-Dist") or []:
        assert not EXTRA_MARKER.search(requirement), requirement


# --- SnowflakeConnection ------------------------------------------------------------------

# The calls of AT-004 that raise: an id, the positional arguments, the keyword arguments and the
# message. The last two break two rules each, and raise the message of the earlier rule.
AT004_REJECTED: list[tuple[str, tuple[str, ...], dict[str, object], str]] = [
    ("host", ("", "shop"), {"user": "ingest", "private_key": PEM}, "host must not be empty"),
    ("database", ("h", ""), {"user": "ingest", "private_key": PEM}, "database must not be empty"),
    ("user", ("h", "shop"), {"user": "", "private_key": PEM}, "user must not be empty"),
    (
        "schema",
        ("h", "shop"),
        {"user": "ingest", "private_key": PEM, "schema": ""},
        "schema must not be empty",
    ),
    (
        "warehouse",
        ("h", "shop"),
        {"user": "ingest", "private_key": PEM, "warehouse": ""},
        "warehouse must not be empty",
    ),
    (
        "role",
        ("h", "shop"),
        {"user": "ingest", "private_key": PEM, "role": ""},
        "role must not be empty",
    ),
    ("no-credential", ("h", "shop"), {"user": "ingest"}, "give private_key or oauth_token"),
    (
        "both-credentials",
        ("h", "shop"),
        {"user": "ingest", "private_key": PEM, "oauth_token": "t"},
        "give private_key or oauth_token, not both",
    ),
    ("empty-token", ("h", "shop"), {"oauth_token": ""}, "oauth_token must not be empty"),
    ("key-without-user", ("h", "shop"), {"private_key": PEM}, "private_key needs user"),
    ("host-first", ("", ""), {"user": ""}, "host must not be empty"),
    ("role-before-token", ("h", "shop"), {"oauth_token": "", "role": ""}, "role must not be empty"),
]

# The calls of AT-004 that build: an id, the positional and the keyword arguments.
AT004_ACCEPTED: list[tuple[str, tuple[str, ...], dict[str, object]]] = [
    ("token", ("h", "shop"), {"oauth_token": "t"}),
    ("token-and-user", ("h", "shop"), {"oauth_token": "t", "user": "ingest"}),
    (
        "conn-with-schema",
        (HOST, "shop"),
        {"user": "ingest", "private_key": PEM, "schema": "public"},
    ),
    (
        "conn-with-warehouse",
        (HOST, "shop"),
        {"user": "ingest", "private_key": PEM, "warehouse": "wh"},
    ),
    ("conn-with-role", (HOST, "shop"), {"user": "ingest", "private_key": PEM, "role": "loader"}),
]


@pytest.mark.parametrize(
    ("args", "kwargs", "message"),
    [
        pytest.param(args, kwargs, message, id=name)
        for name, args, kwargs, message in AT004_REJECTED
    ],
)
def test_at004_invalid_connection_settings_are_rejected_in_the_order_of_the_rules(
    args: tuple[str, ...], kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.SnowflakeConnection(*args, **cast(Any, kwargs))


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [pytest.param(args, kwargs, id=name) for name, args, kwargs in AT004_ACCEPTED],
)
def test_at004_a_token_with_or_without_user_and_the_optional_names_are_accepted(
    args: tuple[str, ...], kwargs: dict[str, object]
) -> None:
    connection = connectors.SnowflakeConnection(*args, **cast(Any, kwargs))

    for name, value in kwargs.items():
        assert getattr(connection, name) == value


GENERIC_KEY_MESSAGE = (
    "private_key must be an unencrypted PKCS#8 key, as PEM text or its base64 body"
)


def _without_last_line(text: str) -> str:
    return "".join(text.splitlines(keepends=True)[:-1])


AT005_ACCEPTED: list[tuple[str, str]] = [
    ("pem", PEM),
    ("pem-crlf", PEM.replace("\n", "\r\n")),
    ("pem-surrounded-by-whitespace", "  \n" + PEM + "\n\n"),
    ("body", KEY),
    ("body-in-lines", "\n".join(KEY_LINES)),
]

AT005_REJECTED: list[tuple[str, str, str]] = [
    (
        "encrypted",
        PEM.replace("PRIVATE KEY", "ENCRYPTED PRIVATE KEY"),
        "private_key must be unencrypted",
    ),
    (
        "pkcs1",
        PEM.replace("PRIVATE KEY", "RSA PRIVATE KEY"),
        "private_key must be PKCS#8 (BEGIN PRIVATE KEY), not PKCS#1 (BEGIN RSA PRIVATE KEY)",
    ),
    ("pem-without-last-line", _without_last_line(PEM), GENERIC_KEY_MESSAGE),
    (
        "certificate",
        "-----BEGIN CERTIFICATE-----\n" + KEY + "\n-----END CERTIFICATE-----",
        GENERIC_KEY_MESSAGE,
    ),
    (
        "empty-body",
        "-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----",
        GENERIC_KEY_MESSAGE,
    ),
    ("empty", "", GENERIC_KEY_MESSAGE),
    ("blank", "   ", GENERIC_KEY_MESSAGE),
    ("not-base64", KEY[:-1] + "!", GENERIC_KEY_MESSAGE),
]


@pytest.mark.parametrize(
    "private_key", [pytest.param(text, id=name) for name, text in AT005_ACCEPTED]
)
def test_at005_the_connector_receives_the_keys_base64_body_without_whitespace(
    private_key: str, log: _Log, spark: _FakeSpark
) -> None:
    connection = connectors.SnowflakeConnection(
        HOST, "shop", user="ingest", private_key=private_key
    )

    _reader(connection, spark).read()

    (load,) = _loads(log)
    assert _lower(load.options)["pem_private_key"] == KEY


@pytest.mark.parametrize(
    ("private_key", "message"),
    [pytest.param(text, message, id=name) for name, text, message in AT005_REJECTED],
)
def test_at005_a_key_of_another_form_is_rejected_without_showing_it(
    private_key: str, message: str
) -> None:
    with pytest.raises(ValueError) as excinfo:
        connectors.SnowflakeConnection(HOST, "shop", user="ingest", private_key=private_key)

    assert str(excinfo.value) == message
    _assert_no_secret(str(excinfo.value), *_notes(excinfo.value))


CONN_REPR = (
    "SnowflakeConnection(host='acme-shop.snowflakecomputing.com', database='shop', schema=None,"
    " warehouse=None, role=None, user='ingest')"
)
READER_REPR = (
    "SnowflakeReader(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com',"
    " database='shop')"
)
WRITER_REPR = (
    "SnowflakeWriter(table_name='shop.public.orders', host='acme-shop.snowflakecomputing.com',"
    " database='shop')"
)


def test_at006_repr_shows_what_identifies_the_database_and_the_table(
    conn: connectors.SnowflakeConnection, oauth: connectors.SnowflakeConnection
) -> None:
    assert repr(conn) == CONN_REPR
    assert str(conn) == CONN_REPR
    assert repr(oauth) == CONN_REPR.replace("user='ingest'", "user=None")
    assert str(oauth) == CONN_REPR.replace("user='ingest'", "user=None")
    assert repr(connectors.SnowflakeReader(TABLE, conn)) == READER_REPR
    assert repr(connectors.SnowflakeWriter(TABLE, conn)) == WRITER_REPR


def test_at006_repr_shows_the_schema_warehouse_and_role_and_not_the_watermark_or_options() -> None:
    connection = connectors.SnowflakeConnection(
        HOST,
        "shop",
        user="ingest",
        private_key=PEM,
        schema="public",
        warehouse="wh",
        role="loader",
    )
    reader = connectors.SnowflakeReader(
        TABLE,
        connection,
        watermark_column="updated_at",
        watermark_value=41,
        options={"autopushdown": "off"},
    )
    writer = connectors.SnowflakeWriter(TABLE, connection, options={"parallelism": 8})

    assert repr(connection) == (
        "SnowflakeConnection(host='acme-shop.snowflakecomputing.com', database='shop',"
        " schema='public', warehouse='wh', role='loader', user='ingest')"
    )
    assert repr(reader) == READER_REPR
    assert repr(writer) == WRITER_REPR


def test_at007_a_connection_exposes_its_arguments_and_is_immutable(
    conn: connectors.SnowflakeConnection,
) -> None:
    assert conn.host == HOST
    assert conn.database == "shop"
    assert conn.user == "ingest"
    assert conn.private_key == PEM
    assert conn.oauth_token is None
    assert conn.schema is None
    assert conn.warehouse is None
    assert conn.role is None
    for name in (
        "host",
        "database",
        "user",
        "private_key",
        "oauth_token",
        "schema",
        "warehouse",
        "role",
    ):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(conn, name, "other")


def test_at007_user_is_keyword_only() -> None:
    build: Any = connectors.SnowflakeConnection

    with pytest.raises(TypeError):
        build("h", "shop", "ingest")


# The rows of AT-008: an id, the keyword arguments of the connection, and its connection options.
AT008_ROWS: list[tuple[str, dict[str, object], dict[str, str]]] = [
    ("conn", {"user": "ingest", "private_key": PEM}, CONN_OPTIONS),
    ("oauth", {"oauth_token": TOKEN}, OAUTH_OPTIONS),
    (
        "oauth-with-user",
        {"oauth_token": TOKEN, "user": "ingest"},
        {**OAUTH_OPTIONS, "sfUser": "ingest"},
    ),
    (
        "conn-with-schema-warehouse-role",
        {
            "user": "ingest",
            "private_key": PEM,
            "schema": "public",
            "warehouse": "wh",
            "role": "loader",
        },
        {**CONN_OPTIONS, "sfSchema": "public", "sfWarehouse": "wh", "sfRole": "loader"},
    ),
]


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [pytest.param(kwargs, expected, id=name) for name, kwargs, expected in AT008_ROWS],
)
def test_at008_a_load_carries_exactly_the_options_of_its_connection(
    kwargs: dict[str, object], expected: dict[str, str], log: _Log, spark: _FakeSpark
) -> None:
    connection = connectors.SnowflakeConnection(HOST, "shop", **cast(Any, kwargs))

    _reader(connection, spark).read()

    (load,) = _loads(log)
    assert _lower(load.options) == _lower({**expected, "dbtable": TABLE})


# --- Building the connectors --------------------------------------------------------------

AT009_ACCEPTED = [
    "orders",
    "public.orders",
    "shop.public.orders",
    "_t$1",
    "ORDERS",
    '"Mixed Case"',
    '"Shop"."Sales.Q1"."O""Brien"',
    '""""',
]
AT009_REJECTED = [
    "",
    "orders.",
    ".orders",
    "a.b.c.d",
    "my table",
    "1orders",
    "$orders",
    '"unterminated',
    'a"b',
    '"a"b"',
    '""',
    'shop."".orders',
    "(SELECT 1)",
    "orders;",
    "ord\u00e9rs",
]


@pytest.mark.parametrize("kind", ["reader", "writer"])
@pytest.mark.parametrize("name", AT009_ACCEPTED)
def test_at009_one_two_or_three_snowflake_identifiers_are_accepted(
    kind: str, name: str, conn: connectors.SnowflakeConnection
) -> None:
    _build(kind, name, conn)


@pytest.mark.parametrize("kind", ["reader", "writer"])
@pytest.mark.parametrize("name", AT009_REJECTED)
def test_at009_any_other_table_name_is_rejected(
    kind: str, name: str, conn: connectors.SnowflakeConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(TABLE_NAME_MESSAGE)):
        _build(kind, name, conn)


def test_at010_a_watermark_value_needs_a_watermark_column(
    conn: connectors.SnowflakeConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_value needs watermark_column")):
        connectors.SnowflakeReader(TABLE, conn, watermark_value=41)


def test_at010_an_empty_watermark_column_is_rejected(
    conn: connectors.SnowflakeConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_column must not be empty")):
        connectors.SnowflakeReader(TABLE, conn, watermark_column="")


AT010_REJECTED = ["updated at", "a.b", '"x', "9lives", "x; DROP TABLE t"]


@pytest.mark.parametrize("column", AT010_REJECTED)
def test_at010_a_watermark_column_that_is_not_an_identifier_is_rejected(
    column: str, conn: connectors.SnowflakeConnection
) -> None:
    message = "watermark_column must be a Snowflake identifier"
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.SnowflakeReader(TABLE, conn, watermark_column=column)


@pytest.mark.parametrize("column", ["updated_at", "UPDATED_AT", '"Updated At"', '"a""b"'])
def test_at010_unquoted_and_quoted_watermark_columns_are_accepted(
    column: str, conn: connectors.SnowflakeConnection
) -> None:
    connectors.SnowflakeReader(TABLE, conn, watermark_column=column)


# The values of AT-011 that the reader rejects, with the message.
AT011_REJECTED: list[tuple[str, object, str]] = [
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
    [pytest.param(value, message, id=name) for name, value, message in AT011_REJECTED],
)
def test_at011_unsupported_watermark_values_are_rejected(
    value: object, message: str, conn: connectors.SnowflakeConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.SnowflakeReader(
            TABLE, conn, watermark_column="updated_at", watermark_value=cast(Any, value)
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
def test_at011_supported_watermark_values_are_accepted(
    value: int | str | Decimal | date, conn: connectors.SnowflakeConnection
) -> None:
    connectors.SnowflakeReader(TABLE, conn, watermark_column="updated_at", watermark_value=value)


RESERVED_BY_BOTH = (
    "sfURL",
    "SFURL",
    "sfDatabase",
    "sfSchema",
    "sfWarehouse",
    "sfRole",
    "sfUser",
    "sfPassword",
    "pem_private_key",
    "PEM_PRIVATE_KEY",
    "sfAuthenticator",
    "sfToken",
    "sfaccount",
    "sfworkloadidentityprovider",
    "oauthclientid",
    "oauthclientsecret",
    "oauthtokenrequesturl",
    "oauthscope",
    "connection_id",
    "dbtable",
    "Query",
)
RESERVED_BY_THE_WRITER = (
    "usestagingtable",
    "TRUNCATE_TABLE",
    "column_mapping",
    "columnmap",
    "preactions",
    "PostActions",
    "internal_skip_write_when_writing_empty_dataframe",
)
READER_ACCEPTED: dict[str, str | int | float | bool] = {
    "autopushdown": "off",
    "sfTimezone": "UTC",
    "preactions": "ALTER SESSION SET QUERY_TAG = 'x'",
    "usestagingtable": "on",
}
WRITER_ACCEPTED: dict[str, str | int | float | bool] = {
    "column_mismatch_behavior": "ignore",
    "continue_on_error": "on",
    "keep_column_case": "on",
    "sfCompress": False,
    "parallelism": 8,
    "sfTimezone": "UTC",
}


@pytest.mark.parametrize(
    ("kind", "key"),
    [
        *[
            pytest.param(kind, key, id=f"{kind}-{key}")
            for kind in ("reader", "writer")
            for key in RESERVED_BY_BOTH
        ],
        *[pytest.param("writer", key, id=f"writer-{key}") for key in RESERVED_BY_THE_WRITER],
    ],
)
def test_at012_options_the_connector_sets_are_rejected_in_any_case(
    kind: str, key: str, conn: connectors.SnowflakeConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        _build(kind, TABLE, conn, options={key: "x"})


def test_at012_other_options_pass_and_the_writers_reserved_keys_pass_on_the_reader(
    conn: connectors.SnowflakeConnection,
) -> None:
    connectors.SnowflakeReader(TABLE, conn, options=READER_ACCEPTED)
    connectors.SnowflakeWriter(TABLE, conn, options=WRITER_ACCEPTED)


def test_at013_building_the_connectors_touches_no_spark_session(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))

    connectors.SnowflakeReader(TABLE, conn)
    connectors.SnowflakeReader(TABLE, conn, spark=_session(spark))
    connectors.SnowflakeReader(TABLE, conn, watermark_column="updated_at")
    connectors.SnowflakeReader(
        TABLE, conn, watermark_column="updated_at", watermark_value=41, spark=_session(spark)
    )
    connectors.SnowflakeWriter(TABLE, conn)
    connectors.SnowflakeWriter(TABLE, conn, options={"parallelism": 8})

    assert log.events == []
    assert log.spark_attributes == []
    assert lookups == []


# --- Full read ----------------------------------------------------------------------------


def test_at014_a_full_read_loads_the_table_with_the_connection_options(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    frame = _reader(conn, spark).read()

    assert _transcript(log.events) == ["load"]
    (load,) = _loads(log)
    assert load.source == "snowflake"
    assert _lower(load.options) == _lower({**CONN_OPTIONS, "dbtable": TABLE})
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []


USER_READ_OPTIONS: dict[str, str | int | float | bool] = {
    "autopushdown": "off",
    "sfTimezone": "UTC",
    "use_copy_unload": True,
}
USER_READ_TEXTS = {"autopushdown": "off", "sfTimezone": "UTC", "use_copy_unload": "true"}


def test_at015_user_options_reach_a_full_read(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    _reader(conn, spark, options=USER_READ_OPTIONS).read()

    (load,) = _loads(log)
    assert _lower(load.options) == _lower({**CONN_OPTIONS, "dbtable": TABLE, **USER_READ_TEXTS})


def test_at015_user_options_reach_the_three_loads_of_a_watermark_read(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    _reader(conn, spark, watermark_column="updated_at", options=USER_READ_OPTIONS).read()

    loads = _loads(log)
    assert len(loads) == 3
    for load in loads:
        options = _lower(load.options)
        assert "query" in options, load.options
        del options["query"]
        assert options == _lower({**CONN_OPTIONS, **USER_READ_TEXTS})


def test_at016_the_active_session_is_looked_up_when_read_runs(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    reader = connectors.SnowflakeReader(TABLE, conn)
    assert lookups == []

    def fake_session() -> SparkSession:
        lookups.append("fake session")
        return _session(spark)

    monkeypatch.setattr(SparkSession, "active", fake_session)
    frame = reader.read()

    assert "fake session" in lookups
    (load,) = _loads(log)
    assert frame is _frame(load.frame)


def test_at016_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, conn: connectors.SnowflakeConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    err = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise err

    reader = connectors.SnowflakeReader(TABLE, conn)
    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert _loads(log) == []


# --- Watermark read -----------------------------------------------------------------------

OCT_1_LITERAL = "'2026-10-01T00:00:00+00:00'"


def test_at017_a_watermark_read_reads_the_type_then_the_bound_then_the_slice(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    frame = _reader(
        conn,
        spark,
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    assert _transcript(log.events) == ["load", "schema", "load", "first", "load"]
    kind, schema, bound, first, data = log.events
    assert isinstance(kind, _Load)
    assert isinstance(schema, _Schema)
    assert isinstance(bound, _Load)
    assert isinstance(first, _First)
    assert isinstance(data, _Load)
    assert schema.frame is kind.frame
    assert first.frame is bound.frame
    assert _attributes(kind.frame) == ["schema"]
    expected = [
        "SELECT updated_at FROM shop.public.orders WHERE FALSE",
        (
            "SELECT TO_VARCHAR(MAX(updated_at), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')"
            " AS UPPER_BOUND FROM shop.public.orders"
            " WHERE updated_at > '2026-10-01T00:00:00+00:00'"
        ),
        (
            "SELECT * FROM shop.public.orders WHERE updated_at > '2026-10-01T00:00:00+00:00'"
            " AND updated_at <= '2026-10-21T09:30:00.500000000+00:00'"
        ),
    ]
    for load, query in zip((kind, bound, data), expected, strict=True):
        assert load.source == "snowflake"
        assert _lower(load.options) == _lower({**CONN_OPTIONS, "query": query})
        assert "dbtable" not in _lower(load.options)
    assert frame is _frame(data.frame)
    assert _attributes(data.frame) == []


def test_at018_without_a_watermark_value_the_slice_ends_at_the_bound(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    _reader(conn, spark, watermark_column="updated_at").read()

    _, bound, data = _loads(log)
    assert _query(bound) == (
        "SELECT TO_VARCHAR(MAX(updated_at), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')"
        " AS UPPER_BOUND FROM shop.public.orders"
    )
    assert _query(data) == (
        "SELECT * FROM shop.public.orders WHERE updated_at <= '2026-10-21T09:30:00.500000000+00:00'"
    )


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
@pytest.mark.parametrize("row", [(None,), None], ids=["null-bound", "no-row"])
def test_at019_without_a_bound_the_slice_is_empty(
    row: tuple[object, ...] | None,
    value: int | None,
    log: _Log,
    conn: connectors.SnowflakeConnection,
) -> None:
    spark = _FakeSpark(log, rows=[row])

    frame = _reader(conn, spark, watermark_column="updated_at", watermark_value=value).read()

    loads = _loads(log)
    assert len(loads) == 3
    assert _query(loads[2]) == "SELECT * FROM shop.public.orders WHERE FALSE"
    assert frame is _frame(loads[2].frame)


AT020_LITERALS = [
    pytest.param(41, "41", id="int"),
    pytest.param(0, "0", id="zero"),
    pytest.param(-7, "-7", id="negative-int"),
    pytest.param(Decimal("12.50"), "12.50", id="decimal"),
    pytest.param(Decimal("1E+3"), "1000", id="decimal-exponent"),
    pytest.param(Decimal("-0.25"), "-0.25", id="negative-decimal"),
    pytest.param("41", "'41'", id="numeric-string"),
    pytest.param("O'Brien", "'O''Brien'", id="quote"),
    pytest.param(r"C:\temp", r"'C:\\temp'", id="backslash"),
    pytest.param("it\\'s", r"'it\\''s'", id="backslash-and-quote"),
    pytest.param("", "''", id="empty-string"),
    pytest.param("\u00e9", "'\u00e9'", id="non-ascii"),
    pytest.param(date(2026, 10, 1), "'2026-10-01'", id="date"),
    pytest.param(
        datetime(2026, 10, 1, 8, 30, tzinfo=UTC), "'2026-10-01T08:30:00+00:00'", id="utc-datetime"
    ),
    pytest.param(
        datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3))),
        "'2026-10-01T11:30:00.123456+00:00'",
        id="offset-datetime",
    ),
]


@pytest.mark.parametrize(("value", "literal"), AT020_LITERALS)
def test_at020_watermark_values_become_numbers_or_escaped_strings(
    value: int | Decimal | str | date,
    literal: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
) -> None:
    _reader(conn, spark, watermark_column="updated_at", watermark_value=value).read()

    _, bound, data = _loads(log)
    assert _query(bound) == _bound_query(literal)
    assert _query(data) == _data_query(f"updated_at > {literal} AND updated_at <= {BOUND}")


AT021_BOUNDS = [
    pytest.param(
        LongType(), "TO_VARCHAR(MAX(w))", "9007199254740993", "9007199254740993", id="long"
    ),
    pytest.param(
        DecimalType(38, 0),
        "TO_VARCHAR(MAX(w))",
        "18446744073709551615",
        "18446744073709551615",
        id="decimal-38-0",
    ),
    pytest.param(DecimalType(10, 2), "TO_VARCHAR(MAX(w))", "12.50", "12.50", id="decimal-10-2"),
    pytest.param(DoubleType(), "TO_VARCHAR(MAX(w))", "1.5e+20", "1.5e+20", id="double"),
    pytest.param(DoubleType(), "TO_VARCHAR(MAX(w))", "inf", "'inf'", id="double-not-a-number"),
    pytest.param(
        LongType(), "TO_VARCHAR(MAX(w))", "1 OR 1=1", "'1 OR 1=1'", id="long-not-a-number"
    ),
    pytest.param(StringType(), "TO_VARCHAR(MAX(w))", "41", "'41'", id="string"),
    pytest.param(
        StringType(), "TO_VARCHAR(MAX(w))", "it's \\x", r"'it''s \\x'", id="string-escaped"
    ),
    pytest.param(BooleanType(), "TO_VARCHAR(MAX(w))", "true", "'true'", id="boolean"),
    pytest.param(
        DateType(), "TO_VARCHAR(MAX(w), 'YYYY-MM-DD')", "2026-10-21", "'2026-10-21'", id="date"
    ),
    pytest.param(
        TimestampType(),
        "TO_VARCHAR(MAX(w), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')",
        "2026-10-21T06:30:00.123456789-03:00",
        "'2026-10-21T06:30:00.123456789-03:00'",
        id="timestamp",
    ),
    pytest.param(
        TimestampNTZType(),
        "TO_VARCHAR(MAX(w), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM')",
        "2026-10-21T09:30:00.000000000+00:00",
        "'2026-10-21T09:30:00.000000000+00:00'",
        id="timestamp-ntz",
    ),
]


@pytest.mark.parametrize(("value_type", "render", "text", "literal"), AT021_BOUNDS)
def test_at021_the_watermark_type_chooses_the_render_and_the_bound_is_unquoted_only_as_a_number(
    value_type: DataType,
    render: str,
    text: str,
    literal: str,
    log: _Log,
    conn: connectors.SnowflakeConnection,
) -> None:
    spark = _FakeSpark(log, schema=StructType([StructField("W", value_type)]), rows=[(text,)])

    _reader(conn, spark, watermark_column="w").read()

    kind, bound, data = _loads(log)
    assert _query(kind) == "SELECT w FROM shop.public.orders WHERE FALSE"
    assert _query(bound) == f"SELECT {render} AS UPPER_BOUND FROM shop.public.orders"
    assert _query(data) == f"SELECT * FROM shop.public.orders WHERE w <= {literal}"


@pytest.mark.parametrize(
    ("table_name", "column"),
    [
        pytest.param("orders", "updated_at", id="unqualified"),
        pytest.param("public.orders", "UPDATED_AT", id="upper-case"),
        pytest.param('"Shop"."Sales.Q1"."O""Brien"', '"Updated At"', id="quoted"),
        pytest.param('shop.public."my table"', '"a""b"', id="mixed"),
    ],
)
def test_at022_table_and_column_names_reach_the_sql_as_given(
    table_name: str,
    column: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
) -> None:
    connectors.SnowflakeReader(table_name, conn, spark=_session(spark)).read()
    connectors.SnowflakeReader(
        table_name, conn, spark=_session(spark), watermark_column=column, watermark_value=1
    ).read()

    full, kind, bound, data = _loads(log)
    assert _lower(full.options)["dbtable"] == table_name
    assert _query(kind) == _type_query(column=column, table=table_name)
    assert _query(bound) == _bound_query("1", column=column, table=table_name)
    assert _query(data) == _data_query(f"{column} > 1 AND {column} <= {BOUND}", table=table_name)


def test_at022_the_examples_of_the_criterion_are_exact(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection
) -> None:
    connectors.SnowflakeReader(
        "orders", conn, spark=_session(spark), watermark_column="updated_at", watermark_value=1
    ).read()
    connectors.SnowflakeReader(
        '"Shop"."Sales.Q1"."O""Brien"',
        conn,
        spark=_session(spark),
        watermark_column='"Updated At"',
        watermark_value=1,
    ).read()

    loads = _loads(log)
    assert _query(loads[0]) == "SELECT updated_at FROM orders WHERE FALSE"
    assert _query(loads[5]) == (
        'SELECT * FROM "Shop"."Sales.Q1"."O""Brien" WHERE "Updated At" > 1'
        " AND \"Updated At\" <= '2026-10-21T09:30:00.500000000+00:00'"
    )


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at023_a_failed_load_propagates_and_nothing_is_loaded_after_it(
    watermark: bool, log: _Log, conn: connectors.SnowflakeConnection
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


def test_at023_a_failed_schema_propagates_and_nothing_is_loaded_after_it(
    log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    err = RuntimeError("down")
    spark = _FakeSpark(log, schema_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        _reader(conn, spark, watermark_column="updated_at", watermark_value=41).read()
    assert excinfo.value is err
    assert len(_loads(log)) == 1


def test_at023_a_failed_bound_row_propagates_and_nothing_is_loaded_after_it(
    log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    err = RuntimeError("down")
    spark = _FakeSpark(log, first_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        _reader(conn, spark, watermark_column="updated_at", watermark_value=41).read()
    assert excinfo.value is err
    assert len(_loads(log)) == 2


def test_at024_each_read_captures_its_own_bound(
    log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    later = "2026-10-22T10:00:00.000000000+00:00"
    spark = _FakeSpark(log, rows=[BOUND_ROW, (later,)])
    reader = _reader(conn, spark, watermark_column="updated_at")

    first = reader.read()
    second = reader.read()

    loads = _loads(log)
    assert len(loads) == 6
    assert _query(loads[2]) == _data_query(f"updated_at <= {BOUND}")
    assert _query(loads[5]) == _data_query(f"updated_at <= '{later}'")
    assert first is _frame(loads[2].frame)
    assert second is _frame(loads[5].frame)
    assert first is not second


# --- Writing ------------------------------------------------------------------------------


def _assert_one_save(
    log: _Log, df: _FakeDataFrame, expected: Mapping[str, str], returned: object
) -> None:
    """The write saved once, in `append` mode, with exactly `expected`, read only `columns` and
    `write` from `df`, loaded nothing and returned `None`."""
    assert _transcript(log.events) == ["save"]
    (save,) = _saves(log)
    assert save.source == "snowflake"
    assert save.mode == "append"
    assert _lower(save.options) == _lower(expected)
    assert set(_attributes(df)) <= {"columns", "write"}, _attributes(df)
    assert returned is None


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at025_write_appends_through_the_connectors_transaction(
    call: WriteCall, log: _Log, conn: connectors.SnowflakeConnection, df: _FakeDataFrame
) -> None:
    returned = call(connectors.SnowflakeWriter(TABLE, conn), _frame(df))

    _assert_one_save(log, df, {**CONN_OPTIONS, **WRITE_OPTIONS}, returned)
    (save,) = _saves(log)
    assert "preactions" not in _lower(save.options)


def test_at026_write_with_replace_adds_the_truncate_preaction_and_still_appends(
    log: _Log, conn: connectors.SnowflakeConnection, df: _FakeDataFrame
) -> None:
    call = EVERY_CALL["write(df, replace=True)"]

    returned = call(connectors.SnowflakeWriter(TABLE, conn), _frame(df))

    _assert_one_save(log, df, {**CONN_OPTIONS, **WRITE_OPTIONS, **TRUNCATE}, returned)


USER_WRITE_OPTIONS: dict[str, str | int | float | bool] = {
    "column_mismatch_behavior": "ignore",
    "sfCompress": False,
    "parallelism": 8,
}
USER_WRITE_TEXTS = {"column_mismatch_behavior": "ignore", "sfCompress": "false", "parallelism": "8"}


@pytest.mark.parametrize("credential", ["conn", "oauth"])
@pytest.mark.parametrize("name", list(EVERY_CALL))
def test_at027_user_options_and_the_connection_options_reach_the_save(
    name: str,
    credential: str,
    log: _Log,
    conn: connectors.SnowflakeConnection,
    oauth: connectors.SnowflakeConnection,
    df: _FakeDataFrame,
) -> None:
    connection, connection_options = (
        (conn, CONN_OPTIONS) if credential == "conn" else (oauth, OAUTH_OPTIONS)
    )
    writer = connectors.SnowflakeWriter(TABLE, connection, options=USER_WRITE_OPTIONS)

    EVERY_CALL[name](writer, _frame(df))

    (save,) = _saves(log)
    truncate = TRUNCATE if _replaces(name) else {}
    assert _lower(save.options) == _lower(
        {**connection_options, **WRITE_OPTIONS, **truncate, **USER_WRITE_TEXTS}
    )


@pytest.mark.parametrize("name", list(EVERY_CALL))
def test_at028_a_failed_save_propagates_unchanged(
    name: str, log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    err = RuntimeError("COPY failed")
    failing = _FakeDataFrame(log, columns=COLUMNS, save_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        EVERY_CALL[name](connectors.SnowflakeWriter(TABLE, conn), _frame(failing))

    assert excinfo.value is err
    assert _notes(err) == []
    assert _transcript(log.events) == ["save"]


@pytest.mark.parametrize("name", list(EVERY_CALL))
def test_at029_a_dataframe_without_columns_is_rejected_before_any_save(
    name: str, log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    empty = _FakeDataFrame(log, columns=[])

    with pytest.raises(ValueError, match=_exactly("the DataFrame has no columns")):
        EVERY_CALL[name](connectors.SnowflakeWriter(TABLE, conn), _frame(empty))

    assert _saves(log) == []


def test_at030_the_writer_sends_the_table_as_given_and_no_column_name(
    log: _Log, conn: connectors.SnowflakeConnection
) -> None:
    table_name = '"Shop".public."Orders"'
    frame = _FakeDataFrame(log, columns=["Id", "my col", 'a"b'])

    connectors.SnowflakeWriter(table_name, conn).write(_frame(frame), replace=True)

    (save,) = _saves(log)
    assert _lower(save.options) == _lower(
        {**CONN_OPTIONS, **WRITE_OPTIONS, "dbtable": table_name, **TRUNCATE}
    )


def test_at031_a_write_looks_up_no_spark_session(
    log: _Log,
    conn: connectors.SnowflakeConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))

    connectors.SnowflakeWriter(TABLE, conn).write(_frame(df))

    assert lookups == []
    assert len(_saves(log)) == 1


# --- In a configuration -------------------------------------------------------------------


def test_at032_read_source_builds_the_reader_of_the_section_without_touching_spark(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
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


def test_at032_the_keys_of_the_section_and_the_arguments_reach_the_reader(
    log: _Log, spark: _FakeSpark, conn: connectors.SnowflakeConnection, tmp_path: Path
) -> None:
    section = {
        **DEFAULT_SOURCE,
        "watermark_column": "updated_at",
        "options": {"autopushdown": "off"},
    }
    path = _config(tmp_path, {"source": section})
    extract = ReadSource(path, connection=conn, watermark_value=41, spark=_session(spark))

    Process([Step("extract", [extract])]).run()

    twin_log = _Log()
    _reader(
        conn,
        _FakeSpark(twin_log),
        watermark_column="updated_at",
        watermark_value=41,
        options={"autopushdown": "off"},
    ).read()
    assert [entry[0] for entry in _shape(twin_log)] == ["load", "schema", "load", "first", "load"]
    assert _shape(log) == _shape(twin_log)


def test_at032_mypy_strict_accepts_a_snowflake_connection_in_the_callbacks_and_the_protocols(
    log: _Log, spark: _FakeSpark, tmp_path: Path
) -> None:
    """mypy strict, which checks `tests/`, accepts the annotated bindings and the calls of this
    test."""
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    conn: connectors.SnowflakeConnection = connectors.SnowflakeConnection(
        HOST, "shop", user="ingest", private_key=PEM
    )

    ReadSource(path, connection=conn)
    AppendOnly(path, connection=conn)
    FullLoad(path, connection=conn)
    SCD2(path, connection=conn)
    reader: Reader[DataFrame] = connectors.SnowflakeReader(TABLE, conn, spark=_session(spark))
    writer: Writer[DataFrame] = connectors.SnowflakeWriter(TABLE, conn)

    frame = reader.read()
    (load,) = _loads(log)
    assert frame is _frame(load.frame)
    assert isinstance(writer, connectors.SnowflakeWriter)


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at033_append_only_and_full_load_build_the_writer_and_choose_the_operation(
    name: str,
    conn: connectors.SnowflakeConnection,
    df: _FakeDataFrame,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"target": {**DEFAULT_TARGET, "options": {"parallelism": 8}}})
    callback = CALLBACKS[name](path, connection=conn)

    calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []

    def write(writer: object, *args: object, **kwargs: object) -> None:
        calls.append((writer, args, kwargs))

    monkeypatch.setattr(connectors.SnowflakeWriter, "write", write)
    Process([Step("load", [cast(Any, callback)])], state={"records": _frame(df)}).run()

    assert len(calls) == 1, calls
    ((writer, args, kwargs),) = calls
    twin = connectors.SnowflakeWriter(TABLE, conn, options={"parallelism": 8})
    _assert_twin(writer, twin, conn)
    assert any(value is conn for value in vars(writer).values()), vars(writer)
    assert len(args) == 1
    assert args[0] is df
    assert kwargs == ({"replace": True} if name == "FullLoad" else {})


# The rows of AT-034: an id, the section, the section's keys and the message after `<path>: `.
AT034_ROWS: list[tuple[str, str, dict[str, object], str]] = [
    (
        "source-without-table-name",
        "source",
        {"system": "snowflake"},
        "source.table_name is required for system 'snowflake'",
    ),
    (
        "target-without-table-name",
        "target",
        {"system": "snowflake"},
        "target.table_name is required for system 'snowflake'",
    ),
    *[
        (
            f"source-key-{key}",
            "source",
            {**DEFAULT_SOURCE, key: "h"},
            f"source.{key} is not a key of system 'snowflake'",
        )
        for key in (
            "host",
            "database",
            "user",
            "private_key",
            "oauth_token",
            "schema",
            "warehouse",
            "role",
            "connection",
            "watermark_value",
            "dbtable",
        )
    ],
    (
        "target-watermark-column",
        "target",
        {**DEFAULT_TARGET, "watermark_column": "updated_at"},
        "target.watermark_column is not a key of system 'snowflake'",
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
        {**DEFAULT_TARGET, "options": {"parallelism": [8]}},
        "target.options must be an object of strings, numbers and booleans",
    ),
]


@pytest.mark.parametrize(
    ("callback", "section", "keys", "message"),
    [
        pytest.param(callback, section, keys, message, id=f"{callback}-{name}")
        for name, section, keys, message in AT034_ROWS
        for callback in _section_callbacks(section)
    ],
)
def test_at034_a_snowflake_section_with_wrong_keys_is_rejected(
    callback: str,
    section: str,
    keys: dict[str, object],
    message: str,
    conn: connectors.SnowflakeConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {section: keys})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        CALLBACKS[callback](path, connection=conn)


def test_at035_read_source_needs_the_connection(tmp_path: Path) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    message = f"{path}: source system 'snowflake' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
def test_at035_append_only_and_full_load_need_the_connection(name: str, tmp_path: Path) -> None:
    path = _config(tmp_path, {"target": DEFAULT_TARGET})

    message = f"{path}: target system 'snowflake' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        CALLBACKS[name](path)


@pytest.mark.parametrize("name", ["ReadSource", "AppendOnly", "FullLoad"])
def test_at035_snowflake_takes_no_client(
    name: str, conn: connectors.SnowflakeConnection, tmp_path: Path
) -> None:
    section = "source" if name == "ReadSource" else "target"
    path = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET})

    message = f"{path}: {section} system 'snowflake' takes no argument client"
    with pytest.raises(ValueError, match=_exactly(message)):
        CALLBACKS[name](path, connection=conn, client=object())


def test_at035_arguments_given_as_none_are_not_given(
    conn: connectors.SnowflakeConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)


# The rows of AT-036: the callback, the file, the `connection` given and the message after
# `<path>: `. `connection` is `mysql`, `mongodb` or `postgres` for the connection of that system
# that the criterion gives, and `conn` for the criteria's `SnowflakeConnection`.
TO_SNOWFLAKE = "system 'snowflake' needs a SnowflakeConnection as the argument connection, not"
AT036_ROWS: list[tuple[str, dict[str, object], str, str]] = [
    ("ReadSource", {"source": DEFAULT_SOURCE}, "mysql", f"source {TO_SNOWFLAKE} MySQLConnection"),
    (
        "ReadSource",
        {"source": DEFAULT_SOURCE},
        "mongodb",
        f"source {TO_SNOWFLAKE} MongoDBConnection",
    ),
    (
        "AppendOnly",
        {"target": DEFAULT_TARGET},
        "postgres",
        f"target {TO_SNOWFLAKE} PostgresConnection",
    ),
    (
        "FullLoad",
        {"target": DEFAULT_TARGET},
        "postgres",
        f"target {TO_SNOWFLAKE} PostgresConnection",
    ),
    (
        "ReadSource",
        {"source": MYSQL_SECTION},
        "conn",
        "source system 'mysql' needs a MySQLConnection as the argument connection,"
        " not SnowflakeConnection",
    ),
    (
        "ReadSource",
        {"source": MONGODB_SECTION},
        "conn",
        "source system 'mongodb' needs a MongoDBConnection as the argument connection,"
        " not SnowflakeConnection",
    ),
    (
        "AppendOnly",
        {"target": POSTGRES_SECTION},
        "conn",
        "target system 'postgres' needs a PostgresConnection as the argument connection,"
        " not SnowflakeConnection",
    ),
    (
        "FullLoad",
        {"target": POSTGRES_SECTION},
        "conn",
        "target system 'postgres' needs a PostgresConnection as the argument connection,"
        " not SnowflakeConnection",
    ),
]


def _mysql() -> MySQLConnection:
    """The `MySQLConnection` of AT-036."""
    return MySQLConnection("db", "shop", "ingest", "pw")


def _given_connection(kind: str, conn: connectors.SnowflakeConnection) -> object:
    """The `connection` that a row of AT-036 gives."""
    if kind == "mysql":
        return _mysql()
    if kind == "mongodb":
        return MongoDBConnection("mongodb://db")
    if kind == "postgres":
        return PostgresConnection("db", "shop", "ingest", "pw")
    assert kind == "conn", kind
    return conn


def _at036_id(callback: str, document: dict[str, object], kind: str) -> str:
    (section,) = document.values()
    return f"{callback}-{cast('dict[str, object]', section)['system']}-{kind}"


@pytest.mark.parametrize(
    ("callback", "document", "kind", "message"),
    [pytest.param(*row, id=_at036_id(row[0], row[1], row[2])) for row in AT036_ROWS],
)
def test_at036_a_connection_of_another_systems_type_is_rejected(
    callback: str,
    document: dict[str, object],
    kind: str,
    message: str,
    conn: connectors.SnowflakeConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, document)

    with pytest.raises(ValueError) as excinfo:
        CALLBACKS[callback](path, connection=_given_connection(kind, conn))

    assert str(excinfo.value) == f"{path}: {message}"
    _assert_no_secret(str(excinfo.value), *_notes(excinfo.value))


def test_at036_a_subclass_of_snowflake_connection_is_accepted(tmp_path: Path) -> None:
    class _OwnConnection(connectors.SnowflakeConnection):
        """A job's own subclass of the connection."""

    path = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET})
    connection = _OwnConnection(HOST, "shop", user="ingest", private_key=PEM)

    ReadSource(path, connection=connection)
    AppendOnly(path, connection=connection)


def test_at037_a_source_system_close_to_snowflake_is_rejected_with_snowflake_in_the_list(
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": "Snowflake"}})

    message = f"{path}: source.system 'Snowflake' is not one of: {SOURCE_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


def test_at037_snowpipe_is_not_a_target_system_and_snowflake_is_in_the_list(
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"target": {**DEFAULT_TARGET, "system": "snowpipe"}})

    message = f"{path}: target.system 'snowpipe' is not one of: {TARGET_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        AppendOnly(path)


# The rows of AT-038: an id, the section, the keys added to its default, and the message of the
# error that the connector raises.
AT038_ROWS: list[tuple[str, str, dict[str, object], str]] = [
    ("four-part-table-name", "source", {"table_name": "a.b.c.d"}, TABLE_NAME_MESSAGE),
    (
        "watermark-column",
        "source",
        {"watermark_column": "a b"},
        "watermark_column must be a Snowflake identifier",
    ),
    (
        "reserved-read-option",
        "source",
        {"options": {"dbtable": "x"}},
        "option 'dbtable' is set by the connector",
    ),
    (
        "reserved-write-option",
        "target",
        {"options": {"preactions": "x"}},
        "option 'preactions' is set by the connector",
    ),
]


def _record_errors(
    connector: Any, monkeypatch: pytest.MonkeyPatch, raised: list[BaseException]
) -> None:
    """Make the constructor of `connector` add every error it raises to `raised`."""
    original = connector.__init__

    def init(self: object, *args: Any, **kwargs: Any) -> None:
        try:
            original(self, *args, **kwargs)
        except Exception as error:
            raised.append(error)
            raise

    monkeypatch.setattr(connector, "__init__", init)


@pytest.mark.parametrize(
    ("row", "callback"),
    [
        pytest.param(row, callback, id=f"{callback}-{row[0]}")
        for row in AT038_ROWS
        for callback in _section_callbacks(row[1])
    ],
)
def test_at038_an_error_of_the_connector_propagates_itself_with_one_note(
    row: tuple[str, str, dict[str, object], str],
    callback: str,
    conn: connectors.SnowflakeConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _, section, added, message = row
    default = DEFAULT_SOURCE if section == "source" else DEFAULT_TARGET
    path = _config(tmp_path, {section: {**default, **added}})
    connector = connectors.SnowflakeReader if section == "source" else connectors.SnowflakeWriter
    raised: list[BaseException] = []
    _record_errors(connector, monkeypatch, raised)

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        CALLBACKS[callback](path, connection=conn)

    assert len(raised) == 1, raised
    assert excinfo.value is raised[0]
    assert str(excinfo.value) == message
    assert _notes(excinfo.value) == [f"while building the {section} of {path}, system 'snowflake'"]


# --- SCD2 ---------------------------------------------------------------------------------


def test_at039_scd2_builds_with_a_snowflake_target_and_checks_its_connection(
    conn: connectors.SnowflakeConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, SCD2_FILE)

    SCD2(path, connection=conn)

    message = f"{path}: target system 'snowflake' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path)
    message = f"{path}: target {TO_SNOWFLAKE} MySQLConnection"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path, connection=_mysql())


def test_at039_scd2_lists_snowflake_among_the_targets_it_supports(tmp_path: Path) -> None:
    target = {"system": "disk", "path": "/data/dim", "format": "parquet"}
    path = _config(tmp_path, {**SCD2_FILE, "target": target})

    message = f"{path}: target.system 'disk' is not one of: {SCD2_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        SCD2(path)


def test_at040_scd2_reads_a_snowflake_target_with_the_batchs_session(
    conn: connectors.SnowflakeConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    err = RuntimeError("stop")
    readers: list[object] = []
    writes: list[object] = []

    def read(reader: object) -> NoReturn:
        readers.append(reader)
        raise err

    def write(writer: object, *args: object, **kwargs: object) -> None:
        writes.append(writer)

    monkeypatch.setattr(connectors.SnowflakeReader, "read", read)
    monkeypatch.setattr(connectors.SnowflakeWriter, "write", write)
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
    twin = connectors.SnowflakeReader("shop.public.dim_customer", conn, spark=_session(session))
    _assert_twin(readers[0], twin, session, conn)
    assert writes == []


# --- Logging and secrets ------------------------------------------------------------------


def test_at041_nothing_writes_a_log_record(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
    oauth: connectors.SnowflakeConnection,
    df: _FakeDataFrame,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    source = {
        **DEFAULT_SOURCE,
        "watermark_column": "updated_at",
        "options": {"autopushdown": "off"},
    }
    sliced = _config(tmp_path, {"source": source}, "sliced.json")

    for _, args, kwargs in AT004_ACCEPTED:
        connectors.SnowflakeConnection(*args, **cast(Any, kwargs))
    for credential in (conn, oauth):
        ReadSource(path, connection=credential, spark=_session(spark))
        ReadSource(sliced, connection=credential, watermark_value=41, spark=_session(spark))
        AppendOnly(path, connection=credential)
        FullLoad(path, connection=credential)
        SCD2(path, connection=credential)
    with pytest.raises(ValueError):
        ReadSource(path)
    with pytest.raises(ValueError):
        AppendOnly(path, connection=_mysql())
    with pytest.raises(ValueError):
        ReadSource(path, connection=conn, client=object())
    reserved = _config(tmp_path, {"target": {**DEFAULT_TARGET, "options": {"preactions": "x"}}})
    with pytest.raises(ValueError):
        FullLoad(reserved, connection=conn)
    SCD2(_config(tmp_path, SCD2_FILE, "dim.json"), connection=conn)

    _reader(conn, spark).read()
    _reader(conn, spark, watermark_column="updated_at", watermark_value=41).read()
    no_bound = _FakeSpark(log, rows=[None])
    _reader(conn, no_bound, watermark_column="updated_at").read()

    writer = connectors.SnowflakeWriter(TABLE, conn, options={"parallelism": 8})
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)
    failing = _FakeDataFrame(log, columns=COLUMNS, save_error=RuntimeError("COPY failed"))
    with pytest.raises(RuntimeError):
        writer.write(_frame(failing))
    with pytest.raises(RuntimeError):
        writer.write(_frame(failing), replace=True)

    assert len(_loads(log)) == 7
    assert len(_saves(log)) == 4
    assert caplog.records == []


def _raised(
    errors: list[BaseException], build: Callable[..., object], *args: Any, **kwargs: Any
) -> None:
    """Call `build` with the arguments, which must raise, and add its error to `errors`."""
    try:
        build(*args, **kwargs)
    except (ValueError, TypeError, RuntimeError) as error:
        errors.append(error)
    else:
        raise AssertionError(f"{build} raised nothing")


def _criteria_errors(
    log: _Log,
    connections: Sequence[connectors.SnowflakeConnection],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> list[BaseException]:
    """The errors that AT-004 to AT-040 raise, raised once more, with each of `connections`
    where a criterion gives one."""
    errors: list[BaseException] = []
    snowflake = connectors.SnowflakeConnection
    for _, args, kwargs, _ in AT004_REJECTED:
        _raised(errors, snowflake, *args, **kwargs)
    for _, text, _ in AT005_REJECTED:
        _raised(errors, snowflake, HOST, "shop", user="ingest", private_key=text)
    _raised(errors, cast(Any, snowflake), "h", "shop", "ingest")

    reader = connectors.SnowflakeReader
    for connection in connections:
        for kind in ("reader", "writer"):
            for name in AT009_REJECTED:
                _raised(errors, _build, kind, name, connection)
            for key in RESERVED_BY_BOTH:
                _raised(errors, _build, kind, TABLE, connection, options={key: "x"})
        for key in RESERVED_BY_THE_WRITER:
            _raised(errors, _build, "writer", TABLE, connection, options={key: "x"})
        _raised(errors, reader, TABLE, connection, watermark_value=41)
        _raised(errors, reader, TABLE, connection, watermark_column="")
        for column in AT010_REJECTED:
            _raised(errors, reader, TABLE, connection, watermark_column=column)
        for _, value, _ in AT011_REJECTED:
            _raised(errors, reader, TABLE, connection, watermark_column="u", watermark_value=value)

        down = _FakeSpark(log, load_error=RuntimeError("down"))
        _raised(errors, _reader(connection, down).read)
        schema_down = _FakeSpark(log, schema_error=RuntimeError("down"))
        _raised(errors, _reader(connection, schema_down, watermark_column="updated_at").read)
        first_down = _FakeSpark(log, first_error=RuntimeError("down"))
        _raised(errors, _reader(connection, first_down, watermark_column="updated_at").read)
        writer = connectors.SnowflakeWriter(TABLE, connection)
        failing = _FakeDataFrame(log, columns=COLUMNS, save_error=RuntimeError("COPY failed"))
        _raised(errors, writer.write, _frame(failing))
        _raised(errors, writer.write, _frame(failing), replace=True)
        _raised(errors, writer.write, _frame(_FakeDataFrame(log, columns=[])))

        for number, (_, section, keys, _) in enumerate(AT034_ROWS):
            path = _config(tmp_path, {section: keys}, f"at034-{number}.json")
            for callback in _section_callbacks(section):
                _raised(errors, CALLBACKS[callback], path, connection=connection)
        both = _config(tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET}, "both.json")
        for callback in ("ReadSource", "AppendOnly", "FullLoad"):
            _raised(errors, CALLBACKS[callback], both)
            _raised(errors, CALLBACKS[callback], both, connection=connection, client=object())
        for number, (callback, document, kind, _) in enumerate(AT036_ROWS):
            path = _config(tmp_path, document, f"at036-{number}.json")
            given = _given_connection(kind, connection)
            _raised(errors, CALLBACKS[callback], path, connection=given)
        for name, system in (("source", "Snowflake"), ("target", "snowpipe")):
            path = _config(tmp_path, {name: {**DEFAULT_SOURCE, "system": system}}, "close.json")
            _raised(errors, CALLBACKS[_section_callbacks(name)[0]], path, connection=connection)
        for number, (_, section, added, _) in enumerate(AT038_ROWS):
            default = DEFAULT_SOURCE if section == "source" else DEFAULT_TARGET
            path = _config(tmp_path, {section: {**default, **added}}, f"at038-{number}.json")
            for callback in _section_callbacks(section):
                _raised(errors, CALLBACKS[callback], path, connection=connection)

        dim = _config(tmp_path, SCD2_FILE, "dim.json")
        _raised(errors, SCD2, dim)
        _raised(errors, SCD2, dim, connection=_mysql())
        disk = {"system": "disk", "path": "/data/dim", "format": "parquet"}
        _raised(errors, SCD2, _config(tmp_path, {**SCD2_FILE, "target": disk}, "disk.json"))
        with monkeypatch.context() as patched:

            def read(reader: object) -> NoReturn:
                raise RuntimeError("stop")

            patched.setattr(connectors.SnowflakeReader, "read", read)
            process = Process(
                [Step("load", [SCD2(dim, connection=connection)])],
                state={"records": _FakeBatch(BATCH_COLUMNS, _FakeSession())},
            )
            _raised(errors, process.run)
    return errors


def test_at042_neither_the_key_nor_the_token_appears_in_any_repr_or_error(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.SnowflakeConnection,
    oauth: connectors.SnowflakeConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _config(
        tmp_path, {"source": DEFAULT_SOURCE, "target": DEFAULT_TARGET, "scd2": SCD2_SECTION}
    )
    connections = [
        conn,
        oauth,
        *[
            connectors.SnowflakeConnection(HOST, "shop", **cast(Any, kwargs))
            for _, kwargs, _ in AT008_ROWS
        ],
        *[
            connectors.SnowflakeConnection(HOST, "shop", user="ingest", private_key=text)
            for _, text in AT005_ACCEPTED
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
                options={"autopushdown": "off"},
            )
        )
        built.append(connectors.SnowflakeWriter(TABLE, connection))
        built.append(connectors.SnowflakeWriter("orders", connection, options={"parallelism": 8}))
        built.extend(
            [
                ReadSource(path, connection=connection, spark=_session(spark)),
                AppendOnly(path, connection=connection),
                FullLoad(path, connection=connection),
                SCD2(path, connection=connection),
            ]
        )
    texts = [text for value in built for text in (repr(value), str(value))]

    errors = _criteria_errors(log, [conn, oauth], monkeypatch, tmp_path)
    assert errors
    messages = [text for error in errors for text in (str(error), *_notes(error))]

    _assert_no_secret(*texts, *messages)


# --- Integration --------------------------------------------------------------------------

SNOWFLAKE_PACKAGES = os.environ.get("INGESTIO_SNOWFLAKE_PACKAGES", "")
ACCOUNT_VARIABLES = (
    "INGESTIO_SNOWFLAKE_HOST",
    "INGESTIO_SNOWFLAKE_USER",
    "INGESTIO_SNOWFLAKE_PRIVATE_KEY",
    "INGESTIO_SNOWFLAKE_DATABASE",
    "INGESTIO_SNOWFLAKE_SCHEMA",
    "INGESTIO_SNOWFLAKE_WAREHOUSE",
)


def _unmet() -> list[str]:
    """Why the integration tests cannot run here, one reason per missing setting."""
    reasons: list[str] = []
    if os.environ.get("INGESTIO_SPARK_TESTS") != "1":
        reasons.append(
            "INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in"
        )
    if not SNOWFLAKE_PACKAGES:
        reasons.append(
            "INGESTIO_SNOWFLAKE_PACKAGES is not set: it names the Maven coordinates of the"
            " Snowflake Connector for Spark and of the Snowflake JDBC driver, separated by a"
            " comma, such as net.snowflake:spark-snowflake_<Scala version>:<version>,"
            "net.snowflake:snowflake-jdbc:<version>"
        )
    missing = [name for name in ACCOUNT_VARIABLES if not os.environ.get(name)]
    if missing:
        reasons.append(
            f"{', '.join(missing)} not set: they name the Snowflake account to run against, its"
            " key-pair user and private key (PEM text), and the database, schema and warehouse"
            " of the tests (INGESTIO_SNOWFLAKE_ROLE too, when the account needs one)"
        )
    return reasons


UNMET = _unmet()
opt_in = pytest.mark.skipif(bool(UNMET), reason="; ".join(UNMET) or "every setting is present")


def _pem_body(text: str) -> str:
    """The base64 body of a PEM text, for the test's own statements: its lines between the
    armour lines, joined."""
    lines = [line.strip() for line in text.strip().splitlines()]
    return "".join(line for line in lines if line and not line.startswith("-----"))


def _sql_text(text: str) -> str:
    """`text` as a single-quoted Snowflake string constant, for the test's own statements."""
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


class _Account:
    """The Snowflake account of the `INGESTIO_SNOWFLAKE_*` variables, for one test.

    Its statements run through the connector's `Utils.runQuery`, over py4j, outside the
    connectors under test. It drops the tables that `table` names, and the schemas that
    `quoted_schema` names, when the test ends.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        environ = os.environ
        host = environ["INGESTIO_SNOWFLAKE_HOST"]
        user = environ["INGESTIO_SNOWFLAKE_USER"]
        private_key = environ["INGESTIO_SNOWFLAKE_PRIVATE_KEY"]
        self.database = environ["INGESTIO_SNOWFLAKE_DATABASE"]
        self.schema = environ["INGESTIO_SNOWFLAKE_SCHEMA"]
        warehouse = environ["INGESTIO_SNOWFLAKE_WAREHOUSE"]
        role = environ.get("INGESTIO_SNOWFLAKE_ROLE") or None
        self.connection = connectors.SnowflakeConnection(
            host,
            self.database,
            user=user,
            private_key=private_key,
            schema=self.schema,
            warehouse=warehouse,
            role=role,
        )
        self._options = {
            "sfURL": host,
            "sfDatabase": self.database,
            "sfSchema": self.schema,
            "sfWarehouse": warehouse,
            "sfUser": user,
            "pem_private_key": _pem_body(private_key),
        }
        if role is not None:
            self._options["sfRole"] = role
        self._drops: list[str] = []

    def _run(self, statement: str) -> Any:
        jvm = cast(Any, self._spark.sparkContext)._jvm
        return jvm.net.snowflake.spark.snowflake.Utils.runQuery(self._options, statement)

    def execute(self, statement: str) -> None:
        self._run(statement)

    def rows(self, query: str) -> list[tuple[str | None, ...]]:
        """The rows of `query`, each value as its text."""
        result = self._run(query)
        width = int(result.getMetaData().getColumnCount())
        rows: list[tuple[str | None, ...]] = []
        while result.next():
            rows.append(tuple(result.getString(index) for index in range(1, width + 1)))
        return rows

    def table(self, stem: str) -> str:
        """A new table name, `INGESTIO_IT_<STEM>_<HEX>`, unquoted, dropped when the test ends."""
        name = f"INGESTIO_IT_{stem.upper()}_{uuid.uuid4().hex[:12].upper()}"
        self._drops.append(f"DROP TABLE IF EXISTS {self.qualified(name)}")
        return name

    def quoted_schema(self, stem: str) -> str:
        """A new schema name, `"<stem> <hex>"`, quoted, dropped when the test ends."""
        name = f'"{stem} {uuid.uuid4().hex[:12]}"'
        self._drops.append(f"DROP SCHEMA IF EXISTS {self.database}.{name} CASCADE")
        return name

    def qualified(self, name: str) -> str:
        """`name` in the schema of the tests, as SQL writes it."""
        return f"{self.database}.{self.schema}.{name}"

    def exists(self, name: str) -> bool:
        """Whether the schema of the tests holds a table named `name`."""
        found = self.rows(f"SHOW TABLES LIKE '{name}' IN SCHEMA {self.database}.{self.schema}")
        return bool(found)

    def close(self) -> None:
        for statement in reversed(self._drops):
            self.execute(statement)


@pytest.fixture(scope="module")
def local_spark() -> Iterator[SparkSession]:
    """A local Spark session in UTC, with the Snowflake Connector for Spark and its driver."""
    if SparkContext._gateway is not None:
        pytest.skip(
            "a JVM was already started in this run, without the Snowflake Connector for Spark:"
            " a JVM loads its packages once, so run this file in its own process"
        )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration-snowflake")
        .config("spark.jars.packages", SNOWFLAKE_PACKAGES)
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


@pytest.fixture
def account(local_spark: SparkSession) -> Iterator[_Account]:
    """The account of the `INGESTIO_SNOWFLAKE_*` variables; the test's tables are dropped."""
    snowflake = _Account(local_spark)
    try:
        yield snowflake
    finally:
        snowflake.close()


def _ids(frame: DataFrame) -> list[int]:
    """The sorted values of the `id` column of `frame`, whatever the case of its name."""
    return sorted(int(row[0]) for row in frame.select("id").collect())


@pytest.mark.integration
@opt_in
def test_snowflake_a_full_read_returns_every_row_of_every_type(
    account: _Account, local_spark: SparkSession
) -> None:
    name = account.table("full")
    table = account.qualified(name)
    account.execute(
        f"CREATE TABLE {table} (ID NUMBER(38,0), AMOUNT NUMBER(10,2), NOTE VARCHAR, DAY DATE,"
        " NTZ TIMESTAMP_NTZ(9), LTZ TIMESTAMP_LTZ(9), TZ TIMESTAMP_TZ(9), DOC VARIANT)"
    )
    account.execute(
        f"INSERT INTO {table}"
        " SELECT 1, 12.50, 'caf\u00e9', '2026-10-01', '2026-10-01 08:30:00.123456789',"
        " '2026-10-01 08:30:00.123456789 +00:00', '2026-10-01 08:30:00.123456789 -03:00',"
        " PARSE_JSON('{\"a\": 1}')"
        " UNION ALL SELECT 2, -3.00, 'plain', '2026-10-02', '2026-10-02 23:59:59.5',"
        " '2026-10-02 23:59:59.5 +00:00', '2026-10-02 23:59:59.5 +05:30', PARSE_JSON('[1, 2]')"
        " UNION ALL SELECT 3, NULL, NULL, NULL, NULL, NULL, NULL, NULL"
    )

    frame = connectors.SnowflakeReader(table, account.connection, spark=local_spark).read()

    rows = frame.select("id", "amount", "note", "day", "ntz", "ltz", "tz", "doc").collect()
    assert sorted(
        (
            int(row[0]),
            row[1],
            row[2],
            row[3],
            row[4] is None,
            row[5] is None,
            row[6] is None,
            None if row[7] is None else json.loads(row[7]),
        )
        for row in rows
    ) == [
        (1, Decimal("12.50"), "caf\u00e9", date(2026, 10, 1), False, False, False, {"a": 1}),
        (2, Decimal("-3.00"), "plain", date(2026, 10, 2), False, False, False, [1, 2]),
        (3, None, None, None, True, True, True, None),
    ]


@pytest.mark.integration
@opt_in
def test_snowflake_a_watermark_read_on_a_number_above_two_to_the_53_is_exact_and_frozen(
    account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("number"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), W NUMBER(38,0))")
    account.execute(
        f"INSERT INTO {table} VALUES (1, 9007199254740992), (2, 9007199254740993),"
        " (3, 9007199254740994), (4, NULL)"
    )

    frame = connectors.SnowflakeReader(
        table,
        account.connection,
        watermark_column="w",
        watermark_value=9007199254740992,
        spark=local_spark,
    ).read()

    assert _ids(frame) == [2, 3]
    account.execute(f"INSERT INTO {table} VALUES (5, 9007199254740995), (6, NULL)")
    assert _ids(frame) == [2, 3]
    first_run = connectors.SnowflakeReader(
        table, account.connection, watermark_column="w", spark=local_spark
    ).read()
    assert _ids(first_run) == [1, 2, 3, 5]


@pytest.mark.integration
@opt_in
@pytest.mark.parametrize("column_type", ["TIMESTAMP_NTZ(9)", "TIMESTAMP_LTZ(9)", "TIMESTAMP_TZ(9)"])
def test_snowflake_a_watermark_read_with_an_aware_datetime_is_exact_to_the_nanosecond(
    column_type: str, account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("temporal"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), W {column_type})")
    account.execute(
        f"INSERT INTO {table} VALUES (1, '2026-10-02T00:30:00+00:00'),"
        " (2, '2026-10-02T01:00:00+00:00'), (3, '2026-10-02T01:00:00.000000001+00:00'),"
        " (4, '2026-10-02T02:00:00+00:00'), (5, NULL)"
    )
    # 22:00 at -03:00 is 01:00 UTC on 2026-10-02: only the rows after it are above the value.
    value = datetime(2026, 10, 1, 22, 0, tzinfo=timezone(timedelta(hours=-3)))

    frame = connectors.SnowflakeReader(
        table, account.connection, watermark_column="w", watermark_value=value, spark=local_spark
    ).read()

    assert _ids(frame) == [3, 4]
    account.execute(f"INSERT INTO {table} VALUES (6, '2026-10-02T02:00:00.000000001+00:00')")
    assert _ids(frame) == [3, 4]


@pytest.mark.integration
@opt_in
def test_snowflake_a_watermark_read_on_text_with_quotes_and_backslashes_is_exact(
    account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("text"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), W VARCHAR)")
    values = ["a'\\a", "b'\\b", "c'\\c", "d'\\d"]
    rows = ", ".join(f"({number}, {_sql_text(text)})" for number, text in enumerate(values, 1))
    account.execute(f"INSERT INTO {table} VALUES {rows}")

    frame = connectors.SnowflakeReader(
        table,
        account.connection,
        watermark_column="w",
        watermark_value=values[1],
        spark=local_spark,
    ).read()

    assert sorted((int(row[0]), row[1]) for row in frame.select("id", "w").collect()) == [
        (3, values[2]),
        (4, values[3]),
    ]


@pytest.mark.integration
@opt_in
def test_snowflake_quoted_and_unquoted_names_are_read_as_sql_writes_them(
    account: _Account, local_spark: SparkSession
) -> None:
    schema = account.quoted_schema("Mixed Case")
    account.execute(f"CREATE SCHEMA {account.database}.{schema}")
    quoted = f'{account.database}.{schema}."O""Brien"'
    account.execute(f"CREATE TABLE {quoted} (ID NUMBER(38,0))")
    account.execute(f"INSERT INTO {quoted} VALUES (1), (2)")
    lower = account.table("lower")
    account.execute(f"CREATE TABLE {account.qualified(lower)} (ID NUMBER(38,0))")
    account.execute(f"INSERT INTO {account.qualified(lower)} VALUES (3)")

    mixed = connectors.SnowflakeReader(
        f'{schema}."O""Brien"', account.connection, spark=local_spark
    ).read()
    folded = connectors.SnowflakeReader(lower.lower(), account.connection, spark=local_spark).read()

    assert _ids(mixed) == [1, 2]
    assert _ids(folded) == [3]


@pytest.mark.integration
@opt_in
def test_snowflake_write_appends_by_column_name_and_needs_every_column_of_the_target(
    account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("append"))
    account.execute(
        f"CREATE TABLE {table} (ID NUMBER(38,0), NAME VARCHAR, STATUS VARCHAR DEFAULT 'new')"
    )
    account.execute(f"INSERT INTO {table} VALUES (1, 'old', 'loaded')")
    select = f"SELECT ID, NAME, STATUS FROM {table} ORDER BY ID"

    reordered = local_spark.createDataFrame([("x", "a", 2)], "status STRING, name STRING, id LONG")
    connectors.SnowflakeWriter(table, account.connection).write(reordered)
    assert account.rows(select) == [("1", "old", "loaded"), ("2", "a", "x")]

    missing = local_spark.createDataFrame([(3, "b")], "id LONG, name STRING")
    with pytest.raises(Exception, match=r"(?i)column number"):
        connectors.SnowflakeWriter(table, account.connection).write(missing)
    assert account.rows(select) == [("1", "old", "loaded"), ("2", "a", "x")]

    ignoring = connectors.SnowflakeWriter(
        table, account.connection, options={"column_mismatch_behavior": "ignore"}
    )
    ignoring.write(missing)
    assert account.rows(select) == [("1", "old", "loaded"), ("2", "a", "x"), ("3", "b", "new")]


@pytest.mark.integration
@opt_in
def test_snowflake_write_with_replace_leaves_exactly_the_rows_of_the_dataframe(
    account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("replace"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), NAME VARCHAR)")
    account.execute(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    writer = connectors.SnowflakeWriter(table, account.connection)
    select = f"SELECT ID, NAME FROM {table} ORDER BY ID"

    writer.write(
        local_spark.createDataFrame([(10, "x"), (11, "y")], "id LONG, name STRING"), replace=True
    )
    assert account.rows(select) == [("10", "x"), ("11", "y")]

    writer.write(local_spark.createDataFrame([], "id LONG, name STRING"), replace=True)
    assert account.rows(select) == []


@pytest.mark.integration
@opt_in
@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_snowflake_a_write_to_a_missing_target_fails_and_creates_nothing(
    replace: bool, account: _Account, local_spark: SparkSession
) -> None:
    name = account.table("missing")
    frame = local_spark.createDataFrame([(1, "a")], "id LONG, name STRING")

    with pytest.raises(Exception, match=r"(?i)does not exist"):
        connectors.SnowflakeWriter(account.qualified(name), account.connection).write(
            frame, replace=replace
        )

    assert not account.exists(name)


@pytest.mark.integration
@opt_in
@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_snowflake_a_value_the_target_cannot_hold_fails_the_copy_and_changes_nothing(
    replace: bool, account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("length"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), CODE VARCHAR(3))")
    account.execute(f"INSERT INTO {table} VALUES (1, 'abc')")
    frame = local_spark.createDataFrame([(2, "toolong")], "id LONG, code STRING")

    with pytest.raises(Exception, match=r"(?i)too long|length limit|exceeded"):
        connectors.SnowflakeWriter(table, account.connection).write(frame, replace=replace)

    assert account.rows(f"SELECT ID, CODE FROM {table}") == [("1", "abc")]


@pytest.mark.integration
@opt_in
def test_snowflake_a_column_the_target_lacks_fails_and_changes_nothing(
    account: _Account, local_spark: SparkSession
) -> None:
    table = account.qualified(account.table("columns"))
    account.execute(f"CREATE TABLE {table} (ID NUMBER(38,0), NAME VARCHAR)")
    account.execute(f"INSERT INTO {table} VALUES (1, 'a')")
    frame = local_spark.createDataFrame([(2, "b", "x")], "id LONG, name STRING, extra STRING")

    with pytest.raises(Exception, match=r"(?i)column number|can't find column"):
        connectors.SnowflakeWriter(table, account.connection).write(frame)

    assert account.rows(f"SELECT ID, NAME FROM {table}") == [("1", "a")]


@pytest.mark.integration
@opt_in
def test_snowflake_scd2_keeps_the_instants_of_the_versions_it_does_not_change(
    account: _Account, local_spark: SparkSession, tmp_path: Path
) -> None:
    table = account.qualified(account.table("dim"))
    account.execute(
        f"CREATE TABLE {table} (CUSTOMER_ID NUMBER(38,0), NAME VARCHAR, CITY VARCHAR,"
        " UPDATED_AT TIMESTAMP_LTZ(9), VALID_FROM TIMESTAMP_LTZ(9), VALID_TO TIMESTAMP_LTZ(9))"
    )
    account.execute(
        f"INSERT INTO {table} VALUES"
        " (1, 'Ana', 'Faro', '2025-01-01T08:00:00.123456-03:00',"
        " '2025-01-01T08:00:00.123456-03:00', '2026-01-01T08:00:00.654321-03:00'),"
        " (1, 'Ana', 'Lisbon', '2026-01-01T08:00:00.654321-03:00',"
        " '2026-01-01T08:00:00.654321-03:00', NULL),"
        " (2, 'Bruno', 'Porto', '2026-02-01T23:30:00.25+05:30',"
        " '2026-02-01T23:30:00.25+05:30', NULL)"
    )
    path = _config(
        tmp_path, {"target": {"system": "snowflake", "table_name": table}, "scd2": SCD2_SECTION}
    )
    batch = local_spark.createDataFrame(
        [(Decimal(1), "Ana", "Braga", datetime(2026, 3, 1, 12, 0, tzinfo=UTC))],
        "customer_id DECIMAL(38,0), name STRING, city STRING, updated_at TIMESTAMP",
    )

    Process(
        [Step("load", [SCD2(path, connection=account.connection)])], state={"records": batch}
    ).run()

    utc = "TO_VARCHAR(CONVERT_TIMEZONE('UTC', {}), 'YYYY-MM-DD HH24:MI:SS.FF6')"
    assert account.rows(
        f"SELECT CUSTOMER_ID, CITY, {utc.format('UPDATED_AT')}, {utc.format('VALID_FROM')},"
        f" {utc.format('VALID_TO')} FROM {table} ORDER BY CUSTOMER_ID, VALID_FROM"
    ) == [
        (
            "1",
            "Faro",
            "2025-01-01 11:00:00.123456",
            "2025-01-01 11:00:00.123456",
            "2026-01-01 11:00:00.654321",
        ),
        (
            "1",
            "Lisbon",
            "2026-01-01 11:00:00.654321",
            "2026-01-01 11:00:00.654321",
            "2026-03-01 12:00:00.000000",
        ),
        ("1", "Braga", "2026-03-01 12:00:00.000000", "2026-03-01 12:00:00.000000", None),
        ("2", "Porto", "2026-02-01 18:00:00.250000", "2026-02-01 18:00:00.250000", None),
    ]
