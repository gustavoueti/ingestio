"""MongoDB reader: `MongoDBConnection`, `MongoDBReader` and the `mongodb` source system.

Covers `specs/mongodb_reader.md`. Every test runs without a JVM, a network or a MongoDB server:
the spec has no integration tests (its decision 18). The doubles below, a fake Spark session and
fake DataFrames, write to one ordered event log. Reading `columns` on a fake DataFrame is an
event of that log, so that the reads of an empty schema can be observed. Configurations are
written with `json.dumps` to a file under `tmp_path`, and a missing pyspark is simulated in a
fresh interpreter.

`MongoDBConnection` and `MongoDBReader` are reached as attributes of `ingestio.connector.all`,
the module the spec exports them from.

The connection strings carry the user `u53r-tok` and the password `p455-tok`, tokens that appear
nowhere else, so that a test can check that a text holds neither.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeAlias, cast

import pytest
from pyspark.sql import DataFrame, SparkSession

import ingestio.connector.all as connectors
from ingestio import SCD2, AppendOnly, FullLoad, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection, Reader

USER = "u53r-tok"
PASSWORD = "p455-tok"
URI = "mongodb+srv://u53r-tok:p455-tok@cluster0.example.net/?retryWrites=true&w=majority"
COLUMNS = ("_id", "total", "updatedAt")
BOUND_ROW: tuple[object, ...] = ("2026-10-21 09:30:00.5", "timestamp")
UPPER = "CAST('2026-10-21 09:30:00.5' AS timestamp)"
UPDATED_AT = "`updatedAt`"
OCT_1 = datetime(2026, 10, 1, tzinfo=UTC)
MINUS_3 = timezone(timedelta(hours=-3))
READER_REPR = "MongoDBReader(database='shop', collection='orders', hosts='cluster0.example.net')"

# The options of every load of the default reader, with their keys in the case the spec gives.
BASE_OPTIONS = {"connection.uri": URI, "database": "shop", "collection": "orders"}

DEFAULT_SOURCE: dict[str, object] = {
    "system": "mongodb",
    "database": "shop",
    "collection": "orders",
}
POSTGRES_SECTION: dict[str, object] = {"system": "postgres", "table_name": "public.orders"}
SCD2_SECTION: dict[str, object] = {"key_columns": ["id"], "effective_time_column": "updated_at"}
SOURCE_NAMES = "bigquery, disk, gcs, json_api, mongodb, mysql, postgres, s3, unity_catalog, volume"
TARGET_NAMES = "bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume"

SCHEME = "uri must start with 'mongodb://' or 'mongodb+srv://'"
AT_AFTER_HOSTS = "uri has an unencoded '@' after its hosts"
NO_HOST = "uri names no host"
PAGINATE_BY_SIZE = "com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner"


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read` of `spark`.

    It records the format or `None`, the merged options and the arguments of `load()`. `frame`
    is the DataFrame that `load()` returned: the source.
    """

    source: str | None
    options: dict[str, str]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    frame: _FakeDataFrame = field(repr=False, compare=False)
    spark: _FakeSpark = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Schema:
    """`schema()` ran on a fake `spark.read`, with this argument."""

    schema: object


@dataclass(frozen=True)
class _Columns:
    """`columns` was read on `on`."""

    on: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Where:
    """`where()` or `filter()` ran on `on`, with this condition, and returned `result`."""

    condition: object
    method: str
    on: _FakeDataFrame = field(repr=False, compare=False)
    result: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Select:
    """`selectExpr()` ran on `on`, with these expressions, and returned `result`."""

    expressions: tuple[object, ...]
    on: _FakeDataFrame = field(repr=False, compare=False)
    result: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _RowAccess:
    """`first()`, `head()`, `collect()` or `take()` ran on `on`."""

    method: str
    on: _FakeDataFrame = field(repr=False, compare=False)


_Event: TypeAlias = _Load | _Schema | _Columns | _Where | _Select | _RowAccess


class _Log:
    """The ordered event log every double of one session writes to.

    `spark_attributes` holds every public attribute accessed on the fake session. Each fake
    DataFrame takes a label, `df#<n>`, in the order the DataFrames of the log are created, so
    that two sessions used in the same way give the same labels.
    """

    def __init__(self) -> None:
        self.events: list[_Event] = []
        self.spark_attributes: list[str] = []
        self._frames = 0

    def label(self) -> str:
        self._frames += 1
        return f"df#{self._frames}"


class _Answers:
    """What the bound queries of one fake session answer.

    The n-th `selectExpr` answers the n-th row, and the last row once the rows run out. When
    `row_error` is set, every row access on the result of a `selectExpr` raises it once
    recorded.
    """

    def __init__(self, rows: Sequence[tuple[object, ...]], row_error: BaseException | None) -> None:
        assert rows, "a session needs at least one bound row"
        self._rows = list(rows)
        self._selects = 0
        self.row_error = row_error

    def next_row(self) -> tuple[object, ...]:
        row = self._rows[min(self._selects, len(self._rows) - 1)]
        self._selects += 1
        return row


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

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does. Each `load()`
    returns a new `_FakeDataFrame` with `columns`, and raises `load_error` once recorded, when it
    is set. The bound queries answer `bound_rows`, as `_Answers` describes.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = COLUMNS,
        bound_rows: Sequence[tuple[object, ...]] = (BOUND_ROW,),
        load_error: BaseException | None = None,
        row_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
        self._answers = _Answers(bound_rows, row_error)
        self._load_error = load_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self)


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_Load` at each `load()`, a `_Schema` at `schema()`."""

    def __init__(self, spark: _FakeSpark) -> None:
        self._spark = spark
        self._log = spark._log
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

    def schema(self, schema: object) -> _FakeDataFrameReader:
        self._log.events.append(_Schema(schema))
        return self

    def load(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(
            self._log, columns=self._spark._columns, answers=self._spark._answers
        )
        self._log.events.append(
            _Load(self._source, dict(self._options), args, dict(kwargs), frame, self._spark)
        )
        if self._spark._load_error is not None:
            raise self._spark._load_error
        return frame


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    Reading `columns` records a `_Columns`. `where`, `filter` and `selectExpr` record their call
    and return a new `_FakeDataFrame`. The DataFrame that `selectExpr` returns answers `first()`,
    `head()`, `collect()` and `take(n)` with the single row its session chose; every row access
    is recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        answers: _Answers | None = None,
        bound_row: tuple[object, ...] | None = None,
    ) -> None:
        self._log = log
        self._label = log.label()
        self._columns = list(columns)
        self._answers = answers
        self._bound_row = bound_row
        self._accessed: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def columns(self) -> list[str]:
        self._log.events.append(_Columns(self))
        return list(self._columns)

    def where(self, condition: object) -> _FakeDataFrame:
        return self._where("where", condition)

    def filter(self, condition: object) -> _FakeDataFrame:
        return self._where("filter", condition)

    def selectExpr(self, *expr: object) -> _FakeDataFrame:  # noqa: N802 - PySpark's name
        # PySpark takes the expressions one by one, or as a single list.
        expressions = expr[0] if len(expr) == 1 and isinstance(expr[0], list) else expr
        row = self._answers.next_row() if self._answers is not None else None
        result = _FakeDataFrame(self._log, answers=self._answers, bound_row=row)
        self._log.events.append(_Select(tuple(expressions), self, result))
        return result

    def first(self) -> tuple[object, ...]:
        return self._row("first")

    def head(self, n: int | None = None) -> tuple[object, ...] | list[tuple[object, ...]]:
        row = self._row("head")
        return row if n is None else [row][:n]

    def collect(self) -> list[tuple[object, ...]]:
        return [self._row("collect")]

    def take(self, num: int) -> list[tuple[object, ...]]:
        return [self._row("take")][:num]

    def _where(self, method: str, condition: object) -> _FakeDataFrame:
        result = _FakeDataFrame(self._log, columns=self._columns, answers=self._answers)
        self._log.events.append(_Where(condition, method, self, result))
        return result

    def _row(self, method: str) -> tuple[object, ...]:
        self._log.events.append(_RowAccess(method, self))
        if self._bound_row is None:
            raise AssertionError("a row access ran on a DataFrame that selectExpr did not return")
        if self._answers is not None and self._answers.row_error is not None:
            raise self._answers.row_error
        return self._bound_row


# --- Fixtures and helpers -----------------------------------------------------------------


@pytest.fixture
def log() -> _Log:
    """The event log of the fake session `spark`."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """The fake session of the acceptance criteria: its loads return a source with `COLUMNS`."""
    return _FakeSpark(log)


@pytest.fixture
def conn() -> connectors.MongoDBConnection:
    """`MongoDBConnection(URI)`, the connection of the acceptance criteria."""
    return connectors.MongoDBConnection(URI)


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the reader returns."""
    return cast(DataFrame, fake)


def _reader(
    conn: connectors.MongoDBConnection, spark: _FakeSpark, **arguments: Any
) -> connectors.MongoDBReader:
    """`MongoDBReader("shop", "orders", conn, spark=spark)`, with `arguments` added."""
    return connectors.MongoDBReader("shop", "orders", conn, spark=_session(spark), **arguments)


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _lower(options: Mapping[str, str]) -> dict[str, str]:
    """`options` with lowercase keys, to compare keys case-insensitively."""
    return {key.lower(): value for key, value in options.items()}


def _loads(log: _Log) -> list[_Load]:
    """The loads recorded, in order."""
    return [event for event in log.events if isinstance(event, _Load)]


def _wheres(log: _Log) -> list[_Where]:
    """The wheres recorded, in order."""
    return [event for event in log.events if isinstance(event, _Where)]


def _selects(log: _Log) -> list[_Select]:
    """The selects recorded, in order."""
    return [event for event in log.events if isinstance(event, _Select)]


def _kinds(log: _Log) -> list[str]:
    """The events as words: `load`, `schema`, `columns`, `where`, `select`, a row's method."""
    kinds: list[str] = []
    for event in log.events:
        if isinstance(event, _Load):
            kinds.append("load")
        elif isinstance(event, _Schema):
            kinds.append("schema")
        elif isinstance(event, _Columns):
            kinds.append("columns")
        elif isinstance(event, _Where):
            kinds.append("where")
        elif isinstance(event, _Select):
            kinds.append("select")
        else:
            kinds.append(event.method)
    return kinds


def _shape(log: _Log) -> list[tuple[object, ...]]:
    """The events in order, each DataFrame by its label, to compare the logs of two sessions."""
    shape: list[tuple[object, ...]] = []
    for event in log.events:
        if isinstance(event, _Load):
            shape.append(
                ("load", event.source, event.options, event.args, event.kwargs, _label(event.frame))
            )
        elif isinstance(event, _Schema):
            shape.append(("schema", event.schema))
        elif isinstance(event, _Columns):
            shape.append(("columns", _label(event.on)))
        elif isinstance(event, _Where):
            shape.append(
                ("where", event.method, event.condition, _label(event.on), _label(event.result))
            )
        elif isinstance(event, _Select):
            shape.append(("select", event.expressions, _label(event.on), _label(event.result)))
        else:
            shape.append((event.method, _label(event.on)))
    return shape


def _label(fake: _FakeDataFrame) -> str:
    """The label of `fake` in its log."""
    return fake._label


def _attributes(fake: _FakeDataFrame) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _bound_expressions(column: str) -> list[str]:
    """The two expressions of the bound query over the quoted `column`, in order."""
    return [
        f"CAST(max({column}) AS STRING) AS upper_bound",
        f"typeof(max({column})) AS upper_bound_type",
    ]


def _config(directory: Path, document: object, name: str = "job.json") -> Path:
    """Write `document` with `json.dumps` to `directory / name`, and return the path."""
    path = directory / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _run_python(code: str, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run `code` in a fresh interpreter, in `cwd`, with `args` as its arguments."""
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _no_session(lookups: list[str]) -> Callable[[], SparkSession]:
    """A stand-in for `SparkSession.active` that records each call in `lookups` and raises."""

    def active() -> SparkSession:
        lookups.append("active")
        raise RuntimeError("no active SparkSession")

    return active


# --- Import surface -----------------------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import MongoDBConnection, MongoDBReader

import ingestio
import ingestio.callback.all as callbacks
import ingestio.connector.all as connectors
import ingestio.connector.mongodb as mongodb

names = {"MongoDBConnection", "MongoDBReader"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(mongodb.__all__), mongodb.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
assert ingestio.ReadSource is callbacks.ReadSource
"""

AT002_CODE = """
import sys

sys.modules["pyspark"] = None
for name in ("pyspark", "pyspark.sql"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import MongoDBConnection, MongoDBReader

loads = []


class _FakeDataFrame:
    columns = ["_id", "total", "updatedAt"]


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def load(self, *args, **kwargs):
        loads.append(_FakeDataFrame())
        return loads[-1]


class _FakeSpark:
    @property
    def read(self):
        return _FakeDataFrameReader()


conn = MongoDBConnection(sys.argv[1])
reader = MongoDBReader("shop", "orders", conn, spark=_FakeSpark())
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connection_and_the_reader_work_without_pyspark_installed(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path, URI)
    assert result.returncode == 0, result.stderr


# --- MongoDBConnection --------------------------------------------------------------------

# The examples of the spec's section `MongoDBConnection`: the connection string and its `repr`.
AT003_EXAMPLES: list[tuple[str, str]] = [
    (URI, "MongoDBConnection(hosts='cluster0.example.net')"),
    (
        "mongodb://u53r-tok:p455-tok@h1:27017,h2:27017,h3:27017/shop?replicaSet=rs0",
        "MongoDBConnection(hosts='h1:27017,h2:27017,h3:27017')",
    ),
    ("mongodb://localhost", "MongoDBConnection(hosts='localhost')"),
    (
        "mongodb://localhost:27017?directConnection=true",
        "MongoDBConnection(hosts='localhost:27017')",
    ),
    ("mongodb://u%40ser:p%40ss@db.internal/shop.orders", "MongoDBConnection(hosts='db.internal')"),
    ("mongodb://u53r-tok@db.internal/admin", "MongoDBConnection(hosts='db.internal')"),
    (
        "mongodb://u53r-tok:p4@p455-tok@db.internal/?authSource=admin",
        "MongoDBConnection(hosts='db.internal')",
    ),
]
AT003_IDS = [
    "srv",
    "replica-set",
    "host-only",
    "options-without-path",
    "encoded",
    "user-only",
    "two-ats",
]


@pytest.mark.parametrize(("uri", "shown"), AT003_EXAMPLES, ids=AT003_IDS)
def test_at003_a_connection_keeps_its_uri_and_shows_only_its_hosts(uri: str, shown: str) -> None:
    connection = connectors.MongoDBConnection(uri)

    assert connection.uri is uri
    assert repr(connection) == shown
    assert str(connection) == shown


def test_at003_a_connection_is_immutable(conn: connectors.MongoDBConnection) -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        cast(Any, conn).uri = "mongodb://db.internal"


# The rows of AT-004: an id, the `uri`, the type of the error and its message.
AT004_ROWS: list[tuple[str, object, type[Exception], str]] = [
    ("bytes", b"mongodb://db.internal", TypeError, "uri must be a str, not bytes"),
    ("none", None, TypeError, "uri must be a str, not NoneType"),
    ("empty", "", ValueError, SCHEME),
    ("uppercase-scheme", "MONGODB://db.internal", ValueError, SCHEME),
    ("leading-space", " mongodb://db.internal", ValueError, SCHEME),
    ("one-slash", "mongodb:/db.internal", ValueError, SCHEME),
    ("postgresql", "postgresql://u53r-tok:p455-tok@db.internal/shop", ValueError, SCHEME),
    ("slash-in-password", "mongodb://u53r-tok:p4/55-tok@db.internal/", ValueError, AT_AFTER_HOSTS),
    ("mark-in-password", "mongodb://u53r-tok:p4?55-tok@db.internal/", ValueError, AT_AFTER_HOSTS),
    ("at-in-option", "mongodb://db.internal/?appName=svc@corp", ValueError, AT_AFTER_HOSTS),
    ("scheme-only", "mongodb://", ValueError, NO_HOST),
    ("path-only", "mongodb:///shop", ValueError, NO_HOST),
    ("credentials-only", "mongodb://u53r-tok:p455-tok@/shop", ValueError, NO_HOST),
    ("options-only", "mongodb+srv://?tls=true", ValueError, NO_HOST),
]


@pytest.mark.parametrize(
    ("uri", "error", "message"),
    [pytest.param(uri, error, message, id=name) for name, uri, error, message in AT004_ROWS],
)
def test_at004_a_connection_string_that_cannot_be_shown_safely_is_rejected(
    uri: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=_exactly(message)):
        connectors.MongoDBConnection(cast(Any, uri))


# --- Building the reader ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("database", "collection", "arguments", "message"),
    [
        pytest.param("", "orders", {}, "database must not be empty", id="empty-database"),
        pytest.param("shop", "", {}, "collection must not be empty", id="empty-collection"),
        pytest.param(
            "shop",
            "orders",
            {"watermark_column": ""},
            "watermark_column must not be empty",
            id="empty-watermark-column",
        ),
        pytest.param(
            "shop",
            "orders",
            {"watermark_value": 41},
            "watermark_value needs watermark_column",
            id="value-without-column",
        ),
    ],
)
def test_at005_empty_names_and_a_value_without_its_column_are_rejected(
    database: str,
    collection: str,
    arguments: dict[str, Any],
    message: str,
    conn: connectors.MongoDBConnection,
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MongoDBReader(database, collection, conn, **arguments)


@pytest.mark.parametrize(
    ("hints", "message"),
    [
        pytest.param("", "schema_hints must not be empty", id="empty"),
        pytest.param("   ", "schema_hints must not be empty", id="spaces"),
        pytest.param("\t\n", "schema_hints must not be empty", id="tab-and-newline"),
        pytest.param(
            '{"type": "struct", "fields": []}',
            "schema_hints must be a DDL string, not JSON",
            id="json",
        ),
        pytest.param(
            '  {"type": "struct"}', "schema_hints must be a DDL string, not JSON", id="spaced-json"
        ),
        pytest.param(
            '\n{"type": "struct"}', "schema_hints must be a DDL string, not JSON", id="newline-json"
        ),
    ],
)
def test_at006_blank_and_json_schema_hints_are_rejected(
    hints: str, message: str, conn: connectors.MongoDBConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MongoDBReader("shop", "orders", conn, schema_hints=hints)


@pytest.mark.parametrize(
    "hints",
    [
        "updatedAt TIMESTAMP, total DECIMAL(18,2)",
        "audit.updatedAt TIMESTAMP",
        "STRUCT<updatedAt: TIMESTAMP>",
        "  updatedAt TIMESTAMP  ",
        "not ( a ddl",
    ],
    ids=["two-fields", "dotted-path", "struct", "spaced", "unparsed"],
)
def test_at006_any_other_schema_hints_are_accepted_unparsed(
    hints: str, conn: connectors.MongoDBConnection
) -> None:
    connectors.MongoDBReader("shop", "orders", conn, schema_hints=hints)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        pytest.param(True, "unsupported watermark_value type: bool", id="bool"),
        pytest.param(1.5, "unsupported watermark_value type: float", id="float"),
        pytest.param([1], "unsupported watermark_value type: list", id="list"),
        pytest.param(Decimal("NaN"), "watermark_value must be finite", id="decimal-nan"),
        pytest.param(Decimal("Infinity"), "watermark_value must be finite", id="decimal-infinity"),
        pytest.param(
            datetime(2026, 10, 1),  # noqa: DTZ001 - the naive datetime under test
            "watermark_value must be timezone-aware",
            id="naive-datetime",
        ),
    ],
)
def test_at007_unsupported_watermark_values_are_rejected(
    value: object, message: str, conn: connectors.MongoDBConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        connectors.MongoDBReader(
            "shop", "orders", conn, watermark_column="updatedAt", watermark_value=cast(Any, value)
        )


@pytest.mark.parametrize(
    "value",
    [0, "", Decimal("12.50"), date(2026, 10, 1), OCT_1],
    ids=["zero", "empty-string", "decimal", "date", "aware-datetime"],
)
def test_at007_supported_watermark_values_are_accepted(
    value: int | Decimal | str | date, conn: connectors.MongoDBConnection
) -> None:
    connectors.MongoDBReader(
        "shop", "orders", conn, watermark_column="updatedAt", watermark_value=value
    )


@pytest.mark.parametrize(
    "key",
    [
        "connection.uri",
        "Connection.URI",
        "database",
        "DATABASE",
        "collection",
        "schemaHints",
        "schemahints",
        "aggregation.pipeline",
        "spark.mongodb.read.connection.uri",
        "spark.mongodb.read.database",
        "spark.mongodb.collection",
        "Spark.MongoDB.Read.SchemaHints",
    ],
)
def test_at008_options_the_reader_sets_are_rejected_in_any_case_and_prefix(
    key: str, conn: connectors.MongoDBConnection
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        connectors.MongoDBReader("shop", "orders", conn, options={key: "x"})


@pytest.mark.parametrize(
    "options",
    [
        {"sampleSize": 5000},
        {
            "partitioner": PAGINATE_BY_SIZE,
            "partitioner.options.partition.size": 64,
        },
        {
            "aggregation.allowDiskUse": True,
            "mode": "FAILFAST",
            "comment": "ingestio",
            "outputExtendedJson": False,
        },
    ],
    ids=["sample-size", "partitioner", "other-read-options"],
)
def test_at008_other_read_options_of_the_connector_are_accepted(
    options: dict[str, str | int | float | bool], conn: connectors.MongoDBConnection
) -> None:
    connectors.MongoDBReader("shop", "orders", conn, options=options)


AT009_URIS = [
    "mongodb://u53r-tok:p455-tok@db.internal/admin",
    "mongodb://db.internal/shop.events",
    "mongodb://db.internal/other.orders?authSource=admin",
    "mongodb://db.internal/shop",
]


@pytest.mark.parametrize(
    "uri", AT009_URIS, ids=["admin", "other-collection", "other-database", "same-database"]
)
def test_at009_the_path_of_the_connection_string_never_changes_what_is_read(
    uri: str, log: _Log, spark: _FakeSpark
) -> None:
    reader = connectors.MongoDBReader(
        "shop", "orders", connectors.MongoDBConnection(uri), spark=_session(spark)
    )

    reader.read()

    (load,) = _loads(log)
    assert _lower(load.options) == {
        "connection.uri": uri,
        "database": "shop",
        "collection": "orders",
    }


def test_at010_building_touches_neither_the_session_nor_the_active_session_lookup(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    hints = "updatedAt TIMESTAMP"
    options: dict[str, str | int | float | bool] = {"sampleSize": 5000}

    connectors.MongoDBReader("shop", "orders", conn)
    connectors.MongoDBReader("shop", "orders", conn, spark=_session(spark))
    connectors.MongoDBReader("shop", "orders", conn, watermark_column="updatedAt")
    connectors.MongoDBReader(
        "shop", "orders", conn, spark=_session(spark), watermark_column="updatedAt"
    )
    connectors.MongoDBReader(
        "shop", "orders", conn, watermark_column="updatedAt", watermark_value=OCT_1
    )
    connectors.MongoDBReader("shop", "orders", conn, schema_hints=hints, options=options)
    connectors.MongoDBReader(
        "shop",
        "orders",
        conn,
        spark=_session(spark),
        watermark_column="updatedAt",
        watermark_value=41,
        schema_hints=hints,
        options=options,
    )

    assert lookups == []
    assert log.spark_attributes == []
    assert log.events == []


def test_at011_repr_shows_the_database_the_collection_and_the_hosts_only(
    spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    plain = _reader(conn, spark)
    full = _reader(
        conn,
        spark,
        watermark_column="updatedAt",
        watermark_value=41,
        schema_hints="updatedAt TIMESTAMP",
        options={"sampleSize": 5000},
    )

    for reader in (plain, full):
        assert repr(reader) == READER_REPR
        assert str(reader) == READER_REPR


def test_at011_repr_shows_the_collection_as_given(conn: connectors.MongoDBConnection) -> None:
    reader = connectors.MongoDBReader("shop", "a,b*", conn)

    shown = "MongoDBReader(database='shop', collection='a,b*', hosts='cluster0.example.net')"
    assert repr(reader) == shown
    assert str(reader) == shown


# --- Full read ----------------------------------------------------------------------------


def test_at012_a_full_read_returns_the_source_of_one_load_unchanged(
    log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    frame = _reader(conn, spark).read()

    assert _kinds(log) == ["load", "columns"]
    (load,) = _loads(log)
    assert load.source == "mongodb"
    assert load.options == BASE_OPTIONS
    assert load.args == ()
    assert load.kwargs == {}
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == ["columns"]
    assert set(log.spark_attributes) == {"read"}


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at013_schema_hints_and_options_reach_the_load_as_given(
    watermark: bool, log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    hints = "  updatedAt TIMESTAMP, total DECIMAL(18,2)"

    _reader(
        conn,
        spark,
        watermark_column="updatedAt" if watermark else None,
        schema_hints=hints,
        options={"sampleSize": 5000, "Partitioner": "x"},
    ).read()

    (load,) = _loads(log)
    assert _lower(load.options) == _lower(
        {**BASE_OPTIONS, "schemaHints": hints, "sampleSize": "5000", "Partitioner": "x"}
    )
    assert "schemaHints" in load.options, load.options
    assert "Partitioner" in load.options, load.options


def test_at013_without_schema_hints_the_load_has_no_schema_hints_option(
    log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    _reader(conn, spark, options={"sampleSize": 5000}).read()

    (load,) = _loads(log)
    assert "schemahints" not in _lower(load.options), load.options


def test_at014_the_active_session_is_looked_up_at_read_not_when_building(
    log: _Log, conn: connectors.MongoDBConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []
    monkeypatch.setattr(SparkSession, "active", _no_session(lookups))
    reader = connectors.MongoDBReader("shop", "orders", conn)
    assert lookups == []

    fake = _FakeSpark(log)
    monkeypatch.setattr(SparkSession, "active", lambda: _session(fake))
    frame = reader.read()

    (load,) = _loads(log)
    assert load.spark is fake
    assert frame is _frame(load.frame)


def test_at014_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, conn: connectors.MongoDBConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = connectors.MongoDBReader("shop", "orders", conn)
    err = RuntimeError("no active SparkSession")

    def active() -> SparkSession:
        raise err

    monkeypatch.setattr(SparkSession, "active", active)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert log.events == []


@pytest.mark.parametrize(
    ("collection", "option"),
    [
        pytest.param("orders.2026", "orders.2026", id="dot"),
        pytest.param("a,b", r"a\,b", id="comma"),
        pytest.param("*", r"\*", id="star"),
        pytest.param("logs*", r"logs\*", id="trailing-star"),
        pytest.param(r"a\b", r"a\\b", id="backslash"),
        pytest.param(r"\,*", r"\\\,\*", id="all-three"),
    ],
)
def test_at015_the_collection_option_escapes_the_connectors_list_syntax(
    collection: str,
    option: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
) -> None:
    connectors.MongoDBReader("shop", collection, conn, spark=_session(spark)).read()

    (load,) = _loads(log)
    assert _lower(load.options)["collection"] == option


# --- Empty schema -------------------------------------------------------------------------


def _assert_empty_read(log: _Log, frame: DataFrame) -> None:
    """The log of AT-016: one load, `columns` on the source, one where on it, with `false`."""
    assert _kinds(log) == ["load", "columns", "where"]
    load, columns, where = log.events
    assert isinstance(load, _Load)
    assert isinstance(columns, _Columns)
    assert isinstance(where, _Where)
    assert columns.on is load.frame
    assert where.on is load.frame
    assert where.condition == "false"
    assert frame is _frame(where.result)
    assert _attributes(where.result) == []


def test_at016_a_full_read_of_a_source_with_no_columns_returns_it_filtered_with_false(
    log: _Log, conn: connectors.MongoDBConnection
) -> None:
    spark = _FakeSpark(log, columns=[])

    frame = _reader(conn, spark).read()

    _assert_empty_read(log, frame)


@pytest.mark.parametrize("value", [None, OCT_1], ids=["without-value", "with-value"])
def test_at017_a_watermark_read_of_a_source_with_no_columns_runs_no_bound_query(
    value: datetime | None, log: _Log, conn: connectors.MongoDBConnection
) -> None:
    spark = _FakeSpark(log, columns=[])

    frame = _reader(
        conn,
        spark,
        watermark_column="updatedAt",
        watermark_value=value,
        schema_hints="updatedAt TIMESTAMP",
    ).read()

    _assert_empty_read(log, frame)


# --- Watermark read -----------------------------------------------------------------------


def test_at018_a_watermark_read_captures_a_bound_then_slices_the_source(
    log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    frame = _reader(conn, spark, watermark_column="updatedAt", watermark_value=OCT_1).read()

    assert _kinds(log) == ["load", "columns", "where", "select", "first", "where"]
    load, columns, lower, select, first, upper = log.events
    assert isinstance(load, _Load)
    assert isinstance(columns, _Columns)
    assert isinstance(lower, _Where)
    assert isinstance(select, _Select)
    assert isinstance(first, _RowAccess)
    assert isinstance(upper, _Where)

    assert _lower(load.options) == _lower(BASE_OPTIONS)
    source = load.frame
    assert columns.on is source
    assert lower.on is source
    assert lower.condition == "`updatedAt` > '2026-10-01 00:00:00+00:00'"
    assert select.on is lower.result
    assert list(select.expressions) == _bound_expressions(UPDATED_AT)
    assert first.on is select.result
    assert upper.on is source
    assert upper.condition == (
        "`updatedAt` > '2026-10-01 00:00:00+00:00'"
        " AND `updatedAt` <= CAST('2026-10-21 09:30:00.5' AS timestamp)"
    )
    assert frame is _frame(upper.result)
    assert _attributes(upper.result) == []


def test_at019_without_a_watermark_value_the_bound_covers_the_whole_source(
    log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    frame = _reader(conn, spark, watermark_column="updatedAt").read()

    assert _kinds(log) == ["load", "columns", "select", "first", "where"]
    (load,) = _loads(log)
    (select,) = _selects(log)
    assert select.on is load.frame
    assert list(select.expressions) == _bound_expressions(UPDATED_AT)
    (where,) = _wheres(log)
    assert where.on is load.frame
    assert where.condition == f"`updatedAt` <= {UPPER}"
    assert frame is _frame(where.result)


@pytest.mark.parametrize("value", [None, OCT_1], ids=["without-value", "with-value"])
def test_at020_without_a_bound_the_slice_is_the_source_filtered_with_false(
    value: datetime | None, log: _Log, conn: connectors.MongoDBConnection
) -> None:
    spark = _FakeSpark(log, bound_rows=[(None, "timestamp")])

    frame = _reader(conn, spark, watermark_column="updatedAt", watermark_value=value).read()

    (load,) = _loads(log)
    last = _wheres(log)[-1]
    assert _kinds(log)[-1] == "where"
    assert last.on is load.frame
    assert last.condition == "false"
    assert frame is _frame(last.result)


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        pytest.param(41, "41", id="int"),
        pytest.param(0, "0", id="zero"),
        pytest.param(-7, "-7", id="negative-int"),
        pytest.param(Decimal("12.50"), "12.50BD", id="decimal"),
        pytest.param(Decimal("1E+3"), "1000BD", id="decimal-exponent"),
        pytest.param(Decimal("-0.25"), "-0.25BD", id="negative-decimal"),
        pytest.param("O'Brien", r"'O\'Brien'", id="single-quote"),
        pytest.param(r"C:\temp", r"'C:\\temp'", id="backslash"),
        pytest.param("", "''", id="empty-string"),
        pytest.param(date(2026, 10, 1), "'2026-10-01'", id="date"),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, tzinfo=UTC), "'2026-10-01 08:30:00+00:00'", id="utc"
        ),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=MINUS_3),
            "'2026-10-01 11:30:00.123456+00:00'",
            id="offset",
        ),
    ],
)
def test_at021_watermark_values_become_literals_by_type(
    value: int | Decimal | str | date,
    literal: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
) -> None:
    _reader(conn, spark, watermark_column="w", watermark_value=value).read()

    assert _kinds(log) == ["load", "columns", "where", "select", "first", "where"]
    lower, upper = _wheres(log)
    assert lower.condition == f"`w` > {literal}"
    assert upper.condition == f"`w` > {literal} AND `w` <= {UPPER}"


@pytest.mark.parametrize(
    ("bound_row", "condition"),
    [
        pytest.param(
            ("12.50", "decimal(18,2)"), "`w` <= CAST('12.50' AS decimal(18,2))", id="decimal"
        ),
        pytest.param(
            ("9007199254740993", "bigint"),
            "`w` <= CAST('9007199254740993' AS bigint)",
            id="bigint",
        ),
        pytest.param(("it's", "string"), r"`w` <= CAST('it\'s' AS string)", id="single-quote"),
        pytest.param(("a\\b", "string"), r"`w` <= CAST('a\\b' AS string)", id="backslash"),
    ],
)
def test_at022_the_bound_is_cast_back_to_the_type_spark_named(
    bound_row: tuple[object, ...], condition: str, log: _Log, conn: connectors.MongoDBConnection
) -> None:
    spark = _FakeSpark(log, bound_rows=[bound_row])

    _reader(conn, spark, watermark_column="w").read()

    assert _wheres(log)[-1].condition == condition


def test_at023_each_read_loads_the_collection_again_and_captures_its_own_bound(
    log: _Log, conn: connectors.MongoDBConnection
) -> None:
    spark = _FakeSpark(log, bound_rows=[BOUND_ROW, ("2026-10-22 10:00:00", "timestamp")])
    reader = _reader(conn, spark, watermark_column="updatedAt")

    first = reader.read()
    second = reader.read()

    assert _kinds(log) == ["load", "columns", "select", "first", "where"] * 2
    first_load, second_load = _loads(log)
    first_where, second_where = _wheres(log)
    assert first_where.on is first_load.frame
    assert first_where.condition == f"`updatedAt` <= {UPPER}"
    assert second_where.on is second_load.frame
    assert second_where.condition == "`updatedAt` <= CAST('2026-10-22 10:00:00' AS timestamp)"
    assert first is _frame(first_where.result)
    assert second is _frame(second_where.result)
    assert first is not second


@pytest.mark.parametrize(
    ("column", "quoted"),
    [
        pytest.param("updatedAt", "`updatedAt`", id="plain"),
        pytest.param("Updated At", "`Updated At`", id="space"),
        pytest.param("my`field", "`my``field`", id="backtick"),
    ],
)
def test_at024_the_watermark_field_is_always_quoted(
    column: str,
    quoted: str,
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
) -> None:
    _reader(conn, spark, watermark_column=column, watermark_value=1).read()

    lower, upper = _wheres(log)
    assert lower.condition == f"{quoted} > 1"
    (select,) = _selects(log)
    assert list(select.expressions) == _bound_expressions(quoted)
    assert upper.condition == f"{quoted} > 1 AND {quoted} <= {UPPER}"


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at025_a_failed_load_propagates_and_nothing_runs_after_it(
    watermark: bool, log: _Log, conn: connectors.MongoDBConnection
) -> None:
    err = RuntimeError("MongoTimeoutException")
    spark = _FakeSpark(log, load_error=err)
    reader = _reader(
        conn,
        spark,
        watermark_column="updatedAt" if watermark else None,
        watermark_value=OCT_1 if watermark else None,
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    kinds = _kinds(log)
    assert "where" not in kinds, kinds
    assert "select" not in kinds, kinds


@pytest.mark.parametrize("value", [None, OCT_1], ids=["without-value", "with-value"])
def test_at025_a_failed_bound_query_propagates_and_nothing_is_filtered_after_it(
    value: datetime | None, log: _Log, conn: connectors.MongoDBConnection
) -> None:
    err = RuntimeError("MongoTimeoutException")
    spark = _FakeSpark(log, row_error=err)
    reader = _reader(conn, spark, watermark_column="updatedAt", watermark_value=value)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    kinds = _kinds(log)
    assert kinds.count("select") == 1, kinds
    assert "where" not in kinds[kinds.index("select") :], kinds


# --- In a configuration -------------------------------------------------------------------


def test_at026_read_source_builds_the_reader_of_the_section_without_touching_spark(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
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
    twin = _FakeSpark(twin_log)
    connectors.MongoDBReader("shop", "orders", conn, spark=_session(twin)).read()
    assert _shape(twin_log), "the twin recorded nothing"
    assert _shape(log) == _shape(twin_log)


def test_at026_the_keys_of_the_section_and_the_arguments_reach_the_reader(
    log: _Log, spark: _FakeSpark, conn: connectors.MongoDBConnection, tmp_path: Path
) -> None:
    section = {
        **DEFAULT_SOURCE,
        "watermark_column": "updatedAt",
        "schema_hints": "updatedAt TIMESTAMP",
        "options": {"sampleSize": 5000},
    }
    path = _config(tmp_path, {"source": section})
    extract = ReadSource(path, connection=conn, watermark_value=OCT_1, spark=_session(spark))

    Process([Step("extract", [extract])]).run()

    twin_log = _Log()
    twin = _FakeSpark(twin_log)
    connectors.MongoDBReader(
        "shop",
        "orders",
        conn,
        watermark_column="updatedAt",
        watermark_value=OCT_1,
        schema_hints="updatedAt TIMESTAMP",
        options={"sampleSize": 5000},
        spark=_session(twin),
    ).read()
    assert _kinds(twin_log) == ["load", "columns", "where", "select", "first", "where"]
    assert _shape(log) == _shape(twin_log)


def test_at026_mypy_strict_accepts_a_mongodb_connection_and_a_reader_of_dataframes(
    log: _Log, spark: _FakeSpark, tmp_path: Path
) -> None:
    """mypy strict, which checks `tests/`, accepts the two annotated bindings of this test."""
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    conn: connectors.MongoDBConnection = connectors.MongoDBConnection(URI)

    ReadSource(path, connection=conn)
    reader: Reader[DataFrame] = connectors.MongoDBReader(
        "shop", "orders", conn, spark=_session(spark)
    )

    frame = reader.read()
    (load,) = _loads(log)
    assert frame is _frame(load.frame)


@pytest.mark.parametrize(
    ("section", "message"),
    [
        pytest.param(
            {"system": "mongodb", "collection": "orders"},
            "source.database is required for system 'mongodb'",
            id="no-database",
        ),
        pytest.param(
            {"system": "mongodb"},
            "source.database is required for system 'mongodb'",
            id="no-keys",
        ),
        pytest.param(
            {"system": "mongodb", "database": "shop"},
            "source.collection is required for system 'mongodb'",
            id="no-collection",
        ),
        *[
            pytest.param(
                {**DEFAULT_SOURCE, key: "mongodb://db.internal"},
                f"source.{key} is not a key of system 'mongodb'",
                id=f"key-{key}",
            )
            for key in (
                "uri",
                "password",
                "host",
                "schema",
                "table_name",
                "connection",
                "watermark_value",
                "sampleSize",
            )
        ],
        pytest.param(
            {**DEFAULT_SOURCE, "database": 1},
            "source.database must be a string",
            id="number-database",
        ),
        pytest.param(
            {**DEFAULT_SOURCE, "collection": None},
            "source.collection must be a string",
            id="null-collection",
        ),
        pytest.param(
            {**DEFAULT_SOURCE, "schema_hints": ["updatedAt TIMESTAMP"]},
            "source.schema_hints must be a string",
            id="array-schema-hints",
        ),
        pytest.param(
            {**DEFAULT_SOURCE, "watermark_column": True},
            "source.watermark_column must be a string",
            id="boolean-watermark-column",
        ),
        pytest.param(
            {**DEFAULT_SOURCE, "options": {"sampleSize": [1]}},
            "source.options must be an object of strings, numbers and booleans",
            id="array-option",
        ),
    ],
)
def test_at027_a_mongodb_section_with_wrong_keys_is_rejected(
    section: dict[str, object],
    message: str,
    conn: connectors.MongoDBConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": section})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        ReadSource(path, connection=conn)


def test_at028_mongodb_without_connection_is_rejected(tmp_path: Path) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    message = f"{path}: source system 'mongodb' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize(
    "with_connection", [True, False], ids=["with-connection", "without-connection"]
)
def test_at028_mongodb_takes_no_client(
    with_connection: bool, conn: connectors.MongoDBConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})
    arguments: dict[str, object] = {"client": object()}
    if with_connection:
        arguments["connection"] = conn

    message = f"{path}: source system 'mongodb' takes no argument client"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path, **cast(Any, arguments))


def test_at028_arguments_given_as_none_are_not_given(
    conn: connectors.MongoDBConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    ReadSource(path, connection=conn, client=None, watermark_value=None, spark=None)


@dataclass(frozen=True)
class _ConnectionCase:
    """A row of AT-029: the callback, its configuration, the `connection` given, the message.

    `connection` is `postgres` for a `PostgresConnection`, `uri` for the string `URI` and `conn`
    for `MongoDBConnection(URI)`. The message follows `<path>: `.
    """

    name: str
    callback: Callable[..., object]
    document: dict[str, object]
    connection: str
    message: str


AT029_TARGET = (
    "target system 'postgres' needs a PostgresConnection as the argument connection,"
    " not MongoDBConnection"
)

AT029_CASES = [
    _ConnectionCase(
        "ReadSource-mongodb-postgres-connection",
        ReadSource,
        {"source": DEFAULT_SOURCE},
        "postgres",
        "source system 'mongodb' needs a MongoDBConnection as the argument connection,"
        " not PostgresConnection",
    ),
    _ConnectionCase(
        "ReadSource-mongodb-string",
        ReadSource,
        {"source": DEFAULT_SOURCE},
        "uri",
        "source system 'mongodb' needs a MongoDBConnection as the argument connection, not str",
    ),
    _ConnectionCase(
        "ReadSource-postgres-mongodb-connection",
        ReadSource,
        {"source": POSTGRES_SECTION},
        "conn",
        "source system 'postgres' needs a PostgresConnection as the argument connection,"
        " not MongoDBConnection",
    ),
    _ConnectionCase(
        "AppendOnly-postgres", AppendOnly, {"target": POSTGRES_SECTION}, "conn", AT029_TARGET
    ),
    _ConnectionCase(
        "FullLoad-postgres", FullLoad, {"target": POSTGRES_SECTION}, "conn", AT029_TARGET
    ),
    _ConnectionCase(
        "SCD2-postgres",
        SCD2,
        {"target": POSTGRES_SECTION, "scd2": SCD2_SECTION},
        "conn",
        AT029_TARGET,
    ),
]


def _given_connection(kind: str, conn: connectors.MongoDBConnection) -> object:
    """The `connection` that a row of AT-029 gives."""
    values: dict[str, object] = {
        "postgres": PostgresConnection("db.internal", "shop", "ingest", "pw"),
        "uri": URI,
        "conn": conn,
    }
    return values[kind]


@pytest.mark.parametrize("case", AT029_CASES, ids=[case.name for case in AT029_CASES])
def test_at029_a_connection_of_another_systems_type_is_rejected(
    case: _ConnectionCase, conn: connectors.MongoDBConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, case.document)

    with pytest.raises(ValueError, match=_exactly(f"{path}: {case.message}")):
        case.callback(path, connection=_given_connection(case.connection, conn))


def test_at029_a_subclass_of_mongodb_connection_is_accepted(tmp_path: Path) -> None:
    class _OwnConnection(connectors.MongoDBConnection):
        """A job's own subclass of the connection."""

    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    ReadSource(path, connection=_OwnConnection(URI))


@pytest.mark.parametrize("build", [AppendOnly, FullLoad], ids=["AppendOnly", "FullLoad"])
def test_at030_mongodb_is_not_a_target_system(build: Callable[..., object], tmp_path: Path) -> None:
    target = {"system": "mongodb", "database": "shop", "collection": "orders"}
    path = _config(tmp_path, {"target": target})

    message = f"{path}: target.system 'mongodb' is not one of: {TARGET_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@pytest.mark.parametrize("system", ["MongoDB", "mongo"])
def test_at030_a_source_system_close_to_mongodb_is_rejected_with_mongodb_in_the_list(
    system: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, "system": system}})

    message = f"{path}: source.system '{system}' is not one of: {SOURCE_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize(
    ("added", "message"),
    [
        pytest.param({"schema_hints": " "}, "schema_hints must not be empty", id="blank-hints"),
        pytest.param(
            {"schema_hints": '{"type": "struct"}'},
            "schema_hints must be a DDL string, not JSON",
            id="json-hints",
        ),
        pytest.param(
            {"options": {"database": "other"}},
            "option 'database' is set by the connector",
            id="reserved-option",
        ),
    ],
)
def test_at031_an_error_of_the_reader_propagates_with_one_note(
    added: dict[str, object],
    message: str,
    conn: connectors.MongoDBConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": {**DEFAULT_SOURCE, **added}})
    with pytest.raises(ValueError) as built_directly:
        connectors.MongoDBReader("shop", "orders", conn, **cast(Any, added))

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        ReadSource(path, connection=conn)

    assert str(excinfo.value) == message
    assert type(excinfo.value) is type(built_directly.value)
    assert str(excinfo.value) == str(built_directly.value)
    note = f"while building the source of {path}, system 'mongodb'"
    assert getattr(excinfo.value, "__notes__", []) == [note]


# --- Logging and secrets ------------------------------------------------------------------


def test_at032_nothing_writes_a_log_record(
    log: _Log,
    spark: _FakeSpark,
    conn: connectors.MongoDBConnection,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    path = _config(tmp_path, {"source": DEFAULT_SOURCE})

    connection = connectors.MongoDBConnection(URI)
    ReadSource(path, connection=connection)
    ReadSource(path, connection=conn, spark=_session(spark))
    _reader(conn, spark).read()
    _reader(
        conn,
        spark,
        watermark_column="updatedAt",
        watermark_value=OCT_1,
        schema_hints="updatedAt TIMESTAMP",
        options={"sampleSize": 5000},
    ).read()
    no_bound = _FakeSpark(log, bound_rows=[(None, "timestamp")])
    _reader(conn, no_bound, watermark_column="updatedAt").read()
    no_columns = _FakeSpark(log, columns=[])
    _reader(conn, no_columns).read()
    _reader(conn, no_columns, watermark_column="updatedAt").read()

    assert len(_loads(log)) == 5
    assert caplog.records == []


def _assert_no_secret(text: str) -> None:
    """`text` holds neither the user nor the password of the criteria's connection strings."""
    assert USER not in text, text
    assert PASSWORD not in text, text


def test_at033_no_repr_or_str_shows_the_user_or_the_password(
    spark: _FakeSpark, conn: connectors.MongoDBConnection
) -> None:
    uris = [uri for uri, _ in AT003_EXAMPLES] + AT009_URIS
    connections = [connectors.MongoDBConnection(uri) for uri in uris] + [conn]
    readers = [_reader(connection, spark) for connection in connections]

    for value in [*connections, *readers]:
        _assert_no_secret(repr(value))
        _assert_no_secret(str(value))


def test_at033_no_error_shows_the_user_or_the_password(
    conn: connectors.MongoDBConnection, tmp_path: Path
) -> None:
    errors: list[Exception] = []
    for _, uri, error, _ in AT004_ROWS:
        with pytest.raises(error) as excinfo:
            connectors.MongoDBConnection(cast(Any, uri))
        errors.append(excinfo.value)
    for number, case in enumerate(AT029_CASES):
        path = _config(tmp_path, case.document, f"job{number}.json")
        with pytest.raises(ValueError) as caught:
            case.callback(path, connection=_given_connection(case.connection, conn))
        errors.append(caught.value)

    for raised in errors:
        _assert_no_secret(str(raised))
        for note in getattr(raised, "__notes__", []):
            _assert_no_secret(note)
