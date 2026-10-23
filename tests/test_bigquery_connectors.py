"""BigQuery connectors: `BigQueryReader` and `BigQueryWriter`.

Covers `specs/bigquery_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no credentials: they use the doubles below, a fake Spark
session, fake DataFrames, a fake BigQuery client and a fake `google.cloud.bigquery.Client`
factory, which all write to one ordered event log. The tables the writer reads and creates are
real `google.cloud.bigquery.Table` objects, built offline.

The integration tests at the end run against BigQuery, in two tiers. They are marked
`integration` and skipped unless `INGESTIO_BIGQUERY_DATASET` holds `project.dataset`, an
existing dataset where the tests may create and drop tables; the credentials are Application
Default Credentials. The first tier uses a real client with the fake Spark session and fake
DataFrames, so it checks the SQL against BigQuery without a JVM. The second tier also needs
`INGESTIO_SPARK_TESTS` set to `1` and `INGESTIO_BIGQUERY_SPARK_PACKAGE` holding the Maven
coordinate of the spark-bigquery-connector build for the installed pyspark, such as
`com.google.cloud.spark:spark-3.5-bigquery:<version>`, which a local Spark session loads
through `spark.jars.packages`; it needs Java 17 or 21 for the locked pyspark. A JVM loads its
packages once, when it starts: when another test module of the same run has already started
one, the second tier skips, so run this file on its own. Each test names its own tables,
`ingestio_it_<stem>_<hex>`, and drops them. They were written without being run: the machine
they were written on has no Google Cloud credentials and no Java 17 or 21.
"""

from __future__ import annotations

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
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeAlias, cast

import pytest
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import bigquery
from google.cloud.bigquery import PolicyTagList, SchemaField, Table
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from ingestio import Process, Reader, ReadSource, Step, Writer, WriteTarget
from ingestio.connector.all import BigQueryReader, BigQueryWriter

TABLE = "acme-data.sales.orders"
TARGET = "`acme-data`.`sales`.`orders`"
UPDATED_AT = "`updated_at`"
BOUND_TEXT = 'TIMESTAMP "2026-10-21 09:30:00.5+00"'
BOUND_ROWS: list[tuple[object, ...]] = [(BOUND_TEXT,)]
STAGING_TABLE_ID = re.compile(r"ingestio_staging_([0-9a-f]{32})")
BIGQUERY = "google.cloud.bigquery"
ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")
BIGQUERY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']bigquery["']""")
MINUS_3 = timezone(timedelta(hours=-3))
WRITER_MISSING = "BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'"
READER_MISSING = "BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'"


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read` of `spark`.

    It records the format, the merged options and the arguments of `load()`. `frame` is the
    DataFrame that `load()` returned.
    """

    source: str | None
    options: dict[str, str]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    frame: _FakeDataFrame = field(repr=False, compare=False)
    spark: _FakeSpark = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Save:
    """`save()` ran on a fake `DataFrame.write` of `frame`.

    It records the format, the mode, the merged options and the arguments of `save()`.
    """

    source: str | None
    mode: str | None
    options: dict[str, str]
    args: tuple[object, ...]
    kwargs: dict[str, object]
    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Call:
    """A call on a BigQuery double, recorded before it runs.

    `target` is `factory` for the replacement of `google.cloud.bigquery.Client`, `client` for a
    fake client and `job` for a fake query job. `owner` is the double the call ran on.
    """

    target: str
    method: str
    args: tuple[object, ...]
    kwargs: dict[str, object]
    owner: object = field(repr=False, compare=False)


_Event: TypeAlias = _Load | _Save | _Call
_Line: TypeAlias = tuple[object, ...]


class _Log:
    """The ordered event log every double writes to.

    `spark_attributes` and `client_attributes` hold every attribute read on a fake session and
    on a fake client, in order.
    """

    def __init__(self) -> None:
        self.events: list[_Event] = []
        self.spark_attributes: list[str] = []
        self.client_attributes: list[str] = []


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
    returns a new `_FakeDataFrame` with `columns`. When `load_error` is set, every load raises
    it once recorded.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        load_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
        self._load_error = load_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self, self._log, self._columns, self._load_error)


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_Load` each time `load()` is called."""

    def __init__(
        self,
        spark: _FakeSpark,
        log: _Log,
        columns: list[str],
        load_error: BaseException | None,
    ) -> None:
        self._spark = spark
        self._log = log
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

    def load(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log, columns=self._columns)
        self._log.events.append(
            _Load(self._source, dict(self._options), args, dict(kwargs), frame, self._spark)
        )
        if self._load_error is not None:
            raise self._load_error
        return frame


SaveHook: TypeAlias = Callable[[Mapping[str, str]], None]


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    Each access to `write` returns a new `_FakeDataFrameWriter`, whose `save()` records a
    `_Save`, then calls `on_save` with the merged options when it is set, then raises
    `save_error` when it is set.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        save_error: BaseException | None = None,
        on_save: SaveHook | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
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
    def write(self) -> _FakeDataFrameWriter:
        return _FakeDataFrameWriter(self._log, self, self._save_error, self._on_save)


class _FakeDataFrameWriter:
    """Stands in for `DataFrame.write`: records a `_Save` when `save()` is called."""

    def __init__(
        self,
        log: _Log,
        frame: _FakeDataFrame,
        save_error: BaseException | None,
        on_save: SaveHook | None,
    ) -> None:
        self._log = log
        self._frame = frame
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

    def mode(self, saveMode: str) -> _FakeDataFrameWriter:  # noqa: N803 - PySpark's name
        self._mode = saveMode
        return self

    def save(self, *args: object, **kwargs: object) -> None:
        options = dict(self._options)
        self._log.events.append(
            _Save(self._source, self._mode, options, args, dict(kwargs), self._frame)
        )
        if self._on_save is not None:
            self._on_save(options)
        if self._save_error is not None:
            raise self._save_error


class _FakeJob:
    """Stands in for a `google.cloud.bigquery.QueryJob`.

    `result()` records its call, then raises `error` when it is set, and otherwise returns a
    copy of the rows its client was given.
    """

    def __init__(
        self, log: _Log, rows: list[tuple[object, ...]], error: BaseException | None
    ) -> None:
        self._log = log
        self._rows = rows
        self._error = error

    def result(self, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        self._log.events.append(_Call("job", "result", args, dict(kwargs), self))
        if self._error is not None:
            raise self._error
        return list(self._rows)


class _FakeClient:
    """Stands in for a `google.cloud.bigquery.Client`, and records every attribute accessed on
    it.

    Every method call is recorded as a `_Call` before it runs, then raises the error that
    `_errors` holds for the method's name, when there is one; `result` there is the error of
    the jobs' `result()`. `query` returns a new `_FakeJob`, kept in `_jobs`, whose `result()`
    answers `rows`. `get_table` returns `table`, `create_table` the table it receives.
    """

    def __init__(
        self,
        log: _Log,
        *,
        rows: Sequence[tuple[object, ...]] = BOUND_ROWS,
        table: Table | None = None,
        errors: Mapping[str, BaseException] | None = None,
    ) -> None:
        self._log = log
        self._rows = [tuple(row) for row in rows]
        self._table = table if table is not None else _target_table()
        self._errors = dict(errors or {})
        self._jobs: list[_FakeJob] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").client_attributes.append(name)
        return object.__getattribute__(self, name)

    def query(self, *args: object, **kwargs: object) -> _FakeJob:
        self._record("query", args, kwargs)
        job = _FakeJob(self._log, self._rows, self._errors.get("result"))
        self._jobs.append(job)
        return job

    def get_table(self, *args: object, **kwargs: object) -> Table:
        self._record("get_table", args, kwargs)
        return self._table

    def create_table(self, *args: object, **kwargs: object) -> object:
        self._record("create_table", args, kwargs)
        return args[0] if args else None

    def delete_table(self, *args: object, **kwargs: object) -> None:
        self._record("delete_table", args, kwargs)

    def close(self, *args: object, **kwargs: object) -> None:
        self._record("close", args, kwargs)

    def _record(self, method: str, args: tuple[object, ...], kwargs: dict[str, object]) -> None:
        self._log.events.append(_Call("client", method, args, dict(kwargs), self))
        error = self._errors.get(method)
        if error is not None:
            raise error


class _FakeClientFactory:
    """Stands in for `google.cloud.bigquery.Client`: records each call and returns `client`.

    When `error` is set, a call raises it once recorded.
    """

    def __init__(self, log: _Log, client: _FakeClient) -> None:
        self._log = log
        self._client = client
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.error: BaseException | None = None

    def __call__(self, *args: object, **kwargs: object) -> _FakeClient:
        self.calls.append((args, dict(kwargs)))
        self._log.events.append(_Call("factory", "Client", args, dict(kwargs), self))
        if self.error is not None:
            raise self.error
        return self._client


class _NoOffset(tzinfo):
    """A time zone whose `utcoffset()` is `None`: a datetime that carries it is naive."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return None


# --- Fixtures and helpers -----------------------------------------------------------------


def _target_table(*fields: SchemaField) -> Table:
    """The target `get_table` returns: `acme-data.sales.orders`, with `fields` as its schema,
    or the schema of the acceptance criteria when none is given."""
    schema = list(fields) or [
        SchemaField("id", "INTEGER", mode="REQUIRED"),
        SchemaField("payload", "STRING"),
        SchemaField("loaded_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
    ]
    return Table(TABLE, schema=schema)


@pytest.fixture
def log() -> _Log:
    """The event log the doubles of one test share."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """The fake session of the acceptance criteria."""
    return _FakeSpark(log)


@pytest.fixture
def client(log: _Log) -> _FakeClient:
    """The fake client of the acceptance criteria: its bound query answers `BOUND_ROWS`."""
    return _FakeClient(log)


def _install(monkeypatch: pytest.MonkeyPatch, log: _Log, client: _FakeClient) -> _FakeClientFactory:
    """Replace `google.cloud.bigquery.Client` with a factory that returns `client`."""
    fake = _FakeClientFactory(log, client)
    monkeypatch.setattr(f"{BIGQUERY}.Client", fake)
    return fake


@pytest.fixture
def factory(log: _Log, client: _FakeClient, monkeypatch: pytest.MonkeyPatch) -> _FakeClientFactory:
    """The factory of the criteria, installed as `google.cloud.bigquery.Client`."""
    return _install(monkeypatch, log, client)


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


def _client(fake: _FakeClient) -> bigquery.Client:
    """The fake, typed as the client the connectors take."""
    return cast(bigquery.Client, fake)


def _fail(fake: _FakeClient, method: str, error: BaseException) -> None:
    """Make `method` of `fake` raise `error`, once recorded; `result` for the jobs' result."""
    fake._errors[method] = error


def _jobs(fake: _FakeClient) -> list[_FakeJob]:
    """The jobs that `query` returned on `fake`, in order."""
    return fake._jobs


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


def _calls(log: _Log, target: str, method: str | None = None) -> list[_Call]:
    """The calls recorded on the doubles of `target`, of `method` when given, in order."""
    return [
        event
        for event in log.events
        if isinstance(event, _Call)
        and event.target == target
        and (method is None or event.method == method)
    ]


def _attributes(frame: _FakeDataFrame) -> list[str]:
    """The attributes accessed on `frame`, in order."""
    return frame._accessed


def _render(value: object) -> object:
    """An argument as the transcript shows it: a `Table` becomes `("Table", <full ID>)`."""
    if isinstance(value, Table):
        return ("Table", f"{value.project}.{value.dataset_id}.{value.table_id}")
    return value


def _transcript(log: _Log) -> list[_Line]:
    """The events as comparable lines, in order.

    A load is `("load", format, options, args, kwargs)` and a save is
    `("save", format, mode, options, args, kwargs)`, with lowercase option keys. A call is
    `("<target>.<method>", args, kwargs)`, and `("factory", args, kwargs)` for the factory.
    """
    lines: list[_Line] = []
    for event in log.events:
        if isinstance(event, _Load):
            lines.append(("load", event.source, _lower(event.options), event.args, event.kwargs))
        elif isinstance(event, _Save):
            lines.append(
                (
                    "save",
                    event.source,
                    event.mode,
                    _lower(event.options),
                    event.args,
                    event.kwargs,
                )
            )
        else:
            name = "factory" if event.target == "factory" else f"{event.target}.{event.method}"
            lines.append((name, tuple(_render(arg) for arg in event.args), event.kwargs))
    return lines


def _kinds(log: _Log) -> list[str]:
    """The first item of every line of the transcript: what happened, in order."""
    return [str(line[0]) for line in _transcript(log)]


def _factory(project: str | None = None) -> _Line:
    """The line of the factory called with exactly `project=<project>`."""
    return ("factory", (), {"project": project})


def _query(statement: str) -> _Line:
    """The line of `client.query(<statement>)`."""
    return ("client.query", (statement,), {})


RESULT: _Line = ("job.result", (), {})
CLOSE: _Line = ("client.close", (), {})
GET_TARGET: _Line = ("client.get_table", (TABLE,), {})


def _bound_query(
    column: str = UPDATED_AT, *, literal: str | None = None, source: str = TARGET
) -> str:
    """The bound query over the quoted `column` of the quoted `source`."""
    query = (
        f"SELECT IF(MAX({column}) IS NULL, NULL, FORMAT('%T', MAX({column}))) AS upper_bound"
        f" FROM {source}"
    )
    return query if literal is None else f"{query} WHERE {column} > {literal}"


@dataclass(frozen=True)
class _Staging:
    """The staging table of one write: its ID, unquoted and quoted, and the `Table` given to
    `create_table`."""

    id: str
    quoted: str
    table: Table


def _created_tables(log: _Log) -> list[Table]:
    """The tables given to `create_table`, in order; each call had one positional argument."""
    tables: list[Table] = []
    for call in _calls(log, "client", "create_table"):
        assert len(call.args) == 1, call
        assert call.kwargs == {}, call
        (table,) = call.args
        assert isinstance(table, Table), table
        tables.append(table)
    return tables


def _staging(log: _Log) -> _Staging:
    """The staging table of the only `create_table` recorded."""
    tables = _created_tables(log)
    assert len(tables) == 1, _transcript(log)
    (table,) = tables
    assert (table.project, table.dataset_id) == ("acme-data", "sales"), table
    assert STAGING_TABLE_ID.fullmatch(table.table_id), table.table_id
    return _Staging(
        id=f"acme-data.sales.{table.table_id}",
        quoted=f"`acme-data`.`sales`.`{table.table_id}`",
        table=table,
    )


def _insert(stg: str, columns: str = "`id`, `payload`") -> str:
    """The statement that appends the staging table `stg`, quoted, to the target."""
    return f"INSERT INTO {TARGET} ({columns}) SELECT {columns} FROM {stg}"


def _merge(
    stg: str, columns: str = "`id`, `payload`", source_columns: str = "S.`id`, S.`payload`"
) -> str:
    """The statement that replaces the rows of the target with the staging table `stg`."""
    return (
        f"MERGE {TARGET} AS T USING {stg} AS S ON FALSE WHEN NOT MATCHED THEN INSERT ({columns})"
        f" VALUES ({source_columns}) WHEN NOT MATCHED BY SOURCE THEN DELETE"
    )


def _save(stg: _Staging, **options: str) -> _Line:
    """The line of the save into `stg`, with `writeMethod=direct` and `options` besides."""
    merged = {"table": stg.id, "writemethod": "direct", **_lower(options)}
    return ("save", "bigquery", "append", merged, (), {})


def _delete(stg: _Staging) -> _Line:
    """The line of `client.delete_table("<stg id>", not_found_ok=True)`."""
    return ("client.delete_table", (stg.id,), {"not_found_ok": True})


def _write_transcript(
    stg: _Staging,
    statement: str,
    *,
    project: str | None = None,
    given: bool = False,
    save: _Line | None = None,
) -> list[_Line]:
    """The transcript of a successful write of `df` through `stg`, applied by `statement`.

    With `given`, the client was given: the factory is not called and the client not closed.
    """
    lines: list[_Line] = [] if given else [_factory(project)]
    lines += [
        GET_TARGET,
        ("client.create_table", (("Table", stg.id),), {}),
        save if save is not None else _save(stg),
        _query(statement),
        RESULT,
        _delete(stg),
    ]
    return lines if given else [*lines, CLOSE]


_Field: TypeAlias = tuple[object, ...]


def _summary(schema_field: SchemaField) -> _Field:
    """What the criteria inspect of a field, with its nested fields, at every depth."""
    return (
        schema_field.name,
        schema_field.field_type,
        schema_field.mode,
        schema_field.max_length,
        schema_field.precision,
        schema_field.scale,
        schema_field.description,
        schema_field.policy_tags,
        schema_field.default_value_expression,
        tuple(_summary(nested) for nested in schema_field.fields),
    )


def _expected(
    name: str,
    field_type: str,
    mode: str = "NULLABLE",
    *,
    max_length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    fields: Sequence[_Field] = (),
) -> _Field:
    """The summary of a staging field: no description, no policy tags, no default value."""
    return (name, field_type, mode, max_length, precision, scale, None, None, None, tuple(fields))


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


def _project(requirement: str) -> str:
    """The normalised name of the project a `Requires-Dist` value names."""
    name = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    assert name, requirement
    return re.sub(r"[-_.]+", "-", name[1]).lower()


WriteCall: TypeAlias = Callable[[BigQueryWriter, DataFrame], object]

APPEND_CALLS: dict[str, WriteCall] = {
    "write(df)": lambda writer, frame: writer.write(frame),
    "write(df, replace=False)": lambda writer, frame: writer.write(frame, replace=False),
}

EVERY_CALL: dict[str, WriteCall] = {
    **APPEND_CALLS,
    "write(df, replace=True)": lambda writer, frame: writer.write(frame, replace=True),
}

BUILDERS = pytest.mark.parametrize(
    "build", [BigQueryReader, BigQueryWriter], ids=["reader", "writer"]
)


# --- Import surface and the extra ---------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import BigQueryReader, BigQueryWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.bigquery as bigquery_connectors

names = {"BigQueryReader", "BigQueryWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(bigquery_connectors.__all__), bigquery_connectors.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys

for name in ("pyspark", "google.cloud.bigquery"):
    sys.modules[name] = None
for name in ("pyspark", "pyspark.sql", "google.cloud.bigquery", "google.cloud.bigquery.client"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")
try:
    from google.cloud import bigquery
except ImportError:
    pass
else:
    raise AssertionError("bigquery can be imported from google.cloud")

import ingestio.connector.all
from ingestio.connector.all import BigQueryReader

loads = []


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


reader = BigQueryReader("acme-data.sales.orders", spark=_FakeSpark())
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
"""

AT003_CODE = """
import json
import sys

sys.modules["google.cloud.bigquery"] = None
try:
    from google.cloud import bigquery
except ImportError:
    pass
else:
    raise AssertionError("bigquery can be imported from google.cloud")

from ingestio.connector.all import BigQueryReader, BigQueryWriter

cases = {
    "writer": lambda: BigQueryWriter("acme-data.sales.orders"),
    "reader": lambda: BigQueryReader("acme-data.sales.orders", watermark_column="updated_at"),
    "reader with client": lambda: BigQueryReader(
        "acme-data.sales.orders", watermark_column="updated_at", client=object()
    ),
}
outcomes = {}
for case, build in cases.items():
    try:
        build()
    except ImportError as error:
        outcomes[case] = {
            "message": str(error),
            "chained": isinstance(error.__cause__, ImportError),
        }
    else:
        outcomes[case] = None
print(json.dumps(outcomes))
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_a_full_read_works_without_pyspark_or_google_cloud_bigquery(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at003_building_what_needs_google_cloud_bigquery_without_it_names_the_extra(
    tmp_path: Path,
) -> None:
    result = _run_python(AT003_CODE, tmp_path)
    assert result.returncode == 0, result.stderr

    outcomes = json.loads(result.stdout.strip().splitlines()[-1])
    assert outcomes == {
        "writer": {"message": WRITER_MISSING, "chained": True},
        "reader": {"message": READER_MISSING, "chained": True},
        "reader with client": {"message": READER_MISSING, "chained": True},
    }


def test_at004_the_distribution_declares_the_bigquery_extra_with_google_cloud_bigquery() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "bigquery" in (metadata.get_all("Provides-Extra") or [])
    requirements: list[str] = metadata.get_all("Requires-Dist") or []
    assert requirements
    for requirement in requirements:
        assert ANY_EXTRA_MARKER.search(requirement.partition(";")[2]), requirement
    assert any(
        _project(requirement) == "google-cloud-bigquery"
        and BIGQUERY_EXTRA_MARKER.search(requirement.partition(";")[2])
        for requirement in requirements
    ), requirements


# --- Building the connectors --------------------------------------------------------------


@BUILDERS
@pytest.mark.parametrize(
    "name",
    [
        "",
        "orders",
        "sales.orders",
        "acme-data.sales.",
        ".sales.orders",
        "acme-data..orders",
        "a.b.c.d",
    ],
)
def test_at005_a_table_name_other_than_three_non_empty_parts_is_rejected(
    build: type[BigQueryReader] | type[BigQueryWriter], name: str
) -> None:
    with pytest.raises(ValueError, match=_exactly("table_name must be 'project.dataset.table'")):
        build(name)


@BUILDERS
@pytest.mark.parametrize("name", ["acme-data.sales.orders", "acme-data.Sales.order lines"])
def test_at005_a_three_part_name_is_accepted(
    build: type[BigQueryReader] | type[BigQueryWriter], name: str
) -> None:
    build(name)


def test_at006_a_watermark_value_needs_a_watermark_column() -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_value needs watermark_column")):
        BigQueryReader(TABLE, watermark_value=41)


def test_at006_an_empty_watermark_column_is_rejected() -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_column must not be empty")):
        BigQueryReader(TABLE, watermark_column="")


@BUILDERS
def test_at006_an_empty_billing_project_is_rejected(
    build: type[BigQueryReader] | type[BigQueryWriter],
) -> None:
    with pytest.raises(ValueError, match=_exactly("billing_project must not be empty")):
        build(TABLE, billing_project="")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        pytest.param(True, "unsupported watermark_value type: bool", id="bool"),
        pytest.param(1.5, "unsupported watermark_value type: float", id="float"),
        pytest.param([1], "unsupported watermark_value type: list", id="list"),
        pytest.param(Decimal("NaN"), "watermark_value must be finite", id="decimal-nan"),
        pytest.param(Decimal("Infinity"), "watermark_value must be finite", id="decimal-infinity"),
        pytest.param(
            Decimal("-Infinity"), "watermark_value must be finite", id="decimal-minus-infinity"
        ),
        pytest.param(
            datetime(2026, 10, 1),  # noqa: DTZ001 - the naive datetime under test
            "watermark_value must be timezone-aware",
            id="naive-datetime",
        ),
        pytest.param(
            datetime(2026, 10, 1, tzinfo=_NoOffset()),
            "watermark_value must be timezone-aware",
            id="tzinfo-without-offset",
        ),
    ],
)
def test_at007_unsupported_watermark_values_are_rejected(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        BigQueryReader(TABLE, watermark_column="updated_at", watermark_value=cast(Any, value))


@pytest.mark.parametrize(
    "value",
    [0, "", Decimal("12.50"), date(2026, 10, 1), datetime(2026, 10, 1, tzinfo=UTC)],
    ids=["zero", "empty-string", "decimal", "date", "aware-datetime"],
)
def test_at007_supported_watermark_values_are_accepted(value: int | Decimal | str | date) -> None:
    BigQueryReader(TABLE, watermark_column="updated_at", watermark_value=value)


@pytest.mark.parametrize(
    "key",
    [
        "table",
        "PATH",
        "project",
        "Dataset",
        "parentproject",
        "credentials",
        "CredentialsFile",
        "gcpAccessToken",
        "gcpAccessTokenProvider",
        "filter",
        "Query",
        "snapshotTimeMillis",
    ],
)
def test_at008_options_the_reader_reserves_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        BigQueryReader(TABLE, options={key: "x"})


@pytest.mark.parametrize(
    "key",
    [
        "table",
        "path",
        "PROJECT",
        "dataset",
        "ParentProject",
        "credentials",
        "credentialsFile",
        "gcpAccessToken",
        "gcpAccessTokenProvider",
        "writeAtLeastOnce",
    ],
)
def test_at008_options_the_writer_reserves_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        BigQueryWriter(TABLE, options={key: "x"})


def test_at008_other_options_are_accepted() -> None:
    BigQueryReader(
        TABLE, options={"maxParallelism": 8, "viewsEnabled": True, "materializationDataset": "tmp"}
    )
    BigQueryWriter(TABLE, options={"writeMethod": "indirect", "temporaryGcsBucket": "acme-tmp"})


Build: TypeAlias = Callable[[bigquery.Client | None, SparkSession], object]

AT009_BUILDS: dict[str, Build] = {
    "full reader": lambda given, session: BigQueryReader(TABLE, spark=session, client=given),
    "watermark reader": lambda given, session: BigQueryReader(
        TABLE,
        spark=session,
        watermark_column="updated_at",
        watermark_value=41,
        billing_project="acme-billing",
        client=given,
    ),
    "writer": lambda given, session: BigQueryWriter(
        TABLE, billing_project="acme-billing", client=given
    ),
}


@pytest.mark.parametrize("given", [False, True], ids=["without-client", "with-client"])
@pytest.mark.parametrize("build", list(AT009_BUILDS.values()), ids=list(AT009_BUILDS))
def test_at009_building_creates_no_client_and_touches_neither_spark_nor_the_client(
    build: Build,
    given: bool,
    log: _Log,
    spark: _FakeSpark,
    client: _FakeClient,
    factory: _FakeClientFactory,
) -> None:
    build(_client(client) if given else None, _session(spark))

    assert factory.calls == []
    assert log.spark_attributes == []
    assert log.client_attributes == []
    assert log.events == []


def test_at010_repr_shows_the_table_name_as_given() -> None:
    reader_repr = "BigQueryReader(table_name='acme-data.sales.orders')"
    assert repr(BigQueryReader(TABLE)) == reader_repr
    assert (
        repr(
            BigQueryReader(
                TABLE,
                watermark_column="updated_at",
                watermark_value=41,
                billing_project="acme-billing",
                options={"maxParallelism": 8},
            )
        )
        == reader_repr
    )
    writer_repr = "BigQueryWriter(table_name='acme-data.lake.orders')"
    assert repr(BigQueryWriter("acme-data.lake.orders")) == writer_repr
    assert (
        repr(
            BigQueryWriter(
                "acme-data.lake.orders",
                billing_project="acme-billing",
                options={"writeMethod": "indirect"},
            )
        )
        == writer_repr
    )


# --- Full read ----------------------------------------------------------------------------


@pytest.mark.parametrize("given", [False, True], ids=["without-client", "with-client"])
def test_at011_a_full_read_is_one_load_of_the_table_and_no_client(
    given: bool,
    log: _Log,
    spark: _FakeSpark,
    client: _FakeClient,
    factory: _FakeClientFactory,
) -> None:
    reader = BigQueryReader(TABLE, spark=_session(spark), client=_client(client) if given else None)

    frame = reader.read()

    assert _transcript(log) == [("load", "bigquery", {"table": TABLE}, (), {})]
    (load,) = _loads(log)
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    assert factory.calls == []
    assert log.client_attributes == []


def test_at012_the_billing_project_is_the_parent_project_and_the_project_of_the_client(
    log: _Log, spark: _FakeSpark, factory: _FakeClientFactory
) -> None:
    BigQueryReader(TABLE, spark=_session(spark), billing_project="acme-billing").read()
    BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=41,
        billing_project="acme-billing",
    ).read()

    full, watermark = _loads(log)
    assert full.options.get("parentProject") == "acme-billing", full.options
    assert watermark.options.get("parentProject") == "acme-billing", watermark.options
    assert factory.calls == [((), {"project": "acme-billing"})]


def test_at012_without_a_billing_project_no_load_has_a_parent_project_and_the_client_none(
    log: _Log, spark: _FakeSpark, factory: _FakeClientFactory
) -> None:
    BigQueryReader(TABLE, spark=_session(spark)).read()
    BigQueryReader(
        TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=41
    ).read()

    loads = _loads(log)
    assert len(loads) == 2
    for load in loads:
        assert "parentproject" not in _lower(load.options), load.options
    assert factory.calls == [((), {"project": None})]


AT013_OPTIONS: dict[str, str | int | float | bool] = {
    "maxParallelism": 8,
    "ReadDataFormat": "AVRO",
    "viewsEnabled": True,
}


def test_at013_read_options_reach_the_load_as_given(
    log: _Log, spark: _FakeSpark, factory: _FakeClientFactory
) -> None:
    BigQueryReader(TABLE, spark=_session(spark), options=AT013_OPTIONS).read()

    (load,) = _loads(log)
    assert _lower(load.options) == {
        "table": TABLE,
        "maxparallelism": "8",
        "readdataformat": "AVRO",
        "viewsenabled": "true",
    }
    for key in AT013_OPTIONS:
        assert key in load.options, load.options


def test_at013_a_watermark_read_sends_the_options_to_the_load_and_none_to_the_query(
    log: _Log, spark: _FakeSpark, factory: _FakeClientFactory
) -> None:
    BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=41,
        options=AT013_OPTIONS,
    ).read()

    (load,) = _loads(log)
    assert _lower(load.options) == {
        "table": TABLE,
        "filter": f"`updated_at` > 41 AND `updated_at` <= {BOUND_TEXT}",
        "maxparallelism": "8",
        "readdataformat": "AVRO",
        "viewsenabled": "true",
    }
    for key in AT013_OPTIONS:
        assert key in load.options, load.options
    (query,) = _calls(log, "client", "query")
    assert query.args == (_bound_query(literal="41"),)
    assert query.kwargs == {}


def test_at014_the_active_session_is_looked_up_when_read_runs_not_when_built(
    log: _Log, factory: _FakeClientFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("active")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = BigQueryReader(TABLE)
    BigQueryReader(TABLE, watermark_column="updated_at", watermark_value=41)
    assert lookups == []

    session = _FakeSpark(log)
    monkeypatch.setattr(SparkSession, "active", lambda: _session(session))
    frame = reader.read()

    (load,) = _loads(log)
    assert load.spark is session
    assert frame is _frame(load.frame)


def test_at014_without_an_active_session_a_watermark_read_raises_before_any_client(
    log: _Log, factory: _FakeClientFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = BigQueryReader(TABLE, watermark_column="updated_at", watermark_value=41)
    err = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise err

    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert factory.calls == []
    assert log.events == []


# --- Watermark read -----------------------------------------------------------------------


def test_at015_a_watermark_read_captures_a_bound_closes_the_client_then_loads_the_slice(
    log: _Log, spark: _FakeSpark, client: _FakeClient, factory: _FakeClientFactory
) -> None:
    frame = BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    query = (
        "SELECT IF(MAX(`updated_at`) IS NULL, NULL, FORMAT('%T', MAX(`updated_at`)))"
        " AS upper_bound FROM `acme-data`.`sales`.`orders`"
        " WHERE `updated_at` > TIMESTAMP '2026-10-01 00:00:00+00:00'"
    )
    condition = (
        "`updated_at` > TIMESTAMP '2026-10-01 00:00:00+00:00'"
        ' AND `updated_at` <= TIMESTAMP "2026-10-21 09:30:00.5+00"'
    )
    assert _transcript(log) == [
        _factory(None),
        _query(query),
        RESULT,
        CLOSE,
        ("load", "bigquery", {"table": TABLE, "filter": condition}, (), {}),
    ]
    (job,) = _jobs(client)
    (result,) = _calls(log, "job")
    assert result.owner is job
    (load,) = _loads(log)
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []


def test_at016_without_a_watermark_value_the_slice_ends_at_the_bound(
    log: _Log, spark: _FakeSpark, factory: _FakeClientFactory
) -> None:
    frame = BigQueryReader(TABLE, spark=_session(spark), watermark_column="updated_at").read()

    (query,) = _calls(log, "client", "query")
    assert query.args == (
        "SELECT IF(MAX(`updated_at`) IS NULL, NULL, FORMAT('%T', MAX(`updated_at`)))"
        " AS upper_bound FROM `acme-data`.`sales`.`orders`",
    )
    (load,) = _loads(log)
    assert _lower(load.options) == {
        "table": TABLE,
        "filter": '`updated_at` <= TIMESTAMP "2026-10-21 09:30:00.5+00"',
    }
    assert frame is _frame(load.frame)


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
@pytest.mark.parametrize("rows", [[(None,)], []], ids=["null-bound", "no-row"])
def test_at017_without_a_bound_the_filter_is_false(
    rows: list[tuple[object, ...]],
    value: int | None,
    log: _Log,
    spark: _FakeSpark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, log, _FakeClient(log, rows=rows))

    frame = BigQueryReader(
        TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=value
    ).read()

    (load,) = _loads(log)
    assert _lower(load.options) == {"table": TABLE, "filter": "FALSE"}
    assert frame is _frame(load.frame)


@pytest.mark.parametrize(
    ("bound", "text"),
    [
        pytest.param('DATE "2026-10-21"', 'DATE "2026-10-21"', id="date"),
        pytest.param("41", "41", id="int64"),
        pytest.param('NUMERIC "12.5"', 'NUMERIC "12.5"', id="numeric"),
        pytest.param(r'"it\'s"', r'"it\'s"', id="string"),
    ],
)
def test_at018_the_bound_goes_into_the_filter_exactly_as_bigquery_gave_it(
    bound: str, text: str, log: _Log, spark: _FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, log, _FakeClient(log, rows=[(bound,)]))

    BigQueryReader(
        TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=41
    ).read()

    (load,) = _loads(log)
    assert load.options["filter"] == f"`updated_at` > 41 AND `updated_at` <= {text}"


@pytest.mark.parametrize(
    ("value", "literal"),
    [
        pytest.param(41, "41", id="int"),
        pytest.param(0, "0", id="zero"),
        pytest.param(-7, "-7", id="negative-int"),
        pytest.param(Decimal("12.50"), "BIGNUMERIC '12.50'", id="decimal"),
        pytest.param(Decimal("1E+3"), "BIGNUMERIC '1000'", id="decimal-exponent"),
        pytest.param("O'Brien", r"'O\'Brien'", id="single-quote"),
        pytest.param("", "''", id="empty-string"),
        pytest.param("a\\b", r"'a\\b'", id="backslash"),
        pytest.param("a\n\rb", r"'a\n\rb'", id="line-feed-and-carriage-return"),
        pytest.param(date(2026, 10, 1), "DATE '2026-10-01'", id="date"),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, tzinfo=UTC),
            "TIMESTAMP '2026-10-01 08:30:00+00:00'",
            id="utc",
        ),
        pytest.param(
            datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=MINUS_3),
            "TIMESTAMP '2026-10-01 11:30:00.123456+00:00'",
            id="offset",
        ),
    ],
)
def test_at019_watermark_values_become_googlesql_literals_of_their_type(
    value: int | Decimal | str | date,
    literal: str,
    log: _Log,
    spark: _FakeSpark,
    factory: _FakeClientFactory,
) -> None:
    BigQueryReader(
        TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=value
    ).read()

    (query,) = _calls(log, "client", "query")
    assert query.args == (_bound_query(literal=literal),)
    (load,) = _loads(log)
    assert load.options["filter"] == (f"`updated_at` > {literal} AND `updated_at` <= {BOUND_TEXT}")


@pytest.mark.parametrize(
    ("table_name", "column", "quoted_table", "quoted_column"),
    [
        pytest.param(TABLE, "updated_at", TARGET, UPDATED_AT, id="plain-names"),
        pytest.param(
            "acme-data.Sales.Orders",
            "UpdatedAt",
            "`acme-data`.`Sales`.`Orders`",
            "`UpdatedAt`",
            id="case-kept",
        ),
        pytest.param(
            "acme-data.sales.order lines",
            "my`col",
            "`acme-data`.`sales`.`order lines`",
            r"`my\`col`",
            id="space-and-backtick",
        ),
        pytest.param(TABLE, "a\\b", TARGET, r"`a\\b`", id="backslash"),
    ],
)
def test_at020_table_and_column_names_are_quoted_identifiers_in_sql_only(
    table_name: str,
    column: str,
    quoted_table: str,
    quoted_column: str,
    log: _Log,
    spark: _FakeSpark,
    factory: _FakeClientFactory,
) -> None:
    BigQueryReader(
        table_name, spark=_session(spark), watermark_column=column, watermark_value=1
    ).read()
    BigQueryReader(table_name, spark=_session(spark)).read()

    (query,) = _calls(log, "client", "query")
    assert query.args == (_bound_query(quoted_column, literal="1", source=quoted_table),)
    watermark, full = _loads(log)
    assert _lower(watermark.options) == {
        "table": table_name,
        "filter": f"{quoted_column} > 1 AND {quoted_column} <= {BOUND_TEXT}",
    }
    assert _lower(full.options) == {"table": table_name}


def test_at021_a_client_given_serves_every_read_and_is_never_closed(
    log: _Log, spark: _FakeSpark, client: _FakeClient, factory: _FakeClientFactory
) -> None:
    reader = BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        billing_project="acme-billing",
        client=_client(client),
    )

    reader.read()
    reader.read()

    assert factory.calls == []
    assert _kinds(log) == ["client.query", "job.result", "load"] * 2
    assert _calls(log, "client", "close") == []
    loads = _loads(log)
    assert len(loads) == 2
    for load in loads:
        assert load.options.get("parentProject") == "acme-billing", load.options


@pytest.mark.parametrize(
    ("failure", "kinds"),
    [
        pytest.param("factory", ["factory"], id="factory"),
        pytest.param("query", ["factory", "client.query", "client.close"], id="query"),
        pytest.param(
            "result", ["factory", "client.query", "job.result", "client.close"], id="result"
        ),
        pytest.param(
            "load",
            ["factory", "client.query", "job.result", "client.close", "load"],
            id="load",
        ),
    ],
)
def test_at022_a_failed_watermark_read_raises_the_error_and_closes_the_client_it_created(
    failure: str, kinds: list[str], log: _Log, client: _FakeClient, factory: _FakeClientFactory
) -> None:
    err = RuntimeError(f"{failure} failed")
    spark = _FakeSpark(log, load_error=err if failure == "load" else None)
    if failure == "factory":
        factory.error = err
    elif failure in ("query", "result"):
        _fail(client, failure, err)
    reader = BigQueryReader(
        TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=41
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()

    assert excinfo.value is err
    assert _kinds(log) == kinds


def test_at022_a_client_given_is_not_closed_when_its_query_fails(
    log: _Log, spark: _FakeSpark, client: _FakeClient, factory: _FakeClientFactory
) -> None:
    err = RuntimeError("query failed")
    _fail(client, "query", err)
    reader = BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=41,
        client=_client(client),
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()

    assert excinfo.value is err
    assert _kinds(log) == ["client.query"]


def test_at022_a_failed_full_read_raises_the_error_of_the_load(
    log: _Log, factory: _FakeClientFactory
) -> None:
    err = RuntimeError("Not found: Table acme-data:sales.orders")
    spark = _FakeSpark(log, load_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        BigQueryReader(TABLE, spark=_session(spark)).read()

    assert excinfo.value is err
    assert _kinds(log) == ["load"]


# --- Writing ------------------------------------------------------------------------------


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at023_write_loads_a_staging_table_then_appends_it_in_one_insert(
    call: WriteCall, log: _Log, client: _FakeClient, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    result = call(BigQueryWriter(TABLE), _frame(df))

    assert result is None
    stg = _staging(log)
    hex_digits = stg.table.table_id.removeprefix("ingestio_staging_")
    statement = (
        "INSERT INTO `acme-data`.`sales`.`orders` (`id`, `payload`) SELECT `id`, `payload`"
        f" FROM `acme-data`.`sales`.`ingestio_staging_{hex_digits}`"
    )
    assert _transcript(log) == _write_transcript(stg, statement)
    assert [_summary(schema_field) for schema_field in stg.table.schema] == [
        _expected("id", "INTEGER"),
        _expected("payload", "STRING"),
    ]
    (job,) = _jobs(client)
    (result_call,) = _calls(log, "job")
    assert result_call.owner is job
    (save,) = _saves(log)
    assert save.frame is df
    assert set(_attributes(df)) == {"columns", "write"}, _attributes(df)


def test_at024_write_with_replace_applies_the_staging_table_in_one_merge(
    log: _Log, client: _FakeClient, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(BigQueryWriter(TABLE), _frame(df))

    assert result is None
    stg = _staging(log)
    hex_digits = stg.table.table_id.removeprefix("ingestio_staging_")
    statement = (
        "MERGE `acme-data`.`sales`.`orders` AS T USING"
        f" `acme-data`.`sales`.`ingestio_staging_{hex_digits}` AS S ON FALSE"
        " WHEN NOT MATCHED THEN INSERT (`id`, `payload`) VALUES (S.`id`, S.`payload`)"
        " WHEN NOT MATCHED BY SOURCE THEN DELETE"
    )
    assert _transcript(log) == _write_transcript(stg, statement)
    assert [_summary(schema_field) for schema_field in stg.table.schema] == [
        _expected("id", "INTEGER"),
        _expected("payload", "STRING"),
    ]
    (save,) = _saves(log)
    assert save.frame is df
    assert set(_attributes(df)) == {"columns", "write"}, _attributes(df)


AT025_TARGET = (
    SchemaField("id", "INTEGER", mode="REQUIRED", description="order key"),
    SchemaField(
        "Payload",
        "RECORD",
        fields=[
            SchemaField("sku", "STRING", mode="REQUIRED", max_length=20),
            SchemaField("tags", "STRING", mode="REPEATED"),
        ],
    ),
    SchemaField(
        "amount",
        "NUMERIC",
        precision=10,
        scale=2,
        policy_tags=PolicyTagList(["projects/p/locations/us/taxonomies/1/policyTags/2"]),
    ),
    SchemaField("loaded_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()"),
)


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at025_the_staging_schema_follows_the_dataframe_with_the_target_types_and_relaxed_modes(
    replace: bool, log: _Log, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, log, _FakeClient(log, table=_target_table(*AT025_TARGET)))
    frame = _FakeDataFrame(log, columns=["amount", "payload", "id"])

    BigQueryWriter(TABLE).write(_frame(frame), replace=replace)

    stg = _staging(log)
    assert [_summary(schema_field) for schema_field in stg.table.schema] == [
        _expected("amount", "NUMERIC", precision=10, scale=2),
        _expected(
            "payload",
            "RECORD",
            fields=[
                _expected("sku", "STRING", max_length=20),
                _expected("tags", "STRING", "REPEATED"),
            ],
        ),
        _expected("id", "INTEGER"),
    ]
    columns = "`amount`, `payload`, `id`"
    statement = (
        _merge(stg.quoted, columns, "S.`amount`, S.`payload`, S.`id`")
        if replace
        else _insert(stg.quoted, columns)
    )
    assert _calls(log, "client", "query")[0].args == (statement,)


def test_at025_a_column_whose_target_field_has_a_default_gets_none_in_the_staging_table(
    log: _Log, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(monkeypatch, log, _FakeClient(log, table=_target_table(*AT025_TARGET)))
    frame = _FakeDataFrame(log, columns=["loaded_at", "id"])

    BigQueryWriter(TABLE).write(_frame(frame))

    stg = _staging(log)
    assert [_summary(schema_field) for schema_field in stg.table.schema] == [
        _expected("loaded_at", "TIMESTAMP"),
        _expected("id", "INTEGER"),
    ]


def test_at026_each_write_gets_a_new_staging_table_that_expires_a_day_later(
    log: _Log, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    writer = BigQueryWriter(TABLE)

    t0 = datetime.now(UTC)
    writer.write(_frame(df))
    t1 = datetime.now(UTC)
    writer.write(_frame(df))

    first, second = _created_tables(log)
    for table in (first, second):
        assert STAGING_TABLE_ID.fullmatch(table.table_id), table.table_id
    assert first.table_id != second.table_id
    for table, save, query, delete in zip(
        (first, second),
        _saves(log),
        _calls(log, "client", "query"),
        _calls(log, "client", "delete_table"),
        strict=True,
    ):
        stg_id = f"acme-data.sales.{table.table_id}"
        assert _lower(save.options)["table"] == stg_id
        assert query.args == (_insert(f"`acme-data`.`sales`.`{table.table_id}`"),)
        assert delete.args == (stg_id,)

    lowest = t0 + timedelta(days=1)
    lowest = lowest.replace(microsecond=lowest.microsecond // 1000 * 1000)
    assert first.expires is not None
    assert lowest <= first.expires <= t1 + timedelta(days=1), (t0, first.expires, t1)


def test_at027_the_billing_project_and_user_options_reach_the_client_and_the_save(
    log: _Log, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    writer = BigQueryWriter(
        TABLE,
        billing_project="acme-billing",
        options={"WriteMethod": "indirect", "temporaryGcsBucket": "acme-tmp"},
    )

    writer.write(_frame(df))

    stg = _staging(log)
    save_line: _Line = (
        "save",
        "bigquery",
        "append",
        {
            "table": stg.id,
            "parentproject": "acme-billing",
            "writemethod": "indirect",
            "temporarygcsbucket": "acme-tmp",
        },
        (),
        {},
    )
    assert _transcript(log) == _write_transcript(
        stg, _insert(stg.quoted), project="acme-billing", save=save_line
    )
    (save,) = _saves(log)
    assert [key for key in save.options if key.lower() == "writemethod"] == ["WriteMethod"]
    assert save.options["WriteMethod"] == "indirect"
    assert save.options["temporaryGcsBucket"] == "acme-tmp"
    assert save.options["parentProject"] == "acme-billing"


def test_at027_without_user_options_the_connector_keys_keep_their_case(
    log: _Log, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    BigQueryWriter(TABLE, billing_project="acme-billing").write(_frame(df))

    (save,) = _saves(log)
    assert save.options["writeMethod"] == "direct"
    assert save.options["parentProject"] == "acme-billing"
    assert factory.calls == [((), {"project": "acme-billing"})]


def test_at028_a_client_given_serves_every_write_and_is_never_closed(
    log: _Log, client: _FakeClient, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    writer = BigQueryWriter(TABLE, client=_client(client))

    writer.write(_frame(df))
    appended = _transcript(log)
    append_stg = _staging(log)
    log.events.clear()
    writer.write(_frame(df), replace=True)

    replace_stg = _staging(log)
    assert factory.calls == []
    assert appended == _write_transcript(append_stg, _insert(append_stg.quoted), given=True)
    assert _transcript(log) == _write_transcript(
        replace_stg, _merge(replace_stg.quoted), given=True
    )
    for call in _calls(log, "client"):
        assert call.owner is client


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_at029_a_failed_save_drops_the_staging_table_and_raises_the_error(
    replace: bool, log: _Log, factory: _FakeClientFactory
) -> None:
    err = RuntimeError("executor lost")
    frame = _FakeDataFrame(log, columns=["id", "payload"], save_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        BigQueryWriter(TABLE).write(_frame(frame), replace=replace)

    assert excinfo.value is err
    stg = _staging(log)
    assert _transcript(log) == [
        _factory(None),
        GET_TARGET,
        ("client.create_table", (("Table", stg.id),), {}),
        _save(stg),
        _delete(stg),
        CLOSE,
    ]


@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
@pytest.mark.parametrize("failure", ["query", "result"])
def test_at030_a_failed_statement_drops_the_staging_table_and_raises_the_error(
    failure: str,
    replace: bool,
    log: _Log,
    client: _FakeClient,
    factory: _FakeClientFactory,
    df: _FakeDataFrame,
) -> None:
    err = RuntimeError("Query error: Required field id cannot be null")
    _fail(client, failure, err)

    with pytest.raises(RuntimeError) as excinfo:
        BigQueryWriter(TABLE).write(_frame(df), replace=replace)

    assert excinfo.value is err
    stg = _staging(log)
    statement = _merge(stg.quoted) if replace else _insert(stg.quoted)
    result = [RESULT] if failure == "result" else []
    assert _transcript(log) == [
        _factory(None),
        GET_TARGET,
        ("client.create_table", (("Table", stg.id),), {}),
        _save(stg),
        _query(statement),
        *result,
        _delete(stg),
        CLOSE,
    ]


@pytest.mark.parametrize(
    ("failure", "kinds"),
    [
        pytest.param("factory", ["factory"], id="factory"),
        pytest.param("get_table", ["factory", "client.get_table", "client.close"], id="get_table"),
        pytest.param(
            "create_table",
            ["factory", "client.get_table", "client.create_table", "client.close"],
            id="create_table",
        ),
    ],
)
def test_at031_a_failed_setup_raises_the_error_and_moves_no_data(
    failure: str,
    kinds: list[str],
    log: _Log,
    client: _FakeClient,
    factory: _FakeClientFactory,
    df: _FakeDataFrame,
) -> None:
    err = RuntimeError(f"{failure} failed")
    if failure == "factory":
        factory.error = err
    else:
        _fail(client, failure, err)

    with pytest.raises(RuntimeError) as excinfo:
        BigQueryWriter(TABLE).write(_frame(df))

    assert excinfo.value is err
    assert _kinds(log) == kinds
    assert _saves(log) == []
    if failure == "factory":
        assert log.client_attributes == []


@pytest.mark.parametrize("failure", ["save", "query", "result"])
def test_at032_a_failed_cleanup_after_a_failed_write_becomes_a_note_on_the_error(
    failure: str, log: _Log, client: _FakeClient, factory: _FakeClientFactory
) -> None:
    err = RuntimeError(f"{failure} failed")
    frame = _FakeDataFrame(
        log, columns=["id", "payload"], save_error=err if failure == "save" else None
    )
    if failure != "save":
        _fail(client, failure, err)
    _fail(client, "delete_table", RuntimeError("refused"))

    with pytest.raises(RuntimeError) as excinfo:
        BigQueryWriter(TABLE).write(_frame(frame))

    assert excinfo.value is err
    stg = _staging(log)
    assert err.__notes__ == [f"staging table {stg.id} could not be dropped: RuntimeError: refused"]
    assert _transcript(log)[-2:] == [_delete(stg), CLOSE]


def test_at033_a_failed_drop_after_a_committed_statement_is_not_raised(
    log: _Log, client: _FakeClient, factory: _FakeClientFactory, df: _FakeDataFrame
) -> None:
    _fail(client, "delete_table", RuntimeError("refused"))
    write = APPEND_CALLS["write(df)"]

    result = write(BigQueryWriter(TABLE), _frame(df))

    assert result is None
    stg = _staging(log)
    assert _transcript(log) == _write_transcript(stg, _insert(stg.quoted))


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at034_a_dataframe_without_columns_is_rejected_before_anything_runs(
    call: WriteCall, log: _Log, factory: _FakeClientFactory
) -> None:
    frame = _FakeDataFrame(log, columns=[])

    with pytest.raises(ValueError, match=_exactly("the DataFrame has no columns")):
        call(BigQueryWriter(TABLE), _frame(frame))

    assert factory.calls == []
    assert log.events == []


def test_at035_a_column_the_target_lacks_is_rejected_after_reading_the_target(
    log: _Log, factory: _FakeClientFactory
) -> None:
    frame = _FakeDataFrame(log, columns=["id", "extra", "other"])

    with pytest.raises(
        ValueError, match=_exactly("table 'acme-data.sales.orders' has no column 'extra'")
    ):
        BigQueryWriter(TABLE).write(_frame(frame))

    assert _transcript(log) == [_factory(None), GET_TARGET, CLOSE]


def test_at035_columns_match_the_target_in_any_case_and_keep_the_dataframe_spelling(
    log: _Log, factory: _FakeClientFactory
) -> None:
    frame = _FakeDataFrame(log, columns=["ID", "Payload"])

    BigQueryWriter(TABLE).write(_frame(frame))

    stg = _staging(log)
    assert [_summary(schema_field) for schema_field in stg.table.schema] == [
        _expected("ID", "INTEGER"),
        _expected("Payload", "STRING"),
    ]
    assert _transcript(log) == _write_transcript(stg, _insert(stg.quoted, "`ID`, `Payload`"))


def test_at036_column_names_are_quoted_identifiers_in_the_statement(
    log: _Log, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _target_table(SchemaField("order id", "STRING"), SchemaField("a`b", "STRING"))
    _install(monkeypatch, log, _FakeClient(log, table=target))
    frame = _FakeDataFrame(log, columns=["order id", "a`b"])

    BigQueryWriter(TABLE).write(_frame(frame))

    stg = _staging(log)
    hex_digits = stg.table.table_id.removeprefix("ingestio_staging_")
    (query,) = _calls(log, "client", "query")
    assert query.args == (
        r"INSERT INTO `acme-data`.`sales`.`orders` (`order id`, `a\`b`)"
        r" SELECT `order id`, `a\`b`"
        f" FROM `acme-data`.`sales`.`ingestio_staging_{hex_digits}`",
    )


# --- Composition --------------------------------------------------------------------------


def test_at037_read_source_and_write_target_move_a_dataframe_and_append(
    log: _Log, factory: _FakeClientFactory
) -> None:
    spark = _FakeSpark(log, columns=["id", "payload"])
    source: Reader[DataFrame] = BigQueryReader(TABLE, spark=_session(spark))
    target: Writer[DataFrame] = BigQueryWriter(TABLE)

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
    assert _transcript(log)[1:] == _write_transcript(stg, _insert(stg.quoted))
    (save,) = _saves(log)
    assert save.frame is load.frame


def test_at038_the_connectors_write_no_log_lines(
    log: _Log,
    spark: _FakeSpark,
    client: _FakeClient,
    factory: _FakeClientFactory,
    df: _FakeDataFrame,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")

    BigQueryReader(TABLE, spark=_session(spark)).read()
    BigQueryReader(
        TABLE,
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
        billing_project="acme-billing",
    ).read()
    writer = BigQueryWriter(TABLE, options={"writeMethod": "indirect"})
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    failing = _FakeDataFrame(log, columns=["id", "payload"], save_error=RuntimeError("lost"))
    _fail(client, "delete_table", RuntimeError("refused"))
    with pytest.raises(RuntimeError):
        writer.write(_frame(failing))
    writer.write(_frame(df))
    _fail(client, "query", RuntimeError("query failed"))
    with pytest.raises(RuntimeError):
        BigQueryReader(
            TABLE, spark=_session(spark), watermark_column="updated_at", watermark_value=41
        ).read()

    assert len(_loads(log)) == 2
    assert len(_saves(log)) == 4
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

DATASET = os.environ.get("INGESTIO_BIGQUERY_DATASET", "")
SPARK_PACKAGE = os.environ.get("INGESTIO_BIGQUERY_SPARK_PACKAGE", "")

needs_bigquery = pytest.mark.skipif(
    not DATASET,
    reason=(
        "INGESTIO_BIGQUERY_DATASET is not set: it names the dataset, project.dataset, where"
        " the tests against BigQuery create and drop their tables"
    ),
)
needs_spark = pytest.mark.skipif(
    os.environ.get("INGESTIO_SPARK_TESTS") != "1",
    reason="INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in",
)
needs_connector = pytest.mark.skipif(
    not SPARK_PACKAGE,
    reason=(
        "INGESTIO_BIGQUERY_SPARK_PACKAGE is not set: it names the spark-bigquery-connector"
        " build for the installed pyspark, such as"
        " com.google.cloud.spark:spark-3.5-bigquery:<version>"
    ),
)


@dataclass(frozen=True)
class _Dataset:
    """The dataset of `INGESTIO_BIGQUERY_DATASET`, and a client billed to its project."""

    project: str
    dataset: str
    client: bigquery.Client

    def run(self, statement: str) -> list[tuple[Any, ...]]:
        """Run `statement` and return its rows as tuples."""
        return [tuple(row.values()) for row in self.client.query(statement).result()]

    def staging_tables(self) -> set[str]:
        """The IDs of the tables of the dataset whose name starts with `ingestio_staging_`."""
        return {
            item.table_id
            for item in self.client.list_tables(f"{self.project}.{self.dataset}")
            if item.table_id.startswith("ingestio_staging_")
        }


def _quoted(table_id: str) -> str:
    """A full table ID with no special characters but `-`, quoted for GoogleSQL."""
    return "`" + table_id.replace(".", "`.`") + "`"


@pytest.fixture(scope="module")
def bq() -> Iterator[_Dataset]:
    """The dataset of `INGESTIO_BIGQUERY_DATASET`, with a real client of its own."""
    project, _, dataset = DATASET.partition(".")
    assert project, "INGESTIO_BIGQUERY_DATASET has no project"
    assert dataset, "INGESTIO_BIGQUERY_DATASET has no dataset"
    client = bigquery.Client(project=project)
    try:
        yield _Dataset(project, dataset, client)
    finally:
        client.close()  # type: ignore[no-untyped-call]


@pytest.fixture
def names(bq: _Dataset) -> Iterator[Callable[[str], str]]:
    """Unique table IDs in the dataset; the tables they name are dropped afterwards."""
    created: list[str] = []

    def name(stem: str) -> str:
        table_id = f"{bq.project}.{bq.dataset}.ingestio_it_{stem}_{uuid.uuid4().hex[:12]}"
        created.append(table_id)
        return table_id

    yield name
    for table_id in reversed(created):
        bq.client.delete_table(table_id, not_found_ok=True)


def _loading(
    bq: _Dataset, log: _Log, columns: Sequence[str], rows: Sequence[dict[str, Any]]
) -> tuple[_FakeDataFrame, list[Table]]:
    """A fake DataFrame whose save loads `rows` into the table its `table` option names.

    The load is a load job of the real client, with the schema of that table, so the rows
    take the staging table's types. The second item collects the staging table as it was
    during each save, read with `get_table`.
    """
    seen: list[Table] = []

    def load(options: Mapping[str, str]) -> None:
        table_id = _lower(options)["table"]
        staging = bq.client.get_table(table_id)
        seen.append(staging)
        config = bigquery.LoadJobConfig(
            schema=staging.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        bq.client.load_table_from_json(list(rows), table_id, job_config=config).result()

    return _FakeDataFrame(log, columns=columns, on_save=load), seen


@dataclass(frozen=True)
class _Typed:
    """A watermark column type and the rows of its table, by their watermark literals.

    `value` is the watermark value of the read, and the last `above` rows are above it.
    `beyond` is a value above every row, and `later` the literal of a watermark above every
    row, for a row inserted after a read.
    """

    column_type: str
    literals: tuple[str, ...]
    value: int | Decimal | str | date
    above: int
    beyond: int | Decimal | str | date
    later: str


TIMESTAMP_ROWS = _Typed(
    "TIMESTAMP",
    (
        "TIMESTAMP '2026-10-01 00:00:00+00'",
        "TIMESTAMP '2026-10-02 12:00:00.123456+00'",
        "TIMESTAMP '2026-10-03 00:00:00.654321+00'",
    ),
    # 21:00 at -03:00 is 2026-10-02 00:00:00 UTC.
    datetime(2026, 10, 1, 21, 0, tzinfo=MINUS_3),
    2,
    datetime(2027, 1, 1, tzinfo=UTC),
    "TIMESTAMP '2026-10-04 00:00:00+00'",
)
DATE_ROWS = _Typed(
    "DATE",
    ("DATE '2026-10-01'", "DATE '2026-10-02'", "DATE '2026-10-03'"),
    date(2026, 10, 1),
    2,
    date(2027, 1, 1),
    "DATE '2026-10-04'",
)
DATETIME_ROWS = _Typed(
    "DATETIME",
    (
        "DATETIME '2026-10-01 11:30:00'",
        "DATETIME '2026-10-01 11:30:00.000001'",
        "DATETIME '2026-10-02 08:00:00.654321'",
    ),
    "2026-10-01 11:30:00",
    2,
    "2027-01-01 00:00:00",
    "DATETIME '2026-10-03 00:00:00'",
)
INT64_ROWS = _Typed(
    "INT64", ("10", "20", "9007199254740993"), 10, 2, 9007199254740994, "9007199254740994"
)
NUMERIC_ROWS = _Typed(
    "NUMERIC",
    (
        "NUMERIC '1.000000001'",
        "NUMERIC '1.000000002'",
        "NUMERIC '12345678901234567890.123456789'",
    ),
    Decimal("1.000000001"),
    2,
    Decimal("1E+25"),
    "NUMERIC '99999999999999999999.1'",
)
STRING_ROWS = _Typed(
    "STRING",
    ("'A'", '"O\'Brien"', '"O\'Brien x"', '"Z it\'s"'),
    "O'Brien",
    2,
    "zzz",
    "'zzz'",
)

TYPED = [
    pytest.param(TIMESTAMP_ROWS, id="timestamp"),
    pytest.param(DATE_ROWS, id="date"),
    pytest.param(DATETIME_ROWS, id="datetime-str"),
    pytest.param(INT64_ROWS, id="int64"),
    pytest.param(NUMERIC_ROWS, id="numeric"),
    pytest.param(STRING_ROWS, id="string"),
]


def _seed_typed(bq: _Dataset, table_id: str, typed: _Typed) -> None:
    """Create `table_id` with `id` and a watermark column `w` of the type, then insert one row
    per literal and a row whose watermark is `NULL`."""
    bq.run(f"CREATE TABLE {_quoted(table_id)} (id INT64, w {typed.column_type})")
    values = [f"({index}, {literal})" for index, literal in enumerate(typed.literals, start=1)]
    values.append(f"({len(typed.literals) + 1}, NULL)")
    bq.run(f"INSERT INTO {_quoted(table_id)} (id, w) VALUES {', '.join(values)}")


def _filter_of_read(bq: _Dataset, table_id: str, value: int | Decimal | str | date) -> str:
    """The `filter` that a watermark read of `w` above `value` gives the data load."""
    log = _Log()
    BigQueryReader(
        table_id,
        watermark_column="w",
        watermark_value=value,
        billing_project=bq.project,
        spark=_session(_FakeSpark(log)),
    ).read()
    (load,) = _loads(log)
    return _lower(load.options)["filter"]


@pytest.mark.integration
@needs_bigquery
@pytest.mark.parametrize("typed", TYPED)
def test_the_filter_of_a_watermark_read_selects_exactly_the_rows_above_the_value(
    typed: _Typed, bq: _Dataset, names: Callable[[str], str]
) -> None:
    table_id = names("typed")
    _seed_typed(bq, table_id, typed)

    condition = _filter_of_read(bq, table_id, typed.value)

    assert bq.run(f"SELECT COUNT(*) FROM {_quoted(table_id)} WHERE {condition}") == [(typed.above,)]
    null_row = len(typed.literals) + 1
    ids = bq.run(f"SELECT id FROM {_quoted(table_id)} WHERE {condition} ORDER BY id")
    assert (null_row,) not in ids
    assert _filter_of_read(bq, table_id, typed.beyond) == "FALSE"


@pytest.mark.integration
@needs_bigquery
def test_a_table_requiring_a_partition_filter_takes_a_watermark_read_with_a_value_only(
    bq: _Dataset, names: Callable[[str], str]
) -> None:
    table_id = names("required_filter")
    bq.run(
        f"CREATE TABLE {_quoted(table_id)} (id INT64, day DATE) PARTITION BY day"
        " OPTIONS (require_partition_filter = TRUE)"
    )
    bq.run(
        f"INSERT INTO {_quoted(table_id)} (id, day)"
        " VALUES (1, DATE '2026-10-01'), (2, DATE '2026-10-02'), (3, DATE '2026-10-03')"
    )
    log = _Log()

    BigQueryReader(
        table_id,
        watermark_column="day",
        watermark_value=date(2026, 10, 1),
        billing_project=bq.project,
        spark=_session(_FakeSpark(log)),
    ).read()

    (load,) = _loads(log)
    condition = _lower(load.options)["filter"]
    assert bq.run(f"SELECT COUNT(*) FROM {_quoted(table_id)} WHERE {condition}") == [(2,)]
    with pytest.raises(GoogleAPICallError, match=r"(?i)filter"):
        BigQueryReader(
            table_id,
            watermark_column="day",
            billing_project=bq.project,
            spark=_session(_FakeSpark(log)),
        ).read()


def _orders(bq: _Dataset, table_id: str) -> None:
    """Create the target of the write tests: a `REQUIRED` `id`, a `payload`, and a `loaded_at`
    that defaults to the current timestamp."""
    bq.run(
        f"CREATE TABLE {_quoted(table_id)} (id INT64 NOT NULL, payload STRING,"
        " loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"
    )


@pytest.mark.integration
@needs_bigquery
def test_write_appends_and_the_target_fills_the_defaults_the_dataframe_lacks(
    bq: _Dataset, names: Callable[[str], str]
) -> None:
    table_id = names("append")
    _orders(bq, table_id)
    bq.run(f"INSERT INTO {_quoted(table_id)} (id, payload) VALUES (1, 'old')")
    before = bq.staging_tables()
    frame, _ = _loading(
        bq, _Log(), ["id", "payload"], [{"id": 2, "payload": "a"}, {"id": 3, "payload": None}]
    )

    BigQueryWriter(table_id, billing_project=bq.project).write(_frame(frame))

    assert bq.run(
        f"SELECT id, payload, loaded_at IS NOT NULL FROM {_quoted(table_id)} ORDER BY id"
    ) == [(1, "old", True), (2, "a", True), (3, None, True)]
    assert bq.staging_tables() <= before


@pytest.mark.integration
@needs_bigquery
def test_write_with_replace_keeps_the_partitioning_clustering_and_description(
    bq: _Dataset, names: Callable[[str], str]
) -> None:
    table_id = names("replace")
    bq.run(
        f"CREATE TABLE {_quoted(table_id)} (id INT64 NOT NULL, day DATE, payload STRING)"
        " PARTITION BY day CLUSTER BY id OPTIONS (description = 'orders by day')"
    )
    bq.run(
        f"INSERT INTO {_quoted(table_id)} (id, day, payload)"
        " VALUES (1, DATE '2026-10-01', 'a'), (2, DATE '2026-10-02', 'b')"
    )
    frame, _ = _loading(
        bq, _Log(), ["id", "day", "payload"], [{"id": 10, "day": "2026-10-05", "payload": "x"}]
    )

    BigQueryWriter(table_id, billing_project=bq.project).write(_frame(frame), replace=True)

    assert bq.run(f"SELECT id, day, payload FROM {_quoted(table_id)}") == [
        (10, date(2026, 10, 5), "x")
    ]
    table = bq.client.get_table(table_id)
    assert table.time_partitioning is not None
    assert table.time_partitioning.field == "day"
    assert table.clustering_fields == ["id"]
    assert table.description == "orders by day"


@pytest.mark.integration
@needs_bigquery
@pytest.mark.parametrize("replace", [False, True], ids=["append", "replace"])
def test_a_null_in_a_required_column_fails_the_write_and_leaves_the_target_unchanged(
    replace: bool, bq: _Dataset, names: Callable[[str], str]
) -> None:
    table_id = names("required")
    _orders(bq, table_id)
    bq.run(f"INSERT INTO {_quoted(table_id)} (id, payload) VALUES (1, 'old')")
    before = bq.staging_tables()
    frame, seen = _loading(bq, _Log(), ["id", "payload"], [{"id": None, "payload": "x"}])

    with pytest.raises(GoogleAPICallError):
        BigQueryWriter(table_id, billing_project=bq.project).write(_frame(frame), replace=replace)

    assert bq.run(f"SELECT id, payload FROM {_quoted(table_id)}") == [(1, "old")]
    (staging,) = seen
    assert staging.table_id not in bq.staging_tables()
    assert bq.staging_tables() <= before


@pytest.mark.integration
@needs_bigquery
def test_a_missing_target_fails_the_write_and_the_staging_table_lives_a_day_during_the_save(
    bq: _Dataset, names: Callable[[str], str]
) -> None:
    missing = names("missing")
    before = bq.staging_tables()
    log = _Log()
    never, unused = _loading(bq, log, ["id", "payload"], [{"id": 1, "payload": "a"}])

    with pytest.raises(NotFound):
        BigQueryWriter(missing, billing_project=bq.project).write(_frame(never))

    assert _saves(log) == []
    assert unused == []
    assert bq.staging_tables() <= before

    table_id = names("expires")
    _orders(bq, table_id)
    frame, seen = _loading(bq, _Log(), ["id", "payload"], [{"id": 1, "payload": "a"}])
    start = datetime.now(UTC)
    BigQueryWriter(table_id, billing_project=bq.project).write(_frame(frame))
    end = datetime.now(UTC)

    (staging,) = seen
    assert STAGING_TABLE_ID.fullmatch(staging.table_id), staging.table_id
    assert (staging.project, staging.dataset_id) == (bq.project, bq.dataset)
    assert staging.expires is not None
    slack = timedelta(minutes=5)
    assert start + timedelta(days=1) - slack <= staging.expires <= end + timedelta(days=1) + slack
    assert staging.table_id not in bq.staging_tables()


@pytest.fixture(scope="module")
def session() -> Iterator[SparkSession]:
    """A local Spark session in UTC, with spark-bigquery-connector."""
    if SparkContext._gateway is not None:
        pytest.skip(
            "a JVM was already started in this run, without spark-bigquery-connector: a JVM"
            " loads its packages once, so run this file on its own"
        )
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration-bigquery")
        .config("spark.jars.packages", SPARK_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield spark_session
    finally:
        spark_session.stop()


def _ids(frame: DataFrame) -> list[int]:
    """The sorted values of the `id` column of `frame`."""
    return sorted(int(row["id"]) for row in frame.collect())


@pytest.mark.integration
@needs_connector
@needs_spark
@needs_bigquery
def test_a_full_read_returns_every_row(
    bq: _Dataset, names: Callable[[str], str], session: SparkSession
) -> None:
    table_id = names("full")
    bq.run(f"CREATE TABLE {_quoted(table_id)} (id INT64, name STRING)")
    bq.run(f"INSERT INTO {_quoted(table_id)} (id, name) VALUES (1, 'a'), (2, 'b'), (3, NULL)")

    frame = BigQueryReader(table_id, billing_project=bq.project, spark=session).read()

    rows = sorted((int(row["id"]), row["name"]) for row in frame.collect())
    assert rows == [(1, "a"), (2, "b"), (3, None)]


SPARK_TYPED = [
    pytest.param(TIMESTAMP_ROWS, id="timestamp"),
    pytest.param(DATE_ROWS, id="date"),
    pytest.param(DATETIME_ROWS, id="datetime-str"),
    pytest.param(STRING_ROWS, id="string"),
]


@pytest.mark.integration
@needs_connector
@needs_spark
@needs_bigquery
@pytest.mark.parametrize("typed", SPARK_TYPED)
def test_a_watermark_read_returns_a_frozen_slice_through_the_storage_read_api(
    typed: _Typed, bq: _Dataset, names: Callable[[str], str], session: SparkSession
) -> None:
    table_id = names("slice")
    _seed_typed(bq, table_id, typed)
    expected = list(range(len(typed.literals) - typed.above + 1, len(typed.literals) + 1))

    frame = BigQueryReader(
        table_id,
        watermark_column="w",
        watermark_value=typed.value,
        billing_project=bq.project,
        spark=session,
    ).read()

    assert _ids(frame) == expected
    later = len(typed.literals) + 2
    bq.run(f"INSERT INTO {_quoted(table_id)} (id, w) VALUES ({later}, {typed.later})")
    assert _ids(frame) == expected
    first_run = BigQueryReader(
        table_id, watermark_column="w", billing_project=bq.project, spark=session
    ).read()
    assert _ids(first_run) == [*range(1, len(typed.literals) + 1), later]


@pytest.mark.integration
@needs_connector
@needs_spark
@needs_bigquery
def test_writes_with_the_direct_method_land_nullable_fields_in_required_columns(
    bq: _Dataset, names: Callable[[str], str], session: SparkSession
) -> None:
    table_id = names("direct")
    bq.run(f"CREATE TABLE {_quoted(table_id)} (id INT64 NOT NULL, name STRING NOT NULL)")
    writer = BigQueryWriter(table_id, billing_project=bq.project)
    schema = "id LONG, name STRING"

    writer.write(session.createDataFrame([(1, "a"), (2, "b")], schema))
    writer.write(session.createDataFrame([(3, "c")], schema))

    assert bq.run(f"SELECT id, name FROM {_quoted(table_id)} ORDER BY id") == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]

    writer.write(session.createDataFrame([(10, "x")], schema), replace=True)

    assert bq.run(f"SELECT id, name FROM {_quoted(table_id)}") == [(10, "x")]

    writer.write(session.createDataFrame([], schema), replace=True)

    assert bq.run(f"SELECT COUNT(*) FROM {_quoted(table_id)}") == [(0,)]
