"""Write strategies as callbacks: `AppendOnly`, `FullLoad`, and `JsonLinesWriter`'s `replace`.

Covers `specs/append_only_callback.md`. Every test writes the job's configuration with
`json.dumps` to a file under `tmp_path` and passes its path. `json_lines` writes under
`tmp_path`, `json_api` reads from the `api_url` fixture, and a missing package is simulated in a
fresh interpreter. A test that holds for either callback is parametrized by its name and reaches
it as `ingestio` exports it.

The other targets need no JVM, no cloud service and no database: the doubles below, a fake Spark
session, fake DataFrames, a fake BigQuery client and a fake `psycopg.connect`, write every call
they receive to one ordered log. A test that maps a target to its writer runs the callback over
one set of doubles and a *twin*, the writer built directly from `ingestio.connector.all`, over a
second set in the same initial state, and compares the two logs. The default clients of the SDKs
and `SparkSession.active` fail when used, so that no test reaches a real service.
"""

from __future__ import annotations

import inspect
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, NoReturn, TypeAlias, cast

import psycopg
import pytest
from google.cloud.bigquery import SchemaField, Table
from psycopg import sql
from pyspark.sql import DataFrame, SparkSession

import ingestio
from ingestio import Callback, Process, ReadSource, Step
from ingestio.connector.all import (
    BigQueryWriter,
    DiskWriter,
    GcsWriter,
    JsonLinesWriter,
    PostgresConnection,
    PostgresWriter,
    S3Writer,
    UnityCatalogWriter,
    VolumeWriter,
)

DAY = timedelta(days=1)
API = "https://api.example.com/v1/orders"
TARGET_NAMES = "bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume"
STAGING_NAME = re.compile(r"ingestio_staging_[0-9a-f]{32}")
COLUMNS = ("id", "payload")
OPTIONS_TYPE = "must be an object of strings, numbers and booleans"
EMPTY_ACCESSES = ("count", "isEmpty", "first", "head", "take", "collect")

# The required keys of each target system, in the order of the spec's table, with values its
# writer accepts.
TARGET_REQUIRED: dict[str, dict[str, object]] = {
    "json_lines": {"path": "/data/out/orders.jsonl"},
    "disk": {"path": "/data/lake/orders", "format": "parquet"},
    "postgres": {"table_name": "public.orders_copy"},
    "s3": {"path": "s3://acme-lake/orders", "format": "delta"},
    "gcs": {"path": "gs://acme-lake/orders", "format": "delta"},
    "volume": {"volume": "main.lake.curated", "path": "orders", "format": "parquet"},
    "unity_catalog": {"table_name": "main.silver.orders"},
    "bigquery": {"table_name": "acme-data.lake.orders"},
}

# The systems of the rows of AT-005.
WRITE_SYSTEMS = ("disk", "postgres", "s3", "gcs", "volume", "unity_catalog", "bigquery")

EITHER = pytest.mark.parametrize("name", ["AppendOnly", "FullLoad"])
SYSTEMS = pytest.mark.parametrize("system", WRITE_SYSTEMS)


def _strategy(name: str) -> Callable[..., Callback]:
    """The callback `name`, `AppendOnly` or `FullLoad`, as `ingestio` exports it."""
    if name == "AppendOnly":
        return ingestio.AppendOnly
    assert name == "FullLoad", name
    return ingestio.FullLoad


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Call:
    """A call one double received, or a property read on it.

    `target` is the double's label, `method` the method or property, and `result` the label of
    the double it returned, when it returned one.
    """

    target: str
    method: str
    args: tuple[object, ...] = ()
    kwargs: dict[str, object] = field(default_factory=dict)
    result: str | None = None


class _Log:
    """The ordered log that every double of one set writes to.

    `calls` holds every call and every property read, `accessed` every public attribute
    accessed on a double, and `fakes` each double by its label. A label numbers the doubles of
    one kind in the order they are created, so two sets used in the same way share their labels.
    """

    def __init__(self) -> None:
        self.calls: list[_Call] = []
        self.accessed: list[str] = []
        self.fakes: dict[str, _Fake] = {}
        self._counts: dict[str, int] = {}

    def label(self, kind: str, fake: _Fake) -> str:
        count = self._counts.get(kind, 0) + 1
        self._counts[kind] = count
        label = f"{kind}#{count}"
        self.fakes[label] = fake
        return label


class _Fake:
    """Base of the doubles: records every public attribute accessed on it, and its calls."""

    def __init__(self, log: _Log, kind: str) -> None:
        self._log = log
        self._label = log.label(kind, self)

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            label = object.__getattribute__(self, "_label")
            object.__getattribute__(self, "_log").accessed.append(f"{label}.{name}")
        return object.__getattribute__(self, name)

    def _record(
        self,
        method: str,
        args: tuple[object, ...] = (),
        kwargs: Mapping[str, object] | None = None,
        result: _Fake | None = None,
    ) -> None:
        returned = None if result is None else result._label
        self._log.calls.append(_Call(self._label, method, args, dict(kwargs or {}), returned))


class _FakeSpark(_Fake):
    """Stands in for a `SparkSession`. Each `read` is a new `_FakeReader`, as in PySpark."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "spark")

    @property
    def read(self) -> _FakeReader:
        reader = _FakeReader(self._log)
        self._record("read", result=reader)
        return reader


class _FakeReader(_Fake):
    """Stands in for `spark.read`. Each `table` returns a new `_FakeDataFrame`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "read")

    def options(self, *args: object, **kwargs: object) -> _FakeReader:
        self._record("options", args, kwargs, self)
        return self

    def table(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("table", args, kwargs, frame)
        return frame


class _FakeDataFrame(_Fake):
    """Stands in for a `DataFrame` with the columns `id` and `payload`.

    Each `write` is a new `_FakeWriter`, whose save raises `save_error` once recorded, when it
    is set.
    """

    def __init__(self, log: _Log, *, save_error: BaseException | None = None) -> None:
        super().__init__(log, "frame")
        self._save_error = save_error

    @property
    def columns(self) -> list[str]:
        self._record("columns")
        return list(COLUMNS)

    @property
    def write(self) -> _FakeWriter:
        writer = _FakeWriter(self._log, self._save_error)
        self._record("write", result=writer)
        return writer


class _FakeWriter(_Fake):
    """Stands in for `DataFrame.write`. `save` and `saveAsTable` raise `save_error` once
    recorded, when it is set."""

    def __init__(self, log: _Log, save_error: BaseException | None) -> None:
        super().__init__(log, "write")
        self._save_error = save_error

    def format(self, *args: object, **kwargs: object) -> _FakeWriter:
        self._record("format", args, kwargs, self)
        return self

    def option(self, *args: object, **kwargs: object) -> _FakeWriter:
        self._record("option", args, kwargs, self)
        return self

    def options(self, *args: object, **kwargs: object) -> _FakeWriter:
        self._record("options", args, kwargs, self)
        return self

    def mode(self, *args: object, **kwargs: object) -> _FakeWriter:
        self._record("mode", args, kwargs, self)
        return self

    def save(self, *args: object, **kwargs: object) -> None:
        self._record("save", args, kwargs)
        if self._save_error is not None:
            raise self._save_error

    def saveAsTable(self, *args: object, **kwargs: object) -> None:  # noqa: N802 - PySpark's name
        self._record("saveAsTable", args, kwargs)
        if self._save_error is not None:
            raise self._save_error


def _table_summary(table: Table) -> tuple[object, ...]:
    """What `create_table` receives, as values two logs can compare."""
    schema = [schema_field.to_api_repr() for schema_field in table.schema]
    return (table.project, table.dataset_id, table.table_id, schema, table.expires)


class _FakeBigQueryClient(_Fake):
    """Stands in for a `google.cloud.bigquery.Client`.

    `query` returns a `_FakeJob`. `get_table` returns the target of AT-005,
    `acme-data.lake.orders`, with the columns `id` and `payload`.
    """

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "bigquery")

    def query(self, *args: object, **kwargs: object) -> _FakeJob:
        job = _FakeJob(self._log)
        self._record("query", args, kwargs, job)
        return job

    def get_table(self, *args: object, **kwargs: object) -> Table:
        self._record("get_table", args, kwargs)
        schema = [SchemaField("id", "INTEGER", mode="REQUIRED"), SchemaField("payload", "STRING")]
        return Table("acme-data.lake.orders", schema=schema)

    def create_table(self, table: Table, *args: object, **kwargs: object) -> Table:
        self._record("create_table", (_table_summary(table), *args), kwargs)
        return table

    def delete_table(self, *args: object, **kwargs: object) -> None:
        self._record("delete_table", args, kwargs)

    def close(self, *args: object, **kwargs: object) -> None:
        self._record("close", args, kwargs)


class _FakeJob(_Fake):
    """Stands in for a `google.cloud.bigquery.QueryJob`, whose result has no rows."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "job")

    def result(self, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        self._record("result", args, kwargs)
        return []


def _statement_text(query: object) -> str:
    """A statement as text: a `psycopg.sql` composable is rendered without a connection."""
    if isinstance(query, sql.Composable):
        return query.as_string()
    return str(query)


class _FakePsycopg(_Fake):
    """Stands in for `psycopg.connect`: each call returns a new `_FakeConnection`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "psycopg")

    def __call__(self, *args: object, **kwargs: object) -> _FakeConnection:
        connection = _FakeConnection(self._log)
        self._record("connect", args, kwargs, connection)
        return connection


class _FakeConnection(_Fake):
    """Stands in for a `psycopg.Connection`. As a context manager it commits on a clean exit,
    rolls back on an exception, then closes."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "connection")

    def execute(self, query: object, *args: object, **kwargs: object) -> None:
        self._record("execute", (_statement_text(query), *args), kwargs)

    def commit(self) -> None:
        self._record("commit")

    def rollback(self) -> None:
        self._record("rollback")

    def close(self) -> None:
        self._record("close")

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


class _World:
    """One set of doubles in the initial state of the criteria, all writing to one log."""

    def __init__(self) -> None:
        self.log = _Log()
        self.spark = _FakeSpark(self.log)
        self.bigquery = _FakeBigQueryClient(self.log)
        self.psycopg = _FakePsycopg(self.log)

    def frame(self, *, save_error: BaseException | None = None) -> _FakeDataFrame:
        """A new DataFrame of this set, with the columns `id` and `payload`."""
        return _FakeDataFrame(self.log, save_error=save_error)


class _RecordingIterable:
    """An iterable over `records` whose `__iter__`, `__len__` and `__bool__` record each call."""

    def __init__(self, records: Sequence[dict[str, object]]) -> None:
        self.calls: list[str] = []
        self._records = list(records)

    def __iter__(self) -> Iterator[dict[str, object]]:
        self.calls.append("__iter__")
        return iter(self._records)

    def __len__(self) -> int:
        self.calls.append("__len__")
        return len(self._records)

    def __bool__(self) -> bool:
        self.calls.append("__bool__")
        return bool(self._records)


class _BrokenSourceError(Exception):
    """What a generator raises in the middle of a batch."""


def _generate(
    *records: dict[str, object], error: BaseException | None = None
) -> Iterator[dict[str, object]]:
    """A generator that yields `records`, then raises `error` when it is given."""
    yield from records
    if error is not None:
        raise error


# --- Fixtures and helpers -----------------------------------------------------------------


def _refuse(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("a default client, the active session or psycopg.connect was used")


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the SDKs' default clients, `SparkSession.active` and `psycopg.connect` fail when
    called. A test that needs `SparkSession.active` or `psycopg.connect` sets them again."""
    for target in (
        "boto3.client",
        "google.cloud.storage.Client",
        "databricks.sdk.WorkspaceClient",
        "google.cloud.bigquery.Client",
    ):
        monkeypatch.setattr(target, _refuse)
    monkeypatch.setattr(SparkSession, "active", _refuse)
    monkeypatch.setattr(psycopg, "connect", _refuse)


@pytest.fixture
def conn() -> PostgresConnection:
    """The connection the job builds from its own secrets."""
    return PostgresConnection("db.internal", "shop", "ingest", "s3cret")


def _config(directory: Path, document: object, name: str = "job.json") -> Path:
    """Write `document` with `json.dumps` to `directory / name`, and return the path."""
    path = directory / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _text(directory: Path, text: str, name: str = "job.json") -> Path:
    """Write `text` as it is to `directory / name`, and return the path."""
    path = directory / name
    path.write_text(text, encoding="utf-8")
    return path


def _target(directory: Path, out: Path, **keys: object) -> Path:
    """A configuration whose target is the `json_lines` file `out`, with `keys` besides `path`."""
    return _config(directory, {"target": {"system": "json_lines", "path": str(out), **keys}})


def _holding(path: Path, *records: dict[str, object]) -> Path:
    """Make `path`, and its directory, hold one JSON line per record of `records`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    return path


def _lines(path: Path, encoding: str = "utf-8") -> list[Any]:
    """The JSON values of the lines of `path`."""
    return [json.loads(line) for line in path.read_text(encoding=encoding).splitlines()]


def _run(callback: Callback, state: Mapping[str, Any]) -> Process:
    """Run `callback` alone, in a step `load`, over `state`."""
    return Process([Step("load", [callback])], state=state).run()


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the connectors take."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


def _label(value: object) -> str:
    """The label of a double."""
    assert isinstance(value, _Fake), value
    return value._label


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _a_day_after(moment: datetime, window: tuple[datetime, datetime]) -> bool:
    """Whether `moment` is a day after an instant of `window`, to BigQuery's millisecond."""
    start, end = window
    lowest = start + DAY
    lowest = lowest.replace(microsecond=lowest.microsecond // 1000 * 1000)
    return lowest <= moment <= end + DAY


def _comparable(value: object, window: tuple[datetime, datetime] | None) -> object:
    """`value` with what a writer generates replaced by placeholders: a staging table name and,
    within `window`, an expiry time a day after the write."""
    if isinstance(value, str):
        return STAGING_NAME.sub("ingestio_staging_<32 hex digits>", value)
    if isinstance(value, datetime):
        if window is not None and _a_day_after(value, window):
            return "<a day after the write>"
        return value
    if isinstance(value, tuple):
        return tuple(_comparable(item, window) for item in value)
    if isinstance(value, list):
        return [_comparable(item, window) for item in value]
    if isinstance(value, dict):
        return {key: _comparable(item, window) for key, item in value.items()}
    return value


def _transcript(
    log: _Log, window: tuple[datetime, datetime] | None = None
) -> list[tuple[object, ...]]:
    """The calls of `log`, in order, as values that the log of another set can equal."""
    return [
        (
            call.target,
            call.method,
            _comparable(call.args, window),
            _comparable(call.kwargs, window),
            call.result,
        )
        for call in log.calls
    ]


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


def _json_job(api_url: str, out: Path) -> dict[str, object]:
    """A configuration with a `json_api` source at `api_url` and a `json_lines` target `out`."""
    return {
        "source": {"system": "json_api", "url": api_url},
        "target": {"system": "json_lines", "path": str(out)},
    }


def _assert_connector_error(error: BaseException, direct: BaseException, note: str) -> None:
    """`error` is the error that building the connector directly raised, `direct`, plus `note`."""
    assert type(error) is type(direct)
    assert str(error) == str(direct)
    assert type(error.__cause__) is type(direct.__cause__)
    assert getattr(error, "__notes__", []) == [note]


_Row: TypeAlias = tuple[dict[str, object], dict[str, Any], Callable[[], Any]]


def _write_row(
    system: str, ours: _World, theirs: _World, conn: PostgresConnection, tmp_path: Path
) -> _Row:
    """The row of AT-005 for `system`: the target section besides `system`, the callback's
    arguments over `ours`, and a function that builds the twin over `theirs`."""
    if system == "disk":
        lake = str(tmp_path / "lake" / "orders")
        return (
            {"path": lake, "format": "parquet", "options": {"compression": "snappy"}},
            {},
            lambda: DiskWriter(lake, "parquet", options={"compression": "snappy"}),
        )
    if system == "postgres":
        return (
            {"table_name": "public.orders_copy", "options": {"batchsize": 1000}},
            {"connection": conn},
            lambda: PostgresWriter("public.orders_copy", conn, options={"batchsize": 1000}),
        )
    if system == "s3":
        return (
            {"path": "s3://acme-lake/orders", "format": "delta"},
            {},
            lambda: S3Writer("s3://acme-lake/orders", "delta"),
        )
    if system == "gcs":
        return (
            {"path": "gs://acme-lake/orders", "format": "delta"},
            {},
            lambda: GcsWriter("gs://acme-lake/orders", "delta"),
        )
    if system == "volume":
        return (
            {"volume": "main.lake.curated", "path": "orders", "format": "parquet"},
            {},
            lambda: VolumeWriter("main.lake.curated", "orders", "parquet"),
        )
    if system == "unity_catalog":
        return (
            {"table_name": "main.silver.orders", "options": {"mergeSchema": True}},
            {},
            lambda: UnityCatalogWriter("main.silver.orders", options={"mergeSchema": True}),
        )
    assert system == "bigquery", system
    return (
        {"table_name": "acme-data.lake.orders", "billing_project": "acme-billing"},
        {"client": ours.bigquery},
        lambda: BigQueryWriter(
            "acme-data.lake.orders",
            billing_project="acme-billing",
            client=cast(Any, theirs.bigquery),
        ),
    )


def _twin_write(writer: Any, df: DataFrame, name: str) -> None:
    """What the callback `name` does with its writer: `write(df)` for `AppendOnly`,
    `write(df, replace=True)` for `FullLoad`."""
    if name == "FullLoad":
        writer.write(df, replace=True)
    else:
        writer.write(df)


def _assert_writes_as_the_twin(
    name: str,
    system: str,
    conn: PostgresConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run the callback `name` over the row of AT-005 for `system`, then its twin's write over
    a second set of doubles, and compare the two logs."""
    ours, theirs = _World(), _World()
    keys, arguments, twin = _write_row(system, ours, theirs, conn, tmp_path)
    path = _config(tmp_path, {"target": {"system": system, **keys}})
    callback = _strategy(name)(path, key="orders", **arguments)

    monkeypatch.setattr(psycopg, "connect", ours.psycopg)
    df = ours.frame()
    start = datetime.now(UTC)
    _run(callback, {"orders": _frame(df)})
    ours_window = (start, datetime.now(UTC))

    monkeypatch.setattr(psycopg, "connect", theirs.psycopg)
    writer, theirs_df = twin(), _frame(theirs.frame())
    start = datetime.now(UTC)
    _twin_write(writer, theirs_df, name)
    theirs_window = (start, datetime.now(UTC))

    assert _transcript(ours.log, ours_window) == _transcript(theirs.log, theirs_window)
    # The DataFrame written is the one in the state, not a copy or a conversion of it.
    assert {call.target for call in ours.log.calls if call.method == "write"} == {_label(df)}
    if system == "bigquery":
        assert any(call.target == _label(ours.bigquery) for call in ours.log.calls)


# --- Import surface and signatures --------------------------------------------------------

AT001_EXPORTS_CODE = """
import ingestio
import ingestio.callback.all as callbacks
from ingestio import AppendOnly, Callback, FullLoad, ReadSource
from ingestio.callback.all import AppendOnly as CallbackAppendOnly
from ingestio.callback.all import FullLoad as CallbackFullLoad
from ingestio.callback.all import ReadSource as CallbackReadSource

assert AppendOnly is CallbackAppendOnly
assert FullLoad is CallbackFullLoad
assert ReadSource is CallbackReadSource
for name in ("AppendOnly", "FullLoad", "ReadSource"):
    assert name in ingestio.__all__, ingestio.__all__
    assert name in callbacks.__all__, callbacks.__all__
assert issubclass(AppendOnly, Callback)
assert issubclass(FullLoad, Callback)
assert not issubclass(AppendOnly, FullLoad)
assert not issubclass(FullLoad, AppendOnly)
"""


def test_at001_append_only_and_full_load_are_exported_by_ingestio_and_callback_all(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_EXPORTS_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


AT001_REMOVED_CODE = """
import ingestio
import ingestio.callback.all as callbacks

assert "WriteTarget" not in ingestio.__all__, ingestio.__all__
assert "WriteTarget" not in callbacks.__all__, callbacks.__all__
for statement in (
    "from ingestio import WriteTarget",
    "from ingestio.callback.all import WriteTarget",
):
    try:
        exec(statement)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{statement} succeeded")
for module in (ingestio, callbacks):
    try:
        module.WriteTarget
    except AttributeError:
        pass
    else:
        raise AssertionError(f"{module.__name__}.WriteTarget exists")
"""


def test_at001_write_target_is_no_longer_exported(tmp_path: Path) -> None:
    result = _run_python(AT001_REMOVED_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


AT002_CODE = """
import sys

for name in ("pyspark", "psycopg", "boto3", "google", "databricks"):
    sys.modules[name] = None
for name in ("pyspark.sql", "psycopg", "boto3", "google.cloud", "databricks.sdk"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

from ingestio import AppendOnly, FullLoad, Process, Step

build = {"AppendOnly": AppendOnly, "FullLoad": FullLoad}[sys.argv[2]]
Process([Step("load", [build(sys.argv[1])])], state={"records": [{"id": 1}]}).run()
"""


@EITHER
def test_at002_json_lines_is_written_without_pyspark_or_any_extra(
    name: str, tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    result = _run_python(AT002_CODE, tmp_path, os.fspath(path), name)

    assert result.returncode == 0, result.stderr
    assert _lines(out) == [{"id": 1}]


@EITHER
def test_at003_the_signature_is_config_then_key_connection_and_client_keyword_only(
    name: str,
) -> None:
    parameters = inspect.signature(_strategy(name)).parameters.values()

    assert [(parameter.name, parameter.kind, parameter.default) for parameter in parameters] == [
        ("config", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty),
        ("key", inspect.Parameter.KEYWORD_ONLY, "records"),
        ("connection", inspect.Parameter.KEYWORD_ONLY, None),
        ("client", inspect.Parameter.KEYWORD_ONLY, None),
    ]


@pytest.mark.parametrize(
    ("name", "replace"),
    [("AppendOnly", True), ("AppendOnly", False), ("FullLoad", True)],
)
def test_at003_replace_is_not_an_argument(name: str, replace: bool, tmp_path: Path) -> None:
    path = _target(tmp_path, tmp_path / "out" / "orders.jsonl")
    build = _strategy(name)

    with pytest.raises(TypeError, match="replace"):
        build(path, replace=replace)


@EITHER
def test_at003_key_cannot_be_given_positionally(name: str, tmp_path: Path) -> None:
    path = _target(tmp_path, tmp_path / "out" / "orders.jsonl")
    build = _strategy(name)

    with pytest.raises(TypeError):
        build(path, "orders")


# --- Appending ----------------------------------------------------------------------------


def test_at004_append_only_adds_the_records_after_what_the_file_holds(tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)

    _run(ingestio.AppendOnly(path), {"records": [{"id": 1}, {"id": 2}]})
    assert _lines(out) == [{"id": 0}, {"id": 1}, {"id": 2}]

    _run(ingestio.AppendOnly(path), {"records": [{"id": 3}]})
    assert _lines(out) == [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]


def test_at004_append_only_creates_the_missing_file_and_its_directory(tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    _run(ingestio.AppendOnly(path), {"records": [{"id": 1}, {"id": 2}]})

    assert _lines(out) == [{"id": 1}, {"id": 2}]


def test_at004_append_only_writes_in_the_encoding_of_the_section(tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out, encoding="utf-16")

    _run(ingestio.AppendOnly(path), {"records": [{"id": 1}]})

    assert _lines(out, "utf-16") == [{"id": 1}]


@SYSTEMS
def test_at005_append_only_writes_as_the_writer_the_target_section_names(
    system: str, conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _assert_writes_as_the_twin("AppendOnly", system, conn, monkeypatch, tmp_path)


# --- Replacing ----------------------------------------------------------------------------


def test_at006_full_load_makes_the_file_hold_exactly_the_records(tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)

    _run(ingestio.FullLoad(path), {"records": [{"id": 1}, {"id": 2}]})
    assert _lines(out) == [{"id": 1}, {"id": 2}]

    _run(ingestio.FullLoad(path), {"records": [{"id": 3}]})
    assert _lines(out) == [{"id": 3}]


def test_at006_full_load_creates_the_missing_file_and_its_directory(tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    _run(ingestio.FullLoad(path), {"records": [{"id": 1}, {"id": 2}]})

    assert _lines(out) == [{"id": 1}, {"id": 2}]


@SYSTEMS
def test_at007_full_load_replaces_as_the_writer_the_target_section_names(
    system: str, conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _assert_writes_as_the_twin("FullLoad", system, conn, monkeypatch, tmp_path)


# --- The value written --------------------------------------------------------------------


@EITHER
def test_at008_key_chooses_the_value_of_the_state_to_write(name: str, tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    _run(_strategy(name)(path, key="raw"), {"raw": [{"id": 1}], "records": [{"id": 9}]})

    assert _lines(out) == [{"id": 1}]


@EITHER
def test_at008_without_key_the_records_are_written(name: str, tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    _run(_strategy(name)(path), {"raw": [{"id": 1}], "records": [{"id": 9}]})

    assert _lines(out) == [{"id": 9}]


@EITHER
def test_at009_the_state_keeps_its_keys_and_its_objects(name: str, tmp_path: Path) -> None:
    records = [{"id": 1}]
    other = object()
    path = _target(tmp_path, tmp_path / "out" / "orders.jsonl")

    process = _run(_strategy(name)(path), {"records": records, "other": other})

    assert sorted(process.state) == ["other", "records"]
    assert process.state["records"] is records
    assert process.state["other"] is other
    assert records == [{"id": 1}]


@EITHER
@SYSTEMS
def test_at009_the_dataframe_stays_in_the_state_as_it_was(
    name: str,
    system: str,
    conn: PostgresConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    world = _World()
    keys, arguments, _ = _write_row(system, world, _World(), conn, tmp_path)
    path = _config(tmp_path, {"target": {"system": system, **keys}})
    monkeypatch.setattr(psycopg, "connect", world.psycopg)
    df = _frame(world.frame())

    process = _run(_strategy(name)(path, key="orders", **arguments), {"orders": df})

    assert list(process.state) == ["orders"]
    assert process.state["orders"] is df


def test_at010_append_only_writes_a_generator_and_exhausts_it(tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)
    records = _generate({"id": 1}, {"id": 2})

    _run(ingestio.AppendOnly(path), {"records": records})

    assert _lines(out) == [{"id": 0}, {"id": 1}, {"id": 2}]
    with pytest.raises(StopIteration):
        next(records)


def test_at010_full_load_writes_a_generator_and_exhausts_it(tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)
    records = _generate({"id": 1}, {"id": 2})

    _run(ingestio.FullLoad(path), {"records": records})

    assert _lines(out) == [{"id": 1}, {"id": 2}]
    with pytest.raises(StopIteration):
        next(records)


@EITHER
def test_at010_the_value_is_iterated_once_and_never_measured(name: str, tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)
    records = _RecordingIterable([{"id": 1}, {"id": 2}])

    _run(_strategy(name)(path), {"records": records})

    assert records.calls == ["__iter__"]


def test_at010_records_yielded_before_a_failure_are_written(tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)
    error = _BrokenSourceError("the source failed")
    process = Process(
        [Step("load", [ingestio.AppendOnly(path)])],
        state={"records": _generate({"id": 1}, error=error)},
    )

    with pytest.raises(_BrokenSourceError) as excinfo:
        process.run()

    assert excinfo.value is error
    assert getattr(error, "__notes__", []) == []
    assert _lines(out) == [{"id": 0}, {"id": 1}]


def test_at011_append_only_of_nothing_creates_the_missing_file_empty(tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)

    _run(ingestio.AppendOnly(path), {"records": []})

    assert out.read_bytes() == b""


def _exhausted() -> Iterator[dict[str, object]]:
    """A generator that has yielded all it had."""
    records = _generate({"id": 9})
    assert list(records) == [{"id": 9}]
    return records


EMPTY_BATCHES: dict[str, Callable[[], object]] = {"list": list, "exhausted": _exhausted}


@pytest.mark.parametrize("batch", list(EMPTY_BATCHES))
def test_at011_append_only_of_nothing_leaves_the_file_as_it_was(batch: str, tmp_path: Path) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)

    _run(ingestio.AppendOnly(path), {"records": EMPTY_BATCHES[batch]()})

    assert _lines(out) == [{"id": 0}]


@pytest.mark.parametrize("batch", list(EMPTY_BATCHES))
def test_at011_full_load_of_nothing_leaves_the_file_existing_and_empty(
    batch: str, tmp_path: Path
) -> None:
    out = _holding(tmp_path / "out" / "orders.jsonl", {"id": 0})
    path = _target(tmp_path, out)

    _run(ingestio.FullLoad(path), {"records": EMPTY_BATCHES[batch]()})

    assert out.read_bytes() == b""


@EITHER
def test_at011_the_dataframe_is_not_inspected_before_it_is_written(
    name: str, conn: PostgresConnection, tmp_path: Path
) -> None:
    ours, theirs = _World(), _World()
    keys, arguments, twin = _write_row("disk", ours, theirs, conn, tmp_path)
    path = _config(tmp_path, {"target": {"system": "disk", **keys}})
    df, theirs_df = ours.frame(), theirs.frame()

    _run(_strategy(name)(path, key="orders", **arguments), {"orders": _frame(df)})
    _twin_write(twin(), _frame(theirs_df), name)

    def on_the_frame(log: _Log, frame: _FakeDataFrame) -> list[str]:
        prefix = f"{_label(frame)}."
        return [access.removeprefix(prefix) for access in log.accessed if access.startswith(prefix)]

    accessed = on_the_frame(ours.log, df)
    assert accessed == on_the_frame(theirs.log, theirs_df)
    assert not set(accessed) & set(EMPTY_ACCESSES), accessed


@EITHER
def test_at012_a_missing_records_key_raises_key_error_when_the_process_runs(
    name: str, tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)
    process = Process([Step("load", [_strategy(name)(path)])], state={})

    with pytest.raises(KeyError) as excinfo:
        process.run()

    assert excinfo.value.args == ("records",)
    assert getattr(excinfo.value, "__notes__", []) == []
    assert not out.parent.exists()


@EITHER
def test_at012_a_missing_key_raises_key_error_naming_it(name: str, tmp_path: Path) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)
    process = Process([Step("load", [_strategy(name)(path, key="raw")])], state={"records": []})

    with pytest.raises(KeyError) as excinfo:
        process.run()

    assert excinfo.value.args == ("raw",)
    assert getattr(excinfo.value, "__notes__", []) == []
    assert not out.parent.exists()


@EITHER
def test_at013_an_error_of_the_write_propagates_unchanged_with_no_note(
    name: str, conn: PostgresConnection, tmp_path: Path
) -> None:
    world = _World()
    error = RuntimeError("task failed")
    keys, _, _ = _write_row("disk", world, _World(), conn, tmp_path)
    path = _config(tmp_path, {"target": {"system": "disk", **keys}})
    state = {"orders": _frame(world.frame(save_error=error))}
    process = Process([Step("load", [_strategy(name)(path, key="orders")])], state=state)

    with pytest.raises(RuntimeError) as excinfo:
        process.run()

    assert excinfo.value is error
    assert getattr(error, "__notes__", []) == []


# --- The configuration --------------------------------------------------------------------


@EITHER
@pytest.mark.parametrize(
    ("config", "type_name"),
    [
        (JsonLinesWriter("/data/out/orders.jsonl"), "JsonLinesWriter"),
        (b"job.json", "bytes"),
        ({"target": {"system": "json_lines", "path": "/data/out/orders.jsonl"}}, "dict"),
    ],
    ids=["JsonLinesWriter", "bytes", "dict"],
)
def test_at014_a_config_that_is_not_a_path_is_rejected(
    name: str, config: object, type_name: str
) -> None:
    build = _strategy(name)

    message = f"config must be a str or os.PathLike, not {type_name}"
    with pytest.raises(TypeError, match=_exactly(message)):
        build(config)


@EITHER
def test_at014_a_missing_file_raises_file_not_found_error(name: str, tmp_path: Path) -> None:
    build = _strategy(name)

    with pytest.raises(FileNotFoundError):
        build(tmp_path / "missing.json")


@EITHER
def test_at014_a_directory_raises_is_a_directory_error(name: str, tmp_path: Path) -> None:
    build = _strategy(name)

    expected = PermissionError if sys.platform == "win32" else IsADirectoryError
    with pytest.raises(expected):
        build(tmp_path)


@EITHER
def test_at014_text_that_is_not_json_raises_value_error_from_the_decode_error(
    name: str, tmp_path: Path
) -> None:
    path = _text(tmp_path, '{"target": ')
    build = _strategy(name)

    with pytest.raises(ValueError) as excinfo:
        build(path)

    assert str(excinfo.value).startswith(f"{os.fspath(path)}: invalid JSON: ")
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError), repr(excinfo.value.__cause__)


@EITHER
@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_at014_nan_and_the_infinities_are_rejected(
    name: str, constant: str, tmp_path: Path
) -> None:
    text = (
        '{"target": {"system": "json_lines", "path": "/data/out/orders.jsonl", '
        f'"encoding": {constant}}}}}'
    )
    path = _text(tmp_path, text)
    build = _strategy(name)

    message = f"{os.fspath(path)}: invalid JSON: {constant} is not allowed"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize("text", ["[]", '"text"', "1", "null"])
def test_at014_a_top_level_that_is_not_an_object_is_rejected(
    name: str, text: str, tmp_path: Path
) -> None:
    path = _text(tmp_path, text)
    build = _strategy(name)

    message = f"{os.fspath(path)}: the configuration must be a JSON object"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize(
    "document",
    [{}, {"source": {"system": "json_api", "url": API}}],
    ids=["empty", "source only"],
)
def test_at014_a_missing_target_section_is_rejected(
    name: str, document: dict[str, object], tmp_path: Path
) -> None:
    path = _config(tmp_path, document)
    build = _strategy(name)

    message = f"{os.fspath(path)}: missing section 'target'"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize("value", [[], "json_lines"], ids=["list", "string"])
def test_at014_a_target_section_that_is_not_an_object_is_rejected(
    name: str, value: object, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": value})
    build = _strategy(name)

    message = f"{os.fspath(path)}: section 'target' must be a JSON object"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize("kind", ["str", "Path"])
def test_at014_a_str_and_a_path_naming_a_valid_file_are_accepted(
    name: str, kind: str, tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _target(tmp_path, out)
    config: str | Path = os.fspath(path) if kind == "str" else path

    _run(_strategy(name)(config), {"records": [{"id": 1}]})

    assert _lines(out) == [{"id": 1}]


def _row(label: str, keys: dict[str, object], message: str) -> tuple[str, dict[str, object], str]:
    """A row of AT-015: its label, the `target` section, and the message after `<path>: `."""
    return (label, keys, message)


def _extra(system: str, key: str, value: object) -> tuple[str, dict[str, object], str]:
    """A row of AT-015: a valid `system` section plus `key`, which the system does not take."""
    keys = {"system": system, **TARGET_REQUIRED[system], key: value}
    return _row(f"{system} {key}", keys, f"target.{key} is not a key of system '{system}'")


AT015_ROWS: list[tuple[str, dict[str, object], str]] = [
    ("no system", {"path": "/data/out/orders.jsonl"}, "target.system is required"),
    *[
        _row(
            f"system {label}",
            {"system": system, "path": "/data/out/orders.jsonl"},
            "target.system must be a string",
        )
        for label, system in (("5", 5), ("null", None), ("list", ["json_lines"]))
    ],
    *[
        _row(
            f"system {system!r}",
            {"system": system},
            f"target.system '{system}' is not one of: {TARGET_NAMES}",
        )
        for system in ("mariadb", "BigQuery", "json_api", "")
    ],
    _extra("json_lines", "append", True),
    _extra("json_lines", "replace", True),
    _extra("json_lines", "mode", "append"),
    _extra("unity_catalog", "watermark_column", "updated_at"),
    _extra("unity_catalog", "mode", "overwrite"),
    _extra("unity_catalog", "replace", True),
    _extra("s3", "client", "x"),
    _extra("s3", "watermark_pattern", "x"),
    ("json_lines path 5", {"system": "json_lines", "path": 5}, "target.path must be a string"),
    (
        "json_lines encoding null",
        {"system": "json_lines", "path": "/data/out/orders.jsonl", "encoding": None},
        "target.encoding must be a string",
    ),
    (
        "unity_catalog options string",
        {"system": "unity_catalog", "table_name": "main.silver.orders", "options": "mergeSchema"},
        f"target.options {OPTIONS_TYPE}",
    ),
    (
        "disk options null",
        {**TARGET_REQUIRED["disk"], "system": "disk", "options": {"compression": None}},
        f"target.options {OPTIONS_TYPE}",
    ),
]


@EITHER
@pytest.mark.parametrize(
    ("keys", "message"),
    [(keys, message) for _, keys, message in AT015_ROWS],
    ids=[label for label, _, _ in AT015_ROWS],
)
def test_at015_a_target_section_that_describes_no_writer_is_rejected(
    name: str, keys: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": keys})
    build = _strategy(name)

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


@EITHER
@pytest.mark.parametrize(
    ("system", "key"),
    [(system, key) for system, required in TARGET_REQUIRED.items() for key in required],
)
def test_at015_a_missing_required_key_is_rejected(
    name: str, system: str, key: str, tmp_path: Path
) -> None:
    keys = {other: value for other, value in TARGET_REQUIRED[system].items() if other != key}
    path = _config(tmp_path, {"target": {"system": system, **keys}})
    build = _strategy(name)

    message = f"{os.fspath(path)}: target.{key} is required for system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize("system", list(TARGET_REQUIRED))
def test_at015_without_any_required_key_the_first_of_the_table_is_named(
    name: str, system: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": {"system": system}})
    first = next(iter(TARGET_REQUIRED[system]))
    build = _strategy(name)

    message = f"{os.fspath(path)}: target.{first} is required for system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
@pytest.mark.parametrize(("first", "second"), [("append", "replace"), ("replace", "append")])
def test_at015_of_two_keys_the_system_does_not_take_the_first_in_the_file_is_named(
    name: str, first: str, second: str, tmp_path: Path
) -> None:
    keys = {"system": "json_lines", first: True, "path": "/data/out/orders.jsonl", second: True}
    path = _config(tmp_path, {"target": keys})
    build = _strategy(name)

    message = f"{os.fspath(path)}: target.{first} is not a key of system 'json_lines'"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
def test_at015_such_a_key_is_named_before_a_missing_required_key(name: str, tmp_path: Path) -> None:
    path = _config(tmp_path, {"target": {"system": "json_lines", "append": True}})
    build = _strategy(name)

    message = f"{os.fspath(path)}: target.append is not a key of system 'json_lines'"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@EITHER
def test_at016_postgres_without_connection_is_rejected(name: str, tmp_path: Path) -> None:
    path = _config(tmp_path, {"target": {"system": "postgres", **TARGET_REQUIRED["postgres"]}})
    build = _strategy(name)

    message = f"{os.fspath(path)}: target system 'postgres' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


AT016_ROWS: list[tuple[str, str]] = [
    *[
        (system, argument)
        for system in ("json_lines", "disk", "s3", "gcs", "volume", "unity_catalog")
        for argument in ("connection", "client")
    ],
    ("postgres", "client"),
    ("bigquery", "connection"),
]


def _given(argument: str, conn: PostgresConnection) -> object:
    """A value, not `None`, of the callback argument `argument`."""
    return conn if argument == "connection" else object()


@EITHER
@pytest.mark.parametrize(("system", "argument"), AT016_ROWS)
def test_at016_an_argument_the_system_does_not_take_is_rejected(
    name: str, system: str, argument: str, conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": {"system": system, **TARGET_REQUIRED[system]}})
    arguments = {argument: _given(argument, conn)}
    if system == "postgres":
        arguments["connection"] = conn
    build = _strategy(name)

    message = f"{os.fspath(path)}: target system '{system}' takes no argument {argument}"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path, **arguments)


@EITHER
def test_at016_given_both_arguments_json_lines_names_connection(
    name: str, conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _target(tmp_path, tmp_path / "out" / "orders.jsonl")
    build = _strategy(name)

    message = f"{os.fspath(path)}: target system 'json_lines' takes no argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path, connection=conn, client=object())


def _grouped(rows: Sequence[tuple[str, str]]) -> list[tuple[str, tuple[str, ...]]]:
    """The arguments of `rows` that each system does not take, together."""
    groups: dict[str, list[str]] = {}
    for system, argument in rows:
        groups.setdefault(system, []).append(argument)
    return [(system, tuple(names)) for system, names in groups.items()]


@EITHER
@pytest.mark.parametrize(("system", "names"), _grouped(AT016_ROWS))
def test_at016_those_arguments_given_as_none_are_not_given(
    name: str, system: str, names: tuple[str, ...], conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"target": {"system": system, **TARGET_REQUIRED[system]}})
    arguments: dict[str, object] = dict.fromkeys(names)
    if system == "postgres":
        arguments["connection"] = conn

    _strategy(name)(path, **arguments)


AT017_ROWS: list[tuple[dict[str, object], str, Callable[[], object]]] = [
    (
        {"system": "bigquery", "table_name": "acme.lake.orders", "options": {"table": "x"}},
        "option 'table' is set by the connector",
        lambda: BigQueryWriter("acme.lake.orders", options={"table": "x"}),
    ),
    (
        {"system": "unity_catalog", "table_name": "orders"},
        "table_name must be 'catalog.schema.table'",
        lambda: UnityCatalogWriter("orders"),
    ),
    (
        {"system": "disk", "path": "lake/orders", "format": "parquet"},
        "path must be an absolute local path",
        lambda: DiskWriter("lake/orders", "parquet"),
    ),
]


@EITHER
@pytest.mark.parametrize(
    ("keys", "message", "direct"), AT017_ROWS, ids=["bigquery", "unity_catalog", "disk"]
)
def test_at017_a_value_the_writer_rejects_raises_its_error_with_one_note(
    name: str,
    keys: dict[str, object],
    message: str,
    direct: Callable[[], object],
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"target": keys})
    with pytest.raises(ValueError) as built_directly:
        direct()
    build = _strategy(name)

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        build(path)

    assert str(excinfo.value) == message
    note = f"while building the target of {os.fspath(path)}, system '{keys['system']}'"
    _assert_connector_error(excinfo.value, built_directly.value, note)


AT017_CODE = """
import json
import sys

missing, name, cases = json.loads(sys.argv[1])
sys.modules[missing] = None
try:
    __import__(missing)
except ImportError:
    pass
else:
    raise AssertionError(f"{missing} can be imported")

from ingestio import AppendOnly, FullLoad
from ingestio.connector.all import PostgresConnection

build = {"AppendOnly": AppendOnly, "FullLoad": FullLoad}[name]
conn = PostgresConnection("db.internal", "shop", "ingest", "s3cret")
outcomes = []
for config, connection in cases:
    given = {"connection": conn} if connection else {}
    try:
        build(config, **given)
    except Exception as error:
        outcomes.append([type(error).__name__, str(error), getattr(error, "__notes__", [])])
    else:
        outcomes.append(None)
print(json.dumps(outcomes))
"""

AT017_MISSING: dict[str, tuple[str, str]] = {
    "psycopg": ("postgres", "PostgresWriter needs psycopg: pip install 'ingestio[postgres]'"),
    "google": (
        "bigquery",
        "BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'",
    ),
}


@EITHER
@pytest.mark.parametrize("missing", list(AT017_MISSING))
def test_at017_a_missing_extra_raises_the_writers_import_error_with_one_note(
    name: str, missing: str, tmp_path: Path
) -> None:
    system, message = AT017_MISSING[missing]
    systems = [system, "s3", "gcs", "volume"]
    cases: list[list[object]] = []
    for system_name in systems:
        document = {"target": {"system": system_name, **TARGET_REQUIRED[system_name]}}
        path = _config(tmp_path, document, f"{system_name}.json")
        cases.append([os.fspath(path), system_name == "postgres"])
    note = f"while building the target of {cases[0][0]}, system '{system}'"

    result = _run_python(AT017_CODE, tmp_path, json.dumps([missing, name, cases]))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [["ImportError", message, [note]], None, None, None]


@EITHER
@SYSTEMS
def test_at018_building_touches_no_session_no_client_and_no_database(
    name: str,
    system: str,
    conn: PostgresConnection,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    world = _World()
    active: list[str] = []

    def raising_active() -> NoReturn:
        active.append("active")
        raise AssertionError("SparkSession.active was called")

    monkeypatch.setattr(SparkSession, "active", raising_active)
    monkeypatch.setattr(psycopg, "connect", world.psycopg)
    keys, arguments, _ = _write_row(system, world, _World(), conn, tmp_path)
    path = _config(tmp_path, {"target": {"system": system, **keys}})

    _strategy(name)(path, key="orders", **arguments)

    assert active == []
    assert world.log.accessed == []
    assert world.log.calls == []


@EITHER
def test_at018_rewriting_the_file_after_building_changes_nothing(name: str, tmp_path: Path) -> None:
    first, second = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    path = _target(tmp_path, first)
    callback = _strategy(name)(path)

    _target(tmp_path, second)
    _run(callback, {"records": [{"id": 1}]})

    assert _lines(first) == [{"id": 1}]
    assert not second.exists()


@EITHER
def test_at018_deleting_the_file_after_building_changes_nothing(name: str, tmp_path: Path) -> None:
    out = tmp_path / "a.jsonl"
    path = _target(tmp_path, out)
    callback = _strategy(name)(path)

    path.unlink()
    _run(callback, {"records": [{"id": 1}]})

    assert _lines(out) == [{"id": 1}]


@EITHER
def test_at019_the_source_section_and_other_top_level_members_are_ignored(
    name: str, tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(
        tmp_path,
        {
            "source": 5,
            "primary_key": "id",
            "job": {"owner": "data"},
            "target": {"system": "json_lines", "path": str(out)},
        },
    )

    _run(_strategy(name)(path), {"records": [{"id": 1}]})

    assert _lines(out) == [{"id": 1}]


# --- Composition --------------------------------------------------------------------------


def _ingest(path: Path, name: str) -> Process:
    """Run a new `ReadSource(path)` in `extract`, then a new callback `name` in `load`."""
    load = _strategy(name)(path)
    return Process([Step("extract", [ReadSource(path)]), Step("load", [load])]).run()


def test_at020_append_only_adds_the_payload_on_each_run(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "bronze" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    process = _ingest(path, "AppendOnly")
    assert _lines(out) == payload
    assert process.state["records"] == payload

    _ingest(path, "AppendOnly")
    assert _lines(out) == payload + payload


def test_at020_full_load_holds_the_payload_once_after_the_second_run(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "bronze" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    process = _ingest(path, "FullLoad")
    assert _lines(out) == payload
    assert process.state["records"] == payload

    _ingest(path, "FullLoad")
    assert _lines(out) == payload


@EITHER
def test_at021_each_section_names_its_own_table(name: str, tmp_path: Path) -> None:
    world = _World()
    path = _config(
        tmp_path,
        {
            "source": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
            "target": {"system": "unity_catalog", "table_name": "main.silver.orders"},
        },
    )

    Process(
        [
            Step("extract", [ReadSource(path, spark=_session(world.spark))]),
            Step("load", [_strategy(name)(path)]),
        ]
    ).run()

    reads = [call for call in world.log.calls if call.method in ("table", "load")]
    saves = [call for call in world.log.calls if call.method in ("saveAsTable", "save")]
    assert [(call.method, call.args) for call in reads] == [
        ("table", ("`main`.`bronze`.`orders`",))
    ]
    assert [(call.method, call.args) for call in saves] == [
        ("saveAsTable", ("`main`.`silver`.`orders`",))
    ]
    (read,), (save,) = reads, saves
    # The writer whose save was called came from the `write` of the DataFrame the read returned.
    writers = [call.target for call in world.log.calls if call.result == save.target]
    assert writers[0] == read.result, writers


# --- JsonLinesWriter ----------------------------------------------------------------------


def test_at022_write_adds_the_records_after_what_the_file_holds(tmp_path: Path) -> None:
    out = _holding(tmp_path / "orders.jsonl", {"id": 0})
    writer = JsonLinesWriter(out)

    writer.write([{"id": 1}])
    assert _lines(out) == [{"id": 0}, {"id": 1}]

    writer.write([{"id": 2}])
    assert _lines(out) == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_at022_replace_false_appends_as_the_default_does(tmp_path: Path) -> None:
    out = _holding(tmp_path / "orders.jsonl", {"id": 0})
    writer = JsonLinesWriter(out)

    writer.write([{"id": 1}], replace=False)
    assert _lines(out) == [{"id": 0}, {"id": 1}]

    writer.write([{"id": 2}], replace=False)
    assert _lines(out) == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_at022_write_creates_the_missing_file_and_parent_directories(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b" / "orders.jsonl"

    JsonLinesWriter(out).write([{"id": 1}])

    assert _lines(out) == [{"id": 1}]


def test_at022_writing_nothing_to_a_missing_file_creates_it_empty(tmp_path: Path) -> None:
    out = tmp_path / "orders.jsonl"

    JsonLinesWriter(out).write([])

    assert out.read_bytes() == b""


def test_at023_replace_makes_the_file_hold_exactly_the_records(tmp_path: Path) -> None:
    out = _holding(tmp_path / "orders.jsonl", {"id": 0})

    JsonLinesWriter(out).write([{"id": 1}], replace=True)

    assert _lines(out) == [{"id": 1}]


def test_at023_replacing_with_nothing_leaves_the_file_existing_and_empty(tmp_path: Path) -> None:
    out = _holding(tmp_path / "orders.jsonl", {"id": 0})

    JsonLinesWriter(out).write([], replace=True)

    assert out.read_bytes() == b""


def test_at023_replace_creates_the_missing_file_and_parent_directories(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b" / "orders.jsonl"

    JsonLinesWriter(out).write([{"id": 1}], replace=True)

    assert _lines(out) == [{"id": 1}]


def test_at023_replace_cannot_be_given_positionally(tmp_path: Path) -> None:
    writer: Any = JsonLinesWriter(tmp_path / "orders.jsonl")

    with pytest.raises(TypeError):
        writer.write([{"id": 1}], True)


@pytest.mark.parametrize("append", [True, False])
def test_at024_append_is_not_an_argument_of_json_lines_writer(append: bool, tmp_path: Path) -> None:
    build: Any = JsonLinesWriter

    with pytest.raises(TypeError):
        build(tmp_path / "orders.jsonl", append=append)


def test_at024_the_signatures_are_path_then_encoding_and_data_then_replace() -> None:
    def shown(target: Callable[..., object]) -> list[tuple[str, object, object]]:
        parameters = inspect.signature(target).parameters.values()
        return [(parameter.name, parameter.kind, parameter.default) for parameter in parameters]

    assert shown(JsonLinesWriter) == [
        ("path", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty),
        ("encoding", inspect.Parameter.KEYWORD_ONLY, "utf-8"),
    ]
    assert shown(JsonLinesWriter.write) == [
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty),
        ("data", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty),
        ("replace", inspect.Parameter.KEYWORD_ONLY, False),
    ]


def test_at024_the_file_is_written_in_the_encoding_given(tmp_path: Path) -> None:
    out = tmp_path / "orders.jsonl"

    JsonLinesWriter(out, encoding="utf-16").write([{"id": 1}])

    assert _lines(out, "utf-16") == [{"id": 1}]
