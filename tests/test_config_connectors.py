"""Choosing the reader from the job's configuration: `ReadSource`.

Covers `specs/config_connectors.md`, except the cases that build a `WriteTarget`, which
`specs/append_only_callback.md` supersedes: the target section is covered by
`tests/test_append_only_callback.py`. Every test writes the job's configuration with
`json.dumps` to a file under `tmp_path` and passes its path. The tests need no JVM, no cloud
service and no database: the doubles below, a fake Spark session, fake DataFrames, fake S3, GCS,
Volumes and BigQuery clients and a fake `psycopg.connect`, write every call they receive to one
ordered log.

A test that maps a system to a connector runs the callback over one set of doubles and a *twin*,
the connector built directly from `ingestio.connector.all`, over a second set in the same initial
state, and compares the two logs. `json_api` reads from the `api_url` fixture, a local HTTP
server, `disk` uses files under `tmp_path`, and a missing package is simulated in a fresh
interpreter. The default clients of the SDKs and `SparkSession.active` fail when used, so that no
test reaches a real service.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, NoReturn, TypeAlias, cast
from urllib.error import HTTPError

import psycopg
import pytest
from psycopg import sql
from pyspark.sql import SparkSession

from ingestio import Process, ReadSource, Step
from ingestio.connector.all import (
    BigQueryReader,
    DiskReader,
    GcsReader,
    JsonApiReader,
    PostgresConnection,
    PostgresReader,
    S3Reader,
    UnityCatalogReader,
    VolumeReader,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
OCT_1 = datetime(2026, 10, 1, tzinfo=UTC)
NAIVE = datetime(2026, 10, 1)  # noqa: DTZ001 - the naive watermark of AT-018
API = "https://api.example.com/v1/orders"
S3_SOURCE = "s3://acme-landing/orders"
GCS_SOURCE = "gs://acme-landing/orders"
DISK_PATTERN = r"orders_(\d{8})\.csv"
GCS_PATTERN = r"orders_(\d{8})\.json"
SOURCE_NAMES = "bigquery, disk, gcs, json_api, postgres, s3, unity_catalog, volume"
COLUMNS = ("id", "payload")
BOUND_ROW = ("2026-10-21 09:30:00+00", "timestamp")
BIGQUERY_ROWS = (('DATE "2026-10-21"',),)

# The required keys of each source system, in the order of the spec's table, with values its
# connector accepts.
SOURCE_REQUIRED: dict[str, dict[str, object]] = {
    "json_api": {"url": API},
    "disk": {"path": "/data/landing/orders", "format": "csv"},
    "postgres": {"table_name": "public.orders"},
    "s3": {"path": S3_SOURCE, "format": "json"},
    "gcs": {"path": GCS_SOURCE, "format": "json"},
    "volume": {"volume": "main.landing.raw", "path": "orders", "format": "parquet"},
    "unity_catalog": {"table_name": "main.bronze.orders"},
    "bigquery": {"table_name": "acme-data.sales.orders"},
}
REQUIRED = {"source": SOURCE_REQUIRED}
BUILD: dict[str, Callable[..., object]] = {"source": ReadSource}

# The systems of the rows of AT-022.
READ_SYSTEMS = ("disk", "postgres", "s3", "gcs", "volume", "unity_catalog", "bigquery")

SECTIONS = pytest.mark.parametrize(
    ("section", "build"),
    [("source", ReadSource)],
    ids=["ReadSource"],
)


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

    def createDataFrame(  # noqa: N802 - PySpark's name
        self, *args: object, **kwargs: object
    ) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("createDataFrame", args, kwargs, frame)
        return frame


class _FakeReader(_Fake):
    """Stands in for `spark.read`. Each `load` and each `table` returns a new `_FakeDataFrame`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "read")

    def format(self, *args: object, **kwargs: object) -> _FakeReader:
        self._record("format", args, kwargs, self)
        return self

    def schema(self, *args: object, **kwargs: object) -> _FakeReader:
        self._record("schema", args, kwargs, self)
        return self

    def option(self, *args: object, **kwargs: object) -> _FakeReader:
        self._record("option", args, kwargs, self)
        return self

    def options(self, *args: object, **kwargs: object) -> _FakeReader:
        self._record("options", args, kwargs, self)
        return self

    def load(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("load", args, kwargs, frame)
        return frame

    def table(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("table", args, kwargs, frame)
        return frame


class _FakeDataFrame(_Fake):
    """Stands in for a `DataFrame` with the columns `id` and `payload`.

    `where` and `selectExpr` return a new `_FakeDataFrame`, and `first()` answers `BOUND_ROW`.
    """

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "frame")

    @property
    def columns(self) -> list[str]:
        self._record("columns")
        return list(COLUMNS)

    def where(self, *args: object, **kwargs: object) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("where", args, kwargs, frame)
        return frame

    def selectExpr(  # noqa: N802 - PySpark's name
        self, *args: object, **kwargs: object
    ) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._record("selectExpr", args, kwargs, frame)
        return frame

    def first(self) -> tuple[object, ...]:
        self._record("first")
        return BOUND_ROW


@dataclass(frozen=True)
class _Blob:
    """A blob that the fake GCS client lists."""

    name: str
    time_created: datetime


@dataclass(frozen=True)
class _DirectoryEntry:
    """An entry that the fake Files API lists; `last_modified` in milliseconds since the epoch."""

    name: str
    is_directory: bool
    last_modified: int


def _ms(moment: datetime) -> int:
    """`moment` in whole milliseconds since the epoch."""
    return (moment - EPOCH) // timedelta(milliseconds=1)


S3_OBJECTS = (
    {"Key": "orders/a.json", "LastModified": datetime(2026, 9, 30, tzinfo=UTC)},
    {"Key": "orders/b.json", "LastModified": datetime(2026, 10, 2, tzinfo=UTC)},
)
GCS_BLOBS = (
    _Blob("orders/orders_20260930.json", datetime(2026, 9, 30, tzinfo=UTC)),
    _Blob("orders/orders_20261002.json", datetime(2026, 10, 2, tzinfo=UTC)),
)
VOLUME_ENTRIES = (
    _DirectoryEntry("a.parquet", False, _ms(datetime(2026, 9, 30, tzinfo=UTC))),
    _DirectoryEntry("b.parquet", False, _ms(datetime(2026, 10, 2, tzinfo=UTC))),
    _DirectoryEntry("archive", True, _ms(datetime(2026, 10, 2, tzinfo=UTC))),
)


class _FakeS3Client(_Fake):
    """Stands in for a boto3 S3 client, whose listing holds `S3_OBJECTS`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "s3")

    def get_paginator(self, *args: object, **kwargs: object) -> _FakePaginator:
        paginator = _FakePaginator(self._log)
        self._record("get_paginator", args, kwargs, paginator)
        return paginator


class _FakePaginator(_Fake):
    """Stands in for the `list_objects_v2` paginator: one page holding `S3_OBJECTS`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "paginator")

    def paginate(self, *args: object, **kwargs: object) -> list[dict[str, Any]]:
        self._record("paginate", args, kwargs)
        return [{"Contents": [dict(entry) for entry in S3_OBJECTS]}]


class _FakeGcsClient(_Fake):
    """Stands in for a `google.cloud.storage.Client`, whose listing holds `GCS_BLOBS`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "gcs")

    def list_blobs(self, *args: object, **kwargs: object) -> list[_Blob]:
        self._record("list_blobs", args, kwargs)
        return list(GCS_BLOBS)


class _FakeWorkspaceClient(_Fake):
    """Stands in for a `databricks.sdk.WorkspaceClient`, whose `files` is a `_FakeFiles`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "workspace")
        self._files = _FakeFiles(log)

    @property
    def files(self) -> _FakeFiles:
        self._record("files", result=self._files)
        return self._files


class _FakeFiles(_Fake):
    """Stands in for the Files API, whose listing holds `VOLUME_ENTRIES`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "files")

    def list_directory_contents(self, *args: object, **kwargs: object) -> list[_DirectoryEntry]:
        self._record("list_directory_contents", args, kwargs)
        return list(VOLUME_ENTRIES)


class _FakeBigQueryClient(_Fake):
    """Stands in for a `google.cloud.bigquery.Client`. `query` returns a `_FakeJob` whose result
    is `BIGQUERY_ROWS`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "bigquery")

    def query(self, *args: object, **kwargs: object) -> _FakeJob:
        job = _FakeJob(self._log)
        self._record("query", args, kwargs, job)
        return job

    def close(self, *args: object, **kwargs: object) -> None:
        self._record("close", args, kwargs)


class _FakeJob(_Fake):
    """Stands in for a `google.cloud.bigquery.QueryJob`."""

    def __init__(self, log: _Log) -> None:
        super().__init__(log, "job")

    def result(self, *args: object, **kwargs: object) -> list[tuple[object, ...]]:
        self._record("result", args, kwargs)
        return list(BIGQUERY_ROWS)


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
        self.s3 = _FakeS3Client(self.log)
        self.gcs = _FakeGcsClient(self.log)
        self.workspace = _FakeWorkspaceClient(self.log)
        self.bigquery = _FakeBigQueryClient(self.log)
        self.psycopg = _FakePsycopg(self.log)


# --- Fixtures and helpers -----------------------------------------------------------------


def _refuse(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("a default client, the active session or psycopg.connect was used")


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the SDKs' default clients, `SparkSession.active` and `psycopg.connect` fail when
    called. A test that needs the active session or `psycopg.connect` sets them again."""
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


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    """`<tmp>/landing`, a real directory with three dated CSV files and one other file."""
    directory = tmp_path / "landing"
    directory.mkdir()
    for name in ("orders_20260930.csv", "orders_20261002.csv", "orders_20261003.csv", "notes.txt"):
        (directory / name).write_text("id,payload\n1,a\n", encoding="utf-8")
    return directory


def _use(world: _World, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `psycopg.connect` the fake of `world`, and `SparkSession.active` return its session."""
    session = world.spark
    monkeypatch.setattr(psycopg, "connect", world.psycopg)
    monkeypatch.setattr(SparkSession, "active", lambda: session)


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


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the connectors take."""
    return cast(SparkSession, fake)


def _label(value: object) -> str:
    """The label of a double."""
    assert isinstance(value, _Fake), value
    return value._label


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _boom(api_url: str) -> str:
    """The URL of the `api_url` server that answers 500."""
    return api_url.replace("/records", "/boom")


def _lines(path: Path) -> list[Any]:
    """The JSON values of the lines of `path`."""
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _transcript(log: _Log) -> list[tuple[object, ...]]:
    """The calls of `log`, in order, as values that the log of another set can equal."""
    return [(call.target, call.method, call.args, call.kwargs, call.result) for call in log.calls]


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


def _read_row(
    system: str, ours: _World, theirs: _World, conn: PostgresConnection, landing: Path
) -> _Row:
    """The row of AT-022 for `system`: the source section besides `system`, the callback's
    arguments over `ours`, and a function that builds the twin over `theirs`."""
    if system == "disk":
        return (
            {
                "path": str(landing),
                "format": "csv",
                "watermark_pattern": DISK_PATTERN,
                "schema": "id INT, payload STRING",
                "options": {"header": True},
            },
            {"watermark_value": "20261001", "spark": _session(ours.spark)},
            lambda: DiskReader(
                str(landing),
                "csv",
                watermark_pattern=DISK_PATTERN,
                watermark_value="20261001",
                schema="id INT, payload STRING",
                options={"header": True},
                spark=_session(theirs.spark),
            ),
        )
    if system == "postgres":
        return (
            {
                "table_name": "public.orders",
                "watermark_column": "updated_at",
                "options": {"fetchsize": 5000},
            },
            {"connection": conn, "watermark_value": OCT_1, "spark": _session(ours.spark)},
            lambda: PostgresReader(
                "public.orders",
                conn,
                watermark_column="updated_at",
                watermark_value=OCT_1,
                options={"fetchsize": 5000},
                spark=_session(theirs.spark),
            ),
        )
    if system == "s3":
        return (
            {"path": S3_SOURCE, "format": "json", "options": {"multiLine": True}},
            {"client": ours.s3, "watermark_value": OCT_1, "spark": _session(ours.spark)},
            lambda: S3Reader(
                S3_SOURCE,
                "json",
                watermark_value=OCT_1,
                options={"multiLine": True},
                client=cast(Any, theirs.s3),
                spark=_session(theirs.spark),
            ),
        )
    if system == "gcs":
        return (
            {"path": GCS_SOURCE, "format": "json", "watermark_pattern": GCS_PATTERN},
            {"client": ours.gcs, "watermark_value": "20261001", "spark": _session(ours.spark)},
            lambda: GcsReader(
                GCS_SOURCE,
                "json",
                watermark_pattern=GCS_PATTERN,
                watermark_value="20261001",
                client=cast(Any, theirs.gcs),
                spark=_session(theirs.spark),
            ),
        )
    if system == "volume":
        return (
            {"volume": "main.landing.raw", "path": "orders", "format": "parquet"},
            {"client": ours.workspace, "watermark_value": OCT_1, "spark": _session(ours.spark)},
            lambda: VolumeReader(
                "main.landing.raw",
                "orders",
                "parquet",
                watermark_value=OCT_1,
                client=cast(Any, theirs.workspace),
                spark=_session(theirs.spark),
            ),
        )
    if system == "unity_catalog":
        return (
            {
                "table_name": "main.bronze.orders",
                "watermark_column": "updated_at",
                "options": {"versionAsOf": 3},
            },
            {"watermark_value": 41, "spark": _session(ours.spark)},
            lambda: UnityCatalogReader(
                "main.bronze.orders",
                watermark_column="updated_at",
                watermark_value=41,
                options={"versionAsOf": 3},
                spark=_session(theirs.spark),
            ),
        )
    assert system == "bigquery", system
    return (
        {
            "table_name": "acme-data.sales.orders",
            "watermark_column": "updated_at",
            "billing_project": "acme-billing",
            "options": {"maxParallelism": 4},
        },
        {
            "client": ours.bigquery,
            "watermark_value": date(2026, 10, 1),
            "spark": _session(ours.spark),
        },
        lambda: BigQueryReader(
            "acme-data.sales.orders",
            watermark_column="updated_at",
            watermark_value=date(2026, 10, 1),
            billing_project="acme-billing",
            options={"maxParallelism": 4},
            client=cast(Any, theirs.bigquery),
            spark=_session(theirs.spark),
        ),
    )


def _required_read_row(
    system: str, ours: _World, theirs: _World, conn: PostgresConnection, landing: Path
) -> _Row:
    """The row of AT-022 for `system` with only its required keys, as AT-023 gives it."""
    if system == "disk":
        return (
            {"path": str(landing), "format": "csv"},
            {},
            lambda: DiskReader(str(landing), "csv"),
        )
    if system == "postgres":
        return (
            {"table_name": "public.orders"},
            {"connection": conn},
            lambda: PostgresReader("public.orders", conn),
        )
    if system == "s3":
        return (
            {"path": S3_SOURCE, "format": "json"},
            {"client": ours.s3},
            lambda: S3Reader(S3_SOURCE, "json", client=cast(Any, theirs.s3)),
        )
    if system == "gcs":
        return (
            {"path": GCS_SOURCE, "format": "json"},
            {"client": ours.gcs},
            lambda: GcsReader(GCS_SOURCE, "json", client=cast(Any, theirs.gcs)),
        )
    if system == "volume":
        return (
            {"volume": "main.landing.raw", "path": "orders", "format": "parquet"},
            {"client": ours.workspace},
            lambda: VolumeReader(
                "main.landing.raw", "orders", "parquet", client=cast(Any, theirs.workspace)
            ),
        )
    if system == "unity_catalog":
        return (
            {"table_name": "main.bronze.orders"},
            {},
            lambda: UnityCatalogReader("main.bronze.orders"),
        )
    assert system == "bigquery", system
    return (
        {"table_name": "acme-data.sales.orders"},
        {},
        lambda: BigQueryReader("acme-data.sales.orders"),
    )


# --- Import surface -----------------------------------------------------------------------

AT001_CODE = """
import ingestio
import ingestio.callback.all as callbacks
import ingestio.connector.all as connectors
from ingestio import ReadSource
from ingestio.callback.all import ReadSource as CallbackReadSource

assert ReadSource is CallbackReadSource
assert "ReadSource" in ingestio.__all__, ingestio.__all__
assert "ReadSource" in callbacks.__all__, callbacks.__all__
exported = {
    "BigQueryReader", "BigQueryWriter", "DiskReader", "DiskWriter", "GcsReader", "GcsWriter",
    "JsonApiReader", "JsonLinesWriter", "PostgresReader", "PostgresWriter", "S3Reader",
    "S3Writer", "UnityCatalogReader", "UnityCatalogWriter", "VolumeReader", "VolumeWriter",
    "PostgresConnection", "Reader", "Writer",
}
assert exported <= set(connectors.__all__), exported - set(connectors.__all__)
assert {"Reader", "Writer"} <= set(ingestio.__all__), ingestio.__all__
assert "PostgresConnection" not in ingestio.__all__, ingestio.__all__
"""


def test_at001_read_source_keeps_its_import_paths_and_the_connectors_stay_exported(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
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

import json

from ingestio import Process, ReadSource, Step

read = ReadSource(sys.argv[1])
print(json.dumps(Process([Step("extract", [read])]).run().state["records"]))
"""


def test_at002_json_api_builds_without_pyspark_or_any_extra(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    result = _run_python(AT002_CODE, tmp_path, os.fspath(path))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == payload


# --- The configuration file ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "name"),
    [
        (JsonApiReader(API), "JsonApiReader"),
        (b"job.json", "bytes"),
        ({"source": {"system": "json_api", "url": API}}, "dict"),
    ],
    ids=["JsonApiReader", "bytes", "dict"],
)
def test_at003_read_source_rejects_a_config_that_is_not_a_path(config: object, name: str) -> None:
    with pytest.raises(
        TypeError, match=_exactly(f"config must be a str or os.PathLike, not {name}")
    ):
        ReadSource(cast(Any, config))


@pytest.mark.parametrize("kind", ["str", "Path"])
def test_at003_a_str_and_a_path_naming_a_valid_file_are_accepted(
    kind: str, api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))
    config: str | Path = os.fspath(path) if kind == "str" else path

    process = Process([Step("extract", [ReadSource(config)])]).run()

    assert process.state["records"] == payload


@SECTIONS
def test_at004_a_missing_file_raises_file_not_found_error(
    section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    with pytest.raises(FileNotFoundError):
        build(tmp_path / "missing.json")


@SECTIONS
def test_at004_a_directory_raises_is_a_directory_error(
    section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    expected = PermissionError if sys.platform == "win32" else IsADirectoryError
    with pytest.raises(expected):
        build(tmp_path)


def test_at005_text_that_is_not_json_raises_value_error_from_the_decode_error(
    tmp_path: Path,
) -> None:
    path = _text(tmp_path, '{"source": ')

    with pytest.raises(ValueError) as excinfo:
        ReadSource(path)

    error = excinfo.value
    assert isinstance(error.__cause__, json.JSONDecodeError), repr(error.__cause__)
    assert str(error).startswith(f"{os.fspath(path)}: invalid JSON: ")
    assert str(error) == f"{os.fspath(path)}: invalid JSON: {error.__cause__}"


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_at005_nan_and_the_infinities_are_rejected(constant: str, tmp_path: Path) -> None:
    text = f'{{"source": {{"system": "json_api", "url": "{API}", "timeout": {constant}}}}}'
    path = _text(tmp_path, text)

    message = f"{os.fspath(path)}: invalid JSON: {constant} is not allowed"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@SECTIONS
@pytest.mark.parametrize("text", ["[]", '"text"', "1", "null"])
def test_at006_a_top_level_that_is_not_an_object_is_rejected(
    text: str, section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _text(tmp_path, text)

    message = f"{os.fspath(path)}: the configuration must be a JSON object"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@SECTIONS
def test_at007_a_missing_section_is_rejected(
    section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _config(tmp_path, {})

    with pytest.raises(ValueError, match=_exactly(f"{path}: missing section '{section}'")):
        build(path)


@SECTIONS
@pytest.mark.parametrize("value", [[], "postgres"], ids=["list", "string"])
def test_at007_a_section_that_is_not_an_object_is_rejected(
    value: object, section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: value})

    message = f"{path}: section '{section}' must be a JSON object"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


def test_at008_read_source_ignores_the_target_section_and_other_top_level_members(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    path = _config(
        tmp_path,
        {
            "source": {"system": "json_api", "url": api_url},
            "target": 5,
            "primary_key": "id",
            "job": {"owner": "data"},
        },
    )

    read = ReadSource(path)

    assert Process([Step("extract", [read])]).run().state["records"] == payload


def test_at009_deleting_the_file_after_building_changes_nothing(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))
    read = ReadSource(path)

    path.unlink()
    process = Process([Step("extract", [read])]).run()

    assert process.state["records"] == payload


def test_at009_rewriting_the_file_after_building_changes_nothing(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": {"system": "json_api", "url": api_url}})
    read = ReadSource(path)

    _config(tmp_path, {"source": {"system": "json_api", "url": _boom(api_url)}})
    process = Process([Step("extract", [read])]).run()

    assert process.state["records"] == payload


def test_at010_a_relative_path_is_resolved_against_the_directory_of_the_moment_of_building(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    _config(tmp_path, {"source": {"system": "json_api", "url": api_url}})
    _config(elsewhere, {"source": {"system": "json_api", "url": _boom(api_url)}})
    monkeypatch.chdir(tmp_path)

    read = ReadSource("job.json")
    monkeypatch.chdir(elsewhere)
    process = Process([Step("extract", [read])]).run()

    assert process.state["records"] == payload


# --- Systems and keys ---------------------------------------------------------------------


@SECTIONS
def test_at011_a_section_without_system_is_rejected(
    section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: dict(SOURCE_REQUIRED["json_api"])})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {section}.system is required")):
        build(path)


@SECTIONS
@pytest.mark.parametrize("system", [5, None, ["postgres"]], ids=["5", "null", "list"])
def test_at011_a_system_that_is_not_a_string_is_rejected(
    system: object, section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system}})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {section}.system must be a string")):
        build(path)


@pytest.mark.parametrize(
    ("section", "system"),
    [
        ("source", "mysql"),
        ("source", "Postgres"),
        ("source", "json_lines"),
        ("source", ""),
    ],
)
def test_at012_a_system_that_is_not_a_name_of_the_section_is_rejected(
    section: str, system: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system}})

    message = f"{path}: {section}.system '{system}' is not one of: {SOURCE_NAMES}"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path)


EXTRA_VALUES: dict[str, object] = {
    "primary_key": "id",
    "password": "s3cret",
    "host": "db.internal",
    "watermark_value": "2026-10-01",
    "connection": "db.internal",
    "spark": "local",
    "client": "default",
    "watermark_column": "updated_at",
    "billing_project": "acme-billing",
}

AT013_ROWS = [
    *[
        ("source", "postgres", key)
        for key in (
            "primary_key",
            "password",
            "host",
            "watermark_value",
            "connection",
            "spark",
            "client",
        )
    ],
    ("source", "disk", "watermark_column"),
    ("source", "disk", "client"),
    ("source", "json_api", "watermark_value"),
    ("source", "json_api", "spark"),
    ("source", "unity_catalog", "billing_project"),
]


@pytest.mark.parametrize(("section", "system", "key"), AT013_ROWS)
def test_at013_a_key_the_system_does_not_take_is_rejected(
    section: str, system: str, key: str, tmp_path: Path
) -> None:
    keys = {"system": system, **REQUIRED[section][system], key: EXTRA_VALUES[key]}
    path = _config(tmp_path, {section: keys})

    message = f"{path}: {section}.{key} is not a key of system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path)


@pytest.mark.parametrize(
    ("first", "second"),
    [("password", "primary_key"), ("primary_key", "password")],
)
def test_at013_of_two_such_keys_the_first_in_the_file_is_named(
    first: str, second: str, tmp_path: Path
) -> None:
    keys = {
        "system": "postgres",
        first: EXTRA_VALUES[first],
        "table_name": "public.orders",
        second: EXTRA_VALUES[second],
    }
    path = _config(tmp_path, {"source": keys})

    message = f"{path}: source.{first} is not a key of system 'postgres'"
    with pytest.raises(ValueError, match=_exactly(message)):
        ReadSource(path)


@pytest.mark.parametrize(
    ("section", "system", "key"),
    [("source", "postgres", "primary_key")],
)
def test_at013_such_a_key_is_named_before_a_missing_required_key(
    section: str, system: str, key: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system, key: EXTRA_VALUES[key]}})

    message = f"{path}: {section}.{key} is not a key of system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path)


AT014_ROWS = [
    (section, system, key)
    for section, table in REQUIRED.items()
    for system, required in table.items()
    for key in required
]


@pytest.mark.parametrize(("section", "system", "key"), AT014_ROWS)
def test_at014_a_missing_required_key_is_rejected(
    section: str, system: str, key: str, tmp_path: Path
) -> None:
    keys = {name: value for name, value in REQUIRED[section][system].items() if name != key}
    path = _config(tmp_path, {section: {"system": system, **keys}})

    message = f"{path}: {section}.{key} is required for system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path)


@pytest.mark.parametrize(
    ("section", "system"),
    [(section, system) for section, table in REQUIRED.items() for system in table],
)
def test_at014_without_any_required_key_the_first_of_the_table_is_named(
    section: str, system: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system}})
    first = next(iter(REQUIRED[section][system]))

    message = f"{path}: {section}.{first} is required for system '{system}'"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path)


OPTIONS_TYPE = "must be an object of strings, numbers and booleans"
AT015_ROWS: list[tuple[str, dict[str, object], str]] = [
    ("source", {"system": "postgres", "table_name": 5}, "source.table_name must be a string"),
    (
        "source",
        {"system": "postgres", "table_name": "public.orders", "watermark_column": None},
        "source.watermark_column must be a string",
    ),
    (
        "source",
        {"system": "json_api", "url": API, "timeout": "10"},
        "source.timeout must be a number",
    ),
    (
        "source",
        {"system": "json_api", "url": API, "timeout": True},
        "source.timeout must be a number",
    ),
    (
        "source",
        {"system": "json_api", "url": API, "headers": {"X-Trace": 1}},
        "source.headers must be an object of strings",
    ),
    (
        "source",
        {"system": "json_api", "url": API, "headers": ["X-Trace"]},
        "source.headers must be an object of strings",
    ),
    (
        "source",
        {**SOURCE_REQUIRED["disk"], "system": "disk", "schema": {"type": "struct", "fields": []}},
        "source.schema must be a string",
    ),
    (
        "source",
        {**SOURCE_REQUIRED["disk"], "system": "disk", "options": {"header": None}},
        f"source.options {OPTIONS_TYPE}",
    ),
    (
        "source",
        {**SOURCE_REQUIRED["disk"], "system": "disk", "options": {"header": [True]}},
        f"source.options {OPTIONS_TYPE}",
    ),
]


@pytest.mark.parametrize(("section", "keys", "message"), AT015_ROWS)
def test_at015_a_value_of_the_wrong_json_type_is_rejected(
    section: str, keys: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: keys})

    with pytest.raises(ValueError, match=_exactly(f"{path}: {message}")):
        BUILD[section](path)


@pytest.mark.parametrize("timeout", [10, 10.5])
def test_at015_a_number_timeout_and_headers_of_strings_are_accepted(
    timeout: float, api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    keys = {"system": "json_api", "url": api_url, "timeout": timeout, "headers": {"X-Trace": "1"}}
    path = _config(tmp_path, {"source": keys})

    read = ReadSource(path)

    assert Process([Step("extract", [read])]).run().state["records"] == payload


def test_at015_options_of_strings_numbers_and_booleans_are_accepted(
    conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    options: dict[str, str | int | float | bool] = {
        "fetchsize": 5000,
        "pushDownPredicate": True,
        "sessionInitStatement": "SET x",
    }
    path = _config(
        tmp_path,
        {"source": {"system": "postgres", "table_name": "public.orders", "options": options}},
    )
    ours, theirs = _World(), _World()

    read = ReadSource(path, connection=conn, spark=_session(ours.spark))
    Process([Step("extract", [read])]).run()
    PostgresReader("public.orders", conn, options=options, spark=_session(theirs.spark)).read()

    assert _transcript(ours.log) == _transcript(theirs.log)


# --- Callback arguments -------------------------------------------------------------------


@SECTIONS
def test_at016_postgres_without_connection_is_rejected(
    section: str, build: Callable[..., object], tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": "postgres", **REQUIRED[section]["postgres"]}})

    message = f"{path}: {section} system 'postgres' needs the argument connection"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


AT017_ROWS: list[tuple[str, str, str]] = [
    *[
        ("source", "json_api", argument)
        for argument in ("connection", "client", "watermark_value", "spark")
    ],
    *[
        ("source", system, argument)
        for system in ("disk", "unity_catalog")
        for argument in ("connection", "client")
    ],
    ("source", "postgres", "client"),
    *[("source", system, "connection") for system in ("s3", "gcs", "volume", "bigquery")],
]


def _grouped(rows: Sequence[tuple[str, str, str]]) -> list[tuple[str, str, tuple[str, ...]]]:
    """The arguments of `rows` that each section and system does not take, together."""
    groups: dict[tuple[str, str], list[str]] = {}
    for section, system, argument in rows:
        groups.setdefault((section, system), []).append(argument)
    return [(section, system, tuple(names)) for (section, system), names in groups.items()]


def _given(argument: str, conn: PostgresConnection) -> object:
    """A value, not `None`, of the callback argument `argument`."""
    values: dict[str, object] = {
        "connection": conn,
        "client": object(),
        "watermark_value": "20261001",
        "spark": _session(_FakeSpark(_Log())),
    }
    return values[argument]


@pytest.mark.parametrize(("section", "system", "argument"), AT017_ROWS)
def test_at017_an_argument_the_system_does_not_take_is_rejected(
    section: str, system: str, argument: str, conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system, **REQUIRED[section][system]}})
    arguments = {argument: _given(argument, conn)}
    if system == "postgres":
        arguments["connection"] = conn

    message = f"{path}: {section} system '{system}' takes no argument {argument}"
    with pytest.raises(ValueError, match=_exactly(message)):
        BUILD[section](path, **arguments)


@pytest.mark.parametrize(("section", "system", "names"), _grouped(AT017_ROWS))
def test_at017_those_arguments_given_as_none_are_not_given(
    section: str, system: str, names: tuple[str, ...], conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _config(tmp_path, {section: {"system": system, **REQUIRED[section][system]}})
    arguments: dict[str, object] = dict.fromkeys(names)
    if system == "postgres":
        arguments["connection"] = conn

    BUILD[section](path, **arguments)


# --- Building the connector ---------------------------------------------------------------

_Arguments: TypeAlias = Callable[[PostgresConnection], dict[str, Any]]
_Direct: TypeAlias = Callable[[PostgresConnection], object]

AT018_SOURCES: list[tuple[dict[str, object], _Arguments, str, _Direct]] = [
    (
        {"system": "unity_catalog", "table_name": "orders"},
        lambda conn: {},
        "table_name must be 'catalog.schema.table'",
        lambda conn: UnityCatalogReader("orders"),
    ),
    (
        {"system": "disk", "path": "landing/orders", "format": "csv"},
        lambda conn: {},
        "path must be an absolute local path",
        lambda conn: DiskReader("landing/orders", "csv"),
    ),
    (
        {"system": "s3", "path": "s3://acme/orders", "format": "delta"},
        lambda conn: {"client": object()},
        "format 'delta' cannot be read file by file",
        lambda conn: S3Reader("s3://acme/orders", "delta", client=cast(Any, object())),
    ),
    (
        {"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"},
        lambda conn: {"connection": conn, "watermark_value": NAIVE},
        "watermark_value must be timezone-aware",
        lambda conn: PostgresReader(
            "public.orders", conn, watermark_column="updated_at", watermark_value=NAIVE
        ),
    ),
]


@pytest.mark.parametrize(
    ("keys", "arguments", "message", "direct"),
    AT018_SOURCES,
    ids=["unity_catalog", "disk", "s3", "postgres"],
)
def test_at018_a_source_value_the_connector_rejects_raises_its_error_with_one_note(
    keys: dict[str, object],
    arguments: _Arguments,
    message: str,
    direct: _Direct,
    conn: PostgresConnection,
    tmp_path: Path,
) -> None:
    path = _config(tmp_path, {"source": keys})
    with pytest.raises(ValueError) as built_directly:
        direct(conn)

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        ReadSource(path, **arguments(conn))

    assert str(excinfo.value) == message
    note = f"while building the source of {path}, system '{keys['system']}'"
    _assert_connector_error(excinfo.value, built_directly.value, note)


AT019_CODE = """
import json
import sys

missing, cases = json.loads(sys.argv[1])
sys.modules[missing] = None
try:
    __import__(missing)
except ImportError:
    pass
else:
    raise AssertionError(f"{missing} can be imported")

from ingestio import ReadSource
from ingestio.connector.all import PostgresConnection

conn = PostgresConnection("db.internal", "shop", "ingest", "s3cret")
outcomes = []
for config, arguments in cases:
    given = {}
    if "connection" in arguments:
        given["connection"] = conn
    if "client" in arguments:
        given["client"] = object()
    try:
        ReadSource(config, **given)
    except Exception as error:
        outcomes.append([type(error).__name__, str(error), getattr(error, "__notes__", [])])
    else:
        outcomes.append(None)
print(json.dumps(outcomes))
"""

_Case: TypeAlias = tuple[dict[str, object], tuple[str, ...]]

S3_SECTION = {"system": "s3", "path": S3_SOURCE, "format": "json"}
GCS_SECTION = {"system": "gcs", "path": GCS_SOURCE, "format": "json"}
VOLUME_SECTION = {"system": "volume", **SOURCE_REQUIRED["volume"]}

AT019_RAISES: dict[str, list[tuple[_Case, str]]] = {
    "boto3": [
        (
            ({"source": S3_SECTION}, ()),
            "S3Reader needs boto3: pip install 'ingestio[s3]'",
        ),
    ],
    "google": [
        (
            ({"source": GCS_SECTION}, ()),
            "GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'",
        ),
        (
            (
                {
                    "source": {
                        "system": "bigquery",
                        "table_name": "acme-data.sales.orders",
                        "watermark_column": "updated_at",
                    }
                },
                (),
            ),
            "BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'",
        ),
    ],
    "databricks": [
        (
            ({"source": VOLUME_SECTION}, ()),
            "VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'",
        ),
    ],
    # Without psycopg, every source of AT019_BUILDS, the `postgres` one included, still builds.
    "psycopg": [],
}

AT019_BUILDS: list[_Case] = [
    ({"source": S3_SECTION}, ("client",)),
    ({"source": GCS_SECTION}, ("client",)),
    ({"source": VOLUME_SECTION}, ("client",)),
    ({"source": {"system": "postgres", "table_name": "public.orders"}}, ("connection",)),
    ({"source": {"system": "bigquery", **SOURCE_REQUIRED["bigquery"]}}, ()),
]


@pytest.mark.parametrize("missing", list(AT019_RAISES))
def test_at019_a_missing_extra_raises_the_connectors_import_error_with_one_note(
    missing: str, tmp_path: Path
) -> None:
    raising = AT019_RAISES[missing]
    cases = [case for case, _ in raising] + AT019_BUILDS
    given: list[list[object]] = []
    expected: list[object] = []
    for number, (document, arguments) in enumerate(cases):
        path = _config(tmp_path, document, f"job{number}.json")
        given.append([os.fspath(path), list(arguments)])
        if number < len(raising):
            system = cast(dict[str, object], document["source"])["system"]
            note = f"while building the source of {path}, system '{system}'"
            expected.append(["ImportError", raising[number][1], [note]])
        else:
            expected.append(None)

    result = _run_python(AT019_CODE, tmp_path, json.dumps([missing, given]))

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected


@pytest.mark.parametrize("system", READ_SYSTEMS)
def test_at020_building_a_read_source_touches_no_session_and_no_client(
    system: str,
    conn: PostgresConnection,
    landing: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    world = _World()
    active: list[str] = []

    def raising_active() -> NoReturn:
        active.append("active")
        raise AssertionError("SparkSession.active was called")

    monkeypatch.setattr(SparkSession, "active", raising_active)
    keys, arguments, _ = _read_row(system, world, _World(), conn, landing)
    path = _config(tmp_path, {"source": {"system": system, **keys}})

    ReadSource(path, into="orders", **arguments)

    assert active == []
    assert world.log.accessed == []
    assert world.log.calls == []


# --- Reading ------------------------------------------------------------------------------


def test_at021_read_source_reads_the_json_api_the_source_section_names(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    keys = {"system": "json_api", "url": api_url, "timeout": 5, "headers": {"X-Trace": "1"}}
    path = _config(tmp_path, {"source": keys})

    process = Process([Step("extract", [ReadSource(path, into="orders")])]).run()

    assert process.state["orders"] == payload


@pytest.mark.parametrize("system", READ_SYSTEMS)
def test_at022_read_source_reads_as_the_reader_the_source_section_names(
    system: str, conn: PostgresConnection, landing: Path, tmp_path: Path
) -> None:
    ours, theirs = _World(), _World()
    keys, arguments, twin = _read_row(system, ours, theirs, conn, landing)
    path = _config(tmp_path, {"source": {"system": system, **keys}})

    process = Process([Step("extract", [ReadSource(path, into="orders", **arguments)])]).run()
    returned = twin().read()

    assert _transcript(ours.log) == _transcript(theirs.log)
    assert process.state["orders"] is ours.log.fakes[_label(returned)]


@pytest.mark.parametrize("system", READ_SYSTEMS)
def test_at023_optional_keys_left_out_leave_the_readers_defaults(
    system: str,
    conn: PostgresConnection,
    landing: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ours, theirs = _World(), _World()
    keys, arguments, twin = _required_read_row(system, ours, theirs, conn, landing)
    path = _config(tmp_path, {"source": {"system": system, **keys}})

    _use(ours, monkeypatch)
    Process([Step("extract", [ReadSource(path, into="orders", **arguments)])]).run()
    _use(theirs, monkeypatch)
    twin().read()

    assert _transcript(ours.log) == _transcript(theirs.log)


# --- Composition --------------------------------------------------------------------------


def test_at026_the_payload_goes_through_records_by_default(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    process = Process([Step("extract", [ReadSource(path)])]).run()

    assert process.state == {"records": payload}


def test_at026_into_chooses_another_state_key(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "out" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    process = Process([Step("extract", [ReadSource(path, into="raw")])]).run()

    assert process.state == {"raw": payload}


def test_at027_one_file_configures_the_read_from_an_api(
    api_url: str, payload: list[dict[str, Any]], tmp_path: Path
) -> None:
    out = tmp_path / "bronze" / "orders.jsonl"
    path = _config(tmp_path, _json_job(api_url, out))

    process = Process([Step("extract", [ReadSource(path)])]).run()

    assert process.state["records"] == payload


def test_at028_read_source_reads_the_table_of_the_source_section(tmp_path: Path) -> None:
    world = _World()
    path = _config(
        tmp_path,
        {
            "source": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
            "target": {"system": "unity_catalog", "table_name": "main.silver.orders"},
        },
    )

    process = Process([Step("extract", [ReadSource(path, spark=_session(world.spark))])]).run()

    reads = [call for call in world.log.calls if call.method in ("table", "load")]
    assert [(call.method, call.args) for call in reads] == [
        ("table", ("`main`.`bronze`.`orders`",))
    ]
    (read,) = reads
    assert read.result is not None
    assert process.state["records"] is world.log.fakes[read.result]


def test_at029_an_error_of_the_read_propagates_unchanged_with_no_note(
    api_url: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, {"source": {"system": "json_api", "url": _boom(api_url)}})
    process = Process([Step("extract", [ReadSource(path)])])

    with pytest.raises(HTTPError) as excinfo:
        process.run()

    assert excinfo.value.code == 500
    assert getattr(excinfo.value, "__notes__", []) == []
