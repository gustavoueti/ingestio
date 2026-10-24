"""The SCD2 write callback: `SCD2` and the `scd2` section of the job's configuration.

Covers `specs/scd2_callback.md`. Every test writes the job's configuration with `json.dumps` to
a file under `tmp_path` and passes its path, and a missing package is simulated in a fresh
interpreter. `SCD2` is reached through `_scd2`, as `ingestio` exports it.

The criteria fall in two sets (decision 34 of the spec):

- AT-001 to AT-014, AT-033, AT-034, AT-036 and AT-037 run in every run of the suite, without
  Spark. Building needs no session, and the criteria that run a process stop at or before the
  check of the target's columns, with the doubles below: a *fake batch*, whose `columns` are the
  batch's column names and whose `sparkSession` is a fake session compared by identity, and a
  *fake target*, with only `columns`.
- AT-015 to AT-032, AT-035, AT-038 and AT-039 to AT-042 need Spark's computation. They are
  marked `integration`, skipped unless `INGESTIO_SPARK_TESTS` is `1`, and run in a local session
  created once for the module.

In both sets `_Spy` replaces `read` on the three readers and `write` on the three writers, on
their classes. Its `write` collects the rows of `data` inside the call, so that a failing
evaluation raises from `write` as it would from the real writer's save. The test of decision 26,
against a real Delta table, also needs `INGESTIO_DELTA_PACKAGE`, and runs in its own interpreter
because a JVM loads its packages once.
"""

from __future__ import annotations

import inspect
import json
import os
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, NoReturn, cast

import psycopg
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, functions

from ingestio import AppendOnly, Callback, FullLoad, Process, ReadSource, Step
from ingestio.connector.all import (
    BigQueryReader,
    BigQueryWriter,
    PostgresConnection,
    PostgresReader,
    PostgresWriter,
    UnityCatalogReader,
    UnityCatalogWriter,
)

API = "https://api.example.com/v1/orders"
TABLE = "main.gold.dim_customer"
SUPPORTED = "bigquery, postgres, unity_catalog"
DEFAULT_TARGET: dict[str, object] = {"system": "unity_catalog", "table_name": TABLE}
DEFAULT_SCD2: dict[str, object] = {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
}
BATCH_COLUMNS = ("customer_id", "name", "city", "updated_at")
TARGET_COLUMNS = (*BATCH_COLUMNS, "valid_from", "valid_to")
BATCH_SCHEMA = "customer_id INT, name STRING, city STRING, updated_at DATE"
TARGET_SCHEMA = f"{BATCH_SCHEMA}, valid_from DATE, valid_to DATE"
NON_EMPTY_ARRAY = "must be a non-empty array of non-empty strings"
NON_EMPTY_STRING = "must be a non-empty string"

# The messages of the checks, for the key `records` and the default column names.
LATE = (
    "state['records'] has a record whose 'updated_at' is earlier than the 'valid_from' of its"
    " entity's current version"
)
SAME_KEY_AND_TIME = (
    "state['records'] has records with the same key columns and the same 'updated_at'"
)
SAME_TIME_OTHER_VALUES = (
    "state['records'] has a record whose 'updated_at' equals the 'valid_from' of its entity's"
    " current version, with other tracked values"
)

# The real `read`, kept before any test replaces it.
REAL_UNITY_CATALOG_READ = UnityCatalogReader.read

SYSTEM_NAMES = ("unity_catalog", "postgres", "bigquery")
SYSTEMS = pytest.mark.parametrize("system", SYSTEM_NAMES)


def _scd2() -> Callable[..., Callback]:
    """`SCD2`, as `ingestio` exports it."""
    from ingestio import SCD2

    build: Callable[..., Callback] = SCD2
    return build


def _date(text: str | None) -> date | None:
    """The date that `text` writes as `YYYY-MM-DD`, or `None` for `None`."""
    return None if text is None else date.fromisoformat(text)


def _record(
    customer_id: int | None, name: str | None, city: str | None, updated_at: str | None
) -> tuple[Any, ...]:
    """A batch row, `(customer_id, name, city, updated_at)`."""
    return (customer_id, name, city, _date(updated_at))


def _version(
    customer_id: int,
    name: str,
    city: str | None,
    updated_at: str,
    valid_from: str,
    valid_to: str | None,
) -> tuple[Any, ...]:
    """A target or written row, `(customer_id, name, city, updated_at, valid_from, valid_to)`."""
    return (customer_id, name, city, _date(updated_at), _date(valid_from), _date(valid_to))


ANA = _version(1, "Ana", "Lisbon", "2026-01-01", "2026-01-01", None)
BRUNO = _record(2, "Bruno", "Porto", "2026-02-01")
BRUNO_OPENED = _version(2, "Bruno", "Porto", "2026-02-01", "2026-02-01", None)
BRUNO_CURRENT = _version(2, "Bruno", "Porto", "2026-01-15", "2026-01-15", None)
PORTO = _record(1, "Ana", "Porto", "2026-03-01")
ANA_CLOSED = _version(1, "Ana", "Lisbon", "2026-01-01", "2026-01-01", "2026-03-01")
PORTO_OPENED = _version(1, "Ana", "Porto", "2026-03-01", "2026-03-01", None)


# --- Test doubles -------------------------------------------------------------------------


class _StopError(Exception):
    """The exception `stop` of the criteria, raised by a replaced `read`."""


class _FakeSession:
    """Stands in for the batch's `SparkSession`: compared by identity, and nothing else."""


class _FakeBatch:
    """Stands in for the batch: only its `columns` and its `sparkSession`."""

    def __init__(self, columns: Sequence[str], session: _FakeSession | None = None) -> None:
        self._columns = list(columns)
        self._session = _FakeSession() if session is None else session

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def sparkSession(self) -> _FakeSession:  # noqa: N802 - PySpark's name
        return self._session


class _FakeTarget:
    """Stands in for the DataFrame of the target: only its `columns`."""

    def __init__(self, columns: Sequence[str]) -> None:
        self._columns = list(columns)

    @property
    def columns(self) -> list[str]:
        return list(self._columns)


class _RecordingClient:
    """Stands in for a BigQuery client: writes the name of every public attribute accessed on
    it to `log`."""

    def __init__(self, log: list[str]) -> None:
        self._log = log

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").append(name)
        return object.__getattribute__(self, name)


@dataclass(frozen=True)
class _Written:
    """One write that collected its data: the writer, `replace`, and `data`'s columns and rows."""

    writer: object
    replace: bool
    columns: list[str]
    rows: list[tuple[Any, ...]]


class _Spy:
    """What the replaced `read` of the readers and `write` of the writers do and record.

    `read` adds its reader to `readers`, then raises `answer` when it is an exception and
    returns it otherwise. `write` adds its writer to `writers`, raises `write_error` when it is
    set, calls `before_write` when it is set, then collects `data` and adds the call to
    `written`; an error of the collection is added to `raised` and propagates.
    """

    def __init__(self) -> None:
        self.answer: object = None
        self.write_error: BaseException | None = None
        self.before_write: Callable[[], object] | None = None
        self.readers: list[object] = []
        self.writers: list[object] = []
        self.written: list[_Written] = []
        self.raised: list[BaseException] = []

    def reset(self) -> None:
        """Forget the calls of a previous run."""
        self.readers.clear()
        self.writers.clear()
        self.written.clear()
        self.raised.clear()

    def read(self, reader: object) -> Any:
        self.readers.append(reader)
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer

    def write(self, writer: object, data: Any, replace: bool) -> None:
        self.writers.append(writer)
        if self.write_error is not None:
            raise self.write_error
        if self.before_write is not None:
            self.before_write()
        try:
            columns = list(data.columns)
            rows = [tuple(row) for row in data.collect()]
        except BaseException as error:
            self.raised.append(error)
            raise
        self.written.append(_Written(writer, replace, columns, rows))


# --- Fixtures and helpers -----------------------------------------------------------------


def _refuse(*args: object, **kwargs: object) -> NoReturn:
    raise AssertionError("the active session, psycopg.connect or a default client was used")


@pytest.fixture(autouse=True)
def _isolated(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `SparkSession.active`, `psycopg.connect` and BigQuery's default client fail when
    called, except in the tests against a local session, which need the real session."""
    if request.node.get_closest_marker("integration") is not None:
        return
    monkeypatch.setattr(SparkSession, "active", _refuse)
    monkeypatch.setattr(psycopg, "connect", _refuse)
    monkeypatch.setattr("google.cloud.bigquery.Client", _refuse)


@pytest.fixture
def spy(monkeypatch: pytest.MonkeyPatch) -> _Spy:
    """Replace `read` on the three readers and `write` on the three writers with one `_Spy`."""
    replaced = _Spy()

    def read(reader: object) -> Any:
        return replaced.read(reader)

    def write(writer: object, data: Any, *, replace: bool = False) -> None:
        replaced.write(writer, data, replace)

    for reader in (UnityCatalogReader, PostgresReader, BigQueryReader):
        monkeypatch.setattr(reader, "read", read)
    for writer in (UnityCatalogWriter, PostgresWriter, BigQueryWriter):
        monkeypatch.setattr(writer, "write", write)
    return replaced


@pytest.fixture
def conn() -> PostgresConnection:
    """The connection the job builds from its own secrets."""
    return PostgresConnection("db.internal", "shop", "ingest", "s3cret")


@pytest.fixture
def bq() -> _RecordingClient:
    """The BigQuery client the job builds."""
    return _RecordingClient([])


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


def _job(
    directory: Path,
    *,
    target: object = DEFAULT_TARGET,
    scd2: object = DEFAULT_SCD2,
    name: str = "job.json",
) -> Path:
    """The configuration of the criteria, with its `target` or `scd2` section replaced."""
    return _config(directory, {"target": target, "scd2": scd2}, name)


def _lines(path: Path) -> list[Any]:
    """The JSON values of the lines of `path`."""
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _run(callback: Callback, state: Mapping[str, Any]) -> Process:
    """Run `callback` alone, in a step `load`, over `state`."""
    return Process([Step("load", [callback])], state=state).run()


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _notes(error: BaseException) -> list[str]:
    """The notes added to `error`."""
    return list(getattr(error, "__notes__", []))


def _run_python(
    code: str, cwd: Path, *args: str, timeout: float = 120
) -> subprocess.CompletedProcess[str]:
    """Run `code` in a fresh interpreter, in `cwd`, with `args` as its arguments."""
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


@dataclass(frozen=True)
class _SystemRow:
    """A row of AT-014: the `target` section, the callback's arguments, the reader twin, built
    with a session, and the writer twin."""

    section: dict[str, object]
    arguments: dict[str, object]
    reader: Callable[[object], object]
    writer: Callable[[], object]


def _system_row(system: str, conn: PostgresConnection, bq: object) -> _SystemRow:
    """The row of AT-014 for `system`."""
    client = cast(Any, bq)
    if system == "unity_catalog":
        return _SystemRow(
            {"system": system, "table_name": TABLE, "options": {"mergeSchema": True}},
            {},
            lambda s: UnityCatalogReader(TABLE, spark=cast(SparkSession, s)),
            lambda: UnityCatalogWriter(TABLE, options={"mergeSchema": True}),
        )
    if system == "postgres":
        return _SystemRow(
            {
                "system": system,
                "table_name": "public.dim_customer",
                "options": {"batchsize": 1000},
            },
            {"connection": conn},
            lambda s: PostgresReader("public.dim_customer", conn, spark=cast(SparkSession, s)),
            lambda: PostgresWriter("public.dim_customer", conn, options={"batchsize": 1000}),
        )
    assert system == "bigquery", system
    return _SystemRow(
        {
            "system": system,
            "table_name": "acme-data.gold.dim_customer",
            "billing_project": "acme-billing",
            "options": {"temporaryGcsBucket": "acme-tmp"},
        },
        {"client": bq},
        lambda s: BigQueryReader(
            "acme-data.gold.dim_customer",
            billing_project="acme-billing",
            client=client,
            spark=cast(SparkSession, s),
        ),
        lambda: BigQueryWriter(
            "acme-data.gold.dim_customer",
            billing_project="acme-billing",
            options={"temporaryGcsBucket": "acme-tmp"},
            client=client,
        ),
    )


def _assert_twin(built: object, twin: object, *same: object) -> None:
    """`built` has the class and the state of `twin`, and holds each object of `same` that the
    twin holds, itself rather than an equal copy."""
    assert type(built) is type(twin), (built, twin)
    assert vars(built) == vars(twin)
    for name, value in vars(twin).items():
        if any(value is item for item in same):
            assert vars(built)[name] is value, name


# --- Import surface and signature ---------------------------------------------------------

AT001_CODE = """
import ingestio
import ingestio.callback.all as callbacks
from ingestio import SCD2, AppendOnly, Callback, FullLoad
from ingestio.callback.all import SCD2 as CallbackSCD2

assert SCD2 is CallbackSCD2
assert "SCD2" in ingestio.__all__, ingestio.__all__
assert "SCD2" in callbacks.__all__, callbacks.__all__
assert issubclass(SCD2, Callback)
assert not issubclass(SCD2, AppendOnly)
assert not issubclass(SCD2, FullLoad)
assert not issubclass(AppendOnly, SCD2)
assert not issubclass(FullLoad, SCD2)
for name in ("ReadSource", "AppendOnly", "FullLoad"):
    assert name in ingestio.__all__, ingestio.__all__
    assert name in callbacks.__all__, callbacks.__all__
    assert getattr(ingestio, name) is getattr(callbacks, name), name
"""


def test_at001_scd2_is_a_callback_exported_by_ingestio_and_callback_all(tmp_path: Path) -> None:
    result = _run_python(AT001_CODE, tmp_path)

    assert result.returncode == 0, result.stderr


def test_at002_the_signature_is_config_then_key_connection_and_client_keyword_only() -> None:
    parameters = inspect.signature(_scd2()).parameters.values()

    assert [(parameter.name, parameter.kind, parameter.default) for parameter in parameters] == [
        ("config", inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.empty),
        ("key", inspect.Parameter.KEYWORD_ONLY, "records"),
        ("connection", inspect.Parameter.KEYWORD_ONLY, None),
        ("client", inspect.Parameter.KEYWORD_ONLY, None),
    ]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("replace", True),
        ("spark", object()),
        ("watermark_value", 1),
        ("key_columns", ["customer_id"]),
    ],
    ids=["replace", "spark", "watermark_value", "key_columns"],
)
def test_at002_an_argument_the_callback_does_not_take_raises_type_error(
    name: str, value: object, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    build = _scd2()

    with pytest.raises(TypeError, match=f"unexpected keyword argument '{name}'"):
        build(path, **{name: value})


def test_at002_key_cannot_be_given_positionally(tmp_path: Path) -> None:
    path = _job(tmp_path)
    build = _scd2()

    with pytest.raises(TypeError):
        build(path, "records")


AT003_CODE = """
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

import ingestio
from ingestio import SCD2

SCD2(sys.argv[1])
"""


def test_at003_scd2_is_built_without_pyspark_or_any_extra(tmp_path: Path) -> None:
    path = _job(tmp_path)

    result = _run_python(AT003_CODE, tmp_path, os.fspath(path))

    assert result.returncode == 0, result.stderr


# --- Building: the target section ---------------------------------------------------------


@pytest.mark.parametrize(
    ("config", "type_name"),
    [
        (UnityCatalogWriter(TABLE), "UnityCatalogWriter"),
        (b"job.json", "bytes"),
        ({"target": DEFAULT_TARGET, "scd2": DEFAULT_SCD2}, "dict"),
    ],
    ids=["UnityCatalogWriter", "bytes", "dict"],
)
def test_at004_a_config_that_is_not_a_path_is_rejected(config: object, type_name: str) -> None:
    build = _scd2()

    message = f"config must be a str or os.PathLike, not {type_name}"
    with pytest.raises(TypeError, match=_exactly(message)):
        build(config)


def test_at004_a_missing_file_raises_file_not_found_error(tmp_path: Path) -> None:
    build = _scd2()

    with pytest.raises(FileNotFoundError):
        build(tmp_path / "missing.json")


def test_at004_text_that_is_not_json_is_rejected(tmp_path: Path) -> None:
    path = _text(tmp_path, '{"target": ')
    build = _scd2()

    with pytest.raises(ValueError) as excinfo:
        build(path)

    assert str(excinfo.value).startswith(f"{os.fspath(path)}: invalid JSON: ")


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ([], "the configuration must be a JSON object"),
        ({"scd2": DEFAULT_SCD2}, "missing section 'target'"),
        (
            {"target": "unity_catalog", "scd2": DEFAULT_SCD2},
            "section 'target' must be a JSON object",
        ),
    ],
    ids=["list", "no target", "target string"],
)
def test_at004_a_file_without_a_target_object_is_rejected(
    document: object, message: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, document)
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


@pytest.mark.parametrize("kind", ["str", "Path"])
def test_at004_a_str_and_a_path_naming_the_file_are_accepted(kind: str, tmp_path: Path) -> None:
    path = _job(tmp_path)
    config: str | Path = os.fspath(path) if kind == "str" else path

    callback = _scd2()(config)

    assert isinstance(callback, Callback)


# The systems of AT-005 that SCD2 does not support, each with the keys its writer requires.
UNSUPPORTED: dict[str, dict[str, object]] = {
    "json_lines": {"path": "/data/out/dim_customer.jsonl"},
    "disk": {"path": "/data/lake/dim_customer", "format": "parquet"},
    "s3": {"path": "s3://acme-lake/dim_customer", "format": "delta"},
    "gcs": {"path": "gs://acme-lake/dim_customer", "format": "delta"},
    "volume": {"volume": "main.lake.curated", "path": "dim_customer", "format": "parquet"},
    "json_api": {},
    "mysql": {},
    "Unity_Catalog": {},
    "": {},
}


@pytest.mark.parametrize("system", list(UNSUPPORTED), ids=[repr(name) for name in UNSUPPORTED])
def test_at005_a_system_scd2_does_not_support_is_rejected(system: str, tmp_path: Path) -> None:
    path = _job(tmp_path, target={"system": system, **UNSUPPORTED[system]})
    build = _scd2()

    message = f"{os.fspath(path)}: target.system '{system}' is not one of: {SUPPORTED}"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


@pytest.mark.parametrize(
    ("section", "message"),
    [
        ({"table_name": TABLE}, "target.system is required"),
        ({"system": 5, "table_name": TABLE}, "target.system must be a string"),
    ],
    ids=["no system", "system 5"],
)
def test_at005_a_missing_or_non_string_system_is_rejected(
    section: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _job(tmp_path, target=section)
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


UC: dict[str, object] = {"system": "unity_catalog", "table_name": TABLE}
PG: dict[str, object] = {"system": "postgres", "table_name": "public.dim_customer"}
BQ: dict[str, object] = {"system": "bigquery", "table_name": "acme-data.gold.dim_customer"}
OPTIONS_TYPE = "must be an object of strings, numbers and booleans"

# The rows of AT-006: a label, the `target` section, the arguments given, and the message.
AT006_ROWS: list[tuple[str, dict[str, object], tuple[str, ...], str]] = [
    (
        "unity_catalog mode",
        {**UC, "mode": "overwrite"},
        (),
        "target.mode is not a key of system 'unity_catalog'",
    ),
    (
        "unity_catalog key_columns",
        {**UC, "key_columns": ["customer_id"]},
        (),
        "target.key_columns is not a key of system 'unity_catalog'",
    ),
    (
        "unity_catalog watermark_column",
        {**UC, "watermark_column": "updated_at"},
        (),
        "target.watermark_column is not a key of system 'unity_catalog'",
    ),
    (
        "postgres no table_name",
        {"system": "postgres"},
        ("connection",),
        "target.table_name is required for system 'postgres'",
    ),
    (
        "bigquery billing_project 5",
        {**BQ, "billing_project": 5},
        (),
        "target.billing_project must be a string",
    ),
    (
        "unity_catalog options null",
        {**UC, "options": {"mergeSchema": None}},
        (),
        f"target.options {OPTIONS_TYPE}",
    ),
    ("postgres no connection", PG, (), "target system 'postgres' needs the argument connection"),
    (
        "unity_catalog connection",
        UC,
        ("connection",),
        "target system 'unity_catalog' takes no argument connection",
    ),
    (
        "unity_catalog client",
        UC,
        ("client",),
        "target system 'unity_catalog' takes no argument client",
    ),
    (
        "postgres client",
        PG,
        ("connection", "client"),
        "target system 'postgres' takes no argument client",
    ),
    (
        "bigquery connection",
        BQ,
        ("connection",),
        "target system 'bigquery' takes no argument connection",
    ),
]


@pytest.mark.parametrize(
    ("section", "given", "message"),
    [row[1:] for row in AT006_ROWS],
    ids=[row[0] for row in AT006_ROWS],
)
def test_at006_a_target_section_or_arguments_that_describe_no_writer_are_rejected(
    section: dict[str, object],
    given: tuple[str, ...],
    message: str,
    conn: PostgresConnection,
    tmp_path: Path,
) -> None:
    path = _job(tmp_path, target=section)
    arguments = {name: conn if name == "connection" else object() for name in given}
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path, **arguments)


@pytest.mark.parametrize("section", [UC, PG, BQ], ids=list(SYSTEM_NAMES))
def test_at006_arguments_given_as_none_are_not_given(
    section: dict[str, object], conn: PostgresConnection, tmp_path: Path
) -> None:
    path = _job(tmp_path, target=section)
    connection = conn if section["system"] == "postgres" else None

    callback = _scd2()(path, connection=connection, client=None)

    assert isinstance(callback, Callback)


AT007_ROWS: list[tuple[dict[str, object], bool, str]] = [
    (
        {"system": "unity_catalog", "table_name": "dim_customer"},
        False,
        "table_name must be 'catalog.schema.table'",
    ),
    (
        {"system": "postgres", "table_name": "a.b.c"},
        True,
        "table_name must be 'table' or 'schema.table'",
    ),
    (
        {"system": "bigquery", "table_name": "acme.gold.dim_customer", "options": {"table": "x"}},
        False,
        "option 'table' is set by the connector",
    ),
]


@pytest.mark.parametrize(("section", "connection", "message"), AT007_ROWS, ids=list(SYSTEM_NAMES))
def test_at007_a_value_the_writer_rejects_raises_its_error_with_one_note(
    section: dict[str, object],
    connection: bool,
    message: str,
    conn: PostgresConnection,
    tmp_path: Path,
) -> None:
    path = _job(tmp_path, target=section)
    arguments = {"connection": conn} if connection else {}
    build = _scd2()

    # Not `match`: pytest matches it against the message and the notes together.
    with pytest.raises(ValueError) as excinfo:
        build(path, **arguments)

    assert str(excinfo.value) == message
    note = f"while building the target of {os.fspath(path)}, system '{section['system']}'"
    assert _notes(excinfo.value) == [note]


AT007_CODE = """
import json
import sys

missing, config, connection = json.loads(sys.argv[1])
sys.modules[missing] = None
try:
    __import__(missing)
except ImportError:
    pass
else:
    raise AssertionError(f"{missing} can be imported")

from ingestio import SCD2
from ingestio.connector.all import PostgresConnection

conn = PostgresConnection("db.internal", "shop", "ingest", "s3cret")
try:
    SCD2(config, **({"connection": conn} if connection else {}))
except Exception as error:
    print(json.dumps([type(error).__name__, str(error), getattr(error, "__notes__", [])]))
else:
    print(json.dumps(None))
"""

AT007_MISSING: dict[str, tuple[dict[str, object], str]] = {
    "psycopg": (PG, "PostgresWriter needs psycopg: pip install 'ingestio[postgres]'"),
    "google": (
        BQ,
        "BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'",
    ),
}


@pytest.mark.parametrize("missing", list(AT007_MISSING))
def test_at007_a_missing_extra_raises_the_writers_import_error_with_one_note(
    missing: str, tmp_path: Path
) -> None:
    section, message = AT007_MISSING[missing]
    path = _job(tmp_path, target=section)
    arguments = json.dumps([missing, os.fspath(path), section["system"] == "postgres"])
    note = f"while building the target of {os.fspath(path)}, system '{section['system']}'"

    result = _run_python(AT007_CODE, tmp_path, arguments)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["ImportError", message, [note]]


# --- Building: the scd2 section -----------------------------------------------------------


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"target": DEFAULT_TARGET}, "missing section 'scd2'"),
        ({"target": DEFAULT_TARGET, "scd2": []}, "section 'scd2' must be a JSON object"),
        (
            {"target": DEFAULT_TARGET, "scd2": "customer_id"},
            "section 'scd2' must be a JSON object",
        ),
        ({"target": DEFAULT_TARGET, "scd2": None}, "section 'scd2' must be a JSON object"),
    ],
    ids=["absent", "list", "string", "null"],
)
def test_at008_a_missing_scd2_section_or_one_that_is_not_an_object_is_rejected(
    document: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, document)
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({}, "missing section 'target'"),
        (
            {"target": {"system": "mysql"}},
            f"target.system 'mysql' is not one of: {SUPPORTED}",
        ),
    ],
    ids=["neither section", "unsupported system"],
)
def test_at008_the_target_section_is_checked_before_the_scd2_section(
    document: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _config(tmp_path, document)
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


def _not_a_key(key: str) -> str:
    return f"scd2.{key} is not a key of section 'scd2'"


# The rows of AT-009: a label, the `scd2` section, and the message.
AT009_ROWS: list[tuple[str, dict[str, object], str]] = [
    ("default plus keys", {**DEFAULT_SCD2, "keys": ["customer_id"]}, _not_a_key("keys")),
    *[
        (f"default plus {key}", {**DEFAULT_SCD2, key: value}, _not_a_key(key))
        for key, value in (
            ("mode", "scd2"),
            ("system", "x"),
            ("is_current_column", "is_current"),
            ("table_name", "x"),
        )
    ],
    ("no key_columns", {"effective_time_column": "updated_at"}, "scd2.key_columns is required"),
    (
        "no effective_time_column",
        {"key_columns": ["customer_id"]},
        "scd2.effective_time_column is required",
    ),
    ("empty", {}, "scd2.key_columns is required"),
    ("keys only", {"keys": ["customer_id"]}, _not_a_key("keys")),
    ("mode then keys", {**DEFAULT_SCD2, "mode": "x", "keys": []}, _not_a_key("mode")),
]


@pytest.mark.parametrize(
    ("section", "message"),
    [row[1:] for row in AT009_ROWS],
    ids=[row[0] for row in AT009_ROWS],
)
def test_at009_an_unknown_or_missing_key_of_the_scd2_section_is_rejected(
    section: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2=section)
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


def _wrong(key: str, expected: str, *values: object) -> list[tuple[str, object, str]]:
    """Rows of AT-010: `key` set to each of `values`, rejected as not being `expected`."""
    return [(key, value, f"scd2.{key} {expected}") for value in values]


# The rows of AT-010: the key, its value, and the message.
AT010_ROWS: list[tuple[str, object, str]] = [
    *_wrong(
        "key_columns",
        NON_EMPTY_ARRAY,
        "customer_id",
        [],
        ["customer_id", 5],
        ["customer_id", ""],
        None,
    ),
    *_wrong("tracked_columns", NON_EMPTY_ARRAY, [], "city", None),
    *_wrong("effective_time_column", NON_EMPTY_STRING, ["updated_at"], "", 5, None),
    *_wrong("valid_from_column", NON_EMPTY_STRING, 5, ""),
    ("valid_to_column", None, f"scd2.valid_to_column {NON_EMPTY_STRING}"),
    (
        "key_columns",
        ["customer_id", "customer_id"],
        "scd2.key_columns names the column 'customer_id' twice",
    ),
    ("tracked_columns", ["city", "City"], "scd2.tracked_columns names the column 'City' twice"),
]


@pytest.mark.parametrize(
    ("key", "value", "message"),
    AT010_ROWS,
    ids=[f"{key} {json.dumps(value)}" for key, value, _ in AT010_ROWS],
)
def test_at010_a_value_of_the_wrong_type_or_a_column_named_twice_is_rejected(
    key: str, value: object, message: str, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, key: value})
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


@pytest.mark.parametrize(
    ("first", "second"),
    [("effective_time_column", "key_columns"), ("key_columns", "effective_time_column")],
)
def test_at010_of_two_values_of_the_wrong_type_the_first_in_the_file_is_named(
    first: str, second: str, tmp_path: Path
) -> None:
    wrong: dict[str, object] = {"key_columns": "customer_id", "effective_time_column": 5}
    expected = {"key_columns": NON_EMPTY_ARRAY, "effective_time_column": NON_EMPTY_STRING}
    path = _job(tmp_path, scd2={first: wrong[first], second: wrong[second]})
    build = _scd2()

    message = f"{os.fspath(path)}: scd2.{first} {expected[first]}"
    with pytest.raises(ValueError, match=_exactly(message)):
        build(path)


def _both(role: str, later: str, name: str) -> str:
    return f"scd2.{role} and scd2.{later} both name the column '{name}'"


# The rows of AT-011: the change to the default `scd2` section, and the message.
AT011_ROWS: list[tuple[dict[str, object], str]] = [
    (
        {"effective_time_column": "customer_id"},
        _both("key_columns", "effective_time_column", "customer_id"),
    ),
    (
        {"tracked_columns": ["city", "customer_id"]},
        _both("key_columns", "tracked_columns", "customer_id"),
    ),
    (
        {"key_columns": ["Customer_ID"], "tracked_columns": ["customer_id"]},
        _both("key_columns", "tracked_columns", "Customer_ID"),
    ),
    (
        {"tracked_columns": ["updated_at"]},
        _both("effective_time_column", "tracked_columns", "updated_at"),
    ),
    (
        {"valid_from_column": "updated_at"},
        _both("effective_time_column", "valid_from_column", "updated_at"),
    ),
    ({"tracked_columns": ["valid_to"]}, _both("tracked_columns", "valid_to_column", "valid_to")),
    (
        {"valid_to_column": "valid_from"},
        _both("valid_from_column", "valid_to_column", "valid_from"),
    ),
    (
        {"valid_from_column": "start", "valid_to_column": "START"},
        _both("valid_from_column", "valid_to_column", "start"),
    ),
    (
        {"effective_time_column": "customer_id", "tracked_columns": ["customer_id"]},
        _both("key_columns", "effective_time_column", "customer_id"),
    ),
]


@pytest.mark.parametrize(
    ("change", "message"),
    AT011_ROWS,
    ids=[json.dumps(change) for change, _ in AT011_ROWS],
)
def test_at011_a_column_with_two_roles_is_rejected(
    change: dict[str, object], message: str, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, **change})
    build = _scd2()

    with pytest.raises(ValueError, match=_exactly(f"{os.fspath(path)}: {message}")):
        build(path)


def test_at011_version_columns_with_other_names_free_their_default_names(tmp_path: Path) -> None:
    section = {
        **DEFAULT_SCD2,
        "valid_from_column": "effective_from",
        "valid_to_column": "effective_to",
        "tracked_columns": ["valid_from"],
    }
    path = _job(tmp_path, scd2=section)

    callback = _scd2()(path)

    assert isinstance(callback, Callback)


# --- Building: what it touches ------------------------------------------------------------


@SYSTEMS
def test_at012_building_touches_no_session_no_client_and_no_database(
    system: str, conn: PostgresConnection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active: list[str] = []
    connects: list[object] = []
    accessed: list[str] = []

    def raising_active() -> NoReturn:
        active.append("active")
        raise AssertionError("SparkSession.active was called")

    def recording_connect(*args: object, **kwargs: object) -> None:
        connects.append((args, kwargs))

    monkeypatch.setattr(SparkSession, "active", raising_active)
    monkeypatch.setattr(psycopg, "connect", recording_connect)
    row = _system_row(system, conn, _RecordingClient(accessed))
    path = _job(tmp_path, target=row.section)

    _scd2()(path, **row.arguments)

    assert active == []
    assert connects == []
    assert accessed == []


@pytest.mark.parametrize("change", ["rewritten", "deleted"])
def test_at012_the_file_is_read_once_when_the_callback_is_built(
    change: str, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    callback = _scd2()(path)
    if change == "rewritten":
        _job(
            tmp_path,
            target={"system": "unity_catalog", "table_name": "main.gold.other"},
            scd2={**DEFAULT_SCD2, "effective_time_column": "inserted_at"},
        )
    else:
        path.unlink()
    stop = _StopError("stop")
    spy.answer = stop

    with pytest.raises(_StopError) as excinfo:
        _run(callback, {"records": _FakeBatch(BATCH_COLUMNS)})

    assert excinfo.value is stop
    assert [repr(reader) for reader in spy.readers] == [
        "UnityCatalogReader(table_name='main.gold.dim_customer')"
    ]


def test_at013_scd2_ignores_the_source_section_and_other_top_level_members(
    tmp_path: Path,
) -> None:
    path = _config(
        tmp_path,
        {
            "source": 5,
            "primary_key": "id",
            "job": {"owner": "data"},
            "target": DEFAULT_TARGET,
            "scd2": DEFAULT_SCD2,
        },
    )

    callback = _scd2()(path)

    assert isinstance(callback, Callback)


def test_at013_the_other_callbacks_ignore_the_scd2_section(tmp_path: Path) -> None:
    out = tmp_path / "out" / "customers.jsonl"
    out.parent.mkdir()
    out.write_text(json.dumps({"id": 0}) + "\n", encoding="utf-8")
    document: dict[str, object] = {
        "source": {"system": "json_api", "url": API},
        "target": {"system": "json_lines", "path": str(out)},
        "scd2": 5,
    }
    path = _config(tmp_path, document)

    ReadSource(path)
    AppendOnly(path)
    FullLoad(path)
    _run(AppendOnly(path), {"records": [{"id": 1}]})
    assert _lines(out) == [{"id": 0}, {"id": 1}]

    _config(tmp_path, {**document, "scd2": DEFAULT_SCD2})
    _run(FullLoad(path), {"records": [{"id": 1}]})
    assert _lines(out) == [{"id": 1}]


# --- Reading the target -------------------------------------------------------------------


@SYSTEMS
def test_at014_the_target_is_read_by_the_reader_of_its_system_with_the_batchs_session(
    system: str, conn: PostgresConnection, bq: _RecordingClient, spy: _Spy, tmp_path: Path
) -> None:
    row = _system_row(system, conn, bq)
    path = _job(tmp_path, target=row.section)
    session = _FakeSession()
    stop = _StopError("stop")
    spy.answer = stop
    callback = _scd2()(path, **row.arguments)

    with pytest.raises(_StopError) as excinfo:
        _run(callback, {"records": _FakeBatch(BATCH_COLUMNS, session)})

    assert excinfo.value is stop
    assert len(spy.readers) == 1, spy.readers
    _assert_twin(spy.readers[0], row.reader(session), session, conn, bq)
    assert spy.writers == []


# --- Checks of the columns ----------------------------------------------------------------

# The rows of AT-033: the batch's columns, the `scd2` section, and the message.
AT033_ROWS: list[tuple[tuple[str, ...], dict[str, object], str]] = [
    (
        ("customer_id", "name", "city"),
        DEFAULT_SCD2,
        "state['records'] has no column 'updated_at', which scd2.effective_time_column names",
    ),
    (
        BATCH_COLUMNS,
        {**DEFAULT_SCD2, "tracked_columns": ["segment"]},
        "state['records'] has no column 'segment', which scd2.tracked_columns names",
    ),
    (
        (*BATCH_COLUMNS, "valid_to"),
        DEFAULT_SCD2,
        "state['records'] already has a column 'valid_to', which scd2.valid_to_column names",
    ),
    (
        (*BATCH_COLUMNS, "VALID_FROM"),
        DEFAULT_SCD2,
        "state['records'] already has a column 'valid_from', which scd2.valid_from_column names",
    ),
]


@pytest.mark.parametrize(
    ("columns", "section", "message"),
    AT033_ROWS,
    ids=["no updated_at", "no segment", "valid_to", "VALID_FROM"],
)
def test_at033_a_batch_that_lacks_a_named_column_or_holds_a_version_column_is_rejected(
    columns: tuple[str, ...],
    section: dict[str, object],
    message: str,
    spy: _Spy,
    tmp_path: Path,
) -> None:
    path = _job(tmp_path, scd2=section)
    callback = _scd2()(path)

    with pytest.raises(ValueError) as excinfo:
        _run(callback, {"records": _FakeBatch(columns)})

    assert str(excinfo.value) == message
    assert _notes(excinfo.value) == []
    assert spy.readers == []
    assert spy.writers == []


def test_at033_the_batchs_columns_are_matched_ignoring_case(spy: _Spy, tmp_path: Path) -> None:
    path = _job(tmp_path)
    stop = _StopError("stop")
    spy.answer = stop
    callback = _scd2()(path)
    batch = _FakeBatch(("Customer_ID", "name", "city", "Updated_At"))

    with pytest.raises(_StopError) as excinfo:
        _run(callback, {"records": batch})

    assert excinfo.value is stop
    assert len(spy.readers) == 1


AT034_ROWS: list[tuple[tuple[str, ...], str]] = [
    (
        ("customer_id", "name", "updated_at", "valid_from", "valid_to"),
        "the target has no column 'city'",
    ),
    (TARGET_COLUMNS[:-1], "the target has no column 'valid_to'"),
    (
        (*TARGET_COLUMNS, "segment"),
        "the target has a column 'segment' that is neither a column of state['records'] nor a"
        " version column",
    ),
]


@pytest.mark.parametrize(
    ("columns", "message"), AT034_ROWS, ids=["no city", "no valid_to", "segment"]
)
def test_at034_a_target_whose_columns_are_not_the_batchs_and_the_version_columns_is_rejected(
    columns: tuple[str, ...], message: str, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    spy.answer = _FakeTarget(columns)
    callback = _scd2()(path)

    with pytest.raises(ValueError) as excinfo:
        _run(callback, {"records": _FakeBatch(BATCH_COLUMNS)})

    assert str(excinfo.value) == message
    assert _notes(excinfo.value) == []
    assert len(spy.readers) == 1
    assert spy.writers == []


# --- Errors -------------------------------------------------------------------------------


def test_at036_a_missing_records_key_raises_key_error_before_anything_is_read(
    spy: _Spy, tmp_path: Path
) -> None:
    process = Process([Step("load", [_scd2()(_job(tmp_path))])], state={})

    with pytest.raises(KeyError) as excinfo:
        process.run()

    assert excinfo.value.args == ("records",)
    assert _notes(excinfo.value) == []
    assert spy.readers == []
    assert spy.writers == []


def test_at036_a_missing_key_raises_key_error_naming_it(spy: _Spy, tmp_path: Path) -> None:
    callback = _scd2()(_job(tmp_path), key="customers")
    process = Process([Step("load", [callback])], state={"records": _FakeBatch(BATCH_COLUMNS)})

    with pytest.raises(KeyError) as excinfo:
        process.run()

    assert excinfo.value.args == ("customers",)
    assert _notes(excinfo.value) == []
    assert spy.readers == []
    assert spy.writers == []


def test_at037_an_error_of_the_read_propagates_unchanged_with_no_note(
    spy: _Spy, tmp_path: Path
) -> None:
    err = RuntimeError("the target cannot be read")
    spy.answer = err
    callback = _scd2()(_job(tmp_path))

    with pytest.raises(RuntimeError) as excinfo:
        _run(callback, {"records": _FakeBatch(BATCH_COLUMNS)})

    assert excinfo.value is err
    assert _notes(err) == []
    assert spy.writers == []


# --- With Spark: opt-in -------------------------------------------------------------------

needs_spark = pytest.mark.skipif(
    os.environ.get("INGESTIO_SPARK_TESTS") != "1",
    reason="INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in",
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """A local Spark session in UTC, with a warehouse of its own for the catalog's tables."""
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-scd2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def dim_customer(spark: SparkSession) -> Iterator[str]:
    """The Parquet table `spark_catalog.default.dim_customer` of the session's own catalog,
    holding `Ana`, dropped afterwards."""
    name = "spark_catalog.default.dim_customer"
    spark.createDataFrame([ANA], TARGET_SCHEMA).write.format("parquet").saveAsTable(name)
    try:
        yield name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _load(
    spark: SparkSession,
    spy: _Spy,
    path: Path,
    batch: Sequence[tuple[Any, ...]] | DataFrame,
    target: Sequence[tuple[Any, ...]],
    *,
    target_schema: str = TARGET_SCHEMA,
) -> Process:
    """Run a new `SCD2(path)` over `batch`, rows of the batch schema or a DataFrame, with the
    replaced `read` returning the rows `target`."""
    callback = _scd2()(path)
    spy.answer = spark.createDataFrame(list(target), target_schema)
    frame = (
        batch if isinstance(batch, DataFrame) else spark.createDataFrame(list(batch), BATCH_SCHEMA)
    )
    return _run(callback, {"records": frame})


def _written(spy: _Spy) -> _Written:
    """The only write of the run, after checking that it was called once, with `replace=True`."""
    assert len(spy.writers) == 1, spy.writers
    assert len(spy.written) == 1, spy.raised
    (written,) = spy.written
    assert written.replace is True
    return written


def _assert_written(spy: _Spy, *rows: tuple[Any, ...]) -> None:
    """`write` was called once, with `replace=True`, and the written rows are exactly `rows`."""
    assert Counter(_written(spy).rows) == Counter(rows)


def _assert_rejected(error: BaseException, message: str, spy: _Spy) -> None:
    """`error` is the `ValueError` of a check, with `message` and no note, and the writer was
    not called."""
    assert type(error) is ValueError, repr(error)
    assert str(error) == message
    assert _notes(error) == []
    assert spy.writers == []


@pytest.mark.integration
@needs_spark
def test_at015_the_target_is_read_with_the_session_of_the_batch(
    spark: SparkSession,
    dim_customer: str,
    spy: _Spy,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sessions: list[SparkSession] = []

    def read(reader: UnityCatalogReader) -> DataFrame:
        frame = REAL_UNITY_CATALOG_READ(reader)
        sessions.append(frame.sparkSession)
        return frame

    monkeypatch.setattr(UnityCatalogReader, "read", read)
    path = _job(tmp_path, target={"system": "unity_catalog", "table_name": dim_customer})
    callback = _scd2()(path)
    other = spark.newSession()
    batch = other.createDataFrame([BRUNO], BATCH_SCHEMA)
    # `createDataFrame` makes its own session the active one: this makes `spark` active again.
    spark.createDataFrame([], "id INT")
    assert SparkSession.active() is spark

    _run(callback, {"records": batch})

    assert len(sessions) == 1
    assert sessions[0] is other
    _assert_written(spy, ANA, BRUNO_OPENED)


# --- Versions -----------------------------------------------------------------------------


@pytest.mark.integration
@needs_spark
def test_at016_a_new_entity_opens_its_first_version(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [BRUNO], [ANA])

    _assert_written(spy, ANA, BRUNO_OPENED)
    assert len(spy.readers) == 1


@pytest.mark.integration
@needs_spark
@pytest.mark.parametrize("name", ["Ana", "Ana Maria"])
def test_at017_a_change_closes_the_current_version_and_opens_a_new_one(
    name: str, spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [_record(1, name, "Porto", "2026-03-01")], [ANA])

    _assert_written(spy, ANA_CLOSED, _version(1, name, "Porto", "2026-03-01", "2026-03-01", None))


@pytest.mark.integration
@needs_spark
def test_at018_a_record_with_the_same_tracked_values_changes_nothing(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [_record(1, "Ana", "Lisbon", "2026-03-01")], [ANA])

    _assert_written(spy, ANA)


@pytest.mark.integration
@needs_spark
def test_at019_closed_versions_are_written_as_they_were_read(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    faro = _version(1, "Ana", "Faro", "2025-01-01", "2025-01-01", "2026-01-01")

    _load(spark, spy, _job(tmp_path), [PORTO], [faro, ANA])

    _assert_written(spy, faro, ANA_CLOSED, PORTO_OPENED)


@pytest.mark.integration
@needs_spark
def test_at020_an_entity_absent_from_the_batch_keeps_its_current_version(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(
        spark,
        spy,
        _job(tmp_path),
        [_record(1, "Ana", "Braga", "2026-03-01")],
        [ANA, BRUNO_CURRENT],
    )

    _assert_written(
        spy,
        BRUNO_CURRENT,
        ANA_CLOSED,
        _version(1, "Ana", "Braga", "2026-03-01", "2026-03-01", None),
    )


@pytest.mark.integration
@needs_spark
@pytest.mark.parametrize("arrangement", ["as listed", "reversed", "four partitions"])
def test_at021_the_records_of_an_entity_are_chained_by_their_effective_time(
    arrangement: str, spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    records = [
        _record(1, "Ana", "Braga", "2026-05-01"),
        _record(1, "Ana", "Porto", "2026-03-01"),
        _record(3, "Carla", "Faro", "2026-02-01"),
        _record(3, "Carla", "Evora", "2026-04-01"),
    ]
    if arrangement == "reversed":
        records.reverse()
    batch = spark.createDataFrame(records, BATCH_SCHEMA)
    if arrangement == "four partitions":
        batch = batch.repartition(4)

    _load(spark, spy, _job(tmp_path), batch, [ANA])

    _assert_written(
        spy,
        ANA_CLOSED,
        _version(1, "Ana", "Porto", "2026-03-01", "2026-03-01", "2026-05-01"),
        _version(1, "Ana", "Braga", "2026-05-01", "2026-05-01", None),
        _version(3, "Carla", "Faro", "2026-02-01", "2026-02-01", "2026-04-01"),
        _version(3, "Carla", "Evora", "2026-04-01", "2026-04-01", None),
    )


AT022_RECORDS = [
    _record(1, "Ana", "Lisbon", "2026-02-01"),
    _record(1, "Ana", "Porto", "2026-03-01"),
    _record(1, "Ana", "Porto", "2026-04-01"),
    _record(1, "Ana", "Lisbon", "2026-05-01"),
]
AT022_WRITTEN = [
    ANA_CLOSED,
    _version(1, "Ana", "Porto", "2026-03-01", "2026-03-01", "2026-05-01"),
    _version(1, "Ana", "Lisbon", "2026-05-01", "2026-05-01", None),
]


@pytest.mark.integration
@needs_spark
def test_at022_a_record_equal_to_the_version_before_it_in_the_chain_opens_no_version(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), AT022_RECORDS, [ANA])

    _assert_written(spy, *AT022_WRITTEN)


@pytest.mark.integration
@needs_spark
def test_at022_the_same_records_in_two_runs_give_the_same_rows(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    _load(spark, spy, path, AT022_RECORDS[:2], [ANA])
    first = _written(spy).rows
    spy.reset()

    _load(spark, spy, path, AT022_RECORDS[2:], first)

    _assert_written(spy, *AT022_WRITTEN)


@pytest.mark.integration
@needs_spark
def test_at023_a_change_in_an_untracked_column_alone_opens_no_version(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, "tracked_columns": ["city"]})

    _load(spark, spy, path, [_record(1, "Ana Maria", "Lisbon", "2026-03-01")], [ANA])

    _assert_written(spy, ANA)


@pytest.mark.integration
@needs_spark
def test_at023_a_change_in_a_tracked_column_opens_a_version_with_every_column_of_the_record(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, "tracked_columns": ["city"]})

    _load(spark, spy, path, [_record(1, "Ana Maria", "Porto", "2026-03-01")], [ANA])

    _assert_written(
        spy, ANA_CLOSED, _version(1, "Ana Maria", "Porto", "2026-03-01", "2026-03-01", None)
    )


AT024_TARGET = [
    _version(1, "Ana", None, "2026-01-01", "2026-01-01", None),
    _version(2, "Bruno", "Porto", "2026-01-01", "2026-01-01", None),
]


@pytest.mark.integration
@needs_spark
def test_at024_two_nulls_are_equal_and_a_null_and_a_value_differ(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    batch = [_record(1, "Ana", None, "2026-03-01"), _record(2, "Bruno", None, "2026-03-01")]

    _load(spark, spy, _job(tmp_path), batch, AT024_TARGET)

    _assert_written(
        spy,
        AT024_TARGET[0],
        _version(2, "Bruno", "Porto", "2026-01-01", "2026-01-01", "2026-03-01"),
        _version(2, "Bruno", None, "2026-03-01", "2026-03-01", None),
    )


@pytest.mark.integration
@needs_spark
def test_at024_a_value_in_place_of_a_null_opens_a_version(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [_record(1, "Ana", "Lisbon", "2026-03-01")], AT024_TARGET)

    _assert_written(
        spy,
        _version(1, "Ana", None, "2026-01-01", "2026-01-01", "2026-03-01"),
        _version(1, "Ana", "Lisbon", "2026-03-01", "2026-03-01", None),
        AT024_TARGET[1],
    )


@pytest.mark.integration
@needs_spark
def test_at025_the_version_columns_take_the_names_of_the_section(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    section = {
        **DEFAULT_SCD2,
        "valid_from_column": "effective_from",
        "valid_to_column": "effective_to",
    }
    schema = f"{BATCH_SCHEMA}, effective_from DATE, effective_to DATE"

    _load(spark, spy, _job(tmp_path, scd2=section), [PORTO], [ANA], target_schema=schema)

    written = _written(spy)
    assert written.columns == [*BATCH_COLUMNS, "effective_from", "effective_to"]
    assert Counter(written.rows) == Counter([ANA_CLOSED, PORTO_OPENED])


@pytest.mark.integration
@needs_spark
def test_at025_by_default_exactly_the_six_columns_of_the_target_are_written(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [PORTO], [ANA])

    assert _written(spy).columns == list(TARGET_COLUMNS)


@pytest.mark.integration
@needs_spark
def test_at026_an_empty_batch_writes_the_rows_the_target_held(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [], [ANA, BRUNO_CURRENT])

    _assert_written(spy, ANA, BRUNO_CURRENT)


@pytest.mark.integration
@needs_spark
def test_at027_rerunning_a_batch_with_one_record_per_entity_changes_nothing(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    batch = [PORTO, _record(2, "Bruno", "Faro", "2026-02-01")]
    _load(spark, spy, path, batch, [ANA])
    first = _written(spy).rows
    spy.reset()

    _load(spark, spy, path, batch, first)

    assert Counter(_written(spy).rows) == Counter(first)


@pytest.mark.integration
@needs_spark
def test_at027_rerunning_a_batch_with_several_records_of_an_entity_fails_as_late_arriving(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path)
    batch = [PORTO, _record(1, "Ana", "Braga", "2026-04-01")]
    _load(spark, spy, path, batch, [ANA])
    first = _written(spy).rows
    spy.reset()

    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, path, batch, first)

    _assert_rejected(excinfo.value, LATE, spy)


# --- Checks -------------------------------------------------------------------------------


@pytest.mark.integration
@needs_spark
def test_at028_a_record_at_the_current_versions_start_with_its_values_changes_nothing(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [_record(1, "Ana", "Lisbon", "2026-01-01")], [ANA])

    _assert_written(spy, ANA)


@pytest.mark.integration
@needs_spark
def test_at028_a_record_at_the_current_versions_start_with_other_values_is_rejected(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, _job(tmp_path), [_record(1, "Ana", "Porto", "2026-01-01")], [ANA])

    _assert_rejected(excinfo.value, SAME_TIME_OTHER_VALUES, spy)


@pytest.mark.integration
@needs_spark
def test_at028_only_the_tracked_values_are_compared_at_the_current_versions_start(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, "tracked_columns": ["city"]})

    _load(spark, spy, path, [_record(1, "Ana Maria", "Lisbon", "2026-01-01")], [ANA])

    _assert_written(spy, ANA)


@pytest.mark.integration
@needs_spark
def test_at029_a_record_earlier_than_its_entitys_current_version_is_rejected(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    batch = [BRUNO, _record(1, "Ana", "Porto", "2025-12-01")]

    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, _job(tmp_path), batch, [ANA])

    _assert_rejected(excinfo.value, LATE, spy)


@pytest.mark.integration
@needs_spark
def test_at029_an_early_record_of_a_new_entity_is_written(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    _load(spark, spy, _job(tmp_path), [_record(2, "Bruno", "Porto", "2025-12-01")], [ANA])

    _assert_written(spy, ANA, _version(2, "Bruno", "Porto", "2025-12-01", "2025-12-01", None))


@pytest.mark.integration
@needs_spark
@pytest.mark.parametrize(
    "batch",
    [
        [PORTO, _record(1, "Ana", "Braga", "2026-03-01")],
        [_record(3, "Carla", "Faro", "2026-02-01"), _record(3, "Carla", "Faro", "2026-02-01")],
    ],
    ids=["other values", "identical"],
)
def test_at030_two_records_with_the_same_key_and_time_are_rejected(
    batch: list[tuple[Any, ...]], spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, _job(tmp_path), batch, [ANA])

    _assert_rejected(excinfo.value, SAME_KEY_AND_TIME, spy)


@pytest.mark.integration
@needs_spark
@pytest.mark.parametrize(
    ("record", "key_columns", "column"),
    [
        (_record(None, "Ana", "Porto", "2026-03-01"), ["customer_id"], "customer_id"),
        (_record(1, "Ana", "Porto", None), ["customer_id"], "updated_at"),
        (_record(1, None, "Porto", None), ["customer_id", "name"], "name"),
    ],
    ids=["null key", "null time", "null second key"],
)
def test_at031_a_null_in_a_key_column_or_the_effective_time_is_rejected(
    record: tuple[Any, ...],
    key_columns: list[str],
    column: str,
    spark: SparkSession,
    spy: _Spy,
    tmp_path: Path,
) -> None:
    path = _job(tmp_path, scd2={**DEFAULT_SCD2, "key_columns": key_columns})

    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, path, [record], [ANA])

    _assert_rejected(excinfo.value, f"state['records'] has a null in column '{column}'", spy)


AT032_ROWS: list[tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], str]] = [
    (
        [_record(None, "Bruno", "Porto", "2026-03-01"), _record(1, "Ana", "Porto", "2025-12-01")],
        [ANA],
        "state['records'] has a null in column 'customer_id'",
    ),
    (
        [
            _record(3, "Carla", "Faro", "2026-02-01"),
            _record(3, "Carla", "Faro", "2026-02-01"),
            _record(1, "Ana", "Porto", "2025-12-01"),
        ],
        [ANA],
        SAME_KEY_AND_TIME,
    ),
    (
        [_record(1, "Ana", "Porto", "2025-12-01"), _record(2, "Bruno", "Porto", "2026-01-01")],
        [ANA, _version(2, "Bruno", "Faro", "2026-01-01", "2026-01-01", None)],
        LATE,
    ),
]


@pytest.mark.integration
@needs_spark
@pytest.mark.parametrize(
    ("batch", "target", "message"),
    AT032_ROWS,
    ids=["null and late", "same key and time and late", "late and same time"],
)
def test_at032_of_two_failing_checks_the_first_in_the_order_raises(
    batch: list[tuple[Any, ...]],
    target: list[tuple[Any, ...]],
    message: str,
    spark: SparkSession,
    spy: _Spy,
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError) as excinfo:
        _load(spark, spy, _job(tmp_path), batch, target)

    _assert_rejected(excinfo.value, message, spy)


@pytest.mark.integration
@needs_spark
def test_at035_a_batch_that_changes_between_the_checks_and_the_write_fails_the_write(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    marker = tmp_path / "write started"

    def effective_time(customer_id: int) -> date:
        return date(2025, 12, 1) if marker.exists() else date(2026, 3, 1)

    moving = functions.udf(effective_time, "date").asNondeterministic()
    batch = spark.createDataFrame(
        [(1, "Ana", "Porto")], "customer_id INT, name STRING, city STRING"
    ).withColumn("updated_at", moving("customer_id"))
    spy.before_write = marker.touch
    spy.answer = spark.createDataFrame([ANA], TARGET_SCHEMA)
    callback = _scd2()(_job(tmp_path))
    fragment = (
        "has a record whose 'updated_at' is earlier than the 'valid_from' of its entity's"
        " current version"
    )

    with pytest.raises(Exception, match=re.escape(fragment)) as excinfo:
        _run(callback, {"records": batch})

    assert len(spy.writers) == 1
    assert spy.written == []
    assert len(spy.raised) == 1
    assert excinfo.value is spy.raised[0]


# --- Errors and the state -----------------------------------------------------------------


@pytest.mark.integration
@needs_spark
def test_at038_the_state_keeps_its_keys_and_its_objects(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    callback = _scd2()(_job(tmp_path))
    spy.answer = spark.createDataFrame([ANA], TARGET_SCHEMA)
    batch = spark.createDataFrame([BRUNO], BATCH_SCHEMA)
    other = object()

    process = _run(callback, {"records": batch, "other": other})

    assert sorted(process.state) == ["other", "records"]
    assert process.state["records"] is batch
    assert process.state["other"] is other


# --- Parts that need Spark of the criteria above ------------------------------------------


@pytest.mark.integration
@needs_spark
@SYSTEMS
def test_at039_the_new_table_is_written_by_the_writer_of_the_target_section(
    system: str,
    conn: PostgresConnection,
    bq: _RecordingClient,
    spark: SparkSession,
    spy: _Spy,
    tmp_path: Path,
) -> None:
    row = _system_row(system, conn, bq)
    callback = _scd2()(_job(tmp_path, target=row.section), **row.arguments)
    spy.answer = spark.createDataFrame([ANA], TARGET_SCHEMA)

    _run(callback, {"records": spark.createDataFrame([BRUNO], BATCH_SCHEMA)})

    _assert_twin(_written(spy).writer, row.writer(), conn, bq)


@pytest.mark.integration
@needs_spark
def test_at040_the_batchs_columns_are_matched_ignoring_case(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    schema = "Customer_ID INT, name STRING, city STRING, Updated_At DATE"
    batch = spark.createDataFrame([PORTO], schema)

    _load(spark, spy, _job(tmp_path), batch, [ANA])

    _assert_written(spy, ANA_CLOSED, PORTO_OPENED)


def _target_order(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """A row of the default target schema in the order of AT-041's target."""
    customer_id, name, city, updated_at, valid_from, valid_to = row
    return (valid_to, customer_id, valid_from, name, city, updated_at)


@pytest.mark.integration
@needs_spark
def test_at041_the_new_table_has_the_columns_of_the_target_in_its_order(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    schema = (
        "valid_to DATE, customer_id INT, valid_from DATE, name STRING, city STRING, updated_at DATE"
    )

    _load(spark, spy, _job(tmp_path), [BRUNO], [_target_order(ANA)], target_schema=schema)

    written = _written(spy)
    assert written.columns == [
        "valid_to",
        "customer_id",
        "valid_from",
        "name",
        "city",
        "updated_at",
    ]
    assert Counter(written.rows) == Counter([_target_order(ANA), _target_order(BRUNO_OPENED)])


@pytest.mark.integration
@needs_spark
def test_at042_an_error_of_the_write_propagates_unchanged_with_no_note(
    spark: SparkSession, spy: _Spy, tmp_path: Path
) -> None:
    err = RuntimeError("the target cannot be written")
    spy.write_error = err

    with pytest.raises(RuntimeError) as excinfo:
        _load(spark, spy, _job(tmp_path), [BRUNO], [ANA])

    assert excinfo.value is err
    assert _notes(err) == []


@pytest.mark.integration
@needs_spark
def test_at042_a_missing_target_raises_the_readers_error_and_writes_nothing(
    spark: SparkSession, spy: _Spy, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    name = "spark_catalog.default.missing"
    assert not spark.catalog.tableExists(name)
    monkeypatch.setattr(UnityCatalogReader, "read", REAL_UNITY_CATALOG_READ)
    callback = _scd2()(_job(tmp_path, target={"system": "unity_catalog", "table_name": name}))

    with pytest.raises(AnalysisException):
        _run(callback, {"records": spark.createDataFrame([BRUNO], BATCH_SCHEMA)})

    assert spy.writers == []


# --- Integration: the Delta test of decision 26 -------------------------------------------

DELTA_PACKAGE = os.environ.get("INGESTIO_DELTA_PACKAGE", "")

needs_delta = pytest.mark.skipif(
    not DELTA_PACKAGE,
    reason=(
        "INGESTIO_DELTA_PACKAGE is not set: it names the Delta Lake package that matches the"
        " installed pyspark, such as io.delta:delta-spark_2.13:<version>"
    ),
)

DELTA_CODE = """
import json
import sys
from collections import Counter
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from ingestio import SCD2, Process, Step

package, directory = sys.argv[1], Path(sys.argv[2])
NAME = "spark_catalog.default.dim_customer"
BATCH = "customer_id INT, name STRING, city STRING, updated_at DATE"
TARGET = BATCH + ", valid_from DATE, valid_to DATE"
LATE = (
    "state['records'] has a record whose 'updated_at' is earlier than the 'valid_from' of its"
    " entity's current version"
)
D = date.fromisoformat

spark = (
    SparkSession.builder.master("local[2]")
    .appName("ingestio-scd2-delta")
    .config("spark.jars.packages", package)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.warehouse.dir", str(directory / "warehouse"))
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
config = directory / "job.json"
config.write_text(
    json.dumps(
        {
            "target": {"system": "unity_catalog", "table_name": NAME},
            "scd2": {"key_columns": ["customer_id"], "effective_time_column": "updated_at"},
        }
    ),
    encoding="utf-8",
)
ana = (1, "Ana", "Lisbon", D("2026-01-01"), D("2026-01-01"), None)
spark.createDataFrame([ana], TARGET).write.format("delta").saveAsTable(NAME)


def version():
    return spark.sql(f"DESCRIBE HISTORY {NAME}").agg({"version": "max"}).first()[0]


def rows():
    return Counter(tuple(row) for row in spark.table(NAME).collect())


def run(record):
    batch = spark.createDataFrame([record], BATCH)
    Process([Step("load", [SCD2(config)])], state={"records": batch}).run()


created = version()

run((1, "Ana", "Porto", D("2026-03-01")))
porto = Counter(
    [
        (1, "Ana", "Lisbon", D("2026-01-01"), D("2026-01-01"), D("2026-03-01")),
        (1, "Ana", "Porto", D("2026-03-01"), D("2026-03-01"), None),
    ]
)
assert rows() == porto, rows()
assert version() == created + 1, version()

run((1, "Ana", "Braga", D("2026-04-01")))
braga = Counter(
    [
        (1, "Ana", "Lisbon", D("2026-01-01"), D("2026-01-01"), D("2026-03-01")),
        (1, "Ana", "Porto", D("2026-03-01"), D("2026-03-01"), D("2026-04-01")),
        (1, "Ana", "Braga", D("2026-04-01"), D("2026-04-01"), None),
    ]
)
assert rows() == braga, rows()
assert version() == created + 2, version()

try:
    run((1, "Ana", "Faro", D("2026-02-01")))
except ValueError as error:
    assert str(error) == LATE, str(error)
else:
    raise AssertionError("a late-arriving record was written")
assert rows() == braga, rows()
assert version() == created + 2, version()
spark.stop()
"""


@pytest.mark.integration
@needs_delta
@needs_spark
def test_scd2_rewrites_a_delta_table_its_own_plan_reads_with_one_version_per_run(
    tmp_path: Path,
) -> None:
    # Decides whether `unity_catalog` stays supported (decision 26). A JVM loads its packages
    # once, so the session with Delta Lake lives in an interpreter of its own.
    result = _run_python(DELTA_CODE, tmp_path, DELTA_PACKAGE, os.fspath(tmp_path), timeout=900)

    assert result.returncode == 0, result.stderr
