"""Databricks volume connectors: `VolumeReader` and `VolumeWriter`.

Covers `specs/volume_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no workspace: they use the doubles below, a fake
`WorkspaceClient` whose Files API lists `DirectoryEntry` objects through an iterator, a fake
`databricks.sdk.WorkspaceClient`, a fake Spark session and fake DataFrames, which all write to
one ordered event log.

The integration tests at the end run the connectors against a Unity Catalog volume of a
Databricks workspace. They are marked `integration` and skipped unless
`INGESTIO_DATABRICKS_VOLUME` holds the three-part name of a scratch volume,
`catalog.schema.volume`. Each test works in a directory of its own at the root of that volume,
`ingestio-<uuid>`, which it creates and deletes at the end through the Files API. The client is
`WorkspaceClient()`, configured by the SDK's standard environment (`DATABRICKS_HOST` with a
token, or a profile). The session is the active one when pytest runs on a cluster, and otherwise
`DatabricksSession.builder.getOrCreate()` of Databricks Connect, which takes the same
configuration and replaces pyspark in that environment. The tests leave the session's time zone
as it is: they compare modification times through `unix_seconds`, which does not depend on it.
They need no local Java.
"""

from __future__ import annotations

import dataclasses
import importlib
import importlib.metadata
import io
import logging
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, TypeAlias, cast

import databricks.sdk
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.files import DirectoryEntry
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import IntegerType, StructField, StructType

from ingestio import Process, Reader, ReadSource, Step, Writer, WriteTarget
from ingestio.connector.all import VolumeReader, VolumeWriter

T0 = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUS_3 = timezone(timedelta(hours=-3))
VOL = "main.landing.raw"
LOC = "/Volumes/main/landing/raw/orders"
TARGET = "main.lake.curated"
TARGET_LOC = "/Volumes/main/lake/curated/orders"
P = r"orders_(\d{8})\.csv"
ORDER_FILES = ("orders_20261019.csv", "orders_20261020.csv", "orders_20261021.csv")
ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")
DATABRICKS_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']databricks["']""")
NO_CONFIGURATION = (
    "default auth: cannot configure default credentials, please check "
    "https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication"
    " to configure credentials for your preferred authentication method."
)


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Listing:
    """`list_directory_contents()` ran on the fake Files API of `client`, with these
    arguments."""

    args: tuple[object, ...]
    kwargs: dict[str, object]
    client: _FakeWorkspaceClient = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Yielded:
    """The iterator that `listing` returned yielded `entry`."""

    entry: DirectoryEntry
    listing: _Listing = field(repr=False, compare=False)


@dataclass(frozen=True)
class _End:
    """The iteration of the iterator that `listing` returned reached its end."""

    listing: _Listing = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Load:
    """`load()` ran on a fake `spark.read`.

    It records the format, the schema object or `None`, the merged options and the `path`
    argument, the very object passed.
    """

    source: str | None
    schema: object
    options: dict[str, str]
    path: object
    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Create:
    """`createDataFrame()` ran on the fake session, with this data and this schema object."""

    data: object
    schema: object
    frame: _FakeDataFrame = field(repr=False, compare=False)


@dataclass(frozen=True)
class _Save:
    """`save()` ran on a fake `DataFrame.write`, by the writer of `frame`.

    It records the format, the mode, the merged options, the `path` argument and the
    `partitionBy` argument of `save()`.
    """

    source: str | None
    mode: str | None
    options: dict[str, str]
    path: object
    partition_by: object
    frame: _FakeDataFrame = field(repr=False, compare=False)


_Event: TypeAlias = _Listing | _Yielded | _End | _Load | _Create | _Save


class _Log:
    """The ordered event log every double writes to."""

    def __init__(self) -> None:
        self.events: list[_Event] = []
        self.spark_attributes: list[str] = []


class _FakeWorkspaceClient:
    """Stands in for a `databricks.sdk.WorkspaceClient`, and records every attribute accessed
    on it.

    Its `files` is a `_FakeFilesApi`. The directory it lists is `_entries`, which `_hold`
    replaces; it starts empty. `listing_error` and `entry_error` are those of `_FakeFilesApi`.
    """

    def __init__(
        self,
        log: _Log,
        *,
        listing_error: BaseException | None = None,
        entry_error: tuple[int, BaseException] | None = None,
    ) -> None:
        self._accessed: list[str] = []
        self._log = log
        self._entries: list[DirectoryEntry] = []
        self._files = _FakeFilesApi(self, listing_error, entry_error)

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def files(self) -> _FakeFilesApi:
        return self._files


class _FakeFilesApi:
    """Stands in for the Files API of a client, `client.files`, and records every attribute
    accessed on it.

    `list_directory_contents()` records a `_Listing` and returns an iterator over the entries of
    its client, as they are when it is called, or raises `listing_error` once recorded, when it
    is set. The iterator records a `_Yielded` before it yields each entry and an `_End` when its
    end is reached. When `entry_error` is `(index, error)`, it raises `error` when the iteration
    reaches the entry at `index`, before yielding it.
    """

    def __init__(
        self,
        client: _FakeWorkspaceClient,
        listing_error: BaseException | None,
        entry_error: tuple[int, BaseException] | None,
    ) -> None:
        self._accessed: list[str] = []
        self._client = client
        self._listing_error = listing_error
        self._entry_error = entry_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    def list_directory_contents(self, *args: object, **kwargs: object) -> Iterator[DirectoryEntry]:
        listing = _Listing(args, dict(kwargs), self._client)
        log = self._client._log
        log.events.append(listing)
        if self._listing_error is not None:
            raise self._listing_error
        return _iterate(log, listing, list(self._client._entries), self._entry_error)


def _iterate(
    log: _Log,
    listing: _Listing,
    entries: Sequence[DirectoryEntry],
    entry_error: tuple[int, BaseException] | None,
) -> Iterator[DirectoryEntry]:
    """The entries of a listing, each recorded as it is yielded, and the end of the iteration."""
    for index, entry in enumerate(entries):
        if entry_error is not None and entry_error[0] == index:
            raise entry_error[1]
        log.events.append(_Yielded(entry, listing))
        yield entry
    log.events.append(_End(listing))


class _Factory:
    """Stands in for `databricks.sdk.WorkspaceClient`: records each call and returns a new fake
    client.

    Each client it returns holds the entries of `entries`. When `error` is set, each call raises
    it once recorded.
    """

    def __init__(self, log: _Log, *, error: BaseException | None = None) -> None:
        self.log = log
        self.error = error
        self.entries: list[DirectoryEntry] = []
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.clients: list[_FakeWorkspaceClient] = []

    def __call__(self, *args: object, **kwargs: object) -> _FakeWorkspaceClient:
        self.calls.append((args, dict(kwargs)))
        if self.error is not None:
            raise self.error
        client = _FakeWorkspaceClient(self.log)
        client._entries = list(self.entries)
        self.clients.append(client)
        return client


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

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does. Each load and
    each `createDataFrame` returns a new `_FakeDataFrame`. When `load_error` is set, every load
    raises it once recorded; when `create_error` is set, every `createDataFrame` does.
    """

    def __init__(
        self,
        log: _Log,
        *,
        load_error: BaseException | None = None,
        create_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._load_error = load_error
        self._create_error = create_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(self._log, self._load_error)

    def createDataFrame(  # noqa: N802 - PySpark's name
        self, data: object, schema: object = None
    ) -> _FakeDataFrame:
        frame = _FakeDataFrame(self._log)
        self._log.events.append(_Create(data, schema, frame))
        if self._create_error is not None:
            raise self._create_error
        return frame


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_Load` each time `load()` is called."""

    def __init__(self, log: _Log, load_error: BaseException | None) -> None:
        self._log = log
        self._load_error = load_error
        self._source: str | None = None
        self._schema: object = None
        self._options: dict[str, str] = {}

    def format(self, source: str) -> _FakeDataFrameReader:
        self._source = source
        return self

    def schema(self, schema: object) -> _FakeDataFrameReader:
        self._schema = schema
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
        if format is not None:
            self._source = format
        if schema is not None:
            self._schema = schema
        for key, value in options.items():
            _set_option(self._options, key, value)
        frame = _FakeDataFrame(self._log)
        self._log.events.append(_Load(self._source, self._schema, dict(self._options), path, frame))
        if self._load_error is not None:
            raise self._load_error
        return frame


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    Each access to `write` returns a new `_FakeDataFrameWriter`, kept in `_writers`, whose
    `save()` raises `save_error` once recorded, when it is set.
    """

    def __init__(self, log: _Log, *, save_error: BaseException | None = None) -> None:
        self._log = log
        self._save_error = save_error
        self._accessed: list[str] = []
        self._writers: list[_FakeDataFrameWriter] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def write(self) -> _FakeDataFrameWriter:
        writer = _FakeDataFrameWriter(self._log, self, self._save_error)
        self._writers.append(writer)
        return writer


class _FakeDataFrameWriter:
    """Stands in for `DataFrame.write`, and records every attribute accessed on it.

    `save()` records a `_Save`, then raises `save_error` when it is set.
    """

    def __init__(self, log: _Log, frame: _FakeDataFrame, save_error: BaseException | None) -> None:
        self._log = log
        self._frame = frame
        self._save_error = save_error
        self._accessed: list[str] = []
        self._source: str | None = None
        self._mode: str | None = None
        self._options: dict[str, str] = {}

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

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

    def mode(self, save_mode: str | None) -> _FakeDataFrameWriter:
        self._mode = save_mode
        return self

    def save(
        self,
        path: object = None,
        format: str | None = None,  # noqa: A002 - PySpark's name
        mode: str | None = None,
        partitionBy: object = None,  # noqa: N803 - PySpark's name
        **options: object,
    ) -> None:
        if format is not None:
            self._source = format
        if mode is not None:
            self._mode = mode
        for key, value in options.items():
            _set_option(self._options, key, value)
        self._log.events.append(
            _Save(self._source, self._mode, dict(self._options), path, partitionBy, self._frame)
        )
        if self._save_error is not None:
            raise self._save_error


class _NoOffset(tzinfo):
    """A time zone whose `utcoffset()` is `None`: a datetime that carries it is naive."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        return None


# --- Fixtures and helpers -----------------------------------------------------------------


@pytest.fixture
def log() -> _Log:
    """The event log the doubles of one test share."""
    return _Log()


@pytest.fixture
def spark(log: _Log) -> _FakeSpark:
    """The fake session of the acceptance criteria."""
    return _FakeSpark(log)


@pytest.fixture
def df(log: _Log) -> _FakeDataFrame:
    """The fake DataFrame the writer receives."""
    return _FakeDataFrame(log)


@pytest.fixture
def client(log: _Log) -> _FakeWorkspaceClient:
    """The fake workspace client of the acceptance criteria. Its directory starts empty."""
    return _FakeWorkspaceClient(log)


@pytest.fixture
def factory(log: _Log, monkeypatch: pytest.MonkeyPatch) -> _Factory:
    """The factory of the criteria, installed as `databricks.sdk.WorkspaceClient`."""
    made = _Factory(log)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", made)
    return made


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


def _client(fake: _FakeWorkspaceClient) -> WorkspaceClient:
    """The fake, typed as the client the reader takes."""
    return cast(WorkspaceClient, fake)


def _reader(client: _FakeWorkspaceClient, spark: _FakeSpark, **arguments: Any) -> VolumeReader:
    """The reader of the criteria, `VolumeReader(VOL, "orders", "csv", client=client,
    spark=spark)`.

    `arguments` are the ones a criterion adds.
    """
    return VolumeReader(
        VOL, "orders", "csv", client=_client(client), spark=_session(spark), **arguments
    )


def _writer(**arguments: Any) -> VolumeWriter:
    """The writer of the criteria, `VolumeWriter("main.lake.curated", "orders", "parquet")`."""
    return VolumeWriter(TARGET, "orders", "parquet", **arguments)


def _ms(moment: datetime) -> int:
    """`ms(t)` of the criteria: whole milliseconds from the Unix epoch to `moment`."""
    return (moment - EPOCH) // timedelta(milliseconds=1)


def _v(name: str) -> str:
    """`v(name)` of the criteria: `LOC + "/" + name`."""
    return f"{LOC}/{name}"


def _vs(*names: str) -> list[str]:
    """`v(name)` for each name, in this order."""
    return [_v(name) for name in names]


def _held(name: str, at: datetime = T0) -> DirectoryEntry:
    """`name` of the criteria: the entry of a file of the directory, modified at `at`."""
    return DirectoryEntry(
        name=name, path=_v(name), is_directory=False, file_size=1, last_modified=_ms(at)
    )


def _subdir(name: str) -> DirectoryEntry:
    """The subdirectory `name` of the criteria."""
    return DirectoryEntry(
        name=name, path=_v(name), is_directory=True, file_size=None, last_modified=None
    )


def _hold(client: _FakeWorkspaceClient, *entries: DirectoryEntry) -> None:
    """The directory that `client` lists holds `entries`, in this order."""
    client._entries = list(entries)


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _lower(options: Mapping[str, str]) -> dict[str, str]:
    """`options` with lowercase keys, to compare keys case-insensitively."""
    return {key.lower(): value for key, value in options.items()}


def _listings(log: _Log) -> list[_Listing]:
    """The listings recorded, in order."""
    return [event for event in log.events if isinstance(event, _Listing)]


def _loads(log: _Log) -> list[_Load]:
    """The loads recorded, in order."""
    return [event for event in log.events if isinstance(event, _Load)]


def _creates(log: _Log) -> list[_Create]:
    """The `createDataFrame` calls recorded, in order."""
    return [event for event in log.events if isinstance(event, _Create)]


def _saves(log: _Log) -> list[_Save]:
    """The saves recorded, in order."""
    return [event for event in log.events if isinstance(event, _Save)]


def _attributes(
    fake: _FakeDataFrame | _FakeDataFrameWriter | _FakeWorkspaceClient | _FakeFilesApi,
) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _files_api(client: _FakeWorkspaceClient) -> _FakeFilesApi:
    """The fake Files API of `client`, reached without recording an access."""
    return client._files


def _writers(frame: _FakeDataFrame) -> list[_FakeDataFrameWriter]:
    """The fake writers that the accesses to `frame.write` returned, in order."""
    return frame._writers


def _assert_selected(log: _Log, names: Sequence[str]) -> None:
    """The read loaded exactly `v(name)` for each of `names`, in this order, as a `list`.

    With no names, the read made no load and called `createDataFrame` once.
    """
    if not names:
        assert _loads(log) == [], [load.path for load in _loads(log)]
        assert len(_creates(log)) == 1
        return
    (load,) = _loads(log)
    assert isinstance(load.path, list), load.path
    assert load.path == _vs(*names)
    assert _creates(log) == []


def _assert_builds_or_raises(build: Callable[[], object], message: str | None) -> None:
    """`build()` succeeds when `message` is `None`, and raises `ValueError(message)` otherwise."""
    if message is None:
        build()
        return
    with pytest.raises(ValueError, match=_exactly(message)):
        build()


def _without_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every way of importing `databricks` and `databricks.sdk`, and their submodules,
    raise `ImportError`."""
    modules = cast("dict[str, object]", sys.modules)
    for name in [name for name in sys.modules if name.split(".")[0] == "databricks"]:
        monkeypatch.setitem(modules, name, None)
    for name in ("databricks", "databricks.sdk"):
        monkeypatch.setitem(modules, name, None)
    for name in ("databricks", "databricks.sdk", "databricks.sdk.service.files"):
        with pytest.raises(ImportError):
            importlib.import_module(name)
    with pytest.raises(ImportError):
        __import__("databricks", fromlist=["sdk"])


def _not_found() -> NotFound:
    """The error the SDK raises when the directory of a listing does not exist."""
    return NotFound("The directory being accessed is not found.")


def _permission_denied() -> PermissionDenied:
    """The error the SDK raises when Unity Catalog denies the listing."""
    return PermissionDenied("User does not have READ VOLUME on Volume 'main.landing.raw'.")


def _project(requirement: str) -> str:
    """The normalised name of the project a `Requires-Dist` value names."""
    name = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    assert name, requirement
    return re.sub(r"[-_.]+", "-", name[1]).lower()


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


WriteCall: TypeAlias = Callable[[VolumeWriter, DataFrame], object]

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
from ingestio.connector.all import VolumeReader, VolumeWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.volume as volume

names = {"VolumeReader", "VolumeWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(volume.__all__), volume.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

for name in ("pyspark", "databricks", "databricks.sdk"):
    sys.modules[name] = None
for name in (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.types",
    "databricks",
    "databricks.sdk",
    "databricks.sdk.errors",
    "databricks.sdk.service.files",
):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")
try:
    from databricks import sdk
except ImportError:
    pass
else:
    raise AssertionError("sdk can be imported from databricks")

import ingestio.connector.all
from ingestio.connector.all import VolumeReader, VolumeWriter

loads = []
saves = []


@dataclass
class _Entry:
    name: str | None = None
    path: str | None = None
    is_directory: bool | None = None
    file_size: int | None = None
    last_modified: int | None = None


class _FakeFilesApi:
    def list_directory_contents(self, *args, **kwargs):
        moment = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
        entry = _Entry(
            name="a.csv",
            path="/Volumes/main/landing/raw/orders/a.csv",
            is_directory=False,
            file_size=1,
            last_modified=int(moment.timestamp()) * 1000,
        )
        return iter([entry])


class _FakeWorkspaceClient:
    files = _FakeFilesApi()


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def schema(self, schema):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def load(self, path=None, format=None, schema=None, **options):
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

    def mode(self, save_mode):
        return self

    def save(self, path=None, format=None, mode=None, partitionBy=None, **options):
        saves.append(path)


class _FakeDataFrame:
    @property
    def write(self):
        return _FakeDataFrameWriter()


reader = VolumeReader(
    "main.landing.raw", "orders", "csv", client=_FakeWorkspaceClient(), spark=_FakeSpark()
)
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
VolumeWriter("main.lake.curated", "orders", "parquet").write(_FakeDataFrame())
assert len(saves) == 1, saves
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connectors_work_without_pyspark_or_the_databricks_sdk(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("with_spark", [False, True], ids=["bare", "with-spark"])
def test_at003_building_a_reader_without_client_or_the_sdk_names_the_extra(
    with_spark: bool, spark: _FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_sdk(monkeypatch)

    message = "VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'"
    with pytest.raises(ImportError, match=_exactly(message)) as excinfo:
        if with_spark:
            VolumeReader(VOL, "orders", "csv", spark=_session(spark))
        else:
            VolumeReader(VOL, "orders", "csv")
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_at003_a_reader_with_a_client_and_the_writer_need_no_sdk(
    client: _FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_sdk(monkeypatch)

    VolumeReader(VOL, "orders", "csv", client=_client(client))
    VolumeWriter(TARGET, "orders", "parquet")


def test_at004_the_distribution_declares_the_databricks_extra_with_the_sdk() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "databricks" in (metadata.get_all("Provides-Extra") or [])
    requirements: list[str] = metadata.get_all("Requires-Dist") or []
    assert requirements
    for requirement in requirements:
        assert ANY_EXTRA_MARKER.search(requirement.partition(";")[2]), requirement
    assert any(
        _project(requirement) == "databricks-sdk"
        and DATABRICKS_EXTRA_MARKER.search(requirement.partition(";")[2])
        for requirement in requirements
    ), requirements


# --- Building the connectors --------------------------------------------------------------

Build: TypeAlias = Callable[[str, str, str], object]


def _build_reader(volume: str, path: str, fmt: str) -> object:
    """`VolumeReader(volume, path, fmt, client=client)`, with a fake client of its own."""
    return VolumeReader(volume, path, fmt, client=_client(_FakeWorkspaceClient(_Log())))


def _build_writer(volume: str, path: str, fmt: str) -> object:
    """`VolumeWriter(volume, path, fmt)`."""
    return VolumeWriter(volume, path, fmt)


BUILDERS = pytest.mark.parametrize(
    "build", [_build_reader, _build_writer], ids=["reader", "writer"]
)

VOLUME_NEEDED = "volume must be 'catalog.schema.volume'"


@BUILDERS
@pytest.mark.parametrize(
    "volume",
    [
        pytest.param("", id="empty"),
        pytest.param("main", id="one-part"),
        pytest.param("main.landing", id="two-parts"),
        pytest.param("main.landing.raw.extra", id="four-parts"),
        pytest.param(".landing.raw", id="empty-catalog"),
        pytest.param("main..raw", id="empty-schema"),
        pytest.param("main.landing.", id="empty-volume"),
        pytest.param("main/x.landing.raw", id="slash-in-catalog"),
        pytest.param("main.landing.r/aw", id="slash-in-volume"),
        pytest.param("/Volumes/main/landing/raw", id="whole-path"),
    ],
)
def test_at005_a_volume_that_is_not_three_plain_parts_is_rejected(
    build: Build, volume: str
) -> None:
    with pytest.raises(ValueError, match=_exactly(VOLUME_NEEDED)):
        build(volume, "orders", "csv")


@BUILDERS
@pytest.mark.parametrize(
    "volume", ["main.landing.raw", "Main.Landing.Raw", "my-catalog.landing_zone.raw_01"]
)
def test_at005_a_three_part_volume_name_is_accepted(build: Build, volume: str) -> None:
    build(volume, "orders", "csv")


@BUILDERS
@pytest.mark.parametrize(
    "volume",
    [
        pytest.param(None, id="none"),
        pytest.param(b"main.landing.raw", id="bytes"),
        pytest.param(("main", "landing", "raw"), id="tuple"),
    ],
)
def test_a_volume_that_is_not_a_str_is_rejected(build: Build, volume: object) -> None:
    with pytest.raises(ValueError, match=_exactly(VOLUME_NEEDED)):
        build(cast(Any, volume), "orders", "csv")


RELATIVE_NEEDED = "path must be a str relative to the volume"
SEGMENTS = "path must not contain empty, '.' or '..' segments"
DIRECTORY_NEEDED = "path must name a directory inside the volume"


@pytest.mark.parametrize(
    ("path", "reader_message", "writer_message"),
    [
        pytest.param("/orders", RELATIVE_NEEDED, RELATIVE_NEEDED, id="absolute"),
        pytest.param("/", RELATIVE_NEEDED, RELATIVE_NEEDED, id="slash"),
        pytest.param(
            "/Volumes/main/landing/raw/orders", RELATIVE_NEEDED, RELATIVE_NEEDED, id="whole-path"
        ),
        pytest.param(Path("orders"), RELATIVE_NEEDED, RELATIVE_NEEDED, id="path-object"),
        pytest.param("landing//orders", SEGMENTS, SEGMENTS, id="empty-segment"),
        pytest.param("./orders", SEGMENTS, SEGMENTS, id="leading-dot"),
        pytest.param("landing/../orders", SEGMENTS, SEGMENTS, id="dot-dot"),
        pytest.param("orders/..", SEGMENTS, SEGMENTS, id="trailing-dot-dot"),
        pytest.param(".", SEGMENTS, SEGMENTS, id="dot"),
        pytest.param("..", SEGMENTS, SEGMENTS, id="only-dot-dot"),
        pytest.param("", None, DIRECTORY_NEEDED, id="volume-root"),
        pytest.param("orders", None, None, id="one-segment"),
        pytest.param("orders/", None, None, id="trailing-slash"),
        pytest.param("landing/orders//", None, None, id="trailing-slashes"),
        pytest.param("a b/ç", None, None, id="space-unicode"),
        pytest.param("_staging/orders", None, None, id="underscore-segment"),
    ],
)
def test_at006_the_path_inside_the_volume_is_checked(
    path: object,
    reader_message: str | None,
    writer_message: str | None,
    client: _FakeWorkspaceClient,
) -> None:
    given = cast(Any, path)
    _assert_builds_or_raises(
        lambda: VolumeReader(VOL, given, "csv", client=_client(client)), reader_message
    )
    _assert_builds_or_raises(lambda: VolumeWriter(TARGET, given, "csv"), writer_message)


def test_at007_repr_shows_the_volume_the_normalised_path_and_the_format(
    client: _FakeWorkspaceClient,
) -> None:
    expected = "VolumeReader(volume='main.landing.raw', path='orders', format='csv')"
    assert repr(VolumeReader(VOL, "orders/", "csv", client=_client(client))) == expected
    assert (
        repr(
            VolumeReader(
                VOL,
                "orders",
                "csv",
                client=_client(client),
                watermark_pattern=P,
                watermark_value="20261021",
            )
        )
        == expected
    )
    assert (
        repr(VolumeReader(VOL, "", "json", client=_client(client)))
        == "VolumeReader(volume='main.landing.raw', path='', format='json')"
    )
    assert (
        repr(VolumeWriter(TARGET, "orders//", "parquet"))
        == "VolumeWriter(volume='main.lake.curated', path='orders', format='parquet')"
    )


@BUILDERS
def test_at008_an_empty_format_is_rejected(build: Build) -> None:
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        build(VOL, "orders", "")


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_reader_rejects_delta_in_any_case(fmt: str, client: _FakeWorkspaceClient) -> None:
    message = f"format '{fmt}' cannot be read file by file"
    with pytest.raises(ValueError, match=_exactly(message)):
        VolumeReader(VOL, "orders", fmt, client=_client(client))


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_writer_accepts_delta(fmt: str) -> None:
    VolumeWriter(TARGET, "orders", fmt)


@BUILDERS
@pytest.mark.parametrize("fmt", ["parquet", "CSV", "com.example.source"])
def test_at008_any_other_format_name_is_accepted(build: Build, fmt: str) -> None:
    build(VOL, "orders", fmt)


def test_at009_an_invalid_watermark_pattern_is_rejected_from_the_regex_error(
    client: _FakeWorkspaceClient,
) -> None:
    with pytest.raises(ValueError, match=r"^invalid watermark_pattern: ") as excinfo:
        VolumeReader(VOL, "orders", "csv", client=_client(client), watermark_pattern="(")
    cause = excinfo.value.__cause__
    assert isinstance(cause, re.error), cause
    assert cause.msg in str(excinfo.value)


@pytest.mark.parametrize(
    "pattern",
    [r"orders_\d{8}\.csv", r"(\d{4})(\d{4})\.csv", ""],
    ids=["no-group", "two-groups", "empty"],
)
def test_at009_a_watermark_pattern_needs_exactly_one_group(
    pattern: str, client: _FakeWorkspaceClient
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_pattern must have exactly one group")):
        VolumeReader(VOL, "orders", "csv", client=_client(client), watermark_pattern=pattern)


@pytest.mark.parametrize(
    "pattern", [P, r"(?:orders|sales)_(?P<day>\d{8})\.csv"], ids=["one-group", "named-group"]
)
def test_at009_a_pattern_with_one_capturing_group_is_accepted(
    pattern: str, client: _FakeWorkspaceClient
) -> None:
    VolumeReader(VOL, "orders", "csv", client=_client(client), watermark_pattern=pattern)


STR_NEEDED = "watermark_value must be a str with watermark_pattern"
DATETIME_NEEDED = "watermark_value must be a datetime without watermark_pattern"
AWARE_NEEDED = "watermark_value must be timezone-aware"


@pytest.mark.parametrize(
    ("pattern", "value", "message"),
    [
        pytest.param(P, 20261021, STR_NEEDED, id="pattern-int"),
        pytest.param(P, T0, STR_NEEDED, id="pattern-datetime"),
        pytest.param(None, "20261021", DATETIME_NEEDED, id="str"),
        pytest.param(None, date(2026, 10, 21), DATETIME_NEEDED, id="date"),
        pytest.param(None, 20261021, DATETIME_NEEDED, id="int"),
        pytest.param(None, True, DATETIME_NEEDED, id="bool"),
        pytest.param(
            None,
            datetime(2026, 10, 21, 9, 0),  # noqa: DTZ001 - the naive datetime under test
            AWARE_NEEDED,
            id="naive",
        ),
        pytest.param(
            None,
            datetime(2026, 10, 21, 9, 0, tzinfo=_NoOffset()),
            AWARE_NEEDED,
            id="tzinfo-without-offset",
        ),
    ],
)
def test_at010_a_watermark_value_of_the_wrong_kind_is_rejected(
    pattern: str | None, value: object, message: str, client: _FakeWorkspaceClient
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        VolumeReader(
            VOL,
            "orders",
            "csv",
            client=_client(client),
            watermark_pattern=pattern,
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize("value", ["", "20261021"], ids=["empty", "date"])
def test_at010_a_str_value_is_accepted_with_a_pattern(
    value: str, client: _FakeWorkspaceClient
) -> None:
    VolumeReader(
        VOL, "orders", "csv", client=_client(client), watermark_pattern=P, watermark_value=value
    )


@pytest.mark.parametrize(
    "value", [T0, datetime(2026, 10, 21, 6, 0, tzinfo=MINUS_3)], ids=["utc", "minus-3"]
)
def test_at010_an_aware_datetime_is_accepted_without_a_pattern(
    value: datetime, client: _FakeWorkspaceClient
) -> None:
    VolumeReader(VOL, "orders", "csv", client=_client(client), watermark_value=value)


@pytest.mark.parametrize(
    "key",
    ["path", "PATHS", "pathGlobFilter", "recursivefilelookup", "modifiedBefore", "ModifiedAfter"],
)
def test_at011_options_the_reader_sets_are_rejected_in_any_case(
    key: str, client: _FakeWorkspaceClient
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        VolumeReader(VOL, "orders", "csv", client=_client(client), options={key: "x"})


@pytest.mark.parametrize("key", ["path", "Paths", "replaceWhere", "PartitionOverwriteMode"])
def test_at011_options_the_writer_sets_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        VolumeWriter(TARGET, "orders", "parquet", options={key: "x"})


def test_at011_other_options_are_accepted(client: _FakeWorkspaceClient) -> None:
    VolumeReader(
        VOL,
        "orders",
        "csv",
        client=_client(client),
        options={"header": True, "inferSchema": "true", "mergeSchema": False},
    )
    VolumeWriter(TARGET, "orders", "parquet", options={"compression": "zstd", "header": True})


def test_at012_building_touches_neither_spark_nor_the_client_nor_the_workspace(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient, factory: _Factory
) -> None:
    VolumeReader(VOL, "orders", "csv", client=_client(client), spark=_session(spark))
    VolumeReader(VOL, "orders", "csv", spark=_session(spark))
    VolumeReader(
        VOL,
        "orders",
        "csv",
        client=_client(client),
        spark=_session(spark),
        watermark_pattern=P,
        watermark_value="20261020",
    )
    VolumeReader(
        VOL, "orders", "csv", client=_client(client), spark=_session(spark), watermark_value=T0
    )
    VolumeWriter(TARGET, "orders", "parquet")

    assert log.spark_attributes == []
    assert _attributes(client) == []
    assert _attributes(_files_api(client)) == []
    assert log.events == []
    assert factory.calls == []


def test_validation_runs_in_the_order_of_the_arguments(client: _FakeWorkspaceClient) -> None:
    def reader(volume: str, path: str, fmt: str, pattern: str, value: object) -> object:
        return VolumeReader(
            volume,
            path,
            fmt,
            watermark_pattern=pattern,
            watermark_value=cast(Any, value),
            options={"path": "x"},
            client=_client(client),
        )

    with pytest.raises(ValueError, match=_exactly(VOLUME_NEEDED)):
        reader("main", "/orders", "", "(", 1)
    with pytest.raises(ValueError, match=_exactly(RELATIVE_NEEDED)):
        reader(VOL, "/orders", "", "(", 1)
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        reader(VOL, "orders", "", "(", 1)
    with pytest.raises(ValueError, match=r"^invalid watermark_pattern: "):
        reader(VOL, "orders", "csv", "(", 1)
    with pytest.raises(ValueError, match=_exactly(STR_NEEDED)):
        reader(VOL, "orders", "csv", P, 1)
    with pytest.raises(ValueError, match=_exactly("option 'path' is set by the connector")):
        reader(VOL, "orders", "csv", P, "20261021")

    def writer(volume: str, path: str, fmt: str) -> object:
        return VolumeWriter(volume, path, fmt, options={"path": "x"})

    with pytest.raises(ValueError, match=_exactly(VOLUME_NEEDED)):
        writer("main", "", "")
    with pytest.raises(ValueError, match=_exactly(DIRECTORY_NEEDED)):
        writer(TARGET, "", "")
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        writer(TARGET, "orders", "")
    with pytest.raises(ValueError, match=_exactly("option 'path' is set by the connector")):
        writer(TARGET, "orders", "parquet")


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        pytest.param({"volume": "main.landing"}, VOLUME_NEEDED, id="volume"),
        pytest.param({"path": "/orders"}, RELATIVE_NEEDED, id="path"),
        pytest.param({"format": "delta"}, "format 'delta' cannot be read file by file", id="delta"),
        pytest.param(
            {"watermark_pattern": ""}, "watermark_pattern must have exactly one group", id="pattern"
        ),
        pytest.param({"watermark_value": "20261021"}, DATETIME_NEEDED, id="value"),
        pytest.param(
            {"options": {"pathGlobFilter": "*.csv"}},
            "option 'pathGlobFilter' is set by the connector",
            id="options",
        ),
    ],
)
def test_validation_comes_before_the_import_check(
    arguments: dict[str, Any], message: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_sdk(monkeypatch)
    given: dict[str, Any] = {"volume": VOL, "path": "orders", "format": "csv", **arguments}

    with pytest.raises(ValueError, match=_exactly(message)):
        VolumeReader(**given)


# --- Reading ------------------------------------------------------------------------------


def test_at013_each_read_makes_one_listing_of_the_directory(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient, factory: _Factory
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    (listing,) = _listings(log)
    assert listing.args == ()
    assert listing.kwargs == {"directory_path": "/Volumes/main/landing/raw/orders"}
    assert isinstance(listing.kwargs["directory_path"], str)
    assert listing.client is client
    assert set(_attributes(client)) == {"files"}, _attributes(client)
    assert set(_attributes(_files_api(client))) == {"list_directory_contents"}, _attributes(
        _files_api(client)
    )
    assert factory.calls == []


def test_at013_at_the_volume_root_the_location_is_the_volume(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("a.csv"))

    VolumeReader(VOL, "", "csv", client=_client(client), spark=_session(spark)).read()

    (listing,) = _listings(log)
    assert listing.args == ()
    assert listing.kwargs == {"directory_path": "/Volumes/main/landing/raw"}
    (load,) = _loads(log)
    assert load.path == ["/Volumes/main/landing/raw/a.csv"]


def test_at013_a_nested_path_is_listed_without_its_trailing_slash(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    nested = "/Volumes/main/landing/raw/landing/orders"
    _hold(client, dataclasses.replace(_held("a.csv"), path=f"{nested}/a.csv"))

    VolumeReader(
        VOL, "landing/orders/", "csv", client=_client(client), spark=_session(spark)
    ).read()

    (listing,) = _listings(log)
    assert listing.args == ()
    assert listing.kwargs == {"directory_path": "/Volumes/main/landing/raw/landing/orders"}
    (load,) = _loads(log)
    assert load.path == ["/Volumes/main/landing/raw/landing/orders/a.csv"]


def test_the_volume_and_the_path_reach_the_location_as_written(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient, df: _FakeDataFrame
) -> None:
    _hold(client, _held("A.csv"))

    VolumeReader(
        "Main.Landing.Raw", "Orders/In", "csv", client=_client(client), spark=_session(spark)
    ).read()
    VolumeWriter("Main.Lake.Curated", "Orders/Out", "parquet").write(_frame(df))

    (listing,) = _listings(log)
    assert listing.kwargs == {"directory_path": "/Volumes/Main/Landing/Raw/Orders/In"}
    (load,) = _loads(log)
    assert load.path == ["/Volumes/Main/Landing/Raw/Orders/In/A.csv"]
    (save,) = _saves(log)
    assert save.path == "/Volumes/Main/Lake/Curated/Orders/Out"


def test_at014_only_visible_files_of_the_directory_are_loaded(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(
        client,
        _held("b.csv"),
        _held("a.csv"),
        _held("_SUCCESS"),
        _held("_metadata"),
        _held(".a.csv.crc"),
        _held("c.csv._COPYING_"),
        _held("_started_123"),
        _held("_committed_123"),
        _subdir("sub"),
        _subdir("x.csv"),
        _subdir("_delta_log"),
    )

    frame = _reader(client, spark).read()

    (load,) = _loads(log)
    assert load.source == "csv"
    assert load.schema is None
    assert load.options == {}
    assert isinstance(load.path, list), load.path
    assert load.path == _vs("a.csv", "b.csv")
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    assert _creates(log) == []


def test_at014_a_single_file_is_still_passed_as_a_list(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv"])


def test_at015_names_are_sorted_by_code_point_and_kept_as_they_are(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("pedidos_ç.csv"), _held("a b.csv"), _held("2026-10-21T09:00:00.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["2026-10-21T09:00:00.csv", "a b.csv", "pedidos_ç.csv"])


def test_at015_an_unset_is_directory_means_a_file_and_size_plays_no_part(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(
        client,
        dataclasses.replace(_held("a.csv"), is_directory=None),
        dataclasses.replace(_held("b.csv"), file_size=0),
    )

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv", "b.csv"])


def test_at015_the_paths_are_built_from_the_location_and_the_name(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, dataclasses.replace(_held("a.csv"), path="dbfs:/Volumes/MAIN/other/place/z.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv"])


def test_at016_every_entry_of_the_listing_is_taken_before_the_load(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("b.csv"), _held("a.csv"), _held("c.csv"))

    _reader(client, spark).read()

    kinds = [type(event) for event in log.events]
    assert kinds == [_Listing, _Yielded, _Yielded, _Yielded, _End, _Load], kinds
    yielded = [event.entry.name for event in log.events if isinstance(event, _Yielded)]
    assert yielded == ["b.csv", "a.csv", "c.csv"]
    _assert_selected(log, ["a.csv", "b.csv", "c.csv"])


def test_at017_the_schema_object_and_the_options_reach_the_load_as_given(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("a.csv"))
    schema = "id INT, name STRING"

    _reader(client, spark, schema=schema, options={"header": True, "sep": ";"}).read()

    (load,) = _loads(log)
    assert load.schema is schema
    assert _lower(load.options) == {"header": "true", "sep": ";"}


def test_at017_a_struct_type_schema_reaches_the_load_as_the_same_object(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("a.csv"))
    schema = StructType([StructField("id", IntegerType())])

    _reader(client, spark, schema=schema).read()

    (load,) = _loads(log)
    assert load.schema is schema


def test_at018_without_spark_the_active_session_is_looked_up_at_each_read(
    log: _Log, client: _FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = VolumeReader(VOL, "orders", "csv", client=_client(client))
    assert lookups == []

    _hold(client, _held("a.csv"))
    first, second = _FakeSpark(log), _FakeSpark(log)
    monkeypatch.setattr(SparkSession, "active", lambda: _session(first))
    first_frame = reader.read()
    monkeypatch.setattr(SparkSession, "active", lambda: _session(second))
    second_frame = reader.read()

    first_load, second_load = _loads(log)
    assert first_frame is _frame(first_load.frame)
    assert second_frame is _frame(second_load.frame)
    assert first_load.path == second_load.path == _vs("a.csv")


def test_at018_without_an_active_session_the_error_propagates_and_nothing_is_listed(
    log: _Log, client: _FakeWorkspaceClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _hold(client, _held("a.csv"))
    reader = VolumeReader(VOL, "orders", "csv", client=_client(client))
    error = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise error

    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert _listings(log) == []


def test_at019_without_client_each_read_creates_the_default_client(
    log: _Log, spark: _FakeSpark, factory: _Factory
) -> None:
    factory.entries = [_held("a.csv")]

    reader = VolumeReader(VOL, "orders", "csv", spark=_session(spark))
    assert factory.calls == []
    reader.read()
    assert factory.calls == [((), {})]
    reader.read()

    assert factory.calls == [((), {}), ((), {})]
    first, second = _listings(log)
    assert first.client is factory.clients[0]
    assert second.client is factory.clients[1]
    assert first.kwargs == second.kwargs == {"directory_path": LOC}
    assert [load.path for load in _loads(log)] == [_vs("a.csv"), _vs("a.csv")]


def test_at019_the_client_class_is_looked_up_at_each_read(
    log: _Log, spark: _FakeSpark, factory: _Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = VolumeReader(VOL, "orders", "csv", spark=_session(spark))
    replacement = _Factory(log)
    replacement.entries = [_held("a.csv")]
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", replacement)

    reader.read()

    assert factory.calls == []
    assert replacement.calls == [((), {})]
    (listing,) = _listings(log)
    assert listing.client is replacement.clients[0]


def test_at019_an_error_of_the_default_client_propagates_unchanged(
    log: _Log, spark: _FakeSpark, factory: _Factory
) -> None:
    error = ValueError(NO_CONFIGURATION)
    factory.error = error
    factory.entries = [_held("a.csv")]
    reader = VolumeReader(VOL, "orders", "csv", spark=_session(spark))

    with pytest.raises(ValueError) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert log.events == []


def test_at020_each_read_lists_the_directory_again(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    reader = _reader(client, spark)

    _hold(client, _held("a.csv"))
    reader.read()
    _hold(client, _held("a.csv"), _held("b.csv"))
    reader.read()

    assert len(_listings(log)) == 2
    first, second = _loads(log)
    assert first.path == _vs("a.csv")
    assert second.path == _vs("a.csv", "b.csv")


EMPTY_LAYOUTS = pytest.mark.parametrize("layout", ["no-entry", "no-candidate"])


def _hold_layout(client: _FakeWorkspaceClient, layout: str) -> None:
    """The directory of AT-021: a listing that yields no entry, or one that holds no
    candidate."""
    if layout == "no-entry":
        _hold(client)
    else:
        _hold(client, _held("_SUCCESS"), _subdir("sub"))


@EMPTY_LAYOUTS
def test_at021_an_empty_selection_creates_an_empty_dataframe_without_columns(
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold_layout(client, layout)

    frame = _reader(client, spark).read()

    assert _loads(log) == []
    (create,) = _creates(log)
    assert isinstance(create.data, list)
    assert create.data == []
    assert isinstance(create.schema, StructType), create.schema
    assert create.schema.fields == []
    assert frame is _frame(create.frame)


@EMPTY_LAYOUTS
def test_at021_an_empty_selection_with_a_schema_creates_an_empty_dataframe_with_it(
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold_layout(client, layout)
    schema = "id INT, name STRING"

    frame = _reader(client, spark, schema=schema).read()

    assert _loads(log) == []
    (create,) = _creates(log)
    assert isinstance(create.data, list)
    assert create.data == []
    assert create.schema is schema
    assert frame is _frame(create.frame)


LISTING_ERRORS = pytest.mark.parametrize(
    "make_error", [_not_found, _permission_denied], ids=["not-found", "permission-denied"]
)


@LISTING_ERRORS
@pytest.mark.parametrize("where", ["call", "first-entry", "after-a-csv"])
def test_at022_a_failed_listing_propagates_unchanged_and_nothing_is_loaded(
    make_error: Callable[[], Exception], where: str, log: _Log, spark: _FakeSpark
) -> None:
    error = make_error()
    if where == "call":
        client = _FakeWorkspaceClient(log, listing_error=error)
    elif where == "first-entry":
        client = _FakeWorkspaceClient(log, entry_error=(0, error))
    else:
        client = _FakeWorkspaceClient(log, entry_error=(1, error))
    _hold(client, _held("a.csv"), _held("b.csv"))

    with pytest.raises(type(error)) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_listings(log)) == 1
    assert _loads(log) == []
    assert _creates(log) == []


def test_at022_a_failed_load_propagates_unchanged(log: _Log, client: _FakeWorkspaceClient) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, load_error=error)
    _hold(client, _held("a.csv"))

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_loads(log)) == 1


def test_at022_a_failed_create_dataframe_propagates_unchanged(
    log: _Log, client: _FakeWorkspaceClient
) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, create_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_creates(log)) == 1


# --- Watermark from the file name ---------------------------------------------------------


def _hold_orders_and_others(client: _FakeWorkspaceClient) -> None:
    """The directory of AT-023: dated orders, newest name oldest time, and names that do not
    match."""
    _hold(
        client,
        _held("orders_20261019.csv", T0 + timedelta(seconds=2)),
        _held("orders_20261020.csv", T0 + timedelta(seconds=1)),
        _held("orders_20261021.csv", T0),
        _held("customers_20261021.csv"),
        _held("orders_latest.csv"),
        _held("orders_20261021.csv.bak"),
    )


def test_at023_files_above_the_value_by_name_are_selected_whatever_their_time(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold_orders_and_others(client)

    _reader(client, spark, watermark_pattern=P, watermark_value="20261019").read()

    _assert_selected(log, ["orders_20261020.csv", "orders_20261021.csv"])


def test_at023_without_a_value_every_matching_file_is_selected(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold_orders_and_others(client)

    _reader(client, spark, watermark_pattern=P).read()

    _assert_selected(log, ORDER_FILES)


PART = r"part_(\d+)\.csv"
PART_FILES = ("part_9.csv", "part_10.csv")


@pytest.mark.parametrize(
    ("pattern", "names", "value", "selected"),
    [
        pytest.param(P, ORDER_FILES, "20261021", (), id="the-highest-value"),
        pytest.param(
            P,
            ORDER_FILES,
            "2026102",
            ("orders_20261020.csv", "orders_20261021.csv"),
            id="a-shorter-value",
        ),
        pytest.param(P, ORDER_FILES, "", ORDER_FILES, id="the-empty-value"),
        pytest.param(PART, PART_FILES, "9", (), id="nine-is-above-ten"),
        pytest.param(PART, PART_FILES, "1", ("part_10.csv", "part_9.csv"), id="name-order"),
    ],
)
def test_at024_name_watermarks_compare_as_text(
    pattern: str,
    names: tuple[str, ...],
    value: str,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeWorkspaceClient,
) -> None:
    _hold(client, *(_held(name) for name in names))

    _reader(client, spark, watermark_pattern=pattern, watermark_value=value).read()

    _assert_selected(log, selected)


@pytest.mark.parametrize(
    ("pattern", "entries", "selected"),
    [
        pytest.param(
            r"(\d{8})\.csv", (_held("orders_20261021.csv"),), (), id="the-whole-name-must-match"
        ),
        pytest.param(
            r"/Volumes/main/landing/raw/orders/orders_(\d{8})\.csv",
            (_held("orders_20261021.csv"),),
            (),
            id="the-name-not-the-path",
        ),
        pytest.param(
            r"orders(_\d{8})?\.csv",
            (_held("orders.csv"), _held("orders_20261021.csv")),
            ("orders_20261021.csv",),
            id="a-group-that-took-no-part",
        ),
        pytest.param(
            r".*_(\d{8})\.csv",
            (
                _held("_orders_20261022.csv"),
                _held(".orders_20261022.csv"),
                _subdir("orders_20261022.csv"),
                _held("orders_20261021.csv"),
            ),
            ("orders_20261021.csv",),
            id="hidden-names-and-subdirectories-stay-out",
        ),
    ],
)
def test_at025_the_pattern_must_match_the_whole_name_of_a_candidate(
    pattern: str,
    entries: tuple[DirectoryEntry, ...],
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeWorkspaceClient,
) -> None:
    _hold(client, *entries)

    _reader(client, spark, watermark_pattern=pattern).read()

    _assert_selected(log, selected)


# --- Watermark from last_modified ---------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(None, ("a.csv", "b.csv", "c.csv"), id="none"),
        pytest.param(T0 - timedelta(microseconds=1), ("a.csv", "b.csv", "c.csv"), id="t0-1us"),
        pytest.param(T0, ("a.csv", "b.csv"), id="t0"),
        pytest.param(T0 + timedelta(milliseconds=500), ("a.csv", "b.csv"), id="t0+500ms"),
        pytest.param(T0 + timedelta(seconds=1), ("a.csv",), id="t0+1s"),
        pytest.param(T0 + timedelta(seconds=2), (), id="t0+2s"),
    ],
)
def test_at026_files_modified_after_the_value_are_selected_in_name_order(
    value: datetime | None,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeWorkspaceClient,
) -> None:
    _hold(
        client,
        _held("c.csv", T0),
        _held("b.csv", T0 + timedelta(seconds=1)),
        _held("a.csv", T0 + timedelta(seconds=2)),
    )

    _reader(client, spark, watermark_value=value).read()

    _assert_selected(log, selected)


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(T0, False, id="t0"),
        pytest.param(T0 + timedelta(milliseconds=500), False, id="t0+500ms"),
        pytest.param(T0 - timedelta(microseconds=1), True, id="t0-1us"),
        pytest.param(datetime(2026, 10, 21, 6, 0, tzinfo=MINUS_3), False, id="minus-3-t0"),
        pytest.param(
            datetime(2026, 10, 21, 5, 59, 59, 999999, tzinfo=MINUS_3), True, id="minus-3-t0-1us"
        ),
    ],
)
def test_at027_last_modified_is_truncated_to_the_second_and_compared_as_an_instant(
    value: datetime, selected: bool, log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, dataclasses.replace(_held("a.csv"), last_modified=_ms(T0) + 999))

    _reader(client, spark, watermark_value=value).read()

    _assert_selected(log, ["a.csv"] if selected else [])


# --- Writing ------------------------------------------------------------------------------


def _assert_one_chain(frame: _FakeDataFrame) -> None:
    """`write` was the only attribute read from `frame`, and its writer saved once."""
    assert _attributes(frame) == ["write"]
    (writer,) = _writers(frame)
    accessed = _attributes(writer)
    assert set(accessed) <= {"format", "option", "options", "mode", "save"}, accessed
    assert accessed.count("save") == 1, accessed


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at028_write_appends_with_one_call_chain(
    call: WriteCall, log: _Log, df: _FakeDataFrame, factory: _Factory
) -> None:
    result = call(_writer(), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "append"
    assert save.options == {}
    assert save.path == TARGET_LOC
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at029_write_with_replace_overwrites(
    log: _Log, df: _FakeDataFrame, factory: _Factory
) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(_writer(), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "overwrite"
    assert save.options == {}
    assert save.path == TARGET_LOC
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at030_options_reach_the_save_as_given_with_the_normalised_path(
    log: _Log, df: _FakeDataFrame
) -> None:
    writer = VolumeWriter(
        TARGET, "landing/orders/", "csv", options={"header": True, "Compression": "gzip"}
    )

    writer.write(_frame(df))

    (save,) = _saves(log)
    assert save.source == "csv"
    assert save.path == "/Volumes/main/lake/curated/landing/orders"
    assert _lower(save.options) == {"header": "true", "compression": "gzip"}
    assert "Compression" in save.options, save.options


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at031_a_failed_save_propagates_unchanged(call: WriteCall, log: _Log) -> None:
    error = RuntimeError("task failed")
    frame = _FakeDataFrame(log, save_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        call(_writer(), _frame(frame))
    assert excinfo.value is error
    assert len(_saves(log)) == 1


# --- Composition --------------------------------------------------------------------------


def test_at032_read_source_and_write_target_move_a_dataframe_and_append(
    log: _Log, spark: _FakeSpark, client: _FakeWorkspaceClient
) -> None:
    _hold(client, _held("a.csv"))
    source: Reader[DataFrame] = VolumeReader(
        VOL, "orders", "csv", client=_client(client), spark=_session(spark)
    )
    target: Writer[DataFrame] = VolumeWriter(TARGET, "orders", "parquet")

    process = Process(
        [
            Step("extract", [ReadSource(source, into="orders")]),
            Step("load", [WriteTarget(target, key="orders")]),
        ]
    ).run()

    (load,) = _loads(log)
    assert process.state["orders"] is _frame(load.frame)
    (save,) = _saves(log)
    assert save.frame is load.frame
    assert save.mode == "append"


def test_at033_the_connectors_write_no_log_lines(
    log: _Log,
    spark: _FakeSpark,
    client: _FakeWorkspaceClient,
    df: _FakeDataFrame,
    factory: _Factory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    _hold(client, _held("a.csv", T0))
    factory.entries = [_held("a.csv", T0)]

    _reader(client, spark).read()
    _reader(client, spark, watermark_value=T0).read()
    VolumeReader(VOL, "orders", "csv", spark=_session(spark)).read()
    VolumeReader(VOL, "orders", "csv", spark=_session(spark), watermark_value=T0).read()
    writer = _writer()
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    assert len(_loads(log)) == 2
    assert len(_creates(log)) == 2
    assert len(_saves(log)) == 2
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

VOLUME = os.environ.get("INGESTIO_DATABRICKS_VOLUME", "")

needs_volume = pytest.mark.skipif(
    not VOLUME,
    reason="INGESTIO_DATABRICKS_VOLUME is not set: no Databricks scratch volume to run against",
)

SCHEMA = "id INT, name STRING"
CSV = {"header": True}


@dataclass(frozen=True)
class _Scratch:
    """A directory of its own for one test, at the root of the volume `INGESTIO_DATABRICKS_VOLUME`,
    and a client of the workspace.

    `path` is the directory relative to the volume, as the connectors take it, and `location` is
    its whole `/Volumes/...` path, as the Files API takes it.
    """

    path: str
    location: str
    client: WorkspaceClient


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    """The active session when pytest runs on a cluster, and otherwise a Databricks Connect
    session.

    Databricks Connect is imported by name: it is installed only where it replaces pyspark.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    connect: Any = importlib.import_module("databricks.connect")
    return cast(SparkSession, connect.DatabricksSession.builder.getOrCreate())


def _volume_root() -> str:
    """The whole path of the volume `INGESTIO_DATABRICKS_VOLUME`, `/Volumes/<c>/<s>/<v>`."""
    return "/Volumes/" + VOLUME.replace(".", "/")


def _child(directory: str, name: str) -> str:
    """The whole path of `name` inside the whole path `directory`."""
    return f"{directory}/{name}"


def _names(scratch: _Scratch, under: str = "") -> list[str]:
    """The names of the entries of the scratch directory, or of its subdirectory `under`."""
    directory = _child(scratch.location, under) if under else scratch.location
    return [str(entry.name) for entry in scratch.client.files.list_directory_contents(directory)]


def _put(scratch: _Scratch, name: str, text: str) -> None:
    """Upload `text` as the file `name` of the scratch directory, which may hold a `/`."""
    scratch.client.files.upload(
        _child(scratch.location, name), io.BytesIO(text.encode()), overwrite=True
    )


def _last_modified(scratch: _Scratch, name: str) -> int:
    """The `last_modified` of the file `name` of the scratch directory, from a listing, in
    milliseconds."""
    for entry in scratch.client.files.list_directory_contents(scratch.location):
        if entry.name == name:
            assert entry.last_modified is not None, entry
            return entry.last_modified
    raise AssertionError(f"{name} is not listed in {scratch.location}")


def _delete_tree(client: WorkspaceClient, directory: str) -> None:
    """Delete `directory`, every file and every subdirectory it holds."""
    for entry in list(client.files.list_directory_contents(directory)):
        child = _child(directory, str(entry.name))
        if entry.is_directory:
            _delete_tree(client, child)
        else:
            client.files.delete(child)
    client.files.delete_directory(directory)


@pytest.fixture
def scratch() -> Iterator[_Scratch]:
    """A directory of its own at the root of the scratch volume, created empty and deleted at
    the end of the test."""
    path = f"ingestio-{uuid.uuid4().hex}"
    made = _Scratch(path, _child(_volume_root(), path), WorkspaceClient())
    made.client.files.create_directory(made.location)
    try:
        yield made
    finally:
        _delete_tree(made.client, made.location)


def _rows(frame: DataFrame) -> list[tuple[Any, ...]]:
    """The rows of `frame`, as sorted tuples."""
    return sorted(tuple(row) for row in frame.collect())


def _csv_reader(scratch: _Scratch, spark: SparkSession, **arguments: Any) -> VolumeReader:
    """A reader of the CSV files of the scratch directory, with a header, `SCHEMA` and the
    scratch client."""
    return VolumeReader(
        VOLUME,
        scratch.path,
        "csv",
        schema=SCHEMA,
        options=CSV,
        client=scratch.client,
        spark=spark,
        **arguments,
    )


@pytest.mark.integration
@needs_volume
def test_parquet_appends_then_replaces_and_a_full_read_returns_the_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    writer = VolumeWriter(VOLUME, scratch.path, "parquet")

    writer.write(spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA))
    writer.write(spark_session.createDataFrame([(3, "c")], SCHEMA))

    names = _names(scratch)
    assert any(name.startswith("_") for name in names), names
    reader = VolumeReader(
        VOLUME, scratch.path, "parquet", client=scratch.client, spark=spark_session
    )
    assert _rows(reader.read()) == [(1, "a"), (2, "b"), (3, "c")]

    writer.write(spark_session.createDataFrame([(10, "x")], SCHEMA), replace=True)

    assert _rows(reader.read()) == [(10, "x")]


@pytest.mark.integration
@needs_volume
def test_csv_written_with_a_header_reads_back_with_the_same_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    frame = spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA)

    VolumeWriter(VOLUME, scratch.path, "csv", options=CSV).write(frame)
    back = _csv_reader(scratch, spark_session).read()

    assert back.columns == ["id", "name"]
    assert _rows(back) == [(1, "a"), (2, "b")]


@pytest.mark.integration
@needs_volume
def test_a_name_watermark_reads_a_frozen_selection_of_the_files_above_it(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "orders_20261019.csv", "id,name\n1,a\n")
    _put(scratch, "orders_20261020.csv", "id,name\n2,b\n")
    _put(scratch, "orders_20261021.csv", "id,name\n3,c\n4,d\n")

    frame = _csv_reader(
        scratch, spark_session, watermark_pattern=P, watermark_value="20261019"
    ).read()

    assert _rows(frame) == [(2, "b"), (3, "c"), (4, "d")]
    assert frame.count() == 3
    _put(scratch, "orders_20261022.csv", "id,name\n5,e\n")
    assert frame.count() == 3


@pytest.mark.integration
@needs_volume
def test_a_last_modified_watermark_matches_the_metadata_column_to_the_second(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")
    a_time = _last_modified(scratch, "a.csv")
    # Upload b.csv until the volume stamps it in a later second than a.csv.
    for _ in range(10):
        time.sleep(1.1)
        _put(scratch, "b.csv", "id,name\n2,b\n")
        b_time = _last_modified(scratch, "b.csv")
        if b_time // 1000 > a_time // 1000:
            break
    assert b_time // 1000 > a_time // 1000, (a_time, b_time)

    frame = _csv_reader(
        scratch, spark_session, watermark_value=EPOCH + timedelta(milliseconds=a_time)
    ).read()

    assert _rows(frame) == [(2, "b")]
    column = functions.unix_seconds("_metadata.file_modification_time")
    row = frame.select(functions.max(column)).first()
    assert row is not None
    seconds = cast(int, row[0])
    assert seconds == b_time // 1000
    again = _csv_reader(
        scratch, spark_session, watermark_value=datetime.fromtimestamp(seconds, UTC)
    )
    assert again.read().count() == 0


@pytest.mark.integration
@needs_volume
def test_an_empty_selection_has_the_columns_of_the_schema_or_none(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    assert _names(scratch) == []

    with_schema = VolumeReader(
        VOLUME, scratch.path, "csv", schema=SCHEMA, client=scratch.client, spark=spark_session
    ).read()
    without_schema = VolumeReader(
        VOLUME, scratch.path, "csv", client=scratch.client, spark=spark_session
    ).read()

    assert with_schema.columns == ["id", "name"]
    assert with_schema.count() == 0
    assert without_schema.columns == []
    assert without_schema.count() == 0


@pytest.mark.integration
@needs_volume
def test_a_directory_that_does_not_exist_fails_the_read_with_not_found(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    reader = VolumeReader(
        VOLUME, f"{scratch.path}/missing", "csv", client=scratch.client, spark=spark_session
    )

    with pytest.raises(NotFound):
        reader.read()


@pytest.mark.integration
@needs_volume
def test_a_subdirectory_and_hidden_files_are_not_read(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    scratch.client.files.create_directory(_child(scratch.location, "sub"))
    _put(scratch, "sub/b.csv", "id,name\n2,b\n")
    _put(scratch, "_hidden.csv", "id,name\n3,c\n")
    _put(scratch, ".hidden.csv", "id,name\n4,d\n")
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = _csv_reader(scratch, spark_session).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_volume
def test_a_reader_without_client_lists_through_the_default_client(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = VolumeReader(
        VOLUME, scratch.path, "csv", schema=SCHEMA, options=CSV, spark=spark_session
    ).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_volume
def test_a_write_whose_computation_fails_raises_out_of_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    def explode(value: int) -> int:
        raise ValueError("udf boom")

    failing = functions.udf(explode, "int")
    frame = spark_session.createDataFrame([(1,)], "id INT").select(failing("id").alias("id"))

    with pytest.raises(Exception, match="udf boom"):
        VolumeWriter(VOLUME, scratch.path, "parquet").write(frame)


@pytest.mark.integration
@needs_volume
def test_replace_removes_files_spark_did_not_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "keep.txt", "keep me\n")

    VolumeWriter(VOLUME, scratch.path, "parquet").write(
        spark_session.createDataFrame([(1, "a")], SCHEMA), replace=True
    )

    assert "keep.txt" not in _names(scratch), _names(scratch)
    reader = VolumeReader(
        VOLUME, scratch.path, "parquet", client=scratch.client, spark=spark_session
    )
    assert _rows(reader.read()) == [(1, "a")]
