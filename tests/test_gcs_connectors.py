"""GCS connectors: `GcsReader` and `GcsWriter`.

Covers `specs/gcs_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no credentials: they use the doubles below, a fake GCS client
whose `list_blobs` returns an iterator over pages shaped like the SDK's, a fake
`google.cloud.storage.Client`, a fake Spark session and fake DataFrames, which all write to one
ordered event log.

The integration tests at the end run the connectors against GCS and a local Spark session,
`local[2]`, in UTC. They are marked `integration` and skipped unless `INGESTIO_GCS_URL` holds a
scratch location, `gs://<bucket>/<prefix>`. Each test works under a unique sub-prefix of it and
deletes that sub-prefix at the end. Credentials come from Application Default Credentials, for
google-cloud-storage and for the Cloud Storage connector for Hadoop alike, which the session
configures with `fs.gs.auth.type=APPLICATION_DEFAULT`. The session puts the connector's shaded
JAR on its classpath through `spark.jars`: by default the release `GCS_CONNECTOR_VERSION` from
Maven Central, built for Hadoop 3.4, the line closest to the Hadoop that pyspark bundles;
`INGESTIO_GCS_CONNECTOR_JAR` replaces it with another path or URL. They need Java 17 or 21 for
the locked pyspark.
"""

from __future__ import annotations

import importlib
import importlib.metadata
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

import pytest
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.storage import Client
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import IntegerType, StructField, StructType

from ingestio.connector.all import GcsReader, GcsWriter

T0 = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUS_3 = timezone(timedelta(hours=-3))
URI = "gs://lake/landing/orders"
BLOB_PREFIX = "landing/orders/"
P = r"orders_(\d{8})\.csv"
ORDER_FILES = ("orders_20261019.csv", "orders_20261020.csv", "orders_20261021.csv")
ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")
GCS_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']gcs["']""")
STORAGE = "google.cloud.storage"


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Obj:
    """A blob of the store: its whole name, its two times, its size and its storage class."""

    name: str
    time_created: datetime = T0
    updated: datetime = T0
    size: int = 2
    storage_class: str = "STANDARD"


@dataclass(frozen=True)
class _Prefix:
    """A sub-prefix that the delimiter gathers into the iterator's `prefixes`, whole."""

    prefix: str


_Item: TypeAlias = _Obj | _Prefix


@dataclass(frozen=True)
class _Page:
    """A page of a listing: its blobs, in order, and its prefixes. Either may be empty."""

    blobs: tuple[_Obj, ...] = ()
    prefixes: frozenset[str] = frozenset()


def _page(*items: _Item) -> _Page:
    """A page holding the blobs and the prefixes among `items`, the blobs in this order."""
    return _Page(
        tuple(item for item in items if isinstance(item, _Obj)),
        frozenset(item.prefix for item in items if isinstance(item, _Prefix)),
    )


@dataclass(frozen=True)
class _Listing:
    """`list_blobs()` ran on the fake `client`, with these arguments, and returned `iterator`."""

    args: tuple[object, ...]
    kwargs: dict[str, object]
    client: _FakeGcsClient = field(repr=False, compare=False)
    iterator: _FakeIterator = field(repr=False, compare=False)


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


_Event: TypeAlias = _Listing | _Load | _Create | _Save


class _Log:
    """The ordered event log every double writes to."""

    def __init__(self) -> None:
        self.events: list[_Event] = []
        self.spark_attributes: list[str] = []


class _FakeBlob:
    """Stands in for a `google.cloud.storage.Blob`, and records every attribute accessed on it.

    Its times are timezone-aware datetimes, as the SDK returns them.
    """

    def __init__(self, obj: _Obj) -> None:
        self._accessed: list[str] = []
        self.name = obj.name
        self.time_created = obj.time_created
        self.updated = obj.updated
        self.size = obj.size
        self.storage_class = obj.storage_class

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)


class _FakeIterator:
    """Stands in for the iterator `list_blobs` returns, and records every attribute accessed
    on it.

    Iterating over it yields the blobs of each page, in order, and can start only once, as the
    SDK's iterator does. A page counts as taken when the iteration reaches it, and `prefixes`
    then gains its prefixes. When `page_error` is `(index, error)`, the iteration raises `error`
    when it reaches the page at `index`.
    """

    def __init__(
        self, pages: Sequence[_Page], page_error: tuple[int, BaseException] | None
    ) -> None:
        self._accessed: list[str] = []
        self._pages = [([_FakeBlob(obj) for obj in page.blobs], page.prefixes) for page in pages]
        self._page_error = page_error
        self._pages_taken = 0
        self._prefixes: set[str] = set()
        self._started = False

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    @property
    def prefixes(self) -> set[str]:
        return self._prefixes

    def __iter__(self) -> Iterator[_FakeBlob]:
        if self._started:
            raise ValueError("Iterator has already started", self)
        self._started = True
        return self._items()

    def _items(self) -> Iterator[_FakeBlob]:
        for index, (blobs, prefixes) in enumerate(self._pages):
            if self._page_error is not None and self._page_error[0] == index:
                raise self._page_error[1]
            self._pages_taken += 1
            self._prefixes.update(prefixes)
            yield from blobs


class _FakeGcsClient:
    """Stands in for a `google.cloud.storage.Client`, and records every attribute accessed on it.

    `list_blobs()` records a `_Listing` and returns a new `_FakeIterator` over the pages of the
    store, or raises `listing_error` once recorded, when it is set. The store is `_pages`, which
    `_hold` and `_hold_pages` replace; it starts as one page with no blob and no prefix. The
    iterators it returns raise `page_error` as `_FakeIterator` describes.
    """

    def __init__(
        self,
        log: _Log,
        *,
        listing_error: BaseException | None = None,
        page_error: tuple[int, BaseException] | None = None,
    ) -> None:
        self._accessed: list[str] = []
        self._log = log
        self._listing_error = listing_error
        self._page_error = page_error
        self._pages: list[_Page] = [_Page()]

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    def list_blobs(self, *args: object, **kwargs: object) -> _FakeIterator:
        iterator = _FakeIterator(self._pages, self._page_error)
        self._log.events.append(_Listing(args, dict(kwargs), self, iterator))
        if self._listing_error is not None:
            raise self._listing_error
        return iterator


class _Factory:
    """Stands in for `google.cloud.storage.Client`: records each call and returns a new fake
    client.

    Each client it returns holds the pages of `pages`. When `error` is set, each call raises it
    once recorded.
    """

    def __init__(self, log: _Log, *, error: BaseException | None = None) -> None:
        self.log = log
        self.error = error
        self.pages: list[_Page] = [_Page()]
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.clients: list[_FakeGcsClient] = []

    def __call__(self, *args: object, **kwargs: object) -> _FakeGcsClient:
        self.calls.append((args, dict(kwargs)))
        if self.error is not None:
            raise self.error
        client = _FakeGcsClient(self.log)
        client._pages = list(self.pages)
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
def client(log: _Log) -> _FakeGcsClient:
    """The fake GCS client of the acceptance criteria. Its store starts empty."""
    return _FakeGcsClient(log)


@pytest.fixture
def factory(log: _Log, monkeypatch: pytest.MonkeyPatch) -> _Factory:
    """The factory of the criteria, installed as `google.cloud.storage.Client`."""
    made = _Factory(log)
    monkeypatch.setattr(f"{STORAGE}.Client", made)
    return made


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


def _client(fake: _FakeGcsClient) -> Client:
    """The fake, typed as the client the reader takes."""
    return cast(Client, fake)


def _reader(client: _FakeGcsClient, spark: _FakeSpark, **arguments: Any) -> GcsReader:
    """The reader of the criteria, `GcsReader(URI, "csv", client=client, spark=spark)`.

    `arguments` are the ones a criterion adds.
    """
    return GcsReader(URI, "csv", client=_client(client), spark=_session(spark), **arguments)


def _held(name: str, at: datetime = T0) -> _Obj:
    """`name` of the criteria: a blob named `landing/orders/<name>`, created and updated at
    `at`."""
    return _Obj(BLOB_PREFIX + name, at, at)


def _sub(prefix: str) -> _Prefix:
    """The prefix `prefix` of the criteria, under `landing/orders/`."""
    return _Prefix(BLOB_PREFIX + prefix)


def _hold(client: _FakeGcsClient, *items: _Item) -> None:
    """The store of `client` holds `items`, in this order, on one page."""
    client._pages = [_page(*items)]


def _hold_pages(client: _FakeGcsClient, *pages: Sequence[_Item]) -> None:
    """The store of `client` holds `pages`, each a page of items, in this order."""
    client._pages = [_page(*items) for items in pages]


def _u(*names: str) -> list[str]:
    """`u(name)` of the criteria for each name: `URI + "/" + name`."""
    return [f"{URI}/{name}" for name in names]


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
    fake: _FakeDataFrame | _FakeDataFrameWriter | _FakeGcsClient | _FakeIterator | _FakeBlob,
) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _writers(frame: _FakeDataFrame) -> list[_FakeDataFrameWriter]:
    """The fake writers that the accesses to `frame.write` returned, in order."""
    return frame._writers


def _blobs(listing: _Listing) -> list[_FakeBlob]:
    """The fake blobs of every page of the iterator that `listing` returned, in order."""
    return [blob for blobs, _ in listing.iterator._pages for blob in blobs]


def _pages_taken(listing: _Listing) -> int:
    """How many pages the iteration of the iterator that `listing` returned has reached."""
    return listing.iterator._pages_taken


def _assert_only_name_and_time_created(listing: _Listing) -> None:
    """No attribute other than `name` and `time_created` was accessed on a blob of `listing`."""
    for blob in _blobs(listing):
        accessed = _attributes(blob)
        assert set(accessed) <= {"name", "time_created"}, (blob.name, accessed)


def _assert_selected(log: _Log, names: Sequence[str]) -> None:
    """The read loaded exactly `u(name)` for each of `names`, in this order, as a `list`.

    With no names, the read made no load and called `createDataFrame` once.
    """
    if not names:
        assert _loads(log) == [], [load.path for load in _loads(log)]
        assert len(_creates(log)) == 1
        return
    (load,) = _loads(log)
    assert isinstance(load.path, list), load.path
    assert load.path == _u(*names)
    assert _creates(log) == []


def _assert_builds_or_raises(build: Callable[[], object], message: str | None) -> None:
    """`build()` succeeds when `message` is `None`, and raises `ValueError(message)` otherwise."""
    if message is None:
        build()
        return
    with pytest.raises(ValueError, match=_exactly(message)):
        build()


def _without_gcs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every way of importing `google.cloud.storage`, and its submodules, raise
    `ImportError`.

    Besides the entries of `sys.modules`, the attribute `storage` of the `google.cloud`
    package goes, so that `from google.cloud import storage` cannot find the loaded module.
    """
    modules = cast("dict[str, object]", sys.modules)
    for name in [name for name in sys.modules if name.startswith(f"{STORAGE}.")]:
        monkeypatch.setitem(modules, name, None)
    monkeypatch.setitem(modules, STORAGE, None)
    parent = sys.modules.get("google.cloud")
    if parent is not None:
        monkeypatch.delattr(parent, "storage", raising=False)
    with pytest.raises(ImportError):
        importlib.import_module(STORAGE)
    with pytest.raises(ImportError):
        importlib.import_module(f"{STORAGE}.client")
    with pytest.raises(ImportError):
        __import__("google.cloud", fromlist=["storage"])


def _no_such_bucket() -> NotFound:
    """The error the SDK raises when the bucket of a listing does not exist.

    google-api-core annotates no exception constructor, which mypy strict reports.
    """
    return NotFound("The specified bucket does not exist.")  # type: ignore[no-untyped-call]


def _no_credentials() -> DefaultCredentialsError:
    """The error the SDK raises when Application Default Credentials find no credentials.

    google-auth annotates no exception constructor, which mypy strict reports.
    """
    return DefaultCredentialsError(  # type: ignore[no-untyped-call]
        "Your default credentials were not found."
    )


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


WriteCall: TypeAlias = Callable[[GcsWriter, DataFrame], object]

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
from ingestio.connector.all import GcsReader, GcsWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.gcs as gcs

names = {"GcsReader", "GcsWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(gcs.__all__), gcs.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys
from datetime import UTC, datetime

for name in ("pyspark", "google.cloud.storage"):
    sys.modules[name] = None
for name in (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.types",
    "google.cloud.storage",
    "google.cloud.storage.client",
):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")
try:
    from google.cloud import storage
except ImportError:
    pass
else:
    raise AssertionError("storage can be imported from google.cloud")

import ingestio.connector.all
from ingestio.connector.all import GcsReader, GcsWriter

loads = []
saves = []


class _FakeBlob:
    name = "landing/orders/a.csv"
    time_created = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
    updated = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)


class _FakeGcsClient:
    def list_blobs(self, *args, **kwargs):
        return iter([_FakeBlob()])


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


reader = GcsReader(
    "gs://lake/landing/orders", "csv", client=_FakeGcsClient(), spark=_FakeSpark()
)
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
GcsWriter("gs://lake/orders", "parquet").write(_FakeDataFrame())
assert len(saves) == 1, saves
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connectors_work_without_pyspark_or_google_cloud_storage(
    tmp_path: Path,
) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("with_spark", [False, True], ids=["bare", "with-spark"])
def test_at003_building_a_reader_without_client_or_the_sdk_names_the_extra(
    with_spark: bool, spark: _FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_gcs(monkeypatch)

    message = "GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'"
    with pytest.raises(ImportError, match=_exactly(message)) as excinfo:
        if with_spark:
            GcsReader(URI, "csv", spark=_session(spark))
        else:
            GcsReader(URI, "csv")
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_at003_a_reader_with_a_client_and_the_writer_need_no_sdk(
    client: _FakeGcsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_gcs(monkeypatch)

    GcsReader(URI, "csv", client=_client(client))
    GcsWriter("gs://lake/orders", "parquet")


def test_at004_the_distribution_declares_the_gcs_extra_with_google_cloud_storage() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "gcs" in (metadata.get_all("Provides-Extra") or [])
    requirements: list[str] = metadata.get_all("Requires-Dist") or []
    assert requirements
    for requirement in requirements:
        assert ANY_EXTRA_MARKER.search(requirement.partition(";")[2]), requirement
    assert any(
        _project(requirement) == "google-cloud-storage"
        and GCS_EXTRA_MARKER.search(requirement.partition(";")[2])
        for requirement in requirements
    ), requirements


# --- Building the connectors --------------------------------------------------------------

Build: TypeAlias = Callable[[str, str], object]


def _build_reader(path: str, fmt: str) -> object:
    """`GcsReader(path, fmt, client=client)`, with a fake client of its own."""
    return GcsReader(path, fmt, client=_client(_FakeGcsClient(_Log())))


def _build_writer(path: str, fmt: str) -> object:
    """`GcsWriter(path, fmt)`."""
    return GcsWriter(path, fmt)


BUILDERS = pytest.mark.parametrize(
    "build", [_build_reader, _build_writer], ids=["reader", "writer"]
)


@BUILDERS
@pytest.mark.parametrize(
    "path",
    [
        pytest.param("", id="empty"),
        pytest.param("lake/landing/orders", id="relative"),
        pytest.param("/Volumes/main/landing/orders", id="local"),
        pytest.param("s3://lake/landing/orders", id="s3"),
        pytest.param("gcs://lake/landing/orders", id="gcs"),
        pytest.param("GS://lake/landing/orders", id="uppercase"),
        pytest.param("gs:/lake/landing/orders", id="one-slash"),
        pytest.param("https://storage.googleapis.com/lake/landing/orders", id="https"),
        pytest.param(Path("gs://lake/landing/orders"), id="path-object"),
    ],
)
def test_at005_a_path_other_than_a_gs_uri_is_rejected(build: Build, path: object) -> None:
    with pytest.raises(ValueError, match=_exactly("path must be a gs:// URI")):
        build(cast(Any, path), "csv")


BUCKET_NEEDED = "path must name a bucket"
SEGMENTS = "path must not contain empty, '.' or '..' segments"
PREFIX_NEEDED = "path must name a prefix inside the bucket"


@pytest.mark.parametrize(
    ("path", "reader_message", "writer_message"),
    [
        pytest.param("gs://", BUCKET_NEEDED, BUCKET_NEEDED, id="nothing"),
        pytest.param("gs:///landing/orders", BUCKET_NEEDED, BUCKET_NEEDED, id="empty-bucket"),
        pytest.param("gs://lake//orders", SEGMENTS, SEGMENTS, id="empty-first-segment"),
        pytest.param("gs://lake/landing//orders", SEGMENTS, SEGMENTS, id="empty-segment"),
        pytest.param("gs://lake/./orders", SEGMENTS, SEGMENTS, id="dot"),
        pytest.param("gs://lake/landing/../orders", SEGMENTS, SEGMENTS, id="dot-dot"),
        pytest.param("gs://lake/landing/orders/..", SEGMENTS, SEGMENTS, id="trailing-dot-dot"),
        pytest.param("gs://lake", None, PREFIX_NEEDED, id="bucket-root"),
        pytest.param("gs://lake/", None, PREFIX_NEEDED, id="bucket-root-slash"),
        pytest.param("gs://lake/landing/orders", None, None, id="prefix"),
        pytest.param("gs://lake/landing/orders/", None, None, id="trailing-slash"),
        pytest.param("gs://my.bucket_01-x/a b/ç", None, None, id="dots-space-unicode"),
        pytest.param("gs://lake/_staging/orders", None, None, id="underscore-segment"),
    ],
)
def test_at006_the_bucket_and_the_prefix_are_checked(
    path: str, reader_message: str | None, writer_message: str | None, client: _FakeGcsClient
) -> None:
    _assert_builds_or_raises(lambda: GcsReader(path, "csv", client=_client(client)), reader_message)
    _assert_builds_or_raises(lambda: GcsWriter(path, "csv"), writer_message)


def test_at007_repr_shows_the_normalised_path_and_the_format(client: _FakeGcsClient) -> None:
    expected = "GcsReader(path='gs://lake/landing/orders', format='csv')"
    assert repr(GcsReader("gs://lake/landing/orders/", "csv", client=_client(client))) == expected
    assert (
        repr(
            GcsReader(
                "gs://lake/landing/orders",
                "csv",
                client=_client(client),
                watermark_pattern=P,
                watermark_value="20261021",
            )
        )
        == expected
    )
    assert (
        repr(GcsReader("gs://lake/", "json", client=_client(client)))
        == "GcsReader(path='gs://lake', format='json')"
    )
    assert (
        repr(GcsWriter("gs://lake/orders//", "parquet"))
        == "GcsWriter(path='gs://lake/orders', format='parquet')"
    )


@BUILDERS
def test_at008_an_empty_format_is_rejected(build: Build) -> None:
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        build("gs://lake/orders", "")


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_reader_rejects_delta_in_any_case(fmt: str, client: _FakeGcsClient) -> None:
    message = f"format '{fmt}' cannot be read file by file"
    with pytest.raises(ValueError, match=_exactly(message)):
        GcsReader(URI, fmt, client=_client(client))


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_writer_accepts_delta(fmt: str) -> None:
    GcsWriter("gs://lake/orders", fmt)


@BUILDERS
@pytest.mark.parametrize("fmt", ["parquet", "CSV", "com.example.source"])
def test_at008_any_other_format_name_is_accepted(build: Build, fmt: str) -> None:
    build("gs://lake/orders", fmt)


def test_at009_an_invalid_watermark_pattern_is_rejected_from_the_regex_error(
    client: _FakeGcsClient,
) -> None:
    with pytest.raises(ValueError, match=r"^invalid watermark_pattern: ") as excinfo:
        GcsReader(URI, "csv", client=_client(client), watermark_pattern="(")
    cause = excinfo.value.__cause__
    assert isinstance(cause, re.error), cause
    assert cause.msg in str(excinfo.value)


@pytest.mark.parametrize(
    "pattern",
    [r"orders_\d{8}\.csv", r"(\d{4})(\d{4})\.csv", ""],
    ids=["no-group", "two-groups", "empty"],
)
def test_at009_a_watermark_pattern_needs_exactly_one_group(
    pattern: str, client: _FakeGcsClient
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_pattern must have exactly one group")):
        GcsReader(URI, "csv", client=_client(client), watermark_pattern=pattern)


@pytest.mark.parametrize(
    "pattern", [P, r"(?:orders|sales)_(?P<day>\d{8})\.csv"], ids=["one-group", "named-group"]
)
def test_at009_a_pattern_with_one_capturing_group_is_accepted(
    pattern: str, client: _FakeGcsClient
) -> None:
    GcsReader(URI, "csv", client=_client(client), watermark_pattern=pattern)


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
    pattern: str | None, value: object, message: str, client: _FakeGcsClient
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        GcsReader(
            URI,
            "csv",
            client=_client(client),
            watermark_pattern=pattern,
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize("value", ["", "20261021"], ids=["empty", "date"])
def test_at010_a_str_value_is_accepted_with_a_pattern(value: str, client: _FakeGcsClient) -> None:
    GcsReader(URI, "csv", client=_client(client), watermark_pattern=P, watermark_value=value)


@pytest.mark.parametrize(
    "value", [T0, datetime(2026, 10, 21, 6, 0, tzinfo=MINUS_3)], ids=["utc", "minus-3"]
)
def test_at010_an_aware_datetime_is_accepted_without_a_pattern(
    value: datetime, client: _FakeGcsClient
) -> None:
    GcsReader(URI, "csv", client=_client(client), watermark_value=value)


@pytest.mark.parametrize(
    "key",
    ["path", "PATHS", "pathGlobFilter", "recursivefilelookup", "modifiedBefore", "ModifiedAfter"],
)
def test_at011_options_the_reader_sets_are_rejected_in_any_case(
    key: str, client: _FakeGcsClient
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        GcsReader(URI, "csv", client=_client(client), options={key: "x"})


@pytest.mark.parametrize("key", ["path", "Paths", "replaceWhere", "PartitionOverwriteMode"])
def test_at011_options_the_writer_sets_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        GcsWriter("gs://lake/orders", "parquet", options={key: "x"})


def test_at011_other_options_are_accepted(client: _FakeGcsClient) -> None:
    GcsReader(
        URI,
        "csv",
        client=_client(client),
        options={"header": True, "inferSchema": "true", "mergeSchema": False},
    )
    GcsWriter("gs://lake/orders", "parquet", options={"compression": "zstd", "header": True})


def test_at012_building_touches_neither_spark_nor_the_client_nor_gcs(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient, factory: _Factory
) -> None:
    GcsReader(URI, "csv", client=_client(client), spark=_session(spark))
    GcsReader(URI, "csv", spark=_session(spark))
    GcsReader(
        URI,
        "csv",
        client=_client(client),
        spark=_session(spark),
        watermark_pattern=P,
        watermark_value="20261020",
    )
    GcsReader(URI, "csv", client=_client(client), spark=_session(spark), watermark_value=T0)
    GcsWriter("gs://lake/orders", "parquet")

    assert log.spark_attributes == []
    assert _attributes(client) == []
    assert log.events == []
    assert factory.calls == []


def test_validation_comes_before_the_import_check(monkeypatch: pytest.MonkeyPatch) -> None:
    _without_gcs(monkeypatch)

    with pytest.raises(ValueError, match=_exactly("path must be a gs:// URI")):
        GcsReader("s3://lake/landing/orders", "csv")


# --- Reading ------------------------------------------------------------------------------


def test_at013_each_read_makes_one_flat_listing_of_the_prefix(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient, factory: _Factory
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    (listing,) = _listings(log)
    assert listing.args == ("lake",)
    assert isinstance(listing.args[0], str)
    assert listing.kwargs == {"prefix": "landing/orders/", "delimiter": "/"}
    assert listing.client is client
    assert set(_attributes(client)) == {"list_blobs"}, _attributes(client)
    assert _attributes(listing.iterator) == []
    _assert_only_name_and_time_created(listing)
    assert factory.calls == []


def test_at013_at_the_bucket_root_the_listing_prefix_is_empty(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _Obj("a.csv"))

    GcsReader("gs://lake/", "csv", client=_client(client), spark=_session(spark)).read()

    (listing,) = _listings(log)
    assert listing.args == ("lake",)
    assert listing.kwargs == {"prefix": "", "delimiter": "/"}
    (load,) = _loads(log)
    assert load.path == ["gs://lake/a.csv"]


def test_at014_only_visible_blobs_directly_under_the_prefix_are_loaded(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(
        client,
        _held("b.csv"),
        _held("a.csv"),
        _held("_SUCCESS"),
        _held("_metadata"),
        _held(".a.csv.crc"),
        _held("c.csv._COPYING_"),
        _held("_committed_123"),
        _Obj("landing/orders/"),
        _Obj("landing/orders/sub/"),
        _Obj("landing/orders/sub/d.csv"),
        _sub("sub/"),
        _sub("x.csv/"),
    )

    frame = _reader(client, spark).read()

    (load,) = _loads(log)
    assert load.source == "csv"
    assert load.schema is None
    assert load.options == {}
    assert isinstance(load.path, list), load.path
    assert load.path == _u("a.csv", "b.csv")
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    assert _creates(log) == []


def test_at014_a_single_blob_is_still_passed_as_a_list(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv"])


def test_at015_names_are_sorted_by_code_point_and_kept_as_they_are(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("pedidos_ç.csv"), _held("a b.csv"), _held("2026-10-21T09:00:00.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["2026-10-21T09:00:00.csv", "a b.csv", "pedidos_ç.csv"])


def test_at015_trailing_slashes_of_the_path_are_removed(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("a.csv"))

    GcsReader(
        "gs://lake/landing/orders///", "csv", client=_client(client), spark=_session(spark)
    ).read()

    (listing,) = _listings(log)
    assert listing.kwargs["prefix"] == "landing/orders/"
    _assert_selected(log, ["a.csv"])


def test_at016_every_page_of_the_listing_is_taken_before_the_load(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold_pages(
        client,
        [_held("b.csv"), _held("a.csv"), _sub("sub/")],
        [_sub("x/")],
        [_held("c.csv")],
    )
    assert client._pages[1].blobs == ()

    _reader(client, spark).read()

    (listing,) = _listings(log)
    assert _pages_taken(listing) == 3
    _assert_selected(log, ["a.csv", "b.csv", "c.csv"])


def test_blobs_of_every_storage_class_and_size_are_candidates(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(
        client,
        _Obj(BLOB_PREFIX + "a.csv", storage_class="NEARLINE"),
        _Obj(BLOB_PREFIX + "b.csv", storage_class="COLDLINE"),
        _Obj(BLOB_PREFIX + "c.csv", storage_class="ARCHIVE", size=0),
    )

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv", "b.csv", "c.csv"])


def test_at017_the_schema_object_and_the_options_reach_the_load_as_given(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("a.csv"))
    schema = "id INT, name STRING"

    _reader(client, spark, schema=schema, options={"header": True, "sep": ";"}).read()

    (load,) = _loads(log)
    assert load.schema is schema
    assert _lower(load.options) == {"header": "true", "sep": ";"}


def test_at017_a_struct_type_schema_reaches_the_load_as_the_same_object(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("a.csv"))
    schema = StructType([StructField("id", IntegerType())])

    _reader(client, spark, schema=schema).read()

    (load,) = _loads(log)
    assert load.schema is schema


def test_at018_without_spark_the_active_session_is_looked_up_at_each_read(
    log: _Log, client: _FakeGcsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = GcsReader(URI, "csv", client=_client(client))
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
    assert first_load.path == second_load.path == _u("a.csv")


def test_at018_without_an_active_session_the_error_propagates_and_nothing_is_listed(
    log: _Log, client: _FakeGcsClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _hold(client, _held("a.csv"))
    reader = GcsReader(URI, "csv", client=_client(client))
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
    factory.pages = [_page(_held("a.csv"))]

    reader = GcsReader(URI, "csv", spark=_session(spark))
    assert factory.calls == []
    reader.read()
    assert factory.calls == [((), {"project": None})]
    reader.read()

    assert factory.calls == [((), {"project": None}), ((), {"project": None})]
    first, second = _listings(log)
    assert first.client is factory.clients[0]
    assert second.client is factory.clients[1]
    assert [load.path for load in _loads(log)] == [_u("a.csv"), _u("a.csv")]


def test_at019_the_client_class_is_looked_up_at_each_read(
    log: _Log, spark: _FakeSpark, factory: _Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = GcsReader(URI, "csv", spark=_session(spark))
    replacement = _Factory(log)
    replacement.pages = [_page(_held("a.csv"))]
    monkeypatch.setattr(f"{STORAGE}.Client", replacement)

    reader.read()

    assert factory.calls == []
    assert replacement.calls == [((), {"project": None})]
    (listing,) = _listings(log)
    assert listing.client is replacement.clients[0]


def test_at019_an_error_of_the_default_client_propagates_unchanged(
    log: _Log, spark: _FakeSpark, factory: _Factory
) -> None:
    error = _no_credentials()
    factory.error = error
    reader = GcsReader(URI, "csv", spark=_session(spark))

    with pytest.raises(DefaultCredentialsError) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert log.events == []


def test_at020_each_read_lists_the_prefix_again(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    reader = _reader(client, spark)

    _hold(client, _held("a.csv"))
    reader.read()
    _hold(client, _held("a.csv"), _held("b.csv"))
    reader.read()

    assert len(_listings(log)) == 2
    first, second = _loads(log)
    assert first.path == _u("a.csv")
    assert second.path == _u("a.csv", "b.csv")


EMPTY_LAYOUTS = pytest.mark.parametrize("layout", ["no-blob", "no-candidate"])


def _hold_layout(client: _FakeGcsClient, layout: str) -> None:
    """The store of AT-021: a single page with no blob and no prefix, or one that holds no
    candidate."""
    if layout == "no-blob":
        client._pages = [_Page()]
        assert client._pages[0].blobs == ()
        assert client._pages[0].prefixes == frozenset()
    else:
        _hold(client, _held("_SUCCESS"), _Obj("landing/orders/"), _sub("sub/"))


@EMPTY_LAYOUTS
def test_at021_an_empty_selection_creates_an_empty_dataframe_without_columns(
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeGcsClient
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
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeGcsClient
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


@pytest.mark.parametrize("where", ["first-page", "second-page", "list-blobs"])
def test_at022_a_failed_listing_propagates_unchanged_and_nothing_is_loaded(
    where: str, log: _Log, spark: _FakeSpark
) -> None:
    error = _no_such_bucket()
    if where == "list-blobs":
        client = _FakeGcsClient(log, listing_error=error)
        _hold(client, _held("a.csv"))
    elif where == "first-page":
        client = _FakeGcsClient(log, page_error=(0, error))
        _hold(client, _held("a.csv"))
    else:
        client = _FakeGcsClient(log, page_error=(1, error))
        _hold_pages(client, [_held("a.csv")], [_held("b.csv")])

    with pytest.raises(NotFound) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert _loads(log) == []
    assert _creates(log) == []


def test_at022_a_failed_load_propagates_unchanged(log: _Log, client: _FakeGcsClient) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, load_error=error)
    _hold(client, _held("a.csv"))

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_loads(log)) == 1


def test_at022_a_failed_create_dataframe_propagates_unchanged(
    log: _Log, client: _FakeGcsClient
) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, create_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_creates(log)) == 1


# --- Watermark from the blob name ---------------------------------------------------------


def _hold_orders_and_others(client: _FakeGcsClient) -> None:
    """The store of AT-023: dated orders, newest name oldest time, and names that do not
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


def test_at023_blobs_above_the_value_by_name_are_selected_whatever_their_time(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold_orders_and_others(client)

    _reader(client, spark, watermark_pattern=P, watermark_value="20261019").read()

    _assert_selected(log, ["orders_20261020.csv", "orders_20261021.csv"])


def test_at023_without_a_value_every_matching_blob_is_selected(
    log: _Log, spark: _FakeSpark, client: _FakeGcsClient
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
    client: _FakeGcsClient,
) -> None:
    _hold(client, *(_held(name) for name in names))

    _reader(client, spark, watermark_pattern=pattern, watermark_value=value).read()

    _assert_selected(log, selected)


@pytest.mark.parametrize(
    ("pattern", "items", "selected"),
    [
        pytest.param(
            r"(\d{8})\.csv", (_held("orders_20261021.csv"),), (), id="the-whole-name-must-match"
        ),
        pytest.param(
            r"landing/orders/orders_(\d{8})\.csv",
            (_held("orders_20261021.csv"),),
            (),
            id="the-name-not-the-full-name",
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
                _Obj("landing/orders/sub/orders_20261022.csv"),
                _held("orders_20261021.csv"),
            ),
            ("orders_20261021.csv",),
            id="hidden-and-nested-names-stay-out",
        ),
    ],
)
def test_at025_the_pattern_must_match_the_whole_name_of_a_candidate(
    pattern: str,
    items: tuple[_Item, ...],
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeGcsClient,
) -> None:
    _hold(client, *items)

    _reader(client, spark, watermark_pattern=pattern).read()

    _assert_selected(log, selected)


# --- Watermark from timeCreated -----------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(None, ("a.csv", "b.csv", "c.csv"), id="none"),
        pytest.param(T0 - timedelta(milliseconds=1), ("a.csv", "b.csv", "c.csv"), id="t0-1ms"),
        pytest.param(T0, ("a.csv", "b.csv"), id="t0"),
        pytest.param(T0 + timedelta(microseconds=499_999), ("a.csv", "b.csv"), id="t0+499999us"),
        pytest.param(T0 + timedelta(milliseconds=500), ("a.csv",), id="t0+500ms"),
        pytest.param(T0 + timedelta(seconds=1), (), id="t0+1s"),
    ],
)
def test_at026_blobs_created_after_the_value_are_selected_in_name_order(
    value: datetime | None,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeGcsClient,
) -> None:
    _hold(
        client,
        _held("c.csv", T0),
        _held("b.csv", T0 + timedelta(milliseconds=500)),
        _held("a.csv", T0 + timedelta(seconds=1)),
    )

    _reader(client, spark, watermark_value=value).read()

    _assert_selected(log, selected)


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(T0 + timedelta(milliseconds=500), False, id="t0+500ms"),
        pytest.param(T0 + timedelta(microseconds=500_500), False, id="t0+500500us"),
        pytest.param(T0 + timedelta(microseconds=499_999), True, id="t0+499999us"),
        pytest.param(
            datetime(2026, 10, 21, 6, 0, 0, 500000, tzinfo=MINUS_3), False, id="minus-3-t0+500ms"
        ),
        pytest.param(
            datetime(2026, 10, 21, 6, 0, 0, 499999, tzinfo=MINUS_3),
            True,
            id="minus-3-t0+499999us",
        ),
    ],
)
def test_at027_time_created_is_truncated_to_the_millisecond_and_compared_as_an_instant(
    value: datetime, selected: bool, log: _Log, spark: _FakeSpark, client: _FakeGcsClient
) -> None:
    _hold(client, _held("a.csv", T0 + timedelta(microseconds=500_999)))

    _reader(client, spark, watermark_value=value).read()

    _assert_selected(log, ["a.csv"] if selected else [])


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(T0 - timedelta(milliseconds=1), ("a.csv", "b.csv"), id="t0-1ms"),
        pytest.param(T0, ("b.csv",), id="t0"),
        pytest.param(T0 + timedelta(seconds=2), (), id="t0+2s"),
    ],
)
def test_at028_the_updated_time_plays_no_part(
    value: datetime,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeGcsClient,
) -> None:
    _hold(
        client,
        _Obj(BLOB_PREFIX + "a.csv", time_created=T0, updated=T0 + timedelta(hours=1)),
        _held("b.csv", T0 + timedelta(seconds=2)),
    )

    _reader(client, spark, watermark_value=value).read()

    _assert_selected(log, selected)
    (listing,) = _listings(log)
    _assert_only_name_and_time_created(listing)


# --- Writing ------------------------------------------------------------------------------


def _assert_one_chain(frame: _FakeDataFrame) -> None:
    """`write` was the only attribute read from `frame`, and its writer saved once."""
    assert _attributes(frame) == ["write"]
    (writer,) = _writers(frame)
    accessed = _attributes(writer)
    assert set(accessed) <= {"format", "option", "options", "mode", "save"}, accessed
    assert accessed.count("save") == 1, accessed


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at029_write_appends_with_one_call_chain(
    call: WriteCall, log: _Log, df: _FakeDataFrame, factory: _Factory
) -> None:
    result = call(GcsWriter("gs://lake/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "append"
    assert save.options == {}
    assert save.path == "gs://lake/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at030_write_with_replace_overwrites(
    log: _Log, df: _FakeDataFrame, factory: _Factory
) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(GcsWriter("gs://lake/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "overwrite"
    assert save.options == {}
    assert save.path == "gs://lake/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at031_options_reach_the_save_as_given_with_the_normalised_path(
    log: _Log, df: _FakeDataFrame
) -> None:
    writer = GcsWriter("gs://lake/orders/", "csv", options={"header": True, "Compression": "gzip"})

    writer.write(_frame(df))

    (save,) = _saves(log)
    assert save.source == "csv"
    assert save.path == "gs://lake/orders"
    assert _lower(save.options) == {"header": "true", "compression": "gzip"}
    assert "Compression" in save.options, save.options


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at032_a_failed_save_propagates_unchanged(call: WriteCall, log: _Log) -> None:
    error = RuntimeError("task failed")
    frame = _FakeDataFrame(log, save_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        call(GcsWriter("gs://lake/orders", "parquet"), _frame(frame))
    assert excinfo.value is error
    assert len(_saves(log)) == 1


# --- Composition --------------------------------------------------------------------------


def test_at034_the_connectors_write_no_log_lines(
    log: _Log,
    spark: _FakeSpark,
    client: _FakeGcsClient,
    df: _FakeDataFrame,
    factory: _Factory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    _hold(client, _held("a.csv", T0))
    factory.pages = [_page(_held("a.csv", T0))]

    _reader(client, spark).read()
    _reader(client, spark, watermark_value=T0).read()
    GcsReader(URI, "csv", spark=_session(spark)).read()
    GcsReader(URI, "csv", spark=_session(spark), watermark_value=T0).read()
    writer = GcsWriter("gs://lake/orders", "parquet")
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    assert len(_loads(log)) == 2
    assert len(_creates(log)) == 2
    assert len(_saves(log)) == 2
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

GCS_URL = os.environ.get("INGESTIO_GCS_URL", "")
GCS_CONNECTOR_VERSION = "4.0.6"
GCS_CONNECTOR_JAR = os.environ.get("INGESTIO_GCS_CONNECTOR_JAR") or (
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
    f"{GCS_CONNECTOR_VERSION}/gcs-connector-{GCS_CONNECTOR_VERSION}-shaded.jar"
)

needs_gcs = pytest.mark.skipif(
    not GCS_URL, reason="INGESTIO_GCS_URL is not set: no GCS scratch location to run against"
)

SCHEMA = "id INT, name STRING"
CSV = {"header": True}


@dataclass(frozen=True)
class _Scratch:
    """A unique sub-prefix of `INGESTIO_GCS_URL` for one test, and a client of its bucket.

    `url` is the sub-prefix as a `gs://` URI, and `prefix` is its name prefix, without the
    trailing `/`.
    """

    url: str
    bucket: str
    prefix: str
    client: Client


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """A local Spark session in UTC that reaches GCS through the Cloud Storage connector."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-gcs-integration")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", GCS_CONNECTOR_JAR)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _blob_name(scratch: _Scratch, name: str) -> str:
    """The full name of the blob `<prefix>/<name>`."""
    return f"{scratch.prefix}/{name}"


def _listed(scratch: _Scratch, under: str = "") -> list[Any]:
    """Every blob whose name starts with the scratch prefix and `under`, listed recursively."""
    return list(scratch.client.list_blobs(scratch.bucket, prefix=scratch.prefix + under))


def _names(scratch: _Scratch) -> list[str]:
    """The names of the blobs under the scratch prefix, relative to it."""
    return [str(blob.name).removeprefix(scratch.prefix + "/") for blob in _listed(scratch, "/")]


def _put(scratch: _Scratch, name: str, text: str) -> None:
    """Upload `text` as the blob `<prefix>/<name>`."""
    blob = scratch.client.bucket(scratch.bucket).blob(_blob_name(scratch, name))
    blob.upload_from_string(text)


def _times(scratch: _Scratch, name: str) -> tuple[datetime, datetime]:
    """The `time_created` and the `updated` of the blob `<prefix>/<name>`, from a listing."""
    full = _blob_name(scratch, name)
    for blob in scratch.client.list_blobs(scratch.bucket, prefix=full):
        if blob.name == full:
            return cast(datetime, blob.time_created), cast(datetime, blob.updated)
    raise AssertionError(f"{full} is not listed")


def _time_created(scratch: _Scratch, name: str) -> datetime:
    """The `time_created` of the blob `<prefix>/<name>`, taken from a listing."""
    return _times(scratch, name)[0]


def _millis(moment: datetime) -> int:
    """Whole milliseconds since the epoch, exactly and truncated."""
    return (moment - EPOCH) // timedelta(milliseconds=1)


def _wait_past(moment: datetime) -> None:
    """Wait until the local clock is past `moment` by a millisecond or more."""
    while datetime.now(UTC) <= moment + timedelta(milliseconds=1):
        time.sleep(0.01)


def _put_after(scratch: _Scratch, name: str, text: str, moment: datetime) -> datetime:
    """Upload `name` until GCS creates it in a later millisecond than `moment`.

    Returns its `time_created`. Each new upload creates a new generation, with its own time.
    """
    for _ in range(10):
        _wait_past(moment)
        _put(scratch, name, text)
        created = _time_created(scratch, name)
        if _millis(created) > _millis(moment):
            return created
        time.sleep(0.1)
    raise AssertionError(f"GCS created {name} no later than {moment}")


def _delete_all(scratch: _Scratch) -> None:
    """Delete every blob whose name starts with the scratch prefix."""
    for blob in _listed(scratch):
        blob.delete()


@pytest.fixture
def scratch() -> Iterator[_Scratch]:
    """A unique sub-prefix of `INGESTIO_GCS_URL`, deleted at the end of the test."""
    _, _, rest = GCS_URL.partition("://")
    bucket, _, base = rest.partition("/")
    prefix = "/".join(part for part in (base.strip("/"), f"ingestio-{uuid.uuid4().hex}") if part)
    made = _Scratch(f"gs://{bucket}/{prefix}", bucket, prefix, Client(project=None))
    try:
        yield made
    finally:
        _delete_all(made)


def _rows(frame: DataFrame) -> list[tuple[Any, ...]]:
    """The rows of `frame`, as sorted tuples."""
    return sorted(tuple(row) for row in frame.collect())


def _csv_reader(scratch: _Scratch, spark: SparkSession, **arguments: Any) -> GcsReader:
    """A reader of the CSV blobs under the scratch prefix, with a header, `SCHEMA` and the
    scratch client."""
    return GcsReader(
        scratch.url,
        "csv",
        schema=SCHEMA,
        options=CSV,
        client=scratch.client,
        spark=spark,
        **arguments,
    )


@pytest.mark.integration
@needs_gcs
def test_parquet_appends_then_replaces_and_a_full_read_returns_the_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    writer = GcsWriter(scratch.url, "parquet")

    writer.write(spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA))
    writer.write(spark_session.createDataFrame([(3, "c")], SCHEMA))

    assert "_SUCCESS" in _names(scratch), _names(scratch)
    reader = GcsReader(scratch.url, "parquet", client=scratch.client, spark=spark_session)
    assert _rows(reader.read()) == [(1, "a"), (2, "b"), (3, "c")]

    writer.write(spark_session.createDataFrame([(10, "x")], SCHEMA), replace=True)

    assert _rows(reader.read()) == [(10, "x")]


@pytest.mark.integration
@needs_gcs
def test_csv_written_with_a_header_reads_back_with_the_same_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    frame = spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA)

    GcsWriter(scratch.url, "csv", options=CSV).write(frame)
    back = _csv_reader(scratch, spark_session).read()

    assert back.columns == ["id", "name"]
    assert _rows(back) == [(1, "a"), (2, "b")]


@pytest.mark.integration
@needs_gcs
def test_a_name_watermark_reads_a_frozen_selection_of_the_blobs_above_it(
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
@needs_gcs
def test_a_time_created_watermark_is_never_above_the_metadata_column(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")
    a_time = _time_created(scratch, "a.csv")
    b_time = _put_after(scratch, "b.csv", "id,name\n2,b\n", a_time)

    frame = _csv_reader(scratch, spark_session, watermark_value=a_time).read()

    assert _rows(frame) == [(2, "b")]
    column = functions.unix_millis("_metadata.file_modification_time")
    row = frame.select(functions.max(column)).first()
    assert row is not None
    millis = cast(int, row[0])
    assert millis >= _millis(b_time), (millis, b_time)
    last = EPOCH + timedelta(milliseconds=millis)
    assert _csv_reader(scratch, spark_session, watermark_value=last).read().count() == 0


@pytest.mark.integration
@needs_gcs
def test_a_metadata_change_does_not_select_a_blob_again_and_an_overwrite_does(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")
    a_time = _time_created(scratch, "a.csv")
    b_time = _put_after(scratch, "b.csv", "id,name\n2,b\n", a_time)
    _wait_past(b_time)
    for name in ("a.csv", "b.csv"):
        blob = scratch.client.bucket(scratch.bucket).get_blob(_blob_name(scratch, name))
        blob.metadata = {"ingestio-test": "touched"}
        blob.patch()
        created, updated = _times(scratch, name)
        assert _millis(updated) > _millis(created), (name, created, updated)

    reader = _csv_reader(scratch, spark_session, watermark_value=b_time)

    assert reader.read().count() == 0
    _put_after(scratch, "a.csv", "id,name\n3,c\n", b_time)
    assert _rows(reader.read()) == [(3, "c")]


@pytest.mark.integration
@needs_gcs
def test_an_empty_selection_has_the_columns_of_the_schema_or_none(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    with_schema = GcsReader(
        scratch.url, "csv", schema=SCHEMA, client=scratch.client, spark=spark_session
    ).read()
    without_schema = GcsReader(
        scratch.url, "csv", client=scratch.client, spark=spark_session
    ).read()

    assert with_schema.columns == ["id", "name"]
    assert with_schema.count() == 0
    assert without_schema.columns == []
    assert without_schema.count() == 0


@pytest.mark.integration
@needs_gcs
def test_a_directory_placeholder_and_nested_blobs_are_not_read(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    scratch.client.bucket(scratch.bucket).blob(f"{scratch.prefix}/").upload_from_string(b"")
    _put(scratch, "nested/b.csv", "id,name\n2,b\n")
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = _csv_reader(scratch, spark_session).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_gcs
def test_a_reader_without_client_lists_through_the_default_client(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = GcsReader(scratch.url, "csv", schema=SCHEMA, options=CSV, spark=spark_session).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_gcs
def test_a_write_whose_computation_fails_raises_out_of_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    def explode(value: int) -> int:
        raise ValueError("udf boom")

    failing = functions.udf(explode, "int")
    frame = spark_session.createDataFrame([(1,)], "id INT").select(failing("id").alias("id"))

    with pytest.raises(Exception, match="udf boom"):
        GcsWriter(scratch.url, "parquet").write(frame)


@pytest.mark.integration
@needs_gcs
def test_replace_removes_blobs_spark_did_not_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "keep.txt", "keep me\n")

    GcsWriter(scratch.url, "parquet").write(
        spark_session.createDataFrame([(1, "a")], SCHEMA), replace=True
    )

    assert "keep.txt" not in _names(scratch), _names(scratch)
    reader = GcsReader(scratch.url, "parquet", client=scratch.client, spark=spark_session)
    assert _rows(reader.read()) == [(1, "a")]
