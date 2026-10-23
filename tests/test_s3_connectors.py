"""S3 connectors: `S3Reader` and `S3Writer`.

Covers `specs/s3_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM, no network and no credentials: they use the doubles below, a fake S3 client
whose listing returns pages shaped like boto3's, a fake `boto3.client`, a fake Spark session and
fake DataFrames, which all write to one ordered event log.

The integration tests at the end run the connectors against S3, or an S3-compatible store, and
a local Spark session, `local[2]`, in UTC. They are marked `integration` and skipped unless
`INGESTIO_S3_URL` holds a scratch location, `s3a://<bucket>/<prefix>`. Each test works under a
unique sub-prefix of it and deletes that sub-prefix at the end. Credentials and the region come
from the standard AWS environment, for boto3 and S3A alike. For an S3-compatible store, the
standard `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` names the endpoint: boto3 reads it itself,
and the session passes it to S3A with path-style access. The session fetches `hadoop-aws` in the
version of the Hadoop that pyspark bundles, through `spark.jars.packages`. They need Java 17 or
21 for the locked pyspark.
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
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import boto3
import pyspark
import pytest
from botocore.exceptions import ClientError, NoRegionError
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import IntegerType, StructField, StructType

from ingestio.connector.all import S3Reader, S3Writer

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

T0 = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
MINUS_3 = timezone(timedelta(hours=-3))
URI = "s3://lake/landing/orders"
KEY_PREFIX = "landing/orders/"
P = r"orders_(\d{8})\.csv"
ORDER_FILES = ("orders_20261019.csv", "orders_20261020.csv", "orders_20261021.csv")
ANY_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["'][^"']+["']""")
S3_EXTRA_MARKER = re.compile(r"""\bextra\s*==\s*["']s3["']""")


# --- Test doubles -------------------------------------------------------------------------


@dataclass
class _Listing:
    """`paginate()` ran on a fake paginator of `client`, with these arguments.

    `pages_taken` counts the pages that the iteration has handed out so far.
    """

    args: tuple[object, ...]
    kwargs: dict[str, object]
    client: _FakeS3Client = field(repr=False, compare=False)
    pages_taken: int = 0


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


@dataclass(frozen=True)
class _Obj:
    """An object of the store: its whole key, its `LastModified` and its storage class."""

    key: str
    last_modified: datetime = T0
    storage_class: str = "STANDARD"
    size: int = 2


@dataclass(frozen=True)
class _Prefix:
    """A common prefix of the listing, whole."""

    prefix: str


_Item: TypeAlias = _Obj | _Prefix


def _page(*items: _Item, truncated: bool = False) -> dict[str, Any]:
    """A page of boto3's `list_objects_v2` paginator holding `items`, in this order.

    Like boto3's, the page has no `Contents` when it holds no object, and no `CommonPrefixes`
    when it holds no common prefix.
    """
    contents = [
        {
            "Key": item.key,
            "LastModified": item.last_modified,
            "Size": item.size,
            "StorageClass": item.storage_class,
        }
        for item in items
        if isinstance(item, _Obj)
    ]
    prefixes = [{"Prefix": item.prefix} for item in items if isinstance(item, _Prefix)]
    page: dict[str, Any] = {"IsTruncated": truncated, "KeyCount": len(contents) + len(prefixes)}
    if contents:
        page["Contents"] = contents
    if prefixes:
        page["CommonPrefixes"] = prefixes
    return page


class _FakeS3Client:
    """Stands in for a boto3 S3 client, and records every attribute accessed on it.

    `get_paginator()` records its arguments and returns a new `_FakePaginator`, or raises
    `paginator_error` once recorded, when it is set. The store is `_pages`, a list of pages
    shaped like boto3's, which `_hold` and `_hold_pages` replace; it starts as one page with
    neither `Contents` nor `CommonPrefixes`. When `page_error` is `(index, error)`, the
    iteration of a listing raises `error` when it reaches the page at `index`.
    """

    def __init__(
        self,
        log: _Log,
        *,
        paginator_error: BaseException | None = None,
        page_error: tuple[int, BaseException] | None = None,
    ) -> None:
        self._log = log
        self._paginator_error = paginator_error
        self._page_error = page_error
        self._pages: list[dict[str, Any]] = [_page()]
        self._accessed: list[str] = []
        self._paginator_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self._paginators: list[_FakePaginator] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    def get_paginator(self, *args: object, **kwargs: object) -> _FakePaginator:
        self._paginator_calls.append((args, dict(kwargs)))
        if self._paginator_error is not None:
            raise self._paginator_error
        paginator = _FakePaginator(self)
        self._paginators.append(paginator)
        return paginator


class _FakePaginator:
    """Stands in for a boto3 paginator, and records every attribute accessed on it.

    `paginate()` records a `_Listing` and returns an iterator over the pages of the store.
    """

    def __init__(self, client: _FakeS3Client) -> None:
        self._client = client
        self._accessed: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
        return object.__getattribute__(self, name)

    def paginate(self, *args: object, **kwargs: object) -> Iterator[dict[str, Any]]:
        listing = _Listing(args, dict(kwargs), self._client)
        self._client._log.events.append(listing)
        return _iterate(self._client, listing)


def _iterate(client: _FakeS3Client, listing: _Listing) -> Iterator[dict[str, Any]]:
    """The pages of the store, counted in `listing` as they are taken.

    It raises the error of `page_error` when the iteration reaches its page.
    """
    for index, page in enumerate(list(client._pages)):
        if client._page_error is not None and client._page_error[0] == index:
            raise client._page_error[1]
        listing.pages_taken += 1
        yield page


class _Factory:
    """Stands in for `boto3.client`: records each call and returns a new fake client.

    Each client it returns holds the pages of `pages`. When `error` is set, each call raises it
    once recorded.
    """

    def __init__(self, log: _Log, *, error: BaseException | None = None) -> None:
        self.log = log
        self.error = error
        self.pages: list[dict[str, Any]] = [_page()]
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.clients: list[_FakeS3Client] = []

    def __call__(self, *args: object, **kwargs: object) -> _FakeS3Client:
        self.calls.append((args, dict(kwargs)))
        if self.error is not None:
            raise self.error
        client = _FakeS3Client(self.log)
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
def client(log: _Log) -> _FakeS3Client:
    """The fake S3 client of the acceptance criteria. Its store starts empty."""
    return _FakeS3Client(log)


@pytest.fixture
def factory(log: _Log, monkeypatch: pytest.MonkeyPatch) -> _Factory:
    """The factory of the criteria, installed as `boto3.client`."""
    made = _Factory(log)
    monkeypatch.setattr(boto3, "client", made)
    return made


def _session(fake: _FakeSpark) -> SparkSession:
    """The fake, typed as the session the reader takes."""
    return cast(SparkSession, fake)


def _frame(fake: _FakeDataFrame) -> DataFrame:
    """The fake, typed as the DataFrame the connectors deal in."""
    return cast(DataFrame, fake)


def _client(fake: _FakeS3Client) -> S3Client:
    """The fake, typed as the client the reader takes."""
    return cast("S3Client", fake)


def _reader(client: _FakeS3Client, spark: _FakeSpark, **arguments: Any) -> S3Reader:
    """The reader of the criteria, `S3Reader(URI, "csv", client=client, spark=spark)`.

    `arguments` are the ones a criterion adds.
    """
    return S3Reader(URI, "csv", client=_client(client), spark=_session(spark), **arguments)


def _held(name: str, at: datetime = T0) -> _Obj:
    """`name` of the criteria: an object whose key is `landing/orders/<name>`, at `at`."""
    return _Obj(KEY_PREFIX + name, at)


def _common(prefix: str) -> _Prefix:
    """The common prefix `prefix` of the criteria, under `landing/orders/`."""
    return _Prefix(KEY_PREFIX + prefix)


def _hold(client: _FakeS3Client, *items: _Item) -> None:
    """The store of `client` holds `items`, in this order, on one page."""
    client._pages = [_page(*items)]


def _hold_pages(client: _FakeS3Client, *pages: Sequence[_Item]) -> None:
    """The store of `client` holds `pages`, each a page of items; every page but the last is
    truncated."""
    client._pages = [
        _page(*items, truncated=index < len(pages) - 1) for index, items in enumerate(pages)
    ]


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
    fake: _FakeDataFrame | _FakeDataFrameWriter | _FakeS3Client | _FakePaginator,
) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _writers(frame: _FakeDataFrame) -> list[_FakeDataFrameWriter]:
    """The fake writers that the accesses to `frame.write` returned, in order."""
    return frame._writers


def _paginator_calls(client: _FakeS3Client) -> list[tuple[tuple[object, ...], dict[str, object]]]:
    """The arguments of each `get_paginator()` call on `client`, in order."""
    return client._paginator_calls


def _paginators(client: _FakeS3Client) -> list[_FakePaginator]:
    """The fake paginators that `client` returned, in order."""
    return client._paginators


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


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make `import boto3` and `import botocore`, and their submodules, raise `ImportError`."""
    modules = cast("dict[str, object]", sys.modules)
    blocked = {"boto3", "botocore"}
    for name in [name for name in sys.modules if name.split(".")[0] in blocked]:
        monkeypatch.setitem(modules, name, None)
    for name in blocked:
        monkeypatch.setitem(modules, name, None)
    for name in blocked:
        with pytest.raises(ImportError):
            importlib.import_module(name)


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


WriteCall: TypeAlias = Callable[[S3Writer, DataFrame], object]

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
from ingestio.connector.all import S3Reader, S3Writer

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.s3 as s3

names = {"S3Reader", "S3Writer"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(s3.__all__), s3.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys
from datetime import UTC, datetime

for name in ("pyspark", "boto3", "botocore"):
    sys.modules[name] = None
for name in (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.types",
    "boto3",
    "botocore",
    "botocore.exceptions",
):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import S3Reader, S3Writer

loads = []
saves = []


class _FakePaginator:
    def paginate(self, *args, **kwargs):
        entry = {
            "Key": "landing/orders/a.csv",
            "LastModified": datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
            "Size": 2,
        }
        return iter([{"Contents": [entry], "KeyCount": 1, "IsTruncated": False}])


class _FakeS3Client:
    def get_paginator(self, name):
        return _FakePaginator()


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


reader = S3Reader(
    "s3://lake/landing/orders", "csv", client=_FakeS3Client(), spark=_FakeSpark()
)
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
S3Writer("s3://lake/orders", "parquet").write(_FakeDataFrame())
assert len(saves) == 1, saves
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connectors_work_without_pyspark_boto3_or_botocore(tmp_path: Path) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("with_spark", [False, True], ids=["bare", "with-spark"])
def test_at003_building_a_reader_without_client_or_boto3_names_the_extra(
    with_spark: bool, spark: _FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)

    message = "S3Reader needs boto3: pip install 'ingestio[s3]'"
    with pytest.raises(ImportError, match=_exactly(message)) as excinfo:
        if with_spark:
            S3Reader(URI, "csv", spark=_session(spark))
        else:
            S3Reader(URI, "csv")
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_at003_a_reader_with_a_client_and_the_writer_need_no_boto3(
    client: _FakeS3Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)

    S3Reader(URI, "csv", client=_client(client))
    S3Writer("s3://lake/orders", "parquet")


def test_at004_the_distribution_declares_the_s3_extra_with_boto3() -> None:
    metadata = importlib.metadata.metadata("ingestio")
    assert "s3" in (metadata.get_all("Provides-Extra") or [])
    requirements: list[str] = metadata.get_all("Requires-Dist") or []
    assert requirements
    for requirement in requirements:
        assert ANY_EXTRA_MARKER.search(requirement.partition(";")[2]), requirement
    assert any(
        _project(requirement) == "boto3" and S3_EXTRA_MARKER.search(requirement.partition(";")[2])
        for requirement in requirements
    ), requirements


# --- Building the connectors --------------------------------------------------------------

Build: TypeAlias = Callable[[str, str], object]


def _build_reader(path: str, fmt: str) -> object:
    """`S3Reader(path, fmt, client=client)`, with a fake client of its own."""
    return S3Reader(path, fmt, client=_client(_FakeS3Client(_Log())))


def _build_writer(path: str, fmt: str) -> object:
    """`S3Writer(path, fmt)`."""
    return S3Writer(path, fmt)


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
        pytest.param("gs://lake/landing/orders", id="gs"),
        pytest.param("s3n://lake/landing/orders", id="s3n"),
        pytest.param("S3://lake/landing/orders", id="uppercase"),
        pytest.param("s3:/lake/landing/orders", id="one-slash"),
        pytest.param("https://lake.s3.amazonaws.com/landing/orders", id="https"),
        pytest.param(Path("s3://lake/landing/orders"), id="path-object"),
    ],
)
def test_at005_a_path_other_than_an_s3_or_s3a_uri_is_rejected(build: Build, path: object) -> None:
    with pytest.raises(ValueError, match=_exactly("path must be an s3:// or s3a:// URI")):
        build(cast(Any, path), "csv")


BUCKET_NEEDED = "path must name a bucket"
SEGMENTS = "path must not contain empty, '.' or '..' segments"
PREFIX_NEEDED = "path must name a prefix inside the bucket"


@pytest.mark.parametrize(
    ("path", "reader_message", "writer_message"),
    [
        pytest.param("s3://", BUCKET_NEEDED, BUCKET_NEEDED, id="s3-nothing"),
        pytest.param("s3a://", BUCKET_NEEDED, BUCKET_NEEDED, id="s3a-nothing"),
        pytest.param("s3:///landing/orders", BUCKET_NEEDED, BUCKET_NEEDED, id="empty-bucket"),
        pytest.param("s3://lake//orders", SEGMENTS, SEGMENTS, id="empty-first-segment"),
        pytest.param("s3://lake/landing//orders", SEGMENTS, SEGMENTS, id="empty-segment"),
        pytest.param("s3://lake/./orders", SEGMENTS, SEGMENTS, id="dot"),
        pytest.param("s3://lake/landing/../orders", SEGMENTS, SEGMENTS, id="dot-dot"),
        pytest.param("s3://lake/landing/orders/..", SEGMENTS, SEGMENTS, id="trailing-dot-dot"),
        pytest.param("s3://lake", None, PREFIX_NEEDED, id="bucket-root"),
        pytest.param("s3a://lake/", None, PREFIX_NEEDED, id="bucket-root-slash"),
        pytest.param("s3://lake/landing/orders", None, None, id="s3"),
        pytest.param("s3a://lake/landing/orders/", None, None, id="s3a-trailing-slash"),
        pytest.param("s3://my.bucket-01/a b/ç", None, None, id="dots-space-unicode"),
        pytest.param("s3://lake/_staging/orders", None, None, id="underscore-segment"),
    ],
)
def test_at006_the_bucket_and_the_prefix_are_checked(
    path: str, reader_message: str | None, writer_message: str | None, client: _FakeS3Client
) -> None:
    _assert_builds_or_raises(lambda: S3Reader(path, "csv", client=_client(client)), reader_message)
    _assert_builds_or_raises(lambda: S3Writer(path, "csv"), writer_message)


def test_at007_repr_shows_the_normalised_path_and_the_format(client: _FakeS3Client) -> None:
    expected = "S3Reader(path='s3://lake/landing/orders', format='csv')"
    assert repr(S3Reader("s3://lake/landing/orders/", "csv", client=_client(client))) == expected
    assert (
        repr(
            S3Reader(
                "s3://lake/landing/orders",
                "csv",
                client=_client(client),
                watermark_pattern=P,
                watermark_value="20261021",
            )
        )
        == expected
    )
    assert (
        repr(S3Reader("s3a://lake/", "json", client=_client(client)))
        == "S3Reader(path='s3a://lake', format='json')"
    )
    assert (
        repr(S3Writer("s3://lake/orders//", "parquet"))
        == "S3Writer(path='s3://lake/orders', format='parquet')"
    )


@BUILDERS
def test_at008_an_empty_format_is_rejected(build: Build) -> None:
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        build("s3://lake/orders", "")


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_reader_rejects_delta_in_any_case(fmt: str, client: _FakeS3Client) -> None:
    message = f"format '{fmt}' cannot be read file by file"
    with pytest.raises(ValueError, match=_exactly(message)):
        S3Reader(URI, fmt, client=_client(client))


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at008_the_writer_accepts_delta(fmt: str) -> None:
    S3Writer("s3://lake/orders", fmt)


@BUILDERS
@pytest.mark.parametrize("fmt", ["parquet", "CSV", "com.example.source"])
def test_at008_any_other_format_name_is_accepted(build: Build, fmt: str) -> None:
    build("s3://lake/orders", fmt)


def test_at009_an_invalid_watermark_pattern_is_rejected_from_the_regex_error(
    client: _FakeS3Client,
) -> None:
    with pytest.raises(ValueError, match=r"^invalid watermark_pattern: ") as excinfo:
        S3Reader(URI, "csv", client=_client(client), watermark_pattern="(")
    cause = excinfo.value.__cause__
    assert isinstance(cause, re.error), cause
    assert cause.msg in str(excinfo.value)


@pytest.mark.parametrize(
    "pattern",
    [r"orders_\d{8}\.csv", r"(\d{4})(\d{4})\.csv", ""],
    ids=["no-group", "two-groups", "empty"],
)
def test_at009_a_watermark_pattern_needs_exactly_one_group(
    pattern: str, client: _FakeS3Client
) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_pattern must have exactly one group")):
        S3Reader(URI, "csv", client=_client(client), watermark_pattern=pattern)


@pytest.mark.parametrize(
    "pattern", [P, r"(?:orders|sales)_(?P<day>\d{8})\.csv"], ids=["one-group", "named-group"]
)
def test_at009_a_pattern_with_one_capturing_group_is_accepted(
    pattern: str, client: _FakeS3Client
) -> None:
    S3Reader(URI, "csv", client=_client(client), watermark_pattern=pattern)


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
    pattern: str | None, value: object, message: str, client: _FakeS3Client
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        S3Reader(
            URI,
            "csv",
            client=_client(client),
            watermark_pattern=pattern,
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize("value", ["", "20261021"], ids=["empty", "date"])
def test_at010_a_str_value_is_accepted_with_a_pattern(value: str, client: _FakeS3Client) -> None:
    S3Reader(URI, "csv", client=_client(client), watermark_pattern=P, watermark_value=value)


@pytest.mark.parametrize(
    "value", [T0, datetime(2026, 10, 21, 6, 0, tzinfo=MINUS_3)], ids=["utc", "minus-3"]
)
def test_at010_an_aware_datetime_is_accepted_without_a_pattern(
    value: datetime, client: _FakeS3Client
) -> None:
    S3Reader(URI, "csv", client=_client(client), watermark_value=value)


@pytest.mark.parametrize(
    "key",
    ["path", "PATHS", "pathGlobFilter", "recursivefilelookup", "modifiedBefore", "ModifiedAfter"],
)
def test_at011_options_the_reader_sets_are_rejected_in_any_case(
    key: str, client: _FakeS3Client
) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        S3Reader(URI, "csv", client=_client(client), options={key: "x"})


@pytest.mark.parametrize("key", ["path", "Paths", "replaceWhere", "PartitionOverwriteMode"])
def test_at011_options_the_writer_sets_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        S3Writer("s3://lake/orders", "parquet", options={key: "x"})


def test_at011_other_options_are_accepted(client: _FakeS3Client) -> None:
    S3Reader(
        URI,
        "csv",
        client=_client(client),
        options={"header": True, "inferSchema": "true", "mergeSchema": False},
    )
    S3Writer("s3://lake/orders", "parquet", options={"compression": "zstd", "header": True})


def test_at012_building_touches_neither_spark_nor_the_client_nor_s3(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client, factory: _Factory
) -> None:
    S3Reader(URI, "csv", client=_client(client), spark=_session(spark))
    S3Reader(URI, "csv", spark=_session(spark))
    S3Reader(
        URI,
        "csv",
        client=_client(client),
        spark=_session(spark),
        watermark_pattern=P,
        watermark_value="20261020",
    )
    S3Reader(URI, "csv", client=_client(client), spark=_session(spark), watermark_value=T0)
    S3Writer("s3://lake/orders", "parquet")

    assert log.spark_attributes == []
    assert _attributes(client) == []
    assert log.events == []
    assert factory.calls == []


def test_validation_comes_before_the_import_check(monkeypatch: pytest.MonkeyPatch) -> None:
    _without_boto3(monkeypatch)

    with pytest.raises(ValueError, match=_exactly("path must be an s3:// or s3a:// URI")):
        S3Reader("gs://lake/landing/orders", "csv")


# --- Reading ------------------------------------------------------------------------------


def test_at013_each_read_makes_one_flat_listing_of_the_prefix(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client, factory: _Factory
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    assert _paginator_calls(client) == [(("list_objects_v2",), {})]
    (listing,) = _listings(log)
    assert listing.args == ()
    assert listing.kwargs == {"Bucket": "lake", "Prefix": "landing/orders/", "Delimiter": "/"}
    assert listing.client is client
    assert set(_attributes(client)) == {"get_paginator"}, _attributes(client)
    (paginator,) = _paginators(client)
    assert set(_attributes(paginator)) == {"paginate"}, _attributes(paginator)
    assert factory.calls == []


def test_at013_at_the_bucket_root_the_listing_prefix_is_empty(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _Obj("a.csv"))

    S3Reader("s3a://lake/", "csv", client=_client(client), spark=_session(spark)).read()

    (listing,) = _listings(log)
    assert listing.args == ()
    assert listing.kwargs == {"Bucket": "lake", "Prefix": "", "Delimiter": "/"}
    (load,) = _loads(log)
    assert load.path == ["s3a://lake/a.csv"]


def test_at014_only_visible_objects_directly_under_the_prefix_are_loaded(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
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
        _common("sub/"),
        _common("x.csv/"),
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


def test_at014_a_single_object_is_still_passed_as_a_list(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("a.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv"])


def test_at015_names_are_sorted_by_code_point_and_kept_as_they_are(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("pedidos_ç.csv"), _held("a b.csv"), _held("2026-10-21T09:00:00.csv"))

    _reader(client, spark).read()

    _assert_selected(log, ["2026-10-21T09:00:00.csv", "a b.csv", "pedidos_ç.csv"])


def test_at015_the_paths_carry_the_scheme_as_written(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("a.csv"))

    S3Reader(
        "s3a://lake/landing/orders", "csv", client=_client(client), spark=_session(spark)
    ).read()

    (load,) = _loads(log)
    assert load.path == ["s3a://lake/landing/orders/a.csv"]


def test_at016_every_page_of_the_listing_is_taken_before_the_load(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold_pages(
        client,
        [_held("b.csv"), _held("a.csv"), _common("sub/")],
        [_common("x/")],
        [_held("c.csv")],
    )
    assert "Contents" not in client._pages[1]

    _reader(client, spark).read()

    (listing,) = _listings(log)
    assert listing.pages_taken == 3
    _assert_selected(log, ["a.csv", "b.csv", "c.csv"])


def test_objects_of_every_storage_class_and_size_are_candidates(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(
        client,
        _Obj(KEY_PREFIX + "a.csv", storage_class="GLACIER"),
        _Obj(KEY_PREFIX + "b.csv", storage_class="DEEP_ARCHIVE"),
        _Obj(KEY_PREFIX + "c.csv", storage_class="STANDARD_IA", size=0),
    )

    _reader(client, spark).read()

    _assert_selected(log, ["a.csv", "b.csv", "c.csv"])


def test_at017_the_schema_object_and_the_options_reach_the_load_as_given(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("a.csv"))
    schema = "id INT, name STRING"

    _reader(client, spark, schema=schema, options={"header": True, "sep": ";"}).read()

    (load,) = _loads(log)
    assert load.schema is schema
    assert _lower(load.options) == {"header": "true", "sep": ";"}


def test_at017_a_struct_type_schema_reaches_the_load_as_the_same_object(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("a.csv"))
    schema = StructType([StructField("id", IntegerType())])

    _reader(client, spark, schema=schema).read()

    (load,) = _loads(log)
    assert load.schema is schema


def test_at018_without_spark_the_active_session_is_looked_up_at_each_read(
    log: _Log, client: _FakeS3Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = S3Reader(URI, "csv", client=_client(client))
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
    log: _Log, client: _FakeS3Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    _hold(client, _held("a.csv"))
    reader = S3Reader(URI, "csv", client=_client(client))
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

    reader = S3Reader(URI, "csv", spark=_session(spark))
    assert factory.calls == []
    reader.read()
    assert factory.calls == [(("s3",), {})]
    reader.read()

    assert factory.calls == [(("s3",), {}), (("s3",), {})]
    first, second = _listings(log)
    assert first.client is factory.clients[0]
    assert second.client is factory.clients[1]
    assert [load.path for load in _loads(log)] == [_u("a.csv"), _u("a.csv")]


def test_at019_boto3_client_is_looked_up_at_each_read(
    log: _Log, spark: _FakeSpark, factory: _Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = S3Reader(URI, "csv", spark=_session(spark))
    replacement = _Factory(log)
    replacement.pages = [_page(_held("a.csv"))]
    monkeypatch.setattr(boto3, "client", replacement)

    reader.read()

    assert factory.calls == []
    assert replacement.calls == [(("s3",), {})]
    (listing,) = _listings(log)
    assert listing.client is replacement.clients[0]


def test_at019_an_error_of_boto3_client_propagates_unchanged(
    log: _Log, spark: _FakeSpark, factory: _Factory
) -> None:
    error = NoRegionError()
    factory.error = error
    reader = S3Reader(URI, "csv", spark=_session(spark))

    with pytest.raises(NoRegionError) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert log.events == []


def test_at020_each_read_lists_the_prefix_again(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
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


EMPTY_LAYOUTS = pytest.mark.parametrize("layout", ["no-object", "no-candidate"])


def _hold_layout(client: _FakeS3Client, layout: str) -> None:
    """The store of AT-021: a page with neither `Contents` nor `CommonPrefixes`, or one that
    holds no candidate."""
    if layout == "no-object":
        client._pages = [_page()]
        assert "Contents" not in client._pages[0]
        assert "CommonPrefixes" not in client._pages[0]
    else:
        _hold(client, _held("_SUCCESS"), _Obj("landing/orders/"), _common("sub/"))


@EMPTY_LAYOUTS
def test_at021_an_empty_selection_creates_an_empty_dataframe_without_columns(
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeS3Client
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
    layout: str, log: _Log, spark: _FakeSpark, client: _FakeS3Client
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


def _no_such_bucket() -> ClientError:
    """The error S3 answers a listing of a bucket that does not exist with."""
    return ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "The specified bucket does not exist"}},
        "ListObjectsV2",
    )


@pytest.mark.parametrize("where", ["first-page", "second-page", "get-paginator"])
def test_at022_a_failed_listing_propagates_unchanged_and_nothing_is_loaded(
    where: str, log: _Log, spark: _FakeSpark
) -> None:
    error = _no_such_bucket()
    if where == "get-paginator":
        client = _FakeS3Client(log, paginator_error=error)
        _hold(client, _held("a.csv"))
    elif where == "first-page":
        client = _FakeS3Client(log, page_error=(0, error))
        _hold(client, _held("a.csv"))
    else:
        client = _FakeS3Client(log, page_error=(1, error))
        _hold_pages(client, [_held("a.csv")], [_held("b.csv")])

    with pytest.raises(ClientError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert _loads(log) == []
    assert _creates(log) == []


def test_at022_a_failed_load_propagates_unchanged(log: _Log, client: _FakeS3Client) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, load_error=error)
    _hold(client, _held("a.csv"))

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_loads(log)) == 1


def test_at022_a_failed_create_dataframe_propagates_unchanged(
    log: _Log, client: _FakeS3Client
) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, create_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        _reader(client, spark).read()
    assert excinfo.value is error
    assert len(_creates(log)) == 1


# --- Watermark from the object name -------------------------------------------------------


def _hold_orders_and_others(client: _FakeS3Client) -> None:
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


def test_at023_objects_above_the_value_by_name_are_selected_whatever_their_time(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold_orders_and_others(client)

    _reader(client, spark, watermark_pattern=P, watermark_value="20261019").read()

    _assert_selected(log, ["orders_20261020.csv", "orders_20261021.csv"])


def test_at023_without_a_value_every_matching_object_is_selected(
    log: _Log, spark: _FakeSpark, client: _FakeS3Client
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
    client: _FakeS3Client,
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
            id="the-name-not-the-key",
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
    client: _FakeS3Client,
) -> None:
    _hold(client, *items)

    _reader(client, spark, watermark_pattern=pattern).read()

    _assert_selected(log, selected)


# --- Watermark from LastModified ----------------------------------------------------------


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
def test_at026_objects_modified_after_the_value_are_selected_in_name_order(
    value: datetime | None,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    client: _FakeS3Client,
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
    value: datetime, selected: bool, log: _Log, spark: _FakeSpark, client: _FakeS3Client
) -> None:
    _hold(client, _held("a.csv", T0 + timedelta(microseconds=999_999)))

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
    result = call(S3Writer("s3://lake/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "append"
    assert save.options == {}
    assert save.path == "s3://lake/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at029_write_with_replace_overwrites(
    log: _Log, df: _FakeDataFrame, factory: _Factory
) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(S3Writer("s3://lake/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "overwrite"
    assert save.options == {}
    assert save.path == "s3://lake/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)
    assert factory.calls == []


def test_at030_options_reach_the_save_as_given_with_the_normalised_path(
    log: _Log, df: _FakeDataFrame
) -> None:
    writer = S3Writer("s3a://lake/orders/", "csv", options={"header": True, "Compression": "gzip"})

    writer.write(_frame(df))

    (save,) = _saves(log)
    assert save.source == "csv"
    assert save.path == "s3a://lake/orders"
    assert _lower(save.options) == {"header": "true", "compression": "gzip"}
    assert "Compression" in save.options, save.options


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at031_a_failed_save_propagates_unchanged(call: WriteCall, log: _Log) -> None:
    error = RuntimeError("task failed")
    frame = _FakeDataFrame(log, save_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        call(S3Writer("s3://lake/orders", "parquet"), _frame(frame))
    assert excinfo.value is error
    assert len(_saves(log)) == 1


# --- Composition --------------------------------------------------------------------------


def test_at033_the_connectors_write_no_log_lines(
    log: _Log,
    spark: _FakeSpark,
    client: _FakeS3Client,
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
    S3Reader(URI, "csv", spark=_session(spark)).read()
    S3Reader(URI, "csv", spark=_session(spark), watermark_value=T0).read()
    writer = S3Writer("s3://lake/orders", "parquet")
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    assert len(_loads(log)) == 2
    assert len(_creates(log)) == 2
    assert len(_saves(log)) == 2
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

S3_URL = os.environ.get("INGESTIO_S3_URL", "")

needs_s3 = pytest.mark.skipif(
    not S3_URL, reason="INGESTIO_S3_URL is not set: no S3 scratch location to run against"
)

SCHEMA = "id INT, name STRING"
CSV = {"header": True}


@dataclass(frozen=True)
class _Scratch:
    """A unique sub-prefix of `INGESTIO_S3_URL` for one test, and a client of its store.

    `url` is the sub-prefix as a URI, with the scheme of `INGESTIO_S3_URL`, and `prefix` is its
    key prefix, without the trailing `/`.
    """

    url: str
    bucket: str
    prefix: str
    client: S3Client


def _endpoint() -> str | None:
    """The endpoint of an S3-compatible store, from the standard AWS environment, if any."""
    return os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or None


def _hadoop_aws() -> str:
    """The Maven coordinates of `hadoop-aws` in the version of the Hadoop pyspark bundles."""
    jars = Path(pyspark.__file__).parent / "jars"
    (api,) = jars.glob("hadoop-client-api-*.jar")
    version = api.name.removeprefix("hadoop-client-api-").removesuffix(".jar")
    return f"org.apache.hadoop:hadoop-aws:{version}"


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """A local Spark session in UTC that reaches S3 through S3A."""
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-s3-integration")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", _hadoop_aws())
        # Lets `INGESTIO_S3_URL` use `s3://` too, as open-source Spark does not map it.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    endpoint = _endpoint()
    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", endpoint).config(
            "spark.hadoop.fs.s3a.path.style.access", "true"
        )
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)
    session = builder.getOrCreate()
    try:
        yield session
    finally:
        session.stop()


def _keys(scratch: _Scratch, under: str = "") -> list[str]:
    """Every key that starts with the scratch prefix and `under`, through a recursive listing."""
    keys: list[str] = []
    paginator = scratch.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=scratch.bucket, Prefix=scratch.prefix + under):
        keys.extend(entry["Key"] for entry in page.get("Contents", []))
    return keys


def _names(scratch: _Scratch) -> list[str]:
    """The keys under the scratch prefix, relative to it."""
    return [key.removeprefix(scratch.prefix + "/") for key in _keys(scratch, "/")]


def _put(scratch: _Scratch, name: str, text: str) -> None:
    """Upload `text` as the object `<prefix>/<name>`."""
    scratch.client.put_object(
        Bucket=scratch.bucket, Key=f"{scratch.prefix}/{name}", Body=text.encode()
    )


def _last_modified(scratch: _Scratch, name: str) -> datetime:
    """The `LastModified` of the object `<prefix>/<name>`, taken from a listing."""
    key = f"{scratch.prefix}/{name}"
    paginator = scratch.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=scratch.bucket, Prefix=key):
        for entry in page.get("Contents", []):
            if entry["Key"] == key:
                return entry["LastModified"]
    raise AssertionError(f"{key} is not listed")


def _delete_all(scratch: _Scratch) -> None:
    """Delete every object whose key starts with the scratch prefix."""
    keys = _keys(scratch)
    for start in range(0, len(keys), 1000):
        chunk = keys[start : start + 1000]
        scratch.client.delete_objects(
            Bucket=scratch.bucket,
            Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
        )


@pytest.fixture
def scratch() -> Iterator[_Scratch]:
    """A unique sub-prefix of `INGESTIO_S3_URL`, deleted at the end of the test."""
    scheme, _, rest = S3_URL.partition("://")
    bucket, _, base = rest.partition("/")
    prefix = "/".join(part for part in (base.strip("/"), f"ingestio-{uuid.uuid4().hex}") if part)
    made = _Scratch(f"{scheme}://{bucket}/{prefix}", bucket, prefix, boto3.client("s3"))
    try:
        yield made
    finally:
        _delete_all(made)


def _rows(frame: DataFrame) -> list[tuple[Any, ...]]:
    """The rows of `frame`, as sorted tuples."""
    return sorted(tuple(row) for row in frame.collect())


@pytest.mark.integration
@needs_s3
def test_parquet_appends_then_replaces_and_a_full_read_returns_the_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    writer = S3Writer(scratch.url, "parquet")

    writer.write(spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA))
    writer.write(spark_session.createDataFrame([(3, "c")], SCHEMA))

    assert "_SUCCESS" in _names(scratch), _names(scratch)
    reader = S3Reader(scratch.url, "parquet", client=scratch.client, spark=spark_session)
    assert _rows(reader.read()) == [(1, "a"), (2, "b"), (3, "c")]

    writer.write(spark_session.createDataFrame([(10, "x")], SCHEMA), replace=True)

    assert _rows(reader.read()) == [(10, "x")]


@pytest.mark.integration
@needs_s3
def test_csv_written_with_a_header_reads_back_with_the_same_rows(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    frame = spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA)

    S3Writer(scratch.url, "csv", options=CSV).write(frame)
    back = S3Reader(
        scratch.url,
        "csv",
        schema=SCHEMA,
        options=CSV,
        client=scratch.client,
        spark=spark_session,
    ).read()

    assert back.columns == ["id", "name"]
    assert _rows(back) == [(1, "a"), (2, "b")]


@pytest.mark.integration
@needs_s3
def test_a_name_watermark_reads_a_frozen_selection_of_the_objects_above_it(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "orders_20261019.csv", "id,name\n1,a\n")
    _put(scratch, "orders_20261020.csv", "id,name\n2,b\n")
    _put(scratch, "orders_20261021.csv", "id,name\n3,c\n4,d\n")

    frame = S3Reader(
        scratch.url,
        "csv",
        watermark_pattern=P,
        watermark_value="20261019",
        schema=SCHEMA,
        options=CSV,
        client=scratch.client,
        spark=spark_session,
    ).read()

    assert _rows(frame) == [(2, "b"), (3, "c"), (4, "d")]
    assert frame.count() == 3
    _put(scratch, "orders_20261022.csv", "id,name\n5,e\n")
    assert frame.count() == 3


@pytest.mark.integration
@needs_s3
def test_a_last_modified_watermark_matches_the_metadata_column_to_the_second(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")
    a_time = _last_modified(scratch, "a.csv")
    # Upload b.csv until S3 stamps it in a later second than a.csv.
    for _ in range(10):
        time.sleep(1.1)
        _put(scratch, "b.csv", "id,name\n2,b\n")
        b_time = _last_modified(scratch, "b.csv")
        if b_time > a_time:
            break
    assert b_time > a_time, (a_time, b_time)

    def read(value: datetime) -> DataFrame:
        return S3Reader(
            scratch.url,
            "csv",
            watermark_value=value,
            schema=SCHEMA,
            options=CSV,
            client=scratch.client,
            spark=spark_session,
        ).read()

    frame = read(a_time)

    assert _rows(frame) == [(2, "b")]
    column = functions.unix_seconds("_metadata.file_modification_time")
    row = frame.select(functions.max(column)).first()
    assert row is not None
    seconds = cast(int, row[0])
    assert seconds == int(b_time.timestamp())
    assert read(datetime.fromtimestamp(seconds, UTC)).count() == 0


@pytest.mark.integration
@needs_s3
def test_an_empty_selection_has_the_columns_of_the_schema_or_none(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    with_schema = S3Reader(
        scratch.url, "csv", schema=SCHEMA, client=scratch.client, spark=spark_session
    ).read()
    without_schema = S3Reader(scratch.url, "csv", client=scratch.client, spark=spark_session).read()

    assert with_schema.columns == ["id", "name"]
    assert with_schema.count() == 0
    assert without_schema.columns == []
    assert without_schema.count() == 0


@pytest.mark.integration
@needs_s3
def test_a_directory_marker_and_nested_objects_are_not_read(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    scratch.client.put_object(Bucket=scratch.bucket, Key=f"{scratch.prefix}/", Body=b"")
    _put(scratch, "nested/b.csv", "id,name\n2,b\n")
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = S3Reader(
        scratch.url,
        "csv",
        schema=SCHEMA,
        options=CSV,
        client=scratch.client,
        spark=spark_session,
    ).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_s3
def test_a_reader_without_client_lists_through_the_default_client(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "a.csv", "id,name\n1,a\n")

    frame = S3Reader(scratch.url, "csv", schema=SCHEMA, options=CSV, spark=spark_session).read()

    assert _rows(frame) == [(1, "a")]


@pytest.mark.integration
@needs_s3
def test_a_write_whose_computation_fails_raises_out_of_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    def explode(value: int) -> int:
        raise ValueError("udf boom")

    failing = functions.udf(explode, "int")
    frame = spark_session.createDataFrame([(1,)], "id INT").select(failing("id").alias("id"))

    with pytest.raises(Exception, match="udf boom"):
        S3Writer(scratch.url, "parquet").write(frame)


@pytest.mark.integration
@needs_s3
def test_replace_removes_objects_spark_did_not_write(
    scratch: _Scratch, spark_session: SparkSession
) -> None:
    _put(scratch, "keep.txt", "keep me\n")

    S3Writer(scratch.url, "parquet").write(
        spark_session.createDataFrame([(1, "a")], SCHEMA), replace=True
    )

    assert "keep.txt" not in _names(scratch), _names(scratch)
    reader = S3Reader(scratch.url, "parquet", client=scratch.client, spark=spark_session)
    assert _rows(reader.read()) == [(1, "a")]
