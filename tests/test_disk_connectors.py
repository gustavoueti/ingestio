"""Disk connectors: `DiskReader` and `DiskWriter`.

Covers `specs/disk_connectors.md`. The tests named after an acceptance criterion always run.
They need no JVM: they use real files under `tmp_path`, and the doubles below, a fake Spark
session and fake DataFrames, which all write to one ordered event log.

The integration tests at the end run the connectors against a local Spark session, `local[2]`,
in UTC, with files under `tmp_path`. They are marked `integration` and skipped unless
`INGESTIO_SPARK_TESTS` is `1`. They need Java 17 or 21 for the locked pyspark.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, TypeAlias, cast

import pytest
from pyspark.sql import DataFrame, SparkSession, functions
from pyspark.sql.types import IntegerType, StructField, StructType

from ingestio import Process, Reader, ReadSource, Step, Writer, WriteTarget
from ingestio.connector.all import DiskReader, DiskWriter

T0 = datetime(2026, 10, 21, 9, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUS_3 = timezone(timedelta(hours=-3))
P = r"orders_(\d{8})\.csv"
ORDER_FILES = ("orders_20261019.csv", "orders_20261020.csv", "orders_20261021.csv")


# --- Test doubles -------------------------------------------------------------------------


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


_Event: TypeAlias = _Load | _Create | _Save


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
def src(tmp_path: Path) -> Path:
    """`tmp_path / "landing"`, an existing directory that starts empty."""
    landing = tmp_path / "landing"
    landing.mkdir()
    return landing


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


def _creates(log: _Log) -> list[_Create]:
    """The `createDataFrame` calls recorded, in order."""
    return [event for event in log.events if isinstance(event, _Create)]


def _saves(log: _Log) -> list[_Save]:
    """The saves recorded, in order."""
    return [event for event in log.events if isinstance(event, _Save)]


def _attributes(fake: _FakeDataFrame | _FakeDataFrameWriter) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _writers(frame: _FakeDataFrame) -> list[_FakeDataFrameWriter]:
    """The fake writers that the accesses to `frame.write` returned, in order."""
    return frame._writers


def _paths(directory: Path, *names: str) -> list[str]:
    """`p(name)` of the criteria for each name: `str(directory / name)`."""
    return [str(directory / name) for name in names]


def _ns(moment: datetime) -> int:
    """`moment` in nanoseconds since the epoch, exactly."""
    return (moment - EPOCH) // timedelta(microseconds=1) * 1000


def _file(directory: Path, name: str, *, at: int | None = None, text: str = "x\n") -> Path:
    """Create the file `name` in `directory`, last modified at `at` ns since the epoch."""
    file = directory / name
    file.write_text(text, encoding="utf-8")
    if at is not None:
        os.utime(file, ns=(at, at))
        assert file.stat().st_mtime_ns == at, "the file system keeps coarser times"
    return file


def _assert_selected(log: _Log, directory: Path, names: Sequence[str]) -> None:
    """The read loaded exactly `names` of `directory`, in this order, as a `list`.

    With no names, the read made no load and called `createDataFrame` once.
    """
    if not names:
        assert _loads(log) == [], [load.path for load in _loads(log)]
        assert len(_creates(log)) == 1
        return
    (load,) = _loads(log)
    assert isinstance(load.path, list), load.path
    assert load.path == _paths(directory, *names)
    assert _creates(log) == []


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


WriteCall: TypeAlias = Callable[[DiskWriter, DataFrame], object]

APPEND_CALLS: dict[str, WriteCall] = {
    "write(df)": lambda writer, frame: writer.write(frame),
    "write(df, replace=False)": lambda writer, frame: writer.write(frame, replace=False),
}

EVERY_CALL: dict[str, WriteCall] = {
    **APPEND_CALLS,
    "write(df, replace=True)": lambda writer, frame: writer.write(frame, replace=True),
}


# --- Import surface -----------------------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import DiskReader, DiskWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.local as local

names = {"DiskReader", "DiskWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(local.__all__), local.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
"""

AT002_CODE = """
import sys
from pathlib import Path

sys.modules["pyspark"] = None
for name in ("pyspark", "pyspark.sql", "pyspark.sql.types"):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError(f"{name} can be imported")

import ingestio.connector.all
from ingestio.connector.all import DiskReader, DiskWriter

loads = []
saves = []


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


landing = Path.cwd() / "landing"
landing.mkdir()
(landing / "a.csv").write_text("x", encoding="utf-8")

reader = DiskReader(landing, "csv", spark=_FakeSpark())
writer = DiskWriter("/data/orders", "parquet")
frame = reader.read()
assert len(loads) == 1, loads
assert frame is loads[0], frame
writer.write(_FakeDataFrame())
assert len(saves) == 1, saves
"""


def test_at001_both_classes_are_exported_from_the_connector_surface_only(
    tmp_path: Path,
) -> None:
    result = _run_python(AT001_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


def test_at002_the_connectors_work_without_pyspark_installed(tmp_path: Path) -> None:
    result = _run_python(AT002_CODE, tmp_path)
    assert result.returncode == 0, result.stderr


# --- Building the connectors --------------------------------------------------------------

BUILDERS = pytest.mark.parametrize("build", [DiskReader, DiskWriter], ids=["reader", "writer"])


@BUILDERS
@pytest.mark.parametrize(
    "path",
    [
        "",
        "landing/orders",
        "./orders",
        "s3://bucket/orders",
        "file:/tmp/orders",
        "dbfs:/Volumes/main/landing/orders",
    ],
)
def test_at003_a_path_other_than_an_absolute_local_path_is_rejected(
    build: type[DiskReader] | type[DiskWriter], path: str
) -> None:
    with pytest.raises(ValueError, match=_exactly("path must be an absolute local path")):
        build(path, "csv")


@BUILDERS
@pytest.mark.parametrize(
    "path", ["/tmp/orders", Path("/Volumes/main/landing/orders")], ids=["str", "path"]
)
def test_at003_an_absolute_local_path_is_accepted(
    build: type[DiskReader] | type[DiskWriter], path: str | Path
) -> None:
    build(path, "csv")


@BUILDERS
def test_at004_an_empty_format_is_rejected(build: type[DiskReader] | type[DiskWriter]) -> None:
    with pytest.raises(ValueError, match=_exactly("format must not be empty")):
        build("/data/orders", "")


@pytest.mark.parametrize("fmt", ["delta", "Delta", "DELTA"])
def test_at004_the_reader_rejects_delta_in_any_case(fmt: str) -> None:
    message = f"format '{fmt}' cannot be read file by file"
    with pytest.raises(ValueError, match=_exactly(message)):
        DiskReader("/data/landing", fmt)


@pytest.mark.parametrize("fmt", ["delta", "Delta"])
def test_at004_the_writer_accepts_delta(fmt: str) -> None:
    DiskWriter("/data/orders", fmt)


@BUILDERS
@pytest.mark.parametrize("fmt", ["parquet", "CSV", "com.example.source"])
def test_at004_any_other_format_name_is_accepted(
    build: type[DiskReader] | type[DiskWriter], fmt: str
) -> None:
    build("/data/orders", fmt)


def test_at005_an_invalid_watermark_pattern_is_rejected_from_the_regex_error() -> None:
    with pytest.raises(ValueError, match=r"^invalid watermark_pattern: ") as excinfo:
        DiskReader("/data/landing", "csv", watermark_pattern="(")
    cause = excinfo.value.__cause__
    assert isinstance(cause, re.error), cause
    assert cause.msg in str(excinfo.value)


@pytest.mark.parametrize(
    "pattern",
    [r"orders_\d{8}\.csv", r"(\d{4})(\d{4})\.csv", ""],
    ids=["no-group", "two-groups", "empty"],
)
def test_at005_a_watermark_pattern_needs_exactly_one_group(pattern: str) -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_pattern must have exactly one group")):
        DiskReader("/data/landing", "csv", watermark_pattern=pattern)


@pytest.mark.parametrize(
    "pattern", [P, r"(?:orders|sales)_(?P<day>\d{8})\.csv"], ids=["one-group", "named-group"]
)
def test_at005_a_pattern_with_one_capturing_group_is_accepted(pattern: str) -> None:
    DiskReader("/data/landing", "csv", watermark_pattern=pattern)


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
def test_at006_a_watermark_value_of_the_wrong_kind_is_rejected(
    pattern: str | None, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        DiskReader(
            "/data/landing",
            "csv",
            watermark_pattern=pattern,
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize("value", ["", "20261021"], ids=["empty", "date"])
def test_at006_a_str_value_is_accepted_with_a_pattern(value: str) -> None:
    DiskReader("/data/landing", "csv", watermark_pattern=P, watermark_value=value)


@pytest.mark.parametrize(
    "value", [T0, datetime(2026, 10, 21, 6, 0, tzinfo=MINUS_3)], ids=["utc", "minus-3"]
)
def test_at006_an_aware_datetime_is_accepted_without_a_pattern(value: datetime) -> None:
    DiskReader("/data/landing", "csv", watermark_value=value)


@pytest.mark.parametrize(
    "key",
    ["path", "PATHS", "pathGlobFilter", "recursivefilelookup", "modifiedBefore", "ModifiedAfter"],
)
def test_at007_options_the_reader_sets_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        DiskReader("/data/landing", "csv", options={key: "x"})


@pytest.mark.parametrize("key", ["path", "Paths", "replaceWhere", "PartitionOverwriteMode"])
def test_at007_options_the_writer_sets_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        DiskWriter("/data/orders", "parquet", options={key: "x"})


def test_at007_other_options_are_accepted() -> None:
    DiskReader(
        "/data/landing",
        "csv",
        options={"header": True, "inferSchema": "true", "mergeSchema": False},
    )
    DiskWriter("/data/orders", "parquet", options={"compression": "zstd", "header": True})


def test_at008_building_touches_neither_spark_nor_the_file_system(
    log: _Log, spark: _FakeSpark, tmp_path: Path
) -> None:
    missing = tmp_path / "missing"

    DiskReader(missing, "csv", spark=_session(spark))
    DiskReader(
        missing,
        "csv",
        spark=_session(spark),
        watermark_pattern=P,
        watermark_value="20261020",
    )
    DiskReader(missing, "csv", spark=_session(spark), watermark_value=T0)
    DiskWriter(missing, "parquet")

    assert log.spark_attributes == []
    assert log.events == []
    assert not missing.exists()


def test_at009_repr_shows_the_normalised_path_and_the_format() -> None:
    expected = "DiskReader(path='/Volumes/main/landing/orders', format='csv')"
    assert repr(DiskReader("/Volumes/main/landing/orders/", "csv")) == expected
    assert (
        repr(
            DiskReader(
                Path("/Volumes/main/landing/orders"),
                "csv",
                watermark_pattern=P,
                watermark_value="20261021",
            )
        )
        == expected
    )
    assert (
        repr(DiskWriter("/Volumes/main/lake/orders", "parquet"))
        == "DiskWriter(path='/Volumes/main/lake/orders', format='parquet')"
    )


# --- Reading ------------------------------------------------------------------------------


def test_at010_only_visible_regular_files_directly_inside_the_directory_are_loaded(
    log: _Log, spark: _FakeSpark, src: Path, tmp_path: Path
) -> None:
    for name in [
        "b.csv",
        "a.csv",
        "_SUCCESS",
        "_metadata",
        "_common_metadata",
        ".a.csv.crc",
        "c.csv._COPYING_",
    ]:
        _file(src, name)
    (src / "sub").mkdir()
    _file(src / "sub", "d.csv")
    (src / "x.csv").mkdir()
    (src / "link.csv").symlink_to(_file(tmp_path, "outside.csv"))
    (src / "broken.csv").symlink_to(tmp_path / "nowhere.csv")

    frame = DiskReader(src, "csv", spark=_session(spark)).read()

    (load,) = _loads(log)
    assert load.source == "csv"
    assert load.schema is None
    assert load.options == {}
    assert isinstance(load.path, list), load.path
    assert load.path == _paths(src, "a.csv", "b.csv", "link.csv")
    assert frame is _frame(load.frame)
    assert _attributes(load.frame) == []
    assert _creates(log) == []


def test_at010_a_single_file_is_still_passed_as_a_list(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv")

    DiskReader(src, "csv", spark=_session(spark)).read()

    _assert_selected(log, src, ["a.csv"])


def test_at011_the_schema_object_and_the_options_reach_the_load_as_given(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv")
    schema = "id INT, name STRING"

    DiskReader(
        src, "csv", spark=_session(spark), schema=schema, options={"header": True, "sep": ";"}
    ).read()

    (load,) = _loads(log)
    assert load.schema is schema
    assert _lower(load.options) == {"header": "true", "sep": ";"}


def test_at011_a_struct_type_schema_reaches_the_load_as_the_same_object(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv")
    schema = StructType([StructField("id", IntegerType())])

    DiskReader(src, "csv", spark=_session(spark), schema=schema).read()

    (load,) = _loads(log)
    assert load.schema is schema


def test_at012_the_active_session_is_looked_up_at_each_read(
    log: _Log, src: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = DiskReader(src, "csv")
    assert lookups == []

    _file(src, "a.csv")
    first, second = _FakeSpark(log), _FakeSpark(log)
    monkeypatch.setattr(SparkSession, "active", lambda: _session(first))
    first_frame = reader.read()
    monkeypatch.setattr(SparkSession, "active", lambda: _session(second))
    second_frame = reader.read()

    first_load, second_load = _loads(log)
    assert first_frame is _frame(first_load.frame)
    assert second_frame is _frame(second_load.frame)
    assert first_load.path == second_load.path == _paths(src, "a.csv")


def test_at012_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, src: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _file(src, "a.csv")
    reader = DiskReader(src, "csv")
    error = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise error

    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is error


def test_at013_each_read_lists_the_directory_again(log: _Log, spark: _FakeSpark, src: Path) -> None:
    reader = DiskReader(src, "csv", spark=_session(spark))

    _file(src, "a.csv")
    reader.read()
    _file(src, "b.csv")
    reader.read()

    first, second = _loads(log)
    assert first.path == _paths(src, "a.csv")
    assert second.path == _paths(src, "a.csv", "b.csv")


def test_at014_a_missing_directory_raises_file_not_found(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    reader = DiskReader(src / "missing", "csv", spark=_session(spark))

    with pytest.raises(FileNotFoundError):
        reader.read()
    assert _loads(log) == []
    assert _creates(log) == []


def test_at014_a_path_to_a_file_raises_not_a_directory(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv")
    reader = DiskReader(src / "a.csv", "csv", spark=_session(spark))

    with pytest.raises(NotADirectoryError):
        reader.read()
    assert _loads(log) == []
    assert _creates(log) == []


@pytest.mark.parametrize("layout", ["empty", "no-candidate"])
def test_at015_an_empty_selection_creates_an_empty_dataframe_without_columns(
    layout: str, log: _Log, spark: _FakeSpark, src: Path
) -> None:
    if layout == "no-candidate":
        _file(src, "_SUCCESS")
        (src / "sub").mkdir()
        _file(src / "sub", "d.csv")

    frame = DiskReader(src, "csv", spark=_session(spark)).read()

    assert _loads(log) == []
    (create,) = _creates(log)
    assert isinstance(create.data, list)
    assert create.data == []
    assert isinstance(create.schema, StructType), create.schema
    assert create.schema.fields == []
    assert frame is _frame(create.frame)


@pytest.mark.parametrize("layout", ["empty", "no-candidate"])
def test_at015_an_empty_selection_with_a_schema_creates_an_empty_dataframe_with_it(
    layout: str, log: _Log, spark: _FakeSpark, src: Path
) -> None:
    if layout == "no-candidate":
        _file(src, "_SUCCESS")
        (src / "sub").mkdir()
        _file(src / "sub", "d.csv")
    schema = "id INT, name STRING"

    frame = DiskReader(src, "csv", spark=_session(spark), schema=schema).read()

    assert _loads(log) == []
    (create,) = _creates(log)
    assert isinstance(create.data, list)
    assert create.data == []
    assert create.schema is schema
    assert frame is _frame(create.frame)


def test_at016_a_failed_load_propagates_unchanged(log: _Log, src: Path) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, load_error=error)
    _file(src, "a.csv")

    with pytest.raises(RuntimeError) as excinfo:
        DiskReader(src, "csv", spark=_session(spark)).read()
    assert excinfo.value is error
    assert len(_loads(log)) == 1


def test_at016_a_failed_create_dataframe_propagates_unchanged(log: _Log, src: Path) -> None:
    error = RuntimeError("[PATH_NOT_FOUND]")
    spark = _FakeSpark(log, create_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        DiskReader(src, "csv", spark=_session(spark)).read()
    assert excinfo.value is error
    assert len(_creates(log)) == 1


# --- Watermark from the file name ---------------------------------------------------------


def _orders_and_others(src: Path) -> None:
    """The files of AT-017: dated orders, newest name oldest time, and names that do not match."""
    _file(src, "orders_20261019.csv", at=_ns(T0 + timedelta(seconds=2)))
    _file(src, "orders_20261020.csv", at=_ns(T0 + timedelta(seconds=1)))
    _file(src, "orders_20261021.csv", at=_ns(T0))
    for name in ["customers_20261021.csv", "orders_latest.csv", "orders_20261021.csv.bak"]:
        _file(src, name)


def test_at017_files_above_the_value_by_name_are_selected_whatever_their_time(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _orders_and_others(src)

    DiskReader(
        src, "csv", spark=_session(spark), watermark_pattern=P, watermark_value="20261019"
    ).read()

    _assert_selected(log, src, ["orders_20261020.csv", "orders_20261021.csv"])


def test_at017_without_a_value_every_matching_file_is_selected(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _orders_and_others(src)

    DiskReader(src, "csv", spark=_session(spark), watermark_pattern=P).read()

    _assert_selected(log, src, ORDER_FILES)


PART = r"part_(\d+)\.csv"
PART_FILES = ("part_9.csv", "part_10.csv")


@pytest.mark.parametrize(
    ("pattern", "files", "value", "selected"),
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
def test_at018_name_watermarks_compare_as_text(
    pattern: str,
    files: tuple[str, ...],
    value: str,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    src: Path,
) -> None:
    for name in files:
        _file(src, name)

    DiskReader(
        src, "csv", spark=_session(spark), watermark_pattern=pattern, watermark_value=value
    ).read()

    _assert_selected(log, src, selected)


def test_at019_the_pattern_must_match_the_whole_name(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "orders_20261021.csv")

    DiskReader(src, "csv", spark=_session(spark), watermark_pattern=r"(\d{8})\.csv").read()

    _assert_selected(log, src, [])


def test_at019_a_name_whose_group_took_no_part_in_the_match_is_not_read(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "orders.csv")
    _file(src, "orders_20261021.csv")

    DiskReader(src, "csv", spark=_session(spark), watermark_pattern=r"orders(_\d{8})?\.csv").read()

    _assert_selected(log, src, ["orders_20261021.csv"])


def test_at019_hidden_names_stay_out_even_when_the_pattern_matches_them(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    for name in ["_orders_20261022.csv", ".orders_20261022.csv", "orders_20261021.csv"]:
        _file(src, name)

    DiskReader(src, "csv", spark=_session(spark), watermark_pattern=r".*_(\d{8})\.csv").read()

    _assert_selected(log, src, ["orders_20261021.csv"])


# --- Watermark from the modification time -------------------------------------------------


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(None, ("a.csv", "b.csv", "c.csv"), id="none"),
        pytest.param(T0 - timedelta(milliseconds=1), ("a.csv", "b.csv", "c.csv"), id="t0-1ms"),
        pytest.param(T0, ("a.csv", "b.csv"), id="t0"),
        pytest.param(T0 + timedelta(milliseconds=500), ("a.csv",), id="t0+500ms"),
        pytest.param(T0 + timedelta(seconds=1), (), id="t0+1s"),
    ],
)
def test_at020_files_modified_after_the_value_are_selected_in_name_order(
    value: datetime | None,
    selected: tuple[str, ...],
    log: _Log,
    spark: _FakeSpark,
    src: Path,
) -> None:
    _file(src, "c.csv", at=_ns(T0))
    _file(src, "b.csv", at=_ns(T0 + timedelta(milliseconds=500)))
    _file(src, "a.csv", at=_ns(T0 + timedelta(seconds=1)))

    DiskReader(src, "csv", spark=_session(spark), watermark_value=value).read()

    _assert_selected(log, src, selected)


@pytest.mark.parametrize(
    ("value", "selected"),
    [
        pytest.param(T0 + timedelta(milliseconds=500), False, id="t0+500ms"),
        pytest.param(T0 + timedelta(microseconds=499_999), True, id="t0+499999us"),
        pytest.param(
            datetime(2026, 10, 21, 6, 0, 0, 500000, tzinfo=MINUS_3), False, id="minus-3-500ms"
        ),
        pytest.param(
            datetime(2026, 10, 21, 6, 0, 0, 499999, tzinfo=MINUS_3), True, id="minus-3-499999us"
        ),
    ],
)
def test_at021_modification_times_are_truncated_to_the_millisecond_and_compared_as_instants(
    value: datetime, selected: bool, log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv", at=_ns(T0) + 500_999_999)

    DiskReader(src, "csv", spark=_session(spark), watermark_value=value).read()

    _assert_selected(log, src, ["a.csv"] if selected else [])


def test_a_link_is_selected_by_the_modification_time_of_its_target(
    log: _Log, spark: _FakeSpark, src: Path, tmp_path: Path
) -> None:
    target = _file(tmp_path, "outside.csv", at=_ns(T0 + timedelta(seconds=1)))
    link = src / "link.csv"
    link.symlink_to(target)
    before = _ns(T0 - timedelta(seconds=1))
    os.utime(link, ns=(before, before), follow_symlinks=False)

    DiskReader(src, "csv", spark=_session(spark), watermark_value=T0).read()

    _assert_selected(log, src, ["link.csv"])


# --- Writing ------------------------------------------------------------------------------


def _assert_one_chain(frame: _FakeDataFrame) -> None:
    """`write` was the only attribute read from `frame`, and its writer saved once."""
    assert _attributes(frame) == ["write"]
    (writer,) = _writers(frame)
    accessed = _attributes(writer)
    assert set(accessed) <= {"format", "option", "options", "mode", "save"}, accessed
    assert accessed.count("save") == 1, accessed


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at022_write_appends_with_one_call_chain(
    call: WriteCall, log: _Log, df: _FakeDataFrame
) -> None:
    result = call(DiskWriter("/data/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "append"
    assert save.options == {}
    assert save.path == "/data/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)


def test_at023_write_with_replace_overwrites(log: _Log, df: _FakeDataFrame) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(DiskWriter("/data/orders", "parquet"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "parquet"
    assert save.mode == "overwrite"
    assert save.options == {}
    assert save.path == "/data/orders"
    assert isinstance(save.path, str)
    assert save.partition_by is None
    _assert_one_chain(df)


def test_at024_options_reach_the_save_as_given_with_the_normalised_path(
    log: _Log, df: _FakeDataFrame
) -> None:
    writer = DiskWriter("/data/orders/", "csv", options={"header": True, "Compression": "gzip"})

    writer.write(_frame(df))

    (save,) = _saves(log)
    assert save.source == "csv"
    assert save.path == "/data/orders"
    assert _lower(save.options) == {"header": "true", "compression": "gzip"}
    assert "Compression" in save.options, save.options


def test_at024_a_path_object_reaches_the_save_as_a_str(log: _Log, df: _FakeDataFrame) -> None:
    DiskWriter(Path("/data/orders"), "parquet").write(_frame(df))

    (save,) = _saves(log)
    assert save.path == "/data/orders"
    assert isinstance(save.path, str)


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at025_a_failed_save_propagates_unchanged(call: WriteCall, log: _Log) -> None:
    error = RuntimeError("task failed")
    frame = _FakeDataFrame(log, save_error=error)

    with pytest.raises(RuntimeError) as excinfo:
        call(DiskWriter("/data/orders", "parquet"), _frame(frame))
    assert excinfo.value is error
    assert len(_saves(log)) == 1


def test_at026_the_writer_creates_lists_and_deletes_nothing_itself(
    log: _Log, tmp_path: Path
) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "keep.txt").write_text("keep me\n", encoding="utf-8")
    new = tmp_path / "new"

    DiskWriter(out, "parquet").write(_frame(_FakeDataFrame(log)), replace=True)
    DiskWriter(new, "parquet").write(_frame(_FakeDataFrame(log)))

    assert [entry.name for entry in out.iterdir()] == ["keep.txt"]
    assert (out / "keep.txt").read_text(encoding="utf-8") == "keep me\n"
    assert not new.exists()
    assert [save.path for save in _saves(log)] == [str(out), str(new)]


# --- Composition --------------------------------------------------------------------------


def test_at027_read_source_and_write_target_move_a_dataframe_and_append(
    log: _Log, spark: _FakeSpark, src: Path
) -> None:
    _file(src, "a.csv")
    source: Reader[DataFrame] = DiskReader(src, "csv", spark=_session(spark))
    target: Writer[DataFrame] = DiskWriter("/data/orders", "parquet")

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


def test_at028_the_connectors_write_no_log_lines(
    log: _Log,
    spark: _FakeSpark,
    src: Path,
    df: _FakeDataFrame,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    _file(src, "a.csv", at=_ns(T0))

    DiskReader(src, "csv", spark=_session(spark)).read()
    DiskReader(src, "csv", spark=_session(spark), watermark_value=T0).read()
    DiskReader(
        src, "csv", spark=_session(spark), watermark_pattern=P, watermark_value="20261020"
    ).read()
    writer = DiskWriter("/data/orders", "parquet")
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    assert len(_loads(log)) == 1
    assert len(_creates(log)) == 2
    assert len(_saves(log)) == 2
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

needs_spark = pytest.mark.skipif(
    os.environ.get("INGESTIO_SPARK_TESTS") != "1",
    reason="INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in",
)

SCHEMA = "id INT, name STRING"


@pytest.fixture(scope="module")
def spark_session() -> Iterator[SparkSession]:
    """A local Spark session in UTC."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _rows(frame: DataFrame) -> list[tuple[Any, ...]]:
    """The rows of `frame`, as sorted tuples."""
    return sorted(tuple(row) for row in frame.collect())


@pytest.mark.integration
@needs_spark
def test_parquet_appends_then_replaces_and_a_full_read_returns_the_rows(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    dst = tmp_path / "lake"
    writer = DiskWriter(dst, "parquet")

    writer.write(spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA))
    writer.write(spark_session.createDataFrame([(3, "c")], SCHEMA))

    names = [entry.name for entry in dst.iterdir()]
    assert "_SUCCESS" in names, names
    assert any(name.endswith(".crc") for name in names), names
    assert _rows(DiskReader(dst, "parquet", spark=spark_session).read()) == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]

    writer.write(spark_session.createDataFrame([(10, "x")], SCHEMA), replace=True)

    assert _rows(DiskReader(dst, "parquet", spark=spark_session).read()) == [(10, "x")]


@pytest.mark.integration
@needs_spark
def test_csv_written_with_a_header_reads_back_with_the_same_rows(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    dst = tmp_path / "csv"
    frame = spark_session.createDataFrame([(1, "a"), (2, "b")], SCHEMA)

    DiskWriter(dst, "csv", options={"header": True}).write(frame)
    back = DiskReader(
        dst, "csv", schema=SCHEMA, options={"header": True}, spark=spark_session
    ).read()

    assert back.columns == ["id", "name"]
    assert _rows(back) == [(1, "a"), (2, "b")]


@pytest.mark.integration
@needs_spark
def test_a_name_watermark_reads_a_frozen_selection_of_the_files_above_it(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    src = tmp_path / "landing"
    src.mkdir()
    _file(src, "orders_20261019.csv", text="id,name\n1,a\n")
    _file(src, "orders_20261020.csv", text="id,name\n2,b\n")
    _file(src, "orders_20261021.csv", text="id,name\n3,c\n4,d\n")

    frame = DiskReader(
        src,
        "csv",
        watermark_pattern=P,
        watermark_value="20261019",
        schema=SCHEMA,
        options={"header": True},
        spark=spark_session,
    ).read()

    assert _rows(frame) == [(2, "b"), (3, "c"), (4, "d")]
    assert frame.count() == 3
    _file(src, "orders_20261022.csv", text="id,name\n5,e\n")
    assert frame.count() == 3


@pytest.mark.integration
@needs_spark
def test_a_modification_time_watermark_matches_the_metadata_column_to_the_millisecond(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    src = tmp_path / "landing"
    src.mkdir()
    _file(src, "old.csv", at=_ns(T0), text="id,name\n1,a\n")
    _file(src, "newer.csv", at=_ns(T0 + timedelta(milliseconds=1500)), text="id,name\n2,b\n")
    _file(src, "newest.csv", at=_ns(T0) + 2_345_678_901, text="id,name\n3,c\n")

    frame = DiskReader(
        src,
        "csv",
        watermark_value=T0 + timedelta(seconds=1),
        schema=SCHEMA,
        options={"header": True},
        spark=spark_session,
    ).read()

    assert _rows(frame) == [(2, "b"), (3, "c")]
    row = frame.select(functions.max("_metadata.file_modification_time")).first()
    assert row is not None
    # Spark hands timestamps to Python as naive local times: `astimezone` reads them so.
    latest = cast(datetime, row[0]).astimezone(UTC)
    assert latest == T0 + timedelta(seconds=2, milliseconds=345)

    again = DiskReader(
        src,
        "csv",
        watermark_value=latest,
        schema=SCHEMA,
        options={"header": True},
        spark=spark_session,
    ).read()
    assert again.count() == 0


@pytest.mark.integration
@needs_spark
def test_an_empty_selection_has_the_columns_of_the_schema_or_none(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    src = tmp_path / "landing"
    src.mkdir()

    with_schema = DiskReader(src, "csv", schema=SCHEMA, spark=spark_session).read()
    without_schema = DiskReader(src, "csv", spark=spark_session).read()

    assert with_schema.columns == ["id", "name"]
    assert with_schema.count() == 0
    assert without_schema.columns == []
    assert without_schema.count() == 0


@pytest.mark.integration
@needs_spark
def test_a_write_whose_computation_fails_raises_out_of_write(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    def explode(value: int) -> int:
        raise ValueError("udf boom")

    failing = functions.udf(explode, "int")
    frame = spark_session.createDataFrame([(1,)], "id INT").select(failing("id").alias("id"))

    with pytest.raises(Exception, match="udf boom"):
        DiskWriter(tmp_path / "out", "parquet").write(frame)


@pytest.mark.integration
@needs_spark
def test_replace_removes_files_spark_did_not_write(
    tmp_path: Path, spark_session: SparkSession
) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "keep.txt").write_text("keep me\n", encoding="utf-8")

    DiskWriter(out, "parquet").write(
        spark_session.createDataFrame([(1, "a")], SCHEMA), replace=True
    )

    assert not (out / "keep.txt").exists()
    assert _rows(DiskReader(out, "parquet", spark=spark_session).read()) == [(1, "a")]
