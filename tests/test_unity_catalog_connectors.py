"""Unity Catalog connectors: `UnityCatalogReader` and `UnityCatalogWriter`.

Covers `specs/unity_catalog_connectors.md`. The tests named after an acceptance criterion always
run. They need no JVM and no catalog: they use the doubles below, a fake Spark session and fake
DataFrames, which all write to one ordered event log.

The integration tests at the end run the connectors against a local Spark session, `local[2]`,
in UTC, with Delta Lake and a warehouse directory under a temporary path. They are marked
`integration` and skipped unless `INGESTIO_SPARK_TESTS` is `1` and `INGESTIO_DELTA_PACKAGE`
holds the Maven coordinate of the Delta Lake release that matches the installed pyspark, such as
`io.delta:delta-spark_2.13:<version>`, which the session loads through `spark.jars.packages`.
They need Java 17 or 21 for the locked pyspark, and access to Maven or a filled Ivy cache. A JVM
loads its packages once, when it starts: when another test module of the same run has already
started one, without Delta Lake, these tests skip, so run this file on its own. Each test names
its own tables, `spark_catalog.default.ingestio_<stem>_<hex>`, and drops them. They were
written without being run: the machine they were written on has no Java 17 or 21.
"""

from __future__ import annotations

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
from pyspark import SparkContext
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, functions

from ingestio import Process, Reader, ReadSource, Step, Writer, WriteTarget
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter

BOUND_ROW: tuple[object, ...] = ("2026-10-21 09:30:00.5", "timestamp")
UPPER = "CAST('2026-10-21 09:30:00.5' AS timestamp)"
ORDERS = "`main`.`sales`.`orders`"
LAKE = "`main`.`lake`.`orders`"
UPDATED_AT = "`updated_at`"
MINUS_3 = timezone(timedelta(hours=-3))


# --- Test doubles -------------------------------------------------------------------------


@dataclass(frozen=True)
class _TableRead:
    """`table()` ran on a fake `spark.read` of `spark`.

    It records the `tableName` argument, the very object passed, the format or `None`, and the
    merged options. `frame` is the DataFrame that `table()` returned: the source.
    """

    name: object
    source: str | None
    options: dict[str, str]
    frame: _FakeDataFrame = field(repr=False, compare=False)
    spark: _FakeSpark = field(repr=False, compare=False)


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


@dataclass(frozen=True)
class _TableSave:
    """`saveAsTable()` ran on a fake `DataFrame.write`, by the writer of `frame`.

    It records the format, the mode, the merged options, the `name` argument, the very object
    passed, and the `partitionBy` argument of `saveAsTable()`.
    """

    source: str | None
    mode: str | None
    options: dict[str, str]
    name: object
    partition_by: object
    frame: _FakeDataFrame = field(repr=False, compare=False)


_Event: TypeAlias = _TableRead | _Where | _Select | _RowAccess | _TableSave

SAVE_CALL = "saveAsTable()"


class _Log:
    """The ordered event log every double writes to.

    `accesses` holds every attribute read on a fake DataFrame or a fake writer, with its owner,
    in order, and `saveAsTable()` when that method runs.
    """

    def __init__(self) -> None:
        self.events: list[_Event] = []
        self.spark_attributes: list[str] = []
        self.accesses: list[tuple[object, str]] = []


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

    Each access to `read` returns a new `_FakeDataFrameReader`, as PySpark does. Each table
    read returns a new `_FakeDataFrame` with `columns`, and raises `table_error` once recorded,
    when it is set. The bound queries answer `bound_rows`, as `_Answers` describes.
    """

    def __init__(
        self,
        log: _Log,
        *,
        bound_rows: Sequence[tuple[object, ...]] = (BOUND_ROW,),
        columns: Sequence[str] = (),
        table_error: BaseException | None = None,
        row_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._answers = _Answers(bound_rows, row_error)
        self._columns = list(columns)
        self._table_error = table_error

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_log").spark_attributes.append(name)
        return object.__getattribute__(self, name)

    @property
    def read(self) -> _FakeDataFrameReader:
        return _FakeDataFrameReader(
            self, self._log, self._answers, self._columns, self._table_error
        )


class _FakeDataFrameReader:
    """Stands in for `spark.read`: records a `_TableRead` each time `table()` is called."""

    def __init__(
        self,
        spark: _FakeSpark,
        log: _Log,
        answers: _Answers,
        columns: list[str],
        table_error: BaseException | None,
    ) -> None:
        self._spark = spark
        self._log = log
        self._answers = answers
        self._columns = columns
        self._table_error = table_error
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

    def table(self, tableName: object) -> _FakeDataFrame:  # noqa: N803 - PySpark's name
        frame = _FakeDataFrame(self._log, columns=self._columns, answers=self._answers)
        self._log.events.append(
            _TableRead(tableName, self._source, dict(self._options), frame, self._spark)
        )
        if self._table_error is not None:
            raise self._table_error
        return frame


class _FakeDataFrame:
    """Stands in for a `DataFrame`, and records every attribute accessed on it.

    `where`, `filter` and `selectExpr` record their call and return a new `_FakeDataFrame`. The
    DataFrame that `selectExpr` returns answers `first()`, `head()`, `collect()` and `take(n)`
    with the single row its session chose; every row access is recorded. Each access to
    `write` returns a new `_FakeDataFrameWriter`, kept in `_writers`, whose `saveAsTable()`
    raises `save_error` once recorded, when it is set.
    """

    def __init__(
        self,
        log: _Log,
        *,
        columns: Sequence[str] = (),
        answers: _Answers | None = None,
        bound_row: tuple[object, ...] | None = None,
        save_error: BaseException | None = None,
    ) -> None:
        self._log = log
        self._columns = list(columns)
        self._answers = answers
        self._bound_row = bound_row
        self._save_error = save_error
        self._accessed: list[str] = []
        self._writers: list[_FakeDataFrameWriter] = []

    def __getattribute__(self, name: str) -> Any:
        if not name.startswith("_"):
            object.__getattribute__(self, "_accessed").append(name)
            object.__getattribute__(self, "_log").accesses.append((self, name))
        return object.__getattribute__(self, name)

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    @property
    def write(self) -> _FakeDataFrameWriter:
        writer = _FakeDataFrameWriter(self._log, self, self._save_error)
        self._writers.append(writer)
        return writer

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


class _FakeDataFrameWriter:
    """Stands in for `DataFrame.write`, and records every attribute accessed on it.

    `saveAsTable()` records a `_TableSave`, then raises `save_error` when it is set. The other
    methods of Spark's writer are here so that an access to them shows in `_accessed`.
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
            object.__getattribute__(self, "_log").accesses.append((self, name))
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

    def mode(self, saveMode: str | None) -> _FakeDataFrameWriter:  # noqa: N803 - PySpark's name
        self._mode = saveMode
        return self

    def saveAsTable(  # noqa: N802 - PySpark's name
        self,
        name: object,
        format: str | None = None,  # noqa: A002 - PySpark's name
        mode: str | None = None,
        partitionBy: object = None,  # noqa: N803 - PySpark's name
        **options: object,
    ) -> None:
        self._log.accesses.append((self, SAVE_CALL))
        if format is not None:
            self._source = format
        if mode is not None:
            self._mode = mode
        for key, value in options.items():
            _set_option(self._options, key, value)
        self._log.events.append(
            _TableSave(
                self._source, self._mode, dict(self._options), name, partitionBy, self._frame
            )
        )
        if self._save_error is not None:
            raise self._save_error

    def insertInto(self, tableName: object, overwrite: bool | None = None) -> None:  # noqa: N802, N803
        """Never called by the writer: an access shows in `_accessed`."""

    def save(self, path: object = None, **options: object) -> None:
        """Never called by the writer: an access shows in `_accessed`."""

    def partitionBy(self, *cols: object) -> _FakeDataFrameWriter:  # noqa: N802 - PySpark's name
        return self

    def bucketBy(self, numBuckets: int, col: object, *cols: object) -> _FakeDataFrameWriter:  # noqa: N802, N803
        return self

    def sortBy(self, col: object, *cols: object) -> _FakeDataFrameWriter:  # noqa: N802
        return self

    def clusterBy(self, *cols: object) -> _FakeDataFrameWriter:  # noqa: N802 - PySpark's name
        return self


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
    """The fake session of the acceptance criteria: its bound query answers `BOUND_ROW`."""
    return _FakeSpark(log)


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


def _exactly(message: str) -> str:
    """A `match` pattern for exactly `message`."""
    return f"^{re.escape(message)}$"


def _lower(options: Mapping[str, str]) -> dict[str, str]:
    """`options` with lowercase keys, to compare keys case-insensitively."""
    return {key.lower(): value for key, value in options.items()}


def _table_reads(log: _Log) -> list[_TableRead]:
    """The table reads recorded, in order."""
    return [event for event in log.events if isinstance(event, _TableRead)]


def _wheres(log: _Log) -> list[_Where]:
    """The wheres recorded, in order."""
    return [event for event in log.events if isinstance(event, _Where)]


def _selects(log: _Log) -> list[_Select]:
    """The selects recorded, in order."""
    return [event for event in log.events if isinstance(event, _Select)]


def _saves(log: _Log) -> list[_TableSave]:
    """The table saves recorded, in order."""
    return [event for event in log.events if isinstance(event, _TableSave)]


def _kinds(log: _Log) -> list[str]:
    """The events as words: `table`, `where`, `select`, the row access's method, `save`."""
    kinds: list[str] = []
    for event in log.events:
        if isinstance(event, _TableRead):
            kinds.append("table")
        elif isinstance(event, _Where):
            kinds.append("where")
        elif isinstance(event, _Select):
            kinds.append("select")
        elif isinstance(event, _RowAccess):
            kinds.append(event.method)
        else:
            kinds.append("save")
    return kinds


def _attributes(fake: _FakeDataFrame | _FakeDataFrameWriter) -> list[str]:
    """The attributes accessed on `fake`, in order."""
    return fake._accessed


def _writers(frame: _FakeDataFrame) -> list[_FakeDataFrameWriter]:
    """The fake writers that the accesses to `frame.write` returned, in order."""
    return frame._writers


def _bound_expressions(column: str) -> list[str]:
    """The two expressions of the bound query over the quoted `column`, in order."""
    return [
        f"CAST(max({column}) AS STRING) AS upper_bound",
        f"typeof(max({column})) AS upper_bound_type",
    ]


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


WriteCall: TypeAlias = Callable[[UnityCatalogWriter, DataFrame], object]

APPEND_CALLS: dict[str, WriteCall] = {
    "write(df)": lambda writer, frame: writer.write(frame),
    "write(df, replace=False)": lambda writer, frame: writer.write(frame, replace=False),
}

EVERY_CALL: dict[str, WriteCall] = {
    **APPEND_CALLS,
    "write(df, replace=True)": lambda writer, frame: writer.write(frame, replace=True),
}

BUILDERS = pytest.mark.parametrize(
    "build", [UnityCatalogReader, UnityCatalogWriter], ids=["reader", "writer"]
)


# --- Import surface -----------------------------------------------------------------------

AT001_CODE = """
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter

import ingestio
import ingestio.connector.all as connectors
import ingestio.connector.unity_catalog as unity_catalog

names = {"UnityCatalogReader", "UnityCatalogWriter"}
assert names <= set(connectors.__all__), connectors.__all__
assert names <= set(unity_catalog.__all__), unity_catalog.__all__
assert not names & set(ingestio.__all__), ingestio.__all__
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
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter

reads = []
saves = []


class _FakeDataFrameReader:
    def format(self, source):
        return self

    def option(self, key, value):
        return self

    def options(self, **options):
        return self

    def table(self, tableName):
        reads.append(object())
        return reads[-1]


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

    def mode(self, saveMode):
        return self

    def saveAsTable(self, name, format=None, mode=None, partitionBy=None, **options):
        saves.append(name)


class _FakeDataFrame:
    columns = ["id", "payload"]

    @property
    def write(self):
        return _FakeDataFrameWriter()


reader = UnityCatalogReader("main.sales.orders", spark=_FakeSpark())
writer = UnityCatalogWriter("main.lake.orders")
frame = reader.read()
assert len(reads) == 1, reads
assert frame is reads[0], frame
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


@BUILDERS
@pytest.mark.parametrize(
    "name",
    ["", "orders", "sales.orders", "a.b.c.d", ".sales.orders", "main..orders", "main.sales."],
)
def test_at003_a_table_name_other_than_three_parts_is_rejected(
    build: type[UnityCatalogReader] | type[UnityCatalogWriter], name: str
) -> None:
    with pytest.raises(ValueError, match=_exactly("table_name must be 'catalog.schema.table'")):
        build(name)


@BUILDERS
@pytest.mark.parametrize(
    "name", ["main.sales.orders", "spark_catalog.default.orders", "My-Catalog.Sales.Order Items"]
)
def test_at003_a_three_part_name_is_accepted(
    build: type[UnityCatalogReader] | type[UnityCatalogWriter], name: str
) -> None:
    build(name)


def test_at004_a_watermark_value_needs_a_watermark_column() -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_value needs watermark_column")):
        UnityCatalogReader("main.sales.orders", watermark_value=41)


def test_at004_an_empty_watermark_column_is_rejected() -> None:
    with pytest.raises(ValueError, match=_exactly("watermark_column must not be empty")):
        UnityCatalogReader("main.sales.orders", watermark_column="")


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
def test_at005_unsupported_watermark_values_are_rejected(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=_exactly(message)):
        UnityCatalogReader(
            "main.sales.orders",
            watermark_column="updated_at",
            watermark_value=cast(Any, value),
        )


@pytest.mark.parametrize(
    "value",
    [0, "", Decimal("12.50"), date(2026, 10, 1), datetime(2026, 10, 1, tzinfo=UTC)],
    ids=["zero", "empty-string", "decimal", "date", "aware-datetime"],
)
def test_at005_supported_watermark_values_are_accepted(value: int | Decimal | str | date) -> None:
    UnityCatalogReader("main.sales.orders", watermark_column="updated_at", watermark_value=value)


@pytest.mark.parametrize("key", ["path", "PATH", "readChangeFeed", "readchangedata"])
def test_at006_options_the_reader_reserves_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        UnityCatalogReader("main.sales.orders", options={key: "x"})


@pytest.mark.parametrize("key", ["path", "Path", "replaceWhere", "PartitionOverwriteMode"])
def test_at006_options_the_writer_reserves_are_rejected_in_any_case(key: str) -> None:
    with pytest.raises(ValueError, match=_exactly(f"option '{key}' is set by the connector")):
        UnityCatalogWriter("main.lake.orders", options={key: "x"})


@pytest.mark.parametrize(
    "options",
    [{"versionAsOf": 3}, {"timestampAsOf": "2026-10-01"}],
    ids=["version", "timestamp"],
)
def test_at006_time_travel_options_are_accepted_by_the_reader(
    options: dict[str, str | int | float | bool],
) -> None:
    UnityCatalogReader("main.sales.orders", options=options)


@pytest.mark.parametrize(
    "options",
    [
        {"mergeSchema": True},
        {"overwriteSchema": "true"},
        {"userMetadata": "run 42", "txnAppId": "orders", "txnVersion": 7},
    ],
    ids=["merge-schema", "overwrite-schema", "metadata-and-idempotence"],
)
def test_at006_delta_write_options_are_accepted_by_the_writer(
    options: dict[str, str | int | float | bool],
) -> None:
    UnityCatalogWriter("main.lake.orders", options=options)


def test_at007_building_touches_neither_the_session_nor_the_active_session_lookup(
    log: _Log, spark: _FakeSpark, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("active")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)

    UnityCatalogReader("main.sales.orders")
    UnityCatalogReader("main.sales.orders", watermark_column="updated_at", watermark_value=41)
    UnityCatalogReader("main.sales.orders", spark=_session(spark))
    UnityCatalogReader("main.sales.orders", spark=_session(spark), watermark_column="updated_at")
    UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    )
    UnityCatalogWriter("main.lake.orders")

    assert lookups == []
    assert log.spark_attributes == []
    assert log.events == []


def test_at008_repr_shows_the_table_name_as_given() -> None:
    reader_repr = "UnityCatalogReader(table_name='main.sales.orders')"
    assert repr(UnityCatalogReader("main.sales.orders")) == reader_repr
    assert (
        repr(
            UnityCatalogReader(
                "main.sales.orders",
                watermark_column="updated_at",
                watermark_value=41,
                options={"versionAsOf": 3},
            )
        )
        == reader_repr
    )
    writer_repr = "UnityCatalogWriter(table_name='main.lake.orders')"
    assert repr(UnityCatalogWriter("main.lake.orders")) == writer_repr
    assert repr(UnityCatalogWriter("main.lake.orders", options={"mergeSchema": True})) == (
        writer_repr
    )


# --- Full read ----------------------------------------------------------------------------


def test_at009_a_full_read_returns_the_table_of_the_quoted_name_unchanged(
    log: _Log, spark: _FakeSpark
) -> None:
    frame = UnityCatalogReader("main.sales.orders", spark=_session(spark)).read()

    assert _kinds(log) == ["table"]
    (read,) = _table_reads(log)
    assert read.name == ORDERS
    assert read.options == {}
    assert frame is _frame(read.frame)
    assert _attributes(read.frame) == []
    assert set(log.spark_attributes) == {"read"}


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at010_read_options_reach_the_table_read_as_given(
    watermark: bool, log: _Log, spark: _FakeSpark
) -> None:
    UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at" if watermark else None,
        options={"versionAsOf": 3, "IgnoreCorruptFiles": True},
    ).read()

    (read,) = _table_reads(log)
    assert _lower(read.options) == {"versionasof": "3", "ignorecorruptfiles": "true"}
    assert "IgnoreCorruptFiles" in read.options, read.options


def test_at011_the_active_session_is_looked_up_at_each_read(
    log: _Log, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookups: list[str] = []

    def no_session() -> SparkSession:
        lookups.append("no session")
        raise RuntimeError("no active SparkSession")

    monkeypatch.setattr(SparkSession, "active", no_session)
    reader = UnityCatalogReader("main.sales.orders")
    assert lookups == []

    first, second = _FakeSpark(log), _FakeSpark(log)
    monkeypatch.setattr(SparkSession, "active", lambda: _session(first))
    first_frame = reader.read()
    monkeypatch.setattr(SparkSession, "active", lambda: _session(second))
    second_frame = reader.read()

    first_read, second_read = _table_reads(log)
    assert first_read.spark is first
    assert second_read.spark is second
    assert first_frame is _frame(first_read.frame)
    assert second_frame is _frame(second_read.frame)
    assert first_read.name == second_read.name == ORDERS


def test_at011_without_an_active_session_the_error_of_pyspark_propagates(
    log: _Log, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = UnityCatalogReader("main.sales.orders")
    error = RuntimeError("no active SparkSession")

    def no_session() -> SparkSession:
        raise error

    monkeypatch.setattr(SparkSession, "active", no_session)

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is error
    assert log.events == []


@pytest.mark.parametrize(
    ("table_name", "column", "quoted_table", "quoted_column"),
    [
        pytest.param("main.sales.orders", "updated_at", ORDERS, UPDATED_AT, id="plain-names"),
        pytest.param(
            "My-Catalog.Sales.Order Items",
            "Updated At",
            "`My-Catalog`.`Sales`.`Order Items`",
            "`Updated At`",
            id="special-characters",
        ),
        pytest.param("a`b.c.d", "my`col", "`a``b`.`c`.`d`", "`my``col`", id="backticks"),
    ],
)
def test_at012_table_and_column_names_are_always_quoted(
    table_name: str,
    column: str,
    quoted_table: str,
    quoted_column: str,
    log: _Log,
    spark: _FakeSpark,
    df: _FakeDataFrame,
) -> None:
    UnityCatalogReader(table_name, spark=_session(spark)).read()
    UnityCatalogReader(
        table_name, spark=_session(spark), watermark_column=column, watermark_value=1
    ).read()
    UnityCatalogWriter(table_name).write(_frame(df))

    full, watermark = _table_reads(log)
    assert full.name == quoted_table
    assert watermark.name == quoted_table
    lower, upper = _wheres(log)
    assert lower.condition == f"{quoted_column} > 1"
    (select,) = _selects(log)
    assert list(select.expressions) == _bound_expressions(quoted_column)
    assert upper.condition == f"{quoted_column} > 1 AND {quoted_column} <= {UPPER}"
    (save,) = _saves(log)
    assert save.name == quoted_table


# --- Watermark read -----------------------------------------------------------------------


def test_at013_a_watermark_read_captures_a_bound_then_slices_the_source(
    log: _Log, spark: _FakeSpark
) -> None:
    frame = UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()

    assert _kinds(log) == ["table", "where", "select", "first", "where"]
    read, lower, select, first, upper = log.events
    assert isinstance(read, _TableRead)
    assert isinstance(lower, _Where)
    assert isinstance(select, _Select)
    assert isinstance(first, _RowAccess)
    assert isinstance(upper, _Where)

    assert read.name == ORDERS
    assert read.options == {}
    source = read.frame
    assert lower.on is source
    assert lower.condition == "`updated_at` > '2026-10-01 00:00:00+00:00'"
    assert select.on is lower.result
    assert list(select.expressions) == _bound_expressions(UPDATED_AT)
    assert first.on is select.result
    assert upper.on is source
    assert upper.condition == (
        "`updated_at` > '2026-10-01 00:00:00+00:00'"
        " AND `updated_at` <= CAST('2026-10-21 09:30:00.5' AS timestamp)"
    )
    assert frame is _frame(upper.result)
    assert _attributes(upper.result) == []


def test_at014_without_a_watermark_value_the_bound_covers_the_whole_source(
    log: _Log, spark: _FakeSpark
) -> None:
    frame = UnityCatalogReader(
        "main.sales.orders", spark=_session(spark), watermark_column="updated_at"
    ).read()

    assert _kinds(log) == ["table", "select", "first", "where"]
    (read,) = _table_reads(log)
    (select,) = _selects(log)
    assert select.on is read.frame
    assert list(select.expressions) == _bound_expressions(UPDATED_AT)
    (where,) = _wheres(log)
    assert where.on is read.frame
    assert where.condition == f"`updated_at` <= {UPPER}"
    assert frame is _frame(where.result)


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
def test_at015_without_a_bound_the_slice_is_the_source_filtered_with_false(
    value: int | None, log: _Log
) -> None:
    spark = _FakeSpark(log, bound_rows=[(None, "timestamp")])

    frame = UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=value,
    ).read()

    (read,) = _table_reads(log)
    last = _wheres(log)[-1]
    assert _kinds(log)[-1] == "where"
    assert last.on is read.frame
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
def test_at016_watermark_values_become_literals_by_type(
    value: int | Decimal | str | date, literal: str, log: _Log, spark: _FakeSpark
) -> None:
    UnityCatalogReader(
        "main.sales.orders", spark=_session(spark), watermark_column="w", watermark_value=value
    ).read()

    assert _kinds(log) == ["table", "where", "select", "first", "where"]
    lower, upper = _wheres(log)
    assert lower.condition == f"`w` > {literal}"
    assert upper.condition == f"`w` > {literal} AND `w` <= {UPPER}"


@pytest.mark.parametrize(
    ("bound_row", "condition"),
    [
        pytest.param(
            ("12.50", "decimal(10,2)"), "`w` <= CAST('12.50' AS decimal(10,2))", id="decimal"
        ),
        pytest.param(
            ("2026-10-21 09:30:00.123456", "timestamp_ntz"),
            "`w` <= CAST('2026-10-21 09:30:00.123456' AS timestamp_ntz)",
            id="timestamp-ntz",
        ),
        pytest.param(("it's", "string"), r"`w` <= CAST('it\'s' AS string)", id="single-quote"),
        pytest.param(("a\\b", "string"), r"`w` <= CAST('a\\b' AS string)", id="backslash"),
    ],
)
def test_at017_the_bound_is_cast_back_to_the_type_spark_named(
    bound_row: tuple[object, ...], condition: str, log: _Log
) -> None:
    spark = _FakeSpark(log, bound_rows=[bound_row])

    UnityCatalogReader("main.sales.orders", spark=_session(spark), watermark_column="w").read()

    assert _wheres(log)[-1].condition == condition


def test_at018_each_read_reads_the_table_again_and_captures_its_own_bound(log: _Log) -> None:
    spark = _FakeSpark(log, bound_rows=[BOUND_ROW, ("2026-10-22 10:00:00", "timestamp")])
    reader = UnityCatalogReader(
        "main.sales.orders", spark=_session(spark), watermark_column="updated_at"
    )

    first = reader.read()
    second = reader.read()

    assert _kinds(log) == ["table", "select", "first", "where"] * 2
    first_read, second_read = _table_reads(log)
    first_where, second_where = _wheres(log)
    assert first_where.on is first_read.frame
    assert first_where.condition == f"`updated_at` <= {UPPER}"
    assert second_where.on is second_read.frame
    assert second_where.condition == "`updated_at` <= CAST('2026-10-22 10:00:00' AS timestamp)"
    assert first is _frame(first_where.result)
    assert second is _frame(second_where.result)
    assert first is not second


@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_at019_a_failed_table_read_propagates_and_nothing_runs_after_it(
    watermark: bool, log: _Log
) -> None:
    err = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
    spark = _FakeSpark(log, table_error=err)
    reader = UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at" if watermark else None,
        watermark_value=41 if watermark else None,
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    assert _kinds(log) == ["table"]


@pytest.mark.parametrize("value", [None, 41], ids=["without-value", "with-value"])
def test_at019_a_failed_bound_query_propagates_and_nothing_is_filtered_after_it(
    value: int | None, log: _Log
) -> None:
    err = RuntimeError("DELTA_MISSING_TRANSACTION_LOG")
    spark = _FakeSpark(log, row_error=err)
    reader = UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=value,
    )

    with pytest.raises(RuntimeError) as excinfo:
        reader.read()
    assert excinfo.value is err
    kinds = _kinds(log)
    assert kinds.count("select") == 1, kinds
    assert "where" not in kinds[kinds.index("select") :], kinds


# --- Writing ------------------------------------------------------------------------------


def _assert_one_chain(frame: _FakeDataFrame) -> None:
    """Only `columns` and `write` were read from `frame`, and its writers saved once."""
    assert set(_attributes(frame)) <= {"columns", "write"}, _attributes(frame)
    accessed = [name for writer in _writers(frame) for name in _attributes(writer)]
    assert set(accessed) <= {"format", "option", "options", "mode", "saveAsTable"}, accessed
    assert accessed.count("saveAsTable") == 1, accessed


@pytest.mark.parametrize("call", list(APPEND_CALLS.values()), ids=list(APPEND_CALLS))
def test_at020_write_appends_to_the_quoted_table_with_one_call_chain(
    call: WriteCall, log: _Log, df: _FakeDataFrame
) -> None:
    result = call(UnityCatalogWriter("main.lake.orders"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "delta"
    assert save.mode == "append"
    assert save.options == {}
    assert save.name == LAKE
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)


def test_at021_write_with_replace_overwrites_with_static_partition_overwrite(
    log: _Log, df: _FakeDataFrame
) -> None:
    replace = EVERY_CALL["write(df, replace=True)"]

    result = replace(UnityCatalogWriter("main.lake.orders"), _frame(df))

    assert result is None
    (save,) = _saves(log)
    assert save.source == "delta"
    assert save.mode == "overwrite"
    assert save.options == {"partitionOverwriteMode": "static"}
    assert save.name == LAKE
    assert save.partition_by is None
    assert save.frame is df
    _assert_one_chain(df)


def test_at022_write_options_reach_the_save_as_given(log: _Log, df: _FakeDataFrame) -> None:
    writer = UnityCatalogWriter(
        "main.lake.orders", options={"mergeSchema": True, "UserMetadata": "run 42"}
    )

    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    append, replace = _saves(log)
    assert _lower(append.options) == {"mergeschema": "true", "usermetadata": "run 42"}
    assert "UserMetadata" in append.options, append.options
    assert _lower(replace.options) == {
        "mergeschema": "true",
        "usermetadata": "run 42",
        "partitionoverwritemode": "static",
    }
    assert (append.mode, replace.mode) == ("append", "overwrite")


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at023_a_dataframe_without_columns_is_rejected_before_anything_is_written(
    call: WriteCall, log: _Log
) -> None:
    frame = _FakeDataFrame(log, columns=[])

    with pytest.raises(ValueError, match=_exactly("the DataFrame has no columns")):
        call(UnityCatalogWriter("main.lake.orders"), _frame(frame))

    assert "write" not in _attributes(frame), _attributes(frame)
    assert _writers(frame) == []
    assert _saves(log) == []


@pytest.mark.parametrize("call", list(EVERY_CALL.values()), ids=list(EVERY_CALL))
def test_at024_a_failed_save_propagates_unchanged_and_nothing_runs_after_it(
    call: WriteCall, log: _Log
) -> None:
    err = RuntimeError("DELTA_FAILED_TO_MERGE_FIELDS")
    frame = _FakeDataFrame(log, columns=["id", "payload"], save_error=err)

    with pytest.raises(RuntimeError) as excinfo:
        call(UnityCatalogWriter("main.lake.orders"), _frame(frame))

    assert excinfo.value is err
    assert len(_saves(log)) == 1
    calls = [index for index, (_, name) in enumerate(log.accesses) if name == SAVE_CALL]
    assert len(calls) == 1, calls
    writers = _writers(frame)
    after = [
        name
        for owner, name in log.accesses[calls[0] + 1 :]
        if owner is frame or any(owner is writer for writer in writers)
    ]
    assert after == []


# --- Composition --------------------------------------------------------------------------


def test_at025_read_source_and_write_target_move_a_dataframe_and_append(log: _Log) -> None:
    spark = _FakeSpark(log, columns=["id", "payload"])
    source: Reader[DataFrame] = UnityCatalogReader("main.sales.orders", spark=_session(spark))
    target: Writer[DataFrame] = UnityCatalogWriter("main.lake.orders")

    process = Process(
        [
            Step("extract", [ReadSource(source, into="orders")]),
            Step("load", [WriteTarget(target, key="orders")]),
        ]
    ).run()

    (read,) = _table_reads(log)
    assert process.state["orders"] is _frame(read.frame)
    (save,) = _saves(log)
    assert save.frame is read.frame
    assert save.mode == "append"
    assert save.name == LAKE


def test_at026_the_connectors_write_no_log_lines(
    log: _Log, spark: _FakeSpark, df: _FakeDataFrame, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.NOTSET)
    caplog.set_level(logging.NOTSET, logger="ingestio")
    empty = _FakeSpark(log, bound_rows=[(None, "timestamp")])

    UnityCatalogReader("main.sales.orders", spark=_session(spark)).read()
    UnityCatalogReader(
        "main.sales.orders",
        spark=_session(spark),
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    ).read()
    UnityCatalogReader("main.sales.orders", spark=_session(empty), watermark_column="w").read()
    writer = UnityCatalogWriter("main.lake.orders", options={"mergeSchema": True})
    writer.write(_frame(df))
    writer.write(_frame(df), replace=True)

    assert len(_table_reads(log)) == 3
    assert len(_saves(log)) == 2
    assert caplog.records == []


# --- Integration --------------------------------------------------------------------------

DELTA_PACKAGE = os.environ.get("INGESTIO_DELTA_PACKAGE", "")
SCHEMA = "id INT, name STRING"

needs_spark = pytest.mark.skipif(
    os.environ.get("INGESTIO_SPARK_TESTS") != "1",
    reason="INGESTIO_SPARK_TESTS is not 1: the tests against a local Spark session are opt-in",
)
needs_delta = pytest.mark.skipif(
    not DELTA_PACKAGE,
    reason=(
        "INGESTIO_DELTA_PACKAGE is not set: it names the Delta Lake package that matches the"
        " installed pyspark, such as io.delta:delta-spark_2.13:<version>"
    ),
)


@pytest.fixture(scope="module")
def delta(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    """A local Spark session in UTC, with Delta Lake and a warehouse of its own."""
    if SparkContext._gateway is not None:
        pytest.skip(
            "a JVM was already started in this run, without Delta Lake: a JVM loads its"
            " packages once, so run this file on its own"
        )
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ingestio-integration-delta")
        .config("spark.jars.packages", DELTA_PACKAGE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
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
def names(delta: SparkSession) -> Iterator[Callable[[str], str]]:
    """Unique names in `spark_catalog.default`; the tables and views they name are dropped."""
    created: list[str] = []

    def name(stem: str) -> str:
        full = f"spark_catalog.default.ingestio_{stem}_{uuid.uuid4().hex[:12]}"
        created.append(full)
        return full

    yield name
    for full in reversed(created):
        if not delta.catalog.tableExists(full):
            continue
        kind = "VIEW" if delta.catalog.getTable(full).tableType == "VIEW" else "TABLE"
        delta.sql(f"DROP {kind} IF EXISTS {full}")


def _rows(frame: DataFrame) -> list[tuple[Any, ...]]:
    """The rows of `frame`, as tuples sorted by their first value, a unique id."""
    return sorted((tuple(row) for row in frame.collect()), key=lambda row: int(row[0]))


def _ids(frame: DataFrame) -> list[int]:
    """The sorted values of the `id` column of `frame`."""
    return sorted(int(row["id"]) for row in frame.collect())


def _version(spark: SparkSession, name: str) -> int:
    """The latest version in the Delta history of the table `name`."""
    row = spark.sql(f"DESCRIBE HISTORY {name}").agg(functions.max("version")).first()
    assert row is not None
    return int(row[0])


def _seed(
    spark: SparkSession,
    name: str,
    column: str,
    column_type: str,
    rows: Sequence[tuple[int, str | None]],
    *,
    mode: str = "errorifexists",
) -> None:
    """Write `rows` to the Delta table `name` with plain Spark: `id`, and `column` cast from
    its text to `column_type`."""
    (
        spark.createDataFrame(list(rows), f"id INT, {column} STRING")
        .selectExpr("id", f"CAST({column} AS {column_type}) AS {column}")
        .write.format("delta")
        .mode(mode)
        .saveAsTable(name)
    )


@pytest.mark.integration
@needs_delta
@needs_spark
def test_write_creates_a_missing_table_as_a_minimal_managed_delta_table(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("created")
    writer = UnityCatalogWriter(name)

    writer.write(delta.createDataFrame([(1, "a"), (2, "b")], SCHEMA))

    detail = delta.sql(f"DESCRIBE DETAIL {name}").first()
    assert detail is not None
    assert detail["format"] == "delta"
    assert list(detail["partitionColumns"]) == []
    extended = {
        str(row["col_name"]).strip(): str(row["data_type"]).strip()
        for row in delta.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
    }
    assert extended.get("Type") == "MANAGED", extended
    created = delta.table(name)
    assert created.columns == ["id", "name"]
    assert _rows(created) == [(1, "a"), (2, "b")]

    writer.write(delta.createDataFrame([("c", 3)], "name STRING, id INT"))

    assert _rows(delta.table(name)) == [(1, "a"), (2, "b"), (3, "c")]

    empty_name = names("created_empty")
    UnityCatalogWriter(empty_name).write(delta.createDataFrame([], SCHEMA))

    empty = delta.table(empty_name)
    assert empty.columns == ["id", "name"]
    assert empty.count() == 0


@pytest.mark.integration
@needs_delta
@needs_spark
def test_write_with_replace_leaves_exactly_the_rows_of_the_dataframe_in_a_new_version(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("replaced")
    writer = UnityCatalogWriter(name)
    writer.write(delta.createDataFrame([(1, "a"), (2, "b")], SCHEMA))
    before = _version(delta, name)

    writer.write(delta.createDataFrame([(10, "x")], SCHEMA), replace=True)

    assert _rows(delta.table(name)) == [(10, "x")]
    assert _version(delta, name) == before + 1
    previous = UnityCatalogReader(name, options={"versionAsOf": before}, spark=delta).read()
    assert _rows(previous) == [(1, "a"), (2, "b")]

    writer.write(delta.createDataFrame([(20,)], "id INT"), replace=True)

    replaced = delta.table(name)
    assert replaced.columns == ["id", "name"]
    assert _rows(replaced) == [(20, None)]


@pytest.mark.integration
@needs_delta
@needs_spark
def test_schema_changes_need_the_delta_options(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("enforced")
    UnityCatalogWriter(name).write(delta.createDataFrame([(1, "a")], SCHEMA))
    version = _version(delta, name)
    wider = delta.createDataFrame([(2, "b", "x")], "id INT, name STRING, extra STRING")

    with pytest.raises(AnalysisException):
        UnityCatalogWriter(name).write(wider)
    assert _version(delta, name) == version

    UnityCatalogWriter(name, options={"mergeSchema": True}).write(wider)
    merged = delta.table(name)
    assert merged.columns == ["id", "name", "extra"]
    assert _rows(merged) == [(1, "a", None), (2, "b", "x")]

    other = delta.createDataFrame([(7, 1.5)], "id INT, value DOUBLE")
    version = _version(delta, name)
    with pytest.raises(AnalysisException):
        UnityCatalogWriter(name).write(other, replace=True)
    assert _version(delta, name) == version

    UnityCatalogWriter(name, options={"overwriteSchema": True}).write(other, replace=True)
    overwritten = delta.table(name)
    assert overwritten.columns == ["id", "value"]
    assert _rows(overwritten) == [(7, 1.5)]


@pytest.mark.integration
@needs_delta
@needs_spark
def test_replace_on_a_partitioned_table_replaces_every_partition_despite_dynamic_mode(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("partitioned")
    delta.sql(f"CREATE TABLE {name} (id INT, day STRING) USING delta PARTITIONED BY (day)")
    delta.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b')")
    delta.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        UnityCatalogWriter(name).write(
            delta.createDataFrame([(3, "a")], "id INT, day STRING"), replace=True
        )
    finally:
        delta.conf.unset("spark.sql.sources.partitionOverwriteMode")

    assert _rows(delta.table(name)) == [(3, "a")]
    detail = delta.sql(f"DESCRIBE DETAIL {name}").first()
    assert detail is not None
    assert list(detail["partitionColumns"]) == ["day"]


@pytest.mark.integration
@needs_delta
@needs_spark
def test_a_write_whose_computation_fails_leaves_the_catalog_as_it_was(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    def explode(value: int) -> int:
        raise ValueError("udf boom")

    failing = functions.udf(explode, "int")
    bad = delta.createDataFrame([(1, "a")], SCHEMA).select(failing("id").alias("id"), "name")
    existing = names("existing")
    UnityCatalogWriter(existing).write(delta.createDataFrame([(1, "a")], SCHEMA))
    version = _version(delta, existing)

    for replace in (False, True):
        with pytest.raises(Exception, match="udf boom"):
            UnityCatalogWriter(existing).write(bad, replace=replace)
        assert _version(delta, existing) == version
    assert _rows(delta.table(existing)) == [(1, "a")]

    missing = names("never_created")
    with pytest.raises(Exception, match="udf boom"):
        UnityCatalogWriter(missing).write(bad)
    assert not delta.catalog.tableExists(missing)


@pytest.mark.integration
@needs_delta
@needs_spark
def test_a_watermark_read_of_a_timestamp_column_returns_a_frozen_slice(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("events")
    _seed(
        delta,
        name,
        "updated_at",
        "TIMESTAMP",
        [
            (1, "2026-10-01 00:00:00"),
            (2, "2026-10-02 12:00:00.123456"),
            (3, "2026-10-03 00:00:00.654321"),
            (4, None),
        ],
    )

    # 21:00 at -03:00 is 2026-10-02 00:00:00 UTC.
    frame = UnityCatalogReader(
        name,
        watermark_column="updated_at",
        watermark_value=datetime(2026, 10, 1, 21, 0, tzinfo=MINUS_3),
        spark=delta,
    ).read()

    assert _ids(frame) == [2, 3]
    assert frame.count() == 2
    _seed(
        delta,
        name,
        "updated_at",
        "TIMESTAMP",
        [(5, "2026-10-04 00:00:00"), (6, None)],
        mode="append",
    )
    assert frame.count() == 2
    assert _ids(frame) == [2, 3]

    first_run = UnityCatalogReader(name, watermark_column="updated_at", spark=delta).read()
    assert _ids(first_run) == [1, 2, 3, 5]


@pytest.mark.integration
@needs_delta
@needs_spark
@pytest.mark.parametrize(
    ("column_type", "values", "value", "selected"),
    [
        pytest.param("BIGINT", ["10", "20", "9007199254740993"], 10, [2, 3], id="bigint-int"),
        pytest.param(
            "DECIMAL(38,18)",
            [
                "1.000000000000000001",
                "1.000000000000000002",
                "12345678901234567890.123456789012345678",
            ],
            Decimal("1.000000000000000001"),
            [2, 3],
            id="decimal-decimal",
        ),
        pytest.param(
            "STRING", ["A", "O'Brien", "O'Brien\\x", "Z\\it's"], "O'Brien", [3, 4], id="string-str"
        ),
        pytest.param(
            "DATE", ["2026-10-01", "2026-10-02", "2026-10-03"], date(2026, 10, 1), [2, 3], id="date"
        ),
        pytest.param(
            "TIMESTAMP_NTZ",
            ["2026-10-01 11:30:00", "2026-10-01 11:30:00.000001", "2026-10-02 08:00:00.654321"],
            # 08:30 at -03:00 is compared as the UTC wall clock, 11:30.
            datetime(2026, 10, 1, 8, 30, tzinfo=MINUS_3),
            [2, 3],
            id="timestamp-ntz-aware-datetime",
        ),
        pytest.param(
            "TIMESTAMP_NTZ",
            ["2026-10-01 11:30:00", "2026-10-01 11:30:00.000001", "2026-10-02 08:00:00.654321"],
            "2026-10-01 11:30:00",
            [2, 3],
            id="timestamp-ntz-str",
        ),
    ],
)
def test_a_watermark_read_slices_every_supported_column_type_exactly(
    column_type: str,
    values: list[str],
    value: int | Decimal | str | date,
    selected: list[int],
    delta: SparkSession,
    names: Callable[[str], str],
) -> None:
    name = names("typed")
    rows: list[tuple[int, str | None]] = list(enumerate(values, start=1))
    rows.append((len(values) + 1, None))
    _seed(delta, name, "w", column_type, rows)

    frame = UnityCatalogReader(
        name, watermark_column="w", watermark_value=value, spark=delta
    ).read()

    assert _ids(frame) == selected
    assert len(values) in selected
    first_run = UnityCatalogReader(name, watermark_column="w", spark=delta).read()
    assert _ids(first_run) == list(range(1, len(values) + 1))


@pytest.mark.integration
@needs_delta
@needs_spark
def test_a_watermark_read_of_a_view_returns_the_rows_of_its_table(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    base, view = names("base"), names("view")
    _seed(
        delta,
        base,
        "updated_at",
        "TIMESTAMP",
        [(1, "2026-10-01 00:00:00"), (2, "2026-10-02 00:00:00.5"), (3, None)],
    )
    delta.sql(f"CREATE VIEW {view} AS SELECT id, updated_at FROM {base}")
    value = datetime(2026, 10, 1, tzinfo=UTC)

    from_view = UnityCatalogReader(
        view, watermark_column="updated_at", watermark_value=value, spark=delta
    ).read()
    from_table = UnityCatalogReader(
        base, watermark_column="updated_at", watermark_value=value, spark=delta
    ).read()

    assert _ids(from_view) == _ids(from_table) == [2]


@pytest.mark.integration
@needs_delta
@needs_spark
def test_a_write_to_a_view_fails_and_leaves_the_view(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    base, view = names("base"), names("view")
    UnityCatalogWriter(base).write(delta.createDataFrame([(1, "a")], SCHEMA))
    delta.sql(f"CREATE VIEW {view} AS SELECT id, name FROM {base}")
    frame = delta.createDataFrame([(2, "b")], SCHEMA)

    for replace in (False, True):
        with pytest.raises(AnalysisException):
            UnityCatalogWriter(view).write(frame, replace=replace)
        assert delta.catalog.getTable(view).tableType == "VIEW"
        assert _rows(delta.table(view)) == [(1, "a")]


@pytest.mark.integration
@needs_delta
@needs_spark
def test_an_append_to_a_parquet_table_fails_and_leaves_it_unchanged(
    delta: SparkSession, names: Callable[[str], str]
) -> None:
    name = names("parquet")
    delta.sql(f"CREATE TABLE {name} (id INT, name STRING) USING parquet")
    delta.sql(f"INSERT INTO {name} VALUES (1, 'a')")

    with pytest.raises(AnalysisException):
        UnityCatalogWriter(name).write(delta.createDataFrame([(2, "b")], SCHEMA))

    assert _rows(delta.table(name)) == [(1, "a")]


@pytest.mark.integration
@needs_delta
@needs_spark
@pytest.mark.parametrize("watermark", [False, True], ids=["full", "watermark"])
def test_a_read_of_a_missing_table_raises_the_error_of_spark(
    watermark: bool, delta: SparkSession, names: Callable[[str], str]
) -> None:
    reader = UnityCatalogReader(
        names("missing"),
        watermark_column="updated_at" if watermark else None,
        spark=delta,
    )

    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        reader.read()
