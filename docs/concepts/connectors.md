# Connectors

A connector is an integration with an external system. It knows how to talk to that system
and nothing else: it does not decide write strategy, format or architecture. Those belong to
[callbacks](callbacks.md). See the [philosophy](../philosophy.md#connector).

## Capabilities

A connector declares what it can do through two capabilities, defined as protocols in
`ingestio.connector.core`:

```python
from typing import Protocol, TypeVar

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class Reader(Protocol[T_co]):
    def read(self) -> T_co: ...


class Writer(Protocol[T_contra]):
    def write(self, data: T_contra) -> None: ...
```

- A connector that can read has a `read()` method; one that can write has `write(data)`; one
  that can do both has both.
- Protocols are structural: a class is a connector because it has the method, not because it
  inherits from anything. There is no base class and no registry.
- The payload type is unconstrained. A reader may return a list of records, a generator, a
  DataFrame or bytes.

`Reader` and `Writer` are exported by `ingestio` and by `ingestio.connector.all`, for type
annotations.

## Using a connector

A connector is injected into the callback that needs it, so a single callback may combine
several. The library ships two callbacks that move data between a connector and the
[shared state](callbacks.md#sharing-data): `ReadSource` and `WriteTarget`.

```python
from pathlib import Path
from typing import Any

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import JsonLinesWriter


class StaticReader:
    """A connector that reads records held in memory."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self.records = records

    def read(self) -> list[dict[str, Any]]:
        return self.records


Process([
    Step("extract", [ReadSource(StaticReader([{"id": 1}, {"id": 2}]))]),
    Step("load", [WriteTarget(JsonLinesWriter("volume/orders.jsonl"))]),
]).run()

print(Path("volume/orders.jsonl").read_text(encoding="utf-8"))
```

`StaticReader` is a complete connector: it satisfies `Reader[Any]` for the type checker
without importing anything from ingestio.

## Connectors shipped with the library

### `JsonApiReader`

`ingestio.connector.all.JsonApiReader(url, *, timeout=30.0, headers=None, encoding="utf-8")`

Fetches a JSON endpoint and returns the decoded payload.

- Built on `urllib`, so it adds no dependency.
- Accepts only `http` and `https` URLs; any other scheme raises `ValueError` when the reader
  is built.
- Sends `Accept: application/json`, plus the `headers` given.
- Decodes the body with the charset the response declares, or `encoding` when it declares
  none.
- HTTP errors are not swallowed: they propagate as `urllib.error.HTTPError`.
- Endpoints that require authentication are not supported.

<!-- example: skip -->
```python
from ingestio.connector.all import JsonApiReader

orders = JsonApiReader("https://api.example.com/v1/orders", timeout=10).read()
```

### `JsonLinesWriter`

`ingestio.connector.all.JsonLinesWriter(path, *, append=False, encoding="utf-8")`

Writes records as newline-delimited JSON, one object per line.

- Creates missing parent directories.
- Replaces the file on each write, or appends to it with `append=True`.
- Serialises values JSON does not know, such as datetimes, with `str`.
- A Databricks volume is an ordinary path, so the same writer serves a local disk and a
  cluster.

### PostgreSQL: `PostgresConnection`, `PostgresReader`, `PostgresWriter`

```python
from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter
```

These three classes are exported by `ingestio.connector.all` only, not by `ingestio`. They are
built on Spark's JDBC data source, so they run distributed on a Databricks cluster: the reader
returns a `pyspark.sql.DataFrame`, and the writer receives one. Both need a Spark runtime, and
the writer also needs the [`postgres` extra](#extras). See [Supported
environments](#supported-environments).

`PostgresReader` satisfies `Reader[DataFrame]` and `PostgresWriter` satisfies
`Writer[DataFrame]`, structurally, so `ReadSource` and `WriteTarget` use them unchanged.

#### How a connector receives an ingestion's parameters

The parameters of one ingestion (the source table, the watermark column) come from the job's
configuration, which the library does not read or parse. A connector takes them as constructor
arguments named after the configuration keys (`table_name`, `watermark_column`), and has no
`from_config`. Credentials are separate arguments, held by `PostgresConnection`, and never live
in the configuration: on Databricks they come from `dbutils.secrets.get`, which the library
never imports. A reader extracts only the rows the job needs, and filters inside the source.
See [ADR 0013](../decisions/0013-connector-configuration-convention.md).

#### `PostgresConnection`

```text
PostgresConnection(
    host: str, database: str, user: str, password: str,
    *, port: int = 5432, sslmode: str | None = None,
)
```

Where and how to reach one database. It opens nothing by itself, and it is not a third
concept: it is a value that groups the settings both connectors need, like the URL of
`JsonApiReader`. It is immutable, and its arguments are readable as attributes of the same
names.

`sslmode` is `None` (the drivers' default), `disable`, `allow`, `prefer`, `require`,
`verify-ca` or `verify-full`. Invalid settings raise `ValueError` when the connection is
built:

| Argument | Rule | Message |
|---|---|---|
| `host` | not empty | `host must not be empty` |
| `database` | not empty | `database must not be empty` |
| `user` | not empty | `user must not be empty` |
| `port` | from 1 to 65535 | `port must be between 1 and 65535` |
| `sslmode` | one of the values above | `unsupported sslmode '<value>'` |

The password is not validated. `repr` shows what identifies the database and never the
password:

```text
PostgresConnection(host='db.internal', port=5432, database='shop', user='ingest')
PostgresReader(table_name='public.orders', host='db.internal', database='shop')
PostgresWriter(table_name='public.orders', host='db.internal', database='shop')
```

For pgJDBC the URL is `jdbc:postgresql://<host>:<port>/<database>`: an IPv6 host is wrapped in
brackets and the database name is percent-encoded. The user and the password are passed as
options, never inside the URL. For psycopg, the writer connects with keyword arguments only,
plus `sslmode` when it is set.

#### Table and column names

`table_name` has the form `table` or `schema.table`, on the reader and on the writer. Anything
else raises `ValueError("table_name must be 'table' or 'schema.table'")` when the connector is
built. Names are case-sensitive, as stored in the PostgreSQL catalog, and always quoted:
`Sales.Orders` is the table `"Sales"."Orders"`, a different table from `sales.orders`. A name
without a schema resolves through the `search_path` of the connections.

#### `PostgresReader`

```text
PostgresReader(
    table_name: str, connection: PostgresConnection,
    *, watermark_column: str | None = None,
    watermark_value: int | Decimal | str | date | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    spark: SparkSession | None = None,
)
```

`read()` returns a Spark DataFrame built with `spark.read.format("jdbc")`. It is lazy: the
executors fetch the rows when an action runs on it. Building the reader touches neither Spark
nor PostgreSQL.

- **Session.** `spark` when given. Otherwise `SparkSession.active()`, looked up when `read()`
  is called, never when the reader is built. The reader never creates a session; when none is
  active, PySpark's error propagates.
- **Full read** (no `watermark_column`): one load of the quoted table. Every action on the
  DataFrame queries the table again.
- **Watermark read** (`watermark_column` given): only the rows with a watermark above
  `watermark_value` (all of them when it is `None`) are read, and the filter runs inside
  PostgreSQL. `read()` first runs one query,
  `SELECT max(<col>)::text AS upper_bound FROM <table>` with `WHERE <col> > <value>` when a
  value is given, and takes the highest watermark of the slice as an upper bound. The
  DataFrame is then a subquery that selects the rows with `<col> > <value> AND <col> <= <bound>`.
  When no row qualifies the slice is empty. The consequences:
  - every action on the DataFrame reads the same range. Rows that arrive after `read()` with a
    higher watermark are left for the next run, so a watermark that the job computes from the
    DataFrame never goes past what the DataFrame holds;
  - the comparison is strictly greater, and rows whose watermark is `NULL` are never
    extracted, the first run included;
  - one index on the watermark column serves both queries.
- The reader decides no strategy. Whether a run is incremental, from which value, and how the
  next watermark is computed and kept, belong to the job. The bound that `read()` captured is
  not exposed.

**Watermark values.** `watermark_value` is "given" when it is not `None`: `0` and `""` are
values. It becomes a quoted SQL literal and PostgreSQL converts it to the column's type, so
`'41'` compared with an integer column is the integer 41.

| Type | Text of the literal |
|---|---|
| `int`, but not `bool` | `str(value)` |
| finite `decimal.Decimal` | `format(value, "f")` |
| `str` | the string itself, single quotes doubled |
| timezone-aware `datetime.datetime` | converted to UTC, then `isoformat(sep=" ")` |
| `datetime.date` | `value.isoformat()` |

An aware datetime is converted to UTC first, so its literal always ends in `+00:00`. For a
`timestamptz` column that is the same instant. For a `timestamp without time zone` column
PostgreSQL ignores the offset of the literal, so the column is compared on the UTC wall clock,
which matches the default UTC session time zone of Spark and Databricks. A `timestamp without
time zone` column that holds wall-clock time in another zone needs its value as a `str`, which
is sent as it is. The quoting assumes `standard_conforming_strings` is on, which is PostgreSQL's
default.

The reader raises `ValueError` when it is built:

| Case | Message |
|---|---|
| `watermark_value` without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `bool`, `float` or any other type | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` | `watermark_value must be timezone-aware` |

A job that takes the watermark from a Delta table gets naive datetimes from `collect()`: attach
the zone, or pass a string.

**Options.** `options` holds Spark JDBC options passed through, such as `partitionColumn`,
`lowerBound`, `upperBound`, `numPartitions`, `fetchsize`, `sessionInitStatement` or
`queryTimeout`. The reader sets `fetchsize` to `10000` by default, because pgJDBC's default of 0
loads a whole partition into memory. A user option replaces the default of the same name, with
names compared case-insensitively as Spark does. In a watermark read the bound query leaves out
`partitionColumn`, `lowerBound`, `upperBound` and `numPartitions`, which Spark rejects
together with a query; the data load carries them. The keys `url`, `dbtable`, `query`, `user`,
`password` and `driver` are set by the connector: giving one, in any case, raises
`ValueError("option '<key>' is set by the connector")` when the connector is built. The reader
has no custom query, no column projection, no automatic partition bounds and no `primary_key`.

#### `PostgresWriter`

```text
PostgresWriter(
    table_name: str, connection: PostgresConnection,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

PostgresWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame into a table that already exists, atomically. It needs psycopg:
building a `PostgresWriter` without it raises
`ImportError("PostgresWriter needs psycopg: pip install 'ingestio[postgres]'")`.

- `write(data)` appends: it inserts every row of `data` and leaves the existing rows alone.
- `write(data, replace=True)` truncates the target and then inserts every row of `data`, in
  the same transaction, so the target ends up holding exactly the rows of `data`.
- The calling callback chooses at call time, by passing `replace`. The connector's
  configuration has no mode and there is no `replace()` method. Anything built on top, such as
  a merge, belongs to callbacks. `WriteTarget` calls `write(data)`, so it always appends.
- `options` holds Spark JDBC write options passed through, such as `numPartitions` or
  `batchsize`. The writer sets `stringtype` to `unspecified`, which lets Spark strings go into
  `uuid`, `jsonb` and enum columns, and `reWriteBatchedInserts` to `true`, which makes batched
  inserts faster. User options replace them. The same reserved keys as the reader's are
  rejected.

**How a write happens.** Spark's JDBC writer commits one transaction per partition, so a failed
parallel write would leave part of the data in the target. The writer avoids that in three
steps:

1. **Setup.** One psycopg connection, on the Spark driver, creates an unlogged staging table in
   the target's schema, with the target's types for exactly the DataFrame's columns and none of
   its constraints, defaults or indexes. A DataFrame column that the target lacks fails here,
   before any row moves.
2. **Load.** Spark writes the DataFrame into the staging table in parallel. No psycopg
   connection is open meanwhile, so a long write does not meet idle timeouts.
3. **Apply.** A new connection runs, in one transaction, `TRUNCATE` (only with
   `replace=True`), an `INSERT ... SELECT` from the staging table into the target, and the
   `DROP` of the staging table.

The staging table is named `ingestio_staging_` followed by 32 hexadecimal digits, new on every
call. Steps 1 and 2 never touch the target, so readers of the target see none of the new rows
until step 3 commits and then all of them. The target's constraints, triggers, identity and the
defaults of the columns the DataFrame lacks apply in step 3.

**Failures.** Nothing is swallowed: every error propagates out of `write()`.

- A DataFrame without columns raises `ValueError("the DataFrame has no columns")` and does
  nothing else.
- When the setup fails, nothing was created and the error propagates.
- When the load or the apply fails, the target is as it was, and the writer drops the staging
  table on a new connection. The original exception propagates, the same object.
- When that cleanup fails too, the original exception gets one note, added with `add_note`:
  `staging table <stg> could not be dropped: <type>: <message>`. The cleanup error is not
  raised.
- If the driver process dies, the cleanup cannot run, and a table named
  `ingestio_staging_<hex>` is left behind for you to drop.

The writer runs no DDL on the target: a missing target fails the setup. It evaluates the
DataFrame once, through the save, and never counts it. An empty DataFrame goes through the same
steps: an append inserts nothing, and `replace=True` leaves the target empty. A callback that
wants to skip empty batches checks for itself and raises `CancelStepException`. The connectors
write no log lines.

#### Example

This runs as written, because building the objects opens nothing. Reading and writing need a
Spark session and a PostgreSQL server.

```python
from datetime import UTC, datetime

from ingestio.connector.all import PostgresConnection, PostgresReader

source = PostgresConnection("pg.internal", "shop", "ingest", "s3cret", sslmode="require")
reader = PostgresReader(
    "public.orders",
    source,
    watermark_column="updated_at",
    watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
)
print(reader)

try:
    PostgresConnection("pg.internal", "shop", "ingest", "s3cret", port=0)
except ValueError as error:
    print(error)
```

A job on Databricks: the configuration and the secrets stay in the job, and the callback that
loads chooses to replace.

<!-- example: skip -->
```python
import json
from pathlib import Path

from ingestio import Callback, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter

config = json.loads(Path("/Workspace/jobs/orders.json").read_text())
# {"table_name": "public.orders", "primary_key": "id", "watermark_column": "updated_at", ...}

source = PostgresConnection(
    "pg.internal",
    "shop",
    dbutils.secrets.get("pg", "user"),
    dbutils.secrets.get("pg", "password"),
    sslmode="require",
)
reader = PostgresReader(
    config["table_name"],
    source,
    watermark_column=config["watermark_column"],
    watermark_value=last_watermark,  # computed by the job: from the target, a control table...
)


class ReplaceTarget(Callback):
    """A full load: the pipeline's choice, made in a callback."""

    def __init__(self, writer: PostgresWriter, key: str) -> None:
        self.writer = writer
        self.key = key

    def run(self) -> None:
        df = self.process.state[self.key]
        self.writer.write(df, replace=True)


Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [ReplaceTarget(PostgresWriter("public.orders_copy", source), "orders")]),
]).run()
```

#### Supported environments

- Databricks classic compute, in dedicated or standard access mode, on Databricks Runtime 15.4
  LTS or later. The PostgreSQL JDBC driver ships with the runtime. Only the DataFrame API is
  used.
- Open-source Spark 3.5 or later, on Java 17 or 21, with the pgJDBC driver on the classpath,
  for example through `spark.jars.packages=org.postgresql:postgresql:<version>`.
- Not Databricks serverless compute: Databricks does not support writing to PostgreSQL from
  it, and the reader is not tested there.
- The Spark driver needs network access to PostgreSQL for psycopg, as the executors do for
  pgJDBC.
- pyspark 3.5 or later is a prerequisite that the runtime provides. It is never installed by
  the extra.

**Privileges.** The reader needs `SELECT` on the table. The writer needs `CREATE` on the
target's schema, `SELECT` and `INSERT` on the target, and `TRUNCATE` for `replace=True`.

#### Known limitations

- **Late commits.** A row committed after a read, with a watermark at or below the bound that
  read captured, is never extracted by a later run. This happens with long transactions and
  with clock skew between the source's writers, and is the classic limit of timestamp
  watermarks. Rows that share the exact value of the previous watermark but commit later are
  missed the same way.
- **Full reads are not frozen.**
- **`TRUNCATE`**, run by `write(data, replace=True)`, holds an `ACCESS EXCLUSIVE` lock until the
  commit, so readers of the target wait. It is not MVCC-safe: a transaction in `REPEATABLE
  READ` or `SERIALIZABLE` whose snapshot predates the commit sees the table as empty. It fails
  on a table referenced by a foreign key, because the writer does not use `CASCADE`; the error
  propagates and the target is unchanged.
- **Spark task retries.** Spark's JDBC writer is not idempotent per partition. A task re-run
  after its partition committed (a lost executor, or speculative execution) writes the
  partition twice into the staging table, and the copy reaches the target. Databricks turns
  speculation off by default.
- **One long transaction.** The apply copies the whole batch at once, with the WAL, the locks
  and the triggers that this implies.
- **Type conversion.** The values of the rows go through Spark's JDBC type mapping and pgJDBC;
  the connectors add no conversion. Timestamps, for instance, pass through the JVM's time zone.
  Only the watermark value is normalised, to UTC.
- **Orphaned staging tables**, when the driver dies or the cleanup fails.
- Databricks recommends Lakehouse Federation for read-only queries. These connectors are for
  jobs that write, or that run on open-source Spark.

### Disk: `DiskReader`, `DiskWriter`

```python
from ingestio.connector.all import DiskReader, DiskWriter
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read
and write files in a directory on disk, such as a Unity Catalog volume, through Spark's file
data sources: the reader returns a `pyspark.sql.DataFrame` and the writer receives one.
`DiskReader` satisfies `Reader[DataFrame]` and `DiskWriter` satisfies `Writer[DataFrame]`,
structurally, so `ReadSource` and `WriteTarget` use them unchanged. They add no dependency and
no extra: pyspark is a prerequisite that the runtime provides, imported only when it is needed.
They follow the [convention for an ingestion's
parameters](#how-a-connector-receives-an-ingestions-parameters): `path`, `format`,
`watermark_pattern` and `schema` are arguments named after configuration keys, and the job
computes `watermark_value`.

The job gives the format on both sides. The connectors never choose one, never infer it from
file extensions and transform nothing: they add no column, cast nothing and filter no rows.
Spark's data source for that format does the conversion.

#### `DiskReader`

```text
DiskReader(
    path: str | PathLike[str], format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    spark: SparkSession | None = None,
)

DiskReader.read() -> DataFrame
```

- `path`: the directory that holds the files, an absolute local path such as
  `/Volumes/<catalog>/<schema>/<volume>/<dir>`.
- `format`: the name of the Spark data source, such as `parquet`, `csv` or `json`.
- `watermark_pattern`: a regular expression that the whole name of a file matches, with one
  group that captures the file's watermark. Without it, a file's watermark is its
  last-modified time.
- `watermark_value`: the last watermark already ingested. Only files above it are read. A `str`
  with `watermark_pattern`, a timezone-aware `datetime` without it.
- `schema`: a `StructType` or a DDL string. Spark infers it when omitted.
- `options`: Spark data source options passed through, such as `header`.
- `spark`: the session to read with. The active session when omitted.

`read()` returns a DataFrame built with `spark.read.format(format)`. Building the reader
validates the arguments and does nothing else: it touches neither Spark nor the file system,
and does not check that `path` exists.

**How a read selects files.** Each call to `read()` goes through these steps, and the selection
is made before Spark loads anything:

1. **Session.** `spark` when given. Otherwise `SparkSession.active()`, looked up when `read()`
   is called, never when the reader is built. The reader never creates a session; when none is
   active, PySpark's error propagates.
2. **Listing.** The reader lists the entries directly inside `path`, once, on the driver, with
   the standard library. A missing `path` raises `FileNotFoundError`, a `path` that is a file
   raises `NotADirectoryError`, and `PermissionError` propagates.
3. **Candidates.** A candidate is a regular file, or a link to one, whose name starts with
   neither `_` nor `.` and does not end with `._COPYING_`. That is Spark's own rule for hidden
   and in-progress files. It leaves out `_SUCCESS`, `.crc` files and files that are still
   being copied. The Parquet summary files `_metadata` and `_common_metadata` are left out
   too. Subdirectories are ignored, with everything in them.
4. **Watermark of each candidate**, as described below.
5. **Selection.** The candidates whose watermark is strictly greater than `watermark_value`,
   or all of them when it is `None`. The comparison is strict because the value is the last
   watermark already ingested.
6. **Load.** One load of the selection, with `format`, `schema` and `options` as given and no
   other option.

**The watermark of a file.**

- *With `watermark_pattern`.* The pattern is matched against the whole file name, not the path,
  with `re.fullmatch`, and the watermark is the text its group captured. A file whose name does
  not match, or whose group took no part in the match, is not read. Watermarks compare as text,
  code point by code point, and are never converted: `"9"` is above `"10"`. Use watermarks that
  are fixed-width and most significant first, such as ISO 8601 dates written the same way in
  every name, or zero-padded sequence numbers. A pattern also filters a directory that several
  feeds share, but a producer that renames its files is silently never read.
- *Without `watermark_pattern`.* The watermark is the file's last-modified time, following
  symbolic links, truncated to whole milliseconds and compared with `watermark_value` as an
  instant, whatever the value's zone. The file name plays no part. Milliseconds are the
  precision of Spark's `_metadata.file_modification_time`, so a job that takes its next
  watermark from that column never selects its last file again.

**The selection is frozen.** Spark receives the list of selected files, a `list` of `str`
sorted by file name even for one file, and never the directory. Every action on the DataFrame
reads those files and no other, so files that appear after `read()` are left for the next run,
whatever their watermark. The selection is not exposed: the reader keeps no record of it. A job
that advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata`
column (`file_name`, `file_path`, `file_modification_time`).

**The empty selection.** When the directory is empty, holds no candidate, holds no name that
matches the pattern, or holds no watermark above the value, `read()` makes no load. It returns
an empty DataFrame, with the columns of `schema` when it is given and no columns otherwise.
That DataFrame is not backed by files, so it has no `_metadata` column. A callback that wants
to skip an empty batch checks for itself, with `df.isEmpty()` for instance, and raises
`CancelStepException`.

**Watermark values.** `watermark_value` is "given" when it is not `None`; `""` is a value. The
reader raises `ValueError` when it is built:

| Case | Message |
|---|---|
| `watermark_pattern` is not a valid regular expression | `invalid watermark_pattern: <error>` |
| `watermark_pattern` has no group, or more than one | `watermark_pattern must have exactly one group` |
| `watermark_pattern` given, `watermark_value` given and not a `str` | `watermark_value must be a str with watermark_pattern` |
| no `watermark_pattern`, `watermark_value` given and not a `datetime` | `watermark_value must be a datetime without watermark_pattern` |
| a naive `datetime` | `watermark_value must be timezone-aware` |

`<error>` is the message of the `re.error`, which is the `__cause__` of the `ValueError`. A
group is a capturing group, named or not; `(?:...)` does not count. A `date` that is not a
`datetime`, an `int` and a `bool` are rejected without a pattern.

#### `DiskWriter`

```text
DiskWriter(
    path: str | PathLike[str], format: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

DiskWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame as files in a directory, in the format it is given: one call chain,
`data.write.format(format).options(options).mode(mode).save(path)`. It takes no session.

- `write(data)` appends: Spark adds new files and leaves the existing ones as they are.
- `write(data, replace=True)` replaces: the directory ends up holding exactly the rows of
  `data`. With a plain file format, Spark deletes `path` and everything under it, files it did
  not write included, and then writes the new files. With Delta, the overwrite is a new version
  of the table.
- The calling callback chooses at call time, by passing `replace`. The configuration has no
  mode and there is no `replace()` method. `WriteTarget` calls `write(data)`, so it always
  appends. Merges and deduplication belong to callbacks.
- Spark creates `path`, and its missing parents, when they do not exist. The writer itself
  creates, lists and deletes nothing, and does not retry.
- The file names are Spark's (`part-<n>-<uuid>-c000...`), so a `DiskReader` that reads them
  back takes their watermark from their last-modified time.
- The writer does not partition its output, calls no `saveAsTable` or `insertInto`, and has no
  staging mechanism. An empty DataFrame goes through the same call; what lands depends on
  Spark. A DataFrame without columns fails with Spark's own error.
- Every error propagates out of `write()` unchanged, the same object.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what Spark's commit
protocol and the format guarantee, and nothing more:

- *Append, plain file formats* (`parquet`, `csv`, `json`, `orc`, ...). Open-source Spark uses
  Hadoop's `FileOutputCommitter`, algorithm version 1 by default. Tasks write under a
  `_temporary` directory inside `path`, and the job's commit moves their files into `path` one
  by one. A job that fails before its commit adds no data file, but can leave a `_temporary`
  directory behind, which Spark and `DiskReader` ignore. A reader that lists `path` during the
  commit can see part of the new files. With algorithm version 2, a failed job can leave part
  of the batch.
- *Replace, plain file formats.* Spark deletes `path` before it writes. If the write then
  fails, `path` is left missing, empty or holding part of the new rows, and the previous
  contents are lost.
- *Delta.* An append or an overwrite is one commit in the table's transaction log. Readers see
  the table before the commit or after it, and a failed write leaves it as it was.
- *On Databricks,* the runtime can use its own commit protocol. What it guarantees is the
  runtime's, and these connectors do not rely on it.

#### Building the disk connectors

Building does nothing but validate. Invalid arguments raise `ValueError`:

| Case | Message |
|---|---|
| `path` is not an absolute local path: relative, empty, or a URI such as `s3://`, `file:/`, `dbfs:/` | `path must be an absolute local path` |
| `format` is empty | `format must not be empty` |
| reader, `format` is `delta` in any case | `format '<format>' cannot be read file by file` |
| a reserved option key, in any case | `option '<key>' is set by the connector` |

- `path` is kept as `str(Path(path))`, so a trailing slash is dropped, and Spark receives
  paths, never URIs. Relative paths are rejected because the driver's Python process and Spark
  can resolve them from different working directories.
- `format` is passed to Spark as given. An unknown format fails in Spark when the connector
  reads or writes. The reader rejects `delta` because a Delta table's transaction log decides
  which of its Parquet files are live, so its files cannot be selected one by one. The writer
  accepts it.
- The connectors set no option of their own: `options` reaches Spark as given, and there are
  no defaults. Reserved keys:

  | Connector | Reserved keys | Why |
  |---|---|---|
  | both | `path`, `paths` | The connector gives Spark its paths. |
  | `DiskReader` | `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`, `modifiedAfter` | They select files, which the reader does itself before loading. |
  | `DiskWriter` | `replaceWhere`, `partitionOverwriteMode` | They turn `replace=True` into a partial replace, a write strategy that belongs to callbacks. |

- `repr` shows the path and the format:

  ```text
  DiskReader(path='/Volumes/main/landing/orders', format='csv')
  DiskWriter(path='/Volumes/main/lake/orders', format='parquet')
  ```

The connectors catch nothing and write no log lines. Errors of the listing, of PySpark and of
Spark propagate out of `read()` and `write()` unchanged.

#### Examples

This runs as written, because building the objects touches neither Spark nor the file system.
Reading and writing need a Spark session and a real directory.

```python
from datetime import UTC, datetime

from ingestio.connector.all import DiskReader, DiskWriter

by_name = DiskReader(
    "/Volumes/main/landing/orders/",
    "csv",
    watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020",
    options={"header": True},
)
by_time = DiskReader(
    "/Volumes/main/landing/events",
    "json",
    watermark_value=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
)
writer = DiskWriter("/Volumes/main/lake/orders", "parquet")
print(by_name)
print(by_time)
print(writer)

try:
    DiskReader("landing/orders", "csv")
except ValueError as error:
    print(error)

try:
    DiskReader("/Volumes/main/landing/orders", "csv", watermark_value=datetime(2026, 10, 21))
except ValueError as error:
    print(error)
```

A job on Databricks: the configuration and the watermark stay in the job, and the next
watermark is taken from the files that were read.

<!-- example: skip -->
```python
import json
from pathlib import Path

from pyspark.sql import functions as F

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import DiskReader, DiskWriter

config = json.loads(Path("/Workspace/jobs/orders_files.json").read_text())
# {"path": "/Volumes/main/landing/orders", "format": "csv",
#  "watermark_pattern": "orders_(\\d{8})\\.csv", ...}

reader = DiskReader(
    config["path"],
    config["format"],
    watermark_pattern=config["watermark_pattern"],
    watermark_value=last_watermark,  # "20261020", kept by the job
    schema="id INT, amount DECIMAL(10,2), updated_at TIMESTAMP",
    options={"header": True},
)

process = Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [WriteTarget(DiskWriter("/Volumes/main/lake/orders", "parquet"), key="orders")]),
]).run()

# The selection is not exposed: the job takes its next watermark from the DataFrame.
# Spark's regex dialect is Java's: plain groups only.
orders = process.state["orders"]
if not orders.isEmpty():
    last_watermark = orders.select(
        F.max(F.regexp_extract("_metadata.file_name", r"^orders_(\d{8})\.csv$", 1))
    ).first()[0]
```

#### Environments of the disk connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- Databricks classic compute on Databricks Runtime 15.4 LTS or later, in dedicated or standard
  access mode, with paths in Unity Catalog volumes, `/Volumes/...`. The driver lists them
  through the volume's FUSE mount, and Spark reads and writes them as volume paths. Other
  paths on Databricks (`/tmp`, `/dbfs/...`, `/Workspace/...`) are not supported, because
  Python and Spark resolve them to different file systems.
- Open-source Spark 3.5 or later, on a Java version that release supports, when Spark's default
  file system is the local one (`fs.defaultFS` is `file:///`, the default outside Hadoop
  clusters). That covers local mode, and clusters where `path` is on a file system mounted at
  the same place on the driver and on every executor.
- Databricks serverless compute: nothing in the design excludes it, but it is not tested.
- Cloud storage URIs (`s3://`, `abfss://`, `gs://`, `dbfs:/`) are rejected.
- Formats outside Spark's built-ins need their package in the session: `delta-spark` for Delta
  Lake on open-source Spark (built into Databricks Runtime), or `spark-avro`. Such plugins are
  prerequisites, never extras.

#### Limitations of the disk connectors

- **Writes are only as atomic as Spark and the format make them** (see Atomicity above). A
  failed replace of plain files loses the previous contents, and a reader of the target during
  a write can see part of it.
- **`replace=True` deletes everything under `path`** with a plain file format, files that Spark
  did not write included. Never point a writer that replaces at a directory that holds
  anything else.
- **Late files.** A run that advances its watermark to the highest one it read never reads a
  file that appears later with a watermark at or below it. By name: a producer that delivers
  files out of order. By modification time: a file moved or renamed into the directory after a
  newer file was read, because a rename keeps the modification time, and so do copies that
  preserve times; files that another Spark job commits one by one; clocks that differ between
  producers. Files that share the watermark of the last file read and arrive after it are
  skipped, because the comparison is strict.
- **Files in progress.** A file that is still being written when `read()` lists the directory
  can be read in part. Producers can write under a name that starts with `_` or `.`, or ends
  with `._COPYING_`, and rename the file when it is complete, which the reader ignores. With
  modification-time watermarks, the rename itself can make the file a late file.
- **The selection is frozen, not the contents.** A selected file that is changed before an
  action is read as it is at that action. A selected file that is deleted makes the action
  fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Modification-time precision.** Times are compared to the millisecond. Files modified in the
  same millisecond as the watermark are not read. File systems that keep coarser times widen
  this window.
- **Name watermarks compare as text.** A watermark that is not fixed-width, or not most
  significant first, does not sort as its value: `part_10.csv` comes before `part_9.csv`.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns. Writing a DataFrame without columns fails in Spark.
- **Large directories.** The driver lists the whole directory at each `read()`, and Spark
  receives one path per selected file.
- **The path must name the same directory for the driver's Python process and for Spark.** On
  Databricks that holds only for volumes.
- **Table formats.** The reader rejects `delta`, but does not detect Iceberg, Hudi or other
  table formats. Reading their directories file by file fails or returns rows that are no
  longer live.
- **Session settings apply.** The connectors override no Spark setting. For example,
  `spark.sql.sources.partitionOverwriteMode=dynamic` in the session makes `replace=True` on a
  partitioned Delta table replace only the partitions present in `data`.
- **Type conversion** is whatever Spark's data source does. The connectors convert nothing.

## Known divergence

`JsonLinesWriter` decides two things the philosophy assigns to callbacks: the write strategy
(`append`) and the format (JSON Lines). Moving them out changes the public API, so it waits
for a spec. See [specs/append_only_callback.md](../../specs/append_only_callback.md).

Until that spec settles what `write` means, `WriteTarget` behaves differently with the
writers: it appends to a `PostgresWriter` and to a `DiskWriter`, whose `write(data)` appends,
but overwrites with a `JsonLinesWriter`, whose `write` replaces the file by default.
`PostgresWriter` and `DiskWriter` leave the choice to the calling callback, through
`write(data, replace=...)`, the opposite of `JsonLinesWriter(append=...)`.

## Extras

A third-party package that a connector needs becomes an optional extra, `ingestio[<name>]`, and
the core stays without dependencies ([ADR 0002](../decisions/0002-zero-dependency-core.md)).
The first extra is `ingestio[postgres]`, installed with `pip install 'ingestio[postgres]'`; it
defines the pattern every later extra follows:

1. An extra is declared in `[project.optional-dependencies]` and named after the system it
   integrates. `postgres` holds `psycopg[binary]>=3.2`; `dependencies` stays empty.
2. A module imports third-party packages only inside the functions that use them, and for
   annotations under `TYPE_CHECKING`. `ingestio` and `ingestio.connector.all` import without
   any extra installed.
3. A connector whose extra is missing raises `ImportError` when it is built, with a message
   that names the extra: `pip install 'ingestio[<extra>]'`.
4. An engine that the runtime provides, such as pyspark on Databricks, is never a dependency of
   an extra. It is a documented prerequisite: here, pyspark 3.5 or later.
5. The `dev` dependency group holds the packages of every extra, so that `uv sync` gives an
   environment where the whole suite and mypy run.
