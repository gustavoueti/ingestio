# Connectors

A connector is an integration with an external system. It knows how to talk to that system
and nothing else: it does not decide write strategy, format or architecture. Those belong to
[callbacks](callbacks.md). See the [philosophy](../philosophy.md#connector).

"Architecture" has one precise reading for a missing target, recorded in [ADR
0014](../decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md). A writer may
create its missing target only in the form the system derives entirely from the data, with no
layout, location, property or type mapping chosen by the connector; anything more is created by
the job. `PostgresWriter` creates nothing, because a PostgreSQL table from a DataFrame needs type
mappings that the connector would have to choose. `UnityCatalogWriter` creates a plain managed
Delta table, because its columns and types are the DataFrame's. Delta is that system's native
table format, so always writing Delta is not the connector choosing a format. The rule is a
permission, not a requirement: `BigQueryWriter` creates nothing either, and a missing target fails,
because the writer runs no DDL on the target.

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

The writer runs no DDL on the target: a missing target fails the setup. (Creating a PostgreSQL
table from a DataFrame would mean choosing column types, which is architecture; see [ADR
0014](../decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md).) It evaluates the
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

### Unity Catalog: `UnityCatalogReader`, `UnityCatalogWriter`

```python
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read and
write Delta tables registered in a Spark catalog, such as Databricks Unity Catalog, named by
their three-part name `catalog.schema.table`. `UnityCatalogReader` returns a
`pyspark.sql.DataFrame` and `UnityCatalogWriter` receives one, so they satisfy `Reader[DataFrame]`
and `Writer[DataFrame]`, structurally, and `ReadSource` and `WriteTarget` use them unchanged. They
follow the [PostgreSQL connectors](#postgresql-postgresconnection-postgresreader-postgreswriter):
a table is read incrementally by a watermark with a frozen upper bound, and an ingestion's
parameters are [constructor arguments](#how-a-connector-receives-an-ingestions-parameters).
The connectors add no dependency and no extra: pyspark and Delta Lake are prerequisites that the
runtime provides, and pyspark is imported only when it is needed. They work with any Spark
catalog that resolves a three-part name to a Delta table.

They are not the disk connectors. `DiskWriter(path, "delta")` writes a Delta table at a path,
which on Databricks is files in a volume; Unity Catalog governs tables by name, with their
privileges, lineage and managed storage, and these connectors write those. `DiskReader` rejects
`delta`, because a Delta table cannot be read file by file.

#### `UnityCatalogReader`

```text
UnityCatalogReader(
    table_name: str,
    *, watermark_column: str | None = None,
    watermark_value: int | Decimal | str | date | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    spark: SparkSession | None = None,
)

UnityCatalogReader.read() -> DataFrame
```

- `table_name`: the source table, `catalog.schema.table`. The configuration key of the same name.
- `watermark_column`: the column that orders changes. The configuration key of the same name.
- `watermark_value`: the last watermark already ingested. Only rows above it are read. Requires
  `watermark_column`.
- `options`: Spark read options passed through, such as Delta's `versionAsOf` or `timestampAsOf`.
- `spark`: the session to read with. The active session when omitted.

`datetime.datetime`, a subclass of `date`, is accepted as a `watermark_value` too.

Building the reader validates its arguments and does nothing else: it imports no third-party
package, touches neither a Spark session nor the catalog, and does not check that the table
exists.

**Table and column names.** `table_name` is `catalog.schema.table`, on the reader and on the
writer. Anything other than three non-empty parts, split on `.`, raises
`ValueError("table_name must be 'catalog.schema.table'")` when the connector is built. The parts
are plain names, never quoted by the job, and a part cannot contain a `.`. One-part and two-part
names are not accepted, because a short name resolves against the session's current catalog and
schema, which differ between clusters and jobs.

The connectors always quote every part of the table name and the watermark column as Spark SQL
identifiers, wrapping each in backticks and doubling the backticks inside, so
`main.sales.orders` reaches Spark as `` `main`.`sales`.`orders` ``. Quoting protects special
characters and reserved words; it does not make a name case-sensitive. Whether case matters is
the catalog's rule (Unity Catalog and the Hive metastore store table names in lowercase), and
columns resolve by Spark's rules, case-insensitively unless the session sets
`spark.sql.caseSensitive`. `watermark_column` is one top-level column: a name with a `.` is a
column with a dot in its name, not a nested field.

**Reading.** Each call to `read()` looks up the session, never when the reader is built: `spark`
when given, otherwise `SparkSession.active()`. The reader never creates a session; when none is
active, PySpark's error propagates. It then builds one DataFrame over the table, the *source*,
with `spark.read.options(**options).table(<quoted table>)` and no other option.

- **Full read** (no `watermark_column`): `read()` returns the source unchanged. It runs no Spark
  action. Every action on the DataFrame reads the table's latest version.
- **Watermark read** (`watermark_column` given): only the rows with a watermark above
  `watermark_value` (all of them when it is `None`) are read, and the filter is part of the
  table's scan, so Delta skips the files whose statistics rule out a match and Parquet skips row
  groups. `read()` first runs one query on the source, filtered by the lower predicate when a
  value is given:

  ```text
  CAST(max(<col>) AS STRING) AS upper_bound
  typeof(max(<col>)) AS upper_bound_type
  ```

  The first gives the highest watermark of the slice as Spark renders it, and the second the
  type Spark names for it. The DataFrame it returns is the source filtered with
  `<col> > <value> AND <col> <= CAST(<bound> AS <type>)`, or `<col> <= CAST(<bound> AS <type>)`
  without a value, or `false` when no row qualifies. Because the bound is Spark's own text cast
  back to the column's own type, it never passes through Python types, and it is exact for
  integer, decimal, string, date and `TIMESTAMP_NTZ` columns. The consequences:
  - every action on the DataFrame reads the same range of the watermark. Rows written after
    `read()` with a higher watermark are left for the next run, so a watermark that the job
    computes from the DataFrame never goes past what the DataFrame holds;
  - the comparison is strictly greater, and rows whose watermark is `NULL` are never extracted,
    the first run included;
  - an empty slice is a DataFrame with the table's columns and no rows;
  - the reader adds no column, cast, projection or Spark setting, and the bound query runs once
    per `read()`.
- The reader decides no strategy. Whether a run is incremental, from which value, and how the
  next watermark is computed and kept, belong to the job. The bound that `read()` captured is not
  exposed, and the reader keeps no state between calls: a reader built once serves every run
  that calls it.
- The reader reads whatever the catalog resolves the name to, table or view, with no check of its
  type. The filter reaches the table's scan when Spark can push it through a view.

**Watermark values.** `watermark_value` is "given" when it is not `None`: `0` and `""` are
values. It becomes a literal of Spark SQL, by its type:

| Type | Literal |
|---|---|
| `int`, but not `bool` | the number, unquoted: `41`, `-7` |
| finite `decimal.Decimal` | `format(value, "f")` followed by `BD`: `12.50BD` |
| `str` | a string literal |
| timezone-aware `datetime.datetime` | converted to UTC, then `isoformat(sep=" ")`, as a string literal: `'2026-10-01 08:30:00+00:00'` |
| `datetime.date` | `value.isoformat()` as a string literal: `'2026-10-01'` |

A string literal wraps the text in single quotes and precedes each backslash and each single
quote inside with a backslash: `O'Brien` becomes `'O\'Brien'`. This assumes
`spark.sql.parser.escapedStringLiterals` is `false`, Spark's default.

- Numbers are not quoted because Spark compares a string with a `DECIMAL` column as `DOUBLE`,
  which loses precision; an integer or `BD` literal is compared as a number, exactly.
- Dates and datetimes are string literals because Spark casts a string compared with a `DATE`,
  `TIMESTAMP` or `TIMESTAMP_NTZ` column to the column's own type. A typed literal would make
  Spark convert the column instead, row by row, which keeps Delta from skipping files.
- An aware datetime is converted to UTC first. For a `TIMESTAMP` column the literal is the same
  instant, whatever the offset it carried and the session's time zone. For a `TIMESTAMP_NTZ`
  column Spark ignores the offset when it casts, so the column is compared on the UTC wall
  clock: 08:30 at `-03:00` is compared as `2026-10-01 11:30:00`, as the PostgreSQL reader does
  with `timestamp without time zone`. A `TIMESTAMP_NTZ` column that holds wall-clock time in
  another zone needs its value as a `str`, which is sent as it is.
- Other combinations follow Spark's type coercion. A value of the column's own type keeps the
  comparison exact and lets Delta skip files.

The reader raises `ValueError` when it is built:

| Case | Message |
|---|---|
| `watermark_value` without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `bool`, `float` or any other type | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` | `watermark_value must be timezone-aware` |

A datetime is naive when its `tzinfo` is `None` or its `utcoffset()` is `None`. A job that takes
its watermark from `collect()` gets a naive datetime: attach the zone, or pass a string. A job
that renders the highest watermark of the DataFrame as text with its offset, as in the example
below, passes that `str` back.

**Options.** The reader's `options` are Spark read options for the table, passed through as
given, and it sets none of its own. Spark turns the values into strings, and booleans into
`true` and `false`. Reserved keys raise `ValueError("option '<key>' is set by the connector")`
when the connector is built, in any case, with the key as written:

| Connector | Reserved keys | Why |
|---|---|---|
| both | `path` | The connector names the table by its three-part name. A path names storage, and on the writer it would make the created table an external one. |
| `UnityCatalogReader` | `readChangeFeed`, `readChangeData` | They make Delta return the table's changes instead of its rows. The change data feed is a later spec. |
| `UnityCatalogWriter` | `replaceWhere`, `partitionOverwriteMode` | They turn `replace=True` into a partial replace, a write strategy that belongs to callbacks. The writer sets `partitionOverwriteMode` itself. |

Time travel passes through the reader's options: a job that needs a full read frozen at a version
passes `versionAsOf` or `timestampAsOf`.

**Errors of a read.** Nothing is caught. Errors of PySpark, Spark, Delta and the catalog
propagate out of `read()` unchanged, the same object. When one surfaces depends on the session:
classic Spark analyses a DataFrame when it is built, so a missing table, a missing column or a
missing privilege fails `read()`; Spark Connect (Databricks Connect, serverless compute) analyses
it later, at the bound query or at the job's first action. The connectors write no log lines.

#### `UnityCatalogWriter`

```text
UnityCatalogWriter(
    table_name: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

UnityCatalogWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame into a table, in one Delta commit. `table_name` is the table to write
into, `catalog.schema.table`, with the rules above. `options` are Delta write options passed
through as given, such as `mergeSchema`, `overwriteSchema`, `userMetadata`, `txnAppId` and
`txnVersion`. Building does nothing but validate. The writer takes no session: it uses
`data.write`.

`write` first checks the columns: a DataFrame without columns raises
`ValueError("the DataFrame has no columns")` and nothing else happens. It then makes one call
chain:

```text
data.write.format("delta").options(<options>).mode(<mode>).saveAsTable(<quoted table>)
```

- `write(data)` appends: it adds every row of `data` to the table and leaves the existing rows as
  they are. `WriteTarget` calls `write(data)`, so it always appends.
- `write(data, replace=True)` replaces: the table ends up holding exactly the rows of `data`. The
  writer adds the option `partitionOverwriteMode` = `static`, so this holds on a partitioned table
  too, even when the session sets `spark.sql.sources.partitionOverwriteMode` to `dynamic`, which
  would replace only the partitions present in `data`.
- The calling callback chooses at call time, by passing `replace`. The configuration has no mode
  and there is no `replace()` method. Merges, upserts and SCD2 belong to callbacks.

**Creating a missing table.** When the table does not exist, the save creates it, with the rows
of `data`, in either mode. The writer makes no existence check and no catalog lookup of its own.
The table has the minimal form, according to [ADR
0014](../decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md):

- a managed table, in the managed storage of its schema or catalog (in open-source Spark, the
  session catalog's warehouse directory);
- in Delta format, which is the system's native table format and not a choice of the connector;
- whose columns are the DataFrame's, with their names, types and order;
- with no partitioning, no clustering, no location, no comment and no table property set by the
  connector. Whatever the catalog, the schema or the workspace adds by default, such as deletion
  vectors or predictive optimization on Databricks, is theirs.

A job that needs anything else, such as partitioning, liquid clustering, constraints, a location
or properties, creates the table itself with DDL before the first write, and the writer then
writes into it like into any existing table. The writer never creates a catalog or a schema: a
missing one fails the save. The table appears with its first commit, which holds the rows of
`data`, so readers never see it empty or partly written. An empty DataFrame creates the table
with its columns and no rows.

**Columns and schema.** Columns are matched by name, never by position, and the order of the
DataFrame's columns does not matter. Delta's schema enforcement applies, with Spark's case rules:

- a column that the DataFrame lacks is written as `NULL`, or with the column's generated, identity
  or default value. A `NOT NULL` or `CHECK` constraint that a row violates fails the whole write;
- a column that the table lacks fails the write, unless the job passes `mergeSchema`;
- a type that Delta cannot store in the column fails the write;
- a replace keeps the table's schema and partitioning, unless the job passes `overwriteSchema`.

The writer converts nothing: values go into the table as Spark has them. It changes an existing
table's schema only when the job passes Delta's own options for it.

**Atomicity.** The writer adds no mechanism of its own: no staging, no cleanup and no retry. A
write guarantees what a Delta commit guarantees:

- An append, an overwrite and the creation of a missing table are each one commit in the table's
  transaction log. Readers see the table before the commit or after it, never in between. A write
  that fails leaves the table as it was, or, when it was missing, leaves no table in the catalog.
- An overwrite is a new version of the table. The previous version stays readable through time
  travel, for instance with `versionAsOf`, until `VACUUM` removes its files.
- Files written by Spark tasks that failed or were retried are never part of a commit, so a task
  retry does not duplicate rows, unlike Spark's JDBC writer.
- Concurrent writes follow Delta's optimistic concurrency. Two appends to the same table do not
  conflict. A write that conflicts with another commit fails with Delta's error and writes
  nothing; the writer does not retry.
- When two writers find the table missing at the same time, one creates it. The other either
  appends after it or fails with Spark's error that the table already exists, depending on Spark,
  Delta and the catalog. Either way each write is all or nothing.

**What the writer does not do.** It reads only `data.columns` and `data.write` from the DataFrame,
never counts, caches or inspects the rows, and the save evaluates the DataFrame once. It calls no
other method of Spark's writer: no `insertInto` (which resolves columns by position), `save`,
`partitionBy`, `bucketBy`, `sortBy` or `clusterBy`. It does not check what the name points to: a
target that is not a Delta table is left to Spark, Delta and the catalog (see the limitations
below). An empty DataFrame goes through the same chain: an append adds nothing, `replace=True`
leaves the table empty. A callback that wants to skip empty batches checks for itself and raises
`CancelStepException`. `write` returns `None`, every error propagates out of it unchanged, the
same object, and the connectors write no log lines.

`repr` shows the table name as given, and nothing else:

```text
UnityCatalogReader(table_name='main.sales.orders')
UnityCatalogWriter(table_name='main.lake.orders')
```

#### Example

This runs as written, because building the objects touches neither Spark nor the catalog.
Reading and writing need a Spark session with Delta Lake.

```python
from datetime import UTC, datetime
from decimal import Decimal

from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter

reader = UnityCatalogReader(
    "main.bronze.orders",
    watermark_column="updated_at",
    watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    options={"versionAsOf": 3},
)
writer = UnityCatalogWriter("main.silver.orders", options={"mergeSchema": True})
print(reader)
print(writer)

for build in (
    lambda: UnityCatalogReader("bronze.orders"),
    lambda: UnityCatalogReader("main.bronze.orders", watermark_value=41),
    lambda: UnityCatalogReader(
        "main.bronze.orders", watermark_column="price", watermark_value=1.5
    ),
    lambda: UnityCatalogReader(
        "main.bronze.orders", watermark_column="price", watermark_value=Decimal("NaN")
    ),
    lambda: UnityCatalogReader(
        "main.bronze.orders", watermark_column="updated_at", watermark_value=datetime(2026, 10, 1)
    ),
    lambda: UnityCatalogReader("main.bronze.orders", options={"readChangeFeed": True}),
    lambda: UnityCatalogWriter("main.silver.orders", options={"replaceWhere": "day = 1"}),
):
    try:
        build()
    except ValueError as error:
        print(error)
```

A job on Databricks: the configuration and the watermark stay in the job, and the next watermark
is taken from the DataFrame that was read. The loading callback appends, as `WriteTarget` does.

<!-- example: skip -->
```python
import json
from pathlib import Path

from pyspark.sql import functions as F

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter

config = json.loads(Path("/Workspace/jobs/orders.json").read_text())
# {"table_name": "main.bronze.orders", "watermark_column": "updated_at", ...}

reader = UnityCatalogReader(
    config["table_name"],
    watermark_column=config["watermark_column"],
    watermark_value=last_watermark,  # kept by the job: a str with its offset, see below
)

process = Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [WriteTarget(UnityCatalogWriter("main.silver.orders"), key="orders")]),
]).run()

# The bound is not exposed: the job takes its next watermark from the DataFrame. Rendering it
# with its offset keeps it exact whatever the time zones of the driver and the session.
orders = process.state["orders"]
next_watermark = orders.select(
    F.date_format(F.max("updated_at"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX")
).first()[0]  # None when nothing was read
```

#### Environments of the Unity Catalog connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides. Delta Lake is a prerequisite
  too: built into Databricks Runtime, and on open-source Spark the Delta Lake JARs
  (`io.delta:delta-spark`) in the session. ingestio installs neither.
- Databricks classic compute on Databricks Runtime 15.4 LTS or later, in dedicated or standard
  access mode, with Unity Catalog tables. Only the DataFrame API is used, never `sparkContext`
  or `_jvm`, which standard access mode does not offer.
- Databricks Connect 15.4 or later, against such a cluster: the session is passed as `spark=`, or
  found by `SparkSession.active()` where PySpark returns the Databricks Connect session. Not
  tested here.
- Open-source Spark 3.5 or later, on a Java version that release supports (17 or 21 for Spark 4),
  with Delta Lake configured: `spark.sql.extensions` set to
  `io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog` set to
  `org.apache.spark.sql.delta.catalog.DeltaCatalog`. Tables are then named
  `spark_catalog.<schema>.<table>`. Other catalog plugins that resolve three-part names to Delta
  tables are not tested.
- Databricks serverless compute: nothing in the design excludes it, but it is not tested.
- Not SQL warehouses: the connectors run only through Spark.
- **Privileges.** The reader needs `USE CATALOG`, `USE SCHEMA` and `SELECT` on the table. The
  writer needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY` and `SELECT` on an existing table, or
  `CREATE TABLE` on the schema when the table does not exist. Row filters and column masks apply
  to the reader as to any query.

The repository's opt-in integration tests of these connectors run against a local Spark session
with Delta Lake, and need `INGESTIO_SPARK_TESTS=1` and `INGESTIO_DELTA_PACKAGE`, the Maven
coordinate of the Delta Lake release that matches the installed pyspark. A JVM loads
`spark.jars.packages` only when it starts, so they must run in their own process: run
`tests/test_unity_catalog_connectors.py` on its own, because they skip when another test module
of the same run has already started a JVM without Delta Lake.

#### Limitations of the Unity Catalog connectors

- **Late commits.** A row committed after a read, with a watermark at or below the bound that
  read captured, is never extracted by a later run. This happens with long-running writers that
  commit rows whose watermark predates another writer's commit, and with clock skew between
  writers. It is the classic limit of timestamp watermarks. Rows that share the exact value of
  the previous watermark but commit later are missed the same way.
- **Full reads are not frozen.** Every action on a full read's DataFrame reads the table's latest
  version. A job that needs a fixed version passes `versionAsOf` in `options`.
- **The slice is frozen by watermark, not by version.** A row of the slice that is deleted, or
  updated without moving its watermark, after `read()`, shows its new state in later actions.
- **`TIMESTAMP` bounds and the session time zone.** Spark renders a `TIMESTAMP` bound as the
  session time zone's wall clock and casts it back in the same zone. The session time zone must
  not change between `read()` and the actions on the DataFrame: Spark Connect analyses the
  DataFrame again at each action. In a zone with daylight saving time, a bound inside the hour
  that repeats when clocks go back can come back up to one hour early. Rows above it are then
  left for the next run, never lost. The default session time zone of Databricks is UTC.
- **Skipping files needs statistics.** Delta records statistics only for the first 32 columns by
  default (`delta.dataSkippingNumIndexedCols`). A watermark column outside them, or a value whose
  type makes Spark convert the column, is still filtered, but every file is read.
- **The bound query** reads the watermark column of every file that the lower predicate cannot
  skip, once per `read()`.
- **Watermark values** must have the column's type for an exact comparison. Naive datetimes are
  rejected.
- **Overwrites keep the previous version** in storage until `VACUUM`.
- **Schema changes** to an existing table happen only through Delta's options. Without them, a
  DataFrame with an extra column, or a replace with another schema, fails.
- **The created table is minimal.** It has no partitioning, clustering or properties; a job that
  needs them creates the table first.
- **A target that is not a Delta table** is not supported, and the writer does not detect one.
  What happens is Spark's: an append to a table in another format fails because the formats
  differ, and a write to a view, a materialized view or a streaming table is refused on
  Databricks. A replace of a table in another format can fail, or, in open-source Spark's session
  catalog, drop that table and create a Delta table in its place. Never point a writer at
  anything but a Delta table, or a name that does not exist yet.
- **Session settings apply.** The connectors override no Spark setting except
  `partitionOverwriteMode` on a replace.
- **Type conversion** is whatever Spark and Delta do. The connectors convert nothing.
- Delta's change data feed, partial replaces (`replaceWhere`, dynamic partition overwrite), merges
  and deletes, DDL other than creating a missing table, one-part and two-part names, and
  streaming are not covered.

### BigQuery: `BigQueryReader`, `BigQueryWriter`

```python
from ingestio.connector.all import BigQueryReader, BigQueryWriter
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read and
write Google BigQuery tables, named by `project.dataset.table`, through Spark's BigQuery data
source (spark-bigquery-connector): the data moves between BigQuery and the Spark executors.
`BigQueryReader` returns a `pyspark.sql.DataFrame` and `BigQueryWriter` receives one, so they
satisfy `Reader[DataFrame]` and `Writer[DataFrame]`, structurally, and `ReadSource` and
`WriteTarget` use them unchanged. They follow the [PostgreSQL
connectors](#postgresql-postgresconnection-postgresreader-postgreswriter) and the [Unity Catalog
connectors](#unity-catalog-unitycatalogreader-unitycatalogwriter): a table is read incrementally
by a watermark with a frozen upper bound, a write is atomic through a staging table, and an
ingestion's parameters are [constructor arguments](#how-a-connector-receives-an-ingestions-parameters).

The native client, google-cloud-bigquery, runs on the Spark driver for control work only: the
bound query, and the creation, application and removal of the staging table. It comes with the
[`bigquery` extra](#extras). pyspark and spark-bigquery-connector are prerequisites that the
runtime provides, and pyspark is imported only when it is needed. Credentials come from the
environment (Application Default Credentials), the project billed can differ from the table's,
and a job can pass its own client. The connectors read no file, environment variable or secret
themselves.

#### `BigQueryReader`

```text
BigQueryReader(
    table_name: str,
    *, watermark_column: str | None = None,
    watermark_value: int | Decimal | str | date | None = None,
    billing_project: str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    client: bigquery.Client | None = None,
    spark: SparkSession | None = None,
)

BigQueryReader.read() -> DataFrame
```

- `table_name`: the source table, `project.dataset.table`. The configuration key of the same name.
- `watermark_column`: the column that orders changes. The configuration key of the same name.
- `watermark_value`: the last watermark already ingested. Only rows above it are read. Requires
  `watermark_column`. `datetime.datetime`, a subclass of `date`, is accepted too.
- `billing_project`: the project billed for the read and for the bound query. The default project
  of the environment's credentials when omitted.
- `options`: options of Spark's BigQuery data source passed through, such as `maxParallelism` or
  `readDataFormat`.
- `client`: the `google.cloud.bigquery.Client` for the bound query. A new one is created at each
  watermark read when omitted.
- `spark`: the session to read with. The active session when omitted.

**Building.** Building a connector validates its arguments, checks that google-cloud-bigquery can
be imported when the connector will need it, and does nothing else: it creates no client, calls no
method of a client it is given, touches no Spark session and makes no network call. The writer
always needs google-cloud-bigquery; the reader only when `watermark_column` is given, so a reader
for full reads works without the extra. When the import fails, building raises `ImportError`,
chained to the original error:

- `BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`
- `BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`

Invalid arguments raise `ValueError` when the connector is built:

| Connector | Case | Message |
|---|---|---|
| both | `table_name` does not have exactly three non-empty parts separated by `.` | `table_name must be 'project.dataset.table'` |
| both | `billing_project == ""` | `billing_project must not be empty` |
| both | a reserved key in `options`, in any case | `option '<key>' is set by the connector` |
| reader | `watermark_column == ""` | `watermark_column must not be empty` |
| reader | `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| reader | `bool`, `float` or any other type | `unsupported watermark_value type: <type name>` |
| reader | a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| reader | a naive `datetime` | `watermark_value must be timezone-aware` |

`<key>` is the key as written and `<type name>` is `type(value).__name__`. A datetime is naive when
its `tzinfo` is `None` or its `utcoffset()` is `None`. `repr` shows the table, and nothing else:

```text
BigQueryReader(table_name='acme-data.sales.orders')
BigQueryWriter(table_name='acme-data.lake.orders')
```

**Names and quoting.** `table_name` is `project.dataset.table`, on the reader and on the writer.
Domain-scoped project IDs, which contain a `.`, and the forms `dataset.table` and `table` are not
supported: Spark and the client would each resolve a short name against their own default project,
and could resolve it to different ones. The parts are kept as given. In SQL, every part of a table
name and every column name is a GoogleSQL quoted identifier, wrapped in backticks, with each `\`
and each `` ` `` inside preceded by a backslash: `acme-data.sales.orders` becomes
`` `acme-data`.`sales`.`orders` ``. BigQuery's own rules then apply: dataset and table names are
case-sensitive, column names are not. `watermark_column` is one top-level column.

**Reading.** `read()` returns a DataFrame built with `spark.read.format("bigquery")`. It is lazy:
the executors read the rows when an action runs on it. The reader uses `spark` when it is given,
and otherwise `SparkSession.active()`, looked up when `read()` is called, never when the reader is
built. It never creates a session; when none is active, PySpark's error propagates.

- **Full read** (no `watermark_column`): one load of the table, with the `table` option, and
  `parentProject` when `billing_project` is given. No client is created or used, even when one is
  given, and `read()` returns the DataFrame unchanged. Every action on it reads the table as it is
  then.
- **Watermark read** (`watermark_column` given): only the rows with a watermark above
  `watermark_value` (all of them when it is `None`) are read, and the filter runs inside BigQuery:
  Spark's BigQuery data source passes its `filter` option to the Storage Read API as the row
  restriction of the read session, so the rows outside the slice never leave BigQuery. `read()`
  first runs one query, through the client:

  ```text
  SELECT IF(MAX(<col>) IS NULL, NULL, FORMAT('%T', MAX(<col>))) AS upper_bound FROM <table>
  ```

  with ` WHERE <col> > <value>` when a value is given. `FORMAT('%T', x)` returns the text of a
  GoogleSQL literal of `x`'s type, such as `TIMESTAMP "2026-10-21 09:30:00.5+00"` or `41`, so the
  bound is the highest watermark of the slice as BigQuery's own literal, or `NULL` when no row
  qualifies. The bound goes back into the filter as it is, and never passes through Python or
  Spark types. The `filter` of the data load is `<col> > <value> AND <col> <= <bound>`, or
  `<col> <= <bound>` without a value, or `FALSE` when no row qualifies. The consequences:
  - every action on the DataFrame reads the same range of the watermark. Rows written after
    `read()` with a higher watermark are left for the next run, so a watermark that the job
    computes from the DataFrame never goes past what the DataFrame holds;
  - the comparison is strictly greater, and rows whose watermark is `NULL` are never extracted,
    the first run included;
  - a value that BigQuery cannot compare with the column fails the bound query, so `read()` raises
    BigQuery's error before any Spark load;
  - the bound query runs once per `read()`. A client that the reader created is closed once the
    bound is taken, also on failure; a client given is never closed.
- The reader decides no strategy. Whether a run is incremental, from which value, and how the next
  watermark is computed and kept, belong to the job. The bound that `read()` captured is not
  exposed.

**Partitioned and clustered sources.** When the table is partitioned by the watermark column
(time-unit column partitioning, or integer-range partitioning), nothing more is needed: the filter
compares the partitioning column with constants, so the bound query and the read session read only
the partitions that the predicate can match. A table clustered by the column lets BigQuery skip
blocks the same way. The value must have the column's type, or one GoogleSQL coerces to it, for
pruning as for comparing. A table partitioned by ingestion time, or by another column, is not
pruned by the watermark filter. A table with `require_partition_filter` partitioned by the
watermark column accepts a watermark read with a value; a full read and a watermark read without a
value fail with BigQuery's error.

**Watermark values.** `watermark_value` is "given" when it is not `None`: `0` and `""` are values.
It becomes a GoogleSQL literal of its own type:

| Type | Literal | Example |
|---|---|---|
| `int`, but not `bool` | `str(value)` | `41`, `-7` |
| finite `decimal.Decimal` | `BIGNUMERIC '<format(value, "f")>'` | `BIGNUMERIC '12.50'` |
| `str` | a string literal | `'O\'Brien'` |
| `datetime.date`, not a `datetime` | `DATE '<isoformat>'` | `DATE '2026-10-01'` |
| timezone-aware `datetime.datetime` | `TIMESTAMP '<UTC, isoformat(sep=" ")>'` | `TIMESTAMP '2026-10-01 08:30:00+00:00'` |

- GoogleSQL does not coerce a string literal to a number, so numbers need numeric literals, unlike
  PostgreSQL's. `BIGNUMERIC` holds every value that `NUMERIC` holds and more, and compares with
  `NUMERIC`, `BIGNUMERIC`, `INT64` and `FLOAT64` columns. A `Decimal` beyond its range fails the
  bound query with BigQuery's error.
- A string literal is wrapped in single quotes. Inside, each `\` becomes `\\`, each `'` becomes
  `\'`, each line feed `\n` and each carriage return `\r`. GoogleSQL escapes with backslashes: a
  doubled quote, as PostgreSQL writes it, is not an escape. A `str` compares with `STRING`
  columns, and with `DATE`, `DATETIME` and `TIMESTAMP` columns by GoogleSQL's coercion of string
  literals.
- An aware datetime is converted to UTC first, so its literal always ends in `+00:00`. On a
  `TIMESTAMP` column it is the same instant, whatever offset the value carried. GoogleSQL does not
  compare a `TIMESTAMP` with a `DATETIME`, so on a `DATETIME` column an aware datetime fails the
  bound query. A `DATETIME` column holds civil time with no zone: its value is a `str` in that
  civil time, such as `"2026-10-01 08:30:00"`.
- A column of another type fails the bound query with BigQuery's error. The connector maps no
  types: it only chooses the literal from the value's Python type. A job that takes its watermark
  from `collect()` gets naive datetimes: attach the zone, or pass a string.

**Options.** The reader's `options` are options of Spark's BigQuery data source, and it sets none
of its own. A user option replaces a connector option of the same name, with names compared
case-insensitively as Spark does. Spark turns the values into strings, and booleans into `true` and
`false`. Reserved keys raise `ValueError("option '<key>' is set by the connector")` when the
connector is built, in any case:

| Connector | Reserved keys | Why |
|---|---|---|
| both | `table`, `path`, `project`, `dataset` | The connector names the table. |
| both | `parentProject` | It is set from `billing_project`. |
| both | `credentials`, `credentialsFile`, `gcpAccessToken`, `gcpAccessTokenProvider` | Credentials come from the environment, never from the options (ADR 0013). |
| `BigQueryReader` | `filter` | The connector sets the watermark filter. |
| `BigQueryReader` | `query` | A query replaces the table, and cannot be bounded by a watermark. |
| `BigQueryReader` | `snapshotTimeMillis` | It reads a snapshot older than the one the bound query sees. |
| `BigQueryWriter` | `writeAtLeastOnce` | It gives up the exactly-once pending streams that load the staging table, so retried tasks could duplicate rows. |

Everything else passes through. For the reader: `maxParallelism`, `preferredMinParallelism`,
`readDataFormat`, `viewsEnabled`, `materializationProject`, `materializationDataset`. A view can be
read with `viewsEnabled` set to `true`; this is possible, not claimed or tested.

**Errors of a read.** Nothing is caught. Errors of PySpark, Spark, the client and BigQuery
propagate out of `read()` unchanged, the same object. The connectors write no log lines.

#### `BigQueryWriter`

```text
BigQueryWriter(
    table_name: str,
    *, billing_project: str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    client: bigquery.Client | None = None,
)

BigQueryWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame into a BigQuery table that already exists, atomically.

- `table_name`: the table to write into, `project.dataset.table`, with the rules above. It must
  exist: the writer runs no DDL on it.
- `billing_project`: the project billed for the load and for the statements that apply it.
- `options`: options of Spark's BigQuery data source passed through, such as `writeMethod` or
  `temporaryGcsBucket`. They replace the connector's defaults. The reserved keys are listed above.
- `client`: the client for the staging table and the statements. A new one is created at each write
  when omitted.

The writer takes no session: it uses `data.write`.

- `write(data)` appends: it inserts every row of `data` into the target and leaves the existing
  rows alone. `WriteTarget` calls `write(data)`, so it always appends.
- `write(data, replace=True)` replaces: in one statement it deletes every row of the target and
  inserts every row of `data`, so the target ends up holding exactly the rows of `data`. On a
  partitioned table this is every partition, not only those `data` touches. The target's
  definition is kept: its schema, partitioning, clustering, descriptions, policy tags, labels and
  options. Only its rows change.
- The calling callback chooses at call time, by passing `replace`. The configuration has no mode
  and there is no `replace()` method. Merges, upserts and replaces of only some partitions belong
  to callbacks.

**The target must exist.** A missing target fails, with BigQuery's error, and nothing is created.
`BigQueryWriter` does not use the permission of [ADR
0014](../decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md): creating a
BigQuery table is DDL on the target, and the writer runs none. It never creates, alters or drops
the target. The staging table below is the writer's own mechanism, created and removed within one
call, and is not the target.

**How a write happens.** Spark's BigQuery data source can append through the Storage Write API
(the direct method, which needs no Cloud Storage bucket) or through files in a bucket and a load
job (the indirect method), and what it does for an overwrite has changed across its versions, so
the writer never lets it write into the target. It makes the write atomic with a staging table:

0. A DataFrame without columns raises `ValueError("the DataFrame has no columns")` and nothing else
   happens.
1. **Client.** `client` when given; otherwise `google.cloud.bigquery.Client(project=billing_project)`,
   created for this write.
2. **Setup.** The writer gets the target's metadata (`client.get_table`) and matches each
   DataFrame column with the target's field of the same name, case-insensitively, as BigQuery does.
   The first column the target lacks raises `ValueError("table '<table_name>' has no column
   '<name>'")`, with the name as the DataFrame spells it. It then creates the staging table.
3. **Load.** Spark appends the DataFrame into the staging table (`writeMethod` defaults to
   `direct`) in parallel. The target is not touched.
4. **Apply.** One DML statement copies the staging table into the target. With `replace=False`:

   ```text
   INSERT INTO <target> (<cols>) SELECT <cols> FROM <staging>
   ```

   and with `replace=True`:

   ```text
   MERGE <target> AS T USING <staging> AS S ON FALSE WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (<S.cols>) WHEN NOT MATCHED BY SOURCE THEN DELETE
   ```

   In the `MERGE` no row matches, so every row of the target is deleted and every row of the
   staging table is inserted, in one statement.
5. **Drop.** The staging table is deleted.
6. A client that the writer created is closed after everything else, whether the write succeeded
   or failed. A client given is never closed.

**The staging table.** Its name is `ingestio_staging_` followed by 32 hexadecimal digits, new on
every call. It is created in the target's project and dataset, so it lives in the target's location,
as the statement that applies it requires, and the job needs no dataset of its own for it. It
expires one day after it is created, so BigQuery removes it even when the writer cannot. Its schema
has one field per DataFrame column, in the DataFrame's order, built from the target's field: same
type, nested fields and type parameters (maximum length, precision, scale), mode `NULLABLE` where
the target's is `REQUIRED` (`NULLABLE` and `REPEATED` are kept), and no description, default value
or policy tag. Spark's rows therefore land with the types the target expects, and only the apply
enforces the target's rules: its `REQUIRED` columns, and the default values of the columns the
DataFrame lacks, which `INSERT` and `MERGE` fill in. This also means a DataFrame whose fields are
nullable, as most are, can be written into a target with `REQUIRED` columns.

**Atomicity.** Steps 1 to 3 never touch the target. Step 4 is one DML statement, which BigQuery
commits atomically: readers of the target see none of the new rows until it commits, and then all
of them; with `replace=True`, they see the old rows until then, and never an empty or partial
table. After a failure at any step, the target is as it was. The write method that loads the staging
table plays no part in this, so the indirect method is as atomic as the direct one.

**Failures.** Every error propagates out of `write()`, with one exception described below.

- When the setup fails (the target is missing, a column is missing, no permission to create
  tables), nothing was created and the error propagates.
- When the load or the apply fails, the target is as it was, and the writer deletes the staging
  table. The original exception propagates, the same object. A statement that BigQuery rejects
  raises from the job's result.
- When that cleanup fails too, the original exception gets one note, added with `add_note`:
  `staging table <staging id> could not be dropped: <type>: <message>`. The cleanup error is not
  raised.
- If the driver process dies, the cleanup cannot run, and the staging table stays until it
  expires.

**The one error these connectors do not raise.** When the apply has committed and only the removal
of the staging table fails, the writer raises nothing: `write` returns `None`, the rows are in the
target, and the staging table expires on its own a day later. Raising would make a committed append
look failed, and a job that retries it would write the rows twice. The failure is not reported
anywhere, because the connectors write no log lines. This is the only case in which a connector of
this library swallows an error, and it concerns housekeeping after a write that has succeeded.

**What the writer does not do.** It reads only `data.columns` and `data.write` from the DataFrame,
and the save evaluates the DataFrame once. It never counts or inspects rows. An empty DataFrame
goes through the same steps: an append inserts nothing, and `replace=True` leaves the target empty.
A callback that wants to skip empty batches checks for itself and raises `CancelStepException`. It
sets no job configuration, label or location on its statements, and it does not retry.

#### Costs

What a read or a write costs is BigQuery's pricing, and the connectors rely on none of it. For
orientation:

- the bound query is a query job, run once per watermark `read()`, billed on the bytes of the
  watermark column in the partitions it reads, or in slots under capacity pricing;
- each action on a DataFrame is a new read session, billed by the Storage Read API on the data it
  reads;
- a write loads the batch once into the staging table, through the Storage Write API or, with the
  indirect method, a load job, and then applies it with a DML statement billed as a query: an
  `INSERT` reads the staging table, a `MERGE` reads the staging table and the target;
- the staging table is stored for the length of the write.

#### Example

This runs as written, because building the objects creates no client, calls no method of one and
touches neither Spark nor BigQuery. It needs google-cloud-bigquery (the [`bigquery`
extra](#extras)) and no credentials. Reading and writing need a Spark session with
spark-bigquery-connector and Google credentials.

```python
from datetime import UTC, datetime
from decimal import Decimal

from ingestio.connector.all import BigQueryReader, BigQueryWriter

reader = BigQueryReader(
    "acme-data.sales.orders",
    watermark_column="updated_at",
    watermark_value=datetime(2026, 10, 1, tzinfo=UTC),
    billing_project="acme-billing",
    options={"maxParallelism": 8},
)
writer = BigQueryWriter(
    "acme-data.lake.orders",
    options={"writeMethod": "indirect", "temporaryGcsBucket": "acme-staging"},
)
print(reader)
print(writer)

for build in (
    lambda: BigQueryReader("sales.orders"),
    lambda: BigQueryReader("acme-data.sales.orders", billing_project=""),
    lambda: BigQueryReader("acme-data.sales.orders", watermark_value=41),
    lambda: BigQueryReader(
        "acme-data.sales.orders", watermark_column="amount", watermark_value=1.5
    ),
    lambda: BigQueryReader(
        "acme-data.sales.orders", watermark_column="amount", watermark_value=Decimal("NaN")
    ),
    lambda: BigQueryReader(
        "acme-data.sales.orders", watermark_column="updated_at", watermark_value=datetime(2026, 10, 1)
    ),
    lambda: BigQueryReader("acme-data.sales.orders", options={"filter": "id > 1"}),
    lambda: BigQueryWriter("acme-data.lake.orders", options={"WriteAtLeastOnce": True}),
):
    try:
        build()
    except ValueError as error:
        print(error)
```

A job on Databricks: the configuration and the watermark stay in the job, the credentials come from
the cluster's environment, and one callback chooses to replace.

<!-- example: skip -->
```python
import json
from pathlib import Path

from ingestio import Callback, Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import BigQueryReader, BigQueryWriter

config = json.loads(Path("/Workspace/jobs/orders.json").read_text())
# {"table_name": "acme-data.sales.orders", "watermark_column": "updated_at", ...}

reader = BigQueryReader(
    config["table_name"],
    watermark_column=config["watermark_column"],
    watermark_value=last_watermark,  # an aware datetime, kept by the job
    billing_project="acme-billing",
)


class ReplaceTarget(Callback):
    """A full load: the pipeline's choice, made in a callback."""

    def __init__(self, writer: BigQueryWriter, key: str) -> None:
        self.writer = writer
        self.key = key

    def run(self) -> None:
        self.writer.write(self.process.state[self.key], replace=True)


Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [
        WriteTarget(BigQueryWriter("acme-data.lake.orders", billing_project="acme-billing"),
                    key="orders"),
        ReplaceTarget(BigQueryWriter("acme-data.mart.orders_snapshot"), "orders"),
    ]),
]).run()
```

A job that does not run with the right default credentials builds its own client and passes it:
`BigQueryReader(..., client=bigquery.Client.from_service_account_info(info))`. Spark then needs the
same identity from its own environment: the client and Spark each find the environment's
credentials on their own.

#### Environments of the BigQuery connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- spark-bigquery-connector is a prerequisite too. Databricks Runtime includes it, as the `bigquery`
  format. Open-source Spark needs the connector's build for its Spark version on the classpath, for
  example `spark.jars.packages=com.google.cloud.spark:spark-3.5-bigquery:<version>`. The minimum
  version is 0.24.2, because earlier versions could delete a table; every build for Spark 3.5 is
  later. The connectors rely only on the `table`, `filter`, `parentProject` and `writeMethod`
  options, and on appending into an existing table.
- Databricks classic compute on Databricks Runtime 15.4 LTS or later, in dedicated access mode,
  with the bundled connector. Standard access mode and serverless compute: no claim, not tested.
  Only the DataFrame API is used, never `sparkContext` or `_jvm`.
- Open-source Spark 3.5 or later, on a Java version that release supports (17 or 21 for Spark 4).
- **Credentials.** Application Default Credentials must reach the driver, for the client and for
  Spark's driver-side calls, and every executor, for the read and write streams. On Databricks on
  Google Cloud, a Google service account attached to the cluster provides them. Elsewhere, the
  cluster's environment must provide them, for example through `GOOGLE_APPLICATION_CREDENTIALS`
  pointing to a key file present on every node.
- **Network.** The driver reaches the BigQuery API, and the executors reach the Storage Read and
  Storage Write APIs.
- **Privileges.** The reader needs to read the table (BigQuery Data Viewer), to create read
  sessions in the billing project (BigQuery Read Session User) and, for watermark reads, to run
  jobs there (BigQuery Job User). The writer needs to get the target, to create, write, read and
  delete tables in its dataset (BigQuery Data Editor), and to run jobs in the billing project
  (BigQuery Job User). The indirect method also needs the bucket.

The repository's opt-in integration tests of these connectors run in two tiers. The first needs
`INGESTIO_BIGQUERY_DATASET`, set to `project.dataset` for an existing dataset where the tests may
create and drop tables, and credentials in the environment; it uses a real client with fake Spark
objects, so it needs no JVM. The second, with a local Spark session, also needs
`INGESTIO_SPARK_TESTS=1` and `INGESTIO_BIGQUERY_SPARK_PACKAGE`, the Maven coordinate of a
spark-bigquery-connector build for the installed pyspark. A JVM loads `spark.jars.packages` only
when it starts, so the second tier must run in its own process: run
`tests/test_bigquery_connectors.py` on its own, because those tests skip when another test module
of the same run has already started a JVM without the connector.

#### Limitations of the BigQuery connectors

The connectors were written without access to BigQuery or to spark-bigquery-connector, and the
integration tests are the check. Not yet verified: that the Storage Read API's row restriction
accepts the typed literals and the literals that `FORMAT('%T')` returns; whether Spark's pushed-down
filters are combined with the `filter` option or replaced by it (the result is the same, only the
amount read differs); that the direct method's append is atomic and exactly once; that the
connector bundled with Databricks Runtime supports the direct method (where it does not, pass
`writeMethod=indirect` and a bucket); that an empty DataFrame can be written with the direct
method; and whether rows still in a table's streaming buffer are returned by the Storage Read API.

Reading:

- **Late commits.** A row written after a read, with a watermark at or below the bound that read
  captured, is never extracted by a later run, as with the PostgreSQL reader. Rows that share the
  exact value of the previous watermark but arrive later are missed the same way.
- **The range is frozen, not the table.** The bound query runs when `read()` is called, and each
  action opens its own read session on the table as it is then. Rows updated or deleted in
  between, and late commits at or below the bound, move into or out of the slice. The reader pins
  no snapshot. A full read is not frozen at all.
- **`DATETIME` columns** need a `str` value. **`FLOAT64` columns**: the bound's text may not
  compare equal to the stored value, so the row holding the highest watermark could be missed.
  `BOOL`, `BYTES`, `TIME` and nested fields are not supported as watermarks.
- **`require_partition_filter`** makes full reads and first runs without a value fail.
- Every action reads BigQuery again, and is billed again. Caching is the job's choice.
- The types of the rows read are Spark's data source's: `DATETIME`, for instance, arrives as a
  timestamp without time zone or as a string, depending on its version.

Writing:

- **`require_partition_filter`** on the target makes `replace=True` fail, because the `MERGE` reads
  the target without a filter on its partitioning column. Appends work.
- **Streaming buffer.** `replace=True` fails when the target holds rows still in the streaming
  buffer of the legacy streaming API, which DML cannot delete. The error propagates and the target
  is unchanged.
- **Concurrent DML.** BigQuery queues or fails mutating statements that run on the same table at
  the same time, which can make a `replace=True` fail when another job writes the target.
- **BigQuery's limits** on the partitions that one statement modifies apply to the apply.
- **Ingestion-time partitioned targets** receive the rows in the partition of the moment the apply
  runs.
- **The staging table** is readable by whoever can read tables in the target's dataset while it
  exists: the target's policy tags and row access policies do not apply to it. A write that lasts
  longer than a day loses its staging table and fails, leaving the target unchanged. A staging table
  whose removal fails stays until it expires.
- **Twice the data movement.** The batch is loaded into the staging table and then copied into the
  target by a statement, which costs more than a direct write into the target.
- **Type conversion** is Spark's data source's, into the staging table's types. The connectors add
  none of their own.
- A client given whose default query job configuration selects legacy SQL fails the statements,
  which are GoogleSQL. Spark's data source parses the `table` option itself, and may reject table
  names that BigQuery accepts, such as names with spaces.
- Time travel, change history, BigQuery change data capture, custom queries, column projection,
  views as first-class sources, job labels, locations and timeouts, credentials as arguments,
  upserts, merges by key and any DDL on the target are not covered. Databricks recommends Lakehouse
  Federation for read-only queries of BigQuery; these connectors are for jobs that write, that
  filter by watermark, or that run on open-source Spark.

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
  staging mechanism. With `delta` it writes a Delta table at the path, creating it when the path
  holds none; nothing is registered in a catalog, so it is not a table of Unity Catalog (the
  [Unity Catalog connectors](#unity-catalog-unitycatalogreader-unitycatalogwriter) write those
  by name). An empty DataFrame goes through the same call; what lands depends on
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
  Python and Spark resolve them to different file systems. The FUSE mount exists only on the
  cluster: a job that runs outside Databricks, through Databricks Connect, reads a volume with
  the [volume connectors](#volumes-volumereader-volumewriter), which list through the Files API.
  See [Disk or volume?](#disk-or-volume).
- Open-source Spark 3.5 or later, on a Java version that release supports, when Spark's default
  file system is the local one (`fs.defaultFS` is `file:///`, the default outside Hadoop
  clusters). That covers local mode, and clusters where `path` is on a file system mounted at
  the same place on the driver and on every executor.
- Databricks serverless compute: nothing in the design excludes it, but it is not tested.
- Cloud storage URIs (`s3://`, `abfss://`, `gs://`, `dbfs:/`) are rejected. Objects in S3 are
  read and written by the [S3 connectors](#s3-s3reader-s3writer), and blobs in Google Cloud
  Storage by the [GCS connectors](#gcs-gcsreader-gcswriter). A volume can also be read through
  the [volume connectors](#volumes-volumereader-volumewriter).
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
- **Glob characters.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in the paths it
  loads. A selected file whose path holds one can be read as other files, or fail. The reader
  does not escape them.

### S3: `S3Reader`, `S3Writer`

```python
from ingestio.connector.all import S3Reader, S3Writer
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read
and write objects in Amazon S3 and in S3-compatible stores, through Spark's file data sources,
with the contract of the [disk connectors](#disk-diskreader-diskwriter): the reader returns a
`pyspark.sql.DataFrame` and the writer receives one. `S3Reader` satisfies `Reader[DataFrame]` and
`S3Writer` satisfies `Writer[DataFrame]`, structurally, so `ReadSource` and `WriteTarget` use
them unchanged. The arguments follow the [convention for an ingestion's
parameters](#how-a-connector-receives-an-ingestions-parameters): `path`, `format`,
`watermark_pattern` and `schema` are named after configuration keys, and the job computes
`watermark_value`.

The reader lists the objects directly under a prefix with boto3, on the driver, selects those
above the job's watermark before anything is loaded, and has Spark load exactly those objects.
boto3 does control work only: it lists. The writer is Spark writing straight to the URI, so it
needs no boto3. Everything not listed under [Differences from the disk
connectors](#differences-from-the-disk-connectors) is as described for `DiskReader` and
`DiskWriter`: hidden and in-progress names, the strict comparison, the sorted and frozen list,
the empty selection, `delta` rejected by the reader, the reserved options, the atomicity limits
of a write.

#### `S3Reader`

```text
S3Reader(
    path: str, format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    client: S3Client | None = None,
    spark: SparkSession | None = None,
)

S3Reader.read() -> DataFrame
```

- `path`: the prefix that holds the objects, as a URI: `s3://<bucket>/<prefix>` or
  `s3a://<bucket>/<prefix>`. Spark receives the scheme as written.
- `format`, `watermark_pattern`, `watermark_value`, `schema`, `options` and `spark`: as for
  `DiskReader`. Without `watermark_pattern`, an object's watermark is its `LastModified` time,
  to the second, and `watermark_value` is a timezone-aware `datetime`.
- `client`: the boto3 S3 client that lists the objects, such as
  `boto3.client("s3", region_name=..., endpoint_url=...)`. Its type is
  `mypy_boto3_s3.client.S3Client`, which users without the boto3 stubs see as `Any`. When
  omitted, each `read()` creates `boto3.client("s3")`, which needs the [`s3` extra](#extras).

**Building** validates the arguments and does nothing else: it creates no client and touches
neither Spark nor S3. The one exception is a reader built without `client`, which checks that
boto3 can be imported and otherwise raises
`ImportError("S3Reader needs boto3: pip install 'ingestio[s3]'")`. A reader given a `client`
imports no boto3, neither when it is built nor when it reads. Validation comes first, so an
invalid `path` raises `ValueError` even when boto3 is missing.

**Paths.** Both connectors take `path` apart the same way. Anything else raises `ValueError`:

| Case | Message |
|---|---|
| not a `str` that starts with `s3://` or `s3a://`, in lowercase: empty, local paths, `gs://`, `s3n://`, `https://`, `S3://`, `s3:/...`, a `pathlib.Path` | `path must be an s3:// or s3a:// URI` |
| no bucket: `s3://`, `s3:///landing/orders` | `path must name a bucket` |
| a prefix with an empty, `.` or `..` segment: `s3://lake//orders`, `s3://lake/./orders` | `path must not contain empty, '.' or '..' segments` |
| writer only, the bucket root: `s3://lake`, `s3a://lake/` | `path must name a prefix inside the bucket` |

- The bucket is the text between `://` and the next `/`. It is not validated further: S3 rejects
  a name it does not know when the reader lists, and Spark when it reads or writes.
- The connectors keep the normalised path, `<scheme>://<bucket>` followed by `/<prefix>`, with
  every trailing `/` removed: `s3://lake/landing/orders/` becomes `s3://lake/landing/orders`.
- The reader accepts the bucket root. The writer does not, because `replace=True` would remove
  every object of the bucket.
- Empty, `.` and `..` segments are rejected because S3 keeps such keys as they are, while
  Hadoop's paths collapse `//` and resolve `.` and `..`, so the listing and Spark would name
  different objects.
- The scheme is the job's choice. On Databricks, `s3://` is the usual one; on open-source Spark
  with hadoop-aws, `s3a://`. The connectors do not know where they run and do not choose.
- `os.PathLike` is not accepted, because `pathlib` collapses the `//` of a URI.

The other rules of building, `format`, `watermark_pattern`, `watermark_value` and the reserved
option keys, are those of the disk connectors, with the same messages. The reserved keys are the
same too: `path` and `paths` on both, `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`
and `modifiedAfter` on the reader, `replaceWhere` and `partitionOverwriteMode` on the writer.
There are no default options.

`repr` shows the normalised path and the format, never the client:

```text
S3Reader(path='s3://lake/landing/orders', format='csv')
S3Writer(path='s3://lake/orders', format='parquet')
```

**How a read selects objects.** Each call to `read()` goes through these steps, and the
selection is made before Spark loads anything:

1. **Session.** `spark` when given. Otherwise `SparkSession.active()`, looked up when `read()`
   is called. The reader never creates a session; when none is active, PySpark's error
   propagates and nothing is listed.
2. **Client.** `client` when given, as it is, for every read. Otherwise `boto3.client("s3")`,
   created anew at each `read()`; the reader keeps none and closes none.
3. **Listing.** One listing per `read()`, on the driver: the `list_objects_v2` paginator of the
   client, with the bucket, the prefix followed by `/` (empty at the bucket root) and
   `Delimiter="/"`. The reader takes every page before anything is loaded, and reads only the
   `Key` and `LastModified` of each entry. Common prefixes, the "subdirectories" that the
   delimiter groups, are ignored with everything under them: the listing is flat. The reader
   opens, downloads and inspects no object, and calls no other operation.
4. **Candidates.** The name of an entry is its key after the listing prefix. An entry is a
   candidate when its name is not empty (the key that equals the prefix is the directory marker
   that consoles create), contains no `/`, starts with neither `_` nor `.` and does not end with
   `._COPYING_`. Every storage class is a candidate.
5. **Watermark of each candidate**, as below.
6. **Selection.** The candidates whose watermark is strictly greater than `watermark_value`, or
   all of them when it is `None`.
7. **Load.** One load of the selection, with `format`, `schema` and `options` as given and no
   other option. Spark receives a `list` of `str`, one per selected object,
   `<normalised path>/<name>` such as `s3://lake/landing/orders/a.csv`, with the scheme the job
   wrote, sorted by name in code-point order, even for one object.

**The watermark of an object.**

- *With `watermark_pattern`.* As for files: the pattern is matched against the whole name, not
  the key, with `re.fullmatch`, and the watermark is the text its group captured. Watermarks
  compare as text and are never converted.
- *Without `watermark_pattern`.* The watermark is the object's `LastModified`, truncated to
  whole seconds and compared with `watermark_value` as an instant, whatever the value's zone.
  The name plays no part. S3 keeps `LastModified` to the second; some S3-compatible stores list
  finer times, and the truncation brings them to the same rule. Spark's
  `_metadata.file_modification_time` is never below that second, so a job that takes its next
  watermark from that column never selects its last object again. The value itself is not
  truncated: an object at `09:00:00` is above `08:59:59.999999` and not above `09:00:00.5`.

**The selection is frozen and not exposed,** as for files: Spark receives the selected objects,
never the prefix, so objects that appear after `read()` are left for the next run. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column.

**The empty selection.** When the listing holds no object, no candidate, no name that matches
the pattern, or no watermark above the value, `read()` makes no load and returns an empty
DataFrame, with the columns of `schema` when it is given and no columns otherwise, and no
`_metadata` column. A prefix that holds no object cannot be told from a prefix that does not
exist, so a typo in the prefix reads as empty. A bucket that does not exist, or a listing that S3
denies, fails with boto3's error.

**Errors.** Nothing is caught, and the connectors write no log lines. Errors of PySpark, of
`boto3.client`, of the listing (botocore's `ClientError` for `NoSuchBucket` or `AccessDenied`,
`NoCredentialsError`, `EndpointConnectionError`) and of Spark propagate out of `read()`
unchanged, the same object. When the listing fails, nothing is loaded.

#### `S3Writer`

```text
S3Writer(
    path: str, format: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

S3Writer.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame as objects under a prefix, in the format it is given: the one call
chain of `DiskWriter`, `data.write.format(format).options(options).mode(mode).save(path)`, with
the normalised path as the URI. It takes no session and no client, imports no boto3, and lists,
creates and deletes nothing itself.

- `write(data)` appends: Spark adds new objects under the prefix and leaves the existing ones as
  they are.
- `write(data, replace=True)` replaces: the prefix ends up holding exactly the rows of `data`.
  With a plain file format, Spark deletes every object under the prefix, objects it did not
  write included, and then writes the new ones. With Delta, the overwrite is a new version of
  the table.
- The calling callback chooses at call time, by passing `replace`. The configuration has no
  mode and there is no `replace()` method. `WriteTarget` calls `write(data)`, so it always
  appends.
- The object names are Spark's (`part-<n>-<uuid>-c000...`), so an `S3Reader` that reads them
  back takes their watermark from their `LastModified`. Spark may also leave directory markers
  and `_SUCCESS`, which the reader ignores.
- The writer does not partition its output, calls no `saveAsTable` or `insertInto`, and has no
  staging mechanism. An empty DataFrame goes through the same call. Every error propagates out of
  `write()` unchanged.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what Spark's commit
protocol, the environment's committer and the format guarantee, and nothing more:

- *Append, plain file formats, open-source Spark.* S3 has no rename, and Hadoop's S3A emulates
  one by copying and deleting. With Hadoop's default `FileOutputCommitter`, tasks write under
  `_temporary/` inside the prefix, which the reader ignores, and the job commit copies each file
  to its final key, one by one. A reader that lists during the commit sees part of the batch,
  and a commit that fails midway leaves part of it. Hadoop's S3A committers, when the session
  enables them, make the objects visible only at the job commit, but still one object at a time.
  Choosing a committer is the environment's concern.
- *Replace, plain file formats.* Spark deletes the objects under the prefix before it writes. If
  the write then fails, the prefix is left empty or holding part of the new rows, and the
  previous contents are lost.
- *Delta.* An append or an overwrite is one commit in the table's log, so readers through Delta
  see the table before the commit or after it. On open-source Spark, Delta Lake's default S3 log
  store is safe only when every write to a table comes from one Spark driver at a time;
  concurrent writers on several drivers need Delta's multi-cluster setup. Databricks
  coordinates Delta commits on S3 itself.
- *On Databricks,* the runtime uses its own commit protocol for file formats. What it
  guarantees is the runtime's, and these connectors do not rely on it.

What the S3 connectors change next to the disk connectors, and what the GCS and volume ones
change, are in [one table](#differences-from-the-disk-connectors) after the Volumes section.

#### Examples

This runs as written, because building the objects creates no client of its own, lists nothing
and touches neither Spark nor S3. It needs boto3 (the [`s3` extra](#extras)) and no credentials.
Reading and writing need a Spark session and S3.

```python
from datetime import UTC, datetime

import boto3

from ingestio.connector.all import S3Reader, S3Writer

client = boto3.client("s3", region_name="eu-west-1")

by_name = S3Reader(
    "s3://acme-landing/orders/",
    "csv",
    watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020",
    options={"header": True},
    client=client,
)
by_time = S3Reader(
    "s3a://acme-landing/events",
    "json",
    watermark_value=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
    client=client,
)
writer = S3Writer("s3://acme-lake/orders", "parquet")
print(by_name)
print(by_time)
print(writer)

try:
    S3Reader("gs://acme-landing/orders", "csv", client=client)
except ValueError as error:
    print(error)

try:
    S3Writer("s3://acme-lake", "parquet")
except ValueError as error:
    print(error)

try:
    S3Reader("s3://acme-landing/orders", "csv", watermark_value=datetime(2026, 10, 21))
except ValueError as error:
    print(error)
```

A job on Databricks: the configuration, the credentials and the watermark stay in the job, and
the next watermark is taken from the objects that were read. `unix_seconds` truncates to the
second, as the reader does.

<!-- example: skip -->
```python
import json
from datetime import UTC, datetime
from pathlib import Path

import boto3
from pyspark.sql import functions as F

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import S3Reader, S3Writer

config = json.loads(Path("/Workspace/jobs/orders_s3.json").read_text())
# {"path": "s3://acme-landing/orders", "format": "json", ...}

reader = S3Reader(
    config["path"],
    config["format"],
    watermark_value=last_watermark,  # an aware datetime, kept by the job
    schema="id INT, amount DECIMAL(10,2), updated_at TIMESTAMP",
    # Optional: without it, each read creates boto3.client("s3").
    client=boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id=dbutils.secrets.get("aws", "key_id"),
        aws_secret_access_key=dbutils.secrets.get("aws", "secret"),
    ),
)

process = Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [WriteTarget(S3Writer("s3://acme-lake/orders", "delta"), key="orders")]),
]).run()

# The selection is not exposed: the job takes its next watermark from the objects it read.
orders = process.state["orders"]
if not orders.isEmpty():
    seconds = orders.select(F.max(F.unix_seconds("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime.fromtimestamp(seconds, UTC)
```

A job against an S3-compatible store passes, for instance,
`boto3.client("s3", endpoint_url="http://minio.internal:9000")`, and configures Spark's own
endpoint in the session, which is the environment's concern.

#### Environments of the S3 connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- Databricks classic compute on Databricks Runtime 15.4 LTS or later, in dedicated or standard
  access mode. Spark reaches S3 through Unity Catalog external locations, or an instance
  profile, as the workspace is set up; the job uses the scheme of its external location, usually
  `s3://`. Only the DataFrame API is used.
- **The listing does not go through Unity Catalog.** boto3 needs AWS credentials of its own on
  the driver, with `s3:ListBucket` on the bucket. What the default client finds depends on the
  cluster; when it finds nothing usable, the job builds a `client` from credentials it obtains
  itself, such as secrets.
- Open-source Spark 3.5 or later, on a Java version that release supports (17 or 21 for Spark
  4), with `hadoop-aws` in the version of Spark's Hadoop and its AWS SDK on the classpath, and
  `s3a://` URIs. `s3://` works there only when the session maps it, with
  `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`. Credentials, endpoint and
  committer go in the Hadoop configuration (`spark.hadoop.fs.s3a.*`).
- S3-compatible stores (MinIO, Cloudflare R2, Ceph): nothing in the design excludes them, but
  they are not tested. The store must support `ListObjectsV2` with a delimiter. The job points
  the client at it with `endpoint_url`, and Spark through its own configuration.
- Databricks serverless compute: no claim; not tested.
- Formats outside Spark's built-ins need their package in the session, as for the disk
  connectors.
- **Privileges.** The listing needs `s3:ListBucket`. Spark's reads need `s3:GetObject`; its
  writes need `s3:PutObject`, and `s3:DeleteObject` for replaces and for the copies of commits.
  On Databricks, through the external location's privileges.
- **Spark's access to S3 is the environment's concern.** The connectors configure neither Spark
  nor Hadoop, and create no bucket.

#### Limitations of the S3 connectors

- **Writes are only as atomic as Spark, the committer and the format make them** (see Atomicity
  above). A failed replace of plain files loses the previous contents, and a reader of the
  prefix during a write can see part of it.
- **`replace=True` deletes everything under the prefix** with a plain file format, objects Spark
  did not write included. Never point a writer that replaces at a prefix that holds anything
  else.
- **Delta with several writers on open-source Spark** needs Delta's multi-cluster log store.
- **Late objects.** A run that advances its watermark to the highest one it read never reads an
  object that appears later with a watermark at or below it. By name: a producer that delivers
  objects out of order. By `LastModified`: for a multipart upload, which boto3's `upload_file`
  uses for large files, S3 sets it to the time the upload was initiated, not the time it
  completed, so an upload that completes after a newer object was read is never read. Objects
  that share the last second read and appear after the read are skipped, because the comparison
  is strict and times are whole seconds. S3 stamps `LastModified` itself, so producers' clocks
  do not matter, and an object copied or moved into the prefix gets a new time.
- **Objects in progress.** S3 makes an object visible only when its upload completes, so the
  reader never sees part of an object. A Spark job that writes plain files into the prefix being
  read can still expose part of its batch, and on Databricks the runtime's commit protocol can
  write data files at their final keys before it marks them committed. The reader lists
  objects; it does not read commit markers.
- **The selection is frozen, not the contents.** An object overwritten before an action is read
  as it is then, or makes the action fail. A selected object that is deleted makes the action
  fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Archived objects.** An object in an archive storage class (S3 Glacier Flexible Retrieval,
  Deep Archive) is selected like any other, and Spark's load fails until it is restored.
- **Glob characters.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in the paths it loads.
  A selected object whose URI holds one can be read as other objects, or fail. The reader does
  not escape them.
- **Two configurations.** The client and Spark are configured separately. A client that lists
  one store or account while Spark reads another is not detected: the load fails, or reads other
  objects with the same keys.
- **Name watermarks compare as text,** as for files.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing prefix, a typo included, reads as empty.
- **Large prefixes.** Each `read()` lists every object directly under the prefix, 1000 keys per
  request, and Spark then requests the status of each selected object before reading it.
- **Table formats.** The reader rejects `delta`, but does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### GCS: `GcsReader`, `GcsWriter`

```python
from ingestio.connector.all import GcsReader, GcsWriter
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read
and write blobs in Google Cloud Storage (GCS) through Spark's file data sources, with the
contract of the [disk connectors](#disk-diskreader-diskwriter) and the design of the [S3
connectors](#s3-s3reader-s3writer): the reader returns a `pyspark.sql.DataFrame` and the writer
receives one. `GcsReader` satisfies `Reader[DataFrame]` and `GcsWriter` satisfies
`Writer[DataFrame]`, structurally, so `ReadSource` and `WriteTarget` use them unchanged. The
arguments follow the [convention for an ingestion's
parameters](#how-a-connector-receives-an-ingestions-parameters): `path`, `format`,
`watermark_pattern` and `schema` are named after configuration keys, and the job computes
`watermark_value`. GCS calls them objects; its Python SDK, and this page, call them blobs.

The reader lists the blobs directly under a prefix with google-cloud-storage, on the driver,
selects those above the job's watermark before anything is loaded, and has Spark load exactly
those blobs. google-cloud-storage does control work only: it lists. The writer is Spark writing
straight to the `gs://` URI, so it needs no google-cloud-storage. Everything not listed under
[Differences from the disk connectors](#differences-from-the-disk-connectors) is as described
for `DiskReader` and `DiskWriter`: hidden and in-progress names, the strict comparison, the
sorted and frozen list, the empty selection, `delta` rejected by the reader, the reserved
options, the atomicity limits of a write.

#### `GcsReader`

```text
GcsReader(
    path: str, format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    client: Client | None = None,
    spark: SparkSession | None = None,
)

GcsReader.read() -> DataFrame
```

- `path`: the prefix that holds the blobs, as a URI: `gs://<bucket>/<prefix>`.
- `format`, `watermark_pattern`, `watermark_value`, `schema`, `options` and `spark`: as for
  `DiskReader`. Without `watermark_pattern`, a blob's watermark is its creation time,
  `timeCreated`, to the millisecond, and `watermark_value` is a timezone-aware `datetime`.
- `client`: the `google.cloud.storage.Client` that lists the blobs, such as
  `storage.Client.from_service_account_info(info)`. When omitted, each `read()` creates
  `storage.Client(project=None)`, with Application Default Credentials, which needs the [`gcs`
  extra](#extras). `Client` is imported for type checking only.

**Building** validates the arguments and does nothing else: it creates no client and touches
neither Spark nor GCS. The one exception is a reader built without `client`, which checks that
google-cloud-storage can be imported and otherwise raises
`ImportError("GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'")`. A reader
given a `client` imports no google-cloud-storage, neither when it is built nor when it reads.
Validation comes first, so an invalid `path` raises `ValueError` even when the package is
missing.

**Paths.** Both connectors take `path` apart the same way. Anything else raises `ValueError`:

| Case | Message |
|---|---|
| not a `str` that starts with `gs://`, in lowercase: empty, local paths, `s3://`, `gcs://`, `https://`, `GS://`, `gs:/...`, a `pathlib.Path` | `path must be a gs:// URI` |
| no bucket: `gs://`, `gs:///landing/orders` | `path must name a bucket` |
| a prefix with an empty, `.` or `..` segment: `gs://lake//orders`, `gs://lake/./orders` | `path must not contain empty, '.' or '..' segments` |
| writer only, the bucket root: `gs://lake`, `gs://lake/` | `path must name a prefix inside the bucket` |

- The bucket is the text between `://` and the next `/`. It is not validated further: GCS rejects
  a name it does not know when the reader lists, and Spark when it reads or writes.
- The connectors keep the normalised path, `gs://<bucket>` followed by `/<prefix>`, with every
  trailing `/` removed: `gs://lake/landing/orders/` becomes `gs://lake/landing/orders`.
- The reader accepts the bucket root. The writer does not, because `replace=True` would remove
  every blob of the bucket.
- Empty, `.` and `..` segments are rejected because GCS keeps such names as they are, while
  Hadoop's paths collapse `//` and resolve `.` and `..`, so the listing and Spark would name
  different blobs.
- `gs://` is the one scheme, on open-source Spark and on Databricks alike. `os.PathLike` is not
  accepted, because `pathlib` collapses the `//` of a URI.

The other rules of building, `format`, `watermark_pattern`, `watermark_value` and the reserved
option keys, are those of the disk connectors, with the same messages and the same reserved
keys. There are no default options.

`repr` shows the normalised path and the format, never the client:

```text
GcsReader(path='gs://lake/landing/orders', format='csv')
GcsWriter(path='gs://lake/orders', format='parquet')
```

**How a read selects blobs.** Each call to `read()` goes through these steps, and the selection
is made before Spark loads anything:

1. **Session.** `spark` when given. Otherwise `SparkSession.active()`, looked up when `read()`
   is called. The reader never creates a session; when none is active, PySpark's error
   propagates and nothing is listed.
2. **Client.** `client` when given, as it is, for every read. Otherwise
   `google.cloud.storage.Client(project=None)`, created anew at each `read()`; the reader keeps
   none and closes none. `project=None` tells the SDK not to look for a project, which a
   listing does not use.
3. **Listing.** One listing per `read()`, on the driver:
   `client.list_blobs(<bucket>, prefix=<listing prefix>, delimiter="/")`, where the listing
   prefix is the prefix followed by `/` (empty at the bucket root). The reader iterates over the
   result to its end, every page, before anything is loaded, and reads only the `name` and
   `time_created` of each blob. The SDK's defaults apply, its retry policy included. Sub-prefixes,
   the "subdirectories" that the delimiter groups, are ignored with everything under them: the
   listing is flat. Only live blobs are listed: noncurrent versions and soft-deleted objects are
   left out. The reader opens, downloads and inspects no blob, and calls no other method of the
   client.
4. **Candidates.** The name of a blob is its full name after the listing prefix. A blob is a
   candidate when its name is not empty (the blob that equals the prefix is a directory
   placeholder that the Google Cloud console and the Cloud Storage connector for Hadoop
   create), contains no `/`, starts with neither `_` nor `.` and does not end with
   `._COPYING_`. Every storage class is a candidate. In a bucket with hierarchical namespace,
   folders and managed folders are not blobs and never appear.
5. **Watermark of each candidate**, as below.
6. **Selection.** The candidates whose watermark is strictly greater than `watermark_value`, or
   all of them when it is `None`.
7. **Load.** One load of the selection, with `format`, `schema` and `options` as given and no
   other option. Spark receives a `list` of `str`, one per selected blob,
   `<normalised path>/<name>` such as `gs://lake/landing/orders/a.csv`, sorted by name in
   code-point order, even for one blob.

**The watermark of a blob.**

- *With `watermark_pattern`.* As for files: the pattern is matched against the whole name, not
  the full name of the blob, with `re.fullmatch`, and the watermark is the text its group
  captured. Watermarks compare as text and are never converted.
- *Without `watermark_pattern`.* The watermark is the blob's `time_created`, the creation time
  of its current generation, stamped by GCS, truncated to whole milliseconds and compared with
  `watermark_value` as an instant, whatever the value's zone. The name plays no part, and
  neither does the `updated` time.
  - An overwrite creates a new generation with a new `timeCreated`, so an overwritten blob is
    selected again. A change of the blob's metadata alone moves `updated`, not `timeCreated`,
    so it does not select the blob again.
  - Spark's `_metadata.file_modification_time` is never below `timeCreated`, so a job that takes
    its next watermark from that column never selects its last blob again.
  - The value itself is not truncated: a blob at `09:00:00.500` is above `09:00:00.499999` and
    not above `09:00:00.500500`.

**The selection is frozen and not exposed,** as for files: Spark receives the selected blobs,
never the prefix, so blobs that appear after `read()` are left for the next run. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column.

**The empty selection.** When the listing holds no blob, no candidate, no name that matches the
pattern, or no watermark above the value, `read()` makes no load and returns an empty
DataFrame, with the columns of `schema` when it is given and no columns otherwise, and no
`_metadata` column. A prefix that holds no blob cannot be told from a prefix that does not
exist, so a typo in the prefix reads as empty. A bucket that does not exist fails with
`google.api_core.exceptions.NotFound`, and a listing that GCS denies with `Forbidden`.

**Errors.** Nothing is caught, and the connectors write no log lines. Errors of PySpark, of
`google.cloud.storage.Client` (google-auth's `DefaultCredentialsError` when Application Default
Credentials find nothing), of the listing and of Spark propagate out of `read()` unchanged, the
same object. When the listing fails, nothing is loaded.

#### `GcsWriter`

```text
GcsWriter(
    path: str, format: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

GcsWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame as blobs under a prefix, in the format it is given: the one call chain
of `DiskWriter`, `data.write.format(format).options(options).mode(mode).save(path)`, with the
normalised path as the URI. It takes no session and no client, imports no google-cloud-storage,
and lists, creates and deletes nothing itself.

- `write(data)` appends: Spark adds new blobs under the prefix and leaves the existing ones as
  they are.
- `write(data, replace=True)` replaces: the prefix ends up holding exactly the rows of `data`.
  With a plain file format, Spark deletes every blob under the prefix, blobs it did not write
  included, and then writes the new ones. With Delta, the overwrite is a new version of the
  table.
- The calling callback chooses at call time, by passing `replace`. The configuration has no
  mode and there is no `replace()` method. `WriteTarget` calls `write(data)`, so it always
  appends.
- The blob names are Spark's (`part-<n>-<uuid>-c000...`), so a `GcsReader` that reads them back
  takes their watermark from their `timeCreated`. The Cloud Storage connector may also leave
  directory placeholders and `_SUCCESS`, which the reader ignores.
- The writer does not partition its output, calls no `saveAsTable` or `insertInto`, and has no
  staging mechanism. An empty DataFrame goes through the same call. Every error propagates out
  of `write()` unchanged.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what Spark's commit
protocol, the environment's committer and the format guarantee, and nothing more:

- *Append, plain file formats, open-source Spark.* In a bucket with a flat namespace, GCS has
  no rename, and the Cloud Storage connector emulates one by copying each blob and deleting the
  original. With Hadoop's default `FileOutputCommitter`, tasks write under `_temporary/` inside
  the prefix, which the reader ignores, and the job commit copies each file to its final name,
  one by one. A reader that lists during the commit sees part of the batch, and a commit that
  fails midway leaves part of it. Choosing a committer and configuring the connector are the
  environment's concern.
- *Replace, plain file formats.* Spark deletes the blobs under the prefix before it writes. If
  the write then fails, the prefix is left empty or holding part of the new rows, and the
  previous contents are lost.
- *Delta.* An append or an overwrite is one commit in the table's log, so readers through Delta
  see the table before the commit or after it. Whether several Spark drivers can write one table
  at the same time on open-source Spark depends on Delta Lake's log store for `gs://`, as Delta
  Lake documents it; no claim is made here. Databricks coordinates Delta commits itself.
- *On Databricks,* the runtime uses its own commit protocol for file formats. What it
  guarantees is the runtime's, and these connectors do not rely on it.

#### Examples

This runs as written, because building the objects creates no client of its own, lists nothing
and touches neither Spark nor GCS. It needs google-cloud-storage (the [`gcs`
extra](#extras)) and no credentials, because the readers are built without a `client`, which
only checks that the package imports. Reading and writing need a Spark session and GCS.

```python
from datetime import UTC, datetime

from ingestio.connector.all import GcsReader, GcsWriter

by_name = GcsReader(
    "gs://acme-landing/orders/",
    "csv",
    watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020",
    options={"header": True},
)
by_time = GcsReader(
    "gs://acme-landing/events",
    "json",
    watermark_value=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
)
writer = GcsWriter("gs://acme-lake/orders", "parquet")
print(by_name)
print(by_time)
print(writer)

try:
    GcsReader("s3://acme-landing/orders", "csv")
except ValueError as error:
    print(error)

try:
    GcsWriter("gs://acme-lake", "parquet")
except ValueError as error:
    print(error)

try:
    GcsReader("gs://acme-landing/orders", "csv", watermark_value=datetime(2026, 10, 21))
except ValueError as error:
    print(error)
```

A job on Databricks: the configuration, the credentials and the watermark stay in the job, and
the next watermark is taken from the blobs that were read. `unix_millis` keeps the milliseconds,
as the reader does, without floating point.

<!-- example: skip -->
```python
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from google.cloud import storage
from pyspark.sql import functions as F

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import GcsReader, GcsWriter

config = json.loads(Path("/Workspace/jobs/orders_gcs.json").read_text())
# {"path": "gs://acme-landing/orders", "format": "json", ...}

reader = GcsReader(
    config["path"],
    config["format"],
    watermark_value=last_watermark,  # an aware datetime, kept by the job
    schema="id INT, amount DECIMAL(10,2), updated_at TIMESTAMP",
    # Optional: without it, each read creates storage.Client(project=None), from ADC.
    client=storage.Client.from_service_account_info(
        json.loads(dbutils.secrets.get("gcp", "ingest_service_account"))
    ),
)

process = Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [WriteTarget(GcsWriter("gs://acme-lake/orders", "delta"), key="orders")]),
]).run()

# The selection is not exposed: the job takes its next watermark from the blobs it read.
orders = process.state["orders"]
if not orders.isEmpty():
    millis = orders.select(F.max(F.unix_millis("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=millis)
```

A job that reaches GCS through another endpoint, such as Private Service Connect, passes
`storage.Client(client_options={"api_endpoint": ...})`, and configures Spark's own endpoint in
the session, which is the environment's concern.

#### Environments of the GCS connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- Databricks on Google Cloud, classic compute, on Databricks Runtime 15.4 LTS or later, in
  dedicated or standard access mode. Spark reaches GCS through Unity Catalog external
  locations, or a Google service account attached to the cluster, as the workspace is set up,
  with `gs://` URIs. Only the DataFrame API is used.
- **The listing does not go through Unity Catalog.** google-cloud-storage needs credentials of
  its own on the driver, with `storage.objects.list` on the bucket. What Application Default
  Credentials find depends on the cluster; when they find nothing usable, the job builds a
  `client` from credentials it obtains itself, such as a service account key kept in a secret
  scope. Only the driver needs the extra: the executors never import google-cloud-storage.
- Open-source Spark 3.5 or later, on a Java version that release supports (17 or 21 for Spark
  4), with the Cloud Storage connector for Hadoop on the classpath
  (`com.google.cloud.bigdataoss:gcs-connector`, the release for the session's Hadoop version,
  usually its shaded JAR). Its file system, registered for `gs`, and its credentials go in the
  Hadoop configuration (`spark.hadoop.*`), as the connector's release documents. Dataproc ships
  the connector: nothing in the design excludes it, but it is not tested.
- Databricks on AWS or Azure reaching GCS, and Databricks serverless compute: no claim; not
  tested.
- Formats outside Spark's built-ins need their package in the session, as for the disk
  connectors.
- **Privileges.** The listing needs `storage.objects.list`. Spark's reads need
  `storage.objects.get`; its writes need `storage.objects.create`, and `storage.objects.delete`
  for replaces, for overwrites and for the copies of commits. The connector may need more, such
  as `storage.buckets.get`, as its release documents. On Databricks, through the external
  location's privileges.
- **Spark's access to GCS is the environment's concern.** The connectors configure neither Spark
  nor Hadoop, and create no bucket.

#### Limitations of the GCS connectors

- **Writes are only as atomic as Spark, the committer and the format make them** (see Atomicity
  above). A failed replace of plain files loses the previous contents, and a reader of the
  prefix during a write can see part of it.
- **`replace=True` deletes everything under the prefix** with a plain file format, blobs Spark
  did not write included. Never point a writer that replaces at a prefix that holds anything
  else. In a bucket with a retention policy, or with blobs under a hold, the deletes fail, and
  so does the replace. With soft delete enabled on the bucket, the deleted blobs are kept, and
  billed, for the bucket's retention period.
- **Delta with several writers on open-source Spark:** no claim.
- **Late blobs.** A run that advances its watermark to the highest one it read never reads a
  blob that appears later with a watermark at or below it. By name: a producer that delivers
  blobs out of order. By `timeCreated`: GCS stamps it itself, so producers' clocks do not
  matter, and it is taken to be stamped when the upload completes; a blob moved into the prefix
  may keep its creation time, which the documentation does not establish, so a producer that
  moves blobs should check it or use name watermarks. Blobs that share the last millisecond
  read and appear after the read are skipped, because the comparison is strict. A selected
  blob whose metadata changes between `read()` and the action that reads it reports a later
  `_metadata.file_modification_time`, so a job that advances its watermark from that column
  skips the blobs created in between.
- **Overwrites and metadata changes.** A blob overwritten after a run read it is read again,
  whole, by the next run. A change of its metadata alone does not make it read again. With name
  watermarks, neither does.
- **Blobs in progress.** GCS makes a blob visible only when its upload completes, so the reader
  never sees part of a blob. A Spark job that writes plain files into the prefix being read can
  still expose part of its batch, and on Databricks the runtime's commit protocol can write
  data files at their final names before it marks them committed. The reader lists blobs; it
  does not read commit markers.
- **The selection is frozen, not the contents.** A blob overwritten before an action is read as
  it is then, or makes the action fail. A selected blob that is deleted makes the action fail,
  unless the job passes Spark's `ignoreMissingFiles`.
- **Cold storage classes.** Nearline, Coldline and Archive blobs are selected and read like any
  other, and each read of them is charged retrieval fees.
- **Not supported:** generations and noncurrent versions, soft-deleted objects, requester-pays
  buckets, customer-supplied encryption keys, recursive listings and partitioned layouts.
- **Glob characters.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in the paths it loads.
  A selected blob whose URI holds one can be read as other blobs, or fail. The reader does not
  escape them.
- **Two configurations.** The client and Spark are configured separately. A client that lists
  with other credentials, or through another endpoint, than Spark's is not detected: the load
  fails, or reads blobs the listing did not see.
- **Name watermarks compare as text,** as for files.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing prefix, a typo included, reads as empty.
- **Large prefixes.** Each `read()` lists every blob directly under the prefix, up to 1000 per
  request, and Spark then requests the status of each selected blob before reading it.
- **Table formats.** The reader rejects `delta`, but does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### Volumes: `VolumeReader`, `VolumeWriter`

```python
from ingestio.connector.all import VolumeReader, VolumeWriter
```

These two classes are exported by `ingestio.connector.all` only, not by `ingestio`. They read
and write files in a directory of a Databricks Unity Catalog volume through Spark's file data
sources, with the contract of the [disk connectors](#disk-diskreader-diskwriter) and the design
of the [S3 connectors](#s3-s3reader-s3writer): the reader returns a `pyspark.sql.DataFrame` and
the writer receives one. `VolumeReader` satisfies `Reader[DataFrame]` and `VolumeWriter`
satisfies `Writer[DataFrame]`, structurally, so `ReadSource` and `WriteTarget` use them
unchanged. The arguments follow the [convention for an ingestion's
parameters](#how-a-connector-receives-an-ingestions-parameters): `volume`, `path`, `format`,
`watermark_pattern` and `schema` are named after configuration keys, and the job computes
`watermark_value`.

The reader lists the files directly inside the directory through the Files API of the Databricks
SDK, on the driver, selects those above the job's watermark before anything is loaded, and has
Spark load exactly those files, given as `/Volumes/...` paths. The Databricks SDK does control
work only: it lists. The writer is Spark writing straight to the `/Volumes/...` path, so it
needs no SDK. Spark's access to the volume is Unity Catalog's, through the volume's privileges.
Everything not listed under [Differences from the disk
connectors](#differences-from-the-disk-connectors) is as described for `DiskReader` and
`DiskWriter`: hidden and in-progress names, the strict comparison, the sorted and frozen list,
the empty selection, `delta` rejected by the reader, the reserved options, the atomicity limits
of a write.

#### Disk or volume?

`DiskReader` and `DiskWriter` also read and write `/Volumes/...` on Databricks compute, but
`DiskReader` lists the directory with `os.scandir`, in the driver's own Python process, through
the volume's FUSE mount. Choose by where the job runs:

- **On Databricks compute,** both work. `DiskReader` and `DiskWriter` take a whole path
  (`/Volumes/main/landing/raw/orders`) and need nothing else; they also run on open-source Spark
  and with any other directory. `VolumeReader` and `VolumeWriter` take the volume and a path
  inside it, and the reader needs the Databricks SDK.
- **Outside Databricks, through Databricks Connect,** choose `VolumeReader`. A process outside
  the workspace has no `/Volumes` to list, and on Windows `/Volumes/...` is not even an absolute
  path, so `DiskReader` cannot list the files. `VolumeReader` lists through the Files API,
  wherever the process runs, and Spark resolves the paths on the cluster.
- **The writers differ little.** `VolumeWriter` makes the call chain of `DiskWriter` with the
  volume's path. What it adds is that it names its location the way the reader does, by the
  volume and a directory inside it, and that it works from any operating system.

#### `VolumeReader`

```text
VolumeReader(
    volume: str, path: str, format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    client: WorkspaceClient | None = None,
    spark: SparkSession | None = None,
)

VolumeReader.read() -> DataFrame
```

- `volume`: the volume, by its three-part name, `catalog.schema.volume`.
- `path`: the directory that holds the files, relative to the volume, such as `landing/orders`,
  or `""` for the volume's root.
- `format`, `watermark_pattern`, `watermark_value`, `schema`, `options` and `spark`: as for
  `DiskReader`. `schema` is the Spark schema of the files; the Unity Catalog schema is the
  middle part of `volume`. Without `watermark_pattern`, a file's watermark is its
  `last_modified` time, to the second, and `watermark_value` is a timezone-aware `datetime`.
- `client`: the Databricks SDK client that lists the directory, such as
  `WorkspaceClient(profile="prod")`. Its type is `databricks.sdk.WorkspaceClient`, which users
  without the SDK see as `Any`. When omitted, each `read()` creates `WorkspaceClient()`, with the
  SDK's default authentication, which needs the [`databricks` extra](#extras).

**Building** validates the arguments and does nothing else: it creates no client and touches
neither Spark nor the workspace. The one exception is a reader built without `client`, which
checks that databricks-sdk can be imported and otherwise raises
`ImportError("VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'")`. A
reader given a `client` imports no databricks-sdk, neither when it is built nor when it reads.
Validation comes first, so an invalid `volume` raises `ValueError` even when the SDK is missing.

**The volume and the path.** Both connectors take them apart the same way. Anything else raises
`ValueError`:

| Case | Message |
|---|---|
| `volume` is not a `str` that splits on `.` into exactly three parts, none empty and none containing `/`: `""`, `main.landing`, `a.b.c.d`, `main..raw`, `main.landing.r/aw`, `/Volumes/main/landing/raw`, a non-`str` | `volume must be 'catalog.schema.volume'` |
| `path` is not a `str`, or starts with `/`: `/orders`, a whole `/Volumes/main/landing/raw/orders`, a `pathlib.Path` | `path must be a str relative to the volume` |
| a `path` with an empty, `.` or `..` segment: `landing//orders`, `./orders`, `orders/..` | `path must not contain empty, '.' or '..' segments` |
| writer only, the volume's root: `""` | `path must name a directory inside the volume` |

- The three parts of `volume` are plain names: a part cannot contain a `.`, and backticks are
  not removed. They are not validated further: Unity Catalog rejects a name it does not know
  when the reader lists, and Spark when it reads or writes. Names are kept as written, never
  lowercased.
- Every trailing `/` of `path` is removed: `orders/` becomes `orders`. The empty path is the
  volume's root. The reader accepts it. The writer does not, because `replace=True` would remove
  everything the volume holds.
- The connectors keep the **location**, `/Volumes/<catalog>/<schema>/<volume>` followed by
  `/<path>` when the path is not empty. `VolumeReader("main.landing.raw", "orders/", "csv")` has
  the location `/Volumes/main/landing/raw/orders`. The location is what the reader lists and
  what Spark receives, without a scheme.
- Empty, `.` and `..` segments are rejected because Spark's paths collapse `//` and resolve `.`
  and `..`, which the Files API may do differently, and `..` would leave the volume: the
  listing and Spark could name different directories.
- `os.PathLike` is not accepted for `path`, because `pathlib` collapses the empty and `.`
  segments and, on Windows, turns `/` into `\`.
- In a job configuration the keys are `volume`, `path` and `format`. Its `path` is relative to
  the volume, while a disk job's `path` is absolute.

The other rules of building, `format`, `watermark_pattern`, `watermark_value` and the reserved
option keys, are those of the disk connectors, with the same messages and the same reserved
keys. There are no default options.

`repr` shows the volume, the normalised path and the format, never the client:

```text
VolumeReader(volume='main.landing.raw', path='orders', format='csv')
VolumeReader(volume='main.landing.raw', path='', format='json')
VolumeWriter(volume='main.lake.curated', path='orders', format='parquet')
```

**How a read selects files.** Each call to `read()` goes through these steps, and the selection
is made before Spark loads anything:

1. **Session.** `spark` when given. Otherwise `SparkSession.active()`, looked up when `read()`
   is called. The reader never creates a session; when none is active, PySpark's error
   propagates and nothing is listed.
2. **Client.** `client` when given, as it is, for every read. Otherwise `WorkspaceClient()`,
   created anew at each `read()` with no argument; the reader keeps none. The SDK resolves its
   configuration and credentials while the client is built, which may send requests, and its
   errors propagate.
3. **Listing.** One listing per `read()`, on the driver:
   `client.files.list_directory_contents(directory_path=<location>)`, with no other argument.
   The reader iterates over the result to its end, before anything is loaded, and reads only the
   `name`, `is_directory` and `last_modified` of each entry. The SDK follows the pages itself.
   A subdirectory is one entry, whose contents are never listed: the listing is flat. The reader
   opens, downloads and inspects no file, never gives the client to Spark, and calls no other
   operation.
4. **Candidates.** An entry is a candidate when its `is_directory` is not `True` (`False` and
   `None` both mean a file), and its `name` starts with neither `_` nor `.` and does not end
   with `._COPYING_`. That leaves out `_SUCCESS`, `_metadata`, `.crc` files and the
   `_started_*` and `_committed_*` markers that Databricks's commit protocol writes. A file's
   size plays no part: an empty file is a candidate.
5. **Watermark of each candidate**, as below.
6. **Selection.** The candidates whose watermark is strictly greater than `watermark_value`, or
   all of them when it is `None`.
7. **Load.** One load of the selection, with `format`, `schema` and `options` as given and no
   other option. Spark receives a `list` of `str`, one per selected file,
   `<location>/<name>` such as `/Volumes/main/landing/raw/orders/a.csv`, built from the location
   and the entry's `name`, sorted by name in code-point order, even for one file.

**The watermark of a file.**

- *With `watermark_pattern`.* As for files: the pattern is matched against the whole name, with
  `re.fullmatch`, and the watermark is the text its group captured. Watermarks compare as text
  and are never converted.
- *Without `watermark_pattern`.* The watermark is the entry's `last_modified`, which the Files
  API gives in milliseconds since the Unix epoch, truncated to whole seconds and compared with
  `watermark_value` as an instant, whatever the value's zone. The name plays no part. A volume's
  files are objects in the cloud storage behind it, and Spark's own time for a file,
  `_metadata.file_modification_time`, is to the second or finer, so a job that takes its next
  watermark from that column never selects its last file again. The value itself is not
  truncated: a file at `09:00:00` is above `08:59:59.999999` and not above `09:00:00.5`.

**The selection is frozen and not exposed,** as for files: Spark receives the selected files,
never the directory, so files that appear after `read()` are left for the next run. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column.

**The empty selection.** When the directory holds nothing, no candidate, no name that matches
the pattern, or no watermark above the value, `read()` makes no load and returns an empty
DataFrame, with the columns of `schema` when it is given and no columns otherwise, and no
`_metadata` column. Unlike S3, a volume has real directories, so a directory that does not exist
is not an empty selection: the listing fails with the SDK's `databricks.sdk.errors.NotFound`,
which also covers a `path` that names a file. A missing volume fails with `NotFound` or
`PermissionDenied`, as Unity Catalog answers.

**Errors.** Nothing is caught, and the connectors write no log lines. Errors of PySpark, of
`WorkspaceClient()` (the SDK's error when it finds no usable configuration), of the listing
(`NotFound`, `PermissionDenied`, `Unauthenticated`, network errors) and of Spark propagate out
of `read()` unchanged, the same object. When the listing fails, nothing is loaded.

#### `VolumeWriter`

```text
VolumeWriter(
    volume: str, path: str, format: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)

VolumeWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

Writes a Spark DataFrame as files in a directory of a volume, in the format it is given: the one
call chain of `DiskWriter`, `data.write.format(format).options(options).mode(mode).save(path)`,
with the location as the path. It takes no session and no client, imports no databricks-sdk, and
lists, creates and deletes nothing itself.

- `write(data)` appends: Spark adds new files to the directory and leaves the existing ones as
  they are.
- `write(data, replace=True)` replaces: the directory ends up holding exactly the rows of
  `data`. With a plain file format, Spark deletes everything under the directory, files it did
  not write included, and then writes the new files. With Delta, the overwrite is a new version
  of the table.
- The calling callback chooses at call time, by passing `replace`. The configuration has no
  mode and there is no `replace()` method. `WriteTarget` calls `write(data)`, so it always
  appends.
- Spark creates the directory, and its missing parents inside the volume, when they do not
  exist. A volume that does not exist, or that the session may not write, fails in Spark.
- The file names are Spark's (`part-<n>-<uuid>-c000...`), so a `VolumeReader` that reads them
  back takes their watermark from their `last_modified`. Databricks's commit protocol also
  leaves `_SUCCESS`, `_started_*` and `_committed_*` files, which the reader ignores.
- The writer does not partition its output, calls no `saveAsTable` or `insertInto`, and has no
  staging mechanism. An empty DataFrame goes through the same call. Every error propagates out
  of `write()` unchanged.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what the runtime's
commit protocol and the format guarantee, and nothing more:

- *Plain file formats.* On Databricks the runtime uses its own commit protocol, which can write
  data files at their final names and mark the files of each committed job with `_committed_*`
  markers. What it guarantees is the runtime's, and these connectors do not rely on it.
  `VolumeReader` does not read those markers.
- *Replace, plain file formats.* Spark deletes the files under the directory before it writes.
  If the write then fails, the directory is left empty or holding part of the new rows, and the
  previous contents are lost.
- *Delta.* An append or an overwrite is one commit in the table's log, so readers through Delta
  see the table before the commit or after it. A Delta table written into a volume is files in
  the volume, not a table of the catalog.

#### Examples

This runs as written, because building the objects creates no client of its own, lists nothing
and touches neither Spark nor the workspace. It needs databricks-sdk (the [`databricks`
extra](#extras)) and no credentials, because the readers are built without a `client`, which
only checks that the package imports. Reading and writing need a Spark session and a workspace.

```python
from datetime import UTC, datetime

from ingestio.connector.all import VolumeReader, VolumeWriter

by_name = VolumeReader(
    "main.landing.raw",
    "orders/",
    "csv",
    watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020",
    options={"header": True},
)
by_time = VolumeReader(
    "main.landing.raw",
    "events",
    "json",
    watermark_value=datetime(2026, 10, 21, 9, 0, tzinfo=UTC),
)
whole_volume = VolumeReader("main.landing.raw", "", "parquet")
writer = VolumeWriter("main.lake.curated", "orders", "parquet")
print(by_name)
print(by_time)
print(whole_volume)
print(writer)

try:
    VolumeReader("/Volumes/main/landing/raw/orders", "", "csv")
except ValueError as error:
    print(error)

try:
    VolumeWriter("main.lake.curated", "", "parquet")
except ValueError as error:
    print(error)

try:
    VolumeReader("main.landing.raw", "orders", "csv", watermark_value=datetime(2026, 10, 21))
except ValueError as error:
    print(error)
```

A job that runs outside Databricks and drives a cluster through Databricks Connect: the
configuration and the watermark stay in the job, the client is the job's own, and the next
watermark is taken from the files that were read. `unix_seconds` truncates to the second, as the
reader does.

<!-- example: skip -->
```python
import json
from datetime import UTC, datetime
from pathlib import Path

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import VolumeReader, VolumeWriter

config = json.loads(Path("jobs/orders_volume.json").read_text())
# {"volume": "main.landing.raw", "path": "orders", "format": "json", ...}

spark = DatabricksSession.builder.profile("prod").getOrCreate()

reader = VolumeReader(
    config["volume"],
    config["path"],
    config["format"],
    watermark_value=last_watermark,  # an aware datetime, kept by the job
    schema="id INT, amount DECIMAL(10,2), updated_at TIMESTAMP",
    # Optional: without it, each read creates WorkspaceClient() with the SDK's default
    # authentication.
    client=WorkspaceClient(profile="prod"),
    spark=spark,
)

process = Process([
    Step("extract", [ReadSource(reader, into="orders")]),
    Step("load", [WriteTarget(VolumeWriter("main.lake.curated", "orders", "parquet"), key="orders")]),
]).run()

# The selection is not exposed: the job takes its next watermark from the files it read.
orders = process.state["orders"]
if not orders.isEmpty():
    seconds = orders.select(F.max(F.unix_seconds("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime.fromtimestamp(seconds, UTC)
```

On a Databricks cluster the same job usually passes neither `client` nor `spark`: the reader
finds the active session, and `WorkspaceClient()` authenticates as the notebook or the job does.

#### Environments of the volume connectors

- pyspark 3.5 or later is a prerequisite that the runtime provides, or the pyspark of Databricks
  Connect. ingestio never installs it.
- Databricks classic compute on Databricks Runtime 15.4 LTS or later, in dedicated or standard
  access mode. Spark reads and writes `/Volumes/...` paths under the volume's privileges. Only
  the DataFrame API is used. The default client follows the SDK's own lookup, which on Databricks
  compute includes the notebook's or the job's own authentication; when it finds nothing usable,
  the job passes a `client`.
- Databricks Connect 15.4 or later, from a Python 3.11 process outside the workspace. The
  session is passed as `spark=`, or found by `SparkSession.active()` where PySpark returns the
  Databricks Connect session. The listing runs in the job's own process, through the Files API,
  with the SDK's configuration there: a profile in `~/.databrickscfg`, or the `DATABRICKS_*`
  environment variables. Spark resolves the paths on the cluster. Databricks Connect is a
  prerequisite of such jobs, never an extra: it replaces pyspark and cannot be installed next to
  it. Not tested here.
- Databricks serverless compute: nothing in the design excludes it, because it uses only the
  Files API and DataFrame calls that Spark Connect supports, but it is not tested.
- **Not open-source Spark.** The listing needs a Databricks workspace, and only Databricks's
  Spark resolves `/Volumes/...` to a volume. A directory mounted at `/Volumes` on open-source
  Spark is `DiskReader`'s case.
- Formats are Spark's built-ins and those that Databricks Runtime includes, such as Delta and
  Avro.
- **Privileges.** The listing runs as the client's identity, which needs `USE CATALOG` on the
  catalog, `USE SCHEMA` on the schema and `READ VOLUME` on the volume. Spark runs as the
  session's identity, which needs the same for its reads, and `WRITE VOLUME` for its writes.
  Unlike S3's, the listing goes through Unity Catalog.
- **Spark's access to the volume is Unity Catalog's.** The connectors configure neither Spark
  nor the workspace, and create no catalog, schema or volume.

#### Limitations of the volume connectors

- **Writes are only as atomic as the runtime's commit protocol and the format make them** (see
  Atomicity above). A failed replace of plain files loses the previous contents.
- **`replace=True` deletes everything under the directory** with a plain file format, files
  Spark did not write included. Never point a writer that replaces at a directory that holds
  anything else.
- **Uncommitted files.** Databricks's commit protocol can write data files at their final names
  and mark them as committed afterwards. A reader that lists the directory while a Spark job
  writes plain files into it, or after such a job failed, can select files of that job. The
  reader lists files; it does not read `_started_*` or `_committed_*` markers.
- **Late files.** A run that advances its watermark to the highest one it read never reads a
  file that appears later with a watermark at or below it. By name: a producer that delivers
  files out of order. By `last_modified`: a file that keeps an older time when it is moved or
  copied into the directory, which depends on the cloud storage behind the volume and on the
  tool; a large file that the storage stamps with the time its upload started rather than
  finished, as S3 does for an upload in parts. Files that share the last second read and appear
  after the read are skipped, because the comparison is strict and times are whole seconds.
- **The selection is frozen, not the contents.** A selected file that is overwritten before an
  action is read as it is then, or makes the action fail. A selected file that is deleted makes
  the action fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Glob characters.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in the paths it loads.
  A selected file whose path holds one can be read as other files, or fail. The reader does not
  escape them.
- **Two identities, two configurations.** The client and the Spark session are configured
  separately. A client that lists another workspace, or runs as an identity that sees other
  privileges than the session's, is not detected: the listing fails, or the load fails or reads
  what the session sees at those paths.
- **Name watermarks compare as text,** as for files.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing directory, a typo included, fails the read instead.
- **Large directories.** Each `read()` lists every entry of the directory through the Files API,
  page by page, and Spark then requests the status of each selected file before reading it.
- **Table formats.** The reader rejects `delta`, but does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### Differences from the disk connectors

What a store or a volume listed by a native client changes. Everything not in this table is as
described for the disk connectors. One table serves the connectors of every store: a new store
adds a column.

| Point | `DiskReader` / `DiskWriter` | `S3Reader` / `S3Writer` | `GcsReader` / `GcsWriter` | `VolumeReader` / `VolumeWriter` |
|---|---|---|---|---|
| Location | absolute local path, `str` or `PathLike` | `s3://` or `s3a://` URI, a `str`, scheme passed to Spark as written | `gs://` URI, a `str`, the one scheme | `volume`, `catalog.schema.volume`, and `path`, a `str` relative to the volume; kept as `/Volumes/<catalog>/<schema>/<volume>[/<path>]` |
| Rejected locations | relative paths and URIs | other schemes, no bucket, empty, `.` or `..` segments; the bucket root for the writer | other schemes, `gs://` not in lowercase, no bucket, empty, `.` or `..` segments; the bucket root for the writer | a `volume` that is not three non-empty parts or has a `/`; a `path` that is not a `str` or starts with `/`; empty, `.` or `..` segments; the volume's root for the writer |
| Listing | `os.scandir` of the directory | boto3 `ListObjectsV2` paginator, `Delimiter="/"`, every page | `client.list_blobs(<bucket>, prefix=..., delimiter="/")`, iterated to its end | `client.files.list_directory_contents(directory_path=<location>)`, every entry; the SDK pages |
| Name of an entry | the file name | the key after the listing prefix | the blob's full name after the listing prefix | the entry's `name` |
| Not candidates, besides hidden names | directories, broken links | empty names (directory markers), names with `/`, common prefixes | empty names (directory placeholders), names with `/`, sub-prefixes; folders and managed folders are not blobs | entries whose `is_directory` is `True` |
| Modification-time watermark | `st_mtime_ns`, truncated to milliseconds | `LastModified`, truncated to seconds | `time_created` (`timeCreated`), truncated to milliseconds; `updated` is not used | `last_modified`, in milliseconds, truncated to seconds |
| Missing location | `FileNotFoundError` | empty selection; a missing bucket fails in boto3 | empty selection; a missing bucket fails with `google.api_core.exceptions.NotFound` | the SDK's `databricks.sdk.errors.NotFound` propagates |
| Paths given to Spark | `str(Path(path) / name)` | `<URI>/<name>`, scheme as written | `<URI>/<name>` | `<location>/<name>`, a `/Volumes/...` path |
| Native client | none | `client`, or `boto3.client("s3")` created at each read | `client`, or `google.cloud.storage.Client(project=None)` created at each read | `client`, or `databricks.sdk.WorkspaceClient()` created at each read |
| Extra | none | `ingestio[s3]`, needed only without `client` | `ingestio[gcs]`, needed only without `client` | `ingestio[databricks]`, needed only without `client` |
| Writer | the same chain | the same chain, with the URI; no boto3 | the same chain, with the URI; no google-cloud-storage | the same chain, with the location; no SDK |
| Where it runs | Databricks classic compute (volumes, through FUSE) and open-source Spark on a local file system | Databricks classic compute and open-source Spark with hadoop-aws | Databricks on Google Cloud and open-source Spark with the Cloud Storage connector | Databricks classic compute and Databricks Connect; not open-source Spark |

## Known divergence

`JsonLinesWriter` decides two things the philosophy assigns to callbacks: the write strategy
(`append`) and the format (JSON Lines). Moving them out changes the public API, so it waits
for a spec. See [specs/append_only_callback.md](../../specs/append_only_callback.md).

Until that spec settles what `write` means, `WriteTarget` behaves differently with the
writers: it appends to a `PostgresWriter`, a `DiskWriter`, an `S3Writer`, a `GcsWriter`, a
`VolumeWriter`, a `UnityCatalogWriter` and a `BigQueryWriter`, whose `write(data)` appends, but
overwrites with a `JsonLinesWriter`, whose `write` replaces the file by default. `PostgresWriter`,
`DiskWriter`, `S3Writer`, `GcsWriter`, `VolumeWriter`, `UnityCatalogWriter` and `BigQueryWriter`
leave the choice to the calling callback, through `write(data, replace=...)`,
the opposite of `JsonLinesWriter(append=...)`.

## Extras

A third-party package that a connector needs becomes an optional extra, `ingestio[<name>]`, and
the core stays without dependencies ([ADR 0002](../decisions/0002-zero-dependency-core.md)).
The first extra is `ingestio[postgres]`, installed with `pip install 'ingestio[postgres]'`; it
defines the pattern every later extra follows. The extras so far:

| Extra | Holds | Needed by |
|---|---|---|
| `postgres` | `psycopg[binary]>=3.2` | `PostgresWriter` |
| `s3` | `boto3>=1.34` | `S3Reader`, only when the job passes no `client` |
| `gcs` | `google-cloud-storage>=2.10` | `GcsReader`, only when the job passes no `client` |
| `databricks` | `databricks-sdk>=0.20` | `VolumeReader`, only when the job passes no `client` |
| `bigquery` | `google-cloud-bigquery>=3.20` | `BigQueryWriter` always; `BigQueryReader` only for watermark reads |

The pattern:

1. An extra is declared in `[project.optional-dependencies]` and named after the system it
   integrates. `postgres` holds `psycopg[binary]>=3.2`; `dependencies` stays empty.
2. A module imports third-party packages only inside the functions that use them, and for
   annotations under `TYPE_CHECKING`. `ingestio` and `ingestio.connector.all` import without
   any extra installed.
3. A connector that needs the package, and does not have it, raises `ImportError` when it is
   built, with a message that names the extra: `pip install 'ingestio[<extra>]'`. A connector
   needs the package only when it would import it: an `S3Reader` given a `client` imports no
   boto3, and an `S3Writer` never does, so neither needs the `s3` extra. The same holds for
   `GcsReader` and `GcsWriter` and the `gcs` extra, and for `VolumeReader` and `VolumeWriter`
   and the `databricks` extra. The BigQuery connectors use the package for control work on the
   driver, so a `BigQueryWriter` always needs the `bigquery` extra and a `BigQueryReader` only
   when it is given a `watermark_column`: a reader for full reads imports nothing.
4. An engine that the runtime provides, such as pyspark on Databricks, is never a dependency of
   an extra. It is a documented prerequisite: here, pyspark 3.5 or later. Delta Lake is one too,
   so the Unity Catalog connectors add no extra.
5. The `dev` dependency group holds the packages of every extra, so that `uv sync` gives an
   environment where the whole suite and mypy run.
