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

## Known divergence

`JsonLinesWriter` decides two things the philosophy assigns to callbacks: the write strategy
(`append`) and the format (JSON Lines). Moving them out changes the public API, so it waits
for a spec. See [specs/append_only_callback.md](../../specs/append_only_callback.md).

Until that spec settles what `write` means, `WriteTarget` behaves differently with the two
writers: it appends to a `PostgresWriter`, whose `write(data)` appends, but overwrites with a
`JsonLinesWriter`, whose `write` replaces the file by default. `PostgresWriter` leaves the
choice to the calling callback, through `write(data, replace=...)`, the opposite of
`JsonLinesWriter(append=...)`.

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
