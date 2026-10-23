# Spec: PostgreSQL reader and writer connectors

- **Status:** validated
- **Slug:** `postgres_connectors`. The spec is `specs/postgres_connectors.md` and its tests are
  `tests/test_postgres_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0004](../docs/decisions/0004-shared-process-state.md),
  [ADR 0009](../docs/decisions/0009-typed-package.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [specs/scd2_callback.md](scd2_callback.md),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for PostgreSQL, built on Spark's JDBC data source so that they run distributed on
a Databricks cluster. `PostgresReader` returns a Spark DataFrame that holds only the rows the job
needs: with a watermark, the filter runs inside PostgreSQL. `PostgresWriter` receives a Spark
DataFrame and appends it to a table, or replaces the table's rows with it. Both operations are
atomic: Spark loads a staging table in parallel, and then one PostgreSQL transaction, run on the
Spark driver with psycopg, applies the staging table to the target.

These are the first connectors that need a third-party package. It goes into a new extra,
`ingestio[postgres]`, and this spec defines the pattern for extras. They are also the first
example of how a connector receives the parameters of an ingestion: constructor arguments named
after the keys of the job's configuration file, with credentials passed separately.

## Motivation

The maintainer wants Python jobs, typically running on a Databricks cluster, to ingest from
PostgreSQL and to write into it:

- **Read.** The read connector extracts data and returns a DataFrame to the next callback. Jobs
  often work with a watermark, so the connector must extract only the rows that are needed, and
  the filtering must happen in the source. The maintainer states this as a general requirement
  for readers.
- **Write.** The write connector receives a DataFrame and writes it to the system. A write must
  be atomic whatever the ingestion approach is: incremental, full load, append-only, or a merge
  computed by a callback. Spark's JDBC writer is not atomic. It commits one transaction per
  partition, so a failed parallel write leaves part of the data in the target, and readers see a
  partial write while it runs.
- **Laziness.** A Spark DataFrame read over JDBC is lazy, and every action on it queries
  PostgreSQL again. A job that counts the rows, writes them, and then computes its next watermark
  from the same DataFrame runs three queries. Without an upper bound those queries can see
  different rows, and the job can record a watermark above rows it never wrote.
- **Configuration.** Ingestion parameters (the source table, the primary key, the watermark
  column) will come from a JSON job configuration that the library does not define yet. These
  connectors set the example for how a connector receives them.
- **Extras.** No extra exists yet. [ADR 0002](../docs/decisions/0002-zero-dependency-core.md)
  says that the first integration that needs one defines the pattern through its spec.

## Fit with the philosophy

- **Concept: connector.** `PostgresReader` satisfies `Reader[DataFrame]` and `PostgresWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). The writer's
  `write(data, *, replace=False)` adds a keyword-only argument with a default, so `write(data)`
  remains a valid call and the method still matches the protocol. `ReadSource` and
  `WriteTarget` use them unchanged.
- **`PostgresConnection` is not a third concept.** It is a value that groups the settings both
  connectors need to reach one database. It performs no action, integrates nothing on its own,
  and nobody extends it. It is an argument of the connectors, like the URL of `JsonApiReader`.
- **The connector decides no strategy.** Append and replace are the two primitive ways
  PostgreSQL offers to put a batch into a table: `INSERT`, and `TRUNCATE` followed by `INSERT`.
  The writer offers both through one method, `write(data, *, replace=False)`, and picks
  neither. The callback that calls it chooses, at call time, by passing `replace=True` or not.
  The connector's configuration has no mode. Anything built on top, such as a merge computed by
  a callback, belongs to callbacks. This is the opposite of `JsonLinesWriter(append=...)`, which
  fixes the choice in the connector's configuration (the
  [Known divergence](../docs/concepts/connectors.md#known-divergence)). The staging table and
  the single transaction are how the writer talks to PostgreSQL so that a write is
  all-or-nothing. They are a mechanism, not a strategy.
- **The reader decides no strategy either.** Whether a run is incremental, and from which value,
  is the job's decision. The reader only turns the column and the value it receives into the SQL
  that PostgreSQL runs.
- **Essential by default.** The connectors catch nothing to carry on: every failure propagates
  out of `read()` or `write()`, and fails the callback that called them.
- **Simplicity.** Three small classes in one module. No base class, no registry, no new
  protocol, no new callback.
- **Compose, do not impose.** The job builds the connectors from its own configuration and its
  own secrets. Nothing reads files, environment variables or `dbutils`. A user can still write
  their own PostgreSQL connector in their own code.
- **No dependencies in the core.** psycopg goes into the `postgres` extra. pyspark is never a
  dependency of the package. Both are imported lazily, so `ingestio` and
  `ingestio.connector.all` still import without them.
- **Streaming.** Nothing here closes the door. See [Decisions](#adrs).

## Scope

In scope:

- `PostgresConnection`: connection settings, validation, and their translation for pgJDBC and
  for psycopg;
- `PostgresReader`: full reads, and watermark reads filtered inside PostgreSQL over a slice
  that `read()` freezes;
- `PostgresWriter`: atomic append (`write(data)`) and atomic replace
  (`write(data, replace=True)`), through a staging table and one driver-side transaction;
- quoting of table names, column names and watermark values;
- the `postgres` extra, and the pattern every later extra follows;
- the seams the test doubles use, and the integration tests against a real PostgreSQL.

Out of scope:

- the job configuration file and recipes: reading or parsing JSON, `from_config`, or mapping
  configuration keys to arguments. A later spec covers them;
- computing, persisting or advancing the watermark, and exposing the bound that `read()`
  captured;
- upsert, merge, delete, SCD2 or any other operation besides append and replace;
- creating, altering or dropping the target table (any DDL on the target);
- a library callback that calls `write(data, replace=True)`. A pipeline that replaces uses its
  own callback (see the [usage sketch](#usage-sketch));
- custom SQL queries, column projection and extra filters in the reader;
- automatic bounds for parallel reads, and defaulting the partition column to the primary key;
- mitigations for late-arriving rows, such as a lookback window;
- connection settings other than `port` and `sslmode`: certificate files, timeouts, application
  name, multiple hosts;
- reporting the number of rows read or written; retries; connection pooling;
- tooling to clean up orphaned staging tables;
- engines other than Spark (pandas, polars), Lakehouse Federation, Databricks serverless
  compute, and streaming or CDC from PostgreSQL (logical replication);
- changes to CI, which belong to the maintainer (see the report).

## Public API

New, in the new module `ingestio/connector/postgres.py`, exported from `ingestio.connector.all`
only (not from `ingestio`):

```python
from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter
```

```python
class PostgresConnection:
    """Where and how to reach one PostgreSQL database. Opens nothing by itself.

    Args:
        host: Server host name or address.
        database: Database name.
        user: User to connect as.
        password: The user's password. Never shown in `repr`.
        port: Server port.
        sslmode: libpq / pgJDBC SSL mode: `disable`, `allow`, `prefer`, `require`,
            `verify-ca` or `verify-full`. `None` leaves the drivers' default.
    """

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        *,
        port: int = 5432,
        sslmode: str | None = None,
    ) -> None: ...
```

`PostgresConnection` is immutable. Its arguments are readable as attributes of the same names.

```python
class PostgresReader:
    """Reads a PostgreSQL table into a Spark DataFrame, filtering inside PostgreSQL.

    Args:
        table_name: Source table, `table` or `schema.table`. The configuration key of the same
            name.
        connection: Where the table lives.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark JDBC options passed through, such as `partitionColumn`, `lowerBound`,
            `upperBound`, `numPartitions`, `fetchsize`, `sessionInitStatement`,
            `queryTimeout`. They override the connector's defaults.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        connection: PostgresConnection,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
```

```python
class PostgresWriter:
    """Writes a Spark DataFrame into a PostgreSQL table, atomically.

    Args:
        table_name: Table to write into, `table` or `schema.table`. It must exist.
        connection: Where the table lives.
        options: Spark JDBC write options passed through, such as `numPartitions` or
            `batchsize`. They override the connector's defaults.
    """

    def __init__(
        self,
        table_name: str,
        connection: PostgresConnection,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one transaction.

        Args:
            data: The rows to write.
            replace: Remove every row of the target first, in the same transaction, so that
                the target ends up holding exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`.
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its
  subclass, is accepted too. `Mapping` is `collections.abc.Mapping`.
- `PostgresWriter` has a single write method, `write`, and no `replace()` method. `replace` is
  keyword-only and defaults to `False`, so `PostgresWriter.write` still satisfies
  `Writer[DataFrame]` structurally. `WriteTarget`, which calls `write(data)`, always appends.
- New installable extra: `ingestio[postgres]`.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, and everything exported by `ingestio`.
- The reader's and writer's attributes are not documented, so they are not public API. Their
  `repr` is specified under [Behaviour](#connection-settings).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration, takes
the credentials from secrets, and computes the last watermark before it builds the process.

```python
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

## Behaviour

### Connection settings

`PostgresConnection` validates its arguments when it is built:

| Argument | Rule | `ValueError` message |
|---|---|---|
| `host` | not empty | `host must not be empty` |
| `database` | not empty | `database must not be empty` |
| `user` | not empty | `user must not be empty` |
| `port` | from 1 to 65535 | `port must be between 1 and 65535` |
| `sslmode` | `None`, `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full` | `unsupported sslmode '<value>'` |

The password is not validated.

The connectors translate the settings for each driver:

- **pgJDBC (Spark):** the URL is `jdbc:postgresql://<host>:<port>/<database>`. A host that
  contains `:` (an IPv6 address) is wrapped in brackets. The database name is percent-encoded
  with `urllib.parse.quote(database, safe="")`. The user and the password never go into the URL:
  they are the Spark options `user` and `password`. The option `driver` is
  `org.postgresql.Driver`. When `sslmode` is set, it is sent as the option `sslmode`.
- **psycopg (driver side, writer only):**
  `psycopg.connect(host=..., port=..., dbname=..., user=..., password=...)`, all as keyword
  arguments, plus `sslmode=...` when it is set. The connector builds no connection string.

`repr` shows what identifies the database or the table, and never the password:

- `PostgresConnection(host='db.internal', port=5432, database='shop', user='ingest')`
- `PostgresReader(table_name='public.orders', host='db.internal', database='shop')`
- `PostgresWriter(table_name='public.orders', host='db.internal', database='shop')`

### Table and column names

- `table_name`, on both connectors, has the form `table` or `schema.table`. The connector
  splits it on `.`. Anything other than one or two non-empty parts raises
  `ValueError("table_name must be 'table' or 'schema.table'")` when the connector is built.
- Every table part, every column name, the watermark column and the staging table's name are
  identifiers as stored in the PostgreSQL catalog. The connectors always quote them: they wrap
  each one in double quotes and double any double quote inside. So `public.orders` becomes
  `"public"."orders"`, and `Sales.Orders` becomes `"Sales"."Orders"`, which is a different table
  from `sales.orders`. No name is ever interpolated unquoted.
- A name without a schema resolves through the `search_path` of the connections.

### Reading

`read()` returns a `pyspark.sql.DataFrame` built with `spark.read.format("jdbc")`. The
executors fetch the rows when an action runs on it. Apart from Spark's own schema query,
`read()` runs at most the bound query described below.

**Spark session.** The reader uses `spark` when it is given. Otherwise it calls
`pyspark.sql.SparkSession.active()`, looked up when `read()` is called, never when the reader is
built. The reader never creates a session. When none is active, PySpark's error propagates.

**Options of the data load:**

- the base options: `url`, `user`, `password`, `driver`, and `sslmode` when it is set;
- `dbtable`, described below;
- `fetchsize` = `10000`. pgJDBC's default of 0 loads a whole partition into memory. With a fetch
  size, Spark's PostgreSQL dialect turns autocommit off so that the cursor streams;
- the user's `options`. A user option replaces the connector's option of the same name, with
  names compared case-insensitively as Spark compares them. Only that key is sent.

**Full read** (no `watermark_column`): one load, with `dbtable` set to the quoted table name.

**Watermark read** (`watermark_column` given):

1. **Bound load.** It takes the options of the data load, with two differences: `query`
   replaces `dbtable`, and the user's `partitionColumn`, `lowerBound`, `upperBound` and
   `numPartitions` are left out (compared case-insensitively), because Spark rejects them
   together with `query`. The query is
   `SELECT max(<col>)::text AS upper_bound FROM <table>`, followed by
   ` WHERE <col> > <value>` when a value is given. The reader runs an action on the result and
   takes the first field of its single row, by position. That field is the bound: the highest
   watermark of the slice as PostgreSQL text, or `None` when no row qualifies.
2. **Data load.** `dbtable` is `(SELECT * FROM <table> WHERE <predicate>) AS ingestio_source`,
   where `<predicate>` is:

   | Value given | Bound | `<predicate>` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= <bound>` |
   | no | not `None` | `<col> <= <bound>` |
   | either | `None` | `false` |

3. `read()` returns the data load's DataFrame unchanged. The reader adds no Spark-side filter.

`<table>` and `<col>` are quoted names. `<value>` and `<bound>` are literals, built as described
in [Watermark values](#watermark-values). Every statement uses single spaces, exactly as shown.

What this gives:

- The filter runs in PostgreSQL and does not depend on Spark's `pushDownPredicate`.
- Every action on the DataFrame reads the same range of the watermark. Rows inserted or updated
  after `read()` with a watermark above the bound are left for the next run, so a watermark that
  the job computes from the DataFrame never goes past what the DataFrame holds.
- Rows whose watermark is `NULL` are never extracted when `watermark_column` is given, first run
  included.
- One index on the watermark column serves both queries.

### Watermark values

`watermark_value` is "given" when it is not `None`. `0` and `""` are values. A value is first
turned into text:

| Type | Text |
|---|---|
| `int`, but not `bool` | `str(value)` |
| `decimal.Decimal`, finite | `format(value, "f")` |
| `str` | the string itself |
| `datetime.datetime`, timezone-aware | `value.astimezone(UTC).isoformat(sep=" ")` |
| `datetime.date` | `value.isoformat()` |

The text then becomes a SQL string literal: it is wrapped in single quotes, and each single
quote inside is doubled. The bound is quoted the same way. PostgreSQL resolves an untyped
literal compared with a column to the column's type, so `'41'` compared with an integer column
is the integer 41. The connector needs no type mapping. Its only conversion is to normalise
timezone-aware datetimes to UTC; nothing else is converted. A value that PostgreSQL cannot
convert fails in PostgreSQL when the DataFrame is read. The quoting assumes
`standard_conforming_strings` is on, which is PostgreSQL's default since 9.1 and the same
assumption Spark's own quoting makes.

**Datetimes and the column's type.** An aware datetime is converted to UTC before it becomes
text, so its literal always ends in `+00:00`:

- For a `timestamptz` column, the literal is the same instant as the value, whatever the
  offset the value carried.
- For a `timestamp without time zone` column, PostgreSQL ignores the offset of the literal:
  `'2026-10-01 08:30:00-03:00'::timestamp` is 08:30. Because the value was converted first, the
  column is compared on the UTC wall clock: 08:30 at `-03:00` is compared as
  `2026-10-01 11:30:00`. That matches the default UTC session time zone of Spark and
  Databricks. A `timestamp without time zone` column that holds wall-clock time in another zone
  needs its value as a `str`, which is sent as it is.

The reader rejects, when it is built:

| Case | `ValueError` message |
|---|---|
| `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `bool`, `float`, or a type not in the table above | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` | `watermark_value must be timezone-aware` |

`<type name>` is `type(value).__name__`.

### Options

- The keys `url`, `dbtable`, `query`, `user`, `password` and `driver` are set by the connector.
  Giving any of them in `options`, in any case, raises
  `ValueError("option '<key>' is set by the connector")` when the connector is built, with
  `<key>` as the user wrote it.
- Values are passed to Spark as given. Spark turns them into strings, and booleans into `true`
  and `false`.
- Spark hands the options it does not know itself to pgJDBC as driver properties, and pgJDBC
  property names are case-sensitive. So the connector sends `stringtype`,
  `reWriteBatchedInserts` and `sslmode` in exactly that case.

### Writing

**Operations.**

- `write(data)`, that is with `replace=False`, appends: it inserts every row of `data` into the
  target and leaves the existing rows untouched. `write` is the method of the `Writer`
  protocol, and `WriteTarget` calls `write(data)`, so `WriteTarget` always appends.
- `write(data, replace=True)` truncates the target and then inserts every row of `data`, in the
  same transaction. The target ends up holding exactly the rows of `data`.
- The calling callback chooses, at call time, by passing `replace`. The connector's
  configuration has no mode, and there is no `replace()` method.

**Sequence.** `<cols>` stands for `data.columns` quoted and joined with `, `. `<target>` is the
quoted target, and `<stg>` the quoted staging table.

0. When `data.columns` is empty, the writer raises `ValueError("the DataFrame has no columns")`
   and does nothing else.
1. **Setup.** The writer opens a connection and executes
   `CREATE UNLOGGED TABLE <stg> AS SELECT <cols> FROM <target> WITH NO DATA`, then commits and
   closes it.
2. **Load.**
   `data.write.format("jdbc").options(...).mode("append").save()`, with the base options,
   `dbtable` = `<stg>`, `stringtype` = `unspecified`, `reWriteBatchedInserts` = `true`, and the
   user's `options`, which replace the connector's options of the same name. The executors write
   in parallel, one transaction per partition, into the staging table only.
3. **Apply.** The writer opens a new connection. With `replace=True` only, it executes
   `TRUNCATE TABLE <target>`. Then it executes
   `INSERT INTO <target> (<cols>) SELECT <cols> FROM <stg>`, then `DROP TABLE <stg>`. All these
   statements are committed in one transaction, and the connection is closed.

**Staging table.**

- Its name is `ingestio_staging_` followed by `uuid.uuid4().hex`: 32 lowercase hexadecimal
  digits, 49 characters in all, within PostgreSQL's 63-byte limit. Each call gets a new name.
- It is created in the target's schema, or unqualified when the target is unqualified.
- It has the target's types for exactly the DataFrame's columns, and no constraints, defaults,
  identity or indexes. So Spark's rows land with the types the target expects: with
  `stringtype=unspecified`, Spark strings go into `uuid`, `jsonb` and enum columns. The target's
  constraints, triggers, identity, and the defaults of columns the DataFrame lacks all apply in
  step 3. A DataFrame column that the target lacks fails step 1, before any data moves.
- No psycopg connection is open while Spark writes: long writes would otherwise run into idle
  timeouts and firewalls that drop idle connections.

**Atomicity.** Steps 1 and 2 never touch the target. Readers of the target see none of the new
rows until step 3 commits, and then all of them. After a failure at any step, the target is as
it was. With `replace=True`, `TRUNCATE` holds its lock until the commit, so readers wait
rather than see an empty or partial table (see [Known limitations](#known-limitations)).

**Failures.**

| Fails | What the writer does | Then |
|---|---|---|
| step 0 | nothing | `ValueError` |
| step 1 (for example: the target or a column is missing, no `CREATE` privilege) | the connection is closed without committing; there is nothing to clean up | the error propagates |
| step 2 or step 3 | step 3's connection, if open, is closed without committing; a new connection executes `DROP TABLE IF EXISTS <stg>`, commits, and is closed | the original exception propagates, the same object |
| the cleanup, after a failed step 2 or 3 | the original exception gets one note, added with `add_note`: `staging table <stg> could not be dropped: <type>: <message>` | the original exception propagates |

- In the note, `<type>` and `<message>` are the cleanup error's `type(...).__name__` and
  `str(...)`. The cleanup error itself is not raised.
- If the driver process dies, the cleanup cannot run, and a table named
  `ingestio_staging_<hex>` is left behind.
- The connectors write no log lines.

**What the writer does not do.**

- It runs no DDL on the target. A missing target fails step 1.
- It reads only `data.columns` and `data.write` from the DataFrame, and calls `save()` once.
  The DataFrame is evaluated exactly once, by the save, so the source is queried once.
- It never counts or inspects rows. An empty DataFrame goes through the same sequence: an
  append inserts nothing, and `replace=True` leaves the target empty.
- `write` returns `None`.

**psycopg.** The writer calls `psycopg.connect` as an attribute of the `psycopg` module each
time it opens a connection. It composes every statement with `psycopg.sql`, using
`sql.Identifier` for names, and never with string formatting. It ends transactions with
`commit()` and `rollback()`, or with the connection's context manager, never with
`Connection.transaction()`. Building a `PostgresWriter` imports psycopg. When that import
fails, the writer raises
`ImportError("PostgresWriter needs psycopg: pip install 'ingestio[postgres]'")`, chained to the
original error.

### The `postgres` extra and the pattern for extras

This spec defines how extras work, as ADR 0002 anticipated:

1. An extra is declared in `[project.optional-dependencies]` and named after the system it
   integrates: `postgres = ["psycopg[binary]>=3.2"]`. `dependencies` stays empty.
2. A module imports third-party packages only inside the functions that use them, and imports
   them for annotations under `TYPE_CHECKING`. `ingestio` and `ingestio.connector.all` import
   without any extra installed.
3. A connector whose extra is missing raises `ImportError` when it is built, with a message that
   names the extra: `pip install 'ingestio[<extra>]'`.
4. An engine that the runtime provides, such as pyspark on Databricks, is never a dependency of
   an extra. It is a documented prerequisite: here, pyspark 3.5 or later.
5. The `dev` dependency group holds the packages of every extra, so that `uv sync` gives an
   environment where the whole suite and mypy run. Here it gains `psycopg[binary]>=3.2` and
   `pyspark>=3.5`.

### Where it lives

- `src/ingestio/connector/postgres.py`: `PostgresConnection`, `PostgresReader`,
  `PostgresWriter`, with a module docstring and a sorted `__all__`.
- `src/ingestio/connector/all.py`: exports the three classes.
- `pyproject.toml`: the `postgres` extra, and `psycopg[binary]>=3.2` and `pyspark>=3.5` in the
  `dev` group.
- No change to `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or `connector/core.py`.

### Supported environments

- Databricks classic compute, in dedicated or standard access mode, on Databricks Runtime 15.4
  LTS or later. That is the first LTS with Python 3.11, which ingestio requires, and with
  Spark 3.5, which has `SparkSession.active()`. The PostgreSQL JDBC driver ships with the
  runtime. Only the DataFrame API is used, never `sparkContext` or `_jvm`, which standard access
  mode does not offer.
- Open-source Spark 3.5 or later, on Java 17 or 21, with the pgJDBC driver on the classpath, for
  example through `spark.jars.packages=org.postgresql:postgresql:<version>`.
- Not Databricks serverless compute. Databricks does not support writing to PostgreSQL from
  serverless compute, and the reader is not tested there.
- The Spark driver needs network access to PostgreSQL for psycopg, as the executors do for
  pgJDBC.
- Privileges: the reader needs `SELECT` on the table. The writer needs `CREATE` on the target's
  schema, `SELECT` and `INSERT` on the target, and `TRUNCATE` for `replace=True`.

### Known limitations

These are documented, not solved here:

- **Late commits.** A row committed after a read, with a watermark at or below the bound that
  read captured, is never extracted by a later run. This happens with long transactions and
  with clock skew between the source's writers. It is the classic limit of timestamp
  watermarks.
- **Full reads are not frozen.** Every action on a full read's DataFrame queries the table
  again.
- **`TRUNCATE`**, run by `write(data, replace=True)`, holds an `ACCESS EXCLUSIVE` lock for the
  whole apply transaction, so readers of the target wait. It is not MVCC-safe: a transaction whose snapshot predates the commit, in
  `REPEATABLE READ` or `SERIALIZABLE`, sees the table as empty. It fails on a table referenced by
  a foreign key, because the writer does not use `CASCADE`. The error propagates and the target
  is unchanged.
- **Spark task retries.** Spark's JDBC writer is not idempotent per partition. A task re-run
  after its partition committed (an executor lost after the commit, or speculative execution)
  writes the partition twice into the staging table, and the copy reaches the target.
  Databricks turns speculation off by default.
- **One long transaction.** The apply copies the whole batch in one transaction, with the WAL,
  the locks and the triggers that this implies.
- **Type conversion.** The values of the rows read and written go through Spark's JDBC type
  mapping and pgJDBC. The connectors add no conversion of their own to them: timestamps, for
  instance, pass through the JVM's time zone. Only the watermark value is normalised, to UTC.
- **Orphaned staging tables**, when the driver dies or the cleanup fails.
- Databricks recommends Lakehouse Federation for read-only queries. These connectors are for
  jobs that write, or that run on open-source Spark.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles and without a JVM or
a PostgreSQL server. The doubles live in `tests/test_postgres_connectors.py` and share one
ordered event log.

- **Fake Spark session.** Each access to `spark.read` returns a new fake reader, as PySpark
  does. It supports `format(name)`, `option(key, value)` and `options(**kwargs)`, and records
  one *load* each time `load()` is called. A load has the format and the merged options: a later
  call replaces an earlier key with the same name, compared case-insensitively, as Spark does.
  The original case of every key is kept. Values are compared as Spark sees them: `str(value)`,
  and booleans as `true` and `false`.
- **Fake DataFrame.** `load()` returns a new fake DataFrame and records every attribute
  accessed on it. A DataFrame from a load with a `query` option answers `collect()`, `first()`,
  `head()` and `take(n)` with the single row the test chose, a one-element tuple such as
  `("2026-10-21 09:30:00.5+00",)` or `(None,)`. A DataFrame given to the writer has `columns`
  and `write`. `write` returns a fake writer that supports `format`, `option`, `options` and
  `mode`, and records a *save*, with its format, mode and merged options, when `save()` is
  called.
- **Fake psycopg.** The test replaces `psycopg.connect` with `monkeypatch.setattr`. Each call
  records the keyword arguments and returns a fake connection. The fake connection supports
  `execute(query, params=None)`, `commit()`, `rollback()`, `close()`, and the context-manager
  protocol with psycopg's semantics: commit on a clean exit, rollback on an exception, then
  close. It also supports `cursor()`, as a context manager whose `execute` records on the same
  connection. Statements are recorded as text: `query.as_string()` for a `psycopg.sql`
  composable (psycopg 3.2 or later renders without a connection), `str(query)` otherwise. A
  statement is *committed* when its connection commits after executing it, with no rollback in
  between.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. To simulate a
  missing package, map its name to `None` in `sys.modules`, in a subprocess or with
  `monkeypatch.setitem`.
- Tests use the public API. They pass the fakes as `spark=` and as DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against PostgreSQL and a local Spark session. They are
marked `@pytest.mark.integration` and skipped, with a reason, when the environment variable
`INGESTIO_POSTGRES_URL` is unset. Its value is a URL of the form
`postgresql://<user>:<password>@<host>:<port>/<database>`. They need Java 17 or 21 and the
pgJDBC driver; how the local session gets the driver is the test-writer's choice. Each test
creates its own uniquely named tables and drops them. They cover:

1. A full read of a table with `uuid`, `jsonb`, `numeric(10,2)`, `timestamptz` and `text`
   columns returns every row.
2. A watermark read returns exactly the rows above the value. A row inserted after `read()`
   with a higher watermark is absent from a second action on the same DataFrame. A row with a
   `NULL` watermark is never returned.
3. `write` appends. Columns the DataFrame lacks take the target's defaults, an identity column
   included. Spark strings land in `uuid` and `jsonb` columns.
4. `write(df, replace=True)` leaves exactly the DataFrame's rows. `write(df, replace=True)` with
   an empty DataFrame that has the right columns leaves the target empty.
5. A DataFrame that violates a constraint of the target (`NOT NULL` or unique) fails the write.
   The target is unchanged, and the schema holds no table whose name starts with
   `ingestio_staging_`.
6. A value that cannot be stored in the target column's type (a string that is not a uuid) fails
   during the Spark write. The target is unchanged and no staging table is left.
7. `write(df, replace=True)` on a target referenced by a foreign key fails and leaves the target
   unchanged.
8. A DataFrame column that the target lacks fails before any row is written. The target is
   unchanged and no staging table is left.

## Acceptance criteria

Unless a criterion says otherwise:

- `conn = PostgresConnection("db.internal", "shop", "ingest", "s3cret")`;
- `spark` is the fake session, and DataFrames and `psycopg.connect` are the doubles of
  [Notes for testing](#notes-for-testing);
- the reader is `PostgresReader("public.orders", conn, spark=spark)`, and the writer is
  `PostgresWriter("public.orders", conn)`;
- the **base options** are `url` = `jdbc:postgresql://db.internal:5432/shop`, `user` = `ingest`,
  `password` = `s3cret` and `driver` = `org.postgresql.Driver`;
- the **connect arguments** are
  `host="db.internal", port=5432, dbname="shop", user="ingest", password="s3cret"`;
- `<stg>` is `"public"."ingestio_staging_<hex>"`, where `<hex>` is 32 lowercase hexadecimal
  digits, the same throughout one call;
- `df` is a fake DataFrame with `columns == ["id", "payload"]`;
- "exactly the options" means these keys and no others, with keys compared case-insensitively
  unless the criterion says the case matters.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import PostgresConnection, PostgresReader, PostgresWriter` runs,
  then it succeeds, the three names are in `ingestio.connector.all.__all__`, and none of them is
  in `ingestio.__all__`.
- **AT-002**: Given an interpreter where `import pyspark` and `import psycopg` both raise
  `ImportError`, when `ingestio.connector.all` is imported, then the import succeeds, and a
  `PostgresConnection` and a `PostgresReader` with `spark=spark` can be built. Calling `read()`
  on that reader returns the fake's DataFrame.
- **AT-003**: Given an interpreter where `import psycopg` raises `ImportError`, when
  `PostgresWriter("public.orders", conn)` is built, then it raises `ImportError` with the message
  `PostgresWriter needs psycopg: pip install 'ingestio[postgres]'`.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `postgres` is among its `Provides-Extra`
  values, every `Requires-Dist` carries an `extra == "postgres"` marker, and one `Requires-Dist`
  names the `psycopg` project.

### Connection settings

- **AT-005**: Given the calls below, then each raises `ValueError` with the message shown:

  | Call | Message |
  |---|---|
  | `PostgresConnection("", "shop", "ingest", "s3cret")` | `host must not be empty` |
  | `PostgresConnection("db.internal", "", "ingest", "s3cret")` | `database must not be empty` |
  | `PostgresConnection("db.internal", "shop", "", "s3cret")` | `user must not be empty` |
  | `PostgresConnection(..., port=0)` and `port=65536` | `port must be between 1 and 65535` |
  | `PostgresConnection(..., sslmode="maybe")` | `unsupported sslmode 'maybe'` |

  `port=1`, `port=65535`, and each of `disable`, `allow`, `prefer`, `require`, `verify-ca` and
  `verify-full` as `sslmode` are accepted.
- **AT-006**: Then `repr(conn)` is
  `PostgresConnection(host='db.internal', port=5432, database='shop', user='ingest')`,
  `repr(PostgresReader("public.orders", conn))` is
  `PostgresReader(table_name='public.orders', host='db.internal', database='shop')`, and
  `repr(PostgresWriter("public.orders", conn))` is
  `PostgresWriter(table_name='public.orders', host='db.internal', database='shop')`. Given in
  addition `sslmode="require"`, a watermark and `options`, then neither `repr` nor `str` of any
  of the three objects contains `s3cret`.

### Building the connectors

- **AT-007**: Given `name` in `""`, `"orders."`, `".orders"` and `"a.b.c"`, when
  `PostgresReader(name, conn)` or `PostgresWriter(name, conn)` is built, then it raises
  `ValueError("table_name must be 'table' or 'schema.table'")`. `"orders"` and
  `"public.orders"` are accepted by both.
- **AT-008**: Given `PostgresReader("public.orders", conn, watermark_value=41)`, then it raises
  `ValueError("watermark_value needs watermark_column")`. Given `watermark_column=""`, then it
  raises `ValueError("watermark_column must not be empty")`.
- **AT-009**: Given `watermark_column="updated_at"` and the `watermark_value` on the left, when
  the reader is built, then it raises `ValueError` with the message on the right:

  | `watermark_value` | Message |
  |---|---|
  | `True` | `unsupported watermark_value type: bool` |
  | `1.5` | `unsupported watermark_value type: float` |
  | `[1]` | `unsupported watermark_value type: list` |
  | `Decimal("NaN")` | `watermark_value must be finite` |
  | `datetime(2026, 10, 1)` | `watermark_value must be timezone-aware` |

  `0` and `""` are accepted.
- **AT-010**: Given `key` in `url`, `DbTable`, `QUERY`, `user`, `password` and `Driver`, when
  `PostgresReader("public.orders", conn, options={key: "x"})` or
  `PostgresWriter("public.orders", conn, options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as given.
- **AT-011**: Given `psycopg.connect` replaced and the fake `spark`, when readers (with and
  without a watermark) and a writer are built, then no load is recorded, no attribute of `spark`
  is accessed, and `psycopg.connect` is not called.

### Full read

- **AT-012**: When `reader.read()` runs, then exactly one load is recorded, with the format
  `jdbc` and exactly the options: the base options, `dbtable` = `"public"."orders"` and
  `fetchsize` = `10000`. `read()` returns the very DataFrame that `load()` returned, and no
  attribute of it was accessed. The `url` option contains neither `s3cret` nor `@`.
- **AT-013**: Given `PostgresConnection("::1", "my db", "ingest", "s3cret", port=6543)`, when
  a reader with it reads, then the `url` option is `jdbc:postgresql://[::1]:6543/my%20db`.
- **AT-014**: Given `options={"partitionColumn": "id", "lowerBound": 1, "upperBound": 1000,
  "numPartitions": 8, "FetchSize": 500}`, when the reader reads, then the load carries
  `partitionColumn` = `id`, `lowerBound` = `1`, `upperBound` = `1000`, `numPartitions` = `8` and
  a single fetch size option, the user's, with the value `500`. The base options and `dbtable`
  are as in AT-012.
- **AT-015**: Given `sslmode="require"` in the connection, when the reader reads, with or
  without a watermark, then every load carries `sslmode` = `require`, with this exact key.
  Without `sslmode`, no load carries an `sslmode` option.
- **AT-016**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced
  with a function that returns the fake session and `read()` runs, then the load is recorded on
  the fake.

### Watermark read

The reader in this section is
`PostgresReader("public.orders", conn, spark=spark, watermark_column="updated_at", ...)`. The
bound query answers `("2026-10-21 09:30:00.5+00",)` unless a criterion says otherwise.

- **AT-017**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs,
  then exactly two loads are recorded, in this order, both with the format `jdbc`:
  1. with exactly the base options, `fetchsize` = `10000` and `query` =
     `SELECT max("updated_at")::text AS upper_bound FROM "public"."orders" WHERE "updated_at" > '2026-10-01 00:00:00+00:00'`;
  2. with exactly the base options, `fetchsize` = `10000` and `dbtable` =
     `(SELECT * FROM "public"."orders" WHERE "updated_at" > '2026-10-01 00:00:00+00:00' AND "updated_at" <= '2026-10-21 09:30:00.5+00') AS ingestio_source`.

  `read()` returns the second load's DataFrame, and no attribute of it was accessed.
- **AT-018**: Given no `watermark_value`, when `read()` runs, then the first load's `query` is
  `SELECT max("updated_at")::text AS upper_bound FROM "public"."orders"`, and the second load's
  `dbtable` is
  `(SELECT * FROM "public"."orders" WHERE "updated_at" <= '2026-10-21 09:30:00.5+00') AS ingestio_source`.
- **AT-019**: Given the bound query answers `(None,)`, with or without a `watermark_value`, when
  `read()` runs, then the second load's `dbtable` is
  `(SELECT * FROM "public"."orders" WHERE false) AS ingestio_source`, and `read()` returns that
  load's DataFrame.
- **AT-020**: Given the `watermark_value` on the left, when `read()` runs, then both loads
  contain `"updated_at" > ` followed by the literal on the right:

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `'41'` |
  | `0` | `'0'` |
  | `Decimal("12.50")` | `'12.50'` |
  | `Decimal("1E+3")` | `'1000'` |
  | `"O'Brien"` | `'O''Brien'` |
  | `""` | `''` |
  | `date(2026, 10, 1)` | `'2026-10-01'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `'2026-10-01 08:30:00+00:00'` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `'2026-10-01 11:30:00.123456+00:00'` |

  Given the bound query answers `("it's",)`, then the second load contains
  `"updated_at" <= 'it''s'`.
- **AT-021**: Given the `table_name` and `watermark_column` on the left, when a full read runs,
  its `dbtable` is the quoted table. When a watermark read with `watermark_value=1` runs, both
  queries use the quoted table and the quoted column:

  | `table_name` | `watermark_column` | Quoted table | Quoted column |
  |---|---|---|---|
  | `orders` | `updated_at` | `"orders"` | `"updated_at"` |
  | `Sales.Orders` | `UpdatedAt` | `"Sales"."Orders"` | `"UpdatedAt"` |
  | `public.my"table` | `my"col` | `"public"."my""table"` | `"my""col"` |

- **AT-022**: Given `watermark_value=1` and `options={"partitionColumn": "id", "lowerBound": 1,
  "upperBound": 1000, "NumPartitions": 8, "queryTimeout": 30}`, when `read()` runs, then the
  bound load carries `queryTimeout` = `30` and none of `partitionColumn`, `lowerBound`,
  `upperBound` or `numPartitions`, in any case. The data load carries all five.
- **AT-023**: Given a fake whose `load()` raises `err = RuntimeError("down")`, when `read()`
  runs, with or without a watermark, then it raises `err` itself, and no load is recorded after
  the one that raised.

### Writing

- **AT-024**: When `writer.write(df)` runs, then the event log shows, in this order:
  1. a connection opened with exactly the connect arguments as keyword arguments, which
     executes
     `CREATE UNLOGGED TABLE <stg> AS SELECT "id", "payload" FROM "public"."orders" WITH NO DATA`,
     commits it, and is closed;
  2. one save, with the format `jdbc`, the mode `append`, and exactly the options: the base
     options, `dbtable` = `<stg>`, `stringtype` = `unspecified` and
     `reWriteBatchedInserts` = `true`;
  3. a second connection opened with the same arguments, which executes
     `INSERT INTO "public"."orders" ("id", "payload") SELECT "id", "payload" FROM <stg>` and
     then `DROP TABLE <stg>`, commits both in one transaction, and is closed.

  No other connection is opened, no other statement is executed, no load or other save is
  recorded, and `write` returns `None`. `writer.write(df, replace=False)` gives the same event
  log.
- **AT-025**: When `writer.write(df, replace=True)` runs, then the event log is the one of
  AT-024, except that the second connection executes `TRUNCATE TABLE "public"."orders"`, then
  the `INSERT`, then the `DROP`, and commits all three in one transaction.
- **AT-026**: Given two consecutive `write(df)` calls, then each uses a staging name that
  matches `ingestio_staging_[0-9a-f]{32}`, and the two names differ. Given
  `PostgresWriter("orders", conn)`, then the staging table is the unqualified
  `"ingestio_staging_<hex>"` in the `CREATE`, the save's `dbtable`, the `INSERT` and the `DROP`.
- **AT-027**: Given `options={"numPartitions": 4, "BatchSize": 5000, "stringtype": "varchar"}`,
  when `write(df)` runs, then the save carries `numPartitions` = `4`, `BatchSize` = `5000`, and
  a single `stringtype` option with the value `varchar`, the user's. The other options are as in
  AT-024. Without user options, the save's keys `stringtype` and `reWriteBatchedInserts` are in
  exactly this case. In every `write` of this section, with or without `replace=True`, the only
  attributes accessed on the DataFrame are `columns` and `write`, and `save()` is called once.
- **AT-028**: Given `sslmode="verify-full"` in the connection, when `write(df)` runs, then every
  `psycopg.connect` call receives the connect arguments plus `sslmode="verify-full"`, all as
  keyword arguments and with no positional argument, and the save carries `sslmode` =
  `verify-full`, with this exact key. Given `psycopg.connect` replaced after the writer was
  built, then the replacement is the one called.
- **AT-029**: Given a save that raises `err = RuntimeError("executor lost")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself. The committed statements are, in
  order, the `CREATE` on the first connection and `DROP TABLE IF EXISTS <stg>` on a second one.
  No `INSERT` or `TRUNCATE` is executed on any connection, and every connection is closed.
- **AT-030**: Given a second connection whose `execute` raises `err` on the `INSERT`, when
  `write(df)` runs, then it raises `err` itself. That connection commits nothing and is closed.
  A third connection executes and commits `DROP TABLE IF EXISTS <stg>`, and is closed. Given
  instead an `execute` that raises `err` on the `TRUNCATE`, when `write(df, replace=True)` runs,
  then the same holds, and no `INSERT` is executed.
- **AT-031**: Given a first connection whose `execute` raises `err` on the `CREATE`, when
  `write(df)` runs, then it raises `err` itself. Only that connection is opened, nothing is
  committed, the connection is closed, and no save is recorded.
- **AT-032**: Given a save that raises `err`, and a cleanup whose `execute` raises
  `RuntimeError("refused")`, when `write(df)` runs, then it raises `err` itself, and
  `err.__notes__` is
  `['staging table "public"."ingestio_staging_<hex>" could not be dropped: RuntimeError: refused']`.
  The same holds when the cleanup's `psycopg.connect` raises `RuntimeError("refused")`, and when
  the failure is the `INSERT` of AT-030 instead of the save.
- **AT-033**: Given a fake DataFrame with `columns == []`, when `write` runs, with or without
  `replace=True`, then it raises `ValueError("the DataFrame has no columns")`, `psycopg.connect`
  is not called, and no save is recorded.
- **AT-034**: Given a DataFrame with `columns == ["Id", 'say "hi"']`, when `write` runs, then the
  `CREATE` is
  `CREATE UNLOGGED TABLE <stg> AS SELECT "Id", "say ""hi""" FROM "public"."orders" WITH NO DATA`
  and the `INSERT` is
  `INSERT INTO "public"."orders" ("Id", "say ""hi""") SELECT "Id", "say ""hi""" FROM <stg>`.

### Composition

- **AT-035**: Given `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])` where the fake load returns a DataFrame
  with `columns == ["id", "payload"]`, when it runs, then `process.state["orders"]` is that
  DataFrame, and the writer's events are those of AT-024 on it: `WriteTarget` calls
  `write(data)`, so it appends and no `TRUNCATE` is executed. The test binds the reader to a
  variable annotated `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`,
  and mypy strict accepts both.

## Decisions

Taken by the maintainer before planning, recorded here and not reopened:

1. **The engine is Spark.** The reader returns a `pyspark.sql.DataFrame`, and the writer
   receives one. Reads and writes go through Spark's JDBC data source, so that they run
   distributed on the executors.
2. **Ingestion parameters come from a JSON job configuration that the library does not define
   yet.** It carries the specifics of one ingestion: the name of the table being ingested,
   `table_name` (the table at the source), `primary_key` and `watermark_column`. A connector
   receives them as constructor arguments named after the configuration keys. It does not read
   or parse JSON and has no `from_config`. A later spec covers the configuration file and
   recipes. Connection credentials are separate arguments and never live in the JSON: on
   Databricks they come from `dbutils.secrets.get`, which the library never imports.
3. **The watermark value goes into the reader's constructor.** The job computes the last
   watermark before it builds the `Process`. There is no new protocol and no new callback:
   `Reader.read()` and `ReadSource` stay as they are. The filter runs inside PostgreSQL.
   Persisting and advancing the watermark are out of scope.
4. **Writes are atomic, whatever the ingestion approach.** The writer is only the endpoint.
   Callbacks consume it, and it writes what it receives and decides no strategy. The mechanism
   is a staging table that Spark loads in parallel, then a single PostgreSQL transaction, run on
   the driver with psycopg 3, that applies the staging table to the target. The staging table is
   dropped afterwards, on failure too. psycopg goes into the new optional extra
   `ingestio[postgres]`, the project's first extra, which defines the pattern.
5. **Two operations: append, and replace** (`TRUNCATE` followed by an append, in the same
   transaction). The connector does no upsert or merge: callbacks compute merges. The calling
   callback chooses the operation, not the connector's configuration.
6. **Tests use doubles and integration.** Every criterion is covered by tests that always run,
   with test doubles. Real integration tests against PostgreSQL and a local Spark are added on
   top, marked `@pytest.mark.integration` and skipped without `INGESTIO_POSTGRES_URL`. CI
   changes belong to the maintainer.

Proposed by this spec and approved by the maintainer, as revised by the answers to the open
questions:

7. **The generic `jdbc` format, not Databricks' `postgresql` format.** It works on open-source
   Spark and on every Databricks Runtime, takes the same options, and is the one the Spark guide
   documents. The named format exists only on Databricks Runtime 11.3 and later.
8. **The watermark filter is written into the SQL that PostgreSQL runs** (a `dbtable` subquery),
   rather than left to Spark's predicate pushdown. It does not depend on `pushDownPredicate`,
   the connector renders its literal, and tests can observe it without a JVM.
9. **Watermark values are sent as quoted, untyped literals,** and PostgreSQL converts them to
   the column's type, so the connector needs no type mapping. The only conversion the connector
   makes is to normalise a timezone-aware datetime to UTC, with `value.astimezone(UTC)`, before
   rendering it. Nothing else is converted. PostgreSQL ignores the offset of a literal compared
   with a `timestamp without time zone` column (`'2026-10-01 08:30:00-03:00'::timestamp` is
   08:30). Converting first means such a column is compared on the UTC wall clock, which matches
   the default UTC session time zone of Spark and Databricks. For `timestamptz`, it is the same
   instant. The main session proposed the UTC normalisation, and the maintainer approved it.
10. **`read()` freezes the slice with an upper bound when `watermark_column` is given.** One
    `max()` query captures the bound as PostgreSQL text, which is compared back as a literal, so
    the bound never passes through Python or Spark types. Every action then reads the same
    range, and a watermark computed from the DataFrame never passes what it holds. The cost is
    one indexed query per `read()`. The captured bound is not exposed: advancing the watermark
    is out of scope.
11. **One connection object for both connectors:**
    `PostgresConnection(host, database, user, password, *, port=5432, sslmode=None)`, passed as
    the second positional argument. It is the one place to validate the settings, to translate
    them for both drivers the writer uses (pgJDBC on the executors, psycopg on the driver), and
    to keep the password out of `repr`. It offers only `port` and `sslmode` as options for now.
12. **One write method: `write(data, *, replace=False)`.** `replace=True` truncates the target
    and then appends, in the same transaction. There is no `replace()` method. `replace` is
    keyword-only, as CONTRIBUTING requires for options, and its default keeps the method a
    structural match for `Writer[DataFrame]`. `WriteTarget` calls `write(data)`, so it always
    appends. The calling callback makes the decision at call time; the connector's
    configuration has no mode, which keeps it different from `JsonLinesWriter(append=...)`.
    Until the append-only spec settles what `write` means, `WriteTarget` appends to a
    `PostgresWriter` but overwrites with a `JsonLinesWriter`, whose `write` replaces by default.
    No library callback passes `replace=True` in this spec.
13. **The staging table is `CREATE UNLOGGED TABLE ... AS SELECT <DataFrame columns> FROM target
    WITH NO DATA`, in the target's schema, named `ingestio_staging_<32 hex digits>`.**
    - The name is recognisable for manual cleanup, and unique without the run id, which
      connectors do not know.
    - `UNLOGGED` writes no WAL, which is faster. The truncation after a crash does not matter for
      a table that lives for one write.
    - The target's schema requires `CREATE` on it. There is no `staging_schema` option.
    - The table has the target's types for exactly the DataFrame's columns and none of the
      target's constraints, so only the final `INSERT` enforces the target's rules and fills its
      defaults. `(LIKE target INCLUDING DEFAULTS)`, the request's example, would copy every
      column with its `NOT NULL` constraint. A DataFrame without such a column would then work
      only because the column's default was copied too. Identity columns would need
      `INCLUDING IDENTITY`, and the copied default of a `serial` column would consume the
      target's sequence.
14. **The apply and the `DROP` of the staging table commit in one transaction.** A successful
    write leaves nothing to clean up, and a failed cleanup can only follow a failed write.
15. **No psycopg connection is held while Spark writes,** because long writes would run into
    idle timeouts.
16. **The cleanup runs on a new connection, and its failure becomes a note on the original
    exception.** Raising the cleanup error instead would hide the real cause. The connectors
    write no log lines, because a connector does not know the run id that CONTRIBUTING requires
    on every line.
17. **The writer evaluates the DataFrame once,** through the save, and never counts it.
18. **Test seams:**
    - `spark=` on the reader is public. It defaults to `SparkSession.active()` at read time and
      also serves jobs that hold several sessions, or a Spark Connect session, explicitly.
    - `psycopg.connect` is looked up on the module at each use. This seam is not public, and
      there is no `connect=` argument.
    - The writer takes no session: it uses `data.write`.
19. **The pattern for extras,** as described in [Behaviour](#the-postgres-extra-and-the-pattern-for-extras):
    - The extra is `postgres = ["psycopg[binary]>=3.2"]`. The binary package bundles libpq,
      which a cluster may lack and which the pure-Python psycopg needs at import time.
    - pyspark is in no extra. It is a documented prerequisite, version 3.5 or later, because
      Databricks Runtime provides it and installing pyspark with pip over the runtime breaks the
      runtime. There is no `ingestio[spark]` extra for now.
    - The `dev` group gets `psycopg[binary]>=3.2` and `pyspark>=3.5`. mypy strict then sees the
      real types, the doubles can use `psycopg.sql`, and the integration tests can run wherever
      Java 17 or 21 is available.
20. **Driver defaults:** `fetchsize=10000` for reads; `stringtype=unspecified` and
    `reWriteBatchedInserts=true` for writes, where `reWriteBatchedInserts` makes batched inserts
    two to three times faster. The user's `options` override all three.
21. **Supported environments:** Databricks classic compute on Runtime 15.4 LTS or later, and
    open-source Spark 3.5 or later. Not serverless.

Taken by the maintainer when answering the open questions. They are numbered after decision 21
so that the earlier numbers do not change:

22. **Names and module:** `PostgresConnection`, `PostgresReader` and `PostgresWriter`, in
    `src/ingestio/connector/postgres.py`, exported only from `ingestio.connector.all`. pyspark
    and psycopg are imported lazily, so the import surface works without the extra.
23. **The watermark comparison is strictly greater (`>`),** because the value is the highest
    watermark already loaded. Rows that share that exact value but commit after the previous
    read are missed, which is part of the late-commit limitation. **Rows with a `NULL`
    watermark are never extracted** when `watermark_column` is given, first run included, which
    is consistent across runs.
24. **Accepted watermark values:** `int` (not `bool`), finite `Decimal`, `str`, `date` and
    timezone-aware `datetime`.
    - Naive datetimes are rejected. The repository's datetimes are timezone-aware, and
      PostgreSQL would read a naive value in the JDBC session's time zone, which shifts it
      silently. `collect()` returns naive datetimes for Spark timestamps, so a job that takes
      the watermark from a Delta table must attach the zone or pass a string.
    - Floats are rejected, because watermarks on floating-point columns are unusual and their
      decimal text may not match the stored value.
25. **Reader arguments beyond the configuration keys:** one `options` mapping of Spark JDBC
    options, passed through. Parallel reads use `partitionColumn`, `lowerBound`, `upperBound`
    and `numPartitions` in it, and `fetchsize` defaults to 10000. The reader has:
    - no first-class partitioning arguments;
    - no automatic bounds, and no partition column defaulting to `primary_key`;
    - no `query=` and no `columns=`. Spark already prunes the columns a callback selects, and a
      free-form query cannot safely be bounded by a watermark.
26. **Neither connector accepts `primary_key`.** A plain read, an append and a replace do not
    use it. It belongs to the callbacks that merge and to the future recipe, and its type is
    for the configuration spec to decide.
27. **The writer's table argument is `table_name`, like the reader's.** The maintainer chose
    this name for the writer too. Both connectors validate it with the same message,
    `table_name must be 'table' or 'schema.table'`.
28. **Table and column names are case-sensitive, as stored in the catalog, and always quoted:**
    `Orders` means `"Orders"`. This is what `psycopg.sql.Identifier` and Spark's dialect do, and
    lowercase names, the norm in PostgreSQL, behave the same either way.
29. **A missing target fails** with PostgreSQL's error at step 1, and nothing is created. DDL on
    the target is architecture, which the philosophy keeps out of connectors.
30. **An empty DataFrame goes through the same sequence as any other.** An append inserts
    nothing, and `replace=True` leaves the target empty. Knowing that the DataFrame is empty
    would take one more action, and so one more query of the source. A callback that wants to
    skip empty batches checks for itself and raises `CancelStepException`.
31. **The configuration-key convention gets a new ADR,** written in the documentation stage with
    the maintainer's approval. The convention:
    - a connector takes an ingestion's parameters as constructor arguments named after the job
      configuration keys, never reads configuration, and has no `from_config`;
    - credentials are separate arguments;
    - readers extract only what the job needs, filtering inside the source.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept, and the writer
  exposes operations without choosing between them.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged, and their
  payload stays open. The batch nature of these connectors is their own: a streaming or CDC
  source for PostgreSQL, such as logical replication, would be a different connector that tracks
  its own offsets. They lean towards batch, though: one slice per `read()`, a watermark fixed
  when the reader is built, and a write that needs the whole DataFrame before it commits. That
  belongs in the "What leans towards batch" list of
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md). This calls for an amendment, not a
  new ADR.
- **The configuration-key convention, together with the rule that readers extract only what
  the job needs by filtering in the source, binds every future connector and the configuration
  spec.** It gets a new ADR, written in the documentation stage with the maintainer's approval
  (decision 31).
- **The pattern for extras** fulfils what ADR 0002 anticipated. Its consequence "No extra exists
  yet" becomes outdated and should be amended once this spec is approved.

## Open questions

None open. The maintainer answered every question; the substance is in
[Decisions](#decisions).

1. (a): `PostgresConnection`, `PostgresReader`, `PostgresWriter` in `connector/postgres.py`
   (decision 22).
2. (a), with only `port` and `sslmode` (decision 11).
3. (c): `write(data, *, replace=False)`, no `replace()` method (decision 12).
4. (a), without (c): `read()` freezes the slice, the bound is not exposed (decision 10).
5. (a) for both: strictly greater, `NULL` watermarks never extracted (decision 23).
6. (a) for both: naive datetimes and floats rejected (decision 24).
7. (a), with `fetchsize` 10000 (decisions 20 and 25).
8. (a): neither connector accepts `primary_key` (decision 26).
9. (c): the writer's argument is `table_name` (decision 27).
10. (a): names case-sensitive and always quoted (decision 28).
11. The proposals, without `staging_schema` (decision 13).
12. (a): a missing target fails, no DDL (decision 29).
13. (a): an empty DataFrame goes through the same sequence (decision 30).
14. (a) for all three parts: `psycopg[binary]>=3.2`, pyspark in no extra, both in `dev`
    (decision 19).
15. (a), with no `connect=` (decision 18).
16. (a): a new ADR, written in the documentation stage with the maintainer's approval
    (decision 31).
17. (a): a note on the original exception (decision 16).

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Drafted by the planner from the maintainer's six decisions: `PostgresConnection`, `PostgresReader` (watermark filter inside PostgreSQL, slice frozen by an upper bound captured at read time) and `PostgresWriter` (`write` appends, `replace` truncates then appends, both atomic through an unlogged staging table and one psycopg transaction), plus the `ingestio[postgres]` extra and the pattern for extras. 35 acceptance criteria with test doubles, 8 integration scenarios, 17 open questions. |
| 2026-10-23 | planning | draft | Revised by the planner with the maintainer's answers: one write method `write(data, *, replace=False)` instead of `replace()`; the writer's argument is `table_name`; timezone-aware datetime watermarks are normalised to UTC (proposed by the main session); every other question took its recommended option. The 17 questions became decisions 7–31; AT numbering unchanged. |
| 2026-10-23 | approval | approved | Approved by the maintainer ("Aprovar tudo + nota UTC"), on condition that the revision was limited to the answers; the main session checked the revised spec and found it limited to them. The main session then added the `postgres` extra and the dev dependencies (`psycopg[binary]>=3.2`, `pyspark>=3.5`) to `pyproject.toml` and refreshed `uv.lock`, which are outside every stage's write scope. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_postgres_connectors.py`: 52 always-run tests covering AT-001 to AT-035 with a fake Spark session, fake DataFrames and a fake `psycopg.connect` sharing one event log, plus the 8 integration scenarios, marked `integration` and skipped without `INGESTIO_POSTGRES_URL` (written blind: no Java 17/21 or PostgreSQL here). Collection fails as expected with `ImportError: cannot import name 'PostgresConnection' from 'ingestio.connector.all'`; ruff passes and mypy reports only the three missing names. Not covered by any criterion: the immutability of `PostgresConnection` and the absence of log lines. |
| 2026-10-23 | documentation | documented | The doc-keeper documented the PostgreSQL connectors in `docs/concepts/connectors.md` (API, reading, writing, environments, limitations, the pattern for extras, the corrected known divergence), added ADR 0013 (proposed, with two `TODO(maintainer)`) for the configuration convention, amended ADRs 0002 and 0012, and updated `docs/index.md` and the architecture KB. Sent back once by the main session because three signature blocks were fenced as `python` and failed `tests/test_docs_examples.py` with a SyntaxError; they are now `text`. The two remaining example failures are the expected `ImportError` of the missing module. |
| 2026-10-23 | implementation | approved | Returned to the test-writer. The implementer wrote `connector/postgres.py` and the exports in `connector/all.py` (ruff and mypy pass; every test passes except three). The three AT-032 tests wrap the call in `pytest.raises(RuntimeError, match="^executor lost$")`, but pytest 9 matches `match=` against the message plus the PEP 678 notes, so the note the spec requires makes them fail. Confirmed by the main session. The tests are wrong, not the code. |
| 2026-10-23 | tests | tested | The test-writer changed the `match=` of the three AT-032 tests from an exact `^...$` pattern to one anchored at the start only; the tests still check the exception's identity and its exact notes. No other test changed. The full suite passes with the existing implementation: 212 passed, 8 integration tests skipped. |
| 2026-10-23 | documentation | documented | Re-run after the return. The doc-keeper confirmed that the documentation still matches the spec and the tests after the AT-032 `match=` fix; no change. |
| 2026-10-23 | implementation | implemented | The implementer wrote `PostgresConnection`, `PostgresReader` and `PostgresWriter` in `src/ingestio/connector/postgres.py` and exported them from `connector/all.py`; pyspark and psycopg are imported lazily, and the writer follows the staging-table write with one transaction. Re-run after the return with no change to `src/`: 212 passed, 8 integration tests skipped, ruff and mypy pass. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-035 covered by 108 passing tests, full suite 212 passed with the 8 integration tests skipped, ruff, format, mypy strict and `uv lock --check` clean; docs, signatures, immutability, no logging and the dependency changes match the spec. Non-blocking findings for the maintainer: the apply connection is left open when its `COMMIT` fails (data unaffected; no criterion covers it), the `ingestio[spark]` example in the philosophy, the `dev` group description in CONTRIBUTING, pyspark checked only against 4.2, and an outdated README. The main session fixed a wording slip in `docs/concepts/connectors.md` (only the writer needs the extra). ADR 0013 accepted by the maintainer with the rationale filled in. |
