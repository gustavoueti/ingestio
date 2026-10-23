# Spec: BigQuery reader and writer connectors

- **Status:** validated
- **Slug:** `bigquery_connectors`. The spec is `specs/bigquery_connectors.md` and its tests are
  `tests/test_bigquery_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/disk_connectors.md](disk_connectors.md),
  [Connectors](../docs/concepts/connectors.md),
  [backlog idea 7](../docs/backlog.md#7-bigquery-reader-and-writer)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for Google BigQuery tables, built on Spark's BigQuery data source
(spark-bigquery-connector), so that the data moves between BigQuery and the Spark executors.
`BigQueryReader` returns a Spark DataFrame over a table. With a watermark, the filter runs inside
BigQuery: the BigQuery Storage Read API applies it as the row restriction of its read session,
and `read()` freezes the slice with an upper bound that one query captures first.
`BigQueryWriter` receives a Spark DataFrame and appends it to a table, or replaces the table's
rows with it. Both operations are atomic: Spark loads a staging table in the target's dataset,
through the Storage Write API by default and without any Cloud Storage bucket, and then one
BigQuery statement applies the staging table to the target.

The native client, google-cloud-bigquery, runs on the Spark driver for control work only: the
bound query, and the creation, application and removal of the staging table. It goes into a new
extra, `ingestio[bigquery]`. Credentials come from the environment, the project billed can
differ from the table's, and a job can pass its own client. Everything else follows the
PostgreSQL and disk connectors.

The criteria follow the recommended option of every [open question](#open-questions). A
criterion or a rule that depends on one names it, as **(Q n)**.

## Motivation

The maintainer listed BigQuery among the connectors of 2026-10-23 (backlog idea 7), and decided
its engine, its read, its write and its credentials in conversation (see
[Decisions](#decisions)).

- **Read.** Jobs ingest BigQuery tables incrementally, with a watermark, and the filter must run
  in the source ([ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)).
  Spark's BigQuery data source reads through the Storage Read API, which filters the rows on
  BigQuery's side, before they reach the executors, and skips the partitions of a table
  partitioned by the filtered column.
- **Laziness.** A DataFrame over BigQuery is lazy, and every action opens a new read session on
  the table as it is at that moment. Without an upper bound, a job that counts the rows, writes
  them and then computes its next watermark from the same DataFrame can see three different sets
  of rows, and record a watermark above rows it never wrote. This is the PostgreSQL reader's
  problem, and it gets the same answer.
- **Write.** A write must be atomic whatever the ingestion approach, as with `PostgresWriter`.
  Spark's BigQuery data source writes in one of two ways. The direct method uses the Storage
  Write API: each task writes into a pending stream, and the driver commits all the streams
  together at the end of the job. The indirect method writes files into a Cloud Storage bucket
  and then runs one load job. As far as the planner knows, both are atomic for an append. For an
  overwrite, what the data source does has changed across its versions (see
  [decision 14](#decisions)), and the planner cannot say which versions are atomic and keep the
  table's definition. The direct method also checks that each field of the DataFrame has the
  mode of the column it lands in, so a DataFrame whose fields are nullable, as most are, fails
  on a target with `REQUIRED` columns. A staging table makes the write atomic whatever the
  method and the version, and leaves the target's definition alone.
- **No bucket.** The indirect method needs a bucket that the job must provide. The maintainer
  wants writes to work without one.
- **Billing.** BigQuery data often lives in one project while the jobs that read and write it
  are billed to another.

## Fit with the philosophy

- **Concept: connector.** `BigQueryReader` satisfies `Reader[DataFrame]` and `BigQueryWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). The philosophy names BigQuery
  among the systems a connector integrates. `write(data, *, replace=False)` adds a keyword-only
  argument with a default, so `write(data)` stays a valid call and the method still matches the
  protocol. `ReadSource` and `WriteTarget` use both connectors unchanged.
- **No third concept.** There is no connection object. The `client` argument is
  google-cloud-bigquery's own client, which the job may build, and `billing_project` is a
  setting of the connectors. Nothing new performs an action or integrates a system on its own.
- **The writer decides no strategy.** Append and replace are the two primitive ways to put a
  batch into a table: insert the rows, or make them the table's only rows. The writer offers
  both through one method and picks neither. The calling callback chooses at call time, by
  passing `replace`, and the connector's configuration has no mode. The staging table and the
  single statement are how the writer talks to BigQuery so that a write is all-or-nothing: a
  mechanism, not a strategy. Merges by key, partition-level replaces and anything built on top
  belong to callbacks.
- **Architecture.** The writer runs no DDL on the target. It never creates, alters or drops it,
  and a replace keeps the target's schema, partitioning, clustering and options. The staging
  table is the writer's own, created and removed within one call.
- **The reader decides no strategy.** Whether a run is incremental, and from which value, is the
  job's decision. The reader only turns the column and the value it receives into the filter
  that BigQuery applies.
- **Essential by default.** The connectors catch nothing to carry on: every failure of a read
  or of a write propagates out of `read()` or `write()` and fails the callback that called
  them. The one error the writer does not raise is the failed removal of its staging table
  after the write has committed, which [question 8](#open-questions) puts to the maintainer.
- **Simplicity.** Two classes in one module. No base class, no registry, no new protocol, no
  new callback.
- **Compose, do not impose.** The job builds the connectors from its own configuration. The
  connectors read no file, environment variable or secret themselves: the environment's
  credentials are looked up by Google's libraries, and a job that wants other credentials for
  the client passes its own. A user can still write their own BigQuery connector in their own
  code.
- **No dependencies in the core.** google-cloud-bigquery goes into the `bigquery` extra.
  pyspark and spark-bigquery-connector are prerequisites that the runtime provides. Both
  Python packages are imported lazily, so `ingestio` and `ingestio.connector.all` still import
  without them.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `BigQueryReader`: full reads, and watermark reads filtered by the Storage Read API over a
  slice that `read()` freezes with an upper bound;
- `BigQueryWriter`: atomic append (`write(data)`) and atomic replace
  (`write(data, replace=True)`), through a staging table and one BigQuery statement;
- the rendering of names, watermark values and the bound in GoogleSQL;
- the billing project, the optional client, and the environment's credentials;
- the reserved options, and the choice of Spark's write method through `options`;
- the `bigquery` extra;
- the test doubles, and the opt-in integration tests.

Out of scope:

- the job configuration file and recipes: reading or parsing JSON, `from_config`, mapping
  configuration keys to arguments;
- computing, persisting or advancing the watermark, and exposing the bound that `read()`
  captured;
- upsert, merge by key, delete, SCD2, a replace of only the partitions present in the batch, or
  any other operation besides append and replace;
- any DDL on the target: creating, altering or dropping it, adding or relaxing its columns;
- custom SQL queries, column projection and extra filters in the reader;
- reading a snapshot of the past (time travel), change history (`APPENDS`, `CHANGES`), and
  BigQuery change data capture (**Q4**);
- views, materialized views and external tables as first-class sources. A view can be read
  through `options`, without a claim (see [Options](#options));
- table names in the form `dataset.table`, and domain-scoped project IDs (**Q2**);
- credentials as arguments or as Spark options (**Q9**);
- job labels, query priorities, locations and timeouts of the control queries;
- reporting the number of rows read or written; retries;
- tooling to clean up staging tables, which expire on their own;
- engines other than Spark (pandas, BigQuery DataFrames), Lakehouse Federation, Databricks
  standard access mode and serverless compute, and streaming reads or writes;
- changes to CI, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/bigquery.py` **(Q1)**, exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import BigQueryReader, BigQueryWriter
```

```python
class BigQueryReader:
    """Reads a BigQuery table into a Spark DataFrame, filtering inside BigQuery.

    Args:
        table_name: Source table, `project.dataset.table`. The configuration key of the same
            name.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        billing_project: Project billed for the read and for the bound query. The default
            project of the environment's credentials when omitted.
        options: Options of Spark's BigQuery data source passed through, such as
            `maxParallelism` or `readDataFormat`.
        client: BigQuery client for the bound query. A new one is created at each watermark
            read when omitted.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
        *,
        watermark_column: str | None = None,
        watermark_value: int | Decimal | str | date | None = None,
        billing_project: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: Client | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the rows the job needs."""
```

```python
class BigQueryWriter:
    """Writes a Spark DataFrame into a BigQuery table, atomically.

    Args:
        table_name: Table to write into, `project.dataset.table`. It must exist.
        billing_project: Project billed for the load and for the statements that apply it.
            The default project of the environment's credentials when omitted.
        options: Options of Spark's BigQuery data source passed through, such as
            `writeMethod` or `temporaryGcsBucket`. They override the connector's defaults.
        client: BigQuery client for the staging table and the statements. A new one is
            created at each write when omitted.
    """

    def __init__(
        self,
        table_name: str,
        *,
        billing_project: str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: Client | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the target, in one statement.

        Args:
            data: The rows to write.
            replace: Make the rows of `data` the target's only rows, in the same statement.
                Appends when `False`.
        """
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`.
  `Client` is `google.cloud.bigquery.Client`. `Decimal` is `decimal.Decimal`; `date` is
  `datetime.date`, and `datetime.datetime`, its subclass, is accepted too. `Mapping` is
  `collections.abc.Mapping`.
- `table_name` is required, so it is positional; every option is keyword-only.
- `BigQueryWriter` has a single write method, `write`, and no `replace()` method. `replace` is
  keyword-only and defaults to `False`, so `BigQueryWriter.write` satisfies `Writer[DataFrame]`
  structurally. `WriteTarget`, which calls `write(data)`, always appends.
- New installable extra: `ingestio[bigquery]`.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL and disk connectors, and everything exported by `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration and
keeps its own watermark. The credentials come from the cluster's environment.

```python
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
`BigQueryReader(..., client=bigquery.Client.from_service_account_info(info))`. Spark then needs
the same identity from its own environment (**Q9**).

## Behaviour

### Building the connectors

Building a connector validates its arguments, checks that google-cloud-bigquery can be imported
when the connector will need it, and does nothing else. It creates no client, calls no method of
a client it is given, touches no Spark session and makes no network call.

**Validation.** Invalid arguments raise `ValueError` when the connector is built:

| Connector | Case | Message |
|---|---|---|
| both | `table_name` does not have exactly three non-empty parts separated by `.` | `table_name must be 'project.dataset.table'` |
| both | `billing_project == ""` | `billing_project must not be empty` |
| both | a reserved key in `options`, in any case | `option '<key>' is set by the connector` |
| reader | `watermark_column == ""` | `watermark_column must not be empty` |
| reader | `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| reader | `bool`, `float`, or a type not in [Watermark values](#watermark-values) | `unsupported watermark_value type: <type name>` |
| reader | a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| reader | a naive `datetime` | `watermark_value must be timezone-aware` |

- `<key>` is the key as the user wrote it. `<type name>` is `type(value).__name__`.
- A datetime is naive when its `tzinfo` is `None` or its `utcoffset()` is `None`.
- `table_name` is split on `.` and kept as given otherwise: the connectors neither strip nor
  change the case of its parts **(Q2)**.

**The extra.** The writer always, and the reader when `watermark_column` is given, import
`google.cloud.bigquery` when they are built. When that import fails, they raise `ImportError`,
chained to the original error:

- `BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`
- `BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`

A reader without `watermark_column` never imports google-cloud-bigquery, at build time or when
it reads.

**`repr`** shows the table:

- `BigQueryReader(table_name='acme-data.sales.orders')`
- `BigQueryWriter(table_name='acme-data.lake.orders')`

### Names and quoting

- `<project>`, `<dataset>` and `<table>` are the three parts of `table_name`.
- Spark's `table` option receives a table as `<project>.<dataset>.<table>`, unquoted. For the
  source and the target, that is `table_name` as given.
- In SQL, every part of a table name and every column name is a GoogleSQL quoted identifier: it
  is wrapped in backticks, and each `\` inside becomes `\\` and each `` ` `` inside becomes
  `` \` ``, in that order. So `acme-data.sales.orders` becomes
  `` `acme-data`.`sales`.`orders` ``, and the column ``my`col`` becomes `` `my\`col` ``. No name
  is ever interpolated unquoted.
- BigQuery's own rules then apply: dataset and table names are case-sensitive, column names are
  not. The connectors change no name.
- The client receives table IDs as strings, `"<project>.<dataset>.<table>"`, unquoted.

### Reading

`read()` returns a `pyspark.sql.DataFrame` built with `spark.read.format("bigquery")`. The
executors read the rows when an action runs on it.

**Spark session.** The reader uses `spark` when it is given. Otherwise it calls
`pyspark.sql.SparkSession.active()` at each `read()`, before anything else, never when the
reader is built. The reader never creates a session. When none is active, PySpark's error
propagates, and no client is created.

**Options of the data load:**

- `table` = `table_name`;
- `parentProject` = `billing_project`, only when it is given;
- `filter`, only in a watermark read, described below;
- the user's `options`, as given.

The load is `spark.read.format("bigquery").options(...).load()`, with no argument to `load()`.

**Full read** (no `watermark_column`): one load. No client is created or used, even when one is
given. `read()` returns the load's DataFrame unchanged.

**Watermark read** (`watermark_column` given):

1. **Client.** The reader uses `client` when it is given. Otherwise it creates one, with
   `google.cloud.bigquery.Client(project=billing_project)`, `project` passed as a keyword
   argument and `None` when `billing_project` is not given. `Client` is looked up as an
   attribute of the `google.cloud.bigquery` module at each `read()`.
2. **Bound query.** The reader calls `client.query(<query>)`, with the query as its only
   argument, then `result()` on the job it returns, once. The bound is the first field, by
   position, of the first row of the result, or `None` when the result has no row. The query
   is

   ```text
   SELECT IF(MAX(<col>) IS NULL, NULL, FORMAT('%T', MAX(<col>))) AS upper_bound FROM <source>
   ```

   followed by ` WHERE <col> > <value>` when a value is given. `FORMAT('%T', x)` turns `x` into
   the text of a GoogleSQL literal of `x`'s type: `TIMESTAMP "2026-10-21 09:30:00.5+00"`,
   `DATE "2026-10-21"`, `DATETIME "2026-10-21 09:30:00"`, `41`, `NUMERIC "12.5"` or `"abc"`. So
   the bound is the highest watermark of the slice, as BigQuery's own literal, or `None` when no
   row qualifies.
3. **Closing.** A client that the reader created is closed with `close()` once the bound is
   taken, and also when creating the job or reading its result raises. A client given is never
   closed.
4. **Data load.** The options of the data load, with `filter` set to:

   | Value given | Bound | `filter` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= <bound>` |
   | no | not `None` | `<col> <= <bound>` |
   | either | `None` | `FALSE` |

5. `read()` returns the data load's DataFrame unchanged. The reader adds no Spark-side filter.

`<source>` is the quoted `table_name`, and `<col>` the quoted `watermark_column`. `<value>` is a
literal, built as described in [Watermark values](#watermark-values). `<bound>` is inserted as
it is: it is already a literal, and it is never quoted again, parsed or converted, so it never
passes through Python or Spark types. Every statement and filter uses single spaces, exactly as
shown.

What this gives:

- The filter runs in BigQuery. Spark's BigQuery data source passes the `filter` option to the
  Storage Read API as the row restriction of the read session, so the rows outside the slice
  never leave BigQuery. It does not depend on Spark pushing down predicates, and tests can
  observe it as an option, without a JVM.
- Every action on the DataFrame reads the same range of the watermark. Rows written after
  `read()` with a watermark above the bound are left for the next run, so a watermark that the
  job computes from the DataFrame never goes past what the DataFrame holds. See
  [Consistency of the slice](#consistency-of-the-slice) for what the range does not freeze.
- Rows whose watermark is `NULL` are never extracted when `watermark_column` is given, first run
  included: `NULL` satisfies neither comparison.
- A value that BigQuery cannot compare with the column fails the bound query, so `read()` raises
  BigQuery's error before any Spark load.
- Apart from the metadata that Spark's data source fetches itself when it loads, `read()` runs at
  most the bound query.

### Watermark values

`watermark_value` is "given" when it is not `None`; `0` and `""` are values. A value becomes a
GoogleSQL literal of its own type:

| Type | Literal | Example | Compares with columns of type |
|---|---|---|---|
| `int`, but not `bool` | `str(value)` | `41`, `-7` | `INT64`, `NUMERIC`, `BIGNUMERIC`, `FLOAT64` |
| `decimal.Decimal`, finite | `BIGNUMERIC '<format(value, "f")>'` | `BIGNUMERIC '12.50'` | `NUMERIC`, `BIGNUMERIC`, `INT64`, `FLOAT64` |
| `str` | a string literal, below | `'O\'Brien'` | `STRING`; `DATE`, `DATETIME` and `TIMESTAMP` by GoogleSQL's coercion of string literals |
| `datetime.date`, but not `datetime` | `DATE '<value.isoformat()>'` | `DATE '2026-10-01'` | `DATE`, `DATETIME` |
| `datetime.datetime`, timezone-aware | `TIMESTAMP '<value.astimezone(UTC).isoformat(sep=" ")>'` | `TIMESTAMP '2026-10-01 08:30:00+00:00'` | `TIMESTAMP` |

- The last column follows GoogleSQL's conversion rules, as the planner knows them. A column of
  another type fails the bound query with BigQuery's error. The connector maps no types: it only
  chooses the literal from the value's Python type.
- **String literals.** The text is wrapped in single quotes. Inside, each `\` becomes `\\`, each
  `'` becomes `\'`, each line feed becomes `\n` and each carriage return becomes `\r`, in that
  order. Every other character is written as it is. GoogleSQL escapes with backslashes: a doubled
  quote, as PostgreSQL writes it, is not an escape in GoogleSQL.
- **Datetimes and the column's type (Q3).** An aware datetime is converted to UTC first, so its
  literal always ends in `+00:00`. On a `TIMESTAMP` column it is the same instant as the value,
  whatever offset the value carried. GoogleSQL does not compare a `TIMESTAMP` with a `DATETIME`,
  so on a `DATETIME` column an aware datetime fails the bound query. A `DATETIME` column holds
  civil time with no zone: its value is a `str` in that civil time, such as
  `"2026-10-01 08:30:00"`, which GoogleSQL coerces to `DATETIME`.
- The only conversion the connector makes is the normalisation of an aware datetime to UTC.

The reader rejects, when it is built, the cases listed under
[Building the connectors](#building-the-connectors).

### Partitioned and clustered sources

- When the table is partitioned by the watermark column (time-unit column partitioning on a
  `DATE`, `TIMESTAMP` or `DATETIME` column, or integer-range partitioning on an `INT64` column),
  nothing more is needed. The filter compares the partitioning column with constants, which is
  what BigQuery uses to prune partitions: the bound query and the read session read only the
  partitions that the predicate can match. A table clustered by the watermark column lets
  BigQuery skip blocks the same way.
- The value's literal must have the column's type, or one GoogleSQL coerces to it, for pruning
  as for comparing: an `int` for `INT64`, a `date` for `DATE`, an aware `datetime` for
  `TIMESTAMP`, a `str` for `DATETIME`.
- A table partitioned by ingestion time (`_PARTITIONTIME`, `_PARTITIONDATE`) or by another
  column is not pruned by the watermark filter. The reader never filters pseudo-columns.
- A table with `require_partition_filter` partitioned by the watermark column accepts a
  watermark read with a value. A full read and a watermark read without a value fail with
  BigQuery's error: the bound query does, and the read session too as far as the planner knows.

### Consistency of the slice

- The bound query runs when `read()` is called. Each action on the DataFrame opens its own read
  session when it runs, on the table as it is then. The reader pins no snapshot of the table
  **(Q4)**.
- Rows written between `read()` and an action with a watermark above the bound are excluded,
  which is what the bound is for.
- Rows written between `read()` and an action with a watermark at or below the bound, late
  commits, are read by the actions that run after they are written and not by those that ran
  before. Rows updated or deleted in between move into or out of the slice the same way.
- This is the PostgreSQL reader's guarantee: the range is frozen, not the table.
- A full read is not frozen: every action reads the table as it is.

### Options

- The reader sets no default option. The writer sets `writeMethod` to `direct` by default.
- A user option replaces the connector's option of the same name, with names compared
  case-insensitively, as Spark compares them. Only the user's key is sent.
- Values are passed to Spark as given. Spark turns them into strings, and booleans into `true`
  and `false`.
- Some keys are reserved. Giving one, in any case, raises
  `ValueError("option '<key>' is set by the connector")` when the connector is built:

  | Connector | Reserved keys | Why |
  |---|---|---|
  | both | `table`, `path`, `project`, `dataset` | The connector names the table. |
  | both | `parentProject` | It is set from `billing_project`. |
  | both | `credentials`, `credentialsFile`, `gcpAccessToken`, `gcpAccessTokenProvider` | Spark's credentials come from its environment, as the client's do **(Q9)**. |
  | `BigQueryReader` | `filter` | The connector sets the watermark filter. |
  | `BigQueryReader` | `query` | A query replaces the table, and cannot be bounded by a watermark. |
  | `BigQueryReader` | `snapshotTimeMillis` | It reads a snapshot older than the one the bound query sees. |
  | `BigQueryWriter` | `writeAtLeastOnce` | It gives up the pending streams that load the staging table exactly once, so retried tasks could duplicate rows. |

- Everything else passes through. For the reader, for example: `maxParallelism`,
  `preferredMinParallelism`, `readDataFormat`, `viewsEnabled`, `materializationProject`,
  `materializationDataset`. For the writer: `writeMethod`, `temporaryGcsBucket`,
  `persistentGcsBucket`, `intermediateFormat`.
- **The indirect method.** `options={"writeMethod": "indirect", "temporaryGcsBucket": "<bucket>"}`
  makes Spark load the staging table through files in that bucket and one load job. The rest of
  the write is the same, and so are its guarantees.
- **Views.** With `viewsEnabled` set to `true` (and `materializationDataset` where Spark's data
  source needs it), the reader reads a view: the data source runs a query over the view, with the
  filter, into a temporary table, and reads that. This is possible, not claimed or tested.

### Writing

**Operations.**

- `write(data)`, that is with `replace=False`, appends: it inserts every row of `data` into the
  target and leaves the existing rows untouched. `write` is the method of the `Writer` protocol,
  and `WriteTarget` calls `write(data)`, so `WriteTarget` always appends.
- `write(data, replace=True)` replaces: in one statement it deletes every row of the target and
  inserts every row of `data`, so the target ends up holding exactly the rows of `data`. On a
  partitioned table this is every partition, not only those that `data` touches. The target's
  definition is kept: its schema, partitioning, clustering, descriptions, policy tags, labels
  and options. Only its rows change.
- The calling callback chooses, at call time, by passing `replace`. The connector's
  configuration has no mode, and there is no `replace()` method.

**Sequence.** `<target>` is the quoted `table_name`. `<stg id>` is
`<project>.<dataset>.ingestio_staging_<hex>`, and `<stg>` the same table quoted. `<cols>` stands
for `data.columns` quoted and joined with `, `, and `<S.cols>` for the same columns, each one
written `S.` followed by its quoted name, joined with `, `.

0. When `data.columns` is empty, the writer raises `ValueError("the DataFrame has no columns")`
   and does nothing else.
1. **Client.** The writer uses `client` when it is given. Otherwise it creates one with
   `google.cloud.bigquery.Client(project=billing_project)`, as the reader does.
2. **Setup.**
   1. `client.get_table("<project>.<dataset>.<table>")`. A missing target fails here, with
      BigQuery's error.
   2. Each DataFrame column, in order, is matched with the target's field of the same name,
      compared case-insensitively (`str.lower()`), as BigQuery compares column names. For the
      first column that has no match, the writer raises
      `ValueError("table '<table_name>' has no column '<name>'")`, with the name as the
      DataFrame spells it.
   3. `client.create_table(<table>)`, with the table as its only argument: a
      `google.cloud.bigquery.Table` with the ID `<stg id>`, the schema described under
      [Staging table](#staging-table), and `expires` set to `datetime.now(UTC)` plus one day.
3. **Load.** `data.write.format("bigquery").options(...).mode("append").save()`, with no
   argument to `save()`, and the options: `table` = `<stg id>`, `writeMethod` = `direct`,
   `parentProject` = `billing_project` only when it is given, and the user's `options`, which
   replace the connector's options of the same name. The executors write into the staging table
   only.
4. **Apply.** `client.query(<statement>)`, with the statement as its only argument, then
   `result()` on the job, once. The statement is, with `replace=False`:

   ```text
   INSERT INTO <target> (<cols>) SELECT <cols> FROM <stg>
   ```

   and with `replace=True`:

   ```text
   MERGE <target> AS T USING <stg> AS S ON FALSE WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (<S.cols>) WHEN NOT MATCHED BY SOURCE THEN DELETE
   ```

   In the `MERGE`, no row matches, so every row of the target is deleted and every row of the
   staging table is inserted, in one statement.
5. **Drop.** `client.delete_table("<stg id>", not_found_ok=True)`.
6. A client that the writer created is closed with `close()`, after everything else, whether
   the write succeeded or failed. A client given is never closed.

Every statement uses single spaces, exactly as shown.

#### Staging table

- Its name is `ingestio_staging_` followed by `uuid.uuid4().hex`: 32 lowercase hexadecimal
  digits. Each call gets a new name.
- It is created in the target's project and dataset, so it lives in the target's location,
  which the statement that applies it requires, and needs no dataset of its own.
- It expires one day after it is created, so BigQuery removes it even when the writer cannot
  **(Q7)**.
- Its schema has one field per DataFrame column, in the DataFrame's order **(Q6)**. Each field
  is built from the target's field that the column matched:
  - its name is the column's name as the DataFrame spells it;
  - its type, its nested fields and its type parameters (maximum length, precision and scale)
    are the target field's;
  - its mode is `NULLABLE` where the target's is `REQUIRED`; `NULLABLE` and `REPEATED` are kept;
  - nested fields follow the same rules at every depth, with the target's names;
  - it has no description, no default value and no policy tags.
- So Spark's rows land with the types the target expects, and only the apply enforces the
  target's rules: its `REQUIRED` columns, and the default values of the columns the DataFrame
  lacks, which `INSERT` and `MERGE` fill in.

#### Atomicity

Steps 1 to 3 never touch the target. Step 4 is one DML statement, which BigQuery commits
atomically: readers of the target see none of the new rows until it commits, and then all of
them; with `replace=True`, they see the old rows until then, and never an empty or partial
table. After a failure at any step, the target is as it was. The write method that loads the
staging table plays no part in this.

#### Failures

| Fails | What the writer does | Then |
|---|---|---|
| step 0 | nothing | `ValueError` |
| step 1, or step 2 (for example: the target is missing, a column is missing, no permission to create tables) | there is nothing to clean up | the error propagates |
| step 3 or step 4 | `client.delete_table("<stg id>", not_found_ok=True)` | the original exception propagates, the same object |
| the cleanup, after a failed step 3 or 4 | the original exception gets one note, added with `add_note`: `staging table <stg id> could not be dropped: <type>: <message>` | the original exception propagates |
| step 5, after a successful step 4 | nothing: the rows are written, and the staging table expires on its own **(Q8)** | `write` returns `None` |

- In the note, `<type>` and `<message>` are the cleanup error's `type(...).__name__` and
  `str(...)`, and `<stg id>` is unquoted. The cleanup error itself is not raised.
- "Fails" covers creating a job and reading its result: a statement that BigQuery rejects
  raises from `result()`.
- A client the writer created is always closed, after the cleanup when there is one.
- If the driver process dies, the cleanup cannot run, and the staging table stays until it
  expires.
- The connectors write no log lines.

#### What the writer does not do

- It runs no DDL on the target. A missing target fails step 2.
- It reads only `data.columns` and `data.write` from the DataFrame, and calls `save()` once. The
  DataFrame is evaluated exactly once, by the save.
- It never counts or inspects rows. An empty DataFrame goes through the same sequence: an append
  inserts nothing, and `replace=True` leaves the target empty.
- It takes no session: it uses `data.write`.
- It sets no job configuration, label or location on the statements: the client's defaults
  apply, and BigQuery runs each statement where its tables are.
- `write` returns `None`. It does not retry.

### Costs

What a read or a write costs is BigQuery's pricing, and this spec relies on none of it. For
orientation, as the planner knows it:

- the bound query is a query job, run once per watermark `read()`, billed on the bytes of the
  watermark column in the partitions it reads, or in slots under capacity pricing;
- each action on the DataFrame is a new read session, billed by the Storage Read API on the
  data it reads;
- a write loads the batch once into the staging table, through the Storage Write API or, with
  the indirect method, a load job, and then applies it with a DML statement billed as a query:
  an `INSERT` reads the staging table, a `MERGE` reads the staging table and the target;
- the staging table is stored for the length of the write.

### The `bigquery` extra

Following the pattern for extras of the PostgreSQL spec:

1. `bigquery = ["google-cloud-bigquery>=3.20"]` in `[project.optional-dependencies]`, with no
   extra of that package: the connectors need neither pandas nor pyarrow.
2. `ingestio/connector/bigquery.py` imports pyspark and google-cloud-bigquery only inside the
   functions that use them, and for annotations under `TYPE_CHECKING`.
3. The connectors raise the `ImportError` described under
   [Building the connectors](#building-the-connectors).
4. pyspark and spark-bigquery-connector are prerequisites, never dependencies. The connector is
   a JAR: Databricks Runtime includes it, and open-source Spark loads it.
5. The `dev` group gains `google-cloud-bigquery>=3.20`. It already holds `pyspark>=3.5`.

The main session adds both lines to `pyproject.toml` and refreshes `uv.lock` after approval.

### Where it lives

- `src/ingestio/connector/bigquery.py` **(Q1)**: `BigQueryReader` and `BigQueryWriter`, with a
  module docstring and a sorted `__all__`.
- `src/ingestio/connector/all.py`: exports the two classes.
- `pyproject.toml` and `uv.lock`: the extra and the `dev` group, by the main session.
- No change to `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or `connector/core.py`.

### Supported environments

- **pyspark** 3.5 or later, a prerequisite that the runtime provides.
- **spark-bigquery-connector**, a prerequisite. Databricks Runtime includes it, as the `bigquery`
  format. Open-source Spark needs the connector's build for its Spark version on the classpath,
  for example `spark.jars.packages=com.google.cloud.spark:spark-3.5-bigquery:<version>`. The
  minimum version is 0.24.2, which the connector's documentation asks for with the direct
  method, because earlier versions could delete a table; every build for Spark 3.5 is later.
  This spec relies only on the `table`, `filter`, `parentProject` and `writeMethod` options,
  and on appending into an existing table. See [Known limitations](#known-limitations) for
  what the planner could not verify.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated access
  mode, with the bundled connector. Standard access mode and serverless compute: no claim, not
  tested. Only the DataFrame API is used, never `sparkContext` or `_jvm`.
- **Open-source Spark** 3.5 or later, on a Java version that release supports (17 or 21 for
  Spark 4).
- **Credentials.** Application Default Credentials must reach the driver, for the client and for
  Spark's driver-side calls, and every executor, for the read and write streams. On Databricks on
  Google Cloud, a Google service account attached to the cluster provides them. Elsewhere, the
  cluster's environment must provide them, for example through `GOOGLE_APPLICATION_CREDENTIALS`
  pointing to a key file present on every node, or through the data source's own Spark
  configuration.
- **google-cloud-bigquery** on the driver, installed with the `bigquery` extra.
- **Network.** The driver reaches the BigQuery API, and the executors reach the Storage Read
  and Storage Write APIs.
- **Permissions.** The reader needs to read the table (BigQuery Data Viewer), to create read
  sessions in the billing project (BigQuery Read Session User) and, for watermark reads, to run
  jobs there (BigQuery Job User). The writer needs to get the target, to create, write, read and
  delete tables in its dataset (BigQuery Data Editor), and to run jobs in the billing project
  (BigQuery Job User). The indirect method also needs the bucket.

### Known limitations

These are documented, not solved here.

**Not verified by the planner.** The planner knows spark-bigquery-connector and BigQuery from
their documentation and code as of its training, and could not run either. The integration
tests are the check. In particular:

- that the Storage Read API's row restriction accepts the typed literals (`TIMESTAMP '...'`,
  `DATE '...'`, `BIGNUMERIC '...'`), the literals that `FORMAT('%T')` returns (with double
  quotes), `FALSE`, and string literals coerced to `DATE`, `DATETIME` or `TIMESTAMP`, as a query
  does;
- whether Spark's pushed-down filters are combined with the `filter` option or replaced by it.
  Older versions of the connector replaced them. Either way Spark applies its own filters to
  the rows it receives, so the result is the same and only the amount read differs;
- that the direct method's append is atomic and exactly once, through pending streams committed
  together and Spark's commit coordinator. The staging table makes this matter only for
  duplicates, not for the target's atomicity;
- that the Databricks Runtime's bundled connector supports the direct method. Where it does not,
  the job passes `writeMethod=indirect` and a bucket;
- that an empty DataFrame can be written with the direct method;
- whether rows that are still in a table's streaming buffer are returned by the Storage Read
  API. The bound query sees them. If a read session does not return them, they are below the
  bound and are never extracted;
- the minimum connector version, 0.24.2, taken from the connector's documentation.

**Reading.**

- **Late commits.** A row written after a read, with a watermark at or below the bound that read
  captured, is never extracted by a later run, as with the PostgreSQL reader. Rows that share the
  exact value of the previous watermark but arrive later are missed the same way.
- **The table is not pinned.** Actions see the table at different moments (see
  [Consistency of the slice](#consistency-of-the-slice)), and full reads are not frozen at all.
- **`DATETIME` columns** need a `str` value **(Q3)**. **`FLOAT64` columns**: their bound's text
  may not compare equal to the stored value, so the row holding the highest watermark could be
  missed. Watermarks on other types (`BOOL`, `BYTES`, `TIME`, nested fields) are not supported:
  `watermark_column` names a top-level column.
- **`require_partition_filter`** makes full reads and first runs without a value fail.
- Every action reads BigQuery again, and is billed again. Caching is the job's choice.
- The types of the rows read are Spark's data source's: for example, `DATETIME` arrives as a
  timestamp without time zone or as a string, depending on its version.

**Writing.**

- **`require_partition_filter`** on the target makes `replace=True` fail, because the `MERGE`
  reads the target without a filter on its partitioning column. Appends work.
- **Streaming buffer.** `replace=True` fails when the target holds rows still in the streaming
  buffer of the legacy streaming API, which DML cannot delete. The error propagates and the
  target is unchanged.
- **Concurrent DML.** BigQuery queues or fails mutating statements that run on the same table at
  the same time, which can make a `replace=True` fail when another job writes the target.
- **BigQuery's limits** on the partitions that one statement modifies apply to the apply.
- **Ingestion-time partitioned targets** receive the rows in the partition of the moment the
  apply runs.
- **The staging table** is readable by whoever can read tables in the target's dataset while it
  exists: the target's policy tags and row access policies do not apply to it. A write that
  lasts longer than a day loses its staging table and fails, leaving the target unchanged. A
  staging table whose removal fails stays until it expires.
- **Twice the data movement.** The batch is loaded into the staging table and then copied into
  the target by a statement, which costs more than a direct write into the target **(Q5)**.
- **Type conversion.** The values go through the type conversion of Spark's data source into the
  staging table's types. The connectors add none of their own.

**Names and clients.**

- `table_name` must be `project.dataset.table`, so domain-scoped project IDs, which contain a
  `.`, are not supported **(Q2)**. Spark's data source parses the `table` option itself, and may
  reject table names that BigQuery accepts, such as names with spaces.
- A client given whose default query job configuration selects legacy SQL fails the statements,
  which are GoogleSQL.
- Spark and the client each find the environment's credentials on their own. A job that gives
  the client other credentials must give Spark the same identity through the environment.

Databricks recommends Lakehouse Federation for read-only queries of BigQuery. These connectors
are for jobs that write, that filter by watermark, or that run on open-source Spark.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM, a
network or credentials. The doubles live in `tests/test_bigquery_connectors.py` and share one
ordered event log.

- **Fake Spark session.** Each access to `spark.read` returns a new fake reader, as PySpark does.
  It supports `format(name)`, `option(key, value)`, `options(**kwargs)` and `load()`, and
  records one *load* each time `load()` is called, with the format, the merged options and the
  arguments of `load()`. A later call replaces an earlier key with the same name, compared
  case-insensitively, as Spark does; the original case of every key is kept. Values are compared
  as Spark sees them: `str(value)`, and booleans as `true` and `false`. `load()` returns a new
  fake DataFrame. The fake records every attribute accessed on the session.
- **Fake DataFrame.** It records every attribute accessed on it. A DataFrame given to the writer
  has `columns` and `write`. `write` returns a fake writer that supports `format`, `option`,
  `options`, `mode` and `save`, and records a *save*, with its format, mode, merged options and
  arguments, when `save()` is called.
- **Fake client.** It records every method call, with its arguments, in the event log. It
  supports `query(...)`, which returns a fake job whose `result()` returns the rows the test
  chose, a list of tuples such as `[('TIMESTAMP "2026-10-21 09:30:00.5+00"',)]` or `[(None,)]`;
  `get_table(...)`, which returns the `google.cloud.bigquery.Table` the test chose;
  `create_table(...)`, `delete_table(...)` and `close()`. Any of them can be made to raise.
- **The client factory.** The test replaces `google.cloud.bigquery.Client` with
  `monkeypatch.setattr`, by a callable that records its arguments and returns the fake client
  or raises.
- **Real types.** google-cloud-bigquery is in the `dev` group, so the tests build the target's
  `Table`, `SchemaField` and `PolicyTagList` objects with the real classes, and inspect the
  `Table` given to `create_table` through its attributes: `project`, `dataset_id`, `table_id`,
  `expires`, and each field's `name`, `field_type`, `mode`, `fields`, `max_length`,
  `precision`, `scale`, `description`, `policy_tags` and `default_value_expression`.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. To simulate a
  missing package, map its name (`pyspark`, `google.cloud.bigquery`) to `None` in `sys.modules`
  in a fresh subprocess. For google-cloud-bigquery a subprocess is needed: once the package has
  been imported, `from google.cloud import bigquery` finds it as an attribute of
  `google.cloud` without consulting `sys.modules`.
- Tests use the public API. They pass the fakes as `spark=`, `client=` and DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against BigQuery. They are marked
`@pytest.mark.integration` and skipped, with a reason, when the environment variable
`INGESTIO_BIGQUERY_DATASET` is unset. Its value is `project.dataset`, an existing dataset where
the tests may create and drop tables, and the credentials come from the environment. Each test
creates its own uniquely named tables and drops them. They are written blind: this machine has
no Google Cloud credentials.

Without a JVM, with a real client and the fake Spark session and DataFrames:

1. For tables whose watermark column is `TIMESTAMP`, `DATE`, `DATETIME`, `INT64`, `NUMERIC` and
   `STRING`, each holding a few rows and one `NULL` watermark, a watermark read with a value of
   the matching type (a `str` for `DATETIME`) records a `filter`. Running
   `SELECT COUNT(*) FROM <table> WHERE <filter>` with the real client counts exactly the rows
   above the value. The `NULL` row is never counted. With a value above every row, the filter is
   `FALSE`.
2. On a table partitioned by a `DATE` watermark column with `require_partition_filter`, a
   watermark read with a value succeeds and one without a value raises.
3. A writer whose fake DataFrame's `save()` loads chosen rows into the table its `table` option
   names, through the real client, appends them. A target column with a default value that the
   DataFrame lacks takes its default, and a `REQUIRED` column receives the values.
4. `write(df, replace=True)` on a target partitioned and clustered, with a description, leaves
   exactly the DataFrame's rows, and the partitioning, the clustering and the description are
   unchanged.
5. A batch with a `NULL` in a `REQUIRED` column fails the write. The target is unchanged, and the
   dataset holds no table whose name starts with `ingestio_staging_`.
6. A missing target fails the write, and no staging table is left. During the save, the staging
   table exists and expires about a day later.

When `INGESTIO_SPARK_TESTS` is also `1`, with a local Spark session, Java 17 or 21 and
spark-bigquery-connector on the classpath (how the session gets it is the test-writer's
choice):

7. A full read returns every row.
8. A watermark read on `TIMESTAMP`, `DATE`, `STRING` and, with a `str` value, `DATETIME` columns
   returns exactly the rows above the value through the Storage Read API. A row inserted after
   `read()` with a higher watermark is absent from a second action on the same DataFrame. A row
   with a `NULL` watermark is never returned.
9. `write(df)` and `write(df, replace=True)` with the direct method leave the expected rows, from
   a DataFrame whose fields are all nullable into a target with `REQUIRED` columns.
   `write(empty_df, replace=True)` leaves the target empty.

## Acceptance criteria

Unless a criterion says otherwise:

- `spark` is the fake session, `client` is the fake client, and DataFrames are the doubles of
  [Notes for testing](#notes-for-testing);
- `google.cloud.bigquery.Client` is replaced by a factory that returns `client`. "The factory"
  is that replacement;
- the reader is `BigQueryReader("acme-data.sales.orders", spark=spark)`, and the writer is
  `BigQueryWriter("acme-data.sales.orders")`;
- `<target>` is `` `acme-data`.`sales`.`orders` ``;
- `get_table` returns a `Table("acme-data.sales.orders")` whose schema is
  `SchemaField("id", "INTEGER", mode="REQUIRED")`, `SchemaField("payload", "STRING")` and
  `SchemaField("loaded_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()")`;
- `<hex>` is 32 lowercase hexadecimal digits, the same throughout one call;
  `<stg id>` is `acme-data.sales.ingestio_staging_<hex>`, and `<stg>` is
  `` `acme-data`.`sales`.`ingestio_staging_<hex>` ``;
- `df` is a fake DataFrame with `columns == ["id", "payload"]`;
- the bound query's result is `[('TIMESTAMP "2026-10-21 09:30:00.5+00"',)]`;
- "exactly the options" means these keys and no others, with keys compared case-insensitively
  unless the criterion says the case matters.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import BigQueryReader, BigQueryWriter` runs, then it succeeds.
  Both names are in `ingestio.connector.all.__all__` and in
  `ingestio.connector.bigquery.__all__`, and neither is in `ingestio.__all__`. Depends on Q1.
- **AT-002**: Given an interpreter where `import pyspark` and `import google.cloud.bigquery`
  both raise `ImportError`, when `ingestio.connector.all` is imported, then the import succeeds,
  and `BigQueryReader("acme-data.sales.orders", spark=<fake session>)` can be built. Its
  `read()` returns the DataFrame of the fake's load.
- **AT-003**: Given an interpreter where `import google.cloud.bigquery` raises `ImportError`,
  when `BigQueryWriter("acme-data.sales.orders")` is built, then it raises `ImportError` with the
  message `BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`. When
  `BigQueryReader("acme-data.sales.orders", watermark_column="updated_at")` is built, with or
  without `client=`, then it raises `ImportError` with the message
  `BigQueryReader needs google-cloud-bigquery: pip install 'ingestio[bigquery]'`. Both are
  chained to the original error.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `bigquery` is among its `Provides-Extra`
  values, and one `Requires-Dist` names the `google-cloud-bigquery` project with an
  `extra == "bigquery"` marker.

### Building the connectors

- **AT-005**: Given `name` in `""`, `"orders"`, `"sales.orders"`, `"acme-data.sales."`,
  `".sales.orders"`, `"acme-data..orders"` and `"a.b.c.d"`, when `BigQueryReader(name)` or
  `BigQueryWriter(name)` is built, then it raises
  `ValueError("table_name must be 'project.dataset.table'")`. `"acme-data.sales.orders"` and
  `"acme-data.Sales.order lines"` are accepted by both. Depends on Q2.
- **AT-006**: Given `BigQueryReader("acme-data.sales.orders", watermark_value=41)`, then it
  raises `ValueError("watermark_value needs watermark_column")`. Given `watermark_column=""`,
  then it raises `ValueError("watermark_column must not be empty")`. Given
  `billing_project=""`, then both connectors raise
  `ValueError("billing_project must not be empty")`.
- **AT-007**: Given `watermark_column="updated_at"` and the `watermark_value` on the left, when
  the reader is built, then it raises `ValueError` with the message on the right:

  | `watermark_value` | Message |
  |---|---|
  | `True` | `unsupported watermark_value type: bool` |
  | `1.5` | `unsupported watermark_value type: float` |
  | `[1]` | `unsupported watermark_value type: list` |
  | `Decimal("NaN")` | `watermark_value must be finite` |
  | `Decimal("Infinity")` | `watermark_value must be finite` |
  | `datetime(2026, 10, 1)` | `watermark_value must be timezone-aware` |

  `0`, `""`, `Decimal("12.50")`, `date(2026, 10, 1)` and `datetime(2026, 10, 1, tzinfo=UTC)`
  are accepted.
- **AT-008**: Given `key` in `table`, `PATH`, `project`, `Dataset`, `parentproject`,
  `credentials`, `CredentialsFile`, `gcpAccessToken`, `gcpAccessTokenProvider`, `filter`,
  `Query` and `snapshotTimeMillis`, when
  `BigQueryReader("acme-data.sales.orders", options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as given.
  Given `key` in `table`, `path`, `PROJECT`, `dataset`, `ParentProject`, `credentials`,
  `credentialsFile`, `gcpAccessToken`, `gcpAccessTokenProvider` and `writeAtLeastOnce`, the same
  holds for `BigQueryWriter("acme-data.sales.orders", options={key: "x"})`. The reader accepts
  `{"maxParallelism": 8, "viewsEnabled": True, "materializationDataset": "tmp"}`, and the writer
  accepts `{"writeMethod": "indirect", "temporaryGcsBucket": "acme-tmp"}`. Depends on Q9 for the
  credential keys.
- **AT-009**: Given the factory and `spark`, when these are built, then the factory is not
  called and no attribute of `spark` is accessed:
  - `BigQueryReader("acme-data.sales.orders", spark=spark)`;
  - `BigQueryReader("acme-data.sales.orders", spark=spark, watermark_column="updated_at",
    watermark_value=41, billing_project="acme-billing")`;
  - `BigQueryWriter("acme-data.sales.orders", billing_project="acme-billing")`.

  Given `client=client` in addition to each of the three, then no method of `client` is called.
- **AT-010**: Then `repr(BigQueryReader("acme-data.sales.orders"))` is
  `BigQueryReader(table_name='acme-data.sales.orders')`, also when a watermark,
  `billing_project` and `options` are given, and `repr(BigQueryWriter("acme-data.lake.orders"))`
  is `BigQueryWriter(table_name='acme-data.lake.orders')`.

### Full read

- **AT-011**: When `reader.read()` runs, then exactly one load is recorded, with the format
  `bigquery`, exactly the option `table` = `acme-data.sales.orders`, and no argument to `load()`.
  `read()` returns the very DataFrame that `load()` returned, and no attribute of it was
  accessed. The factory is not called. Given `client=client` in addition, no method of `client`
  is called.
- **AT-012**: Given `billing_project="acme-billing"`, when a full read runs, then the load
  carries `parentProject` = `acme-billing`, with this exact key. When a watermark read with
  `watermark_value=41` runs, then the factory is called once with exactly `project="acme-billing"`
  as a keyword argument and no positional argument, and the data load carries
  `parentProject` = `acme-billing`. Without `billing_project`, no load carries a `parentProject`
  option, and the factory is called with exactly `project=None`.
- **AT-013**: Given `options={"maxParallelism": 8, "ReadDataFormat": "AVRO", "viewsEnabled":
  True}`, when a full read runs, then the load carries exactly `table`, `maxParallelism` = `8`,
  `ReadDataFormat` = `AVRO` and `viewsEnabled` = `true`, with the user's keys in their case. When
  a watermark read with `watermark_value=41` runs with the same options, the data load carries
  them as well, and `client.query` receives only the query, which contains none of them.
- **AT-014**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced
  with a function that returns the fake session and `read()` runs, then the load is recorded on
  the fake. Given `active` replaced with a function that raises `err` when a watermark `read()`
  runs, then `read()` raises `err` itself, and the factory is not called.

### Watermark read

The reader in this section is
`BigQueryReader("acme-data.sales.orders", spark=spark, watermark_column="updated_at", ...)`.

- **AT-015**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs, then
  the event log shows, in this order:
  1. the factory called once, with exactly `project=None`;
  2. `client.query` called once, with one positional argument and no keyword argument, the
     query

     ```text
     SELECT IF(MAX(`updated_at`) IS NULL, NULL, FORMAT('%T', MAX(`updated_at`))) AS upper_bound FROM `acme-data`.`sales`.`orders` WHERE `updated_at` > TIMESTAMP '2026-10-01 00:00:00+00:00'
     ```

  3. `result()` called once on the job that `query` returned;
  4. `client.close()` called once;
  5. one load, with the format `bigquery` and exactly the options `table` =
     `acme-data.sales.orders` and `filter` =

     ```text
     `updated_at` > TIMESTAMP '2026-10-01 00:00:00+00:00' AND `updated_at` <= TIMESTAMP "2026-10-21 09:30:00.5+00"
     ```

  No other method of `client` is called. `read()` returns the load's DataFrame, and no attribute
  of it was accessed.
- **AT-016**: Given no `watermark_value`, when `read()` runs, then the query is

  ```text
  SELECT IF(MAX(`updated_at`) IS NULL, NULL, FORMAT('%T', MAX(`updated_at`))) AS upper_bound FROM `acme-data`.`sales`.`orders`
  ```

  and the load's `filter` is `` `updated_at` <= TIMESTAMP "2026-10-21 09:30:00.5+00" ``.
- **AT-017**: Given a result of `[(None,)]`, or a result with no row, with or without a
  `watermark_value`, when `read()` runs, then the load's `filter` is `FALSE`, and `read()`
  returns that load's DataFrame.
- **AT-018**: Given the result on the left and `watermark_value=41`, when `read()` runs, then the
  load's `filter` is `` `updated_at` > 41 AND `updated_at` <= `` followed by the text on the
  right, exactly as the result gave it:

  | Result | Text |
  |---|---|
  | `[('DATE "2026-10-21"',)]` | `DATE "2026-10-21"` |
  | `[("41",)]` | `41` |
  | `[('NUMERIC "12.5"',)]` | `NUMERIC "12.5"` |
  | `[('"it\\\'s"',)]` (the text `"it\'s"`) | `"it\'s"` |

- **AT-019**: Given the `watermark_value` on the left, when `read()` runs, then both the query
  and the `filter` contain `` `updated_at` > `` followed by the literal on the right:

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `41` |
  | `0` | `0` |
  | `-7` | `-7` |
  | `Decimal("12.50")` | `BIGNUMERIC '12.50'` |
  | `Decimal("1E+3")` | `BIGNUMERIC '1000'` |
  | `"O'Brien"` | `'O\'Brien'` |
  | `""` | `''` |
  | a `str` holding `a`, one backslash, `b` | `'a\\b'`, with two backslashes |
  | a `str` holding `a`, a line feed, a carriage return, `b` | `'a\n\rb'`, each escape as two characters |
  | `date(2026, 10, 1)` | `DATE '2026-10-01'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `TIMESTAMP '2026-10-01 08:30:00+00:00'` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `TIMESTAMP '2026-10-01 11:30:00.123456+00:00'` |

  Depends on Q3 for the datetimes.
- **AT-020**: Given the `table_name` and `watermark_column` on the left and `watermark_value=1`,
  when `read()` runs, then the query uses the quoted table and the quoted column, and the
  `filter` the quoted column, as on the right, and the load's `table` option is the
  `table_name` as given. A full read with each `table_name` carries the same `table` option.

  | `table_name` | `watermark_column` | Quoted table | Quoted column |
  |---|---|---|---|
  | `acme-data.sales.orders` | `updated_at` | `` `acme-data`.`sales`.`orders` `` | `` `updated_at` `` |
  | `acme-data.Sales.Orders` | `UpdatedAt` | `` `acme-data`.`Sales`.`Orders` `` | `` `UpdatedAt` `` |
  | `acme-data.sales.order lines` | `` my`col `` | `` `acme-data`.`sales`.`order lines` `` | `` `my\`col` `` |
  | `acme-data.sales.orders` | a name holding `a`, one backslash, `b` | `` `acme-data`.`sales`.`orders` `` | `` `a\\b` ``, with two backslashes |

- **AT-021**: Given `client=client` and `billing_project="acme-billing"`, when `read()` runs
  twice, then the factory is never called, `client.query` is called once per read,
  `client.close()` is never called, and each data load carries `parentProject` = `acme-billing`.
- **AT-022**: Given the failure on the left, when a watermark `read()` runs, then it raises `err`
  itself, with the consequences on the right:

  | Failure | Then |
  |---|---|
  | the factory raises `err` | no load is recorded |
  | `client.query` raises `err` | no load is recorded; `client.close()` was called once |
  | `result()` raises `err` | no load is recorded; `client.close()` was called once |
  | the fake's `load()` raises `err` | `client.close()` was called once, before the load |

  Given `client=client` and `client.query` raising `err`, then `read()` raises `err` itself and
  `client.close()` is not called. Given a full read whose `load()` raises `err`, then `read()`
  raises `err` itself.

### Writing

- **AT-023**: When `writer.write(df)` runs, then the event log shows, in this order:
  1. the factory called once, with exactly `project=None`;
  2. `client.get_table("acme-data.sales.orders")`, with that one positional argument;
  3. `client.create_table(table)`, with one positional argument, a `google.cloud.bigquery.Table`
     whose `project`, `dataset_id` and `table_id` are `acme-data`, `sales` and
     `ingestio_staging_<hex>`, and whose schema has two fields, `id` (`INTEGER`, `NULLABLE`)
     and `payload` (`STRING`, `NULLABLE`), in this order, with no description, policy tags or
     default value expression;
  4. one save, with the format `bigquery`, the mode `append`, no argument to `save()`, and
     exactly the options `table` = `<stg id>` and `writeMethod` = `direct`;
  5. `client.query` called once, with one positional argument, the statement

     ```text
     INSERT INTO `acme-data`.`sales`.`orders` (`id`, `payload`) SELECT `id`, `payload` FROM `acme-data`.`sales`.`ingestio_staging_<hex>`
     ```

     and `result()` called once on its job;
  6. `client.delete_table("<stg id>", not_found_ok=True)`;
  7. `client.close()`.

  No other method of `client` is called, no other save or load is recorded, and `write` returns
  `None`. The only attributes accessed on `df` are `columns` and `write`. `writer.write(df,
  replace=False)` gives the same event log. Depends on Q5 and Q6.
- **AT-024**: When `writer.write(df, replace=True)` runs, then the event log is the one of
  AT-023, except that the statement is

  ```text
  MERGE `acme-data`.`sales`.`orders` AS T USING `acme-data`.`sales`.`ingestio_staging_<hex>` AS S ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `payload`) VALUES (S.`id`, S.`payload`) WHEN NOT MATCHED BY SOURCE THEN DELETE
  ```

  Depends on Q5.
- **AT-025**: Given `get_table` returning a table whose schema is:
  - `SchemaField("id", "INTEGER", mode="REQUIRED", description="order key")`;
  - `SchemaField("Payload", "RECORD", fields=[SchemaField("sku", "STRING", mode="REQUIRED",
    max_length=20), SchemaField("tags", "STRING", mode="REPEATED")])`;
  - `SchemaField("amount", "NUMERIC", precision=10, scale=2, policy_tags=PolicyTagList(
    ["projects/p/locations/us/taxonomies/1/policyTags/2"]))`;
  - `SchemaField("loaded_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()")`;

  and a DataFrame with `columns == ["amount", "payload", "id"]`, when `write` runs, then the
  schema of the table given to `create_table` has exactly these fields, in this order:
  - `amount`: `NUMERIC`, `NULLABLE`, precision 10, scale 2;
  - `payload`: `RECORD`, `NULLABLE`, with the nested fields `sku` (`STRING`, `NULLABLE`, maximum
    length 20) and `tags` (`STRING`, `REPEATED`), in this order;
  - `id`: `INTEGER`, `NULLABLE`.

  Each field, nested fields included, has the `name`, `field_type`, `mode`, `max_length`,
  `precision` and `scale` shown (`None` where none is shown), and its `description`,
  `policy_tags` and `default_value_expression` are `None`. The statement's column lists are
  `` `amount`, `payload`, `id` ``, and `` S.`amount`, S.`payload`, S.`id` `` with
  `replace=True`. Depends on Q6.
- **AT-026**: Given two consecutive `write(df)` calls, then each uses a staging table whose
  `table_id` matches `ingestio_staging_[0-9a-f]{32}`, and the two differ. Within one call the
  same name appears in `create_table`, in the save's `table` option, in the statement and in
  `delete_table`. Given `t0 = datetime.now(UTC)` taken just before a call and `t1` just after,
  the table given to `create_table` has `expires` no earlier than `t0 + timedelta(days=1)`
  truncated to the millisecond and no later than `t1 + timedelta(days=1)`.
- **AT-027**: Given `BigQueryWriter("acme-data.sales.orders", billing_project="acme-billing",
  options={"WriteMethod": "indirect", "temporaryGcsBucket": "acme-tmp"})`, when `write(df)`
  runs, then the factory is called with exactly `project="acme-billing"`, and the save carries
  exactly `table` = `<stg id>`, `parentProject` = `acme-billing`, a single write method option,
  `WriteMethod` = `indirect` with the user's key, and `temporaryGcsBucket` = `acme-tmp`. The rest
  of the event log is the one of AT-023. Without user options, the save's keys `writeMethod` and
  `parentProject` are in exactly this case.
- **AT-028**: Given `BigQueryWriter("acme-data.sales.orders", client=client)`, when `write(df)`
  and `write(df, replace=True)` run, then the factory is never called, every call of AT-023 and
  AT-024 is made on `client`, and `client.close()` is never called.
- **AT-029**: Given a save that raises `err = RuntimeError("executor lost")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself. After `get_table` and
  `create_table`, the client receives `delete_table("<stg id>", not_found_ok=True)` and then
  `close()`. `client.query` is never called.
- **AT-030**: Given `client.query` raising `err` on the statement, or the job's `result()`
  raising `err`, when `write(df)` or `write(df, replace=True)` runs, then it raises `err` itself,
  and the client then receives `delete_table("<stg id>", not_found_ok=True)` and `close()`, in
  this order.
- **AT-031**: Given the failure on the left, when `write(df)` runs, then it raises `err` itself,
  and:

  | Failure | Then |
  |---|---|
  | the factory raises `err` | no method of any client is called and no save is recorded |
  | `get_table` raises `err` | `create_table`, `query` and `delete_table` are not called, no save is recorded, `close()` is called |
  | `create_table` raises `err` | `query` and `delete_table` are not called, no save is recorded, `close()` is called |

- **AT-032**: Given a save that raises `err`, and a `delete_table` that raises
  `RuntimeError("refused")`, when `write(df)` runs, then it raises `err` itself, and
  `err.__notes__` is
  `['staging table acme-data.sales.ingestio_staging_<hex> could not be dropped: RuntimeError: refused']`.
  `close()` is called after the `delete_table`. The same holds when the failure is the
  statement of AT-030 instead of the save.
- **AT-033**: Given a successful statement and a `delete_table` that raises
  `RuntimeError("refused")`, when `write(df)` runs, then it returns `None` without raising, and
  `close()` is called after the `delete_table`. Depends on Q8.
- **AT-034**: Given a fake DataFrame with `columns == []`, when `write` runs, with or without
  `replace=True`, then it raises `ValueError("the DataFrame has no columns")`, the factory is not
  called, and no save is recorded.
- **AT-035**: Given a DataFrame with `columns == ["id", "extra", "other"]`, when `write` runs,
  then it raises `ValueError("table 'acme-data.sales.orders' has no column 'extra'")`. The client
  received `get_table` and then `close()`, and nothing else; no save is recorded. Given a
  DataFrame with `columns == ["ID", "Payload"]`, then the write succeeds, the staging schema has
  the fields `ID` (`INTEGER`, `NULLABLE`) and `Payload` (`STRING`, `NULLABLE`), and the
  statement's column list is `` `ID`, `Payload` ``.
- **AT-036**: Given `get_table` returning a table with the fields `order id` and ``a`b``, both
  `STRING`, and a DataFrame with ``columns == ["order id", "a`b"]``, when `write(df)` runs, then
  the statement is

  ```text
  INSERT INTO `acme-data`.`sales`.`orders` (`order id`, `a\`b`) SELECT `order id`, `a\`b` FROM `acme-data`.`sales`.`ingestio_staging_<hex>`
  ```

### Composition

- **AT-037**: Given `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, where the fake load returns a DataFrame
  with `columns == ["id", "payload"]`, when it runs, then `process.state["orders"]` is that
  DataFrame, and the writer's events are those of AT-023 on it: `WriteTarget` calls
  `write(data)`, so it appends and the statement is an `INSERT`. The test binds the reader to a
  variable annotated `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`, and
  mypy strict accepts both.
- **AT-038**: Given `caplog` capturing every level, when readers and writers are built, full and
  watermark reads run, and writes run with and without `replace=True`, successful and failing,
  then no log record is captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Spark is the engine for the data.** The reader returns a Spark DataFrame and the writer
   receives one. Data moves through Spark's BigQuery data source, spark-bigquery-connector, a
   prerequisite like pyspark: Databricks Runtime includes it, and open-source Spark needs the
   JAR. The native client, google-cloud-bigquery, runs on the driver for control work only,
   such as the upper-bound query and whatever the atomic write needs. It goes into a new extra,
   `ingestio[bigquery]`, imported lazily so that the import surface works without it.
2. **Reading follows `PostgresReader`.** An optional `watermark_column` and `watermark_value`;
   the filter `watermark_column > watermark_value` runs inside BigQuery, pushed into the Storage
   Read API as a row restriction; the slice is frozen by an upper bound captured at `read()`
   time, as PostgreSQL decision 10 does, with the bound query running through
   google-cloud-bigquery. Without a watermark column, a full read. On a table partitioned by the
   watermark column, the filter prunes partitions; the spec says whether anything more is
   needed. The accepted values are `int`, `Decimal`, `str`, `date` and timezone-aware
   `datetime`. The spec's answer on partitions: nothing more is needed (see
   [Partitioned and clustered sources](#partitioned-and-clustered-sources)).
3. **Writing is `write(data, *, replace=False)`, atomic for both append and replace, without
   requiring a Cloud Storage bucket.** The planner chooses the write method that is atomic for
   both, preferring the direct method, the Storage Write API, which needs no bucket. The
   indirect method, files in a temporary bucket followed by a load job, stays possible through
   `options`. Where the direct method is not atomic for replace in some versions, the planner
   proposes the mechanism that is, records the minimum connector version as a prerequisite, and
   states the uncertainty in Known limitations.
4. **Credentials come from the environment,** Application Default Credentials, for both Spark
   and the client, with an optional billing project that may differ from the table's project:
   spark-bigquery-connector's `parentProject` and the client's `project`. As with the other
   connectors decided in this session, the job may pass a ready `google.cloud.bigquery.Client`
   through an optional `client` argument. The default client is created at `read()` or
   `write()` time, never at construction. That argument is also the test seam.
5. **Everything else follows the PostgreSQL and disk connectors:** constructor arguments by
   ADR 0013, lazy pyspark import, the session looked up at read time or given as `spark=`,
   errors that propagate unchanged, no log lines, the batch-leaning amendment of ADR 0012, and
   exports from `ingestio.connector.all` only. The main session adds the extra and the `dev`
   packages to `pyproject.toml` and `uv.lock` after approval.

Proposed by this spec, where the precedents give a clear answer or the point is minor, and
accepted by the main session with its approval on 2026-10-23, under the maintainer's delegation:

6. **Building only validates, and imports google-cloud-bigquery when the connector will need
   it.** The writer always needs it; the reader only for watermark reads, so a reader without a
   watermark column works without the extra. This is rule 3 of the pattern for extras, applied
   to what each connector uses. No client is created and no session is touched at
   construction.
7. **The watermark filter goes to the Storage Read API through the data source's `filter`
   option,** not through Spark's predicate pushdown. It does not depend on what Spark pushes
   down, the connector renders its literal, and tests observe it without a JVM. This is the
   counterpart of PostgreSQL decision 8.
8. **The bound is captured with `FORMAT('%T', MAX(<col>))`,** which returns the highest
   watermark as the text of a GoogleSQL literal of the column's type, and that text goes back
   into the filter unchanged. It is the counterpart of PostgreSQL's `max(<col>)::text` compared
   back as an untyped literal: GoogleSQL has no untyped literal that takes the column's type, so
   the bound carries its type in its own text, and never passes through Python or Spark types.
   The query returns SQL `NULL` when no row qualifies, through `IF(MAX(<col>) IS NULL, ...)`,
   because `FORMAT('%T', NULL)` returns the text `NULL`. The slice is then empty, with the
   filter `FALSE`, as PostgreSQL's `WHERE false`.
9. **Watermark values become literals of their own type:** a bare integer, `BIGNUMERIC '...'`,
   a backslash-escaped string, `DATE '...'`, and `TIMESTAMP '...'` in UTC. GoogleSQL does not
   coerce a string literal to a number, so numbers need numeric literals, unlike PostgreSQL.
   `BIGNUMERIC` holds every finite `Decimal` that `NUMERIC` holds and more, and GoogleSQL
   compares it with `NUMERIC`, `BIGNUMERIC`, `INT64` and `FLOAT64` columns, so one rule serves
   every decimal. Aware datetimes are normalised to UTC, as the maintainer approved for
   PostgreSQL. The strict `>` and the exclusion of `NULL` watermarks are PostgreSQL decision 23.
10. **A full read never touches a client,** and needs no extra.
11. **A client the connector creates is closed by it,** after its last use and on failure, so
    that each `read()` or `write()` leaves no open connection behind. A client given is the
    job's, and the connector never closes it.
12. **The staging table** is named `ingestio_staging_<32 hex digits>`, as in PostgreSQL, lives
    in the target's dataset, which shares the target's location, as the statement applying it
    requires, and expires after one day (question 7). The expiration removes it when the driver
    dies or the cleanup fails, which PostgreSQL could not do. One day leaves room for long
    loads.
13. **The apply is one DML statement:** `INSERT ... SELECT` to append, and
    `MERGE ... ON FALSE ... WHEN NOT MATCHED BY SOURCE THEN DELETE` to replace. A single
    statement is atomic, needs no multi-statement transaction, and keeps the target's definition.
    The alternatives were weighed:
    - a copy job with `WRITE_TRUNCATE` costs nothing, but needs a staging table with exactly the
      target's schema and partitioning, which a DataFrame without every column cannot give, and,
      as far as the planner knows, replaces the target's metadata with the source's;
    - `TRUNCATE TABLE` and then `INSERT` are two statements, so the target is empty in between;
    - `DELETE` and `INSERT` in a multi-statement transaction are atomic too, but take a script
      where one statement does.
14. **The writer does not rely on the data source's own overwrite.** As the planner knows it,
    recent versions of spark-bigquery-connector overwrite in the direct method by writing into a
    temporary table and then copying or merging it into the target, and earlier versions deleted
    or truncated the target first. The documentation warns that versions before 0.24.2 could
    delete a table. The planner cannot say from which version the overwrite is atomic and keeps
    the table's definition, nor what the Databricks Runtime bundles, and a session setting
    (`spark.sql.sources.partitionOverwriteMode=dynamic`) can turn an overwrite into a
    partition-level replace. Spark therefore only appends, into the writer's staging table.
15. **`writeMethod` defaults to `direct`** and the job may override it through `options`, as
    decision 3 asks. The staging table makes both methods equally atomic.
16. **Reserved options** are the keys that name the table, that the connector sets, or that
    would change what a read or a write means: listed under [Options](#options). Everything
    else passes through, with no defaults beyond `writeMethod`, as in the disk connectors.
17. **Failures follow `PostgresWriter`:** the original exception propagates, the same object; a
    cleanup failure after a failed write becomes a note on it; there are no log lines.
18. **The writer evaluates the DataFrame once,** never counts it, sends an empty DataFrame
    through the same sequence, and rejects a DataFrame without columns, as `PostgresWriter`
    does (its decisions 17 and 30): the statements need column names.
19. **A missing target fails,** with BigQuery's error from `get_table`, and nothing is created.
    DDL on the target is architecture (PostgreSQL decision 29).
20. **The writer checks the DataFrame's columns against the target** and raises its own
    `ValueError` for the first missing one, because the staging table's types come from the
    target's fields. PostgreSQL left that check to its `CREATE ... AS SELECT`.
21. **Test seams.** `client=` and `spark=` are public. `google.cloud.bigquery.Client` is looked
    up on the module at each use; that seam is not public. The writer takes no session.
22. **The extra is `bigquery = ["google-cloud-bigquery>=3.20"]`,** with no extra of that
    package, and the `dev` group gains the same requirement. As far as the planner knows, 3.20
    is a floor that has every API the connectors and their tests use (`Client.query`,
    `QueryJob.result`, `get_table`, `create_table`, `delete_table(not_found_ok=...)`, `close`,
    `Table.expires`, `PolicyTagList`, and `SchemaField` with type parameters and default value
    expressions) on Python 3.11 and later; the lock resolves a current release. pyspark is already in the `dev` group. The connector JAR is never a Python
    dependency.
23. **Supported environments:** Databricks classic compute on Runtime 15.4 LTS or later in
    dedicated access mode, and open-source Spark 3.5 or later. No claim for standard access
    mode or serverless compute.
24. **Integration tests are opt-in** through `INGESTIO_BIGQUERY_DATASET`, as
    `INGESTIO_POSTGRES_URL` is for PostgreSQL. The tests that need Spark also need
    `INGESTIO_SPARK_TESTS=1`, the disk connectors' variable, because they need Java 17 or 21 and
    the connector JAR. The first tier runs with a real client and fake Spark objects, so it
    checks the SQL against BigQuery without a JVM.
25. **`repr` shows `table_name`,** which identifies the table fully.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept, and the writer
  exposes two operations without choosing between them. Question 8 is the one place where the
  writer does not raise an error, and it concerns housekeeping after a committed write.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged, and their
  payload stays open. Like the PostgreSQL connectors, these lean towards batch: one slice per
  `read()`, a watermark fixed when the reader is built, and a write that needs the whole
  DataFrame before its statement commits. A streaming source, such as BigQuery's change history
  or a Pub/Sub subscription, or a Structured Streaming write, would be a different connector.
  This belongs in the "What leans towards batch" list of
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md): an amendment, not a new ADR.
- **ADR 0013 applies unchanged.** `table_name` and `watermark_column` are configuration keys,
  and the job computes `watermark_value`. The writer's `table_name` names the target, the
  consequence ADR 0013 already records for `PostgresWriter`. The credentials stay out of the
  configuration: they come from the environment or from the client the job builds, the
  counterpart of `PostgresConnection`. Filtering in the source is the Storage Read API's row
  restriction. The doc-keeper may add these points to ADR 0013's consequences.
- **ADR 0002 applies unchanged.** The new extra follows the pattern for extras, and
  spark-bigquery-connector is a prerequisite like pyspark (rule 4).

## Open questions

None open. On 2026-10-23 the maintainer, away for the session, delegated the approval of this
spec to the main session, with the instruction to take the recommended option of every question
that the maintainer's decisions do not settle. The main session took the recommended option of
all nine; none contradicts decisions 1 to 5. The options are kept below as the record of what
was weighed, for the maintainer to review.

1. **Names.**
   - (a) `BigQueryReader` and `BigQueryWriter` in `connector/bigquery.py`, with the arguments
     `billing_project` and `client`. The module is named after the system, as
     `connector/postgres.py` is. "Billing project" says what the setting does.
   - (b) The same classes, with `parent_project`, the data source's own word, for the billing
     argument.
   - (c) A module `connector/google.py` that the GCS connector would share.

   **Recommended: (a).**
2. **Which table names are accepted, and how does the project resolve?**
   - (a) Only `project.dataset.table`. Spark and the client receive the same full name, so they
     can never resolve it to different projects. A configuration that holds `dataset.table`
     must add the project. The disk connectors reject relative paths for the same reason: two
     engines, two defaults.
   - (b) Also `dataset.table`, whose project is `billing_project` when given, and otherwise the
     client's project. A full read would then need a client, and the extra, only to learn the
     project.
   - (c) Also `dataset.table`, passed unqualified to both, each resolving it to its own default
     project. With `billing_project` given, Spark's data source and the client may then resolve
     it to different projects.

   Domain-scoped project IDs (`example.com:project`), which contain a `.`, are not supported in
   any case. **Recommended: (a).**
3. **Watermarks on `DATETIME` columns.**
   - (a) An aware datetime is always a `TIMESTAMP` literal in UTC. A `DATETIME` column takes a
     `str` in its civil time, which GoogleSQL coerces. No type lookup; the job must know its
     column's type.
   - (b) The reader looks up the column's type with `get_table` at each `read()`, and renders an
     aware datetime as `DATETIME '<UTC wall clock>'` on a `DATETIME` column, as PostgreSQL
     compares a `timestamp without time zone` column on the UTC wall clock. One more call, and
     type logic in the connector.
   - (c) Accept naive datetimes as `DATETIME` literals. Against PostgreSQL decision 24 and
     CONTRIBUTING's aware datetimes.

   **Recommended: (a).**
4. **Should `read()` pin a snapshot of the table?**
   - (a) No. The range is frozen by the bound, as in PostgreSQL, and actions see the table at
     their own moment (see [Consistency of the slice](#consistency-of-the-slice)).
   - (b) Yes. The bound query also returns `CURRENT_TIMESTAMP()`, reads
     `FOR SYSTEM_TIME AS OF` it, and the data load gets `snapshotTimeMillis` set to it, so every
     action reads the same snapshot, full reads included. It needs a connector version that has
     `snapshotTimeMillis`, which the planner cannot name, does not work on views, and fails once
     the snapshot is older than the table's time-travel window.

   **Recommended: (a).**
5. **How does the writer make append and replace atomic?**
   - (a) A staging table for both: Spark appends into it, with the direct method by default, and
     one statement applies it, `INSERT ... SELECT` or `MERGE`. Atomic whatever the method and
     the connector's version, the same semantics for both operations (column defaults,
     `REQUIRED` columns, types), no mode-check failure on `REQUIRED` columns, and no DDL on the
     target. Costs one more statement and moves the data twice.
   - (b) Append directly into the target with the direct method, atomic through its pending
     streams, and a staging table only for replace. Cheaper for appends, but an append relies on
     the data source's atomicity, fails on `REQUIRED` columns unless the job passes
     `enableModeCheckForSchemaFields=false`, and lets the data source create a missing target.
   - (c) The data source alone for both: the direct method's append and overwrite, with a
     minimum version that the planner cannot name with confidence, and the overwrite's
     behaviour on the target's definition unverified.

   **Recommended: (a).**
6. **How is the staging table created?**
   - (a) From the target's metadata: `get_table`, then `create_table` with the target's fields for
     the DataFrame's columns, modes relaxed. A metadata call, free, works on targets with
     `require_partition_filter`, keeps type parameters, and needs no read permission on the
     target's data. The connector checks the columns itself.
   - (b) With a query, `CREATE TABLE <stg> OPTIONS (expiration_timestamp = ...) AS SELECT <cols>
     FROM <target> WHERE FALSE`, the counterpart of PostgreSQL's `CREATE ... AS SELECT ... WITH
     NO DATA`. One statement, and BigQuery checks the columns. It fails on targets with
     `require_partition_filter`, so even appends to them fail, drops type parameters, and needs
     read permission on every column of the target, policy-tagged columns included.

   **Recommended: (a).**
7. **How long does a staging table live if it is not dropped?**
   - (a) One day.
   - (b) Seven days, for very long loads, at the price of orphaned copies of the data lingering
     longer.
   - (c) No expiration, as in PostgreSQL.

   **Recommended: (a).**
8. **What happens when the staging table cannot be dropped after the write has committed?**
   - (a) Nothing is raised: the rows are written, and the table expires on its own (question 7).
     Raising would make a committed append look failed, and a job that retries it would write
     the rows twice. The failure is not reported anywhere.
   - (b) The cleanup error is raised, with a note saying that the rows were written. Honest, but
     fails the pipeline after a successful write.
   - (c) A Python warning, through `warnings.warn`. Visible without failing, but a mechanism the
     library does not use yet, and outside its logging.

   In PostgreSQL the drop commits with the apply, so the case did not arise. BigQuery cannot
   drop a table inside a transaction. **Recommended: (a).**
9. **Can `options` carry Spark's credentials?**
   - (a) No: `credentials`, `credentialsFile`, `gcpAccessToken` and `gcpAccessTokenProvider` are
     reserved. Spark takes its credentials from its environment (Application Default Credentials,
     or the data source's settings in the cluster's or the session's Spark configuration), and
     the client from the environment or from the job's `client`. Credentials never travel with
     the options, which ADR 0013 keeps apart from them.
   - (b) They pass through like any option, so a job can give Spark a key per connector. A job
     that does so must give the client the same identity through `client`.

   **Recommended: (a).** Either way, the connectors take no credentials as arguments of their
   own.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions: `BigQueryReader` passes the watermark filter to the Storage Read API through Spark's `filter` option and freezes the slice with an upper bound captured as `FORMAT('%T', MAX(col))` through google-cloud-bigquery; `BigQueryWriter.write(data, *, replace=False)` is atomic for both operations: Spark loads a staging table in the target's dataset (direct method by default, no bucket), and one `INSERT ... SELECT` (append) or `MERGE ... ON FALSE` (replace) applies it; the target must exist. Defines the `ingestio[bigquery]` extra. 38 acceptance criteria, 9 integration scenarios in two opt-in tiers, 25 decisions (5 by the maintainer, 20 proposed), 9 open questions. |
| 2026-10-23 | approval | approved | Approved by the main session under the maintainer's delegation of 2026-10-23 ("Delegar e ir até o fim"), with the recommended option of all 9 questions: `billing_project` and `client`, three-part names only, `DATETIME` columns take a `str`, no snapshot pinning, a staging table for both operations built from the target's metadata and expiring after one day, credential options reserved. Flagged for the maintainer's review: Q8, where a failed drop of the staging table after the apply has committed is not raised (the only error a connector does not propagate; raising would make a committed append look failed, and a retry would duplicate it), and the claims the planner could not verify (typed and `%T` literals in the row restriction, the connector bundled with Databricks Runtime), which only the blind integration tests check. The main session added `bigquery = ["google-cloud-bigquery>=3.20"]` and the dev package `google-cloud-bigquery>=3.20` to `pyproject.toml` and refreshed `uv.lock` (3.46.2 locked). The package ships `py.typed`; under mypy strict only `Client.close()` is untyped, which needs a local `# type: ignore[no-untyped-call]`, not an override. Later stages run last, in the order the maintainer chose. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_bigquery_connectors.py`: 52 tests (141 cases) covering AT-001 to AT-038 with a fake Spark session and DataFrames, a fake BigQuery client and job, and a fake `google.cloud.bigquery.Client` factory patched by string, all on one ordered event log, with real `Table`, `SchemaField` and `PolicyTagList` objects built offline; and 9 integration tests (18 cases) in two opt-in tiers, written blind: `INGESTIO_BIGQUERY_DATASET` (a real client with fake Spark objects), plus `INGESTIO_SPARK_TESTS=1` and `INGESTIO_BIGQUERY_SPARK_PACKAGE` (the spark-bigquery-connector coordinate, loaded through `spark.jars.packages`) for the Spark tier. No existing test changed. Checked by the main session: collection fails as expected with `ImportError: cannot import name 'BigQueryReader' from 'ingestio.connector.all'`; the rest of the suite passes (870 passed, 61 skipped), the other AT-004 tests included; ruff and format clean. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `BigQueryReader` and `BigQueryWriter` in a new section of `docs/concepts/connectors.md`, right after the Unity Catalog one and in its shape: the staging table, the `INSERT` / `MERGE ... ON FALSE` apply, the failure table, the Q8 case as the one error these connectors do not raise and why, costs, environments, limitations marked unverified, and the two opt-in integration tiers; amended ADR 0002 (the `bigquery` extra), ADR 0012 (leans towards batch), ADR 0013 (the writer's `table_name`, the `filter` row restriction, `client` and `billing_project`) and ADR 0014 (its rule is a permission, not a requirement; `BigQueryWriter` creates nothing, its staging table is its own mechanism); updated `docs/index.md` and the architecture KB. Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable BigQuery blocks, with the expected `ImportError` of the missing classes (20 passed). |
| 2026-10-23 | implementation | implemented | The implementer added `BigQueryReader` and `BigQueryWriter` in `src/ingestio/connector/bigquery.py`, mirroring `postgres.py` and `unity_catalog.py`: the reader captures the upper bound with one `FORMAT('%T', MAX(...))` query through google-cloud-bigquery, read by position, closes a client it created, and passes the slice as the `filter` option of the Storage Read API; the writer builds the staging table from the target's schema (modes relaxed to `NULLABLE`, one-day expiry), loads it with Spark's direct method, applies it with one `INSERT` or `MERGE ... ON FALSE`, drops it, adds a note to the original exception when the cleanup after a failure fails, and does not raise a failed drop after the apply committed (Q8); exported from `connector/all.py`; pyspark and google-cloud-bigquery imported lazily. No existing connector changed. Checked by the main session: 1013 passed, 79 skipped (the opt-in integration tests), ruff, format and mypy strict clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-038 covered (141 passed, the 18 integration cases skipped without credentials or Java), the full suite with the 22 documentation examples 1013 passed and 79 skipped, ruff, format, mypy strict and `uv lock --check` clean; code, documentation and the ADR 0002, 0012, 0013 and 0014 amendments agree. Q8 is stated the same way in the spec, the docs, the code and AT-033, and breaks no rule of the philosophy as written (the criticality rules apply to stages, and the write committed), though it leaves no trace until a run result exists to record it: kept flagged for the maintainer. Non-blocking: the docs said "`BIGNUMERIC` holds every finite `Decimal`", which overstates the spec; the main session corrected the sentence in `docs/concepts/connectors.md` to the spec's wording (and that a `Decimal` beyond its range fails the bound query). Left for the maintainer: a failing `close()` of a client the connector created would replace an exception already propagating (the original survives as `__context__`), a case the spec does not define. The integration tests stay unverified. |
| 2026-10-23 | amendment | validated | AT-037 superseded by [specs/config_connectors.md](config_connectors.md), decision 17, approved by the maintainer on 2026-10-23: `ReadSource` and `WriteTarget` no longer take a connector, so the composition this criterion describes is covered by that spec's criteria. The criterion's test goes in the tests stage of that spec; this spec keeps its status. |
