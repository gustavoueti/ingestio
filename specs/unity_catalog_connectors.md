# Spec: Unity Catalog reader and writer connectors

- **Status:** validated
- **Slug:** `unity_catalog_connectors`. The spec is `specs/unity_catalog_connectors.md` and its
  tests are `tests/test_unity_catalog_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/disk_connectors.md](disk_connectors.md),
  [backlog idea 5](../docs/backlog.md#5-databricks-unity-catalog-reader-and-writer),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for Delta tables registered in a Spark catalog, such as Databricks Unity Catalog,
named by their three-part name `catalog.schema.table`. `UnityCatalogReader` returns a Spark
DataFrame over a table: all of it, or, with a watermark, only the rows above the job's last
watermark. The filter is part of the table's scan, so Delta skips the files that cannot hold a
matching row, and `read()` freezes the slice with an upper bound it captures, as the PostgreSQL
reader does. `UnityCatalogWriter` receives a Spark DataFrame and appends it to a table, or
replaces the table's rows with it, each in one Delta commit, and creates the table when it does
not exist yet.

They follow the PostgreSQL and disk connectors: an ingestion's parameters are constructor
arguments ([ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)), pyspark is
a prerequisite imported lazily, the session is looked up when the reader reads, and the writer
has one method, `write(data, *, replace=False)`. They add no dependency and no extra.

Choices that depend on an open question name it, as **(Q n)**. The spec is written with the
recommended option of every question.

## Motivation

The maintainer wants Python jobs, typically on a Databricks cluster, to read from and write to
tables in Unity Catalog:

- **Read.** Jobs ingest from catalog tables, for example from a bronze table into a silver one,
  incrementally by a watermark column such as `updated_at`. ADR 0013 requires a reader to
  extract only what the job needs, filtering inside the source. For a Delta table, a predicate
  on the table's scan is filtering inside the source: Delta compares it with the minimum and
  maximum it records for each file and skips the files that cannot match, and the Parquet reader
  skips row groups in the files that remain.
- **Laziness.** A DataFrame over a Delta table reads the table's latest version at every action.
  A job that counts the rows, writes them and then computes its next watermark from the same
  DataFrame can see different rows each time, and record a watermark above rows it never wrote.
  The PostgreSQL reader solved the same problem with an upper bound captured by `read()` (its
  decision 10), and the maintainer chose the same solution here (decision 3).
- **Write.** Jobs write their results into catalog tables. A Delta commit makes an append or an
  overwrite atomic, so the connector needs none of the staging that `PostgresWriter` uses.
- **Creation.** A new job writes into a table that does not exist yet. The maintainer wants the
  first write to create it (decision 4), so that a plain table needs no separate DDL step.
- **What exists does not fit.** `DiskWriter(path, "delta")` writes a Delta table at a path. On
  Databricks that path is in a volume, which holds files, not catalog tables: Unity Catalog
  governs tables by their name, with their privileges, lineage and managed storage. `DiskReader`
  rejects `delta`, because a Delta table cannot be read file by file. `PostgresReader` and
  `PostgresWriter` speak JDBC to PostgreSQL.

## Fit with the philosophy

- **Concept: connector.** `UnityCatalogReader` satisfies `Reader[DataFrame]` and
  `UnityCatalogWriter` satisfies `Writer[DataFrame]`, structurally, with no base class and no
  registration ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). The external
  system is a catalog and the Delta tables it holds. `write(data, *, replace=False)` adds a
  keyword-only argument with a default, so `write(data)` stays a valid call and the method still
  matches the protocol. `ReadSource` and `WriteTarget` use both connectors unchanged.
- **Format: the system's, not a choice.** The philosophy says a connector "does not decide write
  strategy, format or architecture". The writer always writes Delta, and the job gives no
  format. This is not the connector deciding a format for the job's data:
  - Delta is the storage protocol of the system the connector integrates, as PostgreSQL's own
    storage is for `PostgresWriter`. It is what makes a write atomic, which is the reason for
    decision 4.
  - The connector converts nothing. The DataFrame's columns and types become the table's
    columns and types as they are; Delta has no type mapping of its own to apply.
  - The connector never converts an existing table to Delta on purpose. A table in another
    format is not a target it supports, and what Spark does with one is documented under
    [Known limitations](#known-limitations) **(Q8)**.
- **Write strategy.** Append and replace are the two primitive ways Delta puts a batch into a
  table: the `append` and `overwrite` save modes. The writer offers both through one method and
  picks neither; the calling callback chooses at call time, and the connector's configuration
  has no mode. Options that would turn a replace into a partial replace, a strategy of their own,
  are reserved, and the writer makes a replace replace every row whatever the session's settings
  **(Q4)**. Merges, upserts and SCD2 belong to callbacks. Changing an existing table's schema
  happens only when the job passes Delta's own options for it **(Q3)**.
- **Architecture: creating a missing table.** The maintainer chose that the writer creates its
  target when it does not exist (decision 4), against the precedent of `PostgresWriter`, whose
  decision 29 calls DDL on the target "architecture, which the philosophy keeps out of
  connectors". This spec checks the choice against the philosophy as follows:
  - **What the connector creates is the minimal form, and nothing in it is the connector's
    choice:** a managed Delta table whose columns are the DataFrame's, with no partitioning, no
    clustering, no location, no comment and no table property set by the connector. Everything
    else the table gets comes from the catalog's defaults: the schema's managed storage, and the
    default properties of the catalog, the schema or the workspace.
  - **Where the table is, and that it exists, are the job's decisions.** The job names the table
    and calls `write`; the connector never creates a catalog or a schema.
  - **Why this differs from PostgreSQL.** Creating a PostgreSQL table from a DataFrame means
    choosing: `text` or `varchar`, the precision of `numeric`, `timestamptz` or `timestamp`,
    keys, constraints, defaults. Those choices are architecture. A Delta table created from a
    DataFrame needs none: its schema is the DataFrame's schema. Decision 29 of the PostgreSQL
    spec therefore stands for PostgreSQL.
  - **Conclusion.** The philosophy's sentence does not need to change, and the PostgreSQL
    spec's decision 29 still holds. Its reason, "DDL on the target is architecture", is broader
    than the line this spec draws, though: a connector may create a missing target only when the
    system derives it entirely from the data, with no choice made by the connector. Because this
    reading binds every later connector (a BigQuery writer, for instance, can create a table if
    needed), this spec recommends that a new ADR records it **(Q6)**.
- **Columns by name, never by position.** The writer never calls `insertInto`, which resolves
  columns by position. `saveAsTable` matches the DataFrame's columns with the table's by name
  (decision 4).
- **The reader decides no strategy.** Whether a run is incremental, and from which value, is the
  job's decision. The reader turns the column and the value it receives into the predicate that
  the scan applies.
- **Essential by default.** The connectors catch nothing to carry on: every failure propagates
  out of `read()` or `write()` and fails the callback that called them.
- **Simplicity.** Two small classes in one module. No base class, no registry, no staging, no
  existence check, no new protocol and no new callback. A write is one call chain; a watermark
  read is one aggregate and one filter.
- **Compose, do not impose.** The job builds the connectors from its own configuration. Nothing
  reads files, environment variables or `dbutils`. A user can still write their own table
  connector in their own code.
- **No dependencies in the core.** No new package, so no extra (decision 5). pyspark and Delta
  Lake are prerequisites that the runtime provides, as for the disk connectors. `ingestio` and
  `ingestio.connector.all` still import without pyspark.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `UnityCatalogReader`: full reads, and watermark reads whose filter is part of the table's
  scan, over a slice that `read()` freezes with an upper bound;
- `UnityCatalogWriter`: append (`write(data)`) and replace (`write(data, replace=True)`), each
  one Delta commit, creating the table in its minimal form when it does not exist;
- the rules for table names, column names, watermark values and options, and the reserved
  options;
- the documentation of what a read and a write guarantee and do not guarantee;
- the test doubles, and integration tests against a local Spark session with Delta Lake.

Out of scope:

- the job configuration file and recipes: reading or parsing JSON, `from_config`, and mapping
  configuration keys to arguments;
- computing, persisting or advancing the watermark, and exposing the bound that `read()`
  captured;
- Delta's change data feed (`readChangeFeed`), which returns updates and deletes: a later spec
  (decision 3);
- freezing a read by pinning the table's Delta version. A job can pin one itself through Delta's
  `versionAsOf` option **(Q5)**;
- merge, upsert, delete, SCD2, and partial replaces (`replaceWhere`, dynamic partition
  overwrite);
- any DDL other than the creation of a missing table in its minimal form: creating catalogs or
  schemas; altering, dropping or renaming tables; partitioning, clustering, locations, comments,
  table properties, constraints; tables in a format other than Delta;
- `insertInto`, `DataFrameWriterV2` (`writeTo`), and SQL `INSERT` or `MERGE` statements;
- SQL warehouses, databricks-sql-connector and the Databricks SDK (decision 1);
- one-part and two-part table names **(Q2)**;
- nested fields as the watermark column;
- files in Unity Catalog volumes (backlog idea 6) and Delta tables by path (`DiskWriter`);
- streaming (`readStream`, `writeStream`), Auto Loader, and Lakeflow declarative pipelines;
- Iceberg tables, and Delta Sharing;
- retries, row counts and log lines;
- changes to CI, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/unity_catalog.py` **(Q1)**, exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter
```

```python
class UnityCatalogReader:
    """Reads a catalog table into a Spark DataFrame, filtering in the table's scan.

    Args:
        table_name: Source table, `catalog.schema.table`. The configuration key of the same
            name.
        watermark_column: Column that orders changes. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only rows above it are read.
            Requires `watermark_column`.
        options: Spark read options passed through, such as Delta's `versionAsOf`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        table_name: str,
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
class UnityCatalogWriter:
    """Writes a Spark DataFrame into a catalog table, in one Delta commit.

    Args:
        table_name: Table to write into, `catalog.schema.table`. Created, as a managed Delta
            table with the DataFrame's columns, when it does not exist.
        options: Delta write options passed through, such as `mergeSchema` or
            `userMetadata`.
    """

    def __init__(
        self,
        table_name: str,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` to the table, in one commit.

        Args:
            data: The rows to write.
            replace: Remove every row of the table in the same commit, so that the table ends
                up holding exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame` and `SparkSession` are `pyspark.sql.DataFrame` and `pyspark.sql.SparkSession`.
  `Decimal` is `decimal.Decimal`; `date` is `datetime.date`, and `datetime.datetime`, its
  subclass, is accepted too. `Mapping` is `collections.abc.Mapping`.
- `table_name` is required, so it is positional; every option is keyword-only. Neither
  connector takes a connection object: the session reaches the catalog.
- `UnityCatalogWriter` has a single write method, `write`, and no `replace()` method. `replace`
  is keyword-only and defaults to `False`, so `UnityCatalogWriter.write` satisfies
  `Writer[DataFrame]` structurally. `WriteTarget`, which calls `write(data)`, always appends.
- No new extra.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL and disk connectors, and everything exported by `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration, keeps its
own watermark, and advances it from what it read.

```python
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

## Behaviour

### Building the connectors

Building a connector validates its arguments and does nothing else. It imports no third-party
package, touches no Spark session and no catalog, and does not check that the table exists.

`repr` shows the table name as given:

- `UnityCatalogReader(table_name='main.sales.orders')`
- `UnityCatalogWriter(table_name='main.lake.orders')`

### Table and column names

- **Form (Q2).** `table_name`, on both connectors, is `catalog.schema.table`. The connector
  splits it on `.`. Anything other than three non-empty parts raises
  `ValueError("table_name must be 'catalog.schema.table'")` when the connector is built. The
  parts are plain names, never quoted by the job: a part cannot contain a `.`.
- **Quoting.** The connectors always quote every part of the table name and the watermark
  column, as Spark SQL identifiers: each one is wrapped in backticks, and each backtick inside is
  doubled. So `main.sales.orders` becomes `` `main`.`sales`.`orders` ``, and a catalog named
  `my-cat` becomes `` `my-cat` ``. No name is ever passed to Spark unquoted.
- **Case.** Quoting protects special characters and reserved words; it does not make a name
  case-sensitive. Whether case matters is the catalog's rule: Unity Catalog and the Hive
  metastore store table names in lowercase, so `Main.Sales.Orders` and `main.sales.orders` are
  the same table. Columns resolve by Spark's rules, case-insensitively unless the session sets
  `spark.sql.caseSensitive`. The connectors change neither rule.
- **Watermark column.** `watermark_column` is one top-level column. A name that contains a `.`
  is a column with a dot in its name, not a nested field.

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()` at each `read()`, never when the reader is built. The
   reader never creates a session. When none is active, PySpark's error propagates.
2. **Source.** The reader builds one DataFrame over the table:
   `spark.read.options(**options).table(<quoted table>)`, with the user's `options` as given and
   no other option. This DataFrame is the *source*.
3. **Full read** (no `watermark_column`): `read()` returns the source unchanged.
4. **Watermark read** (`watermark_column` given): the bound and the slice, described below.

The reader adds no column, cast, projection or Spark setting. A full read runs no Spark action;
a watermark read runs exactly one, the bound query.

#### Watermark read

`<col>` is the quoted watermark column, and `<value>` the literal of `watermark_value`, built as
described in [Watermark values](#watermark-values). Every expression uses single spaces, exactly
as shown.

1. **Lower predicate.** When a value is given, `<lower>` is `<col> > <value>`.
2. **Bound.** The reader takes the source, filtered with `where(<lower>)` when a value is given,
   and calls `selectExpr` on it with exactly two expressions, in this order:

   ```text
   CAST(max(<col>) AS STRING) AS upper_bound
   typeof(max(<col>)) AS upper_bound_type
   ```

   It calls `first()` on the result once, and takes the two fields of the row by position: the
   bound's text, as Spark renders it, and its type, as Spark names it. The bound is the highest
   watermark of the slice, or `None` when no row qualifies.
3. **Slice.** The reader calls `where(<predicate>)` on the source, where `<predicate>` is:

   | Value given | Bound | `<predicate>` |
   |---|---|---|
   | yes | not `None` | `<col> > <value> AND <col> <= CAST(<bound> AS <type>)` |
   | no | not `None` | `<col> <= CAST(<bound> AS <type>)` |
   | either | `None` | `false` |

   `<bound>` is the bound's text as a string literal, built as for a `str` value. `<type>` is
   the type's name exactly as Spark returned it, such as `timestamp`, `timestamp_ntz`, `date`,
   `bigint` or `decimal(10,2)`.
4. `read()` returns the slice, the DataFrame that this `where` returned, unchanged.

What this gives:

- **The filter is part of the table's scan.** After Spark folds the literals, the predicate
  compares the column itself with constants, so Delta uses it to skip files and the Parquet
  reader to skip row groups. The bound query reads only the files that the lower predicate
  cannot skip.
- **The slice is frozen.** Every action on the DataFrame reads the same range of the watermark.
  Rows written after `read()` with a watermark above the bound are left for the next run, so a
  watermark that the job computes from the DataFrame never goes past what the DataFrame holds.
- **The bound round-trips exactly** for integer, decimal, string, date and `TIMESTAMP_NTZ`
  columns: Spark renders the highest value as text and casts that text back to the column's own
  type, so the bound never passes through Python types. For a `TIMESTAMP` column, the text is
  the session time zone's wall clock; see [Known limitations](#known-limitations).
- **Rows whose watermark is `NULL` are never extracted** when `watermark_column` is given,
  first run included.
- **An empty slice** (an empty table, a table whose watermarks are all `NULL`, or no row above
  the value) is the source filtered with `false`: a DataFrame with the table's columns and no
  rows.

#### Watermark values

`watermark_value` is "given" when it is not `None`. `0` and `""` are values. A value is first
turned into text, as in the PostgreSQL reader, and then into a literal:

| Type | Text | Literal |
|---|---|---|
| `int`, but not `bool` | `str(value)` | the text, unquoted: `41`, `-7` |
| `decimal.Decimal`, finite | `format(value, "f")` | the text followed by `BD`: `12.50BD` |
| `str` | the string itself | a string literal |
| `datetime.datetime`, timezone-aware | `value.astimezone(UTC).isoformat(sep=" ")` | a string literal |
| `datetime.date` | `value.isoformat()` | a string literal |

A **string literal** is the text wrapped in single quotes, with each backslash and each single
quote inside preceded by a backslash: `O'Brien` becomes `'O\'Brien'`, and `C:\temp` becomes
`'C:\\temp'`. This assumes `spark.sql.parser.escapedStringLiterals` is `false`, Spark's default,
as the PostgreSQL reader assumes `standard_conforming_strings`.

Why numbers are unquoted and the rest is text:

- **Numbers.** Spark compares a string with a `DECIMAL` column as `DOUBLE`, which loses
  precision. An integer literal, or a `BD` decimal literal, is compared as a number with any
  numeric column, exactly.
- **Dates and datetimes.** Spark casts a string literal compared with a `DATE`, `TIMESTAMP` or
  `TIMESTAMP_NTZ` column to the column's own type. A typed literal would not do that: a
  `TIMESTAMP` literal compared with a `TIMESTAMP_NTZ` column makes Spark convert the column
  instead, row by row, which also keeps Delta from skipping files.
- **Datetimes and the column's type.** An aware datetime is converted to UTC first, so its text
  always ends in `+00:00`:
  - for a `TIMESTAMP` column, the literal is the same instant as the value, whatever the offset
    it carried and whatever the session's time zone;
  - for a `TIMESTAMP_NTZ` column, Spark ignores the offset when it casts the text, so the column
    is compared on the UTC wall clock: 08:30 at `-03:00` is compared as `2026-10-01 11:30:00`.
    This is what the PostgreSQL reader does with `timestamp without time zone` columns. A
    `TIMESTAMP_NTZ` column that holds wall-clock time in another zone needs its value as a
    `str`, which is sent as it is.
- **Other combinations** follow Spark's type coercion. A value of the column's own type keeps
  the comparison exact and lets Delta skip files.

The reader rejects, when it is built:

| Case | `ValueError` message |
|---|---|
| `watermark_value` given without `watermark_column` | `watermark_value needs watermark_column` |
| `watermark_column == ""` | `watermark_column must not be empty` |
| `bool`, `float`, or a type not in the table above | `unsupported watermark_value type: <type name>` |
| a `Decimal` that is NaN or infinite | `watermark_value must be finite` |
| a naive `datetime` | `watermark_value must be timezone-aware` |

`<type name>` is `type(value).__name__`. A datetime is naive when its `tzinfo` is `None` or its
`utcoffset()` is `None`.

#### Errors of a read

Nothing is caught. Errors of PySpark, Spark, Delta and the catalog propagate out of `read()`
unchanged, as the same object. When an error surfaces depends on the session: classic Spark
analyses a DataFrame when it is built, so a missing table, a missing column or a missing
privilege fails `read()`; Spark Connect (Databricks Connect, serverless compute) analyses it
later, at the bound query or at the job's first action. The connectors write no log lines.

### Options

- **Reader (Q5).** `options` are Spark read options for the table, passed through as given, such
  as Delta's `versionAsOf` or `timestampAsOf`. The reader sets no option of its own.
- **Writer (Q3, Q4).** `options` are Delta write options, passed through as given, such as
  `mergeSchema`, `overwriteSchema`, `userMetadata`, `txnAppId` and `txnVersion`. The writer sets
  one option of its own: `partitionOverwriteMode` = `static`, on `write(data, replace=True)`
  only.
- Values reach Spark as given. Spark turns them into strings, and booleans into `true` and
  `false`.
- Some keys are reserved. Giving one, in any case, raises
  `ValueError("option '<key>' is set by the connector")` when the connector is built, with
  `<key>` as the user wrote it. This is the message of the PostgreSQL and disk connectors: the
  connector owns what these keys control.

  | Connector | Reserved keys | Why |
  |---|---|---|
  | both | `path` | The connector names the table by its three-part name. A path names storage instead, and on the writer it would make the created table an external one at that location. |
  | `UnityCatalogReader` | `readChangeFeed`, `readChangeData` | They make Delta return the table's changes instead of its rows. The change data feed is a later spec (decision 3). |
  | `UnityCatalogWriter` | `replaceWhere`, `partitionOverwriteMode` | They turn `replace=True` into a partial replace, a write strategy that belongs to callbacks. The writer sets `partitionOverwriteMode` itself (Q4). |

### Writing

`write(data, *, replace=False)` does two things, in order:

1. **Columns.** When `data.columns` is empty, it raises
   `ValueError("the DataFrame has no columns")` and does nothing else.
2. **Save.** It makes exactly one call chain:

   ```text
   data.write.format("delta").options(<options>).mode(<mode>).saveAsTable(<quoted table>)
   ```

   - `<mode>` is `"append"` when `replace` is `False` and `"overwrite"` when it is `True`;
   - `<options>` are the user's, as given, plus `partitionOverwriteMode` = `static` when
     `replace` is `True` **(Q4)**;
   - `<quoted table>` is the quoted three-part name.

**Operations.**

- `write(data)` appends: it adds every row of `data` to the table and leaves the existing rows
  as they are. `write` is the method of the `Writer` protocol, and `WriteTarget` calls
  `write(data)`, so `WriteTarget` appends, as it does with `PostgresWriter` and `DiskWriter`.
- `write(data, replace=True)` replaces: the table ends up holding exactly the rows of `data`.
  `partitionOverwriteMode` = `static` makes this hold on a partitioned table too, even when the
  session sets `spark.sql.sources.partitionOverwriteMode` to `dynamic`, which would otherwise
  replace only the partitions present in `data`.
- The calling callback chooses at call time, by passing `replace`. The connector's
  configuration has no mode, and there is no `replace()` method.

#### Creating a missing table

When the table does not exist, the save creates it, with the rows of `data`, in either mode.
This is what `saveAsTable` does with the `append` and `overwrite` modes; the writer makes no
existence check and no catalog lookup of its own.

The table it creates has this minimal form:

- a managed table, in the managed storage of its schema or catalog (in open-source Spark, the
  session catalog's warehouse directory);
- in Delta format;
- whose columns are the DataFrame's columns, with their names, types and order;
- with no partitioning, no clustering, no location, no comment and no table property set by the
  connector. Whatever the catalog, the schema or the workspace adds by default, such as deletion
  vectors or predictive optimization on Databricks, is theirs.

A job that needs anything else, such as partitioning, liquid clustering, constraints, a
location or properties, creates the table itself, with DDL, before the first write. The writer
then writes into it like into any existing table. The writer never creates a catalog or a
schema: a missing one fails the save.

The table appears with its first commit, which holds the rows of `data`, so readers never see
it empty or partly written. An empty DataFrame creates the table with its columns and no rows.

#### Columns and schema

- **By name.** Columns are matched by name, never by position: the writer never calls
  `insertInto`. The order of the DataFrame's columns does not matter.
- **Delta's schema enforcement applies**, with Spark's case rules:
  - a column that the DataFrame lacks is written as `NULL`, or with the column's generated,
    identity or default value. A `NOT NULL` or `CHECK` constraint that a row violates fails the
    whole write;
  - a column that the table lacks fails the write, unless the job passes `mergeSchema` (Q3);
  - a type that Delta cannot store in the column fails the write;
  - a replace keeps the table's schema and partitioning, unless the job passes `overwriteSchema`
    (Q3).
- The writer converts nothing. Values go into the table as Spark has them.

#### Atomicity

The writer adds no mechanism of its own: no staging, no cleanup and no retry. A write
guarantees what a Delta commit guarantees:

- An append, an overwrite and the creation of a missing table are each one commit in the table's
  transaction log. Readers see the table before the commit or after it, never in between. A
  write that fails leaves the table as it was, or, when it was missing, leaves no table in the
  catalog.
- An overwrite is a new version of the table. The previous version stays readable through
  Delta's time travel until `VACUUM` removes its files.
- Files written by Spark tasks that failed or were retried are never part of a commit, so a
  task retry does not duplicate rows, unlike Spark's JDBC writer.
- **Concurrent writes** follow Delta's optimistic concurrency. Two appends to the same table do
  not conflict. A write that conflicts with another commit fails with Delta's error and writes
  nothing; the writer does not retry.
- **Two first writes at once.** When two writers find the table missing at the same time, one
  creates it. The other either appends after it or fails with Spark's error that the table
  already exists, depending on Spark, Delta and the catalog. Either way each write is all or
  nothing.

#### What the writer does not do

- It reads only `data.columns` and `data.write` from the DataFrame. It never counts, caches or
  inspects the rows, and the save evaluates the DataFrame once.
- It calls no other method of Spark's writer: no `insertInto`, `save`, `partitionBy`,
  `bucketBy`, `sortBy` or `clusterBy`.
- It takes no session: it uses `data.write`, as `PostgresWriter` and `DiskWriter` do.
- It does not check what the name points to **(Q8)**. A target that is not a Delta table, such
  as a view, a materialized view, a streaming table or a table in another format, is left to
  Spark, Delta and the catalog: see [Known limitations](#known-limitations).
- An empty DataFrame goes through the same chain: an append adds nothing, `replace=True` leaves
  the table empty, and a missing table is created with no rows.
- `write` returns `None`. Every error propagates unchanged, as the same object. The writer
  writes no log lines.

### Where it lives

- `src/ingestio/connector/unity_catalog.py` **(Q1)**: `UnityCatalogReader` and
  `UnityCatalogWriter`, with a module docstring and a sorted `__all__`.
- `src/ingestio/connector/all.py`: exports the two classes.
- No change to `pyproject.toml` **(Q7)**, `ingestio/basics.py`, `ingestio/__init__.py`,
  `callback/` or `connector/core.py`, and no change to the public API of the other connector
  modules.

### Supported environments

- pyspark 3.5 or later is a prerequisite that the runtime provides, as for the other Spark
  connectors. Delta Lake is a prerequisite too: built into Databricks Runtime, and on
  open-source Spark the Delta Lake JARs (`io.delta:delta-spark`) in the session. ingestio
  installs neither.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated or
  standard access mode, with Unity Catalog tables. Only the DataFrame API is used, never
  `sparkContext` or `_jvm`, which standard access mode does not offer.
- **Databricks Connect**, version 15.4 or later, against such a cluster. The session is passed
  as `spark=`, or looked up with `SparkSession.active()` where PySpark returns the Databricks
  Connect session. Not tested here.
- **Open-source Spark 3.5 or later**, on a Java version that release supports (17 or 21 for
  Spark 4), with Delta Lake configured: `spark.sql.extensions` set to
  `io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog` set to
  `org.apache.spark.sql.delta.catalog.DeltaCatalog`. Tables are then named
  `spark_catalog.<schema>.<table>`. Other catalog plugins that resolve three-part names to
  Delta tables, such as the open-source Unity Catalog connector, are not tested.
- **Databricks serverless compute:** nothing in the design excludes it, because it uses only
  DataFrame API calls that Spark Connect supports, but it is not tested.
- **Not SQL warehouses** (decision 1).
- **Privileges in Unity Catalog.** The reader needs `USE CATALOG`, `USE SCHEMA` and `SELECT` on
  the table. The writer needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY` and `SELECT` on an
  existing table, or `CREATE TABLE` on the schema when the table does not exist. Row filters and
  column masks apply to the reader as to any query.

### Known limitations

These are documented, not solved here:

- **Late commits.** A row committed after a read, with a watermark at or below the bound that
  read captured, is never extracted by a later run. This happens with long-running writers that
  commit rows whose watermark predates another writer's commit, and with clock skew between
  writers. It is the classic limit of timestamp watermarks. Rows that share the exact value of the previous
  watermark but commit later are missed the same way.
- **Full reads are not frozen.** Every action on a full read's DataFrame reads the table's
  latest version. A job that needs a fixed version passes `versionAsOf` in `options` (Q5).
- **The slice is frozen by watermark, not by version.** A row of the slice that is deleted, or
  updated without moving its watermark, after `read()`, shows its new state in later actions.
- **`TIMESTAMP` bounds and the session time zone.** Spark renders a `TIMESTAMP` bound as the
  session time zone's wall clock and casts it back in the same zone. The session time zone must
  not change between `read()` and the actions on the DataFrame: Spark Connect analyses the
  DataFrame again at each action. In a zone with daylight saving time, a bound inside the hour
  that repeats when clocks go back can come back up to one hour early. Rows above it are then
  left for the next run, never lost. The default session time zone of Databricks is UTC, which
  has no daylight saving time.
- **Skipping files needs statistics.** Delta records statistics only for the first 32 columns
  by default (`delta.dataSkippingNumIndexedCols`). A watermark column outside them, or a value
  whose type makes Spark convert the column, is still filtered, but every file is read.
- **The bound query** reads the watermark column of every file that the lower predicate cannot
  skip, once per `read()`.
- **Watermark values** must have the column's type for an exact comparison; see
  [Watermark values](#watermark-values). Naive datetimes are rejected: a job that takes its
  watermark from `collect()` gets a naive datetime and must attach its zone, or pass a `str`
  (see the [usage sketch](#usage-sketch)).
- **Overwrites keep the previous version** in storage until `VACUUM`, and it stays readable
  through time travel until then.
- **Schema changes** to an existing table happen only through Delta's options (Q3). Without
  them, a DataFrame with an extra column, or a replace with another schema, fails.
- **The created table is minimal.** It has no partitioning, clustering or properties; a job
  that needs them creates the table first.
- **A target that is not a Delta table** is not supported, and the writer does not detect one
  (Q8). What happens is Spark's: an append to a table in another format fails because the
  formats differ, and a write to a view, a materialized view or a streaming table is refused on
  Databricks. A replace of a table in another format can fail, or, in open-source Spark's
  session catalog, drop that table and create a Delta table in its place. Never point a writer
  at anything but a Delta table, or a name that does not exist yet.
- **Type conversion** is whatever Spark and Delta do. The connectors convert nothing.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles and without a JVM.
The doubles live in `tests/test_unity_catalog_connectors.py` and share one ordered event log.

- **Fake Spark session.** Each access to `spark.read` returns a new fake reader, as PySpark
  does. It supports `format(source)`, `option(key, value)`, `options(**options)` and
  `table(tableName)`. Each `table()` records a *table read*, with the name, the very object
  passed, and the merged options: a later call replaces an earlier key with the same name,
  compared case-insensitively as Spark does, and the original case of every key is kept. Values
  are compared as Spark sees them: `str(value)`, and booleans as `true` and `false`.
  `table()` returns a new fake DataFrame, the *source*. The fake records every attribute
  accessed on the session.
- **Fake DataFrame.** One class serves the reader and the writer. It records every attribute
  accessed on it, and every event records which DataFrame it was called on.
  - `where(condition)` and `filter(condition)` record a *where* with the condition, the very
    object passed, and return a new fake DataFrame. `selectExpr(*expr)` records a *select* with
    the expressions and returns a new fake DataFrame. On a DataFrame returned by `selectExpr`,
    `first()` records a *row access* and answers with the single row the test chose, a
    two-element tuple such as `("2026-10-21 09:30:00.5", "timestamp")` or
    `(None, "timestamp")`; `head()`, `collect()` and `take(n)` record a row access too.
  - `columns` is what the test sets. `write` returns a fake writer, which records every
    attribute accessed on it, supports `format`, `option`, `options` and `mode`, and records a
    *table save*, with the format, the mode, the merged options and the name, when
    `saveAsTable(name, format=None, mode=None, partitionBy=None, **options)` is called. It also
    offers `insertInto`, `save`, `partitionBy`, `bucketBy`, `sortBy` and `clusterBy`, so that a
    test can see they were not called.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. To simulate a
  missing pyspark, map `pyspark` to `None` in `sys.modules`, in a subprocess.
- Tests use the public API. They pass the fakes as `spark=` and as DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against a local Spark session with Delta Lake. They are
marked `@pytest.mark.integration` and skipped, with a reason that names what is missing, unless
the environment variable `INGESTIO_SPARK_TESTS` is `1`, as for the disk connectors, and
`INGESTIO_DELTA_PACKAGE` holds the Maven coordinate of the Delta Lake release that matches the
installed pyspark, such as `io.delta:delta-spark_2.13:<version>` **(Q7)**. They need Java 17 or
21, because `uv.lock` holds pyspark 4.2, and access to Maven or a filled Ivy cache.

The session is `local[2]`, with `spark.jars.packages` set to `INGESTIO_DELTA_PACKAGE`, the two
Delta settings of [Supported environments](#supported-environments),
`spark.sql.session.timeZone=UTC`, and `spark.sql.warehouse.dir` under `tmp_path`. Each test uses
its own uniquely named tables, `spark_catalog.default.<name>`, and drops them. They are written
without being run here: this machine has no Java 17 or 21 and no Databricks workspace. They
cover:

1. **Creation.** `write(df)` to a missing table creates it. `DESCRIBE DETAIL` shows the format
   `delta` and no partition columns, `DESCRIBE TABLE EXTENDED` shows the type `MANAGED`, and the
   table holds the rows of `df`, with its columns. A second `write` with the same columns in
   another order appends by name: every value lands in its own column. `write(empty_df)` to
   another missing table creates it with the columns of `empty_df` and no rows.
2. **Replace.** `write(df, replace=True)` leaves exactly the rows of `df`. The table's history
   has one more version, and the previous one is readable with `versionAsOf`. A replace with a
   DataFrame that lacks a nullable column keeps the column, with `NULL` values.
3. **Schema enforcement.** Appending a DataFrame with a column the table lacks fails and leaves
   the table at the same version. With `options={"mergeSchema": True}`, it succeeds and the
   table gains the column. A replace with another schema fails without `overwriteSchema` and
   succeeds with it.
4. **Partitioned target.** On a table that the test creates with `PARTITIONED BY`, with
   `spark.sql.sources.partitionOverwriteMode=dynamic` in the session, `write(df, replace=True)`
   leaves exactly the rows of `df`, the rows of partitions absent from `df` included.
5. **Failed writes.** A write whose computation fails, through a Python UDF that raises, raises
   out of `write`. An existing table stays at the same version, and a missing table is not
   created.
6. **Watermark read on a `TIMESTAMP` column.** Rows at several instants, one with microseconds,
   and one `NULL`. A read with an aware datetime value returns exactly the rows above it, the
   row that holds the highest watermark included. A row inserted after `read()` with a higher
   watermark is absent from a second `count()` on the same DataFrame. The `NULL` row is never
   returned. A first read without a value returns every row but the `NULL` one.
7. **Other watermark types.** The same, more briefly, with a `BIGINT` column and an `int` value,
   a `DECIMAL(38,18)` column and a `Decimal` value, a `STRING` column whose values contain a
   single quote and a backslash and a `str` value, a `DATE` column and a `date` value, and a
   `TIMESTAMP_NTZ` column with both an aware datetime value (compared on the UTC wall clock) and
   a `str` value. In each case the row that holds the highest watermark is in the slice.
8. **Views and other formats.** A watermark read of a view over a table returns the same rows
   as the table. A write to a view, with and without `replace=True`, fails, and the view still
   exists afterwards with its definition. An append to a Parquet table fails and leaves it
   unchanged. These scenarios check what [Known limitations](#known-limitations) says Spark
   does; they test no behaviour of the connector's own.
9. **Missing table.** `read()` of a missing table raises Spark's error.

## Acceptance criteria

The criteria follow the recommended option of every open question. A criterion that depends on
one names it.

Unless a criterion says otherwise:

- `spark` is the fake session, and DataFrames are the doubles of
  [Notes for testing](#notes-for-testing);
- the reader is `UnityCatalogReader("main.sales.orders", spark=spark)`, with whatever the
  criterion adds, and its quoted table is `` `main`.`sales`.`orders` ``;
- the writer is `UnityCatalogWriter("main.lake.orders")`, and its quoted table is
  `` `main`.`lake`.`orders` ``;
- `df` is a fake DataFrame with `columns == ["id", "payload"]`;
- in a watermark read, `watermark_column="updated_at"`, and the bound query answers
  `("2026-10-21 09:30:00.5", "timestamp")`;
- "exactly the options" means these keys and no others, compared case-insensitively unless the
  criterion says the case matters.

### Import surface

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import UnityCatalogReader, UnityCatalogWriter` runs, then it
  succeeds. Both names are in `ingestio.connector.all.__all__` and in
  `ingestio.connector.unity_catalog.__all__`, and neither is in `ingestio.__all__`. Depends on
  Q1.
- **AT-002**: Given an interpreter where `import pyspark` raises `ImportError`, when
  `ingestio.connector.all` is imported, then the import succeeds. A
  `UnityCatalogReader("main.sales.orders", spark=<fake session>)` and a
  `UnityCatalogWriter("main.lake.orders")` can be built. The reader's `read()` returns the
  source of the fake's table read, and `write(df)` records one table save.

### Building the connectors

- **AT-003**: Given `name` in `""`, `"orders"`, `"sales.orders"`, `"a.b.c.d"`,
  `".sales.orders"`, `"main..orders"` and `"main.sales."`, when `UnityCatalogReader(name)` or
  `UnityCatalogWriter(name)` is built, then it raises
  `ValueError("table_name must be 'catalog.schema.table'")`. `"main.sales.orders"`,
  `"spark_catalog.default.orders"` and `"My-Catalog.Sales.Order Items"` are accepted by both.
  Depends on Q2.
- **AT-004**: Given `UnityCatalogReader("main.sales.orders", watermark_value=41)`, then it
  raises `ValueError("watermark_value needs watermark_column")`. Given `watermark_column=""`,
  then it raises `ValueError("watermark_column must not be empty")`.
- **AT-005**: Given `watermark_column="updated_at"` and the `watermark_value` on the left, when
  the reader is built, then it raises `ValueError` with the message on the right:

  | `watermark_value` | Message |
  |---|---|
  | `True` | `unsupported watermark_value type: bool` |
  | `1.5` | `unsupported watermark_value type: float` |
  | `[1]` | `unsupported watermark_value type: list` |
  | `Decimal("NaN")` | `watermark_value must be finite` |
  | `Decimal("Infinity")` | `watermark_value must be finite` |
  | `datetime(2026, 10, 1)` | `watermark_value must be timezone-aware` |

  `0`, `""`, `Decimal("12.50")`, `date(2026, 10, 1)` and
  `datetime(2026, 10, 1, tzinfo=UTC)` are accepted.
- **AT-006**: Given `key` in `path`, `PATH`, `readChangeFeed` and `readchangedata`, when
  `UnityCatalogReader("main.sales.orders", options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as given.
  Given `key` in `path`, `Path`, `replaceWhere` and `PartitionOverwriteMode`, the same holds for
  `UnityCatalogWriter("main.lake.orders", options={key: "x"})`. The reader accepts
  `{"versionAsOf": 3}` and `{"timestampAsOf": "2026-10-01"}`, and the writer accepts
  `{"mergeSchema": True}`, `{"overwriteSchema": "true"}` and
  `{"userMetadata": "run 42", "txnAppId": "orders", "txnVersion": 7}`. Depends on Q3, Q4 and
  Q5.
- **AT-007**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises, when
  readers are built with and without `spark=spark`, with and without a watermark, and a writer
  is built, then each succeeds, `active` is not called, and no attribute of `spark` is accessed.
- **AT-008**: Then `repr(UnityCatalogReader("main.sales.orders"))` is
  `UnityCatalogReader(table_name='main.sales.orders')`, also when the reader is built with
  `watermark_column="updated_at"`, `watermark_value=41` and `options={"versionAsOf": 3}`, and
  `repr(UnityCatalogWriter("main.lake.orders"))` is
  `UnityCatalogWriter(table_name='main.lake.orders')`, also with
  `options={"mergeSchema": True}`.

### Full read

- **AT-009**: When `reader.read()` runs, then exactly one table read is recorded, with the name
  `` `main`.`sales`.`orders` `` and no options. `read()` returns the very DataFrame that
  `table()` returned, and no attribute of that DataFrame is accessed. The only attribute
  accessed on `spark` is `read`.
- **AT-010**: Given `options={"versionAsOf": 3, "IgnoreCorruptFiles": True}`, when the reader
  reads, with or without a watermark, then exactly one table read is recorded, and it carries
  exactly the options `versionAsOf` = `3` and `IgnoreCorruptFiles` = `true`, with the key
  `IgnoreCorruptFiles` in this case. Depends on Q5.
- **AT-011**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced
  with a function that returns the fake session and `read()` runs, then the table read is
  recorded on the fake. Given `active` replaced with a function that raises `err` when `read()`
  runs, then `read()` raises `err` itself.
- **AT-012**: Given the `table_name` and `watermark_column` on the left, when a full read runs,
  its table read has the quoted table on the right. When a watermark read with
  `watermark_value=1` runs, every expression the reader passes to `where` and `selectExpr`
  uses the quoted column on the right, and the writer built with the same `table_name` saves to
  the same quoted table:

  | `table_name` | `watermark_column` | Quoted table | Quoted column |
  |---|---|---|---|
  | `main.sales.orders` | `updated_at` | `` `main`.`sales`.`orders` `` | `` `updated_at` `` |
  | `My-Catalog.Sales.Order Items` | `Updated At` | `` `My-Catalog`.`Sales`.`Order Items` `` | `` `Updated At` `` |
  | ``a`b.c.d`` | ``my`col`` | ``` `a``b`.`c`.`d` ``` | ``` `my``col` ``` |

### Watermark read

- **AT-013**: Given `watermark_value=datetime(2026, 10, 1, tzinfo=UTC)`, when `read()` runs,
  then the event log shows, in this order:
  1. one table read, with the name `` `main`.`sales`.`orders` `` and no options; it returns the
     source;
  2. a where on the source, with the condition
     `` `updated_at` > '2026-10-01 00:00:00+00:00' ``;
  3. a select on the DataFrame that this where returned, with exactly the expressions
     `` CAST(max(`updated_at`) AS STRING) AS upper_bound `` and
     `` typeof(max(`updated_at`)) AS upper_bound_type ``, in this order;
  4. one `first()` on the DataFrame that this select returned;
  5. a where on the source, with the condition
     ```text
     `updated_at` > '2026-10-01 00:00:00+00:00' AND `updated_at` <= CAST('2026-10-21 09:30:00.5' AS timestamp)
     ```

  `read()` returns the DataFrame that the last where returned, and no attribute of it is
  accessed. No other table read, where, select or row access is recorded.
- **AT-014**: Given no `watermark_value`, when `read()` runs, then the select is recorded on the
  source itself, with no where before it, and the only where on the source has the condition
  ``` `updated_at` <= CAST('2026-10-21 09:30:00.5' AS timestamp) ```.
- **AT-015**: Given the bound query answers `(None, "timestamp")`, with or without a
  `watermark_value`, when `read()` runs, then the last where on the source has the condition
  `false`, and `read()` returns the DataFrame it returned.
- **AT-016**: Given `watermark_column="w"` and the `watermark_value` on the left, when `read()`
  runs, then the where before the select and the last where both contain `` `w` > `` followed
  by the literal on the right:

  | `watermark_value` | Literal |
  |---|---|
  | `41` | `41` |
  | `0` | `0` |
  | `-7` | `-7` |
  | `Decimal("12.50")` | `12.50BD` |
  | `Decimal("1E+3")` | `1000BD` |
  | `Decimal("-0.25")` | `-0.25BD` |
  | `"O'Brien"` | `'O\'Brien'` |
  | `r"C:\temp"` | `'C:\\temp'` |
  | `""` | `''` |
  | `date(2026, 10, 1)` | `'2026-10-01'` |
  | `datetime(2026, 10, 1, 8, 30, tzinfo=UTC)` | `'2026-10-01 08:30:00+00:00'` |
  | `datetime(2026, 10, 1, 8, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-3)))` | `'2026-10-01 11:30:00.123456+00:00'` |

- **AT-017**: Given `watermark_column="w"`, no `watermark_value`, and the bound query answer on
  the left, when `read()` runs, then the last where's condition is the one on the right:

  | Bound query answers | Condition |
  |---|---|
  | `("12.50", "decimal(10,2)")` | ``` `w` <= CAST('12.50' AS decimal(10,2)) ``` |
  | `("2026-10-21 09:30:00.123456", "timestamp_ntz")` | ``` `w` <= CAST('2026-10-21 09:30:00.123456' AS timestamp_ntz) ``` |
  | `("it's", "string")` | ``` `w` <= CAST('it\'s' AS string) ``` |
  | `("a\\b", "string")`, whose first field is `a\b` | ``` `w` <= CAST('a\\b' AS string) ``` |

- **AT-018**: Given a reader with a watermark, when `read()` runs twice and the bound query
  answers `("2026-10-21 09:30:00.5", "timestamp")` the first time and
  `("2026-10-22 10:00:00", "timestamp")` the second, then two table reads and two `first()`
  calls are recorded, each read's last where uses its own bound, and the first `read()`'s
  DataFrame is not the second's.
- **AT-019**: Given a fake whose `table()` raises `err = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")`,
  when `read()` runs, with or without a watermark, then it raises `err` itself and no where or
  select is recorded. Given a bound query whose `first()` raises `err`, when `read()` runs, then
  it raises `err` itself and no where is recorded after the select.

### Writing

- **AT-020**: When `writer.write(df)` runs, then exactly one table save is recorded, with the
  format `delta`, the mode `append`, no options, and the name `` `main`.`lake`.`orders` ``. The
  only attributes accessed on `df` are `columns` and `write`. The only attributes accessed on
  the fake writer are `format`, `option` or `options`, `mode` and `saveAsTable`, and
  `saveAsTable` is called once. `write` returns `None`. `writer.write(df, replace=False)` gives
  the same.
- **AT-021**: When `writer.write(df, replace=True)` runs, then the save is the one of AT-020,
  except that its mode is `overwrite` and it carries exactly the option
  `partitionOverwriteMode` = `static`, with this exact key and value. Depends on Q4.
- **AT-022**: Given `UnityCatalogWriter("main.lake.orders", options={"mergeSchema": True,
  "UserMetadata": "run 42"})`, when `write(df)` runs, then the save carries exactly the options
  `mergeSchema` = `true` and `UserMetadata` = `run 42`, with the key `UserMetadata` in this
  case. When `write(df, replace=True)` runs, the save carries those two and
  `partitionOverwriteMode` = `static`. Depends on Q3 and Q4.
- **AT-023**: Given a fake DataFrame with `columns == []`, when `write` runs, with or without
  `replace=True`, then it raises `ValueError("the DataFrame has no columns")`, `write` is not
  accessed on the DataFrame, and no table save is recorded.
- **AT-024**: Given a fake writer whose `saveAsTable` raises
  `err = RuntimeError("DELTA_FAILED_TO_MERGE_FIELDS")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, `saveAsTable` was called once,
  and no attribute is accessed on the fake writer or on `df` after that call.

### Composition

- **AT-025**: Given `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, where the source that the fake's table
  read returns has `columns == ["id", "payload"]`, when it runs, then
  `process.state["orders"]` is that source, and one table save is recorded on it, with the mode
  `append`. The test binds the reader to a variable annotated `Reader[DataFrame]` and the
  writer to one annotated `Writer[DataFrame]`, and mypy strict accepts both.
- **AT-026**: Given `caplog` capturing every level, when readers and writers are built, `read()`
  runs as a full read, as a watermark read and with a bound of `None`, and `write()` runs with
  and without `replace=True`, then no log record is captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Spark is the engine.** The reader returns a Spark DataFrame, and the writer receives one.
   The connector runs only through Spark: a Databricks cluster, Databricks Connect, or any Spark
   session whose catalog resolves the name. No SQL warehouse, and no databricks-sql-connector.
2. **The table is named by its three-part name, `catalog.schema.table`.** This is what
   distinguishes the connector from `DiskWriter(path, "delta")`.
3. **Reading follows `PostgresReader`.** An optional `watermark_column` and `watermark_value`
   filter `watermark_column > watermark_value` inside the source, where Delta uses the filter to
   skip files, and the slice is frozen by an upper bound, a `max`, captured when `read()` runs,
   as the PostgreSQL reader's decision 10 does. Without a watermark column, the read is a full
   read. Delta's change data feed is out of scope, for a later spec.
4. **Writing is `write(data, *, replace=False)`**, append or replace, both atomic through Delta.
   **The writer creates the target table when it does not exist.** This differs from
   `PostgresWriter`, whose decision 29 makes a missing target fail; it is the maintainer's
   explicit choice. This spec defines the minimal form and checks it against the philosophy
   (see [Fit with the philosophy](#fit-with-the-philosophy) and [ADRs](#adrs)). Columns are
   matched by name, never by position.
5. **No new extra.** pyspark is a prerequisite (rule 4 of the extras pattern). Everything else
   follows the precedents of the PostgreSQL and disk specs: ADR 0013 constructor arguments, a
   lazy pyspark import so that the module imports without pyspark, the session looked up at read
   time or passed as `spark=`, errors propagated unchanged, no log lines, an amendment to
   ADR 0012 for what leans towards batch, and exports only from `ingestio.connector.all`.

Proposed by this spec, and accepted by the main session with its approval on 2026-10-23, under
the maintainer's delegation:

6. **Building does nothing but validate.** Each `read()` looks up the session and reads the
   table again, so that a reader built once serves every run that calls it. The writer takes no
   session; it uses `data.write`. As in the PostgreSQL spec (decision 18) and the disk spec
   (decision 6).
7. **The reader expresses its filter as Spark SQL text**, passed to `DataFrame.where` and
   `DataFrame.selectExpr` on `spark.read.options(...).table(...)`, rather than with the Column
   API. The predicate reaches Delta the same way, and tests can observe it without a JVM:
   classic PySpark needs a running JVM to build even `col("x")`. As in the PostgreSQL spec
   (decision 8). `spark.sql()` with a full statement was the alternative; it cannot take read
   options such as `versionAsOf`.
8. **Literals by type** (see [Watermark values](#watermark-values)). Numbers are numeric
   literals, because Spark compares a string with a `DECIMAL` column as `DOUBLE`. Strings, dates
   and datetimes are string literals, because Spark casts them to the column's own type, which
   keeps `TIMESTAMP_NTZ` columns exact and skippable. Aware datetimes are normalised to UTC, as
   in the PostgreSQL spec (decision 9), which gives `TIMESTAMP_NTZ` columns the same UTC wall
   clock comparison as `timestamp without time zone` there. String literals escape backslashes
   and single quotes with a backslash, and assume Spark's default
   `spark.sql.parser.escapedStringLiterals=false`.
9. **The bound is Spark's own text of the highest value, cast back to the type Spark reports
   for it,** `CAST(<bound> AS <typeof>)`. This is the analogue of the PostgreSQL spec's
   decision 10: the bound never passes through Python types. Collecting `max()` as a Python
   value and passing it back would round-trip `TIMESTAMP` values through the driver's Python
   time zone, and `TIMESTAMP_NTZ` values would come back as `TIMESTAMP` literals under Spark's
   default settings. Comparing the bound's text without a cast would compare `DECIMAL` columns
   as `DOUBLE`. The cost is one more expression in the bound query, `typeof`.
10. **The comparison is strictly greater (`>`), and `NULL` watermarks are never extracted,**
    first run included, as in the PostgreSQL spec (decision 23).
11. **Accepted watermark values are those of the PostgreSQL reader:** `int` (not `bool`), finite
    `Decimal`, `str`, `date` and timezone-aware `datetime`. Naive datetimes and floats are
    rejected with the same messages (its decision 24). A job with a `TIMESTAMP_NTZ` watermark in
    a zone other than UTC passes a `str`.
12. **The captured bound is not exposed,** and neither connector keeps state between calls, as
    in the PostgreSQL spec (decision 10) and the disk spec (Q8 there).
13. **Names are always quoted with backticks, and their case is the catalog's.** As in the
    PostgreSQL spec (decision 28), no name reaches Spark unquoted. Unlike PostgreSQL, quoting
    does not make a catalog name case-sensitive: Unity Catalog and the Hive metastore decide.
14. **The reader reads whatever the catalog resolves the name to,** table or view, with no check
    of its type. The filter reaches the table's scan when Spark can push it through the view.
15. **The writer is one call chain on `data.write`, ending in `saveAsTable`,** with the format
    `delta` set explicitly, because open-source Spark's default format is Parquet:
    - `saveAsTable` with the `append` and `overwrite` modes creates a missing table in the same
      commit as its first rows, and matches columns by name. The writer needs no existence check
      and no session, and has no race of its own to handle.
    - `insertInto` is never used, because it resolves columns by position.
    - `DataFrameWriterV2` (`writeTo`) was the alternative. No single call of it appends to an
      existing table and creates a missing one: `append()` fails on a missing table, `create()`
      on an existing one, and `createOrReplace()` also replaces the table's schema and
      properties. It would need an existence check and two code paths.
16. **The created table is minimal:** managed, Delta, the DataFrame's columns, and nothing set
    by the connector. A job that needs more creates the table first.
17. **A DataFrame without columns raises `ValueError("the DataFrame has no columns")`** before
    anything is written, as in the PostgreSQL spec (step 0 there). The disk spec left this to
    Spark (its decision 12), but here the target can exist, and Delta would append rows whose
    every column is `NULL` rather than fail.
18. **An empty DataFrame goes through the same chain,** as in the PostgreSQL spec (decision 30):
    an append adds nothing, a replace leaves the table empty, a missing table is created empty.
    A callback that wants to skip empty batches checks for itself and raises
    `CancelStepException`.
19. **Errors propagate unchanged, and the connectors write no log lines,** as in the PostgreSQL
    spec (decision 16) and the disk spec (decision 13).
20. **Supported environments:** Databricks classic compute on Runtime 15.4 LTS or later,
    Databricks Connect 15.4 or later, and open-source Spark 3.5 or later with Delta Lake.
    Serverless compute is not excluded but not tested, as for the disk connectors. Delta Lake is
    a prerequisite like pyspark, never an extra.

### ADRs

- **Creating a missing target: a new ADR (Q6).** The philosophy's sentence does not change, but
  this spec reads "does not decide ... architecture" as allowing a connector to create a missing
  target when the system derives it entirely from the data, with no choice made by the
  connector, while the PostgreSQL spec's decision 29 gives "DDL on the target is architecture"
  as its reason. Both decisions stand, as explained in
  [Fit with the philosophy](#fit-with-the-philosophy), but the line between them binds every
  later writer, so it belongs in an ADR rather than in two specs. The ADR would state: a writer
  may create its missing target only in the form the system derives from the data, with no
  layout, location, property or type mapping chosen by the connector; anything more is created
  by the job. The doc-keeper writes it in the documentation stage, with the next free number,
  for the approval of the maintainer or of the main session under delegation.
- **The format.** The writer always writes Delta, and this spec records why that is not the
  connector deciding a format (see [Fit with the philosophy](#fit-with-the-philosophy)). The
  same ADR can record it, so that a later connector for a system with one native format cites
  it.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged, and their
  payload stays open. Like the PostgreSQL and disk connectors, these lean towards batch: one
  slice per `read()`, a watermark fixed when the reader is built, and a write that needs the
  whole DataFrame. A streaming source for Delta tables, such as `readStream` on a table or on
  its change data feed, tracks its progress in a checkpoint and would be a different connector.
  This belongs in the "What leans towards batch" list of
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md): an amendment, not a new ADR.
- **ADR 0013 applies unchanged.** `table_name` and `watermark_column` are configuration keys by
  its convention, and the job computes `watermark_value`. The writer's `table_name` names the
  target, while the configuration's names the source, as on `PostgresWriter`; the doc-keeper
  may add the writer to that consequence. A predicate that the table's scan applies, so that
  Delta skips files, is how this reader filters inside the source.
- **ADR 0002 applies unchanged.** There is no new extra. Delta Lake, like pyspark, is a
  prerequisite that the runtime provides.

## Open questions

None open. On 2026-10-23 the maintainer, away for the session, delegated the approval of this
spec to the main session, with the instruction to take the recommended option of every question
that the maintainer's decisions do not settle. The main session took the recommended option of
all eight; none contradicts decisions 1 to 5. The options are kept below as the record of what
was weighed, for the maintainer to review.

1. **Class names, module and slug.**
   - (a) `UnityCatalogReader` and `UnityCatalogWriter` in `connector/unity_catalog.py`, slug
     `unity_catalog_connectors`. The maintainer's name for the system, and the module named
     after it, as `postgres.py` is for `Postgres*`. The documentation says the connectors work
     with any Spark catalog that resolves a three-part name to a Delta table.
   - (b) `TableReader` and `TableWriter` in `connector/table.py`. Accurate for any Spark
     catalog, but vague: a PostgreSQL table is a table too.
   - (c) `DeltaTableReader` and `DeltaTableWriter` in `connector/delta.py`. Names the format
     rather than the system, and the reader also reads views and other tables the catalog
     resolves.
   - (d) The names of (a) in `connector/databricks.py`, a module that a future Volumes
     connector (backlog idea 6) could share. That connector may not exist, and would need its
     own extra.

   **Recommended: (a).**
2. **Which table names are accepted?**
   - (a) Only `catalog.schema.table`. Decision 2 names the table this way, and a shorter name
     resolves against the session's current catalog and schema, which differ between clusters
     and jobs (`main`, `hive_metastore`, a workspace default).
   - (b) Also `schema.table`, resolved against the session's current catalog.
   - (c) One to three parts, as Spark accepts them.

   **Recommended: (a).**
3. **May the job evolve the schema of an existing table through options?**
   - (a) Yes: `mergeSchema` and `overwriteSchema` are Delta write options and pass through like
     any other. The job makes the choice explicitly; the connector decides nothing. Without
     them, Delta's schema enforcement makes the write fail.
   - (b) No: both are reserved, so the writer never changes an existing table's schema. A job
     that needs a new column alters the table itself, with DDL.

   **Recommended: (a).**
4. **Does `write(data, replace=True)` always replace every row of a partitioned table?**
   - (a) Yes: on a replace, the writer sets `partitionOverwriteMode` = `static`, which overrides
     a session's `spark.sql.sources.partitionOverwriteMode=dynamic`. `replace=True` then keeps
     the meaning it has on `PostgresWriter`, at the cost of the only option the connector sets.
     Tables written by other jobs are often partitioned.
   - (b) No, as `DiskWriter` does: no option of the connector's own, and the session setting
     documented as a known limitation, under which a replace leaves the partitions that `data`
     does not hold.

   **Recommended: (a).** The key stays reserved in both cases.
5. **Which read options does the reader take?**
   - (a) An `options` mapping passed through, with `path`, `readChangeFeed` and `readChangeData`
     reserved. Time travel (`versionAsOf`, `timestampAsOf`) passes through, which also lets a
     job freeze a full read. Change data feed stays for its own spec, so reserving its keys now
     avoids a breaking change later.
   - (b) No `options` on the reader: few read options matter for a catalog table.
   - (c) Every option passes through, change data feed included.

   **Recommended: (a).** `path` is reserved on the writer in every case.
6. **Does the creation of a missing target get an ADR?**
   - (a) A new ADR that records when a writer may create its target (only in the form the system
     derives from the data, nothing chosen by the connector) and why Delta is the system's
     format rather than a choice. It binds later writers, such as a BigQuery writer.
   - (b) An amendment to ADR 0013, which is about configuration rather than architecture.
   - (c) No ADR: the reasoning stays in this spec, next to the PostgreSQL spec's decision 29.

   **Recommended: (a).**
7. **How do the integration tests get Delta Lake, and is `delta-spark` added to the `dev`
   group?**
   - (a) Not added. The tests are opt-in through `INGESTIO_SPARK_TESTS=1`, as for the disk
     connectors, plus `INGESTIO_DELTA_PACKAGE`, the Maven coordinate of the Delta release that
     matches the installed pyspark, which the session loads through `spark.jars.packages`. The
     connectors never import the `delta` Python package, and `delta-spark` pins pyspark to its
     own release line, which could hold back the locked pyspark 4.2 or fail to resolve.
   - (b) Add `delta-spark` to the `dev` group in the version that matches pyspark, and configure
     the session with its `configure_spark_with_delta_pip`. It still downloads the JAR from
     Maven at the first run.
   - (c) No integration tests.

   **Recommended: (a).**
8. **Does the writer check that an existing target is a Delta table?**
   - (a) No. The writer does not look the name up, and the limitation is documented. Spark
     refuses an append to a table in another format, and Databricks refuses writes to views,
     materialized views and streaming tables. The remaining risk is a replace of such a target
     in open-source Spark's session catalog, which can drop it and create a Delta table in its
     place; integration scenario 8 checks what Spark does. A target that is not Delta is a
     mistake in the job's configuration, and `replace=True` already means that every row of
     the target goes.
   - (b) Yes. Before the save, the writer looks the name up through the catalog of
     `data.sparkSession`, and raises `ValueError` when it names a view or a table whose format
     is not Delta. One more catalog call and one more failure mode on every write, a check that
     still races with the write, and the fakes grow a catalog.

   **Recommended: (a).**

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions: `UnityCatalogReader` reads `spark.read.options(...).table(...)` and, with a watermark, filters with Spark SQL text, capturing an upper bound with `CAST(max(col) AS STRING)` and `typeof(max(col))` and slicing `col > <value> AND col <= CAST(<bound> AS <type>)`; `UnityCatalogWriter` makes one chain, `data.write.format("delta").options().mode(append\|overwrite).saveAsTable(...)`, which creates a missing table as a minimal managed Delta table and matches columns by name. 26 acceptance criteria, 9 integration scenarios, 20 decisions (5 by the maintainer, 15 proposed), 8 open questions. |
| 2026-10-23 | approval | approved | Approved by the main session under the maintainer's delegation of 2026-10-23 ("Delegar e ir até o fim"), with the recommended option of all 8 questions: `connector/unity_catalog.py`, three-part names only, `mergeSchema`/`overwriteSchema` passed through, `partitionOverwriteMode=static` set on replace, change-data-feed keys reserved, a new ADR for creating a missing target (written in the documentation stage), no `delta-spark` in the `dev` group, no Delta check of the target. The 15 proposed decisions accepted. No change to `pyproject.toml`. Flagged for the maintainer's review: the new ADR, and the planner's unverified claims about `saveAsTable` overwrite on open-source Delta and `TIMESTAMP_NTZ` casts, which only the blind integration scenarios check. Later stages run after S3, GCS and Volumes, in the order the maintainer chose. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_unity_catalog_connectors.py`: 34 tests (97 cases) covering AT-001 to AT-026 on a fake Spark session and fake DataFrames sharing one ordered event log (the fake writer also offers `insertInto`, `save`, `partitionBy` and the like, to show they are never touched), checking the spec's expressions exactly; and 11 integration tests (17 cases) for the 9 Delta scenarios, gated by `INGESTIO_SPARK_TESTS=1` and `INGESTIO_DELTA_PACKAGE`, written blind (Java 27 only), which skip when another module already started a JVM without Delta, so they must run in their own process. No existing test changed. Checked by the main session: collection fails as expected with `ImportError: cannot import name 'UnityCatalogReader' from 'ingestio.connector.all'`; the rest of the suite passes (771 passed, 44 skipped); ruff and format clean. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `UnityCatalogReader` and `UnityCatalogWriter` in a new section of `docs/concepts/connectors.md`, right after the PostgreSQL section and in its shape, noting that the opt-in integration tests run in their own process; wrote ADR 0014 (create a missing target only in the form the system derives from the data; Delta as the system's native format), accepted under the maintainer's delegation and marked for review, with one `TODO(maintainer)` for the reason behind decision 4; cited it from the "does not decide architecture" reading and from `PostgresWriter`'s "no DDL"; said that `DiskWriter(path, "delta")` creates a Delta table at the path and registers nothing; amended ADR 0012 (leans towards batch) and ADR 0013 (the writer's `table_name`; the predicate in the table's scan); updated `docs/index.md` and the architecture KB. Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable Unity Catalog blocks, with the expected `ImportError` of the missing classes (18 passed). |
| 2026-10-23 | implementation | implemented | The implementer added `UnityCatalogReader` and `UnityCatalogWriter` in `src/ingestio/connector/unity_catalog.py`, mirroring `postgres.py`: backtick quoting, watermark literals by type (`BD` decimals, backslash-escaped strings, aware datetimes in UTC), the bound query with `CAST(max(...) AS STRING)` and `typeof(max(...))` cast back in the slice, reserved options, and the single `format("delta").options().mode().saveAsTable()` chain with `partitionOverwriteMode=static` on replace; exported from `connector/all.py`; pyspark imported lazily. Nothing reused from `_files.py`, no existing connector changed. Checked by the main session: 870 passed, 61 skipped (the opt-in integration tests), ruff, format and mypy strict clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-026 covered (97 passed, the 17 opt-in Delta integration cases skipped: no Java 17/21), the full suite with the documentation examples 870 passed and 61 skipped, ruff, format, mypy strict and `uv lock --check` clean; code, documentation, ADR 0014 and the ADR 0012 and 0013 amendments agree; no new dependency or extra; the public API changes only as specified. For the maintainer's review: ADR 0014's stated reason for decision 4 (its Context cites "no separate DDL step", its Rationale atomicity, and a `TODO(maintainer)` asks for the real one), and its premise that Delta is Unity Catalog's one native table format, since Unity Catalog can also manage Iceberg tables; and the planner's Spark/Delta claims, which only the blind integration tests check. |
