# Spec: disk reader and writer connectors

- **Status:** validated
- **Slug:** `disk_connectors`, a working slug (see [question 1](#open-questions)). The spec is
  `specs/disk_connectors.md` and its tests are `tests/test_disk_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for files in a directory on disk, built on Spark's file data sources.
`DiskReader` lists the directory, selects the files above the job's watermark before anything
is loaded, and returns a Spark DataFrame over exactly those files. `DiskWriter` writes a Spark
DataFrame as files, appending them to the directory or replacing what it holds, as the calling
callback chooses when it calls `write`. The job names the format on both sides, and Spark's
data source for that format does the conversion. A file's watermark comes from its name,
through a pattern the job gives, or else from its last-modified time.

The connectors follow the PostgreSQL connectors. An ingestion's parameters are constructor
arguments ([ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)). pyspark
is a prerequisite, imported lazily. The session is looked up when the reader reads. The writer
has one method, `write(data, *, replace=False)`. They add no dependency and no extra.

Many choices below were open questions, which the maintainer answered with the recommended
option of each one. The spec names the question a choice comes from, as **(Q n)**.

## Motivation

The maintainer wants Python jobs, typically on a Databricks cluster, to ingest files that
land in a directory and to write DataFrames as files:

- **Read.** Producers drop files into a landing directory, such as a Unity Catalog volume. A
  job must read only the files it has not ingested yet, and must pick them before Spark loads
  anything. For a file system, the unit the source can filter is the file. Selecting files
  before loading is how this connector filters "inside the source", as ADR 0013 requires of
  readers: Spark never opens a file that was not selected.
- **Watermark.** The job knows the last watermark it ingested. For now the maintainer assumes
  each file's watermark can be read from its name, such as `orders_20261021.csv`. When it
  cannot, the file's last-modified time is the fallback.
- **Format.** The files can be CSV, JSON, Parquet or another format Spark reads. The job knows
  which one it is; the connector converts files in that format into a DataFrame, and a
  DataFrame into files in that format.
- **Write.** A DataFrame is written as files in a directory. The calling callback chooses at
  call time whether to append or to replace, as with `PostgresWriter`. The maintainer chose
  not to build a staging mechanism: whatever atomicity a write has comes from Spark's commit
  protocol and from the format, and its limits are documented.
- **What exists does not fit.** `JsonLinesWriter` writes Python records, not DataFrames, fixes
  its format, and fixes the write mode in its configuration (the
  [Known divergence](../docs/concepts/connectors.md#known-divergence)).
  `spark.read.load(directory)` reads every file. Spark's `modifiedAfter` option filters by
  modification time only, while Spark lists the directory. Structured Streaming's file source
  and Databricks Auto Loader record the files they processed in a checkpoint. They are
  streaming, which is deferred ([ADR 0012](../docs/decisions/0012-streaming-deferred.md)), and
  Auto Loader exists only on Databricks.

## Fit with the philosophy

- **Concept: connector.** `DiskReader` satisfies `Reader[DataFrame]` and `DiskWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). The file system is the
  external system: the philosophy names "a file system" among the systems a connector
  integrates. `write(data, *, replace=False)` adds a keyword-only argument with a default, so
  `write(data)` stays a valid call and the method still matches the protocol. `ReadSource` and
  `WriteTarget` use both connectors unchanged.
- **Format: given, not decided.** The philosophy says a connector "does not decide write
  strategy, format or architecture". These connectors handle a format, but they decide
  nothing about it:
  - The format is a required constructor argument with no default. The job gives it, from its
    configuration, as it gives `table_name` to `PostgresReader`. The connector never picks
    one, never infers one from file extensions, and never changes it.
  - A file system stores bytes. A reader that returns a DataFrame must decode them, just as
    `JsonApiReader` decodes the JSON body of a response and `PostgresReader` turns rows into a
    DataFrame through JDBC. Decoding and encoding in the format the job names is how this
    connector talks to its system. That is the connector's role, not a decision.
  - The philosophy lists "formatting" among the actions of callbacks. That means transforming
    the data: types, names, cleaning, medallion layers. These connectors transform nothing.
    They add no column, cast nothing, rename nothing and filter no rows. Spark's data source
    alone turns the bytes into rows, with the options the job passes through.
  - `JsonLinesWriter` is the counter-example. It fixes JSON Lines inside the connector, which
    is why it is a known divergence. Here nothing is fixed.

  **Conclusion: the philosophy does not need to change.** "Does not decide" already allows a
  connector that applies the format it is given. [Question 16](#open-questions) asks whether
  the maintainer wants that reading recorded in an ADR anyway.
- **Write strategy.** Append and replace are the two primitive ways Spark puts a batch into a
  directory: the `append` and `overwrite` save modes. The writer offers both through one
  method and picks neither. The calling callback chooses at call time, and the connector's
  configuration has no mode (decision 3). Options that would turn a replace into a partial
  replace, a strategy of their own, are reserved **(Q11)**. Merges, deduplication and
  anything built on top belong to callbacks.
- **Architecture.** The writer creates no table, registers nothing in a catalog, and lays out
  no partitions **(Q12)**. The reader reads one directory.
- **The reader decides no strategy.** Whether a run is incremental, and from which value, is
  the job's decision. So is where a file's watermark comes from: the job either passes a
  pattern or does not. The reader turns that into a selection of files.
- **Essential by default.** The connectors catch nothing to carry on: every failure
  propagates out of `read()` or `write()` and fails the callback that called them.
- **Simplicity.** Two small classes in one module. No base class, no registry, no staging,
  no new protocol, no new callback. Spark's data sources do the conversion; the reader's own
  logic is a directory listing and a comparison.
- **Compose, do not impose.** The job builds the connectors from its own configuration.
  Nothing reads files, environment variables or `dbutils`. A user can still write their own
  file connector in their own code.
- **No dependencies in the core.** No new package, so no extra **(Q14)**. pyspark is a
  prerequisite, as for the PostgreSQL connectors, imported only inside the functions that use
  it. `ingestio` and `ingestio.connector.all` still import without it.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `DiskReader`: reads of the files directly inside one directory, all of them or only those
  above a watermark, selected before loading and frozen by `read()`. The watermark comes from
  the file name through a pattern, or from the last-modified time;
- `DiskWriter`: append (`write(data)`) and replace (`write(data, replace=True)`) through
  Spark's file writers;
- the rules for paths, formats, watermark values and options, and the reserved options;
- the documentation of what a write guarantees and does not guarantee;
- the test doubles, and integration tests against a local Spark session.

Out of scope:

- the job configuration file and recipes: reading or parsing JSON, `from_config`, and mapping
  configuration keys to arguments;
- computing, persisting or advancing the watermark, and exposing which files `read()`
  selected **(Q8)**;
- cloud storage URIs such as `s3://`, `abfss://`, `gs://` and `dbfs:/` **(Q3)**;
- subdirectories and partitioned input layouts **(Q9)**, and partitioned output
  (`partitionBy`) **(Q12)**;
- reading table formats (Delta, Iceberg, Hudi) **(Q2)**, writing to catalog tables
  (`saveAsTable`, `insertInto`), and any DDL;
- a watermark column, that is filtering rows inside the files **(Q7)**;
- streaming file sources: Structured Streaming's `readStream`, Auto Loader (`cloudFiles`),
  checkpoints;
- a staging mechanism, or any atomicity beyond Spark's and the format's (decision 4);
- moving, archiving or deleting source files after they are read;
- reading a single file given as `path` **(Q10)**;
- Spark's other save modes, `ignore` and `errorifexists`;
- schema evolution, deduplication of files, checksums;
- engines other than Spark, and file connectors in plain Python;
- changes to `JsonLinesWriter` **(Q13)**;
- changes to CI, which belong to the maintainer.

## Public API

New, in `ingestio/connector/local.py` **(Q1)**, exported from `ingestio.connector.all` only
(not from `ingestio`):

```python
from ingestio.connector.all import DiskReader, DiskWriter
```

```python
class DiskReader:
    """Reads the files of a directory into a Spark DataFrame, in the format it is given.

    Args:
        path: Directory that holds the files: an absolute local path, such as a Databricks
            volume, `/Volumes/<catalog>/<schema>/<volume>/<dir>`. The configuration key of
            the same name.
        format: Name of the Spark data source the files are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a file matches, with one
            group that captures the file's watermark. Without it, a file's watermark is its
            last-modified time. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only files above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the files, as a `StructType` or a DDL string. Spark infers it when
            omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        format: str,
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the files the job needs."""
```

```python
class DiskWriter:
    """Writes a Spark DataFrame as files in a directory, in the format it is given.

    Args:
        path: Directory to write into: an absolute local path, such as a Databricks volume.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        format: str,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` as files in the directory.

        Args:
            data: The rows to write.
            replace: Remove what the directory holds first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame`, `SparkSession` and `StructType` are `pyspark.sql.DataFrame`,
  `pyspark.sql.SparkSession` and `pyspark.sql.types.StructType`. `PathLike` is `os.PathLike`,
  `datetime` is `datetime.datetime`, and `Mapping` is `collections.abc.Mapping`.
- `path` and `format` are required, so they are positional; every option is keyword-only.
- `format` shadows a builtin, which ruff's rule A002 reports **(Q1)**.
- `DiskWriter` has a single write method, `write`. `replace` is keyword-only and defaults to
  `False`, so `DiskWriter.write` satisfies `Writer[DataFrame]` structurally. `WriteTarget`,
  which calls `write(data)`, always appends.
- No new extra.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL connectors, and everything exported by `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration, keeps
its own watermark, and advances it from what it read.

```python
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

# The selection is not exposed: the job takes its next watermark from the files it read,
# through Spark's hidden _metadata column. Spark's regex dialect is Java's: plain groups only.
orders = process.state["orders"]
if not orders.isEmpty():
    last_watermark = orders.select(
        F.max(F.regexp_extract("_metadata.file_name", r"^orders_(\d{8})\.csv$", 1))
    ).first()[0]
```

## Behaviour

### Building the connectors

Building a connector validates its arguments and does nothing else. It imports no third-party
package, touches neither Spark nor the file system, and does not check that `path` exists.

**Paths (Q3).** `path` must be an absolute local path, `Path(path).is_absolute()`. Anything
else raises `ValueError("path must be an absolute local path")` when the connector is built.
This rejects relative paths, the empty string, and every URI: `s3://...`, `abfss://...`,
`file:/...` and `dbfs:/...` are not absolute paths. Relative paths are rejected because the
driver's Python process and Spark can resolve them from different working directories (on
Databricks, the notebook's directory and `/databricks/driver`). The connectors keep the path
as `str(Path(path))`, so `/Volumes/main/landing/orders/` becomes
`/Volumes/main/landing/orders`. They pass Spark paths built from it, never URIs.

**Formats (Q2).** `format` is passed to Spark as given. An empty `format` raises
`ValueError("format must not be empty")`. Spark looks the format up when it reads or writes,
and an unknown one fails there. The reader rejects `delta`, compared case-insensitively, with
`ValueError("format '<format>' cannot be read file by file")`, where `<format>` is the value as
written. A Delta table's transaction log decides which of its Parquet files hold live data, so
its files cannot be selected one by one. The writer accepts `delta`.

`watermark_pattern`, `watermark_value` and `options` are validated as described under
[Watermark values](#watermark-values) and [Options](#options). `schema` is not validated: Spark
parses it when the reader reads.

**`repr`** shows the path and the format:

- `DiskReader(path='/Volumes/main/landing/orders', format='csv')`
- `DiskWriter(path='/Volumes/main/lake/orders', format='parquet')`

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps, in order:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()` at each `read()`, never when the reader is built. The
   reader never creates a session. When none is active, PySpark's error propagates.
2. **Listing (Q3, Q9).** The reader lists the entries directly inside `path`, once, with
   Python's standard library on the driver. Errors of the listing propagate unchanged:
   `FileNotFoundError` when `path` does not exist, `NotADirectoryError` when it is a file,
   `PermissionError` **(Q10)**.
3. **Candidates.** A candidate is an entry that:
   - is a regular file, or a symbolic link to one. Directories and broken links are not
     candidates;
   - has a name that starts with neither `_` nor `.` and does not end with `._COPYING_`.

   This is Spark's own rule for hidden and in-progress files. It leaves out `_SUCCESS`,
   `_temporary`, `_delta_log`, `.crc` files and files that Hadoop is still copying. Spark's
   listing keeps the Parquet summary files `_metadata` and `_common_metadata`. They are not
   data files, and the reader leaves them out too. Subdirectories are ignored, with everything in
   them.
4. **Watermarks.** The reader takes each candidate's watermark, as described in
   [The watermark of a file](#the-watermark-of-a-file).
5. **Selection.** The reader selects the candidates whose watermark is strictly greater than
   `watermark_value`, or every candidate when `watermark_value` is `None`. The comparison is
   strict because the value is the last watermark already ingested, as in the PostgreSQL
   reader.
6. **Load.** The reader loads the selection, or builds an empty DataFrame when the selection
   is empty.

#### The watermark of a file

- **With `watermark_pattern` (Q4, Q5).** The pattern is matched against the whole file name,
  not the path, with `re.fullmatch`. The watermark is the text that the pattern's group
  captured. A candidate is not read when its name does not match, or when its group took no
  part in the match (its value is `None`). Watermarks compare as Python strings, code point
  by code point. So `"20261021" > "20261020"`, but also `"9" > "10"`. A watermark that is
  fixed-width and most significant first sorts the way its value does: ISO 8601 dates and
  timestamps written the same way in every name, or zero-padded sequence numbers. The value
  is never converted.
- **Without `watermark_pattern` (Q6).** The watermark is the file's last-modified time:
  `st_mtime_ns` from `os.stat`, following symbolic links, truncated to whole milliseconds,
  exactly and without floating point, as a timezone-aware UTC `datetime`. It is compared with
  `watermark_value` as an instant, whatever the value's zone. The file name plays no part.
  Milliseconds are the precision Hadoop's file systems report, and with them Spark's
  `_metadata.file_modification_time`. A job that takes its next watermark from that column
  therefore never selects its last file again.

#### Load

When the selection holds at least one file, `read()` makes exactly one load:

- `spark.read.format(format)`;
- `.schema(schema)` when `schema` is given, with the very object given;
- the user's `options`, as given, and no other option;
- `.load(paths)`. `paths` is a `list` of `str`, one per selected file:
  `str(Path(path) / name)`, sorted by file name in code-point order. It is a list even for one
  file.

`read()` returns the load's DataFrame unchanged. The reader adds no column, filter, cast or
projection.

**The selection is frozen (Q8).** Spark receives the selected files, never the directory.
Every action on the DataFrame reads those files and no other, so files that appear after
`read()` are left for the next run, whatever their watermark. A directory path would not give
this guarantee. Classic Spark lists a directory when the DataFrame is created, but under
Spark Connect the plan can be resolved again at each action, which would list the directory
again. A list of files depends on neither.

**The selection is not exposed (Q8).** The reader keeps no record of what it selected. A job
that advances its watermark takes it from the DataFrame. Spark's file sources offer the hidden
column `_metadata`, with `file_name`, `file_path` and `file_modification_time`, as in the
[usage sketch](#usage-sketch).

#### When nothing is selected (Q10)

The selection is empty when the directory is empty, holds only entries that are not
candidates, holds no name that matches the pattern, or holds no watermark above the value.
Then `read()` makes no load. It returns `spark.createDataFrame([], schema)`, with `schema`
when it is given and otherwise a `pyspark.sql.types.StructType` with no fields. The result has
no rows. Its columns are those of `schema`, or none.

This DataFrame is not backed by files, so it has no hidden `_metadata` column. A callback that
wants to skip an empty batch checks for itself, with `df.isEmpty()` for instance, and raises
`CancelStepException`.

#### Errors

Nothing is caught. Errors of the listing, of `os.stat`, of PySpark and of Spark propagate out
of `read()` unchanged, as the same object. An unknown format or an invalid `schema` fails in
Spark. The connectors write no log lines.

### Watermark values

`watermark_value` is "given" when it is not `None`; `""` is a value. Its type depends on where
the watermark comes from **(Q4, Q6)**:

| `watermark_pattern` | Accepted `watermark_value` |
|---|---|
| given | `str` |
| not given | timezone-aware `datetime.datetime` |

The reader validates them when it is built:

| Case | `ValueError` message |
|---|---|
| `watermark_pattern` is not a valid regular expression | `invalid watermark_pattern: <error>` |
| `watermark_pattern` has no group, or more than one | `watermark_pattern must have exactly one group` |
| `watermark_pattern` given, `watermark_value` given and not a `str` | `watermark_value must be a str with watermark_pattern` |
| `watermark_pattern` not given, `watermark_value` given and not a `datetime.datetime` | `watermark_value must be a datetime without watermark_pattern` |
| a naive `datetime.datetime` | `watermark_value must be timezone-aware` |

- `<error>` is the message of the `re.error` that `re.compile` raised. The `ValueError` is
  raised from it, so the `re.error` is its `__cause__`.
- A group is a capturing group, named or not, as `re.compile(pattern).groups` counts it. A
  non-capturing group, `(?:...)`, does not count. The empty pattern has no group.
- A `datetime.date` that is not a `datetime.datetime` is rejected without a pattern, and so
  are `int` and `bool`.
- A datetime is naive when its `tzinfo` is `None` or its `utcoffset()` is `None`.

### Options

- The connectors set no option of their own. `options` reaches Spark as given, and Spark turns
  values into strings, booleans into `true` and `false`.
- Some keys are reserved **(Q11)**. Giving one, in any case, raises
  `ValueError("option '<key>' is set by the connector")` when the connector is built, with
  `<key>` as the user wrote it. This is the PostgreSQL connectors' message: the connector owns
  what these keys control.

  | Connector | Reserved keys | Why |
  |---|---|---|
  | both | `path`, `paths` | The connector gives Spark its paths, and Spark rejects a `path` option given together with path arguments. |
  | `DiskReader` | `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`, `modifiedAfter` | They select files, which the reader does itself before loading. |
  | `DiskWriter` | `replaceWhere`, `partitionOverwriteMode` | They turn `replace=True` into a partial replace, a write strategy that belongs to callbacks. |

### Writing

`write(data, *, replace=False)` makes exactly one call chain:

```text
data.write.format(<format>).options(<options>).mode(<mode>).save(<path>)
```

- `<mode>` is `"append"` when `replace` is `False` and `"overwrite"` when it is `True`;
- `<options>` are the user's, as given, and no other;
- `<path>` is the normalised path, a `str`.

**Operations.**

- `write(data)` appends: Spark adds new files to the directory and leaves the existing ones as
  they are. `write` is the method of the `Writer` protocol, and `WriteTarget` calls
  `write(data)`, so `WriteTarget` appends, as it does with `PostgresWriter`.
- `write(data, replace=True)` replaces: the directory ends up holding exactly the rows of
  `data`. With a plain file format, Spark deletes `path` and everything under it, including
  files it did not write, and then writes the new files. With Delta, the overwrite is a new
  version of the table.
- The calling callback chooses at call time, by passing `replace`. The connector's
  configuration has no mode, and there is no `replace()` method.
- Spark creates `path`, and its missing parents, when they do not exist.
- The file names are Spark's (`part-<n>-<uuid>-c000...`), so a `DiskReader` that reads them
  back takes their watermark from their last-modified time.

**What the writer does not do.**

- It reads only `data.write` from the DataFrame. It never counts, caches or inspects the
  DataFrame, and the save evaluates it once.
- It does not list, create or delete anything itself. There is no staging and no cleanup. It
  does not retry.
- It calls no other method of Spark's writer: no `partitionBy` **(Q12)**, `bucketBy`,
  `sortBy`, `saveAsTable` or `insertInto`.
- It takes no session: it uses `data.write`, as `PostgresWriter` does.
- An empty DataFrame goes through the same call. What lands depends on Spark: for Parquet,
  for instance, a file that holds only the schema. A DataFrame without columns is not
  checked: Spark's own error propagates.
- `write` returns `None`. Every error propagates unchanged, as the same object. The writer
  writes no log lines.

**Atomicity (decision 4).** The writer adds no mechanism of its own. A write guarantees what
Spark's commit protocol and the format guarantee, and nothing more:

- **Append, plain file formats** (`parquet`, `csv`, `json`, `orc`, ...). Open-source Spark
  uses Hadoop's `FileOutputCommitter` by default, with algorithm version 1. Tasks write under
  a `_temporary` directory inside `path`, and the job's commit moves their files into `path`
  one by one. A job that fails before its commit adds no data file, but it can leave a
  `_temporary` directory behind, which Spark and `DiskReader` ignore. A reader that lists
  `path` during the commit can see part of the new files. With algorithm version 2, files are
  moved as each task commits, so a failed job can leave part of the batch.
- **Replace, plain file formats.** Spark deletes `path` before it writes. If the write then
  fails, `path` is left missing, empty or holding part of the new rows, and the previous
  contents are lost. Readers during the write see a missing, empty or partial directory.
- **Delta (Q2).** An append or an overwrite is one commit in the table's transaction log.
  Readers see the table before the commit or after it, and a failed write leaves the table as
  it was.
- **On Databricks,** the runtime can use its own commit protocol for file formats. What it
  guarantees is the runtime's, and this spec does not rely on it.

### Where it lives

- `src/ingestio/connector/local.py` **(Q1)**: `DiskReader` and `DiskWriter`, next to
  `JsonLinesWriter`. The module docstring covers all three, and `__all__` stays sorted.
- `src/ingestio/connector/all.py`: exports the two classes.
- No change to `pyproject.toml`, `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or
  `connector/core.py`.

### Supported environments

- pyspark 3.5 or later is a prerequisite that the runtime provides, as for the PostgreSQL
  connectors. ingestio never installs it.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated or
  standard access mode, with paths in Unity Catalog volumes, `/Volumes/...`. The driver lists
  them through the volume's FUSE mount, and Spark reads and writes them as volume paths. Other
  paths on Databricks (`/tmp`, `/dbfs/...`, `/Workspace/...`) are not supported, because
  Python and Spark resolve them to different file systems. Only the DataFrame API is used,
  never `sparkContext` or `_jvm`.
- **Open-source Spark 3.5 or later**, on a Java version that release supports (17 or 21 for
  Spark 4), when Spark's default file system is the local one (`fs.defaultFS` is
  `file:///`, the default outside Hadoop clusters). That covers local mode, and clusters
  where `path` is on a file system mounted at the same place on the driver and on every
  executor.
- **Databricks serverless compute:** nothing in the design excludes it, but it is not tested
  **(Q3)**.
- **Formats** outside Spark's built-ins need their package in the session: Delta Lake
  (`delta-spark` on open-source Spark, built into Databricks Runtime), or `spark-avro` on
  open-source Spark. Such engine plugins are prerequisites, never extras.

### Known limitations

These are documented, not solved here:

- **Writes are only as atomic as Spark and the format make them** (see
  [Atomicity](#writing)). A failed replace of plain files loses the previous contents. A
  reader of the target during a write can see part of it.
- **`replace=True` deletes everything under `path`** with a plain file format, files that
  Spark did not write included. Never point a writer that replaces at a directory that holds
  anything else.
- **Late files.** A run that advances its watermark to the highest one it read never reads a
  file that appears later with a watermark at or below it:
  - by name: a producer that delivers files out of order;
  - by modification time: a file moved or renamed into the directory after a newer file was
    read, because a rename keeps the modification time, and so do copies that preserve times;
    files that another Spark job is committing one by one; clocks that differ between
    producers.
  - Files that share the watermark of the last file read and arrive after it are skipped,
    because the comparison is strict.
- **Files in progress.** A file that is still being written when `read()` lists the directory
  can be read in part. Producers can write under a name that starts with `_` or `.`, or ends
  with `._COPYING_`, and rename the file when it is complete. The reader ignores such names.
  With modification-time watermarks, the rename itself can make the file a late file.
- **The selection is frozen, not the contents.** A selected file that is changed before an
  action is read as it is at that action. A selected file that is deleted makes the action
  fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Modification-time precision.** Times are compared to the millisecond. Files modified in
  the same millisecond as the watermark are not read. File systems that keep coarser times
  widen this window.
- **Name watermarks compare as text.** A watermark that is not fixed-width, or not most
  significant first, does not sort as its value: `part_10.csv` comes before `part_9.csv`.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. Writing a DataFrame without columns fails in Spark.
- **Large directories.** The driver lists the whole directory at each `read()`, and Spark
  receives one path per selected file.
- **The path must name the same directory for the driver's Python process and for Spark.**
  On Databricks that holds only for volumes.
- **Table formats.** The reader rejects `delta`, but it does not detect other table formats
  such as Iceberg or Hudi. Reading their directories file by file fails or returns rows that
  are no longer live.
- **Session settings apply.** The connector overrides no Spark setting. For example,
  `spark.sql.sources.partitionOverwriteMode=dynamic` in the session makes `replace=True` on a
  partitioned Delta table replace only the partitions present in `data`.
- **Type conversion** is whatever Spark's data source does. The connectors convert nothing.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles and real files,
without a JVM. The doubles live in `tests/test_disk_connectors.py`.

- **Fake Spark session.** Each access to `spark.read` returns a new fake reader. It supports
  `format(source)`, `schema(schema)`, `option(key, value)`, `options(**options)` and
  `load(path=None, format=None, schema=None, **options)`. Each `load()` records one *load*,
  with:
  - the format;
  - the schema object, or none;
  - the merged options. A later call replaces an earlier key with the same name, compared
    case-insensitively as Spark does, and the original case of every key is kept. Values are
    compared as Spark sees them: `str(value)`, and booleans as `true` and `false`;
  - the `path` argument, the very object passed.

  `load()` returns a new fake DataFrame. `spark.createDataFrame(data, schema=None)` records a
  *create* with the data and the schema object, and returns a new fake DataFrame. The fake
  records every attribute accessed on the session.
- **Fake DataFrame.** It records every attribute accessed on it. `write` returns a fake writer
  that supports `format`, `option`, `options`, `mode` and
  `save(path=None, format=None, mode=None, partitionBy=None, **options)`, and records every
  attribute accessed on it. `save()` records a *save*, with the format, the mode, the merged
  options and the path.
- **Files.** Tests create real files under `tmp_path`; their contents do not matter. They set
  modification times with `os.utime(file, ns=(n, n))`, where `n` is the time in nanoseconds
  since the epoch, and create links with `Path.symlink_to`.
- `pyspark.sql.SparkSession.active` can be replaced with `monkeypatch.setattr`. To simulate a
  missing pyspark, map `pyspark` to `None` in `sys.modules`, in a subprocess.
- Tests use the public API. They pass the fakes as `spark=` and as DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against a local Spark session, `local[2]`, with
`spark.sql.session.timeZone=UTC` and files under `tmp_path`. They are marked
`@pytest.mark.integration` and skipped, with a reason, unless the environment variable
`INGESTIO_SPARK_TESTS` is `1` **(Q15)**. They need Java 17 or 21, because `uv.lock` holds
pyspark 4.2. They cover:

1. `DiskWriter(dst, "parquet")`: two calls of `write(df)` leave the rows of both batches, and
   `write(df, replace=True)` then leaves exactly the rows of `df`. A full `DiskReader(dst,
   "parquet")` returns them, and the `_SUCCESS` and `.crc` files Spark wrote do not disturb it.
2. CSV round trip: written with `{"header": True}`, read back with the same option and a DDL
   `schema`, with the same rows.
3. Name watermark: CSV files written by the test in plain Python. Only the rows of the files
   above the value are returned. A file added after `read()`, with a higher name, is absent
   from a second `count()` on the same DataFrame.
4. Modification-time watermark: files whose times the test sets. Only the newer files' rows
   are returned. The highest `_metadata.file_modification_time` of the DataFrame equals the
   newest selected file's time truncated to the millisecond. A second reader built with that
   instant, as an aware UTC datetime, selects no file.
5. Empty selection: with a `schema`, a DataFrame with its columns and no rows; without one, a
   DataFrame with no columns and no rows.
6. A write whose computation fails, through a Python UDF that raises, raises out of `write`.
7. `write(df, replace=True)` on a directory that holds `keep.txt` removes it. This confirms
   the documented limitation.

## Acceptance criteria

The criteria follow the answer to every question, the recommended option in each case. A
criterion that depends on one names it.

Unless a criterion says otherwise:

- `spark` is the fake session and `df` a fake DataFrame, both from
  [Notes for testing](#notes-for-testing);
- `src` is `tmp_path / "landing"`, an existing directory that starts empty, and `p(name)` is
  `str(src / name)`;
- `T0` is `datetime(2026, 10, 21, 9, 0, tzinfo=UTC)`. "A file at `t`" is a file whose
  modification time is set to `t`;
- the reader is `DiskReader(src, "csv", spark=spark)`, with whatever the criterion adds;
- the writer is `DiskWriter("/data/orders", "parquet")`;
- "the paths" of a load are the value its `load()` received;
- "exactly the options" means these keys and no others, compared case-insensitively unless
  the criterion says the case matters.

### Import surface

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import DiskReader, DiskWriter` runs, then it succeeds. Both
  names are in `ingestio.connector.all.__all__` and in `ingestio.connector.local.__all__`, and
  neither is in `ingestio.__all__`. Depends on Q1.
- **AT-002**: Given an interpreter where `import pyspark` raises `ImportError`, when
  `ingestio.connector.all` is imported, then the import succeeds. A
  `DiskReader(<a directory holding a.csv>, "csv", spark=<fake session>)` and a
  `DiskWriter("/data/orders", "parquet")` can be built. The reader's `read()` returns the
  DataFrame of the fake's load, and `write(<fake DataFrame>)` records one save.

### Building the connectors

- **AT-003**: Given `path` in `""`, `"landing/orders"`, `"./orders"`, `"s3://bucket/orders"`,
  `"file:/tmp/orders"` and `"dbfs:/Volumes/main/landing/orders"`, when `DiskReader(path, "csv")`
  or `DiskWriter(path, "csv")` is built, then it raises
  `ValueError("path must be an absolute local path")`. `"/tmp/orders"` and
  `Path("/Volumes/main/landing/orders")` are accepted by both. Depends on Q3.
- **AT-004**: Given `format=""`, when either connector is built, then it raises
  `ValueError("format must not be empty")`. Given `"delta"` or `"Delta"`, when
  `DiskReader("/data/landing", fmt)` is built, then it raises `ValueError` with the message
  `format '<fmt>' cannot be read file by file`, with the format as given, while
  `DiskWriter("/data/orders", fmt)` is accepted. `"parquet"`, `"CSV"` and
  `"com.example.source"` are accepted by both. Depends on Q2.
- **AT-005**: Given the `watermark_pattern` on the left, when
  `DiskReader("/data/landing", "csv", watermark_pattern=pattern)` is built, then it raises
  `ValueError` with the message on the right:

  | `watermark_pattern` | Message |
  |---|---|
  | `"("` | starts with `invalid watermark_pattern: `, and the `__cause__` is a `re.error` |
  | `r"orders_\d{8}\.csv"` | `watermark_pattern must have exactly one group` |
  | `r"(\d{4})(\d{4})\.csv"` | `watermark_pattern must have exactly one group` |
  | `""` | `watermark_pattern must have exactly one group` |

  `r"orders_(\d{8})\.csv"` and `r"(?:orders|sales)_(?P<day>\d{8})\.csv"` are accepted.
  Depends on Q4.
- **AT-006**: Given the arguments on the left, when `DiskReader("/data/landing", "csv", ...)`
  is built with them, then it raises `ValueError` with the message on the right. `P` is
  `r"orders_(\d{8})\.csv"`.

  | `watermark_pattern` | `watermark_value` | Message |
  |---|---|---|
  | `P` | `20261021` | `watermark_value must be a str with watermark_pattern` |
  | `P` | `T0` | `watermark_value must be a str with watermark_pattern` |
  | none | `"20261021"` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `date(2026, 10, 21)` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `20261021` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `datetime(2026, 10, 21, 9, 0)` | `watermark_value must be timezone-aware` |

  With `P`, `""` and `"20261021"` are accepted. Without a pattern, `T0` and
  `datetime(2026, 10, 21, 6, 0, tzinfo=timezone(timedelta(hours=-3)))` are accepted. Depends
  on Q4 and Q6.
- **AT-007**: Given `key` in `path`, `PATHS`, `pathGlobFilter`, `recursivefilelookup`,
  `modifiedBefore` and `ModifiedAfter`, when `DiskReader("/data/landing", "csv",
  options={key: "x"})` is built, then it raises `ValueError` with the message
  `option '<key>' is set by the connector`, with the key as given. Given `key` in `path`,
  `Paths`, `replaceWhere` and `PartitionOverwriteMode`, the same holds for
  `DiskWriter("/data/orders", "parquet", options={key: "x"})`. The reader accepts
  `{"header": True, "inferSchema": "true", "mergeSchema": False}`, and the writer accepts
  `{"compression": "zstd", "header": True}`. Depends on Q11.
- **AT-008**: Given `missing = tmp_path / "missing"`, which does not exist, when these are
  built, then each succeeds, no attribute of `spark` is accessed, and `missing` still does not
  exist:
  - `DiskReader(missing, "csv", spark=spark)`;
  - `DiskReader(missing, "csv", spark=spark, watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020")`;
  - `DiskReader(missing, "csv", spark=spark, watermark_value=T0)`;
  - `DiskWriter(missing, "parquet")`.
- **AT-009**: Then:
  - `repr(DiskReader("/Volumes/main/landing/orders/", "csv"))` is
    `DiskReader(path='/Volumes/main/landing/orders', format='csv')`;
  - `repr(DiskReader(Path("/Volumes/main/landing/orders"), "csv",
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261021"))` is the same;
  - `repr(DiskWriter("/Volumes/main/lake/orders", "parquet"))` is
    `DiskWriter(path='/Volumes/main/lake/orders', format='parquet')`.

### Reading

- **AT-010**: Given `src` holding:
  - the files `b.csv`, `a.csv`, `_SUCCESS`, `_metadata`, `.a.csv.crc` and `c.csv._COPYING_`;
  - a subdirectory `sub` holding `d.csv`, and a subdirectory named `x.csv`;
  - a symbolic link `link.csv` to a file outside `src`, and a symbolic link `broken.csv` to a
    path that does not exist;

  when `reader.read()` runs, then exactly one load is recorded. It has the format `csv`, no
  schema, no options, and the paths `[p("a.csv"), p("b.csv"), p("link.csv")]`, a `list` in
  this order. `read()` returns the very DataFrame that `load()` returned, no attribute of that
  DataFrame is accessed, and `createDataFrame` is not called. Given `src` holding only
  `a.csv`, the paths are `[p("a.csv")]`, still a `list`. Depends on Q9.
- **AT-011**: Given `src` holding `a.csv`, `schema="id INT, name STRING"` and
  `options={"header": True, "sep": ";"}`, when `read()` runs, then the load carries that very
  schema object and exactly the options `header` = `true` and `sep` = `;`. Given instead
  `schema=StructType([StructField("id", IntegerType())])`, the load carries that very object.
- **AT-012**: Given a reader built without `spark`, while `pyspark.sql.SparkSession.active` is
  replaced with a function that raises, then building succeeds. When `active` is then replaced
  with a function that returns the fake session and `read()` runs on `src` holding `a.csv`,
  then the load is recorded on the fake. Given `active` replaced with a function that raises
  `err` when `read()` runs, then `read()` raises `err` itself.
- **AT-013**: Given a reader built while `src` is empty, when `a.csv` is created and `read()`
  runs, then the load's paths are `[p("a.csv")]`. When `b.csv` is then created and `read()`
  runs again, the second load's paths are `[p("a.csv"), p("b.csv")]`, and the first load's
  paths are still `[p("a.csv")]`.
- **AT-014**: Given a reader whose path is `src / "missing"`, which does not exist, when
  `read()` runs, then it raises `FileNotFoundError`. Given a reader whose path is
  `src / "a.csv"`, a file, when `read()` runs, then it raises `NotADirectoryError`. In both
  cases no load is recorded and `createDataFrame` is not called. Depends on Q10.
- **AT-015**: Given `src` empty, and then `src` holding only `_SUCCESS` and `sub/d.csv`, when
  `read()` runs, then no load is recorded and `createDataFrame` is called once. It receives an
  empty list as data and, as schema, a `pyspark.sql.types.StructType` with no fields.
  `read()` returns what `createDataFrame` returned. Given `schema="id INT, name STRING"` in
  addition, the schema passed is that very object. Depends on Q10.
- **AT-016**: Given a fake whose `load()` raises `err = RuntimeError("[PATH_NOT_FOUND]")`,
  when `read()` runs on `src` holding `a.csv`, then it raises `err` itself. Given a fake whose
  `createDataFrame` raises `err`, when `read()` runs on an empty `src`, then it raises `err`
  itself.

### Watermark from the file name

In this section `P` is `r"orders_(\d{8})\.csv"`, and the reader is
`DiskReader(src, "csv", spark=spark, watermark_pattern=P, ...)`.

- **AT-017**: Given `src` holding:
  - `orders_20261019.csv` at `T0 + 2 s`, `orders_20261020.csv` at `T0 + 1 s` and
    `orders_20261021.csv` at `T0`;
  - `customers_20261021.csv`, `orders_latest.csv` and `orders_20261021.csv.bak`;

  when `read()` runs with `watermark_value="20261019"`, then the paths are
  `[p("orders_20261020.csv"), p("orders_20261021.csv")]`. Without `watermark_value`, they are
  the three `orders_<date>.csv` files, sorted by name. Depends on Q4 and Q5.
- **AT-018**: Given the files and the value on the left, when `read()` runs, then the files on
  the right are selected. "None" means no load and one `createDataFrame` call.

  | Pattern | Files in `src` | `watermark_value` | Selected, in the order of the paths |
  |---|---|---|---|
  | `P` | `orders_20261019.csv`, `orders_20261020.csv`, `orders_20261021.csv` | `"20261021"` | none |
  | `P` | the same | `"2026102"` | `orders_20261020.csv`, `orders_20261021.csv` |
  | `P` | the same | `""` | all three |
  | `r"part_(\d+)\.csv"` | `part_9.csv`, `part_10.csv` | `"9"` | none |
  | `r"part_(\d+)\.csv"` | the same | `"1"` | `part_10.csv`, `part_9.csv` |

  Depends on Q4.
- **AT-019**: Given `watermark_pattern=r"(\d{8})\.csv"` and `src` holding
  `orders_20261021.csv`, when `read()` runs, then nothing is selected, because the whole name
  must match. Given `watermark_pattern=r"orders(_\d{8})?\.csv"` and `src` holding `orders.csv`
  and `orders_20261021.csv`, when `read()` runs without `watermark_value`, then the paths are
  `[p("orders_20261021.csv")]`. Given `watermark_pattern=r".*_(\d{8})\.csv"`, which hidden
  names match too, and `src` holding `_orders_20261022.csv`, `.orders_20261022.csv` and
  `orders_20261021.csv`, when `read()` runs without `watermark_value`, then the paths are
  `[p("orders_20261021.csv")]`. Depends on Q4 and Q5.

### Watermark from the modification time

In this section the reader is `DiskReader(src, "csv", spark=spark, ...)`, with no
`watermark_pattern`.

- **AT-020**: Given `src` holding `c.csv` at `T0`, `b.csv` at `T0 + 500 ms` and `a.csv` at
  `T0 + 1 s`, when `read()` runs with the `watermark_value` on the left, then the paths are
  those on the right:

  | `watermark_value` | Paths |
  |---|---|
  | none | `p("a.csv")`, `p("b.csv")`, `p("c.csv")` |
  | `T0 - 1 ms` | `p("a.csv")`, `p("b.csv")`, `p("c.csv")` |
  | `T0` | `p("a.csv")`, `p("b.csv")` |
  | `T0 + 500 ms` | `p("a.csv")` |
  | `T0 + 1 s` | none: no load, one `createDataFrame` call |

  Depends on Q6.
- **AT-021**: Given `src` holding `a.csv` at `T0` plus 500 999 999 nanoseconds, when `read()`
  runs with the `watermark_value` on the left, then `a.csv` is selected or not as on the
  right:

  | `watermark_value` | `a.csv` selected |
  |---|---|
  | `T0 + 500 ms` | no |
  | `T0 + 499 999 µs` | yes |
  | `datetime(2026, 10, 21, 6, 0, 0, 500000, tzinfo=timezone(timedelta(hours=-3)))` | no |
  | `datetime(2026, 10, 21, 6, 0, 0, 499999, tzinfo=timezone(timedelta(hours=-3)))` | yes |

  Depends on Q6.

### Writing

- **AT-022**: When `writer.write(df)` runs, then exactly one save is recorded, with the format
  `parquet`, the mode `append`, no options, and the path `"/data/orders"`, a `str`. The only
  attribute accessed on `df` is `write`. The only attributes accessed on the fake writer are
  `format`, `option` or `options`, `mode` and `save`. `write` returns `None`.
  `writer.write(df, replace=False)` gives the same.
- **AT-023**: When `writer.write(df, replace=True)` runs, then the save is the one of AT-022,
  except that its mode is `overwrite`.
- **AT-024**: Given `DiskWriter("/data/orders/", "csv", options={"header": True,
  "Compression": "gzip"})`, when `write(df)` runs, then the save has the format `csv`, the path
  `"/data/orders"`, and exactly the options `header` = `true` and `Compression` = `gzip`, with
  the key `Compression` in this case. Given `DiskWriter(Path("/data/orders"), "parquet")`, the
  path of the save is the `str` `"/data/orders"`.
- **AT-025**: Given a save that raises `err = RuntimeError("task failed")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, and `save()` was called once.
- **AT-026**: Given `out = tmp_path / "out"` holding `keep.txt`, and `new = tmp_path / "new"`,
  which does not exist, when `DiskWriter(out, "parquet").write(df, replace=True)` and
  `DiskWriter(new, "parquet").write(df)` run with fake DataFrames, then `out` holds exactly
  `keep.txt`, with its content unchanged, and `new` does not exist. The saves record `str(out)`
  and `str(new)`.

### Composition

- **AT-027**: Given `src` holding `a.csv` and
  `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, when it runs, then
  `process.state["orders"]` is the DataFrame that the load returned, and one save is recorded
  on it, with the mode `append`. The test binds the reader to a variable annotated
  `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`, and mypy strict
  accepts both.
- **AT-028**: Given `caplog` capturing every level, when readers and writers are built,
  `read()` runs with a selection that holds files and with an empty one, and `write()` runs
  with and without `replace=True`, then no log record is captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Two connectors, one that reads and one that writes files on disk, on Spark DataFrames.**
   They follow the logic of the PostgreSQL connectors. An ingestion's parameters are
   constructor arguments (ADR 0013). pyspark is imported lazily and is a prerequisite, never a
   dependency. The Spark session is looked up when the connector reads, or passed as `spark`,
   never when the connector is built.
2. **Both connectors receive the file format, on read and on write.** The reader converts
   files in that format into a DataFrame, and the writer converts a DataFrame into files in
   that format, both through Spark's data sources. The job gives the format; the connector
   does not choose it.
3. **Writing is `write(data, *, replace=False)`, as for `PostgresWriter`.** The calling
   callback chooses append or replace at call time; the connector's configuration has no
   mode.
4. **Atomicity comes from Spark's commit protocol and from the format, and from nothing
   else.** The connector has no staging mechanism. The limits are documented as known
   limitations.
5. **Reading with a watermark.** The reader receives the watermark value, the last one
   already ingested, and reads only the files above it, selected before anything is loaded.
   For now each file's watermark is assumed to come from its name; the fallback is the file's
   last-modified time in the directory. Questions 4 to 8 cover the details.

Proposed by this spec, and accepted by the maintainer with its approval on 2026-10-23:

6. **Building does nothing but validate.** It imports nothing, lists nothing and touches no
   session. Each `read()` looks up the session and then lists the directory again, so that a
   reader built once serves every run that calls it.
7. **Candidates follow Spark's rule for hidden and in-progress files** (`_`, `.`,
   `._COPYING_`), and also exclude `_metadata` and `_common_metadata`. The reader and Spark
   then agree on what a data file is, and producers have a documented way to hide files they
   are still writing.
8. **The paths given to Spark are sorted by file name, and always passed as a list.** A
   deterministic plan for the same directory, and one shape for one file or many.
9. **The comparison is strictly greater,** as in the PostgreSQL reader (its decision 23),
   because the value is the last watermark already ingested.
10. **Paths are normalised with `pathlib` and kept as `str(Path(path))`.** `repr` shows the
    path and the format, which identify the connector.
11. **No default options.** The PostgreSQL connectors set driver defaults that pgJDBC needs.
    Spark's file sources need none, and a default such as `header` would be a choice about
    the format. Option values reach Spark as given. Reserved keys raise the PostgreSQL
    connectors' message.
12. **The writer is one call chain on `data.write`.** It takes no session (as in the
    PostgreSQL writer, its decision 18). It has no column check, unlike `PostgresWriter`,
    which needs the columns to build its SQL; Spark's own error for a DataFrame without
    columns is explicit. It creates, lists and deletes nothing itself.
13. **Errors propagate unchanged, and the connectors write no log lines,** as in the
    PostgreSQL connectors (their decision 16): a connector does not know the run id that
    CONTRIBUTING requires on every line.
14. **Spark's `modifiedAfter` option is not used for modification-time watermarks.** It
    filters inside Spark's listing of a directory, so the selection would not be frozen. Its
    documented format stops at the second, in the session's time zone. It cannot serve
    name watermarks either.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept, and they
  apply a format they are given without deciding it (see
  [Fit with the philosophy](#fit-with-the-philosophy)). Question 16 asks whether the
  maintainer wants an ADR that records this reading of "does not decide ... format".
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged, and their
  payload stays open. Like the PostgreSQL connectors, these lean towards batch: one selection
  per `read()`, a watermark fixed when the reader is built, and a write that needs the whole
  DataFrame. A streaming file source, such as Structured Streaming's file source or Auto
  Loader, tracks the files it processed in a checkpoint and would be a different connector.
  This belongs in the "What leans towards batch" list of
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md): an amendment, not a new ADR.
- **ADR 0013 applies unchanged.** `path`, `format`, `watermark_pattern` and `schema` become
  configuration keys by its convention, and `watermark_value` is computed by the job, as for
  PostgreSQL. ADR 0013 records that the writer's `table_name` names the target while the
  configuration's names the source; the writer's `path` has the same consequence. Selecting
  files before loading is how this reader "filters inside the source". The doc-keeper may add
  both points to ADR 0013's consequences.
- **ADR 0002 applies unchanged.** There is no new extra. Engine plugins that a format needs,
  such as Delta Lake, are prerequisites like pyspark (rule 4 of the extras pattern).

## Open questions

None open. On 2026-10-23 the maintainer took the recommended option of every question, so the
rest of the spec stands as written. The options are kept below as the record of what was
weighed.

1. **Class names, module and slug.**
   - (a) `DiskReader` and `DiskWriter` in `connector/local.py`, slug `disk_connectors`. "Disk"
     is the maintainer's word. CONTRIBUTING names modules after the medium, and `local.py`
     is already the module for local disks and volumes, next to `JsonLinesWriter`.
   - (b) The same names in a new `connector/disk.py`. That gives two modules for one medium.
   - (c) `FileReader` and `FileWriter`, slug `file_connectors`. "File" stays accurate if cloud
     URIs are ever accepted (question 3).
   - (d) `SparkFileReader` and `SparkFileWriter`. They name the engine and leave the plain
     names for file connectors in plain Python; the PostgreSQL connectors do not name it.

   **Recommended: (a).** Also, the name of the format argument: `format`, which is Spark's
   word and the natural configuration key, or `file_format`. `format` shadows a builtin, so
   ruff's rule A002 needs a `noqa` in the implementation or a `builtins-ignorelist` entry in
   `pyproject.toml`. **Recommended: `format`.**
2. **Which formats are accepted, and is Delta in scope?**
   - Formats: (a) any non-empty name, passed to Spark, which fails on an unknown one when it
     reads or writes; (b) a closed list checked when the connector is built, with a clear
     error, which needs upkeep and encodes an opinion. **Recommended: (a).**
   - Delta:
     - (i) The writer accepts it and the reader rejects it when built. A Delta table cannot be
       read file by file, because its log decides which Parquet files are live, while its
       writes are atomic, which is the point of decision 4. It needs `delta-spark` on
       open-source Spark. On Databricks, Unity Catalog tables, not volumes, are the usual
       home of tabular data.
     - (ii) Out of scope for both connectors.
     - (iii) In scope for both: a Delta read loads the table's directory and rejects a
       watermark.

     **Recommended: (i).** In every case, other table formats (Iceberg, Hudi) are not
     detected, only documented.
3. **Which paths are accepted?**
   - (a) Absolute local paths only, including Databricks volumes, which the driver's Python
     process lists with the standard library. URIs are rejected. On Databricks, only
     `/Volumes/...` works. Simple, and testable with real files and no JVM.
   - (b) As (a), and `file:` URIs as well.
   - (c) Any path or URI that Spark accepts (`s3://`, `abfss://`, `gs://`, `dbfs:/`), listed
     through Spark's `binaryFile` source, selecting only `path` and `modificationTime`. It
     works without a FUSE mount and in every access mode, but costs a Spark job per `read()`,
     and its selection can only be tested with a JVM or with fakes of that listing.

   Relative paths are rejected under every option. **Recommended: (a);** cloud storage would
   be another connector or a later spec. Also: is Databricks serverless compute in scope?
   **Recommended: no claim; documented as not tested.**
4. **How is a watermark taken from a file name?**
   - (a) `watermark_pattern`, a regular expression that the whole file name must match, with
     exactly one capturing group. The captured text is the watermark, compared as text with
     a `str` `watermark_value`. It needs watermarks that are fixed-width and most significant
     first. It is easy to keep in a JSON configuration, and the reader converts nothing.
   - (b) As (a), with a `watermark_type` (`str`, `int`, `date`, `datetime`) and a `strptime`
     format, so that values compare as values. That means more arguments and more
     validation, and handles unpadded numbers and dates such as `21-10-2026`.
   - (c) A callable, `watermark_of(name)`, which returns a comparable value. The most freedom,
     but it cannot live in a JSON configuration (ADR 0013), and every job writes its own
     parsing.
   - (d) A glob with a placeholder, such as `orders_{watermark}.csv`. A small language of
     ingestio's own.

   **Recommended: (a).** Also the argument's name: `watermark_pattern` (recommended), or
   `file_pattern` / `name_pattern`.
5. **What happens to a file whose name does not match the pattern?**
   - (a) It is not read. The pattern then also filters a directory that several feeds share.
     The risk: if a producer renames its files, they are silently never read.
   - (b) `read()` raises `ValueError` and names the file, so that no file goes unread
     silently. A directory must then hold only the matching files, apart from hidden ones.

   **Recommended: (a).**
6. **When does the fallback to the last-modified time apply, and how precise is it?**
   - (a) Per reader: without `watermark_pattern`, every file's watermark is its modification
     time, and `watermark_value` is a timezone-aware datetime. Naive datetimes are rejected,
     as in the PostgreSQL reader.
   - (b) Per file: a file whose name does not match uses its modification time. Text and
     instants cannot be compared with one value, so this needs two values.
   - (c) Per reader, with a pattern that has no group serving as a filter in modification-time
     mode. It handles shared directories in both modes, but the mode then depends on how many
     groups the pattern has, which is implicit.
   - (d) No fallback in this spec.

   **Recommended: (a).** Precision: (i) truncated to milliseconds, the precision that Hadoop
   and Spark's `_metadata.file_modification_time` report, so that a job that takes its next
   watermark from the DataFrame never reads its last file again; (ii) microseconds, Python's
   `datetime` precision; (iii) whatever the file system gives. **Recommended: (i).**
7. **Does the reader take a watermark column at all?**
   - (a) No. The unit of an incremental read is the file. Filtering rows inside files is
     another feature.
   - (b) Yes, `watermark_column` as well, filtered by Spark (pushed down into Parquet and ORC),
     either after the selection of files or instead of it.

   **Recommended: (a).**
8. **Is the selection frozen by `read()`, and is it exposed?**
   - (a) Frozen and not exposed. `read()` lists once and gives Spark the list of selected
     files, never the directory. The job takes its next watermark from the DataFrame's hidden
     `_metadata` column. This matches the PostgreSQL reader's decision 10.
   - (b) Frozen and exposed, such as the selected files or the highest watermark, as a
     documented attribute after `read()`. That attribute is public API, and the connector
     becomes stateful.
   - (c) Not frozen: give Spark the directory with `modifiedAfter`. This works only for
     modification times, and Spark Connect may list the directory again at each action.

   **Recommended: (a).**
9. **What does the reader see in `path`?**
   - (a) The files directly inside it, for every read. Subdirectories are ignored.
   - (b) Every file under it, at any depth, with the pattern applied to the file name and the
     hidden-name rule to every part of the path.
   - (c) Without a watermark value, the directory as Spark sees it, `load(path)`, with
     partition discovery and nested layouts, and not frozen; (a) otherwise. The first run of
     an incremental job would then read other files than the later runs.

   **Recommended: (a).**
10. **A missing path, a path that is a file, an empty selection, and a `schema` argument.**
    - Missing path: (a) `FileNotFoundError` propagates, so a typo fails the run; (b) treat it
      as empty. **Recommended: (a).**
    - Path that is a file: (a) `NotADirectoryError` propagates; (b) read that one file.
      **Recommended: (a).**
    - Empty directory, or no file above the watermark:
      - (a) An empty DataFrame, with the columns of `schema`, or no columns without it. A
        quiet run then does not fail, and a callback can cancel the step.
      - (b) Raise unless `schema` is given, because the reader cannot know the columns.
      - (c) Always raise.

      **Recommended: (a).**
    - `schema`: (a) an argument, `StructType | str`, passed to Spark's `schema()` and used for
      an empty selection. It also spares CSV and JSON a pass of schema inference. (b) No
      argument. **Recommended: (a).**
11. **Spark options and reserved keys.**
    - (a) Passed through with no defaults. Reserved for both connectors: `path` and `paths`.
      For the reader: `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore` and
      `modifiedAfter`, because the reader selects the files itself. For the writer:
      `replaceWhere` and `partitionOverwriteMode`, because they turn a replace into a partial
      replace, which is a write strategy. The message is the PostgreSQL one.
    - (b) Pass everything through; Spark's filters then apply on top of the reader's
      selection, and its errors propagate.
    - (c) A closed list of accepted options.

    **Recommended: (a).**
12. **Does the writer partition its output?**
    - (a) No. The output directory is flat, as the reader reads it.
    - (b) A `partition_by` argument, passed to `partitionBy`. The writer would force a static
      overwrite, so that `replace=True` still replaces everything, and the reader would need
      recursion to read the output back (question 9).

    **Recommended: (a).**
13. **What happens to `JsonLinesWriter` and the Known divergence?**
    - (a) Leave both alone: `specs/append_only_callback.md` covers them. The doc-keeper only
      adds `DiskWriter` next to `PostgresWriter` in that paragraph: `WriteTarget` appends to
      both.
    - (b) Settle its fate in this spec.

    **Recommended: (a).**
14. **Is an extra needed?**
    - (a) No. No new package is needed, and pyspark is a prerequisite.
    - (b) An `ingestio[spark]` extra holding pyspark. This contradicts rule 4 of the extras
      pattern: installing pyspark over Databricks Runtime breaks the runtime.

    **Recommended: (a).**
15. **How do the tests run?** The criteria run everywhere, with a fake session, fake
    DataFrames and real files under `tmp_path`, with no JVM. pyspark is in the `dev` group,
    so the fakes can use its types. For the integration tests against a local Spark session,
    which need Java 17 or 21 with the locked pyspark 4.2:
    - (a) Opt in through an environment variable, `INGESTIO_SPARK_TESTS=1`, as
      `INGESTIO_POSTGRES_URL` does for PostgreSQL. CI is unaffected until the maintainer
      enables it.
    - (b) Run them whenever `java` is found. CI may then fail on a Java older than 17.
    - (c) No integration tests.

    **Recommended: (a).** Delta in the integration tests would add `delta-spark` to the `dev`
    group, in a version that matches pyspark 4.2. **Recommended: not in this spec.**
16. **Should an ADR record how the philosophy's "format" applies?**
    - (a) No: "does not decide ... format" already allows a connector that applies the format
      it is given. The reasoning stays in this spec.
    - (b) An ADR that clarifies the sentence, for instance: a connector "applies the format it
      is given and decides none". Future connectors that take a format would cite it.

    **Recommended: (a).**

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions: `DiskReader` lists a directory on the driver, selects the files above the watermark (from the file name through a pattern, or from the last-modified time) and loads only them, as a frozen list; `DiskWriter` maps `write(data, *, replace=False)` to Spark's `append` and `overwrite`, with no staging and its limits documented. 28 acceptance criteria, 7 integration scenarios, 14 decisions (5 by the maintainer, 9 proposed), 16 open questions. |
| 2026-10-23 | approval | approved | Approved by the maintainer with the recommended option of all 16 questions: `DiskReader`/`DiskWriter` in `connector/local.py` with a `format` argument (`# noqa: A002`), the modification-time fallback per reader, Delta accepted by the writer only, and every other recommendation. No revision was needed, so the planner did not run again. The main session marked the questions as answered and the proposed decisions as accepted, without changing the substance. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_disk_connectors.py`: tests for AT-001 to AT-028 (106 cases) with a fake session, fake DataFrames and writers recording to one event log, and real files under `tmp_path`, plus one test of `os.stat` following links and the 7 integration scenarios, marked `integration` and skipped unless `INGESTIO_SPARK_TESTS=1` (written blind: the only JDK here is Java 27, on which Spark 4.2 does not start). Collection fails as expected with `ImportError: cannot import name 'DiskReader' from 'ingestio.connector.all'`. The rest of the suite passes (212 passed, 8 skipped), ruff passes, and mypy reports only the two missing names, all checked by the main session. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `DiskReader` and `DiskWriter` in `docs/concepts/connectors.md` (API, the six-step read, watermarks from the name or the modification time, frozen and empty selections, writing and its atomicity, environments, limitations) and added `DiskWriter` to the known divergence; amended ADR 0012 (leans towards batch) and ADR 0013 (the writer's `path` names the target; selecting files is how the reader filters inside the source); updated `docs/index.md` and the architecture KB. Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable blocks, with the expected `ImportError` of the missing classes. |
| 2026-10-23 | implementation | implemented | The implementer added `DiskReader` and `DiskWriter` to `src/ingestio/connector/local.py`, exported from `connector/all.py`, with no new dependency: the reader lists the directory with `os.scandir`, selects by name pattern or by modification time in whole milliseconds with integer arithmetic, and loads exactly that list of files; the writer is one `format().options().mode().save()` chain; `format` carries `# noqa: A002`. Checked by the main session: 322 passed, 15 skipped (the 7 disk and 8 PostgreSQL integration tests), ruff, format and mypy clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-028 covered by 108 passing tests, the full suite with the documentation examples 322 passed and 15 skipped (the 7 disk and 8 PostgreSQL integration tests), ruff, format, mypy strict and `uv lock --check` clean; code, documentation, philosophy, constraints and write scopes match. The 7 Spark integration tests stay unverified: the only JDK here is Java 27. The main session then updated two outdated passages of `README.md` (the list of shipped connectors, and the Spark connectors and requirements link of "Structuring a job"). |
| 2026-10-23 | amendment | validated | AT-027 superseded by [specs/config_connectors.md](config_connectors.md), decision 17, approved by the maintainer on 2026-10-23: `ReadSource` and `WriteTarget` no longer take a connector, so the composition this criterion describes is covered by that spec's criteria. The criterion's test goes in the tests stage of that spec; this spec keeps its status. |
