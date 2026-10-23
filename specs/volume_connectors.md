# Spec: Databricks volume reader and writer connectors

- **Status:** validated
- **Slug:** `volume_connectors`. The spec is `specs/volume_connectors.md` and its tests are
  `tests/test_volume_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/s3_connectors.md](s3_connectors.md), the model of this spec,
  [specs/disk_connectors.md](disk_connectors.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [specs/unity_catalog_connectors.md](unity_catalog_connectors.md),
  [backlog idea 6](../docs/backlog.md#6-databricks-volumes-connector),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for files in a directory of a Databricks Unity Catalog volume, built on Spark's
file data sources, with the contract of `DiskReader` and `DiskWriter`. `VolumeReader` lists the
files directly inside the directory through the Files API of the Databricks SDK, on the driver,
selects those above the job's watermark before anything is loaded, and returns a Spark DataFrame
over exactly those files, given to Spark as `/Volumes/...` paths. `VolumeWriter` writes a Spark
DataFrame into a directory of a volume, appending or replacing as the calling callback chooses
when it calls `write`. Spark writes the `/Volumes/...` path itself, so the writer needs no SDK. A
file's watermark comes from its name, through a pattern the job gives, or else from its
`last_modified` time, to the second.

The listing does not go through the volume's FUSE mount, so the reader works wherever a Spark
session reaches the workspace: on Databricks compute, and from outside it through Databricks
Connect. The Databricks SDK does control work only: it lists. It goes into a new extra,
`ingestio[databricks]`, and is needed only when the job does not pass a `client` of its own.
Spark's access to the volume is Unity Catalog's, through the volume's privileges.

This spec mirrors [specs/s3_connectors.md](s3_connectors.md) wherever a volume behaves as an S3
prefix does, with the same messages, and fills in its section
[Differences from the disk connectors](#differences-from-the-disk-connectors) for volumes.

The choices that are open questions are marked **(Q n)**. The spec is written with the
recommended option of each one.

## Motivation

- **The request.** The maintainer asked for five connectors on 2026-10-23, each with a reader
  and a writer: S3, GCS, Unity Catalog, Databricks Volumes and BigQuery, one spec per system.
  This is the Volumes one ([backlog idea 6](../docs/backlog.md#6-databricks-volumes-connector)).
- **Read.** Producers drop files into a directory of a volume. A job must read only the files it
  has not ingested yet, and must pick them before Spark loads anything. For a volume, as for a
  directory on disk, the unit the source can filter is the file, so selecting files before
  loading is how this reader filters "inside the source", as
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md) requires: Spark never
  opens a file that was not selected.
- **What exists does not fit everywhere.** `DiskReader` already reads `/Volumes/...` on
  Databricks compute, but it lists the directory with `os.scandir`, in the driver's own Python
  process, through the volume's FUSE mount. A process outside Databricks, such as a job or a
  test that drives a remote cluster through Databricks Connect, has no `/Volumes` to list, and
  on Windows `/Volumes/...` is not even an absolute path. `spark.read.load(directory)` reads
  every file. Spark's `modifiedAfter` option filters inside Spark's own listing, so the
  selection is not frozen, and it cannot serve name watermarks. Databricks Auto Loader keeps the
  files it processed in a checkpoint: it is streaming, which is deferred
  ([ADR 0012](../docs/decisions/0012-streaming-deferred.md)).
- **Why the Files API lists.** `WorkspaceClient().files.list_directory_contents(...)` gives the
  driver the same view of a volume directory that `os.scandir` gives `DiskReader` through FUSE,
  wherever the process runs, in a few requests, without a Spark job per read, and governed by
  the volume's own privileges. The disk spec weighed and rejected a listing through Spark's
  `binaryFile` source (its question 3, option c).
- **Write.** Spark writes `/Volumes/...` paths itself, on Databricks compute and through
  Databricks Connect alike. The writer is the `DiskWriter` call chain with the volume's path,
  with the same limits on atomicity for plain files; Delta is atomic through its log. What it
  adds to `DiskWriter("/Volumes/...")` is that it names its location the way the reader does,
  by the volume and a directory inside it, and that it works from any operating system. It
  exists because decision 1 gives every system a reader and a writer.

## Fit with the philosophy

- **Concept: connector.** `VolumeReader` satisfies `Reader[DataFrame]` and `VolumeWriter`
  satisfies `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). A Unity Catalog volume is the
  external system: a file system that the workspace governs. `write(data, *, replace=False)`
  adds a keyword-only argument with a default, so `write(data)` stays a valid call and the
  method still matches the protocol. `ReadSource` and `WriteTarget` use both connectors
  unchanged.
- **The client is not a third concept.** It is the Databricks SDK's own `WorkspaceClient`,
  built by the job or by the SDK's defaults, and passed in like `spark`. It carries the
  workspace's host and the credentials, which ADR 0013 keeps out of the job configuration. The
  reader calls one operation on it, the listing. Nobody extends it, and it performs no action
  of the pipeline.
- **Format: given, not decided.** As the disk spec explains in its own "Fit with the
  philosophy", applying the format the job names is how a file connector talks to its system.
  The format is a required argument with no default; the connector never infers it, adds no
  column, casts nothing and filters no rows.
- **Write strategy.** Append and replace are the two primitive ways Spark puts a batch into a
  directory, the `append` and `overwrite` save modes. The writer offers both through one method
  and picks neither: the calling callback chooses at call time. Options that would turn a
  replace into a partial replace are reserved.
- **Architecture.** The connectors create no catalog, schema, volume or table, register
  nothing, lay out no partitions and configure nothing in Spark or in the workspace. Spark's
  access to the volume is Unity Catalog's.
- **The reader decides no strategy.** Whether a run is incremental, from which value, and where
  a file's watermark comes from, are the job's decisions. The reader turns them into a
  selection of files.
- **Essential by default.** Nothing is caught to carry on: every failure propagates out of
  `read()` or `write()` and fails the callback that called them, a missing directory included
  **(Q4)**.
- **Simplicity.** Two small classes in one module. No base class, no registry, no staging, no
  new protocol, no new callback. The reader's own logic is one listing and a comparison, with
  the selection rules of `DiskReader`.
- **Compose, do not impose.** The job builds the connectors from its configuration, and builds
  its own client when it needs one. The connector itself reads no file, no environment variable
  and no secret store; the default client follows the SDK's own lookup, under the consequence
  that the S3 spec adds to ADR 0013 (see [ADRs](#adrs)).
- **No dependencies in the core.** databricks-sdk goes into the `databricks` extra and is
  imported only inside the functions that use it. pyspark stays a prerequisite. `ingestio` and
  `ingestio.connector.all` import without either.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `VolumeReader`: reads of the files directly inside one directory of one volume, all of them
  or only those above a watermark, selected before loading and frozen by `read()`. The
  watermark comes from the file's name through a pattern, or from its `last_modified` time, to
  the second;
- the `client` argument, and the default client created at each read;
- `VolumeWriter`: append (`write(data)`) and replace (`write(data, replace=True)`) through
  Spark's file writers, straight to the volume's path;
- the rules for volume names, paths inside a volume, formats, watermark values and options,
  and the reserved options;
- the `databricks` extra, and what the main session adds to `pyproject.toml` and `uv.lock`;
- the documentation of what a write guarantees, of the environments and of the limits;
- the test doubles, and integration tests that run only when the environment provides a
  workspace and a volume.

Out of scope:

- the job configuration file and recipes, as in the disk spec;
- computing, persisting or advancing the watermark, and exposing which files `read()`
  selected;
- recursive listings, subdirectories and partitioned input layouts, and partitioned output
  (`partitionBy`);
- reading table formats (Delta, Iceberg, Hudi), writing to catalog tables (the
  [Unity Catalog connectors](unity_catalog_connectors.md)), and any DDL;
- creating catalogs, schemas or volumes, and creating or deleting directories or files through
  the SDK;
- a watermark column, that is filtering rows inside the files;
- streaming sources: Auto Loader (`cloudFiles`), Structured Streaming's file source,
  checkpoints; and starting a job when files arrive, which is the scheduler's concern;
- a staging mechanism, or any atomicity beyond Spark's and the format's;
- moving, archiving or deleting source files; uploading or downloading files with the SDK;
- reading a single file given as `path`;
- DBFS paths (`dbfs:/...` other than volumes, `/dbfs/...`), workspace files (`/Workspace/...`),
  and cloud URIs of external locations, which the S3 and GCS connectors cover;
- open-source Spark, and engines other than Spark;
- an account-level client, a `databricks.sdk.core.Config` argument, or credentials as connector
  arguments: the job builds a `WorkspaceClient`;
- Spark's other save modes, `ignore` and `errorifexists`;
- changes to CI, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/volume.py` **(Q1)**, exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import VolumeReader, VolumeWriter
```

```python
class VolumeReader:
    """Reads the files of a directory in a Unity Catalog volume into a Spark DataFrame.

    Args:
        volume: The volume, by its three-part name, `catalog.schema.volume`. The
            configuration key of the same name.
        path: Directory that holds the files, relative to the volume, such as
            `landing/orders`, or `""` for the volume's root. The configuration key of the
            same name.
        format: Name of the Spark data source the files are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a file matches, with one
            group that captures the file's watermark. Without it, a file's watermark is its
            `last_modified` time, to the second. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only files above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the files, as a `StructType` or a DDL string. Spark infers it when
            omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: Databricks SDK client that lists the directory, such as
            `WorkspaceClient(profile="prod")`. When omitted, each read creates
            `WorkspaceClient()`, with the SDK's default authentication, which needs the
            `databricks` extra.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        volume: str,
        path: str,
        format: str,
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: WorkspaceClient | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the files the job needs."""
```

```python
class VolumeWriter:
    """Writes a Spark DataFrame as files in a directory of a Unity Catalog volume.

    Args:
        volume: The volume, by its three-part name, `catalog.schema.volume`.
        path: Directory to write into, relative to the volume, such as `curated/orders`.
            Never the volume's root.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        volume: str,
        path: str,
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
  `pyspark.sql.SparkSession` and `pyspark.sql.types.StructType`. `datetime` is
  `datetime.datetime`, and `Mapping` is `collections.abc.Mapping`.
- `WorkspaceClient` is `databricks.sdk.WorkspaceClient` **(Q6)**. It is imported under
  `TYPE_CHECKING` only. At run time the reader reads only the attribute `files` of the client
  and calls only `list_directory_contents` on it.
- `volume` and `path` are `str`. `os.PathLike` is not accepted for `path`: `pathlib` would
  collapse the empty and `.` segments that this spec rejects, and on Windows it would turn `/`
  into `\`.
- `volume`, `path` and `format` are required, so they are positional, in the order of the
  maintainer's sketch; every option is keyword-only **(Q2)**. `format` shadows a builtin, which
  ruff's rule A002 reports: `# noqa: A002`, as in `connector/local.py`.
- `schema` keeps the meaning it has on `DiskReader` and `S3Reader`, the Spark schema of the
  files. The Unity Catalog schema is the middle part of `volume` **(Q2)**.
- `VolumeWriter` has a single write method, `write`. `replace` is keyword-only and defaults to
  `False`, so `VolumeWriter.write` satisfies `Writer[DataFrame]` structurally. `WriteTarget`,
  which calls `write(data)`, always appends.
- New installable extra: `ingestio[databricks]`.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL, disk and S3 connectors, and everything exported by
  `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a job that runs outside Databricks and drives a cluster through Databricks Connect,
not a runnable example. The job reads its configuration, keeps its own watermark, builds its
own client, and advances the watermark from what it read.

```python
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
# unix_seconds truncates to the second, as the reader does.
orders = process.state["orders"]
if not orders.isEmpty():
    seconds = orders.select(F.max(F.unix_seconds("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime.fromtimestamp(seconds, UTC)
```

On a Databricks cluster the same job usually passes neither `client` nor `spark`: the reader
finds the active session, and `WorkspaceClient()` authenticates as the notebook or the job does.

## Behaviour

### Building the connectors

Building a connector validates its arguments and does nothing else, with one exception: a
reader built without `client` checks that databricks-sdk can be imported (see
[The client and the extra](#the-client-and-the-extra)). Building creates no client, accesses no
attribute of `client` or `spark`, and touches neither Spark nor the workspace. Validation runs
in the order of the arguments, and the import check comes last.

#### The volume (Q2)

Both connectors take `volume` apart the same way, as the Unity Catalog connectors take
`table_name` apart:

- `volume` must be a `str` that splits on `.` into exactly three parts, the catalog, the schema
  and the volume, none of them empty and none containing `/`. Anything else, every non-`str`
  included, raises `ValueError("volume must be 'catalog.schema.volume'")`. This rejects `""`,
  `"main"`, `"main.landing"`, `"a.b.c.d"`, `".landing.raw"`, `"main..raw"`, `"main.landing."`,
  `"main.landing.r/aw"`, and a whole path such as `"/Volumes/main/landing/raw"`.
- The parts are plain names, never quoted by the job: a part cannot contain a `.`, and
  backticks are not removed. They are not validated further: Unity Catalog rejects a name it
  does not know when the reader lists, and Spark when it reads or writes.
- Names are kept as written, never lowercased. How Unity Catalog matches the case of a name in
  a `/Volumes/...` path is its own rule.

#### The path inside the volume (Q2)

Both connectors take `path` apart the same way:

1. `path` must be a `str` that does not start with `/`. Anything else raises
   `ValueError("path must be a str relative to the volume")`. This rejects `"/orders"`, `"/"`,
   a whole `"/Volumes/main/landing/raw/orders"` given by mistake, and every non-`str`, such as
   a `pathlib.Path`.
2. Every trailing `/` is removed: `"orders/"` and `"orders//"` become `"orders"`.
3. When what remains is not empty and one of its `/`-separated segments is empty, `.` or `..`,
   then `ValueError("path must not contain empty, '.' or '..' segments")`, the message of the
   S3 connectors. Spark's paths collapse `//` and resolve `.` and `..`, which the Files API may
   do differently, and `..` would leave the volume, so the listing and Spark could name
   different directories.
4. The empty path is the volume's root. The reader accepts it. The **writer** rejects it with
   `ValueError("path must name a directory inside the volume")`, because `replace=True` would
   remove everything the volume holds, as the S3 writer rejects the bucket root.

#### The location

The connectors keep the **location**, `/Volumes/<catalog>/<schema>/<volume>`, followed by
`/<path>` when the path is not empty. `VolumeReader("main.landing.raw", "orders/", "csv")` has
the location `/Volumes/main/landing/raw/orders`, and `VolumeReader("main.landing.raw", "",
"csv")` has `/Volumes/main/landing/raw`. The location is what the reader lists and what Spark
receives, without a scheme **(Q5)**.

#### Formats

`format` is passed to Spark as given. An empty `format` raises
`ValueError("format must not be empty")`. Spark looks the format up when it reads or writes, and
an unknown one fails there. The reader rejects `delta`, compared case-insensitively, with
`ValueError("format '<format>' cannot be read file by file")`, where `<format>` is the value as
written: a Delta table's log decides which of its files hold live data. The writer accepts
`delta`.

#### Watermark values

`watermark_value` is "given" when it is not `None`; `""` is a value. Its type depends on where
the watermark comes from:

| `watermark_pattern` | Accepted `watermark_value` |
|---|---|
| given | `str` |
| not given | timezone-aware `datetime.datetime` |

The reader validates them when it is built, with the messages of `DiskReader`:

| Case | `ValueError` message |
|---|---|
| `watermark_pattern` is not a valid regular expression | `invalid watermark_pattern: <error>` |
| `watermark_pattern` has no group, or more than one | `watermark_pattern must have exactly one group` |
| `watermark_pattern` given, `watermark_value` given and not a `str` | `watermark_value must be a str with watermark_pattern` |
| `watermark_pattern` not given, `watermark_value` given and not a `datetime.datetime` | `watermark_value must be a datetime without watermark_pattern` |
| a naive `datetime.datetime` | `watermark_value must be timezone-aware` |

- `<error>` is the message of the `re.error` that `re.compile` raised, which is the
  `__cause__` of the `ValueError`.
- A group is a capturing group, named or not, as `re.compile(pattern).groups` counts it.
  `(?:...)` does not count, and the empty pattern has no group.
- Without a pattern, a `datetime.date` that is not a `datetime.datetime` is rejected, and so
  are `int` and `bool`.
- A datetime is naive when its `tzinfo` is `None` or its `utcoffset()` is `None`.

#### Options

- The connectors set no option of their own. `options` reaches Spark as given, and Spark turns
  values into strings, booleans into `true` and `false`.
- Reserved keys, compared case-insensitively, raise
  `ValueError("option '<key>' is set by the connector")` when the connector is built, with
  `<key>` as the user wrote it. They are the disk connectors' keys, for the same reasons:

  | Connector | Reserved keys |
  |---|---|
  | both | `path`, `paths` |
  | `VolumeReader` | `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`, `modifiedAfter` |
  | `VolumeWriter` | `replaceWhere`, `partitionOverwriteMode` |

#### The client and the extra

- `client` is not validated, and building never accesses it.
- When `client` is `None`, building imports `databricks.sdk`, only to check that the extra is
  installed. When that import fails, the reader raises
  `ImportError("VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'")`,
  chained to the original error. This is rule 3 of the
  [pattern for extras](../docs/concepts/connectors.md#extras).
- When `client` is given, the reader never imports `databricks.sdk`, neither when it is built
  nor when it reads: the job's client is all it needs.
- The writer never imports `databricks.sdk` and has no client.

#### `repr`

`repr` shows the volume, the normalised path and the format, never the client:

- `VolumeReader(volume='main.landing.raw', path='orders', format='csv')`
- `VolumeReader(volume='main.landing.raw', path='', format='json')`
- `VolumeWriter(volume='main.lake.curated', path='orders', format='parquet')`

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps, in order:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()`, at each `read()`. The reader never creates a session.
   When none is active, PySpark's error propagates, and nothing is listed.
2. **Client.** The reader uses `client` when it is given, as it is, for every read. Otherwise it
   calls `databricks.sdk.WorkspaceClient()`, with no argument, looked up as an attribute of the
   `databricks.sdk` module at each `read()`. Each such read creates a new client; the reader
   keeps none. The SDK resolves its configuration and credentials while the client is built,
   which may send requests: that happens at read time, and its errors propagate.
3. **Listing.** See [The listing](#the-listing).
4. **Candidates.** See [Candidates](#candidates).
5. **Watermarks.** The reader takes each candidate's watermark, as described in
   [The watermark of a file](#the-watermark-of-a-file).
6. **Selection.** The reader selects the candidates whose watermark is strictly greater than
   `watermark_value`, or every candidate when `watermark_value` is `None`.
7. **Load.** The reader loads the selection, or builds an empty DataFrame when the selection is
   empty.

#### The listing

The reader makes one listing per `read()`, on the driver:

```text
client.files.list_directory_contents(directory_path=<location>)
```

- This one keyword argument, exactly, and no other argument: no `page_size` and no
  `page_token`.
- The call returns an iterable of `databricks.sdk.service.files.DirectoryEntry`. The reader
  iterates over it once, in order, to its end, before anything is loaded. The SDK sends each
  request as the iteration needs it, and follows the API's `next_page_token` itself.
- From each entry the reader takes `is_directory` and `name`, and, without a pattern, the
  `last_modified` of the candidates. It does not use the entry's `path` or `file_size`.
  Databricks documents `name`, `is_directory` and `last_modified` for every entry, and the
  reader takes them as the SDK returns them; an entry that lacks `name`, or a file that lacks
  `last_modified` in a read without a pattern, is outside this spec.
- The Files API lists only the direct contents of the directory. A subdirectory is one entry,
  whose contents are never listed. The listing is flat.
- The reader calls no other operation: it downloads, opens and inspects no file, and asks for
  no metadata. It never gives the client to Spark. Retries and timeouts are the SDK's own
  defaults, which the connector does not change.

Databricks documents that listing a path where no directory exists fails with HTTP 404, which
covers a missing directory and a path that names a file. The SDK raises
`databricks.sdk.errors.NotFound`, which propagates unchanged **(Q4)**. A missing volume fails
in the same way, or with `PermissionDenied`, as Unity Catalog answers. A listing that Unity
Catalog denies fails with the SDK's `PermissionDenied`, and one whose credentials are rejected
with its `Unauthenticated`. An empty directory gives an empty listing, which is an empty
selection.

#### Candidates

An entry is a candidate when:

- its `is_directory` is not `True`. `False` and `None` both mean a file: Databricks's JSON may
  leave out a boolean that is false, and the SDK then gives `None`;
- its `name` starts with neither `_` nor `.` and does not end with `._COPYING_`. This is Spark's
  rule for hidden and in-progress files, as in `DiskReader`. It leaves out `_SUCCESS`,
  `_metadata`, `_common_metadata`, `.crc` files, and the `_started_*` and `_committed_*`
  markers that Databricks's commit protocol writes.

The file's size plays no part: an empty file is a candidate, as in `DiskReader`.

#### The watermark of a file

- **With `watermark_pattern`.** As in `DiskReader`: the pattern is matched against the whole
  `name`, not the path, with `re.fullmatch`, and the watermark is the text its group captured. A
  candidate is not read when its name does not match, or when its group took no part in the
  match. Watermarks compare as Python strings, code point by code point, and are never
  converted: `"9"` is above `"10"`.
- **Without `watermark_pattern` (Q3).** The watermark is the entry's `last_modified`, an `int`
  of milliseconds since the Unix epoch, **truncated to whole seconds** (`last_modified // 1000`)
  and taken as a UTC instant, exactly and without floating point. It is compared with
  `watermark_value` as an instant, whatever the value's zone. The name plays no part.
  - A volume's files are objects in the cloud storage behind it: S3, ADLS or GCS. Spark's
    `_metadata.file_modification_time` comes from the file status that the runtime's file
    system gives Spark for that object, while `last_modified` comes from the Files API. Both
    are the storage's time for the same object; S3 and ADLS keep it to the second, and this
    spec cannot check, without a workspace, that the Files API and Spark report the same
    milliseconds on every cloud. Spark's time is to the second or finer, so the reader's
    watermark, truncated to the second, is never above Spark's time for the same file. A job
    that takes its next watermark from that column, as in the [usage sketch](#usage-sketch),
    therefore never selects its last file again, whatever the precision on each side.
  - The value itself is not truncated: a file at `09:00:00` is above `08:59:59.999999` and not
    above `09:00:00.5`.

#### Load

When the selection holds at least one file, `read()` makes exactly one load:

- `spark.read.format(format)`;
- `.schema(schema)` when `schema` is given, with the very object given;
- the user's `options`, as given, and no other option;
- `.load(paths)`. `paths` is a `list` of `str`, one per selected file: `<location>/<name>`,
  such as `/Volumes/main/landing/raw/orders/a.csv`, or `/Volumes/main/landing/raw/a.csv` at the
  volume's root **(Q5)**. The paths are built from the location and the entry's `name`, never
  taken from the entry's `path`. The list is sorted by name in code-point order, and is a list
  even for one file.

`read()` returns the load's DataFrame unchanged. The reader adds no column, filter, cast or
projection.

**The selection is frozen.** Spark receives the selected files, never the directory, so every
action on the DataFrame reads those files and no other. Files that appear after `read()` are
left for the next run, whatever their watermark.

**The selection is not exposed.** The reader keeps no record of what it selected. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column
(`file_name`, `file_path`, `file_modification_time`).

#### When nothing is selected

The selection is empty when the directory holds nothing, holds only entries that are not
candidates, holds no name that matches the pattern, or holds no watermark above the value. Then
`read()` makes no load. It returns `spark.createDataFrame([], schema)`, with `schema` when it is
given and otherwise a `pyspark.sql.types.StructType` with no fields. This DataFrame has no
`_metadata` column. A callback that wants to skip an empty batch checks for itself and raises
`CancelStepException`.

#### Errors

Nothing is caught. Errors of PySpark, of `WorkspaceClient()` (the SDK's error when it finds no
usable configuration, for instance), of the listing (`NotFound`, `PermissionDenied`,
`Unauthenticated`, network errors) and of Spark propagate out of `read()` unchanged, as the
same object. When the listing fails, nothing is loaded. The connectors write no log lines.

### Writing

`write(data, *, replace=False)` makes exactly one call chain, the one of `DiskWriter`:

```text
data.write.format(<format>).options(<options>).mode(<mode>).save(<location>)
```

- `<mode>` is `"append"` when `replace` is `False` and `"overwrite"` when it is `True`;
- `<options>` are the user's, as given, and no other;
- `<location>` is the writer's location, a `str`, such as `/Volumes/main/lake/curated/orders`.

**Operations.**

- `write(data)` appends: Spark adds new files to the directory and leaves the existing ones as
  they are. `WriteTarget` calls `write(data)`, so it appends.
- `write(data, replace=True)` replaces: the directory ends up holding exactly the rows of
  `data`. With a plain file format, Spark deletes everything under the directory, files it did
  not write included, and then writes the new files. With Delta, the overwrite is a new version
  of the table.
- The calling callback chooses at call time. The configuration has no mode, and there is no
  `replace()` method.
- Spark creates the directory, and its missing parents inside the volume, when they do not
  exist. A volume that does not exist, or that the session may not write, fails in Spark.
- The file names are Spark's (`part-<n>-<uuid>-c000...`), so a `VolumeReader` that reads them
  back takes their watermark from their `last_modified`. Databricks's commit protocol also
  leaves `_SUCCESS`, `_started_*` and `_committed_*` files, which the reader ignores.

**What the writer does not do.** It reads only `data.write` from the DataFrame and evaluates it
once, through the save. It takes no session and no client, imports no databricks-sdk, and lists,
creates and deletes nothing itself. It calls no other method of Spark's writer: no
`partitionBy`, `bucketBy`, `sortBy`, `saveAsTable` or `insertInto`. An empty DataFrame goes
through the same call. `write` returns `None`, and every error propagates unchanged, as the
same object.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what the runtime's
commit protocol and the format guarantee, and nothing more:

- **Plain file formats.** On Databricks, file formats are written with the runtime's own commit
  protocol, which can write data files at their final names and mark the files of each
  committed job with `_committed_*` markers. What it guarantees is the runtime's, and this spec
  does not rely on it. `VolumeReader` does not read those markers (see
  [Known limitations](#known-limitations)).
- **Replace, plain file formats.** Spark deletes the files under the directory before it
  writes. If the write then fails, the directory is left empty or holding part of the new rows,
  and the previous contents are lost.
- **Delta.** An append or an overwrite is one commit in the table's log, so readers through
  Delta see the table before the commit or after it. A Delta table written into a volume is
  files in the volume, not a table of the catalog: tabular data usually belongs in Unity
  Catalog tables, which the [Unity Catalog connectors](unity_catalog_connectors.md) write.

### Differences from the disk connectors

What a volume listed by the Files API changes. Everything not in this table is as in the
[disk spec](disk_connectors.md). The S3 column is the one of the
[S3 spec](s3_connectors.md#differences-from-the-disk-connectors):

| Point | `DiskReader` / `DiskWriter` | `S3Reader` / `S3Writer` | `VolumeReader` / `VolumeWriter` |
|---|---|---|---|
| Location | absolute local path, `str` or `PathLike` | `s3://` or `s3a://` URI, a `str`, scheme passed to Spark as written | `volume`, `catalog.schema.volume`, and `path`, a `str` relative to the volume; kept as `/Volumes/<catalog>/<schema>/<volume>[/<path>]` |
| Rejected locations | relative paths and URIs | other schemes, no bucket, empty, `.` or `..` segments; the bucket root for the writer | a `volume` that is not three non-empty parts or has a `/`; a `path` that is not a `str` or starts with `/`; empty, `.` or `..` segments; the volume's root for the writer |
| Listing | `os.scandir` of the directory | boto3 `ListObjectsV2` paginator, `Delimiter="/"`, every page | `client.files.list_directory_contents(directory_path=<location>)`, every entry; the SDK pages |
| Name of an entry | the file name | the key after the listing prefix | the entry's `name` |
| Not candidates, besides hidden names | directories, broken links | empty names (directory markers), names with `/`, common prefixes | entries whose `is_directory` is `True` |
| Modification-time watermark | `st_mtime_ns`, truncated to milliseconds | `LastModified`, truncated to seconds | `last_modified`, in milliseconds, truncated to seconds |
| Missing location | `FileNotFoundError` | empty selection; a missing bucket fails in boto3 | the SDK's `NotFound` propagates |
| Paths given to Spark | `str(Path(path) / name)` | `<URI>/<name>`, scheme as written | `<location>/<name>`, a `/Volumes/...` path |
| Native client | none | `client`, or `boto3.client("s3")` created at each read | `client`, or `databricks.sdk.WorkspaceClient()` created at each read |
| Extra | none | `ingestio[s3]`, needed only without `client` | `ingestio[databricks]`, needed only without `client` |
| Writer | the same chain | the same chain, with the URI; no boto3 | the same chain, with the location; no SDK |
| Where it runs | Databricks classic compute (volumes, through FUSE) and open-source Spark on a local file system | Databricks classic compute and open-source Spark with hadoop-aws | Databricks classic compute and Databricks Connect; not open-source Spark |

### The `databricks` extra (Q6)

Following the [pattern for extras](../docs/concepts/connectors.md#extras):

1. `[project.optional-dependencies]` gains `databricks = ["databricks-sdk>=0.20"]`.
   `dependencies` stays empty. The floor is the version that this spec expects Databricks
   Runtime 15.4 LTS to preinstall, and that version is expected to have
   `files.list_directory_contents` already. The main session confirms both facts, against the
   SDK's changelog and the runtime's release notes, before it edits `pyproject.toml`. The rule
   behind the number: the floor is the runtime's version when that version has the directory
   listing, and otherwise the first version that has it.
2. `connector/volume.py` imports `databricks.sdk` only inside the functions that use it, and
   `WorkspaceClient` under `TYPE_CHECKING`.
3. A reader without `client` raises the `ImportError` above when databricks-sdk is missing.
   With a `client`, and for the writer, the extra is not needed. Databricks Runtime preinstalls
   databricks-sdk, and Databricks Connect installs it as one of its own dependencies, so the
   extra matters mostly elsewhere, and for its floor.
4. pyspark stays a prerequisite, in no extra. Databricks Connect is a prerequisite of jobs that
   run outside Databricks, never an extra and never in the `dev` group: it replaces pyspark and
   cannot be installed next to it.
5. The `dev` group gains `databricks-sdk>=0.20`. No type stubs: this spec expects
   databricks-sdk to ship its own `py.typed` marker, so that mypy strict checks
   `databricks.sdk` from the package itself, and no stubs package exists for it. The main
   session confirms it with `uv run mypy` once the package is locked; the fallback is in
   **Q6**. No test-only package is added.

The main session adds these to `pyproject.toml` and refreshes `uv.lock` after approval, as it
did for PostgreSQL and S3. Those files are outside every stage's write scope.

### Where it lives

- `src/ingestio/connector/volume.py` **(Q1)**: `VolumeReader` and `VolumeWriter`, with a module
  docstring and a sorted `__all__`. The [Unity Catalog spec](unity_catalog_connectors.md)
  proposes `connector/unity_catalog.py` for its connectors, so the two modules do not clash.
- `src/ingestio/connector/all.py`: exports the two classes, `__all__` sorted.
- `pyproject.toml` and `uv.lock`: as in [The `databricks` extra](#the-databricks-extra-q6), by
  the main session.
- **Implementation note, not public API.** The rules that `VolumeReader` shares with
  `DiskReader` and `S3Reader` (format and option checks, watermark validation, hidden names,
  the comparison, the sort, the empty DataFrame) may be factored into private helpers, such as
  the private module that the S3 spec suggests, `src/ingestio/connector/_files.py`, if it exists
  by then. The disk and S3 connectors then keep their behaviour and messages exactly, and their
  test files pass unchanged.
- No change to `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or
  `connector/core.py`.

### Supported environments

- pyspark 3.5 or later is a prerequisite that the runtime provides, or the pyspark of
  Databricks Connect. ingestio never installs it.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated or
  standard access mode. Spark reads and writes `/Volumes/...` paths under the volume's
  privileges. Only the DataFrame API is used, never `sparkContext` or `_jvm`. The default
  client follows the SDK's own lookup, which on Databricks compute includes the notebook's or
  the job's own authentication; when it finds nothing usable, the job passes a `client`.
- **Databricks Connect** 15.4 or later, from a Python 3.11 process outside the workspace. The
  session is passed as `spark=`, or found by `SparkSession.active()` where PySpark returns the
  Databricks Connect session. The listing runs in the job's own process, through the Files API,
  with the SDK's configuration there: a profile in `~/.databrickscfg`, or the `DATABRICKS_*`
  environment variables. Spark resolves the paths on the cluster. Not tested here.
- **Databricks serverless compute:** nothing in the design excludes it, because it uses only the
  Files API and DataFrame calls that Spark Connect supports, but it is not tested. This is the
  claim of the disk and Unity Catalog specs.
- **Not open-source Spark.** The listing needs a Databricks workspace, and only Databricks's
  Spark resolves `/Volumes/...` to a volume. A directory mounted at `/Volumes` on open-source
  Spark is `DiskReader`'s case.
- **Formats** are Spark's built-ins and those that Databricks Runtime includes, such as Delta
  and Avro.
- **Privileges.** The listing runs as the client's identity, which needs `USE CATALOG` on the
  catalog, `USE SCHEMA` on the schema and `READ VOLUME` on the volume. Spark runs as the
  session's identity, which needs the same for its reads, and `WRITE VOLUME` for its writes.
  Unlike S3's, the listing goes through Unity Catalog.

### Known limitations

These are documented, not solved here:

- **Writes are only as atomic as the runtime's commit protocol and the format make them** (see
  [Atomicity](#writing)). A failed replace of plain files loses the previous contents.
- **`replace=True` deletes everything under the directory** with a plain file format, files
  Spark did not write included. Never point a writer that replaces at a directory that holds
  anything else.
- **Uncommitted files.** Databricks's commit protocol can write data files at their final names
  and mark them as committed afterwards. A reader that lists the directory while a Spark job
  writes plain files into it, or after such a job failed, can select files of that job. The
  reader lists files; it does not read `_started_*` or `_committed_*` markers.
- **Late files.** A run that advances its watermark to the highest one it read never reads a
  file that appears later with a watermark at or below it:
  - by name: a producer that delivers files out of order;
  - by `last_modified`: a file that keeps an older time when it is moved or copied into the
    directory, which depends on the cloud storage behind the volume and on the tool; a large
    file that the storage stamps with the time its upload started rather than finished, as S3
    does for an upload in parts;
  - files that share the last second read and appear after the read are skipped, because the
    comparison is strict and times are whole seconds.
- **The selection is frozen, not the contents.** A selected file that is overwritten before an
  action is read as it is then, or makes the action fail. A selected file that is deleted makes
  the action fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Glob characters.** Spark expands the characters `*`, `?`, `[`, `]`, `{`, `}` and `\` in the
  paths it loads. A selected file whose path holds one can be read as other files, or fail. The
  reader does not escape them, as the S3 and disk connectors do not.
- **Two identities, two configurations.** The client and the Spark session are configured
  separately. A client that lists another workspace, or runs as an identity that sees other
  privileges than the session's, is not detected: the listing fails, or the load fails or reads
  what the session sees at those paths.
- **Name watermarks compare as text,** as in `DiskReader`.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing directory, a typo included, fails the read instead.
- **Large directories.** Each `read()` lists every entry of the directory through the Files
  API, page by page, and Spark then requests the status of each selected file before reading
  it.
- **Table formats.** The reader rejects `delta`, but it does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM,
without network and without a workspace. The doubles live in `tests/test_volume_connectors.py`.

- **Fake workspace client.** It records every attribute accessed on it. Its `files` attribute
  is a fake Files API, which also records every attribute accessed on it.
  `list_directory_contents(*args, **kwargs)` records a *listing*, with its arguments, and
  returns an iterator over the entries the test set. The iterator records each entry it yields
  and when its end is reached, and can raise an error the test gives when the iteration reaches
  a given position. The call itself can raise an error the test gives.
- **Entries** are `databricks.sdk.service.files.DirectoryEntry(name=..., path=...,
  is_directory=..., file_size=..., last_modified=...)`, the SDK's own dataclass, which the
  `dev` group provides. A criterion that runs without the SDK uses a dataclass of the test file
  with the same five fields.
- **The factory.** Criteria that mention it replace the SDK's client with
  `monkeypatch.setattr(databricks.sdk, "WorkspaceClient", factory)`, where `factory` records
  the positional and keyword arguments of each call and returns the fake client, or raises an
  error the test gives.
- **Fake Spark session and fake DataFrame:** those of the disk spec's Notes for testing, with
  loads, creates and saves recorded the same way. The fake listing and the fake session can
  record into one event log, so that a test can check the order of what happened.
- **Missing packages.** Map `pyspark`, `databricks` and `databricks.sdk`, with their
  submodules, to `None` in `sys.modules`: in a subprocess for a fresh interpreter, or with
  `monkeypatch.setitem`.
- `databricks.sdk.errors.NotFound` and `PermissionDenied` can be built without network, from a
  message: `NotFound("The directory being accessed is not found.")`.
- Tests use the public API. They pass the fakes as `client=`, `spark=` and DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against a Databricks workspace. They are marked
`@pytest.mark.integration` and skipped, with a reason, unless the environment variable
`INGESTIO_DATABRICKS_VOLUME` is set. Its value is the three-part name of a scratch volume,
`catalog.schema.volume`. Each test works in its own directory inside that volume, with a unique
name, and deletes it at the end through the SDK. The client and the session take their
configuration from the SDK's standard environment (`DATABRICKS_HOST` with a token, or a
profile). The session is a Databricks Connect session, in an environment where
`databricks-connect` replaces pyspark, or the session of a cluster where pytest runs; how the
tests get it, and with which `spark.sql.session.timeZone`, is the test-writer's choice. They
need no local Java. On the maintainer's machine they are written blind: it has no workspace and
no credentials. They cover:

1. `VolumeWriter(<vol>, <dir>, "parquet")`: two calls of `write(df)` leave the rows of both
   batches, and `write(df, replace=True)` then leaves exactly the rows of `df`. A full
   `VolumeReader(<vol>, <dir>, "parquet")` returns them; `_SUCCESS` and the commit markers do
   not disturb it.
2. CSV round trip: written with `{"header": True}`, read back with the same option and a DDL
   `schema`, with the same rows.
3. Name watermark: CSV files uploaded by the test with `files.upload`. Only the rows of the
   files above the value are returned. A file uploaded after `read()`, with a higher name, is
   absent from a second `count()` on the same DataFrame.
4. `last_modified` watermark: the test uploads `a.csv`, waits until the next second, uploads
   `b.csv`, and takes `a.csv`'s `last_modified` from a listing. A reader with that value
   returns only `b.csv`'s rows. The highest `unix_seconds(_metadata.file_modification_time)`
   equals `b.csv`'s `last_modified // 1000`, and a reader built with that instant, as an aware
   UTC datetime, selects nothing. This checks the precision of Q3.
5. Empty selection in a directory that the test creates empty: with a `schema`, a DataFrame
   with its columns and no rows; without one, no columns and no rows.
6. A directory that does not exist: `read()` raises `databricks.sdk.errors.NotFound`. This
   checks Q4.
7. A subdirectory with files, and hidden files, are not read.
8. A reader built without `client` lists through the default client.
9. A write whose computation fails, through a Python UDF that raises, raises out of `write`.
10. `write(df, replace=True)` on a directory that holds `keep.txt`, uploaded by the test,
    removes it. This confirms the documented limitation.

## Acceptance criteria

The criteria follow the recommended option of every question. A criterion that depends on one
names it.

Unless a criterion says otherwise:

- `spark` is the fake session, `df` a fake DataFrame and `client` the fake workspace client,
  from [Notes for testing](#notes-for-testing);
- `VOL` is `"main.landing.raw"`, `LOC` is `"/Volumes/main/landing/raw/orders"`, and `v(name)`
  is `LOC + "/" + name`;
- `T0` is `datetime(2026, 10, 21, 9, 0, tzinfo=UTC)`, and `ms(t)` is the `int` number of whole
  milliseconds from the Unix epoch to `t`;
- "the directory holds `name`" means that the fake listing yields an entry with
  `name=name`, `path=v(name)`, `is_directory=False`, `file_size=1` and `last_modified=ms(T0)`,
  or `ms(t)` in "`name` at `t`". "The subdirectory `d`" is an entry with `name=d`,
  `path=v(d)`, `is_directory=True`, `file_size=None` and `last_modified=None`. Entries come in
  the order the criterion lists them;
- the reader is `VolumeReader(VOL, "orders", "csv", client=client, spark=spark)`, with
  whatever the criterion adds;
- the writer is `VolumeWriter("main.lake.curated", "orders", "parquet")`;
- "the paths" of a load are the value its `load()` received;
- "exactly the options" means these keys and no others, compared case-insensitively unless
  the criterion says the case matters.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import VolumeReader, VolumeWriter` runs, then it succeeds. Both
  names are in `ingestio.connector.all.__all__` and in `ingestio.connector.volume.__all__`, and
  neither is in `ingestio.__all__`. Depends on Q1.
- **AT-002**: Given an interpreter where `import pyspark`, `import databricks` and
  `import databricks.sdk` raise `ImportError`, when `ingestio.connector.all` is imported, then
  the import succeeds. `VolumeReader(VOL, "orders", "csv", client=<fake client>,
  spark=<fake session>)` can be built, and with the directory holding `a.csv` its `read()`
  returns the DataFrame of the fake's load. `VolumeWriter("main.lake.curated", "orders",
  "parquet")` can be built, and `write(<fake DataFrame>)` records one save.
- **AT-003**: Given an interpreter where `import databricks.sdk` raises `ImportError`, when
  `VolumeReader(VOL, "orders", "csv")` or `VolumeReader(VOL, "orders", "csv", spark=spark)` is
  built, then it raises `ImportError` with the message
  `VolumeReader needs databricks-sdk: pip install 'ingestio[databricks]'`, whose `__cause__` is
  an `ImportError`. `VolumeReader(VOL, "orders", "csv", client=client)` and the writer are built
  without error.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `databricks` is among its `Provides-Extra`
  values, one `Requires-Dist` names the `databricks-sdk` project and carries an
  `extra == "databricks"` marker, and every `Requires-Dist` carries an `extra == "<name>"`
  marker for some extra. Depends on Q6.

### Building the connectors

- **AT-005**: Given `volume` in `""`, `"main"`, `"main.landing"`, `"main.landing.raw.extra"`,
  `".landing.raw"`, `"main..raw"`, `"main.landing."`, `"main/x.landing.raw"`,
  `"main.landing.r/aw"` and `"/Volumes/main/landing/raw"`, when
  `VolumeReader(volume, "orders", "csv", client=client)` or
  `VolumeWriter(volume, "orders", "csv")` is built, then it raises
  `ValueError("volume must be 'catalog.schema.volume'")`. `"main.landing.raw"`,
  `"Main.Landing.Raw"` and `"my-catalog.landing_zone.raw_01"` are accepted by both. Depends on
  Q2.
- **AT-006**: Given the `path` on the left, when `VolumeReader(VOL, path, "csv",
  client=client)` and `VolumeWriter("main.lake.curated", path, "csv")` are built, then each
  raises `ValueError` with the message on the right, or is built when the cell says so:

  | `path` | `VolumeReader` | `VolumeWriter` |
  |---|---|---|
  | `"/orders"`, `"/"`, `"/Volumes/main/landing/raw/orders"`, `Path("orders")` | `path must be a str relative to the volume` | the same |
  | `"landing//orders"`, `"./orders"`, `"landing/../orders"`, `"orders/.."`, `"."`, `".."` | `path must not contain empty, '.' or '..' segments` | the same |
  | `""` | built | `path must name a directory inside the volume` |
  | `"orders"`, `"orders/"`, `"landing/orders//"`, `"a b/ç"`, `"_staging/orders"` | built | built |

  Depends on Q2.
- **AT-007**: Then:
  - `repr(VolumeReader(VOL, "orders/", "csv", client=client))` is
    `VolumeReader(volume='main.landing.raw', path='orders', format='csv')`;
  - `repr(VolumeReader(VOL, "orders", "csv", client=client,
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261021"))` is the same;
  - `repr(VolumeReader(VOL, "", "json", client=client))` is
    `VolumeReader(volume='main.landing.raw', path='', format='json')`;
  - `repr(VolumeWriter("main.lake.curated", "orders//", "parquet"))` is
    `VolumeWriter(volume='main.lake.curated', path='orders', format='parquet')`.
- **AT-008**: Given `format=""`, when either connector is built, then it raises
  `ValueError("format must not be empty")`. Given `"delta"` or `"Delta"`, when
  `VolumeReader(VOL, "orders", fmt, client=client)` is built, then it raises `ValueError` with
  the message `format '<fmt>' cannot be read file by file`, with the format as given, while
  `VolumeWriter("main.lake.curated", "orders", fmt)` is built. `"parquet"`, `"CSV"` and
  `"com.example.source"` are accepted by both.
- **AT-009**: Given the `watermark_pattern` on the left, when
  `VolumeReader(VOL, "orders", "csv", client=client, watermark_pattern=pattern)` is built, then
  it raises `ValueError` with the message on the right:

  | `watermark_pattern` | Message |
  |---|---|
  | `"("` | starts with `invalid watermark_pattern: `, and the `__cause__` is a `re.error` |
  | `r"orders_\d{8}\.csv"` | `watermark_pattern must have exactly one group` |
  | `r"(\d{4})(\d{4})\.csv"` | `watermark_pattern must have exactly one group` |
  | `""` | `watermark_pattern must have exactly one group` |

  `r"orders_(\d{8})\.csv"` and `r"(?:orders|sales)_(?P<day>\d{8})\.csv"` are accepted.
- **AT-010**: Given the arguments on the left, when
  `VolumeReader(VOL, "orders", "csv", client=client, ...)` is built with them, then it raises
  `ValueError` with the message on the right. `P` is `r"orders_(\d{8})\.csv"`.

  | `watermark_pattern` | `watermark_value` | Message |
  |---|---|---|
  | `P` | `20261021` | `watermark_value must be a str with watermark_pattern` |
  | `P` | `T0` | `watermark_value must be a str with watermark_pattern` |
  | none | `"20261021"` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `date(2026, 10, 21)` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `20261021` | `watermark_value must be a datetime without watermark_pattern` |
  | none | `datetime(2026, 10, 21, 9, 0)` | `watermark_value must be timezone-aware` |

  With `P`, `""` and `"20261021"` are accepted. Without a pattern, `T0` and
  `datetime(2026, 10, 21, 6, 0, tzinfo=timezone(timedelta(hours=-3)))` are accepted.
- **AT-011**: Given `key` in `path`, `PATHS`, `pathGlobFilter`, `recursivefilelookup`,
  `modifiedBefore` and `ModifiedAfter`, when
  `VolumeReader(VOL, "orders", "csv", client=client, options={key: "x"})` is built, then it
  raises `ValueError` with the message `option '<key>' is set by the connector`, with the key
  as given. Given `key` in `path`, `Paths`, `replaceWhere` and `PartitionOverwriteMode`, the
  same holds for `VolumeWriter("main.lake.curated", "orders", "parquet", options={key: "x"})`.
  The reader accepts `{"header": True, "inferSchema": "true", "mergeSchema": False}`, and the
  writer accepts `{"compression": "zstd", "header": True}`.
- **AT-012**: Given the factory, when these are built, then each succeeds, no attribute of
  `spark` or of `client` is accessed, no listing is recorded, and the factory is not called:
  - `VolumeReader(VOL, "orders", "csv", client=client, spark=spark)`;
  - `VolumeReader(VOL, "orders", "csv", spark=spark)`;
  - `VolumeReader(VOL, "orders", "csv", client=client, spark=spark,
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261020")`;
  - `VolumeReader(VOL, "orders", "csv", client=client, spark=spark, watermark_value=T0)`;
  - `VolumeWriter("main.lake.curated", "orders", "parquet")`.

### Reading

- **AT-013**: Given the factory and the directory holding `a.csv`, when `reader.read()` runs,
  then the only attribute accessed on `client` is `files`, and the only attribute accessed on
  the fake Files API is `list_directory_contents`. It is called once, with no positional
  argument and exactly the keyword argument `directory_path="/Volumes/main/landing/raw/orders"`,
  and the factory is not called. Given `VolumeReader(VOL, "", "csv", client=client,
  spark=spark)` and the directory holding `a.csv`, then `directory_path` is
  `"/Volumes/main/landing/raw"` and the paths are `["/Volumes/main/landing/raw/a.csv"]`. Given
  `VolumeReader(VOL, "landing/orders/", "csv", client=client, spark=spark)`, then
  `directory_path` is `"/Volumes/main/landing/raw/landing/orders"`. Depends on Q2 and Q5.
- **AT-014**: Given the directory holding, in this order, `b.csv`, `a.csv`, `_SUCCESS`,
  `_metadata`, `.a.csv.crc`, `c.csv._COPYING_`, `_started_123`, `_committed_123`, and the
  subdirectories `sub`, `x.csv` and `_delta_log`, when `read()` runs, then exactly one load is
  recorded. It has the format `csv`, no schema, no options, and the paths
  `[v("a.csv"), v("b.csv")]`, a `list` in this order. `read()` returns the very DataFrame that
  `load()` returned, no attribute of that DataFrame is accessed, and `createDataFrame` is not
  called. Given the directory holding only `a.csv`, the paths are `[v("a.csv")]`, still a
  `list`.
- **AT-015**: Given the directory holding `pedidos_ç.csv`, `a b.csv` and
  `2026-10-21T09:00:00.csv`, when `read()` runs, then the paths are
  `[v("2026-10-21T09:00:00.csv"), v("a b.csv"), v("pedidos_ç.csv")]`. Given the directory
  holding `a.csv` with `is_directory=None` and `b.csv` with `file_size=0`, then the paths are
  `[v("a.csv"), v("b.csv")]`. Given the directory holding `a.csv` whose `path` is
  `"dbfs:/Volumes/MAIN/other/place/z.csv"`, then the paths are `[v("a.csv")]`: they are built
  from the location and the name. Depends on Q5.
- **AT-016**: Given a listing whose iterator yields `b.csv`, `a.csv` and `c.csv` and records
  each entry it yields and the end of the iteration into the event log of the fake session,
  when `read()` runs, then the log holds the three entries and the end of the iteration before
  the load, and the paths are `[v("a.csv"), v("b.csv"), v("c.csv")]`.
- **AT-017**: Given the directory holding `a.csv`, `schema="id INT, name STRING"` and
  `options={"header": True, "sep": ";"}`, when `read()` runs, then the load carries that very
  schema object and exactly the options `header` = `true` and `sep` = `;`. Given instead
  `schema=StructType([StructField("id", IntegerType())])`, the load carries that very object.
- **AT-018**: Given a reader built with `client` and without `spark`, while
  `pyspark.sql.SparkSession.active` is replaced with a function that raises, then building
  succeeds. When `active` is then replaced with a function that returns the fake session and
  `read()` runs with the directory holding `a.csv`, then the load is recorded on the fake. Given
  `active` replaced with a function that raises `err` when `read()` runs, then `read()` raises
  `err` itself and no listing is recorded.
- **AT-019**: Given the factory, `VolumeReader(VOL, "orders", "csv", spark=spark)` and the
  directory holding `a.csv`, then building does not call the factory. Each `read()` calls it
  exactly once, with no positional and no keyword argument, and the listing is recorded on the
  client it returned: two reads make two calls. Given the factory replaced by another after the
  reader was built, then the replacement is the one called. Given a factory that raises `err`,
  when `read()` runs, then it raises `err` itself, and no listing, load or `createDataFrame`
  call is recorded.
- **AT-020**: Given a reader built while the directory holds nothing, when the directory is set
  to hold `a.csv` and `read()` runs, then the load's paths are `[v("a.csv")]`. When the
  directory is then set to hold `a.csv` and `b.csv` and `read()` runs again, a second listing
  is recorded, the second load's paths are `[v("a.csv"), v("b.csv")]`, and the first load's
  paths are still `[v("a.csv")]`.
- **AT-021**: Given a listing that yields no entry, as the Files API answers for an empty
  directory, and then the directory holding only `_SUCCESS` and the subdirectory `sub`, when
  `read()` runs, then no load is recorded and `createDataFrame` is called once. It receives an
  empty list as data and, as schema, a `pyspark.sql.types.StructType` with no fields. `read()`
  returns what `createDataFrame` returned. Given `schema="id INT, name STRING"` in addition, the
  schema passed is that very object.
- **AT-022**: Given `err = NotFound("The directory being accessed is not found.")`, and then
  `err = PermissionDenied("User does not have READ VOLUME on Volume 'main.landing.raw'.")`, both
  from `databricks.sdk.errors`:
  - when `list_directory_contents` raises `err` when it is called, or its iterator raises it at
    the first entry, or after yielding `a.csv`, then `read()` raises `err` itself, and no load
    and no `createDataFrame` call is recorded;
  - given a fake whose `load()` raises `err2 = RuntimeError("[PATH_NOT_FOUND]")` and the
    directory holding `a.csv`, then `read()` raises `err2` itself;
  - given a fake whose `createDataFrame` raises `err2` and an empty directory, then `read()`
    raises `err2` itself.

  Depends on Q4.

### Watermark from the file name

In this section `P` is `r"orders_(\d{8})\.csv"`, and the reader is
`VolumeReader(VOL, "orders", "csv", client=client, spark=spark, watermark_pattern=P, ...)`.

- **AT-023**: Given the directory holding:
  - `orders_20261019.csv` at `T0 + 2 s`, `orders_20261020.csv` at `T0 + 1 s` and
    `orders_20261021.csv` at `T0`;
  - `customers_20261021.csv`, `orders_latest.csv` and `orders_20261021.csv.bak`;

  when `read()` runs with `watermark_value="20261019"`, then the paths are
  `[v("orders_20261020.csv"), v("orders_20261021.csv")]`. Without `watermark_value`, they are
  the three `orders_<date>.csv` files, sorted by name.
- **AT-024**: Given the files and the value on the left, when `read()` runs, then the files on
  the right are selected. "None" means no load and one `createDataFrame` call.

  | Pattern | Files in the directory | `watermark_value` | Selected, in the order of the paths |
  |---|---|---|---|
  | `P` | `orders_20261019.csv`, `orders_20261020.csv`, `orders_20261021.csv` | `"20261021"` | none |
  | `P` | the same | `"2026102"` | `orders_20261020.csv`, `orders_20261021.csv` |
  | `P` | the same | `""` | all three |
  | `r"part_(\d+)\.csv"` | `part_9.csv`, `part_10.csv` | `"9"` | none |
  | `r"part_(\d+)\.csv"` | the same | `"1"` | `part_10.csv`, `part_9.csv` |

- **AT-025**: Without `watermark_value`:
  - given `watermark_pattern=r"(\d{8})\.csv"` and the directory holding `orders_20261021.csv`,
    then nothing is selected, because the whole name must match;
  - given `watermark_pattern=r"/Volumes/main/landing/raw/orders/orders_(\d{8})\.csv"` and the
    same directory, then nothing is selected, because the pattern sees the name, not the path;
  - given `watermark_pattern=r"orders(_\d{8})?\.csv"` and the directory holding `orders.csv`
    and `orders_20261021.csv`, then the paths are `[v("orders_20261021.csv")]`;
  - given `watermark_pattern=r".*_(\d{8})\.csv"` and the directory holding
    `_orders_20261022.csv`, `.orders_20261022.csv`, the subdirectory `orders_20261022.csv` and
    `orders_20261021.csv`, then the paths are `[v("orders_20261021.csv")]`.

### Watermark from `last_modified`

In this section the reader is `VolumeReader(VOL, "orders", "csv", client=client, spark=spark,
...)`, with no `watermark_pattern`.

- **AT-026**: Given the directory holding `c.csv` at `T0`, `b.csv` at `T0 + 1 s` and `a.csv` at
  `T0 + 2 s`, when `read()` runs with the `watermark_value` on the left, then the paths are
  those on the right:

  | `watermark_value` | Paths |
  |---|---|
  | none | `v("a.csv")`, `v("b.csv")`, `v("c.csv")` |
  | `T0 - 1 µs` | `v("a.csv")`, `v("b.csv")`, `v("c.csv")` |
  | `T0` | `v("a.csv")`, `v("b.csv")` |
  | `T0 + 500 ms` | `v("a.csv")`, `v("b.csv")` |
  | `T0 + 1 s` | `v("a.csv")` |
  | `T0 + 2 s` | none: no load, one `createDataFrame` call |

  Depends on Q3.
- **AT-027**: Given the directory holding `a.csv` with `last_modified=ms(T0) + 999`, when
  `read()` runs with the `watermark_value` on the left, then `a.csv` is selected or not as on
  the right:

  | `watermark_value` | `a.csv` selected |
  |---|---|
  | `T0` | no |
  | `T0 + 500 ms` | no |
  | `T0 - 1 µs` | yes |
  | `datetime(2026, 10, 21, 6, 0, tzinfo=timezone(timedelta(hours=-3)))` | no |
  | `datetime(2026, 10, 21, 5, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-3)))` | yes |

  Depends on Q3.

### Writing

- **AT-028**: Given the factory, when `writer.write(df)` runs, then exactly one save is
  recorded, with the format `parquet`, the mode `append`, no options, and the path
  `"/Volumes/main/lake/curated/orders"`, a `str`. The only attribute accessed on `df` is
  `write`. The only attributes accessed on the fake writer are `format`, `option` or
  `options`, `mode` and `save`. `write` returns `None`, and the factory is not called.
  `writer.write(df, replace=False)` gives the same.
- **AT-029**: When `writer.write(df, replace=True)` runs, then the save is the one of AT-028,
  except that its mode is `overwrite`.
- **AT-030**: Given `VolumeWriter("main.lake.curated", "landing/orders/", "csv",
  options={"header": True, "Compression": "gzip"})`, when `write(df)` runs, then the save has
  the format `csv`, the path `"/Volumes/main/lake/curated/landing/orders"`, and exactly the
  options `header` = `true` and `Compression` = `gzip`, with the key `Compression` in this case.
  Depends on Q2 and Q5.
- **AT-031**: Given a save that raises `err = RuntimeError("task failed")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, and `save()` was called once.

### Composition

- **AT-032**: Given the directory holding `a.csv` and
  `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, when it runs, then
  `process.state["orders"]` is the DataFrame that the load returned, and one save is recorded
  on it, with the mode `append`. The test binds the reader to a variable annotated
  `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`, and mypy strict
  accepts both.
- **AT-033**: Given `caplog` capturing every level and the factory, when readers with and
  without `client` and writers are built, `read()` runs with a selection that holds files and
  with an empty one, and `write()` runs with and without `replace=True`, then no log record is
  captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Five connectors, each with a reader and a writer, one spec per system:** S3, GCS, Unity
   Catalog, Databricks Volumes and BigQuery. This spec is the Volumes one. S3, GCS and Volumes
   are like the disk connectors, because they handle files and receive the file format.
2. **What the connectors add to `DiskReader` and `DiskWriter`: the listing through the
   Databricks SDK.** The reader lists the volume directory through the Files API,
   `WorkspaceClient().files.list_directory_contents(...)`, whose entries carry `name`, `path`,
   `is_directory`, `file_size` and `last_modified` in milliseconds. It selects those above the
   watermark with the rules of `DiskReader` and `S3Reader`, and Spark loads exactly those
   `/Volumes/<catalog>/<schema>/<volume>/...` paths. So it works also from outside the cluster,
   through Databricks Connect, without depending on the FUSE mount. The writer is Spark writing
   to the `/Volumes/...` path, with `write(data, *, replace=False)`, and needs no SDK. In the
   maintainer's sketch, `VolumeReader(catalog, schema, volume, path, format, ...)`, the volume
   is named by its Unity Catalog parts; how that meets the Spark `schema` argument is **Q2**.
3. **Credentials of the listing client: an optional `client` argument,** a
   `databricks.sdk.WorkspaceClient`. Without it the reader creates `WorkspaceClient()`, with the
   SDK's default authentication, at `read()` time, never at construction. Spark's own access to
   the volume is Unity Catalog's, through the volume's privileges.
4. **The extra is `ingestio[databricks]`,** holding databricks-sdk, imported lazily. The main
   session adds the extra, the `dev` package and any typing support that mypy strict needs to
   `pyproject.toml` and `uv.lock` after approval (**Q6** says what).
5. **Everything else follows the disk, PostgreSQL and S3 specs:** the contract of `DiskReader`
   and `DiskWriter` (`format`, `watermark_pattern` on the name or else the last-modified time,
   `watermark_value`, `schema`, `options`, `spark`); hidden names, strictly greater, a sorted
   and frozen list, the empty selection, Delta rejected by the reader, reserved options, a flat
   listing; errors propagated, no log lines; exports only from `ingestio.connector.all`.

Proposed by this spec, following the precedents where they give one, and accepted by the main
session with its approval on 2026-10-23, under the maintainer's delegation:

6. **Building validates and does nothing else, except the extra check of a reader without
   `client`,** as S3's decision 6. A reader with a `client` needs no package of its own, so it
   imports none, and the import surface and the criteria work without databricks-sdk.
7. **One `list_directory_contents(directory_path=<location>)` call per read, every entry
   consumed before anything loads,** with no `page_size`. The SDK follows the page tokens
   itself, as boto3's paginator does for S3, and the fake stays an iterator of entries. No other
   operation of the SDK is called: no `get_directory_metadata` before the listing, which would
   cost a request and duplicate the listing's own 404.
8. **A candidate is an entry whose `is_directory` is not `True`, with the hidden-name rule of
   `DiskReader` applied to its `name`.** `None` counts as a file because the JSON of the Files
   API may leave out a false boolean. The entry's `path` and `file_size` are not used: the
   reader needs only the name, as `DiskReader` needs only the file name.
9. **Spark receives `<location>/<name>` per selected file, sorted by name, always as a list,**
   as in the disk spec's decision 8 and S3's decision 9. Building the paths from the location
   the job gave, rather than from the entry's `path`, keeps one form for every path, whatever
   form the API returns **(Q5)**.
10. **The default client is `databricks.sdk.WorkspaceClient()`, looked up on the module at each
    `read()`, one new client per read, never cached.** A given client is used as it is for
    every read and never reconfigured. Each read then sees the environment as it is, as the
    session lookup does, and as S3's decision 10.
11. **Formats, watermark values, options and reserved keys are the disk connectors', with the
    same messages,** and there are no default options.
12. **The writer is `DiskWriter`'s single call chain with the location.** It takes no session
    and no client, needs no SDK, and lists, creates and deletes nothing itself.
13. **Errors propagate unchanged and the connectors write no log lines,** as in the PostgreSQL
    spec's decision 16, the disk spec's decision 13 and S3's decision 13. That includes the
    SDK's `NotFound` for a missing directory **(Q4)**, `PermissionDenied` and the SDK's
    configuration errors.
14. **`path` is a `str` only, relative to the volume, and `volume` is three plain parts.** The
    messages of the location follow the S3 connectors' (`path must not contain empty, '.' or
    '..' segments`) and the Unity Catalog connectors' (`... must be 'catalog.schema.volume'`).
15. **`repr` shows `volume`, `path` and `format`,** the arguments that identify the connector,
    as `DiskReader`'s `repr` shows its own.
16. **Supported environments:** Databricks classic compute on Runtime 15.4 LTS or later, in
    dedicated and standard access mode, and Databricks Connect 15.4 or later. Serverless:
    nothing excludes it, not tested, the claim of the disk and Unity Catalog specs. Not
    open-source Spark, where `/Volumes/...` names no volume.
17. **The selection code may be shared with `DiskReader` and `S3Reader` through private
    helpers,** with no change to their behaviour, messages or tests. This is an implementation
    note, not public API.
18. **Tests follow the answer to S3's question 7:** the criteria run everywhere with a fake
    client and a fake Spark session, with no test-only package, and the integration tests are
    opt-in through `INGESTIO_DATABRICKS_VOLUME` and written blind here.
19. **The extras criterion follows the answer to S3's question 8:** AT-004 states the general
    rule, so that every extra coexists, and the PostgreSQL criterion it was amended from needs
    no further change.
20. **Glob characters are a documented limitation, following the answer to S3's question 9,**
    with no escaping.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept and apply a
  format they are given, as the disk connectors do.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged. Like the
  disk and S3 connectors, these lean towards batch: one selection per `read()`, a watermark
  fixed when the reader is built, and a write that needs the whole DataFrame. A streaming source
  on a volume, such as Auto Loader or Structured Streaming's file source, tracks the files it
  processed in a checkpoint and would be a different connector. This belongs in the "What leans towards batch"
  list of [ADR 0012](../docs/decisions/0012-streaming-deferred.md): an amendment, not a new
  ADR.
- **ADR 0013 applies.** `volume`, `path`, `format`, `watermark_pattern` and `schema` become
  configuration keys by its convention, `watermark_value` is computed by the job, and `client`
  carries the host and the credentials, separate from the configuration. The writer's `volume`
  and `path` name the target, as `DiskWriter`'s `path` does. Selecting files before loading is
  how this reader filters inside the source. The default client's own lookup of credentials
  falls under the consequence that the answer to S3's question 6 adds to ADR 0013 (an SDK's
  default client, created when the job passes none, follows the SDK's lookup, which the
  connector neither reads nor configures): the doc-keeper names the Databricks SDK in it too. No
  new ADR.
- **ADR 0002 applies unchanged.** The `databricks` extra follows the pattern for extras, with
  rule 3 read as the S3 spec reads it: a reader with a `client` needs no package.

## Open questions

None open. On 2026-10-23 the maintainer, away for the session, delegated the approval of this
spec to the main session, with the instruction to take the recommended option of every question
that the maintainer's decisions do not settle. The main session took the recommended option of
all six; none contradicts decisions 1 to 5. The options are kept below as the record of what
was weighed, for the maintainer to review.

1. **Class names, module and slug.**
   - (a) `VolumeReader` and `VolumeWriter` in a new `connector/volume.py`, slug
     `volume_connectors`. The names of the maintainer's sketch; CONTRIBUTING names modules
     after the medium, as `local.py` and `s3.py` are, and the Unity Catalog spec proposes its
     own `unity_catalog.py`.
   - (b) The same names in `connector/databricks.py`, after the extra. A module that later SDK
     connectors could share, but the Unity Catalog connectors, the other Databricks system,
     need no SDK and live elsewhere, and a module named `databricks` that imports the
     `databricks` package reads ambiguously in imports and tracebacks.
   - (c) `DatabricksVolumeReader` and `DatabricksVolumeWriter`. Longer; in ingestio's
     documentation "volume" already means a Unity Catalog volume.

   **Recommended: (a).**
2. **How the connectors name the location, given that the sketch's `schema` clashes with the
   Spark `schema` argument.**
   - (a) `VolumeReader(volume, path, format, ...)`, with `volume` the three-part name
     `catalog.schema.volume`, as Unity Catalog writes it and as the Unity Catalog connectors
     name tables, and `path` the directory inside the volume, relative, `""` for the root. The
     sketch's catalog, schema and volume stay, joined in Unity Catalog's own notation, and
     `path` and `format` follow in the sketch's order; `schema` keeps the meaning of the
     `DiskReader` contract (decision 5). The configuration of a volume job has the keys
     `volume`, `path` and `format`. Its `path` is relative, while a disk job's `path` is
     absolute.
   - (b) `VolumeReader(path, format, ...)`, with `path` a whole
     `/Volumes/<catalog>/<schema>/<volume>/<dir>`. The signature and configuration key of
     `DiskReader`, so that a job switches between the two by changing the class. It drops the
     sketch's Unity Catalog parts.
   - (c) `VolumeReader(catalog, schema_name, volume, path, format, ...)`: the sketch with the
     Unity Catalog schema renamed. Four positional strings before the format are easy to swap,
     and `schema_name` next to `schema` invites confusion.
   - (d) `VolumeReader(catalog, schema, volume, path, format, ..., data_schema=None)`: the
     sketch as written, with the Spark schema renamed. It breaks the contract that decision 5
     shares with `DiskReader` and `S3Reader`.

   **Recommended: (a).** With (a), and the same for the writer:
   - a `path` that starts with `/` is rejected, which catches a whole `/Volumes/...` path given
     by mistake, rather than stripped. **Recommended: rejected;**
   - empty, `.` and `..` segments are rejected, as in S3, rather than passed as written.
     **Recommended: rejected;**
   - the volume's root, `""`, is accepted by the reader and rejected by the writer, as S3
     treats the bucket root, rather than accepted or rejected by both. **Recommended: as
     S3.**
3. **The precision of `last_modified` watermarks.**
   - (a) Whole seconds, as `S3Reader`. A volume's files live in S3, ADLS or GCS, and Spark's
     `_metadata.file_modification_time` comes from the runtime's file system for that storage,
     not from the Files API. S3 and ADLS keep seconds, so nothing is lost there. Whatever
     precision each side has on each cloud, seconds are never finer than Spark's time, so a job
     that advances its watermark from that column never re-reads its last file. Files that
     share the last second read and arrive after the read are skipped. Integration test 4
     checks the agreement on a real workspace.
   - (b) Milliseconds, as `DiskReader` and as the API reports them. Narrower than (a) where
     the Files API and Spark report the same milliseconds. Where the Files API reports finer
     times than Spark gets, the job's next watermark would be below its last file's listed
     time, and that file would be read again on every run; this spec cannot rule that out
     without a workspace.
   - (c) The time as the API returns it, which is (b).

   **Recommended: (a).**
4. **A missing directory or volume, a path that is a file, and a denied listing.**
   - (a) The SDK's error propagates unchanged: `NotFound` for a missing directory or a path
     that names a file, which Databricks documents as a 404, `NotFound` or `PermissionDenied`
     for a missing volume, as Unity Catalog answers, and `PermissionDenied` for missing
     privileges. A volume has real directories and the Files API tells a missing one from an
     empty one, so S3's reason for an empty selection does not apply, and this is `DiskReader`'s
     behaviour, whose `FileNotFoundError` propagates. A typo fails the run, and so does a
     landing directory that does not exist yet.
   - (b) An empty selection on `NotFound`, as `S3Reader` does for an empty prefix. That catches
     an error to carry on, against decision 5, and reads a typo or a missing volume as empty.
   - (c) Check first with `get_directory_metadata` and raise `FileNotFoundError`. An extra
     request per read and a translated error, against "errors propagate unchanged".

   **Recommended: (a).**
5. **The form of the paths that Spark receives.**
   - (a) `/Volumes/<catalog>/<schema>/<volume>/...`, with no scheme. It is the form Databricks
     documents for Spark, the form `DiskReader` gives Spark on a volume, and the form the
     reader lists, and it resolves on the cluster through Databricks Connect too.
   - (b) `dbfs:/Volumes/...`, with the scheme. Databricks accepts it too and it does not depend
     on the session's default file system, but it names DBFS, which Databricks documents as a
     legacy mechanism, and differs from what `DiskReader` gives Spark on the same volume.

   **Recommended: (a).**
6. **The extra, the `dev` group and the type of `client`.**
   - The extra: (a) `databricks = ["databricks-sdk>=0.20"]`. This spec expects 0.20 to be the
     version that Databricks Runtime 15.4 LTS preinstalls, and the directory listing of the
     Files API to have been added before it (around 0.18). With that floor, installing the
     extra on 15.4 LTS does not upgrade the runtime's SDK. The main session confirms both facts
     against the SDK's changelog and the runtime's release notes before it edits
     `pyproject.toml`, and takes as the floor the first version that has
     `files.list_directory_contents` if the runtime's is older; installing the extra there
     would then upgrade the runtime's SDK, which Databricks supports with `%pip`. (b) No
     floor: an old SDK without the listing fails with `AttributeError` at read time. (c) A
     recent floor, such as the version `uv.lock` resolves: every runtime's SDK is upgraded on
     install. **Recommended: (a).**
   - Typing: (a) no stubs package, because databricks-sdk is expected to ship a `py.typed`
     marker, so that mypy strict reads its annotations, and no stubs package exists for it; if
     `uv run mypy` reports `databricks.sdk` as untyped once it is locked, the main session adds
     a `[[tool.mypy.overrides]]` for `databricks.*` with `ignore_missing_imports = true`
     instead, which changes no criterion; (b) the override from the start, with `client` typed
     `Any`. **Recommended: (a),** together with
     `databricks-sdk>=0.20` in the `dev` group. databricks-connect is in neither the extra nor
     the `dev` group, under every option: it replaces pyspark.
   - The type of `client`: (a) `databricks.sdk.WorkspaceClient | None`, imported under
     `TYPE_CHECKING`, which users without the SDK see as `Any`; (b) `Any`; (c) a `Protocol`
     of ingestio's own with a `files` attribute, a new public type for one attribute.
     **Recommended: (a).**

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions, on the model of the S3 spec: `VolumeReader(volume, path, format, ...)` lists one directory of a Unity Catalog volume with `client.files.list_directory_contents(directory_path=...)` (the job's client, or a `databricks.sdk.WorkspaceClient()` created at each read), applies the `DiskReader`/`S3Reader` selection rules and loads exactly the selected `/Volumes/...` paths; `VolumeWriter` is `DiskWriter`'s call chain on the volume path and needs no SDK; the `ingestio[databricks]` extra. 33 acceptance criteria, 10 integration scenarios, 20 decisions (5 by the maintainer, 15 proposed), 6 open questions. |
| 2026-10-23 | approval | approved | Approved by the main session under the maintainer's delegation of 2026-10-23 ("Delegar e ir até o fim"), with the recommended option of all 6 questions: `connector/volume.py` (no clash with `connector/unity_catalog.py`), `volume` as `catalog.schema.volume` plus a `path` relative to it (the maintainer's sketch named the volume by its parts; this keeps them, in one argument, and leaves `schema` its Spark meaning), `last_modified` truncated to whole seconds, the SDK's `NotFound` and `PermissionDenied` propagated, `/Volumes/...` paths with no scheme, `databricks = ["databricks-sdk>=0.20"]`. The 15 proposed decisions accepted. The main session checked the planner's three unverified facts on the published wheels: `files.list_directory_contents(directory_path, *, page_size, page_token)` and `DirectoryEntry` (`name`, `path`, `is_directory`, `file_size`, `last_modified` in milliseconds) first appear in 0.20.0 (0.18.0 lacks them), and the package ships `py.typed`; mypy strict accepts the calls with no override. It added the extra and the dev package `databricks-sdk>=0.20` to `pyproject.toml` and refreshed `uv.lock` (0.156.0 locked). |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_volume_connectors.py`: 71 tests (175 cases) covering AT-001 to AT-033 with a fake `WorkspaceClient` whose Files API iterator records each entry and the end of the iteration, the SDK's own `DirectoryEntry` as entries, a fake `databricks.sdk.WorkspaceClient` factory and the fake Spark doubles, four tests of Behaviour rules no criterion names (non-`str` volume, validation order, import check last, names kept as written), and the 10 integration scenarios, skipped unless `INGESTIO_DATABRICKS_VOLUME` is set (written blind; no workspace or Databricks Connect here). No existing test changed. Checked by the main session: collection fails as expected with `ImportError: cannot import name 'VolumeReader' from 'ingestio.connector.all'`; the rest of the suite passes (604 passed, 34 skipped), the other AT-004 tests included; ruff and format clean. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `VolumeReader` and `VolumeWriter` in a new section of `docs/concepts/connectors.md`, in the shape of the S3 and GCS sections, with a "Disk or volume?" guide (FUSE on Databricks compute versus the Files API listing, which also works through Databricks Connect) and a Volumes column in the shared differences table; added the `databricks` extra to the extras table and rule 3; amended ADR 0002 (the `databricks` extra), ADR 0012 (leans towards batch) and ADR 0013 (the default-client consequence names `WorkspaceClient()`; the writer's location; the reader's selection); updated `docs/index.md` and the architecture KB. It also fixed the two cosmetic points the GCS validation handed over (the rule-3 line's indentation, the 102-character line in ADR 0013). Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable Volumes blocks, with the expected `ImportError` of the missing classes (16 passed). |
| 2026-10-23 | implementation | implemented | The implementer added `VolumeReader` and `VolumeWriter` in `src/ingestio/connector/volume.py`, mirroring `s3.py` and built on the shared `_files.py` rules with whole-second units: the reader lists with `client.files.list_directory_contents(directory_path=<location>)`, treats an entry as a file unless `is_directory` is `True`, truncates `last_modified // 1000`, and loads exactly the selected `/Volumes/...` paths; the writer is the `DiskWriter` chain on the volume path; exported from `connector/all.py`. Its first pass read a missing `last_modified` as the epoch, which would have dropped such a file silently and forever under a datetime watermark. The spec leaves an entry without `name`, and a file without `last_modified` when the time is needed, outside its scope, so the main session asked for a loud failure, as the philosophy prefers: both now raise `ValueError` during the listing ("the Files API listed an entry without a name", "the Files API gave no last_modified for '<name>'"). No test, doc or spec changed. Checked by the main session: 771 passed, 44 skipped (the opt-in integration tests), ruff, format and mypy strict clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-033 covered (165 passed, the 10 integration scenarios skipped without `INGESTIO_DATABRICKS_VOLUME`), the full suite with the documentation examples 771 passed and 44 skipped, ruff, format, mypy strict (no override for `databricks.*`) and `uv lock --check` clean; code, documentation, philosophy, constraints and write scopes match; the disk, S3 and GCS tests pass unchanged with `_files.py` untouched. The two `ValueError`s for a missing `name` or `last_modified` conflict with nothing the spec or docs state; an entry named `""` also raises the first one. Neither error is documented: making them documented behaviour would take a spec amendment, for the maintainer to decide. The integration tests stay unverified: no workspace or Databricks Connect here. |
| 2026-10-23 | amendment | validated | AT-032 superseded by [specs/config_connectors.md](config_connectors.md), decision 17, approved by the maintainer on 2026-10-23: `ReadSource` and `WriteTarget` no longer take a connector, so the composition this criterion describes is covered by that spec's criteria. The criterion's test goes in the tests stage of that spec; this spec keeps its status. |
