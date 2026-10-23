# Spec: S3 reader and writer connectors

- **Status:** validated
- **Slug:** `s3_connectors`. The spec is `specs/s3_connectors.md` and its tests are
  `tests/test_s3_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/disk_connectors.md](disk_connectors.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [backlog idea 3](../docs/backlog.md#3-s3-reader-and-writer-with-boto3),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for objects in Amazon S3 and in S3-compatible stores, built on Spark's file data
sources, with the contract of `DiskReader` and `DiskWriter`. `S3Reader` lists the objects
directly under a prefix with boto3, on the driver, selects those above the job's watermark
before anything is loaded, and returns a Spark DataFrame over exactly those objects.
`S3Writer` writes a Spark DataFrame under a prefix, appending or replacing as the calling
callback chooses when it calls `write`. Spark writes straight to the URI, so the writer needs
no boto3. An object's watermark comes from its name, through a pattern the job gives, or else
from its `LastModified` time, to the second.

boto3 does control work only: it lists. It goes into a new extra, `ingestio[s3]`, and is needed
only when the job does not pass a `client` of its own. Spark's access to S3 is the
environment's concern, never the connector's.

This spec is also the model for the other stores of the session that are listed by a native
client and loaded by Spark (GCS, Databricks Volumes). The section
[Differences from the disk connectors](#differences-from-the-disk-connectors) lists what a store
changes; everything else stays as in the disk connectors.

The choices that were open questions are marked **(Q n)**. The spec is written with the
recommended option of each one, which the main session took under the maintainer's delegation
(see [Open questions](#open-questions)).

## Motivation

- **The request.** The maintainer asked for five connectors on 2026-10-23, each with a reader
  and a writer: S3, GCS, Unity Catalog, Databricks Volumes and BigQuery, one spec per system.
  This is the S3 one ([backlog idea 3](../docs/backlog.md#3-s3-reader-and-writer-with-boto3)).
- **Read.** Producers drop files into an S3 prefix. A job must read only the objects it has not
  ingested yet, and must pick them before Spark loads anything. For an object store the unit
  the source can filter is the object, so selecting objects before loading is how this reader
  filters "inside the source", as [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)
  requires: Spark never opens an object that was not selected.
- **What exists does not fit.** `DiskReader` rejects every URI, and on Databricks it reads only
  volumes. `spark.read.load(prefix)` reads every object. Spark's `modifiedAfter` option filters
  inside Spark's own listing, so the selection is not frozen, and it cannot serve name
  watermarks. Databricks Auto Loader and Structured Streaming's file source keep the objects
  they processed in a checkpoint: they are streaming, which is deferred
  ([ADR 0012](../docs/decisions/0012-streaming-deferred.md)), and Auto Loader exists only on
  Databricks.
- **Why a native client lists.** boto3 gives the driver the same view of a prefix that
  `os.scandir` gives `DiskReader` of a directory, in a few requests and without a Spark job per
  read. The disk spec weighed and rejected a listing through Spark's `binaryFile` source (its
  question 3, option c).
- **Write.** Spark writes `s3://` and `s3a://` URIs itself. The writer is the `DiskWriter` call
  chain with a URI, with the same limits on atomicity for plain files; Delta is atomic through
  its log.

## Fit with the philosophy

- **Concept: connector.** `S3Reader` satisfies `Reader[DataFrame]` and `S3Writer` satisfies
  `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). S3 is the external system.
  `write(data, *, replace=False)` adds a keyword-only argument with a default, so `write(data)`
  stays a valid call and the method still matches the protocol. `ReadSource` and `WriteTarget`
  use both connectors unchanged.
- **The client is not a third concept.** It is boto3's own object, built by the job or by the
  SDK's defaults, and passed in like `spark`. It carries the credentials, the region and the
  endpoint, which ADR 0013 keeps out of the job configuration. The reader calls one operation on
  it, the listing. Nobody extends it, and it performs no action of the pipeline.
- **Format: given, not decided.** As the disk spec explains in its own "Fit with the
  philosophy", applying the format the job names is how a file connector talks to its system.
  The format is a required argument with no default; the connector never infers it, adds no
  column, casts nothing and filters no rows.
- **Write strategy.** Append and replace are the two primitive ways Spark puts a batch under a
  prefix, the `append` and `overwrite` save modes. The writer offers both through one method
  and picks neither: the calling callback chooses at call time. Options that would turn a
  replace into a partial replace are reserved.
- **Architecture.** The connectors create no bucket, no table and no catalog entry, lay out no
  partitions, and configure neither Spark nor Hadoop. The scheme of the URI is the job's
  **(Q2)**. Spark's access to S3 is the environment's.
- **The reader decides no strategy.** Whether a run is incremental, from which value, and where
  an object's watermark comes from, are the job's decisions. The reader turns them into a
  selection of objects.
- **Essential by default.** Nothing is caught to carry on: every failure propagates out of
  `read()` or `write()` and fails the callback that called them.
- **Simplicity.** Two small classes in one module. No base class, no registry, no staging, no
  new protocol, no new callback. The reader's own logic is one listing and a comparison, with
  the selection rules of `DiskReader`.
- **Compose, do not impose.** The job builds the connectors from its configuration, and builds
  its own client when it needs one. The connector itself reads no file, no environment variable
  and no secret store; the default client follows boto3's own lookup **(Q6)**.
- **No dependencies in the core.** boto3 goes into the `s3` extra and is imported only inside
  the functions that use it. pyspark stays a prerequisite. `ingestio` and
  `ingestio.connector.all` import without either.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `S3Reader`: reads of the objects directly under one prefix, all of them or only those above a
  watermark, selected before loading and frozen by `read()`. The watermark comes from the
  object's name through a pattern, or from its `LastModified` time, to the second;
- the `client` argument, and the default client created at each read;
- `S3Writer`: append (`write(data)`) and replace (`write(data, replace=True)`) through Spark's
  file writers, straight to the URI;
- the rules for URIs, formats, watermark values and options, and the reserved options;
- the `s3` extra, and what the main session adds to `pyproject.toml` and `uv.lock`;
- the documentation of what a write guarantees, of the environments and of the limits;
- the test doubles, and integration tests that run only when the environment provides S3.

Out of scope:

- the job configuration file and recipes, as in the disk spec;
- computing, persisting or advancing the watermark, and exposing which objects `read()`
  selected;
- recursive listings, sub-prefixes and partitioned input layouts, and partitioned output
  (`partitionBy`);
- reading table formats (Delta, Iceberg, Hudi), writing to catalog tables, and any DDL;
- a watermark column, that is filtering rows inside the objects;
- streaming sources: Auto Loader (`cloudFiles`), S3 event notifications, Structured Streaming's
  file source, checkpoints;
- a staging mechanism, or any atomicity beyond Spark's and the format's;
- moving, archiving, tagging or deleting source objects; uploading or downloading objects with
  boto3;
- reading a single object given as `path`;
- object versions, S3 Select, byte ranges, requester-pays buckets, `ExpectedBucketOwner`,
  access point ARNs, multi-region access points;
- creating buckets; configuring Spark's or Hadoop's access to S3 (credentials, endpoints,
  committers);
- a `boto3.Session` argument, or credentials as connector arguments: the job builds a client;
- Spark's other save modes, `ignore` and `errorifexists`;
- engines other than Spark;
- changes to CI, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/s3.py` **(Q1)**, exported from
`ingestio.connector.all` only (not from `ingestio`):

```python
from ingestio.connector.all import S3Reader, S3Writer
```

```python
class S3Reader:
    """Reads the objects under an S3 prefix into a Spark DataFrame, in the format it is given.

    Args:
        path: Prefix that holds the objects, as a URI: `s3://<bucket>/<prefix>` or
            `s3a://<bucket>/<prefix>`. Spark receives the scheme as written. The
            configuration key of the same name.
        format: Name of the Spark data source the objects are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of an object matches, with
            one group that captures the object's watermark. The name is the part of the key
            after the prefix. Without it, an object's watermark is its `LastModified` time, to
            the second. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only objects above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the objects, as a `StructType` or a DDL string. Spark infers it
            when omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: boto3 S3 client that lists the objects, such as
            `boto3.client("s3", region_name=..., endpoint_url=...)`. When omitted, each read
            creates `boto3.client("s3")`, which needs the `s3` extra.
        spark: Session to read with. The active session when omitted.
    """

    def __init__(
        self,
        path: str,
        format: str,
        *,
        watermark_pattern: str | None = None,
        watermark_value: str | datetime | None = None,
        schema: StructType | str | None = None,
        options: Mapping[str, str | int | float | bool] | None = None,
        client: S3Client | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the objects the job needs."""
```

```python
class S3Writer:
    """Writes a Spark DataFrame as objects under an S3 prefix, in the format it is given.

    Args:
        path: Prefix to write under, as a URI: `s3://<bucket>/<prefix>` or
            `s3a://<bucket>/<prefix>`. Spark receives the scheme as written.
        format: Name of the Spark data source to write with, such as `parquet`, `csv` or
            `delta`.
        options: Spark data source write options passed through, such as `compression` or
            `header`.
    """

    def __init__(
        self,
        path: str,
        format: str,
        *,
        options: Mapping[str, str | int | float | bool] | None = None,
    ) -> None: ...

    def write(self, data: DataFrame, *, replace: bool = False) -> None:
        """Write every row of `data` as objects under the prefix.

        Args:
            data: The rows to write.
            replace: Remove every object under the prefix first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame`, `SparkSession` and `StructType` are `pyspark.sql.DataFrame`,
  `pyspark.sql.SparkSession` and `pyspark.sql.types.StructType`. `datetime` is
  `datetime.datetime`, and `Mapping` is `collections.abc.Mapping`.
- `S3Client` is `mypy_boto3_s3.client.S3Client`, the type that `boto3.client("s3")` has under
  boto3's type stubs **(Q5)**. It is imported under `TYPE_CHECKING` only. At run time the
  reader calls only `client.get_paginator("list_objects_v2")` on it.
- `path` is a `str`. `os.PathLike` is not accepted, because `pathlib` collapses the `//` of a
  URI.
- `path` and `format` are required, so they are positional; every option is keyword-only.
  `format` shadows a builtin, which ruff's rule A002 reports: `# noqa: A002`, as in
  `connector/local.py`.
- `S3Writer` has a single write method, `write`. `replace` is keyword-only and defaults to
  `False`, so `S3Writer.write` satisfies `Writer[DataFrame]` structurally. `WriteTarget`, which
  calls `write(data)`, always appends.
- New installable extra: `ingestio[s3]`.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL and disk connectors, and everything exported by `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration, keeps
its own watermark, builds its own client, and advances the watermark from what it read.

```python
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
# unix_seconds truncates to the second, as the reader does.
orders = process.state["orders"]
if not orders.isEmpty():
    seconds = orders.select(F.max(F.unix_seconds("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime.fromtimestamp(seconds, UTC)
```

A job against an S3-compatible store passes, for instance,
`boto3.client("s3", endpoint_url="http://minio.internal:9000")`, and configures Spark's own
endpoint in the session, which is the environment's concern.

## Behaviour

### Building the connectors

Building a connector validates its arguments and does nothing else, with one exception: a
reader built without `client` checks that boto3 can be imported (see
[The client and the extra](#the-client-and-the-extra)). Building creates no client, accesses no
attribute of `client` or `spark`, and touches neither Spark nor S3. Validation runs in the
order of the arguments, and the import check comes last.

#### Paths (Q2)

Both connectors take `path` apart the same way:

1. `path` must be a `str` that starts with `s3://` or `s3a://`, in lowercase. Anything else
   raises `ValueError("path must be an s3:// or s3a:// URI")`. This rejects the empty string,
   local paths, other schemes (`gs://`, `s3n://`, `https://`), `S3://`, `s3:/...`, and every
   non-`str`, such as a `pathlib.Path`.
2. The **bucket** is the text between `://` and the next `/`, or the end. When it is empty,
   `ValueError("path must name a bucket")`. The bucket is not validated further: S3 rejects a
   name it does not know when the reader lists, and Spark when it reads or writes.
3. The **prefix** is the rest, after that `/`, with every trailing `/` removed. When it is not
   empty and one of its `/`-separated segments is empty, `.` or `..`, then
   `ValueError("path must not contain empty, '.' or '..' segments")`. S3 keeps such keys as
   they are, while Hadoop's paths collapse `//` and resolve `.` and `..`, so the listing and
   Spark would name different objects.
4. The connectors keep the **normalised path**: `<scheme>://<bucket>`, followed by
   `/<prefix>` when the prefix is not empty. `s3://lake/landing/orders/` becomes
   `s3://lake/landing/orders`, and `s3a://lake/` becomes `s3a://lake`.
5. The **writer** also needs a prefix: an empty prefix, the bucket root, raises
   `ValueError("path must name a prefix inside the bucket")`, because `replace=True` would
   remove every object of the bucket. The reader accepts the bucket root.

The scheme is passed to Spark exactly as written. On Databricks, `s3://` is the usual one; on
open-source Spark with hadoop-aws, `s3a://`. The connector does not know where it runs, and does
not choose for the job.

#### Formats

`format` is passed to Spark as given. An empty `format` raises
`ValueError("format must not be empty")`. Spark looks the format up when it reads or writes, and
an unknown one fails there. The reader rejects `delta`, compared case-insensitively, with
`ValueError("format '<format>' cannot be read file by file")`, where `<format>` is the value as
written: a Delta table's log decides which of its objects hold live data. The writer accepts
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
  | `S3Reader` | `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`, `modifiedAfter` |
  | `S3Writer` | `replaceWhere`, `partitionOverwriteMode` |

#### The client and the extra

- `client` is not validated, and building never accesses it.
- When `client` is `None`, building imports boto3, only to check that the extra is installed.
  When that import fails, the reader raises
  `ImportError("S3Reader needs boto3: pip install 'ingestio[s3]'")`, chained to the original
  error. This is rule 3 of the [pattern for extras](../docs/concepts/connectors.md#extras).
- When `client` is given, the reader never imports boto3, neither when it is built nor when it
  reads: the job's client is all it needs.
- The writer never imports boto3 and has no client.

#### `repr`

`repr` shows the normalised path and the format, never the client:

- `S3Reader(path='s3://lake/landing/orders', format='csv')`
- `S3Writer(path='s3://lake/orders', format='parquet')`

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps, in order:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()`, at each `read()`. The reader never creates a session.
   When none is active, PySpark's error propagates, and nothing is listed.
2. **Client.** The reader uses `client` when it is given, as it is, for every read. Otherwise it
   calls `boto3.client("s3")`, with that one positional argument and nothing else, looked up as
   an attribute of the `boto3` module at each `read()`. Each such read creates a new client;
   the reader keeps none and closes none.
3. **Listing.** See [The listing](#the-listing).
4. **Candidates.** See [Candidates](#candidates).
5. **Watermarks.** The reader takes each candidate's watermark, as described in
   [The watermark of an object](#the-watermark-of-an-object).
6. **Selection.** The reader selects the candidates whose watermark is strictly greater than
   `watermark_value`, or every candidate when `watermark_value` is `None`.
7. **Load.** The reader loads the selection, or builds an empty DataFrame when the selection is
   empty.

#### The listing

The **listing prefix** is the prefix followed by `/`, or `""` at the bucket root. The reader
makes one listing per `read()`, on the driver:

```text
client.get_paginator("list_objects_v2").paginate(Bucket=<bucket>, Prefix=<listing prefix>, Delimiter="/")
```

- These three keyword arguments, exactly, and no other argument.
- The reader iterates over every page once, in order, before anything is loaded. boto3 sends
  each request as the iteration reaches its page, and follows the continuation tokens itself.
- From each page it takes the entries of `Contents`, when the page has one, and from each entry
  its `Key` and `LastModified`. A page without `Contents` holds no object.
- `CommonPrefixes`, the "subdirectories" that the delimiter groups, are ignored, with
  everything under them. The listing is flat.
- The reader calls no other operation: it opens, downloads and inspects no object. It never
  gives the client to Spark.

A prefix that holds no object gives an empty listing, which S3 does not distinguish from a
prefix that does not exist: the selection is then empty **(Q4)**. A bucket that does not exist,
or a listing that S3 denies, fails with boto3's error.

#### Candidates

The **name** of an entry is its key with the listing prefix removed: for the key
`landing/orders/a.csv` under `s3://lake/landing/orders`, the name is `a.csv`. An entry is a
candidate when its name:

- is not empty. The key that equals the listing prefix, such as `landing/orders/`, is the
  "directory marker" that consoles and some tools create;
- contains no `/`. S3 never returns such a key with the delimiter, but a store that ignores the
  delimiter would return the objects and markers of sub-prefixes (`sub/d.csv`, `sub/`), and the
  listing stays flat;
- starts with neither `_` nor `.` and does not end with `._COPYING_`. This is Spark's rule for
  hidden and in-progress files, as in `DiskReader`. It leaves out `_SUCCESS`, `_metadata`,
  `_common_metadata`, `.crc` files, and the `_started_*` and `_committed_*` markers that commit
  protocols write.

Every storage class is a candidate. The reader does not look at `StorageClass`, `Size` or any
other field.

#### The watermark of an object

- **With `watermark_pattern`.** As in `DiskReader`: the pattern is matched against the whole
  name, not the key, with `re.fullmatch`, and the watermark is the text its group captured. A
  candidate is not read when its name does not match, or when its group took no part in the
  match. Watermarks compare as Python strings, code point by code point, and are never
  converted: `"9"` is above `"10"`.
- **Without `watermark_pattern` (Q3).** The watermark is the entry's `LastModified`, a
  timezone-aware `datetime` as boto3 returns it, **truncated to whole seconds**, exactly and
  without floating point. It is compared with `watermark_value` as an instant, whatever the
  value's zone. The name plays no part.
  - S3 keeps `LastModified` to the second; its listings show `.000` milliseconds. Some
    S3-compatible stores list finer times, and the truncation brings them to the same rule.
  - Spark's `_metadata.file_modification_time` comes from the file status that Hadoop gives
    Spark, to the second or finer, and never below the second the reader uses. On S3 it is
    that very second. So a job that takes its next watermark from that column, as in the
    [usage sketch](#usage-sketch), never selects its last object again, on S3 and on stores
    that list finer times than they report to Spark.
  - The value itself is not truncated: an object at `09:00:00` is above `08:59:59.999999` and
    not above `09:00:00.5`.

#### Load

When the selection holds at least one object, `read()` makes exactly one load:

- `spark.read.format(format)`;
- `.schema(schema)` when `schema` is given, with the very object given;
- the user's `options`, as given, and no other option;
- `.load(paths)`. `paths` is a `list` of `str`, one per selected object:
  `<normalised path>/<name>`, such as `s3://lake/landing/orders/a.csv`, or `s3a://lake/a.csv`
  at the bucket root. The scheme is the one the job wrote. The list is sorted by name in
  code-point order, and is a list even for one object.

`read()` returns the load's DataFrame unchanged. The reader adds no column, filter, cast or
projection.

**The selection is frozen.** Spark receives the selected objects, never the prefix, so every
action on the DataFrame reads those objects and no other. Objects that appear after `read()`
are left for the next run, whatever their watermark.

**The selection is not exposed.** The reader keeps no record of what it selected. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column
(`file_name`, `file_path`, `file_modification_time`).

#### When nothing is selected (Q4)

The selection is empty when the listing holds no object, holds only entries that are not
candidates, holds no name that matches the pattern, or holds no watermark above the value.
Then `read()` makes no load. It returns `spark.createDataFrame([], schema)`, with `schema` when
it is given and otherwise a `pyspark.sql.types.StructType` with no fields. This DataFrame has no
`_metadata` column. A callback that wants to skip an empty batch checks for itself and raises
`CancelStepException`.

#### Errors

Nothing is caught. Errors of PySpark, of `boto3.client`, of the listing (botocore's
`ClientError` for `NoSuchBucket` or `AccessDenied`, `NoCredentialsError`,
`EndpointConnectionError`) and of Spark propagate out of `read()` unchanged, as the same object.
When the listing fails, nothing is loaded. The connectors write no log lines.

### Writing

`write(data, *, replace=False)` makes exactly one call chain, the one of `DiskWriter`:

```text
data.write.format(<format>).options(<options>).mode(<mode>).save(<path>)
```

- `<mode>` is `"append"` when `replace` is `False` and `"overwrite"` when it is `True`;
- `<options>` are the user's, as given, and no other;
- `<path>` is the normalised path, a `str`, with the scheme the job wrote.

**Operations.**

- `write(data)` appends: Spark adds new objects under the prefix and leaves the existing ones as
  they are. `WriteTarget` calls `write(data)`, so it appends.
- `write(data, replace=True)` replaces: the prefix ends up holding exactly the rows of `data`.
  With a plain file format, Spark deletes every object under the prefix, objects it did not
  write included, and then writes the new ones. With Delta, the overwrite is a new version of
  the table.
- The calling callback chooses at call time. The configuration has no mode, and there is no
  `replace()` method.
- The object names are Spark's (`part-<n>-<uuid>-c000...`), so an `S3Reader` that reads them
  back takes their watermark from their `LastModified`. Spark may also leave directory markers
  and `_SUCCESS`, which the reader ignores.

**What the writer does not do.** It reads only `data.write` from the DataFrame and evaluates it
once, through the save. It takes no session and no client, imports no boto3, and lists,
creates and deletes nothing itself. It calls no other method of Spark's writer: no
`partitionBy`, `bucketBy`, `sortBy`, `saveAsTable` or `insertInto`. An empty DataFrame goes
through the same call. `write` returns `None`, and every error propagates unchanged, as the
same object.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what Spark's commit
protocol, the environment's committer and the format guarantee, and nothing more:

- **Append, plain file formats, open-source Spark.** S3 has no rename, and Hadoop's S3A
  emulates one by copying and deleting. With Hadoop's default `FileOutputCommitter`, tasks
  write under `_temporary/` inside the prefix, which the reader ignores, and the job commit
  copies each file to its final key, one by one. A reader that lists during the commit sees
  part of the batch, and a commit that fails midway leaves part of it. Hadoop's S3A committers,
  when the session enables them, make the objects visible only at the job commit, but still one
  object at a time. Choosing a committer is the environment's concern.
- **Replace, plain file formats.** Spark deletes the objects under the prefix before it writes.
  If the write then fails, the prefix is left empty or holding part of the new rows, and the
  previous contents are lost.
- **Delta.** An append or an overwrite is one commit in the table's log, so readers through
  Delta see the table before the commit or after it. On open-source Spark, Delta Lake's
  default S3 log store is safe only when every write to a table comes from one Spark driver at
  a time; concurrent writers on several drivers need Delta's multi-cluster setup. Databricks
  coordinates Delta commits on S3 itself.
- **On Databricks,** the runtime uses its own commit protocol for file formats. What it
  guarantees is the runtime's, and this spec does not rely on it.

### Differences from the disk connectors

What a store listed by a native client changes. Everything not in this table is as in the
[disk spec](disk_connectors.md), and the specs of GCS and Volumes are expected to fill in the
same rows:

| Point | `DiskReader` / `DiskWriter` | `S3Reader` / `S3Writer` |
|---|---|---|
| Location | absolute local path, `str` or `PathLike` | `s3://` or `s3a://` URI, a `str`, scheme passed to Spark as written |
| Rejected locations | relative paths and URIs | other schemes, no bucket, empty, `.` or `..` segments; the bucket root for the writer |
| Listing | `os.scandir` of the directory | boto3 `ListObjectsV2` paginator, `Delimiter="/"`, every page |
| Name of an entry | the file name | the key after the listing prefix |
| Not candidates, besides hidden names | directories, broken links | empty names (directory markers), names with `/`, common prefixes |
| Modification-time watermark | `st_mtime_ns`, truncated to milliseconds | `LastModified`, truncated to seconds |
| Missing location | `FileNotFoundError` | empty selection; a missing bucket fails in boto3 |
| Native client | none | `client`, or `boto3.client("s3")` created at each read |
| Extra | none | `ingestio[s3]`, needed only without `client` |
| Writer | the same chain | the same chain, with the URI; no boto3 |

### The `s3` extra (Q5)

Following the [pattern for extras](../docs/concepts/connectors.md#extras):

1. `[project.optional-dependencies]` gains `s3 = ["boto3>=1.34"]`. `dependencies` stays empty.
2. `connector/s3.py` imports boto3 only inside the functions that use it, and `S3Client` under
   `TYPE_CHECKING`.
3. A reader without `client` raises the `ImportError` above when boto3 is missing. With a
   `client`, and for the writer, the extra is not needed.
4. pyspark stays a prerequisite, in no extra.
5. The `dev` group gains `boto3>=1.34` and `boto3-stubs[s3]>=1.34`. The stubs give mypy strict
   the types of `boto3`, `botocore.exceptions` and `mypy_boto3_s3.client.S3Client`; without
   them, `import boto3` fails mypy strict as an untyped module. No test-only package such as
   moto is added **(Q7)**.

The main session adds these to `pyproject.toml` and refreshes `uv.lock` after approval, as it
did for PostgreSQL. Those files are outside every stage's write scope.

### Where it lives

- `src/ingestio/connector/s3.py` **(Q1)**: `S3Reader` and `S3Writer`, with a module docstring
  and a sorted `__all__`.
- `src/ingestio/connector/all.py`: exports the two classes, `__all__` sorted.
- `pyproject.toml` and `uv.lock`: as in [The `s3` extra](#the-s3-extra-q5), by the main session.
- **Implementation note, not public API.** The rules that `S3Reader` shares with `DiskReader`
  (format and option checks, watermark validation, hidden names, the comparison, the sort, the
  empty DataFrame) may be factored into private helpers, for instance a private module
  `src/ingestio/connector/_files.py` that GCS and Volumes would use too. `DiskReader` and
  `DiskWriter` then keep their behaviour and messages exactly, and
  `tests/test_disk_connectors.py` passes unchanged.
- No change to `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or
  `connector/core.py`.

### Supported environments

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- **Databricks classic compute** on Databricks Runtime 15.4 LTS or later, in dedicated or
  standard access mode. Spark reaches S3 through Unity Catalog external locations, or an
  instance profile, as the workspace is set up; the job uses the scheme of its external
  location, usually `s3://`. Only the DataFrame API is used, never `sparkContext` or `_jvm`.
- **The listing does not go through Unity Catalog.** boto3 needs AWS credentials of its own on
  the driver, with `s3:ListBucket` on the bucket. What the default client finds depends on the
  cluster; when it finds nothing usable, the job builds a `client` from credentials it obtains
  itself, such as secrets.
- **Open-source Spark 3.5 or later**, on a Java version that release supports (17 or 21 for
  Spark 4), with `hadoop-aws` in the version of Spark's Hadoop and its AWS SDK on the
  classpath, and `s3a://` URIs. `s3://` works there only when the session maps it, with
  `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`. Credentials, endpoint and
  committer go in the Hadoop configuration (`spark.hadoop.fs.s3a.*`).
- **S3-compatible stores** (MinIO, Cloudflare R2, Ceph): nothing in the design excludes them,
  but they are not tested. The store must support `ListObjectsV2` with a delimiter. The job
  points the client at it with `endpoint_url`, and Spark through its own configuration.
- **Databricks serverless compute:** no claim; not tested.
- **Formats** outside Spark's built-ins need their package in the session, as for the disk
  connectors: `delta-spark` on open-source Spark, `spark-avro`.
- **Privileges.** The listing needs `s3:ListBucket`. Spark's reads need `s3:GetObject`; its
  writes need `s3:PutObject`, and `s3:DeleteObject` for replaces and for the copies of
  commits. On Databricks, through the external location's privileges.

### Known limitations

These are documented, not solved here:

- **Writes are only as atomic as Spark, the committer and the format make them** (see
  [Atomicity](#writing)). A failed replace of plain files loses the previous contents. A reader
  of the prefix during a write can see part of it.
- **`replace=True` deletes everything under the prefix** with a plain file format, objects Spark
  did not write included. Never point a writer that replaces at a prefix that holds anything
  else.
- **Delta with several writers on open-source Spark** needs Delta's multi-cluster log store.
- **Late objects.** A run that advances its watermark to the highest one it read never reads an
  object that appears later with a watermark at or below it:
  - by name: a producer that delivers objects out of order;
  - by `LastModified`: for a multipart upload, which boto3's `upload_file` uses for large
    files, S3 sets it to the time the upload was initiated, not the time it completed. An
    upload that completes after a newer object was read is never read;
  - objects that share the last second read and appear after the read are skipped, because the
    comparison is strict and times are whole seconds.

  S3 stamps `LastModified` itself, so producers' clocks do not matter, and an object copied or
  moved into the prefix gets a new time.
- **Objects in progress.** S3 makes an object visible only when its upload completes, so the
  reader never sees part of an object. A Spark job that writes plain files into the prefix
  being read can still expose part of its batch (see Atomicity), and on Databricks the
  runtime's commit protocol can write data files at their final keys before it marks them
  committed. The reader lists objects; it does not read commit markers.
- **The selection is frozen, not the contents.** An object overwritten before an action is read
  as it is then, or makes the action fail. A selected object that is deleted makes the action
  fail, unless the job passes Spark's `ignoreMissingFiles`.
- **Archived objects.** An object in an archive storage class (S3 Glacier Flexible Retrieval,
  Deep Archive) is selected like any other, and Spark's load fails until it is restored.
- **Glob characters (Q9).** Spark expands the characters `*`, `?`, `[`, `]`, `{`, `}` and `\` in
  the paths it loads. A selected object whose URI holds one can be read as other objects, or
  fail. The reader does not escape them.
- **Two configurations.** The client and Spark are configured separately. A client that lists
  one store or account while Spark reads another is not detected: the load fails, or reads
  other objects with the same keys.
- **Name watermarks compare as text,** as in `DiskReader`.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing prefix, a typo included, reads as empty.
- **Large prefixes.** Each `read()` lists every object directly under the prefix, 1000 keys per
  request, and Spark then requests the status of each selected object before reading it.
- **Table formats.** The reader rejects `delta`, but it does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM,
without network and without credentials. The doubles live in `tests/test_s3_connectors.py`.

- **Fake S3 client.** It records every attribute accessed on it. `get_paginator(name)` records
  the name and returns a fake paginator, which also records every attribute accessed on it.
  `paginate(*args, **kwargs)` records a *listing*, with its arguments, and returns an iterable
  over the pages the test set. A page is a `dict` shaped like boto3's: `Contents`, a list of
  entries with `Key`, `LastModified` and `Size`; `CommonPrefixes`, a list of
  `{"Prefix": ...}`; `KeyCount` and `IsTruncated`. A page may lack `Contents` or
  `CommonPrefixes`, as boto3's pages do when they are empty. The fake records how many pages
  were taken, and can raise an error the test gives from `get_paginator` or when the
  iteration reaches a given page.
- **`LastModified`** values are timezone-aware datetimes, as botocore returns them; tests use
  `datetime.UTC`.
- **The factory.** Criteria that mention it replace `boto3.client` with
  `monkeypatch.setattr(boto3, "client", factory)`, where `factory` records the positional and
  keyword arguments of each call and returns the fake client, or raises an error the test
  gives. boto3 is in the `dev` group.
- **Fake Spark session and fake DataFrame:** those of the disk spec's Notes for testing, with
  loads, creates and saves recorded the same way.
- **Missing packages.** Map `pyspark`, `boto3` and `botocore`, with their submodules, to `None`
  in `sys.modules`: in a subprocess for a fresh interpreter, or with `monkeypatch.setitem`.
- `botocore.exceptions.ClientError` can be built without network:
  `ClientError({"Error": {"Code": "NoSuchBucket", "Message": "..."}}, "ListObjectsV2")`.
- Tests use the public API. They pass the fakes as `client=`, `spark=` and DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against S3, or an S3-compatible store, and a local Spark
session, `local[2]`, with `spark.sql.session.timeZone=UTC`. They are marked
`@pytest.mark.integration` and skipped, with a reason, unless the environment variable
`INGESTIO_S3_URL` is set **(Q7)**. Its value is a scratch location, `s3a://<bucket>/<prefix>`.
Each test works under its own unique sub-prefix and deletes it at the end. Credentials come
from the standard AWS environment, for boto3 and for S3A alike. They need Java 17 or 21,
because `uv.lock` holds pyspark 4.2, and `hadoop-aws` for its Hadoop version; how the session
gets it, and how an endpoint for an S3-compatible store is passed, is the test-writer's choice.
On the maintainer's machine they are written blind: its only JDK is Java 27, on which Spark 4.2
does not start, and it has no cloud credentials. They cover:

1. `S3Writer(<sub>, "parquet")`: two calls of `write(df)` leave the rows of both batches, and
   `write(df, replace=True)` then leaves exactly the rows of `df`. A full
   `S3Reader(<sub>, "parquet")` returns them; `_SUCCESS` does not disturb it.
2. CSV round trip: written with `{"header": True}`, read back with the same option and a DDL
   `schema`, with the same rows.
3. Name watermark: CSV objects uploaded by the test with boto3. Only the rows of the objects
   above the value are returned. An object uploaded after `read()`, with a higher name, is
   absent from a second `count()` on the same DataFrame.
4. `LastModified` watermark: the test uploads `a.csv`, waits until the next second, uploads
   `b.csv`, and takes `a.csv`'s `LastModified` from a listing. A reader with that value returns
   only `b.csv`'s rows. The highest `unix_seconds(_metadata.file_modification_time)` equals
   `b.csv`'s `LastModified` in seconds, and a reader built with that instant, as an aware UTC
   datetime, selects nothing.
5. Empty selection under a sub-prefix with no object: with a `schema`, a DataFrame with its
   columns and no rows; without one, no columns and no rows.
6. A directory marker `<sub>/`, and objects under `<sub>/nested/`, are not read.
7. A reader built without `client` lists through the default client.
8. A write whose computation fails, through a Python UDF that raises, raises out of `write`.
9. `write(df, replace=True)` on a sub-prefix that holds `keep.txt`, uploaded by the test,
   removes it. This confirms the documented limitation.

## Acceptance criteria

The criteria follow the recommended option of every question. A criterion that depends on one
names it.

Unless a criterion says otherwise:

- `spark` is the fake session, `df` a fake DataFrame and `client` the fake S3 client, from
  [Notes for testing](#notes-for-testing);
- `URI` is `"s3://lake/landing/orders"`, and `u(name)` is `"s3://lake/landing/orders/" + name`;
- "the store holds `name`" means that the fake listing returns, in `Contents`, an entry whose
  `Key` is `"landing/orders/" + name` and whose `LastModified` is `T0`, or the time `t` in
  "`name` at `t`". "The store holds the key `k`" gives the whole key. "The common prefix `p`"
  is an entry `{"Prefix": "landing/orders/" + p}` in `CommonPrefixes`. Entries come in the
  order the criterion lists them, on one page unless it says otherwise;
- `T0` is `datetime(2026, 10, 21, 9, 0, tzinfo=UTC)`;
- the reader is `S3Reader(URI, "csv", client=client, spark=spark)`, with whatever the
  criterion adds;
- the writer is `S3Writer("s3://lake/orders", "parquet")`;
- "the paths" of a load are the value its `load()` received;
- "exactly the options" means these keys and no others, compared case-insensitively unless
  the criterion says the case matters.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import S3Reader, S3Writer` runs, then it succeeds. Both names
  are in `ingestio.connector.all.__all__` and in `ingestio.connector.s3.__all__`, and neither
  is in `ingestio.__all__`. Depends on Q1.
- **AT-002**: Given an interpreter where `import pyspark`, `import boto3` and
  `import botocore` raise `ImportError`, when `ingestio.connector.all` is imported, then the
  import succeeds. `S3Reader(URI, "csv", client=<fake client>, spark=<fake session>)` can be
  built, and with the store holding `a.csv` its `read()` returns the DataFrame of the fake's
  load. `S3Writer("s3://lake/orders", "parquet")` can be built, and `write(<fake DataFrame>)`
  records one save.
- **AT-003**: Given an interpreter where `import boto3` raises `ImportError`, when
  `S3Reader(URI, "csv")` or `S3Reader(URI, "csv", spark=spark)` is built, then it raises
  `ImportError` with the message `S3Reader needs boto3: pip install 'ingestio[s3]'`, whose
  `__cause__` is an `ImportError`. `S3Reader(URI, "csv", client=client)` and the writer are
  built without error.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `s3` is among its `Provides-Extra` values,
  one `Requires-Dist` names the `boto3` project and carries an `extra == "s3"` marker, and
  every `Requires-Dist` carries an `extra == "<name>"` marker for some extra. Depends on Q5
  and Q8.

### Building the connectors

- **AT-005**: Given `path` in `""`, `"lake/landing/orders"`, `"/Volumes/main/landing/orders"`,
  `"gs://lake/landing/orders"`, `"s3n://lake/landing/orders"`, `"S3://lake/landing/orders"`,
  `"s3:/lake/landing/orders"`, `"https://lake.s3.amazonaws.com/landing/orders"` and
  `Path("s3://lake/landing/orders")`, when `S3Reader(path, "csv", client=client)` or
  `S3Writer(path, "csv")` is built, then it raises
  `ValueError("path must be an s3:// or s3a:// URI")`. Depends on Q2.
- **AT-006**: Given the `path` on the left, when `S3Reader(path, "csv", client=client)` and
  `S3Writer(path, "csv")` are built, then each raises `ValueError` with the message on the
  right, or is built when the cell says so:

  | `path` | `S3Reader` | `S3Writer` |
  |---|---|---|
  | `"s3://"`, `"s3a://"`, `"s3:///landing/orders"` | `path must name a bucket` | `path must name a bucket` |
  | `"s3://lake//orders"`, `"s3://lake/landing//orders"`, `"s3://lake/./orders"`, `"s3://lake/landing/../orders"`, `"s3://lake/landing/orders/.."` | `path must not contain empty, '.' or '..' segments` | the same |
  | `"s3://lake"`, `"s3a://lake/"` | built | `path must name a prefix inside the bucket` |
  | `"s3://lake/landing/orders"`, `"s3a://lake/landing/orders/"`, `"s3://my.bucket-01/a b/ç"`, `"s3://lake/_staging/orders"` | built | built |

  Depends on Q2.
- **AT-007**: Then:
  - `repr(S3Reader("s3://lake/landing/orders/", "csv", client=client))` is
    `S3Reader(path='s3://lake/landing/orders', format='csv')`;
  - `repr(S3Reader("s3://lake/landing/orders", "csv", client=client,
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261021"))` is the same;
  - `repr(S3Reader("s3a://lake/", "json", client=client))` is
    `S3Reader(path='s3a://lake', format='json')`;
  - `repr(S3Writer("s3://lake/orders//", "parquet"))` is
    `S3Writer(path='s3://lake/orders', format='parquet')`.
- **AT-008**: Given `format=""`, when either connector is built, then it raises
  `ValueError("format must not be empty")`. Given `"delta"` or `"Delta"`, when
  `S3Reader(URI, fmt, client=client)` is built, then it raises `ValueError` with the message
  `format '<fmt>' cannot be read file by file`, with the format as given, while
  `S3Writer("s3://lake/orders", fmt)` is built. `"parquet"`, `"CSV"` and
  `"com.example.source"` are accepted by both.
- **AT-009**: Given the `watermark_pattern` on the left, when
  `S3Reader(URI, "csv", client=client, watermark_pattern=pattern)` is built, then it raises
  `ValueError` with the message on the right:

  | `watermark_pattern` | Message |
  |---|---|
  | `"("` | starts with `invalid watermark_pattern: `, and the `__cause__` is a `re.error` |
  | `r"orders_\d{8}\.csv"` | `watermark_pattern must have exactly one group` |
  | `r"(\d{4})(\d{4})\.csv"` | `watermark_pattern must have exactly one group` |
  | `""` | `watermark_pattern must have exactly one group` |

  `r"orders_(\d{8})\.csv"` and `r"(?:orders|sales)_(?P<day>\d{8})\.csv"` are accepted.
- **AT-010**: Given the arguments on the left, when `S3Reader(URI, "csv", client=client, ...)`
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
  `datetime(2026, 10, 21, 6, 0, tzinfo=timezone(timedelta(hours=-3)))` are accepted.
- **AT-011**: Given `key` in `path`, `PATHS`, `pathGlobFilter`, `recursivefilelookup`,
  `modifiedBefore` and `ModifiedAfter`, when
  `S3Reader(URI, "csv", client=client, options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as
  given. Given `key` in `path`, `Paths`, `replaceWhere` and `PartitionOverwriteMode`, the same
  holds for `S3Writer("s3://lake/orders", "parquet", options={key: "x"})`. The reader accepts
  `{"header": True, "inferSchema": "true", "mergeSchema": False}`, and the writer accepts
  `{"compression": "zstd", "header": True}`.
- **AT-012**: Given the factory, when these are built, then each succeeds, no attribute of
  `spark` or of `client` is accessed, no listing is recorded, and the factory is not called:
  - `S3Reader(URI, "csv", client=client, spark=spark)`;
  - `S3Reader(URI, "csv", spark=spark)`;
  - `S3Reader(URI, "csv", client=client, spark=spark, watermark_pattern=r"orders_(\d{8})\.csv",
    watermark_value="20261020")`;
  - `S3Reader(URI, "csv", client=client, spark=spark, watermark_value=T0)`;
  - `S3Writer("s3://lake/orders", "parquet")`.

### Reading

- **AT-013**: Given the factory and the store holding `a.csv`, when `reader.read()` runs, then
  `get_paginator` is called once, with exactly the positional argument `"list_objects_v2"`, and
  `paginate` is called once, with no positional argument and exactly the keyword arguments
  `Bucket="lake"`, `Prefix="landing/orders/"` and `Delimiter="/"`. The only attribute accessed
  on `client` is `get_paginator`, the only one accessed on the paginator is `paginate`, and the
  factory is not called. Given `S3Reader("s3a://lake/", "csv", client=client, spark=spark)` and
  the store holding the key `a.csv`, then `paginate` receives `Bucket="lake"`, `Prefix=""` and
  `Delimiter="/"`, and the paths are `["s3a://lake/a.csv"]`. Depends on Q2.
- **AT-014**: Given the store holding, in this order, `b.csv`, `a.csv`, `_SUCCESS`, `_metadata`,
  `.a.csv.crc`, `c.csv._COPYING_`, `_committed_123`, the keys `landing/orders/`,
  `landing/orders/sub/` and `landing/orders/sub/d.csv`, and the common prefixes `sub/` and
  `x.csv/`, when `read()` runs, then exactly one load is recorded. It has the format `csv`, no
  schema, no options, and the paths `[u("a.csv"), u("b.csv")]`, a `list` in this order.
  `read()` returns the very DataFrame that `load()` returned, no attribute of that DataFrame is
  accessed, and `createDataFrame` is not called. Given the store holding only `a.csv`, the
  paths are `[u("a.csv")]`, still a `list`.
- **AT-015**: Given the store holding `pedidos_ç.csv`, `a b.csv` and `2026-10-21T09:00:00.csv`,
  when `read()` runs, then the paths are
  `[u("2026-10-21T09:00:00.csv"), u("a b.csv"), u("pedidos_ç.csv")]`. Given
  `S3Reader("s3a://lake/landing/orders", "csv", client=client, spark=spark)` and the store
  holding `a.csv`, the paths are `["s3a://lake/landing/orders/a.csv"]`: the scheme is the one
  written. Depends on Q2.
- **AT-016**: Given three pages: the first holds `b.csv`, `a.csv` and the common prefix `sub/`,
  the second holds only the common prefix `x/` and no `Contents`, and the third holds `c.csv`,
  when `read()` runs, then `paginate` is called once, the three pages are taken, and the paths
  are `[u("a.csv"), u("b.csv"), u("c.csv")]`.
- **AT-017**: Given the store holding `a.csv`, `schema="id INT, name STRING"` and
  `options={"header": True, "sep": ";"}`, when `read()` runs, then the load carries that very
  schema object and exactly the options `header` = `true` and `sep` = `;`. Given instead
  `schema=StructType([StructField("id", IntegerType())])`, the load carries that very object.
- **AT-018**: Given a reader built with `client` and without `spark`, while
  `pyspark.sql.SparkSession.active` is replaced with a function that raises, then building
  succeeds. When `active` is then replaced with a function that returns the fake session and
  `read()` runs with the store holding `a.csv`, then the load is recorded on the fake. Given
  `active` replaced with a function that raises `err` when `read()` runs, then `read()` raises
  `err` itself and no listing is recorded.
- **AT-019**: Given the factory, `S3Reader(URI, "csv", spark=spark)` and the store holding
  `a.csv`, then building does not call the factory. Each `read()` calls it exactly once, with
  exactly the positional argument `"s3"` and no keyword argument, and the listing is recorded
  on the client it returned: two reads make two calls. Given the factory replaced by another
  after the reader was built, then the replacement is the one called. Given a factory that
  raises `err`, when `read()` runs, then it raises `err` itself, and no listing, load or
  `createDataFrame` call is recorded.
- **AT-020**: Given a reader built while the store holds nothing, when the store is set to hold
  `a.csv` and `read()` runs, then the load's paths are `[u("a.csv")]`. When the store is then
  set to hold `a.csv` and `b.csv` and `read()` runs again, a second listing is recorded, the
  second load's paths are `[u("a.csv"), u("b.csv")]`, and the first load's paths are still
  `[u("a.csv")]`.
- **AT-021**: Given a single page with neither `Contents` nor `CommonPrefixes`, as S3 answers for
  a prefix that holds no object, and then the store holding only `_SUCCESS`, the key
  `landing/orders/` and the common prefix `sub/`, when `read()` runs, then no load is recorded
  and `createDataFrame` is called once. It receives an empty list as data and, as schema, a
  `pyspark.sql.types.StructType` with no fields. `read()` returns what `createDataFrame`
  returned. Given `schema="id INT, name STRING"` in addition, the schema passed is that very
  object. Depends on Q4.
- **AT-022**: Given `err = ClientError({"Error": {"Code": "NoSuchBucket", "Message": "The
  specified bucket does not exist"}}, "ListObjectsV2")`, from `botocore.exceptions`:
  - when the iteration of the pages raises `err` at the first page, or at the second page after
    a first page that holds `a.csv`, or when `get_paginator` raises it, then `read()` raises
    `err` itself, and no load and no `createDataFrame` call is recorded;
  - given a fake whose `load()` raises `err2 = RuntimeError("[PATH_NOT_FOUND]")` and the store
    holding `a.csv`, then `read()` raises `err2` itself;
  - given a fake whose `createDataFrame` raises `err2` and an empty store, then `read()` raises
    `err2` itself.

### Watermark from the object name

In this section `P` is `r"orders_(\d{8})\.csv"`, and the reader is
`S3Reader(URI, "csv", client=client, spark=spark, watermark_pattern=P, ...)`.

- **AT-023**: Given the store holding:
  - `orders_20261019.csv` at `T0 + 2 s`, `orders_20261020.csv` at `T0 + 1 s` and
    `orders_20261021.csv` at `T0`;
  - `customers_20261021.csv`, `orders_latest.csv` and `orders_20261021.csv.bak`;

  when `read()` runs with `watermark_value="20261019"`, then the paths are
  `[u("orders_20261020.csv"), u("orders_20261021.csv")]`. Without `watermark_value`, they are
  the three `orders_<date>.csv` objects, sorted by name.
- **AT-024**: Given the objects and the value on the left, when `read()` runs, then the objects
  on the right are selected. "None" means no load and one `createDataFrame` call.

  | Pattern | Objects in the store | `watermark_value` | Selected, in the order of the paths |
  |---|---|---|---|
  | `P` | `orders_20261019.csv`, `orders_20261020.csv`, `orders_20261021.csv` | `"20261021"` | none |
  | `P` | the same | `"2026102"` | `orders_20261020.csv`, `orders_20261021.csv` |
  | `P` | the same | `""` | all three |
  | `r"part_(\d+)\.csv"` | `part_9.csv`, `part_10.csv` | `"9"` | none |
  | `r"part_(\d+)\.csv"` | the same | `"1"` | `part_10.csv`, `part_9.csv` |

- **AT-025**: Without `watermark_value`:
  - given `watermark_pattern=r"(\d{8})\.csv"` and the store holding `orders_20261021.csv`, then
    nothing is selected, because the whole name must match;
  - given `watermark_pattern=r"landing/orders/orders_(\d{8})\.csv"` and the same store, then
    nothing is selected, because the pattern sees the name, not the key;
  - given `watermark_pattern=r"orders(_\d{8})?\.csv"` and the store holding `orders.csv` and
    `orders_20261021.csv`, then the paths are `[u("orders_20261021.csv")]`;
  - given `watermark_pattern=r".*_(\d{8})\.csv"` and the store holding `_orders_20261022.csv`,
    `.orders_20261022.csv`, the key `landing/orders/sub/orders_20261022.csv` and
    `orders_20261021.csv`, then the paths are `[u("orders_20261021.csv")]`.

### Watermark from `LastModified`

In this section the reader is `S3Reader(URI, "csv", client=client, spark=spark, ...)`, with no
`watermark_pattern`.

- **AT-026**: Given the store holding `c.csv` at `T0`, `b.csv` at `T0 + 1 s` and `a.csv` at
  `T0 + 2 s`, when `read()` runs with the `watermark_value` on the left, then the paths are
  those on the right:

  | `watermark_value` | Paths |
  |---|---|
  | none | `u("a.csv")`, `u("b.csv")`, `u("c.csv")` |
  | `T0 - 1 µs` | `u("a.csv")`, `u("b.csv")`, `u("c.csv")` |
  | `T0` | `u("a.csv")`, `u("b.csv")` |
  | `T0 + 500 ms` | `u("a.csv")`, `u("b.csv")` |
  | `T0 + 1 s` | `u("a.csv")` |
  | `T0 + 2 s` | none: no load, one `createDataFrame` call |

  Depends on Q3.
- **AT-027**: Given the store holding `a.csv` at `T0 + 999 999 µs`, as a store that lists
  times finer than the second would report it, when `read()` runs with the `watermark_value` on
  the left, then `a.csv` is selected or not as on the right:

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
  `"s3://lake/orders"`, a `str`. The only attribute accessed on `df` is `write`. The only
  attributes accessed on the fake writer are `format`, `option` or `options`, `mode` and
  `save`. `write` returns `None`, and the factory is not called.
  `writer.write(df, replace=False)` gives the same.
- **AT-029**: When `writer.write(df, replace=True)` runs, then the save is the one of AT-028,
  except that its mode is `overwrite`.
- **AT-030**: Given `S3Writer("s3a://lake/orders/", "csv", options={"header": True,
  "Compression": "gzip"})`, when `write(df)` runs, then the save has the format `csv`, the path
  `"s3a://lake/orders"`, and exactly the options `header` = `true` and `Compression` = `gzip`,
  with the key `Compression` in this case. Depends on Q2.
- **AT-031**: Given a save that raises `err = RuntimeError("task failed")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, and `save()` was called once.

### Composition

- **AT-032**: Given the store holding `a.csv` and
  `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, when it runs, then
  `process.state["orders"]` is the DataFrame that the load returned, and one save is recorded
  on it, with the mode `append`. The test binds the reader to a variable annotated
  `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`, and mypy strict
  accepts both.
- **AT-033**: Given `caplog` capturing every level and the factory, when readers with and
  without `client` and writers are built, `read()` runs with a selection that holds objects and
  with an empty one, and `write()` runs with and without `replace=True`, then no log record is
  captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Five connectors, each with a reader and a writer, one spec per system:** S3, GCS, Unity
   Catalog, Databricks Volumes and BigQuery. This spec is S3's.
2. **Spark is the engine, with the contract of `DiskReader` and `DiskWriter`.** The reader
   returns a Spark DataFrame and the writer receives one. The native client does control work
   only: boto3 lists the objects and selects those above the watermark, and Spark loads exactly
   those objects. The reader has the arguments of `DiskReader` (`format`, `watermark_pattern`
   on the object's name or else its last-modified time, `watermark_value`, `schema`, `options`,
   `spark`) and its selection rules: hidden and in-progress names, strictly greater, sorted,
   frozen list, empty selection, Delta rejected by the reader, reserved options, flat listing.
   Only the listing differs. The writer is Spark writing straight to the URI, with
   `write(data, *, replace=False)` and the atomicity limits of `DiskWriter` for plain files;
   Delta is atomic through its log. The writer needs no boto3.
3. **Credentials of the listing client: an optional `client` argument.** The job may pass a
   ready client, such as `boto3.client("s3", endpoint_url=..., region_name=...)`. Without it the
   connector creates the SDK's default client at `read()` time, never at construction. This
   covers regions, profiles, and S3-compatible stores such as MinIO or R2 through
   `endpoint_url`, and it is the test seam. Spark's own access to S3 is always the
   environment's concern (Unity Catalog external locations, instance profiles, Hadoop
   configuration), documented and never configured by the connector.
4. **The extra is named after the system:** boto3 goes into a new extra, `ingestio[s3]`. The SDK
   is imported lazily, so the import surface works without the extra. The main session adds the
   extra, the `dev` packages and the type stubs mypy strict needs to `pyproject.toml` and
   `uv.lock` after approval, as it did for PostgreSQL.
5. **Everything else follows the disk and PostgreSQL specs:** ADR 0013's constructor arguments,
   pyspark imported lazily, the session looked up at read time, errors propagated unchanged, no
   log lines, the ADR 0012 batch-leaning amendment, and exports from `ingestio.connector.all`
   only.

Proposed by this spec, following the precedents where they give one, and accepted by the main
session with its approval on 2026-10-23, under the maintainer's delegation:

6. **Building validates and does nothing else, except the extra check of a reader without
   `client`.** Rule 3 of the pattern for extras makes a connector whose extra is missing fail
   when it is built, as `PostgresWriter` does. A reader with a `client` needs no package of its
   own, so it imports none, and the import surface and the criteria work without boto3.
   Building creates no client: decision 3.
7. **One `ListObjectsV2` paginator per read, with `Delimiter="/"`, every page consumed before
   anything loads.** The paginator follows the continuation tokens itself, and the fake stays a
   list of pages. The delimiter makes the listing flat, as `DiskReader`'s, and keeps the
   contents of sub-prefixes out of the requests. No other operation is called.
8. **An object's name is its key after the listing prefix.** Empty names and names with `/` are
   not candidates, and common prefixes are ignored, so directory markers and sub-prefixes never
   reach Spark, whatever the store does with the delimiter. The hidden-name rule of `DiskReader`
   then applies to the name.
9. **Spark receives `<normalised path>/<name>` per selected object, with the scheme as written,
   sorted by name, always as a list,** as in the disk spec's decision 8.
10. **The default client is `boto3.client("s3")`, looked up on the module at each `read()`, one
    new client per read, never cached and never closed.** A given client is used as it is for
    every read and never closed or reconfigured. Each read then sees the environment as it is,
    as the session lookup does.
11. **Formats, watermark values, options and reserved keys are the disk connectors', with the
    same messages,** and there are no default options.
12. **The writer is `DiskWriter`'s single call chain with the URI.** It takes no session and no
    client, needs no boto3, and lists, creates and deletes nothing itself.
13. **Errors propagate unchanged and the connectors write no log lines,** as in the PostgreSQL
    spec's decision 16 and the disk spec's decision 13. That includes botocore's `ClientError`
    for a missing bucket or a denied listing, and `NoCredentialsError`.
14. **Every storage class is a candidate.** The reader does not look at `StorageClass`. An
    archived object that is selected makes Spark's load fail, which is documented; skipping it
    would leave data unread silently.
15. **`path` is a `str` only.** `pathlib` collapses the `//` of a URI, so `PathLike` is
    rejected with the scheme message.
16. **Supported environments:** Databricks classic compute on Runtime 15.4 LTS or later, and
    open-source Spark 3.5 or later with `hadoop-aws`. Serverless: no claim, not tested.
    S3-compatible stores: nothing excludes them, not tested.
17. **The selection code may be shared with `DiskReader` through private helpers,** with no
    change to the disk connectors' behaviour, messages or tests. This is an implementation
    note, not public API.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept and apply a
  format they are given, as the disk connectors do.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged. Like the
  disk connectors, these lean towards batch: one selection per `read()`, a watermark fixed when
  the reader is built, and a write that needs the whole DataFrame. A streaming source on S3,
  such as Auto Loader, S3 event notifications or Structured Streaming's file source, tracks the
  objects it processed in a checkpoint and would be a different connector. This belongs in the
  "What leans towards batch" list of [ADR 0012](../docs/decisions/0012-streaming-deferred.md):
  an amendment, not a new ADR.
- **ADR 0013 applies.** `path`, `format`, `watermark_pattern` and `schema` become configuration
  keys by its convention, `watermark_value` is computed by the job, and `client` carries the
  credentials, separate from the configuration. The writer's `path` names the target, as for
  `DiskWriter`. Selecting objects before loading is how this reader filters inside the source.
  The default client's own lookup of credentials is the subject of **Q6**: the recommendation
  is an amendment to ADR 0013's consequences, not a new ADR.
- **ADR 0002 applies unchanged.** The `s3` extra follows the pattern for extras. Rule 3 is read
  as "when the connector needs the package": a reader with a `client` needs none. The
  doc-keeper may say so in the Extras section of the connectors page.

## Open questions

None open. On 2026-10-23 the maintainer, away for the session, delegated the approval of this
spec to the main session, with the instruction to take the recommended option of every question
that the maintainer's decisions do not settle. The main session took the recommended option of
all nine; none contradicts decisions 1 to 5. The options are kept below as the record of what
was weighed, for the maintainer to review.

1. **Class names, module and slug.**
   - (a) `S3Reader` and `S3Writer` in a new `connector/s3.py`, slug `s3_connectors`.
     CONTRIBUTING names modules after the medium, as `local.py` and `postgres.py` are, and GCS
     and Volumes then get modules of their own.
   - (b) The same names in a shared `connector/object_store.py` for S3 and GCS. One module
     would hold two systems with different SDKs and extras.
   - (c) `S3ObjectReader` and `S3ObjectWriter`. Longer, with nothing gained.

   **Recommended: (a).**
2. **The location, its scheme, and what is rejected.**
   - Location:
     - (a) One `path` URI, `s3://<bucket>/<prefix>`, the configuration key of the disk
       connectors.
     - (b) Two arguments, `bucket` and `prefix`. The connector would then have to choose the
       scheme it gives Spark.

     **Recommended: (a).**
   - Schemes:
     - (i) Accept `s3://` and `s3a://`, in lowercase, and pass the scheme to Spark exactly as
       written. Databricks documents `s3://`; open-source Spark with hadoop-aws implements
       `s3a://`, and `s3://` only when the session maps it. The connector cannot tell where it
       runs, and choosing would be deciding architecture.
     - (ii) Accept both and always give Spark `s3a://`. That suits open-source Spark, but on
       Databricks external locations are declared with `s3://`, and this spec cannot confirm
       that Unity Catalog governs `s3a://` paths the same way.
     - (iii) Accept only `s3://`. Open-source jobs must then map `fs.s3.impl`.

     **Recommended: (i).** `s3n://` is rejected under every option: Hadoop 3 removed it.
   - The bucket root:
     - (x) The reader accepts it, and the writer rejects it with
       `path must name a prefix inside the bucket`, because `replace=True` would delete every
       object of the bucket.
     - (y) Both accept it, as the disk connectors accept `/`.
     - (z) Both reject it. Landing buckets with objects at the root could not be read.

     **Recommended: (x).**
   - Empty, `.` and `..` segments: (1) rejected, because Hadoop's paths collapse and resolve
     them while S3 keys keep them, so the listing and Spark would name different objects; (2)
     accepted as written, with that mismatch documented. **Recommended: (1).**
3. **The precision of `LastModified` watermarks.**
   - (a) Whole seconds. That is S3's own precision, and the truncation keeps the reader in
     agreement with `_metadata.file_modification_time` on stores that list finer times than
     they report to Spark, so a job that advances its watermark from that column never re-reads
     its last object. Objects that share the last second read and arrive after the read are
     skipped.
   - (b) Milliseconds, as `DiskReader`. The same as (a) on AWS, whose listings show `.000`. On a
     store whose listing is finer than the status Spark gets, the job's next watermark could be
     below its last object's listed time, and that object would then be read again on every
     run.
   - (c) The time as boto3 returns it, with the risk of (b).

   **Recommended: (a).**
4. **A prefix with no object, and a missing one.**
   - (a) An empty listing is an empty selection: S3 has no directories, and an empty prefix
     cannot be told from a missing one. A missing bucket still fails, with boto3's error. A
     typo in the prefix reads as empty, which is documented.
   - (b) Raise `FileNotFoundError` when the listing returns no object and no common prefix, as
     `DiskReader` does for a missing directory. A typo fails the run, but so does a landing
     prefix that is legitimately empty, for instance after its objects were archived.
   - (c) (b), with an argument to turn it off. One more argument for one edge case.

   **Recommended: (a).**
5. **The extra, the `dev` group and the type of `client`.**
   - The extra: (a) `s3 = ["boto3>=1.34"]`, a floor that the boto3 Databricks Runtime 15.4 LTS
     preinstalls should satisfy, so that installing the extra there does not upgrade the
     runtime's boto3 (the main session confirms the runtime's version when it edits
     `pyproject.toml`); (b) no floor; (c) a recent floor. **Recommended: (a).**
   - Type stubs in the `dev` group: (a) `boto3-stubs[s3]>=1.34`, which types `boto3`,
     `botocore` and `mypy_boto3_s3`; (b) `types-boto3[s3]`, from the same author with newer
     packaging, whose module is `types_boto3_s3`; (c) no stubs, with a mypy override that
     ignores the missing imports of `boto3` and `botocore`, and `client` typed `Any`.
     **Recommended: (a),** together with `boto3>=1.34` in the `dev` group.
   - The type of `client`: (a) `mypy_boto3_s3.client.S3Client | None`, imported under
     `TYPE_CHECKING`, which users without the stubs see as `Any`; (b) `Any`; (c)
     `botocore.client.BaseClient`, which accepts a client of any service. **Recommended: (a).**
6. **ADR 0013 and the default client.** ADR 0013 says a connector never reads configuration
   files, environment variables or secret stores. The default client of decision 3 does,
   through boto3's own lookup: environment variables, `~/.aws`, instance metadata.
   - (a) Read ADR 0013 as being about the connector's own code, and have the doc-keeper add a
     consequence to it: an SDK's default client, created when the job passes none, follows the
     SDK's lookup, which the connector neither reads nor configures. GCS's default credentials
     would fall under the same consequence.
   - (b) A new ADR that records the exception.
   - (c) No default client. This contradicts decision 3, so it applies only if the maintainer
     reopens it.

   **Recommended: (a).**
7. **How the tests run.**
   - (a) The criteria run everywhere, with a fake client and a fake Spark session, and no new
     test-only package. The integration tests are opt-in through `INGESTIO_S3_URL`, as
     `INGESTIO_POSTGRES_URL` is for PostgreSQL, and are written blind here.
   - (b) moto in the `dev` group for the criteria: real botocore parsing, but a new test-only
     package, and moto stamps `LastModified` with the current time, so the watermark criteria
     would need yet another package to set times.
   - (c) A MinIO container for the integration tests in CI. That needs Docker and Java 17 or
     21 in CI, which are the maintainer's call.

   **Recommended: (a).**
8. **The PostgreSQL criterion AT-004 conflicts with a second extra.** It states that every
   `Requires-Dist` carries `extra == "postgres"`, and `test_at004_...` in
   `tests/test_postgres_connectors.py` asserts exactly that. With `boto3` under
   `extra == "s3"`, that test fails as soon as the main session adds the extra. The Unity
   Catalog and BigQuery specs will meet the same conflict.
   - (a) The main session amends AT-004 of `specs/postgres_connectors.md` to "every
     `Requires-Dist` carries an `extra == "<name>"` marker, and the one that names psycopg
     carries `extra == "postgres"`", records it in that spec's History, and the test-writer of
     this spec updates that one test in the same tests stage. Whichever connector spec lands
     first makes the change, for every later extra.
   - (b) Return `specs/postgres_connectors.md` to planning for that criterion, which runs its
     whole flow again.
   - (c) Leave the PostgreSQL test failing until a later spec. CI fails meanwhile.

   **Recommended: (a).**
9. **Object names with glob characters.** Spark expands `*`, `?`, `[`, `]`, `{`, `}` and `\` in
   the paths it loads.
   - (a) Document it as a limitation, and have the doc-keeper add the same limitation to the
     disk connectors, which share it and do not mention it.
   - (b) Escape those characters in the paths given to Spark. Hadoop's glob syntax allows it,
     but the escaping cannot be checked without a JVM, and how Databricks treats it is not
     known.
   - (c) Leave such objects out of the candidates, or raise. Data left unread silently, or a
     run that fails on a name the job does not control.

   **Recommended: (a).**

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions: `S3Reader` lists the objects directly under a prefix with a boto3 `ListObjectsV2` paginator (`Delimiter="/"`), with the job's `client` or a `boto3.client("s3")` created at each read, selects by name pattern or by `LastModified` truncated to whole seconds, and Spark loads exactly that sorted list of URIs with the scheme as written; `S3Writer` is `DiskWriter`'s single call chain on the URI and needs no boto3. 33 acceptance criteria, 9 integration scenarios, 17 decisions (5 by the maintainer, 12 proposed), 9 open questions, and a "Differences from the disk connectors" table for the GCS and Volumes specs. |
| 2026-10-23 | approval | approved | Approved by the main session under the maintainer's delegation of 2026-10-23 ("Delegar e ir até o fim"), with the recommended option of all 9 questions; the 12 proposed decisions accepted. The main session checked the three environment claims the planner could not: Databricks Runtime 15.4 LTS ships boto3 1.34.39 (the `>=1.34` floor holds), a multipart upload's `LastModified` is its initiation time, and S3A takes a file's status from a HEAD request, to the second. Per Q8 it amended AT-004 of `specs/postgres_connectors.md` (recorded in that spec's History); per Q5 it added `s3 = ["boto3>=1.34"]` and the dev packages `boto3>=1.34` and `boto3-stubs[s3]>=1.34` to `pyproject.toml` and refreshed `uv.lock`. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_s3_connectors.py`: 65 tests (147 cases) covering AT-001 to AT-033 with a fake S3 client, paginator and `boto3.client` factory and a fake Spark session, two tests of behaviour no criterion names (validation before the import check; every storage class is a candidate), and the 9 integration scenarios, marked `integration` and skipped unless `INGESTIO_S3_URL` is set (written blind: Java 27 only, no credentials). Per Q8 it updated `test_at004_...` in `tests/test_postgres_connectors.py` to the amended criterion, which passes. Checked by the main session: collection fails as expected with `ImportError: cannot import name 'S3Reader' from 'ingestio.connector.all'`; the rest of the suite passes (322 passed, 15 skipped); ruff and format clean. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `S3Reader` and `S3Writer` in a new section of `docs/concepts/connectors.md`, in a shape the GCS and Volumes sections can follow (signatures, paths, listing and selection, writing and atomicity, the "Differences from the disk connectors" table, examples, environments, limitations); added the glob-character limitation to the disk connectors (Q9), a table of extras and the "when the connector needs the package" reading of rule 3; amended ADR 0002 (the `s3` extra), ADR 0012 (leans towards batch) and ADR 0013 (SDK default clients follow the SDK's own credential lookup, per Q6; the writer's `path` names the target; selecting objects is how the reader filters in the source); updated `docs/index.md` and the architecture KB. Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable S3 blocks, with the expected `ImportError` of the missing classes (12 passed). |
| 2026-10-23 | implementation | implemented | The implementer added `S3Reader` and `S3Writer` in `src/ingestio/connector/s3.py`, exported from `connector/all.py`: the reader lists with a boto3 `list_objects_v2` paginator (`Delimiter="/"`), imports boto3 only without a `client`, and loads only the selected objects; the writer is one `format().options().mode().save()` chain. The rules shared with the disk connectors (format and option checks, hidden names, the watermark validation and selection in whole units since the epoch) moved into a private `connector/_files.py`, which `DiskReader` now uses with the same names, messages and behaviour. Checked by the main session: 462 passed, 24 skipped (the opt-in integration tests), ruff, format, mypy strict and `uv lock --check` clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-033 covered (138 passed, the 9 integration scenarios skipped without `INGESTIO_S3_URL`), the full suite with the documentation examples 462 passed and 24 skipped, ruff, format, mypy strict and `uv lock --check` clean; code, documentation, philosophy, constraints and write scopes match. A differential probe of the committed `DiskReader`/`DiskWriter` against the refactored ones (2,688 construction cases, 49 reads on a real directory) found identical behaviour and messages. One non-blocking finding: the private `connector/_files.py` lacks the sorted `__all__` that CONTRIBUTING requires; the main session handed it to the implementer of `gcs_connectors`, the next change to use that module. The integration tests stay unverified: no Java 17/21 and no AWS credentials here. |
| 2026-10-23 | amendment | validated | AT-032 superseded by [specs/config_connectors.md](config_connectors.md), decision 17, approved by the maintainer on 2026-10-23: `ReadSource` and `WriteTarget` no longer take a connector, so the composition this criterion describes is covered by that spec's criteria. The criterion's test goes in the tests stage of that spec; this spec keeps its status. |
