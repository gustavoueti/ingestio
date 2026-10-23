# Spec: GCS reader and writer connectors

- **Status:** validated
- **Slug:** `gcs_connectors`. The spec is `specs/gcs_connectors.md` and its tests are
  `tests/test_gcs_connectors.py`.
- **Related:** [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md),
  [specs/s3_connectors.md](s3_connectors.md), the model of this spec,
  [specs/disk_connectors.md](disk_connectors.md),
  [specs/postgres_connectors.md](postgres_connectors.md),
  [backlog idea 4](../docs/backlog.md#4-gcs-reader-and-writer),
  [Connectors](../docs/concepts/connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Two connectors for objects in Google Cloud Storage (GCS), built on Spark's file data sources,
with the contract of `DiskReader` and `DiskWriter` and the design of `S3Reader` and `S3Writer`.
`GcsReader` lists the blobs directly under a prefix with google-cloud-storage, on the driver,
selects those above the job's watermark before anything is loaded, and returns a Spark
DataFrame over exactly those blobs. `GcsWriter` writes a Spark DataFrame under a prefix,
appending or replacing as the calling callback chooses when it calls `write`. Spark writes
straight to the `gs://` URI, so the writer needs no google-cloud-storage. A blob's watermark
comes from its name, through a pattern the job gives, or else from its creation time,
`timeCreated`, to the millisecond **(Q1)**.

GCS calls them objects; its Python SDK, and this spec, call them blobs.

google-cloud-storage does control work only: it lists. It goes into a new extra,
`ingestio[gcs]`, and is needed only when the job does not pass a `client` of its own. Spark's
access to GCS is the environment's concern, never the connector's.

This spec follows [specs/s3_connectors.md](s3_connectors.md) wherever GCS behaves as S3 does,
with its wording and with the answers the main session gave to its questions. The section
[Differences from the disk connectors](#differences-from-the-disk-connectors) puts what GCS
changes next to what S3 changes; everything else stays as in the disk connectors.

The choices that are open questions are marked **(Q n)**. The spec is written with the
recommended option of each one.

## Motivation

- **The request.** The maintainer asked for five connectors on 2026-10-23, each with a reader
  and a writer: S3, GCS, Unity Catalog, Databricks Volumes and BigQuery, one spec per system.
  This is the GCS one ([backlog idea 4](../docs/backlog.md#4-gcs-reader-and-writer)). The
  maintainer grouped GCS with S3 and Volumes: like the disk connectors, they handle files and
  receive the file format.
- **Read.** Producers drop files into a GCS prefix. A job must read only the blobs it has not
  ingested yet, and must pick them before Spark loads anything. For an object store the unit
  the source can filter is the object, so selecting blobs before loading is how this reader
  filters "inside the source", as [ADR 0013](../docs/decisions/0013-connector-configuration-convention.md)
  requires: Spark never opens a blob that was not selected.
- **What exists does not fit.** `DiskReader` rejects every URI. `spark.read.load(prefix)` reads
  every blob. Spark's `modifiedAfter` option filters inside Spark's own listing, so the
  selection is not frozen, and it cannot serve name watermarks. Databricks Auto Loader, Pub/Sub
  notifications for Cloud Storage and Structured Streaming's file source track what they
  processed in a checkpoint or a subscription: they are streaming, which is deferred
  ([ADR 0012](../docs/decisions/0012-streaming-deferred.md)).
- **Why a native client lists.** google-cloud-storage gives the driver the same view of a
  prefix that `os.scandir` gives `DiskReader` of a directory and boto3 gives `S3Reader`, in a
  few requests and without a Spark job per read.
- **Write.** Spark writes `gs://` URIs itself, through the Cloud Storage connector for Hadoop.
  The writer is the `DiskWriter` call chain with a URI, with the same limits on atomicity for
  plain files; Delta is atomic through its log.

## Fit with the philosophy

- **Concept: connector.** `GcsReader` satisfies `Reader[DataFrame]` and `GcsWriter` satisfies
  `Writer[DataFrame]`, structurally, with no base class and no registration
  ([ADR 0003](../docs/decisions/0003-connectors-as-protocols.md)). GCS is the external system.
  `write(data, *, replace=False)` adds a keyword-only argument with a default, so `write(data)`
  stays a valid call and the method still matches the protocol. `ReadSource` and `WriteTarget`
  use both connectors unchanged.
- **The client is not a third concept.** It is google-cloud-storage's own object,
  `google.cloud.storage.Client`, built by the job or by the SDK's defaults, and passed in like
  `spark`. It carries the credentials, and the endpoint when the job sets one, which ADR 0013
  keeps out of the job configuration. The reader calls one method on it, the listing. Nobody
  extends it, and it performs no action of the pipeline.
- **Format: given, not decided.** As the disk spec explains in its own "Fit with the
  philosophy", applying the format the job names is how a file connector talks to its system.
  The format is a required argument with no default; the connector never infers it, adds no
  column, casts nothing and filters no rows.
- **Write strategy.** Append and replace are the two primitive ways Spark puts a batch under a
  prefix, the `append` and `overwrite` save modes. The writer offers both through one method
  and picks neither: the calling callback chooses at call time. Options that would turn a
  replace into a partial replace are reserved.
- **Architecture.** The connectors create no bucket, no table and no catalog entry, lay out no
  partitions, and configure neither Spark nor Hadoop. There is no scheme to choose: `gs://` is
  the one scheme of the Cloud Storage connector for Hadoop, on open-source Spark and on
  Databricks alike. Spark's access to GCS is the environment's.
- **The reader decides no strategy.** Whether a run is incremental, from which value, and where
  a blob's watermark comes from, are the job's decisions. The reader turns them into a
  selection of blobs.
- **Essential by default.** Nothing is caught to carry on: every failure propagates out of
  `read()` or `write()` and fails the callback that called them.
- **Simplicity.** Two small classes in one module. No base class, no registry, no staging, no
  new protocol, no new callback. The reader's own logic is one listing and a comparison, with
  the selection rules of `DiskReader`.
- **Compose, do not impose.** The job builds the connectors from its configuration, and builds
  its own client when it needs one. The connector itself reads no file, no environment variable
  and no secret store; the default client follows the SDK's own lookup of Application Default
  Credentials, under the consequence that the S3 spec's question 6 adds to ADR 0013 (see
  [ADRs](#adrs)).
- **No dependencies in the core.** google-cloud-storage goes into the `gcs` extra and is
  imported only inside the functions that use it. pyspark stays a prerequisite. `ingestio` and
  `ingestio.connector.all` import without either.
- **Streaming.** Nothing here closes the door. See [ADRs](#adrs).

## Scope

In scope:

- `GcsReader`: reads of the blobs directly under one prefix, all of them or only those above a
  watermark, selected before loading and frozen by `read()`. The watermark comes from the
  blob's name through a pattern, or from its `timeCreated`, to the millisecond;
- the `client` argument, and the default client created at each read;
- `GcsWriter`: append (`write(data)`) and replace (`write(data, replace=True)`) through Spark's
  file writers, straight to the URI;
- the rules for URIs, formats, watermark values and options, and the reserved options;
- the `gcs` extra, and what the main session adds to `pyproject.toml` and `uv.lock`;
- the documentation of what a write guarantees, of the environments and of the limits;
- the test doubles, and integration tests that run only when the environment provides GCS.

Out of scope:

- the job configuration file and recipes, as in the disk spec;
- computing, persisting or advancing the watermark, and exposing which blobs `read()`
  selected;
- recursive listings, sub-prefixes and partitioned input layouts, and partitioned output
  (`partitionBy`);
- reading table formats (Delta, Iceberg, Hudi), writing to catalog tables, and any DDL;
- a watermark column, that is filtering rows inside the blobs;
- streaming sources: Auto Loader (`cloudFiles`), Pub/Sub notifications for Cloud Storage,
  Structured Streaming's file source, checkpoints;
- a staging mechanism, or any atomicity beyond Spark's and the format's;
- moving, archiving, deleting or changing the metadata of source blobs; uploading or
  downloading blobs with google-cloud-storage;
- reading a single blob given as `path`;
- generations and noncurrent versions, soft-deleted objects, requester-pays buckets
  (`user_project`), customer-supplied encryption keys, the listing's own filters
  (`match_glob`, `start_offset`, `end_offset`), and folders and managed folders as resources;
- creating buckets; configuring Spark's or Hadoop's access to GCS (the connector's JAR, its
  credentials and settings);
- credentials (a service account key, a `google.auth` credentials object, a project) as
  connector arguments: the job builds a client;
- Spark's other save modes, `ignore` and `errorifexists`;
- engines other than Spark;
- changes to CI, which belong to the maintainer.

## Public API

New, in the new module `ingestio/connector/gcs.py`, exported from `ingestio.connector.all`
only (not from `ingestio`):

```python
from ingestio.connector.all import GcsReader, GcsWriter
```

```python
class GcsReader:
    """Reads the blobs under a GCS prefix into a Spark DataFrame, in the format it is given.

    Args:
        path: Prefix that holds the blobs, as a URI: `gs://<bucket>/<prefix>`. The
            configuration key of the same name.
        format: Name of the Spark data source the blobs are written in, such as `parquet`,
            `csv` or `json`. The configuration key of the same name.
        watermark_pattern: Regular expression that the whole name of a blob matches, with
            one group that captures the blob's watermark. The name is the part of the blob's
            full name after the prefix. Without it, a blob's watermark is its creation time,
            `timeCreated`, to the millisecond. The configuration key of the same name.
        watermark_value: Last watermark already ingested. Only blobs above it are read. A
            `str` with `watermark_pattern`, a timezone-aware `datetime` without it.
        schema: Schema of the blobs, as a `StructType` or a DDL string. Spark infers it
            when omitted.
        options: Spark data source options passed through, such as `header` or
            `mergeSchema`.
        client: google-cloud-storage client that lists the blobs, such as
            `storage.Client.from_service_account_info(info)`. When omitted, each read
            creates `storage.Client(project=None)`, with Application Default Credentials,
            which needs the `gcs` extra.
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
        client: Client | None = None,
        spark: SparkSession | None = None,
    ) -> None: ...

    def read(self) -> DataFrame:
        """Return a DataFrame over the blobs the job needs."""
```

```python
class GcsWriter:
    """Writes a Spark DataFrame as blobs under a GCS prefix, in the format it is given.

    Args:
        path: Prefix to write under, as a URI: `gs://<bucket>/<prefix>`.
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
        """Write every row of `data` as blobs under the prefix.

        Args:
            data: The rows to write.
            replace: Remove every blob under the prefix first, so that it ends up holding
                exactly the rows of `data`. Appends when `False`.
        """
```

- `DataFrame`, `SparkSession` and `StructType` are `pyspark.sql.DataFrame`,
  `pyspark.sql.SparkSession` and `pyspark.sql.types.StructType`. `datetime` is
  `datetime.datetime`, and `Mapping` is `collections.abc.Mapping`.
- `Client` is `google.cloud.storage.Client` **(Q3)**, and `storage` in the docstring is
  `google.cloud.storage`. It is imported under `TYPE_CHECKING` only. At run time the reader
  calls only `client.list_blobs(...)` on it.
- `path` is a `str`. `os.PathLike` is not accepted, because `pathlib` collapses the `//` of a
  URI.
- `path` and `format` are required, so they are positional; every option is keyword-only.
  `format` shadows a builtin, which ruff's rule A002 reports: `# noqa: A002`, as in
  `connector/local.py`.
- `GcsWriter` has a single write method, `write`. `replace` is keyword-only and defaults to
  `False`, so `GcsWriter.write` satisfies `Writer[DataFrame]` structurally. `WriteTarget`, which
  calls `write(data)`, always appends.
- New installable extra: `ingestio[gcs]`.
- Unchanged: `Reader`, `Writer`, `ReadSource`, `WriteTarget`, `JsonApiReader`,
  `JsonLinesWriter`, the PostgreSQL, disk and S3 connectors, and everything exported by
  `ingestio`.
- The connectors' attributes are not documented, so they are not public API. Their `repr` is
  specified under [Building the connectors](#building-the-connectors).

### Usage sketch

A sketch of a Databricks job, not a runnable example. The job reads its configuration, keeps
its own watermark, builds its own client, and advances the watermark from what it read.

```python
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
# unix_millis keeps the milliseconds, as the reader does, without floating point.
orders = process.state["orders"]
if not orders.isEmpty():
    millis = orders.select(F.max(F.unix_millis("_metadata.file_modification_time"))).first()[0]
    last_watermark = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=millis)
```

Spark's `_metadata.file_modification_time` is never below the `timeCreated` that the reader
compares (see [The watermark of a blob](#the-watermark-of-a-blob)), so this job never selects
its last blob again. A job that reaches GCS through another endpoint, such as Private Service
Connect, passes `storage.Client(client_options={"api_endpoint": ...})`, and configures Spark's
own endpoint in the session, which is the environment's concern.

## Behaviour

### Building the connectors

Building a connector validates its arguments and does nothing else, with one exception: a
reader built without `client` checks that google-cloud-storage can be imported (see
[The client and the extra](#the-client-and-the-extra)). Building creates no client, accesses no
attribute of `client` or `spark`, and touches neither Spark nor GCS. Validation runs in the
order of the arguments, and the import check comes last.

#### Paths

Both connectors take `path` apart the same way, with the rules of the S3 spec's question 2 and
`gs://` as the one scheme:

1. `path` must be a `str` that starts with `gs://`, in lowercase. Anything else raises
   `ValueError("path must be a gs:// URI")`. This rejects the empty string, local paths, other
   schemes (`s3://`, `gcs://`, `https://`), `GS://`, `gs:/...`, and every non-`str`, such as a
   `pathlib.Path`.
2. The **bucket** is the text between `://` and the next `/`, or the end. When it is empty,
   `ValueError("path must name a bucket")`. The bucket is not validated further: GCS rejects a
   name it does not know when the reader lists, and Spark when it reads or writes.
3. The **prefix** is the rest, after that `/`, with every trailing `/` removed. When it is not
   empty and one of its `/`-separated segments is empty, `.` or `..`, then
   `ValueError("path must not contain empty, '.' or '..' segments")`. GCS keeps such names as
   they are, while Hadoop's paths collapse `//` and resolve `.` and `..`, so the listing and
   Spark would name different blobs.
4. The connectors keep the **normalised path**: `gs://<bucket>`, followed by `/<prefix>` when
   the prefix is not empty. `gs://lake/landing/orders/` becomes `gs://lake/landing/orders`, and
   `gs://lake/` becomes `gs://lake`.
5. The **writer** also needs a prefix: an empty prefix, the bucket root, raises
   `ValueError("path must name a prefix inside the bucket")`, because `replace=True` would
   remove every blob of the bucket. The reader accepts the bucket root.

#### Formats

`format` is passed to Spark as given. An empty `format` raises
`ValueError("format must not be empty")`. Spark looks the format up when it reads or writes, and
an unknown one fails there. The reader rejects `delta`, compared case-insensitively, with
`ValueError("format '<format>' cannot be read file by file")`, where `<format>` is the value as
written: a Delta table's log decides which of its blobs hold live data. The writer accepts
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
  | `GcsReader` | `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore`, `modifiedAfter` |
  | `GcsWriter` | `replaceWhere`, `partitionOverwriteMode` |

#### The client and the extra

- `client` is not validated, and building never accesses it.
- When `client` is `None`, building imports `google.cloud.storage`, only to check that the
  extra is installed. When that import fails, the reader raises
  `ImportError("GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'")`, chained
  to the original error. This is rule 3 of the
  [pattern for extras](../docs/concepts/connectors.md#extras).
- When `client` is given, the reader never imports google-cloud-storage, neither when it is
  built nor when it reads: the job's client is all it needs.
- The writer never imports google-cloud-storage and has no client.

#### `repr`

`repr` shows the normalised path and the format, never the client:

- `GcsReader(path='gs://lake/landing/orders', format='csv')`
- `GcsWriter(path='gs://lake/orders', format='parquet')`

### Reading

`read()` returns a `pyspark.sql.DataFrame`. Each call goes through these steps, in order:

1. **Session.** The reader uses `spark` when it is given. Otherwise it calls
   `pyspark.sql.SparkSession.active()`, at each `read()`. The reader never creates a session.
   When none is active, PySpark's error propagates, and nothing is listed.
2. **Client (Q2).** The reader uses `client` when it is given, as it is, for every read.
   Otherwise it calls `google.cloud.storage.Client(project=None)`, with that one keyword
   argument and nothing else, looked up as an attribute of the `google.cloud.storage` module at
   each `read()`. The SDK finds the credentials itself, through Application Default
   Credentials. `project=None` tells it not to look for a project: a listing uses none, and an
   omitted `project` makes the SDK raise where the environment has none. Each such read creates
   a new client; the reader keeps none and closes none.
3. **Listing.** See [The listing](#the-listing).
4. **Candidates.** See [Candidates](#candidates).
5. **Watermarks.** The reader takes each candidate's watermark, as described in
   [The watermark of a blob](#the-watermark-of-a-blob).
6. **Selection.** The reader selects the candidates whose watermark is strictly greater than
   `watermark_value`, or every candidate when `watermark_value` is `None`.
7. **Load.** The reader loads the selection, or builds an empty DataFrame when the selection is
   empty.

#### The listing

The **listing prefix** is the prefix followed by `/`, or `""` at the bucket root. The reader
makes one listing per `read()`, on the driver:

```text
client.list_blobs(<bucket>, prefix=<listing prefix>, delimiter="/")
```

- One positional argument, the bucket's name as a `str`, and exactly these two keyword
  arguments. The reader passes no `fields`, `page_size`, `versions`, `soft_deleted`,
  `match_glob`, `include_trailing_delimiter`, `include_folders_as_prefixes`, `timeout` or
  `retry`: the SDK's defaults apply, its retry policy for listings included.
- `list_blobs` returns an iterator over the blobs. The reader iterates over it once, to its
  end, before anything is loaded. The SDK sends each request as the iteration reaches its
  page, and follows the page tokens itself.
- The reader reads no attribute of that iterator: neither `prefixes`, where the SDK gathers the
  sub-prefixes that the delimiter groups, nor `pages`.
- From each blob it reads `name` and `time_created`, and no other attribute.
- The sub-prefixes never reach the iteration, and are ignored with everything under them. The
  listing is flat.
- The reader calls no other method of the client: no `bucket`, `get_bucket` or
  `lookup_bucket`. It opens, downloads and inspects no blob. It never gives the client to
  Spark.

Only live blobs are listed: without `versions`, the noncurrent versions of a bucket with
object versioning are left out, and without `soft_deleted`, so are soft-deleted objects.

A prefix that holds no blob gives an empty listing, which GCS does not distinguish from a
prefix that does not exist, in a bucket with or without hierarchical namespace: the selection
is then empty, as for S3. A bucket that does not exist fails with the SDK's error,
`google.api_core.exceptions.NotFound`, raised when the iteration requests the first page. A
listing that GCS denies fails with `google.api_core.exceptions.Forbidden`.

#### Candidates

The **name** of a blob is its full name with the listing prefix removed: for the blob
`landing/orders/a.csv` under `gs://lake/landing/orders`, the name is `a.csv`. A blob is a
candidate when its name:

- is not empty. The blob whose full name equals the listing prefix, such as
  `landing/orders/`, is a directory placeholder: a zero-byte object that the Google Cloud
  console's "Create folder" and the Cloud Storage connector for Hadoop create in buckets with a
  flat namespace;
- contains no `/`. With the delimiter, GCS returns no such blob: the blobs and placeholders of
  sub-prefixes (`sub/d.csv`, `sub/`) are gathered in the iterator's `prefixes`, as `sub/`. The
  rule keeps the listing flat whatever a listing returns;
- starts with neither `_` nor `.` and does not end with `._COPYING_`. This is Spark's rule for
  hidden and in-progress files, as in `DiskReader`. It leaves out `_SUCCESS`, `_metadata`,
  `_common_metadata`, `.crc` files, and the `_started_*` and `_committed_*` markers that commit
  protocols write.

Every storage class is a candidate: GCS serves Standard, Nearline, Coldline and Archive blobs
alike, with no restore. The reader does not look at `storage_class`, `size` or any other
attribute.

In a bucket with hierarchical namespace, folders are resources of their own, not objects, so
they never appear among the blobs. Managed folders are not objects either. The rules above
apply unchanged.

#### The watermark of a blob

- **With `watermark_pattern`.** As in `DiskReader`: the pattern is matched against the whole
  name, not the blob's full name, with `re.fullmatch`, and the watermark is the text its group
  captured. A candidate is not read when its name does not match, or when its group took no
  part in the match. Watermarks compare as Python strings, code point by code point, and are
  never converted: `"9"` is above `"10"`.
- **Without `watermark_pattern` (Q1).** The watermark is the blob's `time_created`, the
  timezone-aware `datetime` that the SDK makes of the object's `timeCreated`, **truncated to
  whole milliseconds**, exactly and without floating point. It is compared with
  `watermark_value` as an instant, whatever the value's zone. The name plays no part, and
  neither does `updated`.
  - `timeCreated` is the creation time of the blob's current generation, stamped by GCS. An
    overwrite creates a new generation, with a new `timeCreated`, so an overwritten blob is
    selected again. A change of the blob's metadata alone moves its `updated` time, not its
    `timeCreated`, so it does not select the blob again.
  - GCS reports both times to the millisecond. The truncation keeps the rule exact if a client
    ever gives finer times, and matches the precision of Spark's column.
  - Spark's `_metadata.file_modification_time` comes from the file status that the Cloud
    Storage connector for Hadoop gives Spark, in milliseconds. The releases of the connector
    this spec knows of take it from the blob's `updated` time, which is never below
    `timeCreated` and equals it until the blob's metadata changes; a release that took
    `timeCreated` would give the reader's own value. The spec relies only on the column never
    being below `timeCreated`, which [integration test 4](#integration-tests) checks. So a job
    that takes its next watermark from that column, as in the [usage sketch](#usage-sketch),
    never selects its last blob again.
  - The value itself is not truncated: a blob at `09:00:00.500` is above `09:00:00.499999` and
    not above `09:00:00.500500`.

#### Load

When the selection holds at least one blob, `read()` makes exactly one load:

- `spark.read.format(format)`;
- `.schema(schema)` when `schema` is given, with the very object given;
- the user's `options`, as given, and no other option;
- `.load(paths)`. `paths` is a `list` of `str`, one per selected blob:
  `<normalised path>/<name>`, such as `gs://lake/landing/orders/a.csv`, or `gs://lake/a.csv` at
  the bucket root. The list is sorted by name in code-point order, and is a list even for one
  blob.

`read()` returns the load's DataFrame unchanged. The reader adds no column, filter, cast or
projection.

**The selection is frozen.** Spark receives the selected blobs, never the prefix, so every
action on the DataFrame reads those blobs and no other. Blobs that appear after `read()` are
left for the next run, whatever their watermark.

**The selection is not exposed.** The reader keeps no record of what it selected. A job that
advances its watermark takes it from the DataFrame, through Spark's hidden `_metadata` column
(`file_name`, `file_path`, `file_modification_time`).

#### When nothing is selected

The selection is empty when the listing holds no blob, holds only blobs that are not
candidates, holds no name that matches the pattern, or holds no watermark above the value.
Then `read()` makes no load. It returns `spark.createDataFrame([], schema)`, with `schema` when
it is given and otherwise a `pyspark.sql.types.StructType` with no fields. This DataFrame has no
`_metadata` column. A callback that wants to skip an empty batch checks for itself and raises
`CancelStepException`.

#### Errors

Nothing is caught. Errors of PySpark, of `google.cloud.storage.Client` (google-auth's
`DefaultCredentialsError` when Application Default Credentials find no credentials), of the
listing (`google.api_core.exceptions.NotFound` for a missing bucket, `Forbidden` for a denied
listing, transport errors once the SDK's retries are spent) and of Spark propagate out of
`read()` unchanged, as the same object. When the listing fails, nothing is loaded. The
connectors write no log lines.

### Writing

`write(data, *, replace=False)` makes exactly one call chain, the one of `DiskWriter`:

```text
data.write.format(<format>).options(<options>).mode(<mode>).save(<path>)
```

- `<mode>` is `"append"` when `replace` is `False` and `"overwrite"` when it is `True`;
- `<options>` are the user's, as given, and no other;
- `<path>` is the normalised path, a `str`.

**Operations.**

- `write(data)` appends: Spark adds new blobs under the prefix and leaves the existing ones as
  they are. `WriteTarget` calls `write(data)`, so it appends.
- `write(data, replace=True)` replaces: the prefix ends up holding exactly the rows of `data`.
  With a plain file format, Spark deletes every blob under the prefix, blobs it did not write
  included, and then writes the new ones. With Delta, the overwrite is a new version of the
  table.
- The calling callback chooses at call time. The configuration has no mode, and there is no
  `replace()` method.
- The blob names are Spark's (`part-<n>-<uuid>-c000...`), so a `GcsReader` that reads them back
  takes their watermark from their `timeCreated`. The Cloud Storage connector may also leave
  directory placeholders and `_SUCCESS`, which the reader ignores.

**What the writer does not do.** It reads only `data.write` from the DataFrame and evaluates it
once, through the save. It takes no session and no client, imports no google-cloud-storage, and
lists, creates and deletes nothing itself. It calls no other method of Spark's writer: no
`partitionBy`, `bucketBy`, `sortBy`, `saveAsTable` or `insertInto`. An empty DataFrame goes
through the same call. `write` returns `None`, and every error propagates unchanged, as the
same object.

**Atomicity.** The writer adds no mechanism of its own. A write guarantees what Spark's commit
protocol, the environment's committer and the format guarantee, and nothing more:

- **Append, plain file formats, open-source Spark.** In a bucket with a flat namespace, GCS has
  no rename, and the Cloud Storage connector emulates one by copying each blob and deleting
  the original. With Hadoop's default `FileOutputCommitter`, tasks write under `_temporary/`
  inside the prefix, which the reader ignores, and the job commit copies each file to its final
  name, one by one. A reader that lists during the commit sees part of the batch, and a commit
  that fails midway leaves part of it. In a bucket with hierarchical namespace, recent releases
  of the connector can rename a folder atomically when they are configured to; this spec does
  not rely on it. Choosing a committer and configuring the connector are the environment's
  concern.
- **Replace, plain file formats.** Spark deletes the blobs under the prefix before it writes.
  If the write then fails, the prefix is left empty or holding part of the new rows, and the
  previous contents are lost.
- **Delta.** An append or an overwrite is one commit in the table's log, so readers through
  Delta see the table before the commit or after it. Whether several Spark drivers can write
  one table at the same time on open-source Spark is a matter of Delta Lake's log store for
  `gs://`, as Delta Lake documents it; this spec makes no claim. Databricks coordinates Delta
  commits itself.
- **On Databricks,** the runtime uses its own commit protocol for file formats. What it
  guarantees is the runtime's, and this spec does not rely on it.

### Differences from the disk connectors

What a store listed by a native client changes, with the S3 column of the
[S3 spec](s3_connectors.md#differences-from-the-disk-connectors) for comparison. Everything not
in this table is as in the [disk spec](disk_connectors.md):

| Point | `DiskReader` / `DiskWriter` | `S3Reader` / `S3Writer` | `GcsReader` / `GcsWriter` |
|---|---|---|---|
| Location | absolute local path, `str` or `PathLike` | `s3://` or `s3a://` URI, a `str`, scheme passed to Spark as written | `gs://` URI, a `str`, the one scheme |
| Rejected locations | relative paths and URIs | other schemes, no bucket, empty, `.` or `..` segments; the bucket root for the writer | other schemes, `gs://` not in lowercase, no bucket, empty, `.` or `..` segments; the bucket root for the writer |
| Listing | `os.scandir` of the directory | boto3 `ListObjectsV2` paginator, `Delimiter="/"`, every page | `client.list_blobs(<bucket>, prefix=..., delimiter="/")`, iterated to its end |
| Name of an entry | the file name | the key after the listing prefix | the blob's full name after the listing prefix |
| Not candidates, besides hidden names | directories, broken links | empty names (directory markers), names with `/`, common prefixes | empty names (directory placeholders), names with `/`, the iterator's `prefixes`; folders and managed folders are not blobs |
| Modification-time watermark | `st_mtime_ns`, truncated to milliseconds | `LastModified`, truncated to seconds | `time_created` (`timeCreated`), truncated to milliseconds; `updated` is not used |
| Missing location | `FileNotFoundError` | empty selection; a missing bucket fails in boto3 | empty selection; a missing bucket fails with `google.api_core.exceptions.NotFound` |
| Native client | none | `client`, or `boto3.client("s3")` created at each read | `client`, or `google.cloud.storage.Client(project=None)` created at each read |
| Extra | none | `ingestio[s3]`, needed only without `client` | `ingestio[gcs]`, needed only without `client` |
| Writer | the same chain | the same chain, with the URI; no boto3 | the same chain, with the URI; no google-cloud-storage |

### The `gcs` extra (Q3)

Following the [pattern for extras](../docs/concepts/connectors.md#extras):

1. `[project.optional-dependencies]` gains `gcs = ["google-cloud-storage>=2.10"]`.
   `dependencies` stays empty.
2. `connector/gcs.py` imports google-cloud-storage only inside the functions that use it, and
   `Client` under `TYPE_CHECKING`.
3. A reader without `client` raises the `ImportError` above when google-cloud-storage is
   missing. With a `client`, and for the writer, the extra is not needed.
4. pyspark stays a prerequisite, in no extra.
5. The `dev` group gains `google-cloud-storage>=2.10`. No stubs package exists for it: typeshed
   has none, and the project publishes no `-stubs` companion. This spec expects that
   `uv run mypy` then reports `google.cloud.storage` as installed but without library stubs or
   a `py.typed` marker; it cannot check it, because the package is not installed in this
   working copy. If it does, the main session adds one override, and nothing broader:

   ```toml
   [[tool.mypy.overrides]]
   module = ["google.cloud.storage", "google.cloud.storage.*"]
   ignore_missing_imports = true
   ```

   mypy then sees `Client` as `Any`. The tests also import `google.api_core.exceptions`, which
   is expected to ship `py.typed`; if mypy reports it too, the same override names
   `google.api_core.*` as well. No test-only package, such as a GCS emulator, is added.

The floor is meant to be at or below the version that Databricks Runtime 15.4 LTS preinstalls,
if it preinstalls one, so that installing the extra there does not upgrade the runtime's
google-cloud-storage or the google-auth, google-api-core and protobuf it brings. This spec
cannot confirm whether the runtime preinstalls it, nor in which version: the main session
checks the runtime's release notes when it edits `pyproject.toml`, and lowers the floor to the
runtime's version if that is older. The calls the reader makes (`Client(project=None)`,
`Client.list_blobs` with `prefix` and `delimiter`, `Blob.name`, `Blob.time_created`) are older
than that floor and remain in the 3.x line; the lock and the integration tests check only the
locked version.

The main session adds these to `pyproject.toml` and refreshes `uv.lock` after approval, as it
did for PostgreSQL. Those files are outside every stage's write scope.

### Where it lives

- `src/ingestio/connector/gcs.py`: `GcsReader` and `GcsWriter`, with a module docstring and a
  sorted `__all__`.
- `src/ingestio/connector/all.py`: exports the two classes, `__all__` sorted.
- `pyproject.toml` and `uv.lock`: as in [The `gcs` extra](#the-gcs-extra-q3), by the main
  session.
- **Implementation note, not public API.** The rules that `GcsReader` shares with `DiskReader`
  and `S3Reader` (path parsing of a bucket URI, format and option checks, watermark
  validation, hidden names, the comparison, the sort, the empty DataFrame) may be factored
  into private helpers, such as the private module `src/ingestio/connector/_files.py` that the
  S3 spec suggests. The disk and S3 connectors then keep their behaviour and messages exactly,
  and `tests/test_disk_connectors.py` and `tests/test_s3_connectors.py` pass unchanged.
- No change to `ingestio/basics.py`, `ingestio/__init__.py`, `callback/` or
  `connector/core.py`.

### Supported environments

- pyspark 3.5 or later is a prerequisite that the runtime provides. ingestio never installs it.
- **Databricks on Google Cloud, classic compute,** on Databricks Runtime 15.4 LTS or later, in
  dedicated or standard access mode. Spark reaches GCS through Unity Catalog external
  locations, or a Google service account attached to the cluster, as the workspace is set up,
  with `gs://` URIs. Only the DataFrame API is used, never `sparkContext` or `_jvm`.
- **The listing does not go through Unity Catalog.** google-cloud-storage needs credentials of
  its own on the driver, with `storage.objects.list` on the bucket. What Application Default
  Credentials find depends on the cluster; when they find nothing usable, the job builds a
  `client` from credentials it obtains itself, such as a service account key kept in a secret
  scope. Only the driver needs the extra: the executors never import google-cloud-storage.
- **Databricks on AWS or Azure, reaching GCS:** no claim; not tested.
- **Open-source Spark 3.5 or later**, on a Java version that release supports (17 or 21 for
  Spark 4), with the Cloud Storage connector for Hadoop on the classpath
  (`com.google.cloud.bigdataoss:gcs-connector`, the release for the session's Hadoop version,
  usually its shaded JAR). The connector's file system, registered for `gs`, and its
  credentials are set in the Hadoop configuration (`spark.hadoop.*`), as the connector's
  release documents. Dataproc ships the connector: nothing in the design excludes it, but it is
  not tested.
- **Databricks serverless compute:** no claim; not tested.
- **Formats** outside Spark's built-ins need their package in the session, as for the disk
  connectors: `delta-spark` on open-source Spark, `spark-avro`.
- **Privileges.** The listing needs `storage.objects.list`. Spark's reads need
  `storage.objects.get`; its writes need `storage.objects.create`, and `storage.objects.delete`
  for replaces, for overwrites and for the copies of commits. The connector may need more, such
  as `storage.buckets.get`, as its release documents. On Databricks, through the external
  location's privileges.

### Known limitations

These are documented, not solved here:

- **Writes are only as atomic as Spark, the committer and the format make them** (see
  [Atomicity](#writing)). A failed replace of plain files loses the previous contents. A reader
  of the prefix during a write can see part of it.
- **`replace=True` deletes everything under the prefix** with a plain file format, blobs Spark
  did not write included. Never point a writer that replaces at a prefix that holds anything
  else. In a bucket with a retention policy, or with blobs under a hold, the deletes fail, and
  so does the replace. With soft delete enabled on the bucket, the deleted blobs are kept, and
  billed, for the bucket's retention period.
- **Several Delta writers on open-source Spark:** no claim (see Atomicity).
- **Late blobs.** A run that advances its watermark to the highest one it read never reads a
  blob that appears later with a watermark at or below it:
  - by name: a producer that delivers blobs out of order;
  - by `timeCreated`: GCS stamps it itself, so producers' clocks do not matter. This spec takes
    it to be stamped when the upload completes, resumable uploads included, since GCS creates
    the object only then; if it is not, a long upload that completes after a newer blob was
    read is never read, as for S3's multipart uploads;
  - by `timeCreated`, for a blob moved into the prefix: this spec does not establish whether a
    move (`objects.move`, or the rename of a folder in a bucket with hierarchical namespace)
    keeps a blob's creation time. A producer that moves blobs into the prefix should check it,
    or use name watermarks;
  - blobs that share the last millisecond read and appear after the read are skipped, because
    the comparison is strict;
  - a selected blob whose metadata changes between `read()` and the action that reads it
    reports a later `_metadata.file_modification_time`. A job that advances its watermark from
    that column then skips the blobs created in between.
- **Overwrites and metadata changes.** An overwrite creates a new generation, with a new
  `timeCreated`, so a blob overwritten after a run read it is read again, whole, by the next
  run. A change of its metadata alone does not make it read again. With name watermarks,
  neither does.
- **Blobs in progress.** GCS makes a blob visible only when its upload completes, so the reader
  never sees part of a blob. A Spark job that writes plain files into the prefix being read can
  still expose part of its batch (see Atomicity), and on Databricks the runtime's commit
  protocol can write data files at their final names before it marks them committed. The
  reader lists blobs; it does not read commit markers.
- **The selection is frozen, not the contents.** A blob overwritten before an action is read as
  it is then, or makes the action fail. A selected blob that is deleted makes the action fail,
  unless the job passes Spark's `ignoreMissingFiles`.
- **Cold storage classes.** Nearline, Coldline and Archive blobs are selected and read like any
  other, and each read of them is charged retrieval fees.
- **Glob characters.** Spark expands the characters `*`, `?`, `[`, `]`, `{`, `}` and `\` in the
  paths it loads. A selected blob whose URI holds one can be read as other blobs, or fail. The
  reader does not escape them.
- **Two configurations.** The client and Spark are configured separately. A client that lists
  with other credentials, or through another endpoint, than Spark's is not detected: the load
  fails, or reads blobs the listing did not see.
- **Name watermarks compare as text,** as in `DiskReader`.
- **An empty selection** returns a DataFrame that has no `_metadata` column and, without
  `schema`, no columns at all. A missing prefix, a typo included, reads as empty.
- **Large prefixes.** Each `read()` lists every blob directly under the prefix, up to 1000 per
  request, and Spark then requests the status of each selected blob before reading it.
- **Table formats.** The reader rejects `delta`, but it does not detect Iceberg, Hudi or other
  table formats.
- **Session settings apply,** as for the disk connectors, and **type conversion** is whatever
  Spark's data source does.

### Notes for testing

Every criterion has at least one test that always runs, with test doubles, without a JVM,
without network and without credentials. The doubles live in `tests/test_gcs_connectors.py`.

- **Fake GCS client.** It records every attribute accessed on it. `list_blobs(*args,
  **kwargs)` records a *listing*, with its arguments, and returns a fake iterator over the
  pages the test set, or raises an error the test gives.
- **Fake iterator.** It records every attribute accessed on it, and supports iteration. A page
  is a list of fake blobs and a set of prefixes. The iterator yields the blobs of each page in
  order and records how many pages were taken; its `prefixes` attribute holds the prefixes of
  the pages taken, as the SDK's iterator does. It can raise an error the test gives when the
  iteration reaches a given page. A page may hold no blob, as GCS's pages can.
- **Fake blob.** An object with the attributes `name`, `time_created`, `updated`, `size` and
  `storage_class`, which records every attribute accessed on it. Times are timezone-aware
  datetimes, as the SDK returns them; tests use `datetime.UTC`.
- **The factory.** Criteria that mention it replace the client class with
  `monkeypatch.setattr(google.cloud.storage, "Client", factory)`, where `factory` records the
  positional and keyword arguments of each call and returns the fake client, or raises an error
  the test gives. google-cloud-storage is in the `dev` group.
- **Fake Spark session and fake DataFrame:** those of the disk spec's Notes for testing, with
  loads, creates and saves recorded the same way.
- **Missing packages.** Map `pyspark` and `google.cloud.storage`, with their submodules, to
  `None` in `sys.modules`: in a subprocess for a fresh interpreter, or with
  `monkeypatch.setitem`.
- `google.api_core.exceptions.NotFound("The specified bucket does not exist.")` can be built
  without network.
- Tests use the public API. They pass the fakes as `client=`, `spark=` and DataFrames, with
  `typing.cast` to satisfy mypy strict.

### Integration tests

On top of the criteria, real tests run against GCS and a local Spark session, `local[2]`, with
`spark.sql.session.timeZone=UTC`. They are marked `@pytest.mark.integration` and skipped, with
a reason, unless the environment variable `INGESTIO_GCS_URL` is set. Its value is a scratch
location, `gs://<bucket>/<prefix>`. Each test works under its own unique sub-prefix and deletes
it at the end. Credentials come from Application Default Credentials, for google-cloud-storage
and for the Cloud Storage connector, which the session configures to use them, or the same
service account. They need Java 17 or 21, because `uv.lock` holds pyspark 4.2, and the Cloud
Storage connector for that Hadoop version; how the session gets it is the test-writer's
choice. On the maintainer's machine they are written blind: its only JDK is Java 27, on which
Spark 4.2 does not start, and it has no cloud credentials. They cover:

1. `GcsWriter(<sub>, "parquet")`: two calls of `write(df)` leave the rows of both batches, and
   `write(df, replace=True)` then leaves exactly the rows of `df`. A full
   `GcsReader(<sub>, "parquet")` returns them; `_SUCCESS` and any directory placeholder do not
   disturb it.
2. CSV round trip: written with `{"header": True}`, read back with the same option and a DDL
   `schema`, with the same rows.
3. Name watermark: CSV blobs uploaded by the test with google-cloud-storage. Only the rows of
   the blobs above the value are returned. A blob uploaded after `read()`, with a higher name,
   is absent from a second `count()` on the same DataFrame.
4. `timeCreated` watermark: the test uploads `a.csv`, waits until the clock is past its
   `time_created`, uploads `b.csv`, and takes `a.csv`'s `time_created` from a listing. A reader
   with that value returns only `b.csv`'s rows. The highest
   `unix_millis(_metadata.file_modification_time)` is at or above `b.csv`'s `time_created` in
   milliseconds, and a reader built with that instant, as an aware UTC datetime, selects
   nothing.
5. Metadata changes and overwrites: the test uploads `a.csv` and `b.csv` as in test 4, then
   sets custom metadata on both, which moves their `updated` time. A reader built with
   `b.csv`'s `time_created` selects nothing. The test then uploads `a.csv` again, an
   overwrite, and the same reader selects only `a.csv`.
6. Empty selection under a sub-prefix with no blob: with a `schema`, a DataFrame with its
   columns and no rows; without one, no columns and no rows.
7. A directory placeholder `<sub>/`, uploaded by the test as a zero-byte blob, and blobs under
   `<sub>/nested/`, are not read.
8. A reader built without `client` lists through the default client.
9. A write whose computation fails, through a Python UDF that raises, raises out of `write`.
10. `write(df, replace=True)` on a sub-prefix that holds `keep.txt`, uploaded by the test,
    removes it. This confirms the documented limitation.

## Acceptance criteria

The criteria follow the recommended option of every question. A criterion that depends on one
names it.

Unless a criterion says otherwise:

- `spark` is the fake session, `df` a fake DataFrame and `client` the fake GCS client, from
  [Notes for testing](#notes-for-testing);
- `URI` is `"gs://lake/landing/orders"`, and `u(name)` is `"gs://lake/landing/orders/" + name`;
- "the bucket holds `name`" means that the fake listing yields a blob whose `name` is
  `"landing/orders/" + name` and whose `time_created` and `updated` are both `T0`, or both the
  time `t` in "`name` at `t`". "The bucket holds the blob `n`" gives the whole name. "The
  prefix `p`" is `"landing/orders/" + p` among the prefixes of the page. Blobs come in the
  order the criterion lists them, on one page unless it says otherwise;
- `T0` is `datetime(2026, 10, 21, 9, 0, tzinfo=UTC)`;
- the reader is `GcsReader(URI, "csv", client=client, spark=spark)`, with whatever the
  criterion adds;
- the writer is `GcsWriter("gs://lake/orders", "parquet")`;
- "the factory" is the replacement of `google.cloud.storage.Client` described in
  [Notes for testing](#notes-for-testing);
- "the paths" of a load are the value its `load()` received;
- "exactly the options" means these keys and no others, compared case-insensitively unless
  the criterion says the case matters.

### Import surface and the extra

- **AT-001**: Given a fresh interpreter, when
  `from ingestio.connector.all import GcsReader, GcsWriter` runs, then it succeeds. Both names
  are in `ingestio.connector.all.__all__` and in `ingestio.connector.gcs.__all__`, and neither
  is in `ingestio.__all__`.
- **AT-002**: Given an interpreter where `import pyspark` and `import google.cloud.storage`
  raise `ImportError`, when `ingestio.connector.all` is imported, then the import succeeds.
  `GcsReader(URI, "csv", client=<fake client>, spark=<fake session>)` can be built, and with the
  bucket holding `a.csv` its `read()` returns the DataFrame of the fake's load.
  `GcsWriter("gs://lake/orders", "parquet")` can be built, and `write(<fake DataFrame>)`
  records one save.
- **AT-003**: Given an interpreter where `import google.cloud.storage` raises `ImportError`,
  when `GcsReader(URI, "csv")` or `GcsReader(URI, "csv", spark=spark)` is built, then it raises
  `ImportError` with the message
  `GcsReader needs google-cloud-storage: pip install 'ingestio[gcs]'`, whose `__cause__` is an
  `ImportError`. `GcsReader(URI, "csv", client=client)` and the writer are built without error.
- **AT-004**: Given the metadata of the installed distribution,
  `importlib.metadata.metadata("ingestio")`, then `gcs` is among its `Provides-Extra` values,
  one `Requires-Dist` names the `google-cloud-storage` project and carries an `extra == "gcs"`
  marker, and every `Requires-Dist` carries an `extra == "<name>"` marker for some extra.
  Depends on Q3.

### Building the connectors

- **AT-005**: Given `path` in `""`, `"lake/landing/orders"`, `"/Volumes/main/landing/orders"`,
  `"s3://lake/landing/orders"`, `"gcs://lake/landing/orders"`, `"GS://lake/landing/orders"`,
  `"gs:/lake/landing/orders"`, `"https://storage.googleapis.com/lake/landing/orders"` and
  `Path("gs://lake/landing/orders")`, when `GcsReader(path, "csv", client=client)` or
  `GcsWriter(path, "csv")` is built, then it raises
  `ValueError("path must be a gs:// URI")`.
- **AT-006**: Given the `path` on the left, when `GcsReader(path, "csv", client=client)` and
  `GcsWriter(path, "csv")` are built, then each raises `ValueError` with the message on the
  right, or is built when the cell says so:

  | `path` | `GcsReader` | `GcsWriter` |
  |---|---|---|
  | `"gs://"`, `"gs:///landing/orders"` | `path must name a bucket` | `path must name a bucket` |
  | `"gs://lake//orders"`, `"gs://lake/landing//orders"`, `"gs://lake/./orders"`, `"gs://lake/landing/../orders"`, `"gs://lake/landing/orders/.."` | `path must not contain empty, '.' or '..' segments` | the same |
  | `"gs://lake"`, `"gs://lake/"` | built | `path must name a prefix inside the bucket` |
  | `"gs://lake/landing/orders"`, `"gs://lake/landing/orders/"`, `"gs://my.bucket_01-x/a b/ç"`, `"gs://lake/_staging/orders"` | built | built |

- **AT-007**: Then:
  - `repr(GcsReader("gs://lake/landing/orders/", "csv", client=client))` is
    `GcsReader(path='gs://lake/landing/orders', format='csv')`;
  - `repr(GcsReader("gs://lake/landing/orders", "csv", client=client,
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261021"))` is the same;
  - `repr(GcsReader("gs://lake/", "json", client=client))` is
    `GcsReader(path='gs://lake', format='json')`;
  - `repr(GcsWriter("gs://lake/orders//", "parquet"))` is
    `GcsWriter(path='gs://lake/orders', format='parquet')`.
- **AT-008**: Given `format=""`, when either connector is built, then it raises
  `ValueError("format must not be empty")`. Given `"delta"` or `"Delta"`, when
  `GcsReader(URI, fmt, client=client)` is built, then it raises `ValueError` with the message
  `format '<fmt>' cannot be read file by file`, with the format as given, while
  `GcsWriter("gs://lake/orders", fmt)` is built. `"parquet"`, `"CSV"` and
  `"com.example.source"` are accepted by both.
- **AT-009**: Given the `watermark_pattern` on the left, when
  `GcsReader(URI, "csv", client=client, watermark_pattern=pattern)` is built, then it raises
  `ValueError` with the message on the right:

  | `watermark_pattern` | Message |
  |---|---|
  | `"("` | starts with `invalid watermark_pattern: `, and the `__cause__` is a `re.error` |
  | `r"orders_\d{8}\.csv"` | `watermark_pattern must have exactly one group` |
  | `r"(\d{4})(\d{4})\.csv"` | `watermark_pattern must have exactly one group` |
  | `""` | `watermark_pattern must have exactly one group` |

  `r"orders_(\d{8})\.csv"` and `r"(?:orders|sales)_(?P<day>\d{8})\.csv"` are accepted.
- **AT-010**: Given the arguments on the left, when `GcsReader(URI, "csv", client=client, ...)`
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
  `GcsReader(URI, "csv", client=client, options={key: "x"})` is built, then it raises
  `ValueError` with the message `option '<key>' is set by the connector`, with the key as
  given. Given `key` in `path`, `Paths`, `replaceWhere` and `PartitionOverwriteMode`, the same
  holds for `GcsWriter("gs://lake/orders", "parquet", options={key: "x"})`. The reader accepts
  `{"header": True, "inferSchema": "true", "mergeSchema": False}`, and the writer accepts
  `{"compression": "zstd", "header": True}`.
- **AT-012**: Given the factory, when these are built, then each succeeds, no attribute of
  `spark` or of `client` is accessed, no listing is recorded, and the factory is not called:
  - `GcsReader(URI, "csv", client=client, spark=spark)`;
  - `GcsReader(URI, "csv", spark=spark)`;
  - `GcsReader(URI, "csv", client=client, spark=spark,
    watermark_pattern=r"orders_(\d{8})\.csv", watermark_value="20261020")`;
  - `GcsReader(URI, "csv", client=client, spark=spark, watermark_value=T0)`;
  - `GcsWriter("gs://lake/orders", "parquet")`.

### Reading

- **AT-013**: Given the factory and the bucket holding `a.csv`, when `reader.read()` runs, then
  `list_blobs` is called once, with exactly one positional argument, `"lake"`, and exactly the
  keyword arguments `prefix="landing/orders/"` and `delimiter="/"`. The only attribute accessed
  on `client` is `list_blobs`, no attribute of the iterator it returned is accessed, no
  attribute other than `name` and `time_created` is accessed on the blob, and the factory is
  not called. Given `GcsReader("gs://lake/", "csv", client=client, spark=spark)` and the bucket
  holding the blob `a.csv`, then `list_blobs` receives `"lake"`, `prefix=""` and
  `delimiter="/"`, and the paths are `["gs://lake/a.csv"]`.
- **AT-014**: Given the bucket holding, in this order, `b.csv`, `a.csv`, `_SUCCESS`,
  `_metadata`, `.a.csv.crc`, `c.csv._COPYING_`, `_committed_123`, the blobs `landing/orders/`,
  `landing/orders/sub/` and `landing/orders/sub/d.csv`, and the prefixes `sub/` and `x.csv/`,
  when `read()` runs, then exactly one load is recorded. It has the format `csv`, no schema, no
  options, and the paths `[u("a.csv"), u("b.csv")]`, a `list` in this order. `read()` returns
  the very DataFrame that `load()` returned, no attribute of that DataFrame is accessed, and
  `createDataFrame` is not called. Given the bucket holding only `a.csv`, the paths are
  `[u("a.csv")]`, still a `list`.
- **AT-015**: Given the bucket holding `pedidos_ç.csv`, `a b.csv` and
  `2026-10-21T09:00:00.csv`, when `read()` runs, then the paths are
  `[u("2026-10-21T09:00:00.csv"), u("a b.csv"), u("pedidos_ç.csv")]`. Given
  `GcsReader("gs://lake/landing/orders///", "csv", client=client, spark=spark)` and the bucket
  holding `a.csv`, then `list_blobs` receives `prefix="landing/orders/"` and the paths are
  `[u("a.csv")]`.
- **AT-016**: Given three pages: the first holds `b.csv`, `a.csv` and the prefix `sub/`, the
  second holds only the prefix `x/` and no blob, and the third holds `c.csv`, when `read()`
  runs, then `list_blobs` is called once, the three pages are taken, and the paths are
  `[u("a.csv"), u("b.csv"), u("c.csv")]`.
- **AT-017**: Given the bucket holding `a.csv`, `schema="id INT, name STRING"` and
  `options={"header": True, "sep": ";"}`, when `read()` runs, then the load carries that very
  schema object and exactly the options `header` = `true` and `sep` = `;`. Given instead
  `schema=StructType([StructField("id", IntegerType())])`, the load carries that very object.
- **AT-018**: Given a reader built with `client` and without `spark`, while
  `pyspark.sql.SparkSession.active` is replaced with a function that raises, then building
  succeeds. When `active` is then replaced with a function that returns the fake session and
  `read()` runs with the bucket holding `a.csv`, then the load is recorded on the fake. Given
  `active` replaced with a function that raises `err` when `read()` runs, then `read()` raises
  `err` itself and no listing is recorded.
- **AT-019**: Given the factory, `GcsReader(URI, "csv", spark=spark)` and the bucket holding
  `a.csv`, then building does not call the factory. Each `read()` calls it exactly once, with
  no positional argument and exactly the keyword argument `project=None`, and the listing is
  recorded on the client it returned: two reads make two calls. Given the factory replaced by
  another after the reader was built, then the replacement is the one called. Given a factory
  that raises `err`, when `read()` runs, then it raises `err` itself, and no listing, load or
  `createDataFrame` call is recorded. Depends on Q2.
- **AT-020**: Given a reader built while the bucket holds nothing, when the bucket is set to
  hold `a.csv` and `read()` runs, then the load's paths are `[u("a.csv")]`. When the bucket is
  then set to hold `a.csv` and `b.csv` and `read()` runs again, a second listing is recorded,
  the second load's paths are `[u("a.csv"), u("b.csv")]`, and the first load's paths are still
  `[u("a.csv")]`.
- **AT-021**: Given a single page with no blob and no prefix, as GCS answers for a prefix that
  holds nothing, and then the bucket holding only `_SUCCESS`, the blob `landing/orders/` and
  the prefix `sub/`, when `read()` runs, then no load is recorded and `createDataFrame` is
  called once. It receives an empty list as data and, as schema, a
  `pyspark.sql.types.StructType` with no fields. `read()` returns what `createDataFrame`
  returned. Given `schema="id INT, name STRING"` in addition, the schema passed is that very
  object.
- **AT-022**: Given `err = NotFound("The specified bucket does not exist.")`, from
  `google.api_core.exceptions`:
  - when the iteration raises `err` at the first page, or at the second page after a first page
    that holds `a.csv`, or when `list_blobs` itself raises it, then `read()` raises `err`
    itself, and no load and no `createDataFrame` call is recorded;
  - given a fake whose `load()` raises `err2 = RuntimeError("[PATH_NOT_FOUND]")` and the bucket
    holding `a.csv`, then `read()` raises `err2` itself;
  - given a fake whose `createDataFrame` raises `err2` and a listing with no blob, then
    `read()` raises `err2` itself.

### Watermark from the blob name

In this section `P` is `r"orders_(\d{8})\.csv"`, and the reader is
`GcsReader(URI, "csv", client=client, spark=spark, watermark_pattern=P, ...)`.

- **AT-023**: Given the bucket holding:
  - `orders_20261019.csv` at `T0 + 2 s`, `orders_20261020.csv` at `T0 + 1 s` and
    `orders_20261021.csv` at `T0`;
  - `customers_20261021.csv`, `orders_latest.csv` and `orders_20261021.csv.bak`;

  when `read()` runs with `watermark_value="20261019"`, then the paths are
  `[u("orders_20261020.csv"), u("orders_20261021.csv")]`. Without `watermark_value`, they are
  the three `orders_<date>.csv` blobs, sorted by name.
- **AT-024**: Given the blobs and the value on the left, when `read()` runs, then the blobs on
  the right are selected. "None" means no load and one `createDataFrame` call.

  | Pattern | Blobs in the bucket | `watermark_value` | Selected, in the order of the paths |
  |---|---|---|---|
  | `P` | `orders_20261019.csv`, `orders_20261020.csv`, `orders_20261021.csv` | `"20261021"` | none |
  | `P` | the same | `"2026102"` | `orders_20261020.csv`, `orders_20261021.csv` |
  | `P` | the same | `""` | all three |
  | `r"part_(\d+)\.csv"` | `part_9.csv`, `part_10.csv` | `"9"` | none |
  | `r"part_(\d+)\.csv"` | the same | `"1"` | `part_10.csv`, `part_9.csv` |

- **AT-025**: Without `watermark_value`:
  - given `watermark_pattern=r"(\d{8})\.csv"` and the bucket holding `orders_20261021.csv`,
    then nothing is selected, because the whole name must match;
  - given `watermark_pattern=r"landing/orders/orders_(\d{8})\.csv"` and the same bucket, then
    nothing is selected, because the pattern sees the name, not the blob's full name;
  - given `watermark_pattern=r"orders(_\d{8})?\.csv"` and the bucket holding `orders.csv` and
    `orders_20261021.csv`, then the paths are `[u("orders_20261021.csv")]`;
  - given `watermark_pattern=r".*_(\d{8})\.csv"` and the bucket holding `_orders_20261022.csv`,
    `.orders_20261022.csv`, the blob `landing/orders/sub/orders_20261022.csv` and
    `orders_20261021.csv`, then the paths are `[u("orders_20261021.csv")]`.

### Watermark from `timeCreated`

In this section the reader is `GcsReader(URI, "csv", client=client, spark=spark, ...)`, with no
`watermark_pattern`.

- **AT-026**: Given the bucket holding `c.csv` at `T0`, `b.csv` at `T0 + 500 ms` and `a.csv` at
  `T0 + 1 s`, when `read()` runs with the `watermark_value` on the left, then the paths are
  those on the right:

  | `watermark_value` | Paths |
  |---|---|
  | none | `u("a.csv")`, `u("b.csv")`, `u("c.csv")` |
  | `T0 - 1 ms` | `u("a.csv")`, `u("b.csv")`, `u("c.csv")` |
  | `T0` | `u("a.csv")`, `u("b.csv")` |
  | `T0 + 499 999 µs` | `u("a.csv")`, `u("b.csv")` |
  | `T0 + 500 ms` | `u("a.csv")` |
  | `T0 + 1 s` | none: no load, one `createDataFrame` call |

  Depends on Q1.
- **AT-027**: Given the bucket holding `a.csv` at `T0 + 500 999 µs`, finer than GCS's
  milliseconds, as a client that gave finer times would report it, when `read()` runs with the
  `watermark_value` on the left, then `a.csv` is selected or not as on the right:

  | `watermark_value` | `a.csv` selected |
  |---|---|
  | `T0 + 500 ms` | no |
  | `T0 + 500 500 µs` | no |
  | `T0 + 499 999 µs` | yes |
  | `datetime(2026, 10, 21, 6, 0, 0, 500000, tzinfo=timezone(timedelta(hours=-3)))` | no |
  | `datetime(2026, 10, 21, 6, 0, 0, 499999, tzinfo=timezone(timedelta(hours=-3)))` | yes |

  Depends on Q1.
- **AT-028**: Given the bucket holding `a.csv`, whose `time_created` is `T0` and whose
  `updated` is `T0 + 1 h`, and `b.csv` at `T0 + 2 s`, when `read()` runs with the
  `watermark_value` on the left, then the paths are those on the right, and no attribute other
  than `name` and `time_created` is accessed on either blob:

  | `watermark_value` | Paths |
  |---|---|
  | `T0 - 1 ms` | `u("a.csv")`, `u("b.csv")` |
  | `T0` | `u("b.csv")` |
  | `T0 + 2 s` | none: no load, one `createDataFrame` call |

  Depends on Q1.

### Writing

- **AT-029**: Given the factory, when `writer.write(df)` runs, then exactly one save is
  recorded, with the format `parquet`, the mode `append`, no options, and the path
  `"gs://lake/orders"`, a `str`. The only attribute accessed on `df` is `write`. The only
  attributes accessed on the fake writer are `format`, `option` or `options`, `mode` and
  `save`. `write` returns `None`, and the factory is not called.
  `writer.write(df, replace=False)` gives the same.
- **AT-030**: When `writer.write(df, replace=True)` runs, then the save is the one of AT-029,
  except that its mode is `overwrite`.
- **AT-031**: Given `GcsWriter("gs://lake/orders/", "csv", options={"header": True,
  "Compression": "gzip"})`, when `write(df)` runs, then the save has the format `csv`, the path
  `"gs://lake/orders"`, and exactly the options `header` = `true` and `Compression` = `gzip`,
  with the key `Compression` in this case.
- **AT-032**: Given a save that raises `err = RuntimeError("task failed")`, when `write(df)` or
  `write(df, replace=True)` runs, then it raises `err` itself, and `save()` was called once.

### Composition

- **AT-033**: Given the bucket holding `a.csv` and
  `Process([Step("extract", [ReadSource(reader, into="orders")]),
  Step("load", [WriteTarget(writer, key="orders")])])`, when it runs, then
  `process.state["orders"]` is the DataFrame that the load returned, and one save is recorded
  on it, with the mode `append`. The test binds the reader to a variable annotated
  `Reader[DataFrame]` and the writer to one annotated `Writer[DataFrame]`, and mypy strict
  accepts both.
- **AT-034**: Given `caplog` capturing every level and the factory, when readers with and
  without `client` and writers are built, `read()` runs with a selection that holds blobs and
  with an empty one, and `write()` runs with and without `replace=True`, then no log record is
  captured.

## Decisions

Taken by the maintainer on 2026-10-23, in conversation, recorded here and not reopened:

1. **Five connectors, each with a reader and a writer, one spec per system:** S3, GCS, Unity
   Catalog, Databricks Volumes and BigQuery. This spec is GCS's. S3, GCS and Volumes are like
   the disk connectors, because they handle files and receive the file format.
2. **Spark is the engine, with the contract of `DiskReader` and `DiskWriter`.** The reader
   returns a Spark DataFrame and the writer receives one. The native client does control work
   only: google-cloud-storage lists the blobs under a prefix and selects those above the
   watermark, and Spark loads exactly those `gs://` URIs. The reader has the arguments of
   `DiskReader` and `S3Reader` (`format`, `watermark_pattern` on the blob's name or else its
   time, `watermark_value`, `schema`, `options`, `spark`) and their selection rules: hidden and
   in-progress names, strictly greater, sorted, frozen list, empty selection, Delta rejected by
   the reader, reserved options, flat listing. Only the listing differs. The writer is Spark
   writing straight to the `gs://` URI, with `write(data, *, replace=False)` and the atomicity
   limits of plain files; Delta is atomic through its log. The writer needs no
   google-cloud-storage.
3. **Credentials of the listing client: an optional `client` argument,** a
   `google.cloud.storage.Client`. Without it, the reader creates the SDK's default client, with
   Application Default Credentials, at `read()` time, never at construction. Spark's own access
   to GCS is always the environment's concern (on Databricks on Google Cloud, service accounts
   and Unity Catalog external locations; elsewhere, the configuration of the Cloud Storage
   connector for Hadoop), documented and never configured by the connector.
4. **The extra is named after the system:** google-cloud-storage goes into a new extra,
   `ingestio[gcs]`, imported lazily, so the import surface works without it. The main session
   adds the extra, the `dev` packages and whatever typing support mypy strict needs to
   `pyproject.toml` and `uv.lock` after approval.
5. **Everything else follows the disk, PostgreSQL and S3 specs:** ADR 0013's constructor
   arguments, pyspark imported lazily, the session looked up at read time, errors propagated
   unchanged, no log lines, the ADR 0012 batch-leaning amendment, and exports from
   `ingestio.connector.all` only.

Proposed by this spec, following the precedents where they give one, and accepted by the main
session with its approval on 2026-10-23, under the maintainer's delegation:

6. **`GcsReader` and `GcsWriter` in a new `connector/gcs.py`, slug `gcs_connectors`.** This is
   the S3 spec's answer to its question 1: one module per medium, as CONTRIBUTING names them.
   The acronym is written as a word, as in `JsonApiReader` and `JsonLinesWriter`.
7. **Building validates and does nothing else, except the extra check of a reader without
   `client`,** as the S3 spec's decision 6. A reader with a `client` imports no package of its
   own, so the import surface and the criteria work without google-cloud-storage.
8. **One `path` URI, `gs://` only, in lowercase,** with the S3 spec's answers to its question
   2: the bucket root rejected by the writer only, empty, `.` and `..` segments rejected, and
   `path` a `str` only. `gs://` is the one scheme of the Cloud Storage connector for Hadoop and
   of Databricks, so the connector accepts no other, and Spark receives it as written.
9. **One `list_blobs` call per read, on the client, with the bucket's name, `prefix` and
   `delimiter="/"`, iterated to its end before anything loads.** The SDK follows the page
   tokens itself, and the fake stays a list of pages. `client.bucket(name).list_blobs(...)`
   would only put an object between the reader and the same call; `get_bucket` would add a
   request and need `storage.buckets.get`, which read-only roles may lack; a `fields`
   projection that left out the page token would break the paging. The SDK's defaults for
   timeout and retry apply. The delimiter makes the listing flat, as `DiskReader`'s, and keeps
   the contents of sub-prefixes out of the requests.
10. **A blob's name is its full name after the listing prefix.** Empty names and names with
    `/` are not candidates, and the iterator's `prefixes` are never read, so directory
    placeholders, sub-prefixes, folders and managed folders never reach Spark, whatever the
    bucket's namespace. The hidden-name rule of `DiskReader` then applies to the name. This is
    the S3 spec's decision 8.
11. **Spark receives `<normalised path>/<name>` per selected blob, sorted by name, always as a
    list,** as in the disk spec's decision 8.
12. **A default client is created at each `read()`, looked up on the `google.cloud.storage`
    module, one per read, never cached and never closed,** as the S3 spec's decision 10. A
    given client is used as it is for every read, and never closed or reconfigured. The exact
    call is **Q2**.
13. **Formats, watermark values, options and reserved keys are the disk connectors', with the
    same messages,** and there are no default options.
14. **The writer is `DiskWriter`'s single call chain with the URI.** It takes no session and no
    client, needs no google-cloud-storage, and lists, creates and deletes nothing itself.
15. **Errors propagate unchanged and the connectors write no log lines,** as in the S3 spec's
    decision 13. That includes `NotFound` for a missing bucket, `Forbidden` for a denied
    listing, and google-auth's `DefaultCredentialsError`.
16. **Every storage class is a candidate,** as in the S3 spec's decision 14. GCS serves every
    class without a restore, so a selected Nearline, Coldline or Archive blob is read, at its
    retrieval cost.
17. **Only live blobs are listed.** The reader passes neither `versions` nor `soft_deleted`.
18. **A prefix with no blob is an empty selection, and a missing bucket fails with the SDK's
    `NotFound`,** as the S3 spec's answer to its question 4. In a bucket with hierarchical
    namespace, an empty folder and a missing one give the same empty listing.
19. **Supported environments:** Databricks on Google Cloud, classic compute, on Runtime 15.4
    LTS or later, and open-source Spark 3.5 or later with the Cloud Storage connector for
    Hadoop. Databricks on AWS or Azure reaching GCS, and serverless compute: no claim, not
    tested. Dataproc: nothing excludes it, not tested. This adapts the S3 spec's decision 16.
20. **Glob characters are a documented limitation,** as the S3 spec's answer to its question 9.
21. **The criteria run with a fake client and a fake Spark session, with no test-only package;
    the integration tests are opt-in through `INGESTIO_GCS_URL` and written blind,** as the S3
    spec's answer to its question 7. No GCS emulator is added.
22. **The selection code may be shared with `DiskReader` and `S3Reader` through private
    helpers,** with no change to their behaviour, messages or tests, as the S3 spec's
    decision 17. This is an implementation note, not public API.

### ADRs

- **The philosophy does not change.** The connectors fit the connector concept and apply a
  format they are given, as the disk and S3 connectors do.
- **No decision closes the door to streaming.** `Reader` and `Writer` are unchanged. Like the
  disk and S3 connectors, these lean towards batch: one selection per `read()`, a watermark
  fixed when the reader is built, and a write that needs the whole DataFrame. A streaming source
  on GCS, such as Auto Loader, Pub/Sub notifications for Cloud Storage or Structured
  Streaming's file source, tracks what it processed and would be a different connector. This
  belongs in the "What leans towards batch" list of
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md), next to the S3 connectors: an
  amendment, not a new ADR.
- **ADR 0013 applies.** `path`, `format`, `watermark_pattern` and `schema` become configuration
  keys by its convention, `watermark_value` is computed by the job, and `client` carries the
  credentials, separate from the configuration. The writer's `path` names the target, as for
  `DiskWriter`. Selecting blobs before loading is how this reader filters inside the source.
  The default client's lookup of Application Default Credentials falls under the consequence
  that the answer to the S3 spec's question 6 adds to ADR 0013: an SDK's default client,
  created when the job passes none, follows the SDK's own lookup, which the connector neither
  reads nor configures. No new ADR is needed. If the wording that lands names only boto3's
  lookup, the doc-keeper widens it to name Application Default Credentials too.
- **ADR 0002 applies unchanged.** The `gcs` extra follows the pattern for extras, with rule 3
  read as in the S3 spec: a reader with a `client` needs no package.

## Open questions

None open. On 2026-10-23 the maintainer, away for the session, delegated the approval of this
spec to the main session, with the instruction to take the recommended option of every question
that the maintainer's decisions do not settle. The main session took the recommended option of
all three; none contradicts decisions 1 to 5. The options are kept below as the record of what
was weighed, for the maintainer to review.

1. **The time of a blob's watermark, and its precision.** GCS gives a blob two times, both to
   the millisecond: `timeCreated`, the creation time of its current generation, which an
   overwrite renews; and `updated`, which also moves forward whenever only its metadata
   changes. Spark's `_metadata.file_modification_time` is what the Cloud Storage connector for
   Hadoop reports: the `updated` time, in the releases this spec knows of.
   - (a) `time_created`, truncated to milliseconds. The column is never below it, whichever of
     the two times the connector reports, so a job that advances its watermark from that column
     never re-reads its last blob. A blob whose metadata alone changed after it was read, for
     instance by a producer that sets custom metadata after the upload, is not read again; an
     overwritten blob is. This matches the disk connectors' modification time, which follows
     the content and not the metadata.
   - (b) `updated`, truncated to milliseconds. It equals the column when the connector reports
     `updated`. But every change of metadata selects an ingested blob again and reloads its
     rows; and with a connector that reported `timeCreated`, a blob whose metadata had changed
     would be selected again on every run, the failure that the S3 spec's question 3 avoids.
   - (c) Either time, truncated to whole seconds, as in `S3Reader`. Coarser than both GCS and
     Spark, for no gain in agreement: it only widens the window of blobs that share the last
     second read, arrive later, and are skipped.

   **Recommended: (a).**
2. **The default client, exactly.** Listing a bucket needs credentials but no project.
   - (a) `google.cloud.storage.Client(project=None)`: Application Default Credentials, and no
     project. The SDK treats an explicit `None` as "no project" and looks none up, so the
     default works wherever ADC finds credentials, including credentials that carry no
     project.
   - (b) `google.cloud.storage.Client()`, the SDK's literal default. It also looks up a
     project, through the environment, the credentials, the gcloud configuration or the
     metadata server, and raises `OSError("Project was not passed and could not be determined
     from the environment.")` where it finds none, although the listing would not use it.
   - (c) (b), retried as (a) on that `OSError`. It catches an error to carry on, which this
     spec's "nothing is caught" excludes.

   **Recommended: (a).** Integration test 8 checks that it lists.
3. **The extra, the `dev` group and the type of `client`.**
   - The extra: (a) `gcs = ["google-cloud-storage>=2.10"]`, a floor at or below what
     Databricks Runtime 15.4 LTS preinstalls, if it preinstalls the package, so that installing
     the extra there upgrades nothing; the main session confirms the runtime's version when it
     edits `pyproject.toml` and lowers the floor to it if that is older; (b) no floor; (c) a
     recent floor, such as `>=3.0`, which could make pip upgrade the runtime's copy and the
     google-auth, google-api-core and protobuf it brings. **Recommended: (a).**
   - The `dev` group and mypy: (a) `google-cloud-storage>=2.10` in the `dev` group, no stubs
     (none exist), and the override of [The `gcs` extra](#the-gcs-extra-q3) for
     `google.cloud.storage` if mypy strict reports it untyped, as this spec expects; (b) the
     same, with `follow_imports = "skip"` in the override as well, so that mypy keeps ignoring
     the package if a later release ships `py.typed` with incomplete annotations; (c) a private
     protocol for the client in `connector/gcs.py`, with `list_blobs` only, and no override;
     that private name would then show in the public signature. **Recommended: (a).**
   - The type of `client`: (a) `google.cloud.storage.Client | None`, imported under
     `TYPE_CHECKING`, which mypy sees as `Any | None` while the package is untyped, and users
     see as the SDK's class in signatures and documentation; (b) `Any`; (c) the private
     protocol of the previous point. **Recommended: (a).**

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-23 | planning | draft | Drafted by the planner from the maintainer's five decisions, on the model of the S3 spec: `GcsReader`/`GcsWriter` in `connector/gcs.py`; the reader lists one `gs://` prefix with one `client.list_blobs(<bucket>, prefix=<prefix>/, delimiter="/")` iterated to its end, keeps the S3 candidate rules, selects by name pattern or by `time_created` truncated to milliseconds, and loads exactly the sorted list of URIs; the writer is `DiskWriter`'s chain on the URI and needs no google-cloud-storage; the `ingestio[gcs]` extra. 34 acceptance criteria, 10 integration scenarios, 22 decisions (5 by the maintainer, 17 proposed), 3 open questions, and a three-column Disk/S3/GCS "Differences" table. |
| 2026-10-23 | approval | approved | Approved by the main session under the maintainer's delegation of 2026-10-23 ("Delegar e ir até o fim"), with the recommended option of all 3 questions: `time_created` truncated to milliseconds (`updated` not used), the default client `google.cloud.storage.Client(project=None)`, and `gcs = ["google-cloud-storage>=2.10"]`. The 17 proposed decisions accepted. The main session checked on the installed package (3.17.1, locked): `Client(project=None)` sets no project and asks for none; the package ships no `py.typed`, so it added to `pyproject.toml` a mypy override, `ignore_missing_imports` for `google.cloud.storage` and `google.cloud.storage.*`, with which `from google.cloud.storage import Client` passes mypy strict (the forms `from google.cloud import storage` and `import google.cloud.storage` still fail, because mypy then reports the `google.cloud` namespace). It added the extra and the dev package `google-cloud-storage>=2.10` and refreshed `uv.lock`. Not verified: which google-cloud-storage version Databricks Runtime 15.4 LTS preinstalls, and the Hadoop connector's source of `file_modification_time`, which integration test 4 checks. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_gcs_connectors.py`: 67 tests (150 cases) covering AT-001 to AT-034 with a fake GCS client, iterator and blobs, a fake `google.cloud.storage.Client` factory patched by string, and the fake Spark doubles, two tests of behaviour no criterion names, and the 10 integration scenarios, skipped unless `INGESTIO_GCS_URL` is set (written blind; the Spark tier loads the shaded gcs-connector 4.0.6 JAR, replaceable through `INGESTIO_GCS_CONNECTOR_JAR`). Two local `# type: ignore[no-untyped-call]` cover the unannotated constructors of `NotFound` and `DefaultCredentialsError`. Checked by the main session: collection fails as expected with `ImportError: cannot import name 'GcsReader' from 'ingestio.connector.all'`; the rest of the suite passes (462 passed, 24 skipped), the PostgreSQL and S3 AT-004 tests included; ruff and format clean. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `GcsReader` and `GcsWriter` in a new section of `docs/concepts/connectors.md`, in the shape of the S3 section; turned "Differences from the disk connectors" into one shared section with Disk, S3 and GCS columns, ready for Volumes; added the `gcs` extra to the extras table and rule 3; amended ADR 0002 (the `gcs` extra), ADR 0012 (leans towards batch) and ADR 0013 (the default-client consequence names `google.cloud.storage.Client(project=None)` and Application Default Credentials; the writer's `path`; the reader's selection); updated `docs/index.md` and the architecture KB. Run by the main session: `tests/test_docs_examples.py` fails only on the two new runnable GCS blocks, with the expected `ImportError` of the missing classes (14 passed, the S3 blocks included). |
| 2026-10-23 | implementation | implemented | The implementer added `GcsReader` and `GcsWriter` in `src/ingestio/connector/gcs.py`, mirroring `s3.py` and built on the shared `_files.py` rules with millisecond units: the reader lists with `client.list_blobs(bucket, prefix=..., delimiter="/")`, reads only `blob.name` and `blob.time_created`, and creates `Client(project=None)` at read time through `from google.cloud.storage import Client`; exported from `connector/all.py`. It also gave `_files.py` the sorted `__all__` that the S3 validation asked for. Checked by the main session: 604 passed, 34 skipped (the opt-in integration tests), ruff, format, mypy strict and `uv lock --check` clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-034 covered (140 passed, the 10 integration scenarios skipped without `INGESTIO_GCS_URL`), the full suite with the documentation examples 604 passed and 34 skipped, ruff, format, mypy strict and `uv lock --check` clean; code, documentation, philosophy, constraints and write scopes match. The sorted `__all__` of `_files.py` passes ruff's RUF022, and a differential probe of the committed disk connectors against the current ones (40,320 constructions, 672 reads, 560 writes) found identical behaviour and messages; the S3 tests pass unchanged. Two cosmetic documentation points (a rule-3 line not indented under its list item in the Extras section, one 102-character line in ADR 0013) were handed to the doc-keeper of `volume_connectors`, which edits the same pages. Still unverified: the google-cloud-storage version Databricks Runtime 15.4 LTS preinstalls, the Hadoop connector's source of `file_modification_time`, and the blind integration tests. |
| 2026-10-23 | amendment | validated | AT-033 superseded by [specs/config_connectors.md](config_connectors.md), decision 17, approved by the maintainer on 2026-10-23: `ReadSource` and `WriteTarget` no longer take a connector, so the composition this criterion describes is covered by that spec's criteria. The criterion's test goes in the tests stage of that spec; this spec keeps its status. |
