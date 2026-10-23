# 0013. Give a connector an ingestion's parameters as constructor arguments

- **Status:** accepted
- **Recorded:** 2026-10-23
- **Evidence:** [specs/postgres_connectors.md](../../specs/postgres_connectors.md), decisions 2, 3,
  27 and 31, taken by the maintainer; `src/ingestio/connector/postgres.py`; rationale given by
  the maintainer on 2026-10-23. Amended on 2026-10-23 by
  [specs/s3_connectors.md](../../specs/s3_connectors.md), question 6, and extended the same day
  to the GCS connectors by [specs/gcs_connectors.md](../../specs/gcs_connectors.md) and to the
  volume connectors by [specs/volume_connectors.md](../../specs/volume_connectors.md) and to
  the Unity Catalog connectors by
  [specs/unity_catalog_connectors.md](../../specs/unity_catalog_connectors.md) and to the
  BigQuery connectors by [specs/bigquery_connectors.md](../../specs/bigquery_connectors.md).

## Context

The parameters of one ingestion (the source table, the primary key, the watermark column) will
come from a JSON job configuration that the library does not define yet. The PostgreSQL
connectors are the first to need those parameters, so they set the example for every later
connector and for the specification of the configuration. Connection credentials are a
different matter: on Databricks they come from `dbutils.secrets.get`, which the library never
imports.

## Decision

- A connector takes an ingestion's parameters as constructor arguments named after the keys of
  the job configuration (`table_name`, `watermark_column`). It never reads configuration files,
  environment variables or secret stores, and it has no `from_config`. Mapping configuration
  keys to arguments is the job's task, and a later spec covers the configuration file and
  recipes.
- Credentials are separate arguments, never part of the configuration. The PostgreSQL
  connectors group them, with the host and database, in a `PostgresConnection` value.
- A reader extracts only what the job needs, and does the filtering inside the source: a
  watermark filter runs in the system that holds the data, not in the engine that receives it.

## Rationale

The maintainer gave two reasons when accepting this decision:

- **Arguments, not configuration.** The connector does not need to read the configuration:
  the job's `main.py` already reads it, and only has to pass the values to the connector.
- **Filtering in the source.** Filtering inside the source avoids overloading it and saves
  processing. In an incremental ingestion, the amount of data the job handles should be as
  small as possible.

The spec gives one reason for the credentials: they never live in the JSON, because on
Databricks they come from `dbutils.secrets.get`, which the library never imports. It also ties
the rule to the philosophy's "compose, do not impose": the job builds the connectors from its
own configuration and its own secrets.

## Consequences

- Every future connector follows the convention, and the configuration spec must map its keys
  to these arguments. A key that a connector does not use is not an argument: neither
  PostgreSQL connector accepts `primary_key`, because a plain read, an append and a replace do
  not use it.
- Decision 27 of the spec has a consequence. The maintainer chose `table_name` for the writer
  too, so both connectors take an argument of that name, but it does not mean the same thing in
  both places. In the job configuration `table_name` is the source table. On
  `PostgresWriter` it names the target. This is recorded here and not resolved: a job that
  writes to a table other than its source must not pass the configuration's `table_name` to the
  writer.
- `DiskWriter` has the same consequence: its `path` names the target, as `table_name` does on
  `PostgresWriter`, while in the job configuration `path` is the source directory. A job that
  writes somewhere other than where it reads must not pass the configuration's `path` to the
  writer.
- For a file system, the unit a reader can filter is the file. `DiskReader` filters inside the
  source by selecting the files above the watermark before Spark loads anything, so Spark
  never opens a file that was not selected
  ([spec](../../specs/disk_connectors.md)).
- `S3Writer` has the same consequence: its `path` names the target, while in the job
  configuration `path` is the source prefix.
- `GcsWriter` has the same consequence: its `path` names the target, while in the job
  configuration `path` is the source prefix.
- `VolumeWriter` has the same consequence: its `volume` and `path` name the target, while in
  the job configuration they are the source volume and directory.
- `UnityCatalogWriter` has the same consequence: its `table_name` names the target, while in the
  job configuration `table_name` is the source table. A job that writes to a table other than
  its source must not pass the configuration's `table_name` to the writer.
- A predicate in the table's scan is how `UnityCatalogReader` filters inside the source: Delta
  compares it with the statistics of each file and skips the files that cannot match
  ([spec](../../specs/unity_catalog_connectors.md)).
- `BigQueryWriter` has the same consequence: its `table_name` names the target, while in the job
  configuration `table_name` is the source table. A job that writes to a table other than its
  source must not pass the configuration's `table_name` to the writer.
- The `filter` option that `BigQueryReader` passes to Spark's BigQuery data source, which hands it
  to the Storage Read API as the row restriction of the read session, is how it filters inside the
  source: the rows outside the slice never leave BigQuery
  ([spec](../../specs/bigquery_connectors.md)).
- The BigQuery connectors keep credentials out of the configuration and out of `options`: they
  come from the environment (Application Default Credentials) or from the `client` the job builds,
  and `billing_project` only says which project is billed. A default client, created when the job
  passes none, follows Application Default Credentials, which the connector neither reads nor
  configures. Spark and the client each find them on their own.
- Selecting objects before loading is how `S3Reader` filters inside the source: Spark never
  opens an object that was not selected ([spec](../../specs/s3_connectors.md)). `GcsReader`
  filters the same way: Spark never opens a blob that was not selected
  ([spec](../../specs/gcs_connectors.md)), and so does `VolumeReader`: Spark never opens a file
  that was not selected ([spec](../../specs/volume_connectors.md)).
- A client that the job gives a connector, such as the `client` of `S3Reader`, carries the
  credentials, the region and the endpoint, separate from the configuration.
- An SDK's default client, created when the job passes none, follows the SDK's own lookup of
  credentials (environment variables, configuration files, instance metadata). The connector
  neither reads nor configures that lookup, so the rule above, that a connector never reads
  configuration files, environment variables or secret stores, holds for the connector's own
  code. It covers `boto3.client("s3")` for `S3Reader`, `google.cloud.storage.Client(project=None)`
  for `GcsReader`, which follows Application Default Credentials, `WorkspaceClient()` of the
  Databricks SDK for `VolumeReader`, which follows the SDK's default authentication, and the
  default clients of the other SDKs that later connectors use. A job that wants other
  credentials builds the client itself and passes it.
- A reader that filters inside the source cannot offer a free-form query, because a custom
  query cannot safely be bounded by a watermark.
- Nothing here closes the door to streaming.
