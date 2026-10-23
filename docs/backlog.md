# Backlog

Ideas the maintainer has had and wants implemented, recorded before anyone works on them. An
idea here is not a decision: nothing in this file is approved, and nothing here overrides the
[philosophy](philosophy.md), a spec or an ADR. Each idea lists the questions the maintainer
must answer before it can be worked on.

An idea that changes `src/` goes to the planner and becomes a spec, as described in
[workflow.md](workflow.md). Its entry then links to that spec, and the entry is removed when
the work lands.

## Shared by ideas 3 to 9

Ideas 3 to 9 are connectors that the maintainer listed on 2026-10-23. Each one changes `src/`,
so each becomes a spec, and its third-party package goes into an extra named after the system,
like `ingestio[postgres]`. These questions apply to all of them:

- **Engine.** The PostgreSQL and disk connectors move data with Spark: the reader returns a
  Spark DataFrame, the writer receives one, and a native client only does control work
  (psycopg runs the transaction, `os.scandir` lists the files). Do the new connectors follow
  that pattern? The alternatives are that each uses its system's native client without Spark,
  with records, bytes or pandas in the Python process, or that each spec decides for itself.
- **Order and grouping.** One spec per system, or grouped specs: S3 and GCS as object storage,
  Unity Catalog and Volumes as Databricks, MySQL with its CDC? In which order? Every connector
  changes the same files (`docs/concepts/connectors.md`, `src/ingestio/connector/all.py`,
  `pyproject.toml`, ADRs 0012 and 0013), so only the planning of several connectors can run in
  parallel. Every later stage runs one connector at a time.

## 3. S3 reader and writer, with boto3

**Idea.** A connector that reads from and writes to Amazon S3, built on boto3.

**Spec:** [specs/s3_connectors.md](../specs/s3_connectors.md), which answers the questions
below. The entry is removed when the work lands.

**Open questions.**

- What does boto3 do in the reader? With the Spark pattern, boto3 lists the objects under a
  prefix and selects the ones above the watermark, and Spark loads only those, as `DiskReader`
  does with a directory. `DiskReader` rejects `s3://` URIs today. Without Spark, boto3
  downloads the objects itself and the payload is bytes or records.
- What does boto3 do in the writer? If Spark writes to `s3://`, the writer may not need boto3
  at all. Plain files that Spark writes to S3 are not atomic, because S3 has no rename, while
  Delta is atomic through its log. Is that limit acceptable, as it is for `DiskWriter`?
- Where does an object's watermark come from: its key, through a pattern, or its
  `LastModified`? `LastModified` is precise to the second, which is coarser than the
  millisecond `DiskReader` uses.
- How does the connector get its credentials? boto3 and Spark look them up separately. On
  Databricks, Spark reaches S3 through Unity Catalog external locations or an instance profile,
  and boto3 needs its own credentials. Does the job pass them as arguments
  ([ADR 0013](decisions/0013-connector-configuration-convention.md)), or a `boto3.Session`?
- Does it support S3-compatible stores, such as MinIO or R2, through an endpoint URL?

## 4. GCS reader and writer

**Idea.** A connector for Google Cloud Storage.

**Spec:** [specs/gcs_connectors.md](../specs/gcs_connectors.md), which answers the questions
below. The entry is removed when the work lands.

**Open questions.**

- Read, write or both?
- Does it answer the questions of idea 3 the way S3 does, with google-cloud-storage in place of
  boto3? In particular: does the client only list and select the blobs, with Spark loading
  them from `gs://`, or does it read and write their content?
- Which environments must it support? Spark reads `gs://` only where the Hadoop GCS connector
  is installed and configured.
- Credentials: a service account key passed by the job, or the environment's default
  credentials?

## 5. Databricks Unity Catalog reader and writer

**Idea.** A connector for tables in Databricks Unity Catalog.

**Spec:** [specs/unity_catalog_connectors.md](../specs/unity_catalog_connectors.md), which
answers the questions below. The entry is removed when the work lands.

**Open questions.**

- Read, write or both?
- How does it read: through a watermark filter, as `PostgresReader` does, which Delta uses to
  skip files; through Delta's change data feed, which also returns updates and deletes; or
  either?
- Which writes does it support? `PostgresWriter` and `DiskWriter` offer append and replace,
  and both are atomic in Delta. A merge, like SCD2, is a write strategy, which the philosophy
  gives to callbacks. Should the writer create a table that does not exist?
- How does it differ from `DiskWriter(path, "delta")`? This connector names a table by its
  three-part name, `catalog.schema.table`, not by a path.
- Does it run only on a Databricks cluster, through Spark, or also outside Databricks, through
  a SQL warehouse and databricks-sql-connector?

## 6. Databricks Volumes connector

**Idea.** A connector for files in Unity Catalog volumes.

**Spec:** [specs/volume_connectors.md](../specs/volume_connectors.md), which answers the questions
below. The entry is removed when the work lands.

**Open questions.**

- What does it add to `DiskReader` and `DiskWriter`? They already read and write
  `/Volumes/<catalog>/<schema>/<volume>/...` on Databricks classic compute, through the
  volume's FUSE mount ([connectors](concepts/connectors.md)). The options are:
  - no new connector, only documentation of the disk connectors on volumes;
  - a connector for jobs that run outside Databricks, through the Files API of the Databricks
    SDK, in a new extra;
  - a thin connector that builds the path from the catalog, schema and volume names.
- If it is a new connector: read, write or both?

## 7. BigQuery reader and writer

**Idea.** A connector for Google BigQuery.

**Spec:** [specs/bigquery_connectors.md](../specs/bigquery_connectors.md), which answers the questions
below. The entry is removed when the work lands.

**Open questions.**

- Read, write or both?
- Which engine? Spark's BigQuery data source (spark-bigquery-connector, a prerequisite that
  Databricks Runtime includes) pushes filters into the BigQuery Storage Read API. The
  google-cloud-bigquery client runs in the Python process.
- How does the writer make append and replace atomic, as `PostgresWriter` does? The Spark data
  source writes either directly, through the Storage Write API, or indirectly, through files
  in a GCS bucket followed by a load job. Must the job provide that temporary bucket?
- Where does the watermark filter run: on a column, as `PostgresReader` does? Should it also
  prune the partitions of a partitioned table?
- Credentials: a service account key, or the environment's default credentials? Can the
  project that pays for the queries differ from the table's project?

## 8. MySQL reader and writer, with CDC

**Idea.** A connector for MySQL, with an implementation for change data capture (CDC).

**Open questions.**

- Does MySQL get a reader and a writer like the PostgreSQL ones, built on Spark JDBC with a
  client on the driver for the transaction, and a separate CDC reader on top of them?
- How is the writer atomic? `PostgresWriter` loads a staging table and applies it to the
  target in one transaction. In MySQL, DDL such as `CREATE TABLE`, `DROP TABLE` and `TRUNCATE`
  commits the open transaction implicitly, so that recipe does not carry over as it is. A
  replace would need a `DELETE` inside the transaction, or an atomic `RENAME TABLE` swap.
- Which CDC mechanism?
  - Log-based: read the binary log on the driver (python-mysql-replication) from a position up
    to the current one, and return each insert, update and delete with the row before and
    after it. It captures deletes, but needs `binlog_format=ROW`, `binlog_row_image=FULL` and
    replication privileges.
  - Query-based: a watermark column, as `PostgresReader` uses. Nothing new is needed, but
    deletes are missed.
  - External: Debezium captures the changes into Kafka or files, and ingestio reads them from
    there.
- If log-based, who keeps the binlog position? ADR 0013 says the job passes an ingestion's
  parameters, so the job would pass the position the way it passes a watermark and store the
  next one. ADR 0012 says a CDC source for PostgreSQL "would be a different connector that
  tracks its own offsets". Which rule applies? What happens when the server has already purged
  the position? How does the first run work: a full snapshot taken at a consistent binlog
  position?
- Who applies the changes? The philosophy lists CDC replication as a callback. Does the
  connector only return the change events, with a separate callback and spec applying them to
  the target?
- Which client package: PyMySQL or mysql-connector-python? MySQL Connector/J is a prerequisite
  for Spark, as the PostgreSQL JDBC driver is.

## 9. MongoDB reader and writer

**Idea.** A connector for MongoDB.

**Open questions.**

- Read, write or both?
- Which engine? The MongoDB Spark Connector, a prerequisite JAR, pushes filters and
  aggregation pipelines into MongoDB. pymongo runs in the Python process.
- Where does the watermark come from: a field such as `updatedAt`, or, for append-only
  collections, the time inside the `_id` ObjectId?
- Documents have no schema. Does the job pass one, or does Spark infer it by sampling? How are
  nested documents and fields with mixed types handled?
- Is a non-atomic write acceptable? The Spark connector writes in batches, not in one
  transaction, and multi-document transactions need a replica set and have limits. The
  alternative is a staging collection, for example with `$out`, which replaces the target
  collection only once the aggregation finishes.
- Does MongoDB need CDC too, through change streams, as MySQL does?
