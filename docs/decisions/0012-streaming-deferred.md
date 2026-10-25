# 0012. Defer streaming, and record what affects it

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** decided by the maintainer and stated in the
  [philosophy](../philosophy.md#principles)

## Context

The README promises that ingestio "does not assume ... that your processing is batch". Version
0.0.1 implements batch runs only.

## Decision

- Streaming is a future direction and is not implemented now.
- Any decision that closes the door to streaming is recorded in an ADR.

## Rationale

> TODO(maintainer): why streaming is deferred.

## Consequences

What in version 0.0.1 leans towards batch:

- `Reader.read()` returns one payload per call.
- A step finishes all its passes before the next step starts.
- `JsonApiReader` loads the whole response body into memory.
- The PostgreSQL connectors ([spec](../../specs/postgres_connectors.md)) lean towards batch:
  `PostgresReader.read()` returns one slice, with a watermark fixed when the reader is built,
  and `PostgresWriter.write()` needs the whole DataFrame before it commits. A streaming or CDC
  source for PostgreSQL, such as logical replication, would be a different connector that
  tracks its own offsets.
- The disk connectors ([spec](../../specs/disk_connectors.md)) lean towards batch:
  `DiskReader.read()` makes one selection of files, with a watermark fixed when the reader is
  built, and `DiskWriter.write()` needs the whole DataFrame. A streaming file source, such as
  Structured Streaming's file source or Auto Loader, tracks the files it processed in a
  checkpoint and would be a different connector.
- The S3 connectors ([spec](../../specs/s3_connectors.md)) lean towards batch: `S3Reader.read()`
  makes one selection of objects, with a watermark fixed when the reader is built, and
  `S3Writer.write()` needs the whole DataFrame. A streaming source on S3, such as Auto Loader,
  S3 event notifications or Structured Streaming's file source, tracks the objects it processed
  in a checkpoint and would be a different connector.
- The GCS connectors ([spec](../../specs/gcs_connectors.md)) lean towards batch:
  `GcsReader.read()` makes one selection of blobs, with a watermark fixed when the reader is
  built, and `GcsWriter.write()` needs the whole DataFrame. A streaming source on GCS, such as
  Auto Loader, Pub/Sub notifications for Cloud Storage or Structured Streaming's file source,
  tracks the blobs it processed in a checkpoint or a subscription and would be a different
  connector.
- The volume connectors ([spec](../../specs/volume_connectors.md)) lean towards batch:
  `VolumeReader.read()` makes one selection of files, with a watermark fixed when the reader is
  built, and `VolumeWriter.write()` needs the whole DataFrame. A streaming source on a volume,
  such as Auto Loader or Structured Streaming's file source, tracks the files it processed in a
  checkpoint and would be a different connector.
- The Unity Catalog connectors ([spec](../../specs/unity_catalog_connectors.md)) lean towards
  batch: `UnityCatalogReader.read()` returns one slice, with a watermark fixed when the reader is
  built, and `UnityCatalogWriter.write()` needs the whole DataFrame. A streaming source for
  Delta tables, such as `readStream` on a table or on its change data feed, tracks its progress
  in a checkpoint and would be a different connector.
- The BigQuery connectors ([spec](../../specs/bigquery_connectors.md)) lean towards batch:
  `BigQueryReader.read()` returns one slice, with a watermark fixed when the reader is built, and
  `BigQueryWriter.write()` needs the whole DataFrame before its statement commits. A streaming
  source for BigQuery, such as its change history or a Pub/Sub subscription, or a Structured
  Streaming write, would be a different connector.
- The MongoDB reader ([spec](../../specs/mongodb_reader.md)) leans towards batch:
  `MongoDBReader.read()` returns one slice, with a watermark fixed when the reader is built. A
  streaming or CDC source for MongoDB, such as change streams through the connector's streaming
  read, tracks a resume token and would be a different connector.
- The MySQL connectors ([spec](../../specs/mysql_connectors.md)) lean towards batch:
  `MySQLReader.read()` returns one slice, with a watermark fixed when the reader is built, and
  `MySQLWriter.write()` needs the whole DataFrame before its transaction commits. A CDC source for
  MySQL, reading the binary log from a position, is a different connector, the CDC reader of the
  later spec `mysql_cdc`.
- SCD2 ([spec](../../specs/scd2_callback.md)) leans towards batch: each run reads the whole batch
  and the whole target, checks the batch, and rewrites the dimension. A streaming SCD2, applying
  changes per micro-batch, would be another write strategy, a callback of its own (ADR 0016).

What keeps the door open:

- The payload type is unconstrained ([ADR 0003](0003-connectors-as-protocols.md)), so a reader
  may return an iterator.
- `JsonLinesWriter` accepts any iterable of records, so a generator stored in the state is
  consumed lazily when written.
