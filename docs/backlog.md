# Backlog

Ideas the maintainer has had and wants implemented, recorded before anyone works on them. An
idea here is not a decision: nothing in this file is approved, and nothing here overrides the
[philosophy](philosophy.md), a spec or an ADR. Each idea lists the questions the maintainer
must answer before it can be worked on.

An idea that changes `src/` goes to the planner and becomes a spec, as described in
[workflow.md](workflow.md). Its entry then links to that spec, and the entry is removed when
the work lands.

## Shared by ideas 8 and 9

Ideas 8 and 9 are connectors that the maintainer listed on 2026-10-23, together with S3, GCS,
Unity Catalog, Volumes and BigQuery, which have landed. Each one changes `src/`, so each becomes
a spec, and its third-party package goes into an extra named after the system, like
`ingestio[postgres]`.

What the connectors that landed settled, which a new one follows unless its spec says otherwise:

- **Spark moves the data.** Every shipped connector except the JSON ones reads into a Spark
  DataFrame and writes from one, and a native client only does control work: psycopg runs the
  PostgreSQL transaction, boto3 lists the S3 objects.
- **The job names the system in its configuration.** A new connector is one more row of the
  closed table ([ADR 0015](decisions/0015-choose-connectors-from-the-job-configuration.md)): a
  system name, the connector's class name in snake case without `Reader` or `Writer`, whose
  section keys are the constructor arguments of the same name. Credentials, SDK clients and
  per-run values are not keys: they reach `ReadSource`, `AppendOnly` and `FullLoad` as
  arguments.
- **The write callbacks choose the strategy.** `AppendOnly` calls `write(data)` and `FullLoad`
  calls `write(data, replace=True)`
  ([ADR 0016](decisions/0016-make-each-write-strategy-one-callback.md)), so a writer offers
  `write(data, *, replace=False)` and decides nothing else. A merge, or applying CDC events, is
  a write strategy: a callback of its own, with a spec of its own.

These questions apply to both:

- **Engine.** Do MySQL and MongoDB follow the Spark pattern? The alternative is each system's
  native client in the Python process, which none of the shipped connectors uses.
- **Credentials.** The `connection` argument of the callbacks takes a `PostgresConnection`
  today, the only system that needs one. Does a MySQL or MongoDB connection widen that argument
  to one more type, or does each system get an argument of its own? Either changes the public
  API of the three callbacks.
- **Order.** Every connector changes the same files (`docs/concepts/connectors.md`,
  `docs/concepts/configuration.md`, `src/ingestio/connector/all.py`,
  `src/ingestio/callback/_configuration.py`, `pyproject.toml`), so only the planning of several
  connectors can run in parallel. Every later stage runs one connector at a time.

## 8. MySQL reader and writer, with CDC

**Idea.** A connector for MySQL, with an implementation for change data capture (CDC).

**Open questions.**

- Does MySQL get a reader and a writer like the PostgreSQL ones, built on Spark JDBC with a
  client on the driver for the transaction, and a separate CDC reader on top of them? Each would
  be one more system of the table, such as `mysql`, and a CDC reader another one.
- How is the writer atomic? `PostgresWriter` loads a staging table and applies it to the
  target in one transaction. In MySQL, DDL such as `CREATE TABLE`, `DROP TABLE` and `TRUNCATE`
  commits the open transaction implicitly, so that recipe does not carry over as it is.
  `FullLoad`'s `write(data, replace=True)` would need a `DELETE` inside the transaction, or an
  atomic `RENAME TABLE` swap.
- Which CDC mechanism?
  - Log-based: read the binary log on the driver (python-mysql-replication) from a position up
    to the current one, and return each insert, update and delete with the row before and
    after it. It captures deletes, but needs `binlog_format=ROW`, `binlog_row_image=FULL` and
    replication privileges.
  - Query-based: a watermark column, as `PostgresReader` uses. Nothing new is needed, but
    deletes are missed.
  - External: Debezium captures the changes into Kafka or files, and ingestio reads them from
    there.
- If log-based, who keeps the binlog position? Under ADR 0015 a per-run value reaches
  `ReadSource` as an argument, as `watermark_value` does, so the job would pass the position and
  store the next one, and `ReadSource` would get one more argument. ADR 0012 says a CDC source
  for PostgreSQL "would be a different connector that tracks its own offsets". Which rule
  applies? What happens when the server has already purged the position? How does the first run
  work: a full snapshot taken at a consistent binlog position?
- Who applies the changes? The philosophy lists CDC replication as a callback, and ADR 0016
  makes each write strategy one callback. Does the connector only return the change events, with
  a separate callback and spec applying them to the target? No shipped writer updates or deletes
  rows today, so that callback either needs a capability that the shipped writers lack, or
  rewrites the whole target with `write(data, replace=True)`, as `SCD2` does
  ([spec](../specs/scd2_callback.md), decision 1).
- Which client package: PyMySQL or mysql-connector-python? MySQL Connector/J is a prerequisite
  for Spark, as the PostgreSQL JDBC driver is.

## 9. MongoDB reader and writer

**Idea.** A connector for MongoDB.

**Spec.** The reader is [specs/mongodb_reader.md](../specs/mongodb_reader.md): `MongoDBReader`
on the MongoDB Spark Connector, with `schema_hints` and a `watermark_column`. It settles the
engine, the watermark and the schema. The writer and change streams stay here as open ideas.

**Open questions**, for the writer and change streams:

- Is a non-atomic write acceptable for `AppendOnly` and `FullLoad`? The Spark connector writes in
  batches, not in one transaction, and multi-document transactions need a replica set and have
  limits. For `FullLoad`, the alternative is a staging collection, for example with `$out`,
  which replaces the target collection only once the aggregation finishes.
- Does MongoDB need CDC too, through change streams, as MySQL does?
