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
  rows today, so that callback needs a capability that the shipped writers lack, as SCD2 does
  (idea 12).
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
  nested documents and fields with mixed types handled? A section's `schema` key is a DDL string
  in the systems that take one.
- Is a non-atomic write acceptable for `AppendOnly` and `FullLoad`? The Spark connector writes in
  batches, not in one transaction, and multi-document transactions need a replica set and have
  limits. For `FullLoad`, the alternative is a staging collection, for example with `$out`,
  which replaces the target collection only once the aggregation finishes.
- Does MongoDB need CDC too, through change streams, as MySQL does?

## 12. SCD2 write callback

**Idea.** Implement SCD2, one of the two default callbacks the
[philosophy](philosophy.md#callback) names: when a tracked attribute of an entity changes, its
current version is closed and a new version is added, so that every past state stays queryable.
Requested on 2026-10-23 and parked here while the write callbacks `AppendOnly` and `FullLoad`
were built. They have landed
([ADR 0016](decisions/0016-make-each-write-strategy-one-callback.md)).

**Spec:** [specs/scd2_callback.md](../specs/scd2_callback.md), a `draft` written on 2026-10-22
while the workflow was set up and never reviewed by the planner. It predates
[ADR 0015](decisions/0015-choose-connectors-from-the-job-configuration.md), ADR 0016 and the
Spark connectors, so it goes back to the planner first, with the changes listed under "Effects
on the draft specs" in [specs/config_connectors.md](../specs/config_connectors.md). The entry is
removed when the work lands.

**What changed since the draft.**

- Each write strategy is one callback (ADR 0016). `AppendOnly` and `FullLoad` take the path of
  the job's configuration and build their writer from the `target` section, through the closed
  table, with the callback arguments `connection` and `client`. SCD2 is one more, built the same
  way. The configuration has no mode.
- The callback accepts no connector, so the proposal `SCD2(source, target, *, keys, ...)`
  changes, and so does the spec's question 1: a capability that user connectors implement is
  gone.
- Every shipped writer offers `write(data, *, replace=False)`, and every one except `json_lines`
  takes a Spark DataFrame. None updates or merges rows.
- SCD2 reads the current versions from the target, so it also needs a reader of the target's
  system, built from a section meant for a writer.

**Open questions.** They update the spec's own questions, which the numbers refer to.

- How does it apply the changes (question 1)?
  - An atomic rewrite: read the whole target with the reader of its system, compute the new
    table in Spark (the closed versions plus the new ones) and write it with `replace=True`, as
    `FullLoad` does. No connector needs a new capability, but every run rewrites the whole
    dimension.
  - A Delta `MERGE INTO`, in one commit. It scales better, but only Delta targets support it,
    it needs a new merge capability on the Delta connectors, and it leaves PostgreSQL and
    BigQuery out.
  - Both, with the merge where the target supports it.
- Which target systems does it support? A rewrite through `replace=True` is atomic for
  `unity_catalog`, `bigquery`, `postgres`, and the file systems in `delta` format. For parquet,
  CSV or JSON files in `disk`, `s3`, `gcs` or `volume` it is not, so a failure halfway through
  can lose the dimension's history. Spark also refuses to overwrite a path it is reading from.
  `json_lines` has no reader. Is it the atomic systems only?
- How does it build the target's reader? Does the reader take the keys of the `target` section
  (`table_name`, `path`, `format`) and the callback arguments `connection` and `client`?
- What happens on the first run, when the target does not exist? It could treat the target as
  empty and let the writer create it where
  [ADR 0014](decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md) allows
  (`unity_catalog`, Delta files), and fail where the writer creates nothing (`postgres`,
  `bigquery`).
- Where do its parameters go: `keys`, the tracked columns, the names of the version columns?
  Constructor arguments, or members of the file outside `source` and `target` (decision 11 of
  the configuration spec)?
- What does an entity that is current in the target but absent from the batch mean
  (question 5)? Incremental: it stays as it is, which matches the readers that filter on a
  watermark, but deletes in the source are missed. Snapshot: its current version is closed,
  which needs the whole source on every run. Or an option, incremental by default?
- Where does the effective time come from (questions 3 and 6)? `process.started_at` is the
  same for the whole batch and rules out late-arriving records. A column of the record, such
  as the source's `updated_at`, gives the business time but brings late-arriving records into
  scope. An optional column could fall back on `process.started_at`.
- What are the version columns (question 4)? `valid_from`, `valid_to` with `NULL` as the open
  end, and `is_current`; a far-future date as the open end; or no `is_current`. Are the names
  arguments?
- Which columns are tracked (question 2)? Every column except the keys and the version
  columns, or an optional list. With a list, does a change in an untracked column get ignored,
  or overwrite the current version in place, as SCD1?
- Is a duplicate business key in one batch an error before anything is written (question 7)?
  "The last record wins" needs an ordering column, since a DataFrame has no order.
- Are changes detected by a null-safe comparison of the tracked values, or by a hash stored in
  a column (question 8)?
- A Spark DataFrame is lazy, so the comparison with the current versions is a join and not a
  materialised batch (question 9). Does a rewrite of the whole target close the door to
  streaming ([ADR 0012](decisions/0012-streaming-deferred.md))?
- What are the name and the module (question 10)? `SCD2`, or a name in the form of the shipped
  strategies `AppendOnly` and `FullLoad`; in `callback/io.py` next to them, or a module of its
  own?
