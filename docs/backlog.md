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

**Specs.** The work is split in three, one after the other:

1. [specs/mysql_connectors.md](../specs/mysql_connectors.md): `MySQLConnection`, the reader and
   the writer on Spark JDBC, and the extra. Validated on 2026-10-24.
2. [specs/mysql_cdc.md](../specs/mysql_cdc.md): `MySQLCDCReader`, the `mysql_cdc` system and
   the `mysql-cdc` extra. Validated on 2026-10-24.
3. The callback that applies the change events to a target, with a spec of its own, after the
   CDC reader.

**Postponed: the integration tests in CI.** The 17 opt-in cases of the first spec (its decision
43) need a MySQL 8.4 service in `.github/workflows/ci.yml`, the variables `INGESTIO_MYSQL_URL` and
`INGESTIO_MYSQL_JDBC_PACKAGE`, and a user with `SYSTEM_VARIABLES_ADMIN` for the cases that set
the global `sql_mode` or `time_zone`. Until they run, the facts K1 to K16 of that spec stay
unconfirmed. The maintainer left them for later on 2026-10-24.

**Decided by the maintainer on 2026-10-24.**

- **Connectors only talk to MySQL.** The CDC reader returns the change events and nothing
  else, and applying them is a callback: the philosophy lists CDC replication as a callback, and
  ADR 0016 makes each write strategy one callback.
- **The writer is atomic through a staging table and a `DELETE`.** Spark loads a staging table,
  then one transaction on the driver runs `DELETE FROM <target>` when `replace=True` and
  `INSERT … SELECT` from the staging table. In MySQL `CREATE`, `DROP`, `TRUNCATE` and `RENAME
  TABLE` commit the open transaction implicitly, so the `TRUNCATE` of `PostgresWriter` cannot run
  inside it. A `DELETE` keeps the target's grants, triggers and incoming foreign keys, which a
  `RENAME TABLE` swap loses. Its cost is a slow `DELETE` and a large undo log on big tables.
- **CDC reads the binary log on the driver, with `python-mysql-replication`.** It captures
  deletes, which a watermark misses. It needs `binlog_format=ROW`, `binlog_row_image=FULL`,
  `binlog_row_metadata=FULL` and the replication privileges, and the events pass through the
  driver's memory before they become a DataFrame.
- **Each event carries its binlog position.** The job passes the start position as an argument
  of `ReadSource`, as it passes `watermark_value`. The reader reads up to the end of the binary
  log captured when it reads, since building a connector only validates. The job takes the next
  position from the DataFrame, as it takes the next watermark. A table with no changes does not
  move the position forward, and the server can purge it.

The CDC reader's design, the facts read in the source of `python-mysql-replication` and the
answers to its open questions (file and offset, a snapshot on the first run, an error on a
purged position, flat columns plus metadata) are in [its spec](../specs/mysql_cdc.md).

**Open questions**, for the callback that applies the events:

- No shipped writer updates or deletes rows. Does the callback rewrite the whole target with
  `write(data, replace=True)`, as `SCD2` does ([spec](../specs/scd2_callback.md), decision 1),
  or does a writer gain a capability for it?

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
