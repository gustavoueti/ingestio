# 0014. Create a missing target only in the form the system derives from the data

- **Status:** accepted
- **Recorded:** 2026-10-23
- **Evidence:** [specs/unity_catalog_connectors.md](../../specs/unity_catalog_connectors.md),
  decision 4, taken by the maintainer, and its question 6, taken with its recommendation by the
  main session under the maintainer's delegation of 2026-10-23, to be reviewed by the
  maintainer; [specs/postgres_connectors.md](../../specs/postgres_connectors.md), decision 29;
  [specs/bigquery_connectors.md](../../specs/bigquery_connectors.md), decision 19;
  `src/ingestio/connector/unity_catalog.py`

## Context

The [philosophy](../philosophy.md#connector) says a connector "does not decide write strategy,
format or architecture". Two writers had to settle whether creating a missing target is
architecture:

- `PostgresWriter` runs no DDL on the target, and a missing target fails. Decision 29 of its
  spec gives the reason: "DDL on the target is architecture". Creating a PostgreSQL table from
  a DataFrame means choosing `text` or `varchar`, the precision of `numeric`, `timestamptz` or
  `timestamp`, keys, constraints and defaults.
- The maintainer chose that `UnityCatalogWriter` creates its target table when it does not
  exist, so that a plain table needs no separate DDL step. A Delta table created from a
  DataFrame needs none of those choices: its columns and types are the DataFrame's.

The line between the two bears on every later writer. It is a permission, not a requirement: a
writer that could create its target under this rule need not. `BigQueryWriter` is the example. It
creates nothing, and a missing target fails (decision 19 of
[its spec](../../specs/bigquery_connectors.md)). Its staging table is the writer's own mechanism,
created and dropped within one call to make the write atomic, and is not the target.

## Decision

- A writer may create its missing target only in the form the system derives entirely from the
  data, with no layout, location, property or type mapping chosen by the connector. Anything
  more, such as partitioning, clustering, a location, comments, table properties, constraints,
  keys or a type mapping, is created by the job, with its own DDL, before the first write.
- A connector never creates what holds the target (a catalog, a schema, a dataset).
- Decision 29 of the PostgreSQL spec still stands for `PostgresWriter`. For PostgreSQL the system
  derives nothing from a DataFrame: every column would need a type mapping chosen by the
  connector.
- The format of a write is not a choice of the connector when it is the system's native table
  format. `UnityCatalogWriter` always writes Delta, which is the storage protocol of the system
  it integrates, as PostgreSQL's own storage is for `PostgresWriter`. The connector converts
  nothing: the DataFrame's columns and types become the table's as they are. The job gives no
  format, because the system has one. A connector for a system with several formats does not
  fall under this clause: its job gives the format, as `DiskWriter` does.
- The connector never converts an existing table to another format on purpose. A target that is
  not in the system's format is not supported.

## Rationale

The spec gives these reasons, in [Fit with the philosophy](../../specs/unity_catalog_connectors.md#fit-with-the-philosophy):

- What the connector creates is the minimal form, and nothing in it is the connector's choice:
  a managed Delta table whose columns are the DataFrame's, with no partitioning, clustering,
  location, comment or table property set by the connector. Everything else comes from the
  catalog's defaults.
- Where the table is, and that it exists, are the job's decisions: the job names the table and
  calls `write`.
- The philosophy's sentence does not need to change, and decision 29 of the PostgreSQL spec still
  holds. Its reason, "DDL on the target is architecture", is broader than the line drawn here,
  so the line is recorded in an ADR rather than in two specs, because it binds every later
  connector.
- Delta is what makes the write atomic, which is the reason the maintainer gave for creating the
  table in the same write (decision 4 of the spec).

The maintainer gave the decision to create the target (decision 4 of the spec) but not, in the
spec, a reason for it beyond the one above.

> TODO(maintainer): why the writer creates its target instead of failing as `PostgresWriter`
> does, and review this ADR, approved by the main session under your delegation.

## Consequences

- `UnityCatalogWriter` creates a missing table as a managed Delta table with the DataFrame's
  columns, and nothing else. A job that needs partitioning, liquid clustering, constraints, a
  location or properties creates the table first. It never creates a catalog or a schema.
- `PostgresWriter` keeps failing on a missing target, and so does `BigQueryWriter`, which runs no
  DDL on the target: the rule allows creating, it does not require it. A staging table that a
  writer creates and drops within one call is not the target and is outside this rule.
- A later writer follows the line: it may create a missing target only in the form its system
  derives entirely from the data. If it cannot do so without choosing a layout, a location, a
  property or a type mapping, a missing target fails, and the job creates it.
- A writer's documentation says what its created target looks like and what it leaves to the
  system's defaults.
- Nothing here closes the door to streaming.
