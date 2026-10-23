# 0013. Give a connector an ingestion's parameters as constructor arguments

- **Status:** accepted
- **Recorded:** 2026-10-23
- **Evidence:** [specs/postgres_connectors.md](../../specs/postgres_connectors.md), decisions 2, 3,
  27 and 31, taken by the maintainer; `src/ingestio/connector/postgres.py`; rationale given by
  the maintainer on 2026-10-23

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
- A reader that filters inside the source cannot offer a free-form query, because a custom
  query cannot safely be bounded by a watermark.
- Nothing here closes the door to streaming.
