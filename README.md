# ingestio

Small, composable library for building data ingestion pipelines.

`ingestio` describes **any data movement process** as a sequence of steps. It does not
assume your data is a table, that your architecture has bronze and silver layers, or that
your processing is batch. Those are things you build with it, not things it imposes.

> **Status: early alpha.** The core is functional and tested, the API is not stable yet.

## Install

```bash
pip install ingestio
```

No runtime dependencies. A connector that needs a third-party package takes it from an optional
extra, such as `pip install 'ingestio[postgres]'`; the [connectors](#connectors) table says which.

## The idea

Three pieces, and the ordering is always yours:

| | |
|---|---|
| **Step** | a named point of the process; holds the callbacks that run there |
| **Callback** | the code of one stage |
| **Connector** | a reader or a writer of an external system, named in the job's configuration |

Steps run in list order, and callbacks inside a step run in list order. There is no
priority attribute and no hidden reordering: the sequence you write is the sequence that
runs.

The library ships the engine, the [connectors](#connectors) and four generic callbacks:
`ReadSource` reads the source, `AppendOnly` appends to the target, `FullLoad` replaces what it
holds and `SCD2` keeps the history of a dimension. A job names its source and its target in a
JSON file, and those callbacks build the matching connector. The stages that are specific to
your ingestion are yours to write — that is the point.

## Connectors

Every connector ships with the library. A job picks one by naming its `system` in the `source` or
`target` section of its configuration, with nothing to register. The list is closed: a new system
is a change to the library, and this table grows with it.

| System | `system` | Reader | Writer | Spark | Extra |
|---|---|---|---|---|---|
| [JSON API](docs/concepts/connectors.md#jsonapireader) | `json_api` | `JsonApiReader` | — | no | — |
| [JSON Lines file](docs/concepts/connectors.md#jsonlineswriter) | `json_lines` | — | `JsonLinesWriter` | no | — |
| [Files on disk](docs/concepts/connectors.md#disk-diskreader-diskwriter) | `disk` | `DiskReader` | `DiskWriter` | yes | — |
| [PostgreSQL](docs/concepts/connectors.md#postgresql-postgresconnection-postgresreader-postgreswriter) | `postgres` | `PostgresReader` | `PostgresWriter` | yes | `postgres`, for the writer |
| [Amazon S3](docs/concepts/connectors.md#s3-s3reader-s3writer) | `s3` | `S3Reader` | `S3Writer` | yes | `s3`, for a reader without `client` |
| [Google Cloud Storage](docs/concepts/connectors.md#gcs-gcsreader-gcswriter) | `gcs` | `GcsReader` | `GcsWriter` | yes | `gcs`, for a reader without `client` |
| [Databricks volumes](docs/concepts/connectors.md#volumes-volumereader-volumewriter) | `volume` | `VolumeReader` | `VolumeWriter` | yes | `databricks`, for a reader without `client` |
| [Unity Catalog tables](docs/concepts/connectors.md#unity-catalog-unitycatalogreader-unitycatalogwriter) | `unity_catalog` | `UnityCatalogReader` | `UnityCatalogWriter` | yes | — |
| [BigQuery](docs/concepts/connectors.md#bigquery-bigqueryreader-bigquerywriter) | `bigquery` | `BigQueryReader` | `BigQueryWriter` | yes | `bigquery`, for the writer, and for a reader with `watermark_column` |

- **Spark:** the connector reads into a Spark DataFrame and writes from one. It expects a Spark
  session to be running already, and never starts one: Databricks provides it; elsewhere, create
  it before the process runs, for example with `SparkSession.builder.getOrCreate()`. Some systems
  also need a package in that session, such as the PostgreSQL JDBC driver or Delta Lake on
  open-source Spark: the system's link gives its environments and prerequisites.
- **Extra:** installed with `pip install 'ingestio[<extra>]'`, and needed only in the case the
  column names. pyspark is never an extra: the runtime provides it.
- The keys each system takes are in
  [docs/concepts/configuration.md](docs/concepts/configuration.md#systems-and-keys). `SCD2` writes
  only to `unity_catalog`, `postgres` and `bigquery`.
- The classes are importable from `ingestio.connector.all`, as `PostgresConnection` is in
  [Structuring a job](#structuring-a-job).
- The connectors planned next are in the [roadmap](#roadmap).

## Design philosophy

Two concepts, and nothing else: **connectors** talk to external systems through the `Reader`
and `Writer` protocols and decide nothing else, and **callbacks** are the stages that act on
the data. The library composes rather than imposes: the smallest abstraction that works, no
open registries (the one table maps the configuration's system names to the connectors the
library ships), and no dependencies in the core. The full text is in
[docs/philosophy.md](docs/philosophy.md).

## Example: a public API into a volume

`config.json` names the source and the target:

```json
{
  "source": {"system": "json_api", "url": "https://api.example.com/v1/orders"},
  "target": {"system": "json_lines", "path": "/Volumes/main/bronze/orders.jsonl"}
}
```

`main.py` only places the callbacks:

```python
from pathlib import Path

from ingestio import AppendOnly, Process, ReadSource, Step

CONFIG = Path(__file__).with_name("config.json")

process = Process([
    Step("extract", [ReadSource(CONFIG)]),
    Step("load", [AppendOnly(CONFIG)]),
]).run()

print(len(process.state["records"]))
```

`AppendOnly` adds lines to the file; `FullLoad(CONFIG)` in its place replaces the file's content.
A Databricks volume is an ordinary path, so the same job runs on a laptop and on a cluster.

## Writing a callback

A callback is a class with three optional moments. A step runs every `before`, then every
`run`, then every `after`, so a callback gets a real boundary around the whole step while
keeping list order within each pass.

Callbacks exchange data through `process.state` instead of passing a payload down a chain,
so a callback that only observes does not have to hand the data back.

```python
from datetime import UTC, datetime

from ingestio import Callback


class AddLineageColumns(Callback):
    """Stamp every record with where it came from and which run produced it."""

    def __init__(self, source: str, key: str = "records") -> None:
        self.source = source
        self.key = key

    def run(self) -> None:
        ingested_at = datetime.now(UTC).isoformat()
        self.process.state[self.key] = [
            {
                **record,
                "_ingested_at": ingested_at,
                "_source": self.source,
                "_batch_id": self.process.run_id,
            }
            for record in self.process.state[self.key]
        ]
```

Drop it into the step where it belongs:

```python
Step("load", [
    AddLineageColumns(source="orders-api"),
    AppendOnly(CONFIG),
])
```

The column names are yours. `ingestio` has no opinion about them.

## Controlling failure

The library gives you the vocabulary; the policy is yours.

```python
from ingestio import Callback, CancelProcessException


class SkipWhenEmpty(Callback):
    def run(self) -> None:
        if not self.process.state["records"]:
            raise CancelProcessException("nothing to ingest")
```

- `CancelStepException` — skip the rest of this step, carry on with the next one.
- `CancelProcessException` — stop the process cleanly.
- anything else — propagates and kills the run, which is a legitimate choice you make by
  not catching it.

`after` runs in every case, so cleanup stays reliable.

## Logging

Standard `logging`, never `print`, and every line carries the run id — so concurrent
ingestions on the same cluster stay readable. A run logs as a tree: the process, each step,
and every callback moment that ran, with its outcome and how long it took.

```text
[a3f7c9] ┌ process started · 2 steps
[a3f7c9] │ ┌ extract (1/2) · 1 callback
[a3f7c9] │ │ ✔ ReadSource.run 412ms
[a3f7c9] │ └ extract done in 413ms
[a3f7c9] │ ┌ load (2/2) · 2 callbacks
[a3f7c9] │ │ ✔ AddLineageColumns.run 3ms
[a3f7c9] │ │ ✔ AppendOnly.run 15ms
[a3f7c9] │ └ load done in 19ms
[a3f7c9] └ process finished in 432ms
```

A failing callback gets a `✖` line at `ERROR` with the type and message of the exception, and
the step and the process close as failed. The exception itself still propagates.

```text
[a3f7c9] │ │ ✖ AppendOnly.run failed after 2ms: PermissionError: [Errno 13] Permission denied
[a3f7c9] │ └ load failed in 3ms
[a3f7c9] └ process failed in 415ms
```

Nothing shows until logging is configured. To see the tree on the console, opt in with one
call:

```python
from ingestio import log_to_console

log_to_console()  # stderr, colored on a terminal; NO_COLOR turns color off
```

It touches only the `ingestio` logger. A notebook is not a terminal: pass `color=True` when it
renders ANSI colors. The full format is in
[docs/concepts/lifecycle.md](docs/concepts/lifecycle.md#logging).

## Structuring a job

ingestio does not impose a layout on a job. A suggested minimum is two files:

```text
orders_job/
├── config.json   the job's source, its target and its parameters
└── main.py       places the callbacks and runs the process
```

`config.json` names the source and the target of the ingestion. `ReadSource` reads its
`source` section, and `AppendOnly`, `FullLoad` and `SCD2` its `target` section; each builds the
connector that the section's `system` names, with the section's other keys as the connector's
arguments. Credentials never go in it.

```json
{
  "source": {"system": "postgres", "table_name": "public.orders", "watermark_column": "updated_at"},
  "target": {"system": "postgres", "table_name": "raw.orders"},
  "primary_key": "id"
}
```

`main.py` imports ingestio, places the callbacks the library ships, defines the job's own and
runs the process. This one copies the orders changed since the last run from the shop's
database into the warehouse:

```python
import os
from datetime import datetime
from pathlib import Path

from ingestio import (
    AppendOnly,
    Callback,
    CancelProcessException,
    Process,
    ReadSource,
    Step,
    log_to_console,
)
from ingestio.connector.all import PostgresConnection

CONFIG = Path(__file__).with_name("config.json")

# Credentials come from the environment; on Databricks, from dbutils.secrets.get.
shop = PostgresConnection(
    "pg.internal", "shop", os.environ["SHOP_USER"], os.environ["SHOP_PASSWORD"]
)
warehouse = PostgresConnection(
    "dw.internal", "warehouse", os.environ["DW_USER"], os.environ["DW_PASSWORD"]
)


def last_watermark() -> datetime | None:
    """The highest `updated_at` already loaded, timezone-aware. None reads every row.

    Yours to write: query the warehouse, read a control table, ...
    """
    return None


class SkipEmptyBatch(Callback):
    """The job's own stage: stop cleanly when nothing changed since the last run."""

    def run(self) -> None:
        if self.process.state["records"].isEmpty():
            raise CancelProcessException("no new orders")


log_to_console()
Process([
    Step("extract", [
        ReadSource(CONFIG, connection=shop, watermark_value=last_watermark()),
    ]),
    Step("load", [
        SkipEmptyBatch(),
        AppendOnly(CONFIG, connection=warehouse),
    ]),
]).run()
```

- `ReadSource` and `AppendOnly` ship with the library; `last_watermark` and `SkipEmptyBatch`
  belong to the job. `AppendOnly` appends to the target; `FullLoad(CONFIG, connection=warehouse)`
  in its place replaces what the target holds.
- The connection, with its credentials, and the run's watermark are arguments of the
  callbacks, not keys of the file: they are not JSON, and the library reads no secret.
- `primary_key` is a parameter of the job: the library's callbacks ignore every top-level member
  other than their own sections, so it is there for the job's own callbacks, such as a merge. The
  library reserves only `source`, `target` and `scd2`, the section of `SCD2`'s parameters. Inside
  a section, a key the system does not take is an error, so a typo such as `watermark_colum`
  stops the job before anything runs.
- Moving the target is an edit of its section, such as
  `{"system": "unity_catalog", "table_name": "main.bronze.orders"}`. An argument the new system
  does not take is an error too, so that move also drops `connection=warehouse`.
- Every system, its keys and its arguments are in
  [docs/concepts/configuration.md](docs/concepts/configuration.md).

These two files are the whole job, and they run anywhere: in a container, as a Databricks job,
on a laptop, as long as the environment has what the job's connectors need. The
[connectors](#connectors) table says which ones need a running Spark session and which need an
extra; the rest of the library is plain Python.

## Keeping the history of a dimension

`SCD2` writes a dimension, such as customers or products, as a slowly changing dimension of
type 2: when a tracked column of an entity changes, it closes the entity's current version and
opens a new one, so every past state stays queryable. Its parameters go in an `scd2` section of
the same file:

```json
{
  "source": {"system": "postgres", "table_name": "public.customers", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.gold.dim_customer"},
  "scd2": {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
    "tracked_columns": ["name", "city", "segment"]
  }
}
```

In `main.py`, `SCD2(CONFIG)` takes the place of `AppendOnly(CONFIG)`, with the same arguments.

- Each version carries `valid_from` and `valid_to`, taken from the effective-time column; the
  current version has a null `valid_to`. Closed versions never change.
- Several records of one entity in a batch each become a version, in the order of their
  effective time. A record older than the entity's current version fails the run before anything
  is written.
- An entity absent from the batch keeps its current version: deletes in the source are not
  detected.
- Every run rewrites the whole dimension, atomically, on `unity_catalog`, `postgres` and
  `bigquery`. The target must exist before the first run, with the batch's columns plus the two
  version columns.

The whole behaviour, the checks and the errors are in
[docs/concepts/callbacks.md](docs/concepts/callbacks.md#scd2), and the `scd2` section in
[docs/concepts/configuration.md](docs/concepts/configuration.md#the-scd2-section).

## Roadmap

More connectors (MySQL with change data capture, MongoDB).

## License

Apache-2.0
