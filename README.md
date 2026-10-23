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

No runtime dependencies. An integration that needs one is an optional extra (`postgres`,
`s3`, `gcs`, `databricks`, `bigquery`), such as `pip install 'ingestio[postgres]'`.

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

The library ships the engine, the connectors (a JSON API reader, a JSON Lines writer, and
readers and writers for PostgreSQL, files on disk, Amazon S3, Google Cloud Storage, Databricks
volumes, Unity Catalog tables and BigQuery) and two generic callbacks, `ReadSource` and
`WriteTarget`. A job names its source and its target in a JSON file, and those two callbacks
build the matching connector. The stages that are specific to your ingestion are yours to
write — that is the point.

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

from ingestio import Process, ReadSource, Step, WriteTarget

CONFIG = Path(__file__).with_name("config.json")

process = Process([
    Step("extract", [ReadSource(CONFIG)]),
    Step("load", [WriteTarget(CONFIG)]),
]).run()

print(len(process.state["records"]))
```

`WriteTarget` appends to the file; `WriteTarget(CONFIG, replace=True)` replaces it. A
Databricks volume is an ordinary path, so the same job runs on a laptop and on a cluster.

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
    WriteTarget(CONFIG),
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
[a3f7c9] │ │ ✔ WriteTarget.run 15ms
[a3f7c9] │ └ load done in 19ms
[a3f7c9] └ process finished in 432ms
```

A failing callback gets a `✖` line at `ERROR` with the type and message of the exception, and
the step and the process close as failed. The exception itself still propagates.

```text
[a3f7c9] │ │ ✖ WriteTarget.run failed after 2ms: PermissionError: [Errno 13] Permission denied
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
`source` section and `WriteTarget` its `target` section; each builds the connector that the
section's `system` names, with the section's other keys as the connector's arguments.
Credentials never go in it.

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
    Callback,
    CancelProcessException,
    Process,
    ReadSource,
    Step,
    WriteTarget,
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
        WriteTarget(CONFIG, connection=warehouse),
    ]),
]).run()
```

- `ReadSource` and `WriteTarget` ship with the library; `last_watermark` and `SkipEmptyBatch`
  belong to the job. `WriteTarget` appends; `WriteTarget(CONFIG, connection=warehouse,
  replace=True)` replaces the target.
- The connection, with its credentials, and the run's watermark are arguments of the
  callbacks, not keys of the file: they are not JSON, and the library reads no secret.
- `primary_key` is a parameter of the job: both callbacks ignore every top-level member other
  than their own section, so it is there for the job's own callbacks, such as a merge. Inside
  a section, a key the system does not take is an error, so a typo such as `watermark_colum`
  stops the job before anything runs.
- Moving the target is an edit of its section, such as
  `{"system": "unity_catalog", "table_name": "main.bronze.orders"}`. An argument the new system
  does not take is an error too, so that move also drops `connection=warehouse`.
- Every system, its keys and its arguments are in
  [docs/concepts/configuration.md](docs/concepts/configuration.md).

These two files are the whole job, and they run anywhere: in a container, as a Databricks job,
on a laptop. The connectors built on Spark, every reader and writer except the JSON ones, expect
a Spark session to be running already, and never start one. Databricks provides it; elsewhere,
create it before the process runs, for example with `SparkSession.builder.getOrCreate()`. The
rest of the library is plain Python. Some connectors also need their system's extra, such as `postgres`
for the PostgreSQL writer or `bigquery` for the BigQuery writer; the requirements of each
connector are in [docs/concepts/connectors.md](docs/concepts/connectors.md).

## Roadmap

More connectors (MySQL with change data capture, MongoDB), and the append-only and SCD2
write callbacks.

## License

Apache-2.0
