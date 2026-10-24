# Callbacks

A callback is a stage of the pipeline: the code that performs one action, such as formatting
records, applying a medallion layer or writing with a given strategy. See the
[philosophy](../philosophy.md#callback).

## The `Callback` class

A callback subclasses `ingestio.Callback` and overrides any of three moments. All three do
nothing by default.

| Method | Runs |
|---|---|
| `before()` | before any callback of the step does its work |
| `run()` | the work of the stage |
| `after()` | after every callback of the step, including when the step or the process was cancelled |

A step runs every `before`, then every `run`, then every `after`, each pass in list order.
[Lifecycle](lifecycle.md) describes the whole run.

Two attributes come with every callback:

- `process`: the `Process` the callback belongs to, set when the process is built. It is
  unbound before that.
- `name`: the display name used in logs, the class name by default.

Callbacks are extended by subclassing
([ADR 0003](../decisions/0003-connectors-as-protocols.md)). Subclassing in your own code is
all it takes: there is nothing to register. Connectors, by contrast, are extended in the library
([ADR 0015](../decisions/0015-choose-connectors-from-the-job-configuration.md)).

## Sharing data

Callbacks exchange data through `self.process.state`, a dictionary shared by the whole
process, instead of passing a payload from one callback to the next. A callback that only
observes the data does not have to hand it back
([ADR 0004](../decisions/0004-shared-process-state.md)).

```python
from datetime import UTC, datetime

from ingestio import Callback, Process, Step


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


process = Process(
    [Step("lineage", [AddLineageColumns(source="orders-api")])],
    state={"records": [{"id": 1}, {"id": 2}]},
    run_id="run0001",
).run()

assert process.state["records"][0]["_batch_id"] == "run0001"
```

The column names are yours: ingestio has no opinion about them.

## Callbacks shipped with the library

| Callback | Does |
|---|---|
| `ReadSource(config, *, into="records", connection=None, client=None, watermark_value=None, spark=None)` | reads the `source` section of the job's configuration file, builds the reader it names, calls its `read()` and stores the payload in `state[into]` |
| `AppendOnly(config, *, key="records", connection=None, client=None)` | reads the `target` section, builds the writer it names and adds `state[key]` to the target, leaving what the target already holds as it is |
| `FullLoad(config, *, key="records", connection=None, client=None)` | reads the `target` section, builds the writer it names and makes the target hold exactly `state[key]` |
| `SCD2(config, *, key="records", connection=None, client=None)` | reads the `target` and `scd2` sections, builds the writer and the target's reader, and keeps the history of a dimension as a slowly changing dimension of type 2 with `state[key]` |

Each write strategy is one callback ([ADR 0016](../decisions/0016-make-each-write-strategy-one-callback.md)).
All four are exported by `ingestio` and by `ingestio.callback.all`. `config` is the path of the job's
JSON configuration file. The file names the system, such as `postgres` or `s3`, and holds the
connector's parameters; credentials, SDK clients and the watermark reach the callback as its other
arguments, because they do not belong in a JSON file. The callback reads the file when it is built,
so a mistake in it stops the job before anything runs. A callback does not accept a connector
instance: only the connectors the library ships can be named. See
[Configuration](configuration.md) for the file, the systems and their keys, and the errors.

All of them are essential: every error propagates out of the process unchanged.

`connection` is the connection value of the system that needs one. `ReadSource` takes a
`PostgresConnection` for a `postgres` source and a `MongoDBConnection` for a `mongodb` source: its
annotation is `PostgresConnection | MongoDBConnection | None`. `AppendOnly`, `FullLoad` and `SCD2`
keep `PostgresConnection`, because there is no MongoDB writer. Each callback checks the type of
`connection` when it is built, and a connection of another system's class raises a `ValueError`
that names the system and the class ([Configuration](configuration.md#errors)).

`SCD2` is described [below](#scd2). `AppendOnly` and `FullLoad` take the same arguments and subclass `Callback` directly, neither one a
subclass of the other. Neither takes `replace`, `spark` or `watermark_value`. Each calls the writer
once, with the very object that `state[key]` holds: `AppendOnly` calls `write(data)` and `FullLoad`
calls `write(data, replace=True)`, for every system. The callback never copies, converts, iterates,
counts or type-checks the value, so a generator reaches the writer unconsumed. It leaves the state as
it found it, and a missing `state[key]` raises `KeyError` when the process runs. What a write does
to the target is the connector's, as [its section](connectors.md) says: for instance, `AppendOnly`
adds lines to a `json_lines` file and rows to a table, and `FullLoad` empties the file, truncates the
table or overwrites the files.

```python
import json
import tempfile
from pathlib import Path

from ingestio import AppendOnly, FullLoad, Process, Step

with tempfile.TemporaryDirectory() as tmp:
    out = Path(tmp) / "orders.jsonl"
    config = Path(tmp) / "job.json"
    config.write_text(
        json.dumps({"target": {"system": "json_lines", "path": str(out)}}), encoding="utf-8"
    )

    Process(
        [Step("load", [AppendOnly(config, key="orders")])],
        state={"orders": [{"id": 1}, {"id": 2}]},
    ).run()
    Process(
        [Step("load", [AppendOnly(config, key="orders")])],
        state={"orders": [{"id": 3}]},
    ).run()
    print(out.read_text(encoding="utf-8"))  # three lines

    Process(
        [Step("load", [FullLoad(config, key="orders")])],
        state={"orders": [{"id": 4}]},
    ).run()
    print(out.read_text(encoding="utf-8"))  # one line
```

Known limitations of the write callbacks:

- **An append is not idempotent.** Running the same batch twice appends it twice.
- **`FullLoad` over an empty batch empties the target.** The callback calls the writer whatever
  the value holds, an empty list, an exhausted generator or a DataFrame with no rows, because
  checking a DataFrame runs a Spark job and checking a generator consumes it. A job that keeps its
  target when a batch is empty places its own callback before the write, such as the
  `SkipEmptyBatch` of [Configuration](configuration.md#example).
- **A failed `json_lines` write leaves what was written before the failure.** An append leaves
  part of the batch at the end of the file. A `FullLoad` has already emptied the file, so the file
  holds only that part and the previous content is lost.
- **`FullLoad` of plain files is not atomic** on `disk`, `s3`, `gcs` and `volume`: it deletes
  everything under the path, and Spark refuses to overwrite a path that the DataFrame being written
  reads from. A `delta` write is one commit.
- **One target per file.** A job that appends to one target and replaces another uses two
  configuration files, one per callback.

## `SCD2`

`SCD2` keeps the history of a dimension, such as customers or products: instead of overwriting a
row when its values change, it closes the entity's current *version* and opens a new one. Each
version holds the values that were true from `valid_from` until `valid_to`, and the current version
has a null `valid_to`. It is the third write strategy, one more callback beside `AppendOnly` and
`FullLoad` ([ADR 0016](../decisions/0016-make-each-write-strategy-one-callback.md)).

```text
SCD2(config, *, key="records", connection=None, client=None)
```

The arguments are those of `AppendOnly`: `config` is the path of the job's configuration file,
`key` the key of `Process.state` holding the batch, and `connection` and `client` are the target's.
`SCD2` takes no `replace`, `spark` or `watermark_value` and no column names: its parameters, the key
columns, the effective-time column and the rest, are in the [`scd2`
section](configuration.md#the-scd2-section) of the file
([ADR 0017](../decisions/0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md)).
It subclasses `Callback` directly, is essential, and appears in the run's log as `SCD2.run`.

The batch is a Spark DataFrame. The configuration of the example below:

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

### What a run does

When the process runs, `SCD2`:

1. takes `state[key]`, and checks that it holds the columns the `scd2` section names, and none of
   the two version columns;
2. reads the whole target, with the reader of the target's system, built with the batch's own Spark
   session, and checks that the target has exactly the batch's columns plus `valid_from` and
   `valid_to`, compared by name ignoring case;
3. checks the batch in one Spark job (see below);
4. computes the new table and writes it with `write(new_table, replace=True)`, once, as `FullLoad`
   does. The table has the target's columns, in the target's order.

For each entity, the callback chains the entity's current version in the target, if there is one,
and the entity's records of the batch sorted by the effective-time column. A record whose tracked
values differ from the version before it in the chain becomes a new version, with `valid_from` set
to the record's effective time, and the version before it is closed: its `valid_to` becomes that
time. A record whose tracked values are the same as the version before it opens nothing. The
comparison is null-safe: two nulls are equal, and a null differs from a value. So several records
of one entity in a batch each become a version, in the order of their effective time, and the result
is the same however the records are split across runs.

- The effective time comes from a column of the record, not from the time of the run. The column
  keeps its value in every row, and in a new version it equals `valid_from`.
- Closed versions never change. An entity that is current in the target and absent from the batch
  keeps its current version as it is: deletes in the source are not detected.
- A change in a column that is not tracked is ignored: the current version keeps the values it had
  when it opened. Without `tracked_columns`, every column except the key columns and the
  effective-time column is tracked.
- The callback writes whatever the batch holds. A batch with no rows, or whose records change
  nothing, makes the writer replace the target with the rows it already held.
- It leaves the state as it found it, catches nothing and logs nothing of its own. A missing
  `state[key]` raises `KeyError(key)`. It never checks the type of the value: what is not a Spark
  DataFrame fails with Python's or Spark's error.

### The checks

Before anything is written, the batch is checked in one Spark job. Each check is a `ValueError`
with no note; when several fail, the first in this order is raised. No value of the data appears in
a message, only column names.

1. A null in a key column or in the effective-time column:
   `state['records'] has a null in column 'customer_id'`.
2. Two records with the same key columns and the same effective time:
   `state['records'] has records with the same key columns and the same 'updated_at'`.
3. A late-arriving record, whose effective time is earlier than the `valid_from` of its entity's
   current version: `state['records'] has a record whose 'updated_at' is earlier than the
   'valid_from' of its entity's current version`.
4. A record whose effective time equals the `valid_from` of its entity's current version, with other
   tracked values than that version: `state['records'] has a record whose 'updated_at' equals the
   'valid_from' of its entity's current version, with other tracked values`.

A record with the same effective time and the same tracked values as the current version does
nothing. The DataFrame handed to the writer carries the same four conditions as a guard: if the
batch or the target changed between the checks and the write, so that a record now breaks one of
them, the write fails with Spark's exception and the writer's own failure path leaves the target as
it was.

Two more groups of `ValueError` come from the columns, which need no Spark job. They are raised
before the checks and before the writer is called. About the batch, and raised before the target is
read: `state['records'] has no column 'segment', which scd2.tracked_columns names` and
`state['records'] already has a column 'valid_to', which scd2.valid_to_column names`. About the
target, once it is read: `the target has no column 'city'` and `the target has a column 'segment'
that is neither a column of state['records'] nor a version column`. A failure of `read()`, a
missing target included, propagates as the reader's own error, unchanged, and so does a failure of
`write()`.

### Supported target systems

`SCD2` supports `unity_catalog`, `postgres` and `bigquery`, the systems whose
`write(data, replace=True)` is atomic and stays correct when the DataFrame written reads the same
target. Any other system raises when the callback is built, with the configuration's own message and
the narrowed list: `<path>: target.system 'disk' is not one of: bigquery, postgres, unity_catalog`.

| `system` | Reader of the target | Arguments | Write |
|---|---|---|---|
| `unity_catalog` | `UnityCatalogReader` | none | one Delta commit |
| `postgres` | `PostgresReader` | `connection`, required | staging table, then one transaction |
| `bigquery` | `BigQueryReader` | `client`, optional | staging table, then one `MERGE` |

The reader is built when the process runs, from the `target` section's `table_name` (and
`billing_project` for `bigquery`) and the very `connection` and `client` objects the writer
received, with the batch's session. The section's `options` go to the writer only, and the read is a
full read, so an option such as Delta's `versionAsOf` cannot make the rewrite start from an old
version.

### The first run

The target must exist before the first run, on every system, with the batch's columns and the two
version columns: the job creates the dimension once, empty. A missing target fails the run with the
reader's own error, and the writer is not called. An existing empty target is a valid first run:
every record is a new entity.

### Building and errors

Building `SCD2` reads the file once and checks the `target` section as `AppendOnly` does, then the
`scd2` section. It touches no Spark session, client, database or network, and imports no pyspark, so
a mistake in the file raises when the callback is built, before `Process` exists. The errors are in
[Configuration](configuration.md#errors).

<!-- example: skip -->
```python
from pathlib import Path

from ingestio import SCD2, Process, ReadSource, Step
from ingestio.connector.all import PostgresConnection

CONFIG = Path(__file__).with_name("config.json")  # the file shown above

shop = PostgresConnection("pg.internal", "shop", "ingest", "s3cret")

Process([
    Step("extract", [ReadSource(CONFIG, connection=shop)]),
    Step("load", [SCD2(CONFIG)]),
]).run()
```

The dimension `main.gold.dim_customer` exists before the first run. Each run reads the customers
changed since the watermark, and the dimension keeps every state of every customer: a change of
`name`, `city` or `segment` closes the customer's current version at its `updated_at` and opens a
new one. Building the callback runs without Spark, so a mistake in the file shows at once:

```python
import json
import tempfile
from pathlib import Path

from ingestio import SCD2

with tempfile.TemporaryDirectory() as tmp:
    config = Path(tmp) / "job.json"
    job = {
        "target": {"system": "unity_catalog", "table_name": "main.gold.dim_customer"},
        "scd2": {"key_columns": ["customer_id"], "effective_time_column": "updated_at"},
    }
    config.write_text(json.dumps(job), encoding="utf-8")
    SCD2(config)  # builds, reads no table

    job["target"] = {"system": "disk", "path": "/data/dim", "format": "parquet"}
    config.write_text(json.dumps(job), encoding="utf-8")
    try:
        SCD2(config)
    except ValueError as error:
        print(error)  # ... target.system 'disk' is not one of: bigquery, postgres, unity_catalog

    job["target"] = {"system": "unity_catalog", "table_name": "main.gold.dim_customer"}
    job["scd2"]["valid_to_column"] = "updated_at"
    config.write_text(json.dumps(job), encoding="utf-8")
    try:
        SCD2(config)
    except ValueError as error:
        print(error)  # ... scd2.effective_time_column and scd2.valid_to_column both name ...
```

### Known limitations of `SCD2`

- **Every run rewrites the whole dimension.** Its cost grows with the dimension, not with the
  batch. On `postgres`, `TRUNCATE` holds an exclusive lock on the dimension while the transaction
  applies, and fails when a foreign key references it. On `bigquery`, the `MERGE` is billed over the
  whole table. On `unity_catalog`, every run is a new version of the table that rewrites every file,
  kept until `VACUUM`.
- **The batch and the target are each evaluated twice,** once by the checks and once by the write.
  A batch that is a lazy read of a live source reads it twice; the guard fails the write when the
  second evaluation breaks a check.
- **Deletes in the source are not detected.**
- **A late-arriving record fails the run.** The job corrects the target, or the batch, itself.
- **Re-running a batch that holds several records of one entity fails** as late-arriving, because
  its first records are earlier than the current version that its last record opened. A batch with
  one record per entity re-runs without change.
- **The callback assumes it is the dimension's only writer** during a run. A write by someone else
  between the read and the write is lost: overwritten by the `TRUNCATE` or the `MERGE`, or, on
  Delta, subject to Delta's conflict detection.
- **The callback trusts the target:** two current versions of one entity, or a null in its key
  columns or `valid_from` column, give a wrong result, unchecked.
- **No surrogate key.** A version is identified by its key columns and its `valid_from`; a column
  that the system generates on insert cannot be in the target.
- **Schema changes are the job's.** A new column in the batch fails the run until the job adds it to
  the target with its own DDL.
- **On `postgres`, a Spark task retry can duplicate rows** of the staging table, and so duplicate a
  version. **On `bigquery`,** `require_partition_filter` breaks the full read and the replace, and
  rows still in the streaming buffer break the replace.
- **On `unity_catalog`, that Spark lets a plan overwrite a Delta table it reads is unverified.**
  The Delta integration test would verify it, but it runs neither in the development environment
  nor in CI as planned, so the support rests on the code of `UnityCatalogWriter` and on the Delta
  guarantees, not on a test of `SCD2`.
- **`SCD2`'s behaviour is verified only in CI:** the tests that need Spark are skipped wherever
  `INGESTIO_SPARK_TESTS` is not `1`, the development environment included.
- **A tracked column of a map type** cannot be compared: Spark's error. **The effective time is
  ordered by Spark's rules for its type:** a string column orders as text.
- **One dimension per configuration file.** A job that wants another history, such as snapshot
  semantics or columns overwritten in place, writes its own callback over the shipped connectors.

## Controlling the flow

A callback can stop part of the run by raising one of two exceptions, both exported by
`ingestio`:

- `CancelStepException`: skip the rest of this step and carry on with the next one.
- `CancelProcessException`: stop the process cleanly. `process.cancelled` becomes `True`.

Any other exception propagates and fails the run. `after` runs in every case. See
[Lifecycle](lifecycle.md#cancellation-and-failure).

## Criticality

Whether a failing callback fails the pipeline is its criticality, decided by whoever writes
it. It is not implemented yet: see [Criticality](criticality.md).
