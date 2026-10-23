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
| `WriteTarget(config, *, key="records", replace=False, connection=None, client=None)` | reads the `target` section, builds the writer it names and writes `state[key]` with it, appending, or replacing with `replace=True` |

Both are exported by `ingestio` and by `ingestio.callback.all`. `config` is the path of the job's
JSON configuration file. The file names the system, such as `postgres` or `s3`, and holds the
connector's parameters; credentials, SDK clients and the watermark reach the callback as its other
arguments, because they do not belong in a JSON file. The callback reads the file when it is built,
so a mistake in it stops the job before anything runs. A callback does not accept a connector
instance: only the connectors the library ships can be named. See
[Configuration](configuration.md) for the file, the systems and their keys, and the errors.

Both are essential: every error propagates out of the process unchanged.

```python
import json
import tempfile
from pathlib import Path

from ingestio import Process, Step, WriteTarget

with tempfile.TemporaryDirectory() as tmp:
    out = Path(tmp) / "orders.jsonl"
    config = Path(tmp) / "job.json"
    config.write_text(
        json.dumps({"target": {"system": "json_lines", "path": str(out)}}), encoding="utf-8"
    )

    Process(
        [Step("load", [WriteTarget(config, key="orders")])],
        state={"orders": [{"id": 1}, {"id": 2}]},
    ).run()
    print(out.read_text(encoding="utf-8"))
```

Append-only and SCD2 will be
the library's default callbacks; they are planned in
[specs/append_only_callback.md](../../specs/append_only_callback.md) and
[specs/scd2_callback.md](../../specs/scd2_callback.md).

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
