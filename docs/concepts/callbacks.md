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

Unlike connectors, callbacks are extended by subclassing
([ADR 0003](../decisions/0003-connectors-as-protocols.md)). Subclassing in your own code is
all it takes: there is nothing to register.

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
| `ReadSource(reader, *, into="records")` | calls `reader.read()` and stores the payload in `state[into]` |
| `WriteTarget(writer, *, key="records")` | calls `writer.write(state[key])` |

Both are exported by `ingestio` and by `ingestio.callback.all`. Append-only and SCD2 will be
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
