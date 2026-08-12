# ingestio

Small, composable library for building data ingestion pipelines.

`ingestio` describes **any data movement process** as a sequence of steps. It does not
assume your data is a table, that your architecture has bronze and silver layers, or that
your processing is batch. Those are things you build with it, not things it imposes.

> **Status: early alpha (0.0.1).** The core is functional and tested, the API is not stable
> yet.

## Install

```bash
pip install ingestio
```

No runtime dependencies.

## The idea

Three pieces, and the ordering is always yours:

| | |
|---|---|
| **Step** | a named point of the process; holds the callbacks that run there |
| **Callback** | the code of one stage |
| **Connector** | a reader or a writer, injected into the callback that needs it |

Steps run in list order, and callbacks inside a step run in list order. There is no
priority attribute and no hidden reordering: the sequence you write is the sequence that
runs.

The library ships the engine, one reader, one writer and two generic callbacks. The stages
that are specific to your ingestion are yours to write — that is the point.

## Example: a public API into a volume

```python
from ingestio import Process, ReadSource, Step, WriteTarget
from ingestio.connector.all import JsonApiReader, JsonLinesWriter

process = Process([
    Step("extract", [
        ReadSource(JsonApiReader("https://api.example.com/v1/orders")),
    ]),
    Step("load", [
        WriteTarget(JsonLinesWriter("/Volumes/main/bronze/orders.jsonl")),
    ]),
]).run()

print(len(process.state["records"]))
```

A Databricks volume is an ordinary path, so the same code runs on a laptop and on a
cluster.

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
    WriteTarget(JsonLinesWriter("/Volumes/main/bronze/orders.jsonl")),
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

Standard `logging`, never `print`, and every record carries the run id — so concurrent
ingestions on the same cluster stay readable.

```
[a3f7c9] process started | 2 steps
[a3f7c9] > extract (1/2) | 1 callbacks
[a3f7c9] < extract | 0.412s
[a3f7c9] > load (2/2) | 2 callbacks
[a3f7c9] < load | 0.019s
[a3f7c9] process finished | 0.431s
```

## Roadmap

More connectors (object storage, Delta), and recipes — an optional declarative way to
assemble a process, never a required one.

## License

Apache-2.0
