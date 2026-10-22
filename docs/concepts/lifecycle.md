# Lifecycle of a run

How a `Process` turns a list of steps into a run.

## Building a process

`ingestio.Process(steps, *, state=None, run_id=None, log=None)`

- `steps`: the steps to run, in order.
- `state`: the initial shared state, copied into `process.state`.
- `run_id`: an identifier for the run; eight hexadecimal characters are generated when it is
  omitted.
- `log`: the logger to write to, `ingestio.core` by default.

The process rejects an unusable configuration before anything runs: it raises `ValueError`
when there is no step or when two steps share a name. `Step(name, callbacks=())` raises
`ValueError` for a blank name. A step may hold no callbacks.

Building the process binds every callback to it: from then on, `callback.process` is the
process.

## Running

`process.run()` runs every step in list order and returns the process itself, so the state
stays reachable: `Process([...]).run().state`.

Inside a step, the callbacks run in three passes, each in list order: every `before`, then
every `run`, then every `after`. There is no priority attribute and no hidden reordering: the
sequence you write is the sequence that runs.

```python
from ingestio import Callback, Process, Step


class Announce(Callback):
    """Print each moment it goes through."""

    def __init__(self, label: str) -> None:
        self.label = label

    def before(self) -> None:
        print(f"{self.label}.before")

    def run(self) -> None:
        print(f"{self.label}.run")

    def after(self) -> None:
        print(f"{self.label}.after")


Process([
    Step("extract", [Announce("read")]),
    Step("load", [Announce("lineage"), Announce("write")]),
]).run()
```

```text
read.before
read.run
read.after
lineage.before
write.before
lineage.run
write.run
lineage.after
write.after
```

`process.started_at` holds the UTC time the run started.

## Cancellation and failure

| Raised by a callback | Effect |
|---|---|
| `CancelStepException` | the rest of the step is skipped; the process carries on with the next step |
| `CancelProcessException` | the process stops cleanly and `process.cancelled` becomes `True` |
| any other exception | it propagates out of `run()` and the run fails |

In every case the `after` pass of the current step runs, so cleanup stays reliable.

```python
from ingestio import Callback, CancelProcessException, Process, Step


class SkipWhenEmpty(Callback):
    def run(self) -> None:
        if not self.process.state.get("records"):
            raise CancelProcessException("nothing to ingest")


process = Process(
    [Step("check", [SkipWhenEmpty()]), Step("load")],
    state={"records": []},
).run()

assert process.cancelled
```

## Logging

The process logs through the standard `logging` module, and every line carries the run id, so
concurrent runs on the same cluster stay readable:

```text
[a3f7c9] process started | 2 steps
[a3f7c9] > extract (1/2) | 1 callbacks
[a3f7c9] < extract | 0.412s
[a3f7c9] > load (2/2) | 2 callbacks
[a3f7c9] < load | 0.019s
[a3f7c9] process finished | 0.431s
```

A cancellation adds a line with its message: `[a3f7c9] process cancelled: nothing to ingest`.

## Behaviour not specified yet

The code behaves as follows today, but no spec has decided that it should:

- An exception raised in an `after` stops the remaining `after` calls of that step and
  replaces any exception already propagating.
- When a `before` fails, the `after` pass still runs for every callback of the step,
  including those whose `before` never ran.
- Calling `run()` again reuses the state left by the previous run.
- A callback instance placed in two processes stays bound to the one built last.
