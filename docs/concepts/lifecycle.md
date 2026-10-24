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

The process logs through the standard `logging` module, to `ingestio.core` or to the logger
given as `log`. The log is a tree: a line when the process starts and ends, a line when each
step starts and ends, and a line for every callback moment that ran. Every line carries the run
id, so concurrent runs on the same cluster stay readable:

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

The tree lives in the message, so every handler, `caplog`, a file or `logging.basicConfig`
shows it as plain text. Each line is written when its event happens, and nothing is buffered.

### Reading a line

A line is `[<run id>] <guides><marker> <text>`. The guides are `│ ` once per level of depth:
none for the process, one for a step, two for a callback moment. A line depends only on its
own depth.

| Event | Marker | Text | Level |
|---|---|---|---|
| the process starts | `┌` | `process started · 2 steps` | INFO |
| the process ends | `└` | `process finished in <d>`, `process failed in <d>` or `process cancelled in <d>` | INFO, ERROR for failed |
| a step starts | `┌` | `<step> (<i>/<n>) · <c> callbacks` | INFO |
| a step ends | `└` | `<step> done in <d>`, `failed in <d>` or `cancelled in <d>` | INFO, ERROR for failed |
| a moment succeeds | `✔` | `<callback>.<moment> <d>` | INFO |
| a moment fails | `✖` | `<callback>.<moment> failed after <d>: <Type>: <message>` | ERROR |
| a moment cancels the step | `⊘` | `<callback>.<moment> cancelled the step after <d>: <reason>` | INFO |
| a moment cancels the process | `⊘` | `<callback>.<moment> cancelled the process after <d>: <reason>` | INFO |

- `step` and `callback` are singular after a 1 and plural otherwise, 0 included. The count of
  callbacks includes those that implement no moment.
- `<callback>` is `Callback.name`. `<moment>` is `before`, `run` or `after`.
- A callback moment gets a line unless the method the engine calls is the one `Callback`
  itself defines, which does nothing. A moment inherited from a parent class gets a line; a
  callback that implements no moment has none. A moment assigned on the instance, such as
  `self.run = self._work` in `__init__`, gets a line too. The engine still calls every moment.
- The reason of a cancellation is only on the line of the moment that raised it. When the
  message of an exception is empty, the line ends after the type name or the duration, with
  no colon.
- Building a line never raises. When `str(exception)` raises, the line is built as if the
  message were empty. When `callback.name` raises, the line shows the class name of the
  callback. In both cases the run behaves as it would otherwise: the original exception
  leaves `run()` untouched, and a run that would succeed still succeeds.
- A failure line holds the type and the message of the exception, never a traceback. The
  exception leaves `run()` unchanged, so whoever receives it decides how to report it.
- The closing lines of the step and of the process are written whatever the outcome, even when
  an `after` raises. After a cancelled process, the steps that never started write nothing.
- A message on several lines continues on lines that repeat the run id and the guides, then
  two spaces where the marker was, so every line starts with the run id.

A failure and a cancellation:

```text
[a3f7c9] │ │ ✖ AppendOnly.run failed after 2ms: PermissionError: [Errno 13] Permission denied
[a3f7c9] │ └ load failed in 3ms
[a3f7c9] └ process failed in 415ms
```

```text
[a3f7c9] │ ┌ bronze (1/2) · 2 callbacks
[a3f7c9] │ │ ⊘ SkipWhenEmpty.run cancelled the step after 1ms: nothing to ingest
[a3f7c9] │ │ ✔ Cleanup.after 0ms
[a3f7c9] │ └ bronze cancelled in 2ms
```

A `CancelStepException` raised from an `after` is a failure line, because the engine does not
handle it there.

### Durations

A moment line shows the duration of that one call. A step's closing line covers the step from
its start line until its `after` pass ends, and the process's covers the whole `run()`. Units
are truncated, never rounded:

| Duration | Format |
|---|---|
| below one second | `412ms` |
| below one minute | `3.52s` |
| below one hour | `2m 13s` |
| one hour or more | `1h 2m 3s` |

### Showing the tree on the console

`ingestio` configures nothing by default: it only registers a `NullHandler`. To see the tree
while you develop, call `ingestio.log_to_console`:

`log_to_console(*, stream=None, level=logging.INFO, color=None) -> logging.Handler`

- `stream`: where to write, `sys.stderr` as it is when the function is called when omitted.
- `level`: the lowest level written. It must be above `logging.NOTSET`: otherwise the call
  raises `ValueError("level must be above NOTSET")` before touching any logger.
- `color`: `True` always colors, `False` never does, `None` colors when the stream is a
  terminal and the `NO_COLOR` environment variable is unset or empty.

It returns the handler it attached.

```python
import io

from ingestio import Callback, Process, Step, log_to_console


class Hello(Callback):
    def run(self) -> None:
        pass


out = io.StringIO()
log_to_console(stream=out)
Process([Step("greet", [Hello()])], run_id="a3f7c9").run()

assert "[a3f7c9] ┌ process started · 1 step\n" in out.getvalue()
```

Without `stream`, the tree goes to standard error, which keeps standard output free for data:

```python
from ingestio import log_to_console

log_to_console()
```

What the call does:

- It attaches one handler to the `ingestio` logger and writes each line of the tree as its
  message, with no time, level or logger name. Any other record that reaches the handler is
  written as its message, and its traceback when it has one, never colored.
- It lowers the level of the `ingestio` logger to `level` when the records would not reach the
  handler otherwise. It never raises it.
- The last call wins: it removes the handler an earlier call attached, so there is at most one.
  Removing the returned handler with `logging.getLogger("ingestio").removeHandler(handler)`
  undoes the call, apart from the level.
- It never touches the root logger, any other handler, or `propagate`. The records still reach
  the root logger's handlers, so an application that also prints the root logger on the console
  sees every line twice, and chooses which of the two to drop.
- It covers every logger under `ingestio`, including a logger given to `Process` as `log` when
  it is named under `ingestio.`. A logger outside that hierarchy, such as
  `logging.getLogger("my.app")`, receives the same lines for its own handlers, but the helper
  does not cover it.

### Colors

Colors are ANSI escape codes written when the handler writes; a record's message never holds
them, so any other handler writes plain text. The prefix is dim. The marker is cyan for a start,
green for a success, red for a failure and yellow for a cancellation; on failure and
cancellation lines the text takes the color of the marker too. No color is left active at the
end of a line.

Whether to color is decided once, when `log_to_console` is called:

1. `color=True` colors and `color=False` does not, whatever the stream and the environment.
2. With `color=None`, a `NO_COLOR` set to a non-empty value turns colors off
   ([no-color.org](https://no-color.org/)).
3. Otherwise it colors when `stream.isatty()` returns `True`. A stream without `isatty` is not
   a terminal.

Notebooks such as Jupyter or Databricks write to streams that are not terminals. Where they
render ANSI codes, pass `color=True`. Windows consoles without ANSI processing are not handled:
pass `color=False` or set `NO_COLOR`.

### Encoding

The glyphs of the tree are in the message, with no ASCII fallback, so every record the engine
writes is non-ASCII. A handler that writes to a stream that is not UTF-8 makes `logging`
report an encoding error for every line, without stopping the run. This happens with a
`FileHandler` opened without an encoding on Windows before Python 3.15. Open file handlers with
`encoding="utf-8"`:

```python
import logging
import tempfile
from pathlib import Path

from ingestio import Callback, Process, Step


class Hello(Callback):
    def run(self) -> None:
        pass


path = Path(tempfile.mkdtemp()) / "run.log"
handler = logging.FileHandler(path, encoding="utf-8")
log = logging.getLogger("ingestio.core")
log.addHandler(handler)
log.setLevel(logging.INFO)
Process([Step("greet", [Hello()])], run_id="a3f7c9").run()
log.removeHandler(handler)
handler.close()

assert "┌ process started" in path.read_text(encoding="utf-8")
```

## Behaviour not specified yet

The code behaves as follows today, but no spec has decided that it should:

- An exception raised in an `after` stops the remaining `after` calls of that step and
  replaces any exception already propagating.
- When a `before` fails, the `after` pass still runs for every callback of the step,
  including those whose `before` never ran.
- Calling `run()` again reuses the state left by the previous run.
- A callback instance placed in two processes stays bound to the one built last.
