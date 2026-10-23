# Spec: tree-shaped progress log for a run

- **Status:** validated
- **Slug:** `run_progress_log`. The spec is `specs/run_progress_log.md` and its tests are
  `tests/test_run_progress_log.py`.
- **Related:** [ADR 0008](../docs/decisions/0008-stdlib-logging-run-id.md),
  [ADR 0006](../docs/decisions/0006-three-pass-step.md),
  [ADR 0007](../docs/decisions/0007-cancellation-exceptions.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0002](../docs/decisions/0002-zero-dependency-core.md),
  [Lifecycle](../docs/concepts/lifecycle.md#logging)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

The engine's log becomes a tree: a line when the process starts and ends, a line when each step
starts and ends, and a line for every callback moment that actually ran, each closing line with
its outcome and duration. Everything still goes through the standard `logging` module, with the
run id on every line. A new opt-in helper, `ingestio.log_to_console()`, attaches a handler to
the `ingestio` logger that writes the tree to a stream, in color when the stream is a terminal.

## Motivation

Today a run writes one line when the process starts and ends and two per step, with the step's
duration. It does not say which callback is running, in which moment, or how long each took,
and it does not say where a run failed: when `run()` raises, no line records the failure and
the process's closing line is skipped, and when an `after` raises, the step's closing line is
skipped too.

The maintainer wants a developer to see, at each callback point, which step the process is in,
what is happening and how long each part took. Two references set the direction: an in-house
ingestion library that prints a nested tree of start and finish lines with per-callback
durations, and yarn's output (`┌ Resolution step` / `│ ...` / `└ Completed in ...`, in color),
"pretty and very visible". In ingestio terms the hierarchy is process, step, callback moment;
reference 1's "Callback Point" is a `Step`.

## Fit with the philosophy

- **Concept:** neither a connector nor a callback. This is the engine's own logging.
  [ADR 0008](../docs/decisions/0008-stdlib-logging-run-id.md) already makes `Process` write a
  line per step; this change gives those lines a tree shape and adds a line per callback
  moment. The philosophy names `Process` and `Step` as the engine that composes the two
  concepts, not a third concept, because nobody extends them to integrate a system or add an
  action. The helper is in the same position: it is the engine's way of presenting its own log,
  one function that attaches a standard `logging` handler. It integrates no external system,
  performs no pipeline action, is extended by nobody and registers nothing. It is part of the
  engine's logging, not a third concept.
- **Essential by default:** unchanged. A failure now gets a line at `ERROR`, and the exception
  still propagates out of `run()` exactly as before: nothing is swallowed.
- **Simplicity:** one public function. Each line is drawn from its own depth alone, so there is
  no state to carry between lines, and no framework, registry or plugin point.
- **Compose, do not impose:** the helper is opt-in. It touches only the `ingestio` logger,
  never the root logger, and leaves propagation alone. An application that does not call it
  configures `logging` as it likes and still gets the tree as plain text.
- **No dependencies in the core:** colors are ANSI escape codes written directly, decided with
  `isatty()` and the `NO_COLOR` environment variable. No `rich`, `colorama` or similar.
- **Streaming:** nothing here closes the door; see decision 14.

## Scope

In scope:

- the text, level and order of every line the engine writes for a run;
- which callback moments get a line;
- the format of durations;
- the helper `log_to_console`: what it attaches, where it writes, levels, color, repeated
  calls;
- the color rules of the helper's output.

Out of scope:

- callbacks writing their own lines into the tree; a helper for callback authors is a later
  spec;
- extra detail lines from `ReadSource` and `WriteTarget`, such as record counts;
- a name for a `Process`, to tell several processes apart, which a separate spec will cover
  (decision 11);
- lines for non-essential failures and a run result, which belong to the criticality spec
  ([ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md));
- public structured fields on log records, for JSON or structured logging, and a public
  handler or formatter class (decision 12);
- turning on ANSI processing in Windows consoles that lack it: there, pass `color=False` or set
  `NO_COLOR`;
- color conventions other than `NO_COLOR` and the `color` argument, such as `FORCE_COLOR` or
  `TERM=dumb`;
- any change to the lifecycle, including the items under
  [Behaviour not specified yet](../docs/concepts/lifecycle.md#behaviour-not-specified-yet).

## Public API

New: `from ingestio import log_to_console`, in `ingestio.__all__`, through
`ingestio/basics.py`. It is not exported by `ingestio.callback.all` or
`ingestio.connector.all`.

```python
def log_to_console(
    *,
    stream: TextIO | None = None,
    level: int = logging.INFO,
    color: bool | None = None,
) -> logging.Handler:
    """Show the progress tree of every run on a stream.

    Args:
        stream: Stream to write to. `sys.stderr`, as it is when the function is called,
            when omitted.
        level: Lowest level written.
        color: `True` always colors, `False` never does, `None` colors when the stream is a
            terminal and `NO_COLOR` is unset or empty.

    Returns:
        The handler attached to the `ingestio` logger.

    Raises:
        ValueError: If `level` is not above `logging.NOTSET`.
    """
```

`log_to_console` is the only new public name. Its handler, how that handler formats records,
and the structured data the engine attaches to records stay internal (decision 12).

Unchanged: the signatures of `Process`, `Step` and `Callback` and every documented attribute.
`Callback.name`, already documented as the display name used in logs, is the name the lines
show.

Changed documented behaviour, which is not public API as AGENTS.md defines it but is described
in `README.md` and `docs/concepts/lifecycle.md`: the text of every line the engine writes, the
new lines per callback moment, and failure lines at `ERROR`.

## Behaviour

### The shape of a line

Every line the engine writes has this shape:

```text
[<run id>] <guides><marker> <text>
```

- `<run id>` is `process.run_id`.
- `<guides>` is `│ ` (U+2502 and a space) once per level of depth: none for the process
  (depth 0), one for a step (depth 1), two for a callback moment (depth 2).
- `<marker>` is one character, given by the table below.
- A line's prefix depends only on its own depth, never on the lines written before or after it.

| Event | Depth | Marker | Text | Level |
|---|---|---|---|---|
| the process starts | 0 | `┌` | `process started · <S> <step/steps>` | INFO |
| the process finishes | 0 | `└` | `process finished in <D>` | INFO |
| the process fails | 0 | `└` | `process failed in <D>` | ERROR |
| the process is cancelled | 0 | `└` | `process cancelled in <D>` | INFO |
| a step starts | 1 | `┌` | `<step> (<i>/<S>) · <C> <callback/callbacks>` | INFO |
| a step is done | 1 | `└` | `<step> done in <D>` | INFO |
| a step fails | 1 | `└` | `<step> failed in <D>` | ERROR |
| a step is cancelled | 1 | `└` | `<step> cancelled in <D>` | INFO |
| a moment succeeds | 2 | `✔` | `<callback>.<moment> <D>` | INFO |
| a moment fails | 2 | `✖` | `<callback>.<moment> failed after <D>: <detail>` | ERROR |
| a moment cancels the step | 2 | `⊘` | `<callback>.<moment> cancelled the step after <D>: <reason>` | INFO |
| a moment cancels the process | 2 | `⊘` | `<callback>.<moment> cancelled the process after <D>: <reason>` | INFO |

The characters are `┌` U+250C, `└` U+2514, `│` U+2502, `✔` U+2714, `✖` U+2716, `⊘` U+2298
and `·` U+00B7. They are in the message itself, with no ASCII fallback (decision 8), so every
record the engine writes is now non-ASCII. A handler that writes to a stream that is not UTF-8,
such as a `FileHandler` opened without `encoding="utf-8"` on Windows before Python 3.15, makes
`logging` report an encoding error for every line, without stopping the run. The documentation
warns about it.

- `<S>` is the number of steps of the process, `<i>` the position of the step, from 1, and
  `<C>` the number of callbacks of the step, whether or not they implement any moment.
- `step` and `callback` are singular when the number before them is 1 and plural otherwise,
  0 included.
- `<step>` is `step.name`. `<callback>` is `callback.name`, read when the line is written. When
  reading `callback.name` raises an exception deriving from `Exception`, `<callback>` is
  `type(callback).__name__`.
- `<moment>` is `before`, `run` or `after`.
- `<D>` is a duration, in the format of [Durations](#durations).
- `<detail>` is `<type>: <message>`, where `<type>` is `type(exc).__name__` and `<message>` is
  `str(exc)`. When the message is empty, or when `str(exc)` raises an exception deriving from
  `Exception`, `<detail>` is `<type>` alone, with no colon after it.
- `<reason>` is `str(exc)` of the cancel exception. When it is empty, or when `str(exc)` raises
  an exception deriving from `Exception`, the line ends right after the duration, with no colon.

**Building a line never raises.** In the two fallbacks above, the run behaves exactly as if the
line had been built normally: the callback's original exception leaves `run()` untouched, and a
run that would succeed still succeeds (decision 25).

**Text on several lines.** When the text of a line contains line breaks, as `str.splitlines()`
splits it, its first line is written as above and every following line as
`[<run id>] <guides>  <line>`: the same run id and guides, then two spaces where the marker and
its space were. Every line therefore starts with the run id
([ADR 0008](../docs/decisions/0008-stdlib-logging-run-id.md)). One event is one log record; its
message may span several lines.

A successful run:

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

The same run when `WriteTarget` fails, from its failure on:

```text
[a3f7c9] │ │ ✖ WriteTarget.run failed after 2ms: PermissionError: [Errno 13] Permission denied
[a3f7c9] │ └ load failed in 3ms
[a3f7c9] └ process failed in 415ms
```

A step cancelled by a callback:

```text
[a3f7c9] │ ┌ bronze (1/2) · 2 callbacks
[a3f7c9] │ │ ⊘ SkipWhenEmpty.run cancelled the step after 1ms: nothing to ingest
[a3f7c9] │ │ ✔ Cleanup.after 0ms
[a3f7c9] │ └ bronze cancelled in 2ms
```

A failure whose message spans two lines:

```text
[a3f7c9] │ │ ✖ WriteTarget.run failed after 2ms: Py4JJavaError: An error occurred while calling o123.save.
[a3f7c9] │ │   : java.lang.RuntimeException: table not found
```

### Which moments get a line

A callback moment gets a line unless the method the engine calls for it is the one `Callback`
itself defines, which does nothing. The method is found the way Python finds any attribute:

- a moment overridden by the callback's class gets a line;
- a moment overridden by a parent class, and inherited, gets a line: if `Base` overrides `run`
  and `Child(Base)` overrides `before`, a `Child` gets a `before` line and a `run` line, and no
  `after` line;
- a moment assigned on the instance, such as `self.run = self._work` in `__init__`, gets a
  line, because the engine calls the instance's attribute;
- a moment the class sets back to `Callback`'s own method gets no line;
- an override counts even when its body does nothing or only calls `super()`.

The engine still calls every moment of every callback, as it does today; only the line is left
out. There are no header lines for the passes: the order of the lines shows them.

### Outcomes

A moment's outcome:

- it returns: the moment succeeded;
- it raises in `before` or `run`: a `CancelStepException` cancels the step, a
  `CancelProcessException` cancels the process, and any other exception is a failure;
- it raises in `after`: a `CancelProcessException` cancels the process, and any other exception
  is a failure, `CancelStepException` included, because the engine does not handle a
  `CancelStepException` raised from `after`
  ([ADR 0007](../docs/decisions/0007-cancellation-exceptions.md)).

"Any other exception" includes exceptions that do not derive from `Exception`, such as
`KeyboardInterrupt`: they leave `run()` like any failure.

A step's outcome, once its `after` pass has ended, normally or by an exception:

- failed, when an exception other than `CancelProcessException` leaves the step;
- cancelled, when a `CancelProcessException` leaves the step, or when the engine handled a
  `CancelStepException` raised in the step and nothing leaves it;
- done, otherwise.

The process's outcome:

- failed, when an exception leaves `run()`;
- cancelled, when a `CancelProcessException` stopped it (`process.cancelled` is `True`);
- finished, otherwise.

The closing line of the step and of the process is written whatever the outcome, including when
an `after` raises, and before the exception leaves `run()`. After a cancelled process, the
steps that never started write nothing.

### When lines are written

Each line is written when its event happens: a start line before the work it opens, a moment
line right after the moment returns or raises, a closing line once the step or the process has
ended. Nothing is buffered. Inside a step the lines follow the order in which the moments run:
the `before` pass, then the `run` pass, then the `after` pass.

### Durations

- A moment line shows the duration of that one call. A step's closing line covers the step from
  just before its start line until its `after` pass ends. The process's closing line covers
  `run()` from its start to its end.
- Every duration is measured with `time.perf_counter()`, looked up on the `time` module at each
  measurement, so that a test can control durations by replacing it.
- The format works on the whole number of milliseconds, `n = int(seconds * 1000)`, truncated:

| `n` | Format | Examples |
|---|---|---|
| below 1 000 | `<n>ms` | `0ms`, `412ms`, `999ms` |
| below 60 000 | `<seconds>.<hundredths>s` | `1.00s`, `3.52s`, `59.99s` |
| below 3 600 000 | `<minutes>m <seconds>s` | `1m 0s`, `2m 13s` |
| from 3 600 000 | `<hours>h <minutes>m <seconds>s` | `1h 2m 3s` |

Every unit is truncated, never rounded, so a duration never reads `1000ms` or `60.00s`.

### What does not change

This change is observability only.

- The three passes and their order, `after` running in every case, cancellation, the exceptions
  that propagate, `process.cancelled`, `process.started_at` and `run()` returning the process
  behave exactly as before.
- An exception leaves `run()` as the very object the callback raised, with the same traceback
  and no note added.
- No record the engine writes carries `exc_info`: the tree shows no traceback, which stays with
  the exception that leaves `run()`.
- The engine writes to `ingestio.core` by default, or to the logger given as `log`.
- The items under
  [Behaviour not specified yet](../docs/concepts/lifecycle.md#behaviour-not-specified-yet) stay
  unspecified. The tree shows a line for every moment that ran, with the outcome it had,
  whatever the engine does. For example, when an `after` raises while another exception is
  already leaving the step, both moments get their failure line; which exception leaves `run()`
  is not decided here. Running a process a second time writes a second tree with the same run
  id.

### The helper

`log_to_console(*, stream=None, level=logging.INFO, color=None)` raises
`ValueError("level must be above NOTSET")` when `level` is not above `logging.NOTSET`, before
touching any logger (decision 26). Otherwise, on every call, it:

1. removes from the `ingestio` logger the handler that an earlier call attached, if it is still
   there;
2. creates a handler that writes to `stream`, or to `sys.stderr` as it is at the time of the
   call when `stream` is `None`, sets the handler's level to `level`, and attaches it to the
   `ingestio` logger;
3. lowers the `ingestio` logger's level to `level` when the logger's effective level is higher,
   so that the records reach the handler; it never raises the logger's level;
4. returns the handler.

At any time, at most one handler attached by the helper is on the `ingestio` logger, and the
last call decides where it writes. The helper never touches the root logger, any other logger,
the other handlers of the `ingestio` logger (the package's `NullHandler` included) or its
`propagate` attribute. Records therefore still reach the root logger's handlers: an application
that also prints the root logger on the console sees the lines twice, and chooses which to drop.
Removing the returned handler from `logging.getLogger("ingestio")` undoes the call, apart from
the level set in step 3.

The handler covers every logger under `ingestio` whose records propagate to it, including a
logger given to `Process` as `log` when it is named under `ingestio.`. A logger outside that
hierarchy, such as `logging.getLogger("my.app")`, is not covered: the engine writes the same
lines to it, for its own handlers.

**Output.** Each record of the tree is written as its message followed by `\n`, with no time,
level or logger name. Any other record that reaches the handler is written as
`logging.Formatter("%(message)s")` writes it: the message, and the traceback when the record
carries one. Those records are never colored.

**When it colors.** Decided once, when the helper is called:

| `color` | Colors |
|---|---|
| `True` | always, whatever the stream and `NO_COLOR` |
| `False` | never |
| `None` | when `stream.isatty()` returns `True` and `NO_COLOR` is unset or empty |

A stream without `isatty` counts as not a terminal. `NO_COLOR` follows
[no-color.org](https://no-color.org/): set to a non-empty value, it turns color off, and an
explicit argument overrides it. Notebooks such as Jupyter or Databricks write to streams that
are not terminals; where they render ANSI codes, `color=True` turns colors on.

**How it colors.** The codes are SGR sequences: reset `\x1b[0m`, dim `\x1b[2m`, red
`\x1b[31m`, green `\x1b[32m`, yellow `\x1b[33m`, cyan `\x1b[36m`.

1. The prefix, `[<run id>] ` and the guides, is dim.
2. The marker takes the color of the outcome: `┌` cyan; `✔`, and the `└` of `done` and
   `finished` lines, green; `✖`, and the `└` of `failed` lines, red; `⊘`, and the `└` of
   `cancelled` lines, yellow.
3. On failure and cancellation lines, the text after the marker has the marker's color. On the
   other lines it has no color.
4. Each colored part ends with a reset, so no color is active at the end of a line or carries
   over to the next line or record.
5. A continuation line has a dim prefix and its text in the color of the first line's text.
6. Removing every SGR sequence from the output gives back, character for character, what the
   handler writes without color.

The codes are added when the handler writes. A record's message never contains them, so any
other handler that receives the same record writes plain text.

### Where it lives

- The helper and its handler go into a new module, `src/ingestio/console.py`, named after the
  medium it writes to, and are exported from `ingestio/basics.py` and `ingestio/__init__.py`.
- The engine attaches to each record of the tree, through `extra`, the structured data the
  handler needs to color it, such as the depth, the marker and the outcome. The handler colors
  from that data and never parses the message: step names, callback names, run ids and
  exception messages are arbitrary text. That data is not public API and is not documented
  (decision 12).
- No dependency is added: the SGR sequences are written directly, and color is decided with
  `os.environ` and `isatty()`.

### Notes for testing

- Durations: replace `time.perf_counter` with a fake clock that only the test's callbacks
  advance. Pick values away from the unit boundaries, since `seconds * 1000` is a float.
- The helper changes global state, the handlers and level of the `ingestio` logger: each test
  restores them.
- A terminal can be simulated with an `io.StringIO` subclass whose `isatty()` returns `True`,
  and `NO_COLOR` with `monkeypatch.setenv` and `monkeypatch.delenv`.
- `test_run_id_is_stable_when_supplied_and_appears_in_logs` in `tests/test_core.py` keeps
  passing: the new lines still contain the run id, `process started` and the step name.

## Acceptance criteria

`<d>` stands for any duration in the format of [Durations](#durations), that is matching
`\d+ms|\d+\.\d\ds|\d+m \d+s|\d+h \d+m \d+s`. Unless a criterion says otherwise, "the records"
are those the engine writes to `ingestio.core`, captured with `caplog` at `INFO`, and a line is
the message of a record.

### The tree

- **AT-001**: Given `Process([Step("extract", [a]), Step("load", [b, c])], run_id="a3f7c9")`,
  where `a`, `b` and `c` implement only `run` and are named `ReadSource`, `AddLineageColumns`
  and `WriteTarget`, when it runs, then the records are exactly these nine, in this order, all
  at `INFO`:

  ```text
  [a3f7c9] ┌ process started · 2 steps
  [a3f7c9] │ ┌ extract (1/2) · 1 callback
  [a3f7c9] │ │ ✔ ReadSource.run <d>
  [a3f7c9] │ └ extract done in <d>
  [a3f7c9] │ ┌ load (2/2) · 2 callbacks
  [a3f7c9] │ │ ✔ AddLineageColumns.run <d>
  [a3f7c9] │ │ ✔ WriteTarget.run <d>
  [a3f7c9] │ └ load done in <d>
  [a3f7c9] └ process finished in <d>
  ```

- **AT-002**: Given a step `load` holding two callbacks named `First` and `Second` that both
  implement `before`, `run` and `after`, when the process runs, then between the step's start
  and closing lines there are exactly six lines, `✔ First.before <d>`, `✔ Second.before <d>`,
  `✔ First.run <d>`, `✔ Second.run <d>`, `✔ First.after <d>`, `✔ Second.after <d>`, in this
  order, each prefixed with `[<run id>] │ │ `.
- **AT-003**: Given a callback that implements only `after`, when the process runs, then its
  only line is `[<run id>] │ │ ✔ <name>.after <d>`. Given a callback that implements no moment,
  when the process runs, then it has no line, and its step writes only its start and closing
  lines.
- **AT-004**: Given `Base(Callback)` implementing `run` and `Child(Base)` implementing `before`,
  when a process holding a `Child()` runs, then the step has exactly two callback lines,
  `✔ Child.before <d>` and `✔ Child.run <d>`, and none for `after`.
- **AT-005**: Given a callback whose `name` returns `strip_column_suffix` and that implements
  `run`, when the process runs, then its line is
  `[<run id>] │ │ ✔ strip_column_suffix.run <d>`.
- **AT-006**: Given `Process([Step("placeholder")], run_id="r1")`, when it runs, then the
  records are exactly `[r1] ┌ process started · 1 step`,
  `[r1] │ ┌ placeholder (1/1) · 0 callbacks`, `[r1] │ └ placeholder done in <d>` and
  `[r1] └ process finished in <d>`.

### Failure

- **AT-007**: Given the process of AT-001 where `WriteTarget.run` raises
  `PermissionError("denied")`, when it runs, then `run()` raises that error, the records start
  with the first six lines of AT-001 at `INFO`, and end with these three at `ERROR`:
  `[a3f7c9] │ │ ✖ WriteTarget.run failed after <d>: PermissionError: denied`,
  `[a3f7c9] │ └ load failed in <d>`, `[a3f7c9] └ process failed in <d>`. No line contains
  `process finished`.
- **AT-008**: Given a callback named `Check` implementing `before` and `run`, whose `before`
  raises `ValueError("bad")`, when the process runs, then `run()` raises it, the line
  `[<run id>] │ │ ✖ Check.before failed after <d>: ValueError: bad` is written at `ERROR`, no
  `Check.run` line is written, and the step's `failed in <d>` line and the process's
  `failed in <d>` line follow, at `ERROR`. Given instead a callback named `Check` that
  implements only `after`, which raises `ValueError("late")`, then
  `[<run id>] │ │ ✖ Check.after failed after <d>: ValueError: late` is written at `ERROR`,
  followed by the step's and the process's `failed in <d>` lines at `ERROR`.
- **AT-009**: Given a step `s` holding `a`, named `A` and implementing `run` and `after`, then
  `b`, named `B`, whose `run` raises `ValueError("boom")`, when the process runs, then the
  step's lines are, in order: its start line, `✔ A.run <d>`,
  `✖ B.run failed after <d>: ValueError: boom`, `✔ A.after <d>`, `│ └ s failed in <d>`.
- **AT-010**: Given a callback named `X` whose `run` raises an exception with an empty message,
  when the process runs, then its line ends with `failed after <d>: ValueError` for
  `ValueError()`, with `cancelled the step after <d>` for `CancelStepException()`, and with
  `cancelled the process after <d>` for `CancelProcessException()`, with nothing after these
  endings.
- **AT-011**: Given `run_id="r1"` and a callback named `X` whose `run` raises
  `ValueError("first line\nsecond line")`, when the process runs, then the message of its
  failure record is exactly
  `[r1] │ │ ✖ X.run failed after <d>: ValueError: first line\n[r1] │ │   second line`, and
  every line of every record starts with `[r1] `.
- **AT-012**: Given a callback whose `run` raises an exception `err`, when `run()` raises, then
  the exception raised is `err` itself, it has no `__notes__`, and no record the engine wrote
  during the run has `exc_info` set.

### Cancellation

- **AT-013**: Given `[Step("bronze", [first, stop]), Step("silver", [nxt])]`, where `first` is
  named `First` and implements `run` and `after`, `stop` is named `Stop` and its `run` raises
  `CancelStepException("nothing to ingest")`, and `nxt` is named `Next` and implements `run`,
  when the process runs, then the records are, in order and all at `INFO`: the process start
  line; the `bronze` start line; `│ │ ✔ First.run <d>`;
  `│ │ ⊘ Stop.run cancelled the step after <d>: nothing to ingest`; `│ │ ✔ First.after <d>`;
  `│ └ bronze cancelled in <d>`; the `silver` start line; `│ │ ✔ Next.run <d>`;
  `│ └ silver done in <d>`; `└ process finished in <d>`. Each is prefixed with `[<run id>] `.
- **AT-014**: Given `[Step("bronze", [stop]), Step("silver", [nxt])]`, where `stop` is named
  `Stop` and its `run` raises `CancelProcessException("source unchanged")`, when the process
  runs, then the records include
  `[<run id>] │ │ ⊘ Stop.run cancelled the process after <d>: source unchanged`, the last two
  are `[<run id>] │ └ bronze cancelled in <d>` and `[<run id>] └ process cancelled in <d>`, no
  line mentions `silver`, and no record is above `INFO`.
- **AT-015**: Given a callback named `X` implementing only `after`, which raises
  `CancelProcessException("stop")`, when the process runs, then `process.cancelled` is `True`,
  the line `[<run id>] │ │ ⊘ X.after cancelled the process after <d>: stop` is written, the
  last two records are the step's `cancelled in <d>` line and `└ process cancelled in <d>`,
  and no record is above `INFO`.
- **AT-016**: Given a callback named `X` implementing only `after`, which raises
  `CancelStepException("too late")`, when the process runs, then `run()` raises it, and the
  last three records, at `ERROR`, are
  `[<run id>] │ │ ✖ X.after failed after <d>: CancelStepException: too late`, the step's
  `failed in <d>` line and `[<run id>] └ process failed in <d>`.

### Durations

- **AT-017**: Given `time.perf_counter` replaced by a clock that only the callback advances, and
  a callback named `X` whose `run` advances it by the duration on the left, when a process
  holding only that callback runs, then its line is `✔ X.run` followed by the text on the
  right:

  | Advance (seconds) | Text |
  |---|---|
  | 0 | `0ms` |
  | 0.0004 | `0ms` |
  | 0.4125 | `412ms` |
  | 0.9995 | `999ms` |
  | 1 | `1.00s` |
  | 3.5289 | `3.52s` |
  | 59.9995 | `59.99s` |
  | 60 | `1m 0s` |
  | 133.9 | `2m 13s` |
  | 3723.5 | `1h 2m 3s` |

- **AT-018**: Given the same fake clock, a step `s1` holding a callback named `X` that advances
  it by 1 second in `before`, 2 in `run` and 3 in `after`, and a step `s2` holding a callback
  named `Y` that advances it by 0.5 seconds in `run`, when the process runs, then the lines
  include `✔ X.before 1.00s`, `✔ X.run 2.00s`, `✔ X.after 3.00s`, `│ └ s1 done in 6.00s`,
  `✔ Y.run 500ms`, `│ └ s2 done in 500ms` and `└ process finished in 6.50s`.

### Concurrent runs and injected loggers

- **AT-019**: Given a callback named `Outer`, in
  `Process([Step("outer_step", [outer])], run_id="out")`, whose `run` runs
  `Process([Step("inner_step", [y])], run_id="in")`, where `y` is named `Y` and implements
  `run`, when the outer process runs, then the lines starting with `[in] ` are exactly
  `[in] ┌ process started · 1 step`, `[in] │ ┌ inner_step (1/1) · 1 callback`,
  `[in] │ │ ✔ Y.run <d>`, `[in] │ └ inner_step done in <d>` and
  `[in] └ process finished in <d>`; they come after `[out] │ ┌ outer_step (1/1) · 1 callback`
  and before `[out] │ │ ✔ Outer.run <d>`; and the lines starting with `[out] `, taken alone,
  are exactly the outer process's five lines.
- **AT-020**: Given the process of AT-001 built with `log=logging.getLogger("my.app")`, when it
  runs, then the records of `my.app` are exactly the nine lines of AT-001, all at `INFO`, and
  no record is written to `ingestio.core`.

### The helper

- **AT-021**: `from ingestio import log_to_console` works, and `"log_to_console"` is in
  `ingestio.__all__`.
- **AT-022**: Given the root logger at its default `WARNING` level and
  `log_to_console(stream=s)`, with `s` an `io.StringIO`, when the process of AT-001 runs, then
  `s.getvalue()` is exactly the nine lines of AT-001, each followed by `\n`, with no time, level
  or logger name, and with no `\x1b` character.
- **AT-023**: Given `handler = log_to_console(stream=s)`, then `handler` is a `logging.Handler`
  and is in `logging.getLogger("ingestio").handlers`; after
  `logging.getLogger("ingestio").removeHandler(handler)`, a process run writes nothing to `s`.
- **AT-024**: Given `first = log_to_console(stream=a)` followed by
  `second = log_to_console(stream=b)`, then the handlers of the `ingestio` logger are those it
  had before the first call plus `second`, and `first` is not among them; when a process runs,
  `b` holds each line exactly once and `a` holds nothing.
- **AT-025**: Given a handler `h` that the application attached to the `ingestio` logger, and
  the root logger's handlers, the root logger's level and the `ingestio` logger's `propagate`
  noted before the call, when `log_to_console(stream=s)` is called, then `h` and the
  `NullHandler` registered by the package are still attached to the `ingestio` logger, the root
  logger's handlers and level are unchanged, `propagate` is unchanged, and a process run is
  still captured by `caplog`, which listens on the root logger.
- **AT-026**: Given the `ingestio` logger with no level of its own and the root logger at
  `WARNING`, when `log_to_console(stream=s)` is called, then the `ingestio` logger's level is
  `INFO`, the returned handler's level is `INFO`, and a process run writes its `INFO` lines to
  `s`. Given the `ingestio` logger at `DEBUG`, when `log_to_console(stream=s)` is called, then
  it stays at `DEBUG`. Given `log_to_console(stream=s, level=logging.ERROR)`, when the process
  of AT-007 runs, then `s` holds exactly its three `ERROR` lines.
- **AT-027**: Given `sys.stderr` replaced by an `io.StringIO` before `log_to_console()` is
  called with no `stream`, when a process runs, then its lines are written to that
  `io.StringIO`.
- **AT-028**: Given the case on each row, when `log_to_console(stream=..., color=...)` is called
  and a process runs, then the stream contains SGR sequences (`\x1b[`) exactly when the last
  column says so:

  | `color` | `stream.isatty()` | `NO_COLOR` | Colored |
  |---|---|---|---|
  | `None` | `False` | unset | no |
  | `None` | `True` | unset | yes |
  | `None` | `True` | `1` | no |
  | `None` | `True` | empty string | yes |
  | `False` | `True` | unset | no |
  | `True` | `False` | unset | yes |
  | `True` | `True` | `1` | yes |

- **AT-029**: Given `log_to_console(stream=s, color=True)` and `caplog` capturing, when a
  process runs, then removing every SGR sequence (`\x1b\[[0-9;]*m`) from `s.getvalue()` gives
  exactly the messages of the records the engine wrote during the run, each followed by `\n`,
  and on every line of `s` that contains an SGR sequence the last one is `\x1b[0m`.
- **AT-030**: Given `log_to_console(stream=s, color=True)` and a process whose first step holds
  a callback that succeeds, whose second step holds a callback that raises
  `CancelStepException`, and whose third step holds a callback that raises `ValueError`, when it
  runs, then in `s`:
  - every line starts with `\x1b[2m[`;
  - `┌` is immediately preceded by `\x1b[36m`, `✔` by `\x1b[32m`, `✖` by `\x1b[31m` and `⊘` by
    `\x1b[33m`; the `└` of `done` lines by `\x1b[32m`, of `failed` lines by `\x1b[31m`, and of
    `cancelled` lines by `\x1b[33m`;
  - on failure lines, `failed` is preceded by `\x1b[31m` with no other SGR sequence in between;
    on cancellation lines, `cancelled` is preceded by `\x1b[33m` in the same way;
  - on success, start and `done` lines, the text after the marker contains no SGR sequence
    other than resets.

  Given the same helper and a process that succeeds, when it runs, then the `└` of its
  `process finished` line is immediately preceded by `\x1b[32m`, and the text after it contains
  no SGR sequence other than resets.
- **AT-031**: Given `log_to_console(stream=s, color=True)` and `caplog` capturing, when a process
  runs, then no captured record's message contains `\x1b`.
- **AT-032**: Given `log_to_console(stream=s, color=True)`, when
  `logging.getLogger("ingestio.custom").warning("hello")` is called, then `s` receives exactly
  `hello\n`.

### Building a line never raises

- **AT-033**: Given an exception class `BadStr(Exception)` whose `__str__` raises
  `RuntimeError`, and a callback named `X` whose `run` raises an instance `err` of it, when the
  process runs, then `run()` raises `err` itself, and the records include, at `ERROR`,
  `[<run id>] │ │ ✖ X.run failed after <d>: BadStr`, with nothing after `BadStr`, followed by
  the step's and the process's `failed in <d>` lines.
- **AT-034**: Given a callback named `X` whose `run` raises an instance of a subclass of
  `CancelStepException` whose `__str__` raises `RuntimeError`, when the process runs, then
  `run()` returns the process without raising, the line
  `[<run id>] │ │ ⊘ X.run cancelled the step after <d>` ends right after the duration, the
  step's closing line is `cancelled in <d>` and the last record is
  `[<run id>] └ process finished in <d>`. Given instead a subclass of `CancelProcessException`
  with the same `__str__`, then `run()` returns the process without raising,
  `process.cancelled` is `True`, the line
  `[<run id>] │ │ ⊘ X.run cancelled the process after <d>` ends right after the duration, and
  the last record is `[<run id>] └ process cancelled in <d>`.
- **AT-035**: Given a callback of class `Unnamed`, implementing `run`, whose `name` property
  raises `RuntimeError`, when the process runs, then `run()` returns the process without
  raising, the callback's line is `[<run id>] │ │ ✔ Unnamed.run <d>`, and the last record is
  `[<run id>] └ process finished in <d>`.
- **AT-036**: Given a callback of class `Unnamed` whose `name` property raises `RuntimeError`
  and whose `run` raises `err = ValueError("boom")`, when the process runs, then `run()` raises
  `err` itself, and the records include, at `ERROR`,
  `[<run id>] │ │ ✖ Unnamed.run failed after <d>: ValueError: boom`.

### Helper arguments

- **AT-037**: Given the handlers and the level of the `ingestio` logger noted before the call,
  when `log_to_console(stream=s, level=logging.NOTSET)` is called, then it raises `ValueError`
  with the message `level must be above NOTSET`, and the `ingestio` logger's handlers and level
  are unchanged.

### Moments assigned on the instance

- **AT-038**: Given a callback named `X` whose class implements no moment and whose `__init__`
  sets `self.run = self._work`, when the process runs, then `_work` runs and the step has
  exactly one callback line, `[<run id>] │ │ ✔ X.run <d>`.

## Decisions

Taken by the maintainer before planning, recorded here and not reopened:

1. **Channel: `logging` plus an opt-in helper.** Every line goes through the standard `logging`
   module. ADR 0008 stands: never `print`, the run id on every line, an injectable logger, a
   `NullHandler` at the package root. The helper attaches a good-looking handler to the
   `ingestio` logger in one call; colors only when the stream is a terminal, never in files or
   pipes or when `NO_COLOR` is set. Applications that do not call it configure logging as they
   like.
2. **Granularity: step, then `callback.moment`.** One line per callback moment that ran, with
   its duration; moments a callback does not implement are left out; no header lines for the
   passes.
3. **Levels:** everything at `INFO`, failures at `ERROR`, cancellations at `INFO`.
4. **Only the engine writes lines** in this change. Callbacks do not add lines to the tree, and
   `ReadSource` and `WriteTarget` do not gain detail lines.
5. **Log messages are in English.**

Taken by the maintainer when reviewing the spec:

6. **The helper is called `log_to_console`.** The name says what it does and does not suggest
   that logging as a whole, or the root logger, is configured.
7. **The default stream is `sys.stderr`,** as it is when the helper is called: the convention
   of `logging.StreamHandler` and `basicConfig`, which keeps standard output free for data and
   pipes. `stream=` overrides it.
8. **Unicode glyphs in the message, with no ASCII fallback,** as in the approved mockup. UTF-8
   is the norm on Linux, macOS, Databricks and the Windows console. The documentation warns that
   a handler writing to a stream that is not UTF-8 makes `logging` report an encoding error for
   every line. The cancellation marker is `⊘` (U+2298), confirmed over `↷`, `–` and `○`.
9. **The helper only adds.** It touches only the `ingestio` logger: it adds its handler and
   lowers the logger's level when the records would not reach the handler otherwise. It never
   raises that level, never changes `propagate` and never touches the root logger, so `caplog`,
   files and log shippers on the root logger keep receiving the records. Duplicate console
   lines, when the application also prints the root logger, are accepted.
10. **The format details the mockup did not show are as specified:**
    - (a) a cancellation's reason is only on the line of the moment that raised it; the step
      and process closing lines say `cancelled in <D>`;
    - (b) durations from one second read `3.52s`, `2m 13s`, `1h 2m 3s`, and every unit is
      truncated;
    - (c) colors: dim prefix, cyan start, green success, red failure and yellow cancellation,
      for the marker and, on failure and cancellation lines, the text;
    - (d) text on several lines continues on lines that repeat the run id and the guides, as
      ADR 0008 requires.
11. **A process name is out of scope.** It changes `Process`'s signature; a separate spec will
    cover it.
12. **Only `log_to_console` is public.** The handler and formatter classes and the structured
    data the engine attaches to records stay internal and undocumented; exporting them, for
    `dictConfig` users or structured logging, would be a later spec.

Taken by this spec:

13. **The tree lives in the message; color is added on output.** Every handler, `caplog`, a
    file or `basicConfig` shows the tree as plain text. Messages never carry ANSI codes; only the
    helper's handler adds them, when it writes.
14. **A line depends only on its own depth.** No glyph depends on the lines that come next,
    such as a "last child" `└──` or a `├──`, and nothing is buffered: each line is written when
    its event happens. Interleaved runs stay readable, since every line carries its run id and
    the lines of one run id form that run's tree, and the door to streaming stays open: a run
    that never ends still shows each line as it happens. The counts on the start lines come from
    the steps and callbacks declared, which exist before the run, never from the data.
15. **The helper colors from structured data the engine attaches to the record**, never by
    parsing the message, which holds arbitrary text.
16. **One record per event, and every line starts with the run id,** continuation lines
    included (decision 10 (d)).
17. **A failure line holds the exception's type name and message, without a traceback.** No
    record carries `exc_info`, and the exception leaves `run()` unchanged, so whoever receives
    it decides how to report it; a traceback in the tree would print it twice whenever the
    caller or the interpreter prints it too.
18. **The outcome follows what the engine does with the exception, not its type alone.** A
    `CancelStepException` raised from `after` is a failure line, because the engine does not
    handle it there (ADR 0007).
19. **A moment gets a line unless the engine would call `Callback`'s own method for it,** found
    by ordinary attribute lookup, so inherited overrides count. The engine keeps calling every
    moment.
20. **Durations are measured with `time.perf_counter()`,** looked up at each measurement.
21. **The last call of the helper wins,** and it returns its handler so the application can
    remove it.
22. **Color is decided once per call**, from the `color` argument, then `NO_COLOR` as
    no-color.org defines it, then `isatty()`.
23. **No new dependency** (ADR 0002).
24. **The format leaves room for criticality.** A non-essential failure
    ([ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md)) can be a new
    marker and a new outcome, at its own level, without changing the lines defined here. This
    spec does not define it.

Taken by the maintainer when reviewing the spec, after validation. They are numbered after
decision 24 so that the numbers of the earlier decisions, which the History cites, do not
change:

25. **Building a line never raises.** When `str(exc)` raises an exception deriving from
    `Exception`, the failure line's `<detail>` is `<type>` alone and a cancellation line ends
    right after the duration, as with an empty message. When `callback.name` raises an
    exception deriving from `Exception`, the line uses `type(callback).__name__`. In both cases
    the run behaves exactly as if the line had been built normally: the original exception
    leaves `run()` untouched, and a run that would succeed still succeeds.
26. **`log_to_console` rejects a level that is not above `logging.NOTSET`** with
    `ValueError("level must be above NOTSET")`, before touching any logger. A logger set to
    `NOTSET` inherits its level from its parent, so lowering the `ingestio` logger to it would
    let the root logger's `WARNING` hide the `INFO` lines.

**ADRs.** No decision here changes the philosophy or closes the door to streaming, so none
needs a new ADR. ADR 0008 stands, but its Decision quotes a line in the old format
(`[a3f7c9] process started | 2 steps`) and does not mention the helper: it should be amended
once this spec is approved. That is the maintainer's call, not this stage's.

## Open questions

None.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Spec drafted from the maintainer's four decisions (logging plus an opt-in helper, step then `callback.moment`, INFO with failures at ERROR, only the engine writes). Pins the line format, which moments get a line, the duration format and `log_to_console`, with 32 acceptance criteria and 7 open questions. |
| 2026-10-22 | planning | draft | Revised with the maintainer's answers: the seven open questions became decisions 6 to 12 (`log_to_console`, `sys.stderr`, Unicode with no fallback and a UTF-8 warning, the helper only adds, format details, process name out of scope, only the function public). Acceptance criteria unchanged; the `⊘` cancellation marker is left to confirm. |
| 2026-10-22 | approval | approved | The maintainer confirmed the `⊘` cancellation marker and approved the spec explicitly. No open questions remain. |
| 2026-10-22 | tests | tested | Wrote 61 tests in `tests/test_run_progress_log.py`: every criterion AT-001 to AT-032, plus 7 for explicit rules of the Behaviour section (moment lookup, `KeyboardInterrupt`, two failing moments, a stream without `isatty`, color decided at call time, continuation colors). All fail for the missing feature: `log_to_console` is absent and the engine still writes the old format. No existing test changed. |
| 2026-10-22 | documentation | documented | Rewrote the Logging section of `docs/concepts/lifecycle.md` for the tree-shaped log and `log_to_console`, with the UTF-8 warning. Added the spec to `docs/index.md`, updated the architecture kb entry and, with the maintainer's approval, amended ADR 0008: new example line and the opt-in helper. |
| 2026-10-22 | implementation | implemented | Added `src/ingestio/console.py` with `log_to_console`, and rewrote the engine's logging in `core.py` as a tree with a line per process, step and callback moment, with outcome and duration. Failures go to ERROR and the closing lines are written before the exception leaves `run()`. The helper colors from internal record data, so messages stay plain text. 94 tests pass; ruff and mypy are clean. |
| 2026-10-22 | validation | draft | Failed. The criteria, the suite (94), ruff, mypy and the lock pass, and the lifecycle matches HEAD in 32 scenario runs. Returned to planning, the earliest stage named: when `str(exc)` or `callback.name` raises, building the line replaces the callback's exception or fails a run that used to succeed, which the spec does not settle (maintainer decision). Also for the implementer: a moment assigned on the instance runs without a line, against decision 19. Also noted: `log_to_console(level=logging.NOTSET)` sets the `ingestio` logger to NOTSET, which inherits WARNING from the root. |
| 2026-10-22 | planning | draft | Revised for validation's findings with the maintainer's decisions 25 and 26: building a line never raises (fallback to the type alone or the class name), `log_to_console` rejects a level not above NOTSET, and a moment assigned on the instance gets a line. New criteria AT-033 to AT-038; AT-001 to AT-032 unchanged. |
| 2026-10-22 | approval | approved | The maintainer pre-approved a revision limited to those three points; the main session checked the spec and it is limited to them. |
| 2026-10-22 | tests | tested | Added 7 tests for AT-033 to AT-038 and a guard that the default stream is `sys.stderr` as it is at the call. The 7 fail for the missing behaviour: a raising `__str__` or `name` replaces the exception or fails the run, there is no `ValueError` for NOTSET, and a moment assigned on the instance gets no line. The guard passes. No existing test changed. |
| 2026-10-22 | documentation | documented | `docs/concepts/lifecycle.md` and the architecture kb entry now cover decisions 25 and 26: the `ValueError` for a level not above NOTSET, that building a line never raises (type alone, or the class name), and the line for a moment assigned on the instance. |
| 2026-10-22 | implementation | implemented | A moment assigned on the instance now gets a line. Building a line never raises: a raising `str(exc)` or `name` falls back to the type alone or the class name. `log_to_console` rejects a level not above NOTSET, and `_ConsoleFormatter` documents its `color` argument. 102 tests pass; ruff and mypy are clean. |
| 2026-10-22 | validation | validated | Passed. All 38 criteria (69 tests), the suite (102), ruff, mypy and the lock pass. R1, R2 and NOTSET are fixed: on every path the callback's exception leaves `run()` as the same object, with no fallback error in its chain, and the lifecycle matches HEAD in 1208 scenario runs. |
