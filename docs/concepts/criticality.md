# Criticality

> **Not implemented yet.** This page states the target set by the
> [philosophy](../philosophy.md#callback-criticality) and recorded in
> [ADR 0011](../decisions/0011-callbacks-as-stages-with-criticality.md). A spec will settle
> how it works before any code changes.

## The target

- A callback is either essential or non-essential.
- If an essential callback fails, the pipeline fails. If a non-essential one fails, the
  pipeline continues.
- Whoever writes the callback decides its criticality. Without an explicit declaration, a
  callback is essential.
- The library's default callbacks, append-only and SCD2, are essential.
- A non-essential failure is never silent: it is recorded in the result of the run.

## Today

Every exception other than `CancelStepException` and `CancelProcessException` propagates out
of `Process.run()` and stops the run, after the `after` pass of the current step. Every
callback therefore behaves as essential, and there is no way to declare one non-essential.

`Process.run()` returns the process itself. There is no run result yet in which a tolerated
failure could be recorded.

## Open questions for the spec

- How a callback declares its criticality.
- Which moments a non-essential declaration covers: `before`, `run`, `after`, or all three.
- What happens to the rest of the step after a non-essential callback fails, and whether that
  callback's `after` still runs.
- Where the result of a run lives, what it records about a tolerated failure, and how it
  relates to `Process.run()` returning the process.
- How the cancel exceptions behave when a non-essential callback raises them.
