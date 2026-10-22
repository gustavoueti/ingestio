# 0011. Treat callbacks as stages with a criticality

- **Status:** accepted; not implemented yet
- **Recorded:** 2026-10-22
- **Evidence:** decided by the maintainer and stated in the
  [philosophy](../philosophy.md#callback-criticality)

## Context

In version 0.0.1, a callback is a set of lifecycle hooks, `before`, `run` and `after`, that do
nothing by default. Every exception other than the two cancel exceptions propagates and stops
the run, so every callback behaves as essential and none can be declared otherwise.
`Process.run()` returns the process; there is no run result that records what failed.

## Decision

- A callback is a stage of the pipeline that performs an action.
- A stage is essential or non-essential. An essential failure fails the pipeline; a
  non-essential failure lets it continue.
- The author of a callback decides its criticality. Without a declaration, a callback is
  essential.
- The library's default callbacks, append-only and SCD2, are essential.
- A non-essential failure is recorded in the result of the run, never swallowed.

## Rationale

> TODO(maintainer): why callbacks move from hooks to stages with a criticality, and why the
> default is essential.

## Consequences

- A spec has to settle, before any code changes:
  - how a callback declares its criticality;
  - whether the `before`, `run` and `after` moments stay ([ADR 0006](0006-three-pass-step.md)),
    and which of them a non-essential declaration covers;
  - what happens to the rest of a step after a non-essential failure;
  - where the run result lives and how it relates to `Process.run()` returning the process;
  - how the cancel exceptions ([ADR 0007](0007-cancellation-exceptions.md)) behave in a
    non-essential callback.
- The run result is a change to the public API.
- The "Controlling failure" section of `README.md` will have to change.
