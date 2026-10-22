# 0007. Cancel a step or a process by raising an exception

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/callback/core.py`, `src/ingestio/core.py`, `pyproject.toml`
  (ignore of ruff rule `N818`), `README.md`

## Context

A callback sometimes needs to end its step or the whole process early without the run counting
as a crash, for example when there is nothing to ingest.

## Decision

- `CancelStepException` skips the rest of the current step; the process carries on with the
  next one.
- `CancelProcessException` stops the process cleanly and sets `process.cancelled`.
- Any other exception propagates and fails the run.
- The two cancel exceptions do not end in `Error`.

## Rationale

- "The library gives you the vocabulary; the policy is yours." (`README.md`)
- On the names: "N818 wants an `Error` suffix. The cancel exceptions are control flow, not
  failures." (`pyproject.toml`)

> TODO(maintainer): why exceptions rather than, for example, a return value or a method on the
> process.

## Consequences

- A `CancelStepException` raised from `after` is not handled, because `after` runs outside
  the block that catches it, so it fails the run. A `CancelProcessException` raised from
  `after` still stops the process cleanly.
- How cancellation interacts with non-essential callbacks is an open question of
  [ADR 0011](0011-callbacks-as-stages-with-criticality.md).
