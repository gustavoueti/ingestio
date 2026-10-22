# 0006. Run a step in three passes: before, run, after

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/core.py` (`Process._run_step`), `src/ingestio/callback/core.py`,
  `README.md`

## Context

Some callbacks need to act around the work of the whole step, for setup or cleanup, and not
only around their own work.

## Decision

A callback has three moments, `before`, `run` and `after`, which do nothing by default. A step
runs every `before`, then every `run`, then every `after`. The `after` pass runs whether the
step completed, was cancelled, or the process is stopping.

## Rationale

- "A step runs every `before`, then every `run`, then every `after`, so a callback gets a real
  boundary around the whole step while keeping list order within each pass." (`README.md`)
- "`after` runs in every case, so cleanup stays reliable." (`README.md`)

## Consequences

- [ADR 0011](0011-callbacks-as-stages-with-criticality.md) redefines callbacks as stages with
  a criticality. Whether the three moments stay is one of the questions its spec has to
  settle.
- What happens when an `after` raises is not specified yet
  ([Lifecycle](../concepts/lifecycle.md#behaviour-not-specified-yet)).
