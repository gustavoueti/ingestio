# 0004. Share data between callbacks through the process state

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/core.py` (`Process.state`), `README.md`

## Context

Callbacks of a process need to hand data to each other.

## Decision

Callbacks read and write a dictionary shared by the whole process, `process.state`, instead of
receiving the data as an argument and returning it.

## Rationale

"Callbacks exchange data through `process.state` instead of passing a payload down a chain, so
a callback that only observes does not have to hand the data back." (`README.md`)

## Consequences

- Keys are conventions between callbacks: `ReadSource`, `AppendOnly` and `FullLoad` use
  `"records"` by default and take the key as an option.
- A callback can depend on a key another callback must have written; nothing checks that
  before the run.
