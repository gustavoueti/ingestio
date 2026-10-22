# 0008. Log through the standard library, with the run id on every line

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/core.py`, `src/ingestio/__init__.py`, `README.md`

## Context

A run reports its progress as it goes, and several ingestions may run on the same cluster.

## Decision

- The library logs with the standard `logging` module, never with `print`.
- Every line starts with the run id: `[a3f7c9] process started | 2 steps`.
- `Process` accepts the logger to use; it defaults to the module logger.
- The package root registers a `NullHandler`.

## Rationale

"Standard `logging`, never `print`, and every record carries the run id — so concurrent
ingestions on the same cluster stay readable." (`README.md`)

> TODO(maintainer): why the package registers a `NullHandler` and why the logger is
> injectable.

## Consequences

An application sees nothing from ingestio until it configures logging.
