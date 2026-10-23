# 0002. Keep the core free of dependencies

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `pyproject.toml` (`dependencies = []`), `src/ingestio/connector/http.py`; listed
  by the maintainer as a principle of the [philosophy](../philosophy.md#principles)

## Context

ingestio integrates with external systems, and most of them have client libraries of their
own.

## Decision

- `dependencies` stays empty.
- An integration that needs a third-party package declares it as an optional extra,
  `ingestio[<name>]`, added only through an approved spec.
- What the standard library can do, it does: `JsonApiReader` is built on `urllib`.

## Rationale

- "Engine dependencies, when they arrive, go in extras: the core stays installable
  anywhere." (`pyproject.toml`)
- `JsonApiReader` is "built on `urllib` so the package keeps no runtime dependencies."
  (`connector/http.py`)

## Consequences

- `JsonApiReader` does not support endpoints that require authentication.
- The pattern for extras is defined by
  [specs/postgres_connectors.md](../../specs/postgres_connectors.md), with the first extra,
  `ingestio[postgres]`: the extra is named after the system it integrates; third-party
  packages are imported only inside the functions that use them, so the package imports without
  any extra; a connector whose extra is missing raises `ImportError` naming it when it is
  built; an engine the runtime provides, such as pyspark, is a documented prerequisite and
  never part of an extra; the `dev` group holds the packages of every extra. See
  [Extras](../concepts/connectors.md#extras).
