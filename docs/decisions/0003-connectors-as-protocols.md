# 0003. Define connectors as Reader and Writer protocols

- **Status:** accepted, amended by [0015](0015-choose-connectors-from-the-job-configuration.md)
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/connector/core.py`, `src/ingestio/callback/core.py`; listed by
  the maintainer in the [philosophy](../philosophy.md#connector). Amended on 2026-10-23 by
  [ADR 0015](0015-choose-connectors-from-the-job-configuration.md): the library's read and
  write callbacks build their connector from the job's configuration.

## Context

A connector may read from a system, write to it, or both. Callbacks need a type to accept
connectors with.

## Decision

- A connector's capabilities are two `typing.Protocol` classes, generic over the payload:
  `Reader[T_co]` with `read() -> T_co`, and `Writer[T_contra]` with `write(data) -> None`.
- Connectors do not inherit from anything: a class with the right method is a connector.
- The library's read and write callbacks build their connector from the job's configuration
  ([ADR 0015](0015-choose-connectors-from-the-job-configuration.md)); a job's own callback may
  build a shipped connector itself.
- Callbacks, on the other hand, are extended by subclassing `Callback`, which also gives them
  the `process` attribute and the three lifecycle moments.

## Rationale

- Injection, before ADR 0015: "A connector is injected into the callback that needs it, so a
  single callback may combine several. The payload type is unconstrained."
  (`connector/core.py`)
- The philosophy requires connectors without a class hierarchy, and shows a callback created
  "by importing `Callback` in their `main.py`".

> TODO(maintainer): why protocols rather than abstract base classes for connectors, and why
> callbacks are the exception that uses inheritance.

## Consequences

- The protocols are not `runtime_checkable`: nothing checks a connector at runtime; the type
  checker does.
- Users extend callbacks, by subclassing. Connectors are extended in the library, where the
  protocols are the contract every shipped connector satisfies.
- The payload type stays open, which keeps iterators, and with them streaming, possible.
