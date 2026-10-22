# 0012. Defer streaming, and record what affects it

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** decided by the maintainer and stated in the
  [philosophy](../philosophy.md#principles)

## Context

The README promises that ingestio "does not assume ... that your processing is batch". Version
0.0.1 implements batch runs only.

## Decision

- Streaming is a future direction and is not implemented now.
- Any decision that closes the door to streaming is recorded in an ADR.

## Rationale

> TODO(maintainer): why streaming is deferred.

## Consequences

What in version 0.0.1 leans towards batch:

- `Reader.read()` returns one payload per call.
- A step finishes all its passes before the next step starts.
- `JsonApiReader` loads the whole response body into memory.

What keeps the door open:

- The payload type is unconstrained ([ADR 0003](0003-connectors-as-protocols.md)), so a reader
  may return an iterator.
- `JsonLinesWriter` accepts any iterable of records, so a generator stored in the state is
  consumed lazily when written.
