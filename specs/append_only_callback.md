# Spec: append-only write callback

- **Status:** draft
- **Slug:** `append_only_callback`. The spec is `specs/append_only_callback.md` and its tests
  are `tests/test_append_only_callback.py`.
- **Related:** [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md)

## Summary

A default callback that writes the records held in the process state to a target by adding
them after what the target already holds, never changing or removing existing data.

## Motivation

The philosophy names append-only as one of the library's two default callbacks. Today the
only write strategy lives in a connector: `JsonLinesWriter(append=True)`, which the philosophy
does not allow.

## Fit with the philosophy

- **Concept:** callback. Append-only is a write strategy, and write strategies are callbacks.
  The connector only talks to the target.
- **Criticality:** essential, as a default callback of the library. Its failure fails the
  pipeline.
- **Autonomy:** it is used by placing it in a step, like any other callback. The library never
  adds it on its own.
- **No dependencies:** the callback is in the core and needs none.

## Scope

In scope:

- the callback, its options and its errors;
- the connector capability the callback needs in order to append;
- what happens to `JsonLinesWriter(append=...)`.

Out of scope:

- deduplicating records or making a re-run idempotent;
- lineage columns, which are a callback of their own;
- the criticality mechanism itself, which has its own spec under
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md).

## Public API

To be decided; see the open questions. A starting proposal:

```python
from ingestio.callback.all import AppendOnly

AppendOnly(target, *, key="records")
```

## Behaviour

- In its `run` moment, the callback takes `state[key]` and appends every record to the target.
- Records already in the target stay as they are, in the same order.
- A failure while writing propagates and fails the run.

## Acceptance criteria

Tentative until the open questions are answered.

- **AT-001**: Given a target that already holds records and new records in `state["records"]`,
  when the process runs, then the target holds the existing records unchanged, followed by the
  new ones in their original order.
- **AT-002**: Given `key="raw"`, when the process runs, then the callback appends the records
  of `state["raw"]`.
- **AT-003**: Given a target whose append fails, when the process runs, then the error
  propagates out of `Process.run()`.
- **AT-004**: Given the records as a generator, when the process runs, then every record is
  appended and the generator is consumed once, without building a list first.
- **AT-005**: Given a connector written in user code that offers the append capability, with
  no registration and no subclassing, when it is used as the target, then the records are
  appended.
- **AT-006**: Given an empty list of records, when the process runs, then the target is left
  as it was. Depends on question 4.
- **AT-007**: Given no `state["records"]`, when the process runs, then it fails with an error
  that names the missing key. Depends on question 5.

## Decisions

None yet. AT-004 keeps the door to streaming open: if it is dropped, that needs an ADR.

## Open questions

1. **How does the callback append through a connector?** `Writer.write(data)` has no notion of
   a mode. Options:
   - a new capability protocol, such as `Appender` with `append(data)`, that connectors
     implement next to `Reader` and `Writer`;
   - a contract that `write` always appends, which `JsonLinesWriter`'s current replace mode
     breaks;
   - read the target with a `Reader`, then write everything back with a `Writer`, which does
     not scale beyond files.
2. **What happens to `JsonLinesWriter(append=...)`?** Remove it, deprecate it, or keep it as
   it is. Either change touches the public API.
3. **What are the name and the module of the callback?** `AppendOnly`, `Append`,
   `AppendWrite`; next to `WriteTarget` in `callback/io.py`, or in a module of its own.
4. **Does an empty batch call the connector?** Calling it with nothing may still create an
   empty file or table.
5. **Is a missing state key an error?** `WriteTarget` raises `KeyError` today. Should this
   callback raise the same error, a clearer one, or treat it as an empty batch?
6. **Does the callback leave anything in the state,** such as the number of records written?

## History

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Drafted by the main session while setting up the workflow, as a first real case for the template. Not yet reviewed by the planner agent. |
