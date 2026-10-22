# Spec: SCD2 write callback

- **Status:** draft
- **Slug:** `scd2_callback`. The spec is `specs/scd2_callback.md` and its tests are
  `tests/test_scd2_callback.py`.
- **Related:** [ADR 0003](../docs/decisions/0003-connectors-as-protocols.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [specs/append_only_callback.md](append_only_callback.md)

## Summary

A default callback that keeps the history of a dimension as a slowly changing dimension of
type 2: when a tracked attribute of an entity changes, the current version of the entity is
closed and a new version is added, so that every past state stays queryable.

## Motivation

The philosophy names SCD2 as one of the library's two default callbacks.

## Fit with the philosophy

- **Concept:** callback. SCD2 is a write strategy. The connectors only read the current
  versions from the target and write the changes to it.
- **Criticality:** essential, as a default callback of the library. Its failure fails the
  pipeline.
- **Autonomy:** it is used by placing it in a step. Column names are the user's choice.
- **No dependencies:** a generic version lives in the core; versions that delegate to an
  engine, such as a `MERGE` on Delta, would be extras and specs of their own.

## Scope

In scope:

- detecting new, changed and unchanged entities in a batch;
- closing the current version and adding a new one when an entity changes;
- the version columns and how they are named;
- the connector capabilities the callback needs.

Out of scope:

- engine-specific implementations;
- SCD types other than 2;
- late-arriving or out-of-order data, unless question 6 brings it in.

## Public API

To be decided; see the open questions. A starting proposal:

```python
from ingestio.callback.all import SCD2

SCD2(source, target, *, keys, tracked=None, key="records")
```

## Behaviour

- An entity is identified by its business key, the values of `keys`.
- A current version is the one with an open validity interval.
- When the incoming record of an entity differs from its current version in a tracked
  attribute, the current version is closed at the effective time and a new current version
  starts at that time.
- When nothing tracked has changed, nothing is written for that entity.
- Closed versions never change.

## Acceptance criteria

Tentative until the open questions are answered. "Effective time" means the time decided by
question 3.

- **AT-001**: Given an entity absent from the target, when the process runs, then the target
  holds one current version of it, valid from the effective time and with an open end.
- **AT-002**: Given an entity whose tracked attribute changed, when the process runs, then its
  previous version is closed at the effective time and a new current version starts at that
  time.
- **AT-003**: Given an entity with no tracked change, when the process runs, then the target
  is unchanged for that entity.
- **AT-004**: Given the same batch processed twice, when the second run ends, then the target
  is the same as after the first.
- **AT-005**: Given closed versions in the target, when the process runs, then none of them
  changes.
- **AT-006**: Given a target whose write fails, when the process runs, then the error
  propagates out of `Process.run()`.
- **AT-007**: Given custom names for the version columns, when the process runs, then the
  target uses those names.
- **AT-008**: Given two records with the same business key in one batch, when the process
  runs, then it fails before writing anything. Depends on question 7.
- **AT-009**: Given an entity current in the target but absent from the batch, when the
  process runs, then it behaves as question 5 decides.

## Decisions

None yet. Question 9 may close the door to streaming: if it does, that needs an ADR.

## Open questions

1. **Which connector capabilities does SCD2 need?** It has to read the current versions and
   both add rows and close existing ones. Options:
   - a `Reader` for the current versions and a `Writer` that replaces the whole target, which
     only scales for small dimensions;
   - a new capability for updates or merges that connectors implement;
   - delegating the whole operation to an engine, through an extra.
2. **Which columns are tracked?** All except the keys and the version columns, or an explicit
   list. Do changes in untracked columns overwrite the current version (SCD1), or are they
   ignored?
3. **Where does the effective time come from?** `process.started_at`, a column of the record,
   or an argument of the callback.
4. **What are the version columns, their default names, and the open end?** For example
   `valid_from`, `valid_to`, `is_current`; is the open end `None` or a far-future date?
5. **What does an entity absent from the batch mean?** Under snapshot semantics it was deleted
   and its current version is closed; under incremental semantics it stays as it is. One mode,
   or an option?
6. **Are late-arriving records in scope?** A record with an effective time earlier than the
   current version's.
7. **Is a duplicate business key in a batch an error,** or does the last record win?
8. **How are changes detected?** Comparing the tracked values directly, or a hash of them
   stored in a column.
9. **Does the callback need the whole batch at once?** Comparing against current versions may
   require materialising the batch, unlike append-only.
10. **What are the name and the module of the callback?**

## History

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Drafted by the main session while setting up the workflow, as a first real case for the template. Not yet reviewed by the planner agent. |
