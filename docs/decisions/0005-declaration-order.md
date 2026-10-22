# 0005. Run steps and callbacks in the order they are declared

- **Status:** accepted
- **Recorded:** 2026-10-22
- **Evidence:** `src/ingestio/core.py`, `tests/test_core.py`
  (`test_steps_and_callbacks_run_in_declaration_order`), `README.md`

## Context

A process combines many callbacks across many steps, and something has to decide in which
order they run.

## Decision

Steps run in list order, and callbacks run in list order inside each pass of a step. There is
no priority attribute and no reordering.

## Rationale

"There is no priority attribute and no hidden reordering: the sequence you write is the
sequence that runs." (`README.md`)

## Consequences

- To change the order, the user changes the list.
- Step names must be unique, and a process needs at least one step, which `Process` checks
  before anything runs.
