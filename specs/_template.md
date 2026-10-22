# Spec: <title>

- **Status:** draft
- **Slug:** `<slug>`. The spec is `specs/<slug>.md` and its tests are `tests/test_<slug>.py`.
- **Related:** ADRs, other specs

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

What the change does, in two or three sentences.

## Motivation

Why the change is needed.

## Fit with the philosophy

Which concept the change belongs to, connector or callback, and how it respects the principles
of [docs/philosophy.md](../docs/philosophy.md). A change that fits neither concept stops here.

## Scope

In scope:

-

Out of scope:

-

## Public API

The names, signatures and import paths users will see. Write "None" when the public API does
not change.

## Behaviour

How the feature behaves, including errors and edge cases.

## Acceptance criteria

Each criterion is testable on its own. Its tests are named after it: `AT-001` is covered by
`test_at001_<description>` in `tests/test_<slug>.py`.

- **AT-001**: Given ..., when ..., then ....

## Decisions

The design decisions this spec takes. Flag any that changes the philosophy or closes the door
to streaming: it needs an ADR.

## Open questions

Questions for the maintainer. All of them are answered before the spec is approved.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| YYYY-MM-DD | planning | draft | |
