# Design philosophy

This is the canonical statement of how ingestio is designed. It applies to every contributor
and every agent, in every task, and it sits at the top of the
[hierarchy of truth](../AGENTS.md#hierarchy-of-truth): when another document, a spec or a
knowledge-base entry disagrees with it, this document wins. Only an
[architecture decision record](decisions/) can change it.

Parts of it describe where the library is going rather than where it is. The
[concept pages](index.md#concepts) say what is implemented today.

## Two concepts

ingestio is an ingestion library with two central concepts. Every implementation must fit one
of them. If something does not fit, stop and report it instead of creating a third concept.

### Connector

An integration with an external system: MongoDB, PostgreSQL, Salesforce, BigQuery, a file
system. A connector declares what it can do through the `Reader` and/or `Writer`
capabilities, which are protocols, with no class hierarchy. A connector only knows how to talk
to its system; it does not decide write strategy, format or architecture.

### Callback

A stage of the pipeline that performs an action: formatting, applying medallion layers, CDC
replication, write strategies (append, merge, SCD2). Callbacks generic enough to serve many
pipelines live in `ingestio.callback` and are part of the library; append-only and SCD2 will
be its default callbacks. Every other callback is written by whoever uses the library.

A default callback is one the library ships. A process uses it the way it uses any other
callback, by placing it in a step; the library never adds one on its own.

### Vocabulary

- **Stage**: one callback, the unit of work of a pipeline.
- **Process** (`Process`): the pipeline, an ordered list of steps run together.
- **Step** (`Step`): a named point of the process that holds the callbacks running there.

`Process` and `Step` are the engine that composes the two concepts. They are not a third
concept: nobody extends them to integrate a system or to add an action.

## Callback criticality

- A stage is either essential or non-essential. If an essential stage fails, the pipeline
  fails. If a non-essential stage fails, the pipeline continues.
- Whoever writes the callback decides its criticality. Without an explicit declaration, a
  callback is essential: tolerating failure is always an explicit choice.
- The library's default callbacks, append-only and SCD2, are essential. Their successful
  execution is a condition for the pipeline to succeed.
- A non-essential stage never fails silently: its failure is recorded in the result of the
  run.

## Principles

- **Simplicity.** The smallest abstraction that solves the problem. No internal frameworks,
  registries or magic.
- **The user's autonomy.** The library composes; it does not impose. A developer creates their
  own callback in their own code, for example by importing `Callback` in their `main.py`, and
  defines there whatever behaviour they want, criticality included. Extending connectors and
  callbacks must be as simple as possible: no registries, no extra configuration.
- **A core without dependencies.** Integrations with external systems are optional extras,
  such as `ingestio[spark]`.
- **Streaming is a future direction.** It is not implemented now, but every decision that
  closes the door to streaming must be recorded in an ADR.
