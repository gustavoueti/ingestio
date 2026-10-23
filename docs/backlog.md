# Backlog

Ideas the maintainer has had and wants implemented, recorded before anyone works on them. An
idea here is not a decision: nothing in this file is approved, and nothing here overrides the
[philosophy](philosophy.md), a spec or an ADR. Each idea lists the questions the maintainer
must answer before it can be worked on.

An idea that changes `src/` goes to the planner and becomes a spec, as described in
[workflow.md](workflow.md). Its entry then links to that spec, and the entry is removed when
the work lands.

## 1. The planner reports its progress every five minutes

**Idea.** While it works, the planner reports its progress every five minutes. The report
gives an estimate of the total effort and the percentage of that effort already done.

**Where it goes.** `.claude/agents/planner.md`, which is outside the repository. No `src/`
change and no spec. If the rule ends up applying to every agent, it also goes into
[workflow.md](workflow.md).

**Open questions.**

- Does this apply only to the planner, or to every agent in the flow?
- Who sees the report, and where? A subagent's output reaches the main session only when the
  subagent finishes, so it cannot talk to the maintainer while it runs. One option is for the
  planner to write its progress to a file the maintainer can follow. Its write scope is
  `specs/` only, so that file would live there or the scope would have to grow.
- How does the planner measure five minutes? It has no clock and no Bash. The options are a
  hook that tracks elapsed time and reminds it, or reporting at milestones (each section of
  the spec) instead of on a timer.
- What is the unit of effort behind the percentage: time, sections of the spec template, or
  something else?

## 2. A suggested job layout in the README

**Idea.** The README suggests how a job uses the library. A job needs at least two files:

- `config.json`: the job's configuration;
- `main.py`: it imports `ingestio`, imports the callbacks the library ships, and defines the
  job's own new callbacks. It reads `config.json` itself and passes the values to the
  constructors and methods of `ingestio`.

The README stresses that the library runs anywhere: those two files are the minimum
configuration, and the connectors built on Spark assume a Spark session is already running in
the environment. That environment can be a container, Databricks or a local machine: anywhere
that has Spark.

**Decided.**

- The library never reads `config.json`; `main.py` does. The layout is a convention the
  README suggests, not something the library requires, so it adds no public API and needs no
  spec of its own. It does not wait for the later spec on configuration and recipes either:
  the README can show `main.py` loading the file with `json`.
- For now, only the connectors built on Spark assume a running Spark session. The rest of the
  library stays plain Python, as `JsonApiReader` and `JsonLinesWriter` are today, so *no
  dependencies in the core* holds and no ADR is needed.
- The example `config.json` reuses the keys of
  [postgres_connectors.md](../specs/postgres_connectors.md): `table_name`, `primary_key` and
  `watermark_column`.

**Where it goes.** [README.md](../README.md), and possibly a concept page in `docs/`.

**Open questions.**

- When does the README change land? No connector built on Spark exists yet: the PostgreSQL
  connectors are a draft spec. The README can wait for them, or show the layout now with the
  current connectors and add the Spark part when they land.
