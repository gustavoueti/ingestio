# Documentation index

The map of every document in the repository. Agents start every task here.

## Start here

| Document | What it is |
|---|---|
| [philosophy.md](philosophy.md) | The canonical design rules. Top of the hierarchy of truth. |
| [workflow.md](workflow.md) | The development flow every change to `src/` goes through: stages, agents, spec statuses, returns. |
| [../AGENTS.md](../AGENTS.md) | Instructions for AI agents: constraints, hierarchy of truth, how to finish a task. |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Prescriptive standards for code, tests, commits and pull requests. |
| [../README.md](../README.md) | What ingestio is, installation and a first example. |
| [backlog.md](backlog.md) | Ideas the maintainer wants implemented, before they become specs. |

## Concepts

How the library works and how it is used.

| Page | Covers |
|---|---|
| [concepts/connectors.md](concepts/connectors.md) | `Reader` and `Writer`, using a connector, `JsonApiReader`, `JsonLinesWriter`, the PostgreSQL connectors, `DiskReader`, `DiskWriter`, `S3Reader`, `S3Writer`, `GcsReader`, `GcsWriter`, `VolumeReader`, `VolumeWriter`, `UnityCatalogReader`, `UnityCatalogWriter`, `BigQueryReader`, `BigQueryWriter`, extras. |
| [concepts/configuration.md](concepts/configuration.md) | The job's JSON configuration file: the `source` and `target` sections, the `system` names, the keys of each system, the callback arguments, append and replace (the callback chooses), the `scd2` section, errors. |
| [concepts/callbacks.md](concepts/callbacks.md) | `Callback`, sharing data through the state, `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2` (they build their connector from the configuration), the SCD2 checks, target systems and limitations, cancellation. |
| [concepts/criticality.md](concepts/criticality.md) | Essential and non-essential callbacks. Not implemented yet. |
| [concepts/lifecycle.md](concepts/lifecycle.md) | Building and running a process, the three passes of a step, failure, logging. |

## Decisions

Architecture decision records, in [decisions/](decisions/), written from
[the template](decisions/_template.md).

| ADR | Decision | Status |
|---|---|---|
| [0001](decisions/0001-src-layout-uv-hatchling.md) | Package with a src layout, hatchling and uv | accepted |
| [0002](decisions/0002-zero-dependency-core.md) | Keep the core free of dependencies | accepted |
| [0003](decisions/0003-connectors-as-protocols.md) | Define connectors as Reader and Writer protocols | accepted, amended by 0015 |
| [0004](decisions/0004-shared-process-state.md) | Share data between callbacks through the process state | accepted |
| [0005](decisions/0005-declaration-order.md) | Run steps and callbacks in the order they are declared | accepted |
| [0006](decisions/0006-three-pass-step.md) | Run a step in three passes: before, run, after | accepted |
| [0007](decisions/0007-cancellation-exceptions.md) | Cancel a step or a process by raising an exception | accepted |
| [0008](decisions/0008-stdlib-logging-run-id.md) | Log through the standard library, with the run id on every line | accepted |
| [0009](decisions/0009-typed-package.md) | Ship a typed package and check it with mypy in strict mode | accepted |
| [0010](decisions/0010-release-automation.md) | Release from the commit history on main | accepted |
| [0011](decisions/0011-callbacks-as-stages-with-criticality.md) | Treat callbacks as stages with a criticality | accepted, not implemented |
| [0012](decisions/0012-streaming-deferred.md) | Defer streaming, and record what affects it | accepted |
| [0013](decisions/0013-connector-configuration-convention.md) | Give a connector an ingestion's parameters as constructor arguments | accepted, amended by 0015 |
| [0014](decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md) | Create a missing target only in the form the system derives from the data | accepted |
| [0015](decisions/0015-choose-connectors-from-the-job-configuration.md) | Choose the reader and the writer from the job's configuration file | accepted, amended by 0016, 0017 |
| [0016](decisions/0016-make-each-write-strategy-one-callback.md) | Make each write strategy one callback | accepted |
| [0017](decisions/0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md) | Read a library callback's own parameters from a section named after it | accepted |

## Specs

One spec per change to `src/`, in [`specs/`](../specs/), written from
[the template](../specs/_template.md). Each spec carries its own status.

| Spec | Covers |
|---|---|
| [append_only_callback.md](../specs/append_only_callback.md) | Write strategies as callbacks: `AppendOnly` and `FullLoad` replace `WriteTarget`, and `JsonLinesWriter` gets `write(data, *, replace=False)`. |
| [scd2_callback.md](../specs/scd2_callback.md) | `SCD2`, the callback that keeps the history of a dimension: the top-level `scd2` section of the configuration, an atomic rewrite on `unity_catalog`, `postgres` and `bigquery`, four checks before the write, and the opt-in tests that need Spark. |
| [run_progress_log.md](../specs/run_progress_log.md) | The tree-shaped progress log of a run and `log_to_console`. |
| [postgres_connectors.md](../specs/postgres_connectors.md) | The PostgreSQL reader and writer, and the `ingestio[postgres]` extra. |
| [disk_connectors.md](../specs/disk_connectors.md) | The disk reader and writer: Spark file sources, selection by watermark before loading. |
| [s3_connectors.md](../specs/s3_connectors.md) | The S3 reader and writer, listed with boto3, and the `ingestio[s3]` extra. |
| [gcs_connectors.md](../specs/gcs_connectors.md) | The GCS reader and writer, listed with google-cloud-storage, and the `ingestio[gcs]` extra. |
| [volume_connectors.md](../specs/volume_connectors.md) | The Databricks volume reader and writer, listed with the Databricks SDK's Files API, and the `ingestio[databricks]` extra. |
| [unity_catalog_connectors.md](../specs/unity_catalog_connectors.md) | The Unity Catalog reader and writer: Delta tables by their three-part name, a watermark filter in the table's scan, a table created when missing. |
| [bigquery_connectors.md](../specs/bigquery_connectors.md) | The BigQuery reader and writer: a watermark filter in the Storage Read API with a frozen bound, an atomic write through a staging table, and the `ingestio[bigquery]` extra. |
| [config_connectors.md](../specs/config_connectors.md) | `ReadSource` and the write callback (since replaced by `AppendOnly` and `FullLoad`) choose the reader and the writer from the job's JSON configuration, through a closed table of the shipped connectors. |

## Agent tooling

These files live in `.claude/`, which is not part of the repository: they exist only in the
maintainer's working copy.

| Path | What it is |
|---|---|
| `.claude/agents/` | The five agents of the [workflow](workflow.md): `planner`, `test-writer`, `doc-keeper`, `implementer`, `acceptance-validator`. |
| `.claude/kb/architecture/quick-reference.md` | Concepts, modules, lifecycle and public API at a glance. |
| `.claude/kb/code-conventions/quick-reference.md` | Code style, types, errors and logging, from CONTRIBUTING.md. |
| `.claude/kb/testing/quick-reference.md` | Test conventions and acceptance-criteria naming. |
| `.claude/kb/sdd-workflow/quick-reference.md` | Stages, statuses, write scopes and returns, from workflow.md. |
| `.claude/hooks/guard_git_writes.py` | Blocks git and GitHub writes from every agent. |
| `.claude/hooks/restrict_writes.py` | Keeps each agent's file edits inside its stage's directories. |
| `.claude/hooks/progress_timer.py` | Keeps the clock of the `planner` and the `test-writer`, and reminds them every five minutes to report their progress. |
| `.claude/progress/` | The progress reports of the `planner` and the `test-writer`, one file per agent, rewritten on each pass. |
| `.claude/settings.json` | Permissions, attribution off, and the hooks above. |
