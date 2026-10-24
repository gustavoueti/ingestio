# Spec: SCD2 write callback

- **Status:** validated
- **Slug:** `scd2_callback`. The spec is `specs/scd2_callback.md` and its tests are
  `tests/test_scd2_callback.py`.
- **Related:** [backlog idea 12](../docs/backlog.md#12-scd2-write-callback),
  [philosophy](../docs/philosophy.md),
  [ADR 0004](../docs/decisions/0004-shared-process-state.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md),
  [specs/append_only_callback.md](append_only_callback.md),
  [specs/config_connectors.md](config_connectors.md) (its "Effects on the draft specs"), and the
  connector specs [postgres](postgres_connectors.md), [unity_catalog](unity_catalog_connectors.md),
  [bigquery](bigquery_connectors.md) and [disk](disk_connectors.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

`SCD2(config, *, key="records", connection=None, client=None)` keeps the history of a dimension
as a slowly changing dimension of type 2. When the process runs, it reads the whole target with
the reader of the target's system, computes the new table in Spark, in which a record whose
tracked values differ from those of its entity's current version closes that version and opens a
new one, and writes the new table with `write(data, replace=True)`, as `FullLoad` does. It builds
its writer and the target's reader from the `target` section of the job's configuration, through
the closed table of [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
and reads its own parameters, the key columns, the effective-time column and the rest, from a new
top-level section of the same file, `scd2`.

The maintainer took decisions 1 to 6 on 2026-10-23, and decisions 19 to 34 the same day,
answering the sixteen open questions of the planner's first revision; decision 34, on which
criteria need Spark and when they run, was changed on 2026-10-24. This revision replaces the
draft of 2026-10-22, which predates ADR 0015, ADR 0016 and the Spark connectors. It makes the changes
listed in config_connectors' "Effects on the draft specs": the callback takes the path of the
configuration and accepts no connector, so the proposal `SCD2(source, target, *, keys, ...)` is
gone, and so is the draft's idea of a capability that user connectors implement; the reader of the
target is built from a section meant for a writer; the parameters of the callback live outside the
`target` section; the configuration has no mode.

## Motivation

- The [philosophy](../docs/philosophy.md#callback) lists SCD2 among the write strategies and names
  it, with append-only, as a default callback of the library.
- [ADR 0016](../docs/decisions/0016-make-each-write-strategy-one-callback.md) says that a write
  strategy the library adds later, SCD2 or a merge, "is one more callback that builds its writer
  from the `target` section". `AppendOnly` and `FullLoad` have landed; SCD2 has not.
- Today a job that keeps the history of a dimension writes its own callback, which reads the
  dimension, compares it with the batch and writes it back through the shipped connectors, as
  decision 8 of [config_connectors](config_connectors.md) allows.

## Fit with the philosophy

- **Concept: callback.** SCD2 is a write strategy, which the philosophy lists among callbacks. The
  connectors do not change and decide nothing: the target's reader makes a full read, and the
  writer replaces the target with `write(data, replace=True)` (decision 1). Comparing, chaining
  and closing versions is the callback's work.
- **No third concept.** The `scd2` section is a parameter of the callback in the job's file, like
  the `target` section. The private routine and the closed table of ADR 0015 are reused. There is
  no base class of write strategies, no registry of strategies and no mode.
- **Essential.** The philosophy says that "the library's default callbacks, append-only and SCD2,
  are essential" ([ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md)).
  `SCD2` declares no criticality and catches nothing.
- **Simplicity.** One callback, one call to the reader and one to the writer per run, with checks
  of the batch before the write.
- **Compose, do not impose.** A job places `SCD2` in a step. A job that wants another history,
  such as snapshot semantics or columns overwritten in place, writes its own callback over the
  shipped connectors.
- **No dependencies in the core.** pyspark is imported lazily, when the process runs, and stays a
  prerequisite of the runtime, as for the Spark connectors. The extras do not change: a `postgres`
  target needs `ingestio[postgres]` and a `bigquery` target `ingestio[bigquery]`, for their
  writers, as today.
- **Streaming.** A run reads the whole batch and the whole target and rewrites the dimension, so
  the callback leans towards batch. It does not close the door: the `Reader` and `Writer`
  protocols and their payload do not change, and under ADR 0016 a streaming SCD2 would be another
  write strategy, a callback of its own, beside this one. No ADR; a line in ADR 0012's
  consequences (decision 28).
- **Texts that the change touches.**
  1. Decision 6 makes the top-level `scd2` member SCD2's. That contradicts decision 11 of
     [config_connectors](config_connectors.md), under which every callback ignores every
     top-level member other than its own section, and the
     [configuration page](../docs/concepts/configuration.md#the-file), which says such a member
     "belongs to the job". ADR 0015 records the shape of the file, so a new ADR 0017 amends it
     (decision 29, [ADRs](#adrs)).
  2. The philosophy stays unchanged. Its sentence on the configuration, "A job names its source
     and target systems in its configuration, and the library's read and write callbacks build
     the matching connector", still holds; "no extra configuration" is about extending callbacks,
     which stays subclassing `Callback`.
  3. ADR 0016's "The configuration has no mode" still holds: the `scd2` section chooses no write
     strategy, and the other callbacks ignore it (AT-013).

## Scope

In scope:

- `SCD2`: its signature, building, running, errors and exports;
- the `scd2` section of the configuration: its name, keys, JSON types, checks and errors;
- the target systems that SCD2 supports, and how it builds the target's reader from the `target`
  section through the closed table;
- the new table: which rows are written, and the checks of decisions 4, 5, 22 and 23;
- the changes to the private routine of `ingestio/callback/_configuration.py`;
- ADR 0017, the amendment of ADR 0015 and the line in ADR 0012;
- the part of config_connectors' decision 11 that this spec supersedes.

Out of scope:

- any change to a connector, a new capability, and a Delta `MERGE` (decision 1);
- snapshot semantics and detecting deletes in the source (decision 2);
- correcting late-arriving data (decision 4);
- SCD types other than 2, and overwriting untracked columns in place (decision 20);
- surrogate keys (decision 33);
- creating the target (decision 24);
- the target systems `json_lines`, `disk`, `s3`, `gcs` and `volume` (decision 26);
- a payload other than a Spark DataFrame;
- several dimensions in one file: one target per file, as for the other write callbacks;
- the criticality mechanism, which has its own spec under ADR 0011;
- the README, which the maintainer updates, and CI: adding `INGESTIO_SPARK_TESTS: "1"` to
  `.github/workflows/ci.yml`, so that CI runs the criteria that need Spark, is the maintainer's
  change, outside the flow (decision 34).

## Public API

Added, in `ingestio/callback/io.py` next to `AppendOnly` and `FullLoad`, exported by `ingestio` and
by `ingestio.callback.all` (decision 27):

```python
class SCD2(Callback):
    """Keeps the history of the dimension that the job's configuration names, as an SCD of type 2.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            dimension's system and table, and its `scd2` section holds the key columns, the
            effective-time column and the other parameters of the history.
        key: Key of `Process.state` holding the batch, a Spark DataFrame.
        connection: Where a PostgreSQL target lives, with its credentials. Required by the
            `postgres` system.
        client: BigQuery client of the target, for the `bigquery` system.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | None = None,
        client: object | None = None,
    ) -> None: ...
```

- `config` is the only positional argument; the others are keyword-only. The types are those of
  `AppendOnly`: `os` is the standard module, `PostgresConnection` is
  `ingestio.connector.all.PostgresConnection`, and `client` is `object` because its type depends on
  the system.
- `SCD2` takes no `replace`, `spark`, `watermark_value` or column names: its parameters are in the
  `scd2` section (decision 6), and the target is read with the batch's own session (decision 25).
- It subclasses `Callback` directly. It is not a subclass of `AppendOnly` or `FullLoad`, nor they
  of it, and no base class of write strategies is public.
- In the run's log it appears by its class name, `SCD2.run`.

Changed: the job's configuration file may hold a top-level `scd2` section, which `SCD2` reads and
every other callback ignores (decisions 6 and 29). It is described under
[The `scd2` section](#the-scd2-section).

Unchanged: `ReadSource`, `AppendOnly`, `FullLoad`, every connector, `PostgresConnection`, `Reader`,
`Writer`, `Callback`, `Process`, `Step`, the cancel exceptions, `log_to_console`, and their import
paths.

### Usage sketch

A sketch of a job, not a runnable example. `config.json`:

```json
{
  "source": {"system": "postgres", "table_name": "public.customers", "watermark_column": "updated_at"},
  "target": {"system": "unity_catalog", "table_name": "main.gold.dim_customer"},
  "scd2": {
    "key_columns": ["customer_id"],
    "effective_time_column": "updated_at",
    "tracked_columns": ["name", "city", "segment"]
  }
}
```

`main.py`:

```python
CONFIG = Path(__file__).with_name("config.json")

Process([
    Step("extract", [ReadSource(CONFIG, connection=shop, watermark_value=last_watermark())]),
    Step("load", [SCD2(CONFIG)]),
]).run()
```

- `shop` and `last_watermark` are the job's, as in the configuration page's example.
- `main.gold.dim_customer` exists before the first run (decision 24), with the columns of the batch
  plus `valid_from` and `valid_to`.
- Each run reads the customers changed since the last watermark, and the dimension keeps every
  state of every customer: a change of `name`, `city` or `segment` closes the customer's current
  version at its `updated_at` and opens a new one.

## Behaviour

### The `scd2` section

A top-level member of the job's file, a JSON object, read by `SCD2` only (decisions 6 and 29).
`ReadSource`, `AppendOnly` and `FullLoad` ignore it, whatever it holds, as they ignore every
member other than their own section; `SCD2` ignores `source` and every other member.

| Key | Required | JSON type | Meaning |
|---|---|---|---|
| `key_columns` | yes | non-empty array of non-empty strings, no column twice | the columns whose values identify an entity, its business key |
| `effective_time_column` | yes | non-empty string | the column of the record that holds the time from which the record's state holds (decision 3) |
| `tracked_columns` | no | non-empty array of non-empty strings, no column twice | the columns whose change opens a new version. Every column of the batch except the key columns and the effective-time column when left out (decision 20) |
| `valid_from_column` | no | non-empty string | the version column that holds the start of a version. `valid_from` when left out (decision 19) |
| `valid_to_column` | no | non-empty string | the version column that holds the end of a version, `null` while the version is current. `valid_to` when left out (decision 19) |

- The section is strict, as a `source` or `target` section is (config_connectors decision 11): a
  key not in the table is an error, and `null` is never a value.
- The names are column names, compared with each other and with the columns of the batch and of
  the target ignoring case (decision 9). A name is one top-level column: a `.` in it is part of the
  name, not a nested field.
- No column may have two roles: the key columns, the effective-time column, the tracked columns
  and the two version columns, the defaults included, name pairwise different columns.
- The section names no system and chooses no write strategy: the configuration still has no mode
  (decision 6). A file whose `target` is written by `AppendOnly` may hold an `scd2` section, which
  `AppendOnly` ignores.

### Building

Building `SCD2(config, ...)` reads the file once, as `os.fspath(config)` names it, and raises the
first problem it finds, in this order:

1. the nine steps of [config_connectors, "Building the
   callback"](config_connectors.md#building-the-callback) for the `target` section, as
   `AppendOnly` runs them, with the callback arguments `connection` and `client`, except that the
   names valid at step 4 are only the supported systems, `bigquery`, `postgres` and
   `unity_catalog` (decisions 7 and 26). The last step builds the writer;
2. the `scd2` section is present and is a JSON object;
3. its keys, in the order of the file: each one is a key of the table above;
4. its required keys, `key_columns` then `effective_time_column`: each one is present;
5. the type of each value, in the order of the file; for an array, its type, then that no column
   appears twice;
6. that no column has two roles, checking the pairs of roles in this order: `key_columns` with
   each later role, then `effective_time_column` with each later role, then `tracked_columns`,
   then `valid_from_column` with `valid_to_column`, the order of the table above, the defaults of
   the version columns included. Within a pair, the names of the earlier role are taken in their
   order.

- Building touches no Spark session, no client, no database and no network, whatever the system,
  and imports no pyspark. A mistake in the file, a missing extra included, raises when the
  callback is built, before `Process` exists.
- The callback reads the file once. Changing or deleting it afterwards changes nothing.
- The `source` section and every top-level member other than `target` and `scd2` are ignored,
  whatever they hold.
- The target's reader is not built here, because it reads with the batch's session (decision 25):
  it is built when the process runs. Its parameters are the writer's, which step 1 has checked, so
  building it can raise nothing that building the writer did not.

### Target systems

Decision 26: `SCD2` supports `unity_catalog`, `postgres` and `bigquery`, the systems whose
`write(data, replace=True)` is atomic and stays correct when the DataFrame written is a lazy plan
that reads the same target. What the code of each connector establishes:

| `system` | Full read of the target | `write(data, replace=True)` | Atomic | `data` reads the target |
|---|---|---|---|---|
| `unity_catalog` | `spark.read.table(<quoted>)`, the whole table | one `saveAsTable` in `overwrite` mode, one Delta commit | yes: one commit; a failed write leaves the table as it was | the save evaluates `data` once, and Delta reads one snapshot of the table. Whether Spark lets a Delta table that the plan reads be overwritten is not established by the code: the integration test of [Notes for testing](#notes-for-testing) would confirm it, and that section says where it can run |
| `postgres` | one JDBC statement over the table, in one partition, so one snapshot | Spark loads an unlogged staging table from `data`; then one transaction runs `TRUNCATE`, `INSERT ... SELECT` from the staging table, and `DROP` | yes: one transaction; on failure the staging table is dropped and the target is unchanged | `data` is evaluated whole, into the staging table, before the transaction touches the target |
| `bigquery` | Spark's BigQuery source over the table, through one read session | Spark loads a staging table from `data`; then one `MERGE ... ON FALSE` inserts its rows and deletes the others | yes: one statement; on failure the staging table is dropped and the target is unchanged | `data` is evaluated whole, into the staging table, before the statement touches the target |
| `disk`, `s3`, `gcs`, `volume` | the readers reject `delta` (`format 'delta' cannot be read file by file`); plain formats only | Spark's `overwrite` mode, which deletes everything under the path first | no | the readers give Spark a list of files, not the directory. Whether Spark's check against overwriting a path being read catches that is not established; if it does not, the overwrite deletes the files before the job reads them |
| `json_lines` | no reader | | | |

An unsupported system raises when the callback is built, with the configuration's own message and
the narrowed list: `<path>: target.system 'disk' is not one of: bigquery, postgres, unity_catalog`.

### The target's reader

Decision 25. When the process runs, `SCD2` builds the reader of the target's system: the `source`
row of the closed table for the same system, with these parameters, and no other:

| `system` | Reader | From the `target` section | Callback arguments | Session |
|---|---|---|---|---|
| `unity_catalog` | `UnityCatalogReader` | `table_name` | none | `spark=state[key].sparkSession` |
| `postgres` | `PostgresReader` | `table_name` | `connection` | `spark=state[key].sparkSession` |
| `bigquery` | `BigQueryReader` | `table_name`, `billing_project` when given | `client` when given | `spark=state[key].sparkSession` |

- The section's `options` go to the writer only. No `watermark_column` and no `watermark_value`: the
  read is a full read.
- `connection` and `client` are the very objects that the writer received.
- `read()` is called once per run.

### Running

`SCD2.run()` does the following, in this order. Every step before the last raises before the
writer is called, so a failure leaves the target as it was.

1. **The batch.** It takes `state[key]`. A missing key raises `KeyError(key)`, and nothing is read.
2. **The batch's columns.** From `state[key].columns`, which runs no Spark job: every column that
   the section names is a column of the batch, in the order `key_columns`, `effective_time_column`,
   `tracked_columns`; then neither version column is a column of the batch, `valid_from_column`
   first (decision 10). The target is not read when this step fails.
3. **The target.** It builds the target's reader with `spark=state[key].sparkSession` and calls
   `read()`, once.
4. **The target's columns.** From the `columns` of the DataFrame that `read()` returned: every
   column of the batch, in the batch's order, then `valid_from_column` and `valid_to_column`, is a
   column of the target; then every column of the target, in its order, is one of them (decisions
   10 and 33).

Steps 1 to 4 use nothing of the batch but `columns` and `sparkSession`, nothing of the target but
`columns`, and build no Spark expression, so their criteria run without a Spark session
(decision 34).
5. **The checks.** One Spark job over the batch and the target's current versions, described in
   [The checks](#the-checks) (decision 17).
6. **The write.** It builds the new table, described in [The new table](#the-new-table), and calls
   `write(new_table, replace=True)` on the writer, once (decision 1). It does so whatever the
   batch holds, an empty batch and a batch that changes nothing included (decision 12).

- The callback implements only `run`, catches nothing and writes no log line of its own
  (decision 15). Every error propagates out of `Process.run()` unchanged.
- It never checks the type of `state[key]` (decision 14). A value that is not a Spark DataFrame
  fails at its first use with Python's or Spark's error.
- A *current version* is a row of the target whose `valid_to_column` is null. The callback trusts
  the target (decision 13): it assumes at most one current version per entity and no null in the
  target's key columns or `valid_from_column`, which hold in a target that only `SCD2` writes.

### The checks

Each check is a `ValueError` raised by `SCD2.run()`, with no note. They run in one Spark job, and
when several fail, the first in this order raises:

1. **Nulls** (decision 23). A null in a key column or in the effective-time column of a record:
   `state['<key>'] has a null in column '<name>'`, naming the first such column in the order of
   `key_columns`, then the effective-time column.
2. **Same key and time** (decision 5). Two records with equal key columns and an equal effective
   time, whatever their other values:
   `state['<key>'] has records with the same key columns and the same '<E>'`.
3. **Late-arriving** (decision 4). A record whose effective time is earlier than the
   `valid_from_column` of its entity's current version:
   `state['<key>'] has a record whose '<E>' is earlier than the '<VF>' of its entity's current version`.
4. **Same time, other values** (decision 22). A record whose effective time equals the
   `valid_from_column` of its entity's current version, and whose tracked values differ from that
   version's: `state['<key>'] has a record whose '<E>' equals the '<VF>' of its entity's current version, with other tracked values`.

`<key>` is the callback's `key`; `<E>` and `<VF>` are the effective-time column and the
`valid_from_column` as the section names them, or `valid_from` by default; `<name>` is the
column's name as the section writes it. No value of the data appears in a message (decision 11).

**The guard** (decision 32). The batch is a lazy plan, and the write evaluates it again. The
DataFrame handed to the writer carries the same four conditions: if a record that breaks one of
them reaches it, because the batch or the target changed between the checks and the write, the
evaluation of that DataFrame fails with Spark's exception, whose message holds the message of the
check. The writer's own failure path then leaves the target as it was: no Delta commit, or the
staging table dropped before the target is touched.

### The new table

Tracked values are compared null-safe (decision 21): two nulls are equal, and a null and a value
differ. The effective-time column is ordered and compared by Spark's rules for its type; the
callback checks no type (decision 19).

For each entity, the callback walks a *chain*: the entity's current version in the target, when
there is one, then the entity's records of the batch, sorted by the effective-time column. Each
record is compared with the version before it in the chain, the current version or the last new
version:

- when its tracked values differ, the record becomes a new version;
- otherwise it is dropped (decision 30). A record whose effective time equals the current
  version's start has, after check 4, the current version's tracked values, so it is dropped by
  the same rule.

The new table holds:

- every closed version of the target, as it was read (decision 4);
- every current version of an entity that gets no new version, as it was read, its untracked
  columns included (decision 2 for the entities absent from the batch);
- every current version of an entity that gets new versions, with its `valid_to_column` set to
  the effective time of the entity's first new version, and nothing else changed;
- every new version: the record's row, every column as the batch holds it, the untracked ones
  included, with `valid_from_column` set to the record's effective time and `valid_to_column` set
  to the effective time of the entity's next new version, or null for the last (decision 5).

The effective-time column keeps its own value in every row; in a new version it equals
`valid_from_column`. The table's columns are the target's, in the target's order (decision 16).
The order of its rows is not specified.

### Writing

What `write(new_table, replace=True)` does is the connector's, as [Target systems](#target-systems)
summarises: one Delta commit for `unity_catalog`, a staging table and one transaction for
`postgres`, a staging table and one `MERGE` for `bigquery`. Every run rewrites the whole dimension
(decision 1).

### The first run

Decision 24. The target must exist before the first run, on every system, with the batch's columns
and the two version columns. When it does not, `read()` or the schema of its result raises the
reader's own error, which propagates unchanged, and the writer is not called. An existing empty
target is a valid first run: every record is a new entity.

### An empty batch, and a run with no change

Decision 12: the callback writes the new table whatever the batch holds. A batch with no rows, or
whose records change nothing, makes the writer replace the target with the rows it already held. A
DataFrame with no columns, such as a file reader's empty selection without a schema, fails at
step 2, because it lacks the key columns.

### The value in the state

The callback leaves the state as it found it: `state[key]` keeps the same object, and no key is
added or removed (decision 15).

### Errors

`<path>` is `os.fspath(config)`. At building, the errors of the configuration page, with
`<section>` as `target`, then those of the `scd2` section; every error of the callback's own checks
there is a `ValueError` whose message starts with `<path>: `.

| Case | Raised | Error |
|---|---|---|
| `config` is not a `str` or an `os.PathLike`, a connector included | building | `TypeError: config must be a str or os.PathLike, not <type name>` |
| the file is missing, a directory, unreadable, not UTF-8 | building | the `OSError` or `UnicodeDecodeError` of reading, unchanged |
| the JSON, the top level, the `target` section, its `system`, keys, types and callback arguments | building | the `ValueError` of the configuration page, such as `<path>: missing section 'target'` |
| `target.system` is not a supported system | building | `<path>: target.system '<name>' is not one of: bigquery, postgres, unity_catalog` |
| the writer rejects a value, or its extra is not installed | building | the connector's `ValueError` or `ImportError`, the same object, with one note, `while building the target of <path>, system '<system>'` |
| the `scd2` section is missing | building | `<path>: missing section 'scd2'` |
| it is not an object | building | `<path>: section 'scd2' must be a JSON object` |
| a key not in the table | building | `<path>: scd2.<key> is not a key of section 'scd2'` |
| a required key is missing | building | `<path>: scd2.<key> is required` |
| `effective_time_column`, `valid_from_column` or `valid_to_column` is not a non-empty string | building | `<path>: scd2.<key> must be a non-empty string` |
| `key_columns` or `tracked_columns` is not a non-empty array of non-empty strings | building | `<path>: scd2.<key> must be a non-empty array of non-empty strings` |
| a column twice in one array | building | `<path>: scd2.<key> names the column '<name>' twice`, `<name>` as its second occurrence is written |
| a column in two roles | building | `<path>: scd2.<role> and scd2.<later role> both name the column '<name>'`, `<name>` as the earlier role writes it |
| an argument the callback does not take | building | Python's `TypeError` for an unexpected keyword argument |
| `state[key]` is missing | running | `KeyError`, whose `args` are `(key,)`, with no note |
| the batch lacks a column that the section names | running | `ValueError: state['<key>'] has no column '<name>', which scd2.<parameter> names` |
| the batch has a column that a version column names | running | `ValueError: state['<key>'] already has a column '<name>', which scd2.<parameter> names` |
| `read()` fails, a missing target included | running | the reader's error, unchanged, the same object, with no note |
| the target lacks a column of the batch or a version column | running | `ValueError: the target has no column '<name>'` |
| the target has another column | running | `ValueError: the target has a column '<name>' that is neither a column of state['<key>'] nor a version column` |
| a check fails | running | the `ValueError` of [The checks](#the-checks) |
| the batch changed between the checks and the write | running, in the write | Spark's exception, whose message holds the check's message (decision 32) |
| `write()` fails | running | the error, unchanged, the same object, with no note |
| Spark rejects the plan, such as a tracked column of a map type, or types the target cannot hold | running | Spark's error, unchanged |

- `<parameter>` is the key of the section that names the column, such as `key_columns`; for a
  version column left at its default, still `valid_from_column` or `valid_to_column`.
- In the messages about the batch, `<name>` is written as the section writes it; in "the target
  has no column", as the batch or the section writes it; in "the target has a column", as the
  target writes it.
- Every error at running propagates out of `Process.run()`, and the run's log shows the failed
  moment, `SCD2.run`, as for any callback.

### Where it lives

- `src/ingestio/callback/io.py`: `SCD2`, next to `ReadSource`, `AppendOnly` and `FullLoad`. pyspark
  is imported inside `run`, never at module level.
- `src/ingestio/callback/_configuration.py`: the routine builds the writer for a narrowed set of
  target systems; builds, from a checked `target` section, the reader of the same system through
  the `source` row of the table; and reads and checks the `scd2` section. The table, the existing
  checks and their messages do not change, and the file is still read once per callback.
- `src/ingestio/callback/all.py`, `src/ingestio/basics.py` and `src/ingestio/__init__.py`: the
  exports.
- The connector modules, `src/ingestio/connector/all.py` and `pyproject.toml` do not change.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's.

- `docs/concepts/callbacks.md`: the table of shipped callbacks, the sentence "SCD2 will be another
  write callback", and the known limitations of the write callbacks.
- `docs/concepts/configuration.md`: "The file" (the bullet "Every other top-level member ...
  belongs to the job"), "Using it in a job", the `scd2` section, "Errors", and the known
  limitations.
- `docs/concepts/connectors.md`: nothing required; the doc-keeper may say in the `unity_catalog`,
  `postgres` and `bigquery` sections that `SCD2` can write to them.
- New ADR 0017; ADR 0015, amended; ADR 0012, one line in its consequences ([ADRs](#adrs)).
- `docs/index.md`: the description of this spec, the callbacks and configuration pages, and ADR
  0017.
- `.claude/kb/architecture/quick-reference.md`: modules, the configuration lines, the public API.
- `docs/workflow.md` and `.claude/kb/sdd-workflow/quick-reference.md`: a line for criteria whose
  tests are opt-in and skipped where a stage runs (decision 34), as [Notes for
  testing](#notes-for-testing) describes.
- `docs/backlog.md`, idea 12: removed when the work lands, by the main session.
- `README.md`, for the maintainer.

### Known limitations

- **Every run rewrites the whole dimension** (decision 1). Its cost grows with the dimension, not
  with the batch. On `postgres`, `TRUNCATE` holds an exclusive lock on the dimension while the
  transaction applies, and fails when a foreign key references the dimension. On `bigquery`, the
  `MERGE` is billed over the whole table. On `unity_catalog`, every run is a new version that
  rewrites every file, kept until `VACUUM`.
- **The batch and the target are each evaluated twice**, once by the checks and once by the write.
  A batch that is a lazy read of a live source reads it twice; the guard of decision 32 fails the
  write when the second evaluation breaks a check.
- **Deletes in the source are not detected** (decision 2): an entity absent from the batch keeps
  its current version.
- **A late-arriving record fails the run** (decision 4). The job corrects the target, or the batch,
  itself.
- **Re-running a batch that holds several records of one entity fails** as late-arriving, because
  its first records are earlier than the current version that its last record opened (decision
  31). A batch with one record per entity re-runs without change (AT-027).
- **The callback assumes it is the dimension's only writer** during a run. A write by someone else
  between the read and the write is lost: overwritten by the `TRUNCATE` or the `MERGE`, or, on
  Delta, subject to Delta's conflict detection.
- **The callback trusts the target** (decision 13): two current versions of one entity, or a null in
  its key columns or `valid_from_column`, give a wrong result, unchecked.
- **No surrogate key** (decision 33). A version is identified by its key columns and its
  `valid_from_column`; a column that the system generates on insert cannot be in the target.
- **Schema changes are the job's.** A new column in the batch fails step 4 until the job adds it to
  the target with its own DDL.
- **On `postgres`, a Spark task retry can duplicate rows** of the staging table, as the PostgreSQL
  connectors' known gaps say, and so duplicate a version.
- **On `bigquery`, `require_partition_filter`** breaks the full read and the replace, and rows still
  in the streaming buffer break the replace, as the BigQuery connectors' known gaps say.
- **On `unity_catalog`, the overwrite of a table that the plan reads is unverified.** The Delta
  integration test of [Notes for testing](#notes-for-testing) would verify it, but it runs neither
  in the development environment nor in CI as CI is planned. Until it runs, `unity_catalog`
  support rests on the code of `UnityCatalogWriter` and on the Delta guarantees that the Unity
  Catalog connectors spec records, not on a test of SCD2: the criteria that need Spark replace
  `UnityCatalogWriter.write` and never write to Delta.
- **SCD2's behaviour is verified only in CI** (decision 34): the criteria that need Spark are
  skipped wherever `INGESTIO_SPARK_TESTS` is not `1`, the development environment included.
- **A tracked column of a map type** cannot be compared: Spark's error.
- **The effective time is ordered by Spark's rules for its type:** a string column orders as text.
- **One dimension per configuration file.**

### Notes for testing

- **Building** needs no Spark. Tests write each configuration with `json.dumps` to a file under
  `tmp_path` and pass its path, as in `tests/test_config_connectors.py`; a test that needs invalid
  JSON writes the text itself. A missing package is simulated in a subprocess, by mapping its
  top-level name to `None` in `sys.modules`. `pyspark.sql.SparkSession.active` and
  `psycopg.connect` can be replaced with `monkeypatch.setattr`.
- **Two sets of criteria** (decision 34):
  - *Without Spark*, always run: AT-001 to AT-014, AT-033, AT-034, AT-036 and AT-037. Building
    needs no Spark, and the criteria among them that run a process stop at or before step 4 of
    [Running](#running), with the doubles below.
  - *With Spark*, opt-in: AT-015 to AT-032, AT-035, AT-038 and AT-039 to AT-042. They need Spark's
    computation, joins, sorts and null-safe comparisons, which no double executes. Each test is
    marked `integration` and skipped unless `INGESTIO_SPARK_TESTS` is `1`, with the skip reason of
    `tests/test_disk_connectors.py`, "INGESTIO_SPARK_TESTS is not 1: the tests against a local
    Spark session are opt-in". They use a local session, `local[2]`, in UTC, created once per
    module, as the integration tests of that file do.
- **Doubles for the criteria without Spark.** A *fake batch* is an object whose `columns` is the
  list of the batch's column names and whose `sparkSession` is a fake session, an object compared
  by identity. A *fake target* is an object with only `columns`. The reader's `read` is replaced on
  the class, with `monkeypatch.setattr`, to record the reader and return a fake target or raise;
  the writer's `write` is replaced to record its calls. pyspark comes with the `dev` group, and
  importing it starts no JVM.
- **What each stage can run** (decision 34):
  - The criteria that need Spark are skipped in the development environment. The test-writer
    cannot run them: it shows that they are collected and skipped for the reason above, and that
    the criteria without Spark fail for the missing feature. The implementer cannot run them
    either.
  - The validation stage runs the suite with them skipped, validates everything else, and lists
    the skipped criteria in its report.
  - SCD2's behaviour is verified only in CI, once the maintainer has added
    `INGESTIO_SPARK_TESTS: "1"` to `.github/workflows/ci.yml` and pushed. A failure there returns
    the change to the stage it names, as any validation failure does.
  - [docs/workflow.md](../docs/workflow.md) needs a line for skipped criteria. It says that the new
    tests "fail for the expected reason" at the end of the test stage, that the implementer writes
    code "until every test passes", and that the acceptance-validator fails a change when "an
    acceptance criterion has no passing test"; none of these holds for a criterion whose tests are
    skipped where the stage runs. The documentation stage of this spec adds, under Tests,
    Implementation and Validation, that a criterion whose tests the spec makes opt-in, and which are
    skipped where the stage runs, is reported as not run and does not fail the validation; that CI
    runs it after the maintainer pushes; and that a failure there returns the change to the stage
    it names. The same goes into `.claude/kb/sdd-workflow/quick-reference.md`.
- **The computation criteria** use a `unity_catalog` target and replace `UnityCatalogReader.read`
  and `UnityCatalogWriter.write` on the classes, with `monkeypatch.setattr`: the read records the
  reader and returns a DataFrame that the test builds in the session, the target; the write
  records the writer, its `replace` argument, `data.columns`, and the rows of `data`, collected
  inside the call, so that a failing evaluation raises from `write` as it would from the real
  writer's save. The *written rows* are those rows, compared as a multiset of tuples in the order
  of `data.columns`.
- **AT-014 and AT-039** compare the reader and the writer that the callback built with twins built
  directly from `ingestio.connector.all`, by their state, such as `vars(built) == vars(twin)`, where
  the connection, the client and the session are the same objects. AT-014 needs no Spark: its
  replaced `read` records the reader and raises, so the run stops at step 3.
- **AT-015 and AT-042** use the real `UnityCatalogReader` over a table of the session's own catalog,
  `spark_catalog.default.<name>`, created by the test as a Parquet table with `saveAsTable`, under a
  warehouse directory in `tmp_path`. Reading it needs no Delta; the writer stays replaced. A
  session from `spark.newSession()` shares that catalog, so AT-015 can build its batch there while
  the module's session stays the active one, and wraps the real `read` rather than replacing it.
- **AT-035** needs a batch that changes between the checks and the write. One way: the
  effective-time column computed by a Python UDF marked non-deterministic, which returns a valid
  date while a marker file under `tmp_path` is absent and a late date once it exists, the marker
  being created by the replaced `UnityCatalogWriter.write` before it collects `data`.
- **Integration, on top of the criteria: the Delta test that decides `unity_catalog` (decision
  26).** A test against a real Delta table, marked `integration` and skipped unless
  `INGESTIO_SPARK_TESTS` is `1` and `INGESTIO_DELTA_PACKAGE` names the Delta package, as for the
  Unity Catalog connectors, run in its own process. Over a Delta table
  `spark_catalog.default.dim_customer` that the test creates holding `Ana`, with the real reader and
  writer: a run over AT-017's batch leaves the table holding AT-017's written rows, and a second
  run over `(1, "Ana", "Braga", 2026-04-01)` closes the `Porto` version at `2026-04-01`;
  `DESCRIBE HISTORY` shows one new version per run; a run that fails the late-arriving check adds
  no version. If Spark refuses to overwrite the table that the plan reads, the change
  returns to planning, and `unity_catalog` leaves the supported systems.
  - **Where it can run: nowhere, as things stand.** It needs a JVM that the locked pyspark 4.2.0
    starts on, which the development environment lacks (decision 34). It also needs the Delta Lake
    jars, which neither the development environment nor CI has: they are in no dependency group
    and not in `uv.lock`, and the session downloads them from Maven, through
    `spark.jars.packages`, only when `INGESTIO_DELTA_PACKAGE` names a Delta Lake release that
    matches the locked pyspark. CI as planned sets `INGESTIO_SPARK_TESTS` only, so the test is
    skipped there too. It would run in CI if the maintainer also set `INGESTIO_DELTA_PACKAGE`;
    whether a Delta Lake release that matches pyspark 4.2.0 exists is not established here.
  - **Until it runs**, `unity_catalog` support rests on the code of `UnityCatalogWriter` (one
    `saveAsTable` in `overwrite` mode, one Delta commit, `data` evaluated once) and on the Delta
    guarantees that the Unity Catalog connectors spec records, not on a test of SCD2. That Spark
    lets a plan overwrite a Delta table it reads stays unverified.

## Acceptance criteria

Unless a criterion says otherwise:

- `path` is a configuration file under `tmp_path`, and `<path>` is `os.fspath(path)`. Its default
  content is `{"target": {"system": "unity_catalog", "table_name": "main.gold.dim_customer"},
  "scd2": {"key_columns": ["customer_id"], "effective_time_column": "updated_at"}}`; a criterion
  that gives another `target` or `scd2` section replaces that section only.
- Every configuration error is raised by the call that builds the callback, before any `Process`
  exists.
- AT-001 to AT-014, AT-033, AT-034, AT-036 and AT-037 run without Spark, in every run of the
  suite. AT-015 to AT-032, AT-035, AT-038 and AT-039 to AT-042 need Spark and are skipped unless
  `INGESTIO_SPARK_TESTS` is `1` (decision 34). See [Notes for testing](#notes-for-testing).
- The criteria that run a process follow [Notes for testing](#notes-for-testing). The batch
  is `state["records"]`, of schema `customer_id INT, name STRING, city STRING, updated_at DATE`:
  a DataFrame in the criteria that need Spark, a fake batch with these columns in the others. The
  target is what `UnityCatalogReader.read` returns, of schema `customer_id INT, name STRING, city
  STRING, updated_at DATE, valid_from DATE, valid_to DATE`: a DataFrame, or a fake target with
  these columns.
  Batch rows are written `(customer_id, name, city, updated_at)`, target and written rows
  `(customer_id, name, city, updated_at, valid_from, valid_to)`, dates as `2026-01-01`, and `null`
  for a null. "Written" means that `write` was called exactly once, with `replace=True`, and the
  written rows are exactly those listed. "The writer is not called" means that `write` was never
  called.
- `Ana` stands for the target row `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, null)`.

### Import surface and signature

- **AT-001**: Given a fresh interpreter, when `from ingestio import SCD2` and
  `from ingestio.callback.all import SCD2` run, then both succeed and give the same class; `"SCD2"`
  is in `ingestio.__all__` and in `ingestio.callback.all.__all__`; `SCD2` is a subclass of
  `ingestio.Callback` and of neither `AppendOnly` nor `FullLoad`, and neither of them is a subclass
  of `SCD2`. `ReadSource`, `AppendOnly` and `FullLoad` are still exported by both. See decision 27.
- **AT-002**: Given `SCD2`, then `inspect.signature` shows exactly `config`, positional or keyword,
  then the keyword-only `key="records"`, `connection=None` and `client=None`, in this order. Given
  a valid `path`, `SCD2(path, replace=True)`, `SCD2(path, spark=object())`,
  `SCD2(path, watermark_value=1)`, `SCD2(path, key_columns=["customer_id"])` and
  `SCD2(path, "records")` raise `TypeError`. See decisions 6 and 27.
- **AT-003**: Given an interpreter where `pyspark`, `psycopg`, `boto3`, `google` and `databricks`
  are mapped to `None` in `sys.modules`, when `ingestio` is imported and `SCD2(path)` is built for
  the default configuration, then both succeed.

### Building: the `target` section

- **AT-004**: Given the `config` or the file on the left, when `SCD2` is built, then it raises the
  error on the right:

  | Given | Error |
  |---|---|
  | `config` is `UnityCatalogWriter("main.gold.dim_customer")` | `TypeError("config must be a str or os.PathLike, not UnityCatalogWriter")` |
  | `config` is `b"job.json"` or a `dict` | the same `TypeError`, naming `bytes` or `dict` |
  | no file at `path` | `FileNotFoundError` |
  | the file holds `{"target": ` | `ValueError` whose message starts with `<path>: invalid JSON: ` |
  | the file holds `[]` | `ValueError("<path>: the configuration must be a JSON object")` |
  | the file holds only the default `scd2` section | `ValueError("<path>: missing section 'target'")` |
  | `"target": "unity_catalog"` | `ValueError("<path>: section 'target' must be a JSON object")` |

  Given the default configuration, a `str` and a `pathlib.Path` naming the file are both accepted.
- **AT-005**: Given a `target` section whose `system` is on the left, with the keys its writer
  takes when it has one, when `SCD2` is built, then it raises
  `ValueError("<path>: target.system '<name>' is not one of: bigquery, postgres, unity_catalog")`
  for `json_lines`, `disk`, `s3`, `gcs` and `volume`, each with the keys its writer requires, and for
  `json_api`, `mysql`, `Unity_Catalog` and `""`. Given a section without `system`,
  it raises `ValueError("<path>: target.system is required")`; given `"system": 5`,
  `ValueError("<path>: target.system must be a string")`. See decision 26.
- **AT-006**: Given the `target` section and the arguments on the left, when `SCD2` is built, then
  it raises `ValueError` with the message on the right, prefixed with `<path>: `:

  | `target` section, besides `system`; arguments | Message |
  |---|---|
  | `unity_catalog`: `table_name`, plus `"mode": "overwrite"` | `target.mode is not a key of system 'unity_catalog'` |
  | `unity_catalog`: `table_name`, plus `"key_columns": ["customer_id"]` | `target.key_columns is not a key of system 'unity_catalog'` |
  | `unity_catalog`: `table_name`, plus `"watermark_column": "updated_at"` | `target.watermark_column is not a key of system 'unity_catalog'` |
  | `postgres`: no key; `connection=conn` | `target.table_name is required for system 'postgres'` |
  | `bigquery`: `table_name`, plus `"billing_project": 5` | `target.billing_project must be a string` |
  | `unity_catalog`: `table_name`, plus `"options": {"mergeSchema": null}` | `target.options must be an object of strings, numbers and booleans` |
  | `postgres`: `"table_name": "public.dim_customer"`; no `connection` | `target system 'postgres' needs the argument connection` |
  | `unity_catalog`: `table_name`; `connection=conn`, or `client=object()` | `target system 'unity_catalog' takes no argument <name>` |
  | `postgres`: `table_name`; `connection=conn`, `client=object()` | `target system 'postgres' takes no argument client` |
  | `bigquery`: `table_name`; `connection=conn` | `target system 'bigquery' takes no argument connection` |

  Given every argument as `None`, with `connection` given for `postgres`, building succeeds.
- **AT-007**: Given the `target` section on the left, when `SCD2` is built, then it raises the
  connector's `ValueError` with the message on the right and exactly one note,
  `while building the target of <path>, system '<system>'`:

  | `target` section | Message |
  |---|---|
  | `{"system": "unity_catalog", "table_name": "dim_customer"}` | `table_name must be 'catalog.schema.table'` |
  | `{"system": "postgres", "table_name": "a.b.c"}`, `connection=conn` | `table_name must be 'table' or 'schema.table'` |
  | `{"system": "bigquery", "table_name": "acme.gold.dim_customer", "options": {"table": "x"}}` | `option 'table' is set by the connector` |

  Given an interpreter where `psycopg` is mapped to `None` in `sys.modules`, a `postgres` target
  with `connection=conn` raises `ImportError("PostgresWriter needs psycopg: pip install
  'ingestio[postgres]'")`; where `google` is, a `bigquery` target raises
  `ImportError("BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'")`;
  each with exactly one note, `while building the target of <path>, system '<system>'`.

### Building: the `scd2` section

- **AT-008**: Given the default `target` and the `scd2` member on the left, when `SCD2` is built,
  then it raises the error on the right:

  | `scd2` member | Error |
  |---|---|
  | absent | `ValueError("<path>: missing section 'scd2'")` |
  | `[]`, `"customer_id"` or `null` | `ValueError("<path>: section 'scd2' must be a JSON object")` |

  Given a file with neither `target` nor `scd2`, the error is `<path>: missing section 'target'`;
  given `"target": {"system": "mysql"}` and no `scd2`, it is the `target.system` error of AT-005.
  The `target` section is checked first.
- **AT-009**: Given the `scd2` section on the left, when `SCD2` is built, then it raises
  `ValueError` with the message on the right, prefixed with `<path>: `:

  | `scd2` section | Message |
  |---|---|
  | the default plus `"keys": ["customer_id"]` | `scd2.keys is not a key of section 'scd2'` |
  | the default plus `"mode": "scd2"`, `"system": "x"`, `"is_current_column": "is_current"` or `"table_name": "x"` | `scd2.<key> is not a key of section 'scd2'` |
  | `{"effective_time_column": "updated_at"}` | `scd2.key_columns is required` |
  | `{"key_columns": ["customer_id"]}` | `scd2.effective_time_column is required` |
  | `{}` | `scd2.key_columns is required` |
  | `{"keys": ["customer_id"]}` | `scd2.keys is not a key of section 'scd2'` |
  | the default plus `"mode": "x"` then `"keys": []`, in this order of the file | `scd2.mode is not a key of section 'scd2'` |

- **AT-010**: Given the default `scd2` section with the key on the left set to the value in the
  middle, when `SCD2` is built, then it raises `ValueError` with the message on the right,
  prefixed with `<path>: `:

  | Key | Value | Message |
  |---|---|---|
  | `key_columns` | `"customer_id"`, `[]`, `["customer_id", 5]`, `["customer_id", ""]` or `null` | `scd2.key_columns must be a non-empty array of non-empty strings` |
  | `tracked_columns` | `[]`, `"city"` or `null` | `scd2.tracked_columns must be a non-empty array of non-empty strings` |
  | `effective_time_column` | `["updated_at"]`, `""`, `5` or `null` | `scd2.effective_time_column must be a non-empty string` |
  | `valid_from_column` | `5` or `""` | `scd2.valid_from_column must be a non-empty string` |
  | `valid_to_column` | `null` | `scd2.valid_to_column must be a non-empty string` |
  | `key_columns` | `["customer_id", "customer_id"]` | `scd2.key_columns names the column 'customer_id' twice` |
  | `tracked_columns` | `["city", "City"]` | `scd2.tracked_columns names the column 'City' twice` |

  Given two values of the wrong type, the message names the first in the order of the file.
- **AT-011**: Given the default `scd2` section changed as on the left, when `SCD2` is built, then
  it raises `ValueError` with the message on the right, prefixed with `<path>: `:

  | Change | Message |
  |---|---|
  | `"effective_time_column": "customer_id"` | `scd2.key_columns and scd2.effective_time_column both name the column 'customer_id'` |
  | `"tracked_columns": ["city", "customer_id"]` | `scd2.key_columns and scd2.tracked_columns both name the column 'customer_id'` |
  | `"key_columns": ["Customer_ID"]`, `"tracked_columns": ["customer_id"]` | `scd2.key_columns and scd2.tracked_columns both name the column 'Customer_ID'` |
  | `"tracked_columns": ["updated_at"]` | `scd2.effective_time_column and scd2.tracked_columns both name the column 'updated_at'` |
  | `"valid_from_column": "updated_at"` | `scd2.effective_time_column and scd2.valid_from_column both name the column 'updated_at'` |
  | `"tracked_columns": ["valid_to"]` | `scd2.tracked_columns and scd2.valid_to_column both name the column 'valid_to'` |
  | `"valid_to_column": "valid_from"` | `scd2.valid_from_column and scd2.valid_to_column both name the column 'valid_from'` |
  | `"valid_from_column": "start"`, `"valid_to_column": "START"` | `scd2.valid_from_column and scd2.valid_to_column both name the column 'start'` |
  | `"effective_time_column": "customer_id"`, `"tracked_columns": ["customer_id"]` | `scd2.key_columns and scd2.effective_time_column both name the column 'customer_id'` |

  Given `"valid_from_column": "effective_from"` and `"valid_to_column": "effective_to"`, with
  `"tracked_columns": ["valid_from"]`, building succeeds. See decisions 9 and 19.

### Building: what it touches

- **AT-012**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises,
  `psycopg.connect` replaced with a recording double, and a recording `client`, when `SCD2` is built
  for every row of AT-014, then each building succeeds, `active` is not called and nothing is
  recorded. Given `SCD2(path)` built from the default configuration, and the file then rewritten
  with `"table_name": "main.gold.other"` and `"effective_time_column": "inserted_at"`, or deleted,
  and a replaced `UnityCatalogReader.read` that records the reader and raises an exception `stop`,
  when a process runs it over a fake batch, then `Process.run()` raises `stop`, not the
  `ValueError` of a missing `inserted_at` column, and the recorded reader's `repr` is
  `UnityCatalogReader(table_name='main.gold.dim_customer')`.
- **AT-013**: Given a file holding `"source": 5`, `"primary_key": "id"`, `"job": {"owner": "data"}`
  and the default `target` and `scd2` sections, when `SCD2(path)` is built, then it succeeds.
  Given a file holding a valid `source`
  section, a `json_lines` target and `"scd2": 5`, `ReadSource(path)`, `AppendOnly(path)` and
  `FullLoad(path)` are built without error, and a process running `AppendOnly(path)` over
  `state={"records": [{"id": 1}]}` appends `{"id": 1}` to the file. Given the same file with the
  default `scd2` section, `FullLoad(path)` over the same state leaves the file holding exactly
  `{"id": 1}`. See decision 6.

### Reading the target

- **AT-014**: Given the `target` section and the arguments of a row, a fake batch as
  `state["records"]`, `PostgresReader.read`, `BigQueryReader.read` and `UnityCatalogReader.read`
  replaced so that each records the reader and raises an exception `stop`, and the three writers'
  `write` replaced to record their calls, when `Process([Step("load", [SCD2(path,
  **arguments)])], state=state).run()` runs, then it raises `stop`, exactly one reader's `read` was
  called, once, on a reader whose state equals the reader twin's, and no writer's `write` was
  called. `s` is `state["records"].sparkSession`. AT-039 covers the writer twins.

  | `system` | Section, besides `system` | Arguments | Reader twin | Writer twin |
  |---|---|---|---|---|
  | `unity_catalog` | `"table_name": "main.gold.dim_customer"`, `"options": {"mergeSchema": true}` | none | `UnityCatalogReader("main.gold.dim_customer", spark=s)` | `UnityCatalogWriter("main.gold.dim_customer", options={"mergeSchema": True})` |
  | `postgres` | `"table_name": "public.dim_customer"`, `"options": {"batchsize": 1000}` | `connection=conn` | `PostgresReader("public.dim_customer", conn, spark=s)` | `PostgresWriter("public.dim_customer", conn, options={"batchsize": 1000})` |
  | `bigquery` | `"table_name": "acme-data.gold.dim_customer"`, `"billing_project": "acme-billing"`, `"options": {"temporaryGcsBucket": "acme-tmp"}` | `client=bq` | `BigQueryReader("acme-data.gold.dim_customer", billing_project="acme-billing", client=bq, spark=s)` | `BigQueryWriter("acme-data.gold.dim_customer", billing_project="acme-billing", options={"temporaryGcsBucket": "acme-tmp"}, client=bq)` |

  See decision 25.
- **AT-015**: Given a Parquet table `spark_catalog.default.dim_customer` holding `Ana`, the target
  section `{"system": "unity_catalog", "table_name": "spark_catalog.default.dim_customer"}`, the
  real `UnityCatalogReader.read` wrapped so that it records the `sparkSession` of the DataFrame it
  returns, and a batch holding `(2, "Bruno", "Porto", 2026-02-01)` built in `other =
  spark.newSession()` while `spark` stays the active session, when a process runs `SCD2(path)`,
  then the recorded session is `other`, and the written rows are `Ana` and
  `(2, "Bruno", "Porto", 2026-02-01, 2026-02-01, null)`. See decision 25.

### Versions

- **AT-016**: Given a target holding `Ana` and a batch holding `(2, "Bruno", "Porto",
  2026-02-01)`, when a process runs `SCD2(path)`, then the written rows are `Ana` and
  `(2, "Bruno", "Porto", 2026-02-01, 2026-02-01, null)`, and the reader's `read` was called once.
- **AT-017**: Given a target holding `Ana` and a batch holding `(1, "Ana", "Porto", 2026-03-01)`,
  when a process runs `SCD2(path)`, then the written rows are
  `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, 2026-03-01)` and
  `(1, "Ana", "Porto", 2026-03-01, 2026-03-01, null)`. Given the batch
  `(1, "Ana Maria", "Porto", 2026-03-01)`, the new version is
  `(1, "Ana Maria", "Porto", 2026-03-01, 2026-03-01, null)`.
- **AT-018**: Given a target holding `Ana` and a batch holding `(1, "Ana", "Lisbon", 2026-03-01)`,
  when a process runs `SCD2(path)`, then the written rows are exactly `Ana`: the current version
  keeps its `updated_at`, `valid_from` and null `valid_to`.
- **AT-019**: Given a target holding `(1, "Ana", "Faro", 2025-01-01, 2025-01-01, 2026-01-01)` and
  `Ana`, and a batch holding `(1, "Ana", "Porto", 2026-03-01)`, when a process runs `SCD2(path)`,
  then the written rows are `(1, "Ana", "Faro", 2025-01-01, 2025-01-01, 2026-01-01)`,
  `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, 2026-03-01)` and
  `(1, "Ana", "Porto", 2026-03-01, 2026-03-01, null)`. See decision 4.
- **AT-020**: Given a target holding `Ana` and `(2, "Bruno", "Porto", 2026-01-15, 2026-01-15,
  null)`, and a batch holding `(1, "Ana", "Braga", 2026-03-01)`, when a process runs `SCD2(path)`,
  then Bruno's row is written as it was read, and Ana's rows are those of AT-017 with `Braga` in
  place of `Porto`. See decision 2.
- **AT-021**: Given a target holding `Ana` and a batch holding, in this order,
  `(1, "Ana", "Braga", 2026-05-01)`, `(1, "Ana", "Porto", 2026-03-01)`,
  `(3, "Carla", "Faro", 2026-02-01)` and `(3, "Carla", "Evora", 2026-04-01)`, when a process runs
  `SCD2(path)`, then the written rows are `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01,
  2026-03-01)`, `(1, "Ana", "Porto", 2026-03-01, 2026-03-01, 2026-05-01)`,
  `(1, "Ana", "Braga", 2026-05-01, 2026-05-01, null)`, `(3, "Carla", "Faro", 2026-02-01,
  2026-02-01, 2026-04-01)` and `(3, "Carla", "Evora", 2026-04-01, 2026-04-01, null)`. The written
  rows are the same with the batch's rows in the reverse order, and with the batch repartitioned
  into four partitions. See decision 5.
- **AT-022**: Given a target holding `Ana` and a batch holding `(1, "Ana", "Lisbon", 2026-02-01)`,
  `(1, "Ana", "Porto", 2026-03-01)`, `(1, "Ana", "Porto", 2026-04-01)` and `(1, "Ana", "Lisbon",
  2026-05-01)`, when a process runs `SCD2(path)`, then the written rows are
  `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, 2026-03-01)`,
  `(1, "Ana", "Porto", 2026-03-01, 2026-03-01, 2026-05-01)` and
  `(1, "Ana", "Lisbon", 2026-05-01, 2026-05-01, null)`. Given the same four records in two runs,
  the first two then the last two, each run's target being the rows the previous run wrote, the
  rows written by the second run are the same. See decision 30.
- **AT-023**: Given the default `scd2` section plus `"tracked_columns": ["city"]` and a target
  holding `Ana`: a batch holding `(1, "Ana Maria", "Lisbon", 2026-03-01)` makes the written rows
  exactly `Ana`; a batch holding `(1, "Ana Maria", "Porto", 2026-03-01)` makes them
  `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, 2026-03-01)` and
  `(1, "Ana Maria", "Porto", 2026-03-01, 2026-03-01, null)`. See decision 20.
- **AT-024**: Given a target holding `(1, "Ana", null, 2026-01-01, 2026-01-01, null)` and
  `(2, "Bruno", "Porto", 2026-01-01, 2026-01-01, null)`, and a batch holding
  `(1, "Ana", null, 2026-03-01)` and `(2, "Bruno", null, 2026-03-01)`, when a process runs
  `SCD2(path)`, then Ana's row is written as it was read, and Bruno's rows are
  `(2, "Bruno", "Porto", 2026-01-01, 2026-01-01, 2026-03-01)` and
  `(2, "Bruno", null, 2026-03-01, 2026-03-01, null)`. Given the batch `(1, "Ana", "Lisbon",
  2026-03-01)` instead, Ana's current version is closed at `2026-03-01` and a version with
  `Lisbon` opens. See decision 21.
- **AT-025**: Given the default `scd2` section plus `"valid_from_column": "effective_from"` and
  `"valid_to_column": "effective_to"`, and a target of schema `customer_id INT, name STRING, city
  STRING, updated_at DATE, effective_from DATE, effective_to DATE` holding
  `(1, "Ana", "Lisbon", 2026-01-01, 2026-01-01, null)`, when a process runs over the batch of
  AT-017, then `data.columns` are the target's and the written rows are AT-017's. With the default
  configuration, `data.columns` are exactly the target's six columns: no `is_current` column is
  written. See decision 19.
- **AT-026**: Given a target holding `Ana` and `(2, "Bruno", "Porto", 2026-01-15, 2026-01-15,
  null)`, and a batch with the batch schema and no rows, when a process runs `SCD2(path)`, then the
  written rows are exactly the target's two rows. See decision 12.
- **AT-027**: Given a target holding `Ana` and a batch holding `(1, "Ana", "Porto", 2026-03-01)`
  and `(2, "Bruno", "Faro", 2026-02-01)`, when a first process runs `SCD2(path)` and a second one,
  with a new `SCD2(path)` and the same batch, runs over the rows the first wrote, then the second
  run's written rows equal the first's. Given a batch holding `(1, "Ana", "Porto", 2026-03-01)`
  and `(1, "Ana", "Braga", 2026-04-01)`, the second run raises the late-arriving `ValueError` of
  AT-029 and does not call the writer. See decision 31.

### Checks

- **AT-028**: Given a target holding `Ana`: a batch holding `(1, "Ana", "Lisbon", 2026-01-01)`
  makes the written rows exactly `Ana`; a batch holding `(1, "Ana", "Porto", 2026-01-01)` makes
  `Process.run()` raise `ValueError("state['records'] has a record whose 'updated_at' equals the
  'valid_from' of its entity's current version, with other tracked values")`, with no note, and
  the writer is not called. With `"tracked_columns": ["city"]`, the batch
  `(1, "Ana Maria", "Lisbon", 2026-01-01)` makes the written rows exactly `Ana`. See decision 22.
- **AT-029**: Given a target holding `Ana` and a batch holding `(2, "Bruno", "Porto", 2026-02-01)`
  and `(1, "Ana", "Porto", 2025-12-01)`, when a process runs `SCD2(path)`, then `Process.run()`
  raises `ValueError("state['records'] has a record whose 'updated_at' is earlier than the
  'valid_from' of its entity's current version")`, with no note, and the writer is not called. A
  batch holding `(2, "Bruno", "Porto", 2025-12-01)` alone is written, as a new entity. See
  decision 4.
- **AT-030**: Given a target holding `Ana` and a batch holding `(1, "Ana", "Porto", 2026-03-01)`
  and `(1, "Ana", "Braga", 2026-03-01)`, when a process runs `SCD2(path)`, then `Process.run()`
  raises `ValueError("state['records'] has records with the same key columns and the same
  'updated_at'")`, with no note, and the writer is not called. The same holds for two identical
  records `(3, "Carla", "Faro", 2026-02-01)`. See decision 5.
- **AT-031**: Given a target holding `Ana`, when a process runs `SCD2(path)` over a batch holding
  `(null, "Ana", "Porto", 2026-03-01)`, then `Process.run()` raises
  `ValueError("state['records'] has a null in column 'customer_id'")`; over
  `(1, "Ana", "Porto", null)`, `ValueError("state['records'] has a null in column 'updated_at'")`;
  in both cases with no note, and the writer is not called. With `"key_columns": ["customer_id",
  "name"]` and the batch `(1, null, "Porto", null)`, the message names `name`. See decision 23.
- **AT-032**: Given a target holding `Ana`, when a process runs `SCD2(path)` over the batch on the
  left, which fails two checks, then `Process.run()` raises the `ValueError` on the right, and the
  writer is not called:

  | Batch | Error |
  |---|---|
  | `(null, "Bruno", "Porto", 2026-03-01)`, `(1, "Ana", "Porto", 2025-12-01)` | the null in `customer_id` of AT-031 |
  | `(3, "Carla", "Faro", 2026-02-01)` twice, `(1, "Ana", "Porto", 2025-12-01)` | the same key columns and time of AT-030 |
  | `(1, "Ana", "Porto", 2025-12-01)`, `(2, "Bruno", "Porto", 2026-01-01)`, with a target that also holds `(2, "Bruno", "Faro", 2026-01-01, 2026-01-01, null)` | the late-arriving record of AT-029 |

- **AT-033**: Given a fake batch with the columns on the left, when a process runs `SCD2(path)`
  with the `scd2` section of the row, then `Process.run()` raises the `ValueError` on the right,
  with no note, and neither the reader's `read` nor the writer is called:

  | Batch columns | `scd2` section | Message |
  |---|---|---|
  | `customer_id`, `name`, `city` | default | `state['records'] has no column 'updated_at', which scd2.effective_time_column names` |
  | the batch schema's | default plus `"tracked_columns": ["segment"]` | `state['records'] has no column 'segment', which scd2.tracked_columns names` |
  | the batch schema's plus `valid_to` | default | `state['records'] already has a column 'valid_to', which scd2.valid_to_column names` |
  | the batch schema's plus `VALID_FROM` | default | `state['records'] already has a column 'valid_from', which scd2.valid_from_column names` |

  Given a fake batch with the columns `Customer_ID`, `name`, `city`, `Updated_At` and a replaced
  `read` that records the reader and raises `stop`, the run raises `stop`: step 2 passes. AT-040
  covers the rows written from such a batch. See decisions 9 and 10.
- **AT-034**: Given a fake batch with the batch schema's columns and a fake target with the columns
  on the left, when a process runs `SCD2(path)`, then `Process.run()` raises the `ValueError` on the
  right, with no note, and the writer is not called:

  | Target columns | Error |
  |---|---|
  | `customer_id`, `name`, `updated_at`, `valid_from`, `valid_to` | `ValueError("the target has no column 'city'")` |
  | the target schema's without `valid_to` | `ValueError("the target has no column 'valid_to'")` |
  | the target schema's plus `segment` | `ValueError("the target has a column 'segment' that is neither a column of state['records'] nor a version column")` |

  AT-041 covers a target whose columns are in another order. See decision 33.
- **AT-035**: Given a target holding `Ana` and a batch whose only record is
  `(1, "Ana", "Porto", d)`, where `d` is `2026-03-01` until the replaced `UnityCatalogWriter.write`
  starts and `2025-12-01` afterwards (see [Notes for testing](#notes-for-testing)), when a process
  runs `SCD2(path)`, then the checks pass, `write` is called, the evaluation of `data` inside it
  raises an exception whose message contains `has a record whose 'updated_at' is earlier than the
  'valid_from' of its entity's current version`, and `Process.run()` raises that exception. See
  decision 32.

### Errors and the state

- **AT-036**: Given `state={}`, when a process runs `SCD2(path)`, then `Process.run()` raises
  `KeyError` whose `args` are `("records",)`, with no note, and neither the reader's `read` nor the
  writer is called. Given `SCD2(path, key="customers")` and `state={"records": batch}`, where
  `batch` is a fake batch, the `args` are `("customers",)`.
- **AT-037**: Given a fake batch and a replaced `UnityCatalogReader.read` that raises an exception
  `err`, when a process runs `SCD2(path)`, then `Process.run()` raises `err` itself, with no note,
  and the writer is not called. AT-042 covers a failing write and a missing table. See decision 24.
- **AT-038**: Given `batch` and `other = object()`, and `state={"records": batch, "other":
  other}`, when a process runs `SCD2(path)` as in AT-016, then `process.state` has exactly the keys
  `records` and `other`, `process.state["records"] is batch` and `process.state["other"] is other`.
  See decision 15.

### Parts that need Spark of the criteria above

Each of these takes the part of AT-014, AT-033, AT-034 or AT-037 that runs Spark's computation, so
that those criteria run without Spark (decision 34).

- **AT-039**: Given the `target` section and the arguments of a row of AT-014, and a batch holding
  `(2, "Bruno", "Porto", 2026-02-01)`, when `Process([Step("load", [SCD2(path, **arguments)])],
  state=state).run()` runs with `PostgresReader.read`, `BigQueryReader.read` and
  `UnityCatalogReader.read` returning a target that holds `Ana`, and the three writers' `write`
  replaced as for `UnityCatalogWriter`, then exactly one writer's `write` was called, once, with
  `replace=True`, on a writer whose state equals the writer twin of the row. See decisions 7 and
  25.
- **AT-040**: Given a batch whose columns are `Customer_ID INT, name STRING, city STRING,
  Updated_At DATE` and a target holding `Ana`, when a process runs `SCD2(path)` over the batch of
  AT-017 written with these column names, then the written rows are AT-017's. See decision 9.
- **AT-041**: Given the batch of AT-016 and a target of schema `valid_to DATE, customer_id INT,
  valid_from DATE, name STRING, city STRING, updated_at DATE` holding `Ana`, when a process runs
  `SCD2(path)`, then `data.columns` are in this order and the written rows are AT-016's, in that
  column order. See decision 16.
- **AT-042**: Given a replaced `UnityCatalogWriter.write` that raises an exception `err`, when a
  process runs `SCD2(path)` as in AT-016, then `Process.run()` raises `err` itself, with no note.
  Given the real `UnityCatalogReader` and the target section `{"system": "unity_catalog",
  "table_name": "spark_catalog.default.missing"}`, naming no table, `Process.run()` raises Spark's
  `AnalysisException` and the writer is not called. See decision 24.

## Decisions

Taken by the maintainer on 2026-10-23. The maintainer chose each option from a list and gave no
reason beyond the choice, except for decision 6, whose reason they gave on 2026-10-24. The
trade-off recorded under decision 1 is the one presented with the options, not a reason of the
maintainer's. Where an ADR needs the rationale of one of the others, it is `TODO(maintainer)`.

1. **The changes are applied by an atomic rewrite** (draft question 1). SCD2 reads the whole target
   with the reader of the target's system, computes the new table in Spark (the closed versions,
   the unchanged current versions, the closed former current versions and the new versions) and
   writes it with `write(data, replace=True)`, as `FullLoad` does. No connector gets a new
   capability, and there is no Delta `MERGE`. Presented trade-off: every run rewrites the whole
   dimension.
2. **An entity that is current in the target but absent from the batch stays as it is:**
   incremental semantics (draft question 5). Deletes in the source are not detected. There is no
   snapshot mode and no option for one.
3. **The effective time comes from a column of the record,** not from `process.started_at` (draft
   question 3). The name of that column is a parameter of the callback (decision 6).
4. **A late-arriving record fails the process before anything is written** (draft question 6). A
   record whose effective time is earlier than the `valid_from` of its entity's current version in
   the target raises, and the target stays as it was. Closed versions never change.
5. **Several records with the same business key in one batch each become a version,** chained in
   the order of the effective-time column, so that every intermediate state stays queryable (draft
   question 7). Two records with the same business key and the same effective time fail the
   process before anything is written.
6. **SCD2's own parameters go in a member of the job's configuration file, outside `source` and
   `target`:** the key columns, the effective-time column, and the tracked columns and the names of
   the version columns if those are parameters at all. The callback reads that member itself; they
   are not constructor arguments. The configuration still has no mode (decision 13 of
   config_connectors, in the part that stands). This contradicts decision 11 of
   config_connectors, under which every callback ignores every top-level member other than its own
   section: decision 29 names the member and plans ADR 0017, and decision 18 records the part of
   decision 11 that is superseded. Reason, given by the maintainer on 2026-10-24: these parameters
   are part of the job's configuration, so they belong in its configuration file, which makes the
   callback more customisable for whoever writes the job.

Carried over, not reopened: the philosophy, under which SCD2 is a default callback and is
essential; ADR 0011 (essential by default); ADR 0014 (a writer creates a missing target only in the
form the system derives from the data, and `PostgresWriter` and `BigQueryWriter` create none); ADR
0015 (the closed table, no connector instance, configuration errors when the callback is built, the
callback arguments); ADR 0016 (each write strategy is one callback that builds its writer from the
`target` section; no mode); and the decisions of [config_connectors](config_connectors.md) that
apply to the `target` section: 1, 4, 10, 12, 15, 16 and 22 to 24, and 11 except as decision 18 says.

Proposed by this spec. Each follows from a rule of the repository, given with it:

7. **`SCD2` builds its writer as `AppendOnly` does,** through the same private routine, with the
   section `target`, the same nine steps and the same messages, except that the names valid at step
   4 are the supported systems (decision 26). An unsupported system therefore gets the
   configuration's own message, with the narrowed list. Reason: one way to build a writer from a
   `target` section (append_only decision 16).
8. **The `scd2` section follows the rules of a section:** strict keys, JSON types checked, `null`
   never a value, a `ValueError` prefixed with the path, raised when the callback is built, after
   the `target` section and the writer. Reason: config_connectors decisions 10, 11, 22 and 23,
   which make a mistake in the file stop the job before anything runs.
9. **Column names are compared ignoring case,** within the section and against the columns of the
   batch and the target. Reason: Spark resolves columns case-insensitively unless the session sets
   `spark.sql.caseSensitive`, as the Unity Catalog spec records, and `BigQueryWriter` matches
   columns case-insensitively, so two names that differ only in case name one column.
10. **The callback checks the columns of the batch and of the target itself,** before the checks
    job, with its own `ValueError`s. Reason: they need only the schemas, which run no Spark job,
    and Spark would otherwise accept a batch that already holds a version column, whose values
    `withColumn` would silently replace.
11. **No value of the data appears in an error message;** messages name columns. Reason: the run's
    log writes the message of a failure ([lifecycle](../docs/concepts/lifecycle.md)), and the
    batch's key values can be personal data.
12. **Every run writes,** an empty batch and a batch that changes nothing included. Reason: decision
    1 ("every run rewrites the whole dimension") and ADR 0016's rule that an empty batch calls the
    writer; the callback counts nothing to decide whether to write.
13. **The callback trusts the target's invariants** (at most one current version per entity, no
    null in its key columns or `valid_from_column`) and does not check them. Reason: checking them
    is one more aggregation over the whole dimension in every run, for invariants that a target
    only `SCD2` writes keeps. Recorded as a known limitation.
14. **The callback never checks the type of `state[key]`.** Reason: ADR 0016's rule that the write
    callbacks never type-check the value; and Spark Connect has a DataFrame class of its own,
    `pyspark.sql.connect.dataframe.DataFrame`, which an `isinstance` check on
    `pyspark.sql.DataFrame` would have to account for.
15. **The callback implements only `run`, catches nothing, logs nothing of its own and leaves the
    state as it found it,** as `AppendOnly` and `FullLoad` do (append_only decisions 8 and 18).
16. **The written DataFrame has the target's columns, in the target's order.** Reason: every
    supported writer matches columns by name, and the target's order is the table's.
17. **The four checks run in one Spark job, before the write.** Reason: each evaluation reads the
    target and the batch again, a billed read on BigQuery; one job bounds a run to two
    evaluations, the checks and the write.
18. **This spec supersedes decision 11 of config_connectors in part:** the top level of the file is
    the job's except for the `scd2` section, which `SCD2` reads. The main session records it in the
    History of config_connectors, which keeps its status; the planner does not edit it. No test is
    superseded: `ReadSource`, `AppendOnly` and `FullLoad` still ignore every member other than their
    own section, `scd2` included (AT-013), as `tests/test_config_connectors.py` and
    `tests/test_append_only_callback.py` check.

Taken by the maintainer on 2026-10-23, answering the sixteen open questions of the planner's first
revision (questions 1 to 16 became decisions 19 to 34). The maintainer chose each option as written
and gave no reason of their own; the reasons given with decisions 19 to 33 are the technical
reasons that the question gave from the repository. The maintainer changed decision 34 on
2026-10-24, with the measured reasons recorded under it:

19. **Two version columns, `null` as the open end, no `is_current`** (question 1). Two columns are
    added to every record, `valid_from` and `valid_to`, holding values of the effective-time
    column's type, with `null` as the open end of the current version, and no `is_current` column.
    Their names are the optional `valid_from_column` and `valid_to_column` of the `scd2` section.
    The effective-time column stays as it is, so in a new version it equals `valid_from`. The
    callback checks no type. Reason: the effective-time column's type is the job's (decision 3),
    and `null` is the only open end valid for every type it can have: a far-future value would be
    one the library chooses per type, and there is no natural one for a string column. With `null`
    as the open end, `valid_to IS NULL` is the current version, so `is_current` holds nothing the
    other two lack, and the callback would have to say which one wins when they disagree in the
    target it reads. The names are parameters because `postgres` and `bigquery` targets are
    created by the job's DDL (ADR 0014), possibly under other names, and a batch may already hold a
    column named `valid_from`. Keeping the effective-time column unchanged means the callback adds
    columns and changes none of the record's.
20. **An optional `tracked_columns`; a change in an untracked column is ignored** (question 2).
    Without the list, every column of the batch except the key columns and the effective-time
    column is tracked. A change in an untracked column is ignored: the current version keeps the
    values it had when it opened. Reason: without the list, the common case needs no parameter. The
    list lets a job leave out columns that change on every run, such as the lineage columns of the
    callbacks page's example or a reader's `_metadata`, which would otherwise open a version on
    every run. Overwriting in place is SCD type 1, another write strategy, which ADR 0016 makes a
    callback of its own; ignoring keeps one rule: a version's row is written when it opens, and only
    its `valid_to` changes, once, when it closes.
21. **Changes are detected by a null-safe comparison of the tracked values,** in the same job, with
    no stored hash (question 3). Reason: decision 1 reads the whole target on every run, so a
    stored hash saves no read. A hash would be a third added column whose value depends on the hash
    function and on how each type is encoded, and it would go stale when `tracked_columns` changes.
    The null-safe comparison counts two nulls as equal, so a null that stays null opens no version.
22. **A record whose effective time equals its current version's `valid_from`** does nothing when
    its tracked values are the same, and fails the process before anything is written when they
    differ (question 4). Reason: a different record at the current version's own time would either
    change a version that a previous run wrote, which decision 4 refuses for earlier times and
    decision 5 for equal times within a batch, or open a version whose `valid_from` equals its
    `valid_to`. Failing keeps the target as it was, as decisions 4 and 5 do. Doing nothing when the
    values are equal keeps a re-run of a batch with one record per entity without effect.
23. **A null in a key column or in the effective-time column fails the process before anything is
    written,** naming the column (question 5). Reason: a null key identifies no entity: Spark's
    equality never matches a null, so such a record would open a new current version on every run.
    A null effective time can be neither compared with `valid_from` (decision 4) nor chained
    (decision 5), and as `valid_from` it would leave the version's interval undefined. Failing
    before the write follows decisions 4 and 5; dropping a record would lose data silently.
24. **On the first run, a missing target fails the process on every system,** with the reader's own
    error, before anything is written; the job creates the dimension once, empty, with the batch's
    columns and the version columns (question 6). Reason: the callback can learn that the target is
    missing only from the error of its reader, whose type differs by system (Spark's
    `AnalysisException`, the JDBC driver's error, the BigQuery connector's error). Telling a missing
    table from an unreachable one would mean matching the errors of three systems, or asking the
    system directly, which is the connector's role and not the callback's. `postgres` and
    `bigquery` need the job's DDL anyway (ADR 0014), since their writers create nothing.
25. **The target's reader is the `source` row of the closed table for the target's system,** built
    when the process runs, with `table_name` (and `billing_project` for `bigquery`) from the
    `target` section, the callback arguments `connection` and `client`, and
    `spark=state[key].sparkSession`. The section's `options` go to the writer only. `SCD2` takes no
    `spark` argument (question 7). Reason: the rewrite is correct only if the read returns the whole
    target, and read options passed through to Spark can restrict it: Delta's `versionAsOf` or
    `timestampAsOf` would read an old version, which the rewrite would then write over the current
    one, and the writer accepts such keys in its `options`. The reader and the writer also reserve
    different option keys (`filter` is reserved by `BigQueryReader`, not by `BigQueryWriter`). The
    computation joins the batch with the target, so both must belong to one session; reading with
    the batch's own session guarantees it, where the active session or a `spark` argument could
    name another. The reader's parameters are the writer's, so building it when the process runs
    can raise nothing that building the writer did not.
26. **The supported target systems are `unity_catalog`, `postgres` and `bigquery`;** any other raises
    when the callback is built (question 8). Reason: as [Target systems](#target-systems) shows from
    the code, the three writers replace the target atomically, and `PostgresWriter` and
    `BigQueryWriter` evaluate the DataFrame into a staging table before they touch the target; the
    Delta case rests on the integration test. The file readers reject `delta`, so a file target
    would be in a plain format, whose replace deletes everything under the path before writing, is
    not atomic, and may delete the files that the plan still has to read. `json_lines` has no
    reader.
27. **`SCD2(config, *, key="records", connection=None, client=None)`, in
    `ingestio/callback/io.py`, exported by `ingestio` and `ingestio.callback.all`, essential**
    (question 9). Reason: `SCD2` is the philosophy's name for the strategy, in CapWords with the
    acronym in capitals as PEP 8 asks, as `AppendOnly` is the philosophy's "append-only". The
    module's docstring makes it the home of the callbacks that move data between the configured
    systems and the state, and it holds the other write strategies (append_only decision 6), with
    which `SCD2` shares the private routine; the module is not public, so the choice changes no
    import path. The signature is `AppendOnly`'s, because SCD2 needs exactly its arguments: the
    file, the state key, and the target's `connection` and `client`, which the reader takes too. It
    is essential by the philosophy.
28. **The full rewrite leans towards batch without closing the door to streaming:** one line in ADR
    0012's consequences records it, and no ADR is needed (question 10). Reason: each run reads the
    whole batch and the whole target and rewrites the dimension, as the connectors that ADR 0012
    lists lean towards batch. It changes neither the `Reader` and `Writer` protocols nor their
    payload, and under ADR 0016 a streaming SCD2, such as one applying each micro-batch with a
    merge, would be another write strategy, a callback of its own, beside this one. Nothing in this
    spec stands in its way.
29. **The member is a top-level section `scd2`,** with `key_columns`, `effective_time_column`,
    `tracked_columns`, `valid_from_column` and `valid_to_column`, as in [The `scd2`
    section](#the-scd2-section). ADR 0017 amends ADR 0015 and states the general rule: a library
    callback with parameters of its own reads them from one top-level section named after its
    class in snake case (question 11). Reason: `scd2` is the callback's class name in snake case,
    the rule by which the `system` names come from the connector classes (config_connectors
    decision 7). The keys end in `_column` or `_columns` like `watermark_column`, the
    configuration's existing key for a column name, and `key_columns` avoids `keys`, because the
    callback's `key` argument already names a key of the state. The general rule makes the
    top-level names that the library reserves a closed set derived from its callbacks, so a job
    knows which names its own members must avoid. A new ADR is needed because ADR 0015 records the
    shape of the file; ADR 0016 amended ADR 0015 for a narrower change.
30. **The change rule applies along the chain:** a record whose tracked values equal those of the
    version before it in the chain opens no version, so two consecutive records with the same
    tracked values give one version (question 12). Reason: otherwise the target would depend on how
    the same records were split across runs: in two runs, the second of two identical records is
    unchanged against the current version and adds nothing; in one batch it would add a version
    that changes nothing. With this rule the result is the same however the records are batched,
    and "every intermediate state" of decision 5 stays queryable, each state once.
31. **Re-running a batch that holds several records of one entity fails, as decision 4 says**
    (question 13): its first records are earlier than the current version that its last record
    opened.
    Reason: the checks compare each record with its entity's current version only, one row per
    entity, where skipping records identical to a version of the target would need a comparison with
    every version of the entity. A failure leaves the target unchanged, so a re-run loses nothing,
    and a batch with one record per entity re-runs without effect.
32. **The checks of decisions 4 and 5 run in a Spark job before the write, and the DataFrame written
    carries the same conditions as a guard** (question 14). The job before the write raises the
    `ValueError`; the guard makes the write's evaluation fail with Spark's exception. Reason: with
    the job alone, the write evaluates the batch again, and a batch that changed in between, such as
    a full read of a live table or a late commit within a reader's frozen bound, would be written
    unchecked and could break the chain with overlapping versions, silently. With the guard alone,
    the writer is called before the failure and loads its staging table, and the error is Spark's
    exception rather than a `ValueError`, where decisions 4 and 5 say "before anything is written".
    Both together give the `ValueError` before the writer is called, and make the write fail, with
    the target unchanged, in the rare case where the data changed. Materialising the batch once
    depends on a method that not every runtime the connectors claim may offer, and holds the batch
    on the executors.
33. **The target has exactly the batch's columns and the two version columns,** compared by name
    ignoring case; anything else fails before the write. Surrogate keys are out of scope: a version
    is identified by its key columns and its `valid_from` (question 15). Reason: the new table is the
    union, by name, of the target's rows and the new versions, so both need the same columns.
    Otherwise the callback would decide the content of a column the batch does not hold, and schema
    changes, which ADR 0014 leaves to the job's DDL, would happen as a side effect of a write. A
    surrogate key generated on insert would be generated again for every row on every run of a
    rewrite (decision 1), and one generated by the callback would be a format choice of its own.
34. **The criteria that need Spark are opt-in,** skipped unless `INGESTIO_SPARK_TESTS` is `1`,
    following the convention of the connectors' integration tests (`tests/test_disk_connectors.py`,
    its `integration` marker and its skip reason). CI runs them: the maintainer adds
    `INGESTIO_SPARK_TESTS: "1"` to `.github/workflows/ci.yml`, a change of the maintainer's, outside
    the flow. Locally they are skipped. Taken by the maintainer on 2026-10-24, in place of the answer
    to question 16 of 2026-10-23, under which these criteria ran in every run of the suite. Reasons,
    measured or read from a file:
    - The development environment has only Java 27 (`/usr/lib/jvm/java-27-openjdk`). On it, the
      locked pyspark 4.2.0 cannot start a local session: `java.lang.ClassNotFoundException:
      jdk.internal.ref.Cleaner` from `org.apache.spark.unsafe.Platform`, as the main session measured
      on 2026-10-24. No container runtime is available either.
    - GitHub's `ubuntu-latest` runner (Ubuntu 24.04) ships Java 17 as its default, and 21 too, per
      `images/ubuntu/Ubuntu2404-Readme.md` in actions/runner-images. So CI needs no Java setup step,
      only the variable.
    - Under the earlier answer, the suite would fail in the development environment, and no stage
      of the flow could reach a passing suite there.

    What follows from it, recorded by this spec: the criteria are split into a set that runs without
    Spark and a set that needs it, keeping outside the opt-in set every criterion, or part of one,
    that the doubles can cover; steps 1 to 4 of [Running](#running) use only `columns` and
    `sparkSession`, so that their criteria run without Spark; the stages run what
    [Notes for testing](#notes-for-testing) says, and `docs/workflow.md` gets a line for skipped
    criteria.

None of these decisions changes the philosophy. Decision 6 changes the shape of the file that ADR
0015 records, so it needs ADR 0017 (decision 29). Decision 1 leans towards batch without closing
the door to streaming (decision 28).

### ADRs

The doc-keeper writes them in the documentation stage, from this spec; a reason it cannot trace to
the maintainer's words or to the repository is left as `TODO(maintainer)`, never invented.

- **New: ADR 0017, "Read a library callback's own parameters from a section named after it"**
  (decisions 6 and 29). Status accepted with this spec. Evidence: this spec, decisions 6 and 29,
  taken by the maintainer on 2026-10-23. It amends ADR 0015. It records:
  - **Context:** ADR 0015 shapes the file as one `source` and one `target` section, both holding a
    connector's parameters, and leaves the top level to the job: every other member is ignored by
    every callback (config_connectors decision 11). SCD2 has parameters that belong to neither the
    connector nor the run: the key columns, the effective-time column, the tracked columns, the
    names of the version columns. config_connectors' "Effects on the draft specs" left the choice
    between constructor arguments and members outside `source` and `target` to this spec.
  - **Decision:** SCD2 reads its parameters from the top-level `scd2` section, with the keys and
    rules of [The `scd2` section](#the-scd2-section), checked when the callback is built. They are
    not constructor arguments. No other callback reads the section, and SCD2 reads no member other
    than `target` and `scd2`. The section names no system and chooses no write strategy: the
    configuration still has no mode, and the callback placed in the step chooses the strategy. The
    top level stays the job's, except for the names of the library's sections, `source`, `target`
    and `scd2`. The general rule (decision 29): a library callback that has parameters of its own
    reads them from one top-level section named after its class in snake case, and no other
    callback reads it.
  - **Rationale:** for decision 6 itself, a section of the file rather than constructor arguments,
    the maintainer's reason recorded under decision 6. For the name and the
    shape of the section, the reasons of decision 29, which the doc-keeper may quote: `scd2` is the
    callback's class name in snake case, the rule by which the `system` names come from the
    connector classes (config_connectors decision 7); the keys end in `_column` or `_columns` like
    `watermark_column`, the configuration's existing key for a column name, and `key_columns`
    avoids `keys`, because the callback's `key` argument already names a key of the state; the
    general rule makes the top-level names that the library reserves a closed set derived from its
    callbacks, so a job knows which names its own members must avoid. A new ADR rather than a
    spec decision alone, because ADR 0015 records the shape of the file.
  - **Consequences:** ADR 0015's shape of the file has a third section, read by one callback;
    decision 11 of config_connectors is superseded in part; a job whose own top-level member is
    named `scd2` must rename it before placing `SCD2`; changing the key columns or the tracked
    columns is an edit of the JSON; one dimension per file; nothing closes the door to streaming.
- **ADR 0015, amended:** its "shape of the file" names the `scd2` section, read by `SCD2`; its
  Evidence line records the amendment by ADR 0017, and the index shows "accepted, amended by 0016,
  0017".
- **ADR 0012:** one line in its consequences, among what leans towards batch, as each connector spec
  added one (decision 28): "SCD2 ([spec](../../specs/scd2_callback.md)) leans towards batch: each
  run reads the whole batch and the whole target, checks the batch, and rewrites the dimension. A
  streaming SCD2, applying changes per micro-batch, would be another write strategy, a callback of
  its own (ADR 0016)."
- **Unchanged:** the philosophy, ADR 0003, ADR 0011, ADR 0013, ADR 0014 and ADR 0016.

## Open questions

None.

## History

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Drafted by the main session while setting up the workflow, as a first real case for the template. Not yet reviewed by the planner agent. |
| 2026-10-23 | planning | draft | Rewritten by the planner against ADR 0015, ADR 0016 and the Spark connectors, with the maintainer's decisions 1 to 6 of 2026-10-23 (atomic rewrite, incremental semantics, effective time from a column, late-arriving records fail, one version per record of an entity, parameters in a section of the file). `SCD2(config, *, key="records", connection=None, client=None)` reads a new top-level `scd2` section; 38 criteria, 12 proposed decisions, ADR 0017 planned, 16 open questions with one recommendation each. |
| 2026-10-23 | planning | draft | The maintainer answered the 16 open questions with the recommended option each; the planner recorded them as decisions 19 to 34 and left no open question. ADR 0017 now states the general rule (decision 29). No change to the behaviour or the criteria beyond recording the answers. |
| 2026-10-24 | planning | draft | Decision 34 replaced by the maintainer's decision of 2026-10-24: the criteria that need Spark are opt-in (`INGESTIO_SPARK_TESTS`), run in CI and skipped in the development environment, where pyspark 4.2.0 cannot start on Java 27. 18 criteria run without Spark, 24 are opt-in (AT-039 to AT-042 added). `docs/workflow.md` gets a rule for skipped criteria in the documentation stage. The Delta test behind `unity_catalog` support (decision 26) runs nowhere until `INGESTIO_DELTA_PACKAGE` is set in CI. |
| 2026-10-24 | planning | approved | Approved by the maintainer, with `unity_catalog` kept among the supported systems (decision 26) although the Delta integration test runs nowhere until `INGESTIO_DELTA_PACKAGE` is set in CI, and with the rule for skipped criteria that `docs/workflow.md` gets in the documentation stage. |
| 2026-10-24 | tests | tested | The test-writer wrote `tests/test_scd2_callback.py`, 158 cases for AT-001 to AT-042 and the Delta test of decision 26. The 114 always-run cases give 113 failures on the missing `SCD2` (`ImportError`) and 1 pass by design (AT-013: the other callbacks already ignore `scd2`). The 44 opt-in cases are collected and skipped (`INGESTIO_SPARK_TESTS`, plus `INGESTIO_DELTA_PACKAGE` for the Delta test, which runs in its own interpreter), and could not be executed in the development environment. Suite: 113 failed, 1429 passed, 123 skipped; ruff clean; mypy reports only the missing `SCD2`. |
| 2026-10-24 | documentation | documented | The doc-keeper documented `SCD2` in `docs/concepts/callbacks.md` and `docs/concepts/configuration.md` (behaviour, the `scd2` section, reserved top-level names, supported systems, checks, errors, limitations), wrote ADR 0017 with one `TODO(maintainer)` for decision 6, amended ADR 0015, added the line to ADR 0012, added the rule for skipped opt-in criteria to `docs/workflow.md`, and updated `docs/index.md` and `.claude/kb/`. `tests/test_docs_examples.py`: 25 passed, and the new runnable example in `callbacks.md` fails only on the missing `SCD2`. |
| 2026-10-24 | implementation | implemented | The implementer added `SCD2` to `callback/io.py`, with `build_scd2` and the checks of the `scd2` section in `callback/_configuration.py`, and exported it from `ingestio`, `basics` and `callback.all`. The new table is one window-based chain over the target's current versions and the batch; the four checks are one Spark job over a per-row code; the guard is a `raise_error` in the written `valid_from` column. Suite: 1543 passed, 123 skipped (all opt-in); ruff and mypy clean. The Spark computation, the checks and the guard were never executed, since the development environment has no usable JVM. |
| 2026-10-24 | validation | documented | Failed by the acceptance-validator, back to the implementer. All 114 always-run cases, the full suite (1543 passed, 123 opt-in skipped), the 26 documentation examples, ruff, mypy and the lock check pass, and the static review found the rows, the order and messages of the checks and the guard correct. Blocking: the written plan scans the target twice (current and closed versions), against the single-snapshot full read of the Target systems table, decision 17's two evaluations per run and the documented known limitation. The 24 opt-in criteria and the Delta test were not run (no usable JVM). |
| 2026-10-24 | implementation | implemented | After the validation return, the implementer made the written plan read the target in one scan: every target row enters the chain with a kind (current or closed), the windows partition by the key columns plus a closed flag, so closed versions stay out of the chain, the checks and the guard and are written as read. Only `_new_table` in `callback/io.py` changed. Suite: 1543 passed, 123 skipped (opt-in); ruff and mypy clean. |
| 2026-10-24 | validation | validated | Passed by the acceptance-validator after the return: all 114 always-run cases, the full suite (1543 passed, 123 opt-in skipped), the 26 documentation examples, ruff, mypy and the lock check pass; the checks job and the write each read the target once, closed versions are written as read, and the guard covers every check. The 24 opt-in criteria and the Delta test of decision 26 were not run (no usable JVM in the development environment); CI runs them once `INGESTIO_SPARK_TESTS` is set, and the Delta test also needs `INGESTIO_DELTA_PACKAGE`. Backlog idea 12 removed by the main session. |
| 2026-10-24 | validation | validated | After validation, by the main session at the maintainer's request: the maintainer gave the reason for decision 6 (the parameters are part of the job's configuration, so they belong in its configuration file, which makes the callback more customisable for whoever writes the job), recorded under decision 6 and in ADR 0017's Rationale in place of its `TODO(maintainer)`. Also updated: `.github/workflows/ci.yml` sets `INGESTIO_SPARK_TESTS` (decision 34), the README presents `SCD2` as shipped, and the philosophy and ADR 0016 no longer describe SCD2 as future work. |
