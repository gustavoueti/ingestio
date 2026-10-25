# Spec: write strategies as callbacks: `AppendOnly` and `FullLoad`

- **Status:** validated
- **Slug:** `append_only_callback`. The spec is `specs/append_only_callback.md` and its tests
  are `tests/test_append_only_callback.py`.
- **Related:** [philosophy](../docs/philosophy.md),
  [ADR 0004](../docs/decisions/0004-shared-process-state.md),
  [ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md),
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md),
  [ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md),
  [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md),
  [specs/config_connectors.md](config_connectors.md) (its "Effects on the draft specs"),
  [specs/scd2_callback.md](scd2_callback.md)

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

## Summary

Each write strategy becomes one callback. `AppendOnly(config, ...)` adds a value of the shared
state to the target that the `target` section of the job's configuration names, and
`FullLoad(config, ...)` makes that target hold exactly the value. Both build their writer from
the configuration as `WriteTarget` does today, through the private routine and the closed table
of [ADR 0015](../docs/decisions/0015-choose-connectors-from-the-job-configuration.md), and accept
no connector. `WriteTarget` leaves the public API. `JsonLinesWriter` loses `append=` and gets
`write(data, *, replace=False)`, like every other shipped writer, so that no connector decides a
write strategy.

The change breaks the public API; the package is alpha (0.3.0). The maintainer took decisions 1
to 4 on 2026-10-23, and decisions 5 to 14 the same day, answering the open questions of the
first revision.

This revision replaces the draft of 2026-10-22, written before
[config_connectors](config_connectors.md) and ADR 0015 landed in v0.3.0. It makes the changes
listed in that spec's "Effects on the draft specs": the callbacks take the path of the
configuration and build their writer from its `target` section; no connector instance is
accepted; the draft's AT-005 (a connector written in user code) and its idea of a new capability
protocol are gone; laziness over a generator is a criterion for `json_lines` only, because the
Spark writers take a DataFrame.

## Motivation

The [philosophy](../docs/philosophy.md) names append-only as a default callback, lists write
strategies among callbacks, and says a connector "does not decide write strategy". Today:

- The write strategy is an argument of one callback, `WriteTarget(config, *, replace=False)`,
  rather than a callback of its own.
- `JsonLinesWriter(path, *, append=False)` fixes the strategy when the writer is built, the
  opposite of every other shipped writer, whose `write(data, *, replace=False)` leaves the choice
  to its caller. `WriteTarget` hides this by building the writer with `append=not replace` and
  branching on the writer's type when it writes (`src/ingestio/callback/io.py`), and a job's own
  callback that builds a `JsonLinesWriter` still replaces by default: the "Known divergence" of
  [the connectors page](../docs/concepts/connectors.md#known-divergence).

The maintainer's reason for decision 1: each write strategy is one callback, as SCD2 will be.

## Fit with the philosophy

- **Concept.** `AppendOnly` and `FullLoad` are callbacks: each is a stage that performs one action,
  a write strategy, and the philosophy lists write strategies (append, merge, SCD2) among
  callbacks. `JsonLinesWriter` is a connector. The change takes the write strategy out of it:
  afterwards it only knows how to talk to its file and offers the same `write(data, *,
  replace=False)` as every other shipped writer, and the caller chooses.
- **No third concept.** The configuration routine and the closed table are the private
  implementation that ADR 0015 introduced, reused unchanged. There is no base class of write
  strategies, no registry of strategies and no mode in the configuration.
- **Essential by default.** Neither callback declares a criticality, so both are essential
  ([ADR 0011](../docs/decisions/0011-callbacks-as-stages-with-criticality.md)), as the philosophy
  requires of append-only. Nothing is caught: every error propagates.
- **Simplicity.** Two small callbacks, each one call to the writer. The type check that
  `WriteTarget.run` needs today, and the `append` parameter of the private routine, go away.
- **Compose, do not impose.** A job places the callback that matches its strategy in a step. A
  job's own callback can still build a shipped writer and call `write(data, replace=...)` itself.
- **No dependencies in the core.** The callbacks use nothing beyond the private routine; the
  extras do not change.
- **Streaming.** Nothing here closes the door. The callbacks hand the value to the writer as it
  is and never inspect it (decision 3), so a generator in the state reaches `JsonLinesWriter`
  unconsumed and is written record by record, which
  [ADR 0012](../docs/decisions/0012-streaming-deferred.md) lists among what keeps the door open.
- **Text that the change touches.** Nothing in the change contradicts the philosophy, but four
  texts are affected:
  1. ADR 0015 names `WriteTarget` in its Decision and Consequences: a new ADR 0016 amends it
     (decision 10).
  2. The philosophy names "append-only and SCD2" as the default callbacks, and `FullLoad` is a
     third shipped write strategy: the philosophy stays unchanged (decision 11).
  3. The "Known divergence" of the connectors page names two things `JsonLinesWriter` decides,
     the write strategy and the format. Decision 2 ends the first only, and the section narrows
     to the format (decision 12).
  4. Decisions 6 (for `WriteTarget`) and 13 of [config_connectors](config_connectors.md) are
     superseded by decisions 1 and 2 here (see [Superseded tests and
     criteria](#superseded-tests-and-criteria)).

## Scope

In scope:

- `AppendOnly` and `FullLoad`: signatures, building, running, errors, exports;
- the removal of `WriteTarget` from `ingestio`, `ingestio.callback.all` and `ingestio.basics`,
  and from the documentation and its examples;
- `JsonLinesWriter`: the removal of `append`, and `write(data, *, replace=False)`;
- the private routine of `ingestio/callback/_configuration.py` losing its `append` parameter;
- the ADR that records the change (decision 10);
- the tests and criteria that this spec supersedes.

Out of scope:

- `ReadSource`, which does not change;
- SCD2 ([its own spec](scd2_callback.md)), merges and upserts, and any new capability of a
  connector;
- deduplicating records or making a re-run idempotent;
- lineage columns, which are a callback of their own;
- any change to a connector other than `JsonLinesWriter`; the JSON Lines format itself
  (decision 12);
- making a `JsonLinesWriter` write atomic (decision 13);
- a write mode in the configuration: the configuration still has no mode
  ([config_connectors](config_connectors.md), decision 13, in the part that stands);
- the criticality mechanism, which has its own spec under ADR 0011;
- the README, which the maintainer updates, CI and the release.

## Public API

Added, in `ingestio/callback/io.py` (decision 6), exported by `ingestio` and by
`ingestio.callback.all` (decision 7):

```python
class AppendOnly(Callback):
    """Appends a value of the shared state to the target that the job's configuration names.

    What the target already holds stays as it is.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            system and holds the writer's parameters.
        key: Key of `Process.state` holding the value to write.
        connection: Where a PostgreSQL target lives, with its credentials. Required by the
            `postgres` system.
        client: SDK client of the target, for the systems that take one.
    """

    def __init__(
        self,
        config: str | os.PathLike[str],
        *,
        key: str = "records",
        connection: PostgresConnection | None = None,
        client: object | None = None,
    ) -> None: ...


class FullLoad(Callback):
    """Makes the target that the job's configuration names hold exactly a value of the state.

    Args:
        config: Path of the job's JSON configuration file. Its `target` section names the
            system and holds the writer's parameters.
        key: Key of `Process.state` holding the value to write.
        connection: Where a PostgreSQL target lives, with its credentials. Required by the
            `postgres` system.
        client: SDK client of the target, for the systems that take one.
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
  `WriteTarget` today: `os` is the standard module, `PostgresConnection` is
  `ingestio.connector.all.PostgresConnection`, and `client` is `object` because its type depends
  on the system.
- Neither takes `replace`, `spark` or `watermark_value`.
- Both subclass `Callback` directly. Neither is a subclass of the other, and no base class of
  write strategies is public.
- In the run's log they appear by their class name, `AppendOnly.run` and `FullLoad.run`.

Changed, `ingestio.connector.all.JsonLinesWriter`:

```python
class JsonLinesWriter:
    def __init__(self, path: Path | str, *, encoding: str = "utf-8") -> None: ...

    def write(self, data: Iterable[Mapping[str, Any]], *, replace: bool = False) -> None:
        """Write every record as one JSON line.

        Args:
            data: The records to write.
            replace: Make the file hold exactly the records of `data`. Appends when `False`.
        """
```

Removed:

- `WriteTarget`, from `ingestio`, `ingestio.callback.all` and their `__all__`, and from
  `ingestio.basics`. A job that imports it gets Python's `ImportError` or `AttributeError`
  (decision 9).
- The `append` argument of `JsonLinesWriter`.
- The default of `JsonLinesWriter.write(data)` changes from replacing the file to appending to
  it, as a consequence of decision 2.

Unchanged: `ReadSource`, every other connector, `PostgresConnection`, `Reader`, `Writer`,
`Callback`, `Process`, `Step`, the cancel exceptions, `log_to_console`, and their import paths.
`JsonLinesWriter` stays exported by `ingestio.connector.all` and still satisfies `Writer`
structurally.

### Mapping for the stages

A map from today's API to the new one, for the stages of this change. The documentation carries
no migration table: it describes `AppendOnly` and `FullLoad` without a migration section
(decision 14).

| Before | After |
|---|---|
| `WriteTarget(config)` | `AppendOnly(config)` |
| `WriteTarget(config, replace=False)` | `AppendOnly(config)` |
| `WriteTarget(config, replace=True)` | `FullLoad(config)` |
| `WriteTarget(config, key=..., connection=..., client=...)` | the same arguments, to `AppendOnly` or `FullLoad` |
| `JsonLinesWriter(path).write(data)`, which replaced | `JsonLinesWriter(path).write(data, replace=True)` |
| `JsonLinesWriter(path, append=True).write(data)` | `JsonLinesWriter(path).write(data)` |

## Behaviour

### Building

Building `AppendOnly(config, ...)` or `FullLoad(config, ...)` does what building `WriteTarget`
does today, through the same private routine and the same closed table, with the section
`target` and the callback arguments `connection` and `client`: the nine steps of
[config_connectors, "Building the callback"](config_connectors.md#building-the-callback), in
their order, the first problem found raising.

- The callback reads the file once, when it is built. Changing or deleting the file afterwards
  changes nothing. Each callback reads the file for itself.
- The `source` section and every other top-level member are ignored, whatever they hold.
- Building touches no Spark session, no client, no database and no network, whatever the system.
- A mistake in the file, a missing extra included, raises when the callback is built, before
  `Process` exists and before anything runs.

The target systems, unchanged from [the configuration page](../docs/concepts/configuration.md):

| `system` | Builds | Required keys | Optional keys | `connection` | `client` |
|---|---|---|---|---|---|
| `json_lines` | `JsonLinesWriter` | `path` | `encoding` | no | no |
| `disk` | `DiskWriter` | `path`, `format` | `options` | no | no |
| `postgres` | `PostgresWriter` | `table_name` | `options` | **required** | no |
| `s3` | `S3Writer` | `path`, `format` | `options` | no | no |
| `gcs` | `GcsWriter` | `path`, `format` | `options` | no | no |
| `volume` | `VolumeWriter` | `volume`, `path`, `format` | `options` | no | no |
| `unity_catalog` | `UnityCatalogWriter` | `table_name` | `options` | no | no |
| `bigquery` | `BigQueryWriter` | `table_name` | `billing_project`, `options` | no | optional: a `google.cloud.bigquery.Client` |

`append`, `replace` and `mode` are not keys of any system: like any other key a system does not
take, they are rejected.

### Running

- `AppendOnly.run()` calls `write(state[key])` on the writer it built.
- `FullLoad.run()` calls `write(state[key], replace=True)` on the writer it built.
- Both do so for every system, `json_lines` included, with no branch on the writer's type.
- The writer receives the very object that `state[key]` holds. The callback does not copy it,
  convert it, iterate it, take its length or truth value, count it or check its type (decision
  3). A value the writer cannot take fails in the writer, and its error propagates.
- The callbacks implement only `run`, catch nothing and write no log line of their own.

What the write does is the connector's, as [its section](../docs/concepts/connectors.md) says. In
short:

| `system` | `AppendOnly`: `write(data)` | `FullLoad`: `write(data, replace=True)` |
|---|---|---|
| `json_lines` | adds one line per record after what the file holds; creates the file and its parent directories when missing | empties the file, or creates it and its parent directories, then writes one line per record |
| `disk`, `s3`, `gcs`, `volume` | Spark's `append` mode: new files, the existing ones untouched | Spark's `overwrite` mode: everything under the path is removed first; one commit for `delta`, not atomic for plain files |
| `postgres` | `INSERT ... SELECT` from a staging table, in one transaction | `TRUNCATE`, then `INSERT ... SELECT`, in one transaction |
| `unity_catalog` | one Delta commit that adds the rows | one Delta commit that overwrites the table |
| `bigquery` | `INSERT ... SELECT` from a staging table, in one statement | a `MERGE` that inserts the new rows and deletes the others, in one statement |

### An empty batch

Decision 3: the callback calls the writer whatever the value holds, an empty list, an exhausted
generator or a DataFrame with no rows. It never checks: counting a DataFrame runs a Spark job,
and checking a generator consumes it. What an empty write does to the target is the connector's
([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md)
for a missing target). As the connectors behave today:

- `json_lines`: `AppendOnly` leaves an existing file as it is and creates a missing one, empty,
  with its parent directories; `FullLoad` leaves the file empty.
- The Spark writers receive the DataFrame as it is. The connectors' sections say what an empty
  one does: for `postgres` and `bigquery`, an append inserts nothing and a replace leaves the
  target empty.
- `UnityCatalogWriter` and `BigQueryWriter` reject a DataFrame with no columns with
  `ValueError("the DataFrame has no columns")`, when the process runs. A file reader's empty
  selection without `schema` returns such a DataFrame.
- So `FullLoad` over an empty batch empties the target. A job that keeps its target when a batch
  is empty places its own callback before the write, as the `SkipEmptyBatch` of the
  documentation does.

### The value in the state

Decision 8: the callbacks leave the state as they found it: `state[key]` keeps the same object, and no key is added or removed. A generator is
exhausted once it is written, so a later callback that reads the same key gets no records.

### Errors

`<path>` is `os.fspath(config)` and `<section>` is `target`. The errors are those of
[the configuration page, "Errors"](../docs/concepts/configuration.md#errors), unchanged, with
`<section>` as `target`:

| Case | Raised | Error |
|---|---|---|
| `config` is not a `str` or an `os.PathLike`, a connector instance included | building | `TypeError: config must be a str or os.PathLike, not <type name>` |
| the file is missing, a directory, unreadable, not UTF-8 | building | the `OSError` or `UnicodeDecodeError` of reading, unchanged |
| the JSON, the section, `system`, the keys, the types, the callback arguments | building | the `ValueError` starting `<path>: ` of the configuration page, such as `<path>: missing section 'target'` or `<path>: target system 'postgres' needs the argument connection` |
| the connector rejects a value, or its extra is not installed | building | the connector's `ValueError` or `ImportError`, the same object, with one note, `while building the target of <path>, system '<system>'` |
| an argument the callback does not take, such as `replace=True` | building | Python's `TypeError` for an unexpected keyword argument |
| `state[key]` is missing | running | `KeyError`, whose `args` are `(key,)`, with no note; the writer is not called (decision 4) |
| `write()` fails | running | the error, unchanged, the same object, with no note |

A job still written with `WriteTarget` fails when it imports it (decision 9):
`from ingestio import WriteTarget` and `from ingestio.callback.all import WriteTarget` raise
Python's `ImportError`, and `ingestio.WriteTarget` raises `AttributeError`.

### `JsonLinesWriter`

- `JsonLinesWriter(path, *, encoding="utf-8")`. `append` is not an argument any more: passing it
  raises `TypeError`.
- `write(data, *, replace=False)` creates the missing parent directories, then:
  - with `replace=False`, the default, adds one line per record after what the file holds, and
    creates the file when it is missing;
  - with `replace=True`, empties the file, or creates it, and writes one line per record.
- `replace` is keyword-only, as on every other shipped writer.
- What does not change: one JSON object per line, `ensure_ascii=False`, values JSON does not know
  serialised with `str`, the `encoding`, and `data` consumed once, record by record, as it is
  iterated.
- Consequence of decision 2: `JsonLinesWriter(path).write(data)`, which replaced the file, now
  appends to it. A job's own callback that relied on the old default writes more lines than it
  did, and nothing fails.
- In a configuration nothing changes: `"system": "json_lines"`, with the keys `path` and
  `encoding`.

### Where it lives

- `src/ingestio/callback/io.py`: `ReadSource` unchanged; `WriteTarget` removed; `AppendOnly` and
  `FullLoad` added (decision 6). The import of `JsonLinesWriter` and the type branch go.
- `src/ingestio/callback/_configuration.py`: `build_connector` loses its `append` parameter and
  the special case of `JsonLinesWriter`. The table, the checks and the messages do not change.
- `src/ingestio/connector/local.py`: `JsonLinesWriter`.
- `src/ingestio/callback/all.py`, `src/ingestio/basics.py` and `src/ingestio/__init__.py`: the
  exports.
- `src/ingestio/connector/all.py` and `pyproject.toml` do not change.

### What becomes outdated

The documentation stage updates `docs/` and `.claude/kb/`; the README is the maintainer's. The
documentation describes `AppendOnly` and `FullLoad` without a migration section (decision 14).

- `docs/concepts/callbacks.md`: the table of shipped callbacks, its example, and the sentence
  that append-only and SCD2 "will be" the default callbacks.
- `docs/concepts/configuration.md`: "Using it in a job", the signature block, "Append and
  replace", the "Not keys" bullet on `append`, the bullet "the configuration has no mode", the
  `WriteTarget` rows of "Errors", and both examples.
- `docs/concepts/connectors.md`: "Using a connector" (the paragraph on `ReadSource` and
  `WriteTarget`; its `WriteOnce(JsonLinesWriter(...))` example now appends), the `JsonLinesWriter`
  section, every sentence "`WriteTarget` calls `write(data)` ... unless it is given
  `replace=True`", every example that places a `WriteTarget` (the BigQuery example's
  `WriteTarget(SNAPSHOT, key="orders", replace=True)` becomes a `FullLoad`), and "Known
  divergence", which narrows to the format (decision 12).
- `docs/concepts/lifecycle.md`: the log examples that show `WriteTarget.run`.
- ADR 0015, ADR 0004 (its "`ReadSource` and `WriteTarget` use `"records"`"), and the new ADR
  0016 (decision 10). `docs/index.md`: the description of this spec, the callbacks page and the
  ADRs.
- `docs/backlog.md`, idea 12: its question on the name mentions `WriteTarget`.
- `.claude/kb/architecture/quick-reference.md`: modules, the configuration lines on
  `WriteTarget` and append/replace, every "`WriteTarget` appends" of the connector sections, and
  the known gap on `JsonLinesWriter(append=...)`.
- `README.md`, for the maintainer: every `WriteTarget`.

### Superseded tests and criteria

This spec supersedes (decision 21):

- In [config_connectors](config_connectors.md), the part of each criterion that builds a
  `WriteTarget`: AT-001 (its `WriteTarget` import), AT-002, AT-003, AT-004, AT-006, AT-007,
  AT-008, AT-009, AT-011 to AT-019 (their target rows), AT-020 (its writer rows), AT-024, AT-025,
  AT-026, AT-027, AT-028 and AT-029 (its write). Their `ReadSource` part stands. Its decisions 6,
  for `WriteTarget`, and 13, for the `replace` argument and `JsonLinesWriter(append=not
  replace)`, are superseded by decisions 1 and 2; the rest of decision 13, no mode in the
  configuration, stands.
- In `tests/test_config_connectors.py`, every case that builds a `WriteTarget`. The test-writer
  removes those cases and keeps every `ReadSource` case: a parametrisation over both callbacks
  keeps its `ReadSource` case, and a test that builds both keeps its `ReadSource` assertions.
- In `tests/test_connectors.py`, `test_writing_twice_replaces_by_default` and
  `test_append_mode_keeps_previous_records`, whose behaviour decision 2 changes. The other
  `JsonLinesWriter` tests there stand.

Where the superseded behaviour is covered here:

| Superseded in config_connectors | Covered by |
|---|---|
| AT-001, the `WriteTarget` import | AT-001 |
| AT-002 | AT-002 |
| AT-003, AT-004, AT-006, AT-007, target side | AT-014 |
| AT-008, `WriteTarget` ignores the source | AT-019 |
| AT-009, the file read once | AT-018 |
| AT-011 to AT-015, target rows | AT-015 |
| AT-016, AT-017, target rows | AT-016 |
| AT-018, AT-019, target rows | AT-017 |
| AT-020, writer rows | AT-018 |
| AT-024 | AT-005, AT-007 |
| AT-025 | AT-004, AT-006 |
| AT-026, `key` | AT-008 |
| AT-027 | AT-020 |
| AT-028 | AT-021 |
| AT-029, the write | AT-013 |

`tests/test_run_progress_log.py` uses `"WriteTarget"` only as the name of a stand-in callback in
its expected log lines and imports nothing: it is not superseded.

### Known limitations

- **An append is not idempotent.** Running the same batch twice appends it twice.
- **`FullLoad` over an empty batch empties the target.** See [An empty batch](#an-empty-batch).
- **A failed `json_lines` write leaves what was written before the failure** (decision 13).
  An append leaves part of the batch at the end of the file. A replace has already emptied the
  file, so the file holds only that part, and the previous content is lost.
- **`FullLoad` of plain files is not atomic** on `disk`, `s3`, `gcs` and `volume`, and deletes
  everything under the path, as their sections say. For plain files Spark also refuses to
  overwrite a path that the DataFrame being written reads from.
- **One target per file.** A job that appends to one target and replaces another uses two
  configuration files, one per callback.

### Notes for testing

- Tests write each configuration with `json.dumps` to a file under `tmp_path` and pass its path,
  as in `tests/test_config_connectors.py`. A test that needs invalid JSON writes the text itself.
- The doubles, twins and transcripts of `tests/test_config_connectors.py` (the call log, the fake
  Spark session and DataFrames, the fake BigQuery client, `psycopg.connect` replaced with
  `monkeypatch.setattr`) are reused: rebuilt in this spec's test file, or moved to a shared test
  module, as the test-writer chooses. The generated staging names, `ingestio_staging_` followed
  by 32 hexadecimal digits, and `BigQueryWriter`'s expiry time are compared as in that spec's
  "Notes for testing".
- `json_api` uses the `api_url` fixture; `json_lines` writes under `tmp_path`.
- A missing package is simulated in a subprocess, by mapping its top-level name to `None` in
  `sys.modules`.
- A *recording iterable* is an object whose `__iter__`, `__len__` and `__bool__` record each
  call; `__iter__` returns an iterator over its records.

## Acceptance criteria

Unless a criterion says otherwise, `path` is a configuration file under `tmp_path`, `<path>` is
`os.fspath(path)`, "either callback" means that the criterion holds for `AppendOnly` and for
`FullLoad`, each tested on its own, and every configuration error is raised by the call that
builds the callback, before any `Process` exists.

### Import surface and signatures

- **AT-001**: Given a fresh interpreter, when `from ingestio import AppendOnly, FullLoad` and
  `from ingestio.callback.all import AppendOnly, FullLoad` run, then both succeed and give the same
  classes; `"AppendOnly"` and `"FullLoad"` are in `ingestio.__all__` and in
  `ingestio.callback.all.__all__`; both are subclasses of `ingestio.Callback`, and neither is a
  subclass of the other. `ReadSource` is still exported by both. `"WriteTarget"` is in neither
  `__all__`; `from ingestio import WriteTarget` and `from ingestio.callback.all import WriteTarget`
  raise `ImportError`; `ingestio.WriteTarget` and `ingestio.callback.all.WriteTarget` raise
  `AttributeError`. See decisions 5, 7 and 9.
- **AT-002**: Given an interpreter where `pyspark`, `psycopg`, `boto3`, `google` and `databricks`
  are mapped to `None` in `sys.modules`, and a configuration with the target
  `{"system": "json_lines", "path": "<tmp>/out/orders.jsonl"}`, when `ingestio` is imported and
  a process runs either callback over `state={"records": [{"id": 1}]}`, then it succeeds and the
  file holds `{"id": 1}`.
- **AT-003**: Given either callback, then `inspect.signature` shows exactly `config`, positional
  or keyword, then the keyword-only `key="records"`, `connection=None` and `client=None`, in this
  order. Given a valid `json_lines` target, `AppendOnly(path, replace=True)`,
  `AppendOnly(path, replace=False)`, `FullLoad(path, replace=True)` and either callback given
  `key` positionally, `(path, "orders")`, raise `TypeError`.

### Appending

- **AT-004**: Given the target `{"system": "json_lines", "path": "<tmp>/out/orders.jsonl"}`, a
  file at that path holding the line `{"id": 0}`, and `state={"records": [{"id": 1}, {"id":
  2}]}`, when a process runs `AppendOnly(path)`, then the file holds `{"id": 0}`, `{"id": 1}` and
  `{"id": 2}`, one per line, in this order. When a second process, with a new `AppendOnly(path)`,
  runs over `[{"id": 3}]`, the file holds those three lines followed by `{"id": 3}`. Given no file
  and no `out` directory, the run creates both and the file holds the new records. Given
  `"encoding": "utf-16"` in the section, the file decodes as UTF-16.
- **AT-005**: Given the target section and the callback arguments of a row below, and
  `state={"orders": df}` where `df` is a fake DataFrame with `columns == ["id", "payload"]`, when
  `Process([Step("load", [AppendOnly(path, key="orders", **arguments)])], state=state).run()`
  runs, then the doubles record the same events, in the same order, as `twin.write(df)` over
  doubles in the same initial state, with the generated names and times compared as in [Notes
  for testing](#notes-for-testing). The DataFrame written is `df` itself, the calls on the BigQuery
  client are recorded on the `bq` given, and the parameters given to `psycopg.connect` are those
  of the `conn` given.

  | `system` | Section, besides `system` | Arguments | `twin` |
  |---|---|---|---|
  | `disk` | `"path": "<tmp>/lake/orders"`, `"format": "parquet"`, `"options": {"compression": "snappy"}` | none | `DiskWriter("<tmp>/lake/orders", "parquet", options={"compression": "snappy"})` |
  | `postgres` | `"table_name": "public.orders_copy"`, `"options": {"batchsize": 1000}` | `connection=conn` | `PostgresWriter("public.orders_copy", conn, options={"batchsize": 1000})` |
  | `s3` | `"path": "s3://acme-lake/orders"`, `"format": "delta"` | none | `S3Writer("s3://acme-lake/orders", "delta")` |
  | `gcs` | `"path": "gs://acme-lake/orders"`, `"format": "delta"` | none | `GcsWriter("gs://acme-lake/orders", "delta")` |
  | `volume` | `"volume": "main.lake.curated"`, `"path": "orders"`, `"format": "parquet"` | none | `VolumeWriter("main.lake.curated", "orders", "parquet")` |
  | `unity_catalog` | `"table_name": "main.silver.orders"`, `"options": {"mergeSchema": true}` | none | `UnityCatalogWriter("main.silver.orders", options={"mergeSchema": True})` |
  | `bigquery` | `"table_name": "acme-data.lake.orders"`, `"billing_project": "acme-billing"` | `client=bq` | `BigQueryWriter("acme-data.lake.orders", billing_project="acme-billing", client=bq)` |

### Replacing

- **AT-006**: Given the target and the file of AT-004, holding `{"id": 0}`, and
  `state={"records": [{"id": 1}, {"id": 2}]}`, when a process runs `FullLoad(path)`, then the file
  holds exactly `{"id": 1}` and `{"id": 2}`, in this order. When a second process, with a new
  `FullLoad(path)`, runs over `[{"id": 3}]`, the file holds exactly `{"id": 3}`. Given no file and
  no `out` directory, the run creates both and the file holds the new records.
- **AT-007**: Given each row of AT-005 and the same state, when
  `Process([Step("load", [FullLoad(path, key="orders", **arguments)])], state=state).run()` runs,
  then the doubles record the same events, in the same order, as `twin.write(df, replace=True)`,
  compared as in AT-005, and the DataFrame, the client and the connection are those given, as in
  AT-005.

### The value written

- **AT-008**: Given a `json_lines` target with no file, and
  `state={"raw": [{"id": 1}], "records": [{"id": 9}]}`, when a process runs either callback with
  `key="raw"`, then the file holds exactly `{"id": 1}`. Without `key`, the file holds exactly
  `{"id": 9}`.
- **AT-009**: Given `records = [{"id": 1}]`, `other = object()`,
  `state={"records": records, "other": other}` and a `json_lines` target, when a process runs
  either callback, then `process.state` has exactly the keys `records` and `other`,
  `process.state["records"] is records`, `process.state["other"] is other`, and `records` still
  equals `[{"id": 1}]`. Given the state and a row of AT-005, `process.state["orders"]` is `df` and
  the state has no other key. See decision 8.
- **AT-010**: Given a `json_lines` target whose file holds `{"id": 0}`:
  - given `state["records"]` a generator that yields `{"id": 1}` and `{"id": 2}`, when a process
    runs `AppendOnly(path)`, then the file holds `{"id": 0}`, `{"id": 1}` and `{"id": 2}`, and
    `next()` on the generator raises `StopIteration`. With `FullLoad(path)`, the file holds exactly
    `{"id": 1}` and `{"id": 2}`, and the generator is exhausted;
  - given `state["records"]` a recording iterable over `{"id": 1}` and `{"id": 2}`, when a process
    runs either callback, then `__iter__` was called exactly once, and `__len__` and `__bool__`
    never;
  - given `state["records"]` a generator that yields `{"id": 1}` and then raises an exception
    `err`, when a process runs `AppendOnly(path)`, then `Process.run()` raises `err` itself, with
    no note, and the file holds `{"id": 0}` and then `{"id": 1}`: the record yielded before the
    failure was written, so the generator was not turned into a list first. See decision 13.
- **AT-011**: Given an empty batch, the writer is called (decision 3):
  - given a `json_lines` target at `<tmp>/out/orders.jsonl` where neither the file nor `out`
    exists, and `state={"records": []}`, when a process runs `AppendOnly(path)`, then the file
    exists afterwards and is empty;
  - given the file holding `{"id": 0}` and `state={"records": []}`, `AppendOnly(path)` leaves it
    holding `{"id": 0}`, and `FullLoad(path)` leaves it existing and empty;
  - given the file holding `{"id": 0}` and an exhausted generator as `state["records"]`, the
    results are the same as with `[]`;
  - given the `disk` row of AT-005 and a fake DataFrame that records every attribute accessed on
    it, when a process runs `AppendOnly(path, key="orders")`, then the attributes accessed on it
    are exactly those that `twin.write(df)` accesses, in the same order; with `FullLoad`, those of
    `twin.write(df, replace=True)`. In particular, none of `count`, `isEmpty`, `first`, `head`,
    `take` and `collect` is accessed.
- **AT-012**: Given a `json_lines` target at `<tmp>/out/orders.jsonl` where `out` does not exist,
  and `state={}`, when the callback is built, then building succeeds; when the process runs, then
  `Process.run()` raises `KeyError` whose `args` are `("records",)`, with no note, and `out` still
  does not exist. Given `key="raw"` and `state={"records": [{"id": 1}]}`, the `KeyError`'s `args`
  are `("raw",)`. Both hold for either callback. See decision 4.
- **AT-013**: Given the `disk` row of AT-005 and a fake DataFrame whose save raises an exception
  `err`, when a process runs either callback with `key="orders"`, then `Process.run()` raises
  `err` itself, with no note added.

### The configuration

- **AT-014**: Given the `config` or the file on the left, when either callback is built, then it
  raises the error on the right:

  | Given | Error |
  |---|---|
  | `config` is `JsonLinesWriter("/data/out/orders.jsonl")` | `TypeError("config must be a str or os.PathLike, not JsonLinesWriter")` |
  | `config` is `b"job.json"`, or the dict `{"target": {"system": "json_lines", "path": "/data/out/orders.jsonl"}}` | the same `TypeError`, naming `bytes` or `dict` |
  | no file at `path` | `FileNotFoundError` |
  | `path` is a directory | `IsADirectoryError` (on Windows, the `PermissionError` of reading) |
  | the file holds `{"target": ` | `ValueError` whose message starts with `<path>: invalid JSON: ` and whose `__cause__` is a `json.JSONDecodeError` |
  | a valid `json_lines` target with `"encoding": NaN`, `Infinity` or `-Infinity` | `ValueError("<path>: invalid JSON: <constant> is not allowed")` |
  | the file holds `[]`, `"text"`, `1` or `null` | `ValueError("<path>: the configuration must be a JSON object")` |
  | the file holds `{}`, or only a valid `source` section | `ValueError("<path>: missing section 'target'")` |
  | `"target": []` or `"target": "json_lines"` | `ValueError("<path>: section 'target' must be a JSON object")` |

  Given a valid `json_lines` target, a `str` and a `pathlib.Path` naming the file are both
  accepted, and the process writes the records.
- **AT-015**: Given the `target` section on the left, when either callback is built, then it
  raises `ValueError` with the message on the right, prefixed with `<path>: `:

  | `target` section | Message |
  |---|---|
  | `{"path": "/data/out/orders.jsonl"}` | `target.system is required` |
  | `system` as `5`, `null` or `["json_lines"]`, with a valid `path` | `target.system must be a string` |
  | `system` as `mariadb`, `BigQuery`, `json_api` or `""` | `target.system '<name>' is not one of: bigquery, disk, gcs, json_lines, mysql, postgres, s3, unity_catalog, volume` |
  | a valid `json_lines` section plus `"append": true`, `"replace": true` or `"mode": "append"` | `target.<key> is not a key of system 'json_lines'` |
  | a valid `unity_catalog` section plus `"watermark_column": "updated_at"`, `"mode": "overwrite"` or `"replace": true` | `target.<key> is not a key of system 'unity_catalog'` |
  | a valid `s3` section plus `"client": "x"` or `"watermark_pattern": "x"` | `target.<key> is not a key of system 's3'` |
  | `json_lines` with `"path": 5` | `target.path must be a string` |
  | `json_lines` with a valid `path` and `"encoding": null` | `target.encoding must be a string` |
  | `unity_catalog` with a valid `table_name` and `"options": "mergeSchema"` | `target.options must be an object of strings, numbers and booleans` |
  | `disk` with valid keys and `"options": {"compression": null}` | `target.options must be an object of strings, numbers and booleans` |

  Given a section of each system of the table in [Building](#building) that lacks one of its
  required keys, for every required key, the message is
  `target.<key> is required for system '<system>'`; lacking all of them, it names the first in
  the table's order. Given two keys the system does not take, the message names the first in the
  order of the file; given such a key and a missing required key, it names the key the system
  does not take.
- **AT-016**: Given a `postgres` target and no `connection`, when either callback is built, then
  it raises `ValueError("<path>: target system 'postgres' needs the argument connection")`. Given
  the argument on the left, not `None`, it raises
  `ValueError("<path>: target system '<system>' takes no argument <name>")`:

  | `system` | Argument |
  |---|---|
  | `json_lines`, `disk`, `s3`, `gcs`, `volume`, `unity_catalog` | `connection`, `client` |
  | `postgres` | `client` |
  | `bigquery` | `connection` |

  Given both `connection` and `client` to `json_lines`, the message names `connection`. Given
  every one of these arguments as `None`, building succeeds, with `connection` given for
  `postgres`.
- **AT-017**: Given the `target` section on the left, when either callback is built, then it
  raises the connector's `ValueError` with the message on the right and exactly one note,
  `while building the target of <path>, system '<system>'`:

  | `target` section | Message |
  |---|---|
  | `{"system": "bigquery", "table_name": "acme.lake.orders", "options": {"table": "x"}}` | `option 'table' is set by the connector` |
  | `{"system": "unity_catalog", "table_name": "orders"}` | `table_name must be 'catalog.schema.table'` |
  | `{"system": "disk", "path": "lake/orders", "format": "parquet"}` | `path must be an absolute local path` |

  Given an interpreter where `psycopg` is mapped to `None` in `sys.modules`, a `postgres` target
  with `connection=conn` raises `ImportError("PostgresWriter needs psycopg: pip install
  'ingestio[postgres]'")`; where `google` is, a `bigquery` target raises
  `ImportError("BigQueryWriter needs google-cloud-bigquery: pip install 'ingestio[bigquery]'")`;
  each with exactly one note, `while building the target of <path>, system '<system>'`. In the
  same interpreters, an `s3`, `gcs` or `volume` target builds without error.
- **AT-018**: Given `pyspark.sql.SparkSession.active` replaced with a function that raises,
  `psycopg.connect` replaced with a recording double, and doubles that record every attribute
  accessed on them, when either callback is built for every row of AT-005, then each building
  succeeds, `active` is not called, and nothing is recorded. Given either callback built from a
  `json_lines` target at `<tmp>/a.jsonl`, and the configuration then rewritten to name
  `<tmp>/b.jsonl`, when the process runs, then `a.jsonl` holds the records and `b.jsonl` does not
  exist; given the configuration deleted after building, the process writes `a.jsonl` all the
  same.
- **AT-019**: Given a file holding `"source": 5`, `"primary_key": "id"`, `"job": {"owner":
  "data"}` and a valid `json_lines` target, when either callback is built and a process runs it
  over `state={"records": [{"id": 1}]}`, then it succeeds and the file holds `{"id": 1}`.

### Composition

- **AT-020**: Given one file with `{"source": {"system": "json_api", "url": "<api_url>"},
  "target": {"system": "json_lines", "path": "<tmp>/bronze/orders.jsonl"}}`, when
  `Process([Step("extract", [ReadSource(path)]), Step("load", [AppendOnly(path)])]).run()` runs,
  then the file holds one JSON line per record of the payload, in order, and `state["records"]`
  equals those records. When a second process, built the same way with new callbacks, runs, the
  file holds the payload twice. With `FullLoad(path)` in place of `AppendOnly(path)`, the file holds
  the payload once after the second run.
- **AT-021**: Given `{"source": {"system": "unity_catalog", "table_name": "main.bronze.orders"},
  "target": {"system": "unity_catalog", "table_name": "main.silver.orders"}}` and a fake session,
  when `ReadSource(path, spark=spark)` and either callback run in one process, then the only
  table read is `` `main`.`bronze`.`orders` `` and the only table saved is
  `` `main`.`silver`.`orders` ``, of the DataFrame the read returned.

### `JsonLinesWriter`

- **AT-022**: Given a file holding `{"id": 0}`, when `JsonLinesWriter(file).write([{"id": 1}])`
  runs, then the file holds `{"id": 0}` and then `{"id": 1}`; when the same writer then writes
  `[{"id": 2}]`, the file holds the three lines, in order. `write(data, replace=False)` behaves the
  same. Given a path whose file and parent directories are missing, `write(data)` creates them and
  the file holds the records. Given `write([])` and a missing file, the file exists afterwards and
  is empty.
- **AT-023**: Given a file holding `{"id": 0}`, when `JsonLinesWriter(file).write([{"id": 1}],
  replace=True)` runs, then the file holds exactly `{"id": 1}`; `write([], replace=True)` leaves
  it existing and empty; given a missing file and parent directories, `write(data,
  replace=True)` creates them. `write([{"id": 1}], True)`, with `replace` given positionally,
  raises `TypeError`.
- **AT-024**: Given a path, `JsonLinesWriter(path, append=True)` and
  `JsonLinesWriter(path, append=False)` raise `TypeError`. `inspect.signature(JsonLinesWriter)`
  shows exactly `path`, then the keyword-only `encoding="utf-8"`, and
  `inspect.signature(JsonLinesWriter.write)` exactly `self`, `data`, then the keyword-only
  `replace=False`. `JsonLinesWriter(path, encoding="utf-16").write([{"id": 1}])` writes a file
  that decodes as UTF-16.

## Decisions

Taken by the maintainer on 2026-10-23. They are fixed and are not reopened by this spec:

1. **Separate strategies.** A new callback for append-only, built like `WriteTarget` from the
   configuration: `AppendOnly(config, *, key="records", connection=None, client=None)` as the
   starting point, with no `replace` argument. `WriteTarget` is removed from the public API, and
   replace becomes a callback of its own. Each write strategy is one callback, as SCD2 will be.
   This breaks the public API; the package is alpha. The spec covers both the append-only
   callback and the replace callback, and the removal of `WriteTarget` (exports from `ingestio`
   and `ingestio.callback.all`, docs, examples). The maintainer's reason: each write strategy is
   one callback, as SCD2 will be.
2. **`JsonLinesWriter(append=...)` is removed.** `JsonLinesWriter` gets `write(data, *,
   replace=False)` like every other shipped writer, so no connector decides the write strategy
   any more and the "Known divergence" in `docs/concepts/connectors.md` ends. This breaks the
   public API.
   - *What follows from it, recorded by this spec:* `JsonLinesWriter(path).write(data)` appends,
     where it replaced. The divergence also names the format, which decision 2 does not touch:
     see decision 12.
3. **An empty batch always calls the writer.** The callback never inspects the payload: checking
   a DataFrame runs a Spark job, checking a generator consumes it. What an empty write does to
   the target is the connector's
   ([ADR 0014](../docs/decisions/0014-create-a-missing-target-only-when-derived-from-the-data.md)).
4. **A missing `state[key]` raises `KeyError`** when the process runs, as `WriteTarget` does
   today.

Taken by the maintainer on 2026-10-23, answering the open questions of the planner's first
revision (1 to 9) and one question asked separately (10):

5. **The callbacks are `AppendOnly` and `FullLoad`** (question 1).
   `FullLoad(config, *, key="records", connection=None, client=None)` calls
   `write(state[key], replace=True)`.
6. **They live in `ingestio/callback/io.py`,** in place of `WriteTarget` (question 2).
7. **Both are exported by `ingestio` and by `ingestio.callback.all`** (question 3).
8. **The callbacks leave the state unchanged** (question 4): `state[key]` keeps the same
   object, and no key is added or removed.
9. **`WriteTarget` is removed plainly** (question 5): `from ingestio import WriteTarget` and
   `from ingestio.callback.all import WriteTarget` raise Python's `ImportError`, and
   `ingestio.WriteTarget` raises `AttributeError`. The library is still in development, so its
   API can change.
10. **A new ADR 0016, "Make each write strategy one callback", amends ADR 0015** (question 6),
    as planned under [ADRs](#adrs).
11. **The philosophy stays unchanged** (question 7).
12. **The "Known divergence" narrows to the format** (question 8): the JSON Lines format stays a
    known divergence, out of this spec's scope.
13. **A failed `json_lines` write is unchanged** (question 9), and is documented as a known
    limitation.
14. **The documentation carries no migration table** from `WriteTarget` to `AppendOnly` and
    `FullLoad` (question 10): it describes `AppendOnly` and `FullLoad` without a migration
    section. The table under [Mapping for the stages](#mapping-for-the-stages) stays in this
    spec, as a map for the stages.

Carried over, not reopened: ADR 0015, and every decision of
[config_connectors](config_connectors.md) that applies to the `target` section, among them
decision 1 (no connector instance), 4 (only shipped connectors), 10 (errors when the callback is
built), 11 (strict sections), 12 (arguments a system does not take), 15 (one target per file) and
16 (the note on the connector's errors). Two are superseded by decisions 1 and 2: decision 6, for
`WriteTarget`, and decision 13, except its rule that the configuration has no mode, which stands.

Proposed by this spec:

15. **The two callbacks take what `WriteTarget` takes, without `replace`.** `config` positional;
    `key`, `connection` and `client` keyword-only, with the same types and defaults. `FullLoad`
    has the same signature as `AppendOnly`. Neither takes `spark` or `watermark_value`, because
    no writer does.
16. **They build their writer as `WriteTarget` does,** through the same private routine, with the
    section `target`; the table, the checks, the order and the messages do not change. The
    routine loses its `append` parameter, which no writer takes any more.
17. **`AppendOnly` calls `write(data)` and `FullLoad` calls `write(data, replace=True)`, for
    every system,** `json_lines` included, with no branch on the writer's type. What each does
    to the target is the connector's.
18. **The value is handed over as it is:** the very object, never copied, converted, iterated,
    counted or type-checked by the callback. The callbacks implement only `run`, catch nothing and
    log nothing of their own. This is what keeps a generator unconsumed until the writer iterates
    it (decision 3, ADR 0012).
19. **Both subclass `Callback` directly; neither subclasses the other,** and no base class of
    write strategies is public. Code they share stays private.
20. **Both are essential.** Neither declares a criticality (ADR 0011).
21. **This spec supersedes** the `WriteTarget` parts of config_connectors' criteria, the parts of
    its decisions 6 and 13 named above, the `WriteTarget` cases of
    `tests/test_config_connectors.py` and two tests of `tests/test_connectors.py`, as listed in
    [Superseded tests and criteria](#superseded-tests-and-criteria). The test-writer removes
    those cases. The main session records in the History of config_connectors that they are
    superseded by `append_only_callback`; that spec keeps its status, and the planner does not
    edit it. This follows decision 17 of config_connectors.
22. **The slug stays `append_only_callback`,** with its test file, although the spec covers
    `FullLoad` too: the index and config_connectors link to it under this name.

None of these decisions changes the philosophy or closes the door to streaming. Decisions 1 and
2 change the text of ADR 0015, which names `WriteTarget`: decision 10 records the change in a new
ADR 0016, which amends ADR 0015.

### ADRs

Decision 10. The doc-keeper writes them in the documentation stage, from this spec; a reason it
cannot trace to the maintainer's words or to the repository is left as `TODO(maintainer)`, never
invented.

- **New: ADR 0016, "Make each write strategy one callback".** Status accepted with this spec.
  Evidence: this spec, decisions 1 to 14, taken by the maintainer on 2026-10-23. It records:
  - **Context:** `WriteTarget` chose append or replace with its `replace` argument;
    `JsonLinesWriter(append=...)` fixed the strategy in a connector, against the philosophy, and
    `WriteTarget` branched on the writer's type to hide it; SCD2 is planned as a write strategy.
  - **Decision:** decisions 1 to 3. A write strategy the library adds later, such as SCD2 or a
    merge, is one more callback that builds its writer from the `target` section. Every shipped
    writer offers `write(data, *, replace=False)`, and the callback chooses. The configuration
    has no mode.
  - **Rationale:** the maintainer's reason for decision 1, "each write strategy is one callback,
    as SCD2 will be"; for decision 2, the purpose the maintainer stated, that no connector
    decides the write strategy any more, which is the philosophy's sentence on connectors; for
    decision 3, the reason the maintainer stated, that checking a DataFrame runs a Spark job and
    checking a generator consumes it; for the plain removal of `WriteTarget` (decision 9), that
    the library is still in development, so its API can change.
  - **Consequences:** the change breaks the public API; ADR 0015 is amended;
    `JsonLinesWriter(path).write(data)` now appends; `FullLoad` over an empty batch empties the
    target; the "Known divergence" narrows to the format (decision 12); nothing closes the door
    to streaming, because the callbacks hand the value over as it is and never inspect it.
- **ADR 0015, amended:** its Decision and Consequences name the library's write callbacks,
  `AppendOnly` and `FullLoad`, where they name `WriteTarget`: each reads the `target` section. Its
  Evidence line records the amendment by ADR 0016, and the index shows "accepted, amended by
  0016".
- **ADR 0004:** "`ReadSource` and `WriteTarget` use `"records"` by default" names the new
  callbacks. A wording fix, not an amendment.
- **Unchanged:** the philosophy (decision 11), ADR 0003, ADR
  0011, ADR 0012 (its "What keeps the door open" still holds; the doc-keeper may add that the
  write callbacks hand `state[key]` to the writer as it is), ADR 0013 and ADR 0014.

## Open questions

None.

## History

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-10-22 | planning | draft | Drafted by the main session while setting up the workflow, as a first real case for the template. Not yet reviewed by the planner agent. |
| 2026-10-23 | planning | draft | Revised by the planner after config_connectors and ADR 0015, with the maintainer's four decisions of 2026-10-23. Each write strategy is one callback built from the `target` section: `AppendOnly` calls `write(data)` and `Replace` calls `write(data, replace=True)`. `WriteTarget` is removed, and `JsonLinesWriter` gets `write(data, *, replace=False)` with no `append`. 24 criteria written without Spark, 12 decisions (4 the maintainer's), an ADR plan, the list of superseded tests and criteria, and 9 open questions. |
| 2026-10-23 | planning | draft | Revised by the planner with the maintainer's answers to the 9 open questions and one asked separately: the replace callback is `FullLoad`; the answers are decisions 5 to 14 and the proposed decisions are renumbered 15 to 22. The docs get no migration table; the `WriteTarget` mapping stays in the spec as "Mapping for the stages". No open questions remain. |
| 2026-10-23 | planning | approved | Approved by the main session under the maintainer's conditional pre-approval of 2026-10-23 ("approve if the revision is limited to these answers"), after checking that the diff of the revision touches only what the answers require. |
| 2026-10-23 | tests | tested | The test-writer wrote `tests/test_append_only_callback.py` (66 tests, 288 cases, AT-001 to AT-024): 284 fail for the missing `AppendOnly`/`FullLoad`, `WriteTarget` still exported and the old `JsonLinesWriter` API, and 4 pass by design because today's `JsonLinesWriter` already satisfies them. Following decision 21 it removed the 105 `WriteTarget` cases of `tests/test_config_connectors.py`, keeping every `ReadSource` case (5 tests renamed), and the two superseded `JsonLinesWriter` tests of `tests/test_connectors.py`. Suite: 284 failed, 1143 passed, 79 skipped; ruff clean; mypy errors only for the missing API. |
| 2026-10-23 | documentation | documented | The doc-keeper documented `AppendOnly` and `FullLoad`, in place of `WriteTarget`, and `JsonLinesWriter.write(data, *, replace=False)` in the callbacks, configuration, connectors and lifecycle pages, with no migration section (decision 14). It added ADR 0016 with five `TODO(maintainer)` markers, amended ADR 0015, fixed the wording of ADR 0004, and updated the index and the architecture KB. `docs/backlog.md` was left alone: its idea 12 was added by another session and is not committed. |
| 2026-10-23 | implementation | implemented | The implementer added `AppendOnly` and `FullLoad` in place of `WriteTarget` in `callback/io.py`, both built through the same private routine, which lost its `append` parameter and the `JsonLinesWriter` special case. `JsonLinesWriter` has `write(data, *, replace=False)` and no `append`. Exports updated in `ingestio`, `ingestio.callback.all` and `ingestio.basics`. Suite: 1428 passed, 79 skipped; ruff and mypy clean. |
| 2026-10-23 | validation | validated | The acceptance-validator passed all seven checks: AT-001 to AT-024 in 288 passing cases, full suite 1428 passed and 79 opt-in integration skips, all 25 documentation examples passing, and ruff, format, mypy strict and `uv lock --check` clean. The superseded `WriteTarget` cases match the spec's list and no `ReadSource` case was lost. Non-blocking notes: the uncommitted backlog idea 12 still names `WriteTarget`, the README is outdated, ADR 0016 has five `TODO(maintainer)` markers, and a few wording points in the docs. |
| 2026-10-23 | correction | validated | At the maintainer's request, the main session removed from decision 9, the ADR plan and ADR 0016 the remark that no job written with `WriteTarget` exists, which was an observation and not a reason. The reason recorded for the plain removal is the maintainer's: the library is still in development, so its API can change. |
| 2026-10-24 | amendment | validated | Amended by the main session, as [mysql_connectors](mysql_connectors.md) (What becomes outdated) asks, approved on 2026-10-24: `mysql` becomes a target system there, so the row of unknown systems names `mariadb` instead, which that spec does not support (its decision 41), and its message lists `mysql` among the targets. What the criterion checks is unchanged; the test-writer of `mysql_connectors` updates the test and `TARGET_NAMES` to match. |
